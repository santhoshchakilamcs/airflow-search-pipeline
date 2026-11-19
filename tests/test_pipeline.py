"""Unit tests for the pipeline logic. No Airflow needed: these exercise
the same functions the DAG tasks call.
"""

import pandas as pd
import pytest

from pipeline import (
    assert_all_passed,
    build_mart,
    extract_day,
    extract_range,
    get_watermark,
    init_warehouse,
    load_day,
    load_range,
    run_checks,
)

DAG_ID = "search_contacts_daily"
SOURCE = "data/daily_totals.csv"


@pytest.fixture()
def con(tmp_path):
    db = init_warehouse(str(tmp_path / "test.duckdb"))
    yield db
    db.close()


def test_extract_day_returns_the_right_row():
    row = extract_day(SOURCE, "2023-01-01")
    assert row == {"day": "2023-01-01", "contacts": 6209}


def test_extract_day_outside_range_is_empty_not_an_error():
    assert extract_day(SOURCE, "2022-12-31") is None
    assert extract_day(SOURCE, "2025-01-01") is None


def test_extract_range_is_ordered_and_exact():
    rows = extract_range(SOURCE, "2023-01-01", "2023-01-03")
    assert [r["day"] for r in rows] == ["2023-01-01", "2023-01-02", "2023-01-03"]
    assert [r["contacts"] for r in rows] == [6209, 6680, 8255]


def test_load_day_inserts_and_advances_watermark(con):
    result = load_day(con, DAG_ID, {"day": "2023-01-01", "contacts": 6209})
    assert result == {"inserted": True, "day": "2023-01-01",
                      "watermark": "2023-01-01"}
    assert get_watermark(con, DAG_ID) == "2023-01-01"


def test_load_day_is_idempotent(con):
    row = {"day": "2023-01-01", "contacts": 6209}
    assert load_day(con, DAG_ID, row)["inserted"] is True
    assert load_day(con, DAG_ID, row)["inserted"] is False
    count = con.execute("SELECT COUNT(*) FROM contacts_daily").fetchone()[0]
    assert count == 1


def test_load_range_backfill_and_replay(con):
    rows = extract_range(SOURCE, "2023-06-01", "2023-06-30")
    first = load_range(con, DAG_ID, rows)
    assert first == {"inserted": 30, "skipped": 0, "watermark": "2023-06-30"}
    replay = load_range(con, DAG_ID, rows)
    assert replay == {"inserted": 0, "skipped": 30, "watermark": "2023-06-30"}
    assert con.execute("SELECT COUNT(*) FROM contacts_daily").fetchone()[0] == 30


def test_mart_rolling_average_matches_pandas(con):
    load_range(con, DAG_ID, extract_range(SOURCE, "2023-01-01", "2023-01-07"))
    mart = build_mart(con)
    assert mart == {"rows": 7, "max_day": "2023-01-07"}
    got = con.execute(
        "SELECT contacts_7d_avg FROM contacts_daily_mart "
        "WHERE day = DATE '2023-01-07'").fetchone()[0]
    source = pd.read_csv(SOURCE, parse_dates=["day"])
    expected = source[(source["day"] >= "2023-01-01")
                      & (source["day"] <= "2023-01-07")]["contacts"].mean()
    assert abs(got - expected) < 1e-6


def test_quality_checks_pass_on_clean_load(con):
    load_range(con, DAG_ID, extract_range(SOURCE, "2024-01-01", "2024-01-07"))
    build_mart(con)
    checks = run_checks(con, DAG_ID)
    assert all(c["passed"] for c in checks)
    assert_all_passed(checks)


def test_quality_checks_catch_a_missing_day(con):
    load_day(con, DAG_ID, {"day": "2024-01-01", "contacts": 7269})
    load_day(con, DAG_ID, {"day": "2024-01-03", "contacts": 9517})
    build_mart(con)
    checks = {c["name"]: c for c in run_checks(con, DAG_ID)}
    assert checks["no_missing_days"]["passed"] is False
    with pytest.raises(ValueError, match="no_missing_days"):
        assert_all_passed(run_checks(con, DAG_ID))


def test_duplicate_day_is_blocked_by_the_primary_key(con):
    load_range(con, DAG_ID, extract_range(SOURCE, "2024-01-01", "2024-01-03"))
    # A manual write of the same day must fail at the database, not
    # silently double-count.
    with pytest.raises(Exception):
        con.execute(
            "INSERT INTO contacts_daily (day, contacts) "
            "VALUES ('2024-01-02', 9999)")
    assert con.execute("SELECT COUNT(*) FROM contacts_daily").fetchone()[0] == 3
    build_mart(con)
    checks = {c["name"]: c for c in run_checks(con, DAG_ID)}
    assert checks["no_duplicate_days"]["passed"] is True


def test_quality_checks_catch_a_duplicate_day(tmp_path):
    # The check itself is exercised against a table built without the
    # primary key, the way a corrupted warehouse table would look.
    import duckdb

    db = duckdb.connect(str(tmp_path / "dup.duckdb"))
    db.execute(
        "CREATE TABLE contacts_daily (day DATE, contacts INTEGER, "
        "loaded_at TIMESTAMP DEFAULT current_timestamp)")
    db.execute(
        "CREATE TABLE pipeline_watermark (dag_id VARCHAR PRIMARY KEY, "
        "max_day DATE, updated_at TIMESTAMP DEFAULT current_timestamp)")
    db.execute(
        "INSERT INTO contacts_daily (day, contacts) VALUES "
        "('2024-01-01', 7269), ('2024-01-02', 9517), ('2024-01-02', 9999)")
    db.execute(
        "INSERT INTO pipeline_watermark (dag_id, max_day) VALUES "
        "('search_contacts_daily', '2024-01-02')")
    build_mart(db)
    checks = {c["name"]: c for c in run_checks(db, DAG_ID)}
    assert checks["no_duplicate_days"]["passed"] is False
    db.close()
