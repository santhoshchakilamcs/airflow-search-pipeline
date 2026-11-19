#!/usr/bin/env python3
"""End-to-end verification: run the pipeline the way the DAG would,
without an Airflow scheduler.

1. Seven daily runs (2024-01-01 to 2024-01-07), one day at a time,
   exactly as seven daily DAG runs would load them.
2. A 30-day backfill (2023-06-01 to 2023-06-30) on a fresh warehouse.
3. Idempotency: replay the same 7 days and confirm nothing is
   double-counted.
4. A DagBag parse of the DAG when Airflow is installed.

Every number printed is computed from data/daily_totals.csv and
asserted against the warehouse, so a mismatch fails the script.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import duckdb

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
SOURCE = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                      "data", "daily_totals.csv")


def _check_block(label, con):
    checks = run_checks(con, DAG_ID)
    for c in checks:
        print(f"  [{ 'PASS' if c['passed'] else 'FAIL'}] {c['name']}: {c['detail']}")
    assert_all_passed(checks)
    print(f"  {label}: all {len(checks)} checks passed")


def main() -> int:
    print("== 1. Seven daily DAG runs, 2024-01-01 to 2024-01-07 ==")
    con = init_warehouse(":memory:")
    total = 0
    for day in ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04",
                "2024-01-05", "2024-01-06", "2024-01-07"]:
        row = extract_day(SOURCE, day)
        result = load_day(con, DAG_ID, row)
        assert result["inserted"] is True, f"{day} was not inserted"
        total += row["contacts"]
    print(f"  loaded 7 days, {total} contacts, "
          f"watermark = {get_watermark(con, DAG_ID)}")
    assert total == 60496, f"7-day total mismatch: {total}"
    mart = build_mart(con)
    print(f"  mart: {mart['rows']} rows, max day {mart['max_day']}")
    avg = con.execute(
        "SELECT contacts_7d_avg FROM contacts_daily_mart "
        "WHERE day = DATE '2024-01-07'").fetchone()[0]
    print(f"  7-day rolling average on 2024-01-07: {avg:.1f}")
    assert abs(avg - total / 7) < 1e-6
    _check_block("daily runs", con)
    con.close()

    print("== 2. 30-day backfill, 2023-06-01 to 2023-06-30 ==")
    con = init_warehouse(":memory:")
    rows = extract_range(SOURCE, "2023-06-01", "2023-06-30")
    result = load_range(con, DAG_ID, rows)
    print(f"  backfill: inserted {result['inserted']}, "
          f"skipped {result['skipped']}, watermark = {result['watermark']}")
    assert result == {"inserted": 30, "skipped": 0, "watermark": "2023-06-30"}
    backfill_total = sum(r["contacts"] for r in rows)
    stored = con.execute("SELECT SUM(contacts) FROM contacts_daily").fetchone()[0]
    print(f"  backfill total contacts: {stored}")
    assert stored == backfill_total == 267168, f"total mismatch: {stored}"
    build_mart(con)
    _check_block("backfill", con)
    con.close()

    print("== 3. Replay is idempotent (no double-count) ==")
    con = init_warehouse(":memory:")
    rows = extract_range(SOURCE, "2024-01-01", "2024-01-07")
    load_range(con, DAG_ID, rows)
    replay = load_range(con, DAG_ID, rows)
    count = con.execute("SELECT COUNT(*) FROM contacts_daily").fetchone()[0]
    print(f"  replay: inserted {replay['inserted']}, skipped {replay['skipped']}, "
          f"rows still {count}")
    assert replay["inserted"] == 0 and count == 7
    con.close()

    print("== 4. DAG parse ==")
    try:
        from airflow.models import DagBag
        bag = DagBag(dag_folder=os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "dags"))
        assert not bag.import_errors, bag.import_errors
        # bag.dags is the parsed DAGs dict; get_dag() needs an initialized
        # metadata DB, which this repo does not require.
        dag = bag.dags.get(DAG_ID)
        print(f"  DagBag parsed {DAG_ID}: tasks "
              f"{sorted(dag.task_dict.keys())}, schedule {dag.schedule}, "
              f"catchup {dag.catchup}")
    except ImportError:
        print("  Airflow is not installed here; DAG structure is covered "
              "by tests/test_dag.py via the test-only stand-in, not by a "
              "real DagBag parse.")

    print("ALL VERIFICATION PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
