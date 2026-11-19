"""Data-quality contract checks, run after every load and every mart
rebuild. A failed check raises, so the DAG run fails loudly instead of
publishing bad data.

Checks:
1. no_duplicate_days - the primary key should make this impossible; the
   check exists so a manual or out-of-band write is caught.
2. no_missing_days - every calendar day between the first and last
   loaded day is present (an incremental load that silently skipped a
   day is a data bug, not an empty day).
3. non_negative_counts - contact counts are never negative.
4. watermark_matches - the watermark equals the newest loaded day.
5. mart_covers_all_days - the mart has one row per loaded day.
"""

from __future__ import annotations

import duckdb


def run_checks(con: duckdb.DuckDBPyConnection,
               dag_id: str) -> list[dict]:
    checks = []

    total, distinct = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT day) FROM contacts_daily"
    ).fetchone()
    checks.append({
        "name": "no_duplicate_days",
        "passed": total == distinct,
        "detail": f"{total} rows, {distinct} distinct days",
    })

    first, last = con.execute(
        "SELECT MIN(day), MAX(day) FROM contacts_daily").fetchone()
    if first is None:
        checks.append({"name": "no_missing_days", "passed": True,
                       "detail": "table is empty, nothing to check"})
    else:
        expected = (last - first).days + 1
        checks.append({
            "name": "no_missing_days",
            "passed": total == expected,
            "detail": f"{total} rows over {expected} calendar days "
                      f"({first} to {last})",
        })

    min_contacts = con.execute(
        "SELECT MIN(contacts) FROM contacts_daily").fetchone()[0]
    checks.append({
        "name": "non_negative_counts",
        "passed": min_contacts is None or min_contacts >= 0,
        "detail": f"minimum contacts = {min_contacts}",
    })

    watermark = con.execute(
        "SELECT max_day FROM pipeline_watermark WHERE dag_id = ?",
        [dag_id]).fetchone()
    watermark = watermark[0] if watermark else None
    checks.append({
        "name": "watermark_matches",
        "passed": watermark == last,
        "detail": f"watermark = {watermark}, newest loaded day = {last}",
    })

    mart_rows = con.execute(
        "SELECT COUNT(*) FROM contacts_daily_mart").fetchone()[0]
    checks.append({
        "name": "mart_covers_all_days",
        "passed": mart_rows == total,
        "detail": f"mart has {mart_rows} rows, warehouse has {total}",
    })

    return checks


def assert_all_passed(checks: list[dict]) -> None:
    failed = [c for c in checks if not c["passed"]]
    if failed:
        names = ", ".join(f"{c['name']} ({c['detail']})" for c in failed)
        raise ValueError(f"data-quality checks failed: {names}")
