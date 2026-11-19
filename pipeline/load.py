"""Load: append one day (or a range) into the DuckDB warehouse, with
dedupe on the day and a watermark that records the newest day loaded.

The warehouse is DuckDB here; in production the same SQL runs against
Snowflake or BigQuery. Re-running a day is a no-op (the primary key on
day plus the existence check), so a retried or overlapping DAG run can
never double-count a day.
"""

from __future__ import annotations

from datetime import datetime, timezone

import duckdb

WAREHOUSE_SCHEMA = """
CREATE TABLE IF NOT EXISTS contacts_daily (
    day DATE PRIMARY KEY,
    contacts INTEGER NOT NULL,
    loaded_at TIMESTAMP DEFAULT current_timestamp
);
CREATE TABLE IF NOT EXISTS pipeline_watermark (
    dag_id VARCHAR PRIMARY KEY,
    max_day DATE,
    updated_at TIMESTAMP DEFAULT current_timestamp
);
"""


def init_warehouse(db_path: str) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(db_path)
    con.execute(WAREHOUSE_SCHEMA)
    return con


def load_day(con: duckdb.DuckDBPyConnection, dag_id: str,
             row: dict | None) -> dict:
    """Load one extracted day. Returns {"inserted": bool, "day": str|None,
    "watermark": str|None}. A None row (no source data for the day) still
    advances nothing and is reported, not treated as a failure."""
    if row is None:
        return {"inserted": False, "day": None,
                "watermark": get_watermark(con, dag_id)}
    existing = con.execute(
        "SELECT COUNT(*) FROM contacts_daily WHERE day = ?",
        [row["day"]]).fetchone()[0]
    if existing:
        return {"inserted": False, "day": row["day"],
                "watermark": get_watermark(con, dag_id)}
    con.execute("INSERT INTO contacts_daily (day, contacts) VALUES (?, ?)",
                [row["day"], row["contacts"]])
    _set_watermark(con, dag_id)
    return {"inserted": True, "day": row["day"],
            "watermark": get_watermark(con, dag_id)}


def load_range(con: duckdb.DuckDBPyConnection, dag_id: str,
               rows: list[dict]) -> dict:
    """Load a backfill window. Returns {"inserted": int, "skipped": int,
    "watermark": str|None}."""
    inserted, skipped = 0, 0
    for row in rows:
        result = load_day(con, dag_id, row)
        if result["inserted"]:
            inserted += 1
        else:
            skipped += 1
    return {"inserted": inserted, "skipped": skipped,
            "watermark": get_watermark(con, dag_id)}


def get_watermark(con: duckdb.DuckDBPyConnection,
                  dag_id: str) -> str | None:
    row = con.execute(
        "SELECT max_day FROM pipeline_watermark WHERE dag_id = ?",
        [dag_id]).fetchone()
    return row[0].isoformat() if row and row[0] is not None else None


def _set_watermark(con: duckdb.DuckDBPyConnection, dag_id: str) -> None:
    max_day = con.execute("SELECT MAX(day) FROM contacts_daily").fetchone()[0]
    now = datetime.now(timezone.utc)
    con.execute(
        """INSERT INTO pipeline_watermark (dag_id, max_day, updated_at)
           VALUES (?, ?, ?)
           ON CONFLICT (dag_id) DO UPDATE
           SET max_day = excluded.max_day,
               updated_at = excluded.updated_at""",
        [dag_id, max_day, now])
