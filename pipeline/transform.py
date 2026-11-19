"""Transform: rebuild the reporting mart from the loaded days.

The mart adds a 7-day rolling average and a day-over-day change to each
loaded day, so downstream consumers (search ranking evaluation, the
self-service dashboard) read one table instead of recomputing windows.
Rebuilding from the raw loaded table keeps the mart consistent after a
backfill: a late-arriving day changes the rolling averages for the days
after it, and the rebuild picks that up.
"""

from __future__ import annotations

import duckdb

MART_SQL = """
CREATE OR REPLACE TABLE contacts_daily_mart AS
SELECT
    day,
    contacts,
    AVG(contacts) OVER (
        ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS contacts_7d_avg,
    contacts - LAG(contacts) OVER (ORDER BY day) AS contacts_dod_change
FROM contacts_daily
ORDER BY day
"""


def build_mart(con: duckdb.DuckDBPyConnection) -> dict:
    """Rebuild the mart and return {"rows": int, "max_day": str|None}."""
    con.execute(MART_SQL)
    rows, max_day = con.execute(
        "SELECT COUNT(*), MAX(day) FROM contacts_daily_mart").fetchone()
    return {"rows": int(rows),
            "max_day": max_day.isoformat() if max_day else None}
