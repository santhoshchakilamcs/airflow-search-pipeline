"""Incremental extract: read one logical day (or a date range) from the
source file.

The source is data/daily_totals.csv, real NYC 311 daily contact totals
(731 days, 2023-01-01 to 2024-12-31, 6,681,493 contacts in total),
derived from the 6,681,493-contact event-level dataset in the
contact-center-forecasting repo. Each Airflow DAG run extracts only its
own logical date, so a daily run touches one row and a backfill touches
exactly the dates it covers.
"""

from __future__ import annotations

import pandas as pd

SOURCE_COLUMNS = ["day", "contacts"]


def _read_source(source_csv: str) -> pd.DataFrame:
    df = pd.read_csv(source_csv, parse_dates=["day"])
    return df.sort_values("day").reset_index(drop=True)


def extract_day(source_csv: str, logical_date: str) -> dict | None:
    """Return the source row for one logical date, or None if the source
    has no row for that date (a day with no data is a valid empty load,
    not an error)."""
    df = _read_source(source_csv)
    day = pd.Timestamp(logical_date).normalize()
    row = df[df["day"] == day]
    if row.empty:
        return None
    return {"day": row.iloc[0]["day"].date().isoformat(),
            "contacts": int(row.iloc[0]["contacts"])}


def extract_range(source_csv: str, start: str, end: str) -> list[dict]:
    """Return every source row in [start, end] inclusive, in date order.
    This is the backfill path: a backfill run extracts exactly the dates
    it covers, nothing more."""
    df = _read_source(source_csv)
    lo, hi = pd.Timestamp(start).normalize(), pd.Timestamp(end).normalize()
    window = df[(df["day"] >= lo) & (df["day"] <= hi)]
    return [{"day": r["day"].date().isoformat(), "contacts": int(r["contacts"])}
            for _, r in window.iterrows()]
