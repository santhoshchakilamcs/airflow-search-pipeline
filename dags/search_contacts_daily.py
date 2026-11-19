"""Airflow DAG: daily incremental load of NYC 311 search-contact totals.

One DAG run handles exactly one logical date (``ds``):

    extract (one day from the source file)
        -> load (append into DuckDB, deduped, watermark updated)
        -> build_reporting_mart (7-day rolling average, day-over-day change)
        -> quality_checks (contract checks; the run fails if any check fails)

Incremental loads: each run reads only its own day, so a daily run is
cheap and a re-run of the same day is a no-op (the load dedupes on the
day and the watermark does not move twice).

Backfills: ``catchup=True`` with ``start_date`` 2023-01-01, so Airflow
backfills every day from the start date when the DAG is first turned
on, and ``airflow dags backfill search_contacts_daily -s <start> -e
<end>`` reprocesses any window on demand. A backfill that covers a
late-arriving day rebuilds the mart, so rolling averages after that day
pick up the correction.

Warehouse: DuckDB here (single file, ``CONTACTS_DUCKDB`` env var or
/tmp/airflow_search_pipeline/contacts.duckdb). In production the same
SQL runs against Snowflake or BigQuery; the DAG tasks would then run
against a shared warehouse instead of a local file.
"""

import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    # Airflow 3.x: airflow.decorators is deprecated.
    from airflow.sdk import dag, task
except ImportError:  # Airflow 2.x
    from airflow.decorators import dag, task

from pipeline import (
    assert_all_passed,
    build_mart,
    extract_day,
    init_warehouse,
    load_day,
    run_checks,
)

DAG_ID = "search_contacts_daily"
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SOURCE_CSV = os.path.join(PROJECT_ROOT, "data", "daily_totals.csv")
WAREHOUSE_PATH = os.environ.get(
    "CONTACTS_DUCKDB", "/tmp/airflow_search_pipeline/contacts.duckdb"
)

default_args = {
    "owner": "data",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id=DAG_ID,
    schedule="@daily",
    start_date=datetime(2023, 1, 1, tzinfo=timezone.utc),
    catchup=True,
    default_args=default_args,
    tags=["search", "incremental", "data-quality"],
    doc_md=__doc__,
)
def search_contacts_daily():
    @task()
    def extract(ds=None):
        return extract_day(SOURCE_CSV, ds)

    @task()
    def load(row):
        con = init_warehouse(WAREHOUSE_PATH)
        try:
            return load_day(con, DAG_ID, row)
        finally:
            con.close()

    @task()
    def build_reporting_mart(load_result):
        con = init_warehouse(WAREHOUSE_PATH)
        try:
            return build_mart(con)
        finally:
            con.close()

    @task()
    def quality_checks(mart_result):
        con = init_warehouse(WAREHOUSE_PATH)
        try:
            checks = run_checks(con, DAG_ID)
            assert_all_passed(checks)
            return checks
        finally:
            con.close()

    row = extract()
    loaded = load(row)
    mart = build_reporting_mart(loaded)
    quality_checks(mart)


search_contacts_daily = search_contacts_daily()
