# Airflow Search Pipeline

Incremental search-data pipelines with Apache Airflow: daily incremental
loads, backfills, and data-quality contract checks on real NYC 311
contact data.

Built to close a real gap: a job description for a Senior Data Scientist
role asked for working knowledge of batch pipelines with Airflow
(incremental loads, backfills, and the order changes must ship in).
There was no Airflow project on the author's GitHub, so this one was
built from scratch.

## What it does

One DAG, `search_contacts_daily`, loads one logical day per run:

```
extract (one day from the source file)
    -> load (append into DuckDB, deduped, watermark updated)
    -> build_reporting_mart (7-day rolling average, day-over-day change)
    -> quality_checks (contract checks; the run fails if any check fails)
```

- **Incremental loads.** Each run reads only its own day, so a daily
  run is cheap. Re-running a day is a no-op: the load dedupes on the
  day (primary key) and the watermark does not move twice.
- **Backfills.** `catchup=True` with `start_date` 2023-01-01, so Airflow
  backfills every day from the start date when the DAG is first turned
  on, and `airflow dags backfill search_contacts_daily -s <start> -e
  <end>` reprocesses any window on demand. A backfill that covers a
  late-arriving day rebuilds the mart, so rolling averages after that
  day pick up the correction.
- **Data-quality contract checks.** Every run checks for duplicate days,
  missing days in the loaded range, negative counts, a watermark that
  matches the newest loaded day, and a mart that covers all loaded days.
  A failed check raises, so the run fails loudly instead of publishing
  bad data.

## Data

`data/daily_totals.csv` — real NYC 311 daily contact totals: 731 days
(2023-01-01 to 2024-12-31), 6,681,493 contacts in total, mean 9,140.2
per day (min 5,596, max 17,962), no duplicates, no nulls. It is derived
from the 6,681,493-contact event-level dataset in the
[contact-center-forecasting-insights](https://github.com/santhoshchakilamcs/contact-center-forecasting-insights)
repo. The full event-level file is not committed here; the daily totals
are the incremental source the DAG loads.

## Verified numbers

Run with `python verify_results.py` (13 pytest tests also pass;
`pytest tests/`):

- **Seven daily DAG runs, 2024-01-01 to 2024-01-07:** 60,496 contacts
  loaded, watermark 2024-01-07, 7-day rolling average 8,642.3 on
  2024-01-07, all 5 quality checks passed.
- **30-day backfill, 2023-06-01 to 2023-06-30:** 30 rows inserted,
  0 skipped, 267,168 contacts, watermark 2023-06-30, all 5 quality
  checks passed.
- **Replay is idempotent:** re-running the same 7 days inserted 0 rows
  and skipped 7; the table still holds 7 rows.
- **DAG parse:** `DagBag` (Apache Airflow 3.4.0) parses
  `search_contacts_daily` with no import errors: tasks
  `extract`, `load`, `build_reporting_mart`, `quality_checks`,
  schedule `@daily`, `catchup=True`.

## Run it

```bash
python -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/python -m pytest tests/ -q
.venv/bin/python verify_results.py
```

To run the DAG in a real Airflow (3.x):

```bash
export AIRFLOW_HOME=/path/to/airflow-home
airflow db migrate
cp -r dags "$AIRFLOW_HOME/dags"   # or point dags_folder at this repo's dags/
airflow dags list | grep search_contacts_daily
airflow dags backfill search_contacts_daily -s 2023-01-01 -e 2023-01-31
```

The warehouse path is the `CONTACTS_DUCKDB` env var
(default `/tmp/airflow_search_pipeline/contacts.duckdb`).

## Honest limits

- DuckDB stands in for Snowflake or BigQuery; the same SQL runs against
  either. The demo runs all tasks against one DuckDB file on a single
  node; in production the tasks would write to a shared warehouse.
- The source is daily totals, not the full 6.68M event rows. The
  event-level dataset lives in the contact-center-forecasting repo.
- The DAG is validated by a real `DagBag` parse and the task logic by
  pytest; the Airflow scheduler itself was not run end-to-end in this
  repo.
- No Airflow, Snowflake, or Amplitude experience is claimed as job
  experience. This is a personal project.
