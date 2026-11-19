"""Incremental-load pipeline for the Airflow search-contacts DAG.

Each module is deliberately small and independently testable: the Airflow
tasks in dags/search_contacts_daily.py are thin wrappers around these
functions, so the pipeline logic is verified by plain pytest with no
Airflow instance running.
"""

from .extract import extract_day, extract_range
from .load import init_warehouse, load_day, load_range, get_watermark
from .transform import build_mart
from .quality import run_checks, assert_all_passed

__all__ = [
    "extract_day",
    "extract_range",
    "init_warehouse",
    "load_day",
    "load_range",
    "get_watermark",
    "build_mart",
    "run_checks",
    "assert_all_passed",
]
