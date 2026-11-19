"""DAG structure tests. Uses a real DagBag parse when Airflow is
installed; otherwise asserts the same structure through the test-only
stand-in from conftest.py (which is clearly not Airflow).
"""

import os

import pytest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DAGS_FOLDER = os.path.join(PROJECT_ROOT, "dags")

EXPECTED_TASKS = ["extract", "load", "build_reporting_mart", "quality_checks"]


def _load_dag():
    try:
        from airflow.models import DagBag
        bag = DagBag(dag_folder=DAGS_FOLDER)
        assert not bag.import_errors, f"DAG import errors: {bag.import_errors}"
        # bag.dags is the parsed DAGs dict; get_dag() needs an initialized
        # metadata DB, which this repo does not require.
        return bag.dags.get("search_contacts_daily"), True
    except ImportError:
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "search_contacts_daily",
            os.path.join(DAGS_FOLDER, "search_contacts_daily.py"))
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module.search_contacts_daily, False


def test_dag_structure():
    dag, real_airflow = _load_dag()
    assert dag is not None
    assert dag.dag_id == "search_contacts_daily"
    assert dag.catchup is True
    if real_airflow:
        sched = getattr(dag, "schedule", None)
        if sched is None:
            sched = getattr(dag, "schedule_interval", None)
        assert str(sched) in ("@daily", "0 0 * * *"), f"schedule is {sched!r}"
    else:
        assert dag.schedule == "@daily"
    assert sorted(dag.task_dict.keys()) == sorted(EXPECTED_TASKS)


def test_dag_dependencies_are_linear():
    dag, real_airflow = _load_dag()
    order = ["extract", "load", "build_reporting_mart", "quality_checks"]
    for upstream_id, downstream_id in zip(order, order[1:]):
        upstream = dag.task_dict[upstream_id]
        downstream = dag.task_dict[downstream_id]
        if real_airflow:
            assert downstream.task_id in {
                t.task_id for t in upstream.downstream_list}
        else:
            assert downstream in upstream.downstream
