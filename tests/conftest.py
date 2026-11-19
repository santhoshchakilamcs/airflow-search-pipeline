"""Test setup: make the project importable and provide a minimal
Airflow stand-in when real Airflow is not installed.

The stand-in (used only when ``import airflow`` fails) implements the
TaskFlow decorators well enough for the DAG module to import and for
tests to assert the task graph: task ids, the schedule, catchup, and
the dependency edges. It is NOT Airflow and the tests say so.
"""

import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


def _airflow_available() -> bool:
    try:
        import airflow  # noqa: F401
        return True
    except Exception:
        return False


if not _airflow_available():
    import types

    class _XComArg:
        def __init__(self, task):
            self.task = task

        def __rshift__(self, other):
            self.task.downstream.append(other.task)
            other.task.upstream.append(self.task)
            return other

    class _Task:
        def __init__(self, fn):
            self.fn = fn
            self.task_id = fn.__name__
            self.upstream = []
            self.downstream = []

        def __call__(self, *args, **kwargs):
            return _XComArg(self)

    class _DAG:
        def __init__(self, dag_id, schedule, catchup, tasks):
            self.dag_id = dag_id
            self.schedule = schedule
            self.catchup = catchup
            self.task_dict = {t.task_id: t for t in tasks}

    def _task_decorator(*dargs, **dkwargs):
        def wrap(fn):
            return _Task(fn)
        if dargs and callable(dargs[0]):
            return wrap(dargs[0])
        return wrap

    def _dag_decorator(*dargs, **dkwargs):
        def wrap(fn):
            def build(*args, **kwargs):
                created = []
                orig_call = _Task.__call__

                def recording_call(self, *a, **k):
                    created.append(self)
                    # Passing one task's output into another task is how
                    # TaskFlow declares a dependency, so record it.
                    for arg in list(a) + list(k.values()):
                        if isinstance(arg, _XComArg):
                            arg.task.downstream.append(self)
                            self.upstream.append(arg.task)
                    return orig_call(self, *a, **k)

                _Task.__call__ = recording_call
                try:
                    fn()
                finally:
                    _Task.__call__ = orig_call
                return _DAG(
                    dag_id=dkwargs.get("dag_id"),
                    schedule=dkwargs.get("schedule"),
                    catchup=dkwargs.get("catchup"),
                    tasks=created,
                )
            return build
        if dargs and callable(dargs[0]):
            return wrap(dargs[0])
        return wrap

    airflow_mod = types.ModuleType("airflow")
    decorators_mod = types.ModuleType("airflow.decorators")
    decorators_mod.task = _task_decorator
    decorators_mod.dag = _dag_decorator
    airflow_mod.decorators = decorators_mod
    sys.modules["airflow"] = airflow_mod
    sys.modules["airflow.decorators"] = decorators_mod
