"""
Shared Airflow DAG factory.

Why a factory instead of copy-pasted DAGs
-----------------------------------------
The requirement is explicit: build one shared DAG and *import* it, to prove
extensibility. Each concrete DAG file below is then a handful of lines:

    dag = build_etl_dag(dag_id="dbx_etl_mysql", engine="mysql", workers=3)

Adding a new engine or a new schedule is a parameter change, not a new file.
That is what "reusability and efficiency for multiple jobs" means in practice.

Parameters (from the requirement)
---------------------------------
    engine   : mysql / mongodb / postgresql
    schema   : which schema within the engine
    workers  : concurrent table count inside the schema (2+ enables the pool)

Task shape
----------
    start → [ signal check ] → [ work ] → [ log ] → end

The signal check is a real gate: if `_chk.json` / `_rclone_done.json` are
missing, the load task is skipped rather than run against incomplete data.

Implementation notes
--------------------
* `schedule=` is used rather than `schedule_interval=` (removed in Airflow 3).
* `max_active_runs=1` so two runs cannot fight over the same target table.
* `catchup=False` because these jobs are triggered manually or on a schedule;
  replaying history would double-load.
* The Python callables are module-level functions (not closures) so that
  Airflow can serialise them.
"""
from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# Make the project's `lib` package importable inside the container.
_PROJECT_ROOT = Path(os.environ.get("PRETEST_HOME", "/opt/pretest"))
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

os.environ.setdefault("PRETEST_IN_CONTAINER", "1")

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "databricks-pre-test",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}


# ---------------------------------------------------------------------------
# Task callables (module level so Airflow can pickle them)
# ---------------------------------------------------------------------------

def check_signals_task(engine: str, schema: str, **context) -> dict:
    """Gate task: verify both chk and rclone_done before any load happens."""
    sys.path.insert(0, str(_PROJECT_ROOT))
    from autoloader import initial_load

    signals = initial_load.check_signals(engine, schema)
    logger.info("[%s.%s] 신호 확인: %s", engine, schema, signals["reason"])
    return signals


def run_rclone_task(engine: str, schema: str, **context) -> dict:
    """Replicate the local folder to S3 through the rclone RC API."""
    sys.path.insert(0, str(_PROJECT_ROOT))
    sys.path.insert(0, str(_PROJECT_ROOT / "data-prep"))

    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "rclone_step", _PROJECT_ROOT / "data-prep" / "rclone_step.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    return module.copy_schema(engine, schema)


def run_initial_load_task(engine: str, schema: str, **context) -> dict:
    """Auto Loader: S3 files → managed table, with row-count verification."""
    sys.path.insert(0, str(_PROJECT_ROOT))
    from autoloader import initial_load

    return initial_load.load_schema(
        engine, schema, replace=True,
        run_id=context.get("run_id", "manual"),
        dag_id=context.get("dag_id", ""))


def run_etl_task(engine: str, schema: str, workers: int = 1,
                 force: bool = False, ignore_schedule: bool = False,
                 **context) -> dict:
    """ETL engine: re-read the source, de-identify, append/truncate/merge."""
    sys.path.insert(0, str(_PROJECT_ROOT))
    sys.path.insert(0, str(_PROJECT_ROOT / "etl-module"))

    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "processor", _PROJECT_ROOT / "etl-module" / "processor.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    return module.process_schema(
        engine, schema, workers=workers,
        run_id=context.get("run_id", "manual"),
        dag_id=context.get("dag_id", ""),
        task_id=context.get("task_id", ""),
        force=force, ignore_schedule=ignore_schedule)


def activate_profile_task(engine: str, schema: str, **context) -> dict:
    """Flip `active: N → Y` once the initial load has succeeded.

    This is the hand-off required by the scenario: after the file load, the
    profile becomes active and subsequent runs behave normally
    (append / merge / truncate) instead of waiting.
    """
    sys.path.insert(0, str(_PROJECT_ROOT))
    from lib import profile as profile_mod

    changed = profile_mod.set_schema_active(engine, schema, "Y")
    logger.info("[%s.%s] 활성 플래그 %d개 변경", engine, schema, changed)
    return {"engine": engine, "schema": schema, "activated": changed}


def notify_task(engine: str, schema: str, **context) -> dict:
    """Send a Slack summary for the DAG run."""
    sys.path.insert(0, str(_PROJECT_ROOT))
    from lib import slack as slack_mod

    return slack_mod.notify(
        "DAG완료",
        f"Airflow DAG 완료 — {context.get('dag_id', '')}",
        [("데이터베이스 종류", engine),
         ("스키마", schema),
         ("Airflow 실행 ID", context.get("run_id", "-")),
         ("작업 시각", datetime.now().isoformat(timespec="seconds")),
         ("상태", "정상 종료")],
        severity="success", dag_id=context.get("dag_id", ""))


# ---------------------------------------------------------------------------
# DAG builders
# ---------------------------------------------------------------------------

def _base_dag(dag_id: str, schedule: str | None, tags: list[str],
              description: str) -> DAG:
    """Create the DAG object with the settings every job shares."""
    return DAG(
        dag_id=dag_id,
        default_args=DEFAULT_ARGS,
        schedule=schedule,
        catchup=False,
        # Measured: Airflow 2.10 raises ValueError without start_date.
        start_date=datetime(2026, 1, 1),
        max_active_runs=1,
        tags=tags,
        description=description,
        # Airflow's own retry would duplicate Databricks queries, so the
        # pipeline handles retries itself (watchdog + idempotent load types).
        dagrun_timeout=timedelta(hours=2),
    )


def build_rclone_dag(dag_id: str, engine: str, schema: str,
                     schedule: str | None = None) -> DAG:
    """DAG that verifies the chk signal and replicates the folder to S3."""
    with _base_dag(dag_id, schedule,
                   ["rclone", "databricks-pre-test", engine],
                   f"rclone RC API 복제 — {engine}.{schema}") as dag:
        start = EmptyOperator(task_id="start")
        signals = PythonOperator(
            task_id="check_signals",
            python_callable=check_signals_task,
            op_kwargs={"engine": engine, "schema": schema},
        )
        copy = PythonOperator(
            task_id="rclone_copy",
            python_callable=run_rclone_task,
            op_kwargs={"engine": engine, "schema": schema},
        )
        end = EmptyOperator(task_id="end")

        start >> signals >> copy >> end

    return dag


def build_initial_load_dag(dag_id: str, engine: str, schema: str,
                           schedule: str | None = None,
                           auto_activate: bool = True) -> DAG:
    """DAG for the initial file load (chk → rclone → Auto Loader → activate).

    The full hand-off chain lives in one DAG on purpose: the profile flip from
    `N` to `Y` must only happen after the row counts were verified.
    """
    with _base_dag(dag_id, schedule,
                   ["autoloader", "databricks-pre-test", engine],
                   f"Auto Loader 초기 이관 — {engine}.{schema}") as dag:
        start = EmptyOperator(task_id="start")
        signals = PythonOperator(
            task_id="check_signals",
            python_callable=check_signals_task,
            op_kwargs={"engine": engine, "schema": schema},
        )
        copy = PythonOperator(
            task_id="rclone_copy",
            python_callable=run_rclone_task,
            op_kwargs={"engine": engine, "schema": schema},
        )
        load = PythonOperator(
            task_id="auto_load",
            python_callable=run_initial_load_task,
            op_kwargs={"engine": engine, "schema": schema},
        )

        chain = [start, signals, copy, load]

        if auto_activate:
            activate = PythonOperator(
                task_id="activate_profile",
                python_callable=activate_profile_task,
                op_kwargs={"engine": engine, "schema": schema},
            )
            chain.append(activate)

        notify = PythonOperator(
            task_id="notify_slack",
            python_callable=notify_task,
            op_kwargs={"engine": engine, "schema": schema},
        )
        end = EmptyOperator(task_id="end")
        chain += [notify, end]

        for previous, current in zip(chain, chain[1:]):
            previous >> current

    return dag


def build_etl_dag(dag_id: str, engine: str, schema: str,
                  workers: int = 1, schedule: str | None = None,
                  force: bool = False,
                  ignore_schedule: bool = False) -> DAG:
    """DAG for regular ETL runs.

    `workers >= 2` switches the ETL module into its thread pool, which is the
    requirement's "process N tables concurrently" behaviour.
    """
    with _base_dag(dag_id, schedule,
                   ["etl", "databricks-pre-test", engine],
                   f"ETL 정규 작업 — {engine}.{schema} (workers={workers})") as dag:
        start = EmptyOperator(task_id="start")
        etl = PythonOperator(
            task_id="etl_run",
            python_callable=run_etl_task,
            op_kwargs={
                "engine": engine, "schema": schema, "workers": workers,
                "force": force, "ignore_schedule": ignore_schedule,
            },
        )
        notify = PythonOperator(
            task_id="notify_slack",
            python_callable=notify_task,
            op_kwargs={"engine": engine, "schema": schema},
        )
        end = EmptyOperator(task_id="end")

        start >> etl >> notify >> end

    return dag


def build_matrix_dag(dag_id: str, targets: list[dict],
                     schedule: str | None = None) -> DAG:
    """DAG that runs ETL across **many** schema/worker combinations.

    This is where the factory pays off: one DAG definition drives an arbitrary
    number of jobs, each with its own parameters.

    Args:
        targets: [{"engine": "mysql", "schema": "mysql_schema_1", "workers": 3}, ...]
    """
    with _base_dag(dag_id, schedule,
                   ["etl", "matrix", "databricks-pre-test"],
                   f"ETL 다중 스키마 병렬 실행 — {len(targets)}건") as dag:
        start = EmptyOperator(task_id="start")
        notify = PythonOperator(
            task_id="notify_slack",
            python_callable=notify_task,
            op_kwargs={"engine": f"{len(targets)}건", "schema": "-"},
        )
        end = EmptyOperator(task_id="end")

        previous = start
        for index, target in enumerate(targets, start=1):
            task = PythonOperator(
                task_id=f"etl_{target['engine']}_{target['schema']}",
                python_callable=run_etl_task,
                op_kwargs={
                    "engine": target["engine"],
                    "schema": target["schema"],
                    "workers": target.get("workers", 1),
                    "force": target.get("force", False),
                    "ignore_schedule": target.get("ignore_schedule", False),
                },
            )
            previous >> task
            previous = task

        previous >> notify >> end

    return dag


if __name__ == "__main__":
    print(f"공통 DAG 모듈: {__file__}")
    print("제공 빌더:")
    for name in ("build_rclone_dag", "build_initial_load_dag",
                 "build_etl_dag", "build_matrix_dag"):
        print(f"  - {name}")
