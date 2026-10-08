"""
Auto Loader DAG definitions.

Every file here is a **parameter block**, not a pipeline. The pipelines live in
`dbx_common.py`, so adding a schema is a one-line change rather than a
copy-pasted DAG — which is precisely the extensibility the requirement asks to
demonstrate.

DAG groups
----------
    dbx_<engine>_schema_<n>_rclone     : signal check + rclone RC API copy
    dbx_<engine>_schema_<n>_initial    : check → copy → Auto Loader → activate
    dbx_<engine>_schema_<n>_etl        : regular ETL (workers=1, schedule applied)
    dbx_<engine>_schema_<n>_etl_p      : same but workers=3 (parallel path)
    dbx_matrix_all                     : all 12 schemas in one DAG

Why `initial` runs copy inside itself
-------------------------------------
The scenario requires the chain
`chk → rclone → Auto Loader → active=Y` to be one automatic unit. Splitting
the copy into its own DAG would let a human trigger the load before the copy
finished, which is exactly the failure mode the requirement warns about.
"""
from __future__ import annotations

import sys
from pathlib import Path

# The dags folder is mounted at /opt/airflow/dags, and the project at
# /opt/pretest. Import the shared factory from the same folder.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from dbx_common import (build_etl_dag, build_initial_load_dag,   # noqa: E402
                        build_matrix_dag, build_rclone_dag)

ENGINES = ["mysql", "mongodb", "postgresql"]
SCHEMAS = {
    "mysql": ["mysql_schema_1", "mysql_schema_2", "mysql_schema_3", "mysql_schema_4"],
    "mongodb": ["mongo_schema_1", "mongo_schema_2", "mongo_schema_3", "mongo_schema_4"],
    "postgresql": ["postgres_schema_1", "postgres_schema_2",
                   "postgres_schema_3", "postgres_schema_4"],
}

#: Daily schedule. Kept identical across the 12 jobs so the matrix is
#: comparable; individual table-level schedules live in the profiles.
DAILY = "0 2 * * *"


# ---------------------------------------------------------------------------
# One DAG per (engine, schema): rclone
# ---------------------------------------------------------------------------

for _engine in ENGINES:
    for _schema in SCHEMAS[_engine]:
        _short = _schema.split("_")[-1]           # "1" .. "4"
        globals()[f"dag_rclone_{_engine}_{_short}"] = build_rclone_dag(
            dag_id=f"dbx_{_engine}_schema_{_short}_rclone",
            engine=_engine, schema=_schema, schedule=DAILY)


# ---------------------------------------------------------------------------
# One DAG per (engine, schema): initial load
# ---------------------------------------------------------------------------

for _engine in ENGINES:
    for _schema in SCHEMAS[_engine]:
        _short = _schema.split("_")[-1]
        globals()[f"dag_initial_{_engine}_{_short}"] = build_initial_load_dag(
            dag_id=f"dbx_{_engine}_schema_{_short}_initial",
            engine=_engine, schema=_schema, schedule=None, auto_activate=True)


# ---------------------------------------------------------------------------
# One DAG per (engine, schema): ETL, single worker
# ---------------------------------------------------------------------------

for _engine in ENGINES:
    for _schema in SCHEMAS[_engine]:
        _short = _schema.split("_")[-1]
        globals()[f"dag_etl_{_engine}_{_short}"] = build_etl_dag(
            dag_id=f"dbx_{_engine}_schema_{_short}_etl",
            engine=_engine, schema=_schema, workers=1, schedule=DAILY)


# ---------------------------------------------------------------------------
# One DAG per (engine, schema): ETL, parallel workers
# ---------------------------------------------------------------------------
# Same work as above but with workers=3, which is how the requirement's
# "process N tables concurrently" path is exercised.

for _engine in ENGINES:
    for _schema in SCHEMAS[_engine]:
        _short = _schema.split("_")[-1]
        globals()[f"dag_etlp_{_engine}_{_short}"] = build_etl_dag(
            dag_id=f"dbx_{_engine}_schema_{_short}_etl_p",
            engine=_engine, schema=_schema, workers=3, schedule=DAILY)


# ---------------------------------------------------------------------------
# Matrix DAG: all 12 schemas in one place
# ---------------------------------------------------------------------------

MATRIX_TARGETS = [
    {"engine": engine, "schema": schema, "workers": 3}
    for engine in ENGINES
    for schema in SCHEMAS[engine]
]

dag_matrix_all = build_matrix_dag(
    dag_id="dbx_matrix_all", targets=MATRIX_TARGETS, schedule=None)
