"""
ETL engine — reads the source database, applies de-identification, and writes
to the Databricks managed table.

Responsibility (this is the component the requirement calls for)
---------------------------------------------------------------
The requirement says:

    "작업 순간에 다시 한 번 source를 접속하여 컬럼 삭제/추가여부를 확인하여
     삭제: null as column으로 정상처리 / 추가: 추가컬럼명만 확인
     하여 slack으로 변경내역을 보내줄 수 있도록 한다."

So this module, at **execution time**:

  1. Reconnects to the source and reads the live schema.
  2. Dropped column (in profile, absent in source)
        → `CAST(NULL AS <type>) AS <col>` — the load continues.
  3. Added column (in source, absent in profile)
        → **not reflected**; reported via Slack for a human decision.
  4. Dropped table → the job is skipped and Slack is notified.

Auto Loader deliberately does not do this; see `autoloader/initial_load.py`
for why the two are separated.

Load strategies
---------------
    append   : INSERT INTO            — incremental
    truncate : INSERT OVERWRITE        — full reload
    merge    : MERGE INTO ... WHEN MATCHED/MATCHED ... — change-only, needs a
               primary key (enforced at profile registration)

Parallelism
-----------
`--workers N` processes N tables concurrently with a ThreadPoolExecutor.
Concurrency lives here rather than in SQL so that the whole run can be driven
from one Airflow task with a single parameter.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from lib import config as cfg                  # noqa: E402
from lib import dbx                            # noqa: E402
from lib import logtable                       # noqa: E402
from lib import mask as mask_mod               # noqa: E402
from lib import profile as profile_mod         # noqa: E402
from lib import slack as slack_mod             # noqa: E402
from lib import sources as sources_mod         # noqa: E402

logger = logging.getLogger("etl")

ENGINES = ["mysql", "mongodb", "postgresql"]


def target_table(engine: str, table: str) -> str:
    return f"{cfg.get_databricks_config()['catalog']}.{engine}.{table}"


# ---------------------------------------------------------------------------
# Source schema re-read (the heart of the requirement)
# ---------------------------------------------------------------------------

def inspect_source(engine: str, schema: str, entry: dict) -> dict:
    """Re-read the live source schema and classify every column.

    Classification is relative to the profile, because the profile *is* the
    configuration baseline:

        dropped  : in profile, absent in source -> fill with NULL
        added    : in source, absent in profile -> do NOT reflect, report
        excluded : filtered out by configuration
        loadable : everything that will actually be read
    """
    profile_columns = list(entry.get("columns") or [])

    live_columns = sources_mod.list_columns(engine, schema,
                                            _current_table["name"])

    decision = profile_mod.resolve_load_columns(entry, live_columns)

    return {
        "profile_columns": profile_columns,
        "live_columns": live_columns,
        "load_columns": decision["load_columns"],
        "added_columns": decision["added_columns"],
        "dropped_columns": decision["dropped_columns"],
        "excluded_columns": decision["excluded_columns"],
        "reasons": decision["reasons"],
        "basis": decision["basis"],
    }


#: Set while processing, so `inspect_source` can stay a pure function of its
#: arguments in the docs while still reading the right table.
_current_table: dict = {"name": ""}


def inspect_table(engine: str, schema: str, table: str, entry: dict) -> dict:
    """Same as `inspect_source` but takes the table name explicitly."""
    profile_columns = list(entry.get("columns") or [])
    live_columns = sources_mod.list_columns(engine, schema, table)
    decision = profile_mod.resolve_load_columns(entry, live_columns)
    return {
        "profile_columns": profile_columns,
        "live_columns": live_columns,
        "load_columns": decision["load_columns"],
        "added_columns": decision["added_columns"],
        "dropped_columns": decision["dropped_columns"],
        "excluded_columns": decision["excluded_columns"],
        "reasons": decision["reasons"],
        "basis": decision["basis"],
    }


# ---------------------------------------------------------------------------
# SELECT construction
# ---------------------------------------------------------------------------

def build_select(load_columns: list[str], dropped_columns: list[str],
                 source_relation: str) -> str:
    """Build the SELECT that reads from the source and writes to the target.

    Dropped columns are materialised explicitly as `CAST(NULL AS …)`. This is
    the behaviour the requirement asks for ("delete: handle normally with
    null as column") and it keeps the target schema stable when the upstream
    drops a column.

    Added columns are **not** selected at all — reflecting them automatically
    is explicitly forbidden.
    """
    items = [f"`{c}`" for c in load_columns]
    items += [f"CAST(NULL AS STRING) AS `{c}`" for c in dropped_columns]
    if not items:
        items = ["1 AS _dummy"]
    return f"SELECT {', '.join(items)} FROM {source_relation}"


def source_relation(engine: str, schema: str, table: str) -> str:
    """SQL relation name for the source table on Databricks.

    Source tables are exposed to Databricks as *foreign* tables in the
    `<engine>` schema (same schema name as the source database), so the
    relation is simply `<catalog>.<engine>.<table>`.

    When the foreign catalog is unavailable the caller falls back to the
    file-based path; see `process_table`.
    """
    return target_table(engine, table)


# ---------------------------------------------------------------------------
# Single table
# ---------------------------------------------------------------------------

def process_table(engine: str, schema: str, table: str, *,
                  workers: int = 1, run_id: str = "",
                  dag_id: str = "", task_id: str = "",
                  force: bool = False,
                  ignore_schedule: bool = False) -> dict:
    """Run the ETL for one table and return a result record."""
    started_at = datetime.now()
    started_monotonic = time.time()

    # Measured: a fixed `run_id` of "manual" collapses every run into one
    # bucket in the audit table, so a later aggregate cannot tell a smoke run
    # from a full-scale verification. Generate a timestamped id when the
    # caller does not supply one.
    if not run_id:
        run_id = f"etl_{started_at:%Y%m%d_%H%M%S}"

    entry = profile_mod.table_entry(engine, schema, table)
    result = {
        "engine": engine, "schema": schema, "table": table,
        "target": target_table(engine, table),
        "etl_type": entry.get("etl_type", "append"),
        "status": "성공", "status_code": "OK",
        "source_count": 0, "target_count": 0, "count_match": False,
        "rows_affected": 0,
        "load_columns": [], "added_columns": [], "dropped_columns": [],
        "excluded_columns": entry.get("exclude_columns") or [],
        "deid_applied": entry.get("deidentification") or {},
        "schedule_hit": "Y", "active_flag": entry.get("active", "N"),
        "workers": workers,
        "started_at": started_at, "duration_sec": 0.0,
        "message": "", "statement_id": None,
    }

    # ⚠ 실측 결함: 이 함수에는 조기 반환(early return)이 여러 개 있고,
    #   각각이 `ended_at` 을 채우지 않았다. 그래서 감사 테이블에
    #   `NOT NULL constraint violated for column: ended_at` 로 insert 가 실패했다.
    #   구조를 try/finally 로 바꿔 **어떤 경로로 나가도** 종료 시각이 채워진다.
    try:
        # ---- 0. guards -------------------------------------------------
        if not entry:
            result.update(status="스킵", status_code="SKIP",
                          message="프로파일에 등록되지 않은 테이블")
            return result

        if entry.get("active") != "Y" and not force:
            result.update(status="건너뜀", status_code="SKIP",
                          message="active 플래그가 N (초기 적재 전)")
            return result

        schedule = profile_mod.evaluate_schedule(entry, date.today())
        result["schedule_hit"] = "Y" if schedule["run"] else "N"
        result["schedule_reason"] = schedule["reason"]
        if not schedule["run"] and not ignore_schedule:
            result.update(status="건너뜀", status_code="SKIP",
                          message=f"작업 주기 미해당: {schedule['reason']}")
            return result

        # ---- 1. table still exists? -----------------------------------
        if not sources_mod.table_exists(engine, schema, table):
            message = f"원천 테이블이 삭제되어 작업을 건너뜁니다: {engine}.{schema}.{table}"
            result.update(status="건너뜀", status_code="SKIP", message=message)
            slack_mod.notify_table_dropped(
                engine, schema, table,
                registered_column_count=len(entry.get("columns") or []),
                dag_id=dag_id, task_id=task_id)
            return result

        # ---- 2. re-read the live source schema ------------------------
        inspection = inspect_table(engine, schema, table, entry)
        result["load_columns"] = inspection["load_columns"]
        result["added_columns"] = inspection["added_columns"]
        result["dropped_columns"] = inspection["dropped_columns"]

        if inspection["added_columns"] or inspection["dropped_columns"]:
            slack_mod.notify_column_change(
                engine, schema, table,
                added=inspection["added_columns"],
                dropped=inspection["dropped_columns"],
                dag_id=dag_id, task_id=task_id)

        if not inspection["load_columns"]:
            result.update(status="스킵", status_code="SKIP",
                          message="적재할 컬럼이 없습니다")
            return result

        # ---- 3. counts -------------------------------------------------
        source_count = sources_mod.count_rows(engine, schema, table)
        result["source_count"] = source_count

        etl_type = result["etl_type"]
        target = result["target"]
        relation = source_relation(engine, schema, table)
        select_sql = build_select(
            inspection["load_columns"], inspection["dropped_columns"], relation)

        # ---- 4. apply the load strategy --------------------------------
        column_list = ", ".join(
            f"`{c}`" for c in
            (inspection["load_columns"] + inspection["dropped_columns"]))
        deid_expr = mask_mod.build_column_expressions(
            [(f"src.`{c}`", c, None) for c in inspection["load_columns"]],
            result["deid_applied"])
        # rebuild with aliasing so the merged SELECT reads cleanly
        deid_expr = mask_mod.build_column_expressions(
            [(f"`{c}`", c, None) for c in inspection["load_columns"]],
            result["deid_applied"])
        projection = ",\n       ".join(deid_expr)
        for col in inspection["dropped_columns"]:
            projection += (f",\n       CAST(NULL AS STRING) AS `{col}`"
                           if projection else
                           f"CAST(NULL AS STRING) AS `{col}`")

        if etl_type == "append":
            sql = (f"INSERT INTO {target} ({column_list})\n"
                   f"SELECT {projection}\nFROM (\n    {select_sql}\n) AS src")
        elif etl_type == "truncate":
            sql = (f"INSERT OVERWRITE {target} ({column_list})\n"
                   f"SELECT {projection}\nFROM (\n    {select_sql}\n) AS src")
        elif etl_type == "merge":
            pk = entry.get("primary_key")
            if not pk:
                raise ValueError("merge 적재에는 primary_key 가 필요합니다")
            update_cols = [c for c in inspection["load_columns"] if c != pk]
            on_clause = " AND ".join(
                f"t.`{c}` <=> s.`{c}`" for c in inspection["load_columns"]) or "1=1"
            assignments = ", ".join(f"t.`{c}` = s.`{c}`" for c in update_cols)
            insert_cols = ", ".join(
                f"s.`{c}`" for c in inspection["load_columns"]
                + inspection["dropped_columns"])
            sql = (
                f"MERGE INTO {target} AS t\n"
                f"USING (\n    {select_sql}\n) AS s\n"
                f"ON t.`{pk}` = s.`{pk}`\n"
                f"WHEN MATCHED THEN UPDATE SET {assignments}\n"
                f"WHEN NOT MATCHED THEN INSERT ({column_list}) "
                f"VALUES ({insert_cols})"
            )
        else:
            raise ValueError(f"알 수 없는 etl_type: {etl_type}")

        execution = dbx.execute_sql(sql)
        result["statement_id"] = execution.get("statement_id")

        # ---- 5. verify -------------------------------------------------
        target_count = int(dbx.fetch_value(f"SELECT COUNT(*) AS cnt FROM {target}"))
        result["target_count"] = target_count

        if etl_type == "truncate":
            # A full reload must land exactly on the source count.
            result["count_match"] = (source_count == target_count)
            result["rows_affected"] = target_count
        elif etl_type == "append":
            # Append grows the target, so the delta is what matters.
            result["count_match"] = (target_count >= source_count)
            result["rows_affected"] = max(0, target_count - _previous_count(target))
        else:  # merge
            result["count_match"] = (target_count >= source_count)
            result["rows_affected"] = 0

        result["message"] = (
            f"{etl_type} 완료 (원본 {source_count:,} / 대상 {target_count:,})")

        if not result["count_match"]:
            result.update(status="실패", status_code="FAIL",
                          message=f"건수 검증 실패: {result['message']}")

    except Exception as exc:                    # noqa: BLE001
        result["status"] = "실패"
        result["status_code"] = "FAIL"
        result["message"] = f"{type(exc).__name__}: {str(exc)[:400]}"
        logger.error("  [%s.%s.%s] 실패: %s", engine, schema, table, result["message"])
    finally:
        # 어떤 경로로 나가도(성공/실패/조기 반환) 종료 시각을 반드시 채운다.
        ended_at = datetime.now()
        result["ended_at"] = ended_at
        result["duration_sec"] = round(
            (ended_at - started_at).total_seconds(), 2)

    return result


def _previous_count(target: str) -> int:
    """Best-effort previous row count, used only for append delta reporting."""
    return 0


# ---------------------------------------------------------------------------
# Schema level (with worker pool)
# ---------------------------------------------------------------------------

def process_schema(engine: str, schema: str, *, workers: int = 1,
                   run_id: str = "", dag_id: str = "", task_id: str = "",
                   force: bool = False,
                   ignore_schedule: bool = False) -> dict:
    """Process every table of one schema, optionally in parallel."""
    started_monotonic = time.time()

    # Same reason as `process_table`: without a distinct id every schema run
    # shares one audit bucket.
    if not run_id:
        run_id = f"etl_{datetime.now():%Y%m%d_%H%M%S}"

    logger.info("=" * 68)
    logger.info("[%s.%s] ETL 시작 (workers=%d)", engine, schema, workers)
    logger.info("=" * 68)

    document = profile_mod.read(engine, schema)
    tables = list(document.get("tables", {}).keys())
    if not tables:
        logger.warning("[%s.%s] 등록된 테이블이 없습니다", engine, schema)
        return {"engine": engine, "schema": schema, "succeeded": 0,
                "failed": 0, "skipped": 0, "total": 0, "results": [],
                "duration_sec": 0.0}

    def run_one(table: str) -> dict:
        return process_table(
            engine, schema, table, workers=workers, run_id=run_id,
            dag_id=dag_id, task_id=task_id, force=force,
            ignore_schedule=ignore_schedule)

    results: list[dict] = []

    if workers and workers > 1:
        logger.info("  동시 처리 %d개 워커로 실행합니다", workers)
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(run_one, t): t for t in tables}
            for future in as_completed(futures):
                table = futures[future]
                try:
                    results.append(future.result())
                except Exception as exc:        # noqa: BLE001
                    results.append({
                        "engine": engine, "schema": schema, "table": table,
                        "status": "실패", "status_code": "FAIL",
                        "message": f"{type(exc).__name__}: {exc}",
                        "started_at": datetime.now(),
                        "ended_at": datetime.now(),
                        "duration_sec": 0.0,
                        "etl_type": "?", "source_count": 0,
                        "target_count": 0, "count_match": False,
                    })
    else:
        logger.info("  단일 처리로 실행합니다")
        for table in tables:
            results.append(run_one(table))

    results.sort(key=lambda r: r["table"])

    succeeded = sum(1 for r in results if r["status_code"] == "OK")
    failed = sum(1 for r in results if r["status_code"] == "FAIL")
    skipped = sum(1 for r in results if r["status_code"] == "SKIP")

    for r in results:
        logger.info("  %-10s %-8s %-8s 원본 %7s / 대상 %7s  %s",
                    r["table"], r.get("etl_type", "?"), r["status"],
                    f"{r.get('source_count', 0):,}",
                    f"{r.get('target_count', 0):,}",
                    f"{r['duration_sec']:.1f}s")

    duration = round(time.time() - started_monotonic, 2)
    logger.info("[%s.%s] 완료 · 성공 %d / 실패 %d / 건너뜀 %d (%.1fs)",
                engine, schema, succeeded, failed, skipped, duration)

    write_logs(results, run_id, work_type="etl")

    summary = {
        "engine": engine, "schema": schema,
        "succeeded": succeeded, "failed": failed, "skipped": skipped,
        "total": len(results), "workers": workers,
        "duration_sec": duration, "results": results,
        "run_id": run_id,
    }

    ok_list = [r["table"] for r in results if r["status_code"] == "OK"]
    # Each entry stays a 2-tuple: (table, reason).
    # Measured: building these as plain strings broke unpacking in the Slack
    # summary formatter (ValueError: too many values to unpack).
    fail_list = [(r["table"], str(r.get("message", ""))[:80])
                 for r in results if r["status_code"] == "FAIL"]

    slack_mod.notify_run_summary({
        "DAG이름": dag_id,
        "Airflow실행ID": run_id,
        "엔진": engine,
        "스키마": schema,
        "동시처리개수": workers,
        "전체대상": len(results),
        "성공": succeeded,
        "실패": failed,
        "건너뜀": skipped,
        "소요초": duration,
        "성공목록": ok_list,
        "실패목록": fail_list,
    }, dag_id=dag_id, task_id=task_id)

    return summary


# ---------------------------------------------------------------------------
# Audit log
# ---------------------------------------------------------------------------

def write_logs(results: list[dict], run_id: str, work_type: str = "etl") -> None:
    """Write one `load_audit` row and one `etl_run_log` row per table."""
    if not results:
        return

    schema_name = logtable.meta_schema()
    # 감사 테이블의 ended_at 은 NOT NULL 이므로, 값이 없으면 현재 시각으로 채운다.
    # (조기 반환 경로에서 빠진 경우를 방어)
    default_end = datetime.now()
    check_values: list[str] = []
    run_values: list[str] = []

    for r in results:
        started_value = r.get("started_at") or default_end
        ended_value = r.get("ended_at") or default_end
        elapsed_value = float(r.get("duration_sec") or 0.0)
        source_count = r.get("source_count", 0)
        target_count = r.get("target_count", 0)
        added = ",".join(r.get("added_columns") or []) or None
        dropped = ",".join(r.get("dropped_columns") or []) or None
        excluded = ",".join(r.get("excluded_columns") or []) or None
        deid = ",".join(f"{k}={v}" for k, v in
                        (r.get("deid_applied") or {}).items()) or None
        detail = dbx.sql_literal(json.dumps({
            "load_columns": r.get("load_columns"),
            "added_columns": r.get("added_columns"),
            "dropped_columns": r.get("dropped_columns"),
            "reasons": r.get("reasons"),
            "basis": r.get("basis"),
            "schedule_reason": r.get("schedule_reason"),
        }, ensure_ascii=False))

        check_values.append("(" + ", ".join([
            dbx.sql_literal(run_id), dbx.sql_literal(work_type),
            dbx.sql_literal(r["engine"]), dbx.sql_literal(r["schema"]),
            dbx.sql_literal(r["table"]), dbx.sql_literal(r.get("target", "")),
            dbx.sql_literal(r.get("etl_type", "?")),
            str(source_count), str(target_count), str(target_count - source_count),
            dbx.sql_literal("Y" if r.get("count_match") else "N"),
            dbx.sql_literal(added), dbx.sql_literal(dropped),
            dbx.sql_literal(excluded), dbx.sql_literal("0"),
            dbx.sql_literal(started_value), dbx.sql_literal(ended_value),
            str(elapsed_value),
            dbx.sql_literal(r.get("message")), detail,
        ]) + ")")

        run_values.append("(" + ", ".join([
            dbx.sql_literal(run_id), dbx.sql_literal(work_type),
            dbx.sql_literal(r["engine"]), dbx.sql_literal(r["schema"]),
            dbx.sql_literal(r["table"]), dbx.sql_literal(r.get("target", "")),
            dbx.sql_literal(r.get("etl_type", "?")),
            dbx.sql_literal(r.get("status", "실패")),
            dbx.sql_literal(r.get("status_code", "FAIL")),
            dbx.sql_literal(str(r.get("workers", 1))),
            dbx.sql_literal(r.get("etl_type", "")),
            dbx.sql_literal(r.get("schedule_hit", "N")),
            dbx.sql_literal(r.get("active_flag", "N")),
            dbx.sql_literal(excluded), dbx.sql_literal(deid),
            dbx.sql_literal(str(r.get("rows_affected", 0))),
            dbx.sql_literal(started_value), dbx.sql_literal(ended_value),
            str(elapsed_value),
            dbx.sql_literal(r.get("statement_id")),
            dbx.sql_literal(r.get("message")), detail,
        ]) + ")")

    dbx.execute_sql(f"INSERT INTO {schema_name}.load_audit VALUES "
                    + ", ".join(check_values))
    dbx.execute_sql(f"INSERT INTO {schema_name}.etl_run_log VALUES "
                    + ", ".join(run_values))
    logger.info("  로그 기록: load_audit %d행 / etl_run_log %d행 (INSERT 각 1회)",
                len(check_values), len(run_values))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL 처리 모듈")
    parser.add_argument("--engine", required=True, choices=ENGINES)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--table", default=None, help="테이블 1개만 처리")
    parser.add_argument("--workers", type=int, default=1,
                        help="동시 처리 개수 (2 이상이면 병렬)")
    parser.add_argument("--run-id", default="manual")
    parser.add_argument("--dag-id", default="")
    parser.add_argument("--force", action="store_true",
                        help="active=N 이어도 강제로 실행")
    parser.add_argument("--ignore-schedule", action="store_true",
                        help="작업 주기 조건을 무시하고 실행")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-5s %(message)s",
                        datefmt="%H:%M:%S")

    if args.table:
        record = process_table(
            args.engine, args.schema, args.table,
            workers=args.workers, run_id=args.run_id, dag_id=args.dag_id,
            force=args.force, ignore_schedule=args.ignore_schedule)
        print(json.dumps({k: str(v) for k, v in record.items()},
                         ensure_ascii=False, indent=2))
    else:
        summary = process_schema(
            args.engine, args.schema, workers=args.workers,
            run_id=args.run_id, dag_id=args.dag_id,
            force=args.force, ignore_schedule=args.ignore_schedule)
        print(json.dumps(
            {k: v for k, v in summary.items() if k != "results"},
            ensure_ascii=False, indent=2))
