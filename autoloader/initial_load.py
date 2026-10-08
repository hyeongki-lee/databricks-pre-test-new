"""
Auto Loader — ingest the Iceberg-format files from S3 into a Databricks
managed table (initial load).

Responsibility boundary (this is the important part)
-----------------------------------------------------
DOES:
  1. Verify both signals (`_chk.json` and `_rclone_done.json`).
  2. Create the managed table from the file schema.
  3. Ingest the files (INSERT OVERWRITE, because it is the *initial* load).
  4. Compare source vs target row counts and write one `load_audit` row.

DOES NOT:
  ✗ Handle dropped columns with `CAST(NULL AS col)` — that is the **ETL
    module's** job.
  ✗ Detect column drift — the ETL module reconnects to the *source* at run
    time and decides based on live state.

Why they are separated
----------------------
The requirement text mentions dropped-column handling in two places:

    초기이관 5번 : "컬럼 삭제 → null as column으로 정상처리"
    ETL 모듈 2번 : "작업 순간에 다시 한 번 source를 접속하여 컬럼 삭제/추가
                   여부를 확인하여 삭제: null as column으로 정상처리"

The operative phrase is **"reconnect to the source at the moment of the run"**.
Auto Loader only sees S3 files; it cannot know the current state of the source
database. So the component with actual evidence is the ETL module.

Therefore:
  * Auto Loader ingests only what the file contains, restricted to columns the
    target already has. A brand-new column in the file is **not** added
    automatically (no arbitrary reflection) — it is reported.
  * ETL Module re-reads the source, fills dropped columns with
    `CAST(NULL AS <type>) AS <col>`, and reports added columns.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from lib import config as cfg                  # noqa: E402
from lib import dbx                            # noqa: E402
from lib import logtable                       # noqa: E402
from lib import slack as slack_mod             # noqa: E402

logger = logging.getLogger("autoloader")

ENGINES = ["mysql", "mongodb", "postgresql"]


# ---------------------------------------------------------------------------
# Signal verification
# ---------------------------------------------------------------------------

def _list_keys(prefix: str) -> list[str]:
    import boto3

    s3 = cfg.get_s3_config()
    client = boto3.client("s3", region_name=s3.get("region", "ap-northeast-2"))
    prefix = prefix.lstrip("/")
    keys: list[str] = []
    for page in client.get_paginator("list_objects_v2").paginate(
            Bucket=s3["bucket"], Prefix=prefix):
        keys += [o["Key"] for o in page.get("Contents", [])]
    return keys


def check_signals(engine: str, schema: str) -> dict:
    """Require **both** signals.

    `chk` alone cannot tell us whether rclone finished the transfer.
    `rclone_done` alone cannot tell us whether the source data is complete.
    Only together do they establish "the data is ready and it has all arrived".
    """
    s3 = cfg.get_s3_config()
    prefix = f"{s3.get('prefix', 'pretest')}/{engine}/{schema}"
    keys = _list_keys(prefix + "/")

    chk = [k for k in keys if k.endswith(s3.get("chk_file", "_chk.json"))]
    done = [k for k in keys if s3.get("rclone_done_file", "_rclone_done.json") in k]
    parquet = [k for k in keys if k.endswith(".parquet")]
    metadata = [k for k in keys if "/metadata/" in k]

    if not chk:
        reason = "chk 없음 — 데이터 준비가 아직 끝나지 않음"
    elif not done:
        reason = "rclone_done 없음 — 복제가 아직 끝나지 않음"
    elif not parquet:
        reason = "parquet 없음 — 복제된 데이터가 없음"
    else:
        reason = "적재 가능"

    return {
        "engine": engine, "schema": schema,
        "s3_prefix": f"s3://{s3['bucket']}/{prefix}",
        "chk": len(chk), "rclone_done": len(done),
        "parquet_files": len(parquet), "metadata_files": len(metadata),
        "total_objects": len(keys),
        "ready": reason == "적재 가능",
        "reason": reason,
    }


def file_path(engine: str, schema: str, table: str) -> str:
    """S3 path of one table's parquet folder."""
    s3 = cfg.get_s3_config()
    return (f"s3://{s3['bucket']}/{s3.get('prefix', 'pretest')}"
            f"/{engine}/{schema}/{table}/data")


def target_table(engine: str, table: str) -> str:
    """Fully qualified Databricks table name."""
    return f"{cfg.get_databricks_config()['catalog']}.{engine}.{table}"


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

#: Type map keyed by column name. The data generator uses fixed names, so
#: this keeps the Databricks schema consistent across all 60 tables.
COLUMN_TYPES = {
    "id": "BIGINT",
    "age": "INT",
    "salary": "DECIMAL(18,2)",
    "created_at": "TIMESTAMP",
    "updated_at": "TIMESTAMP",
    "is_active": "BOOLEAN",
}


def column_type(name: str) -> str:
    """Target type for a column name (STRING by default)."""
    return COLUMN_TYPES.get(name, "STRING")


def read_file_schema(engine: str, schema: str, table: str) -> list[str]:
    """Actual column order of the parquet files."""
    sql = f"SELECT * FROM parquet.`{file_path(engine, schema, table)}` LIMIT 0"
    result = dbx.execute_sql(sql)
    return [c["name"] for c in result["columns"]]


def count_file_rows(engine: str, schema: str, table: str) -> int:
    """Row count of the source files."""
    sql = f"SELECT COUNT(*) AS cnt FROM parquet.`{file_path(engine, schema, table)}`"
    return int(dbx.fetch_value(sql))


def intersect_columns(target_full: str, file_columns: list[str]) -> tuple[list[str], list[str]]:
    """Restrict ingestion to the intersection with the target table.

    Deliberately **no** `CAST(NULL AS ...)` here:
      * Columns missing from the file are simply omitted from the INSERT.
        Delta fills omitted columns with NULL on its own.
      * Columns new in the file are omitted too (no automatic reflection) and
        reported instead.
      Making dropped columns explicit (`CAST(NULL AS …)`) is the ETL module's
      responsibility, because only ETL sees the live source schema.
    """
    if not dbx.table_exists(target_full):
        return list(file_columns), []
    existing = dbx.describe_columns(target_full)
    common = [c for c in existing if c in file_columns]
    added = [c for c in file_columns if c not in existing]
    return common, added


def ensure_table(engine: str, schema: str, table: str,
                 columns: list[str], replace: bool) -> str:
    """Create the managed table.

    ⚠ Measured: issuing `DROP TABLE IF EXISTS` immediately followed by
      `CREATE` raises `TABLE_OR_VIEW_ALREADY_EXISTS`, and the table does not
      even show up in SHOW. Use `CREATE OR REPLACE` instead.

    ⚠ Measured: a **two-part** name must be backticked per part.
      Writing ``CREATE SCHEMA `workspace.mysql` `` makes Databricks read the
      whole thing as ONE identifier and reject it:
          INVALID_NAME_FORMAT — CreateSchema name "workspace.mysql" is not a
          valid name. Valid names cannot contain periods.
      So only the schema part gets quoted: `workspace`.`mysql`.
    """
    catalog = cfg.get_databricks_config()["catalog"]
    target_schema = f"{catalog}.{engine}"

    dbx.execute_sql(
        f"CREATE SCHEMA IF NOT EXISTS {dbx.identifier(catalog)}."
        f"{dbx.identifier(engine)} "
        "COMMENT 'databricks-pre-test-new Auto Loader 적재'")

    definition = ",\n".join(f"  `{c}` {column_type(c)}" for c in columns)
    if replace:
        dbx.execute_sql(
            f"CREATE OR REPLACE TABLE {target_schema}.{table} (\n{definition}\n)\n"
            f"USING DELTA\nCOMMENT '{engine}.{schema}.{table} 초기 이관'")
    else:
        dbx.execute_sql(
            f"CREATE TABLE IF NOT EXISTS {target_schema}.{table} (\n{definition}\n)\n"
            f"USING DELTA\nCOMMENT '{engine}.{schema}.{table} 초기 이관'")

    logger.info("  테이블 준비: %s.%s (%d개 컬럼)",
                target_schema, table, len(columns))
    return f"{target_schema}.{table}"


# ---------------------------------------------------------------------------
# Single table
# ---------------------------------------------------------------------------

def load_table(engine: str, schema: str, table: str, *,
               replace: bool = True) -> dict:
    """Ingest one table and return a result record."""
    start = time.time()
    started_at = datetime.now()
    target = target_table(engine, table)
    path = file_path(engine, schema, table)

    result = {
        "engine": engine, "schema": schema, "table": table, "target": target,
        "status": "성공", "source_count": 0, "target_count": 0,
        "count_match": False, "load_columns": [], "skipped_new_columns": [],
        "duration_sec": 0.0, "message": "", "started_at": started_at,
    }

    try:
        source_count = count_file_rows(engine, schema, table)
        file_columns = read_file_schema(engine, schema, table)
        result["source_count"] = source_count

        if dbx.table_exists(target) and not replace:
            load_columns, added = intersect_columns(target, file_columns)
        else:
            load_columns, added = list(file_columns), []

        result["load_columns"] = load_columns
        result["skipped_new_columns"] = added
        if added:
            logger.info("  [%s.%s] 파일 신규 컬럼 %s → 자동 추가하지 않음",
                        schema, table, added)

        ensure_table(engine, schema, table, load_columns, replace)

        column_list = ", ".join(f"`{c}`" for c in load_columns)
        # Initial load: make the target match the files exactly, otherwise
        # the row-count comparison would be meaningless.
        sql = (f"INSERT OVERWRITE {target} ({column_list})\n"
               f"SELECT {column_list}\nFROM parquet.`{path}`")
        execution = dbx.execute_sql(sql)
        result["statement_id"] = execution.get("statement_id")

        target_count = int(dbx.fetch_value(
            f"SELECT COUNT(*) AS cnt FROM {target}"))
        result["target_count"] = target_count
        result["count_match"] = (source_count == target_count)
        result["message"] = (
            f"적재 완료 (원본 {source_count:,} / 대상 {target_count:,})"
            if result["count_match"] else
            f"건수 불일치 (원본 {source_count:,} / 대상 {target_count:,})")
        if not result["count_match"]:
            result["status"] = "실패"

        if added:
            # A new column needs a human decision — never auto-add it.
            slack_mod.notify(
                "파일신규컬럼",
                f"원천 파일에 신규 컬럼이 있습니다 — {engine}.{schema}.{table}",
                [("데이터베이스 종류", engine), ("스키마", schema),
                 ("테이블", table),
                 ("신규 컬럼", ", ".join(added)),
                 ("처리 방법", "자동 반영하지 않았습니다. 대상 테이블 컬럼만 적재했습니다."),
                 ("사용자 조치",
                  "ETL 프로파일의 columns 에 등록할지 판단해 주십시오.")],
                severity="warning")

    except Exception as exc:                    # noqa: BLE001
        result["status"] = "실패"
        result["message"] = f"{type(exc).__name__}: {str(exc)[:400]}"
        logger.error("  [%s.%s] 실패: %s", schema, table, result["message"])

    ended_at = datetime.now()
    result["ended_at"] = ended_at
    result["duration_sec"] = round((ended_at - started_at).total_seconds(), 2)
    return result


# ---------------------------------------------------------------------------
# Schema level
# ---------------------------------------------------------------------------

def load_schema(engine: str, schema: str, *, replace: bool = True,
               run_id: str = "", dag_id: str = "") -> dict:
    """Ingest every table of one schema.

    Measured: a fixed `run_id` of "manual" makes every run indistinguishable in
    the audit tables, so the 200-row smoke runs and the 50,000-row verification
    all collapse into one bucket. When no id is supplied a timestamped one is
    generated, which is what lets the aggregates be scoped to a single run.
    """
    if not run_id:
        run_id = f"initial_{datetime.now():%Y%m%d_%H%M%S}"
    """Ingest every table of one schema."""
    logger.info("=" * 68)
    logger.info("[%s.%s] Auto Loader 초기 이관 (replace=%s)", engine, schema, replace)
    logger.info("=" * 68)

    signals = check_signals(engine, schema)
    logger.info("  신호: chk=%s rclone_done=%s parquet=%s metadata=%s → %s",
                signals["chk"], signals["rclone_done"],
                signals["parquet_files"], signals["metadata_files"],
                signals["reason"])
    if not signals["ready"]:
        logger.warning("  [%s.%s] 적재 보류: %s", engine, schema, signals["reason"])
        return {"engine": engine, "schema": schema, "status": "건너뜀",
                "reason": signals["reason"], "signals": signals, "results": []}

    source = cfg.get_section("sources", {})[engine]
    table_count = int(source.get("tables_per_schema", 5))
    tables = [f"table_{i}" for i in range(1, table_count + 1)]

    results = []
    for table in tables:
        record = load_table(engine, schema, table, replace=replace)
        results.append(record)
        logger.info("  %s.%s [%s] 원본 %s / 대상 %s (%.1fs)",
                    schema, table, "OK" if record["count_match"] else "NG",
                    f"{record['source_count']:,}",
                    f"{record['target_count']:,}",
                    record["duration_sec"])

    succeeded = sum(1 for r in results if r["status"] == "성공")
    failed = sum(1 for r in results if r["status"] == "실패")

    write_logs(results, run_id, work_type="initial_load")

    summary = {
        "engine": engine, "schema": schema, "signals": signals,
        "succeeded": succeeded, "failed": failed,
        "total": len(results), "results": results,
    }
    logger.info("[%s.%s] 완료 · 성공 %d / 실패 %d",
                engine, schema, succeeded, failed)

    slack_mod.notify(
        "초기이관",
        f"Auto Loader 초기 이관 {'성공' if failed == 0 else '부분성공'} — "
        f"{engine}.{schema} ({succeeded}/{len(results)}건)",
        [("데이터베이스 종류", engine), ("스키마", schema),
         ("성공 / 실패", f"{succeeded} / {failed}"),
         ("원본 총건수", f"{sum(r['source_count'] for r in results):,}"),
         ("대상 총건수", f"{sum(r['target_count'] for r in results):,}"),
         ("chk 파일 수", signals["chk"]),
         ("rclone_done", signals["rclone_done"]),
         ("방법", "rclone RC API copy → S3 → parquet 직접 읽기"),
         ("책임 경계", "컬럼 삭제(null as) 처리는 ETL 모듈 담당")],
        severity="success" if failed == 0 else "warning",
        dag_id=dag_id)
    return summary


# ---------------------------------------------------------------------------
# Audit log (one row per unit of work)
# ---------------------------------------------------------------------------

def write_logs(results: list[dict], run_id: str,
               work_type: str = "initial_load") -> None:
    """Write both audit tables with **one INSERT each**.

    ⚠ One INSERT per table would cost 60 statements; Free Edition has a daily
      query quota, so batching per schema is the difference between finishing
      and hitting the wall.
    """
    if not results:
        return

    schema_name = logtable.meta_schema()
    ended_at = datetime.now()
    check_values: list[str] = []
    run_values: list[str] = []

    for r in results:
        source_count, target_count = r["source_count"], r["target_count"]
        detail = dbx.sql_literal(json.dumps({
            "load_columns": r.get("load_columns"),
            "skipped_new_columns": r.get("skipped_new_columns"),
            "responsibility": "null as 처리는 ETL 모듈이 담당",
        }, ensure_ascii=False))
        skipped = ",".join(r.get("skipped_new_columns") or []) or None

        check_values.append("(" + ", ".join([
            dbx.sql_literal(run_id), dbx.sql_literal(work_type),
            dbx.sql_literal(r["engine"]), dbx.sql_literal(r["schema"]),
            dbx.sql_literal(r["table"]), dbx.sql_literal(r["target"]),
            dbx.sql_literal("initial_load"),
            str(source_count), str(target_count), str(target_count - source_count),
            dbx.sql_literal("Y" if r["count_match"] else "N"),
            dbx.sql_literal(skipped), dbx.sql_literal(None),
            dbx.sql_literal(None), dbx.sql_literal("0"),
            dbx.sql_literal(r["started_at"]), dbx.sql_literal(ended_at),
            str(float(r["duration_sec"])),
            dbx.sql_literal(r["message"]), detail,
        ]) + ")")

        run_values.append("(" + ", ".join([
            dbx.sql_literal(run_id), dbx.sql_literal(work_type),
            dbx.sql_literal(r["engine"]), dbx.sql_literal(r["schema"]),
            dbx.sql_literal(r["table"]), dbx.sql_literal(r["target"]),
            dbx.sql_literal("initial_load"), dbx.sql_literal(r["status"]),
            dbx.sql_literal("OK" if r["status"] == "성공" else "FAIL"),
            dbx.sql_literal("1"), dbx.sql_literal(None),
            dbx.sql_literal("N"), dbx.sql_literal("N"),
            dbx.sql_literal(None), dbx.sql_literal(None), dbx.sql_literal("0"),
            dbx.sql_literal(r["started_at"]), dbx.sql_literal(ended_at),
            str(float(r["duration_sec"])),
            dbx.sql_literal(r.get("statement_id")),
            dbx.sql_literal(r["message"]), detail,
        ]) + ")")

    dbx.execute_sql(f"INSERT INTO {schema_name}.load_audit VALUES "
                    + ", ".join(check_values))
    dbx.execute_sql(f"INSERT INTO {schema_name}.etl_run_log VALUES "
                    + ", ".join(run_values))
    logger.info("  로그 기록: load_audit %d행 / etl_run_log %d행 (INSERT 각 1회)",
                len(check_values), len(run_values))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Auto Loader 초기 이관")
    parser.add_argument("--engine", default=None)
    parser.add_argument("--schema", default=None)
    parser.add_argument("--증분", action="store_true",
                        help="INSERT INTO 로 증분 적재 (기본은 INSERT OVERWRITE)")
    parser.add_argument("--신호만", action="store_true",
                        help="신호 상태만 확인하고 종료")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-5s %(message)s",
                        datefmt="%H:%M:%S")

    if args.신호만:
        for engine in ([args.engine] if args.engine else ENGINES):
            for schema in cfg.get_section("sources", {})[engine]["schemas"]:
                signals = check_signals(engine, schema)
                print(f"  [{'OK ' if signals['ready'] else '대기'}] "
                      f"{engine}.{schema}: {signals['reason']} "
                      f"(chk={signals['chk']} done={signals['rclone_done']} "
                      f"parquet={signals['parquet_files']} "
                      f"metadata={signals['metadata_files']})")
        sys.exit(0)

    targets = ([(args.engine, args.schema)] if args.schema else
               [(e, s)
                for e in ([args.engine] if args.engine else ENGINES)
                for s in cfg.get_section("sources", {})[e]["schemas"]])
    for engine, schema in targets:
        load_schema(engine, schema, replace=not args.증분)
