"""
Audit log tables (2 of them).

Requirement
-----------
  (1) Compare row counts between source and target, and record the result in a
      **check table**.
  (2) Record the success/failure of each job in a separate **run log table**.

      "Design both tables as detailed as possible, but store exactly one row
       per unit of work, and verify that design."

Namespace
---------
Uses `workspace.pretest_meta`.

⚠ `workspace.meta` belongs to an earlier project (etl-fabric). This project
  must not touch it, and never creates it.

Design — one row per unit of work
---------------------------------
A single INSERT carries many VALUES tuples, so 60 tables cost a handful of
statements instead of 60. Free Edition has a daily query quota, and per-table
INSERTs are the fastest way to exhaust it.

Column inventory (reproduced verbatim in the manual)
----------------------------------------------------
    run_id           : ties rows from the same DAG run together
    work_type        : what this row records (initial_load / etl)
    engine / schema_name / table_name : minimal unit of aggregation
    etl_type         : append / truncate / merge / initial_load
    status           : 성공 / 실패 / 건너뜀 / 스킵
    status_code      : machine-readable (OK / FAIL / SKIP)
    source_count / target_count / count_diff / count_match : the two axes of
        the count comparison
    column_added / column_deleted : schema drift, so a single row tells the
        whole story without a second lookup
    excluded_columns : columns removed by profile configuration
    deid_applied     : which de-identification codes ran (e.g. name=D1)
    duration_sec / started_at / ended_at : timing
    statement_id     : Databricks statement id for tracing
    message          : failure reason, or '-' when normal
    detail_json      : everything else, as JSON text

`detail_json` exists so that new information never forces a schema change —
that is what keeps the table stable across runs.
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from lib import config as cfg                  # noqa: E402
from lib import dbx                            # noqa: E402

logger = logging.getLogger("logtable")


# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

DDL_LOAD_AUDIT = """
CREATE TABLE IF NOT EXISTS {schema}.load_audit (
  run_id           STRING    NOT NULL COMMENT 'DAG 실행 식별자',
  work_type        STRING    NOT NULL COMMENT 'initial_load / etl',
  engine           STRING    NOT NULL COMMENT '원천 엔진 (mysql/mongodb/postgresql)',
  schema_name      STRING    NOT NULL COMMENT '원천 스키마',
  table_name       STRING    NOT NULL COMMENT '원천 테이블',
  target_table     STRING    NOT NULL COMMENT '적재된 대상 테이블 전체 이름',
  etl_type         STRING    NOT NULL COMMENT 'initial_load/append/truncate/merge',
  source_count     BIGINT    NOT NULL COMMENT '원본 건수',
  target_count     BIGINT    NOT NULL COMMENT '대상 건수',
  count_diff       BIGINT    NOT NULL COMMENT '대상-원본 차이',
  count_match      STRING    NOT NULL COMMENT 'Y=일치 N=불일치',
  column_added     STRING    COMMENT '원천에만 있는 컬럼(미반영, 개발자 확인 요망)',
  column_deleted   STRING    COMMENT '프로파일에만 있는 컬럼(ETL 이 NULL 대체)',
  excluded_columns STRING    COMMENT 'ETL 제외로 빠진 컬럼',
  file_count       BIGINT    COMMENT '적재 대상 파일 수',
  started_at       TIMESTAMP NOT NULL,
  ended_at         TIMESTAMP NOT NULL,
  duration_sec     DOUBLE    NOT NULL,
  message          STRING    COMMENT '특이사항. 정상이면 NULL',
  detail_json      STRING    COMMENT '그 밖의 부가 정보(JSON)'
)
USING DELTA
COMMENT '파일 적재 건수 체크 기록 — 작업 1건당 1행'
"""

DDL_ETL_RUN_LOG = """
CREATE TABLE IF NOT EXISTS {schema}.etl_run_log (
  run_id           STRING    NOT NULL COMMENT 'DAG 실행 식별자',
  work_type        STRING    NOT NULL COMMENT 'initial_load / etl',
  engine           STRING    NOT NULL,
  schema_name      STRING    NOT NULL,
  table_name       STRING    NOT NULL,
  target_table     STRING    NOT NULL,
  etl_type         STRING    NOT NULL COMMENT 'append/truncate/merge',
  status           STRING    NOT NULL COMMENT '성공/실패/건너뜀/스킵',
  status_code      STRING    NOT NULL COMMENT 'machine 판정용 (OK/FAIL/SKIP)',
  workers          INT       COMMENT '동시처리 개수',
  schedule_code    STRING    COMMENT '작업 주기 코드',
  schedule_hit     STRING    COMMENT '이번 실행이 주기 조건에 걸렸는가 Y/N',
  active_flag      STRING    COMMENT '실행 당시 프로파일 활성 플래그',
  excluded_columns STRING    COMMENT '제외된 컬럼',
  deid_applied     STRING    COMMENT '적용된 비식별화 코드 (예: name=D1,phone=D3)',
  row_affected     BIGINT    COMMENT 'MERGE 로 갱신된 행 수',
  started_at       TIMESTAMP NOT NULL,
  ended_at         TIMESTAMP NOT NULL,
  duration_sec     DOUBLE    NOT NULL,
  statement_id     STRING    COMMENT 'Databricks statement 식별자(추적용)',
  message          STRING    COMMENT '실패 사유 / 처리 메모',
  detail_json      STRING
)
USING DELTA
COMMENT 'ETL 작업 실행 기록 — 작업 1건당 1행'
"""


def meta_schema() -> str:
    """Fully qualified name of the audit schema."""
    settings = cfg.get_databricks_config()
    return f"{settings['catalog']}.{settings.get('meta_schema', 'pretest_meta')}"


def create_tables() -> dict:
    """Create the audit schema and both tables."""
    full = meta_schema()
    catalog, name = full.split(".")

    logger.info("로그 스키마/테이블 생성: %s", full)
    dbx.execute_sql(
        f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{name}` "
        "COMMENT 'databricks-pre-test-new 로그·감사'")
    dbx.execute_sql(DDL_LOAD_AUDIT.format(schema=full))
    dbx.execute_sql(DDL_ETL_RUN_LOG.format(schema=full))

    # Verify that the commented columns actually landed.
    verified: dict = {}
    for table in ("load_audit", "etl_run_log"):
        described = dbx.execute_sql(f"DESCRIBE TABLE {full}.{table}")
        columns = [row[0] for row in described["rows"] if row and row[0]]
        verified[table] = columns
        logger.info("  %s.%s : %d개 컬럼", full, table, len(columns))

    logger.info("생성 완료")
    return {"schema": full, "tables": verified}


def truncate_all() -> None:
    """Empty both tables (used when repeating a test run)."""
    full = meta_schema()
    dbx.execute_sql(f"TRUNCATE TABLE {full}.load_audit")
    dbx.execute_sql(f"TRUNCATE TABLE {full}.etl_run_log")
    logger.info("로그 테이블 초기화 완료")


def row_counts() -> dict:
    """Current row counts in both tables."""
    full = meta_schema()
    return {
        "load_audit": dbx.fetch_value(f"SELECT COUNT(*) FROM {full}.load_audit"),
        "etl_run_log": dbx.fetch_value(f"SELECT COUNT(*) FROM {full}.etl_run_log"),
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="로그 테이블 생성/확인")
    parser.add_argument("--초기화", action="store_true",
                        help="기존 로그를 비운다")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-5s %(message)s",
                        datefmt="%H:%M:%S")
    create_tables()
    if args.초기화:
        truncate_all()
    print(row_counts())
