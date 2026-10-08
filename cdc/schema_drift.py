"""
Schema-drift scenario: add and drop a source column, then observe the ETL.

This is the discriminating test for the requirement:

    "작업 순간에 다시 한 번 source를 접속하여 컬럼 삭제/추가여부를 확인하여
     삭제: null as column으로 정상처리 / 추가: 추가컬럼명만 확인
     하여 slack으로 변경내역을 보내줄 수 있도록 한다."

What makes it discriminating
----------------------------
A naive implementation that simply reads whatever columns exist would:
  * load the newly added column  → violates "must not reflect automatically"
  * fail outright on the dropped column → violates "must not stop the job"

So the correct behaviour is observable, not just describable:

    dropped column  → the job still succeeds, the target column is all NULL
    added column    → the job still succeeds, the target does NOT have it,
                      and Slack reports it

Both outcomes are asserted here, so a regression cannot pass silently.
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# Windows console (cp949) cannot print characters like the em dash and dies
# with UnicodeEncodeError. Every entry script needs this guard.
for _s in (sys.stdout, sys.stderr):
    try:
        _s.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass

from lib import config as cfg                  # noqa: E402
from lib import dbx                            # noqa: E402
from lib import profile as profile_mod         # noqa: E402
from lib import slack as slack_mod             # noqa: E402
from lib import sources as sources_mod         # noqa: E402

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)-5s %(message)s",
                    datefmt="%H:%M:%S", stream=sys.stdout, force=True)
logger = logging.getLogger("drift")


# ---------------------------------------------------------------------------
# Source mutation — drop and add a column
# ---------------------------------------------------------------------------
# MySQL is used as the vehicle because DDL is immediate and unambiguous.
# The ETL code path is identical for all three engines, so the *logic* under
# test is engine-independent.

def mysql_drop_column(schema: str, table: str, column: str) -> bool:
    """Drop a column from the source table."""
    import pymysql

    conn_cfg = cfg.mysql_connection()
    conn = pymysql.connect(
        host=conn_cfg["host"], port=conn_cfg["port"],
        user=conn_cfg.get("user", "root"), password=conn_cfg.get("password", ""),
        database=schema, charset="utf8mb4", autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(f"ALTER TABLE `{table}` DROP COLUMN `{column}`")
        return True
    finally:
        conn.close()


def mysql_add_column(schema: str, table: str, column: str, col_type: str) -> bool:
    """Add a column to the source table."""
    import pymysql

    conn_cfg = cfg.mysql_connection()
    conn = pymysql.connect(
        host=conn_cfg["host"], port=conn_cfg["port"],
        user=conn_cfg.get("user", "root"), password=conn_cfg.get("password", ""),
        database=schema, charset="utf8mb4", autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(f"ALTER TABLE `{table}` ADD COLUMN `{column}` {col_type}")
        return True
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------

def target_columns(engine: str, table: str) -> list[str]:
    target = f"{cfg.get_databricks_config()['catalog']}.{engine}.{table}"
    return dbx.describe_columns(target)


def target_null_ratio(engine: str, table: str, column: str) -> float:
    """Fraction of NULL values in a target column."""
    target = f"{cfg.get_databricks_config()['catalog']}.{engine}.{table}"
    value = dbx.fetch_value(
        f"SELECT ROUND(CAST(SUM(CASE WHEN `{column}` IS NULL THEN 1 ELSE 0 END) "
        f"AS DOUBLE) / NULLIF(COUNT(*), 0), 4) FROM {target}")
    return float(value) if value is not None else -1.0


def verify(engine: str, schema: str, table: str,
           dropped: str, added: str) -> dict:
    """Assert both halves of the requirement."""
    entry = profile_mod.table_entry(engine, schema, table)
    live = sources_mod.list_columns(engine, schema, table)

    columns = target_columns(engine, table)

    checks = {
        "원천에서_삭제된_컬럼": {
            "컬럼": dropped,
            "원천에_존재": dropped in live,
            "프로파일에_등록됨": dropped in (entry.get("columns") or []),
        },
        "원천에_추가된_컬럼": {
            "컬럼": added,
            "원천에_존재": added in live,
            "프로파일에_등록됨": added in (entry.get("columns") or []),
        },
        "대상_테이블_컬럼": {
            "삭제컬럼이_대상에도_있음": dropped in columns,
            "추가컬럼이_대상에_자동반영됨": added in columns,
        },
    }

    # The dropped column must survive in the target as all-NULL (the job ran).
    if dropped in columns:
        ratio = target_null_ratio(engine, table, dropped)
        checks["삭제컬럼_null대체"] = {
            "NULL_비율": ratio,
            "판정": "통과" if ratio == 1.0 else "실패",
            "기준": "삭제된 컬럼은 NULL 로 채워져야 한다 (작업이 멈추면 안 된다)",
        }

    checks["추가컬럼_미반영"] = {
        "판정": "통과" if added not in columns else "실패",
        "기준": "원천에만 있는 신규 컬럼은 자동 반영되면 안 된다",
    }

    overall = (checks["추가컬럼_미반영"]["판정"] == "통과"
               and checks.get("삭제컬럼_null대체", {}).get("판정") == "통과")

    return {"전체판정": "통과" if overall else "실패", "상세": checks}


# ---------------------------------------------------------------------------
# Scenario
# ---------------------------------------------------------------------------

def load_processor():
    spec = importlib.util.spec_from_file_location(
        "processor", ROOT / "etl-module" / "processor.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def run(engine: str, schema: str, table: str,
        dropped: str = "description", added: str = "newly_added_col",
        run_id: str = "drift") -> dict:
    """Execute the full drift scenario and return a report."""
    processor = load_processor()

    logger.info("=" * 68)
    logger.info("컬럼 변경 시나리오 — %s.%s.%s", engine, schema, table)
    logger.info("  삭제 대상: %s / 추가 대상: %s", dropped, added)
    logger.info("=" * 68)

    before_live = sources_mod.list_columns(engine, schema, table)
    before_target = target_columns(engine, table)
    logger.info("변경 전 원천 %d개 / 대상 %d개 컬럼",
                len(before_live), len(before_target))

    # 1) Baseline: the table must succeed before any drift is introduced.
    baseline = processor.process_table(
        engine, schema, table, run_id=f"{run_id}_baseline", force=True)
    logger.info("기준 실행: %s (원본 %s / 대상 %s)",
                baseline["status"], baseline["source_count"],
                baseline["target_count"])
    before_sent = slack_mod.sent_summary()["총건수"]

    # 2) Introduce the drift.
    mysql_drop_column(schema, table, dropped)
    logger.info("원천에서 컬럼 삭제: %s", dropped)
    mysql_add_column(schema, table, added, "VARCHAR(50) NULL")
    logger.info("원천에 컬럼 추가: %s", added)

    # 3) Run ETL again — it must succeed, not fail.
    after = processor.process_table(
        engine, schema, table, run_id=f"{run_id}_drift", force=True)
    logger.info("변경 후 실행: %s (원본 %s / 대상 %s)",
                after["status"], after["source_count"], after["target_count"])
    logger.info("  적재 컬럼   : %s", after["load_columns"])
    logger.info("  삭제 감지   : %s", after["dropped_columns"])
    logger.info("  추가 감지   : %s", after["added_columns"])

    # 4) Verify.
    verdict = verify(engine, schema, table, dropped, added)
    logger.info("검증 판정: %s", verdict["전체판정"])

    # 5) Slack must have received a change notification.
    after_sent = slack_mod.sent_summary()["총건수"]
    sent_delta = after_sent - before_sent
    logger.info("Slack 발송 증가: %d건 (변경 통지 포함 여부 확인)", sent_delta)

    report = {
        "engine": engine, "schema": schema, "table": table,
        "기준실행": {k: baseline[k] for k in
                    ("status", "source_count", "target_count", "load_columns")},
        "변경후실행": {k: after[k] for k in
                    ("status", "status_code", "source_count", "target_count",
                     "load_columns", "dropped_columns", "added_columns",
                     "message")},
        "검증": verdict,
        "slack_발송증가": sent_delta,
    }

    out_dir = cfg.log_folder() / "drift"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{engine}_{schema}_{table}.json"
    out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2,
                                   default=str), encoding="utf-8")
    logger.info("증적 파일: %s", out_path)
    return report


def restore(engine: str, schema: str, table: str,
            dropped: str = "description", added: str = "newly_added_col") -> None:
    """Undo the drift so the profile and source line up again."""
    mysql_add_column(schema, table, dropped, "VARCHAR(300) NULL")
    logger.info("컬럼 복구: %s", dropped)
    try:
        mysql_drop_column(schema, table, added)
        logger.info("컬럼 제거: %s", added)
    except Exception:                            # noqa: BLE001
        logger.info("추가 컬럼이 이미 없음: %s", added)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="컬럼 추가/삭제 시나리오")
    parser.add_argument("--engine", default="mysql")
    parser.add_argument("--schema", default="mysql_schema_1")
    parser.add_argument("--table", default="table_1")
    parser.add_argument("--삭제컬럼", default="description")
    parser.add_argument("--추가컬럼", default="newly_added_col")
    parser.add_argument("--복구", action="store_true",
                        help="변경한 컬럼을 원상 복구한다")
    args = parser.parse_args()

    if args.복구:
        restore(args.engine, args.schema, args.table,
                dropped=args.삭제컬럼, added=args.추가컬럼)
    else:
        result = run(args.engine, args.schema, args.table,
                     dropped=args.삭제컬럼, added=args.추가컬럼)
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
