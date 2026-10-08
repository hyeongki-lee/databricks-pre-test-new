"""
CDC loader — applies the change stream to the Databricks target and proves it.

The single statement
--------------------
One `MERGE` expresses insert, update and delete together:

    MERGE INTO target AS t
    USING delta AS s
    ON t.id = s.id
    WHEN MATCHED AND s._cdc_op = 'D' THEN DELETE
    WHEN MATCHED THEN UPDATE SET ...
    WHEN NOT MATCHED AND s._cdc_op <> 'D' THEN INSERT *

That `WHEN MATCHED AND ... THEN DELETE` clause is what the first attempt was
missing, and its absence is precisely why deleted rows survived.

Verification is on **values**, not row counts
--------------------------------------------
Row counts alone would have passed the broken implementation, because an
append-only load still moves the total. So each of the three change kinds is
asserted separately:

    A. UPDATE  — every updated id must carry the change marker in its values
    B. DELETE  — no deleted id may remain in the target
    C. INSERT  — every new id must be present
    D. count   — net delta must equal inserts − deletes
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime
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

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)-5s %(message)s",
                    datefmt="%H:%M:%S", stream=sys.stdout, force=True)
logger = logging.getLogger("cdc-load")

#: Must match `cdc/simulate.py`.
#: Marker text written into `description` by every engine on an UPDATE.
#: Measured: the SQL engines write "[changed]" while MongoDB writes
#: "CDC [changed]". Verifying the Korean substring only matched two of the
#: three engines and produced a false failure, so the marker is now a
#: single shared constant that all three writers and the checker use.
CDC_CHANGE_MARKER = "CDC [changed]"

OP_COLUMN = "_cdc_op"
OP_DELETE = "D"

#: Columns updated on a MATCHED row (the key itself is excluded).
UPDATE_COLUMNS = ["name", "email", "phone", "address", "age", "salary",
                  "created_at", "updated_at", "is_active", "description"]


def target_table(engine: str, table: str) -> str:
    return f"{cfg.get_databricks_config()['catalog']}.{engine}.{table}"


def delta_path(engine: str, schema: str, table: str) -> str:
    """Path of the CDC change stream (delta folder)."""
    s3 = cfg.get_s3_config()
    return (f"s3://{s3['bucket']}/{s3.get('prefix', 'pretest')}"
            f"/{engine}/{schema}/{table}/delta/data")


def snapshot_path(engine: str, schema: str, table: str) -> str:
    """Path of the full-source snapshot used to re-seed the target."""
    s3 = cfg.get_s3_config()
    return (f"s3://{s3['bucket']}/{s3.get('prefix', 'pretest')}"
            f"/{engine}/{schema}/{table}/data")


# ---------------------------------------------------------------------------
# Apply
# ---------------------------------------------------------------------------

def merge_sql(engine: str, schema: str, table: str) -> str:
    """The three-way MERGE that handles insert, update and delete."""
    target = target_table(engine, table)
    source = f"parquet.`{delta_path(engine, schema, table)}`"

    assignments = ",\n               ".join(
        f"t.`{c}` = s.`{c}`" for c in UPDATE_COLUMNS)

    # THEN INSERT * fails with DELTA_METADATA_MISMATCH because the delta file
    # carries the extra _cdc_op marker column, which the target schema does
    # not declare, so the insert column list must be explicit.
    target_columns = dbx.describe_columns(target)
    insert_columns = [c for c in target_columns if c != OP_COLUMN]
    bt = chr(96)
    insert_list = ", ".join(bt + c + bt for c in insert_columns)
    insert_values = ", ".join("s." + bt + c + bt for c in insert_columns)

    return (
        f"MERGE INTO {target} AS t\n"
        f"USING {source} AS s\n"
        "ON t.`id` = s.`id`\n"
        # ① 삭제: 마커가 D 인 행은 대상에서 제거한다.
        f"WHEN MATCHED AND s.`{OP_COLUMN}` = '{OP_DELETE}' THEN DELETE\n"
        # ② 갱신: 키가 같고 마커가 D 가 아니면 값을 덮어쓴다.
        "WHEN MATCHED THEN UPDATE SET "
        f"{assignments}\n"
        # ③ 신규: 대상에 없는 행은 마커가 D 가 아닐 때만 넣는다.
        f"WHEN NOT MATCHED AND s.`{OP_COLUMN}` <> '{OP_DELETE}' "
        f"THEN INSERT (" + insert_list + ") VALUES (" + insert_values + ")"
    )


def apply(engine: str, schema: str, table: str) -> dict:
    """Run the MERGE and return timing plus the statement id."""
    started = datetime.now()
    logger.info("CDC MERGE 실행 (삭제 마커 반영)")
    sql = merge_sql(engine, schema, table)
    execution = dbx.execute_sql(sql)

    return {
        "sql": sql,
        "statement_id": execution.get("statement_id"),
        "started_at": started.isoformat(timespec="seconds"),
        "duration_sec": round(
            (datetime.now() - started).total_seconds(), 2),
    }


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------

def fetch_ids(engine: str, table: str, ids: list[int],
              columns: list[str] | None = None) -> list[dict]:
    """Fetch specific rows by id."""
    target = target_table(engine, table)
    id_list = ", ".join(str(int(i)) for i in ids)
    cols = columns or ["id"]
    projection = ", ".join(f"`{c}`" for c in cols)
    result = dbx.execute_sql(
        f"SELECT {projection} FROM {target} "
        f"WHERE id IN ({id_list}) ORDER BY id")
    names = [c["name"] for c in result["columns"]]
    return [dict(zip(names, r)) for r in result["rows"]]


def reset_target(engine: str, schema: str, table: str) -> dict:
    """Restore the target to the source baseline before applying CDC.

    Why this is necessary (and not merely convenient)
    --------------------------------------------------
    Earlier ETL runs used `append`, which accumulates by design. After a
    few runs the target holds tens of thousands of rows for a 200-row
    source table, with the same ids repeated. A CDC verification against
    that polluted baseline fails for the wrong reason and proves nothing.

    CDC is only meaningful from a known starting state, so the target is
    re-seeded with INSERT OVERWRITE from the current file snapshot.
    """
    target = target_table(engine, table)
    source = f"parquet.`{snapshot_path(engine, schema, table)}`"

    before = int(dbx.fetch_value(f"SELECT COUNT(*) FROM {target}"))
    target_columns = dbx.describe_columns(target)
    probe = dbx.execute_sql(f"SELECT * FROM {source} LIMIT 0")
    file_columns = {c["name"] for c in probe["columns"]}
    shared = [c for c in target_columns if c in file_columns]
    if not shared:
        raise RuntimeError(f"no shared columns for {target}")
    column_list = ", ".join(chr(96) + c + chr(96) for c in shared)
    dbx.execute_sql(f"INSERT OVERWRITE {target} ({column_list}) "
                    f"SELECT {column_list} FROM {source}")
    after = int(dbx.fetch_value(f"SELECT COUNT(*) FROM {target}"))

    logger.info("대상 기준선 복구: %s건 → %s건 (%s.%s.%s)",
                f"{before:,}", f"{after:,}", engine, schema, table)
    return {"before": before, "after": after, "columns": shared}


def verify(engine: str, schema: str, table: str, plan: dict) -> dict:
    """Assert insert, update and delete independently."""
    target = target_table(engine, table)

    observed_total = int(dbx.fetch_value(f"SELECT COUNT(*) FROM {target}"))

    # ① UPDATE — the changed rows must carry the marker in their values.
    updated = fetch_ids(engine, table, plan["update_ids"],
                        ["id", "description", "salary"])
    marked = [r for r in updated
              if CDC_CHANGE_MARKER in str(r.get("description") or "")]

    # ② DELETE — none of the deleted ids may remain.
    survivors = fetch_ids(engine, table, plan["delete_ids"], ["id"])

    # ③ INSERT — every new id must exist.
    inserted = fetch_ids(engine, table, plan["insert_ids"], ["id"])

    checks = {
        "A_갱신반영": {
            "판정": "통과" if len(marked) == len(plan["update_ids"]) else "실패",
            "대상": len(plan["update_ids"]),
            "확인": len(marked),
            "기준": f"갱신된 행의 description 에 '{CDC_CHANGE_MARKER}' 가 들어 있어야 한다",
            "샘플": marked[:4],
        },
        "B_삭제반영": {
            "판정": "통과" if not survivors else "실패",
            "삭제대상": len(plan["delete_ids"]),
            "잔존": len(survivors),
            "기준": "삭제된 id 가 대상에 하나도 없어야 한다",
            "잔존샘플": survivors[:4],
        },
        "C_신규반영": {
            "판정": ("통과" if len(inserted) == len(plan["insert_ids"]) else "실패"),
            "신규대상": len(plan["insert_ids"]),
            "확인": len(inserted),
            "기준": "신규 id 가 모두 존재해야 한다",
            "샘플": inserted[:4],
        },
        "D_건수": {
            "원천_변경전": plan["before_count"],
            "원천_변경후": plan["after_count"],
            "대상_현재": observed_total,
            "순변화": observed_total - plan["before_count"],
            "예상순변화": plan["expected_delta"],
            "판정": ("통과"
                    if observed_total - plan["before_count"]
                    == plan["expected_delta"] else "실패"),
        },
    }

    overall = all(
        checks[k]["판정"] == "통과"
        for k in ("A_갱신반영", "B_삭제반영", "C_신규반영", "D_건수"))

    return {"전체판정": "통과" if overall else "실패", "상세": checks}


def run(engine: str, schema: str, table: str, plan_path: str,
        reset: bool = True) -> dict:
    """Load the plan, apply, verify."""
    plan = json.loads(Path(plan_path).read_text(encoding="utf-8"))
    logger.info("=" * 68)
    logger.info("CDC 적재 및 검증 — %s.%s.%s", engine, schema, table)
    logger.info("=" * 68)

    if reset:
        reset_target(engine, schema, table)

    applied = apply(engine, schema, table)
    verdict = verify(engine, schema, table, plan)

    for key, value in verdict["상세"].items():
        logger.info("  %-12s %s", key, value["판정"])

    report = {
        "engine": engine, "schema": schema, "table": table,
        "적용": applied, "검증": verdict,
    }

    out_dir = cfg.log_folder() / "cdc"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{engine}_{schema}_{table}_result.json"
    out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2,
                                   default=str), encoding="utf-8")

    logger.info("검증 판정: %s", verdict["전체판정"])
    logger.info("증적 파일: %s", out_path)
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CDC 적재 및 검증 (MERGE)")
    parser.add_argument("--engine", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--table", default="table_1")
    parser.add_argument("--plan", required=True)
    parser.add_argument("--no-reset", action="store_true",
                        help="대상 기준선을 복구하지 않고 그대로 적용한다")
    args = parser.parse_args()

    result = run(args.engine, args.schema, args.table, args.plan,
                reset=not args.no_reset)
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
