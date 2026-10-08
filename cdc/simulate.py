"""
CDC delta writer — produces a real change stream, tombstones included.

Why the first attempt failed (and what this fixes)
--------------------------------------------------
The initial version wrote only the *inserted* rows into the delta file and fed
it to `MERGE`. Verification then showed:

    A_갱신반영 : 0 / 10   — updates never reached the target
    B_삭제반영 : 2560 잔존 — deleted rows survived

That is not a tuning problem, it is a design gap. A delta file that contains
only new rows cannot express "this row was deleted", so `MERGE` has nothing to
act on. Row-count arithmetic would even look plausible, which is exactly why
the verification checks values rather than counts.

The fix: every changed row appears in the delta with an operation marker.

    _cdc_op = 'I'  insert only
    _cdc_op = 'U'  update (row exists upstream with new values)
    _cdc_op = 'D'  delete (tombstone — only the key is meaningful)

The loader then issues a single `MERGE` that handles all three:

    WHEN MATCHED AND s._cdc_op = 'D' THEN DELETE
    WHEN MATCHED THEN UPDATE SET ...
    WHEN NOT MATCHED AND s._cdc_op <> 'D' THEN INSERT ...

This is the standard CDC contract (the same shape Debezium/Kafka use with
`op`/`op_type` columns), expressed for a Delta/Iceberg target.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timedelta
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
from lib import iceberg as iceberg_mod         # noqa: E402
from lib import sources as sources_mod         # noqa: E402

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)-5s %(message)s",
                    datefmt="%H:%M:%S", stream=sys.stdout, force=True)
logger = logging.getLogger("cdc-stream")

#: Operation marker column added to every delta row.
OP_COLUMN = "_cdc_op"

OP_INSERT = "I"
OP_UPDATE = "U"
OP_DELETE = "D"

OP_LABEL = {
    OP_INSERT: "신규",
    OP_UPDATE: "갱신",
    OP_DELETE: "삭제(마커)",
}

#: Column set shared with the target table.
#: Marker written into description on an UPDATE. Shared by all three
#: engines so the verifier can assert on one string.
MARKER = "CDC [changed]"

PAYLOAD_COLUMNS = ["id", "name", "email", "phone", "address", "age",
                   "salary", "created_at", "updated_at",
                   "is_active", "description"]


# ---------------------------------------------------------------------------
# Source mutation
# ---------------------------------------------------------------------------

def mysql_apply(schema: str, table: str, *,
                update_ids: list[int], delete_ids: list[int],
                insert_rows: list[dict]) -> dict:
    """Apply UPDATE / DELETE / INSERT to a MySQL table."""
    import pymysql

    conn_cfg = cfg.mysql_connection()
    conn = pymysql.connect(
        host=conn_cfg["host"], port=conn_cfg["port"],
        user=conn_cfg.get("user", "root"), password=conn_cfg.get("password", ""),
        database=schema, charset="utf8mb4", autocommit=True,
        # `cur.fetchone()["cnt"]` 로 접근하므로 DictCursor 가 필요하다.
        cursorclass=pymysql.cursors.DictCursor)
    try:
        with conn.cursor() as cur:
            if update_ids:
                # ⚠ 실측 함정: pymysql 은 쿼리 문자열의 `%` 를 서식 문자로 본다.
                #   바인딩하는 쿼리에 `%` 리터럴이 섞이면
                #   `not enough arguments for format string` 로 죽는다.
                #   id 는 정수임을 알고 있으므로 값으로 직접 넣는다.
                ids = ",".join(str(int(i)) for i in update_ids)
                cur.execute(
                    f"UPDATE `{table}` "
                    "SET `salary` = `salary` + 7777, "
                                        f"    `description` = CONCAT(`description`, '{MARKER}'), "
                    "    `updated_at` = NOW() "
                    f"WHERE `id` IN ({ids})")

            if delete_ids:
                ids = ",".join(str(int(i)) for i in delete_ids)
                cur.execute(f"DELETE FROM `{table}` WHERE `id` IN ({ids})")

            if insert_rows:
                col_sql = ",".join(f"`{c}`" for c in PAYLOAD_COLUMNS)
                ids = ",".join(str(int(r["id"])) for r in insert_rows)
                # 반복 실행에 대비한 멱등 처리: 지난번에 넣은 행을 먼저 지운다.
                cur.execute(f"DELETE FROM `{table}` WHERE `id` IN ({ids})")

                def literal(v):
                    if v is None:
                        return "NULL"
                    if isinstance(v, bool):
                        return "1" if v else "0"
                    if isinstance(v, (int, float)):
                        return str(v)
                    if isinstance(v, datetime):
                        return f"'{v:%Y-%m-%d %H:%M:%S}'"
                    return "'" + str(v).replace("\\", "\\\\").replace("'", "''") + "'"

                values = [[r.get(c) for c in PAYLOAD_COLUMNS] for r in insert_rows]
                rows_sql = ",".join(
                    "(" + ",".join(literal(v) for v in row) + ")" for row in values)
                cur.execute(f"INSERT INTO `{table}` ({col_sql}) VALUES {rows_sql}")

            cur.execute(f"SELECT COUNT(*) AS cnt FROM `{table}`")
            after = int(cur.fetchone()["cnt"])
    finally:
        conn.close()

    return {"after_count": after, "updated": len(update_ids),
            "deleted": len(delete_ids), "inserted": len(insert_rows)}


def postgresql_apply(schema: str, table: str, **kwargs) -> dict:
    """Apply the same change set to PostgreSQL."""
    import psycopg2

    conn_cfg = cfg.postgres_connection()
    conn = psycopg2.connect(
        host=conn_cfg["host"], port=conn_cfg["port"],
        user=conn_cfg.get("user", "postgres"), password=conn_cfg.get("password", ""),
        dbname="postgres", connect_timeout=15)
    conn.autocommit = True
    try:
        cur = conn.cursor()
        update_ids = kwargs["update_ids"]
        delete_ids = kwargs["delete_ids"]
        insert_rows = kwargs["insert_rows"]

        if update_ids:
            ids = ",".join(str(int(i)) for i in update_ids)
            cur.execute(
                f'UPDATE "{schema}"."{table}" '
                "SET \"salary\" = \"salary\" + 7777, "
                                f'    "description" = "description" || \'{MARKER}\', '
                "    \"updated_at\" = NOW() "
                f'WHERE "id" IN ({ids})')
        if delete_ids:
            ids = ",".join(str(int(i)) for i in delete_ids)
            cur.execute(f'DELETE FROM "{schema}"."{table}" WHERE "id" IN ({ids})')
        if insert_rows:
            from psycopg2.extras import execute_values
            col_sql = ",".join(f'"{c}"' for c in PAYLOAD_COLUMNS)
            ids = ",".join(str(int(r["id"])) for r in insert_rows)
            cur.execute(f'DELETE FROM "{schema}"."{table}" WHERE "id" IN ({ids})')
            execute_values(
                cur,
                f'INSERT INTO "{schema}"."{table}" ({col_sql}) VALUES %s',
                [tuple(r.get(c) for c in PAYLOAD_COLUMNS) for r in insert_rows],
                template="(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                page_size=500)

        cur.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
        after = int(cur.fetchone()[0])
    finally:
        conn.close()

    return {"after_count": after, "updated": len(update_ids),
            "deleted": len(delete_ids), "inserted": len(insert_rows)}


def mongodb_apply(schema: str, table: str, **kwargs) -> dict:
    """Apply the same change set to MongoDB."""
    from pymongo import MongoClient

    conn_cfg = cfg.mongodb_connection()
    client = MongoClient(
        host=conn_cfg["host"], port=conn_cfg["port"],
        username=conn_cfg.get("user", "admin"),
        password=conn_cfg.get("password", ""),
        authSource=conn_cfg.get("auth_source", "admin"),
        serverSelectionTimeoutMS=15000)
    try:
        collection = client[schema][table]
        update_ids = kwargs["update_ids"]
        delete_ids = kwargs["delete_ids"]
        insert_rows = kwargs["insert_rows"]

        if update_ids:
            collection.update_many(
                {"id": {"$in": update_ids}},
                {"$inc": {"salary": 7777},
                 "$set": {
                     "updated_at": datetime.now(),
                     # Measured: $push appends and turns the field
                     # into an ARRAY, which breaks the parquet write
                     # (WriteError: the field must be a scalar). $set
                     # writes the same marker text the SQL engines use, so
                     # all three sources stay shape-compatible.
                     "description": MARKER}})
        if delete_ids:
            collection.delete_many({"id": {"$in": delete_ids}})
        if insert_rows:
            ids = [r["id"] for r in insert_rows]
            collection.delete_many({"id": {"$in": ids}})
            collection.insert_many(insert_rows, ordered=False)
        after = collection.count_documents({})
    finally:
        client.close()

    return {"after_count": after, "updated": len(update_ids),
            "deleted": len(delete_ids), "inserted": len(insert_rows)}


def apply_to_source(engine: str, schema: str, table: str, **kwargs) -> dict:
    if engine == "mysql":
        return mysql_apply(schema, table, **kwargs)
    if engine == "postgresql":
        return postgresql_apply(schema, table, **kwargs)
    if engine == "mongodb":
        return mongodb_apply(schema, table, **kwargs)
    raise ValueError(f"unknown engine: {engine}")


# ---------------------------------------------------------------------------
# Delta stream with tombstones
# ---------------------------------------------------------------------------

def build_delta(engine: str, schema: str, table: str, plan: dict) -> list[dict]:
    """Assemble the delta rows: inserts + updates + delete tombstones.

    Update rows carry the *post-change* values read back from the source, so
    the MERGE writes exactly what upstream now holds.
    """
    delta: list[dict] = []

    # Inserts
    for row in plan["insert_rows"]:
        payload = {c: row.get(c) for c in PAYLOAD_COLUMNS}
        payload[OP_COLUMN] = OP_INSERT
        delta.append(payload)

    # Updates — read the current (changed) source values.
    if plan["update_ids"]:
        ids = plan["update_ids"]
        current = sources_mod.read_rows(engine, schema, table, PAYLOAD_COLUMNS)
        for row in current:
            if row.get("id") in ids:
                payload = {c: row.get(c) for c in PAYLOAD_COLUMNS}
                payload[OP_COLUMN] = OP_UPDATE
                delta.append(payload)

    # Delete tombstones — only the key carries meaning.
    for row_id in plan["delete_ids"]:
        payload = {c: None for c in PAYLOAD_COLUMNS}
        payload["id"] = row_id
        payload[OP_COLUMN] = OP_DELETE
        delta.append(payload)

    logger.info("델타 구성: 신규 %d / 갱신 %d / 삭제마커 %d = %d행",
                len(plan["insert_rows"]), len(plan["update_ids"]),
                len(plan["delete_ids"]), len(delta))
    return delta


def write_delta_snapshot(engine: str, schema: str, table: str,
                         delta: list[dict], version: int) -> dict:
    """Write the delta as an Iceberg snapshot that records the operation mix."""
    # Delta and the full snapshot live in SEPARATE folders. Seeding the
    # target from the delta would make the baseline itself the change.
    folder = cfg.local_data_root() / engine / schema / table / "delta"
    result = iceberg_mod.write_table(
        folder, engine=engine, schema=schema, table=table,
        rows=delta, version=version, rows_per_file=20000)

    meta_path = Path(result["metadata_path"])
    document = json.loads(meta_path.read_text(encoding="utf-8"))
    snapshot = document["snapshots"][0]

    counts: dict[str, int] = {}
    for row in delta:
        op = row.get(OP_COLUMN, "?")
        counts[op] = counts.get(op, 0) + 1

    snapshot["summary"]["operation"] = "cdc"
    snapshot["summary"]["CDC_신규"] = str(counts.get(OP_INSERT, 0))
    snapshot["summary"]["CDC_갱신"] = str(counts.get(OP_UPDATE, 0))
    snapshot["summary"]["CDC_삭제"] = str(counts.get(OP_DELETE, 0))
    snapshot["summary"]["순건수변화"] = str(
        counts.get(OP_INSERT, 0) - counts.get(OP_DELETE, 0))
    meta_path.write_text(json.dumps(document, ensure_ascii=False, indent=2),
                         encoding="utf-8")

    logger.info("CDC 스냅샷 기록: %s.%s.%s v%d %s",
                engine, schema, table, version, counts)
    return result


# ---------------------------------------------------------------------------
# Scenario
# ---------------------------------------------------------------------------

def simulate(engine: str, schema: str, table: str, *,
             update_count: int = 10, delete_count: int = 10,
             insert_count: int = 10, version: int = 2) -> dict:
    """Full CDC simulation: mutate the source, then emit the delta stream."""
    logger.info("=" * 68)
    logger.info("CDC 시뮬레이션 — %s.%s.%s", engine, schema, table)
    logger.info("=" * 68)

    base = datetime(2026, 6, 1, 9, 0, 0)
    update_ids = list(range(6, 6 + update_count))
    delete_ids = list(range(106, 106 + delete_count))
    insert_ids = list(range(206, 206 + insert_count))

    insert_rows = []
    for i, row_id in enumerate(insert_ids):
        insert_rows.append({
            "id": row_id,
            "name": f"cdc_new_{row_id}",
            "email": f"cdc_new_{row_id}@example.com",
            "phone": f"010-9{row_id:03d}-99",
            "address": f"서울특별시 종로구 CDC로 {row_id}",
            "age": 25 + (row_id % 40),
            "salary": 41000000.0 + row_id * 1000,
            "created_at": base + timedelta(minutes=i),
            "updated_at": base + timedelta(minutes=i),
            "is_active": True,
            "description": f"CDC 신규 삽입 행 {row_id}",
        })

    before = sources_mod.count_rows(engine, schema, table)
    logger.info("변경 전 원천 건수: %s", f"{before:,}")

    applied = apply_to_source(
        engine, schema, table,
        update_ids=update_ids, delete_ids=delete_ids, insert_rows=insert_rows)

    logger.info("변경 후 원천 건수: %s (기대 %s)",
                f"{applied['after_count']:,}",
                f"{before + insert_count - delete_count:,}")

    plan = {
        "engine": engine, "schema": schema, "table": table,
        "update_ids": update_ids, "delete_ids": delete_ids,
        "insert_ids": insert_ids, "insert_rows": insert_rows,
        "before_count": before, "after_count": applied["after_count"],
        "expected_delta": insert_count - delete_count,
        "op_column": OP_COLUMN,
    }

    delta = build_delta(engine, schema, table, plan)
    plan["delta_rows"] = delta
    snapshot = write_delta_snapshot(engine, schema, table, delta, version)
    plan["snapshot"] = snapshot

    out_dir = cfg.log_folder() / "cdc"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{engine}_{schema}_{table}_v{version}.json"
    # delta_rows 는 datetime 을 포함하므로 문자열화한다.
    out_path.write_text(json.dumps(plan, ensure_ascii=False, indent=2,
                                   default=str), encoding="utf-8")
    plan["plan_file"] = str(out_path)

    logger.info("계획 파일 저장: %s", out_path)
    return plan


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CDC 시뮬레이션 (마커 포함)")
    parser.add_argument("--engine", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--table", default="table_1")
    parser.add_argument("--갱신", type=int, default=10)
    parser.add_argument("--삭제", type=int, default=10)
    parser.add_argument("--신규", type=int, default=10)
    parser.add_argument("--버전", type=int, default=2)
    args = parser.parse_args()

    result = simulate(args.engine, args.schema, args.table,
                      update_count=args.갱신, delete_count=args.삭제,
                      insert_count=args.신규, version=args.버전)
    print(json.dumps({k: v for k, v in result.items()
                      if k not in ("insert_rows", "delta_rows", "snapshot")},
                     ensure_ascii=False, indent=2, default=str))
