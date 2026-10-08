"""
데이터 준비 (Data preparation).

요구사항
--------
  * 원천 DB: MySQL · MongoDB · PostgreSQL
  * 엔진당 스키마 4개 → 전체 12개
  * 스키마당 테이블 5개 → 엔진당 20개, 전체 60개
  * 테이블당 5만 건
  * Iceberg 형식 파일로 로컬에 기록
  * 다 끝나면 해당 스키마/테이블 폴더에 `chk` 파일 생성

설계 — chk 파일은 **로컬** 에 만든다
-----------------------------------
기존 구현은 boto3 로 S3 에 직접 chk 를 썼다. 그러면
"chk 가 확인되면 rclone 복제 시작" 이라는 요구사항이 무의미해진다.
(이미 S3 에 있으니까 확인할 게 없다)

그래서 이렇게 한다.

    data-prep  → 로컬에 데이터 파일 + metadata + `_chk.json`  ← 이때 끝
    rclone     → copy 로 S3 로 보낸다.  (chk 도 함께 올라간다)
    Airflow    → rclone API 가 SUCCEEDED 를 확인한 뒤 S3 에
                 `_rclone_done.json` 을 직접 쓴다  ← 복제 완료 신호

이렇게 하면 `chk`(데이터 준비 완료)와 `_rclone_done`(복제 완료)이
역시로 분리돼서 인과가 명확해진다.

테이블 스키마
------------
세 엔진이 같은 컬럼을 갖되 타입만 다르다. 그래야 Auto Loader 와 ETL 이
"같은 원천 구조"를 다룬다는 것을 검증할 수 있다.

    id           정수   기본키. 세 엔진 모두 1부터 순차 증가
    name         문자열 ← 비식별화 후보
    email        문자열 ← 비식별화 후보
    phone        문자열 ← 비식별화 후보
    address      문자열
    age          정수
    salary       소수   ← 엔진마다 타입이 다르다 (DECIMAL / NUMERIC / double)
    created_at   시간   ← 엔진마다 이름/타입이 다르다
    updated_at   시간
    is_active    참거짓
    description  문자열
"""
from __future__ import annotations

import argparse
import json
import logging
import shutil
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# Windows console (cp949) cannot print every character used in the messages.
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass

from lib import config as cfg                  # noqa: E402
from lib import iceberg as iceberg_mod         # noqa: E402
from lib import sources as sources_mod         # noqa: E402

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)-5s %(message)s",
                    datefmt="%H:%M:%S", stream=sys.stdout, force=True)
logger = logging.getLogger("dataprep")

ENGINES = ["mysql", "mongodb", "postgresql"]

#: Columns shared by all three engines. The type note is documentation only;
#: the physical DDL differs per engine on purpose (see the DDL constants).
SHARED_COLUMNS: list[tuple[str, str]] = [
    ("id",          "정수"),
    ("name",        "문자열"),
    ("email",       "문자열"),
    ("phone",       "문자열"),
    ("address",     "문자열"),
    ("age",         "정수"),
    ("salary",      "소수"),
    ("created_at",  "시간"),
    ("updated_at",  "시간"),
    ("is_active",   "참거짓"),
    ("description", "문자열"),
]

COLUMN_NAMES = [c[0] for c in SHARED_COLUMNS]

#: Every generated timestamp is derived from this instant, so the same id
#: always produces the same row. Reproducibility matters more than variety
#: here because the CDC verification asserts on exact values.
BASE_TIME = datetime(2026, 1, 1, 9, 0, 0)

#: Rows are inserted in batches so memory stays flat at 50k rows per table.
INSERT_BATCH = 5000


# ---------------------------------------------------------------------------
# Sample data
# ---------------------------------------------------------------------------

def sample_row(offset: int, start: int = 1) -> dict:
    """Row at `offset`. The same number must always yield the same values."""
    number = start + offset
    created = BASE_TIME + timedelta(minutes=number % 300000)
    updated = created + timedelta(minutes=number % 120)
    return {
        "id": number,
        "name": f"user_{number}",
        "email": f"user_{number}@example.com",
        "phone": f"010-{number % 10000:04d}-{number % 100:02d}",
        "address": f"서울특별시 강남구 테헤란로 {number}길 {number % 100 + 1}",
        "age": 20 + (number % 50),
        "salary": round(30000000 + (number % 1000) * 1000, 2),
        "created_at": created,
        "updated_at": updated,
        "is_active": number % 2 == 0,
        "description": f"사용자 {number} 설명 텍스트",
    }


def build_rows(count: int, start: int = 1) -> list[dict]:
    return [sample_row(i, start) for i in range(count)]


# ---------------------------------------------------------------------------
# MySQL
# ---------------------------------------------------------------------------

MYSQL_DDL = """
CREATE TABLE `{schema}`.`{table}` (
  `id`          INT             NOT NULL,
  `name`        VARCHAR(100)    NULL,
  `email`       VARCHAR(120)    NULL,
  `phone`       VARCHAR(24)     NULL,
  `address`     VARCHAR(200)    NULL,
  `age`         INT             NULL,
  `salary`      DECIMAL(14,2)   NULL,
  `created_at`  TIMESTAMP       NULL,
  `updated_at`  TIMESTAMP       NULL,
  `is_active`   TINYINT(1)      NULL,
  `description` VARCHAR(300)    NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""


def mysql_prepare(schemas: list[str], tables: list[str]) -> None:
    """Create the schemas and tables, dropping any previous version first.

    Measured: `CREATE TABLE IF NOT EXISTS` alone is not enough. A rerun after a
    column change would silently keep the old structure, so the table is
    dropped explicitly. The primary key is a plain INT (not AUTO_INCREMENT)
    because ids are assigned by the generator, which is what makes rows
    reproducible across reruns.
    """
    import pymysql

    conn_cfg = cfg.mysql_connection()
    connection = pymysql.connect(
        host=conn_cfg["host"], port=conn_cfg["port"],
        user=conn_cfg.get("user", "root"), password=conn_cfg.get("password", ""),
        charset="utf8mb4", autocommit=True,
        cursorclass=pymysql.cursors.DictCursor)
    try:
        with connection.cursor() as cur:
            for schema in schemas:
                cur.execute(f"CREATE DATABASE IF NOT EXISTS `{schema}` "
                            "DEFAULT CHARACTER SET utf8mb4")
            for schema in schemas:
                for table in tables:
                    cur.execute(f"DROP TABLE IF EXISTS `{schema}`.`{table}`")
                    cur.execute(MYSQL_DDL.format(schema=schema, table=table))
        logger.info("MySQL 준비 완료 · 스키마 %d개 · 테이블 %d개",
                    len(schemas), len(schemas) * len(tables))
    finally:
        connection.close()


def mysql_load(schema: str, table: str, rows: list[dict]) -> None:
    """Insert in batches. One statement for 50k rows would exhaust memory."""
    import pymysql

    placeholders = ",".join(["%s"] * len(COLUMN_NAMES))
    column_sql = ",".join(f"`{c}`" for c in COLUMN_NAMES)
    statement = (f"INSERT INTO `{schema}`.`{table}` ({column_sql}) "
                 f"VALUES ({placeholders})")

    conn_cfg = cfg.mysql_connection()
    connection = pymysql.connect(
        host=conn_cfg["host"], port=conn_cfg["port"],
        user=conn_cfg.get("user", "root"), password=conn_cfg.get("password", ""),
        database=schema, charset="utf8mb4", autocommit=False,
        cursorclass=pymysql.cursors.DictCursor)
    try:
        with connection.cursor() as cur:
            for start in range(0, len(rows), INSERT_BATCH):
                cur.executemany(statement, [
                    tuple(row.get(c) for c in COLUMN_NAMES)
                    for row in rows[start:start + INSERT_BATCH]])
        connection.commit()
    finally:
        connection.close()


# ---------------------------------------------------------------------------
# PostgreSQL
# ---------------------------------------------------------------------------

POSTGRES_DDL = """
CREATE TABLE IF NOT EXISTS "{schema}"."{table}" (
  id           SERIAL         PRIMARY KEY,
  name         VARCHAR(100),
  email        VARCHAR(120),
  phone        VARCHAR(24),
  address      VARCHAR(200),
  age          INTEGER,
  salary       NUMERIC(14,2),
  created_at   TIMESTAMP,
  updated_at   TIMESTAMP,
  is_active    BOOLEAN,
  description  VARCHAR(300)
)
"""


def postgres_prepare(schemas: list[str], tables: list[str]) -> None:
    """Measured: PostgreSQL reserves the `pg_` prefix for system schemas, so
    the schema names are `postgres_schema_1..4`. Not a naming preference —
    `CREATE SCHEMA pg_schema_1` fails with `unacceptable schema name`.
    """
    connection = sources_mod._postgres_connection()
    try:
        cur = connection.cursor()
        for schema in schemas:
            cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        for schema in schemas:
            for table in tables:
                # Same reason as MySQL: drop first so reruns are clean.
                cur.execute(f'DROP TABLE IF EXISTS "{schema}"."{table}" CASCADE')
                cur.execute(POSTGRES_DDL.format(schema=schema, table=table))
        connection.commit()
        logger.info("PostgreSQL 준비 완료 · 스키마 %d개 · 테이블 %d개",
                    len(schemas), len(schemas) * len(tables))
    finally:
        connection.close()


def postgres_load(schema: str, table: str, rows: list[dict]) -> None:
    """`execute_values` is used because row-by-row INSERT of 50k rows is slow."""
    from psycopg2.extras import execute_values

    quote = chr(34)
    column_sql = ",".join(quote + c + quote for c in COLUMN_NAMES)
    statement = (f'INSERT INTO "{schema}"."{table}" '
                 f'({column_sql}) VALUES %s')

    connection = sources_mod._postgres_connection()
    try:
        cur = connection.cursor()
        for start in range(0, len(rows), INSERT_BATCH):
            execute_values(cur, statement,
                           [tuple(row.get(c) for c in COLUMN_NAMES)
                            for row in rows[start:start + INSERT_BATCH]],
                           page_size=1000)
        connection.commit()
    finally:
        connection.close()


# ---------------------------------------------------------------------------
# MongoDB
# ---------------------------------------------------------------------------

def mongo_prepare(schemas: list[str], tables: list[str]) -> None:
    client = sources_mod._mongo_connection()
    try:
        for schema in schemas:
            db = client[schema]
            for table in tables:
                db[table].delete_many({})
        logger.info("MongoDB 준비 완료 · 스키마 %d개 · 테이블 %d개",
                    len(schemas), len(schemas) * len(tables))
    finally:
        client.close()


def mongo_load(schema: str, table: str, rows: list[dict]) -> None:
    """Insert into MongoDB.

    `_id` is left to the server and a separate integer `id` carries the key.
    Two reasons, both measured:
      1. ObjectId cannot be converted to parquet —
         `ArrowInvalid: Could not convert ObjectId`.
      2. The ETL `merge` needs a primary key. With the same integer `id`
         across all three engines, Auto Loader and MERGE are written once.
    """
    client = sources_mod._mongo_connection()
    try:
        collection = client[schema][table]
        for start in range(0, len(rows), INSERT_BATCH):
            collection.insert_many(rows[start:start + INSERT_BATCH],
                                   ordered=False)
    finally:
        client.close()


def mongo_rows_for_file(schema: str, table: str, limit: int) -> list[dict]:
    """Read back from MongoDB with `_id` excluded.

    Reading back rather than reusing the generated rows is deliberate: it is
    the same path the loader will take, so a type the driver returns
    differently (Decimal, int-for-bool) is discovered here and not at load
    time.
    """
    client = sources_mod._mongo_connection()
    try:
        collection = client[schema][table]
        projection = {c: 1 for c in COLUMN_NAMES}
        projection["_id"] = 0
        return list(collection.find({}, projection).sort("id", 1).limit(limit))
    finally:
        client.close()


# ---------------------------------------------------------------------------
# chk file (local)
# ---------------------------------------------------------------------------

def write_chk(folder: Path, engine: str, schema: str, table: str, *,
              row_count: int, file_count: int, total_size: int,
              columns: list[str], elapsed_sec: float) -> Path:
    """The "data preparation finished" signal, written **locally**.

    rclone moves it to S3 along with the data. It is deliberately not written
    straight to S3: that would make the requirement "start replicating once
    chk appears" vacuous, because it would already be there.
    """
    folder.mkdir(parents=True, exist_ok=True)
    name = cfg.get_s3_config().get("chk_file", "_chk.json")
    path = folder / name
    document = {
        "단계": "데이터 준비 완료",
        "엔진": engine, "스키마": schema, "표": table,
        "건수": row_count, "파일수": file_count, "총크기": total_size,
        "컬럼목록": columns,
        "완료시각": datetime.now().astimezone().isoformat(timespec="seconds"),
        "소요초": round(elapsed_sec, 1),
        "상태": "completed",
        "안내": "이 파일이 S3 에 보이면 로컬 준비가 끝났다는 뜻입니다. "
                "복제 완료는 Airflow 가 별도로 쓰는 _rclone_done.json 을 봅니다.",
    }
    path.write_text(json.dumps(document, ensure_ascii=False, indent=2),
                    encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def prepare(rows_per_table: int | None = None,
            wipe_local: bool = True,
            write_files: bool = True) -> dict:
    """Run the whole data preparation.

    Args:
        rows_per_table: rows per table. Defaults to the configured value
            (50,000). Lower it only for a quick smoke run.
        wipe_local: clear the local file folder first, so a rerun does not
            leave stale files from a previous scale.
        write_files: also write the Iceberg-format files. False loads the
            databases only.
    """
    started = time.time()
    root = cfg.local_data_root()

    if wipe_local and root.exists():
        logger.info("기존 로컬 파일 삭제: %s", root)
        shutil.rmtree(root)

    result: dict[str, Any] = {
        "시작시각": datetime.now().astimezone().isoformat(timespec="seconds"),
        "엔진별": {}, "총테이블": 0, "총건수": 0, "총파일": 0, "총크기": 0,
    }

    for engine in ENGINES:
        source_cfg = cfg.get_section("sources", {})[engine]
        schemas = list(source_cfg["schemas"])
        table_count = int(source_cfg.get("tables_per_schema", 5))
        tables = [f"table_{i}" for i in range(1, table_count + 1)]
        count = int(rows_per_table or source_cfg.get("rows_per_table", 50000))

        logger.info("=" * 70)
        logger.info("[%s] 스키마 %d개 × 테이블 %d개 × %s건",
                    engine, len(schemas), table_count, f"{count:,}")
        logger.info("=" * 70)

        engine_started = time.time()
        if engine == "mysql":
            mysql_prepare(schemas, tables)
        elif engine == "mongodb":
            mongo_prepare(schemas, tables)
        else:
            postgres_prepare(schemas, tables)

        engine_result = {"스키마": {}, "테이블수": 0, "총건수": 0,
                         "총파일": 0, "총크기": 0}

        for schema in schemas:
            schema_folder = root / engine / schema
            schema_folder.mkdir(parents=True, exist_ok=True)
            schema_result = {"테이블": {}, "총건수": 0}

            for table in tables:
                table_started = time.time()
                rows = build_rows(count)

                if engine == "mysql":
                    mysql_load(schema, table, rows)
                elif engine == "mongodb":
                    mongo_load(schema, table, rows)
                else:
                    postgres_load(schema, table, rows)

                # Count the rows that actually landed rather than trusting
                # the number of rows we meant to insert.
                actual = sources_mod.count_rows(engine, schema, table)

                # Rows to serialise. MongoDB returns `_id` (ObjectId), which
                # pyarrow cannot convert, so it is projected away.
                file_rows = rows
                if engine == "mongodb":
                    file_rows = mongo_rows_for_file(schema, table, actual)

                file_count = total_size = 0
                columns = list(COLUMN_NAMES)
                if write_files:
                    written = iceberg_mod.write_table(
                        schema_folder / table,
                        engine=engine, schema=schema, table=table,
                        rows=file_rows, version=1, rows_per_file=20000)
                    file_count = written["file_count"]
                    total_size = written["total_size"]
                    columns = written["columns"]

                chk = write_chk(
                    schema_folder / table, engine, schema, table,
                    row_count=actual, file_count=file_count,
                    total_size=total_size, columns=columns,
                    elapsed_sec=time.time() - table_started)

                elapsed = time.time() - table_started
                logger.info("  %s.%s : %s건 · 파일 %d개 · %.1fs",
                            schema, table, f"{actual:,}", file_count, elapsed)

                schema_result["테이블"][table] = {
                    "건수": actual, "파일수": file_count,
                    "총크기": total_size, "chk": str(chk),
                    "소요초": round(elapsed, 1),
                }
                schema_result["총건수"] += actual
                engine_result["테이블수"] += 1
                engine_result["총건수"] += actual
                engine_result["총파일"] += file_count
                engine_result["총크기"] += total_size
                result["총테이블"] += 1
                result["총건수"] += actual
                result["총파일"] += file_count
                result["총크기"] += total_size

            engine_result["스키마"][schema] = schema_result
            logger.info("  → %s 소계 %s건", schema, f"{schema_result['총건수']:,}")

        engine_result["소요초"] = round(time.time() - engine_started, 1)
        result["엔진별"][engine] = engine_result
        logger.info("[%s] 소계 %s건 · %.0fs", engine,
                    f"{engine_result['총건수']:,}", engine_result["소요초"])

    result["종료시각"] = datetime.now().astimezone().isoformat(timespec="seconds")
    result["총소요초"] = round(time.time() - started, 1)

    out_dir = cfg.log_folder()
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"01_dataprep_{datetime.now():%Y%m%d_%H%M%S}.json"
    out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2),
                        encoding="utf-8")
    result["증적"] = str(out_path)

    logger.info("=" * 70)
    logger.info("데이터 준비 완료 · 테이블 %d개 · %s건 · 파일 %d개 · %.0fs",
                result["총테이블"], f"{result['총건수']:,}",
                result["총파일"], result["총소요초"])
    logger.info("증적: %s", out_path)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="데이터 준비")
    parser.add_argument("--건수", type=int, default=None,
                        help="테이블당 행 수 (기본 설정값 50000)")
    parser.add_argument("--파일유지", action="store_true",
                        help="기존 로컬 파일을 지우지 않는다")
    parser.add_argument("--DB만", action="store_true",
                        help="원천 DB 에만 넣고 Iceberg 파일은 쓰지 않는다")
    args = parser.parse_args()

    prepare(rows_per_table=args.건수,
            wipe_local=not args.파일유지,
            write_files=not args.DB만)