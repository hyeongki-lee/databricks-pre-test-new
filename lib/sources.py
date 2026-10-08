"""
Source database access layer (MySQL / MongoDB / PostgreSQL).

Why this module exists
----------------------
ETL has to read the source schema **twice**:

  1. Once when the dashboard registers a table (to capture the baseline).
  2. Again at **execution time**, to detect columns that were added or
     dropped since then (requirement: "reconnect to the source at the moment
     of the run and check for column deletions/additions").

Having a single façade here keeps that logic in one place and guarantees all
three engines are treated identically.

Query-quota discipline
----------------------
Databricks Free Edition has a daily query quota, but the *source* databases
do not. Still, this module batches on purpose because a 60-table run would
otherwise issue 60 round trips:

    per-table `SHOW COLUMNS`   -> 60 queries
    per-schema `information_schema` -> 12 queries

Engine-specific quirks (all verified by measurement)
----------------------------------------------------
* **PostgreSQL** — the database *is* the catalog, so `information_schema`
  queries must filter on `table_schema`, not `table_name`.
* **PostgreSQL** — schema names may not start with `pg_`. Postgres reserves
  that prefix for system schemas:
      CREATE SCHEMA pg_schema_1
      -> ERROR: unacceptable schema name "pg_schema_1"
  We therefore use `postgres_schema_1` … `postgres_schema_4`.
* **MongoDB** — a schema is a *database*, a table is a *collection*.
* **MongoDB** — reading only the first document misses sparse fields.
  We sample N documents and take the **union** of all keys, which is the
  only way column-addition detection can work.
* **MongoDB** — `list_database_names()` can return an empty string.
* **MongoDB** — `_id` (ObjectId) cannot be converted by pyarrow, so it is
  excluded everywhere.
"""
from __future__ import annotations

import logging
from typing import Any

try:
    from . import config as cfg
except ImportError:                            # standalone script execution
    import config as cfg                    # type: ignore

logger = logging.getLogger(__name__)

ENGINES = ["mysql", "mongodb", "postgresql"]

#: MongoDB field discovery sample size.
#: Large enough to catch sparse fields, small enough to stay cheap.
MONGO_SAMPLE_SIZE = 500


# ---------------------------------------------------------------------------
# MySQL
# ---------------------------------------------------------------------------

def _mysql_connection(schema: str | None = None):
    """Open a PyMySQL connection (autocommit on, utf8mb4)."""
    import pymysql

    conn_cfg = cfg.mysql_connection()
    return pymysql.connect(
        host=conn_cfg["host"],
        port=conn_cfg["port"],
        user=conn_cfg.get("user", "root"),
        password=conn_cfg.get("password", ""),
        database=schema,
        charset="utf8mb4",
        autocommit=True,
        connect_timeout=15,
        cursorclass=pymysql.cursors.DictCursor,
    )


def list_mysql_schemas() -> list[str]:
    """Test schemas only (`mysql_schema_*`)."""
    conn = _mysql_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SHOW DATABASES")
            rows = [list(r.values())[0] for r in cur.fetchall()]
        return [r for r in rows if r.startswith("mysql_schema_")]
    finally:
        conn.close()


def get_mysql_schema_columns(schema: str) -> dict[str, list[dict]]:
    """Columns of *every* table in one schema, using a single query.

    Returns:
        {table_name: [{"name","type","nullable","key"}, ...]}
    """
    conn = _mysql_connection()
    try:
        with conn.cursor() as cur:
            # Alias every column explicitly. The DictCursor key is case
            # sensitive and MySQL 8 returns these headers in upper case,
            # which made `row["table_name"]` raise KeyError.
            cur.execute(
                "SELECT table_name  AS tbl, "
                "       column_name AS col, "
                "       data_type   AS dtype, "
                "       is_nullable AS nullable_flag, "
                "       column_key  AS col_key "
                "FROM information_schema.columns "
                "WHERE table_schema = %s "
                "ORDER BY table_name, ordinal_position",
                (schema,),
            )
            result: dict[str, list[dict]] = {}
            for row in cur.fetchall():
                result.setdefault(row["tbl"], []).append({
                    "name": row["col"],
                    "type": row["dtype"],
                    "nullable": row["nullable_flag"] == "YES",
                    "key": row["col_key"] or "",
                })
            return result
    finally:
        conn.close()


def mysql_null_literal(data_type: str = "") -> str:
    """Empty-value literal for MySQL. Used for dropped-column handling."""
    return "NULL"


# ---------------------------------------------------------------------------
# PostgreSQL
# ---------------------------------------------------------------------------

def _postgres_connection(schema: str | None = None):
    """Open a psycopg2 connection. Note: the DB itself is the catalog."""
    import psycopg2

    conn_cfg = cfg.postgres_connection()
    conn = psycopg2.connect(
        host=conn_cfg["host"],
        port=conn_cfg["port"],
        user=conn_cfg.get("user", "postgres"),
        password=conn_cfg.get("password", ""),
        dbname="postgres",
        connect_timeout=15,
    )
    conn.autocommit = True
    return conn


def list_postgres_schemas() -> list[str]:
    """Test schemas only (`postgres_schema_*`).

    `pg_` is reserved by Postgres, hence the different prefix.
    """
    conn = _postgres_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT schema_name FROM information_schema.schemata "
            "WHERE schema_name LIKE 'postgres\\_schema\\_%' ORDER BY schema_name"
        )
        return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


def get_postgres_schema_columns(schema: str) -> dict[str, list[dict]]:
    """Columns of every table in one schema, using a single query.

    `table_schema` is the discriminator here because in PostgreSQL the
    database is the catalog — `table_name` alone would be ambiguous.
    """
    conn = _postgres_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT table_name, column_name, data_type, is_nullable "
            "FROM information_schema.columns "
            "WHERE table_schema = %s "
            "ORDER BY table_name, ordinal_position",
            (schema,),
        )
        result: dict[str, list[dict]] = {}
        for table, column, dtype, nullable in cur.fetchall():
            result.setdefault(table, []).append({
                "name": column,
                "type": dtype,
                "nullable": nullable == "YES",
                "key": "",
            })
        return result
    finally:
        conn.close()


def postgres_null_literal(data_type: str = "") -> str:
    """Empty-value literal for PostgreSQL."""
    return "NULL"


# ---------------------------------------------------------------------------
# MongoDB
# ---------------------------------------------------------------------------

def _mongo_connection():
    """Open a pymongo client (SCRAM auth against `authSource`)."""
    from pymongo import MongoClient

    conn_cfg = cfg.mongodb_connection()
    return MongoClient(
        host=conn_cfg["host"],
        port=conn_cfg["port"],
        username=conn_cfg.get("user", "admin"),
        password=conn_cfg.get("password", ""),
        authSource=conn_cfg.get("auth_source", "admin"),
        serverSelectionTimeoutMS=15000,
        connect=True,
    )


def list_mongo_schemas() -> list[str]:
    """Test databases only (`mongo_schema_*`).

    `list_database_names()` can include an empty string, so filter it out.
    """
    client = _mongo_connection()
    try:
        names = client.list_database_names()
        return sorted(
            n for n in names
            if n.startswith("mongo_schema_")
            and n not in ("", "admin", "local", "config")
        )
    finally:
        client.close()


def list_mongo_collections(schema: str) -> list[str]:
    """Collections (= tables) inside one database."""
    client = _mongo_connection()
    try:
        return sorted(client[schema].list_collection_names())
    finally:
        client.close()


def get_mongo_fields(schema: str, table: str,
                     sample_size: int = MONGO_SAMPLE_SIZE) -> list[dict]:
    """Fields that actually appear in a collection.

    Reading a single document is not enough — sparse documents hide fields.
    So we `$sample` N documents, explode each into key/value pairs, group by
    (key, type), and take the **union**. That is what makes column-addition
    detection reliable on a schema-less store.
    """
    client = _mongo_connection()
    try:
        collection = client[schema][table]
        pipeline = [
            {"$sample": {"size": sample_size}},
            {"$project": {"kv": {"$objectToArray": "$$ROOT"}}},
            {"$unwind": "$kv"},
            {"$group": {"_id": {"k": "$kv.k", "t": {"$type": "$kv.v"}},
                        "n": {"$sum": 1}}},
            {"$sort": {"_id.k": 1}},
        ]
        seen: dict[str, dict] = {}
        for row in collection.aggregate(pipeline, allowDiskUse=True):
            name = row["_id"]["k"]
            count = row["n"]
            # A field may appear with several types; keep the most frequent.
            if name not in seen or count > seen[name]["count"]:
                seen[name] = {"name": name, "type": row["_id"]["t"], "count": count}

        return sorted(
            ({"name": v["name"], "type": v["type"], "nullable": True, "key": ""}
             for v in seen.values() if v["name"] != "_id"),
            key=lambda d: d["name"],
        )
    finally:
        client.close()


def get_mongo_schema_fields(schema: str) -> dict[str, list[dict]]:
    """Fields of every collection in one database."""
    result: dict[str, list[dict]] = {}
    for table in list_mongo_collections(schema):
        result[table] = get_mongo_fields(schema, table)
    return result


def mongo_null_literal(data_type: str = "") -> str:
    """Empty-value literal for a MongoDB aggregation pipeline."""
    return '{"$literal": null}'


# ---------------------------------------------------------------------------
# Engine-agnostic façade
# ---------------------------------------------------------------------------

def list_schemas(engine: str) -> list[str]:
    """Test schemas for one engine."""
    if engine == "mysql":
        return list_mysql_schemas()
    if engine == "mongodb":
        return list_mongo_schemas()
    if engine == "postgresql":
        return list_postgres_schemas()
    raise ValueError(f"unknown engine: {engine}")


def get_schema_columns(engine: str, schema: str) -> dict[str, list[dict]]:
    """Every table's columns in one schema, fetched in a single round trip."""
    if engine == "mysql":
        return get_mysql_schema_columns(schema)
    if engine == "mongodb":
        return get_mongo_schema_fields(schema)
    if engine == "postgresql":
        return get_postgres_schema_columns(schema)
    raise ValueError(f"unknown engine: {engine}")


def list_columns(engine: str, schema: str, table: str) -> list[str]:
    """Column names of a single table."""
    return [c["name"] for c in get_schema_columns(engine, schema).get(table, [])]


def list_tables(engine: str, schema: str) -> list[str]:
    """Table (or collection) names in a schema."""
    if engine == "mongodb":
        return list_mongo_collections(schema)
    return sorted(get_schema_columns(engine, schema).keys())


def table_exists(engine: str, schema: str, table: str) -> bool:
    """Whether the source table still exists.

    This is the signal ETL uses to decide "the table was dropped upstream —
    skip the job and notify", so it must never raise.
    """
    try:
        if engine == "mongodb":
            return table in list_mongo_collections(schema)

        if engine == "mysql":
            conn = _mysql_connection()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT 1 FROM information_schema.tables "
                        "WHERE table_schema = %s AND table_name = %s",
                        (schema, table))
                    return cur.fetchone() is not None
            finally:
                conn.close()

        if engine == "postgresql":
            conn = _postgres_connection()
            try:
                cur = conn.cursor()
                cur.execute(
                    "SELECT 1 FROM information_schema.tables "
                    "WHERE table_schema = %s AND table_name = %s",
                    (schema, table))
                return cur.fetchone() is not None
            finally:
                conn.close()
    except Exception as exc:                    # noqa: BLE001
        logger.error("existence check failed %s.%s.%s: %s",
                     engine, schema, table, exc)
        return False

    return False


def count_rows(engine: str, schema: str, table: str) -> int:
    """Row count of a source table."""
    if engine == "mongodb":
        client = _mongo_connection()
        try:
            return client[schema][table].count_documents({})
        finally:
            client.close()

    if engine == "mysql":
        conn = _mysql_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) AS cnt FROM `{schema}`.`{table}`")
                return int(cur.fetchone()["cnt"])
        finally:
            conn.close()

    if engine == "postgresql":
        conn = _postgres_connection()
        try:
            cur = conn.cursor()
            cur.execute(f'SELECT COUNT(*) AS cnt FROM "{schema}"."{table}"')
            return int(cur.fetchone()[0])
        finally:
            conn.close()

    return 0


def sample_rows(engine: str, schema: str, table: str,
                columns: list[str], limit: int = 3) -> list[dict]:
    """A few sample rows, used for before/after de-identification evidence.

    MongoDB projects `_id` out because ObjectId is not JSON friendly and
    cannot be written to parquet.
    """
    if engine == "mongodb":
        client = _mongo_connection()
        try:
            projection = {c: 1 for c in columns}
            projection["_id"] = 0
            docs = list(client[schema][table].find({}, projection).limit(limit))
            for doc in docs:
                doc.pop("_id", None)
            return docs
        finally:
            client.close()

    if engine == "mysql":
        conn = _mysql_connection()
        try:
            with conn.cursor() as cur:
                cols = ", ".join(f"`{c}`" for c in columns)
                cur.execute(f"SELECT {cols} FROM `{schema}`.`{table}` LIMIT {limit}")
                return list(cur.fetchall())
        finally:
            conn.close()

    if engine == "postgresql":
        conn = _postgres_connection()
        try:
            cur = conn.cursor()
            cols = ", ".join(f'"{c}"' for c in columns)
            cur.execute(f'SELECT {cols} FROM "{schema}"."{table}" LIMIT {limit}')
            names = [d[0] for d in cur.description]
            return [dict(zip(names, row)) for row in cur.fetchall()]
        finally:
            conn.close()

    return []


def read_rows(engine: str, schema: str, table: str,
              columns: list[str], limit: int | None = None) -> list[dict]:
    """Read source rows into memory (no `_id`, uniform keys across engines).

    Used by the ETL module: it needs real source values so that the
    de-identification result can be compared against the target table.
    """
    if engine == "mongodb":
        client = _mongo_connection()
        try:
            projection = {c: 1 for c in columns}
            projection["_id"] = 0
            cursor = client[schema][table].find({}, projection).sort("id", 1)
            if limit:
                cursor = cursor.limit(limit)
            return list(cursor)
        finally:
            client.close()

    if engine == "mysql":
        conn = _mysql_connection()
        try:
            with conn.cursor() as cur:
                cols = ", ".join(f"`{c}`" for c in columns)
                limit_sql = f" LIMIT {int(limit)}" if limit else ""
                cur.execute(f"SELECT {cols} FROM `{schema}`.`{table}`"
                            f" ORDER BY `id`{limit_sql}")
                return list(cur.fetchall())
        finally:
            conn.close()

    if engine == "postgresql":
        conn = _postgres_connection()
        try:
            cur = conn.cursor()
            cols = ", ".join(f'"{c}"' for c in columns)
            limit_sql = f" LIMIT {int(limit)}" if limit else ""
            cur.execute(f'SELECT {cols} FROM "{schema}"."{table}" '
                        f'ORDER BY "id"{limit_sql}')
            names = [d[0] for d in cur.description]
            return [dict(zip(names, row)) for row in cur.fetchall()]
        finally:
            conn.close()

    return []


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    for engine in ENGINES:
        try:
            schemas = list_schemas(engine)
            print(f"{engine:12s} OK   schemas={len(schemas)}")
            for schema in schemas:
                tables = list_tables(engine, schema)
                col_counts = {len(list_columns(engine, schema, t)) for t in tables}
                print(f"    {schema}: tables={len(tables)} column_counts={sorted(col_counts)}")
        except Exception as exc:                # noqa: BLE001
            print(f"{engine:12s} FAIL {type(exc).__name__}: {exc}")
