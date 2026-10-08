"""
Databricks SQL client for the Free Edition SQL warehouse.

Why a dedicated client
----------------------
`POST /api/2.0/sql/statements` does **not** return a finished result. It
returns a statement id plus a status, and the caller has to poll
`GET /api/2.0/sql/statements/{id}` until the state settles. Without that
polling a "successful" call can still be a running or a failed query.

Measured quirks this module exists to absorb
--------------------------------------------
1. `wait_timeout` only bounds *how long the server waits*. It is not a
   completion signal. Passing `on_wait_timeout=CONTINUE` plus polling is the
   only way to learn the real outcome, including FAILED and CLOSED.
2. String literals do not accept a doubled quote as an escape. Only a
   backslash before the quote works, which `sql_literal()` emits.
3. SQL aliases must be ASCII. A non-ASCII alias fails outright.
4. `DROP TABLE IF EXISTS` immediately followed by `CREATE` raises
   `TABLE_OR_VIEW_ALREADY_EXISTS`, and the table does not even appear in
   SHOW. Use `CREATE OR REPLACE` instead.
5. A two-part name must be quoted **per part**. Quoting the whole string
   produces one identifier containing a dot, which Spark then resolves
   against a table name — so `SHOW TABLES` echoes the schema back and every
   existence check falsely reports "missing".
6. `SHOW TABLES LIKE 'x' IN schema` is a syntax error; the grammar is
   `SHOW TABLES IN <schema> LIKE '<pattern>'`.
7. Free Edition has daily limits on queries and jobs. A limit failure
   surfaces as HTTP 403 or 400 and resets at 00:00 UTC (09:00 KST).
8. `system.query.history` is populated with roughly 405 seconds of lag, so
   reverse lookups by statement id return nothing immediately.
"""
from __future__ import annotations

import time
from typing import Any

import requests

try:
    from . import config
except ImportError:                            # standalone execution
    import config                          # type: ignore

#: Backtick used to quote identifiers.
BT = chr(96)


class SqlExecutionError(RuntimeError):
    """Raised when a statement fails. Carries Databricks' own message."""


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

def _resolve_connection() -> tuple[str, str]:
    """Return (host, token).

    Lookup order
    ------------
    1. Environment variables — how the host runs, and how compose injects them.
    2. Airflow Variables — needed because the Airflow containers in this stack
       are **not** given the Databricks credentials by compose (they only get
       AWS keys). Without this the token was empty inside a DAG even though
       the same code worked from the host.
    3. `DATABRICKS_HOST` has the same fallback, for the same reason.

    Failing loudly is deliberate: an empty token produces a 401 that looks
    like a permissions problem rather than a configuration problem.
    """
    conf = config.get_databricks_config()

    host = config.get_secret("DATABRICKS_HOST") or conf.get("host", "")
    token = config.get_secret("DATABRICKS_TOKEN")

    if config.in_container() and not token:
        # Only attempt an Airflow import inside the container.
        try:
            from airflow.models import Variable
            token = Variable.get("DATABRICKS_TOKEN", default_var="") or ""
            if not host:
                host = Variable.get("DATABRICKS_HOST", default_var="") or ""
        except Exception:                       # noqa: BLE001
            pass

    if not host:
        host = conf.get("host", "")
    if not token:
        raise SqlExecutionError(
            "DATABRICKS_TOKEN 이 없습니다. 환경변수 또는 Airflow Variable "
            "'DATABRICKS_TOKEN' 을 설정하십시오."
        )
    return host.rstrip("/"), token


def _auth_headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


# ---------------------------------------------------------------------------
# Execution
# ---------------------------------------------------------------------------

def execute_sql(sql: str, *, wait_timeout: str = "30s",
                poll_interval: float = 0.5, max_wait_seconds: int = 600,
                retries: int = 3) -> dict:
    """Run one statement and wait until it actually settles.

    Returns:
        {"state", "statement_id", "columns", "rows", "manifest", "message"}

    Raises:
        SqlExecutionError: on FAILED / CLOSED, or after the wait budget.
    """
    host, token = _resolve_connection()
    warehouse_id = config.get_databricks_config()["warehouse_id"]
    interval = float(poll_interval
                     or config.get_databricks_config().get("poll_interval", 0.5))

    payload = {
        "warehouse_id": warehouse_id,
        "statement": sql,
        "wait_timeout": wait_timeout,
        # Return immediately instead of blocking, so we can poll.
        "on_wait_timeout": "CONTINUE",
    }

    last_error: Exception | None = None

    for attempt in range(1, retries + 1):
        try:
            response = requests.post(
                f"{host}/api/2.0/sql/statements",
                headers=_auth_headers(token), json=payload, timeout=120)
            if response.status_code != 200:
                raise SqlExecutionError(
                    f"SQL 접수 실패 (HTTP {response.status_code}): "
                    f"{response.text[:500]}"
                )

            state = response.json()
            statement_id = state["statement_id"]

            # ---- poll ------------------------------------------------
            # PENDING / RUNNING is expected because of on_wait_timeout.
            elapsed = 0.0
            while state.get("status", {}).get("state") in ("PENDING", "RUNNING"):
                if elapsed >= max_wait_seconds:
                    raise SqlExecutionError(
                        f"SQL 대기 시간 초과({max_wait_seconds}초): {statement_id}")
                time.sleep(interval)
                elapsed += interval
                polled = requests.get(
                    f"{host}/api/2.0/sql/statements/{statement_id}",
                    headers=_auth_headers(token), timeout=60)
                if polled.status_code == 200:
                    state = polled.json()

            final = state.get("status", {}).get("state")
            if final != "SUCCEEDED":
                error = state.get("status", {}).get("error", {})
                raise SqlExecutionError(
                    f"SQL 실패 [{final}] statement_id={statement_id}\n"
                    f"  오류코드: {error.get('error_code')}\n"
                    f"  메시지  : {str(error.get('message'))[:800]}\n"
                    f"  SQL     : {sql[:300]}"
                )

            return {
                "state": final,
                "statement_id": statement_id,
                "columns": state.get("manifest", {})
                                .get("schema", {}).get("columns", []),
                "rows": state.get("result", {}).get("data_array", []),
                "manifest": state.get("manifest", {}),
                "message": state.get("status", {}).get("message"),
            }

        except (requests.RequestException, SqlExecutionError) as exc:
            last_error = exc
            # A quota failure (403) will not change on retry.
            if isinstance(exc, SqlExecutionError) and "HTTP 403" in str(exc):
                raise
            if attempt < retries:
                time.sleep(1.5 * attempt)

    assert last_error is not None
    raise last_error


def fetch_value(sql: str) -> Any:
    """Single value, e.g. from `SELECT COUNT(*)`."""
    rows = execute_sql(sql)["rows"]
    return rows[0][0] if rows else None


def fetch_values(sql: str) -> list:
    """First column of every row."""
    return [row[0] for row in execute_sql(sql)["rows"]]


def describe_columns(table: str) -> list[str]:
    """Column names via DESCRIBE.

    DESCRIBE mixes in partition/block sections whose rows start with `#`.
    Those and blank names are dropped.
    """
    result = execute_sql(f"DESCRIBE TABLE {table}")
    names: list[str] = []
    for row in result["rows"]:
        raw = row[0] if row else ""
        name = str(raw).strip() if raw is not None else ""
        if not name or name.startswith("#"):
            continue
        names.append(name)
    return names


def table_exists(table: str) -> bool:
    """Whether a table exists, queried through `information_schema`.

    Measured
    --------
    `SHOW TABLES IN <schema>` is unusable for this:

      * Quoting the whole two-part name yields one identifier containing a
        dot, which Spark resolves against a *table* name.
      * With a correct schema the command returns the **schema name itself**
        when the schema holds no tables, e.g.
        `SHOW TABLES IN pretest_meta` -> `['pretest_meta', 'pretest_meta']`
      * Results are not de-duplicated, so the same table appears twice.

    Any name-based comparison built on that output is wrong in both
    directions, which is exactly what was observed: existing tables reported
    missing, and a missing table reported present.

    `information_schema.tables` answers the question directly and is used
    for every lookup from here on.
    """
    parts = table.split(".")
    name = parts[-1]
    schema_parts = parts[:-1]

    # Measured: Databricks information_schema has no catalog_name column,
    # so a catalog filter fails with UNRESOLVED_COLUMN. Only table_schema and
    # table_name exist, and for this project every catalog is workspace.
    schema = schema_parts[-1] if schema_parts else "default"
    predicate = f"table_schema = {sql_literal(schema)}"

    sql = ("SELECT COUNT(*) FROM information_schema.tables WHERE "
           + predicate + f" AND table_name = {sql_literal(name)}")

    try:
        return int(fetch_value(sql) or 0) > 0
    except Exception:                            # noqa: BLE001
        return False


# ---------------------------------------------------------------------------
# Literals and identifiers
# ---------------------------------------------------------------------------

def sql_literal(value: Any) -> str:
    """Quote a value as a SQL string literal.

    Measured: Databricks does not accept a doubled quote as an escape. Only a
    backslash before the quote works, which is what this emits.
    """
    if value is None:
        return "NULL"
    text = str(value)
    escaped = text.replace(chr(92), chr(92) * 2)
    escaped = escaped.replace(chr(39), chr(92) + chr(39))
    return chr(39) + escaped + chr(39)


def identifier(value: str) -> str:
    """Quote an identifier with backticks.

    Measured: an unquoted column name that is a reserved word, or that
    contains non-ASCII characters, fails to parse. Backticks handle both
    cases, so every identifier produced here is quoted.
    """
    escaped = str(value).replace(BT, BT * 2)
    return BT + escaped + BT


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        outcome = execute_sql(sys.argv[1])
        print(f"state={outcome['state']} rows={len(outcome['rows'])} "
              f"id={outcome['statement_id']}")
        for row in outcome["rows"][:20]:
            print("  ", row)
    else:
        print("SELECT 1 AS chk")
        print(execute_sql("SELECT 1 AS chk"))
