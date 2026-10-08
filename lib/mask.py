"""
De-identification (D1 / D2 / D3).

Requirement mapping
-------------------
    D1 = hash(sha256)   hashing
    D2 = null 처리      replace with null
    D3 = masking        masking

Why the transformation lives in SQL
-----------------------------------
The previous project built a 50k-row pandas DataFrame in the Airflow process
and tried `COPY INTO ... FROM '/tmp/x.parquet'`. That fails for two reasons:

  1. A Databricks SQL warehouse cannot see the Airflow container's disk.
  2. Materialising 50k rows in Python memory defeats the point of parallel
     workers — memory pressure grows with the worker count.

So de-identification is expressed as SQL and evaluated inside Databricks.
The intermediate data never touches Python memory, which means the row count
can grow without changing the architecture.

Spark SQL mapping
-----------------
    D1  sha2(CAST(col AS STRING), 256)
    D2  CAST(NULL AS <type>)
    D3  keep first N + '*' * 6 + last N

Gotchas worth remembering
-------------------------
* `sha2()` only accepts strings. Applying it straight to a numeric or
  timestamp column triggers an implicit cast whose result may not match
  expectations, so we cast explicitly.
* D3 produces a string. If the target column is numeric the load fails, so
  D3 targets are typed as STRING on the Databricks side.
* Databricks does **not** accept `''` as a string escape — only `\'`.
"""
from __future__ import annotations

from typing import Any

try:
    from . import config as cfg
except ImportError:
    import config as cfg                    # type: ignore

#: Supported de-identification codes.
DEID_CODES = {
    "D1": "hash",
    "D2": "null",
    "D3": "masking",
}

#: Korean labels shown in the dashboard and manual.
DEID_LABELS = {
    "D1": "해시(SHA-256)",
    "D2": "null 처리",
    "D3": "마스킹",
}

#: Fixed number of asterisks inserted by D3 (independent of value length, so
#: the output length does not leak the original length).
MASK_STARS = "******"


def mask_keep_length() -> int:
    """How many characters D3 keeps at each end."""
    return int(cfg.get_etl_config().get("mask_keep", 2))


# ---------------------------------------------------------------------------
# SQL expression builders
# ---------------------------------------------------------------------------

def build_expression(source_expr: str, code: str,
                     target_type: str | None = None,
                     alias: str | None = None) -> str:
    """Build the SQL expression for one de-identification code.

    Args:
        source_expr: pre-transform column expression, e.g. `` `email` ``
        code:        D1 / D2 / D3
        target_type: target column type (required by D2). Defaults to STRING.
        alias:       output column name.
    """
    code = str(code).strip().upper()
    suffix = f" AS {alias}" if alias else ""

    if code == "D1":
        # CAST first: sha2() only accepts strings.
        return f"sha2(CAST({source_expr} AS STRING), 256){suffix}"

    if code == "D2":
        # The column itself is kept — only its value becomes unknown.
        # Dropping the column would break later MERGE/compare logic.
        dtype = target_type or "STRING"
        return f"CAST(NULL AS {dtype}){suffix}"

    if code == "D3":
        n = mask_keep_length()
        # Two guards: values at or below 2n+1 chars become all-stars so
        # short values are not partially revealed.
        return (
            "CASE"
            f" WHEN {source_expr} IS NULL THEN NULL"
            f" WHEN length(CAST({source_expr} AS STRING)) <= {2 * n + 1}"
            f"   THEN '{MASK_STARS}'"
            " ELSE concat("
            f"substring(CAST({source_expr} AS STRING), 1, {n}),"
            f"'{MASK_STARS}',"
            f"substring(CAST({source_expr} AS STRING),"
            f"length(CAST({source_expr} AS STRING)) - {n} + 1, {n})"
            f")"
            f" END{suffix}"
        )

    raise ValueError(
        f"unknown de-identification code '{code}'. allowed: {list(DEID_CODES)}"
    )


def build_column_expressions(items: list[tuple[str, str, str | None]],
                             deidentification: dict[str, str],
                             default_type: str = "STRING") -> list[str]:
    """Build the SELECT list for a whole ETL target column set.

    Args:
        items: [(source_expr, output_name, target_type), ...]
        deidentification: {column_name: D-code}

    Returns:
        SQL fragments ready to be joined with ", ".
    """
    fragments: list[str] = []
    for source_expr, name, dtype in items:
        code = deidentification.get(name)
        if code:
            fragments.append(
                build_expression(source_expr, code, dtype or default_type, alias=name))
        else:
            fragments.append(f"{source_expr} AS `{name}`")
    return fragments


# ---------------------------------------------------------------------------
# Python implementation (verification only)
# ---------------------------------------------------------------------------
# Used to prove that what Databricks produced matches what we expect.
# The manual captures these values as "source → target" evidence.

def apply_in_python(value: Any, code: str) -> Any:
    """Apply one de-identification rule in Python (verification only)."""
    import hashlib

    code = str(code).strip().upper()
    if value is None:
        return None

    text = str(value)

    if code == "D1":
        return hashlib.sha256(text.encode("utf-8")).hexdigest()

    if code == "D2":
        return None

    if code == "D3":
        n = mask_keep_length()
        if len(text) <= 2 * n + 1:
            return MASK_STARS
        return text[:n] + MASK_STARS + text[-n:]

    raise ValueError(f"unknown de-identification code '{code}'")


if __name__ == "__main__":
    print("=== D1 / D2 / D3 SQL expressions (email column) ===")
    for code in ("D1", "D2", "D3"):
        print(f"  {code} ({DEID_LABELS[code]}):")
        print(f"    {build_expression('`email`', code, 'STRING', alias='email')}")

    print()
    print("=== Python reference values (verification) ===")
    for raw in ("user_1@example.com", "010-0001-01", "서울특별시", "ab", "가나다", None):
        cells = [f"{raw!r:24s}"]
        for code in ("D1", "D2", "D3"):
            cells.append(f"{code}={str(apply_in_python(raw, code))[:30]}")
        print("  " + "  ".join(cells))
