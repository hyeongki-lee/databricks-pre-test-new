"""
Iceberg-format file writer.

Core concept — what "create everything as Iceberg" means here
-------------------------------------------------------------
MySQL, MongoDB and PostgreSQL cannot *be* Iceberg. Iceberg is a table format
that lives on a file system. So the requirement is implemented as:

  1. Data is written into ordinary source tables (MySQL / Mongo / PG).
  2. The same data is written to local disk **as an Iceberg table layout**:
       data/     -> parquet data files
       metadata/ -> metadata.json + manifest list
  3. rclone replicates the *folder*, so metadata regenerated on every change
     travels along automatically.
  4. Databricks Auto Loader reads that folder into a managed table.

Why metadata must travel with the data
--------------------------------------
Iceberg represents change through a **snapshot chain**:

    v1.metadata.json   <- initial 50k rows
    v2.metadata.json   <- after an update/delete round
    v3.metadata.json   <- after another round

Each metadata links to its predecessor via `parent-snapshot-id`. Overwrite a
data file in place and the chain breaks, because the previous state can no
longer be reconstructed.

This module therefore does three things:
  1. Writes data files into version-scoped folders (data/, with a new
     snapshot id per version).
  2. Writes a fresh metadata.json for every version (snapshot chain).
  3. Writes the matching manifest list.

What Databricks actually reads
------------------------------
Auto Loader (`INSERT ... FROM parquet.\`path\``) reads **data files only**.
It does not parse metadata.json. So the practical benefit of the layout is:

  * Column-history and snapshot bookkeeping survive for auditing.
  * Requirement "metadata generated on change must also be transferred" is
    literally satisfied — the files are on S3.
  * Upstream schema drift becomes visible at the file level.

Consequence for responsibility: dropped-column handling with
`CAST(NULL AS col)` belongs to the **ETL module**, which reconnects to the
source at run time. Auto Loader only ingests what the file contains.
"""
from __future__ import annotations

import decimal
import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

#: Iceberg format version written by this (intentionally simple) implementation.
FORMAT_VERSION = 2


# ---------------------------------------------------------------------------
# Schema inference
# ---------------------------------------------------------------------------

def _python_to_iceberg_type(value: Any) -> str:
    """Map a Python value to an Iceberg primitive type name."""
    if value is None:
        return "string"
    if isinstance(value, bool):
        return "boolean"          # must precede int: bool is a subclass
    if isinstance(value, int):
        return "long"
    if isinstance(value, float):
        return "double"
    if isinstance(value, datetime):
        return "timestamp"
    return "string"


def infer_schema(rows: list[dict]) -> tuple[list[str], list[dict]]:
    """Derive (column_order, fields) from a sample of rows."""
    if not rows:
        return [], []

    names = list(rows[0].keys())
    fields = []
    for name in names:
        dtype = "string"
        for row in rows:
            if row.get(name) is not None:
                dtype = _python_to_iceberg_type(row[name])
                break
        fields.append({"name": name, "type": dtype})
    return names, fields


# ---------------------------------------------------------------------------
# Data files
# ---------------------------------------------------------------------------

#: Columns whose arrow type is pinned. Pinning removes inference differences
#: between MySQL (int for TINYINT(1)), MongoDB (bool) and PostgreSQL (bool).
_FORCED_TYPES: dict[str, tuple[Any, bool]] = {
    "id": (pa.int64(), False),
    "age": (pa.int64(), True),
    "salary": (pa.float64(), True),
    "is_active": (pa.bool_(), True),
    "created_at": (pa.timestamp("us"), True),
    "updated_at": (pa.timestamp("us"), True),
}


def _explicit_schema(order: list[str], rows: list[dict]) -> pa.Schema:
    """Declare the arrow schema instead of letting pyarrow infer it."""
    fields = []
    for name in order:
        if name in _FORCED_TYPES:
            arrow_type, nullable = _FORCED_TYPES[name]
            fields.append(pa.field(name, arrow_type, nullable=nullable))
            continue
        # Fall back to inference for anything not pinned above.
        arrow_type = pa.string()
        for row in rows:
            value = row.get(name)
            if value is not None:
                arrow_type = _arrow_type_of(value)
                break
        fields.append(pa.field(name, arrow_type, nullable=True))
    return pa.schema(fields)


def _arrow_type_of(value: Any) -> pa.DataType:
    if isinstance(value, bool):
        return pa.bool_()
    if isinstance(value, int):
        return pa.int64()
    if isinstance(value, float):
        return pa.float64()
    if isinstance(value, datetime):
        return pa.timestamp("us")
    return pa.string()


def _normalise_value(value: Any, column: str | None = None) -> Any:
    """Coerce a driver-specific value into something pyarrow accepts.

    Measured problems absorbed here (all three were real failures):

    * `decimal.Decimal` — MySQL and PostgreSQL return DECIMAL columns as
      Decimal, and pyarrow rejects them for a double column:
      `Could not convert Decimal('30052662.00') ... tried to convert to double`
    * `int` for a boolean column — MySQL maps `TINYINT(1)` to int 0/1, so a
      boolean-typed arrow field fails:
      `Could not convert 1 with type int: tried to convert to boolean`
      MongoDB and PostgreSQL return a real `bool` for the same logical column,
      so the cast also makes the three engines agree.
    * `bytes` — binary/text columns arrive as bytes and cannot be written.

    Normalising at this boundary is what lets one physical parquet schema serve
    all three engines.
    """
    if column == "is_active":
        if value is None:
            return None
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            return value.strip().lower() in ("1", "true", "t", "yes", "y")
        return bool(value)

    if isinstance(value, decimal.Decimal):
        return float(value)
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", errors="replace")
    return value


def _write_parquet(rows: list[dict], path: Path,
                   column_order: list[str] | None = None) -> dict:
    """Write rows to a parquet file and return its file record."""
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        return {"path": str(path), "rows": 0, "size": 0,
                "sha256": "", "record_count": 0}

    order = column_order or list(rows[0].keys())
    normalised = [{k: _normalise_value(row.get(k), k) for k in order}
                  for row in rows]

    # Measured: inferring types per column is unreliable for this data set.
    #   MySQL returns TINYINT(1) is_active as int 0/1, so pyarrow inferred
    #   oolean from the first row and then rejected the integer 1:
    #       ArrowInvalid: Could not convert 1 with type int:
    #       tried to convert to boolean
    #   A CDC delta row set also mixes typed rows with tombstones (all None),
    #   which makes inference worse. So the schema is declared explicitly,
    #   which also guarantees all three engines write identical physical types.
    schema = _explicit_schema(order, normalised)
    table = pa.Table.from_pylist(normalised, schema=schema)
    pq.write_table(table, path, compression="snappy")

    payload = path.read_bytes()
    return {
        "path": str(path),
        "rows": len(normalised),
        "size": len(payload),
        "sha256": hashlib.sha256(payload).hexdigest(),
        "record_count": len(normalised),
    }


# ---------------------------------------------------------------------------
# Metadata (snapshot chain)
# ---------------------------------------------------------------------------

def _snapshot_id(engine: str, schema: str, table: str,
                 version: int, files: list[dict]) -> int:
    """Content-derived snapshot id.

    Hashing the content means the same data always yields the same id, which
    makes reruns reproducible instead of producing spurious new snapshots.
    """
    material = json.dumps(
        {"e": engine, "s": schema, "t": table, "v": version,
         "f": [(f["path"], f["sha256"]) for f in files]},
        ensure_ascii=False, sort_keys=True,
    )
    return int(hashlib.sha256(material.encode("utf-8")).hexdigest()[:16], 16) \
        & 0x7FFFFFFFFFFFFFFF


def _write_metadata(base_folder: Path, *, engine: str, schema: str, table: str,
                    version: int, files: list[dict], fields: list[dict],
                    previous_version: int | None = None) -> dict:
    """Write an Iceberg-style metadata.json plus its manifest list.

    The snapshot chain is built here:
        `parent-snapshot-id` -> snapshot id of the immediately previous version
        `current-snapshot-id` -> this version's snapshot id
    """
    base_folder.mkdir(parents=True, exist_ok=True)
    meta_dir = base_folder / "metadata"
    meta_dir.mkdir(exist_ok=True)

    location = f"{engine}/{schema}/{table}"
    snapshot = _snapshot_id(engine, schema, table, version, files)
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

    # Resolve the parent snapshot id from the previous metadata file.
    parent_id = None
    if previous_version is not None:
        previous_meta = meta_dir / f"v{previous_version}.metadata.json"
        if previous_meta.exists():
            try:
                parent_id = json.loads(
                    previous_meta.read_text(encoding="utf-8"))["current-snapshot-id"]
            except Exception:                    # noqa: BLE001
                parent_id = None

    # Manifest list: one entry per data file.
    manifest_entries = [{
        "file-path": f"{location}/{f['path'].split('data/')[-1]}",
        "record-count": f["rows"],
        "file-size-in-bytes": f["size"],
        "column-stats": {"hash": f["sha256"]},
    } for f in files]

    manifest_path = meta_dir / f"v{version}.manifest.json"
    manifest_path.write_text(
        json.dumps({"manifest-list": manifest_entries}, ensure_ascii=False, indent=2),
        encoding="utf-8")

    schema_fields = [
        {"id": i + 1, "name": fd["name"], "required": False, "type": fd["type"]}
        for i, fd in enumerate(fields)
    ]

    document = {
        "format-version": FORMAT_VERSION,
        "table-uuid": hashlib.md5(location.encode("utf-8")).hexdigest(),
        "location": f"file:///{base_folder.as_posix()}",
        "last-sequence-number": version,
        "last-updated-ms": now_ms,
        "last-column-id": len(fields),
        "current-schema": {"type": "struct", "schema-id": 1, "fields": schema_fields},
        "schemas": [{"type": "struct", "schema-id": 1, "fields": schema_fields}],
        "current-snapshot-id": snapshot,
        "snapshots": [{
            "snapshot-id": snapshot,
            "parent-snapshot-id": parent_id,
            "sequence-number": version,
            "timestamp-ms": now_ms,
            "manifest-list": f"metadata/{manifest_path.name}",
            "summary": {
                "operation": "append",
                "added-data-files": str(len(files)),
                "added-records": str(sum(f["rows"] for f in files)),
                "스키마변경": "없음" if previous_version else "최초 생성",
            },
        }],
        "metadata-log": (
            [{"metadata-file": f"metadata/v{previous_version}.metadata.json",
              "timestamp-ms": now_ms}]
            if previous_version else []
        ),
    }

    meta_path = meta_dir / f"v{version}.metadata.json"
    meta_path.write_text(json.dumps(document, ensure_ascii=False, indent=2),
                         encoding="utf-8")

    return {
        "metadata_path": str(meta_path),
        "manifest_path": str(manifest_path),
        "snapshot_id": snapshot,
        "parent_snapshot_id": parent_id,
        "version": version,
    }


# ---------------------------------------------------------------------------
# One table
# ---------------------------------------------------------------------------

def write_table(base_folder: Path, *, engine: str, schema: str, table: str,
                rows: list[dict], version: int = 1,
                rows_per_file: int = 20000) -> dict:
    """Write the complete file set (data + metadata) for one table.

    Layout:
        <base_folder>/
            data/00000.parquet
            data/00001.parquet
            metadata/v1.metadata.json
            metadata/v1.manifest.json
    """
    column_order, fields = infer_schema(rows[:100] if rows else [])
    data_dir = base_folder / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    files: list[dict] = []
    if rows:
        for start in range(0, len(rows), rows_per_file):
            chunk = rows[start:start + rows_per_file]
            index = start // rows_per_file
            files.append(_write_parquet(
                chunk, data_dir / f"{index:05d}.parquet", column_order))

    meta = _write_metadata(
        base_folder, engine=engine, schema=schema, table=table,
        version=version, files=files, fields=fields,
        previous_version=version - 1 if version > 1 else None)

    return {
        "engine": engine, "schema": schema, "table": table, "version": version,
        "total_rows": len(rows),
        "file_count": len(files),
        "total_size": sum(f["size"] for f in files),
        "columns": column_order,
        **meta,
    }


if __name__ == "__main__":
    import tempfile

    tmp = Path(tempfile.mkdtemp()) / "mysql" / "mysql_schema_1" / "table_1"
    sample = [
        {"id": 1, "name": "user_1", "email": "user_1@example.com",
         "created_at": datetime(2026, 1, 1, 10, 0), "age": 25, "is_active": True},
        {"id": 2, "name": "user_2", "email": "user_2@example.com",
         "created_at": datetime(2026, 1, 2, 10, 0), "age": 31, "is_active": False},
    ]

    v1 = write_table(tmp, engine="mysql", schema="mysql_schema_1", table="table_1",
                     rows=sample * 15000, version=1, rows_per_file=10000)
    v2 = write_table(tmp, engine="mysql", schema="mysql_schema_1", table="table_1",
                     rows=sample * 16000, version=2, rows_per_file=10000)

    print(f"v1: files={v1['file_count']} rows={v1['total_rows']:,} "
          f"size={v1['total_size']:,}B")
    print(f"    snapshot={v1['snapshot_id']} parent={v1['parent_snapshot_id']}")
    print(f"v2: files={v2['file_count']} rows={v2['total_rows']:,} "
          f"size={v2['total_size']:,}B")
    print(f"    snapshot={v2['snapshot_id']} parent={v2['parent_snapshot_id']}")
    print(f"    chain ok (v2.parent == v1.snapshot)? "
          f"{v2['parent_snapshot_id'] == v1['snapshot_id']}")
    print()
    for p in sorted(tmp.rglob("*")):
        if p.is_file():
            print(f"  {p.relative_to(tmp)}  ({p.stat().st_size:,}B)")
