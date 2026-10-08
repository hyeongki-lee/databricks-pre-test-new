"""
rclone replication step — RC API only, never a shell command.

Signal chain
------------
    1. `_chk.json` exists locally            -> data preparation finished
    2. rclone RC API `copy`                 -> incremental transfer to S3
    3. jobid polled until `completed`        -> transfer finished
    4. `_rclone_done.json` written to S3     -> Airflow writes this itself

Why two separate files
----------------------
`chk` is produced by `data-prep` and travels *with* the data.
`_rclone_done` is produced by Airflow *after* it confirms the transfer.

Keeping them distinct is what makes the causality checkable. An earlier
implementation wrote `chk` straight to S3 with boto3, which made
"start replicating once chk is present" meaningless — it was already there.

Why `copy` and not `sync`
-------------------------
`sync` deletes anything on the destination that is absent from the source. If
only some tables were refreshed, `sync` would wipe the untouched ones.
`copy` only moves what exists, so an incremental folder is safe.

Polling is the point of using the API
-------------------------------------
The RC API starts the transfer and returns a `jobid`; the job then runs in the
background. Completion is observable via `/job/status`. With a shell command
there is nothing to poll — the process exit code is all you get.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# Windows console (cp949) cannot print every character used in the messages.
for _s in (sys.stdout, sys.stderr):
    try:
        _s.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass

from lib import config as cfg                  # noqa: E402
from lib import rclone_rc                      # noqa: E402

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)-5s %(message)s",
                    datefmt="%H:%M:%S", stream=sys.stdout, force=True)
logger = logging.getLogger("rclone-step")

ENGINES = ["mysql", "mongodb", "postgresql"]


def s3_prefix() -> str:
    """Destination prefix: `<bucket>/<prefix>`."""
    s3 = cfg.get_s3_config()
    return f"{s3['bucket']}/{s3.get('prefix', 'pretest')}"


def local_ready_tables(engine: str, schema: str) -> list[str]:
    """Tables whose local `chk` exists (= data preparation finished)."""
    root = cfg.local_data_root()
    name = cfg.get_s3_config().get("chk_file", "_chk.json")
    folder = root / engine / schema
    if not folder.exists():
        return []
    return sorted(p.parent.name for p in folder.glob(f"*/{name}"))


def write_done_signal(engine: str, schema: str, result: dict,
                      tables: list[str]) -> str:
    """Write `_rclone_done.json` to S3.

    Measured: putting the **bucket name into the Key** makes the object
    invisible — `put_object` takes the bucket via the `Bucket` parameter, so
    the Key must contain only the prefix.
    """
    import boto3

    s3 = cfg.get_s3_config()
    name = s3.get("rclone_done_file", "_rclone_done.json")
    key = f"{s3.get('prefix', 'pretest')}/{engine}/{schema}/{name}"

    client = boto3.client("s3", region_name=s3.get("region", "ap-northeast-2"))
    document = {
        "단계": "rclone 복제 완료",
        "엔진": engine, "스키마": schema,
        "테이블목록": tables, "테이블수": len(tables),
        "rclone_jobid": result.get("jobid"),
        "옮긴바이트": result.get("옮긴바이트", 0),
        "옮긴파일": result.get("옮긴파일", 0),
        "완료시각": datetime.now().astimezone().isoformat(timespec="seconds"),
        "소요초": result.get("소요초", 0),
        "상태": "completed",
    }
    client.put_object(
        Bucket=s3["bucket"], Key=key,
        Body=json.dumps(document, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    return f"s3://{s3['bucket']}/{key}"


def copy_schema(engine: str, schema: str, *, dry_run: bool = False) -> dict:
    """Replicate one schema (all its tables) to S3 and wait for completion."""
    root = cfg.local_data_root()
    local = root / engine / schema
    if not local.exists():
        raise FileNotFoundError(f"로컬 폴더가 없습니다: {local}")

    # Signal 1: data preparation finished.
    tables = local_ready_tables(engine, schema)
    if not tables:
        raise RuntimeError(
            f"{engine}.{schema} 에 chk 파일이 없습니다. "
            "data-prep/prepare.py 를 먼저 실행하십시오."
        )

    started = time.time()
    source = f"/data/{engine}/{schema}"
    destination = f"{s3_prefix()}/{engine}/{schema}"

    logger.info("[%s.%s] chk %d개 확인 → rclone copy 시작 (→ %s)",
                engine, schema, len(tables), destination)

    result = rclone_rc.copy_and_wait(source, destination, dry_run=dry_run)

    # Signal 2: replication confirmed, so record it on the S3 side.
    done_signal = write_done_signal(engine, schema, result, tables)

    elapsed = time.time() - started
    logger.info("[%s.%s] 복제 완료 · %.0f초 · %s바이트 → %s",
                engine, schema, elapsed,
                f"{result.get('옮긴바이트', 0):,}", done_signal)

    return {
        "engine": engine, "schema": schema,
        "테이블수": len(tables),
        "로컬경로": str(local),
        "대상S3": destination,
        "소요초": round(elapsed, 1),
        "완료신호": done_signal,
        **result,
    }


def copy_all(engines: list[str] | None = None,
             dry_run: bool = False) -> dict:
    """Replicate every schema."""
    started = time.time()
    result = {
        "시작시각": datetime.now().astimezone().isoformat(timespec="seconds"),
        "스키마별": {}, "드라이런": dry_run,
    }

    logger.info("=" * 70)
    logger.info("rclone 복제 시작 (RC API · copy 증분 모드%s)",
                " · 드라이런" if dry_run else "")
    logger.info("=" * 70)

    for engine in (engines or ENGINES):
        source = cfg.get_section("sources", {})[engine]
        for schema in source["schemas"]:
            try:
                result["스키마별"][f"{engine}.{schema}"] = copy_schema(
                    engine, schema, dry_run=dry_run)
            except Exception as exc:            # noqa: BLE001
                logger.error("[%s.%s] 실패: %s", engine, schema, exc)
                result["스키마별"][f"{engine}.{schema}"] = {
                    "engine": engine, "schema": schema,
                    "실패사유": f"{type(exc).__name__}: {exc}",
                }

    # ⚠ Measured: the rclone result dict contains `"오류": 0` (transfer error
    #   count), so judging success with `"오류" in result` classified *success*
    #   as failure. Success/failure is therefore keyed off the exception key.
    ok = [k for k, v in result["스키마별"].items() if "실패사유" not in v]
    ng = [k for k, v in result["스키마별"].items() if "실패사유" in v]
    result["성공"] = len(ok)
    result["실패"] = len(ng)
    result["실패목록"] = ng
    result["종료시각"] = datetime.now().astimezone().isoformat(timespec="seconds")
    result["총소요초"] = round(time.time() - started, 1)

    out_dir = cfg.log_folder()
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"02_rclone_{datetime.now():%Y%m%d_%H%M%S}.json"
    out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2),
                        encoding="utf-8")
    result["증적"] = str(out_path)

    logger.info("=" * 70)
    logger.info("복제 종료 · 성공 %d / 실패 %d · %.0fs",
                result["성공"], result["실패"], result["총소요초"])
    if ng:
        logger.error("실패: %s", ", ".join(ng))
    logger.info("증적: %s", out_path)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="rclone RC API 복제")
    parser.add_argument("--engine", nargs="*", default=None)
    parser.add_argument("--schema", default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if args.schema:
        copy_schema(args.engine[0], args.schema, dry_run=args.dry_run)
    else:
        copy_all(args.engine, dry_run=args.dry_run)
