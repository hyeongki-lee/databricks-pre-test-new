"""
원천 스냅샷 재생성 — CDC 기준선 복구용.

왜 필요한가
------------
CDC 검증은 "알려진 시작 상태"에서만 의미가 있다. ETL 의 `append` 는 설계상
행을 누적하므로, 몇 번 실행하고 나면 200건 원천에 수만 건의 대상이 쌓이고
같은 id 가 반복된다. 그 상태에서 CDC 를 검증하면 **엉뚱한 이유로** 실패한다.

그래서 CDC 전에 대상을 원천 스냅샷과 정확히 일치시킨다.

폴더 분리 (아키텍처)
--------------------
    <table>/data/    ← 전체 스냅샷 (기준선). Auto Loader 가 읽는다.
    <table>/delta/   ← CDC 변경 스트림. MERGE 가 읽는다.

두 폴더를 섞으면 목표 reset 이 "변경 자체"로 덮여 기준선이 무의미해진다.
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

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
logger = logging.getLogger("snapshot")

COLUMNS = ["id", "name", "email", "phone", "address", "age", "salary",
           "created_at", "updated_at", "is_active", "description"]


def rebuild(engine: str, schema: str, table: str) -> dict:
    """Read the whole source table and rewrite the `data/` snapshot."""
    rows = sources_mod.read_rows(engine, schema, table, COLUMNS)
    logger.info("[%s.%s.%s] 원천 %s건 조회", engine, schema, table, f"{len(rows):,}")

    folder = cfg.local_data_root() / engine / schema / table
    result = iceberg_mod.write_table(
        folder, engine=engine, schema=schema, table=table,
        rows=rows, version=1, rows_per_file=20000)

    logger.info("[%s.%s.%s] 스냅샷 재생성: 파일 %d개, %s건, %sKB → %s",
                engine, schema, table, result["file_count"],
                f"{result['total_rows']:,}",
                f"{result['total_size'] // 1024:,}", folder)
    return result


def rebuild_all(engines: list[str] | None = None) -> dict:
    """Rebuild every source snapshot."""
    sources = cfg.get_section("sources", {})
    results: dict = {}
    total = 0

    for engine in (engines or list(sources.keys())):
        for schema in sources[engine]["schemas"]:
            for i in range(1, int(sources[engine].get("tables_per_schema", 5)) + 1):
                table = f"table_{i}"
                key = f"{engine}.{schema}.{table}"
                try:
                    r = rebuild(engine, schema, table)
                    results[key] = {"건수": r["total_rows"],
                                    "파일수": r["file_count"]}
                    total += r["total_rows"]
                except Exception as exc:        # noqa: BLE001
                    results[key] = {"오류": f"{type(exc).__name__}: {exc}"}
                    logger.error("%s 실패: %s", key, exc)

    logger.info("=" * 66)
    logger.info("스냅샷 재생성 완료 · 총 %s건", f"{total:,}")
    logger.info("=" * 66)
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="원천 스냅샷 재생성")
    parser.add_argument("--engine")
    parser.add_argument("--schema")
    parser.add_argument("--table", default="table_1")
    parser.add_argument("--all", action="store_true")
    args = parser.parse_args()

    if args.all or not (args.engine and args.schema):
        rebuild_all([args.engine] if args.engine else None)
    else:
        rebuild(args.engine, args.schema, args.table)
