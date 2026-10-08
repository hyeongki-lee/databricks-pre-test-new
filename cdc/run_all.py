"""
CDC 시나리오 일괄 실행 (3개 엔진 전수 검증).

한 스키마씩 engine/schema/table 을 순회하며
    ① 스냅샷 재생성 → ② CDC 시뮬레이션 → ③ rclone 복제 → ④ CDC 적재·검증
을 수행하고, 결과를 하나의 JSON 으로 모아 매뉴얼의 증적으로 쓴다.

왜 엔진별로 따로 도는지
----------------------
MySQL·PostgreSQL·MongoDB 는 같은 논리 경로를 쓰지만 드라이버가 반환하는 타입이
다르다(Decimal / float, int / bool, tuple / dict). 그래서 "한 엔진에서 됐으니
다 된 것"으로 넘길 수 없다. 세 엔진 모두에서 4단계 검증이 통과해야
"This works" 라고 말할 근거가 생긴다.
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

for _s in (sys.stdout, sys.stderr):
    try:
        _s.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass

from lib import config as cfg                  # noqa: E402

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)-5s %(message)s",
                    datefmt="%H:%M:%S", stream=sys.stdout, force=True)
logger = logging.getLogger("cdc-all")


def _load(path: str, name: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


snapshot = _load("cdc/rebuild_snapshot.py", "rebuild_snapshot")
simulate = _load("cdc/simulate.py", "cdc_simulate")
applycdc = _load("cdc/apply_cdc.py", "cdc_apply")
rclone_step = _load("data-prep/rclone_step.py", "rclone_step")


def run_one(engine: str, schema: str, table: str, *,
            update: int = 10, delete: int = 10, insert: int = 10) -> dict:
    """Full CDC cycle for a single table."""
    key = f"{engine}.{schema}.{table}"
    logger.info("=" * 70)
    logger.info("CDC 전수 실행 — %s", key)
    logger.info("=" * 70)

    started = time.time()
    step: dict = {}

    # ① baseline snapshot
    try:
        snap = snapshot.rebuild(engine, schema, table)
        step["스냅샷"] = {"건수": snap["total_rows"], "파일수": snap["file_count"]}
    except Exception as exc:                    # noqa: BLE001
        return {"대상": key, "오류": f"스냅샷 실패: {type(exc).__name__}: {exc}"}

    # ② CDC simulation (mutates the source and writes the delta stream)
    try:
        plan = simulate.simulate(engine, schema, table,
                                 update_count=update, delete_count=delete,
                                 insert_count=insert, version=2)
        step["시뮬레이션"] = {
            "변경전": plan["before_count"], "변경후": plan["after_count"],
            "갱신": len(plan["update_ids"]), "삭제": len(plan["delete_ids"]),
            "신규": len(plan["insert_ids"]),
            "델타행수": len(plan["delta_rows"]),
            "계획파일": plan["plan_file"],
        }
    except Exception as exc:                    # noqa: BLE001
        return {"대상": key, "스텝": step,
                "오류": f"시뮬레이션 실패: {type(exc).__name__}: {exc}"}

    # ③ replicate
    try:
        copied = rclone_step.copy_schema(engine, schema)
        step["복제"] = {"테이블수": copied.get("테이블수"),
                        "jobid": copied.get("jobid"),
                        "소요초": copied.get("소요초")}
    except Exception as exc:                    # noqa: BLE001
        return {"대상": key, "스텝": step,
                "오류": f"복제 실패: {type(exc).__name__}: {exc}"}

    # ④ apply + verify
    try:
        report = applycdc.run(engine, schema, table,
                              plan["plan_file"], reset=True)
        step["검증"] = report["검증"]
        step["MERGE_sql"] = report["적용"]["sql"]
        step["소요초"] = report["적용"]["duration_sec"]
    except Exception as exc:                    # noqa: BLE001
        return {"대상": key, "스텝": step,
                "오류": f"적재 실패: {type(exc).__name__}: {exc}"}

    verdict = step["검증"]["전체판정"]
    logger.info("[%s] 판정: %s (%.0fs)", key, verdict, time.time() - started)

    return {"대상": key, "판정": verdict, "스텝": step,
            "총소요초": round(time.time() - started, 1)}


def run_all(targets: list[tuple[str, str, str]] | None = None,
            update: int = 10, delete: int = 10, insert: int = 10) -> dict:
    """Run the CDC cycle across several tables."""
    if targets is None:
        targets = [
            ("mysql", "mysql_schema_4", "table_1"),
            ("mongodb", "mongo_schema_4", "table_1"),
            ("postgresql", "postgres_schema_4", "table_1"),
        ]

    started = time.time()
    results = []
    for engine, schema, table in targets:
        results.append(run_one(engine, schema, table,
                               update=update, delete=delete, insert=insert))

    passed = [r for r in results if r.get("판정") == "통과"]
    failed = [r for r in results if r.get("판정") != "통과"]

    summary = {
        "실행시각": datetime.now().astimezone().isoformat(timespec="seconds"),
        "대상수": len(results),
        "통과": len(passed),
        "실패": len(failed),
        "전체판정": "통과" if not failed else "실패",
        "총소요초": round(time.time() - started, 1),
        "결과": results,
    }

    out_dir = cfg.log_folder() / "cdc"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "cdc_all_result.json"
    out_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2,
                                   default=str), encoding="utf-8")
    summary["증적"] = str(out_path)

    logger.info("=" * 70)
    logger.info("CDC 전수 결과 · 통과 %d / 실패 %d · %.0fs",
                len(passed), len(failed), summary["총소요초"])
    for r in results:
        logger.info("  %-40s %s", r.get("대상"),
                    r.get("판정") or r.get("오류", "?")[:60])
    logger.info("증적: %s", out_path)
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CDC 전수 실행 (3엔진)")
    parser.add_argument("--갱신", type=int, default=10)
    parser.add_argument("--삭제", type=int, default=10)
    parser.add_argument("--신규", type=int, default=10)
    args = parser.parse_args()

    result = run_all(update=args.갱신, delete=args.삭제, insert=args.신규)
    raise SystemExit(0 if result["전체판정"] == "통과" else 1)
