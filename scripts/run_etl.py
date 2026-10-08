"""
ETL 실행기 (watchdog 으로 감시하기 위한 진입점).

왜 스크립트 파일인가
-------------------
PowerShell 인라인 `python -c "..."` 는 따옴표와 한글이 깨진다.
프로젝트 메모에도 적어 둔 결함이다. 그래서 긴 명령은 전부 파일로 만든다.

사용법
------
    python scripts/run_etl.py --engine mysql --schema mysql_schema_1 --workers 3
    python scripts/run_etl.py --all --workers 3          # 12스키마 전부
    python scripts/run_etl.py --all --workers 3 --no-force   # 주기 조건 무시 X
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import logging
import sys
import time
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
from lib import slack as slack_mod             # noqa: E402

logger = logging.getLogger("run_etl")


def load_processor():
    """etl-module/processor.py 를 모듈로 적재한다 (하이픈 때문에 직접 로드 필요)."""
    spec = importlib.util.spec_from_file_location(
        "processor", ROOT / "etl-module" / "processor.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main() -> int:
    parser = argparse.ArgumentParser(description="ETL 실행기")
    parser.add_argument("--engine")
    parser.add_argument("--schema")
    parser.add_argument("--table")
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--dag-id", default="")
    parser.add_argument("--no-force", action="store_true",
                        help="active=N 이어도 실행하지 않는다(주기 조건 준수)")
    parser.add_argument("--ignore-schedule", action="store_true",
                        help="작업 주기 조건을 무시하고 강제 실행")
    parser.add_argument("--all", action="store_true", help="12스키마 전부 실행")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-5s %(message)s",
                        datefmt="%H:%M:%S",
                        stream=sys.stdout, force=True)

    processor = load_processor()
    run_id = args.run_id or time.strftime("run_%Y%m%d_%H%M%S")

    # 대상 목록 결정
    targets: list[tuple[str, str]] = []
    if args.all:
        sources = cfg.get_section("sources", {})
        targets = [(e, s) for e in sources for s in sources[e]["schemas"]]
    elif args.engine and args.schema:
        targets = [(args.engine, args.schema)]
    elif args.engine:
        targets = [(args.engine, s)
                   for s in cfg.get_section("sources", {})[args.engine]["schemas"]]
    else:
        parser.error("--engine 과 --schema 을 함께 주거나 --all 을 사용하십시오")

    force = not args.no_force

    logger.info("ETL 실행 시작 · 대상 %d개 · workers=%d · run_id=%s",
                len(targets), args.workers, run_id)

    summaries: list[dict] = []
    for engine, schema in targets:
        if args.table:
            record = processor.process_table(
                engine, schema, args.table, workers=args.workers,
                run_id=run_id, dag_id=args.dag_id, force=force,
                ignore_schedule=args.ignore_schedule)
            summaries.append({"engine": engine, "schema": schema,
                              "results": [record]})
        else:
            summaries.append(processor.process_schema(
                engine, schema, workers=args.workers, run_id=run_id,
                dag_id=args.dag_id, force=force,
                ignore_schedule=args.ignore_schedule))

    # 전체 집계
    total_ok = sum(s.get("succeeded", 0) for s in summaries)
    total_fail = sum(s.get("failed", 0) for s in summaries)
    total_skip = sum(s.get("skipped", 0) for s in summaries)
    total_all = sum(s.get("total", 0) for s in summaries)

    logger.info("=" * 68)
    logger.info("ETL 전체 종료 · 성공 %d / 실패 %d / 건너뜀 %d (합계 %d)",
                total_ok, total_fail, total_skip, total_all)
    logger.info("=" * 68)

    slack_mod.notify(
        "ETL전체요약",
        f"ETL 전체 실행 {'완료' if total_fail == 0 else '부분 실패'} — "
        f"{total_all}개 대상",
        [("run_id", run_id),
         ("대상 스키마 수", len(targets)),
         ("동시 처리 개수", args.workers),
         ("성공 / 실패 / 건너뜀", f"{total_ok} / {total_fail} / {total_skip}"),
         ("활성 강제 실행", "예" if force else "아니오"),
         ("주기 조건 무시", "예" if args.ignore_schedule else "아니오")],
        severity="success" if total_fail == 0 else "error",
        dag_id=args.dag_id)

    # 증적 저장
    out_dir = cfg.log_folder() / "etl"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"etl_{run_id}.json"
    out_path.write_text(
        json.dumps({"run_id": run_id, "workers": args.workers,
                    "targets": len(targets),
                    "succeeded": total_ok, "failed": total_fail,
                    "skipped": total_skip,
                    "summaries": [{k: v for k, v in s.items() if k != "results"}
                                   for s in summaries]},
                   ensure_ascii=False, indent=2, default=str),
        encoding="utf-8")
    logger.info("증적 파일: %s", out_path)

    return 0 if total_fail == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
