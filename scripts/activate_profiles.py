"""
프로파일 활성 플래그 일괄 변경 (Auto Loader 적재 → 정규작업 전환).

요구사항 (파일 데이터 처리 7번)
------------------------------
"파일 적재가 완료되면, 이후에는 ETL 모듈이 참조하는 profile yaml/json 에 해당
테이블의 활성화 flag를 Y로 지정하여 다음 작업부터는 append/merge/truncate 등
등록된 속성별로 작업되도록 초기적재 → 정규작업이 자연스럽게 이루어지도록 구성"

이 스크립트가 그 전환을 담당한다.

왜 별도 스크립트인가
-------------------
Auto Loader DAG 안에서 바로 바꾸면 되지만, 수동 실행과 DAG 실행을 분리하면
"초기 이관이 끝났는지" 와 "정규 작업을 시작할지" 를 사람이 판단할 수 있다.
테스트에서는 이 분리를 그대로 사용한다.
"""
from __future__ import annotations

import argparse
import logging
import sys
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
from lib import profile as profile_mod         # noqa: E402

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)-5s %(message)s",
                    datefmt="%H:%M:%S",
                    stream=sys.stdout, force=True)


def run(flag: str = "Y", engines: list[str] | None = None) -> dict:
    """Set the active flag across every profile."""
    sources = cfg.get_section("sources", {})
    engine_list = engines or list(sources.keys())

    total = 0
    detail: dict = {}

    for engine in engine_list:
        engine_total = 0
        for schema in sources[engine]["schemas"]:
            changed = profile_mod.set_schema_active(engine, schema, flag)
            engine_total += changed
            detail[f"{engine}.{schema}"] = changed
            print(f"  {engine:12s} {schema:20s} {changed}개 → {flag}")
        total += engine_total
        print(f"  → {engine} 소계 {engine_total}개")

    print()
    print(f"총 {total}개 테이블의 active 플래그를 '{flag}' 로 변경했습니다.")
    return {"total": total, "detail": detail}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="프로파일 활성 플래그 일괄 변경")
    parser.add_argument("--flag", default="Y", choices=["Y", "N"])
    parser.add_argument("--engine", nargs="*", default=None)
    args = parser.parse_args()

    run(flag=args.flag, engines=args.engine)
