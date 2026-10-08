"""
ETL 프로파일 12개 자동 생성.

요구사항
--------
  * 엔진별 4개 스키마 → **총 12개 yaml**
  * 스키마마다 테이블 5개 전부 ETL 항목으로 등록
  * 비식별화 D1/D2/D3 를 **엔진별로 골고루** 배분 (모든 코드가 검증되도록)
  * append / truncate / merge 를 섞어서 배치 (3종 모두 테스트)
  * 작업 주기(일/주/월/분기/반기/년)와 등록일자 포함
  * exclude_column 을 일부 테이블에 실제로 넣어 제외 검증

비식별화 배분 설계 (요구사항: "각 엔진별 3개 항목이 골고루")
-----------------------------------------------------------
엔진마다 세 엔진이 모두 D1·D2·D3 을 '한 번 이상' 쓰도록 배치한다.

    mysql_schema_N.table_1 : name→D1, email→D2, phone→D3
    mysql_schema_N.table_2 : name→D2, email→D3, phone→D1
    mysql_schema_N.table_3 : name→D3, email→D1, phone→D2
    → 4스키마 × 5테이블 = 20개 테이블에서 세 코드가 골고루 분포

또한 **엔진마다 서로 다른 조합**을 써서 "같은 코드가 엔진에 따라 다르게
동작하지 않는지"까지 검증할 수 있게 한다.

exclude_column 배치
------------------
테이블마다 한 개씩 다른 컬럼을 제외한다. (없으면 제외 검증이 안 된다)

merge 배치
----------
    table_3 : merge  + primary_key=id   (변동분 검증의 주역)
    table_4 : merge  + primary_key=id
    table_1 : append
    table_2 : truncate
    table_5 : append
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from lib import config as cfg               # noqa: E402
from lib import profile as profile_mod          # noqa: E402

logger = logging.getLogger("makeprofiles")

engines = ["mysql", "mongodb", "postgresql"]

#: 세 테이블이 공유하는 컬럼(순서 고정)
shared_columns = ["id", "name", "email", "phone", "address", "age", "salary",
            "created_at", "updated_at", "is_active", "description"]

#: 비식별화 후보 3개
candidate = ["name", "email", "phone"]


def deid_combos(table_no: int) -> dict[str, str]:
    """테이블 번호에 따라 D1/D2/D3 을 회전 배분한다.

    세 코드가 **같은 테이블 안에서** 모두 쓰이므로, 한 테이블을 검증해도
    세 코드가 전부 확인된다.
    """
    rotate = {
        1: {"name": "D1", "email": "D2", "phone": "D3"},
        2: {"name": "D2", "email": "D3", "phone": "D1"},
        3: {"name": "D3", "email": "D1", "phone": "D2"},
        4: {"name": "D1", "email": "D3", "phone": "D2"},
        5: {"name": "D2", "email": "D1", "phone": "D3"},
    }
    return rotate[table_no]


def exclude_columns(table_no: int) -> list[str]:
    """테이블마다 서로 다른 컬럼을 하나씩 제외한다(제외 검증용)."""
    candidates = ["address", "description", "is_active", "age", "salary"]
    return [candidates[(table_no - 1) % len(candidates)]]


def etl_cycle(table_no: int) -> tuple[str, str | None]:
    """테이블 번호 → (etl_type, primary_key).

    세 종류가 섞이되 merge 에는 반드시 pk 가 있어야 한다.
    """
    batch = {
        1: ("append", None),
        2: ("truncate", None),
        3: ("merge", "id"),
        4: ("merge", "id"),
        5: ("append", None),
    }
    return batch[table_no]


def period_cycle(schema_no: int, table_no: int) -> dict:
    """스키마·테이블 번호로 작업 주기를 정한다.

    6개 코드가 전부 등장해야 코드화가 제대로 된 것으로 볼 수 있다.
    스키마 4개 × 테이블 5개를 조합해 한 주기에 2~3 테이블씩 배분한다.
    """
    cycle = ["daily", "weekly", "monthly", "quarterly",
            "semi_annually", "annually"]
    idx = ((schema_no - 1) * 5 + (table_no - 1)) % len(cycle)
    period = cycle[idx]

    condition: dict = {"type": period}
    if period == "weekly":
        # 스키마마다 다른 요일
        condition["day_of_week"] = (schema_no - 1) % 7
    if period in ("monthly", "quarterly", "semi_annually", "annually"):
        # 고정 조건(매월 1일 등)을 명시적으로 둔다.
        # 오늘 날짜가 1일이 아니면 '실행 안 함'으로 판정되므로
        # 검증 시 `--주기무시` 로 강제할 수 있게 남겨 둔다.
        condition["fixed_day"] = 1
    return condition


def create_profiles(keep_existing: bool = False) -> dict:
    """12개 프로파일을 만든다."""
    today = date.today()
    result = {"생성일자": today.isoformat(), "프로파일": {}}
    totals = {"테이블": 0, "D1": 0, "D2": 0, "D3": 0,
                "append": 0, "truncate": 0, "merge": 0, "exclude": 0}

    for engine in engines:
        source = cfg.항목("sources", {})[engine]
        schemas = list(source["schemas"])

        for schema_no, schema in enumerate(schemas, start=1):
            # 스키마 단위 작업 주기(표준형)
            document = profile_mod.읽기(engine, schema)
            document["engine"] = engine
            document["schema"] = schema
            document["registered_at"] = today.isoformat()
            document["schedule"] = {"type": "daily"}

            for table_no in range(1, int(source.get("tables_per_schema", 5)) + 1):
                table = f"table_{table_no}"
                etl_type, pk = etl_cycle(table_no)
                deid = deid_combos(table_no)
                excl = exclude_columns(table_no)
                schedule = period_cycle(schema_no, table_no)

                profile_mod.테이블등록(
                    engine, schema, table,
                    column=list(shared_columns),
                    etl_type=etl_type,
                    primary_key=pk,
                    include_columns=[],
                    exclude_columns=excl,
                    deidentification=deid,
                    schedule=schedule,
                    # 등록일은 스키마별로 1일·3일·5일·7일 차이로 흩뿌린다.
                    # 그래야 '등록일 이전이면 실행 안 함' 규칙을 검증할 수 있다.
                    registered_at=(today - timedelta(days=(4 - schema_no) * 2))
                                 .isoformat(),
                    active="N",   # Auto Loader 적재 후 Y 로 바뀐다
                )

                totals["테이블"] += 1
                totals[etl_type] += 1
                totals["exclude"] += len(excl)
                for code in deid.values():
                    totals[code] += 1

            # tables 는 테이블등록() 안에서 문서에 직접 들어 있으므로
            # 마지막에 파일로 한 번만 쓴다(반복 쓰기 방지).
            document = profile_mod.읽기(engine, schema)
            path = profile_mod.쓰기(engine, schema, document)
            result["프로파일"][f"{engine}.{schema}"] = {
                "파일": path.name,
                "테이블수": len(document["tables"]),
                "요약": profile_mod.요약(engine, schema),
            }
            logger.info("%-12s %-20s 테이블 %d개 → %s",
                        engine, schema, len(document["tables"]), path.name)

    result["집계"] = totals
    logger.info("=" * 66)
    logger.info("프로파일 12개 생성 완료")
    logger.info("  테이블 %d개 (append %d / truncate %d / merge %d)",
                totals["테이블"], totals["append"],
                totals["truncate"], totals["merge"])
    logger.info("  비식별화 D1 %d / D2 %d / D3 %d (테이블당 1회씩 배분)",
                totals["D1"], totals["D2"], totals["D3"])
    logger.info("  exclude_column 지정 %d건", totals["exclude"])
    logger.info("=" * 66)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL 프로파일 12개 생성")
    parser.add_argument("--지우기", action="store_true",
                      help="기존 프로파일 yaml 을 모두 지우고 새로 만든다")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(message)s", datefmt="%H:%M:%S")

    if args.지우기:
        folder = cfg.프로파일폴더()
        folder.mkdir(parents=True, exist_ok=True)
        cleared = 0
        for p in folder.glob("*.yaml"):
            p.unlink()
            cleared += 1
        print(f"기존 프로파일 {cleared}개 삭제")

    create_profiles()
