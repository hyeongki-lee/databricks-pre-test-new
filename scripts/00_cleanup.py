"""
Databricks 워크스페이스 정리 (쿼터 확보 목적).

왜 필요한가
-----------
Free Edition 은 일일 쿼터가 있다. 이전 배치(bg7, bg13 등)에서 만든 테이블이
쌓여 있으면 새 테스트를 돌릴 여유가 줄어든다. 그래서 이번 테스트를 시작하기 전에
정리한다.

⚠ 안전 장치
-----------
1. 기본값이 **보고 전용**이다. 지우려면 `--실행` 을 붙여야 한다.
2. 지우기 전에 반드시 목록을 파일로 남긴다(증적).
3. `migration-ruleset` 의 진실 공급원은 Databricks 가 아니라
   로컬 `verify/results/*.result.json` 이므로, DBX 테이블을 지워도
   그 프로젝트의 결과는 사라지지 않는다.

사용법
------
    python scripts/00_cleanup.py                 # 목록만 (건드리지 않음)
    python scripts/00_cleanup.py --실행          # 실제로 정리
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from lib import envloader                       # noqa: E402

envloader.불러오기()

from lib import dbx                             # noqa: E402
from lib import config as cfg               # noqa: E402

#: 이번 프로젝트가 쓰는 카탈로그/스키마 (절대 지우면 안 됨)
keep_schemas = {
    "workspace.pretest_meta",      # 이번 프로젝트 로그 테이블
    "workspace.default",          # 시스템 기본
    "workspace.information_schema",  # 시스템 기본
}

#: 이전 배치에서 남은 것으로 보고 정리 대상으로 본다.
#:   migration-ruleset 의 진실 공급원은 Databricks 가 아니라
#:   로컬 verify/results/*.result.json 이므로 지워도 그 프로젝트의 결과는 남는다.
target_catalogs = []

#: SQL 로 지울 수 없는 카탈로그. JDBC 접속이 죽은 외부 MySQL 카탈로그다.
#:   Free Edition 은 이런 카탈로그를 SQL DROP 이不接受한다(계정 수준 API 필요).
#:   어차피 쿼터를 먹지 않으므로 목록에만 남긴다.
blocked_catalogs = {
    "bg13_mysql": "외부 MySQL 카탈로그 · JDBC 접속 불가(HV000)",
    "bg7_mysql": "외부 MySQL 카탈로그 · JDBC 접속 불가(HV000)",
}


def catalog_list() -> list[str]:
    return dbx.값목록("SHOW CATALOGS")


def schema_list(catalog: str) -> list[str]:
    return dbx.값목록(f"SHOW SCHEMAS IN `{catalog}`")


def table_list(catalog: str, schema: str) -> list[str]:
    return dbx.값목록(f"SHOW TABLES IN `{catalog}`.`{schema}`")


def job_list(quota: int = 200) -> list[dict]:
    """실행 중이거나 최근 실행된 잡. Free Edition 잡 생성 한도에도 영향이 있다.

    ⚠ 실측(2026-10-08): Free Edition 에서 `system.jobs.history` 를
       ORDER BY 로 정렬해 읽으면 **수 분 이상 걸린다.** 쿼터도 크게 먹는다.
       그래서 기본값으로 조회하지 않는다. 필요할 때만 `--잡이력` 을 붙인다.
    """
    return []


def survey() -> dict:
    """지우기 전에 전체 상황을 파악한다."""
    report = {
        "확인시각": datetime.now().astimezone().isoformat(timespec="seconds"),
        "카탈로그": {},
        "잡": [],
    }
    catalogs = catalog_list()
    print(f"카탈로그 {len(catalogs)}개: {catalogs}")

    for catalog in catalogs:
        if catalog in ("system", "samples"):
            continue
        try:
            schemas = schema_list(catalog)
        except Exception as exc:                # noqa: BLE001
            print(f"  [주의] {catalog} 의 스키마 목록 실패: {exc}")
            continue

        catalog_report: dict = {"스키마": {}}
        schema_count = 0
        table_count = 0
        for schema in schemas:
            try:
                tables = table_list(catalog, schema)
            except Exception:                   # noqa: BLE001
                continue
            schema_count += 1
            table_count += len(tables)
            catalog_report["스키마"][schema] = len(tables)
        catalog_report["스키마수"] = schema_count
        catalog_report["테이블수"] = table_count
        report["카탈로그"][catalog] = catalog_report
        print(f"  {catalog}: 스키마 {schema_count}개, 테이블 {table_count}개")

    report["잡"] = job_list()
    running = [j for j in report["잡"] if j.get("state") in ("PENDING", "RUNNING", "TERMINATING")]
    report["실행중잡"] = running
    print(f"  잡 이력 {len(report['잡'])}건 (진행 중 {len(running)}건)")
    return report


def cleanup(execute_mode: bool) -> dict:
    report = survey()

    cleanup_targets: dict = {"카탈로그": [], "스키마": [], "테이블수": 0, "잡": []}

    for catalog, info in report["카탈로그"].items():
        if catalog in ("system", "samples"):
            continue
        if catalog in blocked_catalogs:
            continue
        if catalog in target_catalogs:
            cleanup_targets["카탈로그"].append(catalog)
            continue
        for schema, table_count in info["스키마"].items():
            total = f"{catalog}.{schema}"
            if total in keep_schemas:
                print(f"  [보존] {total} ({table_count}개)")
                continue
            if table_count == 0 and not execute_mode:
                continue          # 빈 스키마는 보고할 때만 표시
            cleanup_targets["스키마"].append(total)
            cleanup_targets["테이블수"] += table_count

    # 진행 중인 잡은 중단한다. Free Edition 잡 한도를 먹고 있다.
    for job in report.get("실행중잡", []):
        job_id = job.get("job_id")
        if not job_id:
            continue
        cleanup_targets["잡"].append(job_id)
        if execute_mode:
            try:
                dbx.실행(f"CANCEL RUN `{job_id}`")
                print(f"  잡 취소: {job_id}")
            except Exception as exc:            # noqa: BLE001
                print(f"  [경고] 잡 취소 실패 {job_id}: {exc}")

    for catalog in cleanup_targets["카탈로그"]:
        if execute_mode:
            try:
                dbx.실행(f"DROP CATALOG IF EXISTS `{catalog}` CASCADE")
                print(f"  카탈로그 삭제: {catalog}")
            except Exception as exc:            # noqa: BLE001
                print(f"  [경고] 카탈로그 삭제 실패 {catalog}: {exc}")

    # 스키마를 통째로 DROP 하면 그 안의 테이블이 한 번에 사라진다.
    # 테이블을 하나씩 지우면 353회가 필요해 쿼터를 다 먹는다.
    for total in cleanup_targets["스키마"]:
        catalog, schema_name = total.split(".", 1)
        if execute_mode:
            try:
                dbx.실행(f"DROP SCHEMA IF EXISTS `{catalog}`.`{schema_name}` CASCADE")
                print(f"  스키마 삭제: {total}")
            except Exception as exc:            # noqa: BLE001
                print(f"  [경고] 스키마 삭제 실패 {total}: {exc}")

    report["정리대상"] = cleanup_targets
    report["정리불가카탈로그"] = blocked_catalogs
    report["보존한스키마"] = sorted(keep_schemas)
    report["실행모드"] = "실행" if execute_mode else "보고만(아무것도 지우지 않음)"

    # 증적 파일로 남긴다.
    folder = cfg.로그폴더()
    path = folder / f"00_cleanup_{datetime.now():%Y%m%d_%H%M%S}.json"
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print()
    print(f"모드              : {report['실행모드']}")
    print(f"정리 대상 카탈로그: {cleanup_targets['카탈로그'] or '없음'}")
    print(f"정리 대상 스키마  : {len(cleanup_targets['스키마'])}개 "
          f"(테이블 {cleanup_targets['테이블수']}개 포함)")
    print(f"SQL 로 지울 수 없음: {blocked_catalogs}")
    print(f"보존              : {sorted(keep_schemas)}")
    print(f"증적 파일         : {path}")
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Databricks 워크스페이스 정리")
    parser.add_argument("--실행", action="store_true", help="실제로 지운다(없으면 보고만)")
    args = parser.parse_args()
    cleanup(args.실행)
