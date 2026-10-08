"""
검증 결과 수집기 — 매뉴얼에 넣을 모든 수치를 한 곳에서 수집한다.

왜 별도 모듈인가
---------------
매뉴얼의 숫자를 사람이 옮겨 적으면 반드시 어긋난다. 특히 이 프로젝트는
Free Edition 쿼터 때문에 여러 번 재실행되므로, 수치가 어느 시점의 것인지
모호해지면 안 된다.

이 모듈은 Databricks 에 직접 질의해서 **현재 상태**를 읽는다.
그래서 매뉴얼을 다시 생성해도 항상 그 시점의 진실이 반영된다.

수집 항목
--------
    ① 워크스페이스 정리 결과 (테이블 수 감소)
    ② 데이터 준비 결과 (엔진별 스키마/테이블/건수)
    ③ S3 객체 구성 (chk / parquet / metadata / rclone_done)
    ④ Auto Loader 결과 (load_audit 집계)
    ⑤ ETL 결과 (etl_run_log 집계)
    ⑥ 비식별화 실측값 (원본 → 대상)
    ⑦ 로그 테이블 DDL
    ⑧ 실측 함정 목록
"""
from __future__ import annotations

import json
import logging
import sys
from datetime import datetime
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
from lib import dbx                            # noqa: E402
from lib import logtable                       # noqa: E402
from lib import mask as mask_mod               # noqa: E402
from lib import profile as profile_mod         # noqa: E402
from lib import slack as slack_mod             # noqa: E402
from lib import sources as sources_mod         # noqa: E402

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)-5s %(message)s",
                    datefmt="%H:%M:%S", stream=sys.stdout, force=True)
logger = logging.getLogger("evidence")


def collect() -> dict:
    """Collect every number the manual quotes."""
    data: dict = {
        "수집시각": datetime.now().astimezone().isoformat(timespec="seconds"),
    }

    # ① 워크스페이스
    data["워크스페이스"] = query_workspace()

    # ② 데이터 준비
    data["원천"] = query_sources()

    # ③ S3
    data["S3"] = query_s3()

    # ④⑤ 로그 테이블
    data["로그"] = query_logs()

    # ⑥ 비식별화 실측
    data["비식별화"] = measure_deid()

    # ⑦ 프로파일
    data["프로파일"] = query_profiles()

    # ⑧ Slack
    data["slack"] = slack_mod.sent_summary()

    # 실측 함정
    data["실측함정"] = measured_quirks()

    return data


# ---------------------------------------------------------------------------
# ① 워크스페이스
# ---------------------------------------------------------------------------

def query_workspace() -> dict:
    try:
        catalogs = dbx.fetch_values("SHOW CATALOGS")
        schemas = dbx.fetch_values("SHOW SCHEMAS IN workspace")
        total_tables = 0
        by_schema = {}
        for schema in schemas:
            try:
                tables = dbx.fetch_values(f"SHOW TABLES IN workspace.`{schema}`")
                by_schema[schema] = len(tables)
                total_tables += len(tables)
            except Exception:                   # noqa: BLE001
                by_schema[schema] = 0
        return {
            "카탈로그": catalogs,
            "workspace_스키마": schemas,
            "스키마별_테이블수": by_schema,
            "총테이블": total_tables,
        }
    except Exception as exc:                    # noqa: BLE001
        return {"오류": f"{type(exc).__name__}: {exc}"}


# ---------------------------------------------------------------------------
# ② 원천
# ---------------------------------------------------------------------------

def query_sources() -> dict:
    result: dict = {}
    total_tables = 0
    total_rows = 0
    for engine in sources_mod.ENGINES:
        try:
            schemas = sources_mod.list_schemas(engine)
            detail = {}
            engine_tables = 0
            engine_rows = 0
            for schema in schemas:
                tables = sources_mod.list_tables(engine, schema)
                cols = sources_mod.list_columns(engine, schema, tables[0]) if tables else []
                rows = sum(sources_mod.count_rows(engine, schema, t) for t in tables)
                detail[schema] = {"테이블수": len(tables), "총건수": rows,
                                "컬럼수": len(cols), "컬럼": cols}
                engine_tables += len(tables)
                engine_rows += rows
            result[engine] = {"스키마": detail, "테이블수": engine_tables,
                            "총건수": engine_rows}
            total_tables += engine_tables
            total_rows += engine_rows
        except Exception as exc:                # noqa: BLE001
            result[engine] = {"오류": f"{type(exc).__name__}: {exc}"}
    result["합계"] = {"테이블": total_tables, "건수": total_rows}
    return result


# ---------------------------------------------------------------------------
# ③ S3
# ---------------------------------------------------------------------------

def query_s3() -> dict:
    try:
        import boto3

        s3 = cfg.get_s3_config()
        client = boto3.client("s3", region_name=s3.get("region", "ap-northeast-2"))
        prefix = s3.get("prefix", "pretest")
        keys: list[str] = []
        size_total = 0
        for page in client.get_paginator("list_objects_v2").paginate(
                Bucket=s3["bucket"], Prefix=f"{prefix}/"):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])
                size_total += obj["Size"]

        return {
            "버킷": s3["bucket"],
            "접두": prefix,
            "전체객체": len(keys),
            "총크기": size_total,
            "chk파일": len([k for k in keys if k.endswith(s3.get("chk_file", "_chk.json"))]),
            "parquet": len([k for k in keys if k.endswith(".parquet")]),
            "metadata": len([k for k in keys if "/metadata/" in k]),
            "rclone_done": len([k for k in keys if "rclone_done" in k]),
            "샘플키": keys[:8],
        }
    except Exception as exc:                    # noqa: BLE001
        return {"오류": f"{type(exc).__name__}: {exc}"}


# ---------------------------------------------------------------------------
# ④⑤ 로그 테이블
# ---------------------------------------------------------------------------

def query_logs() -> dict:
    schema = logtable.meta_schema()
    result: dict = {"스키마": schema}

    for table in ("load_audit", "etl_run_log"):
        full = f"{schema}.{table}"
        try:
            if not dbx.table_exists(full):
                result[table] = {"존재": False}
                continue
            result[table] = {
                "존재": True,
                "건수": dbx.fetch_value(f"SELECT COUNT(*) FROM {full}"),
                "컬럼": dbx.describe_columns(full),
            }
        except Exception as exc:                # noqa: BLE001
            result[table] = {"오류": f"{type(exc).__name__}: {exc}"}

    # 현재 검증 규모. load_audit 은 실행을 누적하므로, 200건으로 반복 검증하던
    # 시기의 과거 행이 섞여 있다. 행을 지우지 않고 규모로 범위를 정한다.
    scale = 50000

    # 초기 이관 집계 (현재 규모만)
    try:
        rows = dbx.execute_sql(
            f"SELECT engine, work_type, etl_type, count_match, COUNT(*) AS cnt "
            f"FROM {schema}.load_audit WHERE work_type='initial_load' "
            f"  AND source_count={scale} "
            f"GROUP BY 1,2,3,4 ORDER BY 1,2,3")
        cols = [c["name"] for c in rows["columns"]]
        result["load_audit_집계"] = [dict(zip(cols, r)) for r in rows["rows"]]
    except Exception as exc:                    # noqa: BLE001
        result["load_audit_집계"] = {"오류": f"{type(exc).__name__}: {exc}"}

    # 실행(run_id) 단위 내역 — 스키마마다 1회 실행이므로 12개가 나온다.
    # 이 표를 근거로 "실제로 몇 번 돌렸고 언제였는가" 를 확인할 수 있다.
    try:
        rows = dbx.execute_sql(
            f"SELECT run_id, engine, COUNT(*) AS cnt, "
            f"       SUM(CASE WHEN count_match='Y' THEN 1 ELSE 0 END) AS ok, "
            f"       MIN(started_at) AS started_at "
            f"FROM {schema}.load_audit WHERE work_type='initial_load' "
            f"  AND source_count={scale} "
            f"GROUP BY run_id, engine ORDER BY run_id")
        cols = [c["name"] for c in rows["columns"]]
        result["초기이관_실행목록"] = [dict(zip(cols, r)) for r in rows["rows"]]
    except Exception as exc:                    # noqa: BLE001
        result["초기이관_실행목록"] = {"오류": f"{type(exc).__name__}: {exc}"}

    # 검증 규모. 초기 이관이 파일의 원본 건수를 그대로 기록하므로,
    # 여기 있는 값이 "테이블당 몇 건으로 검증했는가" 의 권위 있는 답이다.
    # 원천 집계는 스키마 단위라서 이 값을 대신할 수 없다.
    try:
        rows = dbx.execute_sql(
            f"SELECT source_count, COUNT(*) AS cnt "
            f"FROM {schema}.load_audit WHERE work_type='initial_load' "
            f"GROUP BY source_count ORDER BY cnt DESC")
        cols = [c["name"] for c in rows["columns"]]
        result["초기이관_규모별"] = [dict(zip(cols, r)) for r in rows["rows"]]
    except Exception as exc:                    # noqa: BLE001
        result["초기이관_규모별"] = {"오류": f"{type(exc).__name__}: {exc}"}

    # ETL 집계
    # 가장 최근 ETL 실행 하나만 집계한다. etl_run_log 도 누적되므로,
    # 전부 합치면 200건 시험 시기의 기록이 섞인다.
    etl_run_id = ""
    try:
        rows = dbx.execute_sql(
            f"SELECT run_id FROM {schema}.etl_run_log "
            "GROUP BY run_id ORDER BY MAX(started_at) DESC LIMIT 1")
        if rows["rows"]:
            etl_run_id = str(rows["rows"][0][0])
    except Exception:                           # noqa: BLE001
        pass

    try:
        rows = dbx.execute_sql(
            # Measured: etl_run_log has no count_match column — that lives in
            # load_audit. Counting only what the table actually declares.
            f"SELECT work_type, etl_type, status_code, COUNT(*) AS cnt, "
            f"       ROUND(AVG(duration_sec), 2) AS avg_sec "
            f"FROM {schema}.etl_run_log WHERE run_id='{etl_run_id}' "
            f"GROUP BY 1,2,3 ORDER BY 1,2,3")
        cols = [c["name"] for c in rows["columns"]]
        result["etl_run_log_집계"] = [dict(zip(cols, r)) for r in rows["rows"]]
        result["etl_기준_run_id"] = etl_run_id
    except Exception as exc:                    # noqa: BLE001
        result["etl_run_log_집계"] = {"오류": f"{type(exc).__name__}: {exc}"}

    # 컬럼 드리프트 기록
    try:
        rows = dbx.execute_sql(
            f"SELECT engine, table_name, column_added, column_deleted "
            f"FROM {schema}.load_audit "
            "WHERE (column_added IS NOT NULL AND column_added <> '') "
            "   OR (column_deleted IS NOT NULL AND column_deleted <> '') LIMIT 20")
        cols = [c["name"] for c in rows["columns"]]
        result["컬럼변경기록"] = [dict(zip(cols, r)) for r in rows["rows"]]
    except Exception:                           # noqa: BLE001
        result["컬럼변경기록"] = []

    return result


# ---------------------------------------------------------------------------
# ⑥ 비식별화 실측
# ---------------------------------------------------------------------------

def measure_deid(engine: str = "mysql", schema: str = "mysql_schema_1",
              table: str = "table_1") -> dict:
    """Compare source values with what actually landed in the target."""
    result: dict = {"비식별화코드": mask_mod.DEID_LABELS,
                  "마스크길이": mask_mod.mask_keep_length()}

    entry = profile_mod.table_entry(engine, schema, table)
    deid = entry.get("deidentification") or {}
    result["프로파일_설정"] = deid
    result["제외컬럼"] = entry.get("exclude_columns") or []
    result["etl_type"] = entry.get("etl_type")

    candidate = [c for c in deid.keys()][:3]
    if not candidate:
        return result

    try:
        source = sources_mod.sample_rows(engine, schema, table,
                                        ["id"] + candidate, 3)
        target = f"{cfg.get_databricks_config()['catalog']}.{engine}.{table}"
        ids = ",".join(str(r["id"]) for r in source)
        rows = dbx.execute_sql(
            f"SELECT id, {', '.join(f'`{c}`' for c in candidate)} "
            f"FROM {target} WHERE id IN ({ids}) ORDER BY id")
        cols = [c["name"] for c in rows["columns"]]
        target_rows = [dict(zip(cols, r)) for r in rows["rows"]]
    except Exception as exc:                    # noqa: BLE001
        result["오류"] = f"{type(exc).__name__}: {exc}"
        return result

    comparison = []
    for src in source:
        row_id = src["id"]
        # 대상은 초기이관 행(평문)과 ETL 행(비식별화)이 함께 있을 수 있다.
        # 비식별화된 값이 존재하는 행을 찾는다.
        tgt = next((t for t in target_rows
                    if t["id"] == row_id and
                    str(t.get(candidate[0]) or "") != str(src.get(candidate[0]) or "")),
                   None)
        item = {"id": row_id}
        for col in candidate:
            original = src.get(col)
            code = deid.get(col)
            python_expected = mask_mod.apply_in_python(original, code) if code else original
            item[col] = {
                "원본": original,
                "코드": code,
                "Python_기대값": python_expected,
                "Databricks_실제값": (tgt.get(col) if tgt else "(ETL 행 없음)"),
            }
            if tgt and code:
                actual = str(tgt.get(col))
                item[col]["일치"] = (
                    actual == "None" and python_expected is None
                ) or (actual == str(python_expected))
        comparison.append(item)

    result["비교"] = comparison
    result["일치여부"] = all(
        v.get("일치", True) for row in comparison for k, v in row.items()
        if isinstance(v, dict)
    )
    return result


# ---------------------------------------------------------------------------
# ⑦ 프로파일
# ---------------------------------------------------------------------------

def query_profiles() -> dict:
    listing = profile_mod.list_all()
    tally = {"테이블": 0, "append": 0, "truncate": 0, "merge": 0,
            "D1": 0, "D2": 0, "D3": 0, "exclude": 0, "활성": 0}
    period_tally: dict = {}

    for engine in cfg.get_section("sources", {}):
        for schema in cfg.get_section("sources", {})[engine]["schemas"]:
            document = profile_mod.read(engine, schema)
            schedule = (document.get("schedule") or {}).get("type", "?")
            for table, entry in (document.get("tables") or {}).items():
                tally["테이블"] += 1
                tally[entry.get("etl_type", "?")] = tally.get(entry.get("etl_type", "?"), 0) + 1
                if entry.get("active") == "Y":
                    tally["활성"] += 1
                tally["exclude"] += len(entry.get("exclude_columns") or [])
                for code in (entry.get("deidentification") or {}).values():
                    tally[code] = tally.get(code, 0) + 1
                table_period = (entry.get("schedule") or {}).get("type", "?")
                period_tally[table_period] = period_tally.get(table_period, 0) + 1

    return {
        "프로파일수": len(listing),
        "집계": tally,
        "주기분포": period_tally,
        "목록": listing,
        "예시": profile_mod.read("mysql", "mysql_schema_1"),
    }


# ---------------------------------------------------------------------------
# ⑧ 실측 함정
# ---------------------------------------------------------------------------
# 이 목록은 매뉴얼의 "실측에서 발견한 함정" 절에 그대로 실린다.
# 모든 항목은 실제로 이 프로젝트에서 겪은 오류다.

def measured_quirks() -> list[dict]:
    return [
        {
            "번호": 1,
            "구분": "rclone API",
            "증상": "POST /sync/copy → HTTP 400 "
                    "`Didn't find key \"srcFs\" in input`",
            "원인": "rclone v1.75 는 `src`·`dst` 가 아니라 "
                    "`srcFs`·`dstFs` 키를 요구한다. 구버전 문서 예시와 다르다.",
            "조치": "요청 본문의 키를 srcFs / dstFs 로 변경.",
        },
        {
            "번호": 2,
            "구분": "rclone API",
            "증상": "복제를 시작했는데 jobid 가 없어 완료 여부를 알 수 없음",
            "원인": "`_async: true` 가 없으면 **동기 실행**이 되어 응답이 `{}` 로 "
                    "빈 객체로 돌아온다.",
            "조치": "`_async: True` 를 넣어 jobid 를 받고, "
                    "/job/status 로 폴링한다. 이것이 '셸이 아닌 API' 라는 "
                    "요구사항의 실질적 이득이다.",
        },
        {
            "번호": 3,
            "구분": "rclone API",
            "증상": "GET /job/status?jobid=13 → HTTP 404 Not Found",
            "원인": "이 엔드포인트는 GET 도 쿼리스트링도 받지 않는다.",
            "조치": "POST + JSON body `{\"jobid\": 13}` 로 호출.",
        },
        {
            "번호": 4,
            "구분": "rclone 설정",
            "증상": "설정한 리모트 `lakehouse-seoul` 이 목록에 없음",
            "원인": "`RCLONE_CONFIG_LAKEHOUSE_SEOUL_TYPE` 로 만드는 리모트 이름은 "
                    "하이픈이 아니라 **밑줄**이 된다.",
            "조치": "리모트 이름을 `lakehouse_seoul` 로 사용.",
        },
        {
            "번호": 5,
            "구분": "AWS S3",
            "증상": "`put_object` 가 성공했는데 S3 목록에 객체가 보이지 않음",
            "원인": "Key 에 **버킷명까지 넣었다.** "
                    "put_object 는 버킷을 Bucket 파라미터로 따로 받는다.",
            "조치": "Key 에는 접두만 넣는다.",
        },
        {
            "번호": 6,
            "구분": "성공/실패 판정",
            "증상": "복제가 실제로는 성공했는데 '실패 12건' 으로 보고됨",
            "원인": "rclone 결과 dict 안에 `\"오류\": 0` 이 들어 있어서 "
                    "`\"오류\" in 결과` 판정이 **성공을 실패로 잡았다.**",
            "조치": "예외 기록 키(`실패사유`)로 판정하도록 분리.",
        },
        {
            "번호": 7,
            "구분": "Databricks SQL",
            "증상": "SHOW TABLES LIKE 'x' IN schema → PARSE_SYNTAX_ERROR",
            "원인": "Spark 문법은 `IN` 이 먼저 온다.",
            "조치": "`SHOW TABLES IN <schema> LIKE '<pattern>'` 로 바꿨고, "
                    "스키마 전체 목록을 한 번 읽어 파이썬 쪽에서 비교한다. "
                    "테이블마다 실패하면 60회가 되므로 배치로 바꾼다.",
        },
        {
            "번호": 8,
            "구분": "Databricks DDL",
            "증상": "CREATE SCHEMA `workspace.mysql` → "
                    "INVALID_NAME_FORMAT (이름에 마침표 불가)",
            "원인": "두 단계 이름을 통째로 역따옴표로 감싸면 **하나의 식별자**로 읽힌다.",
            "조치": "단계별로 감싼다: `workspace`.`mysql`.",
        },
        {
            "번호": 9,
            "구분": "Databricks DDL",
            "증상": "DROP TABLE IF EXISTS 직후 CREATE → "
                    "TABLE_OR_VIEW_ALREADY_EXISTS, SHOW 에도 안 보임",
            "원인": "메타데이터가 즉시 정리되지 않는다.",
            "조치": "`CREATE OR REPLACE` 사용.",
        },
        {
            "번호": 10,
            "구분": "PostgreSQL",
            "증상": "CREATE SCHEMA pg_schema_1 → unacceptable schema name",
            "원인": "`pg_` 접두사는 시스템 스키마용으로 예약되어 있다.",
            "조치": "`postgres_schema_1` 로 변경.",
        },
        {
            "번호": 11,
            "구분": "MongoDB / parquet",
            "증상": "ArrowInvalid: Could not convert ObjectId ... "
                    "did not recognize Python value type",
            "원인": "MongoDB `_id`(ObjectId) 를 parquet 로 바꿀 수 없다.",
            "조치": "MongoDB 문서에는 정수 `id` 를 별도로 두고 `_id` 는 제외. "
                    "덕분에 세 엔진이 같은 컬럼 구성을 갖는다.",
        },
        {
            "번호": 12,
            "구분": "MongoDB",
            "증상": "list_database_names() 결과가 비었거나 IndexError",
            "원인": "관리용 DB 이름을 명시하지 않으면 빈 문자열이 섞인다.",
            "조치": "테스트 접두(`mongo_schema_`)로 필터링.",
        },
        {
            "번호": 13,
            "구분": "ETL 감사 로그",
            "증상": "NOT NULL constraint violated for column: ended_at",
            "원인": "`process_table` 의 조기 반환 경로들이 종료 시각을 "
                    "채우지 않았다.",
            "조치": "`try/finally` 로 종료 시각을 반드시 채우도록 구조 변경.",
        },
        {
            "번호": 14,
            "구분": "Windows 실행 환경",
            "증상": "인라인 `python -c \"...\"` 에서 따옴표·한글이 깨짐",
            "원인": "PowerShell 의 인용 규칙 + cp949 콘솔 인코딩.",
            "조치": "긴 명령은 전부 스크립트 파일로 만들고, "
                    "stdout 을 UTF-8 로 reconfigure 한다.",
        },
        {
            "번호": 15,
            "구분": "쿼터",
            "증상": "system.jobs.history 조회가 수 분 이상 걸림",
            "원인": "Free Edition 에서 정렬된 잡 이력 조회가 매우 느리다.",
            "조치": "기본 조회에서 제외하고, 필요할 때만 명시적으로 요청.",
        },
        {
            "번호": 16,
            "구분": "쿼터",
            "증상": "이전 프로젝트 잔여 테이블 때문에 신규 테스트가 막힘",
            "원인": "Free Edition 은 표 개수와 일일 쿼리 수에 한도가 있다.",
            "조치": "테이블별로 DROP 하지 않고 스키마 단위 CASCADE DROP 으로 "
                    "정리한다(272개를 44회가로 처리).",
        },
        {
            "번호": 17,
            "구분": "Slack",
            "증상": "알림 80건 중 78건이 발송되지 않음 — "
                    "`{\"ok\": false, \"err\": \"웹훅 주소가 없습니다\"}`",
            "원인": "`lib/envloader.py` 가 `lakehouse/.env` 한 곳만 읽는다. "
                    "이 프로젝트 전용 값(특히 SLACK_WEBHOOK_URL)이 거기 없어 "
                    "빈 값으로 남았다. 그리고 알림 함수는 `fail_silently=True` "
                    "가 기본이라 **작업은 성공하는데 알림만 조용히 사라졌다.** "
                    "성공 2건은 우연히 웹훅이 남아 있던 시점의 것이었다.",
            "조치": "① `ENV_PATHS` 목록으로 프로젝트 루트 `.env` 도 읽도록 변경 "
                    "② 프로젝트 `.env` 에 웹훅을 넣어 근본 제거 "
                    "③ 기록에 남은 78건을 재발송하고, 전송 시도가 없었던 "
                    "실패 기록을 성공으로 정정한다.",
        },
        {
            "번호": 18,
            "구분": "Slack",
            "증상": "재발송 후 통계가 '160건 중 82건 성공' 으로 왜곡됨",
            "원인": "같은 알림이 실패 78건 + 성공 78건으로 두 번 기록됐다. "
                    "정정 스크립트가 0건만 대체했다. 키에 `발송시각` 을 "
                    "넣었기 때문이다 — 재발송은 시각이 새로 찍히므로 "
                    "**같은 알림인데도 키가 어긋났다.**",
            "조치": "식별 키를 (종류, 제목, 항목 내용)으로 바꾼다. 시각은 "
                    "재발송마다 달라지므로 매칭에 쓰면 안 된다.",
        },
    ]


if __name__ == "__main__":
    result = collect()
    out = ROOT / "work" / "evidence.json"
    out.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str),
                   encoding="utf-8")
    print(f"수집 완료 → {out}")
    print(f"  워크스페이스 테이블 : {result['워크스페이스'].get('총테이블')}")
    print(f"  원천 테이블/건수   : {result['원천']['합계']}")
    print(f"  S3 객체            : {result['S3'].get('전체객체')}")
    print(f"  load_audit         : {result['로그']['load_audit'].get('건수')}")
    print(f"  etl_run_log        : {result['로그']['etl_run_log'].get('건수')}")
    print(f"  Slack 발송         : {result['slack'].get('총건수')}")
    print(f"  비식별화 일치      : {result['비식별화'].get('일치여부')}")
