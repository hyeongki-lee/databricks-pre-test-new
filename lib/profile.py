"""
ETL 프로파일 (스키마 단위 yaml) 관리.

요구사항
--------
  * 엔진별 4개 스키마 → **총 12개의 yaml** (테이블이 아니라 스키마 단위 파일)
  * 대시보드에서 새 테이블이 생기면 기존 yaml 을 갱신하거나 없으면 새로 만든다
  * include / exclude 컬럼 관리
  * 작업 주기 코드화 + 등록일자 + 고정 조건(매월 1일 등)
  * 활성 플래그: Auto Loader 적재가 끝나면 `active: N → Y`

설계 — include / exclude (요구사항 7번이 설계 제안을 요구했다)
-----------------------------------------------------------
    columns         : 등록 시점의 스키마 스냅샷. **형상관리의 기준선**
    include_columns : 화이트리스트. 비어 있으면 `columns` 전체를 쓴다.
    exclude_columns : 항상 빼는 컬럼. include 이후에 적용된다.

왜 이 구조인가
--------------
    ① 컬럼 추가가 자동으로 반영되면 안 된다(요구사항 7번).
       `columns` 에 없는데 파일에 있는 컬럼은 무시하고 Slack 으로 알린다.
    ② 그래서 `columns` 가 사실상 화이트리스트 역할을 한다.
       그러면 `include_columns` 는 **언제 필요한가**가 분명해진다.
       → 컬럼 삭제 후 되살리거나, 일부 컬럼만 임시로 적재하고 싶을 때.
    ③ `exclude_columns` 는 include 와 상충하지 않게 **항상 마지막에** 적용한다.

그래서 ETL 모듈의 컬럼 결정 규칙은 세 단계다.

    1순위  include_columns 가 있으면 → 그 목록만 쓴다
    2순위  없으면                   → columns(기준선) 전체
    3단계  거기서 exclude_columns 를 뺀다
"""
from __future__ import annotations

import logging
from datetime import date, datetime
from pathlib import Path
from typing import Any

import yaml

try:
    from . import config
except ImportError:
    import config                        # type: ignore

logger = logging.getLogger("profile")

#: 작업 주기 코드 (요구사항: 일/주/월/분기/반기/년 코드화)
SCHEDULE_CODES = {
    "daily":         "일간",
    "weekly":        "주간",
    "monthly":       "월간",
    "quarterly":     "분기",
    "semi_annually": "반기",
    "annually":      "년간",
}

#: ETL 처리 종류
LOAD_TYPES = {
    "append":   "증분 적재",
    "truncate": "전체 재적재",
    "merge":    "변동분 병합",
}


# ---------------------------------------------------------------------------
# 경로
# ---------------------------------------------------------------------------

def file_path(engine: str, schema: str) -> Path:
    """스키마 단위 프로파일 yaml 경로."""
    return config.profile_folder() / f"{engine}_{schema}.yaml"


def exists(engine: str, schema: str) -> bool:
    """해당 스키마의 프로파일 yaml 이 있는지."""
    return file_path(engine, schema).exists()


def list_all() -> list[dict]:
    """등록된 프로파일 전체 목록(대시보드·검증용)."""
    folder = config.profile_folder()
    if not folder.exists():
        return []
    result = []
    for path in sorted(folder.glob("*.yaml")):
        try:
            document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        except Exception as exc:                # noqa: BLE001
            logger.error("프로파일 읽기 실패 %s: %s", path.name, exc)
            continue
        result.append({
            "파일": path.name,
            "엔진": document.get("engine", ""),
            "스키마": document.get("schema", ""),
            "테이블수": len(document.get("tables", {})),
            "활성수": sum(1 for t in (document.get("tables", {}).values())
                          if isinstance(t, dict) and t.get("active") == "Y"),
            "등록일자": document.get("registered_at", ""),
            "작업주기": document.get("schedule", {}).get("type", ""),
        })
    return result


# ---------------------------------------------------------------------------
# 읽기 / 쓰기
# ---------------------------------------------------------------------------

def read(engine: str, schema: str) -> dict:
    """프로파일을 읽는다. 없으면 빈 골격으로 시작한다."""
    path = file_path(engine, schema)
    if path.exists():
        return yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return {
        "engine": engine,
        "schema": schema,
        "registered_at": date.today().isoformat(),
        "updated_at": "",
        "schedule": {"type": "daily"},
        "tables": {},
    }


def write(engine: str, schema: str, document: dict) -> Path:
    """프로파일을 UTF-8 으로 저장한다(형상관리 대상이므로 들여쓰기를 고정)."""
    path = file_path(engine, schema)
    path.parent.mkdir(parents=True, exist_ok=True)
    document = dict(document)
    document["updated_at"] = datetime.now().astimezone().isoformat(timespec="seconds")
    path.write_text(
        yaml.safe_dump(document, allow_unicode=True, sort_keys=False,
                       default_flow_style=False, width=100),
        encoding="utf-8")
    return path


def table_entry(engine: str, schema: str, table: str) -> dict:
    """테이블 1건의 프로파일 정의를 꺼낸다."""
    return read(engine, schema).get("tables", {}).get(table, {}) or {}


# ---------------------------------------------------------------------------
# 생성 / 갱신 (요구사항 대시보드 8번)
# ---------------------------------------------------------------------------

def register_table(engine: str, schema: str, table: str, *,
                   columns: list[str],
                   etl_type: str = "append",
                   primary_key: str | None = None,
                   include_columns: list[str] | None = None,
                   exclude_columns: list[str] | None = None,
                   deidentification: dict[str, str] | None = None,
                   schedule: dict | None = None,
                   registered_at: str | None = None,
                   active: str = "N") -> dict:
    """테이블을 프로파일에 등록한다.

    yaml 이 있으면 갱신하고, 없으면 스키마 단위로 새로 만든다.
    """
    if etl_type not in LOAD_TYPES:
        raise ValueError(f"etl_type 은 {list(LOAD_TYPES)} 중 하나여야 합니다: {etl_type}")
    if etl_type == "merge" and not primary_key:
        raise ValueError("merge 는 변동 대상 기본키(pk 컬럼)가 반드시 필요합니다.")

    document = read(engine, schema)
    tables = document.setdefault("tables", {})
    existing = tables.get(table) or {}

    entry = {
        # 형상관리 기준선. 컬럼 추가는 여기 반영되지 않는다.
        "columns": list(columns),
        "include_columns": list(include_columns or []),
        "exclude_columns": list(exclude_columns or []),
        "deidentification": dict(deidentification or {}),
        "etl_type": etl_type,
        "primary_key": primary_key or "",
        # 작업 주기(코드화) + 등록일자. 주기 판정은 실행 시점에 한다.
        "schedule": dict(schedule or {"type": "daily"}),
        "registered_at": registered_at or date.today().isoformat(),
        # 적재 완료를 기다리는 동안 N. Auto Loader 가 끝나면 Y 로 바꾼다.
        "active": active,
    }
    # 사용자가 이미 값을 넣어 둔 항목은 덮어쓰지 않는다.
    for key, value in entry.items():
        if key in ("columns", "include_columns", "exclude_columns",
                   "deidentification", "registered_at", "active") \
                and existing.get(key) not in (None, "", [], {}) and not value:
            entry[key] = existing[key]

    tables[table] = entry
    path = write(engine, schema, document)
    logger.info("테이블 등록: %s.%s.%s (%s) → %s",
                engine, schema, table, etl_type, path.name)
    return entry


def set_active_flag(engine: str, schema: str, table: str, active: str = "Y") -> bool:
    """활성 플래그를 바꾼다(Auto Loader 적재 완료 → 'Y').

    ⚠ 형상관리 관점에서 이 파일은 **호스트**에 있는 것이 원본이다.
      컨테이너에서 바꿔도 마운트 덕에 호스트 파일에 반영되지만,
      안전하게 하려면 호스트 스크립트나 git 커밋을 거치는 편이 낫다.
      여기서는 마운트가 rw 인 전제로 바로 쓴다.
    """
    document = read(engine, schema)
    tables = document.get("tables", {})
    if table not in tables:
        logger.warning("프로파일에 없는 테이블: %s.%s.%s", engine, schema, table)
        return False
    tables[table]["active"] = active
    write(engine, schema, document)
    logger.info("활성 플래그 변경: %s.%s.%s → %s", engine, schema, table, active)
    return True


def set_schema_active(engine: str, schema: str, active: str = "Y",
                      table_names: list[str] | None = None) -> int:
    """스키마 단위로 여러 테이블의 플래그를 한 번에 바꾼다."""
    document = read(engine, schema)
    tables = document.get("tables", {})
    targets = table_names if table_names is not None else list(tables.keys())
    changed = 0
    for table in targets:
        if table in tables:
            tables[table]["active"] = active
            changed += 1
    write(engine, schema, document)
    return changed


# ---------------------------------------------------------------------------
# 컬럼 결정 (ETL 모듈이 쓰는 유일한 규칙)
# ---------------------------------------------------------------------------

def resolve_load_columns(entry: dict, source_columns: list[str]) -> dict:
    """실제로 적재할 컬럼과, 처리 방식을 함께 결정한다.

    Returns:
        {
          "load_columns": [...],        # 실제로 읽어들일 컬럼
          "basis": "...",
          "excluded_columns": [...],      # 사유와 함께
          "added_columns": [...],        # 원천에만 있음 → 반영 안 함
          "dropped_columns": [...],        # 프로파일에만 있음 → NULL 대체
          "reasons": {컬럼: 사유},
        }
    """
    baseline = list(entry.get("columns") or [])
    include = list(entry.get("include_columns") or [])
    exclude = list(entry.get("exclude_columns") or [])

    # 1·2단계: 화이트리스트 결정
    if include:
        candidates = [c for c in include if c in baseline or c in source_columns]
        decision_basis = "include_columns"
    else:
        candidates = list(baseline)
        decision_basis = "columns(기준선)"

    # 3단계: exclude 는 항상 마지막
    selected = [c for c in candidates if c not in exclude]

    # 원천에 있는 컬럼 중 프로파일에 없는 것 → 반영 금지
    added = [c for c in source_columns if c not in baseline]
    # 프로파일에 있는 컬럼 중 원천에 없는 것 → NULL 대체로 진행
    removed = [c for c in selected if c not in source_columns]

    # 실제 원천에 존재하는 것만 적재 대상
    actual_columns = [c for c in selected if c in source_columns]

    exclusion_reasons = {}
    for c in include:
        if c in exclude:
            exclusion_reasons[c] = "include 과 exclude 가 동시에 지정됨 → exclude 우선"
    for c in exclude:
        exclusion_reasons[c] = "exclude_columns 에 의해 제외"
    for c in added:
        exclusion_reasons[c] = "원천에만 있는 신규 컬럼 → 개발자 판단 대기(자동 반영 안 함)"
    for c in removed:
        exclusion_reasons[c] = "프로파일에 있으나 원천에 없음 → NULL 로 대체해 계속"

    return {
        "load_columns": actual_columns,
        "basis": decision_basis,
        "excluded_columns": exclude,
        "added_columns": added,
        "dropped_columns": removed,
        "reasons": exclusion_reasons,
    }


# ---------------------------------------------------------------------------
# 작업 주기 판정 (요구사항 대시보드 6번)
# ---------------------------------------------------------------------------

def _weekday(base: date) -> int:
    """월=0 … 일=6"""
    return base.weekday()


def evaluate_schedule(entry: dict, base_date: date | None = None) -> dict:
    """이번 실행 시점에 이 테이블을 돌려야 하는지 판단한다.

    등록일자(registered_at) 이후여야 하고, 등록된 주기 조건에 걸려야 한다.
    고정 조건(`fixed_day`)이 있으면 그것도 함께 본다.

    Returns:
        {"run": bool, "reason": str, "schedule": 코드}
    """
    base_date = base_date or date.today()
    schedule_type = (entry.get("schedule") or {}).get("type", "daily")
    registered = entry.get("registered_at")
    condition = entry.get("schedule") or {}

    # 0) 등록일 이전이면 실행하지 않는다.
    if registered:
        try:
            registered_date = date.fromisoformat(str(registered)[:10])
            if base_date < registered_date:
                return {"run": False,
                        "reason": f"등록일({registered_date}) 이전",
                        "schedule": schedule_type}
        except ValueError:
            logger.warning("registered_at 형식이 잘못됨: %r", registered)

    # 1) 고정 조건이 먼저 걸린다 (예: 매월 1일)
    fixed_day = condition.get("fixed_day")
    if fixed_day:
        try:
            fixed = int(fixed_day)
            if base_date.day != fixed:
                return {"run": False,
                        "reason": f"고정 조건 불일치 ({base_date.day}일 ≠ 매월 {fixed}일)",
                        "schedule": schedule_type}
        except (TypeError, ValueError):
            logger.warning("fixed_day 형식이 잘못됨: %r", fixed_day)

    # 2) 주기 코드 판정
    if schedule_type == "daily":
        passed = True
        reason = "일간 — 매일"
    elif schedule_type == "weekly":
        day_of_week = condition.get("day_of_week", 0)
        passed = (_weekday(base_date) == int(day_of_week))
        reason = f"주간({['월', '화', '수', '목', '금', '토', '일'][int(day_of_week)]})"
    elif schedule_type == "monthly":
        passed = (base_date.day == 1)
        reason = "월간 — 매월 1일"
    elif schedule_type == "quarterly":
        passed = (base_date.day == 1 and base_date.month in (1, 4, 7, 10))
        reason = f"분기 — {base_date.month}월 1일"
    elif schedule_type == "semi_annually":
        passed = (base_date.day == 1 and base_date.month in (1, 7))
        reason = f"반기 — {base_date.month}월 1일"
    elif schedule_type == "annually":
        passed = (base_date.day == 1 and base_date.month == 1)
        reason = "연간 — 매년 1월 1일"
    else:
        passed = True
        reason = f"알 수 없는 주기 '{schedule_type}' — 일간으로 간주"

    return {"run": bool(passed),
            "reason": (reason if passed else f"{reason} 조건 미달"),
            "schedule": schedule_type}


# ---------------------------------------------------------------------------
# 검증용 출력
# ---------------------------------------------------------------------------

def summary(engine: str, schema: str) -> dict:
    """스키마 프로파일 집계(대시보드·검증용)."""
    document = read(engine, schema)
    tables = document.get("tables", {})
    by_type: dict[str, int] = {}
    active_count = 0
    deid: dict[str, int] = {}
    for table, entry in tables.items():
        if not isinstance(entry, dict):
            continue
        by_type[entry.get("etl_type", "?")] = by_type.get(entry.get("etl_type", "?"), 0) + 1
        if entry.get("active") == "Y":
            active_count += 1
        for c in (entry.get("deidentification") or {}):
            deid[c] = deid.get(c, 0) + 1
    return {
        "엔진": engine, "스키마": schema,
        "테이블수": len(tables),
        "처리종류별": by_type,
        "활성": active_count, "비활성": len(tables) - active_count,
        "비식별화분포": deid,
        "작업주기": document.get("schedule", {}).get("type", ""),
        "등록일자": document.get("registered_at", ""),
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    items = list_all()
    if not items:
        print("등록된 프로파일이 없습니다. scripts/01_make_profiles.py 를 먼저 실행하십시오.")
    else:
        print(f"등록 프로파일 {len(items)}개")
        for item in items:
            print(f"  {item['파일']:42s} 테이블 {item['테이블수']:2d}개 "
                  f"활성 {item['활성수']:2d}개  주기={item['작업주기']}")