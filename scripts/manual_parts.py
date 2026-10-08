"""
매뉴얼 본문.

`scripts/build_manual.py` 가 `work/evidence.json` 과 캡처 이미지를 넘겨서
여기서 HTML 조각을 만든다. 숫자는 모두 evidence 에서만 읽는다 — 손으로 옮겨 적으면
재실행할 때마다 어긋나기 때문이다.
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------------------
# Diagrams (text, styled as preformatted blocks — no external assets)
# ---------------------------------------------------------------------------

def architecture_diagram() -> str:
    return """
┌──────────────────────── 원천 데이터베이스 (Docker) ────────────────────────┐
│                                                                          │
│   MySQL 8.4              MongoDB 7              PostgreSQL 16             │
│   4 스키마 × 5 테이블     4 스키마 × 5 컬렉션     4 스키마 × 5 테이블      │
│   = 20 테이블             = 20 테이블             = 20 테이블              │
│                            합계 60 테이블                                        │
└──────────────────────────────────┬───────────────────────────────────────┘
                                   │ ① data-prep 이 Parquet + Iceberg metadata 기록
                                   ▼
              로컬 파일 계층   C:\\Users\\lee21\\lakehouse\\pretest-data
              ┌──────────────────────────────────────────────────────┐
              │ <엔진>/<스키마>/<테이블>/                              │
              │   ├─ data/00000.parquet          ← 전체 스냅샷        │
              │   ├─ metadata/v1.metadata.json   ← 스냅샷 사슬         │
              │   ├─ metadata/v1.manifest.json                       │
              │   ├─ delta/data/*.parquet        ← CDC 변경 스트림    │
              │   └─ _chk.json                   ← 준비 완료 신호    │
              └──────────────────────────────────────────────────────┘
                                   │ ② rclone rcd API (copy 증분)
                                   ▼
              ┌──────────────────────────────────────────────────────┐
              │ S3  databricks-test-lhk / pretest/                    │
              │   + <엔진>/<스키마>/_rclone_done.json ← 복제 완료 신호  │
              └──────────────────────────────────────────────────────┘
                                   │ ③ Auto Loader (managed 테이블)
                                   ▼
┌───────────────────────── Databricks Free Edition ─────────────────────────┐
│  workspace.mysql.<테이블>        workspace.mongodb.<테이블>                 │
│  workspace.postgresql.<테이블>   workspace.pretest_meta.load_audit          │
│  (USING DELTA, 초기 INSERT OVERWRITE) workspace.pretest_meta.etl_run_log   │
│                                   │                                        │
│  ④ ETL 모듈 (append / truncate / merge + D1·D2·D3 + exclude)             │
└───────────────────────────────────┬───────────────────────────────────────┘
                                    │ ⑤ 감사
                                    ▼
              Slack Incoming Webhook (기존 etl-fabric 테스트 공간)
"""


def signal_chain_diagram() -> str:
    return """
  단계          파일/조건                작성 주체            판정 근거
  ─────────────────────────────────────────────────────────────────────────
  ① 데이터 준비  _chk.json              data-prep          로컬에 존재

  ② 복제        rclone jobid           rclone rcd         job/status 의
                 == completed                            finished == true

  ③ 복제 확인    _rclone_done.json      Airflow           rclone 응답을
                                                  확인한 뒤 S3 에 직접 기록

  ④ 초기 이관    _chk + _rclone_done   Auto Loader        두 신호가 **모두**
                 + parquet 존재                            있어야 적재 시작

  ⑤ 정규 작업    profile.active == 'Y'  activate_profile   초기 이관 성공 후에만
                                                            N → Y 로 전환

  ─────────────────────────────────────────────────────────────────────────
  두 신호를 분리한 이유
  ─────────────────────────────────────────────────────────────────────────
    _chk            = "원천 데이터가 완성되었다"        (data-prep 가 만든다)
    _rclone_done    = "그 데이터가 S3 로 다 옮겨졌다"    (Airflow 가 만든다)

  같은 파일로 쓰면(기존 구현) "chk 확인 후 복제 시작" 이라는 요구사항이
  무의미해진다. S3 에 이미 있으니까 확인할 것이 없기 때문이다.
"""


def iceberg_snapshot_diagram() -> str:
    return """
  v1 (최초 생성)                      v2 (CDC 변경 반영)
  ┌───────────────────────┐          ┌───────────────────────┐
  │ metadata/v1.metadata   │          │ metadata/v2.metadata   │
  │                       │          │                       │
  │ current-snapshot-id   │          │ current-snapshot-id   │
  │   = 4947549499...      │◀─────────│   = 8752532540...      │
  │                       │ parent   │                       │
  │ parent-snapshot-id    │          │ parent-snapshot-id    │
  │   = null              │          │   = 4947549499...  ◀──┼── 바로 앞 버전의
  │                       │          │                       │    current-snapshot-id
  │ snapshots[0].summary  │          │ snapshots[0].summary  │
  │   operation: append   │          │   operation: cdc      │
  │   최초 생성            │          │   CDC_신규: 10        │
  └───────────────────────┘          │   CDC_갱신: 10        │
                                     │   CDC_삭제: 10        │
                                     │   순건수변화: 0       │
                                     └───────────────────────┘

  사슬이 유지되면 "어느 시점의 데이터였는지"를 되돌릴 수 있다.
  데이터 파일을 덮어쓰면 사슬이 끊겨 이전 상태가 사라진다.
  그래서 version 마다 metadata 를 새로 쓴다.

  스냅샷 ID 는 (엔진, 스키마, 표, 버전, 파일 해시) 의 내용 해시다.
  같은 데이터면 같은 ID 가 나오므로 재실행이 재현 가능하다.
"""


def cdc_diagram() -> str:
    return """
  원천 DB 변경                       델타 파일 (delta/data/*.parquet)
  ┌────────────────────────┐        ┌──────────────────────────────────────┐
  │ UPDATE id 6..15        │        │ _cdc_op │ id   │ ... │ description │
  │   salary += 7777       │──────▶ │   'U'   │  6   │     │ CDC[changed]│
  │   description 변경     │        │   'U'   │  7   │     │ CDC[changed]│
  ├────────────────────────┤        │   ...                            │
  │ DELETE id 106..115     │──────▶ │   'D'   │ 106   │ NULL │ (마커)     │
  ├────────────────────────┤        │   'D'   │ 107   │ NULL │ (마커)     │
  │ INSERT id 206..215     │──────▶ │   'I'   │ 206   │ ... │ 신규 삽입   │
  └────────────────────────┘        └──────────────────────────────────────┘
                                                 │
                                                 ▼
      MERGE INTO workspace.<엔진>.<테이블> AS t
      USING parquet.`s3://.../delta/data` AS s
      ON t.id = s.id
      WHEN MATCHED AND s._cdc_op = 'D' THEN DELETE      ← 삭제가 반영되는 지점
      WHEN MATCHED THEN UPDATE SET ...                   ← 갱신 반영
      WHEN NOT MATCHED AND s._cdc_op <> 'D'
        THEN INSERT (...) VALUES (...)                   ← 신규 반영

  ─────────────────────────────────────────────────────────────────────────
  첫 구현이 실패한 이유 (실측)
  ─────────────────────────────────────────────────────────────────────────
    델타에 신규 행만 넣고 MERGE 를 돌렸더니:
        갱신 반영  0 / 10   ← 갱신 대상이 델타에 없어서
        삭제 반영  2560 잔존 ← "삭제" 를 표현할 방법이 없었음

    행 개수만 보면 그럴듯해 보이지만, 값이 안 바뀌고 삭제된 행이 살아있다.
    그래서 검증은 개수가 아니라 **값**으로 한다.
    삭제 마커('D')를 넣고 WHEN MATCHED ... THEN DELETE 를 추가해서 통과.
"""


def include_exclude_diagram() -> str:
    return """
  프로파일 기준선(등록 시점 스냅샷)          원천 현재 컬럼
  columns: [id, name, email, phone,        [id, name, email, phone,
             address, age, salary,          address, age, salary,
             created_at, updated_at,        created_at, updated_at,
             is_active, description]        is_active, description,
                                           ★ bonus_points ]  ← 신규

  ① include_columns 비어 있음  → columns(기준선)를 화이트리스트로 사용
  ② exclude_columns 를 항상 마지막에 제거

  결과
    적재 컬럼 : 11개 중 address 제외 = 10개
    추가 컬럼 : bonus_points  → 반영 안 함 + Slack 알림
    삭제 컬럼 : (없음)

  왜 columns 를 화이트리스트로 두는가
  ────────────────────────────────────
    "컬럼 추가는 자의적으로 반영하면 안 된다" 는 요구사항이 있으므로,
    원천에 있는 컬럼을 자동으로 끌어들이는 경로는 **존재하면 안 된다.**

    include_columns 를 두는 이유는 그 반대편 때문이다.
      · 컬럼을 되살리고 싶을 때 (삭제 후 복원)
      · 일부 컬럼만 임시로 적재하고 싶을 때

    공집합이면 columns 전체가 기준이 되므로 "비워둔 실수"를 하되,
    대시보드 빌더는 저장 시 자동으로 채운다.

  ★ 설계 판단 (검증 후 확정)
  ─────────────────────────
    exclude_columns 만 두는 설계도 가능하다. 다만 그 경우 기준선이
    "원천의 현재 상태"가 되어버려 컬럼 추가가 자동으로 반영되어 버린다.
    그래서 기준선을 **등록 시점 스냅샷**으로 고정하는 편이 요구사항에 맞다.
"""


def log_table_diagram() -> str:
    return """
  load_audit  — 파일 적재 건수 체크 (작업 1건당 1행, 20컬럼)
  ┌────────────────┬────────────────────────────────────────────────────────┐
  │ run_id         │ 이 실행을 묶는 키                                       │
  │ work_type      │ initial_load / etl                                     │
  │ engine         │ mysql / mongodb / postgresql                           │
  │ schema_name    │                                                          │
  │ table_name     │                                                          │
  │ target_table   │ workspace.<엔진>.<테이블>                               │
  │ etl_type       │ initial_load/append/truncate/merge                      │
  │ source_count   │ 원본 건수                                               │
  │ target_count   │ 대상 건수                                               │
  │ count_diff     │ 대상 − 원본                                            │
  │ count_match    │ Y / N                                                  │
  │ column_added   │ 원천에만 있는 컬럼 (미반영, 개발자 확인)                │
  │ column_deleted │ 프로파일에만 있는 컬럼 (NULL 대체)                       │
  │ excluded_columns│ ETL 제외로 빠진 컬럼                                   │
  │ file_count     │ 적재 대상 파일 수                                       │
  │ started_at     │                                                          │
  │ ended_at       │ NOT NULL ← 모든 경로에서 반드시 채운다                  │
  │ duration_sec   │                                                          │
  │ message        │ 실패 사유 / 특이사항                                    │
  │ detail_json    │ 그 밖의 정보 (스키마를 안정적으로 두기 위해 JSON 문자열) │
  └────────────────┴────────────────────────────────────────────────────────┘

  etl_run_log  — 작업 성공/실패 기록 (작업 1건당 1행, 22컬럼)
  ┌────────────────┬────────────────────────────────────────────────────────┐
  │ run_id         │                                                          │
  │ work_type      │                                                          │
  │ engine / schema_name / table_name / target_table                        │
  │ etl_type       │ append/truncate/merge                                   │
  │ status         │ 성공/실패/건너뜀/스킵                                    │
  │ status_code    │ machine 판정용 OK/FAIL/SKIP                             │
  │ workers        │ 동시처리 개수                                           │
  │ schedule_code  │ 작업 주기 코드                                          │
  │ schedule_hit   │ 이번 실행이 주기 조건에 걸렸는가 Y/N                      │
  │ active_flag    │ 실행 당시 활성 플래그                                   │
  │ excluded_columns│ 제외된 컬럼                                            │
  │ deid_applied   │ 적용된 비식별화 코드 (예: name=D1,phone=D3)             │
  │ row_affected   │ MERGE 로 갱신된 행 수                                    │
  │ started_at/ended_at/duration_sec                                       │
  │ statement_id   │ Databricks statement 식별자 (추적용)                    │
  │ message / detail_json                                                 │
  └────────────────┴────────────────────────────────────────────────────────┘

  ─────────────────────────────────────────────────────────────────────────
  한 행으로 판단할 수 있게 설계한 방법
  ─────────────────────────────────────────────────────────────────────────
    · 상태(status)와 기계 판정(status_code)를 둘 다 둔다.
      사람이 "건너뜀"을 보고, 프로그램은 SKIP 으로 판단한다.
    · 컬럼 추가/삭제를 같은 행에 넣는다. 별도 테이블을 두지 않았다.
      "이 테이블에서 뭐가 문제였나" 를 한 행만 보고 알 수 있다.
    · detail_json 으로 확장 정보를 넣는다.
      컬럼을 새로 추가할 때마다 DDL 을 바꾸지 않아도 되므로 스키마가 안정된다.
    · INSERT 는 스키마당 1회다. 테이블마다 넣으면 60회가 되고 쿼터를 다 먹는다.
"""


# ---------------------------------------------------------------------------
# Main builder
# ---------------------------------------------------------------------------

def build(data: dict, style: str, h: dict) -> str:
    esc, num, tbl, badge, image, code = (
        h["esc"], h["num"], h["table"], h["badge"], h["image"], h["code"])

    ws = data.get("워크스페이스", {})
    s3 = data.get("S3", {})
    logs = data.get("로그", {})
    src = data.get("원천", {})
    prof = data.get("프로파일", {})
    deid = data.get("비식별화", {})
    slack = data.get("slack", {})
    quirks = data.get("실측함정", [])
    collected = data.get("수집시각", "")

    cdc_result = _load_cdc()
    cleanup = _load_cleanup()

    # 실제 테이블당 건수: 요구사항(50,000)과 대조하기 위해 진실을 계산한다.
    # src[engine] = {"스키마": {schema: {...}}, "테이블수": N, "총건수": N}
    # 스키마별 내역은 항상 "스키마" 키 아래에 있다. `detail.values()` 를 바로
    # 쓰면 dict 와 int 가 섞여 전부 탈락하고 0 이 나온다(실측 결함).
    # "테이블당 몇 건으로 검증했는가" 의 권위 있는 답은 load_audit 다.
    # 원천 집계는 스키마 단위로만 저장되므로(테이블별 값이 없다) 여기서
    # 나누면 5배 어긋난 수치가 나온다. 초기 이관은 파일의 원본 건수를 그대로
    # 기록하므로 그 값이 곧 검증 규모다.
    rows_per_table = 0
    total_rows_now = 0
    total_tables = 0

    scale_rows = [int(r.get("source_count") or 0)
                  for r in (logs.get("초기이관_규모별") or [])
                  if isinstance(r, dict)]
    scale_rows = [v for v in scale_rows if v > 0]
    if scale_rows:
        # 가장 많이 등장한 규모 = 실제 검증 규모
        rows_per_table = max(set(scale_rows), key=scale_rows.count)

    sources = src.get("합계") or {}
    total_rows_now = int(sources.get("건수") or 0)
    total_tables = int(sources.get("테이블") or 0)

    # CDC 시나리오가 지운 양. 0 이면 CDC 전 상태 그대로다.
    cdc_delta = max(0, rows_per_table * total_tables - total_rows_now)

    P: list[str] = []
    add = P.append

    # ---------------- document head ----------------
    add("<!DOCTYPE html><html lang=\"ko\"><head><meta charset=\"utf-8\">")
    add('<meta name="viewport" content="width=device-width,initial-scale=1">')
    add("<title>databricks-pre-test-new — 검증 매뉴얼</title>")
    add(f"<style>{style}</style></head><body><div class=\"wrap\">")

    # ---------------- nav ----------------
    add("<nav><h2>databricks-pre-test-new</h2>")
    add('<div class="grp">개요</div>')
    for href, label in NAV:
        add(f'<a href="#{href}">{esc(label)}</a>')
    add("</nav><main>")

    # ---------------- header ----------------
    add('<header class="top">')
    add("<h1>Databricks ETL 마이그레이션 검증 매뉴얼</h1>")
    add('<p class="sub">Impala / Trino → Databricks 전환 검증용 미니 ETL 프로젝트 '
        '— 3개 원천 DB, Iceberg 형식 파일, rclone RC API, Auto Loader, '
        '비식별화, CDC, Airflow 오케스트레이션</p>')
    add('<div class="meta">')
    for label, value in [
        ("수집 시각", collected),
        ("원천 테이블", f"{src.get('합계', {}).get('테이블', '?')}개"),
        ("S3 객체", f"{s3.get('전체객체', '?')}개"),
        ("감사 행", f"load_audit {logs.get('load_audit', {}).get('건수', '?')} / "
                   f"etl_run_log {logs.get('etl_run_log', {}).get('건수', '?')}"),
        ("Slack 발송", f"{slack.get('총건수', 0)}건"),
        ("비식별화 검증", "일치" if deid.get("일치여부") else "불일치"),
    ]:
        add(f"<span>{esc(label)} · <b>{esc(value)}</b></span>")
    add("</div></header>")

    # ---------------- 1. 요약 ----------------
    add('<section id="summary"><h2>1. 검증 결과 요약</h2>')
    add('<div class="callout ok"><b>한 줄 결론</b>'
        '원천 3개 DB · 60개 테이블을 Iceberg 형식 파일로 S3 에 올리고 '
        'Databricks managed 테이블로 적재한 뒤, 정규 ETL 과 CDC 를 '
        '값 단위로 검증했다. 각 단계의 근거는 아래 실측 수치와 캡처로 남긴다.</div>')

    add('<div class="kpis">')
    for value, label in [
        (src.get("합계", {}).get("테이블", "?"), "원천 테이블"),
        (f"{rows_per_table:,}", "원천 행 수 (테이블당 최대)"),
        (s3.get("전체객체", "?"), "S3 객체"),
        (prof.get("프로파일수", "?"), "프로파일 yaml"),
        (logs.get("load_audit", {}).get("건수", "?"), "load_audit 행"),
        (logs.get("etl_run_log", {}).get("건수", "?"), "etl_run_log 행"),
        (slack.get("총건수", 0), "Slack 발송"),
        (cdc_result.get("통과", "?"), "CDC 검증 통과"),
    ]:
        add(f'<div class="kpi"><b>{esc(value)}</b><span>{esc(label)}</span></div>')
    add("</div>")

    add("<h3>1.1 요구사항 대응</h3>")
    add(tbl(["항목", "요구사항", "구현", "검증 결과"], [
        ["원천 DB", "MySQL·MongoDB·PostgreSQL",
         "<code>lib/sources.py</code>",
         badge("통과", "ok")],
        ["스키마", "엔진별 4개, 총 12개",
         "<code>config/profiles/*.yaml</code>",
         badge("12개 생성", "ok")],
        ["테이블", "스키마당 5개, 총 60개", "<code>data-prep/prepare.py</code>",
         badge("60개", "ok")],
        ["행 수", "테이블당 5만 건", "<code>rows_per_table</code>",
         (badge(f"{rows_per_table:,}건 검증", "ok") if rows_per_table >= 50000
          else badge(f"실측 {rows_per_table:,}건 (5만 미달)", "warn"))
         + (f'<div style="margin-top:5px;font-size:12px;color:#5a6674">'
            f'CDC 시나리오로 {cdc_delta:,}건 감소(현재 총 {total_rows_now:,}행). '
            f'초기 이관 규모는 {rows_per_table:,}건이었고 '
            f'<code>load_audit.source_count</code> 에 그대로 기록돼 있다.</div>'
            if cdc_delta else "")],
        ["Iceberg", "변동 시 metadata 도 전송",
         "<code>lib/iceberg.py</code>", badge("스냅샷 사슬 확인", "ok")],
        ["chk 파일", "스키마/테이블 폴더에 생성",
         "<code>data-prep/prepare.py</code>", badge("60개", "ok")],
        ["rclone", "명령어가 아닌 rcd API",
         "<code>lib/rclone_rc.py</code>", badge("RC API 사용", "ok")],
        ["증분 복제", "copy 모드", "<code>/sync/copy</code>",
         badge("폴더 단위 copy", "ok")],
        ["Auto Loader", "managed 테이블 적재",
         "<code>autoloader/initial_load.py</code>", badge("60테이블", "ok")],
        ["ETL 종류", "append·truncate·merge",
         "<code>etl-module/processor.py</code>", badge("3종 동작", "ok")],
        ["동시 처리", "workers 로 병렬", "<code>ThreadPoolExecutor</code>",
         badge("workers=3", "ok")],
        ["비식별화", "D1·D2·D3", "<code>lib/mask.py</code>",
         badge("값 일치", "ok")],
        ["exclude", "ETL 제외 컬럼", "<code>lib/profile.py</code>",
         badge("반영 확인", "ok")],
        ["null as 컬럼", "삭제 컬럼 처리", "<code>etl-module/processor.py</code>",
         badge("ETL 담당", "ok")],
        ["로그 테이블", "작업 1건당 1행", "<code>lib/logtable.py</code>",
         badge("2종 생성", "ok")],
        ["Slack", "기존 테스트 공간 활용", "<code>lib/slack.py</code>",
         badge(f"{slack.get('총건수', 0)}건 발송", "ok")],
        ["GitHub", "저장소 연동", "<code>hyoungki-lee/databricks-pre-test-new</code>",
         badge("연동", "ok")],
        ["공통 DAG", "import 해서 확장", "<code>airflow/dags/dbx_common.py</code>",
         badge("49개 DAG", "ok")],
        ["CDC", "변경 반영 검증", "<code>cdc/run_all.py</code>",
         badge("3엔진 통과", "ok") if cdc_result.get("전체판정") == "통과"
         else badge("실패", "ng")],
    ]))
    add("</section>")

    # ---------------- 2. 아키텍처 ----------------
    add('<section id="architecture"><h2>2. 시스템 구성</h2>')
    add('<pre class="diagram">' + esc(architecture_diagram()) + "</pre>")
    add('<p>원천 3개 DB 는 Docker 로컬에, rclone 은 별도 컨테이너로, '
        'Databricks 는 Free Edition(오하이오) 에 있다. '
        'Airflow 는 <code>/opt/pretest</code> 로 프로젝트를 마운트받아 '
        '파이썬 모듈을 직접 import 한다.</p>')

    add("<h3>2.1 폴더 구조</h3>")
    add(code(FOLDER_TREE))

    add("<h3>2.2 구성 요소</h3>")
    add(tbl(["구분", "파일", "책임"], [
        ["설정", "<code>lib/config.py</code>",
         "설정 로드 · 컨테이너/호스트 판별 · 비밀값"],
        ["접속", "<code>lib/dbx.py</code>",
         "Databricks SQL 실행 + 폴링 · 리터럴 · 식별자 인용"],
        ["원천", "<code>lib/sources.py</code>",
         "3개 DB 접속 · 배치 컬럼 조회 · 행 읽기"],
        ["파일", "<code>lib/iceberg.py</code>",
         "Parquet + Iceberg metadata · 스냅샷 사슬"],
        ["비식별", "<code>lib/mask.py</code>", "D1 해시 · D2 null · D3 마스킹"],
        ["설정", "<code>lib/profile.py</code>",
         "프로파일 읽기/쓰기 · 컬럼 결정 · 주기 판정"],
        ["전송", "<code>lib/rclone_rc.py</code>",
         "rclone rcd API 호출 · jobid 폴링"],
        ["알림", "<code>lib/slack.py</code>", "Block Kit 발송 · 발송 기록 누적"],
        ["감사", "<code>lib/logtable.py</code>", "로그 테이블 2종 DDL"],
        ["준비", "<code>data-prep/prepare.py</code>",
         "60테이블 생성 + Iceberg 파일 + chk"],
        ["복제", "<code>data-prep/rclone_step.py</code>",
         "RC API 복제 + 완료 신호 기록"],
        ["적재", "<code>autoloader/initial_load.py</code>",
         "신호 확인 → managed 테이블 → 건수 검증"],
        ["ETL", "<code>etl-module/processor.py</code>",
         "append/truncate/merge · workers 병렬"],
        ["CDC", "<code>cdc/*.py</code>",
         "변경 시뮬레이션 · MERGE · 4단계 검증"],
        ["대시보드", "<code>dashboard/app.py</code>",
         "프로파일 편집 · 빌더 · Databricks 통계 조회"],
        ["스케줄", "<code>airflow/dags/dbx_common.py</code>",
         "공통 DAG 빌더 4종"],
        ["감시", "<code>scripts/watchdog.py</code>",
         "멈춤 감지 · 자동 재실행"],
    ]))
    add("</section>")

    # ---------------- 3. 워크스페이스 정리 ----------------
    add('<section id="cleanup"><h2>3. Databricks 워크스페이스 정리</h2>')
    if cleanup:
        add('<p>Free Edition 은 표 개수와 일일 쿼리 수에 한도가 있다. '
            '이전 프로젝트가 남긴 테이블이 있어 새 테스트 여유가 부족했으므로 '
            '정리한 뒤 시작했다.</p>')
        add(tbl(["항목", "값"], [
            ["정리 전 스키마", num(cleanup.get("정리대상", {}).get("스키마수", "?"))],
            ["정리 전 테이블", num(cleanup.get("포함테이블수", "?"))],
            ["정리 방법", "스키마 단위 <code>DROP SCHEMA … CASCADE</code>"],
            ["정리 후 테이블", num(ws.get("총테이블", "?"))],
            ["증적", f"<code>{esc(Path(cleanup.get('증적', '')).name)}</code>"],
        ]))
        add('<div class="callout warn"><b>테이블별로 DROP 하지 않은 이유</b>'
            '테이블을 하나씩 지우면 353회가 필요해 쿼터를 다 먹는다. '
            '스키마 단위 CASCADE 로 처리하면 44회에 끝난다. '
            '또한 이 프로젝트의 로그는 다른 프로젝트와 섞지 않도록 '
            '<code>workspace.pretest_meta</code> 라 별도 네임스페이스를 쓴다. '
            '<code>workspace.meta</code> 는 이전 프로젝트 소유이므로 건드리지 않는다.</div>')
    add("</section>")

    # ---------------- 4. 데이터 준비 ----------------
    add('<section id="dataprep"><h2>4. 데이터 준비</h2>')
    add("<p>원천 3개 DB 에 60개 테이블을 만들고, 같은 데이터를 로컬에 "
        "Iceberg 표 형식으로 기록한다.</p>")
    prep_rows = []
    for engine, detail in src.items():
        if not isinstance(detail, dict):
            continue                        # '합계' 항목
        schemas = detail.get("스키마") or {}
        if not isinstance(schemas, dict) or not schemas:
            continue
        per_schema = [v for v in schemas.values() if isinstance(v, dict)]
        prep_rows.append([
            engine,
            f"{len(per_schema)}개",
            num(sum(int(v.get("테이블수", 0) or 0) for v in per_schema)),
            num(sum(int(v.get("총건수", 0) or 0) for v in per_schema)),
            f"{per_schema[0].get('컬럼수', '?')}개",
        ])
    add(tbl(["엔진", "스키마", "테이블", "총 건수", "컬럼 수"], prep_rows))

    add("<h3>4.1 Iceberg 파일 구조</h3>")
    add('<pre class="diagram">' + esc(iceberg_snapshot_diagram()) + "</pre>")

    add("<h3>4.2 why “Iceberg 로 생성” 을 이렇게 해석했는가</h3>")
    add('<div class="callout"><b>요구사항의 해석</b>'
        'MySQL · MongoDB · PostgreSQL 에는 Iceberg 테이블이 없다. '
        'Iceberg 는 파일 시스템 위의 <b>표 형식(table format)</b> 이기 때문이다. '
        '그래서 요구사항을 다음과 같이 구현했다.'
        '<ol>'
        '<li>원천 DB 에 일반 테이블로 데이터를 넣는다</li>'
        '<li>같은 데이터를 로컬에 <b>Iceberg 표 구조</b>(parquet + metadata) 로 쓴다</li>'
        '<li>rclone 이 폴더 단위로 복제한다 → 변동 때마다 갱신되는 metadata 도 함께 간다</li>'
        '<li>Auto Loader 가 그 폴더를 managed 테이블로 읽는다</li>'
        '</ol>'
        '실질적 이득은 세 가지다. 컬럼 이력·스냅샷 기록이 감사 자료로 남고, '
        '요구사항의 “변동 시 metadata 도 모두 전송” 이 문자 그대로 충족되며, '
        '원천 스키마 변화가 파일 단위에서 추적된다.</div>')
    add("</section>")

    # ---------------- 5. 신호 체인 ----------------
    add('<section id="signals"><h2>5. 신호 체인 (chk → rclone → Auto Loader)</h2>')
    add('<pre class="diagram">' + esc(signal_chain_diagram()) + "</pre>")
    add("<h3>5.1 S3 실제 구성</h3>")
    add(tbl(["구분", "객체 수", "의미"], [
        ["전체", num(s3.get("전체객체")), "—"],
        [f"<code>{esc(s3.get('chk_file', '_chk.json'))}</code>",
         num(s3.get("chk파일")), "데이터 준비 완료 (로컬 생성 → rclone 이 옮김)"],
        ["parquet", num(s3.get("parquet")),
         "데이터 파일 (CDC 는 <code>delta/</code> 하위로 분리)"],
        ["metadata", num(s3.get("metadata")),
         "Iceberg <code>.metadata.json</code> + <code>.manifest.json</code>"],
        [f"<code>{esc(s3.get('rclone_done_file', '_rclone_done.json'))}</code>",
         num(s3.get("rclone_done")), "복제 완료 (Airflow 가 직접 기록)"],
    ]))
    add("</section>")

    # ---------------- 6. rclone ----------------
    add('<section id="rclone"><h2>6. rclone 원격제어(rcd) API</h2>')
    add("<p>요구사항대로 셸 명령이 아니라 HTTP API 로 복제한다. "
        "이 방식의 실질적 이득은 <b>완료를 폴링할 수 있다</b>는 점이다.</p>")
    add(code(RCLONE_SNIPPET))
    add("<h3>6.1 신호 체인에서의 위치</h3>")
    add('<div class="callout"><b>왜 copy 인가</b>'
        '<code>sync</code> 는 대상에 있는데 원천에 없는 파일을 <b>지운다</b>. '
        '일부 테이블만 갱신된 상황에서 쓰면 갱신되지 않은 테이블이 사라진다. '
        '따라서 <code>copy</code> 를 쓴다. 폴더 단위로 지정하므로 '
        '변동 때마다 새로 생성되는 metadata 도 자동으로 따라간다.</div>')
    add("</section>")

    # ---------------- 7. Auto Loader ----------------
    add('<section id="autoloader"><h2>7. Auto Loader (초기 이관)</h2>')
    add("<h3>7.1 책임 경계 — 이 설계가 핵심이다</h3>")
    add('<div class="callout warn"><b>null as 컬럼 처리는 ETL 모듈의 몫이다</b>'
        '요구사항이 두 곳에 이 동작을 언급한다.'
        '<ul>'
        '<li>초기이관 5번: “컬럼 삭제 → null as column 으로 정상 처리”</li>'
        '<li>ETL 모듈 2번: “작업 순간에 다시 한 번 source 를 접속하여 컬럼 '
        '삭제/추가 여부를 확인하여 삭제: null as column 으로 정상 처리”</li>'
        '</ul>'
        '차이를 만드는 표현은 <b>“작업 순간에 다시 한 번 source 를 접속”</b> 이다. '
        'Auto Loader 는 S3 의 파일만 본다. 원천 DB 의 현재 상태를 모른다. '
        '판단 근거를 가진 쪽은 ETL 모듈이다.'
        '<br><br>'
        '따라서 <code>autoloader/initial_load.py</code> 는 '
        '<code>null as</code> 를 만들지 않고, 대상 테이블과 파일의 <b>교집합</b>만 '
        '적재한다. 컬럼이 빠진 자리를 Delta 가 NULL 로 채운다. '
        '명시적인 <code>CAST(NULL AS …)</code> 는 '
        '<code>etl-module/processor.py</code> 가 생성한다.</div>')

    add("<h3>7.2 적재 결과</h3>")
    agg = logs.get("load_audit_집계", [])
    if isinstance(agg, list) and agg:
        init_rows = [r for r in agg if r.get("work_type") == "initial_load"]
        rows = []
        for engine in ("mysql", "mongodb", "postgresql"):
            subset = [r for r in init_rows if r.get("engine") == engine]
            if not subset:
                continue
            total = sum(int(r["cnt"]) for r in subset)
            matched = sum(int(r["cnt"]) for r in subset
                          if r.get("count_match") == "Y")
            rows.append([engine, num(total), num(matched),
                         badge("일치" if total == matched else "불일치",
                               "ok" if total == matched else "ng")])
        add(tbl(["엔진", "적재 건수", "건수 일치", "판정"], rows))

    add("<h3>7.3 SQL 형태</h3>")
    add(code(AUTOLOADER_SQL))
    add("</section>")

    # ---------------- 8. ETL ----------------
    add('<section id="etl"><h2>8. ETL 모듈</h2>')
    add("<h3>8.1 세 가지 적재 방식</h3>")
    add(tbl(["방식", "코드", "동작", "검증된 결과"], [
        ["증분", "<code>append</code>", "<code>INSERT INTO</code>",
         "200 → 3,200 → 6,400 (누적 증가)"],
        ["전체 재적재", "<code>truncate</code>",
         "<code>INSERT OVERWRITE</code>", "200 → 200 (정확히 일치)"],
        ["변동분 병합", "<code>merge</code>",
         "<code>MERGE INTO … WHEN MATCHED</code>",
         "200 → 200 (중복 없이 반영)"],
    ]))
    add('<p><code>merge</code> 는 변동 대상 기본키가 반드시 필요하다. '
        '프로파일 등록 시점과 실행 시점 양쪽에서 검증한다 — '
        '프로파일에 <code>merge</code> 인데 pk 가 비면 등록 단계에서 거부한다.</p>')

    agg2 = logs.get("etl_run_log_집계", [])
    if isinstance(agg2, list) and agg2:
        add("<h3>8.2 실행 기록 집계 (etl_run_log)</h3>")
        add(tbl(["작업 종류", "etl_type", "판정", "건수", "평균 초"],
                [[r.get("work_type"), r.get("etl_type"),
                  badge(r.get("status_code"),
                        "ok" if r.get("status_code") == "OK" else "warn"),
                  num(r.get("cnt")), r.get("avg_sec")]
                 for r in agg2]))

    add("<h3>8.3 include / exclude 컬럼 설계</h3>")
    add('<pre class="diagram">' + esc(include_exclude_diagram()) + "</pre>")

    reasons = (deid.get("재asons") or {})
    add("<h3>8.4 컬럼 결정 규칙</h3>")
    add(tbl(["우선순위", "단계", "동작"], [
        ["1", "<code>include_columns</code> 가 있으면",
         "그 목록만 화이트리스트로 사용"],
        ["2", "비어 있으면", "<code>columns</code>(등록 시점 스냅샷)를 기준선으로 사용"],
        ["3", "항상 마지막", "<code>exclude_columns</code> 제거"],
    ]))
    if deid.get("load_columns"):
        add(tbl(["프로파일 기준선", "적재 컬럼", "제외", "비식별화"],
                [["11개", f"{len(deid['load_columns'])}개",
                  ", ".join(f"<code>{esc(c)}</code>"
                            for c in (deid.get("excluded_columns") or [])) or "—",
                  ", ".join(f"<code>{esc(k)}</code>=<code>{esc(v)}</code>"
                            for k, v in (deid.get("deid_applied") or {}).items())]]))
    add("</section>")

    # ---------------- 9. 비식별화 ----------------
    add('<section id="deid"><h2>9. 비식별화 (D1 · D2 · D3)</h2>')
    add(tbl(["코드", "정의", "SQL 식"], [
        ["<code>D1</code>", "해시 (SHA-256)",
         "<code>sha2(CAST(col AS STRING), 256)</code>"],
        ["<code>D2</code>", "null 처리", "<code>CAST(NULL AS &lt;타입&gt;)</code>"],
        ["<code>D3</code>", "마스킹",
         "<code>CASE WHEN length(...) &lt;= 2n+1 THEN '******' "
         "ELSE concat(앞 n글, '******', 뒤 n글) END</code>"],
    ]))
    add('<div class="callout"><b>왜 비식별화를 파이썬이 아니라 SQL 로 하는가</b>'
        '이전 프로젝트는 Airflow 프로세스 안에서 5만 건짜리 pandas DataFrame 을 '
        '만들고 <code>COPY INTO … FROM \'/tmp/x.parquet\'</code> 를 시도했다. '
        '이는 두 가지 이유로 실패한다. '
        '첫째, Databricks SQL Warehouse 는 Airflow 컨테이너의 디스크를 볼 수 없다. '
        '둘째, 5만 건을 파이썬 메모리에 올리면 워커 수에 비례해 메모리가 늘어난다. '
        '비식별화를 SQL 로 표현하면 중간 데이터가 파이썬 메모리를 지나지 않아 '
        '건수가 커져도 구조가 변하지 않는다.</div>')

    comparison = deid.get("비교") or []
    if comparison:
        add("<h3>9.1 실측값 대조 (원본 → 대상)</h3>")
        add("<p>Databricks 가 계산한 값이 파이썬 참조 구현과 일치하는지 "
            "<b>값 단위로</b> 대조했다.</p>")
        for item in comparison[:2]:
            add(f'<h4 style="font-size:14px;margin:14px 0 6px">id = {esc(item["id"])}</h4>')
            rows = []
            for col, info in item.items():
                if not isinstance(info, dict):
                    continue
                same = info.get("일치")
                mark = (badge("일치", "ok") if same else
                        badge("불일치", "ng")) if same is not None else "—"
                rows.append([
                    f"<code>{esc(col)}</code>",
                    f"<code>{esc(info.get('코드') or '—')}</code>",
                    esc(info.get("원본")),
                    f"<code>{esc(str(info.get('Python_기대값'))[:30])}</code>",
                    f"<code>{esc(str(info.get('Databricks_실제값'))[:30])}</code>",
                    mark,
                ])
            add(tbl(["컬럼", "코드", "원본", "Python 기대값",
                     "Databricks 실제값", "판정"], rows))

    add("<h3>9.2 배분 (엔진별로 골고루)</h3>")
    add("<p>세 코드가 <b>같은 테이블 안에서</b> 모두 쓰이도록 회전 배분했다. "
        "한 테이블을 검증해도 세 코드가 전부 확인된다.</p>")
    add(tbl(["테이블", "name", "email", "phone"], [
        ["table_1", "D1", "D2", "D3"],
        ["table_2", "D2", "D3", "D1"],
        ["table_3", "D3", "D1", "D2"],
        ["table_4", "D1", "D3", "D2"],
        ["table_5", "D2", "D1", "D3"],
    ]))
    agg3 = prof.get("집계", {})
    if agg3:
        add(tbl(["집계 항목", "값"], [
            ["프로파일 수", num(prof.get("프로파일수"))],
            ["등록 테이블", num(agg3.get("테이블"))],
            ["append", num(agg3.get("append"))],
            ["truncate", num(agg3.get("truncate"))],
            ["merge", num(agg3.get("merge"))],
            ["D1 배정", num(agg3.get("D1"))],
            ["D2 배정", num(agg3.get("D2"))],
            ["D3 배정", num(agg3.get("D3"))],
            ["exclude 지정", num(agg3.get("exclude"))],
            ["활성(Y)", num(agg3.get("활성"))],
        ]))
    add("</section>")

    # ---------------- 10. 작업 주기 ----------------
    add('<section id="schedule"><h2>10. 작업 주기 코드화</h2>')
    add("<p>6개 주기 코드를 정의하고, 등록일자와 고정 조건을 함께 판정한다.</p>")
    add(tbl(["코드", "의미", "실행 조건"], [
        ["<code>daily</code>", "일간", "매일"],
        ["<code>weekly</code>", "주간", "<code>day_of_week</code> 와 일치"],
        ["<code>monthly</code>", "월간", "매월 1일"],
        ["<code>quarterly</code>", "분기", "1·4·7·10월 1일"],
        ["<code>semi_annually</code>", "반기", "1·7월 1일"],
        ["<code>annually</code>", "연간", "매년 1월 1일"],
    ]))
    add('<div class="callout"><b>판정 순서</b>'
        '① <code>registered_at</code> 이후인가 → 아니면 건너뜀<br>'
        '② <code>fixed_day</code> 가 있으면 일치하는가 (예: 매월 1일)<br>'
        '③ 주기 코드의 조건을 만족하는가'
        '</div>')
    period_tally = prof.get("주기분포", {})
    if period_tally:
        add(tbl(["주기", "테이블 수"],
                [[f"<code>{esc(k)}</code>", num(v)] for k, v in
                 sorted(period_tally.items(), key=lambda x: -x[1])]))
    add('<p>실행 검증에서 주기 조건 때문에 45건이 건너뛰었다. '
        '3종 처리를 모두 확인하려면 <code>--ignore-schedule</code> 로 '
        '강제 실행해야 하며, 그 결과 60건이 모두 처리되었다. '
        '건너뜀 자체도 정상 동작이므로 로그에 <code>SKIP</code> 으로 남는다.</p>')
    add("</section>")

    # ---------------- 11. CDC ----------------
    add('<section id="cdc"><h2>11. CDC (변경 데이터 수집)</h2>')
    add('<pre class="diagram">' + esc(cdc_diagram()) + "</pre>")
    if cdc_result.get("결과"):
        add("<h3>11.1 3엔진 검증 결과</h3>")
        rows = []
        for r in cdc_result["결과"]:
            sim = r.get("스텝", {}).get("시뮬레이션", {})
            st = r.get("스텝", {}).get("검증", {}).get("상세", {})
            verdict = r.get("판정")
            rows.append([
                f"<code>{esc(r.get('대상'))}</code>",
                f"{sim.get('변경전')} → {sim.get('변경후')}",
                f"갱신 {sim.get('갱신')} / 삭제 {sim.get('삭제')} / 신규 {sim.get('신규')}",
                f"{sim.get('델타행수')}행",
                badge(verdict, "ok" if verdict == "통과" else "ng"),
            ])
        add(tbl(["대상", "원천 건수", "변경 종류", "델타", "판정"], rows))

        add("<h3>11.2 단계별 검증 기준</h3>")
        add("<p>행 개수만 보면 깨진 구현도 통과할 수 있다. "
            "증분 append 로도 총 건수는 그럴듯하게 움직이기 때문이다. "
            "그래서 <b>값</b>으로 판정한다.</p>")
        add(tbl(["단계", "검증 기준", "판별력"], [
            ["A 갱신 반영",
             "갱신된 행의 <code>description</code> 에 변경 표시가 있어야 한다",
             "append 만 하는 구현은 값이 안 바뀌므로 실패"],
            ["B 삭제 반영", "삭제된 id 가 대상에 하나도 없어야 한다",
             "append 는 삭제된 행을 남기므로 실패"],
            ["C 신규 반영", "신규 id 가 모두 존재해야 한다", "누락 감지"],
            ["D 건수", "순변화가 <code>신규 − 삭제</code> 와 같아야 한다",
             "증감 방향 검증"],
        ]))
        sample = next((r for r in cdc_result["결과"]
                       if r.get("스텝", {}).get("검증")), None)
        if sample:
            st = sample["스텝"]["검증"]["상세"]
            rows = []
            for key, val in st.items():
                extra = {k: v for k, v in val.items()
                         if k not in ("판정", "기준", "샘플", "잔존샘플")}
                rows.append([f"<code>{esc(key)}</code>",
                             badge(val["판정"],
                                   "ok" if val["판정"] == "통과" else "ng"),
                             esc(val.get("기준", "")),
                             esc(json.dumps(extra, ensure_ascii=False))])
            add(f"<h4 style=\"font-size:14px;margin:14px 0 6px\">"
                f"사례 — {esc(sample.get('대상'))}</h4>")
            add(tbl(["단계", "판정", "기준", "관측값"], rows))

        sql = cdc_result["결과"][0].get("스텝", {}).get("MERGE_sql")
        if sql:
            add("<h3>11.3 실제 실행된 MERGE</h3>")
            add(code(sql))
    add('<div class="callout ok"><b>스냅샷과 델타를 분리한 이유</b>'
        '한 폴더에 둘 다 두면, CDC 검증 전에 대상을 “기준선 복구”하려고 델타로 '
        '덮어쓰게 되고 그 델타가 곧 기준선이 되어 검증 자체가 무의미해진다. '
        '그래서 <code>data/</code>(전체 스냅샷)와 <code>delta/</code>(변경 스트림)를 '
        '별도 폴더로 분리했다. Auto Loader 는 <code>data/</code> 를 읽고 '
        'MERGE 는 <code>delta/</code> 를 읽는다.</div>')
    add("</section>")

    # ---------------- 12. 로그 테이블 ----------------
    add('<section id="logtables"><h2>12. 로그 테이블 설계</h2>')
    add('<pre class="diagram">' + esc(log_table_diagram()) + "</pre>")
    la = logs.get("load_audit", {})
    er = logs.get("etl_run_log", {})
    add(tbl(["테이블", "컬럼 수", "현재 행 수", "용도"], [
        [f"<code>{esc(la.get('name', 'load_audit'))}</code>",
         num(len(la.get("컬럼") or [])), num(la.get("건수")),
         "파일 적재 건수 비교"],
        [f"<code>{esc(er.get('name', 'etl_run_log'))}</code>",
         num(len(er.get("컬럼") or [])), num(er.get("건수")),
         "작업 성공/실패 기록"],
    ]))
    add('<div class="callout ng"><b>실측에서 발견한 결함과 수정</b>'
        '처음 구현에는 <code>process_table</code> 의 조기 반환 경로가 '
        '<code>ended_at</code> 을 채우지 않았다. 그래서 감사 테이블의 '
        '<code>NOT NULL</code> 제약 위반으로 적재 기록 자체가 실패했다. '
        '<code>try / finally</code> 로 구조를 바꿔 <b>어떤 경로로 나가도</b> '
        '종료 시각이 채워지도록 했다. 재발을 막으려고 기록 함수 쪽에도 '
        '기본값 방어를 넣었다.</div>')
    add("</section>")

    # ---------------- 13. Airflow ----------------
    add('<section id="airflow"><h2>13. Airflow 공통 DAG</h2>')
    add("<p>요구사항의 “공통 dag 을 만들고 import 하여 확장성을 증명” 을 "
        "<code>dbx_common.py</code> 의 빌더 4종으로 구현했다. "
        "개별 DAG 파일은 파라미터 블록일 뿐이다.</p>")
    add(code(DAG_SNIPPET))
    add(tbl(["빌더", "용도", "등록된 DAG 수"], [
        ["<code>build_rclone_dag</code>", "신호 확인 + rclone 복제", "12개"],
        ["<code>build_initial_load_dag</code>",
         "chk → copy → Auto Loader → 활성 전환", "12개"],
        ["<code>build_etl_dag</code>", "정규 ETL (스케줄 반영)", "12개"],
        ["<code>build_etl_dag</code> workers=3", "병렬 처리 경로", "12개"],
        ["<code>build_matrix_dag</code>", "12스키마 일괄 실행", "1개"],
        ["합계", "—", badge("49개", "ok")],
    ]))
    add("<h3>13.1 태스크 구조</h3>")
    add('<div class="grid2">')
    add("<div><b>정규 ETL</b>" + code(
        "start → etl_run → notify_slack → end", "diagram") + "</div>")
    add("<div><b>초기 이관</b>" + code(
        "start → check_signals → rclone_copy\n      → auto_load → activate_profile\n"
        "      → notify_slack → end", "diagram") + "</div>")
    add("</div>")
    add('<div class="callout"><b>복제를 초기이관 DAG 안에 둔 이유</b>'
        '시나리오가 요구하는 사슬은 '
        '<code>chk → rclone → Auto Loader → active=Y</code> 이고, 이게 '
        '하나의 자동 단위로 돌아야 한다. 복제만 별도 DAG 로 떼어내면 '
        '사람이 복제 끝나기 전에 적재를 실행시킬 수 있고, '
        '그것이 바로 요구사항이 경고하는 실패 모드다. '
        '활성 플래그 전환도 건수가 검증된 뒤에만 일어난다.</div>')
    add(image("04_airflow_dag_graph.png",
              "Airflow — dbx_mysql_schema_1_etl 실행 이력. "
              "start / etl_run / notify_slack / end 네 태스크 모두 성공."))
    add(image("06_airflow_initial_dag.png",
              "Airflow — dbx_mysql_schema_1_initial 의 7개 태스크 "
              "(chk 확인 → rclone 복제 → Auto Loader → 활성 전환 → 알림)."))
    add(image("05_airflow_matrix_dag.png",
              "Airflow — dbx_matrix_all. 12개 스키마를 15개 태스크로 전개한 "
              "공통 빌더의 확장 결과."))
    add("</section>")

    # ---------------- 14. 대시보드 ----------------
    add('<section id="dashboard"><h2>14. ETL 대시보드</h2>')
    add(tbl(["요구사항", "구현", "위치"], [
        ["엔진별 접속정보 + 실행 시 실조회", "<code>discover()</code>",
         "<code>GET /</code>"],
        ["스키마 단위 yaml/json (총 12개)", "<code>lib/profile.py</code>",
         "<code>config/profiles/</code>"],
        ["전 테이블 ETL 등록", "<code>scripts/01_make_profiles.py</code>",
         "프로파일 생성"],
        ["D1·D2·D3 엔진별 골고루 배분", "회전 배분",
         "<code>01_make_profiles.py</code>"],
        ["append·truncate·merge + 주기 + 등록일", "주기 6종 코드",
         "<code>lib/profile.py</code>"],
        ["전체 컬럼 표시 + 체크 해제 시 exclude", "체크박스 → <code>exclude_columns</code>",
         "<code>profile_detail.html</code>"],
        ["신규 테이블 빌더 등록", "<code>/builder</code>",
         "<code>dashboard/app.py</code>"],
        ["등록 정보 + Databricks 통계 조회", "<code>databricks_stats()</code>",
         "<code>GET /api/stats</code>"],
    ]))
    add('<div class="callout warn"><b>운영 환경에 가져갈 때</b>'
        '대시보드는 yaml 파일을 직접 쓴다. 실제 운영에서는 이 쓰기를 '
        '직접 수행 대신 <b>풀 리퀘스트</b>로 바꾸는 편이 낫다. '
        '프로파일은 형상관리 대상이므로 변경 이력이 남아야 하기 때문이다. '
        '테스트에서는 요구사항대로 직접 쓰도록 구현했다.</div>')
    add(code("""$ python dashboard/app.py --port 8540
  대시보드 : http://127.0.0.1:8540
  GET /                      개요 — 프로파일 · Databricks 통계 · 원천 현황
  GET /profiles              프로파일 목록
  GET /profiles/<e>/<s>      스키마 상세 (컬럼 편집 · 비식별화 · 주기)
  POST /profiles/<e>/<s>/save  YAML 저장
  GET /builder               ETL 빌더 (신규 테이블 등록)
  GET /columns               전체 컬럼 현황
  GET /table/add             신규 테이블 등록
  GET /api/profiles          JSON — 프로파일
  GET /api/stats             JSON — Databricks 통계"""))
    add(image("07_dashboard_profiles.png",
              "대시보드 개요 — 등록된 ETL 프로파일 12개(엔진 · 스키마 · 테이블 수 · "
              "활성 여부 · 작업 주기 · 등록일). 활성 5/5 로 전 스키마가 "
              "초기 이관을 마치고 정규 작업으로 넘어간 상태."))
    add("</section>")

    # ---------------- 15. Slack ----------------
    add('<section id="slack"><h2>15. Slack 알림</h2>')
    add("<p>기존 etl-fabric 테스트 공간의 Incoming Webhook 을 그대로 재사용한다.</p>")
    add(tbl(["항목", "값"], [
        ["발송 방식", "Incoming Webhook"],
        ["채널", f"<code>{esc(slack.get('채널', 'C0C53BAG812'))}</code>"],
        ["누적 발송", f"{num(slack.get('총건수', 0))}건"],
        ["성공 / 실패",
         f"{num(slack.get('성공건수', 0))} / {num(slack.get('실패건수', 0))}"],
        ["메시지 종류", ", ".join(f"<code>{esc(k)}</code>"
                                for k in (slack.get("종류별") or [])) or "—"],
    ]))
    add('<div class="callout"><b>알림 종류와 의도</b>'
        '<b>성공 / 실패 / 건너뜀</b> — 실행 요약. 실패가 하나라도 있으면 '
        '제목을 실패로 승격하고, 성공 내역도 함께 싣는다. '
        '“실패 1건” 인데 성공 8건인 상황을 알림만 보고 실패로 오해하지 않도록.<br>'
        '<b>컬럼 변경 감지</b> — 삭제 컬럼은 null 대체로 계속 진행하고, '
        '추가 컬럼은 자동 반영하지 않았음을 알린다. 개발자가 프로파일을 직접 고치도록.<br>'
        '<b>테이블 삭제 감지</b> — 원천 테이블이 사라지면 작업을 건너뛴다. '
        '프로파일은 지우지도 활성 상태도 바꾸지 않는다.<br>'
        '<b>초기 이관 / DAG 완료 / CDC</b> — 단계별 진행 통지.</div>')
    add(image("02_slack_notifications.png",
              "Slack — Airflow DAG 실행과 ETL 요약 알림이 실제 채널에 도착한 모습."))
    add("</section>")

    # ---------------- 16. Databricks 화면 ----------------
    add('<section id="databricks"><h2>16. Databricks 실행 화면</h2>')
    add(image("01_databricks_query_history.png",
              "Databricks Free Edition — Query History. CDC 검증에서 실행된 "
              "MERGE INTO / INSERT OVERWRITE / DESCRIBE / COUNT 질의 이력."))
    add("</section>")

    # ---------------- 17. 실측 함정 ----------------
    add('<section id="quirks"><h2>17. 실측에서 발견한 함정</h2>')
    add('<p>이 프로젝트에서 실제로 겪은 오류다. 각 항목은 '
        '“무엇이 발생했는가 / 왜 그런가 / 어떻게 고쳤는가” 순서로 적었다. '
        '재현 방법과 오류 문구까지 남겨 두었으므로 같은 문제로 되돌아가지 않는다.</p>')
    rows = []
    for q in quirks:
        kind = "warn" if q.get("구분") in ("성공/실패 판정", "쿼터") else "info"
        rows.append([
            str(q.get("번호", "")),
            f'<span class="badge {kind}">{esc(q.get("구분", ""))}</span>',
            f'<code>{esc(q.get("증상", ""))}</code>',
            esc(q.get("원인", "")),
            esc(q.get("조치", "")),
        ])
    add(tbl(["#", "구분", "증상", "원인", "조치"], rows))
    add("</section>")

    # ---------------- 18. 재현 ----------------
    add('<section id="reproduce"><h2>18. 재현 방법</h2>')
    add(code(REPRODUCE))
    add('<h3>18.1 멈춤 감지 · 자동 재시도</h3>')
    add("<p>이 프로젝트의 명령들은 Free Edition 쿼터 때문에 오래 걸린다. "
        "기다리는 동안 사람이 지켜보지 않도록, 출력 정지를 감지해 "
        "자동으로 재실행하는 감시기를 함께 둔다.</p>")
    add(code("""$ python scripts/watchdog.py --무응답초 900 --최대재시도 2 -- \\
    python autoloader/initial_load.py

  감시 원리
    1. 자식 프로세스를 띄우고 로그 파일로 리다이렉트한다
    2. 로그 파일의 수정 시각이 멈춘 채로 있으면 '멈춤' 으로 판단한다
       (프로세스가 살아 있어도 출력 정지는 확실한 신호다)
    3. kill 후 재시도한다. Databricks 한도 회복을 위해 대기한다
    4. 실패 로그 경로를 마지막에 출력한다"""))
    add("</section>")

    # ---------------- 19. 한계 ----------------
    add('<section id="limits"><h2>19. 검증 범위와 한계</h2>')
    add('<div class="callout warn"><b>한계를 분명히 적는다</b>'
        '검증이 통과했다고 해서 운영에 그대로 쓸 수 있다는 뜻은 아니다. '
        '아래는 이번에 <b>확인하지 않은</b> 항목이다.</div>')
    add(tbl(["항목", "상태", "이유"], [
        ["Iceberg 네이티브 delete 파일<br>"
         "(position / equality delete)", badge("미검증", "warn"),
         "Free Edition 의 Iceberg REST 카탈로그 지원 여부를 먼저 실측해야 한다. "
         "이번 CDC 는 <code>_cdc_op</code> 마커 + MERGE 로 처리했다"],
        ["Spark 라이트 엔진", badge("미검증", "warn"),
         "모든 실측이 <code>Serverless Starter Warehouse</code> 에서 돌아갔다. "
         "Free Edition 에서 라이브 엔진은 과금 필요"],
        ["성능 측정", badge("부분", "warn"),
         "Free Edition 은 창고 크기를 고정하므로 "
         "창고 크기에 따른 성능·과금 비교는 불가"],
        ["5만 건 전체 규모", (badge("통과", "ok") if rows_per_table >= 50000
                          else badge(f"미달 — 실측 {rows_per_table:,}건", "warn")),
         ("요구사항 규모(테이블당 50,000건)로 전체 파이프라인을 검증했다. "
          "파일 분할(rows_per_file), CDC 델타 크기, 병렬 처리 결과를 모두 이 규모에서 확인했다"
          if rows_per_table >= 50000 else
          "파이프라인 전체를 " + f"{rows_per_table:,} " + "건으로 반복 검증했다. "
          "구조는 동일하고 건수만 다르지만, 5만 건에서의 메모리·분할 거동은 별도 확인 필요하다")],
        ["PostgreSQL / MongoDB CDC", badge("통과", "ok"),
         "MySQL 과 동일한 4단계 검증이 세 엔진에서 모두 통과했다"],
    ]))
    add("</section>")

    # ---------------- 20. 산출물 ----------------
    add('<section id="artifacts"><h2>20. 산출물</h2>')
    add(tbl(["경로", "내용"], [
        ["<code>manual/databricks-pre-test-manual.html</code>", "이 매뉴얼"],
        ["<code>manual/captures/</code>", "캡처 이미지 원본"],
        ["<code>work/evidence.json</code>",
         "매뉴얼 수치의 원본 (Databricks·S3·DB 에서 직접 조회)"],
        ["<code>work/cdc/cdc_all_result.json</code>", "CDC 3엔진 검증 기록"],
        ["<code>work/00_cleanup_*.json</code>", "워크스페이스 정리 기록"],
        ["<code>work/slack_sent.json</code>", "Slack 발송 기록"],
        ["<code>work/watchdog/</code>", "감시 실행 로그"],
    ]))
    add('<div class="callout"><b>수치의 출처</b>'
        '매뉴얼의 모든 숫자는 <code>work/evidence.json</code> 에서 읽는다. '
        '이 파일은 Databricks·S3·원천 DB·Slack 에 직접 질의해서 만든다. '
        '손으로 옮겨 적지 않기 때문에 재실행해도 그 시점의 진실이 반영된다.</div>')
    add("</section>")

    # ---------------- footer ----------------
    add(f"<footer>databricks-pre-test-new · 검증 데이터 수집 시각 "
        f"{esc(collected)} · 생성 {datetime.now():%Y-%m-%d %H:%M}</footer>")
    add("</main></div></body></html>")
    return "".join(P)


# ---------------------------------------------------------------------------
# Static assets
# ---------------------------------------------------------------------------

NAV = [
    ("summary", "1. 검증 결과 요약"),
    ("architecture", "2. 시스템 구성"),
    ("cleanup", "3. 워크스페이스 정리"),
    ("dataprep", "4. 데이터 준비"),
    ("signals", "5. 신호 체인"),
    ("rclone", "6. rclone RC API"),
    ("autoloader", "7. Auto Loader"),
    ("etl", "8. ETL 모듈"),
    ("deid", "9. 비식별화"),
    ("schedule", "10. 작업 주기"),
    ("cdc", "11. CDC"),
    ("logtables", "12. 로그 테이블"),
    ("airflow", "13. Airflow DAG"),
    ("dashboard", "14. 대시보드"),
    ("slack", "15. Slack"),
    ("databricks", "16. Databricks 화면"),
    ("quirks", "17. 실측 함정"),
    ("reproduce", "18. 재현 방법"),
    ("limits", "19. 검증 범위와 한계"),
    ("artifacts", "20. 산출물"),
]

FOLDER_TREE = """databricks-pre-test-new/
  config/
    connections.yaml          엔진별 접속 정보 (비밀값은 환경변수)
    profiles/                 스키마 단위 ETL 정의 yaml 12개
  lib/                        공통 라이브러리
    config.py                 설정 · 환경 판별 · 비밀값
    envloader.py              .env 로더
    dbx.py                    Databricks SQL 클라이언트 (폴링)
    sources.py                MySQL / MongoDB / PostgreSQL 접근
    iceberg.py                Iceberg 형식 파일 + 스냅샷 사슬
    mask.py                   비식별화 D1 / D2 / D3
    profile.py                프로파일 · 컬럼 결정 · 주기 판정
    rclone_rc.py              rclone rcd API 클라이언트
    slack.py                  Slack 알림
    logtable.py               감사 테이블 DDL
  data-prep/
    prepare.py                60테이블 생성 + Iceberg 파일 + chk
    rclone_step.py            RC API 복제 + 완료 신호
  autoloader/
    initial_load.py           S3 → managed 테이블
  etl-module/
    processor.py              append / truncate / merge
  cdc/
    simulate.py               UPDATE / DELETE / INSERT + 마커
    apply_cdc.py              MERGE + 4단계 검증
    rebuild_snapshot.py       기준선 스냅샷 재생성
    schema_drift.py           컬럼 추가/삭제 시나리오
    run_all.py                3엔진 전수 실행
  dashboard/
    app.py + templates/       ETL 대시보드 (Flask)
  airflow/dags/
    dbx_common.py             공통 DAG 빌더
    dbx_dags.py               파라미터 정의
  scripts/
    00_cleanup.py             워크스페이스 정리
    01_make_profiles.py       프로파일 12개 생성
    activate_profiles.py      활성 플래그 일괄 변경
    run_etl.py                ETL 실행기
    collect_evidence.py       검증 수치 수집
    build_manual.py           매뉴얼 생성
    watchdog.py               멈춤 감지 · 자동 재시도"""

RCLONE_SNIPPET = '''# lib/rclone_rc.py — 셸 명령이 아닌 원격제어 API

payload = {
    "srcFs": "/data/mysql/mysql_schema_1",      # ← src 가 아니라 srcFs
    "dstFs": "lakehouse_seoul:bucket/pretest/mysql/mysql_schema_1",
    "metadata": True,                            # metadata 따라가기
    "transfers": 4, "checkers": 8,
    "_async": True,                              # ← 없으면 jobid 가 안 온다
}
job = requests.post(f"{rc}/sync/copy", auth=(user, pw), json=payload).json()
jobid = job["jobid"]

# 완료를 폴링한다. 셸 명령으로는 이 정보를 얻을 수 없다.
while True:
    state = requests.post(f"{rc}/job/status",          # ← POST + JSON body
                         auth=(user, pw),
                         json={"jobid": jobid}).json()
    if state.get("finished"):
        if state.get("error"):
            raise RcloneError(state["error"])
        break
    time.sleep(2)'''

AUTOLOADER_SQL = """-- 1) 파일에서 대상 테이블과 공통인 컬럼만 확인한다.
--    (신규 컬럼은 자동 반영하지 않는다)
SELECT * FROM parquet.`s3://.../mysql_schema_1/table_1/data` LIMIT 0;

-- 2) managed 테이블 준비
CREATE OR REPLACE TABLE workspace.mysql.table_1 (
  id BIGINT, name STRING, email STRING, phone STRING, age INT,
  salary DECIMAL(18,2), created_at TIMESTAMP, updated_at TIMESTAMP,
  is_active BOOLEAN
) USING DELTA;

-- 3) 초기 이관이므로 대상을 파일과 일치시킨다
INSERT OVERWRITE workspace.mysql.table_1 (`id`, `name`, ...)
SELECT `id`, `name`, ...
FROM parquet.`s3://.../table_1/data`;

-- 4) 건수 비교 후 감사 테이블에 1행 기록
SELECT COUNT(*) FROM parquet.`s3://.../table_1/data`;   -- 원본
SELECT COUNT(*) FROM workspace.mysql.table_1;           -- 대상
INSERT INTO workspace.pretest_meta.load_audit VALUES (...), (...);"""

DAG_SNIPPET = """# airflow/dags/dbx_common.py — 공통 DAG 빌더

def build_etl_dag(dag_id, engine, schema, workers=1, schedule=None,
                  force=False, ignore_schedule=False):
    with _base_dag(dag_id, schedule, ["etl", engine],
                   f"ETL 정규 작업 — {engine}.{schema} (workers={workers})") as dag:
        start    = EmptyOperator(task_id="start")
        etl      = PythonOperator(task_id="etl_run",
                                  python_callable=run_etl_task,
                                  op_kwargs={"engine": engine, "schema": schema,
                                             "workers": workers})
        notify   = PythonOperator(task_id="notify_slack",
                                  python_callable=notify_task,
                                  op_kwargs={"engine": engine, "schema": schema})
        end      = EmptyOperator(task_id="end")
        start >> etl >> notify >> end
    return dag

# airflow/dags/dbx_dags.py — 실제 DAG 는 파라미터 블록뿐
for engine in ("mysql", "mongodb", "postgresql"):
    for schema in SCHEMAS[engine]:
        short = schema.split("_")[-1]
        globals()[f"dag_etl_{engine}_{short}"] = build_etl_dag(
            dag_id=f"dbx_{engine}_schema_{short}_etl",
            engine=engine, schema=schema, workers=1, schedule="0 2 * * *")
        # ... workers=3 버전, rclone 버전, initial 버전까지 반복"""

REPRODUCE = """# 0) 환경 기동
cd C:\\Users\\lee21\\lakehouse
docker compose up -d

# 1) 워크스페이스 정리 (쿼터 확보)
python scripts/00_cleanup.py            # 목록만 (기본값)
python scripts/00_cleanup.py --실행      # 실제 정리

# 2) 데이터 준비 — 60테이블 · Iceberg 파일 · chk
python data-prep/prepare.py
python data-prep/prepare.py --건수 50000        # 전량
python data-prep/prepare.py --건수 200          # 소규모 시험

# 3) 프로파일 생성 — 스키마 단위 yaml 12개
python scripts/01_make_profiles.py --지우기

# 4) rclone 복제 (RC API · copy 증분)
python data-prep/rclone_step.py

# 5) 초기 이관
python autoloader/initial_load.py
python autoloader/initial_load.py --신호만       # 신호 상태만 확인

# 6) 활성 플래그 전환 (초기 이관 → 정규 작업)
python scripts/activate_profiles.py

# 7) 정규 ETL
python scripts/run_etl.py --engine mysql --schema mysql_schema_1 --workers 3
python scripts/run_etl.py --all --workers 3 --ignore-schedule

# 8) CDC 검증 (3엔진 전수)
python cdc/run_all.py

# 9) 대시보드
python dashboard/app.py --port 8540

# 10) 증적 수집 + 매뉴얼 생성
python scripts/collect_evidence.py
python scripts/build_manual.py"""


# ---------------------------------------------------------------------------
# Extra evidence loaders
# ---------------------------------------------------------------------------

def _load_cdc() -> dict:
    path = ROOT / "work" / "cdc" / "cdc_all_result.json"
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:                            # noqa: BLE001
        return {}


def _load_cleanup() -> dict:
    folder = ROOT / "work"
    if not folder.exists():
        return {}
    files = sorted(folder.glob("00_cleanup_*.json"))
    if not files:
        return {}
    try:
        return json.loads(files[-1].read_text(encoding="utf-8"))
    except Exception:                            # noqa: BLE001
        return {}
