# databricks-pre-test-new

Impala/Trino → Databricks 전환 검증용 미니 ETL 프로젝트.

Three source databases → Iceberg-format files on S3 → Databricks managed
tables, orchestrated entirely by Airflow, with de-identification, incremental
loading and CDC.

## 프로젝트 구조

```
databricks-pre-test-new/
  config/
    connections.yaml          엔진별 접속 정보 (비밀값은 환경변수)
    profiles/                 스키마 단위 ETL 정의 YAML (12개)
  lib/                        공통 라이브러리
    config.py                 설정 로더 · 환경 판별 · 비밀값
    envloader.py              .env 로더
    dbx.py                    Databricks SQL 클라이언트 (폴링 포함)
    sources.py                MySQL / MongoDB / PostgreSQL 접근 계층
    iceberg.py                Iceberg 형식 파일 + 스냅샷 사슬
    mask.py                   비식별화 D1 / D2 / D3
    profile.py                프로파일 읽기·쓰기 · 컬럼 결정 · 주기 판정
    rclone_rc.py              rclone rcd(원격제어) API 클라이언트
    slack.py                  Slack 알림 (Block Kit + 발송 기록)
    logtable.py               감사 테이블 2종 DDL
  data-prep/
    prepare.py                60테이블 × 5만건 생성 + chk 파일
    rclone_step.py            rclone RC API 복제 + 완료 신호
  autoloader/
    initial_load.py           S3 → managed 테이블 초기 이관
  etl-module/
    processor.py              ETL 엔진 (append / truncate / merge)
  cdc/
    simulate.py               UPDATE / DELETE / INSERT 시뮬레이션
    apply_cdc.py              CDC 적재 + 3단계 검증
  dashboard/
    app.py                    ETL 대시보드 (Flask)
    templates/                7개 템플릿
  airflow/dags/
    dbx_common.py             공통 DAG 팩토리
    dbx_dags.py               파라미터만 적은 DAG 정의
  scripts/
    00_cleanup.py             Databricks 워크스페이스 정리
    01_make_profiles.py       프로파일 12개 생성
    run_etl.py                ETL 실행기
    watchdog.py               멈춤 감지 · 자동 재시도
  manual/
    databricks-pre-test-manual.html
```

## 실행 순서

```powershell
# 0. 환경 기동
cd C:\Users\lee21\lakehouse
docker compose up -d

# 1. 워크스페이스 정리 (쿼터 확보)
python scripts/00_cleanup.py --실행

# 2. 데이터 준비 — 60테이블 생성 + Iceberg 파일 + chk
python data-prep/prepare.py

# 3. 프로파일 생성 — 스키마 단위 YAML 12개
python scripts/01_make_profiles.py --지우기

# 4. rclone 복제 (RC API, copy 증분 모드)
python data-prep/rclone_step.py

# 5. 초기 이관 — S3 → managed 테이블, 로그 기록
python autoloader/initial_load.py

# 6. 프로파일 활성 (N → Y)
python scripts/activate_profiles.py

# 7. 정규 ETL (append / truncate / merge + D1~D3)
python scripts/run_etl.py --all --workers 3

# 8. CDC 검증 (3엔진 전수)
python cdc/run_all.py

# 9. 대시보드
python dashboard/app.py --port 8540

# 10. 증적 수집 + 매뉴얼 생성
python scripts/collect_evidence.py
python scripts/build_manual.py
```

긴 명령은 watchdog 으로 감시한다 (멈추면 자동 재실행).

```powershell
python scripts/watchdog.py --무응답초 900 --최대재시도 2 -- `
    python autoloader/initial_load.py
```

## 실측 소요 시간 (테이블당 5만 건 기준)

아래는 Free Edition · 로컬 Docker · 60테이블 × 50,000건 = **300만 행**을
실제로 돌린 수치다. 추정치가 아니다.

| 단계 | 대상 | 소요 |
|---|---|---|
| 데이터 준비 (DB + Iceberg 파일) | 300만 행 / 180 parquet | 약 185초 |
| ├ MySQL | 100만 행 | 약 59초 |
| ├ MongoDB | 100만 행 | 약 36초 |
| └ PostgreSQL | 100만 행 | 약 89초 |
| rclone 복제 (RC API) | 12스키마 · 386객체 · 147MB | 약 31초 |
| Auto Loader | 60테이블 × 5만 건 | 테이블당 약 30초 |
| ETL (append/truncate/merge) | 60테이블 · workers=3 | 스키마당 약 7초 |

Auto Loader 가 가장 느린 단계다. Databricks 관리형 테이블 생성 + 5만 건 적재 +
건수 대조가 테이블마다 반복되기 때문이다. Free Edition 쿼터에 걸리지 않도록
`scripts/watchdog.py` 로 감시하며 돈다.

## 검증 시나리오

| 시나리오 | 명령 | 판정 |
|---|---|---|
| 초기 이관 건수 일치 | `python -m autoloader.initial_load` | 60테이블 전부 일치 |
| 비식별화 D1/D2/D3 | `python scripts/collect_evidence.py` | Databricks 값 = 파이썬 참조값 |
| CDC 갱신·삭제·신규 | `python cdc/run_all.py` | 3엔진 × 4단계 통과 |
| 컬럼 추가/삭제 | `python cdc/schema_drift.py` | 삭제→NULL 대체 / 추가→미반영+Slack |
| 공통 DAG 재사용 | Airflow `dbx_matrix_all` | 49개 DAG 등록 |

## Airflow

`airflow/dags` 는 컨테이너 `/opt/airflow/dags` 에 마운트되어 있다.
프로젝트는 `/opt/pretest` 에 마운트되어 있다 (읽기/쓰기).

- `dbx_common.py` — DAG 빌더 4종 (rclone / initial / etl / matrix)
- `dbx_dags.py` — 12스키마 × 4종 + 매트릭스 = **49개 DAG** 를 파라미터로 생성

각 DAG 파일은 파라미터 블록일 뿐이고 파이프라인 정의는 `dbx_common.py` 에 있다.
그래서 스키마를 하나 추가할 때 코드를 복사하지 않는다.

```python
globals()[f"dag_etl_{engine}_{short}"] = build_etl_dag(
    dag_id=f"dbx_{engine}_schema_{short}_etl",
    engine=engine, schema=schema, workers=1, schedule="0 2 * * *")
```

## 식별자와 주석

- **식별자**는 영문 `snake_case`
- **주석·도큐스트링·로그 메시지**는 한국어
- 증적 JSON 의 **키**도 한국어다. 사람이 읽는 기록이 목적이므로 그대로 둔다.
  (`lib/` 은 영문 키, `work/` 의 증적 JSON 은 한국어 키)

## 시크릿

평문으로 저장하지 않는다.

- 호스트 실행: `C:\Users\lee21\lakehouse\.env` (자동 로드)
- 컨테이너 실행: compose 환경변수 + Airflow Variable
