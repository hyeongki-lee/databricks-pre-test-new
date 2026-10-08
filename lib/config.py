"""
설정 로더 (connections.yaml + 비밀값).

모든 스크립트는 이 모듈 하나로 설정만 읽는다. 규칙은 세 가지다.

  1. 비밀번호·토큰 같은 비밀 값은 이 파일(yaml)에 쓰지 않는다.
     값은 환경변수 또는 Airflow Variable 에서만 읽는다.
  2. 경로는 "컨테이너 안" 과 "호스트" 를 구분한다.
     Airflow 컨테이너에서는 프로젝트 루트가 /opt/pretest 이고,
     윈도우 호스트에서는 이 파일의 상위 폴더다. 아래 in_container() 가 구분한다.
  3. 실행 환경(컨테이너/호스트)에 따라 접속 대상이 다르다.
     컨테이너 안에서는 mysql·mongodb·postgres 라는 서비스 이름으로 붙고,
     호스트에서는 localhost 와 공개 포트로 붙는다.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

# ---------------------------------------------------------------------------
# 상수
# ---------------------------------------------------------------------------

#: 이 파일은 lib/ 에 있다. 그래서 상위가 프로젝트 루트다.
PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent

#: 연결 설정 yaml 의 절대 경로.
CONNECTIONS_PATH: Path = PROJECT_ROOT / "config" / "connections.yaml"

#: 컨테이너 안에서 쓰는 프로젝트 루트.
CONTAINER_ROOT: Path = Path("/opt/pretest")

#: "컨테이너 안에서 실행 중" 을 알리는 환경변수 이름.
IN_CONTAINER_ENV: str = "PRETEST_IN_CONTAINER"


# ---------------------------------------------------------------------------
# 실행 환경 판별
# ---------------------------------------------------------------------------

def in_container() -> bool:
    """Airflow 컨테이너 안에서 실행 중인지 판별한다.

    판별 근거는 둘이다.
      1. `PRETEST_IN_CONTAINER=1` 환경변수
      2. 컨테이너에서만 잡히는 `/opt/pretest` 경로의 존재 여부
    """
    if os.environ.get(IN_CONTAINER_ENV) == "1":
        return True
    # 컨테이너에서만 잡히는 경로를 확인한다.
    return CONTAINER_ROOT.exists()


def project_path(relative_path: str) -> Path:
    """프로젝트 안의 상대 경로를 실행 환경에 맞는 실제 경로로 바꾼다."""
    if in_container():
        return CONTAINER_ROOT / relative_path
    return PROJECT_ROOT / relative_path


# ---------------------------------------------------------------------------
# 비밀값 자동 로드
# ---------------------------------------------------------------------------
# 왜 여기서 하는가
# --------------
# 이전에는 각 스크립트가 `envloader.load()` 를 직접 불러야 했다.
# 하나라도 빠뜨리면 **비밀번호가 빈 문자열로 들어가 401 / 접속 실패** 로 이어진다.
# 실제로 rclone-rcd 연결에서 이 빠뜨림 때문에 인증 실패(HTTP 401)가 났다.
#
# 그래서 여기가 가장 확실한 자리다. 어떤 경로로 들어오든 config 를 거치면
# 비밀값이 자동으로 채워진다. 이미 있는 환경변수는 덮어쓰지 않는다.
def _auto_load_env() -> None:
    """임포트 시점에 `.env` 를 환경변수로 주입한다(호스트 실행일 때만)."""
    if in_container():
        # 컨테이너에는 .env 가 없다. 환경변수는 이미 주입되어 있다.
        return
    try:
        from . import envloader
    except ImportError:
        try:
            import envloader            # type: ignore
        except ImportError:
            return
    envloader.load()


_auto_load_env()


# ---------------------------------------------------------------------------
# 기본 로더
# ---------------------------------------------------------------------------

_config_cache: dict[str, Any] | None = None


def load_config() -> dict:
    """연결 설정을 읽는다(프로세스당 한 번만 파싱)."""
    global _config_cache
    if _config_cache is None:
        with open(CONNECTIONS_PATH, "r", encoding="utf-8") as fp:
            _config_cache = yaml.safe_load(fp)
    return _config_cache


def get_section(key: str, default: Any = None) -> Any:
    """connections.yaml 의 최상위 구역 하나를 꺼낸다."""
    return load_config().get(key, default)


def get_secret(env_key: str, default: str = "") -> str:
    """비밀 값을 환경변수에서 읽는다.

    Airflow 컨테이너에서 .env 파일은 없다. 그래서 환경변수를 쓴다.
    호스트에서 실행할 때는 .env 에서 값을 끌어온다(호출하는 쪽에서 주입).
    """
    value = os.environ.get(env_key, "")
    return value if value else default


# ---------------------------------------------------------------------------
# 원천 데이터베이스
# ---------------------------------------------------------------------------

def _source_connection(engine: str) -> dict:
    """엔진별 원천 접속 정보를 만든다. 실행 환경에 따라 호스트가 달라진다."""
    sources = get_section("sources", {})
    conf = dict(sources.get(engine, {}))
    if not conf:
        raise KeyError(f"connections.yaml 에 '{engine}' 원천 설정이 없습니다.")

    is_container = in_container()
    # 컨테이너 안에서는 compose 서비스 이름, 호스트에서는 localhost.
    conf["host"] = conf["host"] if is_container else conf.get("host_local", conf["host"])
    conf["port"] = int(conf["port"])
    conf["in_container"] = is_container

    # 비밀번호: 지정된 환경변수 이름에서 읽고, 없으면 compose 기본값으로 간다.
    password_env = conf.get("password_env")
    default_password = str(conf.get("password_default", "") or "")
    if password_env:
        conf["password"] = get_secret(password_env, default_password)
    return conf


def mysql_connection() -> dict:
    """MySQL 접속 정보."""
    conf = _source_connection("mysql")
    # MySQL 컨테이너의 root 비밀번호는 MYSQL_ROOT_PASSWORD 다.
    conf.setdefault("user", "root")
    conf["password"] = get_secret("MYSQL_ROOT_PASSWORD", str(conf.get("password_default", "")))
    return conf


def postgres_connection() -> dict:
    """PostgreSQL 접속 정보."""
    conf = _source_connection("postgresql")
    conf["password"] = get_secret("POSTGRES_PASSWORD", str(conf.get("password_default", "")))
    return conf


def mongodb_connection() -> dict:
    """MongoDB 접속 정보.

    ⚠ 실측 함정: `.env` 에 `MONGO_ROOT_PASSWORD` 가 없다.
       docker-compose.yml 이 `${MONGO_ROOT_PASSWORD:-rootpass}` 로 기본값을 쓰기
       때문에 실제 비밀번호는 `rootpass` 다. `password_default` 로 백업해 둔다.
    """
    conf = _source_connection("mongodb")
    conf["password"] = get_secret("MONGO_ROOT_PASSWORD", str(conf.get("password_default", "")))
    return conf


# ---------------------------------------------------------------------------
# 외부 시스템 설정
# ---------------------------------------------------------------------------

def get_databricks_config() -> dict:
    """Databricks 연결 정보(토큰 제외)."""
    conf = dict(get_section("databricks", {}))
    conf["host"] = get_secret("DATABRICKS_HOST", conf.get("host", "")).rstrip("/")
    return conf


def get_s3_config() -> dict:
    """S3 버킷/접두 설정."""
    return dict(get_section("s3", {}))


def get_rclone_config() -> dict:
    """rclone rcd 원격 제어 API 설정."""
    return dict(get_section("rclone", {}))


def get_etl_config() -> dict:
    """ETL 처리 상수 설정."""
    return dict(get_section("etl", {}))


def get_slack_config() -> dict:
    """Slack 알림 설정."""
    return dict(get_section("slack", {}))


# ---------------------------------------------------------------------------
# 파생 경로
# ---------------------------------------------------------------------------

def local_data_root() -> Path:
    """data-prep 이 파일을 쓰는 로컬 폴더."""
    return Path(get_section("project", {}).get("local_data_root", ""))


def profile_folder() -> Path:
    """스키마 단위 ETL 정의(yaml) 폴더."""
    return project_path("config/profiles")


def library_folder() -> Path:
    """파이썬 모듈(lib) 폴더."""
    return project_path("lib")


def log_folder() -> Path:
    """실행 기록을 남길 폴더. 없으면 만들어 쓴다."""
    folder = project_path("work")
    folder.mkdir(parents=True, exist_ok=True)
    return folder