"""
`.env` 로더 — 호스트(windows)에서 직접 실행할 때 비밀값을 환경변수로 올린다.

왜 필요한가
-----------
Airflow 컨테이너에는 `.env` 파일이 없다. 값은 compose 가 환경변수로 주입한다.
하지만 호스트에서 스크립트를 직접 실행할 때는 아무것도 주입되지 않아
`DATABRICKS_TOKEN` 이 빈 문자열이 되고, 401 / 접속 실패로 이어진다.

`lib.config` 가 **모듈 임포트 시점에** 이 모듈을 호출하도록 해 두었다.
그래서 어떤 경로로 들어오든 비밀값이 자동으로 채워진다.

사용법 (보통은 직접 부르지 않는다)
----------------------------------
    import envloader
    envloader.load()

Airflow 컨테이너 안에서는 아무 일도 하지 않는다.
"""
from __future__ import annotations

import os
from pathlib import Path

#: 비밀 파일 탐색 순서. 앞의 것이 우선한다(단, 환경변수가 이미 있으면 안 덮어씀).
#:
#: Measured: lakehouse/.env 하나만 보면 이 프로젝트 전용 값이 빠져서
#: `SLACK_WEBHOOK_URL` 이 빈 값으로 남았고, 알림 80건 중 78건이
#: "웹훅 주소가 없습니다" 로 전송되지 않았다. 프로젝트 루트의 `.env` 도
#: 함께 읽도록 여기를 추가했다.
ENV_PATHS: list[Path] = [
    Path(r"C:\Users\lee21\lakehouse\.env"),
    Path(__file__).resolve().parent.parent / ".env",
]

#: 중복 로드 방지
_loaded = False


def _parse_line(line: str) -> tuple[str, str] | None:
    """`.env` 한 줄을 (키, 값) 튜플로 바꾼다. 주석·빈 줄은 None."""
    line = line.strip()
    if not line or line.startswith("#") or "=" not in line:
        return None

    key, _, value = line.partition("=")
    key = key.strip()
    value = value.strip()

    # 감싼 따옴표를 벗긴다.
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
        value = value[1:-1]
    return key, value


def load() -> dict[str, str]:
    """`.env` 의 모든 값을 `os.environ` 에 넣는다.

    Returns:
        읽어 들인 {키: 값} 딕셔너리. 파일이 없으면 빈 딕셔너리.

    주의:
        이미 설정된 환경변수는 **덮어쓰지 않는다.** 그래서 Airflow Variable
        이나 컨테이너 주입값이 `.env` 보다 우선한다.
    """
    global _loaded
    if _loaded:
        return {}

    _loaded = True

    parsed: dict[str, str] = {}
    for env_path in ENV_PATHS:
        if not env_path.exists():
            continue
        try:
            content = env_path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            content = env_path.read_text(encoding="utf-8-sig")

        for line in content.splitlines():
            pair = _parse_line(line)
            if pair is None:
                continue
            key, value = pair
            # 나중 파일이 앞 결과를 덮어쓰지 않게 한다.
            parsed.setdefault(key, value)
            os.environ.setdefault(key, value)

    return parsed


if __name__ == "__main__":
    loaded = load()
    print(f"{len(ENV_PATHS)}개 경로에서 {len(loaded)}개 값을 읽었습니다.")
    for env_path in ENV_PATHS:
        print(f"  {'있음' if env_path.exists() else '없음'}  {env_path}")
    print("키 이름:", ", ".join(sorted(loaded)))
