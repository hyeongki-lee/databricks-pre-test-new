"""
rclone rcd(원격 제어) API 클라이언트.

왜 셸 명령이 아니라 API 인가 (요구사항 4번)
------------------------------------------
`rclone copy 로컬 s3:버킷/경로` 를 셸로 치면 다음 문제가 생긴다.

    * 호출 프로세스가 끝나면 복제 상태를 알 수 없다.
    * 실패해도 로그가 STREAM 에 흩어진다.
    * Airflow 태스크가 "끝났다"는 것과 "복제가 끝났다"는 것이 다르다.

rclone 의 `rcd`(remote control daemon)는 HTTP API 로 동작한다.
    POST /sync/copy        → 복제 시작, jobid 를 돌려준다
    POST /job/status       → jobid 로 상태 조회 (GET 이 아니라 POST 여야 한다)
    POST /core/version     → 버전/연결 확인

그래서 복제 완료를 **jobid 로 폴링**할 수 있다. 이게 신호 체인의 핵심이다.

신호 체인 (인과가 명확해야 한다)
------------------------------
    data-prep        → 로컬에 데이터 파일 + metadata + `_chk.json`
    rclone RC API    → `copy` 로 S3 전송.  완료되면 jobid 상태가 SUCCEEDED
    Airflow          → 확인 후 S3 에 `_rclone_done.json` 을 직접 쓴다
    Auto Loader      → `_rclone_done.json` 이 있고 실제 파일이 보일 때만 적재

`_chk.json`(로컬 생성 → rclone 이 복제)과 `_rclone_done.json`(Airflow 가 직접 작성)은
**서로 다른 파일**이다. 그래야 "데이터 준비 완료"와 "복제 완료"를 구분할 수 있다.
기존 구현은 둘을 같은 파일로 써서 인과가 뒤집혔다.
"""
from __future__ import annotations

import logging
import time
from typing import Any

import requests

try:
    from . import config
except ImportError:
    import config                        # type: ignore

logger = logging.getLogger(__name__)


class RcloneError(RuntimeError):
    """rclone API 호출이 실패했을 때."""


# ---------------------------------------------------------------------------
# 연결
# ---------------------------------------------------------------------------

def _endpoint() -> tuple[str, str, str]:
    """(주소, 사용자, 암호) 를 설정에서 만든다."""
    conf = config.get_rclone_config()
    url = conf.get("rc_url", "http://localhost:5572").rstrip("/")
    user = config.get_secret(conf.get("rc_user_env", "RCLONE_RC_USER"), "admin")
    # 주 환경변수 → 대체 환경변수 순으로 본다.
    # lakehouse/.env 에 RCLONE_RC_PASS 가 없기 때문에 대체값이 사실상 쓰인다.
    password = (config.get_secret(conf.get("rc_pass_env", "RCLONE_RC_PASS"))
                or config.get_secret(conf.get("rc_pass_fallback_env", "RCLONE_GUI_PASS")))
    return url, user, password


def check_connection() -> dict:
    """rcd 가 살아 있고 인증이 통과하는지 확인한다.

    `POST /core/version` 이 가장 가벼운 확인 수단이다.
    실제 데모: 인증이 없으면 401, 비밀번호가 틀리면 401 이 돌아온다.
    """
    url, user, password = _endpoint()
    try:
        response = requests.post(
            f"{url}/core/version",
            auth=(user, password) if password else None,
            timeout=15,
        )
    except requests.RequestException as exc:
        raise RcloneError(
            f"rclone rcd 에 연결할 수 없습니다: {url}\n"
            f"  ({type(exc).__name__}: {exc})\n"
            f"  docker compose up -d rclone-rcd 로 기동하십시오."
        ) from exc

    if response.status_code == 401:
        raise RcloneError(
            "rclone rcd 인증에 실패했습니다(HTTP 401).\n"
            "  RCLONE_RC_USER / RCLONE_RC_PASS 를 확인하십시오."
        )
    if response.status_code != 200:
        raise RcloneError(
            f"rclone rcd 응답 이상 (HTTP {response.status_code}): {response.text[:300]}"
        )

    data = response.json()
    # `os` 는 rclone 버전마다 객체일 수도 문자열일 수도 있다. 안전하게 처리한다.
    os_value = data.get("os")
    if isinstance(os_value, dict):
        os_value = os_value.get("type") or os_value.get("arch")
    return {
        "주소": url,
        "버전": data.get("version"),
        "선언버전": data.get("beta"),
        "운영체제": os_value,
        "연결됨": True,
    }


def list_remotes() -> list[str]:
    """사용 가능한 리모트 이름."""
    conf = config.get_rclone_config()
    expected = conf.get("remote_name", "lakehouse-seoul")
    url, user, password = _endpoint()
    response = requests.post(
        f"{url}/config/listremotes",
        auth=(user, password) if password else None, timeout=30)
    response.raise_for_status()
    remotes = [r.rstrip(":") for r in response.json().get("remotes", [])]
    if expected not in remotes:
        raise RcloneError(
            f"설정한 리모트 '{expected}' 이 없습니다. 사용 가능: {remotes}\n"
            f"  docker-compose.override.yml 의 rclone-rcd 환경변수를 확인하십시오."
        )
    return remotes


# ---------------------------------------------------------------------------
# 복제
# ---------------------------------------------------------------------------

def start_copy(src_path: str, dst_path: str, *,
               extra_options: dict | None = None,
               dry_run: bool = False) -> dict:
    """copy 모드로 복제를 시작하고 jobid 를 돌려준다.

    `sync` 가 아니라 **`copy`** 를 쓴다. 요구사항 3번(증분 복제).
    sync 는 대상에서 사라진 파일까지 지워버려서, 원천이 일부만 갱신된
    상황에서 위험하다. copy 는 있는 것만 옮기고 기존 것은 남긴다.

    Returns:
        {"jobid", "executeId", "드라이런", "원격경로", "대상경로"}
    """
    conf = config.get_rclone_config()
    remote_name = conf.get("remote_name", "lakehouse-seoul")
    url, user, password = _endpoint()

    # ⚠ 실측 함정 1: rclone v1.75 의 `/sync/copy` 는 `src`·`dst` 가 아니라
    #   **`srcFs`·`dstFs`** 키를 요구한다. `src`·`dst` 를 보내면
    #   `Didn't find key "srcFs" in input` (HTTP 400) 으로 거절된다.
    #
    # ⚠ 실측 함정 2: `_async: true` 를 안 주면 **동기 실행**이 되어 응답이
    #   `{}` 로 비어 돌아온다. jobid 를 받을 수 없어 폴링이 불가능하다.
    #   `_async: true` 를 주면 `{"jobid": 13, "executeId": "..."}` 가 돌아온다.
    #   이 jobid 로 완료를 폴링하는 것이 "셸 명령이 아니라 API"라는 것의 실질적 이득이다.
    body = {
        "srcFs": src_path,
        "dstFs": f"{remote_name}:{dst_path}",
        # copy = 증분. 대상에 이미 있는 파일은 건너뛴다.
        # (sync 는 대상에만 있는 파일을 지워버린다)
        "createEmptySrcDirs": False,
        "dryRun": dry_run,
        # 메타데이터(변동 때마다 갱신되는 metadata 포함)를 반드시 따라간다.
        "metadata": True,
        "transfers": 4,
        "checkers": 8,
        # ★ 이게 있어야 jobid 가 나온다
        "_async": True,
    }
    if extra_options:
        body.update(extra_options)
        # 사용자가 동기 호출을 원하면 다시 끈다.
        body.setdefault("_async", True)

    response = requests.post(
        f"{url}/sync/copy",
        auth=(user, password) if password else None,
        json=body, timeout=120,
    )
    if response.status_code != 200:
        raise RcloneError(
            f"복제 요청 실패 (HTTP {response.status_code}): {response.text[:400]}\n"
            f"  srcFs={src_path}\n  dstFs={body['dstFs']}"
        )

    result = response.json()
    jobid = result.get("jobid")
    if jobid is None:
        raise RcloneError(
            "복제 요청은 200 이었지만 jobid 가 없습니다.\n"
            f"  응답: {result}\n"
            "  `_async: true` 가 빠졌는지 확인하십시오."
        )
    return {
        "jobid": jobid,
        "executeId": result.get("executeId"),
        "드라이런": dry_run,
        "원격경로": src_path,
        "대상경로": body["dstFs"],
    }


def job_status(jobid: int) -> dict:
    """복제 작업 상태를 조회한다.

    ⚠ 실측 함정: `/job/status` 는 **GET 도 쿼리스트링도 아니다.**
      `GET /job/status?jobid=13` → `404 Not Found`
      반드시 **POST + JSON body** 여야 한다.
      (`POST /job/status` 에 `{"jobid": 13}`)

    반환 필드 중 `finished` 가 진짜 완료 신호다.
    """
    url, user, password = _endpoint()
    response = requests.post(
        f"{url}/job/status",
        auth=(user, password) if password else None,
        json={"jobid": jobid}, timeout=60,
    )
    if response.status_code != 200:
        raise RcloneError(
            f"작업 상태 조회 실패 (HTTP {response.status_code}): {response.text[:300]}"
        )
    return response.json()


def wait_for_copy(jobid: int, *, max_wait_seconds: int = 1800,
                  poll_seconds: float = 2.0) -> dict:
    """복제가 끝날 때까지 기다린다.

    완료 판단 근거 (우선순위 순)
    ------------------------
    1. `finished` 가 True 이고 `error` 가 빈 문자열  → 정상 완료
    2. `finished` 가 True 이고 `error` 가 있음        → 실패
    3. 시간 초과                                     → 중단 후 실패

    성공/실패를 가리지 않고 DAG 를 실패시킨다. 조용히 넘어가면
    "복제됐다"고 잘못 보고하는 사고가 난다.
    """
    started_at = time.time()
    last_logged = 0.0

    while True:
        status = job_status(jobid)
        elapsed = time.time() - started_at
        finished = bool(status.get("finished"))
        error = str(status.get("error") or "")
        stats = status.get("stats", {}) or {}

        # 10초마다 한 번만 진행 상황을 남긴다(로그 폭탄 방지).
        if elapsed - last_logged >= 10:
            logger.info(
                "rclone 진행 중... %5.0f초  완료=%s  바이트=%s 오류=%s",
                elapsed, finished, stats.get("bytes", 0), stats.get("errors", 0))
            last_logged = elapsed

        if finished:
            result = {
                "jobid": jobid,
                "소요초": round(elapsed, 1),
                "옮긴바이트": int(status.get("bytes", 0) or 0),
                "옮긴파일": int(status.get("transfers", 0) or 0),
                "검사한파일": int(status.get("checks", 0) or 0),
                "오류": int(status.get("errors", 0) or 0),
                "오류메시지": error,
                "리모트메시지": str(status.get("output", "") or "")[:500],
            }
            if error or result["오류"] > 0:
                raise RcloneError(
                    f"rclone 복제 실패 (jobid={jobid})\n"
                    f"  오류: {error}\n"
                    f"  통계: {result}"
                )
            logger.info("rclone 복제 완료 (jobid=%s): %s", jobid, result)
            return result

        if elapsed >= max_wait_seconds:
            # 시간 초과 — 진행 중인 작업을 멈춰 놓고 실패한다.
            try:
                requests.post(f"{url}/job/stop",
                              auth=(user, password) if password else None,
                              json={"jobid": jobid}, timeout=30)
            except Exception:                    # noqa: BLE001
                pass
            raise RcloneError(
                f"rclone 복제 대기 시간 초과({max_wait_seconds}초), jobid={jobid}"
            )

        time.sleep(poll_seconds)


def copy_and_wait(src_path: str, dst_path: str, **kwargs) -> dict:
    """복제 + 완료 대기 convenience."""
    started = start_copy(src_path, dst_path, **kwargs)
    completed = wait_for_copy(started["jobid"])
    return {**started, **completed}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    try:
        info = check_connection()
        print("rclone rcd 연결 OK")
        for key, value in info.items():
            print(f"  {key}: {value}")
        print("리모트:", ", ".join(list_remotes()))
    except RcloneError as exc:
        print("연결 실패:")
        print(f"  {exc}")