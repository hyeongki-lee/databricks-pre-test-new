"""
Slack 알림 모듈.

발송 방식
--------
기존 etl-fabric 테스트에서 실제로 21건을 보낸 경로를 그대로 재사용한다.

  * 발송: Incoming Webhook (`SLACK_WEBHOOK_URL`)
  * 채널: Airflow Variable `SLACK_CHANNEL_ID`

Incoming Webhook 은 **메시지마다 채널을 지정할 수 없다.** 웹훅이 만들어진 채널로만
전송된다. 그래서 채널 ID 를 코드에 박아도 실제로는 그 웹훅이 연결된 곳으로 간다.
발송 위치는 캡처로 확인한다.

값을 읽는 순서
--------------
  1. Airflow Variable (컨테이너 안에서 실행할 때)
  2. 환경변수 (호스트에서 직접 실행할 때)
  3. 기본값

발송 기록
--------
보낸 메시지는 전부 `work/slack_sent.json` 에 누적한다.
매뉴얼의 증적(근거 자료)이 되므로 성공/실패를 가리지 않고 기록한다.
"""
from __future__ import annotations

import datetime as dt
import json
import os
from pathlib import Path
from typing import Any

import requests

try:
    from . import config
except ImportError:                       # 단독 실행(직접 스크립트) 대비
    import config                        # type: ignore

#: 발송 기록이 쌓이는 파일 이름(work 폴더 아래).
LOG_FILE_NAME = "slack_sent.json"

#: 심각도 → (이모지, 색상)
SEVERITY_STYLES = {
    "success": ("✅", "#1a6b3c"),
    "error":   ("💥", "#c0392b"),
    "warning": ("⚠️", "#b8860b"),
    "info":    ("ℹ️", "#2c5aa0"),
}


# ---------------------------------------------------------------------------
# 값 읽기
# ---------------------------------------------------------------------------

def _airflow_variable(name: str) -> str:
    """Airflow Variable 을 읽는다. 컨테이너 밖이면 빈 문자열을 돌려준다."""
    if os.environ.get("PRETEST_IN_CONTAINER") != "1":
        return ""
    try:
        from airflow.models import Variable
        return Variable.get(name, default_var="") or ""
    except Exception:
        return ""


def webhook_url() -> str:
    """수신 Webhook 주소."""
    return (_airflow_variable("SLACK_WEBHOOK_URL")
            or os.environ.get("SLACK_WEBHOOK_URL", "")
            or os.environ.get("SLACK_WEBHOOK", ""))


def channel_id() -> str:
    """채널 ID (기록용. 실제 전송 위치는 웹훅이 결정한다)."""
    return (_airflow_variable("SLACK_CHANNEL_ID")
            or os.environ.get("SLACK_CHANNEL_ID", "")
            or config.get_slack_config().get("channel", ""))


# ---------------------------------------------------------------------------
# 발송 기록
# ---------------------------------------------------------------------------

def _log_file_path() -> Path:
    """`work/slack_sent.json` 경로. work 폴더가 없으면 만들어 쓴다."""
    folder = config.log_folder()
    return folder / LOG_FILE_NAME


def _append_record(entry: dict) -> dict:
    """발송 1건을 누적한다. 실패해도 DAG 를 죽이지 않는다."""
    path = _log_file_path()
    try:
        document = {"채널": entry.get("채널"), "메시지목록": []}
        if path.exists():
            loaded = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict) and isinstance(loaded.get("메시지목록"), list):
                document = loaded
        entry = dict(entry)
        entry["일련번호"] = len(document["메시지목록"]) + 1
        document["메시지목록"].append(entry)
        document["총건수"] = len(document["메시지목록"])
        document["성공건수"] = sum(1 for m in document["메시지목록"]
                                   if m.get("성공여부") == "성공")
        document["마지막갱신"] = entry.get("발송시각")
        path.write_text(json.dumps(document, ensure_ascii=False, indent=2),
                        encoding="utf-8")
    except Exception as exc:               # noqa: BLE001
        print(f"[slack] 발송 기록 저장 실패(무시): {exc}")
    return entry


def sent_summary() -> dict:
    """지금까지 보낸 메시지 개요(매뉴얼 검증용)."""
    path = _log_file_path()
    if not path.exists():
        return {"총건수": 0, "성공건수": 0, "실패건수": 0}
    document = json.loads(path.read_text(encoding="utf-8"))
    items = document.get("메시지목록", [])
    succeeded = [m for m in items if m.get("성공여부") == "성공"]
    return {
        "총건수": len(items),
        "성공건수": len(succeeded),
        "실패건수": len(items) - len(succeeded),
        "종류별": sorted({str(m.get("종류")) for m in items}),
        "기록파일": str(path),
    }


# ---------------------------------------------------------------------------
# 페이로드
# ---------------------------------------------------------------------------

def _safe_text(value: Any, max_length: int = 1900) -> str:
    """Slack 이 거부하는 제어문자를 걷어내고 길이를 자른다."""
    if value is None:
        return "-"
    text = "".join(ch for ch in str(value) if ch in "\n\t" or ord(ch) >= 32)
    if len(text) <= max_length:
        return text or "-"
    return text[: max_length - 20] + " ...(잘림)"


def build_payload(title: str, fields: list[tuple[str, Any]],
                  severity: str = "info", detail: str | None = None) -> dict:
    """한국어 Block Kit 페이로드를 만든다."""
    emoji, color = SEVERITY_STYLES.get(severity, ("🔔", "#666666"))

    blocks: list[dict] = [{
        "type": "header",
        "text": {"type": "plain_text", "text": f"{emoji} {title}"[:150], "emoji": True},
    }]

    pairs: list[dict] = []
    for name, value in (fields or [])[:10]:
        pairs.append({"type": "mrkdwn",
                      "text": f"*{name}*\n{_safe_text(value)}"})
    if len(pairs) % 2:
        pairs.append({"type": "mrkdwn", "text": " "})
    for i in range(0, len(pairs), 2):
        blocks.append({"type": "section", "fields": pairs[i:i + 2]})

    if detail:
        blocks.append({"type": "section",
                       "text": {"type": "mrkdwn",
                                "text": f"```{_safe_text(detail, 1800)}```"}})

    blocks.append({"type": "context", "elements": [
        {"type": "mrkdwn",
         "text": f"databricks-pre-test-new · {dt.datetime.now():%Y-%m-%d %H:%M:%S}"}]})

    return {
        "text": f"{emoji} {title}",     # 알림 미리보기용(plain text 필수)
        "blocks": blocks,
        "attachments": [{"color": color, "blocks": []}],
    }


def _post_to_webhook(payload: dict) -> dict:
    """웹훅으로 보낸다."""
    url = webhook_url()
    if not url.startswith("https://hooks.slack.com"):
        return {"ok": False,
                "err": "웹훅 주소가 없습니다. Airflow Variable 'SLACK_WEBHOOK_URL' "
                       "또는 환경변수 SLACK_WEBHOOK_URL 을 설정하십시오."}
    try:
        response = requests.post(url, json=payload, timeout=20)
        # Incoming Webhook 은 200 + 본문 "ok" 를 돌려준다.
        if response.status_code == 200 and response.text.strip() == "ok":
            return {"ok": True, "경로": "incoming-webhook"}
        return {"ok": False, "res": f"{response.status_code} {response.text[:300]}"}
    except Exception as exc:                # noqa: BLE001
        return {"ok": False, "err": f"{type(exc).__name__}: {str(exc)[:200]}"}


# ---------------------------------------------------------------------------
# 공개 API
# ---------------------------------------------------------------------------

def notify(kind: str, title: str, fields: list[tuple[str, Any]], *,
           severity: str = "info", detail: str | None = None,
           dag_id: str = "", task_id: str = "",
           fail_silently: bool = True) -> dict:
    """Slack 메시지 1건을 보내고 결과를 돌려준다.

    Returns:
        {"종류","제목","채널","발송시각","성공여부","메시지"}
    """
    sent_at = dt.datetime.now().astimezone().isoformat(timespec="seconds")
    channel = channel_id()

    try:
        response = _post_to_webhook(build_payload(title, fields, severity, detail))
    except Exception as exc:                # noqa: BLE001
        response = {"ok": False, "err": f"{type(exc).__name__}: {str(exc)[:300]}"}

    result_flag = "성공" if response.get("ok") else "실패"
    record = {
        "발송시각": sent_at,
        "종류": kind,
        "제목": title,
        "심각도": severity,
        "채널": channel,
        "DAG이름": dag_id,
        "태스크": task_id,
        "성공여부": result_flag,
        "응답": json.dumps(response, ensure_ascii=False)[:500],
        "항목": [{"이름": str(k), "값": _safe_text(v, 400)} for k, v in (fields or [])],
    }
    if detail:
        record["상세"] = _safe_text(detail, 900)
    _append_record(record)

    result = {"종류": kind, "제목": title, "채널": channel, "발송시각": sent_at,
              "성공여부": result_flag,
              "메시지": (response.get("err") or response.get("res") or "")}
    if result_flag == "실패" and not fail_silently:
        raise RuntimeError(f"Slack 알림 발송 실패[{kind}]: {result['메시지']}")
    return result


# ---------------------------------------------------------------------------
# 자주 쓰는 형태
# ---------------------------------------------------------------------------

def format_duration(seconds: float | None) -> str:
    """초를 사람이 읽는 한국어로 바꾼다."""
    total = float(seconds or 0)
    if total < 60:
        return f"{round(total)}초"
    minutes, remainder = divmod(int(round(total)), 60)
    if minutes < 60:
        return f"{minutes}분 {remainder}초" if remainder else f"{minutes}분"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}시간 {minutes}분" if minutes else f"{hours}시간"


def notify_column_change(engine: str, schema: str, table: str,
                         added: list[str], removed: list[str],
                         *, dag_id: str = "", task_id: str = "") -> dict:
    """원천 컬럼 추가/삭제를 알린다.

    설계상 처리 방식이 다르다.
      * 삭제 → `NULL AS 컬럼` 으로 처리하고 작업을 계속한다.
      * 추가 → **반영하지 않는다.** 개발자가 프로파일을 고쳐야 한다.
    """
    title = f"원천 컬럼 변경 감지 — {engine}.{schema}.{table}"
    fields = [
        ("데이터베이스 종류", engine),
        ("스키마", schema),
        ("테이블", table),
        ("추가된 컬럼", ", ".join(added) if added else "없음"),
        ("삭제된 컬럼", ", ".join(removed) if removed else "없음"),
        ("추가 컬럼 처리", "적재하지 않았습니다. 개발자가 프로파일 등록 여부를 판단해 주십시오."),
        ("삭제 컬럼 처리", "NULL 로 대체해 적재를 계속했습니다."),
        ("사용자 조치", "프로파일 yaml 을 직접 수정해 주십시오. 모듈은 임의로 고치지 않습니다."),
    ]
    return notify("컬럼변경감지", title, fields, severity="warning",
                  dag_id=dag_id, task_id=task_id)


def notify_table_dropped(engine: str, schema: str, table: str,
                         registered_column_count: int,
                         *, dag_id: str = "", task_id: str = "") -> dict:
    """원천 테이블이 사라진 사실을 알린다. 작업은 하지 않는다."""
    title = f"원천 테이블 삭제 감지 — {engine}.{schema}.{table}"
    fields = [
        ("데이터베이스 종류", engine),
        ("스키마", schema),
        ("테이블", table),
        ("프로파일 등록 컬럼 수", registered_column_count),
        ("처리 방법", "작업을 하지 않고 건너뛰었습니다."),
        ("보안 장치", "프로파일 정의를 지우지도 활성 상태도 바꾸지 않았습니다."),
        ("사용자 조치", "복원 여부를 직접 판단해 주십시오."),
    ]
    return notify("테이블삭제감지", title, fields, severity="warning",
                  dag_id=dag_id, task_id=task_id)


def notify_rclone_done(engine: str, schema: str, folder_count: int,
                       elapsed_seconds: float,
                       *, dag_id: str = "", task_id: str = "") -> dict:
    """rclone RC API 복제가 끝났음을 알린다."""
    title = f"rclone 복제 완료 — {engine}.{schema} ({folder_count}개 폴더)"
    fields = [
        ("데이터베이스 종류", engine),
        ("스키마", schema),
        ("복제 폴더 수", folder_count),
        ("방식", "rclone rcd 원격 제어 API · copy 모드(증분)"),
        ("소요 시간", format_duration(elapsed_seconds)),
    ]
    return notify("rclone복제", title, fields, severity="success",
                  dag_id=dag_id, task_id=task_id)


def notify_run_summary(summary: dict, *, dag_id: str = "", task_id: str = "") -> dict:
    """ETL 실행 요약(성공/실패/건너뜀)을 보낸다.

    오류가 하나라도 있으면 실패로 승격한다. "실패 1건" 인데 성공 8건인 상황을
    제목만 보고 실패로 오해하지 않도록 성공 내역도 함께 싣는다.
    """
    succeeded = int(summary.get("성공", 0) or 0)
    failed = int(summary.get("실패", 0) or 0)
    skipped = int(summary.get("건너뜀", 0) or 0)
    total = int(summary.get("전체대상", 0) or 0)
    failed_list = list(summary.get("실패목록") or [])

    fields = [
        ("실행 DAG", summary.get("DAG이름", "-")),
        ("Airflow 실행 ID", summary.get("Airflow실행ID", "-")),
        ("데이터베이스 종류", summary.get("엔진", "-")),
        ("스키마", summary.get("스키마", "-")),
        ("동시 처리 개수", summary.get("동시처리개수", 1)),
        ("대상 테이블", f"{total}개"),
        ("성공 / 실패 / 건너뜀", f"{succeeded} / {failed} / {skipped}"),
        ("소요 시간", format_duration(summary.get("소요초"))),
    ]
    if summary.get("성공목록"):
        fields.append(("성공한 테이블", ", ".join(summary["성공목록"][:10])))
    if failed_list:
        fields.append(("실패한 테이블",
                       "\n".join(f"• {t} : {r}" for t, r in failed_list[:8])))

    if failed or (total and not succeeded and not skipped):
        title = (f"ETL 부분 성공 — 성공 {succeeded}건, 실패 {failed}건 (대상 {total}건 중)"
                 if succeeded else
                 f"ETL 실패 — 성공 0건 (대상 {total}건)")
        severity = "warning" if succeeded else "error"
        kind = "부분성공" if succeeded else "실패"
    elif total and not succeeded and not skipped:
        title = f"ETL 실패 — 대상 {total}건이 모두 처리되지 않았습니다"
        severity, kind = "error", "실패"
    elif skipped:
        title = (f"ETL 부분 성공 — 성공 {succeeded}건, 건너뜀 {skipped}건"
                 if succeeded else f"ETL 건너뜀 — {skipped}건")
        severity, kind = "warning", "건너뜀"
    elif total:
        title = f"ETL 성공 — {succeeded}건"
        severity, kind = "success", "성공"
    else:
        title = "ETL 실패 — 처리 대상이 0건입니다"
        severity, kind = "error", "실패"

    return notify(kind, title, fields, severity=severity,
                  dag_id=dag_id, task_id=task_id)


if __name__ == "__main__":
    import sys
    if "--요약" in sys.argv or "--summary" in sys.argv:
        print(json.dumps(sent_summary(), ensure_ascii=False, indent=2))
    else:
        result = notify("연결확인",
                        "databricks-pre-test-new Slack 연동 확인",
                        [("목적", "기존 etl-fabric 테스트 공간 재사용 확인"),
                         ("프로젝트", "databricks-pre-test-new"),
                         ("발송 방식", "Incoming Webhook")],
                        severity="info")
        print(json.dumps(result, ensure_ascii=False, indent=2))