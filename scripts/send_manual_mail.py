"""
매뉴얼 메일 발송.

수신자 : mercy.lee@kakaopaycorp.com

왜 첨부와 HTML 본문을 함께 보내는가
---------------------------------
매뉴얼은 캡처 이미지가 base64 로 내장돼 있어 1 MB 넘는다. 어떤 메일 클라이언트는
그 크기의 HTML 본문을 잘라먹는다. 그래서 두 갈래로 보낸다.

    본문  : 핵심 요약 + 브라우저 링크  (항상 정상 표시)
    첨부  : 전체 HTML 파일            (원할 때 오프라인 열람)

수치는 메일과 매뉴얼이 **같은 evidence.json** 에서 읽는다. 두 곳에서 따로 만들면
언젠가는 반드시 어긋난다.

필요한 값은 `.env` 에서 읽는다 (값을 화면에 찍지 않는다).
"""
from __future__ import annotations

import json
import mimetypes
import os
import smtplib
import sys
from datetime import datetime
from email.message import EmailMessage
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MANUAL = ROOT / "manual" / "databricks-pre-test-manual.html"
EVIDENCE = ROOT / "work" / "evidence.json"
CDC_RESULT = ROOT / "work" / "cdc" / "cdc_all_result.json"

OWNER = "hyeongki-lee"
REPO = f"{OWNER}/databricks-pre-test-new"
REPO_URL = f"https://github.com/{REPO}"

# Measured: Pages serves the repository root, so the path includes `manual/`.
# Without it the site returns 404 ("The site configured at this address does
# not contain the requested file"), because there is no index.html at the root.
PAGES_URL = (f"https://{OWNER}.github.io/databricks-pre-test-new"
             f"/{MANUAL.parent.name}/{MANUAL.name}")

DEFAULT_TO = "mercy.lee@kakaopaycorp.com"
REQUIRED_SCALE = 50000


# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------

def load_env() -> dict[str, str]:
    """Read `.env` files without ever printing a value."""
    env: dict[str, str] = dict(os.environ)
    for candidate in (Path(r"C:\Users\lee21\lakehouse\.env"),
                      ROOT / ".env"):
        if not candidate.exists():
            continue
        for line in candidate.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            env.setdefault(key.strip(), value.strip().strip('"').strip("'"))
    return env


# ---------------------------------------------------------------------------
# Summary, read from the same evidence the manual uses
# ---------------------------------------------------------------------------

def build_summary() -> dict:
    data: dict = {}
    if EVIDENCE.exists():
        data = json.loads(EVIDENCE.read_text(encoding="utf-8"))

    sources = data.get("원천") or {}

    # 검증 규모는 원천 집계에서 나눠 구하지 않는다.
    # Measured: 그 결과는 CDC 시나리오가 지운 행까지 평균에 섞여 49,995 가 되고,
    # "요구사항 5만 건 미달" 로 잘못 표시된다. 초기 이관이 파일의 원본 건수를
    # 그대로 기록한 값이 권위 있는 답이다 (매뉴얼과 동일하게 맞춘다).
    logs = data.get("로그") or {}
    scale_rows = [int(r.get("source_count") or 0)
                  for r in (logs.get("초기이관_규모별") or [])
                  if isinstance(r, dict)]
    scale_rows = [v for v in scale_rows if v > 0]
    per_table = (max(set(scale_rows), key=scale_rows.count)
                 if scale_rows else 0)

    cdc: dict = {}
    if CDC_RESULT.exists():
        cdc = json.loads(CDC_RESULT.read_text(encoding="utf-8"))

    logs = data.get("로그") or {}

    def as_int(value) -> int:
        """Counts come back from Databricks as strings ('390'), so a plain
        `f"{value:,}"` raises `Cannot specify ',' with 's'`."""
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    return {
        "per_table": per_table,
        "tables": as_int((sources.get("합계") or {}).get("테이블", 0)),
        "rows": as_int((sources.get("합계") or {}).get("건수", 0)),
        "s3_objects": as_int((data.get("S3") or {}).get("전체객체", 0)),
        "load_audit": as_int((logs.get("load_audit") or {}).get("건수", 0)),
        "etl_run_log": as_int((logs.get("etl_run_log") or {}).get("건수", 0)),
        "slack": as_int((data.get("slack") or {}).get("총건수", 0)),
        "deid_ok": bool((data.get("비식별화") or {}).get("일치여부")),
        "cdc_verdict": cdc.get("전체판정", "-"),
        "cdc_pass": cdc.get("통과", 0),
        "collected": data.get("수집시각", "-"),
    }


def _row(label: str, value: str, note: str = "") -> str:
    border = "border-bottom:1px solid #eee"
    return (
        f"<tr><td style='padding:6px 12px;{border}'>{label}</td>"
        f"<td style='padding:6px 12px;{border};text-align:right;"
        f"font-weight:bold'>{value}</td>"
        f"<td style='padding:6px 12px;{border};color:#666;font-size:12px'>"
        f"{note}</td></tr>")


def summary_html(s: dict) -> str:
    scale_ok = ("통과" if s["per_table"] >= REQUIRED_SCALE
                else f"미달 ({s['per_table']:,}건)")
    return f"""
    <table cellpadding="0" cellspacing="0" style="border-collapse:collapse;
           font-size:13px;border:1px solid #ddd">
      {_row('원천 테이블', f"{s['tables']:,}개", 'MySQL · MongoDB · PostgreSQL')}
      {_row('테이블당 행 수', f"{s['per_table']:,}건",
            f"요구사항 {REQUIRED_SCALE:,}건 — {scale_ok}")}
      {_row('원천 총 행 수', f"{s['rows']:,}행", '')}
      {_row('S3 객체', f"{s['s3_objects']:,}개",
            'chk · parquet · Iceberg metadata · rclone_done')}
      {_row('초기 이관 기록', f"{s['load_audit']:,}행", 'load_audit')}
      {_row('ETL 기록', f"{s['etl_run_log']:,}행", 'etl_run_log')}
      {_row('비식별화 D1·D2·D3', '일치' if s['deid_ok'] else '불일치',
            'Databricks 계산값 = 파이썬 참조값')}
      {_row('CDC 3엔진 검증', s['cdc_verdict'],
            f"{s['cdc_pass']}/3 엔진 · 갱신·삭제·신규 4단계")}
      {_row('Slack 알림', f"{s['slack']:,}건", '기존 테스트 채널')}
    </table>"""


BODY_TEMPLATE = """<!DOCTYPE html>
<html><head><meta charset="utf-8"></head>
<body style="margin:0;background:#f4f6f8;
      font-family:'Malgun Gothic','Apple SD Gothic Neo',sans-serif;color:#16212e">
<div style="max-width:760px;margin:0 auto;padding:28px 20px">

  <div style="background:#fff;border:1px solid #dde2e8;border-radius:10px;
              padding:24px 26px">
    <h1 style="margin:0 0 6px;font-size:20px">
      Databricks ETL 마이그레이션 검증 — 결과 공유</h1>
    <p style="margin:0 0 18px;color:#5a6674;font-size:14px;line-height:1.7">
      3개 원천 DB → Iceberg 형식 파일 → rclone RC API → S3 → Auto Loader →
      managed 테이블 → 정규 ETL(비식별화 포함) → CDC 까지 한 경로로 묶어
      검증했습니다. 상세 매뉴얼에 코드 · SQL · 실측 수치 · 화면 캡처 ·
      <b>실측에서 발견한 함정과 그 원인</b>을 모두 담았습니다.</p>

    <div style="margin:0 0 18px">
      <a href="{pages}"
         style="display:inline-block;background:#0b4f9e;color:#fff;
                padding:11px 20px;border-radius:7px;text-decoration:none;
                font-weight:bold;font-size:14px">브라우저에서 매뉴얼 열기</a>
      <a href="{repo}"
         style="display:inline-block;margin-left:8px;color:#0b4f9e;
                padding:11px 4px;text-decoration:none;font-size:14px">
         GitHub 저장소</a>
    </div>
{summary}

    <p style="margin:18px 0 0;color:#5a6674;font-size:13px;line-height:1.8">
      전체 매뉴얼(캡처 이미지 내장)은 이 메일에 <b>첨부</b>했습니다.
      첨부를 열지 않고 위 링크로 보셔도 동일합니다.<br>
      수치는 모두 Databricks · S3 · 원천 DB 에 직접 질의해 만든 값이며,
      수집 시각은 {collected} 입니다.</p>
  </div>

  <p style="text-align:center;color:#8a94a0;font-size:12px;margin-top:20px">
    databricks-pre-test-new · {sent}</p>
</div>
</body></html>"""


# ---------------------------------------------------------------------------
# Send
# ---------------------------------------------------------------------------

def main() -> int:
    env = load_env()
    host = env.get("SMTP_HOST")
    port = int(env.get("SMTP_PORT", "587"))
    user = env.get("SMTP_USER")
    # Measured: this environment names the secret `SMTP_PASS`, not
    # `SMTP_PASSWORD`. Accept either so the script works with both layouts.
    password = env.get("SMTP_PASSWORD") or env.get("SMTP_PASS")
    sender = env.get("EMAIL_FROM") or user
    # Same story for the recipient key: `EMAIL_TO` here, `MAIL_TO` elsewhere.
    recipient = (env.get("MAIL_TO") or env.get("EMAIL_TO") or DEFAULT_TO)

    missing = [name for name, value in
               (("SMTP_HOST", host), ("SMTP_USER", user),
                ("SMTP_PASSWORD", password), ("EMAIL_FROM", sender))
               if not value]
    if missing:
        print(f"누락된 환경값: {', '.join(missing)}")
        print("C:\\Users\\lee21\\lakehouse\\.env 또는 .env 에 설정하십시오.")
        return 2

    if not MANUAL.exists():
        print(f"매뉴얼이 없습니다: {MANUAL}")
        print("먼저 `python scripts/build_manual.py` 를 실행하십시오.")
        return 1

    summary = build_summary()
    html = BODY_TEMPLATE.format(
        repo=REPO_URL, pages=PAGES_URL, summary=summary_html(summary),
        collected=summary["collected"],
        sent=datetime.now().strftime("%Y-%m-%d %H:%M"))

    message = EmailMessage()
    message["Subject"] = ("[Databricks 전환 검증] ETL 파이프라인 검증 결과 및 상세 매뉴얼")
    message["From"] = sender
    message["To"] = recipient
    message.set_content(
        "Databricks ETL 마이그레이션 검증 결과를 정리해 보냅니다.\n\n"
        f"브라우저에서 보기 : {PAGES_URL}\n"
        f"GitHub 저장소     : {REPO_URL}\n\n"
        f"테이블당 행 수    : {summary['per_table']:,}건\n"
        f"CDC 3엔진 검증    : {summary['cdc_verdict']}\n"
        f"비식별화 검증     : {'일치' if summary['deid_ok'] else '불일치'}\n\n"
        "상세 매뉴얼은 HTML 첨부로 함께 보냅니다.")
    message.add_alternative(html, subtype="html")

    attachment = MANUAL.read_bytes()
    maintype, _, subtype = (mimetypes.guess_type(MANUAL.name)[0]
                            or "text/html").partition("/")
    message.add_attachment(attachment, maintype=maintype, subtype=subtype,
                           filename=MANUAL.name)

    print(f"발송 대상 : {sender} → {recipient}")
    print(f"첨부     : {MANUAL.name} ({len(attachment) / 1024 / 1024:.2f} MB)")
    print(f"서버     : {host}:{port}")

    with smtplib.SMTP(host, port, timeout=60) as server:
        server.starttls()
        server.login(user, password)
        server.send_message(message)

    print(f"발송 완료 · {datetime.now():%Y-%m-%d %H:%M:%S}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())