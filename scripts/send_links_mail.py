"""
링크 안내 메일 발송.

대시보드 · 매뉴얼 · 저장소 주소를 정리해 보낸다.

이전 매뉴얼 메일(`scripts/send_manual_mail.py`)과 다른 점
---------------------------------------------------------
이 메일은 첨부 없이 **접속 방법 안내**에 초점을 둔다. 특히 대시보드는
"GitHub 로는 읽기 전용, 실시간 저장은 로컬" 이라는 구분이 필요하므로
그 차이를 본문에 분명히 적는다.
"""
from __future__ import annotations

import json
import os
import smtplib
import sys
from datetime import datetime
from email.message import EmailMessage
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.stdout.reconfigure(encoding="utf-8")

OWNER = "hyeongki-lee"
REPO = "databricks-pre-test-new"
REPO_URL = f"https://github.com/{OWNER}/{REPO}"
PAGES = f"https://{OWNER}.github.io/{REPO}"
MANUAL = f"{PAGES}/manual/databricks-pre-test-manual.html"
DASHBOARD = f"{PAGES}/docs/dashboard.html"

TO = "mercy.lee@kakaopaycorp.com"
ENV_CANDIDATES = (Path(r"C:\Users\lee21\lakehouse\.env"), ROOT / ".env")


def load_env() -> dict[str, str]:
    env: dict[str, str] = dict(os.environ)
    for path in ENV_CANDIDATES:
        if not path.exists():
            continue
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            env.setdefault(key.strip(), value.strip().strip('"').strip("'"))
    return env


def link(url: str, label: str) -> str:
    return (f'<a href="{url}" style="display:block;padding:11px 15px;'
            f'margin-bottom:8px;border:1px solid #dde2e8;border-radius:7px;'
            f'background:#fbfcfd;text-decoration:none;color:#0b4f9e">'
            f'<b style="font-size:14px">{label}</b>'
            f'<div style="font-size:12px;color:#5a6674;margin-top:3px">'
            f'{url}</div></a>')


BODY = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"></head>
<body style="margin:0;background:#f4f6f8;
      font-family:'Malgun Gothic','Apple SD Gothic Neo',sans-serif;color:#16212e">
<div style="max-width:760px;margin:0 auto;padding:28px 20px">

  <div style="background:#fff;border:1px solid #dde2e8;border-radius:10px;
              padding:24px 26px">
    <h1 style="margin:0 0 6px;font-size:20px">
      Databricks ETL 검증 — 접속 주소 안내</h1>
    <p style="margin:0 0 20px;color:#5a6674;font-size:14px;line-height:1.75">
      ETL 대시보드와 상세 매뉴얼에 접속할 수 있는 주소입니다.
      GitHub Pages 로는 <b>열람만</b> 가능하고, 저장까지 하려면
      <b>로컬에서 실행</b>하셔야 합니다. 아래에 둘 다 적었습니다.</p>

    <div style="font-weight:bold;font-size:14px;margin-bottom:10px">
      1. GitHub Pages — 아무 브라우저나, 읽기 전용</div>
    {link(DASHBOARD, "ETL 대시보드 (스키마 12개 · 테이블 60개)")}
    {link(MANUAL, "상세 검증 매뉴얼 (20개 장 · 화면 캡처 포함)")}
    {link(REPO_URL, "GitHub 저장소 (소스 코드)")}

    <div style="border-left:4px solid #9a6700;background:#fdf8ec;
                padding:12px 15px;border-radius:0 8px 8px 0;margin:16px 0;
                font-size:13.5px;line-height:1.75">
      <b>왜 일부 기능이 안 되는가</b><br>
      GitHub Pages 는 정적 파일만 제공해서 Python 을 실행하지 못합니다.
      그래서 대시보드는 프로파일 YAML 을 읽어 만든 <b>읽기 전용 화면</b>입니다.
      <ul style="margin:8px 0 0;padding-left:20px">
        <li>안 됨 — 원천 DB · Databricks 실시간 조회</li>
        <li>안 됨 — 컬럼 체크박스로 exclude_columns 저장</li>
        <li>안 됨 — 신규 테이블 등록</li>
      </ul></div>

    <div style="font-weight:bold;font-size:14px;margin:22px 0 10px">
      2. 로컬 실행 — 실시간 조회와 저장 모두 가능</div>
    <pre style="background:#16212e;color:#e2e8f0;padding:14px 16px;
                border-radius:8px;overflow-x:auto;font-size:12.5px;
                line-height:1.7;margin:0 0 10px">cd C:\\Users\\lee21\\OneDrive\\문서\\Default Project\\databricks-pre-test-new
C:\\Users\\lee21\\AppData\\Local\\Programs\\Python\\Python312\\python.exe ^
    dashboard\\app.py --port 8540</pre>
    <p style="margin:0 0 18px;font-size:13.5px">
      브라우저에서 <b>http://127.0.0.1:8540</b> 접속<br>
      DB 컨테이너(MySQL 3306 · MongoDB 27017 · PostgreSQL 5432)가 떠 있어야
      실시간 조회가 됩니다. 없으면 lakehouse 폴더에서
      <code>docker compose up -d</code> 를 먼저 실행하십시오.</p>

    <div style="font-weight:bold;font-size:14px;margin:22px 0 10px">
      3. 확인된 검증 결과</div>
    <table cellpadding="0" cellspacing="0"
           style="border-collapse:collapse;font-size:13px;border:1px solid #ddd">
      <tr style="background:#eef2f6">
        <td style="padding:6px 12px;border-bottom:1px solid #eee;font-weight:bold">항목</td>
        <td style="padding:6px 12px;border-bottom:1px solid #eee;font-weight:bold;text-align:right">결과</td>
      </tr>
      <tr><td style="padding:6px 12px;border-bottom:1px solid #eee">원천 테이블</td>
          <td style="padding:6px 12px;border-bottom:1px solid #eee;text-align:right;font-weight:bold">60개 × 5만건</td></tr>
      <tr><td style="padding:6px 12px;border-bottom:1px solid #eee">초기 이관</td>
          <td style="padding:6px 12px;border-bottom:1px solid #eee;text-align:right;font-weight:bold">120건 전부 건수 일치</td></tr>
      <tr><td style="padding:6px 12px;border-bottom:1px solid #eee">정규 ETL</td>
          <td style="padding:6px 12px;border-bottom:1px solid #eee;text-align:right;font-weight:bold">72건 전부 성공</td></tr>
      <tr><td style="padding:6px 12px;border-bottom:1px solid #eee">CDC</td>
          <td style="padding:6px 12px;border-bottom:1px solid #eee;text-align:right;font-weight:bold">3엔진 × 4단계 통과</td></tr>
      <tr><td style="padding:6px 12px;border-bottom:1px solid #eee">비식별화 D1·D2·D3</td>
          <td style="padding:6px 12px;border-bottom:1px solid #eee;text-align:right;font-weight:bold">Databricks 값 = 참조값</td></tr>
      <tr><td style="padding:6px 12px">Slack 알림</td>
          <td style="padding:6px 12px;text-align:right;font-weight:bold">160건 / 실패 0</td></tr>
    </table>

    <p style="margin:18px 0 0;color:#5a6674;font-size:13px;line-height:1.8">
      ⚠ 남은 요청 사항 — Slack Incoming Webhook URL 이 Git 이력에 올라간 적이
      있어 <b>Slack 에서 해당 웹훅을 삭제하고 새로 만들어 주십시오</b>
      (이력에서 지웠다고 안전해지지 않습니다). 새 URL 은 프로젝트
      <code>.env</code> 의 <code>SLACK_WEBHOOK_URL</code> 에 넣습니다.
      자세한 절차는 저장소의 <code>.env.example</code> 에 적어 두었습니다.</p>
  </div>

  <p style="text-align:center;color:#8a94a0;font-size:12px;margin-top:20px">
    databricks-pre-test-new · __SENT__</p>
</div>
</body></html>"""


def main() -> int:
    env = load_env()
    host = env.get("SMTP_HOST")
    port = int(env.get("SMTP_PORT", "587"))
    user = env.get("SMTP_USER")
    # 이 환경은 `SMTP_PASS` 다 (`SMTP_PASSWORD` 아님).
    password = env.get("SMTP_PASSWORD") or env.get("SMTP_PASS")
    sender = env.get("EMAIL_FROM") or user
    recipient = env.get("MAIL_TO") or env.get("EMAIL_TO") or TO

    missing = [n for n, v in (("SMTP_HOST", host), ("SMTP_USER", user),
                               ("SMTP_PASSWORD", password),
                               ("EMAIL_FROM", sender)) if not v]
    if missing:
        print(f"누락된 환경값: {', '.join(missing)}")
        return 2

    print("링크 사전 확인:")
    import urllib.request
    for name, url in (("대시보드", DASHBOARD), ("매뉴얼", MANUAL),
                      ("저장소", REPO_URL)):
        try:
            with urllib.request.urlopen(url, timeout=60) as response:
                print(f"  OK   {response.status}  {name}")
        except Exception as exc:                # noqa: BLE001
            print(f"  FAIL {type(exc).__name__}  {name}  {url}")

    # BODY 는 f-string 이므로 `{...}` 를 쓰는 자리표시자는 이미 평가돼 있다.
    # 그래서 치환 표식은 __SENT__ 처럼 충돌이 없는 이름으로 둔다.
    html = BODY.replace("__SENT__", datetime.now().strftime("%Y-%m-%d %H:%M"))

    message = EmailMessage()
    message["Subject"] = "[Databricks 전환 검증] 대시보드 · 매뉴얼 접속 주소"
    message["From"] = sender
    message["To"] = recipient
    message.set_content(
        "Databricks ETL 검증 결과물을 보냅니다.\n\n"
        f"ETL 대시보드 (읽기 전용) : {DASHBOARD}\n"
        f"상세 매뉴얼            : {MANUAL}\n"
        f"GitHub 저장소          : {REPO_URL}\n\n"
        "GitHub Pages 는 정적 파일만 제공하므로 Python 을 실행하지 못합니다.\n"
        "따라서 대시보드는 읽기 전용 화면이며, 실시간 조회와 저장은\n"
        "로컬 실행이 필요합니다.\n\n"
        "로컬 실행:\n"
        "  cd C:\\Users\\lee21\\OneDrive\\문서\\Default Project\\databricks-pre-test-new\n"
        "  python dashboard\\app.py --port 8540\n"
        "  → http://127.0.0.1:8540\n\n"
        "⚠ Slack Incoming Webhook URL 이 Git 이력에 올라간 적이 있어\n"
        "  Slack 에서 해당 웹훅을 삭제하고 새로 만들어 주십시오.\n")
    message.add_alternative(html, subtype="html")

    print(f"\n발송 대상 : {sender} → {recipient}")
    with smtplib.SMTP(host, port, timeout=60) as server:
        server.starttls()
        server.login(user, password)
        server.send_message(message)
    print(f"발송 완료 · {datetime.now():%Y-%m-%d %H:%M:%S}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())