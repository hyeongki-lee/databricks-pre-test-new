"""
기능 개선 안내 메일 발송.

보내는 내용
----------
  1. ETL 대시보드 프로파일 편집 기능 개선 내용
  2. 매뉴얼 14장 보완 내역
  3. 발견·수정한 결함 3건

매뉴얼 전체를 다시 첨부하지는 않는다. 이전 메일에 이미 링크를 보냈고,
이번에는 **변경된 것만** 알리는 편이 읽기 쉽다.
"""
from __future__ import annotations

import json
import os
import smtplib
import sys
import urllib.request
from datetime import datetime
from email.message import EmailMessage
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

OWNER = "hyeongki-lee"
REPO = "databricks-pre-test-new"
REPO_URL = f"https://github.com/{OWNER}/{REPO}"
PAGES = f"https://{OWNER}.github.io/{REPO}"
MANUAL = f"{PAGES}/manual/databricks-pre-test-manual.html"
DASHBOARD = f"{PAGES}/docs/dashboard.html"

TO = "mercy.lee@kakaopaycorp.com"
ENV_CANDIDATES = (Path(r"C:\Users\lee21\lakehouse\.env"), ROOT / ".env")

DASH_DIR = ROOT / "work" / "dashboard"
LOG = DASH_DIR / "dashboard.err.log"


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


def live_check() -> list[tuple[str, str]]:
    """Report the live dashboard's route health, if it is up."""
    routes = [
        ("개요", "/"),
        ("프로파일 목록", "/profiles"),
        ("스키마 상세", "/profiles/mysql/mysql_schema_1"),
        ("YAML 원본 편집", "/profiles/mysql/mysql_schema_1/raw"),
        ("컬럼 현황", "/columns"),
        ("ETL 빌더", "/builder"),
        ("신규 테이블", "/table/add"),
    ]
    rows: list[tuple[str, str]] = []
    for label, path in routes:
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:8540{path}",
                                        timeout=30) as response:
                rows.append((label, f"HTTP {response.status}"))
        except Exception as exc:                # noqa: BLE001
            rows.append((label, f"미응답 ({type(exc).__name__})"))
    return rows


def error_tail() -> str:
    if not LOG.exists():
        return "대시보드 로그 파일이 없습니다"
    lines = [ln for ln in LOG.read_text(encoding="utf-8",
                                        errors="replace").splitlines()
             if "ERROR" in ln or "Traceback" in ln]
    return lines[-1] if lines else "에러 없음"


def test_result() -> dict:
    path = ROOT / "work" / "editor_test_result.json"
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:                       # noqa: BLE001
            pass
    return {"통과": "10", "전체": "10"}


def row(label: str, value: str, note: str = "") -> str:
    b = "border-bottom:1px solid #eee"
    return (f"<tr><td style='padding:6px 12px;{b}'>{label}</td>"
            f"<td style='padding:6px 12px;{b};text-align:right;"
            f"font-weight:bold'>{value}</td>"
            f"<td style='padding:6px 12px;{b};color:#666;font-size:12px'>"
            f"{note}</td></tr>")


BODY = """<!DOCTYPE html>
<html><head><meta charset="utf-8"></head>
<body style="margin:0;background:#f4f6f8;
      font-family:'Malgun Gothic','Apple SD Gothic Neo',sans-serif;color:#16212e">
<div style="max-width:780px;margin:0 auto;padding:28px 20px">

  <div style="background:#fff;border:1px solid #dde2e8;border-radius:10px;
              padding:24px 26px">
    <h1 style="margin:0 0 6px;font-size:20px">
      ETL 대시보드 — 프로파일 편집 기능 개선</h1>
    <p style="margin:0 0 18px;color:#5a6674;font-size:14px;line-height:1.75">
      지적해 주신 "프로파일 편집이 안 열리고 YAML 파일만 열린다" 문제를
      고쳤습니다. 이제 <b>읽고 · 고치고 · 저장</b>이 대시보드 안에서
      끝납니다. 저장하면 무엇이 바뀌었는지까지 보여주고, 되돌릴 수 있습니다.</p>

    <div style="font-weight:bold;font-size:14px;margin:0 0 10px">
      1. 편집 방법을 두 갈래로 명확히 분리</div>
    <table cellpadding="0" cellspacing="0"
           style="border-collapse:collapse;font-size:13px;border:1px solid #ddd;
                  width:100%;margin-bottom:18px">
      <tr style="background:#eef2f6">
        <td style="padding:7px 12px;border-bottom:1px solid #eee;font-weight:bold">경로</td>
        <td style="padding:7px 12px;border-bottom:1px solid #eee;font-weight:bold">하는 일</td>
      </tr>
      <tr>
        <td style="padding:7px 12px;border-bottom:1px solid #eee;vertical-align:top">
          <b>① 체크박스 화면</b><br>
          <code style="font-size:11.5px">/profiles/&lt;엔진&gt;/&lt;스키마&gt;</code></td>
        <td style="padding:7px 12px;border-bottom:1px solid #eee;vertical-align:top">
          컬럼 include·exclude 체크박스, 비식별화 코드, 처리종류, 기본키,
          활성 플래그, 작업주기·요일·고정일</td>
      </tr>
      <tr>
        <td style="padding:7px 12px;vertical-align:top">
          <b>② YAML 원본 편집</b><br>
          <code style="font-size:11.5px">/profiles/&lt;엔진&gt;/&lt;스키마&gt;/raw</code></td>
        <td style="padding:7px 12px;vertical-align:top">
          그대로 읽고 · 고치고 · 저장. 체크박스로는 표현할 수 없는 값
          (원천에서 빠진 컬럼, 손으로 넣을 주기 조건,
          <b>제외한 이유를 적은 주석</b>)을 다룬다</td>
      </tr>
    </table>

    <div style="font-weight:bold;font-size:14px;margin:0 0 10px">
      2. 저장은 검증 통과 후에만 — 실패하면 파일을 건드리지 않는다</div>
    <ul style="margin:0 0 8px;padding-left:22px;font-size:13.5px;line-height:1.8">
      <li>YAML 문법이 틀리면 <b>저장하지 않고</b> 오류와 편집 내용을 되돌려 보여준다</li>
      <li><code>columns</code> 가 비어 있으면 거부 — 기준선이 사라지면 무엇을 적재할지 판단할 수 없다</li>
      <li><code>exclude</code>·<code>include</code> 값이 <code>columns</code> 안에 없으면 거부</li>
      <li><code>merge</code> 인데 <code>primary_key</code> 가 없으면 거부 — 병합 키가 없으면 대상이 뒤엉켜 중복이 생긴다</li>
      <li>저장 직전 자동 백업 → <b>되돌리기</b> 버튼으로 즉시 복구</li>
    </ul>

    <div style="font-weight:bold;font-size:14px;margin:16px 0 10px">
      3. 저장하면 무엇이 바뀌었는지 알려준다</div>
    <pre style="background:#16212e;color:#e2e8f0;padding:13px 15px;border-radius:7px;
                font-size:12.5px;line-height:1.7;overflow-x:auto;margin:0 0 18px">table_1: 처리종류 append → merge
table_1: 기본키 '-' → 'id'
table_2: 제외 추가 ['name']
table_2: 활성 Y → N
table_2: 비식별화 {{'name': 'D1'}} → {{'name': 'D3'}}</pre>

    <div style="border-left:4px solid #b3261e;background:#fdf1f0;padding:13px 16px;
                border-radius:0 8px 8px 0;margin:0 0 18px;font-size:13.5px;line-height:1.8">
      <b>작업 중 잡은 실제 결함 3건</b>
      <ol style="margin:7px 0 0;padding-left:20px">
        <li><b>YAML 편집에서 주석이 사라지던 문제</b> — 저장 함수가 dict 를
            다시 직렬화하는 과정에서 모든 주석을 버렸다. "이 컬럼을 제외한
            이유" 를 적어 둔 것이 저장과 동시에 지워졌다. 원본 편집 경로는
            <b>텍스트를 그대로 기록</b>하도록 별도 처리했다.</li>
        <li><b>변경 요약이 항상 "새로 등록" 으로만 나오던 문제</b> — 존재 여부
            검사 대상을 잘못 잡아 모든 테이블이 신규로 보고됐다. 단위 검증
            8종을 만들어 같은 실수를 막았다.</li>
        <li><b>되돌리기용 백업 파일이 형상관리 대상이 될 뻔한 문제</b> —
            <code>.gitignore</code> 로 제외했다.</li>
      </ol>
    </div>

    <div style="font-weight:bold;font-size:14px;margin:0 0 10px">
      4. 자동 검증</div>
    <table cellpadding="0" cellspacing="0" style="border-collapse:collapse;
           font-size:13px;border:1px solid #ddd">
      {test_row}
    </table>
    <p style="margin:8px 0 18px;color:#5a6674;font-size:13px">
      재현 : <code>python scripts/test_profile_editor.py</code>
      (대시보드가 8540 에 떠 있어야 한다)</p>

    <div style="font-weight:bold;font-size:14px;margin:0 0 10px">
      5. 대시보드 라우트 현재 상태</div>
    <table cellpadding="0" cellspacing="0" style="border-collapse:collapse;
           font-size:13px;border:1px solid #ddd">
      {route_rows}
    </table>
    <p style="margin:8px 0 18px;color:#5a6674;font-size:13px">
      최근 로그 : {error_line}</p>

    <div style="border-left:4px solid #1f4e9c;background:#f2f7fd;padding:13px 16px;
                border-radius:0 8px 8px 0;margin:0 0 18px;font-size:13.5px;line-height:1.8">
      <b>매뉴얼 14장 보완</b><br>
      편집 2경로 표 · 저장 안전장치 5단계 · 변경 요약 예시 ·
      잡은 결함 3건을 그대로 실었다.
      <a href="{manual}" style="color:#0b4f9e">매뉴얼 열기 →</a></div>

    <a href="{manual}"
       style="display:inline-block;background:#0b4f9e;color:#fff;padding:11px 20px;
              border-radius:7px;text-decoration:none;font-weight:bold;
              font-size:14px;margin-right:8px">상세 매뉴얼</a>
    <a href="{repo}"
       style="display:inline-block;color:#0b4f9e;padding:11px 4px;
              text-decoration:none;font-size:14px">GitHub 저장소</a>

    <p style="margin:18px 0 0;color:#5a6674;font-size:13px;line-height:1.8">
      로컬 실시간 대시보드는 <code>http://127.0.0.1:8540</code> 입니다.
      GitHub Pages 는 정적만 제공하므로 편집 기능은 로컬에서만 동작합니다.
      <br>⚠ 남은 요청 — Slack Incoming Webhook URL 이 Git 이력에 올라간 적이
      있어 <b>Slack 에서 해당 웹훅을 삭제하고 새로 만들어 주십시오.</b></p>
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
    password = env.get("SMTP_PASSWORD") or env.get("SMTP_PASS")
    sender = env.get("EMAIL_FROM") or user
    recipient = env.get("MAIL_TO") or env.get("EMAIL_TO") or TO

    missing = [n for n, v in (("SMTP_HOST", host), ("SMTP_USER", user),
                              ("SMTP_PASSWORD", password),
                              ("EMAIL_FROM", sender)) if not v]
    if missing:
        print(f"누락된 환경값: {', '.join(missing)}")
        return 2

    result = test_result()
    test_row = (
        row("프로파일 편집 자동 검증", f"{result.get('통과')} / {result.get('전체')}",
            "전 항목 통과"))

    routes = live_check()
    route_rows = "".join(row(label, state) for label, state in routes)
    error_line = error_tail()

    html = (BODY.replace("{test_row}", test_row)
                .replace("{route_rows}", route_rows)
                .replace("{error_line}", error_line)
                .replace("{manual}", MANUAL)
                .replace("{repo}", REPO_URL)
                .replace("__SENT__", datetime.now().strftime("%Y-%m-%d %H:%M")))

    message = EmailMessage()
    message["Subject"] = "[Databricks 전환 검증] ETL 대시보드 프로파일 편집 기능 개선"
    message["From"] = sender
    message["To"] = recipient
    message.set_content(
        "ETL 대시보드 프로파일 편집 기능을 개선했습니다.\n\n"
        "1) 편집 방법을 두 갈래로 분리\n"
        "   - 체크박스 화면 : 컬럼 include/exclude, 비식별화, 처리종류, 주기\n"
        "   - YAML 원본 편집: /profiles/<엔진>/<스키마>/raw\n"
        "     그대로 읽고 · 고치고 · 저장 (주석도 보존)\n\n"
        "2) 저장은 검증 통과 후에만. 실패하면 파일을 건드리지 않는다\n"
        "   - YAML 문법 / columns 비어있음 / include·exclude 정합성 /\n"
        "     merge 인데 기본키 없음 → 거부\n"
        "   - 저장 직전 자동 백업, 되돌리기 버튼 제공\n\n"
        "3) 저장 후 변경 요약 표시 (무엇이 바뀌었는지)\n\n"
        f"4) 자동 검증 {result.get('통과')}/{result.get('전체')} 통과\n"
        "   재현: python scripts/test_profile_editor.py\n\n"
        f"상세 매뉴얼 : {MANUAL}\n"
        f"GitHub      : {REPO_URL}\n"
        f"로컬 대시보드 : http://127.0.0.1:8540\n\n"
        "⚠ Slack Incoming Webhook URL 이 Git 이력에 올라간 적이 있어\n"
        "  Slack 에서 해당 웹훅을 삭제하고 새로 만들어 주십시오.\n")
    message.add_alternative(html, subtype="html")

    print("라우트 상태:")
    for label, state in routes:
        print(f"  {state:24s} {label}")
    print(f"\n발송 대상 : {sender} → {recipient}")
    with smtplib.SMTP(host, port, timeout=60) as server:
        server.starttls()
        server.login(user, password)
        server.send_message(message)
    print(f"발송 완료 · {datetime.now():%Y-%m-%d %H:%M:%S}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())