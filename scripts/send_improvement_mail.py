"""
기능 개선 안내 메일 발송.

보내는 내용
----------
  1. 프로파일 편집 기능 개선 (YAML 원본 읽기·고치기·저장, 변경 요약, 되돌리기)
  2. 대시보드 응답 시간 개선 (25초 → 0초)
  3. 작업 중 발견·수정한 결함

매뉴얼은 이미 링크가 돌아가고 있으니 첨부하지 않고 변경분만 알린다.
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

TO = "mercy.lee@kakaopaycorp.com"
ENV_CANDIDATES = (Path(r"C:\Users\lee21\lakehouse\.env"), ROOT / ".env")

DASH = "http://127.0.0.1:8540"
LOG = ROOT / "work" / "dashboard" / "dashboard.err.log"

ROUTES = [
    ("개요", "/"),
    ("프로파일 목록", "/profiles"),
    ("스키마 상세 (체크박스)", "/profiles/mysql/mysql_schema_1"),
    ("YAML 원본 편집", "/profiles/mysql/mysql_schema_1/raw"),
    ("컬럼 현황", "/columns"),
    ("ETL 빌더", "/builder"),
    ("신규 테이블", "/table/add"),
]


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


def check_routes() -> list[tuple[str, str, float]]:
    """HTTP status and elapsed seconds for each route."""
    import time
    rows: list[tuple[str, str, float]] = []
    for label, path in ROUTES:
        start = time.time()
        try:
            with urllib.request.urlopen(f"{DASH}{path}", timeout=120) as r:
                rows.append((label, f"HTTP {r.status}", time.time() - start))
        except Exception as exc:                # noqa: BLE001
            rows.append((label, f"미응답 ({type(exc).__name__})",
                         time.time() - start))
    return rows


def run_editor_test() -> str:
    """Run the editor test and summarise the tail."""
    import subprocess

    python = sys.executable
    script = ROOT / "scripts" / "test_profile_editor.py"
    result = subprocess.run([python, str(script)], cwd=ROOT,
                            capture_output=True, text=True,
                            encoding="utf-8", errors="replace", timeout=900)
    for line in reversed((result.stdout or "").splitlines()):
        if "통과" in line and "/" in line:
            return line.strip()
    return "실행 결과 확인 못함"


def error_line() -> str:
    if not LOG.exists():
        return "로그 파일 없음"
    hits = [ln for ln in LOG.read_text(encoding="utf-8",
                                       errors="replace").splitlines()
            if "ERROR" in ln or "Traceback" in ln]
    return hits[-1].strip()[:110] if hits else "에러 없음"


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
      ETL 대시보드 — 프로파일 편집 · 응답 시간 개선</h1>
    <p style="margin:0 0 18px;color:#5a6674;font-size:14px;line-height:1.75">
      "프로파일 편집이 안 열리고 YAML 파일만 열린다" 는 지적을 고쳤습니다.
      이제 <b>읽고 · 고치고 · 저장</b>이 대시보드 안에서 끝나고,
      저장하면 무엇이 바뀌었는지 보여주며 되돌릴 수 있습니다.
      함께 발견한 개요 화면의 느린 응답도 고쳤습니다.</p>

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
table_2: 비식별화 {'name': 'D1'} → {'name': 'D3'}</pre>

    <div style="font-weight:bold;font-size:14px;margin:0 0 10px">
      4. 대시보드 응답 시간 — 25초에서 0초로</div>
    <p style="margin:0 0 10px;font-size:13.5px;line-height:1.8">
      개요 페이지가 원천 DB 3곳(테이블 60개의 컬럼·건수)과 Databricks 를
      매번 새로 조회해 <b>25초</b>, 첫 요청은 45초까지 걸렸습니다.
      라우트 점검 중 30초 제한에 타임아웃을 받아 실제로 죽은 것도 확인했습니다.</p>
    <table cellpadding="0" cellspacing="0"
           style="border-collapse:collapse;font-size:13px;border:1px solid #ddd;
                  width:100%;margin-bottom:10px">
      <tr style="background:#eef2f6">
        <td style="padding:6px 12px;border-bottom:1px solid #eee;font-weight:bold">처리</td>
        <td style="padding:6px 12px;border-bottom:1px solid #eee;font-weight:bold;text-align:right">소요</td>
      </tr>
      <tr><td style="padding:6px 12px;border-bottom:1px solid #eee">수정 전 · 화면마다 새로 조회</td>
          <td style="padding:6px 12px;border-bottom:1px solid #eee;text-align:right">25초 (첫 요청 45초)</td></tr>
      <tr><td style="padding:6px 12px;border-bottom:1px solid #eee">TTL 캐시 적용 (원천 120초 · 통계 90초)</td>
          <td style="padding:6px 12px;border-bottom:1px solid #eee;text-align:right">0초</td></tr>
      <tr><td style="padding:6px 12px">시작 시 백그라운드 예열 추가</td>
          <td style="padding:6px 12px;text-align:right;font-weight:bold">첫 요청도 0초</td></tr>
    </table>
    <div style="border-left:4px solid #1f4e9c;background:#f2f7fd;padding:12px 15px;
                border-radius:0 8px 8px 0;margin:0 0 18px;font-size:13.5px;line-height:1.8">
      캐시를 넣으면서 지킨 것<br>
      <b>① 값의 신선도를 숨기지 않는다</b> — 화면에 "N초 전에 조회한 값을
      재사용하고 있습니다" 를 그대로 적는다.<br>
      <b>② 조회가 실패하면 화면을 비우지 않는다</b> — 이전 값을 유지하고
      실패 사유를 함께 보여준다.<br>
      <b>③ <code>/api/stats</code> 는 캐시를 우회한다</b> — 스크립트가
      <i>현재 값</i> 을 얻으려고 쓰는 곳이므로 오래된 값을 주면
      목적에 어긋난다. 캐시는 사람용 페이지에만 적용한다.</div>

    <div style="border-left:4px solid #b3261e;background:#fdf1f0;padding:13px 16px;
                border-radius:0 8px 8px 0;margin:0 0 18px;font-size:13.5px;line-height:1.8">
      <b>작업 중 발견·고친 결함</b>
      <ol style="margin:7px 0 0;padding-left:20px">
        <li><b>YAML 편집에서 주석이 사라지던 문제</b> — 저장 함수가 dict 를
            다시 직렬화하는 과정에서 모든 주석을 버렸다. "이 컬럼을 제외한
            이유" 를 적어 둔 것이 저장과 동시에 지워졌다. 원본 편집 경로는
            <b>텍스트를 그대로 기록</b>하도록 별도 처리했다.</li>
        <li><b>변경 요약이 항상 "새로 등록" 으로만 나오던 문제</b> — 존재 여부
            검사 대상을 잘못 잡아 모든 테이블이 신규로 보고됐다. 단위 검증
            8종을 만들어 막았다.</li>
        <li><b>되돌리기용 백업이 형상관리 대상이 될 뻔한 문제</b> —
            <code>.gitignore</code> 로 제외했다.</li>
      </ol>
    </div>

    <div style="font-weight:bold;font-size:14px;margin:0 0 10px">
      5. 자동 검증과 라우트 상태</div>
    <p style="margin:0 0 8px;font-size:13.5px">
      프로파일 편집 검증 : <b>{test_line}</b><br>
      재현 : <code>python scripts/test_profile_editor.py</code></p>
    <table cellpadding="0" cellspacing="0"
           style="border-collapse:collapse;font-size:13px;border:1px solid #ddd;
                  width:100%">
      {route_rows}
    </table>
    <p style="margin:8px 0 18px;color:#5a6674;font-size:12.5px">
      최근 서버 로그 : {error_line}</p>

    <div style="border-left:4px solid #1f4e9c;background:#f2f7fd;padding:13px 16px;
                border-radius:0 8px 8px 0;margin:0 0 18px;font-size:13.5px;line-height:1.8">
      <b>매뉴얼 14장 보완</b><br>
      14.1 편집 2경로 · 14.2 저장 안전장치 5단계 · 14.3 응답 시간(캐시·예열) ·
      14.4 변경 요약 예시 · 잡은 결함을 그대로 실었다.
      <a href="{manual}" style="color:#0b4f9e">매뉴얼 열기 →</a></div>

    <a href="{manual}"
       style="display:inline-block;background:#0b4f9e;color:#fff;padding:11px 20px;
              border-radius:7px;text-decoration:none;font-weight:bold;
              font-size:14px;margin-right:8px">상세 매뉴얼</a>
    <a href="{repo}"
       style="display:inline-block;color:#0b4f9e;padding:11px 4px;
              text-decoration:none;font-size:14px">GitHub 저장소</a>

    <p style="margin:18px 0 0;color:#5a6674;font-size:13px;line-height:1.8">
      로컬 실시간 대시보드는 <code>{dash}</code> 입니다.
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

    print("라우트 점검 …")
    routes = check_routes()
    for label, state, seconds in routes:
        print(f"  {state:22s} {seconds:5.1f}초  {label}")

    test_line = run_editor_test()
    print(f"편집 검증 : {test_line}")

    route_rows = "".join(
        row(label, state, f"{seconds:.1f}초") for label, state, seconds in routes)

    html = (BODY.replace("{route_rows}", route_rows)
                .replace("{test_line}", test_line)
                .replace("{error_line}", error_line())
                .replace("{manual}", MANUAL)
                .replace("{repo}", REPO_URL)
                .replace("{dash}", DASH)
                .replace("__SENT__", datetime.now().strftime("%Y-%m-%d %H:%M")))

    message = EmailMessage()
    message["Subject"] = ("[Databricks 전환 검증] ETL 대시보드 프로파일 편집 · "
                          "응답 시간 개선")
    message["From"] = sender
    message["To"] = recipient
    message.set_content(
        "ETL 대시보드를 개선했습니다.\n\n"
        "1) 프로파일 편집 — YAML 원본을 읽고·고치고·저장\n"
        "   /profiles/<엔진>/<스키마>/raw\n"
        "   저장은 검증 통과 후에만. 실패하면 파일을 건드리지 않는다\n"
        "   (문법 / columns 비어있음 / include·exclude 정합성 /\n"
        "    merge 인데 기본키 없음 → 거부)\n"
        "   저장 직전 자동 백업 + 되돌리기\n\n"
        "2) 저장 후 변경 요약 표시 (무엇이 바뀌었는지)\n\n"
        "3) 대시보드 응답 시간 25초 → 0초\n"
        "   TTL 캐시(원천 120초·통계 90초) + 시작 시 백그라운드 예열\n"
        "   화면에 값의 신선도를 적고, 조회가 실패하면 이전 값을 유지한다.\n"
        "   /api/stats 는 캐시를 우회한다(현재 값이 필요하므로).\n\n"
        f"4) 자동 검증 : {test_line}\n"
        f"   재현 : python scripts/test_profile_editor.py\n\n"
        f"매뉴얼 : {MANUAL}\n"
        f"GitHub  : {REPO_URL}\n"
        f"로컬    : {DASH}\n\n"
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