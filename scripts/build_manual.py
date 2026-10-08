"""
HTML 매뉴얼 생성기.

왜 수치를 코드에서 직접 읽는가
---------------------------
매뉴얼의 숫자를 손으로 옮겨 적으면 반드시 어긋난다. 이 프로젝트는 Free Edition
쿼터 때문에 여러 번 재실행했으므로, 수치가 어느 시점의 것인지 모호해지면 안 된다.

`work/evidence.json` 은 Databricks·S3·원천 DB·Slack 에 직접 질의해서 만든
"그 시점의 진실" 이다. 이 생성기는 그 파일과 캡처 이미지만 읽는다.

출력물
------
    manual/databricks-pre-test-manual.html   (인라인 스타일 단일 파일)

⚠ HTML 은 외부 CSS 를 참조하지 않는다. 매뉴얼을 메일/GitHub 으로 옮길 때
  스타일이 깨지지 않도록 모든 스타일을 인라인으로 넣는다.
"""
from __future__ import annotations

import argparse
import base64
import json
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

for _s in (sys.stdout, sys.stderr):
    try:
        _s.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass

EVIDENCE = ROOT / "work" / "evidence.json"
CAPTURE_DIR = ROOT / "manual" / "captures"
OUT_DIR = ROOT / "manual"
OUT_FILE = OUT_DIR / "databricks-pre-test-manual.html"


# ---------------------------------------------------------------------------
# Small HTML helpers
# ---------------------------------------------------------------------------

def esc(value) -> str:
    """Escape for HTML text content."""
    text = str(value)
    return (text.replace("&", "&amp;").replace("<", "&lt;")
                .replace(">", "&gt;").replace('"', "&quot;"))


def num(value) -> str:
    """Thousands-separated number when it looks numeric."""
    try:
        return f"{int(value):,}"
    except (TypeError, ValueError):
        return esc(value)


def table(headers: list[str], rows: list[list], cls: str = "") -> str:
    """Render a table. `rows` may contain raw HTML strings."""
    out = [f'<table class="{cls}">', "<thead><tr>"]
    out += [f"<th>{h}</th>" for h in headers]
    out.append("</tr></thead><tbody>")
    for row in rows:
        out.append("<tr>")
        for cell in row:
            out.append(f"<td>{cell}</td>")
        out.append("</tr>")
    out.append("</tbody></table>")
    return "".join(out)


def badge(text: str, kind: str = "ok") -> str:
    return f'<span class="badge {kind}">{esc(text)}</span>'


def image(name: str, caption: str) -> str:
    """Embed a capture as base64 so the manual stays a single portable file."""
    path = CAPTURE_DIR / name
    if not path.exists():
        return (f'<figure class="missing">'
                f'<figcaption>캡처 없음 — {esc(caption)}</figcaption></figure>')
    payload = base64.b64encode(path.read_bytes()).decode("ascii")
    return (f'<figure><img src="data:image/png;base64,{payload}" '
            f'alt="{esc(caption)}">'
            f'<figcaption>{esc(caption)}</figcaption></figure>')


def code(text: str, lang: str = "") -> str:
    return f'<pre class="code {lang}">{esc(text)}</pre>'


STYLE = """
:root{--bg:#f4f6f8;--card:#fff;--line:#dde2e8;--txt:#16212e;--sub:#5a6674;
      --ok:#137547;--ng:#b3261e;--warn:#9a6700;--info:#1f4e9c;--accent:#0b4f9e}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--txt);
     font:15px/1.75 -apple-system,"Segoe UI","Malgun Gothic","Noto Sans KR",sans-serif}
.wrap{display:grid;grid-template-columns:250px minmax(0,1fr);gap:0}
nav{position:sticky;top:0;align-self:start;height:100vh;overflow-y:auto;
    background:#16212e;color:#cbd5e1;padding:22px 16px}
nav h2{font-size:14px;color:#fff;margin:0 0 14px;letter-spacing:.02em}
nav a{display:block;color:#cbd5e1;text-decoration:none;font-size:13.5px;
       padding:5px 8px;border-radius:5px;margin-bottom:1px}
nav a:hover{background:#243447;color:#fff}
nav .grp{font-size:11px;color:#7b8a9c;margin:14px 0 4px;letter-spacing:.08em;
         text-transform:uppercase}
main{padding:28px 32px 80px;max-width:1180px}
header.top{background:#fff;border:1px solid var(--line);border-radius:10px;
           padding:22px 26px;margin-bottom:20px}
header.top h1{margin:0 0 6px;font-size:23px}
header.top .sub{color:var(--sub);font-size:14px;margin:0}
.meta{display:flex;flex-wrap:wrap;gap:8px;margin-top:14px}
.meta span{background:#eef2f6;border:1px solid var(--line);border-radius:20px;
           padding:3px 12px;font-size:12.5px;color:var(--sub)}
section{background:var(--card);border:1px solid var(--line);border-radius:10px;
        padding:22px 26px;margin-bottom:18px}
section>h2{margin:0 0 6px;font-size:18px;padding-bottom:9px;
           border-bottom:2px solid var(--line)}
section>h3{font-size:15px;margin:20px 0 8px;color:#243447}
section>h3:first-of-type{margin-top:12px}
p{margin:9px 0}
table{border-collapse:collapse;width:100%;margin:12px 0;font-size:13.5px}
th,td{border:1px solid var(--line);padding:7px 10px;text-align:left;
      vertical-align:top}
th{background:#eef2f6;font-weight:600;font-size:13px}
tbody tr:nth-child(even){background:#fafbfc}
td code,th code{background:#eef2f6;padding:1px 5px;border-radius:3px;font-size:12.5px}
pre.code{background:#16212e;color:#e2e8f0;padding:14px 16px;border-radius:8px;
         overflow-x:auto;font-size:12.5px;line-height:1.6;margin:10px 0}
pre.diagram{background:#fbfcfe;border:1px solid var(--line);color:var(--txt);
            padding:16px;border-radius:8px;overflow-x:auto;font-size:12.5px;
            line-height:1.5;margin:12px 0}
.badge{display:inline-block;padding:2px 9px;border-radius:11px;font-size:12px;
       font-weight:600}
.badge.ok{background:#e3f5ea;color:var(--ok);border:1px solid #a9d9bd}
.badge.ng{background:#fdeceb;color:var(--ng);border:1px solid #f2b6b2}
.badge.warn{background:#fdf5e3;color:var(--warn);border:1px solid #ecd9a0}
.badge.info{background:#e8f0fb;color:var(--info);border:1px solid #b9cdf0}
figure{margin:16px 0;padding:0}
figure img{width:100%;border:1px solid var(--line);border-radius:8px;
           display:block;background:#fff}
figcaption{font-size:12.5px;color:var(--sub);margin-top:7px;
           padding-left:2px;border-left:3px solid var(--line);padding-left:9px}
figure.missing{border:1px dashed #c9d2db;border-radius:8px;padding:18px;
               color:var(--sub);font-size:13px}
.kpis{display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,1fr));
      gap:12px;margin:16px 0}
.kpi{border:1px solid var(--line);border-radius:8px;padding:13px 15px;
     background:#fbfcfd}
.kpi b{display:block;font-size:25px;line-height:1.2;color:var(--accent)}
.kpi span{font-size:12.5px;color:var(--sub)}
ul,ol{margin:9px 0;padding-left:22px}
li{margin:4px 0}
.callout{border-left:4px solid var(--info);background:#f2f7fd;
         padding:12px 15px;border-radius:0 8px 8px 0;margin:14px 0;font-size:14px}
.callout.warn{border-left-color:var(--warn);background:#fdf8ec}
.callout.ng{border-left-color:var(--ng);background:#fdf1f0}
.callout.ok{border-left-color:var(--ok);background:#eef8f2}
.callout b{display:block;margin-bottom:4px}
.toc-inline{background:#fbfcfd;border:1px solid var(--line);border-radius:8px;
            padding:14px 18px;margin:14px 0}
.grid2{display:grid;grid-template-columns:repeat(auto-fit,minmax(320px,1fr));
       gap:16px}
footer{text-align:center;color:var(--sub);font-size:12.5px;padding:24px 0 8px}
@media (max-width:900px){.wrap{grid-template-columns:1fr}
  nav{position:static;height:auto}}
"""


def main() -> int:
    if not EVIDENCE.exists():
        print(f"증적 파일이 없습니다: {EVIDENCE}")
        print("먼저 `python scripts/collect_evidence.py` 를 실행하십시오.")
        return 1

    data = json.loads(EVIDENCE.read_text(encoding="utf-8"))

    from manual_parts import build          # noqa: E402

    html = build(data, STYLE, {
        "esc": esc, "num": num, "table": table, "badge": badge,
        "image": image, "code": code,
    })

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    OUT_FILE.write_text(html, encoding="utf-8")

    size_mb = OUT_FILE.stat().st_size / 1024 / 1024
    print(f"매뉴얼 생성: {OUT_FILE}")
    print(f"  크기: {size_mb:.2f} MB")
    print(f"  생성시각: {datetime.now():%Y-%m-%d %H:%M:%S}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
