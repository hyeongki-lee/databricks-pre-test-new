"""
ETL 대시보드 정적 스냅샷 생성기.

왜 정적 버전이 필요한가
----------------------
GitHub Pages 는 **정적 파일만** 제공한다. Python 을 실행하지 않으므로
Flask 대시보드(`dashboard/app.py`)를 그대로 올려 "실행"할 수는 없다.

그래서 두 가지를 나눈다.

    이 스크립트   → 프로파일 12개를 읽어 **읽기 전용 HTML** 로 굽는다.
                   GitHub Pages 에 올려 아무 브라우저나 조회 가능.
    dashboard/app.py → 로컬에서 실행하는 실시간 버전. DB·Databricks 를
                   직접 조회하고 YAML 을 저장한다.

정적 버전으로 **안 되는** 것 (정직하게 표시한다)
    · 원천 DB / Databricks 실시간 조회
    · 컬럼 체크박스 → exclude_columns 저장 (POST 필요)
    · 신규 테이블 등록

정적 버전에 포함되는 것
    · 프로파일 12개 (엔진·스키마·테이블·ETL 유형·비식별화·제외 컬럼·주기)
    · 집계 (append/truncate/merge, D1/D2/D3 배분, 활성 상태)
    · 실제 실행 이력 (load_audit · etl_run_log 에서 읽어 스냅샷으로 포함)
"""
from __future__ import annotations

import json
import sys
from collections import Counter
from datetime import datetime
from html import escape
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

for _s in (sys.stdout, sys.stderr):
    try:
        _s.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass

import yaml                                       # noqa: E402

from lib import config as cfg                     # noqa: E402

PROFILE_DIR = cfg.profile_folder()
OUT_DIR = ROOT / "docs"
OUT_FILE = OUT_DIR / "dashboard.html"

PERIOD_LABEL = {
    "daily": "일간", "weekly": "주간", "monthly": "월간",
    "quarterly": "분기", "semi_annually": "반기", "annually": "연간",
}

DEID_LABEL = {"D1": "D1 해시", "D2": "D2 null", "D3": "D3 마스킹"}

STYLE = """
:root{--bg:#f4f6f8;--card:#fff;--line:#dde2e8;--txt:#16212e;--sub:#5a6674;
      --ok:#137547;--info:#1f4e9c;--warn:#9a6700;--accent:#0b4f9e}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--txt);
     font:15px/1.7 -apple-system,"Segoe UI","Malgun Gothic","Noto Sans KR",sans-serif}
header{background:#16212e;color:#fff;padding:16px 26px}
header h1{margin:0;font-size:18px}
header .sub{color:#9fb0c3;font-size:13px;margin-top:3px}
.wrap{max-width:1240px;margin:0 auto;padding:22px 20px 60px}
.notice{border-left:4px solid var(--warn);background:#fdf8ec;padding:13px 16px;
        border-radius:0 8px 8px 0;margin-bottom:18px;font-size:13.5px}
.notice b{display:block;margin-bottom:5px}
.notice code{background:#fff;padding:1px 5px;border-radius:3px}
.kpis{display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));
      gap:12px;margin-bottom:20px}
.kpi{border:1px solid var(--line);border-radius:8px;padding:13px 15px;
     background:#fbfcfd}
.kpi b{display:block;font-size:24px;line-height:1.25;color:var(--accent)}
.kpi span{font-size:12.5px;color:var(--sub)}
nav{display:flex;flex-wrap:wrap;gap:8px;margin-bottom:16px}
nav a{background:#fff;border:1px solid var(--line);border-radius:20px;
      padding:5px 14px;font-size:13px;color:#243447;text-decoration:none}
nav a:hover{background:#eef2f6}
section{background:var(--card);border:1px solid var(--line);border-radius:10px;
        padding:20px 24px;margin-bottom:18px}
section>h2{margin:0 0 12px;font-size:17px;padding-bottom:8px;
           border-bottom:2px solid var(--line)}
table{border-collapse:collapse;width:100%;font-size:13px}
th,td{border:1px solid var(--line);padding:6px 9px;text-align:left;
      vertical-align:top}
th{background:#eef2f6;font-weight:600;font-size:12.5px}
tbody tr:nth-child(even){background:#fafbfc}
code{background:#eef2f6;padding:1px 5px;border-radius:3px;font-size:12px}
.pill{display:inline-block;padding:1px 8px;border-radius:10px;font-size:11.5px;
      font-weight:600}
.pill.ok{background:#e3f5ea;color:var(--ok);border:1px solid #a9d9bd}
.pill.info{background:#e8f0fb;color:var(--info);border:1px solid #b9cdf0}
.pill.warn{background:#fdf5e3;color:var(--warn);border:1px solid #ecd9a0}
.muted{color:var(--sub);font-size:12.5px}
footer{text-align:center;color:var(--sub);font-size:12.5px;padding:20px 0}
@media(max-width:760px){table{font-size:12px}th,td{padding:5px 6px}}
"""


def load_profiles() -> list[dict]:
    """Read every profile YAML into a flat list."""
    profiles: list[dict] = []
    for path in sorted(PROFILE_DIR.glob("*.yaml")):
        document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        engine = document.get("engine", "")
        schema = document.get("schema", "")
        schema_schedule = (document.get("schedule") or {}).get("type", "?")
        for table, entry in (document.get("tables") or {}).items():
            profiles.append({
                "file": path.name,
                "engine": engine,
                "schema": schema,
                "table": table,
                "etl_type": entry.get("etl_type", "?"),
                "primary_key": entry.get("primary_key") or "",
                "active": entry.get("active", "N"),
                "columns": entry.get("columns") or [],
                "include_columns": entry.get("include_columns") or [],
                "exclude_columns": entry.get("exclude_columns") or [],
                "deidentification": entry.get("deidentification") or {},
                "schedule": (entry.get("schedule") or {}).get(
                    "type", schema_schedule),
                "registered_at": entry.get("registered_at", ""),
                "row_count": _row_count(engine, schema, table),
            })
    return profiles


def _row_count(engine: str, schema: str, table: str) -> int | None:
    """Live source row count. Unavailable offline, then None."""
    try:
        from lib import sources as sources_mod
        return sources_mod.count_rows(engine, schema, table)
    except Exception:                           # noqa: BLE001
        return None


def load_runs() -> dict:
    """Latest audit records, used to show what actually ran."""
    try:
        from lib import dbx
        from lib import logtable
        meta = logtable.meta_schema()
        result = dbx.execute_sql(
            f"SELECT engine, etl_type, status_code, COUNT(*) AS cnt "
            f"FROM {meta}.etl_run_log GROUP BY 1,2,3 ORDER BY 1,2,3")
        columns = [c["name"] for c in result["columns"]]
        etl = [dict(zip(columns, row)) for row in result["rows"]]

        result = dbx.execute_sql(
            f"SELECT engine, COUNT(*) AS cnt, "
            f"       SUM(CASE WHEN count_match='Y' THEN 1 ELSE 0 END) AS ok "
            f"FROM {meta}.load_audit WHERE work_type='initial_load' "
            f"  AND source_count=50000 GROUP BY 1 ORDER BY 1")
        columns = [c["name"] for c in result["columns"]]
        load = [dict(zip(columns, row)) for row in result["rows"]]
        return {"etl": etl, "load": load}
    except Exception as exc:                    # noqa: BLE001
        return {"오류": f"{type(exc).__name__}: {exc}"}


def pill(text: str, kind: str = "info") -> str:
    return f'<span class="pill {kind}">{escape(str(text))}</span>'


def build() -> str:
    profiles = load_profiles()
    runs = load_runs()

    tally = Counter(p["etl_type"] for p in profiles)
    deid = Counter(code for p in profiles
                   for code in p["deidentification"].values())
    active = sum(1 for p in profiles if p["active"] == "Y")
    excluded = sum(len(p["exclude_columns"]) for p in profiles)
    schemas = {(p["engine"], p["schema"]) for p in profiles}

    p: list[str] = []
    add = p.append

    add("<!DOCTYPE html><html lang=\"ko\"><head><meta charset=\"utf-8\">")
    add('<meta name="viewport" content="width=device-width,initial-scale=1">')
    add("<title>ETL 대시보드 — databricks-pre-test-new</title>")
    add(f"<style>{STYLE}</style></head><body>")
    add('<header><h1>ETL 대시보드</h1>'
        '<div class="sub">databricks-pre-test-new · 스키마 단위 ETL 등록 현황</div>'
        "</header><div class=\"wrap\">")

    stamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    add('<div class="notice"><b>이 페이지는 정적 스냅샷입니다</b>'
        'GitHub Pages 는 정적 파일만 제공하므로 Python 을 실행하지 못합니다. '
        '따라서 이 화면은 <b>프로파일 YAML 12개를 읽어 굽은 읽기 전용</b>视图입니다.'
        '<br>아래는 <b>안 됩니다</b>: 원천 DB · Databricks 실시간 조회, '
        '컬럼 체크박스로 exclude_columns 저장, 신규 테이블 등록.'
        '<br>실시간 조회와 저장이 필요하면 로컬에서 실행하십시오: '
        '<code>python dashboard/app.py --port 8540</code> → '
        '<code>http://127.0.0.1:8540</code>'
        f'<br>스냅샷 생성 시각: {escape(stamp)}</div>')

    add('<div class="kpis">')
    for value, label in [(len(schemas), "프로파일 스키마"),
                         (len(profiles), "등록 테이블"),
                         (active, "활성(Y)"),
                         (excluded, "제외 컬럼 지정"),
                         (tally.get("append", 0), "append"),
                         (tally.get("truncate", 0), "truncate"),
                         (tally.get("merge", 0), "merge"),
                         (deid.get("D1", 0), "D1 배정"),
                         (deid.get("D2", 0), "D2 배정"),
                         (deid.get("D3", 0), "D3 배정")]:
        add(f'<div class="kpi"><b>{value}</b><span>{escape(label)}</span></div>')
    add("</div>")

    # ---------------- 내비게이션 ----------------
    add("<nav>")
    for engine in sorted({p_["engine"] for p_ in profiles}):
        anchor = engine.replace(" ", "-")
        add(f'<a href="#{anchor}">{escape(engine)}</a>')
    add('<a href="#runs">실행 이력</a><a href="#rules">컬럼 결정 규칙</a>')
    add("</nav>")

    # ---------------- 엔진별 테이블 ----------------
    for engine in sorted({p_["engine"] for p_ in profiles}):
        rows_profiles = [p_ for p_ in profiles if p_["engine"] == engine]
        add(f'<section id="{engine.replace(" ", "-")}">')
        add(f"<h2>{escape(engine)} — "
            f"{len(rows_profiles)}개 테이블</h2>")
        add("<table><thead><tr>"
            "<th>스키마</th><th>테이블</th><th>ETL 유형</th><th>PK</th>"
            "<th>주기</th><th>활성</th><th>원천 건수</th>"
            "<th>적재 컬럼</th><th>제외</th><th>비식별화</th>"
            "</tr></thead><tbody>")
        for row in rows_profiles:
            columns = row["columns"]
            excluded = set(row["exclude_columns"])
            load_columns = [c for c in columns if c not in excluded]
            deid_text = ", ".join(
                f"{escape(k)}={escape(v)}"
                for k, v in sorted(row["deidentification"].items())) or "—"
            count = row["row_count"]
            add("<tr>"
                f'<td>{escape(row["schema"])}</td>'
                f'<td><code>{escape(row["table"])}</code></td>'
                f'<td>{pill(row["etl_type"])}</td>'
                f'<td>{("<code>" + escape(row["primary_key"]) + "</code>") if row["primary_key"] else "<span class=\'muted\'>-</span>"}</td>'
                f'<td>{escape(PERIOD_LABEL.get(row["schedule"], row["schedule"]))}</td>'
                f'<td>{pill("Y", "ok") if row["active"] == "Y" else pill("N", "warn")}</td>'
                f'<td>{f"{count:,}" if count is not None else "<span class=\'muted\'>확인 불가</span>"}</td>'
                f'<td class="muted">{len(load_columns)} / {len(columns)}</td>'
                f'<td>{", ".join(f"<code>{escape(c)}</code>" for c in sorted(excluded)) or "-"}</td>'
                f'<td class="muted">{deid_text}</td>'
                "</tr>")
        add("</tbody></table></section>")

    # ---------------- 실행 이력 ----------------
    add('<section id="runs"><h2>실제 실행 이력 (Databricks 감사 테이블)</h2>')
    if "오류" in runs:
        add(f'<p class="muted">감사 테이블을 읽지 못했습니다: '
            f'{escape(str(runs["오류"])[:200])}</p>')
    else:
        add("<h3>초기 이관 (테이블당 5만 건)</h3>")
        add("<table><thead><tr><th>엔진</th><th>테이블</th>"
            "<th>건수 일치</th><th>판정</th></tr></thead><tbody>")
        for row in runs["load"]:
            total = int(row["cnt"])
            ok = int(row["ok"])
            add(f'<tr><td>{escape(row["engine"])}</td>'
                f'<td>{total}</td><td>{ok}</td>'
                f'<td>{pill("일치", "ok") if total == ok else pill("불일치", "warn")}</td></tr>')
        add("</tbody></table>")

        add("<h3>정규 ETL</h3>")
        add("<table><thead><tr><th>엔진</th><th>etl_type</th>"
            "<th>판정</th><th>건수</th></tr></thead><tbody>")
        for row in runs["etl"]:
            kind = "ok" if row["status_code"] == "OK" else "warn"
            add(f'<tr><td>{escape(row["engine"])}</td>'
                f'<td>{pill(row["etl_type"])}</td>'
                f'<td>{pill(row["status_code"], kind)}</td>'
                f'<td>{row["cnt"]}</td></tr>')
        add("</tbody></table>")
    add('<p class="muted">출처: <code>workspace.pretest_meta.load_audit</code>, '
        '<code>etl_run_log</code></p>')
    add("</section>")

    # ---------------- 컬럼 결정 규칙 ----------------
    add('<section id="rules"><h2>컬럼 결정 규칙</h2>'
        "<table><thead><tr><th>순서</th><th>단계</th><th>동작</th>"
        "</tr></thead><tbody>"
        "<tr><td>1</td><td><code>include_columns</code> 가 있으면</td>"
        "<td>그 목록만 화이트리스트로 사용</td></tr>"
        "<tr><td>2</td><td>비어 있으면</td>"
        "<td><code>columns</code>(등록 시점 스냅샷)를 기준선으로 사용</td></tr>"
        "<tr><td>3</td><td>항상 마지막</td>"
        "<td><code>exclude_columns</code> 제거</td></tr>"
        "</tbody></table>"
        '<p class="muted">기준선을 등록 시점 스냅샷으로 고정하므로, '
        '원천에 컬럼이 새로 생겨도 자동 반영되지 않는다(요구사항).</p>'
        "</section>")

    add(f'<footer>databricks-pre-test-new · 정적 스냅샷 {escape(stamp)}</footer>')
    add("</div></body></html>")
    return "".join(p)


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    html = build()
    OUT_FILE.write_text(html, encoding="utf-8")
    print(f"정적 대시보드 생성: {OUT_FILE}")
    print(f"  크기: {OUT_FILE.stat().st_size / 1024:.0f} KB")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())