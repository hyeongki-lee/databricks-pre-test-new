"""
ETL dashboard (Flask).

What the dashboard must do (requirement mapping)
-----------------------------------------------
1. Hold connection info per engine and **query the live database at startup**,
   rather than trusting a static list.
2. Store ETL definitions as **one YAML/JSON per schema** (12 files for
   4 schemas × 3 engines), not in database rows — the requirement is explicit
   that this is for configuration management.
3. Register all 4 schemas × 5 tables as ETL entries up front.
4. Map de-identification to codes D1 / D2 / D3, spread across engines so every
   combination is exercised.
5. Support append / truncate / merge plus a coded schedule (day/week/month/
   quarter/half/year), with a registration date and fixed conditions
   ("every 1st of the month").
6. Show every column; unchecking one marks it `exclude_columns`.
7. Provide a **builder** flow: a newly created source table can be registered
   from the UI, updating an existing YAML or creating a new one.
8. Display registered ETL definitions **and** statistics read back from
   Databricks.

Design note on 7
----------------
The builder writes the YAML file directly. In a real deployment this would be
a pull request rather than a direct write, so that the profile stays
reviewable — the manual calls this out explicitly.
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from flask import (Flask, flash, jsonify, redirect, render_template,
                   request, url_for)

from lib import config as cfg                  # noqa: E402
from lib import dbx                            # noqa: E402
from lib import mask as mask_mod               # noqa: E402
from lib import profile as profile_mod         # noqa: E402
from lib import sources as sources_mod         # noqa: E402

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)-5s %(message)s")
logger = logging.getLogger("dashboard")

#: 검증에 사용한 테이블당 행 수. 감사 테이블 집계를 이 규모로 한정한다.
#: (감사 테이블은 실행을 누적하므로 규모를 걸지 않으면 과거 200건 시험
#:  기록의 FAIL 이 현재 결과와 나란히 표시된다)
VERIFIED_SCALE = 50000

app = Flask(__name__)
app.secret_key = "databricks-pre-test-new-dashboard"   # noqa: S105

#: Code tables rendered in the UI.
DEID_CODES = mask_mod.DEID_LABELS
LOAD_TYPES = profile_mod.LOAD_TYPES
SCHEDULE_CODES = profile_mod.SCHEDULE_CODES

WEEKDAYS = ["monday", "tuesday", "wednesday", "thursday",
            "friday", "saturday", "sunday"]
WEEKDAY_KO = ["월요일", "화요일", "수요일", "목요일", "금요일", "토요일", "일요일"]

#: (index, label) 쌍으로 미리 묶어 템플릿에 넘긴다.
#: Measured: Jinja2 에는 `zip` 이 없어 `{% for code, ko in zip(...) %}` 가
#: `jinja2.exceptions.UndefinedError: 'zip' is undefined` 로 죽었다.
#: 스키마 상세 화면(컬럼 편집·제외 체크박스)이 실제로 500 을 내는 원인이었다.
WEEKDAY_OPTIONS = list(enumerate(WEEKDAY_KO))


# ---------------------------------------------------------------------------
# Live discovery
# ---------------------------------------------------------------------------

def discover() -> dict:
    """Query the live source databases and read Databricks statistics.

    Requirement 1 and 9: do not trust stored metadata — ask the database.
    """
    result: dict = {"engines": {}, "errors": []}

    for engine in sources_mod.ENGINES:
        try:
            schemas = sources_mod.list_schemas(engine)
            detail = {}
            for schema in schemas:
                tables = sources_mod.list_tables(engine, schema)
                detail[schema] = {
                    "tables": {
                        t: {
                            "columns": sources_mod.list_columns(engine, schema, t),
                            "rows": sources_mod.count_rows(engine, schema, t),
                        } for t in tables
                    }
                }
            result["engines"][engine] = detail
        except Exception as exc:                # noqa: BLE001
            result["errors"].append(f"{engine}: {type(exc).__name__}: {exc}")

    return result


def databricks_stats() -> dict:
    """Read run statistics back from the audit tables (requirement 9).

    Measured: both audit tables accumulate every run, so grouping only by
    `work_type` mixes the 200-row smoke runs with the 50,000-row
    verification. That made the panel show `FAIL 3` next to the real
    result, which reads as a current failure when it is a superseded one.

    Rows are therefore scoped to the current scale — nothing is deleted:
      · load_audit → `source_count = 50000`
      · etl_run_log → the most recent `run_id`
    The cumulative totals are reported next to the scoped figures so nothing
    is hidden.
    """
    try:
        schema = f"{cfg.get_databricks_config()['catalog']}." \
                 f"{cfg.get_databricks_config().get('meta_schema', 'pretest_meta')}"
        if not dbx.table_exists(f"{schema}.load_audit"):
            return {"사용 가능": False, "사유": "로그 테이블이 아직 생성되지 않았습니다"}

        latest = latest_run_id(schema)

        rows = dbx.execute_sql(
            f"SELECT work_type, etl_type, status_code, COUNT(*) AS cnt "
            f"FROM {schema}.etl_run_log WHERE run_id='{latest}' "
            "GROUP BY work_type, etl_type, status_code "
            "ORDER BY work_type, etl_type, status_code")

        load_rows = dbx.execute_sql(
            f"SELECT engine, COUNT(*) AS cnt, "
            f"       SUM(CASE WHEN count_match='Y' THEN 1 ELSE 0 END) AS ok "
            f"FROM {schema}.load_audit WHERE work_type='initial_load' "
            f"  AND source_count={VERIFIED_SCALE} "
            "GROUP BY engine ORDER BY engine")

        return {
            "사용 가능": True,
            "기준_run_id": latest,
            "검증규모": VERIFIED_SCALE,
            "집계": [dict(zip([c["name"] for c in rows["columns"]], r))
                    for r in rows["rows"]],
            "초기이관": [dict(zip([c["name"] for c in load_rows["columns"]], r))
                     for r in load_rows["rows"]],
            "load_audit_건수": dbx.fetch_value(
                f"SELECT COUNT(*) FROM {schema}.load_audit"),
            "etl_run_log_건수": dbx.fetch_value(
                f"SELECT COUNT(*) FROM {schema}.etl_run_log"),
            "최근건": recent_logs(schema),
        }
    except Exception as exc:                    # noqa: BLE001
        return {"사용 가능": False, "사유": f"{type(exc).__name__}: {exc}"}


def latest_run_id(schema: str) -> str:
    """Most recent ETL `run_id`, used to scope the aggregates."""
    rows = dbx.execute_sql(
        f"SELECT run_id FROM {schema}.etl_run_log "
        "GROUP BY run_id ORDER BY MAX(started_at) DESC LIMIT 1")
    return str(rows["rows"][0][0]) if rows["rows"] else ""


def recent_logs(schema: str, limit: int = 20) -> list[dict]:
    """Most recent audit rows, newest first.

    Measured: `etl_run_log` has **no** `source_count` / `target_count` columns.
    Those live in `load_audit`, which is the file-load count check. Selecting
    them here produced

        [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column ... 'source_count'
        cannot be resolved. Did you mean ... ['run_id', 'schedule_code', ...]

    and the whole statistics panel rendered that error instead of data.
    Only the columns this table actually declares are selected, plus
    `row_affected`, which is its own equivalent of an affected-row count.
    """
    result = dbx.execute_sql(
        f"SELECT run_id, work_type, engine, schema_name, table_name, "
        f"etl_type, status, status_code, workers, row_affected, "
        f"duration_sec, ended_at "
        f"FROM {schema}.etl_run_log ORDER BY ended_at DESC LIMIT {limit}")
    column_name = [c["name"] for c in result["columns"]]
    return [dict(zip(column_name, r)) for r in result["rows"]]


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    """Overview: profiles, live discovery, Databricks statistics."""
    found = discover()
    return render_template(
        "index.html",
        profiles=profile_mod.list_all(),
        discovery=found,
        stats=databricks_stats(),
        engines=sources_mod.ENGINES,
        deid_codes=DEID_CODES,
        load_types=LOAD_TYPES,
        schedule_codes=SCHEDULE_CODES,
    )


@app.route("/profiles")
def profiles():
    """All registered profiles."""
    return render_template("profiles.html",
                           profiles=profile_mod.list_all(),
                           engines=sources_mod.ENGINES)


@app.route("/profiles/<engine>/<schema>")
def profile_detail(engine: str, schema: str):
    """One schema's YAML definition plus live source columns."""
    document = profile_mod.read(engine, schema)
    live: dict = {}
    try:
        live = sources_mod.get_schema_columns(engine, schema)
    except Exception as exc:                    # noqa: BLE001
        flash(f"원천 조회 실패: {exc}", "error")

    return render_template(
        "profile_detail.html",
        engine=engine, schema=schema, document=document,
        live=live, summary=profile_mod.summary(engine, schema),
        deid_codes=DEID_CODES, load_types=LOAD_TYPES,
        schedule_codes=SCHEDULE_CODES, weekday_options=WEEKDAY_OPTIONS,
    )


@app.route("/profiles/<engine>/<schema>/save", methods=["POST"])
def profile_save(engine: str, schema: str):
    """Persist the edited definition back to the schema's YAML file."""
    document = profile_mod.read(engine, schema)

    form = request.form
    document["schedule"] = {"type": form.get("schedule_type", "daily")}
    document["registered_at"] = form.get("registered_at", date.today().isoformat())

    tables = form.getlist("table_name")
    entries = document.setdefault("tables", {})

    for table in tables:
        entry = entries.setdefault(table, {"columns": [], "include_columns": [],
                                           "exclude_columns": [],
                                           "deidentification": {}})
        # Columns: checked = keep, unchecked = exclude.
        all_columns = form.getlist(f"columns__{table}")
        excluded = form.getlist(f"exclude__{table}")
        entry["columns"] = all_columns
        entry["exclude_columns"] = [c for c in all_columns if c in excluded]
        entry["include_columns"] = form.getlist(f"include__{table}")

        entry["etl_type"] = form.get(f"etl_type__{table}", "append")
        entry["primary_key"] = form.get(f"pk__{table}", "")
        entry["active"] = form.get(f"active__{table}", "N")

        entry["deidentification"] = {
            c: form[f"deid__{table}__{c}"]
            for c in all_columns
            if form.get(f"deid__{table}__{c}")
        }

        schedule_type = form.get(f"schedule_type__{table}", "daily")
        schedule = {"type": schedule_type}
        if schedule_type == "weekly":
            schedule["day_of_week"] = int(form.get(f"dow__{table}", 0) or 0)
        if form.get(f"fixed_day__{table}"):
            schedule["fixed_day"] = int(form[f"fixed_day__{table}"])
        entry["schedule"] = schedule
        entry["registered_at"] = form.get(
            f"registered__{table}", date.today().isoformat())

        if entry["etl_type"] == "merge" and not entry.get("primary_key"):
            flash(f"{table}: merge 는 기본키가 필요합니다.", "error")
            return redirect(url_for("profile_detail", engine=engine, schema=schema))

    path = profile_mod.write(engine, schema, document)
    flash(f"{path.name} 저장 완료 ({len(tables)}개 테이블)", "success")
    return redirect(url_for("profile_detail", engine=engine, schema=schema))


@app.route("/builder")
def builder():
    """ETL builder: pick an engine/schema/table and register it."""
    return render_template("builder.html",
                           engines=sources_mod.ENGINES,
                           deid_codes=DEID_CODES,
                           load_types=LOAD_TYPES,
                           schedule_codes=SCHEDULE_CODES,
                           live=discover())


@app.route("/builder/register", methods=["POST"])
def builder_register():
    """Register one table into its schema YAML (create or update)."""
    engine = request.form["engine"]
    schema = request.form["schema"]
    table = request.form["table"]

    try:
        columns = sources_mod.list_columns(engine, schema, table)
    except Exception as exc:                    # noqa: BLE001
        flash(f"원천 컬럼 조회 실패: {exc}", "error")
        return redirect(url_for("builder"))

    if not columns:
        flash("원천에 해당 테이블이 없거나 컬럼이 없습니다.", "error")
        return redirect(url_for("builder"))

    entry = profile_mod.register_table(
        engine, schema, table,
        columns=columns,
        etl_type=request.form.get("etl_type", "append"),
        primary_key=request.form.get("primary_key") or None,
        include_columns=request.form.getlist("include_columns"),
        exclude_columns=request.form.getlist("exclude_columns"),
        deidentification={c: request.form[f"deid__{c}"]
                          for c in columns
                          if request.form.get(f"deid__{c}")},
        schedule={"type": request.form.get("schedule_type", "daily")},
        active=request.form.get("active", "N"),
    )
    flash(f"{engine}.{schema}.{table} 등록 완료 "
          f"(etl_type={entry['etl_type']}, 컬럼 {len(columns)}개)", "success")
    return redirect(url_for("profile_detail", engine=engine, schema=schema))


@app.route("/columns")
def columns():
    """Column-level view across every schema."""
    matrix = []
    for engine in sources_mod.ENGINES:
        try:
            schemas = sources_mod.list_schemas(engine)
        except Exception as exc:                # noqa: BLE001
            flash(f"{engine} 조회 실패: {exc}", "error")
            continue
        for schema in schemas:
            try:
                cols = sources_mod.get_schema_columns(engine, schema)
            except Exception:                   # noqa: BLE001
                continue
            for table, meta in sorted(cols.items()):
                matrix.append({
                    "engine": engine, "schema": schema, "table": table,
                    "columns": [c["name"] for c in meta],
                    "types": {c["name"]: c["type"] for c in meta},
                    "rows": sources_mod.count_rows(engine, schema, table),
                })
    return render_template("columns.html", matrix=matrix,
                           engines=sources_mod.ENGINES)


@app.route("/table/add", methods=["GET", "POST"])
def table_add():
    """Quick registration for a newly created source table."""
    engine = request.values.get("engine", sources_mod.ENGINES[0])
    schema = request.values.get("schema", "")

    tables: list[str] = []
    if schema:
        try:
            tables = sources_mod.list_tables(engine, schema)
        except Exception:                       # noqa: BLE001
            pass

    if request.method == "POST":
        table = request.form["table"]
        try:
            cols = sources_mod.list_columns(engine, schema, table)
        except Exception as exc:                # noqa: BLE001
            flash(f"컬럼 조회 실패: {exc}", "error")
            return redirect(url_for("table_add", engine=engine, schema=schema))

        profile_mod.register_table(
            engine, schema, table, columns=cols,
            etl_type=request.form.get("etl_type", "append"),
            primary_key=request.form.get("primary_key") or None,
            exclude_columns=request.form.getlist("exclude_columns"),
            deidentification={c: request.form[f"deid__{c}"] for c in cols
                              if request.form.get(f"deid__{c}")},
            schedule={"type": request.form.get("schedule_type", "daily")},
        )
        flash(f"{table} 이 {engine}_{schema}.yaml 에 등록되었습니다.", "success")
        return redirect(url_for("profile_detail", engine=engine, schema=schema))

    return render_template("table_add.html",
                           engines=sources_mod.ENGINES,
                           engine=engine, schema=schema, tables=tables,
                           schemas=cfg.get_section("sources", {}).get(engine, {}).get("schemas", []),
                           deid_codes=DEID_CODES, load_types=LOAD_TYPES,
                           schedule_codes=SCHEDULE_CODES)


@app.route("/api/profiles")
def api_profiles():
    """JSON view of every profile — handy for scripted checks."""
    return jsonify(profile_mod.list_all())


@app.route("/api/stats")
def api_stats():
    """JSON view of the Databricks statistics."""
    return jsonify(databricks_stats())


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL 대시보드")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8540)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    profiles_dir = cfg.profile_folder()
    templates_dir = Path(__file__).parent / "templates"
    print(f"프로파일 폴더 : {profiles_dir}")
    print(f"템플릿 폴더  : {templates_dir}")
    print(f"대시보드      : http://{args.host}:{args.port}")
    app.run(host=args.host, port=args.port, debug=args.debug)
