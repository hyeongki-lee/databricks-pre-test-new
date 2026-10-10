"""
프로파일 편집 개선 — YAML 원본 편집 · 변경 요약 · 되돌리기.

왜 필요한가
------------
기존 화면은 체크박스와 드롭다운으로만 편집됐다. 그래서 두 가지 불편이 있었다.

  1. "무엇이 바뀌는지" 를 알 수 없었다. 저장하면 리다이렉트만 되고
     목록으로 돌아와서, 저장이 실제로 반영됐는지 눈으로 확인이 안 됐다.
  2. YAML 원문을 보고 싶거나, 폰으로 만들 수 없는 고친 내용을 그대로
     넣을 방법이 없었다. 그래서 결국 저장소에서 YAML 파일을 직접 열게 된다.

추가한 것
--------
  GET  /profiles/<e>/<s>/raw        YAML 원문을 textarea 로 편집
  POST /profiles/<e>/<s>/raw        저장 (검증 통과 시에만)
  GET  /profiles/<e>/<s>/diff        직전 저장과의 차이
  POST /profiles/<e>/<s>/rollback    되돌리기

저장 안전장치
-------------
  · YAML 파싱 실패 → 저장하지 않고 오류 표시 (파일 보호)
  · 구조 검증 실패 → 저장하지 않음
  · 저장 직전 자동 백업 → 롤백 가능
"""
from __future__ import annotations

import difflib
import shutil
import sys
from datetime import datetime
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from lib import config as cfg                     # noqa: E402
from lib import profile as profile_mod            # noqa: E402

#: 저장 직전에 남기는 백업 확장자
BACKUP_SUFFIX = ".bak"


# ---------------------------------------------------------------------------
# Backup / rollback
# ---------------------------------------------------------------------------

def backup_path(engine: str, schema: str) -> Path:
    return profile_mod.file_path(engine, schema).with_suffix(
        profile_mod.file_path(engine, schema).suffix + BACKUP_SUFFIX)


def make_backup(engine: str, schema: str) -> Path | None:
    """Copy the current YAML aside so the write can be undone."""
    source = profile_mod.file_path(engine, schema)
    if not source.exists():
        return None
    target = backup_path(engine, schema)
    shutil.copy2(source, target)
    return target


def rollback(engine: str, schema: str) -> tuple[bool, str]:
    """Restore the backup taken before the last write."""
    backup = backup_path(engine, schema)
    if not backup.exists():
        return False, "되돌릴 백업이 없습니다"
    target = profile_mod.file_path(engine, schema)
    shutil.copy2(backup, target)
    return True, f"{backup.name} 로 되돌렸습니다"


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_yaml_text(text: str) -> tuple[bool, dict | None, str]:
    """Parse and structurally check a YAML document before it is written.

    Measured: writing first and validating afterwards loses the file whenever
    the text is malformed — that is exactly the situation this page exists to
    serve. So parsing happens on the submitted string, and only a valid
    document ever reaches disk.
    """
    import yaml

    try:
        document = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        return False, None, f"YAML 문법 오류: {exc}"
    if not isinstance(document, dict):
        return False, None, "최상위는 맵(dict)이어야 합니다"

    tables = document.get("tables")
    if not isinstance(tables, dict) or not tables:
        return False, None, "tables 가 비어 있거나 형식이 아닙니다"

    problems: list[str] = []
    for name, entry in tables.items():
        if not isinstance(entry, dict):
            problems.append(f"{name}: 항목이 맵이 아닙니다")
            continue
        columns = entry.get("columns")
        if not isinstance(columns, list) or not columns:
            problems.append(f"{name}: columns 가 비어 있습니다")
            continue
        exclude = entry.get("exclude_columns") or []
        unknown = [c for c in exclude if c not in columns]
        if unknown:
            problems.append(
                f"{name}: exclude 에 없는 컬럼 {unknown} — columns 에서 "
                f"제외된 컬럼이기에")
        include = entry.get("include_columns") or []
        for c in include:
            if c not in columns:
                problems.append(
                    f"{name}: include 의 '{c}' 가 columns 에 없습니다")
        if entry.get("etl_type") == "merge" and not entry.get("primary_key"):
            problems.append(f"{name}: merge 인데 primary_key 가 비어 있습니다")

    if problems:
        return False, document, " · ".join(problems)
    return True, document, ""


def summarise_changes(before: dict | None, after: dict | None) -> list[str]:
    """Human-readable list of what a write actually changed."""
    if not isinstance(before, dict):
        return ["(기존 문서 없음 — 새로 저장)"]

    lines: list[str] = []
    old_tables = (before or {}).get("tables") or {}
    new_tables = (after or {}).get("tables") or {}

    for name in sorted(set(old_tables) | set(new_tables)):
        old = old_tables.get(name) or {}
        new = new_tables.get(name) or {}
        # Measured: the membership test must be against `old_tables`, not
        # `old`. `old` holds a table's *fields*, so `name not in old` is
        # always True and every table was reported as "새로 등록" — the
        # change summary never reported a single real change.
        if name not in old_tables:
            lines.append(f"{name}: 새로 등록")
            continue
        if name not in new_tables:
            lines.append(f"{name}: 삭제됨")
            continue

        if old.get("etl_type") != new.get("etl_type"):
            lines.append(f"{name}: 처리종류 {old.get('etl_type')} → "
                         f"{new.get('etl_type')}")
        if old.get("primary_key") != new.get("primary_key"):
            lines.append(f"{name}: 기본키 '{old.get('primary_key') or '-'}' → "
                         f"'{new.get('primary_key') or '-'}'")
        if old.get("active") != new.get("active"):
            lines.append(f"{name}: 활성 {old.get('active')} → {new.get('active')}")

        old_ex = list(old.get("exclude_columns") or [])
        new_ex = list(new.get("exclude_columns") or [])
        if sorted(old_ex) != sorted(new_ex):
            added = [c for c in new_ex if c not in old_ex]
            removed = [c for c in old_ex if c not in new_ex]
            if added:
                lines.append(f"{name}: 제외 추가 {added}")
            if removed:
                lines.append(f"{name}: 제외 해제 {removed}")

        old_inc = list(old.get("include_columns") or [])
        new_inc = list(old.get("include_columns") or [])
        if sorted(old_inc) != sorted(new_inc):
            lines.append(f"{name}: include {old_inc or '-'} → {new_inc or '-'}")

        old_deid = old.get("deidentification") or {}
        new_deid = new.get("deidentification") or {}
        if old_deid != new_deid:
            lines.append(f"{name}: 비식별화 {old_deid or '-'} → "
                         f"{new_deid or '-'}")

        old_sched = (old.get("schedule") or {}).get("type")
        new_sched = (new.get("schedule") or {}).get("type")
        if old_sched != new_sched:
            lines.append(f"{name}: 작업주기 {old_sched} → {new_sched}")

    if (before or {}).get("schedule") != (after or {}).get("schedule"):
        lines.append(f"스키마 작업주기 "
                     f"{(before or {}).get('schedule')} → "
                     f"{(after or {}).get('schedule')}")
    return lines or ["변경 없음"]


def unified_diff(engine: str, schema: str, new_text: str) -> str:
    """Diff between the stored YAML and the submitted text."""
    current = profile_mod.file_path(engine, schema).read_text(encoding="utf-8")
    return "".join(difflib.unified_diff(
        current.splitlines(keepends=True),
        new_text.splitlines(keepends=True),
        fromfile="현재 저장됨", tofile="편집 중인 내용", n=3))


def touch(engine: str, schema: str) -> None:
    """Refresh `updated_at` so a save is visible in the file itself."""
    path = profile_mod.file_path(engine, schema)
    document = profile_mod.read(engine, schema)
    document["updated_at"] = datetime.now().isoformat(timespec="seconds")
    profile_mod.write(engine, schema, document)


def write_raw_text(engine: str, schema: str, text: str) -> Path:
    """Write the submitted YAML **verbatim**.

    Measured: `profile.write()` round-trips through `yaml.safe_dump`, which
    **drops every comment**. That is fine for the checkbox editor but wrong
    for the raw editor — a user who writes "# 원천에서 빠진 컬럼" to record
    intent would silently lose it on save, which is the one reason to use a
    raw editor at all.

    So the raw editor writes the exact text it was given. The document has
    already been parsed and validated by `validate_yaml_text` before this is
    reached, so the file is known to be well-formed.
    """
    path = profile_mod.file_path(engine, schema)
    path.parent.mkdir(parents=True, exist_ok=True)

    # 탭 문자와 줄바꿈을 정규화한다. Windows 편집기에서 옮긴 붙여넣기의
    # 흔적을 남기지 않기 위함이며, 내용은 바꾸지 않는다.
    normalised = text.replace("\r\n", "\n").replace("\r", "\n")
    if not normalised.endswith("\n"):
        normalised += "\n"

    path.write_text(normalised, encoding="utf-8")
    return path


def has_comment(text: str) -> bool:
    """Whether the text carries a comment line (used for a save warning)."""
    return any(line.strip().startswith("#") for line in text.splitlines())


if __name__ == "__main__":
    # 자체 점검
    folder = cfg.profile_folder()
    files = sorted(folder.glob("*.yaml"))
    print(f"프로파일 {len(files)}개")
    for f in files:
        engine, _, schema = f.stem.partition("_")
        ok, document, error = validate_yaml_text(f.read_text(encoding="utf-8"))
        print(f"  {'OK  ' if ok else 'NG  '}{f.name}  {error[:70]}")