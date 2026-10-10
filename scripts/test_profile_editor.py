"""
프로파일 편집 기능 검증 (1회 실행 후 삭제).

확인할 것
--------
  ① YAML 원본 편집 화면이 열린다
  ② 정상 YAML 을 저장하면 파일에 반영된다
  ③ 변경 요약(무엇이 바뀌었는지)이 보인다
  ④ 문법이 틀린 YAML 은 **파일을 건드리지 않고** 거부된다
  ⑤ 구조 위반(merge 인데 primary_key 없음)도 거부된다
  ⑥ 되돌리기로 원상 복구된다
  ⑦ 전부 끝나면 파일이 원래대로다
"""
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from http.cookiejar import CookieJar
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from lib import profile as profile_mod            # noqa: E402

BASE = "http://127.0.0.1:8540"
ENGINE, SCHEMA = "mysql", "mysql_schema_1"

jar = CookieJar()
opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))
results: list[tuple[bool, str]] = []


def check(ok: bool, label: str, detail: str = "") -> None:
    results.append((ok, label))
    mark = "PASS" if ok else "FAIL"
    print(f"[{mark}] {label}" + (f"  — {detail}" if detail else ""))


def get(url: str) -> tuple[int, str]:
    try:
        with opener.open(url, timeout=180) as r:
            return r.status, r.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8", errors="replace")


def post(url: str, data: dict) -> tuple[int, str]:
    body = urllib.parse.urlencode(data, doseq=True).encode("utf-8")
    request = urllib.request.Request(url, data=body, method="POST")
    request.add_header("Content-Type", "application/x-www-form-urlencoded")
    try:
        with opener.open(request, timeout=180) as r:
            return r.status, r.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8", errors="replace")


def main() -> int:
    path = profile_mod.file_path(ENGINE, SCHEMA)
    original = path.read_text(encoding="utf-8")

    # ① 편집 화면
    status, html = get(f"{BASE}/profiles/{ENGINE}/{SCHEMA}/raw")
    check(status == 200 and "yaml_text" in html, "① YAML 편집 화면 열림")
    check("구문 검사 통과" in html, "   저장 전 구문 검사 표시")

    raw = f"{BASE}/profiles/{ENGINE}/{SCHEMA}/raw"

    # ② 정상 저장 — 제외 컬럼 하나를 추가
    modified = original.replace(
        "      exclude_columns: [description]",
        "      exclude_columns: [address, description]", 1)
    if modified == original:
        modified = original + "\n# 검증용 주석\n"
        touched = False
    else:
        touched = True

    status, body = post(raw, {"text": modified, "action": "save"})
    after_text = path.read_text(encoding="utf-8")
    # Measured: comparing bytes is wrong here. The writer normalises the final
    # newline, and the checkbox editor re-serialises through yaml.safe_dump.
    # Compare parsed documents instead — that is what "the edit landed" means.
    import yaml
    saved = yaml.safe_load(after_text)
    check(status == 200 and saved == yaml.safe_load(modified),
          "② 정상 YAML 저장 반영",
          "address 추가" if touched else "주석 추가")
    check(after_text.rstrip().endswith("# 검증용 주석".rstrip())
          or "address" in str(saved["tables"]["table_2"]["exclude_columns"]),
          "   저장된 파일 내용 확인")

    # ③ 변경 요약
    check("제외 추가" in body or "변경 없음" in body,
          "③ 변경 요약 표시", "flash 로 표시")

    # ④ 문법 오류 거부
    broken = original.replace("tables:", "tables: [ 이건 잘못된 문법", 1)
    status, body = post(raw, {"text": broken, "action": "save"})
    unchanged = path.read_text(encoding="utf-8") == modified
    check(status == 400 and unchanged,
          "④ 문법 오류 거부 + 파일 보존",
          f"HTTP {status}, 파일 변경 {'없음' if unchanged else '됨(위험!)'}")

    # ⑤ 구조 위반 거부 (merge 인데 primary_key 비움)
    import yaml
    document = yaml.safe_load(modified)
    first = next(iter(document["tables"]))
    document["tables"][first]["etl_type"] = "merge"
    document["tables"][first]["primary_key"] = ""
    status, body = post(raw, {"text": yaml.safe_dump(document,
                                                     allow_unicode=True),
                              "action": "save"})
    unchanged = path.read_text(encoding="utf-8") == modified
    check(status == 400 and unchanged,
          "⑤ merge+primary_key 없음 거부 + 파일 보존",
          f"HTTP {status}")
    check("primary_key" in body or "기본키" in body,
          "   거부 사유에 primary_key 언급")

    # ⑥ 되돌리기
    status, body = post(f"{BASE}/profiles/{ENGINE}/{SCHEMA}/rollback",
                        {"noop": "1"})
    restored = path.read_text(encoding="utf-8")
    check(status == 200 and restored == original,
          "⑥ 되돌리기로 원상 복구")

    # ⑦ 최종 확인
    check(path.read_text(encoding="utf-8") == original,
          "⑦ 최종 상태 원본과 동일")

    print("\n" + "=" * 60)
    passed = sum(1 for ok, _ in results if ok)
    print(f"통과 {passed} / {len(results)}")
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    raise SystemExit(main())