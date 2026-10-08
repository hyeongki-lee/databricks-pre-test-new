"""
긴 작업 감시자 — 멈추면 자동으로 재실행한다.

왜 필요한가
-----------
이 프로젝트는 Free Edition 쿼터 한도 때문에 **길게 걸리는 명령**이 많다.
(rclone 복제, 60테이블 적재, ETL 병렬 처리 등)

관찰된 문제
-----------
    * 어떤 명령이 중간에 멈추고 끝내 결과를 내지 않는다.
    * 프로세스는 살아 있는데 CPU 를几乎 쓰지 않는다(교착).
    * 사람이 계속 지켜봐야 한다.

이 해결책
--------
각 명령을 **자식 프로세스**로 띄우고, 여기서 감시한다.

    1. 자식이 출력(log 파일)을 계속 갱신하는지 본다.
    2. `무응답초`(기본 180초) 동안 **출력이 전혀 없으면** 멈춘 것으로 판단.
    3. 멈췄으면 kill 하고 **재시도**한다(최대 3회).
    4. 다음 시도 전까지 시간을 둔다(Databricks 한도 회복).

"프로세스 살아 있음"만으로는 판단할 수 없다. 확실한 신호는 **출력 정지**다.
그래서 모든 자식 프로세스는 반드시 `> log` 로 리다이렉트해서 실행한다.

사용법
------
    # 간단 실행(자동 감시 포함)
    python scripts/watchdog.py -- python data-prep/rclone_step.py

    # 여러 명령을 순서대로
    python scripts/watchdog.py --sh "cmd1" "cmd2" "cmd3"
"""
from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LOG_FOLDER = PROJECT_ROOT / "work" / "watchdog"
LOG_FOLDER.mkdir(parents=True, exist_ok=True)

# ⚠ 실측 함정: Windows 콘솔은 cp949(cp1252) 이라 `—` 같은 문자를 출력할 때
#   `UnicodeEncodeError: 'cp949' codec can't encode character` 로 죽는다.
#   watchdog 는 감시 중이므로 로그 한 줄 때문에 죽으면 안 된다.
#   stdout 을 UTF-8 로 강제하고, 실패하면 errors='replace' 로 물러선다.
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass

DEFAULT_TIMEOUT_SEC = 180        # 이 시간 동안 출력이 없으면 멈춘 것으로 판단
DEFAULT_MAX_RETRY = 3
DEFAULT_WAIT_SEC = 20          # 재시도 전 대기(Databricks 한도 회복 여유)


def log(message: str) -> None:
    moment = datetime.now().strftime("%H:%M:%S")
    line = f"[{moment}] {message}"
    print(line, flush=True)


def monitor(command_list: list[str], *,
          timeout_sec: int = DEFAULT_TIMEOUT_SEC,
          max_retries: int = DEFAULT_MAX_RETRY,
          wait_sec: int = DEFAULT_WAIT_SEC) -> int:
    """명령을 하나씩 실행하고, 멈추면 재시도한다.

    Returns:
        프로세스 종료 코드 (0 이면 전부 성공)
    """
    exit_code = 0

    for index, command in enumerate(command_list, start=1):
        log(f"[{index}/{len(command_list)}] 실행: {command}")
        success = False
        last_error = ""

        for attempt in range(1, max_retries + 1):
            log_path = LOG_FOLDER / (
                f"{datetime.now():%H%M%S}_{index}_{attempt}.log")
            log(f"  시도 {attempt}/{max_retries} · 로그: {log_path.name}")

            try:
                with open(log_path, "w", encoding="utf-8") as fp:
                    child = subprocess.Popen(
                        command,
                        shell=True,
                        stdout=fp,
                        stderr=subprocess.STDOUT,
                        cwd=str(PROJECT_ROOT),
                        # Windows 에서 자식 프로세스가 Ctrl+C 를 따로 받지 않게
                        creationflags=(subprocess.CREATE_NEW_PROCESS_GROUP
                                       if os.name == "nt" else 0),
                    )
            except Exception as exc:             # noqa: BLE001
                last_error = f"실행 실패: {type(exc).__name__}: {exc}"
                log(f"  [실패] {last_error}")
                time.sleep(wait_sec)
                continue

            start = time.time()
            last_output = start
            unresponsive = False

            while True:
                elapsed = time.time() - start

                # 로그 파일이 갱신되었는지 확인 → 출력이 살아 있다는 신호
                try:
                    if log_path.stat().st_mtime > last_output:
                        last_output = log_path.stat().st_mtime
                except OSError:
                    pass

                code = child.poll()
                if code is not None:
                    log(f"  종료 코드 {code} · {elapsed:.0f}초 경과")
                    if code == 0:
                        success = True
                    else:
                        last_error = f"종료 코드 {code} (로그: {log_path.name})"
                    break

                if time.time() - last_output > timeout_sec:
                    unresponsive = True
                    log(f"  [멈춤 감지] {timeout_sec}초 동안 출력 없음 → kill")
                    try:
                        if os.name == "nt":
                            subprocess.run(
                                ["taskkill", "/F", "/T", "/PID", str(child.pid)],
                                capture_output=True, timeout=30)
                        else:
                            os.killpg(os.getpgid(child.pid), signal.SIGKILL)
                    except Exception as exc:      # noqa: BLE001
                        log(f"  [참고] kill 실패: {exc}")
                    break

                time.sleep(5)

            if success:
                break
            if attempt < max_retries:
                log(f"  {wait_sec}초 대기 후 재시도 …")
                time.sleep(wait_sec)

        if success:
            log(f"[{index}/{len(command_list)}] 성공")
        else:
            exit_code = 1
            log(f"[{index}/{len(command_list)}] 최종 실패 — {last_error}")
            log(f"  로그 확인: {LOG_FOLDER}")

    return exit_code


def progress(log_name: str, last_line_count: int = 25) -> None:
    """저장된 로그의 끝부분을 보여준다(문제 진단용)."""
    path = LOG_FOLDER / log_name
    if not path.exists():
        print(f"로그 없음: {path}")
        return
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    print(f"--- {path.name} (총 {len(lines)}줄, 끝 {last_line_count}줄) ---")
    for line in lines[-last_line_count:]:
        print(line)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="멈춤 감지 · 자동 재시도")
    # Measured: argparse derives `dest` from the option string, so a Korean
    # flag such as `--무응답초` would expose `args.무응답초`. The attributes are
    # now English, so every `dest` is declared explicitly. The option strings
    # stay Korean on purpose — every invocation in the runbook and in the
    # README already uses them, and renaming the flags would break them.
    parser.add_argument("--무응답초", dest="timeout_sec", type=int,
                        default=DEFAULT_TIMEOUT_SEC,
                        help="이 시간 동안 출력이 없으면 멈춘 것으로 판단")
    parser.add_argument("--최대재시도", dest="max_retries", type=int,
                        default=DEFAULT_MAX_RETRY)
    parser.add_argument("--대기초", dest="wait_sec", type=int,
                        default=DEFAULT_WAIT_SEC)
    parser.add_argument("--sh", dest="sh", nargs="+", default=None,
                        help="순서대로 실행할 셸 명령들")
    parser.add_argument("--로그", dest="log", default=None,
                        help="저장된 로그의 끝을 출력")
    parser.add_argument("command", nargs="*", help="실행할 명령 1개")
    args = parser.parse_args()

    if args.log:
        progress(args.log)
        sys.exit(0)

    commands = args.sh or ([args.command] if args.command else [])
    if not commands:
        parser.print_help()
        sys.exit(2)

    log(f"감시 시작 · 무응답 {args.timeout_sec}초 · 최대 재시도 {args.max_retries}회")
    code = monitor(commands, timeout_sec=args.timeout_sec,
                  max_retries=args.max_retries, wait_sec=args.wait_sec)
    log(f"감시 종료 · 코드 {code}")
    sys.exit(code)
