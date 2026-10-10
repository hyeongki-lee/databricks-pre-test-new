"""
개요 화면 캐시.

문제
----
`/` 는 원천 DB 3곳(테이블 60개의 컬럼·건수)과 Databricks 를 매번 새로 조회한다.
실측하니 **25초**가 걸렸고, 첫 요청은 연결 초기화로 30초를 넘겨 라우트 점검
스크립트가 타임아웃을 받아 실제로 죽었다.

요구사항이 "대시보드 실행 시 접속하여 조회" 이므로 값을 버리면 안 된다.
그래서 **짧은 시간 동안만 재사용**한다.

설계
----
  · TTL 동안 같은 결과를 돌려준다(기본 120초)
  · 조회가 실패해도 이전 값을 계속 쓴다 — 잠깐의 연결 실패로
    화면이 통째로 죽는 것보다 낫다
  · 화면에 "이 값은 언제 조회한 것인지" 를 함께 보여준다
    (캐시가 만료됐는데 조회가 실패했을 때 그 사실을 감춰야 안 된다)
  · `/api/stats` 는 원본 값을 그대로 준다. 캐시를 우회한다
"""
from __future__ import annotations

import threading
import time
from typing import Any, Callable

#: 캐시 유지 시간(초). 이보다 오래되면 값을 즉시 내보내고 백그라운드로
#: 갱신한다(stale-while-revalidate).
#:
#: Measured: 조회가 34초 걸린다. TTL 이 짧으면 그 비용을 방문자가 주게
#: 되므로 10분으로 두었다. 값의 나이는 화면에 표시되므로 오래된 값을
#: 새 값으로 착각하지 않는다.
DEFAULT_TTL = 600


class TTLCache:
    """Stale-while-revalidate cache.

    Plain TTL caching was measured to be insufficient here. The query costs
    about 34 seconds, and a 120-second TTL meant somebody still paid that
    cost roughly every two minutes — the page was "fast most of the time",
    which is worse than a predictable behaviour.

    So the rule is: **never make the visitor wait.**

      · value younger than `ttl`  → return it
      · value older than `ttl`    → return it *immediately* and refresh in a
                                   background thread
      · no value at all           → query inline (the first load has no
                                   choice), then cache it

    The page shows the age, so a stale value is never mistaken for a fresh
    one. A refresh failure keeps the previous value and records the reason.
    """

    def __init__(self, ttl: int = DEFAULT_TTL) -> None:
        self.ttl = ttl
        self._value: Any = None
        self._stored_at: float = 0.0
        self._error: str = ""
        self._refreshing = False
        self._lock = threading.Lock()

    def age(self) -> float:
        """Seconds since the value was produced."""
        return 0.0 if self._value is None else time.time() - self._stored_at

    def error(self) -> str:
        return self._error

    def refreshing(self) -> bool:
        return self._refreshing

    def _refresh_inline(self, producer: Callable[[], Any]) -> Any:
        try:
            self._value = producer()
            self._stored_at = time.time()
            self._error = ""
        except Exception as exc:                # noqa: BLE001
            self._error = f"{type(exc).__name__}: {str(exc)[:160]}"
            if self._value is None:
                raise
        return self._value

    def _refresh_background(self, producer: Callable[[], Any]) -> None:
        def run() -> None:
            try:
                self._refresh_inline(producer)
            except Exception:                   # noqa: BLE001
                pass                              # already recorded in _error
            finally:
                with self._lock:
                    self._refreshing = False

        with self._lock:
            if self._refreshing:
                return
            self._refreshing = True
        threading.Thread(target=run, daemon=True,
                         name="cache-refresh").start()

    def get_or_refresh(self, producer: Callable[[], Any]) -> Any:
        """Return a value immediately, refreshing in the background."""
        with self._lock:
            if self._value is None:
                # First load: nothing to serve, so the query must happen now.
                self._refreshing = False
                return self._refresh_inline(producer)

            stale = (time.time() - self._stored_at) >= self.ttl
            value = self._value

        if stale:
            self._refresh_background(producer)
        return value


#: 개요 화면이 쓰는 캐시. 라우트마다 따로 두지 않는다.
discovery_cache = TTLCache()

#: 컬럼 목록 캐시. /columns 와 /builder 가 함께 쓴다.
column_matrix_cache = TTLCache(ttl=600)

#: Databricks 통계 캐시. 같은 이유지만 별도로 둔다 — 요약과 원천 조회는
#: 실패해도 서로를拖累시키지 않아야 하기 때문이다.
stats_cache = TTLCache(ttl=600)