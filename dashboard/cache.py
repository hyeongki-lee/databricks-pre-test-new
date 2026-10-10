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

#: 캐시 유지 시간(초). 이 안에 다시 누르면 같은 값을 쓴다.
DEFAULT_TTL = 120


class TTLCache:
    """Single-slot cache with stale-on-error behaviour."""

    def __init__(self, ttl: int = DEFAULT_TTL) -> None:
        self.ttl = ttl
        self._value: Any = None
        self._stored_at: float = 0.0
        self._error: str = ""
        self._lock = threading.Lock()

    def age(self) -> float:
        """Seconds since the value was produced."""
        return 0.0 if self._value is None else time.time() - self._stored_at

    def error(self) -> str:
        return self._error

    def get_or_refresh(self, producer: Callable[[], Any]) -> Any:
        """Return a cached value, refreshing it when stale.

        A refresh failure keeps the previous value and records the reason, so
        one dropped connection does not blank the page.
        """
        with self._lock:
            fresh = self._value is not None and (time.time() - self._stored_at) < self.ttl
            if fresh:
                return self._value

            try:
                self._value = producer()
                self._stored_at = time.time()
                self._error = ""
            except Exception as exc:                # noqa: BLE001
                self._error = f"{type(exc).__name__}: {str(exc)[:160]}"
                if self._value is not None:
                    # 이전 값을 그대로 둔다. 단, 오래된 값임을 알린다.
                    return self._value
                raise
            return self._value


#: 개요 화면이 쓰는 캐시. 라우트마다 따로 두지 않는다.
discovery_cache = TTLCache()

#: Databricks 통계 캐시. 같은 이유지만 별도로 둔다 — 요약과 원천 조회는
#: 실패해도 서로를拖累시키지 않아야 하기 때문이다.
stats_cache = TTLCache(ttl=90)