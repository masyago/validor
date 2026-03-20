from __future__ import annotations

import os
import re
import time
from collections import Counter, defaultdict
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import event
from sqlalchemy.engine import Engine


def _bool_env(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def query_metrics_enabled() -> bool:
    """Feature flag for query metrics.

    Enabled via `CLA_QUERY_METRICS=1`.
    """

    return _bool_env("CLA_QUERY_METRICS", False)


_WS_RE = re.compile(r"\s+")
_LITERAL_RE = re.compile(
    r"(?:'(?:''|[^'])*')|(?:\b\d+(?:\.\d+)?\b)", re.IGNORECASE
)


def fingerprint_sql(statement: str) -> str:
    """Create a coarse SQL fingerprint for grouping similar statements.

    This intentionally avoids heavy dependencies. It:
    - collapses whitespace
    - replaces quoted strings and numeric literals with '?'

    It's good enough for 'top statements' reporting.
    """

    s = statement.strip()
    s = _WS_RE.sub(" ", s)
    s = _LITERAL_RE.sub("?", s)
    return s


@dataclass
class QueryCollector:
    query_count: int = 0
    total_db_time_s: float = 0.0
    fingerprint_count: Counter[str] = field(default_factory=Counter)
    fingerprint_time_s: dict[str, float] = field(
        default_factory=lambda: defaultdict(float)
    )

    def record(self, statement: str, duration_s: float) -> None:
        fp = fingerprint_sql(statement)
        self.query_count += 1
        self.total_db_time_s += duration_s
        self.fingerprint_count[fp] += 1
        self.fingerprint_time_s[fp] += duration_s

    def top_by_total_time(self, n: int = 10) -> list[dict[str, Any]]:
        items = []
        for fp, total_time in self.fingerprint_time_s.items():
            items.append(
                {
                    "fingerprint": fp,
                    "total_time_s": total_time,
                    "count": int(self.fingerprint_count.get(fp, 0)),
                }
            )
        items.sort(key=lambda d: d["total_time_s"], reverse=True)
        return items[:n]

    def top_by_count(self, n: int = 10) -> list[dict[str, Any]]:
        items = []
        for fp, count in self.fingerprint_count.items():
            items.append(
                {
                    "fingerprint": fp,
                    "total_time_s": float(
                        self.fingerprint_time_s.get(fp, 0.0)
                    ),
                    "count": int(count),
                }
            )
        items.sort(key=lambda d: d["count"], reverse=True)
        return items[:n]


_ACTIVE_COLLECTOR: ContextVar[QueryCollector | None] = ContextVar(
    "ACTIVE_QUERY_COLLECTOR", default=None
)


def collect_queries() -> "_CollectQueries":
    """Context manager that activates query collection for the current context."""

    return _CollectQueries()


class _CollectQueries:
    def __init__(self) -> None:
        self.collector = QueryCollector()
        self._token = None

    def __enter__(self) -> QueryCollector:
        self._token = _ACTIVE_COLLECTOR.set(self.collector)
        return self.collector

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._token is not None:
            _ACTIVE_COLLECTOR.reset(self._token)


_INSTALLED_ENGINES: set[int] = set()


def install_listeners(engine: Engine) -> None:
    """Install SQLAlchemy listeners on the given Engine (idempotent)."""

    engine_id = id(engine)
    if engine_id in _INSTALLED_ENGINES:
        return
    _INSTALLED_ENGINES.add(engine_id)

    @event.listens_for(engine, "before_cursor_execute")
    def _before_cursor_execute(
        conn,
        cursor,
        statement: str,
        parameters: Any,
        context,
        executemany: bool,
    ) -> None:
        context._cla_query_start_time_s = time.perf_counter()

    @event.listens_for(engine, "after_cursor_execute")
    def _after_cursor_execute(
        conn,
        cursor,
        statement: str,
        parameters: Any,
        context,
        executemany: bool,
    ) -> None:
        collector = _ACTIVE_COLLECTOR.get()
        if collector is None:
            return
        start = getattr(context, "_cla_query_start_time_s", None)
        if start is None:
            return
        collector.record(statement, time.perf_counter() - start)
