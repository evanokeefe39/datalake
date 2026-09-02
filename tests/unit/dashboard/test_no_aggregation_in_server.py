"""Grep guard: dashboard/server.py must be a thin view projector.

Metrics centralization contract (2026-09-02): all metric/aggregation logic lives
in canonical DuckDB serving views. The server may SELECT from views and shape
JSON — it may never compute metrics itself.
"""

from __future__ import annotations

from pathlib import Path

_SERVER = Path(__file__).resolve().parents[3] / "dashboard" / "server.py"

# Aggregation expressions, inline window functions, and the deleted Python
# fan-out helpers. Any hit means aggregation crept back into the server.
_FORBIDDEN = [
    "AVG(",
    "SUM(",
    "GROUP BY",
    "ROW_NUMBER(",
    "PERCENT_RANK(",
    "_post_counts",
    "_standout_and_hot_counts",
    "_attach_relative_performance",
]


def test_no_aggregation_in_server():
    src = _SERVER.read_text(encoding="utf-8")
    offenders = [pat for pat in _FORBIDDEN if pat in src]
    assert not offenders, (
        "Aggregation logic found back in dashboard/server.py: "
        f"{offenders}. Metrics belong in canonical serving views "
        "(defs/serving/assets.py); the server is a thin projector."
    )
