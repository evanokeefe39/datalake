# ADR-0005: Metrics computed in warehouse views; dashboard/API is a thin projector

- Status: Accepted
- Decided: 2026-08 (backfilled 2026-09-03)

## Context

Dashboard metrics must be computed in exactly one place with consistent
semantics across every surface (posts tables, creators page, hot/rising cards,
overview). If the dashboard/API recomputed aggregates (AVG/SUM/GROUP BY/window
functions) itself, semantics would drift between endpoints and the Python
server would duplicate warehouse logic.

## Decision

All metrics are computed in the warehouse as canonical serving views
(`v_post_metrics`, `v_creator_metrics`, `v_profile_metrics`, `v_overview`,
`v_standout_calendar`, and the creator/topic/baseline views added later). The
dashboard (`dashboard/server.py`) is a **thin projector**: it only does view
SELECT + WHERE/ORDER/LIMIT + row→JSON — no `AVG`/`SUM`/`GROUP BY`/`ROW_NUMBER`/
`PERCENT_RANK` in the server. Enforced by
`tests/unit/dashboard/test_no_aggregation_in_server.py`.

## Alternatives considered

- Computing aggregates in the API layer: rejected — semantics drift, duplicated
  logic, harder to keep point-in-time-correct (see ADR-0006).

## Consequences

Positive: single source of metric truth; server stays simple and testable; new
metrics are added as views, consumed unchanged. Negative: server endpoints are
tied to view shapes (a view change is a server change); the server cannot do
novel ad-hoc aggregation without a new view.

## Supersedes / Superseded by

Superseded by: none. Related: ADR-0003 (no external calls — this keeps the server
also free of non-SELECT logic), ADR-0006.
