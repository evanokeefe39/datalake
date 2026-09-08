# P4 — Dashboard User

- **Canonical actor string:** dashboard user
- **Consolidates:** dashboard user
- **Status:** active (product consumer)

## Who
Browses the product UI to understand the creator roster and performance at a
glance: overview, posts, signals, creators. Values navigation and click-through
over raw analysis.

## Primary intent / goals
- See creators, posts, standout/hot signals, and rising momentum; reach a post
  or creator's page in one click from anywhere a handle appears.
- Read honest, label-backed standout/outlier views (no future-leak math).

## Agent-mediated mode
Mostly human-browser, so lighter than the internal personas. An agent is useful
for natural-language questions over the same data ("which reels overperformed
last month?") → needs an NL→SQL path over the canonical serving views, not ad
hoc aggregation. The platform should keep the server a thin projector so a
question-answering agent queries the same views a human sees.

## Related
`dashboard`, `serving-analytics`, `identity`.
