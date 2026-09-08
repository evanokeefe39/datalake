# P3 — Growth Analyst

- **Canonical actor string:** growth analyst
- **Consolidates:** analyst, creator-growth analyst, content analyst
- **Status:** active (primary human consumer)

## Who
The primary *human* consumer of the platform's signal. Studies what drives
standout/hot vs underperforming posts and creators across media types and
niches; wants hooks, formats, CTAs, and audience mechanics — and wants to know
which content choices actually separate winners from losers.

## Primary intent / goals
- Slice standout/hot vs underperformer per media type / niche (engagement
  utility, no human gold).
- Read cross-modal facets (V3) + visual summaries; ask "what do winning posts
  show/say".
- Reproduce analysis from committed scripts over stable views; never over-read
  thin/sparse cells.

## Agent-mediated mode
An AI agent runs the DuckDB/SQL queries and EDA scripts on the analyst's
behalf, then writes the analysis. It needs: stable, deterministic serving
views; documented re-runnable scripts (repeatability contract); honest
coverage/thin-cell flags so it doesn't over-claim from sparse data; grounded,
verifiable numbers in outputs. The persona's intent is insight; the agent is the
hands that must report uncertainty honestly.

## Related
`enrich-facets`, `enrich-summaries`, `serving-analytics`, `analysis/`, the
creator-growth research.
