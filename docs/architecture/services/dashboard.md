# Dashboard Service (`dashboard/server.py`)

> Serving surface of the lake. Index: [../README.md](../README.md) · System shape: [../design.md](../design.md)

The dashboard is a FastAPI HTTP server (`dashboard/server.py`, run on port 3002) that exposes the lakehouse's DuckDB serving views as JSON endpoints. It connects to `data/state.duckdb` **read-only** and shapes view rows into API responses — it owns no transformation logic and computes no metrics. It also fronts the curated ops registry in `data/ops.sqlite` (creators/profiles CRUD) and triggers details scrapes into bronze via background tasks.

## The architectural rule: thin projector

Metrics are computed in exactly one place — the canonical serving views in `src/datalake/defs/serving/assets.py`. The server is a **thin projector**: it may `SELECT` from views and add `WHERE`/`ORDER BY`/`LIMIT`, then shape rows to JSON. It must never contain `AVG`, `SUM`, `GROUP BY`, or window functions. This is ratified in [ADR-0005: thin-projector serving](../adr/0005-thin-projector-serving.md) and enforced by the grep guard `tests/unit/dashboard/test_no_aggregation_in_server.py`, which fails if aggregation expressions (or the deleted Python fan-out helpers) reappear in `server.py`. Point-in-time semantics for those metrics are specified in ADR-0006.

## What it reads (and must never read)

The dashboard reads **only** the canonical DuckDB serving views: `v_overview`, `v_signal`, `v_post_detail`, `v_post_metrics`, `v_standout_calendar`, `v_recent_hot_posts`, `v_creator_metrics`, `v_creator_profile`, `v_creator_quality`, `v_creator_topics`, `v_rising_creators`, and `v_profile_metrics`.

Verified negative: the server code contains **no reference** to the enrichment pipeline's tables — not `gold_analyses`, `gold_growth_facets`, or the ops.sqlite queue tables (`batch_jobs`, `batch_items`, `dead_letter`). The dashboard consumes the serving layer's projections, never the raw enrichment/queue state directly. If an endpoint needs new enrichment data, the change happens upstream in the views, not in the server.

## Media serving

The server has two media endpoints, and they are distinct from the scrape-time media-byte cache (see [ADR-0003](../adr/0003-no-api-in-transform-layer.md)):

- `/api/media/thumbnail/{shortcode}` — fetches post thumbnail bytes from Instagram's public media endpoint **on first request**, then serves from disk. This is a request-time byte cache for a read-optimized surface (CDN URLs expire in ~4–5 days); it is *not* the scrape-time `media_cache` that backs enrichment, which downloads bytes at ingestion into `data/media/posts/` (AGENTS.md explicitly separates the two).
- `/api/media/avatar/{username}` — serves avatars populated at pipeline time by `ig_profiles_slv`, falling back to a DiceBear identicon redirect.

Both are the one place the server talks to the outside world (Instagram CDN) at request time; it performs no enrichment or external API calls otherwise.

## Where to change it

- **New or changed metric** → `src/datalake/defs/serving/assets.py` (add/change a view), then project it in `server.py`. Never compute the metric in the server.
- **Endpoint/shape changes** → `dashboard/server.py`; unit tests live in `tests/unit/dashboard/`.
- **Registry (creators/profiles) behavior** → `src/datalake/defs/instagram/creators.py`.

Note: a view-shape change is a server change — endpoints are tied to view shapes by design (ADR-0005, consequences). The dashboard's `/api/overview` and weekly-summary aggregations already moved into `v_overview` and `v_standout_calendar`; do not let them drift back into Python.
