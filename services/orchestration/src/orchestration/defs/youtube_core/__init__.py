"""youtube_core — future domain package (skeleton, pre-reorg).

Will hold the YouTube scrape → bronze → silver → labels assets, mirroring
ig_core/: scrape.py, posts.py, labels.py, checks.py. Created as a placeholder
under the target layout (services/orchestration) ahead of the reorg; nothing
here is registered with Dagster yet.

Rules for the future implementer:
- Client code (YouTube Data API / Apify actor client) belongs in
  defs/integration/, not here.
- The roster (which channels to scrape) is read via packages/opsdb — the
  profiles table is already keyed (platform, handle); add rows, not tables.
- This package must not import from engine/ — enrichment payloads live in
  youtube_enriched/, the machinery is shared.
"""
