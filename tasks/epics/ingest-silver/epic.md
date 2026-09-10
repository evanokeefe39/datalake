# Epic E-INGEST — Bronze→silver ingestion & state foundation

- **Theme:** Ingestion / state foundation
- **Owner:** dlc-worker
- **Status:** Done (foundation)
- **Depends on:** none
- **Feeds:** E-MEDIA, E-ENRICH-ENGINE, E-IDENTITY

## Outcome
Raw Apify bronze (Parquet) is captured and normalized into domain-scoped silver
with honest schema state, dedup, and watermarks — the upstream everything else
reads.

## Scope highlights (from source plans)
- Bronze Parquet lake; domain-based `defs/` layout.
- `silver_ig_posts`: dedup (DISTINCT ON), `processed_on` first-seen semantics
  (never re-stamped), media_count/media_files.
- Generic `watermarks(name, timestamp)` replacing per-pipeline progress tables.
- Schema catalog (`defs/common/schemas.py`) + drift detection / readiness tests.
- Enrichment responses (all external model workloads) land verbatim in
  `bronze_enrichment_raw` (Parquet, append-only, idempotent on
  `(post_id, platform, workload, prompt_hash, run_id)`); the
  validation/quality contract (parse-ability, required fields, enum
  conformance via `schema_version`, length bounds, cross-field checks,
  completeness; quarantine/dead-letter on failure — never a silent NULL or
  dropped row) lives in SILVER, as a deterministic pure function of bronze.
  Owned by E-ENRICH-ENGINE; recorded here as the bronze/silver foundation.
- State split: DuckDB analytical, SQLite operational.

## Cross-cutting
- DuckDB single-writer; additive-only DDL; schema catalog is canonical (any new
  table/column must be added there or readiness fails).

## Source of truth
`tasks/plans/phase-1-foundation.md`, `phase-2-silver-asset.md`,
`ig-local-ingestion.md`, `watermark-deadletter-refactor.md`,
`state-readiness-impl.md`, `state-readiness-tests.md`.

## DoD (epic level)
- [x] Silver silver assets write through Polars/watermark pattern; catalog matches
      running DBs (readiness green).
