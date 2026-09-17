# migrations

One-off and recurring data-migration scripts. Kept out of `scripts/` (which holds
live operational tooling) so the ops surface stays uncluttered. Importable as
`from migrations.<name> import ...`.

| Script | What it changes | Status |
|---|---|---|
| `migrate_schema_drift.py` | Rename drifted tables, move data between DBs, drop vestigial tables (e.g. `gold_ig_analyses` → `gold_analyses`, drop `silver_ig_progress`, stale media table). Idempotent. | RUN against live (when `tests/operational/test_state_compatibility.py` flags a stale table) |
| `migrate_to_v2.py` | One-shot migration from Phase 1–4 schema to v2 domain-scoped tables. | HISTORICAL — pre-ADR-0011 world; do not re-run against current state |
| `migrate_from_ig_pipeline.py` | Import bronze Parquet from the legacy `ig-pipeline` repo. | HISTORICAL (import already done); re-runnable only for a fresh legacy import |
| `migrate_owner_username.py` | Backfill null `owner_username` in silver from bronze `username` fallback. Idempotent, `--dry-run`. | RUN against live (historical nulls); monitored by the `ig_posts_slv_owner_not_null` DQ check |
| `migrate_creators_profiles.py` | Split `scrape_targets` → `creators` + `profiles` (1:1 backfill), create `media_metadata`, drop `scrape_targets`. Idempotent. | RUN against live (once; `scrape_targets` already dropped in live) — **mutates live ops state; explicit approval required before touching `data/ops.sqlite`** (WATCHDOG) |
| `migrate_curated_creator_merge.py` | Consolidate curated creators: retire duplicate auto-creators keyed by `owner_username`, ledgered in `creator_merges`, reversible with `--undo`. | RECURRING — every curated-creator merge must go through this script (WATCHDOG: creator identity is a human decision) |
| `migrate_media_entity.py` | Media entity extraction/enrichment migration over existing rows. | HISTORICAL |
| `migrate_backfill_labels.py` | Bootstrap `ig_post_labels` with `bootstrap=True` — every row forced to `day0_heuristic`/provisional. Point-in-time snapshot, never a day7 judgment. | HISTORICAL (bootstrap run); re-run only with a new `LABEL_VERSION` |
| `migrate_backfill_observations.py` | Backfill `silver_ig_post_observations` from existing silver posts. | HISTORICAL backfill |
| `migrate_backfill_owner_id.py` | Backfill `owner_id` for rows ingested before bronze carried the author's `ownerId`. | HISTORICAL backfill (idempotent by post_id) |
| `migrate_backfill_profile_observations.py` | Backfill profile observations; exports `apply_backfill`/`collect_observations` used by `tests/unit/instagram/test_profile_observations.py`. | RUN against live (idempotent) |
| `migrate_quarantine_o44.py` | Quarantine affected rows for the O44 issue. | HISTORICAL |
| `migrate_classification_to_silver.py` | Gold→bronze→silver classification backfill: lands verbatim responses in `bronze_enrichment_raw`, conforms, registers `silver_content_classification`. | HISTORICAL (already run — 9,576 rows). **WARNING (WATCHDOG): `classification.py::CLASSIFICATION_DDL` does NOT match the live table (missing provenance columns, different column order); replaying `register_silver` (positional `INSERT … SELECT *`) against a conform-built table would corrupt rows positionally. The live publisher is `scripts/conform_silver.py` — reconcile `classification.py` before any replay.** |

## Deleted

- `migrate_enrichment_queue.py` — **deleted 2026-09-15.** It used raw
  `CREATE TABLE IF NOT EXISTS batch_jobs` / `batch_items` DDL, which silently
  resurrects the queue tables retired by ADR-0012 (WATCHDOG-sanctioned deletion).
- `scripts/_diag_retrieve.py` — throwaway diagnostic, deleted 2026-09-15.

## Notes

- Never hand-write `CREATE TABLE` in a migration: add specs to
  ``orchestration.defs.platform.schemas` (DuckDB) + `opsdb.schema` (SQLite)` and use `duckdb_ddl()` / `sqlite_ddl()`.
- ADR-0012 retires `batch_jobs` / `batch_items` / `dead_letter` /
  `facets_batch_jobs`; no migration here may recreate them.
