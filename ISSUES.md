# Issues & deferred work

Issue tracking is local — this file, not GitHub Issues.

## Active


### 1. Comprehensive medallion testing strategy

**Context:** Current test suite is minimal (1 parity test). Industry standard
(DataKitchen, 2025) calls for systematic coverage across all medallion layers.

**Scope of work:**

| Layer | Tests needed |
|-------|-------------|
| Bronze | Schema validation (columns, types, nulls), row count > 0, `.meta` sidecar integrity, run_id non-null |
| Silver | Dedup correctness (no duplicate post_ids), latest-dataset-wins ordering, column coercion (types match schema), row count ≤ bronze, `silver_progress` watermark integrity |
| Gold | Enrichment idempotency (re-run = same rows, no double-insert), SCHEMA_VERSION check, admiralty code validity (A1–F6), JSON parseability of educational_json/actionable_json, resumability (analysed posts skipped, failed retried) |
| Serving | SCD2 integrity (`effective_from` ≤ `effective_to`, no overlapping intervals per entity, no gaps), dim_time date spine continuity, view query correctness vs raw tables |

**Test categories to implement:**
- **Unit tests:** per transformation function, in-memory DuckDB, mocked APIs
- **Data quality tests:** null checks, uniqueness constraints, referential integrity
- **SCD2 tests:** version chaining, effective date ranges, dual-upstream reconciliation (details wins over post-derived)
- **Dedup tests:** INSERT OR REPLACE behavior, multi-dataset ordering, no silent data loss
- **Contract tests:** asset I/O shapes match upstream expectations
- **Integration tests:** bronze→silver→gold→views end-to-end with real (small) data

**Reference:** [DataKitchen — Data Quality Test Coverage in Medallion Architecture](https://datakitchen.io/blog/data-quality-test-coverage-in-a-medallion-data-architecture/)
- Rule of thumb: ≥2 tests per table, ≥2 tests per column, ≥1 test per business metric
- Shift-left: catch issues at bronze/silver, not gold/serving
- Shift-down: end-to-end integration tests across layer boundaries

### 2. S3 / R2 storage backend for GitHub Actions

**Context:** Pipeline currently writes Parquet to local filesystem (`data/lake/`).
GitHub Actions runners have ephemeral disks — data is lost between runs.
Need cloud object storage so CI can run the full medallion pipeline.

**Approach:** DuckDB's `httpfs` extension natively supports S3-compatible object
storage (AWS S3, Cloudflare R2, MinIO). Parquet files are read/written directly
from S3 URLs — no intermediate download step.

**Design (Dagster + DuckDB + S3 pattern, per dagster.io blog):**
- `DuckPondIOManager`: stores assets as Parquet on S3, DuckDB queries via `s3://` URLs
- DuckDB `httpfs` extension loaded with S3 credentials at connection time
- LocalStack for local S3 emulation during `dg dev`
- Env vars: `S3_ENDPOINT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_BUCKET`
- R2 recommended: zero egress fees, S3-compatible API

**Caveat — DuckDB state database:** The state DB (`data/state.duckdb`) is a
DuckDB database file that requires block-level filesystem access. S3 is an
object store, not a filesystem — can't open a `.duckdb` file over `s3://`.
Options:
1. Keep state DB local, sync to S3 on pipeline completion as backup
2. Use DuckDB's S3-attached mode (attach Parquet files, keep catalog in-memory)
3. Rebuild state DB from Parquet lake on each run (idempotent by design)
Recommended: option 3 for CI — state DB is derived from Parquet lake + SCD2
reconstruction. Option 1 for production deployments where state persistence
matters across runs.

**Tasks:**
- [ ] Choose S3-compatible provider (R2 likely — free tier, zero egress)
- [ ] Implement S3 I/O manager wrapping DuckDB + Parquet
- [ ] Decide state DB strategy (rebuild from lake for CI, local file for prod)
- [ ] Add S3 env vars to `.env.example`
- [ ] Add S3 secrets to GitHub Actions
- [ ] LocalStack config for local dev parity
- [ ] Update `lake.py` paths to support `s3://` prefix

**Reference:** [Dagster — Build a Data Lake with DuckDB](https://dagster.io/blog/duckdb-data-lake)

## Deferred

### Phase 1: Wire IG library imports
See `tasks/todo.md` for full plan. The old `ig_pipeline` functions need thin wrappers so assets can call them.

### Phase 2–7: Asset implementation
Full 8-asset pipeline (bronze → silver → gold → serving) plus sensor, schedule, tests, cutover.

## Won't fix

- GitHub Issues — local tracking only
