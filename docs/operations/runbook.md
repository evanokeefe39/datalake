# Operating Guide

## Prerequisites

- `.env` at the repo root with `APIFY_API_TOKEN` and `OPENROUTER_API_KEY`
  (`GEMINI_*` is retired — see "Retired: the Gemini path" below).
- `data/state.duckdb` and `data/ops.sqlite` exist (created on first run).
- `DAGSTER_HOME` set to `data/dagster_home` (set in `.env`).
- The inference service running for any enrichment work (see below).

## The shape of the system

```
uv workspace root
├── services/orchestration/   the Dagster code location
├── services/jobs/            the inference service (own repo checkout today)
├── services/dashboard/       FastAPI + vite
└── packages/opsdb/           ops.sqlite contract (dashboard writes, pipeline reads)
```

Layers: bronze (Parquet, verbatim) → silver (six `silver_*` tables, deterministic
from bronze, zero API calls) → gold (four marts). Orchestration state lives in the
Dagster instance; `ops.sqlite` holds only `media_cache`, `creators`, `profiles`,
`creator_merges`. Full rationale: `docs/architecture/adr/0015-repository-layout.md`
and `docs/architecture/pipelines/enrichment.md`.

## The command home

**The repo root is the command home** for `dg`, `dagster`, `pytest` and `ruff`.
`[tool.dagster] module_name = "orchestration.definitions"` and
`DAGSTER_HOME` both resolve from there, so run everything from the root.

```bash
uv sync --dev                     # resolve + install every workspace member
uv run dagster dev                # UI at http://localhost:3000
uv run dagster definitions validate   # load the graph without the UI
```

There is no custom pipeline CLI. It was removed because it called asset
functions outside a Dagster run — no run record, no event log, no asset checks —
which is exactly the orchestration state ADR-0012 makes authoritative. Use the
Dagster CLI, which records what it does.

### Materialize assets

```bash
# the medallion path end to end
uv run dagster asset materialize -m orchestration.definitions \
  --select silver_ig_posts+v_post_detail+dim_profile+dim_date

# one asset
uv run dagster asset materialize -m orchestration.definitions --select silver_ig_posts
```

`-m orchestration.definitions` is required: the code location is that module, not
a package root.

## Sensors and schedules

**Everything ships stopped.** Nothing triggers automatically until you start it in
the UI (Automation → sensors/schedules). This is deliberate — enabling a sensor
that submits paid work is an operator decision, not a deployment side effect.

| Driver | Interval | What it does |
|---|---|---|
| `enrichment_harvest_sensor` | 30 s | Re-derives the in-flight set; requests a harvest run for terminal partitions |
| `enrichment_submit_sensor` | 300 s | Discovers eligible posts across every workload; requests a submit run only when the pending set is non-empty |
| `daily_medallion` | schedule | Scrape → silver → labels → serving |
| `core_refresh` | schedule | Serving refresh |

The submit sensor gates *paid* work, so it polls slower than harvest: the harvest
leg is what needs to be prompt, the submit leg is what must not be wasteful.

Both sensors open a short read per tick. If a concurrent writer holds the DuckDB
file the tick raises — loudly, never silently. That is the intended behaviour; do
not add a swallow-and-retry.

## The enrichment seam (operational contract)

Enrichment submits work to the inference service over HTTP and polls it back.
Three verbs: `submit` → `poll-to-terminal` → `retrieve`. **There is no local
ledger** — the service owns its job store and Dagster polls it (ADR-0013).

Start the service before any enrichment run:

```bash
docker compose up        # or `uv run` in the service checkout
curl -s http://127.0.0.1:8462/health
```

Default base URL: `http://127.0.0.1:8462` (override with `JOBS_SERVICE_URL`).
The client is `orchestration.defs.integration.batch_client`.

**The submit path pings `GET /health` first and fails LOUDLY if the service is
down** — never a quiet "nothing to do". If you see a loud health failure, the
service is the problem, not the pipeline.

The service is deliberately domain-agnostic: it does not know what a `post_id`
is; the `post_id ↔ custom_key` mapping lives on our side.

## Inspecting state

There is no `batches` subcommand — the queue is retired. Read state directly.

### Watermarks

```bash
uv run python -c "
import duckdb
print(duckdb.connect('data/state.duckdb', read_only=True)
        .execute('SELECT name, timestamp FROM watermarks ORDER BY name').df())
"
```

Watermarks are reset by an explicit `UPDATE`, and **`state.duckdb` is
single-writer** — stop `dagster dev` and the dashboard before writing, or the
write fails or blocks:

```bash
# stop dagster dev first
uv run python -c "
import duckdb
con = duckdb.connect('data/state.duckdb')
con.execute(\"UPDATE watermarks SET timestamp = '2000-01-01' WHERE name = 'silver_ig'\")
"
```

### Enrichment progress

With the UI open, the asset graph shows landed vs conformed directly. For a
scripted view, the anti-join is the truth — rows landed in bronze with no
conformed silver row:

```bash
uv run python -c "
import duckdb
print(duckdb.connect('data/state.duckdb', read_only=True).execute('''
  SELECT COUNT(*) AS landed_without_row FROM (
    SELECT DISTINCT post_id, platform FROM bronze_enrichment_raw
    EXCEPT SELECT post_id, platform FROM silver_content_classification
  )''').df())
"
```

Asset checks run in the UI under each asset. The blocking one is
`check_no_silent_loss`; `check_quarantine_growth` and
`check_silver_snapshot_freshness` are the other two guards.

### Quarantine triage

A payload that fails validation lands LOUDLY in `silver_enrichment_quarantine`
with a machine-readable `reason_code` — never as a silently-null silver row:

```bash
uv run python -c "
import duckdb
print(duckdb.connect('data/state.duckdb', read_only=True)
        .execute('SELECT reason_code, COUNT(*) FROM v_quarantine_triage GROUP BY 1 ORDER BY 2 DESC').df())
"
```

| Reason code | Meaning | Fix |
|---|---|---|
| `provider_error` | The provider returned a failure | Transient — retry via the retry round |
| `parse_error` | Response was not valid JSON | Prompt or model regression — investigate |
| `missing_required_field` / `enum_violation` / `type_violation` | Schema drift | Bump `DERIVATION_VERSION` and replay from bronze |
| `length_violation` | Field exceeded its bound | Bump the bound in the prompt schema, then replay |
| `cross_field_violation` | e.g. carousel `n != len(image_summaries)` | Payload defect — inspect the row |
| `unsupported_workload` | A landed workload the conform layer does not map | Register it in the conform dispatcher |

**A quarantine row is never a silent loss.** If the anti-join count and the
quarantine count disagree, that is a defect — see ISSUES.

## Workloads

Everything enrichment does is a **workload**: a declared pass with its own
eligibility query, item builder, cost estimate, prompt identity and job options.
The registry is `orchestration.defs.ig_enriched.slv.workloads.WORKLOADS`, and
`engine/submit.py` iterates it without knowing what any of them mean.

| Workload | Silver table | Media | Job options |
|---|---|---|---|
| `content-classification` | `silver_content_classification` | yes | — |
| `growth-facets-visual` | `silver_visual_annotations` | yes | `mode=visual`, `max_tokens=4096` |
| `growth-facets-text` | `silver_text_annotations` | no | `mode=text`, `max_tokens=1024` |

Restrict a run to one workload (a typo raises; it never runs empty):

```
SubmitConfig(workload="growth-facets-visual")
```

Adding a workload means adding a `Workload` to the registry — its
`silver_table` is what makes it visible to `check_no_silent_loss`, and its
`prompt_hash`/`schema_version` are what the harvest stage stamps on every
bronze landing. The engine names no provider and no payload.

### Projecting cost without spending

`SubmitConfig(dry_run=True)` runs discovery, the guard and item building, and
projects tokens and dollars with the same arithmetic a real run uses — writing
nothing to the instance. A dry run therefore does not change what the next real
run sees.

## Retry rounds

Retry is a new partition key, not a queue row. A post that failed retryably gets
re-materialized under an incremented round. `MAX_ROUNDS` bounds this; a partition
at the ceiling makes submit **raise** ("the retry budget is exhausted; refusing to
submit again") rather than looping silently.

To force a retry of a specific post, materialize its submit partition with the
next round — the key is derived by `engine.partitions.partition_key`.

## Stale analysis re-processing

When the prompt or schema changes, existing rows carry a stale `prompt_hash`.
`check_prompt_currency` fails while any exist. `CURRENT_PROMPT_HASH` lives in
`orchestration.defs.ig_enriched.slv.prompts`.

```bash
uv run python -c "
import duckdb
from orchestration.defs.ig_enriched.slv.prompts import CURRENT_PROMPT_HASH
print(duckdb.connect('data/state.duckdb', read_only=True).execute(
  'SELECT COUNT(*) FROM silver_content_classification WHERE prompt_hash IS NULL OR prompt_hash != ?',
  [CURRENT_PROMPT_HASH]).fetchone())
"
```

Re-processing is a **replay**, not a re-bill: bronze holds the verbatim responses,
so a schema or mapping change republishes silver deterministically with zero API
calls. Run the submit sensor (or materialize `silver_enrichment`) after bumping
`DERIVATION_VERSION` in `engine/silver_rt.py`.

## Schema drift

The catalog is split by database and lives in two packages:

- DuckDB half: `orchestration.defs.platform.schemas`
  (`DUCKDB_TABLES`, `DUCKDB_VIEWS`, `duckdb_ddl`).
- SQLite half: `opsdb.schema` (`SQLITE_TABLES`, `sqlite_ddl`).

Readiness test:

```bash
uv run pytest tests/operational/test_state_compatibility.py -v
```

"Run the pipeline or migration" means a table or column is in the catalog but not
in the running database. A "stale table" message means the reverse:

```bash
uv run python migrations/migrate_schema_drift.py
```

## Data migrations

| Script | Purpose |
|---|---|
| `migrations/migrate_owner_username.py` | Backfill null `owner_username` from the bronze fallback. Idempotent, `--dry-run`. |
| `migrations/migrate_schema_drift.py` | Rename tables, move data between DBs, drop vestigial tables. |
| `migrations/migrate_to_v2.py` | One-shot: Phase 1-4 schema → v2 domain-scoped tables. |
| `migrations/migrate_drop_prompt_registry.py` | Archive then retire `prompt_registry` (provenance now rides on the rows). `--plan` / `--apply`. |

Executed one-shots live in `scripts/archive/` and are **not runnable** — they
target a schema that no longer exists and are kept for provenance only. See
`scripts/archive/README.md`.

## Backups

`ops.sqlite` and `state.duckdb` are single-writer, gitignored and local-only.
`scripts/backup_databases.py` pushes consistent snapshots off-machine (Cloudflare
R2 via the `r2-sessions` AWS profile) plus a dated local copy under
`data/backups/`:

```bash
uv run python scripts/backup_databases.py            # R2 + local
uv run python scripts/backup_databases.py --local-only
```

Consistency is per-engine: `ops.sqlite` via the SQLite online-backup API (safe
while open), `state.duckdb` after a DuckDB checkpoint so the restored file is
self-contained.

## Smoke slice (verification plane)

A deterministic dev slice — ~100 posts with media bytes and its own lake roots —
so an end-to-end run costs cents and touches no live state:

```bash
uv run python scripts/make_smoke_slice.py --posts 100 --creators 5 --out data/smoke --seed 42
```

Re-running with the same seed and unchanged live data reproduces the same
selection. Use it to prove a changed path actually runs before trusting it.

## Retired: the Gemini path

The Gemini batch backend is gone (ADR-0009, executed 2026-09-15): `gemini_batch.py`,
`DirectBatchAdapter`, `GeminiResource`, `GeminiTier`/`GeminiTierConfig`,
`google-genai` and the tier gate are deleted, and provider selection no longer
reads configuration. The jobs service is the only provider on the paid path.

`GEMINI_TIER` / `GEMINI_API_KEY` may still be present in `.env`; nothing reads
them. The service's own concurrency control replaces the old rate-limit table.

## Common troubleshooting

**Submit sensor does nothing while posts are pending.** Check the sensor is
started in the UI, then check the health gate — a down service raises loudly at
the sensor's first tick, which shows in the sensor's evaluation log.

**DuckDB lock errors.** DuckDB allows one writer at a time. Stop `dagster dev`
before running anything that writes to `state.duckdb`, or vice versa. Read-only
connections (`read_only=True`) are safe alongside a writer.

**Bronze files not picked up.** Silver uses the `silver_ig` watermark. If files
were written after the last silver run, materialize silver again. If the watermark
is ahead, reset it explicitly (see Watermarks) — `state.duckdb` is single-writer,
so stop the daemon first.

**Asset checks failing on a fresh environment.** `check_no_silent_loss` fails when
bronze has rows with no conformed counterpart — on a fresh environment, run
`silver_enrichment` before believing the failure.

## Dagster UI

```bash
dagster dev            # http://localhost:3000
```

Assets appear in the global asset graph; materialization events, asset-check
results and sensor ticks are all recorded. This is the run ledger — there is no
`ops.sqlite` queue to consult.
