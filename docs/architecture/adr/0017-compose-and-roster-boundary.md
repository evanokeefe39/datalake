# ADR-0017: Run orchestration and jobs under one Compose, and move the roster boundary

- Status: Accepted
- Decided: 2026-09-16

## Context

ADR-0015 fixed where things live but left two operational gaps that made the platform
unusable on the owner's machine, plus one ownership violation it named but did not fix.

*The lifecycle was authored but could not run.* The Dagster-native enrichment path
(submit → poll → harvest) existed as jobs, sensors, partitions and a publisher asset, and
all of it was internally consistent. What was missing was everything around it: the
inference service lived in a **separate checkout** started by hand on loopback, this repo
had no Docker artifact at all, every sensor and schedule shipped stopped, and there was no
single command that brought the platform up.

*The graph contained references to nothing.* `silver_enrichment_conform` was named by an
`@asset_check`, by a view's `deps=`, and in two messages, while the registered producer was
`silver_enrichment`. `ig_posts_local_raw` was fully implemented and tested but absent from
`all_assets`, so the local-ingest path was unreachable. `dagster definitions validate`
passed in both cases — it checks that a code location *loads*, never that the keys inside it
name anything.

*Container paths did not resolve.* Persisted media paths were written by whichever process
fetched the bytes — Windows-absolute, today. `opsdb.media_cache.cached_local_path` then
tested the stored path with `os.path.exists` **before** any translation, so inside a Linux
container every media row read as a miss and the media cache looked empty. Separately,
`platform.paths` called `_repo_root()` eagerly at import and raised when no ancestor carried
`.git` — which is every container built from a `.dockerignore`d context — and the io
manager's `lake_root` was a cwd-relative `"data/lake"` literal that survived every `IG_*`
override.

*Two services shared one database.* The dashboard owns `creators`/`profiles` and writes
them into `ops.sqlite`; orchestration read the same tables from the same file. ADR-0015's
convention 7 says the only cross-service dependency path is a package — but a shared mutable
database is not that, and it left the roster with two owners.

## Decision

**One Compose file brings up all three services, and the roster crosses the boundary over
HTTP.**

### The platform runs as containers from one root Compose

`services/jobs` is the inference service moved in as a uv workspace member (package
`jobs`); `services/dashboard` and `services/orchestration` are members too. Both images
build from the **repo-root** context, because a uv workspace cannot be consumed by a
standalone install of one directory. A root `.dockerignore` is what keeps the ~55 GB
`data/` tree out of the build context.

`docker-compose.yml` runs `jobs`, `orchestration` and `dashboard`. Or orchestration uses
`dagster dev`, which starts the webserver *and* the daemon in one process — a
webserver-only deployment would expose the UI while nothing scheduled ever fires.

### Paths are translated, not assumed

`platform.paths` gains `runtime_path(stored)`: a host↔container prefix map configured by
`IG_HOST_PATH_PREFIX` / `IG_CONTAINER_PATH_PREFIX`. Empty host prefix means identity, so a
host run is unchanged. The media lookup splits in two:

- `opsdb.media_cache.stored_local_path` — the persisted path, **no** filesystem check;
- `engine.media.local_media_path` — translates through the map, *then* tests existence.

Both containers mount the same host `data/` tree at `/data`, so one vocabulary works at the
wire and no second translation belongs in the adapter.

`platform.paths` also resolves `IG_DATA_DIR` **before** the `.git` marker walk, which is
what lets the package import in a container while still raising loudly on a host that
configured nothing. The io-manager root follows `DATA_DIR` rather than a cwd-relative
literal.

### The roster is owned by the dashboard and served over HTTP

The dashboard owns `creators`/`profiles`/`creator_merges` and serves
`GET /api/roster`. The pipeline lands that response as append-only bronze
(`ig_roster_raw`, one snapshot per `fetched_at`) and publishes it to `silver_ig_roster`,
which its assets read. The pipeline never opens `ops.sqlite` for the roster; the dashboard
never imports the pipeline. `media_cache` stays pipeline-owned (it is written on the scrape
hot path).

The dashboard's scrape trigger is removed with this. A roster row **is** the scrape intent:
the pipeline reconciles `results_type='details'` against `updated_at` past a watermark and
scrapes what is new, so adding a profile is a registration rather than a paid side effect of
an HTTP request, and a failed scrape is retried by the sweep instead of dying with the
request.

### Readiness and default status are explicit

`/health` returns 503 when the store is unreadable or the worker thread is dead — every
datalake gate site already treats non-200 as "unusable", so a hardcoded 200 reported a dead
service as ready. `enrichment_harvest_sensor` ships **RUNNING** (it only polls provider
state and spends nothing); submit stays manual because it spends money; schedules stay
STOPPED.

### The asset graph is checked, not assumed

`tests/operational/test_asset_graph_integrity.py` asserts that every asset check targets a
registered asset, that every dependency resolves, and that the two lifecycle partition
spaces exist. Keys that legitimately name something other than an asset (the six `silver_*`
tables published by one asset, `bronze_enrichment_raw`) are declared explicitly, so an
undeclared dangling key still fails. The test was proven to fail on the injected dead key.

## Alternatives considered

*Keep the jobs service in its own checkout and run it natively.* This is the pre-Compose
status quo: two processes, hand-started, on loopback. Rejected because it is exactly the
state the owner could not operate — and it needs no path map only because a host run has
one filesystem.

*A `parents[N]`-free but absolute `/data` hardcode.* Rejected: it would work in the
container and break the host run, which is the failure mode the prefix map exists to avoid.

*A reverse map for container-written paths.* Needed only if the pipeline alternates between
host and container runs; the decision is to make Compose the runtime of record. Recorded as
a known caveat rather than solved speculatively.

*Have orchestration read `ops.sqlite` for the roster but behind an `opsdb` function.* Would
have satisfied "one implementation" while leaving two writers on one file — the ownership
violation, not the code duplication, is the defect.

*Split `media_cache` into its own database.* Raised while designing this. Rejected for now:
`opsdb` already carries the `media_cache` contract, and the dashboard writing to it is a
pipeline-owned table with one shared INSERT — the same shape as the roster table before this
change, but without a second *owner* to separate.

## Consequences

Positive: `docker compose up` is the whole platform; the enrichment lifecycle advances
without a hand-started process; media genuinely reaches the model through the container; the
roster has one owner and one direction of dependency; and three classes of defect that
`definitions validate` cannot see are now caught by a test.

Negative: there is now a soft runtime dependency from the pipeline to the dashboard — if the
dashboard is down, `ig_roster_raw` fails loudly and the pipeline continues on the last landed
snapshot (stale, never broken). The pipeline's roster changes take effect at the next
publish rather than immediately.

It also commits future work: `services/jobs` holds exactly one replica (its store uses an
in-process lock, not a database-level one), so scaling it is a store change, not a Compose
change.

Neutral: `data/jobs/state.sqlite` carries the job store across restarts and is why
`docker compose down -v` must never be used once real jobs exist.

## Supersedes / Superseded by

Extends ADR-0015 (convention 7 becomes an HTTP boundary rather than a shared file) and
ADR-0012 (orchestration state stays Dagster-native; the roster join moves out of the
pipeline's direct database access). ADR-0011's layered model is unchanged — the roster is an
ingestion source, not an enrichment table, so it adds `ig_roster_raw` / `silver_ig_roster`
without touching the six enrichment silver tables or the four marts.
