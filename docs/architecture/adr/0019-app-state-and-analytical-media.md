# ADR-0019: Separate application state from analytical media, and move the app database to Postgres

- Status: Proposed
- Decided: 2026-09-18
- Supersedes: the SQLite half of ADR-0017 (the shared `ops.sqlite` boundary). ADR-0017's
  roster-over-HTTP decision stands unchanged.

## Context

ADR-0017 moved the roster boundary to HTTP and named a violation it did not fix: the
dashboard owns `creators`/`profiles`/`creator_merges`, orchestration read the same tables
from the same file, and both wrote one database. That decision fixed the roster's
read path. It left `ops.sqlite` itself in place, and the remaining coupling has now
produced a second, harder failure.

Three observations, all measured on live state 2026-09-18:

**One table serves two unrelated consumers.** `media_cache` holds 30,436 rows that split
cleanly by key space:

| Key shape | Rows | Written by | Read by | Content |
|---|---|---|---|---|
| `sha256(url)` (64-hex) | 29,343 | Dagster pipeline (`engine/media.py`) | the enrichment pipeline | post media for analysis — 55.6 GB, images **and** video |
| `thumb:<shortcode>` | 1,093 | dashboard (`server.py:119`) | the web app | post thumbnails — 24.6 MB, `image/jpeg` only |

**Neither consumer reads the other's rows.** Verified: the pipeline references no
`thumb:`/`avatar:` key anywhere, and the dashboard calls no `local_media_path` /
`stored_url_hash` accessor. The split is mechanical — zero keys are ambiguous.

**The two classes have opposite characteristics.** The pipeline's 29,343 rows are 55.6 GB
of analytical media whose only purpose is to be fed to a model. The dashboard's 1,093 rows
are 24.6 MB of application assets served to a browser. They differ by ~2,250x in size, by
consumer, by lifecycle, and by whether they are *content to analyse* or *assets to render*.
They share a table because they share a cache abstraction, not because they share a purpose.

**A single-file database cannot be shared across the container boundary cleanly.**
`opsdb.schema.connect()` runs `PRAGMA journal_mode=WAL` unconditionally, and SQLite in WAL
mode requires a mmap-able `-shm` sidecar beside the file. Through Docker Desktop's Windows
bind mount that sidecar cannot be attached: a **clean throwaway container with nothing else
running** gets `disk I/O error` on `data/ops.sqlite`, while (a) a *fresh* WAL database on
the same mount works, and (b) a **sidecar-free copy of the same file works** (30,436 rows
read). The file is fine; sharing it across the mount is not. Any host connection recreates
the sidecars, so cleanup cannot hold — this is not a hygiene bug, it is the wrong storage
shape for a file that two processes on two operating systems must both open.

The severity of that last point is not the seeding pass. **Every Dagster materialization
that writes `media_cache` from inside the container is currently broken**, and the failure
mode is `disk I/O error` raised from `PRAGMA`, which reads as a transient lock rather than
a structural mismatch.

## Decision

**Split by consumer, and give each half the storage shape its characteristics call for.**

### 1. Application state moves to Postgres, owned by the dashboard alone

`creators`, `profiles`, `creator_merges`, and the dashboard's thumbnails become tables in a
Postgres database that **only the dashboard connects to**. Every other service reaches them
through the dashboard's HTTP API — the existing `GET /api/roster`, plus whatever endpoints
the dashboard needs to own. There is no second direct writer, for any table.

Postgres is the correct shape here because the app half is genuinely relational, small,
concurrently written, and transaction-shaped. It also removes the bind-mount question
entirely: Postgres speaks TCP, so container and host connect to the same server rather than
opening the same file.

### 2. Analytical media bytes live in object storage, not in a database

The pipeline's post media is not database content. 55.6 GB of carousels and video destined
for model analysis belongs in object storage — the same R2/S3 surface the repo already uses
for post content — with the database holding **keys and metadata only**. No row in any
database should carry a byte count that can reach 55 GB.

This is the larger correction, and it is independent of the Postgres move: moving 55 GB of
image bytes from one database to another would preserve the mistake.

**Local development uses MinIO as the S3 stand-in, with the existing media directory
bind-mounted.** MinIO speaks the S3 API that R2 speaks, so code is written against one
interface and the only difference between local and production is an endpoint URL and
credentials. Critically, the existing `data/media/posts` tree is **mounted in place**, not
copied: the 55 GB stays where it already is, and the migration becomes a metadata
operation (record the object key) rather than a byte relocation. The copy-to-verify-and-flip
step in the consequences below therefore applies only to the eventual R2 upload, and can be
done lazily as objects are read.

**The upload boundary already exists and this does not move it.** `bronze_ig_posts`'s own
docstring states the shape: media bytes are cached locally at scrape time (ingestion), and
*"the enrichment worker later uploads from those local bytes"* — i.e. the object-storage
upload is the enrichment service's job, not this repo's, and the seam-purity scanner in
`ig_enriched/slv/checks.py` enforces that provider calls stay out of pure modules. So what
this ADR changes is where the **pipeline's own** media lives for analysis, not who uploads
it. Any implementation must keep the upload on the enrichment side of the seam.

*(Added 2026-09-18 after review.)*

### 3. Every media class is explicitly owned

The three media paths are currently distinguishable only by convention. They become
unambiguous:

| Class | Owner | Storage | Access |
|---|---|---|---|
| Post media for analysis | pipeline | object storage | pipeline reads by key |
| Post thumbnails (app) | dashboard | object storage or app-local disk | dashboard serves by API |
| Profile avatars | pipeline writes, dashboard serves | app-adjacent | `GET /api/media/avatar/{username}` |

`WATCHDOG.md`'s statement that "`media_cache` is the one shared table, and it is
pipeline-OWNED" is **stale and wrong** — it is pipeline-owned for one key class and
dashboard-owned for another. `opsdb/media_cache.py`'s own docstring already states the
truth (it names both writers); WATCHDOG is the artifact to correct.

### 4. The roster keeps crossing over HTTP

ADR-0017's decision stands: the pipeline reads the roster from the dashboard API, never by
opening the dashboard's database. This ADR extends that rule from the roster to **all** app
state.

## Consequences

### What this fixes

- The container can write. No shared file, no sidecar ownership, no `disk I/O error`.
- The roster boundary stops being a convention that one shared `INSERT` quietly breaches.
- Concurrency, transactions, and real constraint enforcement for app state.
- The pipeline's analytical media stops being coupled to the app's database availability.

### What it costs

- **A storage migration**, with a backfill for ~30k rows of metadata and a byte relocation
  for 55.6 GB of media. The relocation is the bulk of the work and is mechanical
  (copy, verify by checksum, flip the key reference, retire the old path).
- **`opsdb` is reimplemented.** The package is sqlite3-specific today: `PRAGMA` calls,
  `sqlite_ddl()`, `INSERT OR REPLACE` → `ON CONFLICT`, `?` → `%s` parameters, and the
  `ConnectionFactory` protocol. It becomes a Postgres client, and its DDL factory moves to
  the Postgres dialect.
- **Every writer and reader migrates**: the dashboard server (~20 endpoints) and the
  pipeline's media paths.
- **Compose gains a Postgres service** with credentials, a health check, and a dev
  workflow. CI needs a Postgres.
- **A meaningful share of the test suite** exercises the SQLite contract and must be
  retargeted.

### What must not change

- **Post media for analysis is never routed through the app API.** The dashboard does not
  become the pipeline's storage. The two halves stay independent; only app state is
  API-mediated.
- **Bronze stays a verbatim replay.** This ADR moves where bytes and app rows live, not how
  silver is derived. ADR-0018's determinism boundary is untouched: no new
  auto-materialization path from silver to `submit`.
- **Raw history is not rewritten.** Relocating media writes new keys and retires old paths;
  it does not mutate landed bronze.

### Sequencing

This is an epic, not a fix. It does **not** gate the current media-recovery work, and —
correcting an earlier claim made while scoping this — **the recovery backlog is not on a
clock**: the paid path re-fetches by permalink and Apify returns a *fresh* `displayUrl`,
so stored-URL expiry does not apply to it. (The ~4.5-day signature window is why losses
*recur on new scrapes*; it does not expire the existing backlog.) Order on merit.

## Open questions

1. **Does the pipeline's media *metadata* need a database at all?** If media lives in object
   storage beside the lake, its key/checksum metadata could live in the lake too, leaving
   Postgres as purely application state. This is the cleaner end state; it is deferred
   rather than decided because it changes the enrichment read path.
2. **Object storage for media: the existing R2 bucket, or a new one?** Reusing the post-content
   bucket is simpler; a separate bucket gives independent lifecycle and access policy.
3. **Does the dashboard's thumbnail cache survive the split, or does it become a CDN
   concern?** At 24.6 MB it is small, but it is a cache with a fetch-on-miss lifecycle —
   which is a job object storage does natively.
4. **Migration cutover**: dual-write with a backfill, or a stop-the-world copy? The media
   relocation makes the former more attractive, but it needs a reconciliation pass.
