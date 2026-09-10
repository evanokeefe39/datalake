# Inference service and the provider seam

How the pipeline hands work to an outside model and takes results back.

Related: [ADR-0008](../adr/0008-hermetic-with-explicit-api-seam.md) (one explicit
API seam) · [ADR-0009](../adr/0009-qwen-batch-service.md) (the qwen batch service) ·
[ADR-0007](../adr/0007-batch-native-enrichment-deprecate-worker.md) (the
submit/harvest bridge and its amendments) · [enrichment.md](../pipelines/enrichment.md) (what
the results become) · [ADR-0012](../adr/0012-dagster-native-orchestration.md)
(where the orchestration state lives)

> **Status: the seam is NOT built.** This documents the target design. Today the
> repo runs two entirely independent lifecycles (see "Current state"). The seam
> and its adapters are proven in `~/repos/enrichment-spike` (`spike_defs/adapter.py`,
> spike S2) but are not wired into the pipeline.

## 1. The seam is a boundary, not a component

The seam is **not** a service, a class, or a process. Nothing runs "inside" it.
It is the **boundary** where our pipeline hands work to an outside system and
takes results back, and its entire contract is two things:

- **one shared job ledger** — `external_jobs`, a table both sides read and write
  (name settled by [ADR-0010](../adr/0010-enrichment-naming-and-provenance.md); its
  *location* is still open, see §6);
- **three verbs** — `submit`, `poll-to-terminal`, `retrieve`.

Everything else is implementation. In particular, **`harvest` is not a seam
verb**: it is our own step that *composes* `poll-to-terminal` + `retrieve` + an
idempotent verbatim landing. Keeping that distinction matters, because it is what
lets the same three verbs serve a natively-async provider and a synchronous one
alike.

What sits on each side:

| Side | Owns |
|---|---|
| **Dagster** (inside the boundary) | discovery, batching, the sub-second `submit`, the idempotent `harvest`; orchestration, lineage, quality. **It never calls a provider directly** — the ledger is the only thing that crosses. |
| **External service** (outside) | durability, retry, backoff, dead-letter, resume, media handling. |
| **Provider** (outside both) | the model call itself. |

## 2. One seam, two adapters, swap by config

The provider is swappable because everything downstream sees a **canonical
state vocabulary** — `pending` / `processing` / `completed` / `failed` — and
provider-neutral `Item` / `Result` types. No provider-native state string ever
escapes an adapter.

| Adapter | Wraps | Who owns job state |
|---|---|---|
| `ServiceBackedAdapter` | `qwen-batch-service` over HTTP | the service |
| `DirectBatchAdapter` | a provider with its own job model (Gemini-shaped) | the provider |
| `ChunkedDirectBatchAdapter` | a direct provider with a bounded per-request size | one logical unit → N provider requests, one handle |

`build_adapter(name)` is the **only** place a provider is named. Swapping
providers is that string and nothing else. Consumers branch on an adapter's
`Capabilities`, never on its name — otherwise the swap is nominal rather than real.

**Keep a second real adapter in production.** It is the regression fixture that
proves the abstraction is an abstraction; two providers on one lifecycle is the
test.

## 3. Why the service exists: sync made async

The load-bearing fact that shapes this whole design:

> **OpenRouter/qwen is synchronous.** It answers in-request and holds nothing
> server-side.

Gemini batch is natively async — the provider holds the job. qwen is not, so
somebody has to hold the queue, run the slow work, and remember how far it got.
That somebody is **our own service**.

The adapter absorbs exactly that difference, which is why one seam covers both.

### The service contract

`~/repos/qwen-batch-service` — FastAPI with its own SQLite job store (lease,
exponential backoff, dead-letter, resume).

| Endpoint | Purpose |
|---|---|
| `POST /jobs` | submit a batch of items — `{items: [{custom_key, prompt, images}], model, max_tokens}` → `{job_id, total}` |
| `GET /jobs` | list jobs |
| `GET /jobs/{id}` | job state |
| `GET /jobs/{id}/results` | retrieve item results once terminal |
| `GET /health` | liveness + the model in use |

Two contract rules:

- **Deliberately domain-agnostic.** The service does not know what a `post_id`
  is. The `post_id ↔ custom_key` mapping lives on our side, which is what keeps
  the service reusable and free of pipeline concepts.
- **Client-side media.** Video is frame-sampled with ffmpeg on our side (ADR-0009);
  there is no File-API upload and no GCS mirror on this path.

**Health gate is mandatory and must fail loudly.** The submit path pings
`GET /health` first and raises if the service is down — never a quiet "nothing to
do", which would look like an empty backlog rather than an outage (US-EENG-2).

## 4. Workloads

One seam, pluggable by workload: **`qwen-vision`** (images and video frames),
**`whisper`** (ASR, local), **`text-LLM`** (caption + transcript). The provider is
recorded in per-row metadata, never in a table name — so a provider swap never
renames a table.

### One submit per pass — harvest fans out

**Cost-critical.** A single model call returns multiple artifacts. The graph
therefore has **one submit per pass**, and the harvest fans that one job's
results out to every destination it produced:

```
submit(workload) → one job → harvest → <landing> → ┬→ <artifact A>
                                                   └→ <artifact B>
```

**Never one submit per artifact.** Submitting per destination doubles the bill
for a call that already returned both.

*Which* artifacts each pass produces, and the table shapes they land in, is
domain knowledge and lives with the layer model — see
[enrichment.md](../pipelines/enrichment.md) §8 for the concrete mapping. This document
deliberately does not name those tables.

## 5. Current state — what actually runs

| | Gemini path | qwen path |
|---|---|---|
| submit | `submit.py` | `facets_batch.submit_facets_batch()` |
| poll | `harvest.gemini_batch_harvest_sensor()` (**ships STOPPED**) | `facets_batch.wait_for_facets_batches()` |
| retrieve | `gemini_batch.retrieve()` | `qwen_client.get_results()` |
| ledger | `ops.sqlite` `batch_jobs` + `batch_items` | `ops.sqlite` `facets_batch_jobs` |
| landing | **none** | **none** |

They share **no** submit/poll/harvest code — two wholly independent
implementations of the same idea, which is exactly what ADR-0007's first
amendment flagged and asked to converge before a third workload forks it again.

Neither path lands the raw response: both parse in flight and discard, so a
schema change re-buys the corpus. Fixing that (the `bronze_enrichment_raw`
landing) is the economic point of the seam work, not a nicety.

**Migration:** `tasks/plans/inference-service-seam.md` (Phase 1 of
`tasks/plans/enrichment-v3-migration-master.md`).

## 6. Open questions

1. **Where does `external_jobs` live** — `ops.sqlite`, or the Dagster instance?
   ADR-0012 says orchestration state is Dagster-native, but the seam contract
   requires *both sides* to read the ledger. Only the location is open; the name
   is settled.
   **Phase 1 gate — resolve before the ledger is created.** Phase 1 is what
   introduces `external_jobs` and reconciles the `facets_batch_jobs` rows into
   it, so the location cannot be deferred to Phase 2 (which retires the queue but
   inherits the ledger). See `tasks/plans/enrichment-v3-migration-master.md`.
2. **Retain the Gemini File-API media path** for the alternative adapter, or make
   client-side frame-sampling universal? ADR-0009 chose sampling for qwen;
   keeping the File-API path costs maintenance for a non-default provider.
