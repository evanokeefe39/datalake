# scripts/archive — executed one-shots, kept for provenance

**Nothing in this directory is runnable.** These scripts were executed once,
against a schema and module layout that no longer exists, and are kept only so
the operations they performed are auditable.

They are not maintained, not importable, and not part of any pipeline. If you
need the behavior of one of them, read it as a record of what was done and write
a current script — do not "fix" these.

| script | what it did | when |
|---|---|---|
| `retire_queue_tables.py` | W9 retirement: reconciled service handles, archived each queue table (export count == live count in the same run), then dropped them per-table with a KEEP-set assertion. | 2026-09-15 |
| `reconcile_facets_jobs.py` | Reconciled `facets_batch_jobs` against the inference service's job store. Superseded by the reconciliation step inside `retire_queue_tables.py`. | 2026-09-15 |
| `poll_qwen_run.py` | Read-only progress poller for a live inference-service job (state / done / failed / rate / ETA). Kept until the facets pass became observable in the Dagster UI. | 2026-09 to 2026-09-15 |
| `discover_accounts.py` | One-shot account discovery for the roster. | pre-2026-09 |
| `re_scrape_profiles.py` | One-shot profile re-scrape. | pre-2026-09 |
| `snapshot_roster.py` | One-shot roster snapshot before a migration. | pre-2026-09 |
| `experiments/` | Scratch work against retired modules. Unmodified. | pre-2026-09 |

The archived scripts still reference the old service name and the old
`~/repos/qwen-batch-service` checkout path; that is historical and correct for
what they are. Current names are `services/jobs` and
`orchestration.defs.integration.batch_client`.
