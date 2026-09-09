"""Batch-native growth-facets driver — THE enrichment execution vehicle.

The synchronous interactive paths (``enrich_interactive.py``,
``enrich_facets_pilot.py``, ``enrich_facets_full.py``) were removed
2026-09-08; enrichment is BATCH-NATIVE ONLY. This driver runs the facets →
``gold_growth_facets`` path end-to-end on the standalone **qwen-batch
service** (``defs.enrichment.qwen_client``, default
``http://127.0.0.1:8462``, override with ``QWEN_SERVICE_URL`` or
``--service-url``) via ``defs.enrichment.facets_batch``:

    enumerate targets → build items (visual: media resolved to cached local
      paths; the service frame-samples videos with ffmpeg on its own host)
      → check_health (LOUD fail if the service is down) → submit ONE service
      job → poll to terminal state (bounded) → harvest → validate → MERGE upsert

Resume-safe: submitted service job ids are persisted in the
``facets_batch_jobs`` ledger on ops.sqlite; a re-run polls + harvests any
still-SUBMITTED ledger jobs before discovering new targets, and target
enumeration skips posts already done under the current prompt hash (visual)
or already carrying the full text-layer sub-schema (text).

Subcommands:
    --plan       offline dry-run: target count + qwen cost projection.
                 No spend, no writes beyond reading state.
    --run        health-check + build + submit + poll + harvest (bounded by
                 --limit / --posts; the FULL corpus is intentionally never
                 implicit).
    --harvest    health-check + poll + harvest only (ledger jobs), no new
                 submissions.

Examples:
    uv run python scripts/enrich_facets_batch.py --plan --mode visual
    uv run python scripts/enrich_facets_batch.py --run --mode visual --limit 2 \
        --state-db data/_gate.duckdb --posts <id1>,<id2>
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))
sys.path.insert(0, str(REPO_ROOT))

from dotenv import load_dotenv  # noqa: E402

load_dotenv()

from datalake.defs.common.resources import SQLiteResource  # noqa: E402
from datalake.defs.enrichment import facets_batch, qwen_client  # noqa: E402
from datalake.defs.enrichment.prompts import _DEFAULT_QWEN_MODEL  # noqa: E402

logger = logging.getLogger("enrich_facets_batch")

DEFAULT_STATE_DB = "data/state.duckdb"
DEFAULT_OPS_DB = "data/ops.sqlite"


def _csv(value: str | None) -> list[str] | None:
    return [v.strip() for v in value.split(",") if v.strip()] if value else None


def _service_url(args: argparse.Namespace) -> str | None:
    """Explicit --service-url, else the qwen_client default (env-aware)."""
    return args.service_url or None


def _open_state(state_db: str):
    """Open the state duckdb read-write (creates the file if absent)."""
    import duckdb

    return duckdb.connect(state_db)


def _modes(args: argparse.Namespace) -> list[str]:
    return [args.mode] if args.mode else ["visual", "text"]


def run_plan(args: argparse.Namespace) -> int:
    """Offline projection: counts + qwen list-price cost. No spend."""
    conn = _open_state(args.state_db)
    try:
        for mode in _modes(args):
            targets = facets_batch.enumerate_targets(
                conn, mode, limit=args.limit, post_ids=_csv(args.posts)
            )
            items = facets_batch.build_facets_batch_requests(
                SQLiteResource(database=args.ops_db),
                conn,
                targets,
                mode,
            )
            input_tokens, cost = facets_batch.estimate_facets_cost(items)
            print(
                f"[{mode}] targets={len(targets)} submittable={len(items)} "
                f"est_input_tokens={input_tokens} "
                f"qwen_cost_usd~{cost:.4f} (advisory list-price projection)"
            )
    finally:
        conn.close()
    return 0


def run_harvest(args: argparse.Namespace) -> int:
    args.model = args.model or _DEFAULT_QWEN_MODEL
    base_url = _service_url(args)
    # US-EENG-2: loud health check — never a quiet 'nothing to do'.
    qwen_client.check_health(base_url)
    ops = SQLiteResource(database=args.ops_db)
    conn = _open_state(args.state_db)
    try:
        pending = facets_batch.pending_ledger_jobs(ops)
        if not pending:
            print("No pending facet batch jobs in the ledger.")
            return 0
        facets_batch.wait_for_facets_batches(
            [j["job_id"] for j in pending],
            poll_seconds=args.poll_seconds,
            timeout_seconds=args.timeout_seconds,
            base_url=base_url,
        )
        print(
            "Harvest:",
            facets_batch.harvest_facets_batches(
                ops, conn, model=args.model, base_url=base_url
            ),
        )
    finally:
        conn.close()
    return 0


def run_batch(args: argparse.Namespace) -> int:
    args.model = args.model or _DEFAULT_QWEN_MODEL
    base_url = _service_url(args)
    # US-EENG-2: loud health check before ANY submit.
    qwen_client.check_health(base_url)
    ops = SQLiteResource(database=args.ops_db)
    conn = _open_state(args.state_db)
    try:
        # Resume: poll + harvest anything still in flight first.
        pending = facets_batch.pending_ledger_jobs(ops)
        if pending:
            logger.info("Resuming %d pending ledger job(s)", len(pending))
            facets_batch.wait_for_facets_batches(
                [j["job_id"] for j in pending],
                poll_seconds=args.poll_seconds,
                timeout_seconds=args.timeout_seconds,
                base_url=base_url,
            )
            print(
                "Resumed:",
                facets_batch.harvest_facets_batches(
                    ops, conn, model=args.model, base_url=base_url
                ),
            )

        for mode in _modes(args):
            targets = facets_batch.enumerate_targets(
                conn, mode, limit=args.limit, post_ids=_csv(args.posts)
            )
            if not targets:
                print(f"[{mode}] nothing to do — all targets done.")
                continue
            items = facets_batch.build_facets_batch_requests(
                ops, conn, targets, mode
            )
            if not items:
                print(f"[{mode}] no submittable items (media resolution).")
                continue
            input_tokens, cost = facets_batch.estimate_facets_cost(items)
            print(
                f"[{mode}] submitting {len(items)} items "
                f"(~{input_tokens} est. tokens, qwen cost ~${cost:.4f})"
            )
            job_id = facets_batch.submit_facets_batch(
                ops, items, mode, model=args.model, base_url=base_url
            )
            facets_batch.wait_for_facets_batches(
                [job_id],
                poll_seconds=args.poll_seconds,
                timeout_seconds=args.timeout_seconds,
                base_url=base_url,
            )
            print(
                f"[{mode}] harvest:",
                facets_batch.harvest_facets_batches(
                    ops, conn, job_ids=[job_id], model=args.model,
                    base_url=base_url,
                ),
            )
    finally:
        conn.close()
    return 0


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--plan", action="store_true", help="offline dry-run (no spend)")
    p.add_argument("--run", action="store_true", help="submit + poll + harvest")
    p.add_argument("--harvest", action="store_true", help="poll + harvest only")
    p.add_argument("--mode", choices=["visual", "text"], default=None)
    p.add_argument("--limit", type=int, default=None, help="max targets this run")
    p.add_argument("--posts", default=None, help="comma-separated post_id subset")
    p.add_argument("--state-db", default=DEFAULT_STATE_DB)
    p.add_argument("--ops-db", default=DEFAULT_OPS_DB)
    p.add_argument("--model", default=None, help="model override for this run")
    p.add_argument(
        "--service-url",
        default=os.environ.get("QWEN_SERVICE_URL")
        or qwen_client.DEFAULT_QWEN_SERVICE_URL,
        help="qwen-batch service base URL",
    )
    p.add_argument("--poll-seconds", type=int, default=60)
    p.add_argument("--timeout-seconds", type=int, default=24 * 3600)
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    args = _parse_args(argv)
    if args.plan:
        return run_plan(args)
    if args.harvest:
        return run_harvest(args)
    if args.run:
        return run_batch(args)
    print("Nothing to do — pass --plan, --run or --harvest.")
    return 2


if __name__ == "__main__":
    sys.exit(main())
