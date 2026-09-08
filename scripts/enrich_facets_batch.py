"""Batch-native growth-facets driver — supersedes the interactive
``enrich_facets_full.py`` submit loop (batch-native migration plan).

Runs the facets → ``gold_growth_facets`` path end-to-end on the Gemini BATCH
API (~50% of interactive list price) via ``defs.enrichment.facets_batch``:

    enumerate targets → build requests (media pre-resolution included)
      → gemini_batch.submit (tier gate + in-flight token caps)
      → poll to terminal state (bounded) → harvest → validate → MERGE upsert

Resume-safe: submitted Gemini job names are persisted in the
``facets_batch_jobs`` ledger on ops.sqlite; a re-run polls + harvests any
still-SUBMITTED ledger jobs before discovering new targets, and target
enumeration skips posts already done under the current prompt hash (visual)
or already carrying the full text-layer sub-schema (text).

Subcommands:
    --plan       offline dry-run: target count + BATCH cost projection at the
                 50% discount. No spend, no writes beyond reading state.
    --run        build + submit + poll + harvest (bounded by --limit /
                 --posts; the FULL corpus is intentionally never implicit).
    --harvest    poll + harvest only (ledger jobs), no new submissions.

Examples:
    uv run python scripts/enrich_facets_batch.py --plan --mode visual
    uv run python scripts/enrich_facets_batch.py --run --mode visual --limit 2 \
        --state-db data/_gate.duckdb --posts <id1>,<id2>
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))
sys.path.insert(0, str(REPO_ROOT))

from dotenv import load_dotenv  # noqa: E402

load_dotenv()

from datalake.defs.common.resources import GeminiResource, SQLiteResource  # noqa: E402
from datalake.defs.enrichment import facets_batch  # noqa: E402
from datalake.defs.instagram.config import GeminiTierConfig  # noqa: E402

logger = logging.getLogger("enrich_facets_batch")

DEFAULT_STATE_DB = "data/state.duckdb"
DEFAULT_OPS_DB = "data/ops.sqlite"


from datalake.defs.enrichment.prompts import _DEFAULT_GEMINI_MODEL  # noqa: E402

def _csv(value: str | None) -> list[str] | None:
    return [v.strip() for v in value.split(",") if v.strip()] if value else None


def _open_state(state_db: str):
    """Open the state duckdb read-write (creates the file if absent)."""
    import duckdb
    Path(state_db).parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(state_db)


def _tier_gate() -> None:
    tier = GeminiTierConfig.detect()
    if not tier.supports_batch:
        raise RuntimeError(
            f"Gemini batch API requires Tier 1+ (active tier: {tier.tier.value}). "
            "Set GEMINI_TIER=tier1 with a paid key."
        )


def _modes(args: argparse.Namespace) -> list[str]:
    return [args.mode] if args.mode else ["visual", "text"]


def run_plan(args: argparse.Namespace) -> int:
    """Offline projection: counts + BATCH-discounted cost. No spend."""
    conn = _open_state(args.state_db)
    try:
        for mode in _modes(args):
            targets = facets_batch.enumerate_targets(
                conn, mode, limit=args.limit, post_ids=_csv(args.posts)
            )
            requests = facets_batch.build_facets_batch_requests(
                SQLiteResource(database=args.ops_db),
                GeminiResource(),
                conn,
                targets,
                mode,
            )
            input_tokens, cost = facets_batch.estimate_facets_cost(requests)
            print(
                f"[{mode}] targets={len(targets)} submittable={len(requests)} "
                f"est_input_tokens={input_tokens} "
                f"batch_cost_usd~{cost:.4f} (50% discount applied)"
            )
    finally:
        conn.close()
    return 0


def run_harvest(args: argparse.Namespace) -> int:
    _tier_gate()
    args.model = args.model or _DEFAULT_GEMINI_MODEL
    ops = SQLiteResource(database=args.ops_db)
    gemini = GeminiResource()
    conn = _open_state(args.state_db)
    try:
        pending = facets_batch.pending_ledger_jobs(ops)
        if not pending:
            print("No pending facet batch jobs in the ledger.")
            return 0
        facets_batch.wait_for_facets_batches(
            gemini,
            [j["name"] for j in pending],
            poll_seconds=args.poll_seconds,
            timeout_seconds=args.timeout_seconds,
        )
        print("Harvest:", facets_batch.harvest_facets_batches(ops, gemini, conn))
    finally:
        conn.close()
    return 0


def run_batch(args: argparse.Namespace) -> int:
    _tier_gate()
    args.model = args.model or _DEFAULT_GEMINI_MODEL
    ops = SQLiteResource(database=args.ops_db)
    gemini = GeminiResource()
    conn = _open_state(args.state_db)
    try:
        # Resume: poll + harvest anything still in flight first.
        pending = facets_batch.pending_ledger_jobs(ops)
        if pending:
            logger.info("Resuming %d pending ledger job(s)", len(pending))
            facets_batch.wait_for_facets_batches(
                gemini,
                [j["name"] for j in pending],
                poll_seconds=args.poll_seconds,
                timeout_seconds=args.timeout_seconds,
            )
            print("Resumed:", facets_batch.harvest_facets_batches(ops, gemini, conn))

        for mode in _modes(args):
            targets = facets_batch.enumerate_targets(
                conn, mode, limit=args.limit, post_ids=_csv(args.posts)
            )
            if not targets:
                print(f"[{mode}] nothing to do — all targets done.")
                continue
            requests = facets_batch.build_facets_batch_requests(
                ops, gemini, conn, targets, mode, model=args.model
            )
            if not requests:
                print(f"[{mode}] no submittable requests (media resolution).")
                continue
            input_tokens, cost = facets_batch.estimate_facets_cost(requests)
            print(
                f"[{mode}] submitting {len(requests)} requests "
                f"(~{input_tokens} est. tokens, batch cost ~${cost:.4f})"
            )
            names = facets_batch.submit_facets_batch(
                ops, gemini, requests, mode, model=args.model
            )
            facets_batch.wait_for_facets_batches(
                gemini,
                names,
                poll_seconds=args.poll_seconds,
                timeout_seconds=args.timeout_seconds,
            )
            print(
                f"[{mode}] harvest:",
                facets_batch.harvest_facets_batches(
                    ops, gemini, conn, names=names, model=args.model
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
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main())
