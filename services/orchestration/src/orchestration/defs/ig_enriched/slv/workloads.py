"""IG enrichment payload seam — who is eligible, and what each item looks like.

This module is the one place a per-domain payload enters the engine: the
eligibility query (which posts should be enriched) and the item builder (what
the provider is asked to do with one post) live together, keyed by workload.

Branch `refactor/submit-discovery` rewrites this file into the `Workload`
registry that `engine/submit.py` iterates; here it holds the drain verbatim so
the move is behaviour-preserving.
"""

import json
import logging
import os

import polars as pl
from dagster import AssetExecutionContext, AssetKey, AssetMaterialization, Config, asset

from orchestration.defs.engine import landing
from orchestration.defs.engine.media import is_video_path, media_urls_to_local_paths
from orchestration.defs.engine.partitions import (
    SUBMITTED_ASSET_NAME,
    PartitionSnapshot,
    in_flight_partitions,
    partition_key,
    post_partition_state,
)
from orchestration.defs.ig_core.slv.labels import LABEL_VERSION
from orchestration.defs.ig_core.slv.posts import ensure_state_tables as _ensure_state_tables
from orchestration.defs.ig_enriched.slv.prompts import (
    _DEFAULT_QWEN_MODEL,
    CURRENT_PROMPT_HASH,
    build_growth_facets_prompt,
    build_text_facets_prompt,
)
from orchestration.defs.ig_enriched.slv.visual import UNIVERSAL_MAX_OUTPUT_TOKENS
from orchestration.defs.platform.resources import DuckDBResource, SQLiteResource

logger = logging.getLogger(__name__)


QWEN_INPUT_PRICE_PER_M = float(os.environ.get("QWEN_INPUT_PRICE_PER_M", "0.03"))
QWEN_OUTPUT_PRICE_PER_M = float(os.environ.get("QWEN_OUTPUT_PRICE_PER_M", "0.13"))

# Estimated output tokens per response — cost projection only (not billing).
_EST_OUTPUT_TOKENS_VISUAL = UNIVERSAL_MAX_OUTPUT_TOKENS
_EST_OUTPUT_TOKENS_TEXT = 1024

# Advisory per-media input-token estimates for the qwen service: images are
# vision tokens; videos are frame-sampled server-side into 8 frames.
_TOKENS_PER_IMAGE = 258
_VIDEO_FRAMES = 8


class GoldConfig(Config):
    """Configuration for ``ig_posts_gen_batches`` (gold batch creation).

    ``post_ids`` (optional) restricts enrichment to specific posts.
    Default (empty) = all pending posts.
    ``whole_corpus`` opts into corpus-wide admission: ALL silver posts with
    non-empty captions (including label-pass ``skip`` posts) are enqueued for
    a text-only enrichment pass (ADR-0001). Default OFF — the standard path
    Batches are created in ``gemini-batch`` mode by default (regardless of
    ``whole_corpus``) whenever the active Gemini tier supports the BATCH API;
    they fall back to ``interactive`` on the free tier.
    ``prefer_interactive`` explicitly opts out of the batch API so jobs are
    created in ``interactive`` mode even on a batch-capable tier.
    """

    post_ids: list[str] = []
    whole_corpus: bool = False
    prefer_interactive: bool = False


# ── Growth-facets payloads (moved from the retired facets_batch) ────────────
#
# The eligibility query, the per-post item builder and the cost estimator are
# payload concerns, so they live with the other workload payloads rather than
# with the code that drives the seam. `refactor/facets-native` registers them
# as workloads; until then they are imported by the transient engine driver.


from orchestration.defs.ig_enriched.slv.schemas import (  # noqa: E402
    GROWTH_FACETS_SCHEMA_VERSION,
)

DRAIN_WORKLOAD: str = landing.WORKLOAD_CONTENT_CLASSIFICATION
"""The discovery drain's partition workload — classification enrichment."""

_drain_instance: "PartitionSnapshot | None" = None


def drain_in_flight_keys(instance: "PartitionSnapshot") -> frozenset[str]:
    """THE drain's definition of "in flight" (US-EENG-4 AC2/AC5).

    ``partitions.in_flight_partitions(instance)`` — the Dagster instance's
    ``materialized(enrichment_submitted) − materialized(enrichment_harvested)``
    — verbatim. The drain MUST NOT derive in-flight state any other way: the
    accounting identity and the guard below share this one function, so they
    cannot disagree. A divergence here is a silent double-submit.
    """
    return frozenset(in_flight_partitions(instance))


def drain_suppressed_post_ids(
    candidates: list[str], instance: "PartitionSnapshot"
) -> list[str]:
    """Posts whose in-flight partition suppresses them from the candidate set.

    Partition contract (ADR-0014 D1): the partition covering post ``p`` is a
    readable per-post composite whose round derives from the materialized
    keys themselves — ``partitions.post_partition_state`` tests EVERY
    materialized round for the post, so a retry round in flight suppresses
    exactly as a first attempt does, and a harvested round never suppresses
    (completion genuinely releases work). Per-post granularity keeps
    suppression order-independent and stable when other posts complete
    between runs.
    """
    return [
        pid
        for pid in candidates
        if post_partition_state(instance, DRAIN_WORKLOAD, pid).suppressed
    ]


def _materialize_submitted_partitions(
    instance: PartitionSnapshot,
    rounds_by_post: dict[str, int],
) -> None:
    """THE drain enqueue (ADR-0012/0013/0014): one ``enrichment_submitted``
    partition PER POST at that post's CURRENT round, materialized runlessly
    on the SAME instance the in-flight guard reads — never an instance this
    function opens itself.

    The key grammar is the ADR-0014 D1 composite
    (``<workload>\\x00r<N>\\x00<post_id>``), identical to what the guard and
    the retry driver parse back out. A first attempt enqueues at round 0; a
    post whose previous round harvested with a failure re-enqueues at
    round N+1 (the round comes from ``post_partition_state`` — derived from
    the materialized keys, no ledger).

    Sequence mirrors the real-instance roundtrip pinned in
    ``tests/unit/enrichment/test_partitions.py``: register the dynamic
    partition, then report the runless materialization.
    """
    keys = [
        partition_key(DRAIN_WORKLOAD, rounds_by_post[pid], [pid])
        for pid in sorted(rounds_by_post)
    ]
    instance.add_dynamic_partitions(SUBMITTED_ASSET_NAME, keys)
    submitted_key = AssetKey(SUBMITTED_ASSET_NAME)
    for key in keys:
        instance.report_runless_asset_event(
            AssetMaterialization(asset_key=submitted_key, partition=key)
        )


@asset(
    name="ig_posts_gen_batches",
    group_name="instagram",
    description="Drain triage-approved labels into Gemini enrichment batches.",
    deps=["ig_post_labels"],
)
def ig_posts_gen_batches(
    context: AssetExecutionContext,
    config: GoldConfig,
    duckdb: DuckDBResource,
    ops: SQLiteResource,
) -> pl.DataFrame:
    """Dumb drain over ``ig_post_labels`` (US-L4): any post whose label pass
    approved it for enrichment that has no current-prompt conformed
    classification and nothing in flight on the Dagster instance is enqueued.
    The ``gold_ig`` watermark is retired — the labels table is the discovery
    source. Explicit ``post_ids`` re-enrichment bypasses all guards.

    Stale rows (prompt_hash != CURRENT_PROMPT_HASH, e.g. pre-multimodal
    text-only analyses) are re-enqueue-eligible (US-L5) — only a current
    prompt_hash blocks. Empty-caption posts never reach this asset: the
    label pass sets enrich_decision='skip' for them (US-L6).

    ``whole_corpus`` (GoldConfig) opts into corpus-wide admission: ALL silver
    posts with non-empty captions (including the label pass's ``skip``
    posts) are enqueued for a text-only pass (ADR-0001). Still excludes
    current-prompt conformed rows + in-flight work so a re-run never re-pays.

    Execution mode stays tier-driven — batches default to ``gemini-batch``
    (BATCH API, ~50% cheaper) whenever the active Gemini tier supports it
    (``GeminiTierConfig.detect().supports_batch``, i.e. Tier 1+), falling
    back to ``interactive`` on the free tier or by explicit
    ``GoldConfig.prefer_interactive`` opt-out. The retired queue stored the
    mode on ``batch_jobs`` for worker claim routing (ADR-0012); the submit
    stage now owns execution, so the drain only SURFACES the mode in the
    result frame and the free-tier gate stays loud at the submit call
    (RuntimeError in ``gemini_batch.submit_gemini_batch``).

    The enqueue itself is Dagster-native (ADR-0012/0013): one
    ``enrichment_submitted`` partition PER POST on the injected instance,
    under the same per-post key contract the in-flight guard derives.
    """
    db = duckdb
    _ensure_state_tables(db)

    post_ids = list(config.post_ids or [])

    with db.get_connection() as conn:
        # The completed-work record is silver post-ADR-0011: guard against
        # silver_content_classification (which exists and is populated), not
        # gold_analyses (whose domain='instagram' overload is the known bug).
        from orchestration.defs.ig_enriched.slv import classification as _cls

        conn.execute(_cls.CLASSIFICATION_DDL)  # additive, idempotent
        if post_ids:
            # Targeted re-enrichment: ad-hoc post_ids bypass labels, the
            # completion guard, and the in-flight guard (re-process at will).
            pending = conn.execute(
                """SELECT sp.post_id
                   FROM silver_ig_posts sp
                   WHERE list_contains(?, sp.post_id)""",
                [post_ids],
            ).fetchall()
            candidates = [r[0] for r in pending]
            candidates_seen = len(candidates)
        elif config.whole_corpus:
            # Corpus-wide admission (opt-in): bypass the label gate entirely.
            candidates = [
                r[0]
                for r in conn.execute(
                    """
                    SELECT sp.post_id
                    FROM silver_ig_posts sp
                    WHERE sp.caption IS NOT NULL AND trim(sp.caption) <> ''
                      AND NOT EXISTS (
                          SELECT 1 FROM silver_content_classification c
                          WHERE c.post_id = sp.post_id
                            AND c.platform = 'instagram'
                            AND c.prompt_hash = ?
                      )
                    """,
                    [CURRENT_PROMPT_HASH],
                ).fetchall()
            ]
            candidates_seen = len(candidates)
        else:
            candidates = [
                r[0]
                for r in conn.execute(
                    """
                    SELECT l.post_id
                    FROM ig_post_labels l
                    WHERE l.enrich_decision IN ('standout', 'control', 'floor_filler')
                      AND l.label_version = ?
                      AND NOT EXISTS (
                          SELECT 1 FROM silver_content_classification c
                          WHERE c.post_id = l.post_id
                            AND c.platform = 'instagram'
                            AND c.prompt_hash = ?
                      )
                    """,
                    [LABEL_VERSION, CURRENT_PROMPT_HASH],
                ).fetchall()
            ]
            candidates_seen = len(candidates)

    # ── In-flight guard (US-EENG-4): instance-derived, never the queue. ──
    # The in-flight set comes from the Dagster instance via
    # partitions.in_flight_partitions — the SAME function the accounting
    # identity uses, so guard and identity cannot disagree. The instance
    # ALWAYS comes from the asset context (context.instance): ONE instance
    # serves both the in-flight guard here and the enqueue materialization
    # in _materialize_submitted_partitions below. A non-resource, non-config
    # `instance` function parameter would be misread by Dagster as an asset
    # INPUT named "instance" (DagsterInvalidDefinitionError) — which is why
    # the parameter is banned. The module-level _drain_instance is a
    # TEST-ONLY override; production never sets it. There is NO
    # DagsterInstance.get() fallback: an instance the drain cannot obtain
    # from the context fails loudly rather than silently reading a foreign
    # one.
    instance = _drain_instance if _drain_instance is not None else context.instance
    suppressed: list[str] = []
    rounds_by_post: dict[str, int] = {}
    if candidates:
        for pid in candidates:
            state = post_partition_state(instance, DRAIN_WORKLOAD, pid)
            if state.suppressed and not post_ids:
                # The in-flight guard: suppressed ONLY on the discovery
                # path. Explicit post_ids re-enrichment bypasses all guards.
                suppressed.append(pid)
            else:
                # next_round is one past the highest MATERIALIZED harvested
                # round — a first attempt enqueues at 0; a failed post
                # re-enqueues at the retry round its harvested keys imply.
                rounds_by_post[pid] = state.next_round
        if suppressed:
            in_flight_keys = sorted(drain_in_flight_keys(instance))
            logger.warning(
                "ig_posts_gen_batches: %d post(s) IN FLIGHT — not re-enqueued "
                "(post_ids=%s; in-flight partition keys=%s)",
                len(suppressed),
                suppressed,
                in_flight_keys,
            )
            candidates = [pid for pid in candidates if pid not in set(suppressed)]


    n_suppressed = len(suppressed)
    enqueued_ids = sorted(rounds_by_post)

    if not enqueued_ids and not post_ids and candidates_seen == 0:
        # Distinguish "nothing to do" from the in-flight skip above (AC4):
        # a bare "no run" for both cases is an operator-facing ambiguity.
        logger.info("ig_posts_gen_batches: nothing to do (0 candidates)")
    elif not enqueued_ids and n_suppressed:
        logger.warning(
            "ig_posts_gen_batches: enqueueing nothing — ALL %d candidate(s) "
            "in flight (US-EENG-4 AC4 skip)",
            n_suppressed,
        )

    # Execution mode is surfaced in the result frame only — the submit stage
    # owns execution through the seam and enforces the provider readiness
    # gate loudly there. ``consumer="gemini"`` was queue claim routing,
    # replaced by the workload identity carried inside the partition key
    # (DRAIN_WORKLOAD).
    mode: str | None = "seam" if rounds_by_post else None
    if rounds_by_post:
        # ── The enqueue (ADR-0012/0013/0014): Dagster-native, no queue. ──
        # One enrichment_submitted partition per post at its CURRENT round,
        # on the SAME instance the in-flight guard reads. Materializing
        # here is what makes the NEXT run's guard suppress these posts.
        _materialize_submitted_partitions(instance, rounds_by_post)

    return pl.DataFrame(
        {
            "enqueued": pl.Series([len(enqueued_ids)], dtype=pl.Int32),
            "candidates_seen": pl.Series([candidates_seen], dtype=pl.Int32),
            "in_flight_suppressed": pl.Series([n_suppressed], dtype=pl.Int32),
            "mode": pl.Series([mode], dtype=pl.Utf8),
        }
    )


def enumerate_targets(
    conn,
    mode: str,
    limit: int | None = None,
    post_ids: list[str] | None = None,
) -> list[dict]:
    """Posts eligible for the given pass, not yet conformed into silver.

    visual: media-bearing, non-empty caption, no conformed
    ``silver_visual_annotations`` row for the current engine. text:
    non-empty caption, no conformed ``silver_text_annotations`` row.
    """
    if mode not in ("visual", "text"):
        raise ValueError(f"unknown mode: {mode}")
    where = "TRIM(caption) <> ''"
    params: list = []
    if mode == "visual":
        where += " AND media_files IS NOT NULL AND media_files <> '[]'"
    if post_ids:
        where += " AND post_id IN (" + ",".join("?" * len(post_ids)) + ")"
        params.extend(post_ids)
    rows = conn.execute(
        f"SELECT post_id, caption, media_files FROM silver_ig_posts "
        f"WHERE {where} ORDER BY post_id",
        params,
    ).fetchall()
    done = _done_post_ids(conn, mode)
    targets = [
        {"post_id": r[0], "caption": r[1] or "", "media_files": r[2]}
        for r in rows
        if r[0] not in done
    ]
    return targets[:limit] if limit else targets


def _done_post_ids(conn, mode: str) -> set[str]:
    """Post_ids already CONFORMED into silver for ``mode``.

    ADR-0012 D4: completion is a CONFORMED ROW, not a legacy gold write.
    Done iff the pass's typed silver table carries a row for the post that
    was produced by the current engine (model) and, for visual, the current
    facet schema version. Validation lives in silver (ADR-0011): a conformed
    row implies every required field passed, so no JSON sniffing is needed.
    This guard previously read the retired ``gold_growth_facets`` table,
    which marked conform-QUARANTINED posts as done (their gold row was
    written before conform rejected the payload) — 21 posts were silently
    skipped, caught 2026-09-15 when the quarantine anti-join was computed.
    """
    table = (
        "silver_visual_annotations" if mode == "visual"
        else "silver_text_annotations"
    )
    where = (
        "model = ? AND schema_version = ?" if mode == "visual"
        else "model = ?"
    )
    params: list = (
        [_DEFAULT_QWEN_MODEL, GROWTH_FACETS_SCHEMA_VERSION] if mode == "visual"
        else [_DEFAULT_QWEN_MODEL]
    )
    # A state DB that has never run conform has no silver tables yet — that
    # means NOTHING is done, not an error. Guard on THIS mode's table: a
    # text-only state DB has no silver_visual_annotations, and vice versa —
    # checking the wrong table re-bills the other pass on every --run.
    names = {
        r[0]
        for r in conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'main'"
        ).fetchall()
    }
    if table not in names:
        return set()
    sql = f"SELECT post_id FROM {table} WHERE {where}"
    return {r[0] for r in conn.execute(sql, params).fetchall()}

# ── Request building ─────────────────────────────────────────────────────────


def build_facets_batch_requests(
    ops: SQLiteResource,
    conn,
    targets: list[dict],
    mode: str,
) -> list[dict]:
    """Build qwen service job items for facet targets.

    Visual targets resolve media URLs to scrape-time cached LOCAL file paths
    (``media_urls_to_local_paths``) at submit time — the service reads files
    from its own host disk and frame-samples videos with ffmpeg. Text targets
    send ``images=[]`` (caption only). ``custom_key`` is the post_id so
    harvest maps responses directly.

    A visual target whose media resolves to NO cached path is skipped with a
    logged reason (deterministic, re-discovered on the next run) — see the
    module docstring.
    """
    items: list[dict] = []
    for t in targets:
        post_id = t["post_id"]
        caption = t["caption"]
        if mode == "visual":
            images = media_urls_to_local_paths(
                ops, t["media_files"], include_video=True
            )
            if not images:
                logger.warning(
                    "Post %s: no cached media paths resolvable — skipped "
                    "(re-discovered on the next run after media cache fill)",
                    post_id,
                )
                continue
            req = {
                "custom_key": post_id,
                "prompt": build_growth_facets_prompt(caption, len(images)),
                "images": images,
            }
        else:
            req = {
                "custom_key": post_id,
                "prompt": build_text_facets_prompt(caption),
                "images": [],
            }
        items.append(req)
    return items


# ── Cost projection ──────────────────────────────────────────────────────────


def estimate_facets_cost(items: list[dict]) -> tuple[int, float]:
    """(estimated input tokens, projected USD) for a qwen item list.

    Advisory only (not billing): prompt chars/4 + vision tokens per image
    (videos counted as their server-side frame sample), priced at the qwen
    list rates.
    """
    input_tokens = 0
    out_tokens = 0
    for r in items:
        n_video = sum(1 for p in r.get("images") or [] if is_video_path(p))
        n_img = len(r.get("images") or []) - n_video
        input_tokens += (
            len(r.get("prompt") or "") // 4
            + n_img * _TOKENS_PER_IMAGE
            + n_video * _VIDEO_FRAMES * _TOKENS_PER_IMAGE
        )
        out_tokens += (
            _EST_OUTPUT_TOKENS_VISUAL if r.get("images") else _EST_OUTPUT_TOKENS_TEXT
        )
    cost = (
        input_tokens / 1_000_000 * QWEN_INPUT_PRICE_PER_M
        + out_tokens / 1_000_000 * QWEN_OUTPUT_PRICE_PER_M
    )
    return input_tokens, cost


# ── Submit / wait / harvest ──────────────────────────────────────────────────


def _n_media_for(conn, post_id: str) -> int:
    row = conn.execute(
        "SELECT media_files FROM silver_ig_posts WHERE post_id = ?", [post_id]
    ).fetchone()
    if not row or not row[0]:
        return 0
    try:
        urls = json.loads(row[0])
    except json.JSONDecodeError:
        return 0
    return len(urls) if isinstance(urls, list) else 0
