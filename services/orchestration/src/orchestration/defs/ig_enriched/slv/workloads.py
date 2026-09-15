"""The enrichment workload registry — who is eligible, and what one item is.

This module is where a per-domain payload enters the engine. A ``Workload``
binds three things that must agree:

* ``candidates(conn, cfg)`` — the eligibility query: which posts should be
  enriched, in post_id order.
* ``build_item(ops, conn, post_id)`` — the item builder: what the provider is
  asked to do with one of those posts, or a reason it cannot be built.
* ``estimate(items)`` — the cost projection, so a dry run and a real run use
  the same arithmetic.

``engine/submit.py`` iterates this registry and knows nothing domain-specific;
adding a workload is adding a ``Workload`` here, never editing the engine.

ADR-0016: discovery and the in-flight guard are one derivation, and it runs in
the submit stage. The registry supplies candidates; submit reads the in-flight
set once, applies the ``MAX_ROUNDS`` guard, materializes the placeholder, and
submits. No asset writes partitions for another asset to read back.
"""

import json
import logging
import os
from collections.abc import Callable
from dataclasses import dataclass

from dagster import Config

from orchestration.defs.engine.landing import (
    WORKLOAD_CONTENT_CLASSIFICATION,
    WORKLOAD_GROWTH_FACETS_TEXT,
    WORKLOAD_GROWTH_FACETS_VISUAL,
)
from orchestration.defs.engine.media import (
    is_video_path,
    media_urls_to_local_paths,
)
from orchestration.defs.engine.provider import DEFAULT_JOBSPEC, Item, JobSpec
from orchestration.defs.ig_core.slv.labels import LABEL_VERSION
from orchestration.defs.ig_enriched.slv.prompts import (
    _DEFAULT_QWEN_MODEL,
    CURRENT_FACETS_PROMPT_HASH,
    CURRENT_PROMPT_HASH,
    CURRENT_TEXT_FACETS_PROMPT_HASH,
    IG_GOLD_PROMPT,
    build_growth_facets_prompt,
    build_text_facets_prompt,
)
from orchestration.defs.ig_enriched.slv.schemas import GROWTH_FACETS_SCHEMA_VERSION
from orchestration.defs.ig_enriched.slv.visual import UNIVERSAL_MAX_OUTPUT_TOKENS
from orchestration.defs.platform.resources import SQLiteResource

logger = logging.getLogger("enrichment.workloads")


QWEN_INPUT_PRICE_PER_M = float(os.environ.get("QWEN_INPUT_PRICE_PER_M", "0.03"))
QWEN_OUTPUT_PRICE_PER_M = float(os.environ.get("QWEN_OUTPUT_PRICE_PER_M", "0.13"))

# Estimated output tokens per response — cost projection only (not billing).
_EST_OUTPUT_TOKENS_VISUAL = UNIVERSAL_MAX_OUTPUT_TOKENS
_EST_OUTPUT_TOKENS_TEXT = 1024

# Advisory per-media input-token estimates: images are vision tokens; videos
# are frame-sampled server-side into 8 frames.
_TOKENS_PER_IMAGE = 258
_VIDEO_FRAMES = 8


# ── Config ──────────────────────────────────────────────────────────────────


class SubmitConfig(Config):
    """Enrichment admission — configured here because submit decides it.

    Replaces ``GoldConfig``, which was named for the retired ``gold_analyses``
    table and documented a batch drain that no longer exists (ADR-0016).
    """

    #: Target specific posts, bypassing every eligibility guard (re-enrich at
    #: will). Empty = the standard eligibility path.
    post_ids: list[str] = []

    #: Corpus-wide admission: every silver post with a non-empty caption,
    #: including the label pass's ``skip`` posts (ADR-0001).
    whole_corpus: bool = False

    #: Restrict this run to one workload name. Empty = every registered
    #: workload, in registry order.
    workload: str = ""

    #: Cap the number of posts submitted this run.
    limit: int = 250

    #: Project cost and tokens without submitting. Nothing is written to the
    #: instance, so a dry run never makes a post eligible-or-not.
    dry_run: bool = False


# ── Candidate queries ───────────────────────────────────────────────────────


def _classified_source(conn) -> str:
    """The eligible-post SELECT for the classification workload.

    Label-driven by default; ``whole_corpus`` (handled by the caller) widens
    it to every non-empty caption. Explicit ``post_ids`` bypass it entirely.
    """
    return """
        SELECT l.post_id, sp.caption, sp.media_files
        FROM ig_post_labels l
        JOIN silver_ig_posts sp ON sp.post_id = l.post_id
        WHERE l.enrich_decision IN ('standout', 'control', 'floor_filler')
          AND l.label_version = ?
          AND sp.caption IS NOT NULL AND trim(sp.caption) <> ''
          AND NOT EXISTS (
              SELECT 1 FROM silver_content_classification c
              WHERE c.post_id = l.post_id
                AND c.platform = 'instagram'
                AND c.prompt_hash = ?
          )
        ORDER BY l.post_id
    """


def _whole_corpus_source(conn) -> str:
    """Every silver post with a non-empty caption, not yet classified."""
    return """
        SELECT sp.post_id, sp.caption, sp.media_files
        FROM silver_ig_posts sp
        WHERE sp.caption IS NOT NULL AND trim(sp.caption) <> ''
          AND NOT EXISTS (
              SELECT 1 FROM silver_content_classification c
              WHERE c.post_id = sp.post_id
                AND c.platform = 'instagram'
                AND c.prompt_hash = ?
          )
        ORDER BY sp.post_id
    """


def _by_post_ids_source(conn) -> str:
    """Explicit re-enrichment: these posts, guards bypassed."""
    return """
        SELECT sp.post_id, sp.caption, sp.media_files
        FROM silver_ig_posts sp
        WHERE list_contains(?, sp.post_id)
        ORDER BY sp.post_id
    """


def classification_candidates(conn, cfg: SubmitConfig) -> list[dict]:
    """Posts eligible for content classification, in post_id order.

    Three admission arms, in precedence order: explicit ``post_ids`` (bypasses
    everything), ``whole_corpus`` (bypasses the label gate), and the standard
    label-driven path. A completed pass is a CONFORMED row for the current
    prompt hash — never a legacy gold write (ADR-0012 D4).
    """
    if cfg.post_ids:
        rows = conn.execute(
            _by_post_ids_source(conn), [list(cfg.post_ids)]
        ).fetchall()
    elif cfg.whole_corpus:
        rows = conn.execute(
            _whole_corpus_source(conn), [CURRENT_PROMPT_HASH]
        ).fetchall()
    else:
        rows = conn.execute(
            _classified_source(conn), [LABEL_VERSION, CURRENT_PROMPT_HASH]
        ).fetchall()
    return [
        {"post_id": r[0], "caption": r[1] or "", "media_files": r[2]} for r in rows
    ]


# ── Item building ───────────────────────────────────────────────────────────


class UnbuildablePostError(Exception):
    """A candidate cannot become a provider item.

    ``retryable`` distinguishes a cause that may clear (a media cache miss)
    from a deterministic one (empty caption), because only the former mints a
    retry round. The caller turns this into a terminal failure for that post
    alone — never a silent skip that strands its partition in flight.
    """

    def __init__(self, reason: str, *, retryable: bool) -> None:
        super().__init__(reason)
        self.retryable = retryable


def _resolve_media_paths(
    ops: SQLiteResource, media_files_json: str | None
) -> tuple[str, ...]:
    """Resolve a post's media URLs to CACHED local byte paths.

    Cache-only by design: the scrape-time byte cache is the reliable copy
    (CDN URLs die in ~4-5 days), and no provider transport is named here.
    A cache miss raises — the caller makes it a terminal failure for that post
    alone; submitting partial media would silently change the analysis input.
    """
    from orchestration.defs.engine.media import cached_local_path

    if not media_files_json:
        return ()
    try:
        urls = [u for u in json.loads(media_files_json) if u]
    except json.JSONDecodeError as exc:
        raise UnbuildablePostError(
            f"unparseable media_files: {exc}", retryable=False
        ) from exc
    paths: list[str] = []
    for url in urls:
        path = cached_local_path(ops, url)
        if not path:
            raise UnbuildablePostError(
                f"media cache miss for {url[:120]} — not submitting partial media",
                retryable=True,
            )
        paths.append(path)
    return tuple(paths)


def _classification_item(
    ops: SQLiteResource, conn, candidate: dict
) -> Item:
    """The classification item: the gold prompt plus the caption.

    The caption is appended verbatim. This literal was previously duplicated
    across two modules; it lives here now, once.
    """
    post_id = candidate["post_id"]
    caption = (candidate.get("caption") or "").strip()
    if not caption:
        raise UnbuildablePostError("empty caption — nothing to enrich", retryable=False)
    images = _resolve_media_paths(ops, candidate.get("media_files"))
    return Item(
        custom_key="",  # the submit stage owns the key (it owns the partition)
        prompt=f"{IG_GOLD_PROMPT}\n{caption}",
        images=images,
        post_id=post_id,
        platform="instagram",
    )


# ── The growth-facets passes (visual + text) ────────────────────────────────


def _facets_candidates(conn, cfg: SubmitConfig, mode: str) -> list[dict]:
    """Posts eligible for a facet pass, in post_id order.

    Eligibility is "no CONFORMED row for the current engine under this pass's
    schema", read from that pass's typed silver table — never from the
    retired ``gold_growth_facets`` table, which marked conform-quarantined
    posts as done (21 posts were silently skipped; caught 2026-09-15).
    """
    where = "TRIM(caption) <> ''"
    params: list = []
    if mode == "visual":
        where += " AND media_files IS NOT NULL AND media_files <> '[]'"
    if cfg.post_ids:
        where += " AND post_id IN (" + ",".join("?" * len(cfg.post_ids)) + ")"
        params.extend(cfg.post_ids)
    rows = conn.execute(
        f"SELECT post_id, caption, media_files FROM silver_ig_posts "
        f"WHERE {where} ORDER BY post_id",
        params,
    ).fetchall()
    done = _facets_done_post_ids(conn, mode)
    return [
        {"post_id": r[0], "caption": r[1] or "", "media_files": r[2]}
        for r in rows
        if r[0] not in done
    ]


def _facets_done_post_ids(conn, mode: str) -> set[str]:
    """Post_ids already CONFORMED into silver for this facet pass.

    ADR-0012 D4: completion is a CONFORMED row. A state DB that has never run
    conform has no silver tables yet — that means NOTHING is done, not an
    error. Guard on THIS pass's table: a text-only state DB has no
    ``silver_visual_annotations``, and checking the wrong table re-bills the
    other pass on every run.
    """
    table = (
        "silver_visual_annotations" if mode == "visual"
        else "silver_text_annotations"
    )
    where = (
        "model = ? AND schema_version = ?" if mode == "visual" else "model = ?"
    )
    params: list = (
        [_DEFAULT_QWEN_MODEL, GROWTH_FACETS_SCHEMA_VERSION]
        if mode == "visual"
        else [_DEFAULT_QWEN_MODEL]
    )
    names = {
        r[0]
        for r in conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'main'"
        ).fetchall()
    }
    if table not in names:
        return set()
    return {
        r[0]
        for r in conn.execute(f"SELECT post_id FROM {table} WHERE {where}", params).fetchall()
    }


def visual_facets_candidates(conn, cfg: SubmitConfig) -> list[dict]:
    """Media-bearing posts with no current conformed visual facet row."""
    return _facets_candidates(conn, cfg, "visual")


def text_facets_candidates(conn, cfg: SubmitConfig) -> list[dict]:
    """Caption-bearing posts with no current conformed text facet row."""
    return _facets_candidates(conn, cfg, "text")


def _visual_facets_item(ops: SQLiteResource, conn, candidate: dict) -> Item:
    """A visual facet item: the growth prompt plus CACHED media paths.

    The service frame-samples video files with ffmpeg on its own host, so the
    client hands it absolute paths to the scrape-time byte cache, never URLs.
    """
    post_id = candidate["post_id"]
    caption = (candidate.get("caption") or "").strip()
    if not caption:
        raise UnbuildablePostError(
            "empty caption — nothing to enrich", retryable=False
        )
    images = media_urls_to_local_paths(
        ops, candidate.get("media_files"), include_video=True
    )
    if not images:
        # Deterministic but cache-dependent: the media may arrive later, so
        # this is retryable rather than a permanent skip.
        raise UnbuildablePostError(
            "no cached media paths resolvable", retryable=True
        )
    return Item(
        custom_key="",
        prompt=build_growth_facets_prompt(caption, len(images)),
        images=tuple(images),
        post_id=post_id,
        platform="instagram",
    )


def _text_facets_item(ops: SQLiteResource, conn, candidate: dict) -> Item:
    """A text facet item: the text prompt, caption only, no media."""
    post_id = candidate["post_id"]
    caption = (candidate.get("caption") or "").strip()
    if not caption:
        raise UnbuildablePostError(
            "empty caption — nothing to enrich", retryable=False
        )
    return Item(
        custom_key="",
        prompt=build_text_facets_prompt(caption),
        images=(),
        post_id=post_id,
        platform="instagram",
    )


def estimate_facets_cost(items: list[Item]) -> tuple[int, float]:
    """(estimated input tokens, projected USD) for a facet item list."""
    input_tokens = 0
    out_tokens = 0
    for it in items:
        n_video = sum(1 for p in it.images if is_video_path(p))
        n_img = len(it.images) - n_video
        input_tokens += (
            len(it.prompt) // 4
            + n_img * _TOKENS_PER_IMAGE
            + n_video * _VIDEO_FRAMES * _TOKENS_PER_IMAGE
        )
        out_tokens += (
            _EST_OUTPUT_TOKENS_VISUAL if it.images else _EST_OUTPUT_TOKENS_TEXT
        )
    cost = (
        input_tokens / 1_000_000 * QWEN_INPUT_PRICE_PER_M
        + out_tokens / 1_000_000 * QWEN_OUTPUT_PRICE_PER_M
    )
    return input_tokens, cost


def _parse_visual_facets(conn, post_id: str, text: str) -> dict:
    """Parse a visual facet response, with the post's media count as the
    carousel guard (``n`` must equal ``len(image_summaries)``)."""
    from orchestration.defs.ig_enriched.slv.visual import parse_universal_response

    return parse_universal_response(text, n_media_for(conn, post_id))


def _parse_text_facets(conn, post_id: str, text: str) -> dict:
    """Parse a text facet response — no media, so the guard is caption-side."""
    from orchestration.defs.ig_enriched.slv.text import parse_text_response

    return parse_text_response(text)


def n_media_for(conn, post_id: str) -> int:
    """How many media files a post carries — the parse-time carousel guard."""
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


# ── Cost projection ─────────────────────────────────────────────────────────


def estimate_classification_cost(items: list[Item]) -> tuple[int, float]:
    """(estimated input tokens, projected USD) for classification items."""
    input_tokens = 0
    out_tokens = 0
    for it in items:
        n_video = sum(1 for p in it.images if is_video_path(p))
        n_img = len(it.images) - n_video
        input_tokens += (
            len(it.prompt) // 4
            + n_img * _TOKENS_PER_IMAGE
            + n_video * _VIDEO_FRAMES * _TOKENS_PER_IMAGE
        )
        out_tokens += _EST_OUTPUT_TOKENS_VISUAL if it.images else _EST_OUTPUT_TOKENS_TEXT
    cost = (
        input_tokens / 1_000_000 * QWEN_INPUT_PRICE_PER_M
        + out_tokens / 1_000_000 * QWEN_OUTPUT_PRICE_PER_M
    )
    return input_tokens, cost


# ── The registry ────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Workload:
    """One enrichment pass: eligibility, item construction, cost projection.

    The pass declares everything the engine would otherwise have to know
    about it — including the job-level options its provider call needs — so
    ``engine/submit.py`` stays domain-free.
    """

    name: str
    #: The silver table a completed pass lands in — the anti-join check's
    #: left-hand side. A workload with no entry here is invisible to
    #: ``check_no_silent_loss``.
    silver_table: str
    candidates: Callable[..., list[dict]]
    build_item: Callable[..., Item]
    estimate: Callable[[list[Item]], tuple[int, float]]
    #: Media-bearing passes submit only posts with cached media; a caption-only
    #: pass submits everything eligible.
    media_bearing: bool = False
    #: Job-level options for this pass's provider call (``max_tokens``,
    #: ``mode``). The classification pass needs none; the facet passes set
    #: both, which is why they are per-workload and not per-run.
    job_spec: JobSpec = DEFAULT_JOBSPEC
    #: The prompt identity recorded on every bronze landing this pass makes.
    #: Provenance rides on the row (ADR-0010/0011), so the pass must declare
    #: it here rather than have the harvest stage guess per workload.
    prompt_hash: str = CURRENT_PROMPT_HASH
    #: The schema version the landing is validated against. The parse-time
    #: guard for a carousel is ``n_media``; this is what conform reads.
    schema_version: str = ""
    #: The workload's parser: turns a landed response body into the pass's
    #: typed facets, given the post's media count. ``None`` for passes whose
    #: validation lives entirely in conform.
    parse: Callable[..., dict] | None = None


WORKLOADS: tuple[Workload, ...] = (
    Workload(
        name=WORKLOAD_CONTENT_CLASSIFICATION,
        silver_table="silver_content_classification",
        candidates=classification_candidates,
        build_item=_classification_item,
        estimate=estimate_classification_cost,
        media_bearing=True,
    ),
    Workload(
        name=WORKLOAD_GROWTH_FACETS_VISUAL,
        silver_table="silver_visual_annotations",
        candidates=visual_facets_candidates,
        build_item=_visual_facets_item,
        estimate=estimate_facets_cost,
        media_bearing=True,
        job_spec=JobSpec(
            max_tokens=UNIVERSAL_MAX_OUTPUT_TOKENS, mode="visual"
        ),
        prompt_hash=CURRENT_FACETS_PROMPT_HASH,
        schema_version=GROWTH_FACETS_SCHEMA_VERSION,
        parse=_parse_visual_facets,
    ),
    Workload(
        name=WORKLOAD_GROWTH_FACETS_TEXT,
        silver_table="silver_text_annotations",
        candidates=text_facets_candidates,
        build_item=_text_facets_item,
        estimate=estimate_facets_cost,
        media_bearing=False,
        job_spec=JobSpec(max_tokens=1024, mode="text"),
        prompt_hash=CURRENT_TEXT_FACETS_PROMPT_HASH,
        schema_version=GROWTH_FACETS_SCHEMA_VERSION,
        parse=_parse_text_facets,
    ),
)

#: Workload name → declaration, for dispatch and for the anti-join check.
WORKLOAD_BY_NAME: dict[str, Workload] = {w.name: w for w in WORKLOADS}


def workloads_for(cfg: SubmitConfig) -> tuple[Workload, ...]:
    """The workloads this run covers, in registry order.

    ``cfg.workload`` that names nothing registered is an ERROR: a typo would
    otherwise submit nothing and look like a successful empty run.
    """
    if not cfg.workload:
        return WORKLOADS
    match = WORKLOAD_BY_NAME.get(cfg.workload)
    if match is None:
        raise ValueError(
            f"unknown workload {cfg.workload!r}; registered: "
            f"{sorted(WORKLOAD_BY_NAME)}"
        )
    return (match,)
