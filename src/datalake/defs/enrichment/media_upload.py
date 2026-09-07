"""Dagster-native media pre-upload — Phase 2a of the batch-native migration.

gemini-batch requires media to be resolvable before submit (File API URIs or
inline bytes). In the worker era that upload happened inside the long submit
cycle; here it is its own bounded op: a short run resolves media for a bounded
set of pending batch candidates ahead of submit, so the submit run stays short
and the crash blast radius is one bounded chunk of uploads.

Media semantics are the worker's, reused unchanged: the scrape-time byte cache
(``media_cache`` table) + Gemini File API via ``lookup_or_upload_all`` —
cache-first and idempotent (keyed by URL hash, with the same TOCTOU claim and
per-file failure isolation). Tier and per-item token gates are applied at
submit time (the submit path re-resolves through the same helpers).

This module also hosts the ADR-0008 seam-purity scanner (Phase 5 guard) that
backs the ``check_enrichment_seam_purity`` asset check: pure enrichment
modules must contain no Gemini API markers, and every op in the enrichment
package must carry the ``{adr: 0008, seam: enrichment-api}`` tag.
"""

from __future__ import annotations

import importlib
import inspect
import json
import logging
import pkgutil

from dagster import AssetMaterialization, OpDefinition, op, job

from datalake.defs.common.resources import DuckDBResource, GeminiResource, SQLiteResource
from datalake.defs.enrichment.batch import _ensure_schema
from datalake.defs.enrichment.media_cache import lookup_or_upload_all

logger = logging.getLogger("enrichment.media_upload")

# ADR-0008 seam tag — every op that may touch the Gemini API carries this.
SEAM_TAGS = {"adr": "0008", "seam": "enrichment-api"}

# Mirror of the worker's domain → silver table map (media resolution only
# needs the media_files column; captions are re-read at submit time).
_SILVER_TABLES: dict[str, str] = {
    "instagram": "silver_ig_posts",
}

# Default bound: at most this many distinct posts get media resolved per run.
DEFAULT_UPLOAD_LIMIT = 50

# Source markers that indicate a Gemini API call. Any hit inside a "pure"
# enrichment module is an ADR-0008 violation.
_API_MARKERS: tuple[str, ...] = (
    "generate_content",
    "GeminiClient",
    "files.upload",
    "batches.get",
    "batches.submit",
    "gemini_batch.",
    "lookup_or_upload_all",
    ".analyze(",
)

# Enrichment modules that must stay hermetic (no API calls, no ops).
_PURE_MODULES: tuple[str, ...] = (
    "datalake.defs.enrichment.assets",
    "datalake.defs.enrichment.registry",
    "datalake.defs.enrichment.prompts",
)


# ── Candidate discovery ─────────────────────────────────────────────────────


def pending_media_candidates(ops: SQLiteResource, limit: int) -> list[dict]:
    """Pending batch items on jobs that have not been submitted to Gemini yet.

    Returns ``{"item_id", "post_id", "domain"}`` dicts in batch-item order,
    bounded to ``limit`` items. Items on already-submitted jobs are in-flight
    ('processing') and not candidates.
    """
    conn = ops.get_connection()
    try:
        rows = conn.execute(
            "SELECT bi.id, bi.payload FROM batch_items bi "
            "JOIN batch_jobs bj ON bj.id = bi.job_id "
            "WHERE bi.status = 'pending' AND bj.gemini_batch_name IS NULL "
            "ORDER BY bi.id LIMIT ?",
            [limit],
        ).fetchall()
    finally:
        conn.close()
    out: list[dict] = []
    for item_id, payload in rows:
        try:
            p = json.loads(payload)
            out.append(
                {
                    "item_id": item_id,
                    "post_id": p["post_id"],
                    "domain": p["domain"],
                }
            )
        except (json.JSONDecodeError, KeyError) as exc:
            logger.warning("Skipping unparseable batch item %d: %s", item_id, exc)
    return out


def _uploaded_uris(ops: SQLiteResource, media_files_json: str | None) -> set[str]:
    """File API URIs already recorded as uploaded for these media URLs."""
    if not media_files_json:
        return set()
    from datalake.defs.enrichment.media_cache import url_hash

    try:
        urls = json.loads(media_files_json)
    except (json.JSONDecodeError, TypeError):
        return set()
    if not urls:
        return set()
    conn = ops.get_connection()
    try:
        rows = conn.execute(
            "SELECT file_api_uri FROM media_metadata WHERE media_url_hash IN "
            f"({','.join('?' * len(urls))}) AND upload_state = 'uploaded'",
            [url_hash(u) for u in urls],
        ).fetchall()
    finally:
        conn.close()
    return {r[0] for r in rows if r[0]}


# ── Core (mock-testable) ────────────────────────────────────────────────────


def upload_media_for_pending_batches(
    ops: SQLiteResource,
    duckdb: DuckDBResource,
    gemini: GeminiResource,
    limit: int = DEFAULT_UPLOAD_LIMIT,
) -> dict:
    """Resolve media for a bounded set of pending batch candidates.

    Idempotent: ``lookup_or_upload_all`` is cache-first — a URL whose File API
    URI is still live is served from ``media_metadata`` and never re-uploaded,
    so re-running the op (or a crashed run re-running) issues no duplicate
    File API uploads. Per-post failures are isolated and counted, never abort
    the run (the submit path re-resolves and applies its own per-item failure
    routing). Bounded: at most ``limit`` distinct posts per run.

    Returns ``{"posts", "uploads", "cache_hits", "failed"}``.
    """
    _ensure_schema(ops)
    # media_metadata must exist before the cache pre-read (the real upload
    # path re-ensures it inside lookup_or_upload_all; we read earlier).
    from datalake.defs.enrichment.media_cache import (
        _ensure_schema as _ensure_media_schema,
    )

    _ensure_media_schema(ops)
    candidates = pending_media_candidates(ops, limit)

    posts = 0
    uploads = 0
    cache_hits = 0
    failed = 0
    seen_posts: set[str] = set()

    for cand in candidates:
        post_id, domain = cand["post_id"], cand["domain"]
        if post_id in seen_posts:
            continue
        seen_posts.add(post_id)

        table = _SILVER_TABLES.get(domain)
        if not table:
            continue
        with duckdb.get_connection() as conn:
            row = conn.execute(
                f"SELECT media_files FROM {table} WHERE post_id = ?",
                [post_id],
            ).fetchone()
        media_files_json = row[0] if row else None
        if not media_files_json:
            continue  # text-only post — nothing to pre-upload

        pre = _uploaded_uris(ops, media_files_json)
        try:
            media = lookup_or_upload_all(
                ops, gemini, media_files_json, inline_images=True
            )
        except Exception as exc:
            failed += 1
            logger.warning("Media pre-upload failed for %s: %s", post_id, exc)
            continue
        posts += 1
        for mf in media:
            uri = mf.get("uri")
            if uri is None:
                continue  # inline_data part — no File API upload involved
            if uri in pre:
                cache_hits += 1
            else:
                uploads += 1

    result = {
        "posts": posts,
        "uploads": uploads,
        "cache_hits": cache_hits,
        "failed": failed,
    }
    logger.info(
        "Media pre-upload: %d post(s) resolved, %d upload(s), %d cache hit(s), "
        "%d failure(s)",
        posts, uploads, cache_hits, failed,
    )
    return result


# ── Dagster op + job ────────────────────────────────────────────────────────


@op(tags=SEAM_TAGS)
def media_upload_pending_batches_op(context, ops, duckdb, gemini) -> dict:
    """Pre-upload media for pending batch candidates (short, bounded)."""
    result = upload_media_for_pending_batches(ops, duckdb, gemini)
    if result["posts"]:
        context.log_event(
            AssetMaterialization(
                asset_key="gold_analyses",
                metadata={
                    "writer": "media_upload_pending_batches",
                    "posts": result["posts"],
                    "uploads": result["uploads"],
                    "cache_hits": result["cache_hits"],
                },
            )
        )
    return result


@job(name="media_upload_pending_batches_job")
def media_upload_pending_batches_job():
    """Resolve + upload media for pending batch candidates ahead of submit."""
    media_upload_pending_batches_op()


# ── ADR-0008 seam guard (Phase 5) ───────────────────────────────────────────


def _module_ops(module) -> list[OpDefinition]:
    return [
        attr
        for attr in vars(module).values()
        if isinstance(attr, OpDefinition)
    ]


def seam_violations(
    pure_modules: list | None = None,
    extra_modules: list | None = None,
) -> list[str]:
    """ADR-0008 seam purity scan over the enrichment domain.

    Two rules:

    1. Pure modules (assets/registry/prompts by default) must contain no
       Gemini API markers in their source and must not define Dagster ops —
       this is what rejects a ``generate_content`` call added to a pure asset.
    2. Every op in the enrichment package (plus ``extra_modules``, for tests)
       must carry the ``seam: enrichment-api`` tag — an untagged op is a seam
       violation even if its API use is currently benign.

    ``pure_modules``/``extra_modules`` accept any module object so tests can
    inject synthetic modules without editing the package.
    """
    violations: list[str] = []

    modules = pure_modules
    if modules is None:
        modules = [importlib.import_module(m) for m in _PURE_MODULES]
    for module in modules:
        src = inspect.getsource(module)
        for marker in _API_MARKERS:
            if marker in src:
                violations.append(
                    f"{module.__name__}: hermetic module contains API marker "
                    f"'{marker}'"
                )
        for op_def in _module_ops(module):
            violations.append(
                f"{module.__name__}: op '{op_def.name}' defined in a "
                "hermetic module"
            )

    # NOTE: `import datalake.defs.enrichment as pkg` fails repo-wide —
    # datalake/__init__.py shadows the `defs` attribute with the Definitions
    # object — so resolve the package through importlib instead.
    pkg = importlib.import_module("datalake.defs.enrichment")
    walk = extra_modules or []
    try:
        for mod_info in pkgutil.iter_modules(pkg.__path__):
            walk.append(importlib.import_module(f"{pkg.__name__}.{mod_info.name}"))
    except Exception as exc:  # pragma: no cover - defensive
        violations.append(f"enrichment package walk failed: {exc}")

    for module in walk:
        for op_def in _module_ops(module):
            tags = op_def.tags or {}
            if tags.get("seam") != "enrichment-api":
                violations.append(
                    f"{module.__name__}: op '{op_def.name}' is missing the "
                    f"ADR-0008 seam tag {SEAM_TAGS}"
                )
    return violations
