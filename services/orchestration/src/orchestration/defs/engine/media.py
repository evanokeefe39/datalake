"""Post-media byte cache — scrape-time bytes on disk, keyed by source URL.

Instagram CDN URLs expire in days while enrichment may run much later, so media
bytes are downloaded at scrape time into the post-media directory and recorded
in the media cache. The row contract (key scheme, table DDL, the shared INSERT)
lives in `opsdb.media_cache`; this module owns the *bytes* — downloading,
atomic writes, and the URL → local-path resolution the submit path uses.

NO ffmpeg here. Images and video files both pass through as absolute paths;
video is frame-sampled by the inference service on its own host, never by this
client or this repo.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import urllib.request
from pathlib import Path

from opsdb.media_cache import (
    _ensure_media_cache_table,
    cached_local_path,
    record_media_cache_row,
    url_hash,
)

from orchestration.defs.platform.paths import POST_MEDIA_DIR
from orchestration.defs.platform.resources import SQLiteResource

logger = logging.getLogger("engine.media")

# from the local bytes, falling back to the live CDN only on a cache miss.

_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

_EXT_BY_MIME = {
    "image/jpeg": ".jpg",
    "image/png": ".png",
    "image/webp": ".webp",
    "image/gif": ".gif",
    "video/mp4": ".mp4",
    "video/webm": ".webm",
    "video/quicktime": ".mov",
}

_CONTENT_TYPE_BY_EXT = {v: k for k, v in _EXT_BY_MIME.items()}


def _download_bytes(url: str) -> tuple[bytes, str] | None:
    """Download ``url``; return ``(bytes, content_type)`` or None on failure."""
    req = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            content_type = resp.headers.get("Content-Type", "")
            return resp.read(), content_type
    except Exception as exc:  # network errors are non-fatal — cache is best-effort
        logger.warning("media download failed for %s: %s", url[:80], exc)
        return None


def _atomic_write(path: Path, data: bytes) -> None:
    """Write bytes atomically (temp file + rename) to avoid partial reads."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(data)
    os.replace(tmp, path)


def cache_media_bytes(
    ops: SQLiteResource,
    media_url: str,
    *,
    media_dir: Path | None = None,
) -> str | None:
    """Download media bytes and record them in ``media_cache`` (best-effort).

    Returns the local file path, or None if the download failed or the URL was
    already cached. Failure is non-fatal — the worker falls back to the CDN.
    """
    _ensure_media_cache_table(ops)
    existing = cached_local_path(ops, media_url)
    if existing:
        return existing

    downloaded = _download_bytes(media_url)
    if downloaded is None:
        return None
    data, content_type = downloaded
    base_type = content_type.split(";")[0].strip().lower()
    ext = _EXT_BY_MIME.get(base_type, ".bin")

    cache_key = url_hash(media_url)
    dest = (media_dir or POST_MEDIA_DIR) / f"{cache_key}{ext}"
    _atomic_write(dest, data)

    record_media_cache_row(
        ops, cache_key, str(dest), content_type, dest.stat().st_size, media_url
    )

    logger.info("cached %s → %s", media_url[:80], dest.name)
    return str(dest)



def seed_media_from_file(
    ops: SQLiteResource,
    media_url: str,
    src_path: Path,
    *,
    media_dir: Path | None = None,
) -> str | None:
    """Seed ``media_cache`` from an existing local file — no download.

    A SEED, not a fetch: copies already-downloaded bytes into the
    ``POST_MEDIA_DIR`` cache keyed by sha256(url) so the cache is
    self-contained (the source checkout may move or disappear). Use for
    local-disk ingestion where the CDN URLs in the metadata are already
    stale/expiring and re-downloading is both wasteful and lossy.

    Idempotent: returns the cached path when the URL is already cached.
    Returns None when the source file is missing (caller decides severity).
    """
    _ensure_media_cache_table(ops)
    existing = cached_local_path(ops, media_url)
    if existing:
        return existing
    if not src_path.exists():
        logger.warning("seed: source missing for %s: %s", media_url[:80], src_path)
        return None

    cache_key = url_hash(media_url)
    ext = src_path.suffix.lower()
    content_type = _CONTENT_TYPE_BY_EXT.get(ext, "application/octet-stream")
    dest = (media_dir or POST_MEDIA_DIR) / f"{cache_key}{ext}"
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".tmp")
    shutil.copyfile(src_path, tmp)
    os.replace(tmp, dest)

    record_media_cache_row(
        ops, cache_key, str(dest), content_type, dest.stat().st_size, media_url
    )

    logger.info("seeded %s → %s", media_url[:80], dest.name)
    return str(dest)


VIDEO_EXTENSIONS = frozenset({".mp4", ".mov", ".webm", ".m4v"})


logger = logging.getLogger("enrichment.media_paths")


def is_video_path(path: str | Path) -> bool:
    """True when ``path`` has a video file extension."""
    return Path(path).suffix.lower() in VIDEO_EXTENSIONS


def media_urls_to_local_paths(
    ops: SQLiteResource,
    media_files_json: str | list[str],
    *,
    include_video: bool = True,
) -> list[str]:
    """Resolve media URLs to existing cached local paths, in input order.

    Args:
        ops: SQLite resource holding the ``media_cache`` table.
        media_files_json: JSON array of media URLs, or a list of URLs.
        include_video: When False, video files are skipped (images only).

    Returns deduped, absolute (``Path.resolve()``) paths for URLs whose
    cached bytes exist on disk, in deterministic (input) order. A URL with
    no cached bytes is skipped silently — never raises.

    Note: video files are passed through unmodified and are frame-sampled
    by the inference service, not by this client.
    """
    if isinstance(media_files_json, str):
        try:
            urls = json.loads(media_files_json)
        except (ValueError, TypeError):
            logger.warning(
                "media_files_json is not valid JSON — skipping (%r)",
                media_files_json[:120],
            )
            return []
    else:
        urls = media_files_json

    resolved: list[str] = []
    seen: set[str] = set()
    for url in urls:
        local = cached_local_path(ops, url)
        if local is None:
            continue
        if not include_video and is_video_path(local):
            continue
        abs_path = str(Path(local).resolve())
        if abs_path not in seen and Path(abs_path).exists():
            seen.add(abs_path)
            resolved.append(abs_path)
    return resolved
