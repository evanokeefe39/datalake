"""Thin resolver: media URLs -> scrape-time cached local file paths.

NO ffmpeg here. Native images AND video files are both passed through
unmodified as absolute paths; video files are frame-sampled by the
qwen-batch service (which runs ffmpeg on its own host), never by this
client or the datalake repo.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

from datalake.defs.common.resources import SQLiteResource
from datalake.defs.enrichment.media_cache import cached_local_path

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
    by the qwen-batch service, not by this client.
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
