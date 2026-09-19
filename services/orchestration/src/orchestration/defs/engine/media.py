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
import sqlite3
import time
import urllib.error
import urllib.request
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from opsdb.media_cache import (
    _ensure_media_cache_table,
    media_key,
    record_media_cache_row,
    stored_local_path,
    url_hash,  # noqa: F401 — re-exported: callers import it from this module
)

from orchestration.defs.platform.paths import POST_MEDIA_DIR, runtime_path
from orchestration.defs.platform.resources import SQLiteResource


def _safe_name(cache_key: str) -> str:
    """A cache key rendered safe for use as a filename stem.

    The stable key is ``mid:<id>`` and a colon is illegal on Windows, so it is
    spelled ``mid-<id>`` on disk. The mapping is one-way and never parsed back —
    the row's ``cache_key`` is the authority, and the filename is just storage.
    """
    return cache_key.replace(":", "-")

logger = logging.getLogger("engine.media")

#: Fetch attempts per media URL before giving up.
#:
#: A media fetch is the LAST chance at these bytes: the CDN URL is signed and
#: expires in days, and nothing re-fetches it later (the cache is filled only at
#: scrape time), so a transient failure here permanently loses the media and
#: makes the post un-enrichable. 3 attempts rides out a brief CDN wobble at a
#: cost far below a lost post.
MEDIA_CACHE_ATTEMPTS: int = 3

#: Base seconds for the retry backoff; attempt N waits base * 2**(N-1).
MEDIA_CACHE_BACKOFF_BASE: float = 1.0

#: 4xx codes that are actually TRANSIENT and must take the retry path.
#:
#: 429 is a rate limit and 408 a request timeout — both clear on retry. Treating
#: them as permanent would burn zero retries, mislabel a throttled burst as
#: "expired", and erase the one signal that would tell us the CDN is limiting us
#: (which is the open production-egress question).
_TRANSIENT_HTTP_CODES: frozenset[int] = frozenset({408, 429})


class _PermanentFetchError(Exception):
    """A 4xx media fetch — the URL will never succeed, so do not retry.

    Instagram's CDN signs URLs with an ``oe`` expiry (~4.5 days, measured) and
    returns 403 once it passes. Retrying an expired signature cannot help, and
    the corpus has 556 such posts, so the short-circuit matters: 1 attempt
    instead of 3 turns ~4.4s per dead URL into ~0.6s.
    """

    def __init__(
        self, code: int, url: str, *, expiry: datetime | None = None
    ) -> None:
        super().__init__(f"HTTP {code} for {url[:120]}")
        self.code = code
        self.url = url
        #: The URL's own signed expiry (`oe`), when it carries one.
        self.expiry = expiry

    def diagnosis(self) -> str:
        """Why this failed, in terms an operator can act on.

        Distinguishes the two cases a bare 403 conflates: an expired signature
        (nothing to do but mint a fresh URL) from a 403 on a URL that is still
        signed (a block or revocation — a different problem entirely).
        """
        if self.expiry is None:
            return "no signed expiry in the URL — treat as revoked/unavailable"
        if self.expiry <= datetime.now(UTC):
            return f"signed URL EXPIRED at {self.expiry.isoformat()} (expected)"
        return (
            f"HTTP {self.code} on a URL still valid until {self.expiry.isoformat()}"
            " — NOT expiry; investigate (possible block or revocation)"
        )


def _signed_url_expiry(url: str) -> datetime | None:
    """Decode the Instagram CDN ``oe`` parameter to its UTC expiry, if present.

    ``oe`` is a hex-encoded unix timestamp. Reading it turns an ambiguous 403
    into a definite cause, which is what the antibot question needed and did
    not have.
    """
    try:
        from urllib.parse import parse_qs, urlparse

        oe = parse_qs(urlparse(url).query).get("oe", [None])[0]
        if not oe:
            return None
        return datetime.fromtimestamp(int(oe, 16), UTC)
    except (ValueError, TypeError):
        return None


def local_media_path(
    ops: SQLiteResource,
    media_url: str,
    *,
    conn: sqlite3.Connection | None = None,
) -> str | None:
    """Resolve a URL to a byte path THIS process can open, or None on a miss.

    The stored path is expressed in the vocabulary of whichever process fetched
    the bytes, so it is translated through the configured host↔container prefix
    map BEFORE the existence test — a stored Windows path is unopenable inside a
    Linux container even though the bytes are mounted right there. A stored path
    outside the map passes through unchanged (a host run, the common case).

    `conn`, when given, is an already-open connection to reuse; opening one costs
    ~23 ms, which dominates a batch caller that resolves thousands of URLs.

    Returns None when there is no row, or the translated path is absent on disk:
    a row whose file was deleted is a miss, so a caller re-fetches rather than
    opening a dead path.
    """
    stored = stored_local_path(ops, media_url, conn=conn)
    if not stored:
        return None
    path = runtime_path(stored)
    return str(path.resolve()) if path.exists() else None

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
    """Download ``url``; return ``(bytes, content_type)`` or None on failure.

    Raises :class:`_PermanentFetchError` for a 4xx: the URL is signed and
    expired (Instagram's CDN returns 403 once ``oe`` passes), a 404, or a
    revoked link. Those never succeed on retry, so the caller must not spend
    the attempt budget — measured at 4.4s per dead URL at 3 attempts.
    """
    req = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            content_type = resp.headers.get("Content-Type", "")
            return resp.read(), content_type
    except urllib.error.HTTPError as exc:
        if exc.code in _TRANSIENT_HTTP_CODES:
            # 429 (rate limited) and 408 (request timeout) are TRANSIENT despite
            # being 4xx. Classifying them permanent would spend zero retries,
            # log them as "expired or gone", and land a throttled burst as
            # permanent misses — which would also make a real rate-limit signal
            # indistinguishable from expiry in the sidecar counts. That is
            # precisely the blindness this change exists to remove.
            logger.warning(
                "media download TRANSIENT failure for %s: HTTP %s (retrying)",
                url[:80],
                exc.code,
            )
            return None
        if 400 <= exc.code < 500:
            # PERMANENT (403/404/410): a signature that expired never revives.
            #
            # Say WHICH 4xx, and whether the URL's own signed expiry had passed.
            # A 403 is ambiguous on its face — expired signature, revoked link,
            # and (in principle) rate limiting all return it — and that
            # ambiguity is exactly what made the antibot question take a
            # dedicated burst test to settle. The `oe` parameter removes it:
            # an expired `oe` proves expiry, a live `oe` with a 403 means
            # something else (block, revocation) and wants a different response.
            raise _PermanentFetchError(exc.code, url, expiry=_signed_url_expiry(url)) from exc
        # 5xx is TRANSIENT — fall through to the retryable None.
        logger.warning("media download failed for %s: HTTP %s", url[:80], exc.code)
        return None
    except Exception as exc:  # network errors are transient
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
    attempts: int = MEDIA_CACHE_ATTEMPTS,
    backoff_base: float = MEDIA_CACHE_BACKOFF_BASE,
) -> str | None:
    """Download media bytes and record them in ``media_cache``.

    Returns the local file path, or None if every attempt failed or the URL was
    already cached.

    RETRIED, because a media fetch is the last chance at these bytes: the CDN
    URL is signed and expires in days, so a transient 403/timeout at scrape
    time permanently loses the media — the post then cannot be enriched, and
    nothing re-fetches it (the cache is filled ONLY here and at scrape time).
    A retry is cheap next to a permanently un-enrichable post.

    The caller MUST account for the return value: a bare ``cache_media_bytes(...)``
    discards the miss and makes a whole run's media loss invisible. Prefer
    :func:`cache_media_urls`, which counts and reports.
    """
    _ensure_media_cache_table(ops)
    existing = local_media_path(ops, media_url)
    if existing:
        return existing

    for attempt in range(1, attempts + 1):
        try:
            downloaded = _download_bytes(media_url)
        except _PermanentFetchError as exc:
            # 4xx — an expired signature never revives. Stop at attempt 1 and
            # say it is permanent, so the count is honest and no wall-clock is
            # wasted. A re-scrape minting a FRESH url is the only cure.
            logger.error(
                "media fetch PERMANENTLY failed (HTTP %s) for %s — %s",
                exc.code,
                media_url[:80],
                exc.diagnosis(),
            )
            return None
        if downloaded is not None:
            data, content_type = downloaded
            base_type = content_type.split(";")[0].strip().lower()
            ext = _EXT_BY_MIME.get(base_type, ".bin")
            if ext == ".bin":
                # An unrecognized content-type still caches, but say so: the
                # .bin extension hides the file from any extension-based audit.
                logger.warning(
                    "media %s: unrecognized content-type %r -> storing as .bin",
                    media_url[:80],
                    content_type,
                )

            # STABLE key: the media id survives re-signing, so the row stays
            # reachable after the CDN signature rotates (~4.5 days).
            cache_key = media_key(media_url)
            dest = (media_dir or POST_MEDIA_DIR) / f"{_safe_name(cache_key)}{ext}"
            _atomic_write(dest, data)

            record_media_cache_row(
                ops, cache_key, str(dest), content_type, dest.stat().st_size, media_url
            )

            logger.info("cached %s → %s", media_url[:80], dest.name)
            return str(dest)

        if attempt < attempts:
            delay = backoff_base * (2 ** (attempt - 1))
            logger.info(
                "media fetch attempt %d/%d failed for %s — retrying in %.1fs",
                attempt,
                attempts,
                media_url[:80],
                delay,
            )
            time.sleep(delay)

    logger.error(
        "media fetch FAILED after %d attempts: %s — the bytes are gone once the"
        " CDN URL expires, so this post cannot be enriched until a re-scrape",
        attempts,
        media_url[:80],
    )
    return None


@dataclass
class MediaCacheReport:
    """Attempted / cached / failed counts for one run's media caching.

    A whole scrape run's media loss has to be VISIBLE. The failure paths here
    are individually logged, but with no aggregate only a census days later
    reveals that a run cached nothing — which is exactly how 790 posts came to
    have no media (ISSUES.md #25).
    """

    attempted: int = 0
    cached: int = 0
    failed: int = 0
    failed_urls: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        """True when every attempted URL cached (or there was nothing to do)."""
        return self.failed == 0

    def summary(self) -> str:
        return (
            f"media cache: {self.cached}/{self.attempted} cached"
            + (f", {self.failed} FAILED" if self.failed else "")
        )


def cache_media_urls(
    ops: SQLiteResource,
    urls: Iterable[str],
    *,
    media_dir: Path | None = None,
) -> MediaCacheReport:
    """Cache every URL once, and return the accounting.

    Deduplicates, skips blanks, and never raises: a failure is counted and
    returned, not swallowed. Callers surface the report loudly.
    """
    report = MediaCacheReport()
    seen: set[str] = set()
    for url in urls:
        if not url or url in seen:
            continue
        seen.add(url)
        report.attempted += 1
        if cache_media_bytes(ops, url, media_dir=media_dir):
            report.cached += 1
        else:
            report.failed += 1
            report.failed_urls.append(url)
    return report



def seed_media_from_file(
    ops: SQLiteResource,
    media_url: str,
    src_path: Path,
    *,
    media_dir: Path | None = None,
    conn: sqlite3.Connection | None = None,
) -> str | None:
    """Seed ``media_cache`` from an existing local file — no download.

    A SEED, not a fetch: copies already-downloaded bytes into the
    ``POST_MEDIA_DIR`` cache keyed by sha256(url) so the cache is
    self-contained (the source checkout may move or disappear). Use for
    local-disk ingestion where the CDN URLs in the metadata are already
    stale/expiring and re-downloading is both wasteful and lossy.

    `conn`, when given, is an already-open connection reused for the lookup and
    the write. A batch caller MUST pass one: this function otherwise opens up to
    three connections per URL (~23 ms each on Windows, dominated by the WAL
    pragma), which is what made a 24,000-URL seed pass take hours rather than
    minutes. The caller owns a supplied connection and it is left open.

    Idempotent: returns the cached path when the URL is already cached.
    Returns None when the source file is missing (caller decides severity).
    """
    _ensure_media_cache_table(ops, conn)
    existing = local_media_path(ops, media_url, conn=conn)
    if existing:
        return existing
    if not src_path.exists():
        logger.warning("seed: source missing for %s: %s", media_url[:80], src_path)
        return None

    cache_key = media_key(media_url)
    ext = src_path.suffix.lower()
    content_type = _CONTENT_TYPE_BY_EXT.get(ext, "application/octet-stream")
    dest = (media_dir or POST_MEDIA_DIR) / f"{_safe_name(cache_key)}{ext}"
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".tmp")
    shutil.copyfile(src_path, tmp)
    os.replace(tmp, dest)

    record_media_cache_row(
        ops,
        cache_key,
        str(dest),
        content_type,
        dest.stat().st_size,
        media_url,
        conn=conn,
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
        local = local_media_path(ops, url)
        if local is None:
            continue
        if not include_video and is_video_path(local):
            continue
        abs_path = str(Path(local).resolve())
        if abs_path not in seen and Path(abs_path).exists():
            seen.add(abs_path)
            resolved.append(abs_path)
    return resolved
