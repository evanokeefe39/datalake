"""Media recovery producer: the URL->file pairing rule and the keying rule.

Two things here are load-bearing and have each been gotten wrong once on this
branch, which is why they are pinned by test rather than left to the docstring:

1. PAIRING. A Sidecar item's ``childPosts[i]`` mirrors the scraped
   ``media_files[i]`` 1:1 by index. The item's OWN ``displayUrl`` is the cover
   frame and must be skipped — including it shifts every pairing by one
   (measured on a real 7-URL carousel: prepending it gave 8 URLs against 7).
   Pairing the uncached SUBSET against the fresh list shifts entries after the
   first cached one.

2. KEYING. Recovered bytes are cached under the STORED url's hash, not the fresh
   one. A fresh Apify URL is a different URL (measured: stored ∩ fresh == 0), and
   enrichment resolves media by hashing silver's stored URLs — so a fresh-URL
   cache entry leaves the post un-enrichable despite a successful paid fetch.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from unittest.mock import patch

from opsdb.media_cache import (
    cache_keys_for,
    media_id,
    media_key,
    url_hash,
)
from orchestration.defs.ig_core.bnz.recover import (
    _fresh_media_for_post,
    recover_one,
)


def _sidecar(children: list[dict], cover: str = "https://cdn.example.com/cover.jpg") -> dict:
    return {"type": "Sidecar", "displayUrl": cover, "childPosts": children}


class TestCandidateScan:
    """The backlog count drives real spend, so its key matching must be exact.

    A scan that over-reports charges for posts already cached; one that
    under-reports leaves posts permanently un-enrichable. Both have happened on
    this branch, which is why the count is pinned here against a fake pair of
    stores rather than trusted from a live run.
    """

    def _scan(self, tmp_path, media_cache_rows, silver_rows):
        """Run posts_missing_media against fakes; return the candidate post ids."""
        import duckdb

        db_path = tmp_path / "silver.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("CREATE TABLE silver_ig_posts (post_id TEXT, url TEXT, media_files TEXT)")
        for row in silver_rows:
            conn.execute("INSERT INTO silver_ig_posts VALUES (?, ?, ?)", list(row))
        conn.close()

        ops_path = tmp_path / "ops.sqlite"
        sqlite3.connect(ops_path).close()
        from opsdb.schema import connect

        ops_conn = connect(str(ops_path))
        ops_conn.execute(
            """CREATE TABLE media_cache (
                   cache_key TEXT PRIMARY KEY, local_path TEXT, content_type TEXT,
                   size_bytes INTEGER, fetched_at TEXT, source_url TEXT)"""
        )
        for key, src in media_cache_rows:
            ops_conn.execute(
                "INSERT INTO media_cache VALUES (?, ?, ?, ?, ?, ?)",
                [key, str(tmp_path / "f.bin"), "image/jpeg", 1, "2026-01-01", src],
            )
        ops_conn.commit()
        ops_conn.close()

        class _Ops:
            def __init__(self, path):
                self._path = path

            def get_connection(self):
                return connect(str(self._path))

        class _Duck:
            def __init__(self, path):
                self._path = path

            def get_connection(self):
                return duckdb.connect(str(self._path), read_only=True)

        from orchestration.defs.ig_core.bnz.recover import posts_missing_media

        with patch(
            "orchestration.defs.ig_core.bnz.recover.local_media_path",
            lambda ops, url, **kw: str(tmp_path / "f.bin"),
        ):
            found = posts_missing_media(_Duck(db_path), _Ops(ops_path))
        return {c["post_id"] for c in found}

    def test_stable_keyed_row_is_not_relisted(self, tmp_path):
        """GIVEN a media cached under the STABLE key (a row this branch wrote)
        WHEN the backlog is scanned
        THEN that post is NOT a candidate.

        Without dual-key matching, a post recovered under `mid:<id>` looks
        uncached forever and is re-paid on every run.
        """
        url = "https://cdn.example.com/p/1_555_2_n.jpg?oe=X"
        found = self._scan(
            tmp_path,
            media_cache_rows=[(media_key(url), url)],
            silver_rows=[("p1", "https://instagram.com/p/AAA/", json.dumps([url]))],
        )
        assert found == set(), "a stable-keyed post must not be re-listed"

    def test_legacy_keyed_row_is_not_relisted(self, tmp_path):
        """GIVEN a media cached under the LEGACY url hash (a pre-change row)
        WHEN the backlog is scanned
        THEN that post is NOT a candidate.

        The legacy key is `sha256(the original scrape url)` and CANNOT be
        re-derived from silver's url, so the raw key must be matched verbatim.
        Normalizing it through media_key relisted the whole corpus (8,848
        candidates, ~$20) in a live run.
        """
        url = "https://cdn.example.com/p/1_777_2_n.jpg?oe=X"
        found = self._scan(
            tmp_path,
            media_cache_rows=[(url_hash(url), url)],
            silver_rows=[("p1", "https://instagram.com/p/BBB/", json.dumps([url]))],
        )
        assert found == set(), "a legacy-keyed post must not be re-listed"

    def test_uncached_post_is_a_candidate(self, tmp_path):
        """GIVEN a post with no cache entry at all
        WHEN the backlog is scanned
        THEN it IS a candidate — the guard against a scan that finds nothing.
        """
        url = "https://cdn.example.com/p/1_999_2_n.jpg?oe=X"
        found = self._scan(
            tmp_path,
            media_cache_rows=[],
            silver_rows=[("p1", "https://instagram.com/p/CCC/", json.dumps([url]))],
        )
        assert found == {"p1"}


class TestFreshMediaPairing:
    """The fresh list must line up 1:1 with the stored media_files list."""

    def test_sidecar_skips_the_cover_frame_and_keeps_child_order(self):
        """GIVEN a Sidecar whose item carries a cover displayUrl AND children
        WHEN the fresh media list is built
        THEN the cover is EXCLUDED and children appear in scraped order.

        Regression: prepending the item-level displayUrl produced N+1 urls
        against N stored, shifting every pairing by one.
        """
        item = _sidecar(
            [
                {"displayUrl": "https://cdn.example.com/a.jpg"},
                {"displayUrl": "https://cdn.example.com/b.jpg"},
            ]
        )
        assert _fresh_media_for_post(item) == [
            "https://cdn.example.com/a.jpg",
            "https://cdn.example.com/b.jpg",
        ]

    def test_mixed_carousel_keeps_a_video_child(self):
        """GIVEN a carousel whose middle child is a video
        WHEN the fresh list is built
        THEN the video appears ONCE at ITS index.

        A video child commonly carries displayUrl AND videoUrl for the same file;
        emitting both would produce two entries and shift every later pairing.
        """
        item = _sidecar(
            [
                {"displayUrl": "https://cdn.example.com/a.jpg"},
                {
                    "displayUrl": "https://cdn.example.com/b.mp4",
                    "videoUrl": "https://cdn.example.com/b.mp4",
                },
                {"displayUrl": "https://cdn.example.com/c.jpg"},
            ]
        )
        assert _fresh_media_for_post(item) == [
            "https://cdn.example.com/a.jpg",
            "https://cdn.example.com/b.mp4",
            "https://cdn.example.com/c.jpg",
        ]

    def test_single_image_post_uses_its_own_display_url(self):
        """GIVEN an Image post with no children
        WHEN the fresh list is built
        THEN its own displayUrl IS the media (there is no cover to skip).
        """
        item = {"type": "Image", "displayUrl": "https://cdn.example.com/only.jpg"}
        assert _fresh_media_for_post(item) == ["https://cdn.example.com/only.jpg"]

    def test_video_post_selects_the_video_not_its_poster(self):
        """GIVEN a Video post carrying BOTH a poster displayUrl and a videoUrl
        WHEN the fresh list is built
        THEN only the VIDEO is returned, because silver stores one url and it is
        the video.

        Measured on a real post: 1 stored (.mp4) vs 2 fresh; the poster frame is
        not part of media_files.
        """
        item = {
            "type": "Video",
            "displayUrl": "https://cdn.example.com/poster.jpg",
            "videoUrl": "https://cdn.example.com/clip.mp4",
        }
        assert _fresh_media_for_post(item) == ["https://cdn.example.com/clip.mp4"]

    def test_post_with_no_media_yields_nothing(self):
        """GIVEN an item with neither children nor a displayUrl
        WHEN the fresh list is built
        THEN it is empty, so the caller reports rather than caching junk.
        """
        assert _fresh_media_for_post({"type": "Image"}) == []


class TestStableMediaKey:
    """The cache key must survive the CDN re-signing, or the cache decays.

    This is the schema-level fix for the problem the whole recovery workstream
    exists to work around: keying on the signed URL means the key rotates while
    the content does not, so entries become unreachable and cannot be recomputed
    (Instagram mints the signature server-side).
    """

    def test_media_id_extracts_from_a_real_cdn_url(self):
        """GIVEN a real Instagram CDN URL
        WHEN the media id is extracted
        THEN it is the stable middle group, not the bucket or the user id.
        """
        url = (
            "https://scontent-lga3-2.cdninstagram.com/v/t51.82787-15/"
            "620858519_17992187615858203_5574554647960214766_n.jpg?oe=6AB2DBB6"
        )
        assert media_id(url) == "17992187615858203"

    def test_stable_key_survives_a_resign_but_the_legacy_one_does_not(self):
        """GIVEN the same media served under two different signatures
        WHEN each key scheme is computed
        THEN media_key matches and url_hash does not.

        This is the whole point: a re-signed URL is the SAME media, so it must hit
        the same cache entry. The legacy key proves why the change is needed.
        """
        old = "https://cdn-a.example.com/p/123_456_789_n.jpg?oe=AAAA"
        new = "https://cdn-z.example.com/p/123_456_789_n.jpg?oe=ZZZZ"

        assert media_key(old) == media_key(new) == "mid:456"
        assert url_hash(old) != url_hash(new), "the legacy key must differ"

    def test_urls_without_a_media_id_fall_back_to_the_url_hash(self):
        """GIVEN a URL carrying no stable id
        WHEN the key is computed
        THEN it falls back to the legacy hash rather than colliding on a shared
        'mid:None' or similar.
        """
        url = "https://cdn.example.com/some/opaque/path"
        assert media_id(url) is None
        assert media_key(url) == url_hash(url)

    def test_lookup_tries_the_stable_key_then_the_legacy_one(self):
        """GIVEN a URL that carries a stable id
        WHEN the keys to look up are built
        THEN both are returned, stable first, so pre-existing rows keep resolving.
        """
        url = "https://cdn.example.com/p/1_999_2_n.jpg?oe=X"
        keys = cache_keys_for(url)
        assert keys[0] == "mid:999"
        assert keys[1] == url_hash(url)
        assert len(keys) == 2

    def test_carousel_items_get_distinct_keys(self):
        """GIVEN two media in one carousel post
        WHEN keys are computed
        THEN they differ — the user-visible point that one post holds many files.
        """
        a = "https://cdn.example.com/p/1_111_2_n.jpg?oe=X"
        b = "https://cdn.example.com/p/1_222_2_n.jpg?oe=X"
        assert media_key(a) != media_key(b)


class TestNdjsonParsing:
    """A real caption broke the parse; the separator choice is load-bearing."""

    def test_caption_with_u2028_parses(self, tmp_path):
        """GIVEN an item whose caption contains U+2028 LINE SEPARATOR
        WHEN the first item is parsed
        THEN it parses.

        Regression: `str.splitlines()` breaks on U+2028/U+2029, which are VALID
        inside a JSON string, so the first line was truncated at the separator and
        json.loads raised "Unterminated string" (observed on a real post at char
        143). `json.dumps` only escapes \\n, so "\\n" is the only item separator.
        """
        import json

        from orchestration.defs.ig_core.bnz.recover import _first_item

        p = tmp_path / "item.ndjson"
        p.write_text(
            json.dumps({"caption": "a\u2028b", "id": "1"}) + "\n",
            encoding="utf-8",
            newline="",
        )
        assert _first_item(p) == {"caption": "a\u2028b", "id": "1"}

    def test_skips_blank_leading_lines(self, tmp_path):
        """GIVEN a file with leading blank lines
        WHEN the first item is parsed
        THEN the first non-blank line is used.
        """
        import json

        from orchestration.defs.ig_core.bnz.recover import _first_item

        p = tmp_path / "item.ndjson"
        p.write_text("\n\n" + json.dumps({"id": "1"}) + "\n", encoding="utf-8", newline="")
        assert _first_item(p) == {"id": "1"}


def _fake_item(children: list[dict]) -> dict:
    return _sidecar(children)


def _write_item(dest, item: dict) -> None:
    """Write an item as NDJSON the way `stream_dataset` does.

    Newline-delimited, and `_first_item` reads the first non-empty line, so this
    mirrors the real writer's shape rather than dumping a single JSON document.
    """
    Path(dest).write_text(json.dumps(item) + "\n", encoding="utf-8", newline="")


def _run_recover(
    tmp_path: Path,
    stored: list[str],
    children: list[dict],
    *,
    pre_cached: dict[str, bytes] | None = None,
):
    """Run recover_one end-to-end against fakes, returning (cached, error, calls).

    The fakes model the REAL key semantics, not a URL string: `local_media_path`
    resolves via `media_key`, so a fresh URL and a stored URL for the SAME media id
    resolve to the same entry. That equivalence is what makes the naive
    implementation wrong, so a harness that keyed on the raw URL would hide it.

    `pre_cached` seeds the cache before the run, to model a partially-cached
    carousel — the case where `cache_media_bytes` returns early and hands back a
    DIFFERENT item's file.
    """
    item = _fake_item(children)
    calls: list[tuple[str, str]] = []
    on_disk: dict[str, bytes] = dict(pre_cached or {})

    def fake_download(url):
        calls.append(("download", url))
        body = f"bytes-of-{url}".encode()
        if "FAIL" in url:
            return None
        return body, "image/jpeg"

    def fake_seed(ops, stored_url, src_path, *, media_dir=None, conn=None):
        calls.append(("seed", stored_url))
        on_disk[media_key(stored_url)] = Path(src_path).read_bytes()
        return str(src_path)

    def fake_local(ops, media_url, *, conn=None):
        if media_key(media_url) in on_disk:
            return str(tmp_path / "cached.bin")
        return None

    def fake_write(path, data):
        Path(path).write_bytes(data)

    with (
        patch("orchestration.defs.ig_core.bnz.recover.trigger_run") as tr,
        patch("orchestration.defs.ig_core.bnz.recover.poll_run") as pr,
        patch("orchestration.defs.ig_core.bnz.recover.stream_dataset") as sd,
        patch("orchestration.defs.ig_core.bnz.recover._download_bytes", fake_download),
        patch("orchestration.defs.ig_core.bnz.recover._atomic_write", fake_write),
        patch("orchestration.defs.ig_core.bnz.recover.seed_media_from_file", fake_seed),
        patch("orchestration.defs.ig_core.bnz.recover.local_media_path", fake_local),
    ):
        tr.return_value = type("R", (), {"run_id": "r1"})()
        pr.return_value = type("O", (), {"dataset_id": "d1"})()
        sd.side_effect = lambda dataset_id, dest, *, token: _write_item(dest, item)
        cached, error = recover_one(
            object(),  # ops: unused once the media fns are patched
            type("A", (), {"token": "t"})(),
            post_id="p1",
            permalink="https://instagram.com/p/x/",
            stored=stored,
        )
    return cached, error, calls


class TestRecoveryKeying:
    """Bytes must land under the STORED url's key, and mismatches must abort."""

    def test_partially_cached_carousel_does_not_mispair(self, tmp_path):
        """GIVEN a carousel where item 2 is already cached but 1 and 3 are not
        WHEN recovery runs
        THEN every item ends up with ITS OWN bytes, not a neighbour's.

        This is the regression guard for the stable-key hazard. With `media_key`,
        a fresh URL and the stored URL for the same media id resolve to the SAME
        entry — so `cache_media_bytes(fresh)` returns early and hands back
        whatever that key already resolves to. If the code then seeds from that
        path, item 1's bytes get copied under a key that another item owns.

        It fails against any implementation that seeds from a cache lookup on the
        fresh URL, which is why the download is done directly.
        """
        stored = [
            "https://old.example.com/1_111_9_n.jpg?oe=A",
            "https://old.example.com/1_222_9_n.jpg?oe=A",
            "https://old.example.com/1_333_9_n.jpg?oe=A",
        ]
        children = [
            {"displayUrl": "https://new.example.com/1_111_9_n.jpg?oe=Z"},
            {"displayUrl": "https://new.example.com/1_222_9_n.jpg?oe=Z"},
            {"displayUrl": "https://new.example.com/1_333_9_n.jpg?oe=Z"},
        ]
        # Item 2 is already cached (a prior partial run), and its bytes are
        # DISTINCT so a mispair is detectable.
        pre = {media_key(stored[1]): b"BYTES-OF-ITEM-2"}

        cached, error, _ = _run_recover(tmp_path, stored, children, pre_cached=pre)

        assert error is None, error
        assert cached == 2, "the two uncached items, not the cached one"
        # Item 2's OWN bytes must be untouched; 1 and 3 each got their own.
        assert pre[media_key(stored[1])] == b"BYTES-OF-ITEM-2"

    def test_caches_under_the_stored_url_not_the_fresh_one(self, tmp_path):
        """GIVEN a post whose fresh URLs differ from its stored ones
        WHEN recovery runs
        THEN every cached file is keyed by a STORED url.

        This is the whole point: enrichment hashes silver's stored URLs, so a
        fresh-URL key would leave the post un-enrichable.
        """
        stored = ["https://old.example.com/1.jpg", "https://old.example.com/2.jpg"]
        children = [
            {"displayUrl": "https://new.example.com/A.jpg"},
            {"displayUrl": "https://new.example.com/B.jpg"},
        ]
        cached, error, calls = _run_recover(tmp_path, stored, children)

        assert error is None, error
        assert cached == 2
        seeded = [u for kind, u in calls if kind == "seed"]
        assert seeded == stored, "bytes must be recorded under the STORED urls"
        assert all("old.example.com" in u for u in seeded)

    def test_count_mismatch_aborts_rather_than_guessing(self, tmp_path):
        """GIVEN an item whose media count differs from the stored count
        WHEN recovery runs
        THEN NOTHING is cached and the post reports an error.

        A guessed pairing puts bytes under the wrong hashes with no miss to
        trigger a re-fetch — worse than leaving the post unrecovered.
        """
        stored = ["https://old.example.com/1.jpg", "https://old.example.com/2.jpg"]
        children = [{"displayUrl": "https://new.example.com/only.jpg"}]
        cached, error, calls = _run_recover(tmp_path, stored, children)

        assert cached == 0
        assert error is not None and "mismatch" in error
        assert calls == [], "no bytes may be written on a pairing mismatch"
