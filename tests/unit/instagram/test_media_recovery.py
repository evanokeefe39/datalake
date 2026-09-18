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
from opsdb.media_recovery import UNRECOVERABLE_POST_GONE
from orchestration.defs.ig_core.bnz.recover import (
    _all_items,
    _fresh_media_for_post,
    _item_shortcode,
    _shortcode,
    recover_batch,
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

    def _scan(self, tmp_path, media_cache_rows, silver_rows, exhausted=()):
        """Run posts_missing_media against fakes; return the candidate post ids."""
        import duckdb

        tmp_path.mkdir(parents=True, exist_ok=True)

        db_path = tmp_path / "silver.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("CREATE TABLE silver_ig_posts (post_id TEXT, url TEXT, media_files TEXT)")
        for row in silver_rows:
            conn.execute("INSERT INTO silver_ig_posts VALUES (?, ?, ?)", list(row))
        conn.close()

        ops_path = tmp_path / "ops.sqlite"
        sqlite3.connect(ops_path).close()
        from opsdb.schema import connect, sqlite_ddl

        ops_conn = connect(str(ops_path))
        # Catalog DDL, not a hand-written column list: a copied list drifts from
        # the schema silently, which is exactly how a column rename becomes a
        # partial write nobody notices.
        ops_conn.execute(sqlite_ddl("media_cache"))
        for key, src in media_cache_rows:
            ops_conn.execute(
                "INSERT INTO media_cache VALUES (?, ?, ?, ?, ?, ?)",
                [key, str(tmp_path / "f.bin"), "image/jpeg", 1, "2026-01-01", src],
            )
        ops_conn.commit()
        ops_conn.close()

        if exhausted:
            from opsdb.media_recovery import (
                UNRECOVERABLE_POST_GONE,
                record_exhausted,
            )

            class _Rec:
                def get_connection(self):
                    return connect(str(ops_path))

            for pid in exhausted:
                record_exhausted(_Rec(), pid, UNRECOVERABLE_POST_GONE)

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


class TestExhaustedPosts:
    """An unrecoverable post must not be re-paid on every run.

    The recovery mechanism is STANDING — it runs repeatedly — so a verdict that
    is only logged is a verdict that is forgotten. The pilot measured roughly a
    third of candidates as permanently unrecoverable (deleted/private posts
    return no media), so without durable memory a meaningful slice of every pass
    is spend on posts that can never succeed.
    """

    def _store(self, tmp_path, post_ids=()):
        from opsdb.schema import connect

        path = tmp_path / "ops.sqlite"
        sqlite3.connect(path).close()
        conn = connect(str(path))
        conn.close()

        class _Ops:
            def get_connection(self):
                return connect(str(path))

        ops = _Ops()
        if post_ids:
            from opsdb.media_recovery import UNRECOVERABLE_POST_GONE, record_exhausted

            for pid in post_ids:
                record_exhausted(ops, pid, UNRECOVERABLE_POST_GONE)
        return ops

    def test_recorded_post_is_read_back(self, tmp_path):
        """GIVEN a post recorded as unrecoverable
        WHEN the exhausted set is read
        THEN it contains that post.

        Round-trip first: every other guarantee here depends on the write landing.
        """
        from opsdb.media_recovery import exhausted_post_ids

        ops = self._store(tmp_path, post_ids=["p1", "p2"])
        assert exhausted_post_ids(ops) == {"p1", "p2"}

    def test_recording_is_idempotent(self, tmp_path):
        """GIVEN a post already recorded
        WHEN it is recorded again (a re-run)
        THEN no error is raised and it appears once.
        """
        from opsdb.media_recovery import (
            UNRECOVERABLE_POST_GONE,
            exhausted_post_ids,
            record_exhausted,
        )

        ops = self._store(tmp_path)
        record_exhausted(ops, "p1", UNRECOVERABLE_POST_GONE)
        record_exhausted(ops, "p1", UNRECOVERABLE_POST_GONE)
        assert exhausted_post_ids(ops) == {"p1"}

    def test_absent_table_reads_as_empty(self, tmp_path):
        """GIVEN a database where nothing has been recorded
        WHEN the exhausted set is read
        THEN it is empty, not an error.

        A scan on a fresh database must behave exactly as it did before this
        table existed.
        """
        from opsdb.media_recovery import exhausted_post_ids

        ops = self._store(tmp_path)
        assert exhausted_post_ids(ops) == set()

    def test_unknown_reason_raises(self, tmp_path):
        """GIVEN a reason that is not a declared permanent verdict
        WHEN it is recorded
        THEN a ValueError is raised and nothing is stored.

        This is the guard the table's one-way semantics rest on: without it,
        `record_exhausted(ops, pid, "timeout")` would permanently condemn a post
        the mechanism could still recover on a later run.
        """
        import pytest
        from opsdb.media_recovery import exhausted_post_ids, record_exhausted

        ops = self._store(tmp_path)
        with pytest.raises(ValueError, match="unknown exhaustion reason"):
            record_exhausted(ops, "p1", "timeout")
        assert exhausted_post_ids(ops) == set(), "nothing may be stored"

    def test_exhausted_post_is_excluded_from_the_scan(self, tmp_path):
        """GIVEN a post with no cached media that a prior run marked exhausted
        WHEN the backlog is scanned
        THEN it is NOT a candidate — so it is not re-paid.

        This is the guarantee: without it, every unrecoverable post is
        re-selected on every run forever, at the full per-fetch price. The
        counter-case (same post, no exhaustion row) is asserted in
        TestCandidateScan.test_uncached_post_is_a_candidate.
        """
        url = "https://cdn.example.com/p/1_321_2_n.jpg?oe=X"
        silver = [("p1", "https://instagram.com/p/X/", json.dumps([url]))]

        scanned = TestCandidateScan()._scan(
            tmp_path / "before", media_cache_rows=[], silver_rows=silver
        )
        assert scanned == {"p1"}, "sanity: without the verdict it IS a candidate"

        found = TestCandidateScan()._scan(
            tmp_path / "after",
            media_cache_rows=[],
            silver_rows=silver,
            exhausted=["p1"],
        )
        assert found == set(), "an exhausted post must not be re-listed"


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


class TestBatchAttribution:
    """A batched run returns items in no guaranteed order, so each must be routed
    to ITS post by shortcode. Positional pairing would cache one post's media under
    another's keys — silently wrong, and nothing would re-fetch it.
    """

    def test_shortcode_read_from_the_dedicated_field(self):
        """GIVEN an item carrying `shortCode`
        WHEN its shortcode is read
        THEN the field is used, not the URL.
        """
        assert _item_shortcode({"shortCode": "ABC123", "url": "https://x/p/ZZZ/"}) == "ABC123"

    def test_shortcode_falls_back_to_the_permalink(self):
        """GIVEN an item with no `shortCode` field
        WHEN its shortcode is read
        THEN it comes from the permalink, so attribution survives a schema change
        that drops the field.
        """
        assert _item_shortcode({"url": "https://www.instagram.com/p/ABC123/"}) == "ABC123"

    def test_shortcode_matches_the_permalink_shape_we_request(self):
        """GIVEN the permalink we send and the URL Apify returns
        WHEN both are reduced to a shortcode
        THEN they agree.

        Verified against a real 2-URL batch: every returned item carried `url` in
        this exact form and `shortCode` matching it.
        """
        requested = "https://www.instagram.com/p/CBL8httj7aK/"
        returned = "https://www.instagram.com/p/CBL8httj7aK/"
        assert _shortcode(requested) == _shortcode(returned) == "CBL8httj7aK"

    def test_all_items_skips_a_malformed_line_without_losing_the_rest(self, tmp_path):
        """GIVEN an NDJSON file where one line is not valid JSON
        WHEN every item is parsed
        THEN the valid items survive.

        One corrupt line must not discard the media recovered for every other post
        in the batch.
        """
        p = tmp_path / "batch.ndjson"
        p.write_text(
            '{"shortCode": "A"}\n'
            "this is not json\n"
            '{"shortCode": "B"}\n',
            encoding="utf-8",
            newline="",
        )
        codes = [_item_shortcode(i) for i in _all_items(p)]
        assert codes == ["A", "B"]

    def test_each_item_is_cached_under_its_own_posts_keys(self, tmp_path):
        """GIVEN a batch of two posts whose items come back in the dataset
        WHEN recover_batch routes them
        THEN each post's stored URL holds ITS OWN item's bytes, not the other's.

        This is the guard for the batched path's one silent failure mode. A
        batched run returns items in no guaranteed order, so routing by position
        would write post A's bytes under post B's keys — a mispair that resolves
        cleanly and sends the wrong media to the model, with nothing to re-fetch
        it. Byte-level assertion, because a file-count assertion cannot tell the
        two apart.
        """
        a_stored = ["https://old.example.com/1_111_9_n.jpg?oe=A"]
        b_stored = ["https://old.example.com/1_222_9_n.jpg?oe=B"]
        a_fresh = "https://new.example.com/1_111_9_n.jpg?oe=Z"
        b_fresh = "https://new.example.com/1_222_9_n.jpg?oe=Z"
        candidates = [
            {"post_id": "pa", "url": "https://instagram.com/p/AAAA/", "stored": a_stored},
            {"post_id": "pb", "url": "https://instagram.com/p/BBBB/", "stored": b_stored},
        ]
        # Items returned in REVERSE order, and each tagged with its own shortCode.
        items = [
            {"shortCode": "BBBB", "url": "https://instagram.com/p/BBBB/", "displayUrl": b_fresh},
            {"shortCode": "AAAA", "url": "https://instagram.com/p/AAAA/", "displayUrl": a_fresh},
        ]
        on_disk: dict[str, bytes] = {}

        def fake_download(url):
            return f"bytes-of-{url}".encode(), "image/jpeg"

        def fake_seed(ops, stored_url, src_path, *, media_dir=None, conn=None):
            on_disk[media_key(stored_url)] = Path(src_path).read_bytes()
            return str(src_path)

        def fake_local(ops, media_url, *, conn=None):
            return str(tmp_path / "c.bin") if media_key(media_url) in on_disk else None

        with (
            patch("orchestration.defs.ig_core.bnz.recover.trigger_run") as tr,
            patch("orchestration.defs.ig_core.bnz.recover.poll_run") as pr,
            patch("orchestration.defs.ig_core.bnz.recover.stream_dataset") as sd,
            patch("orchestration.defs.ig_core.bnz.recover._download_bytes", fake_download),
            patch("orchestration.defs.ig_core.bnz.recover._atomic_write",
                  lambda p, d: Path(p).write_bytes(d)),
            patch("orchestration.defs.ig_core.bnz.recover.seed_media_from_file", fake_seed),
            patch("orchestration.defs.ig_core.bnz.recover.local_media_path", fake_local),
        ):
            tr.return_value = type("R", (), {"run_id": "r1"})()
            pr.return_value = type("O", (), {"dataset_id": "d1"})()
            sd.side_effect = lambda dataset_id, dest, *, token: _write_items(dest, items)
            recovered, cached, failed = recover_batch(
                object(), type("A", (), {"token": "t"})(), candidates=candidates
            )

        assert (recovered, cached, failed) == (2, 2, [])
        assert on_disk[media_key(a_stored[0])] == f"bytes-of-{a_fresh}".encode()
        assert on_disk[media_key(b_stored[0])] == f"bytes-of-{b_fresh}".encode()

    def test_an_item_for_a_post_outside_the_chunk_is_ignored(self, tmp_path):
        """GIVEN a returned item whose shortcode matches no candidate
        WHEN recover_batch routes
        THEN it is skipped, not attributed to an arbitrary post — and the drop is
        LOUD.

        A silently dropped item is paid-for media that reached no post: the
        candidate is reported failed, retried, and re-paid on every run, never
        recorded or reported. So the log must name the unmatched shortcode.
        """
        candidates = [
            {"post_id": "pa", "url": "https://instagram.com/p/AAAA/",
             "stored": ["https://old.example.com/1_111_9_n.jpg?oe=A"]},
        ]
        items = [
            {"shortCode": "ZZZZ", "url": "https://instagram.com/p/ZZZZ/",
             "displayUrl": "https://new.example.com/other.jpg"},
        ]
        on_disk: dict[str, bytes] = {}

        with (
            patch("orchestration.defs.ig_core.bnz.recover.trigger_run") as tr,
            patch("orchestration.defs.ig_core.bnz.recover.poll_run") as pr,
            patch("orchestration.defs.ig_core.bnz.recover.stream_dataset") as sd,
            patch("orchestration.defs.ig_core.bnz.recover.logger") as lg,
            patch("orchestration.defs.ig_core.bnz.recover._download_bytes",
                  lambda u: (b"x", "image/jpeg")),
            patch("orchestration.defs.ig_core.bnz.recover._atomic_write",
                  lambda p, d: Path(p).write_bytes(d)),
            patch("orchestration.defs.ig_core.bnz.recover.seed_media_from_file",
                  lambda ops, u, src, **kw: on_disk.setdefault(u, b"y") and str(src)),
            patch("orchestration.defs.ig_core.bnz.recover.local_media_path",
                  lambda ops, u, **kw: on_disk.get(u)),
        ):
            tr.return_value = type("R", (), {"run_id": "r1"})()
            pr.return_value = type("O", (), {"dataset_id": "d1"})()
            sd.side_effect = lambda dataset_id, dest, *, token: _write_items(dest, items)
            recovered, cached, failed = recover_batch(
                object(), type("A", (), {"token": "t"})(), candidates=candidates
            )

        assert recovered == 0
        assert on_disk == {}, "nothing may be written for an unmatched item"
        assert failed == ["https://instagram.com/p/AAAA/"], (
            "the post must still be reported, so the drop is visible not silent"
        )
        loud = [c for c in lg.error.call_args_list if "matched no candidate" in str(c)]
        assert loud, "an unmatched item must be logged, not silently dropped"
        assert "ZZZZ" in str(loud[0]), "the log must name the unmatched shortcode"

    def test_a_chunk_returning_no_items_is_retryable_not_a_verdict(self, tmp_path):
        """GIVEN a run that returns NO items for its chunk
        WHEN recover_batch processes it
        THEN the posts are reported failed and NO exhaustion verdict is written.

        A missing item means the FETCH failed, not that the media is gone. The
        verdict table is one-way, so recording here would permanently condemn
        posts the mechanism could still recover — and a batch mapping bug would
        write verdicts for every post in the chunk.
        """
        candidates = [
            {"post_id": "pa", "url": "https://instagram.com/p/AAAA/",
             "stored": ["https://old.example.com/1_111_9_n.jpg?oe=A"]},
        ]
        verdicts: list[str] = []

        with (
            patch("orchestration.defs.ig_core.bnz.recover.trigger_run") as tr,
            patch("orchestration.defs.ig_core.bnz.recover.poll_run") as pr,
            patch("orchestration.defs.ig_core.bnz.recover.stream_dataset") as sd,
            patch("orchestration.defs.ig_core.bnz.recover.record_exhausted",
                  lambda ops, pid, reason, **kw: verdicts.append(pid)),
        ):
            tr.return_value = type("R", (), {"run_id": "r1"})()
            pr.return_value = type("O", (), {"dataset_id": "d1"})()
            sd.side_effect = lambda dataset_id, dest, *, token: _write_items(dest, [])
            recovered, cached, failed = recover_batch(
                object(), type("A", (), {"token": "t"})(), candidates=candidates
            )

        assert recovered == 0
        assert failed == ["https://instagram.com/p/AAAA/"]
        assert verdicts == [], "a missing item must stay retryable"


def _write_items(dest, items: list[dict]) -> int:
    """Write SEVERAL items as NDJSON, as a batched `stream_dataset` does.

    Returns the count, mirroring the real writer's contract.
    """
    Path(dest).write_text(
        "".join(json.dumps(i) + "\n" for i in items), encoding="utf-8", newline=""
    )
    return len(items)


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
    no_media: bool = False,
    error_item: dict | None = None,
):
    """Run recover_one end-to-end against fakes.

    Returns (cached, error, calls, on_disk). `on_disk` maps each cache key to the
    BYTES written under it — that is what makes the mispair assertions real. A
    harness returning only call names cannot distinguish item 1 receiving its own
    bytes from receiving item 2's, which is the entire bug being guarded.

    The fakes model the REAL key semantics, not a URL string: `local_media_path`
    resolves via `media_key`, so a fresh URL and a stored URL for the SAME media id
    resolve to the same entry. That equivalence is what makes the naive
    implementation wrong, so a harness that keyed on the raw URL would hide it.

    `pre_cached` seeds the cache before the run, to model a partially-cached
    carousel — the case where a fresh URL resolves to a DIFFERENT item's file.

    `no_media` makes the fetched item report the post is GONE — the shape Apify
    returns for a deleted post (an error item, not an item with empty media).
    `error_item` builds an arbitrary error item, so a caller can model a
    temporary error such as `restricted_page`.
    """
    item = _fake_item(children)
    if no_media:
        item = {
            "error": "not_found",
            "errorDescription": "Post does not exist",
            "url": "https://instagram.com/p/x/",
        }
    if error_item is not None:
        item = {
            "errorDescription": "Restricted access, only partial data available",
            "url": "https://instagram.com/p/x/",
            **error_item,
        }
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
        # The exhaustion recorder writes durable ops state; a fake keeps the unit
        # test off the live database. `calls` records it so a test can assert the
        # verdict was recorded (and, for a curable failure, was NOT).
        patch(
            "orchestration.defs.ig_core.bnz.recover.record_exhausted",
            lambda ops, pid, reason, **kw: calls.append(("exhausted", f"{pid}:{reason}")),
        ),
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
    return cached, error, calls, on_disk


class TestRecoveryKeying:
    """Bytes must land under the STORED url's key, and mismatches must abort."""

    def test_partially_cached_carousel_does_not_mispair(self, tmp_path):
        """GIVEN a carousel where item 2 is already cached but 1 and 3 are not
        WHEN recovery runs
        THEN each of 1 and 3 holds ITS OWN downloaded bytes.

        The assertion is on BYTES, not on file count: the pre-fix code satisfied
        "3 files exist" while item 1 could hold item 2's bytes. With `media_key`,
        a fresh URL and the stored URL for the same media id resolve to the SAME
        entry, so `cache_media_bytes(fresh)` returns early and hands back
        whatever that key already resolves to — seeding from that path is the
        mispair. The download must therefore be unconditional.
        """
        stored = [
            "https://old.example.com/1_111_9_n.jpg?oe=A",
            "https://old.example.com/1_222_9_n.jpg?oe=A",
            "https://old.example.com/1_333_9_n.jpg?oe=A",
        ]
        fresh = [
            "https://new.example.com/1_111_9_n.jpg?oe=Z",
            "https://new.example.com/1_222_9_n.jpg?oe=Z",
            "https://new.example.com/1_333_9_n.jpg?oe=Z",
        ]
        children = [{"displayUrl": u} for u in fresh]
        pre = {media_key(stored[1]): b"BYTES-OF-ITEM-2"}

        _, error, _, on_disk = _run_recover(
            tmp_path, stored, children, pre_cached=pre
        )

        assert error is None, error
        assert on_disk[media_key(stored[1])] == b"BYTES-OF-ITEM-2", (
            "the already-cached item must be left alone"
        )
        for i in (0, 2):
            assert on_disk[media_key(stored[i])] == f"bytes-of-{fresh[i]}".encode(), (
                f"item {i} must hold its OWN fresh bytes, not a neighbour's"
            )

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
        cached, error, calls, _ = _run_recover(tmp_path, stored, children)

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
        cached, error, calls, _ = _run_recover(tmp_path, stored, children)

        assert cached == 0
        assert error is not None and "mismatch" in error
        assert calls == [], "no bytes may be written on a pairing mismatch"

    def test_count_mismatch_is_not_a_permanent_verdict(self, tmp_path):
        """GIVEN a pairing mismatch — a CURABLE condition
        WHEN recovery runs
        THEN the post is NOT recorded as exhausted.

        The verdict is one-way, so recording a condition that a later run might
        pair correctly would condemn a post the mechanism could still fix. This
        is the guard on the write site: the positive case passes even if the
        recorder is called unconditionally.
        """
        stored = ["https://old.example.com/1.jpg", "https://old.example.com/2.jpg"]
        children = [{"displayUrl": "https://new.example.com/only.jpg"}]
        _, error, calls, _ = _run_recover(tmp_path, stored, children)

        assert error is not None
        assert not [c for c in calls if c[0] == "exhausted"], (
            "a curable failure must stay retryable"
        )

    def test_failed_download_is_not_a_permanent_verdict(self, tmp_path):
        """GIVEN a download that failed transiently
        WHEN recovery runs
        THEN the post is NOT recorded as exhausted.

        A 403/timeout/429 is exactly the class the retry logic exists for; only
        "Instagram has no media for this post" is permanent.
        """
        stored = ["https://old.example.com/1_FAIL.jpg"]
        children = [{"displayUrl": "https://new.example.com/1_FAIL.jpg"}]
        _, error, calls, _ = _run_recover(tmp_path, stored, children)

        assert error is not None, "a failed fetch must report an error"
        assert not [c for c in calls if c[0] == "exhausted"], (
            "a transient failure must stay retryable"
        )

    def test_post_with_no_media_is_recorded_as_exhausted(self, tmp_path):
        """GIVEN an Apify item reporting the post does not exist
        WHEN recovery runs
        THEN the post IS recorded as permanently unrecoverable.

        Without the durable verdict the standing mechanism re-selects and
        RE-PAYS for this post on every future run, forever.
        """
        stored = ["https://old.example.com/1.jpg"]
        _, error, calls, _ = _run_recover(tmp_path, stored, [], no_media=True)

        assert error is not None and "gone" in error
        verdicts = [c for c in calls if c[0] == "exhausted"]
        assert len(verdicts) == 1, "exactly one verdict must be recorded"
        assert verdicts[0][1] == f"p1:{UNRECOVERABLE_POST_GONE}"

    def test_restricted_post_is_not_recorded_as_exhausted(self, tmp_path):
        """GIVEN an Apify item erroring with `restricted_page`
        WHEN recovery runs
        THEN NO permanent verdict is written.

        Measured on real posts: `restricted_page` means "Restricted access, only
        partial data available" — a temporary state, so a later run can succeed.
        The verdict table is one-way, so recording it here would permanently
        exclude posts the mechanism could still recover. This was a real defect:
        five posts were condemned this way before the error field was inspected.
        """
        stored = ["https://old.example.com/1.jpg"]
        _, error, calls, _ = _run_recover(
            tmp_path, stored, [], error_item={"error": "restricted_page"}
        )

        assert error is not None and "no permanent verdict" in error
        assert not [c for c in calls if c[0] == "exhausted"], (
            "a restricted post must stay retryable"
        )

    def test_unrecognized_error_shape_is_not_recorded_as_exhausted(self, tmp_path):
        """GIVEN an item with no media and an error we do not recognize
        WHEN recovery runs
        THEN NO permanent verdict is written.

        The closed set is the safeguard: an unknown cause must be assumed
        temporary, because wrongly condemning a recoverable post is permanent
        while wrongly retrying a dead one costs $0.0023.
        """
        stored = ["https://old.example.com/1.jpg"]
        _, error, calls, _ = _run_recover(
            tmp_path, stored, [], error_item={"error": "some_new_apify_error"}
        )

        assert error is not None
        assert not [c for c in calls if c[0] == "exhausted"]
