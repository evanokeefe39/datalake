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

from pathlib import Path
from unittest.mock import patch

from orchestration.defs.ig_core.bnz.recover import (
    _fresh_media_for_post,
    recover_one,
)


def _sidecar(children: list[dict], cover: str = "https://cdn.example.com/cover.jpg") -> dict:
    return {"type": "Sidecar", "displayUrl": cover, "childPosts": children}


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

    def test_post_with_no_media_yields_nothing(self):
        """GIVEN an item with neither children nor a displayUrl
        WHEN the fresh list is built
        THEN it is empty, so the caller reports rather than caching junk.
        """
        assert _fresh_media_for_post({"type": "Image"}) == []


def _fake_item(children: list[dict]) -> dict:
    return _sidecar(children)


def _run_recover(tmp_path: Path, stored: list[str], children: list[dict]):
    """Run recover_one end-to-end against fakes, returning (cached, error, calls)."""
    item = _fake_item(children)
    calls: list[tuple[str, str]] = []
    # Model the cache faithfully: after cache_media_bytes(fresh) the fresh url
    # resolves, and after seed_media_from_file(stored) the stored url resolves.
    # A blanket local_media_path -> None would make _cache_under always report
    # failure and the test would pass for the wrong reason.
    on_disk: dict[str, str] = {}

    def fake_cache(ops, media_url, *, media_dir=None, attempts=None, backoff_base=None):
        calls.append(("cache", media_url))
        p = tmp_path / f"{len(on_disk)}.jpg"
        p.write_bytes(b"bytes-" + media_url.encode())
        on_disk[media_url] = str(p)
        return str(p)

    def fake_seed(ops, stored_url, src_path, *, media_dir=None, conn=None):
        calls.append(("seed", stored_url))
        on_disk[stored_url] = str(src_path)
        return str(src_path)

    def fake_local(ops, media_url, *, conn=None):
        return on_disk.get(media_url)

    with (
        patch("orchestration.defs.ig_core.bnz.recover.trigger_run") as tr,
        patch("orchestration.defs.ig_core.bnz.recover.poll_run") as pr,
        patch("orchestration.defs.ig_core.bnz.recover.stream_dataset") as sd,
        patch("orchestration.defs.ig_core.bnz.recover.cache_media_bytes", fake_cache),
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


def _write_item(dest: Path, item: dict) -> int:
    import json

    dest.write_text(json.dumps(item), encoding="utf-8")
    return 1


class TestRecoveryKeying:
    """Bytes must land under the STORED url's hash, and mismatches must abort."""

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
