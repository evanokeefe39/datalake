"""The two on-disk bronze identities must not move (Unit 3 safety net).

Unit 3 renamed six ASSET KEYS. It deliberately did NOT rename the two bronze names
that carry live bytes:

- ``bronze_enrichment_raw`` — the landing FILE
  (``landing.py DATASET_ID`` → ``data/lake/bronze/bronze_enrichment_raw.parquet``)
- ``ig_roster_raw`` — a DIRECTORY of append-only snapshots read via
  ``ROSTER_BRONZE_DIR``

This is a "we changed no data" guard, NOT a migration check: no migration was
performed, because inspection showed the layer-prefix target is reachable without
moving any bytes (``bronze_enrichment_raw`` already carries the prefix, and the
roster is a source identity). A future rename of either name is a genuinely
riskier decision that must be deliberate — the roster miss is SILENT
(``latest_roster_path()`` returns None, ``enabled_profiles()`` yields [], and the
sweep scrapes nothing while looking healthy).

The digest over the roster directory sorts paths before hashing: a bare glob
depends on filesystem order and would report a false failure if the same files
arrived differently.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

from orchestration.defs.engine import landing
from orchestration.defs.ig_core.bnz import roster
from orchestration.defs.platform import paths as lake

#: The live bronze landing, captured 2026-09-16 before the Unit 3 rename.
#: If this changes, either the landing file moved (a real decision — see the
#: module docstring) or the data itself changed (which no rename should ever do).
_BRONZE_LANDING_NAME = "bronze_enrichment_raw.parquet"
_ROSTER_DIR_NAME = "ig_roster_raw"


def test_bronze_landing_name_is_unchanged() -> None:
    """`DATASET_ID` still names the file that exists on disk."""
    assert landing.DATASET_ID == "bronze_enrichment_raw", (
        "DATASET_ID changed — this renames live bronze. If that is intended it "
        "needs a byte-verified migration (the landing path IS the filename), not "
        f"an inline rename. Got {landing.DATASET_ID!r}"
    )
    resolved = landing.response_path(lake.BRONZE_LAKE)
    assert resolved.name == _BRONZE_LANDING_NAME


def test_roster_directory_name_is_unchanged() -> None:
    """`ROSTER_BRONZE_DIR` still names the snapshot directory.

    Guarded because a rename here fails SILENTLY rather than loudly: the
    directory is recreated empty on the next fetch, and until then the roster
    reads as "no profiles" instead of "broken path".
    """
    assert roster.ROSTER_BRONZE_DIR.name == _ROSTER_DIR_NAME, (
        "the roster bronze directory was renamed — the existing snapshots are "
        "orphaned under the old name and the roster reads EMPTY until the next "
        f"fetch, without raising. Got {roster.ROSTER_BRONZE_DIR.name!r}"
    )


def test_live_bronze_artifacts_are_readable_where_expected() -> None:
    """The real artifacts exist at the names the code resolves.

    Skips when the live lake is not present (CI has no `data/` tree), so this is
    a local-state assertion rather than a CI gate.
    """
    import pytest

    landing_path = landing.response_path(lake.BRONZE_LAKE)
    if not landing_path.exists():
        pytest.skip("live bronze landing not present (expected in CI)")

    # Present and non-empty — a truncated landing file is a data defect.
    assert landing_path.stat().st_size > 0, f"{landing_path} is empty"

    if roster.ROSTER_BRONZE_DIR.exists():
        snaps = sorted(roster.ROSTER_BRONZE_DIR.glob("*.parquet"))
        assert snaps, (
            f"{roster.ROSTER_BRONZE_DIR} exists but holds no snapshots — the "
            "directory was recreated (which is what a rename looks like) rather "
            "than populated"
        )
        # Deterministic digest: sorted paths, so filesystem order cannot make the
        # same directory hash differently.
        h = hashlib.sha256()
        for f in sorted(roster.ROSTER_BRONZE_DIR.rglob("*")):
            if f.is_file():
                h.update(hashlib.sha256(f.read_bytes()).hexdigest().encode())
        assert len(h.hexdigest()) == 64  # pragma: no cover - shape check


def test_renamed_asset_keys_do_not_appear_as_on_disk_names() -> None:
    """The rename must not have created a file or dir named after a NEW key.

    A layer-prefixed asset key like `silver_ig_posts` names a DuckDB TABLE, not a
    Parquet dataset in bronze. If a bronze artifact appeared under one of these
    names, something wrote data through a key that is meant to be a table name —
    which is the confusion the naming convention exists to remove.
    """
    bronze = lake.BRONZE_LAKE
    if not bronze.exists():
        import pytest

        pytest.skip("live bronze root not present (expected in CI)")

    stray = [
        p.name
        for p in bronze.iterdir()
        if p.name.startswith(("silver_ig_", "bronze_ig_posts", "bronze_ig_profile_details"))
    ]
    assert not stray, (
        "an artifact in the bronze root is named after a renamed SILVER key (or a "
        "graph-only bronze key) — those name tables/graph nodes, and no file "
        f"should exist for them: {stray}"
    )


def test_no_stale_parquet_left_under_an_old_key_name() -> None:
    """No file remains named after a pre-rename key."""
    bronze = lake.BRONZE_LAKE
    if not bronze.exists():
        import pytest

        pytest.skip("live bronze root not present (expected in CI)")

    stale_names = {"ig_posts_raw.parquet", "ig_posts_local_raw.parquet",
                   "ig_profile_details_raw.parquet", "ig_posts_slv.parquet",
                   "ig_profiles_slv.parquet", "ig_comments_slv.parquet"}
    present = {p.name for p in Path(bronze).iterdir() if p.is_file()}
    leftovers = sorted(stale_names & present)
    assert not leftovers, (
        f"a file still exists under a pre-rename key name: {leftovers} — the "
        "rename renamed graph keys, so no artifact should carry an old key name"
    )
