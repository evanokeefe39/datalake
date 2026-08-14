"""Tests for the ``creators`` + ``profiles`` control tables and CRUD helpers."""

from __future__ import annotations

import pytest

from datalake.defs.common.resources import SQLiteResource
from datalake.defs.common.schemas import SQLITE_TABLES
from datalake.defs.instagram.creators import (
    add_profile,
    batch_add_profiles,
    create_creator,
    creator_for_handle,
    creator_map,
    edit_depth,
    enabled_profiles,
    ensure_schema,
    get_creator,
    list_creators,
    remove_creator,
    remove_profile,
    rename_creator,
)


@pytest.fixture
def ops(tmp_path) -> SQLiteResource:
    return SQLiteResource(database=str(tmp_path / "ops.sqlite"))


def test_ensure_schema_creates_tables(ops):
    ensure_schema(ops)
    conn = ops.get_connection()
    try:
        names = {
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
    finally:
        conn.close()
    assert {"creators", "profiles"} <= names


def test_create_creator_inserts(ops):
    creator = create_creator(ops, "Jane Doe")
    assert creator["id"] is not None
    assert creator["name"] == "Jane Doe"


def test_create_creator_upserts_by_name(ops):
    first = create_creator(ops, "Jane Doe")
    second = create_creator(ops, "Jane Doe")
    assert second["id"] == first["id"]
    assert len(list_creators(ops)) == 1


def test_create_creator_rejects_empty(ops):
    with pytest.raises(ValueError):
        create_creator(ops, "   ")


def test_add_profile_and_get_creator(ops):
    creator = create_creator(ops, "Jane Doe")
    add_profile(ops, creator_id=creator["id"], platform="instagram", handle="jane")

    got = get_creator(ops, creator["id"])
    assert got is not None
    assert [p["handle"] for p in got["profiles"]] == ["jane"]
    assert got["profiles"][0]["platform"] == "instagram"


def test_add_profile_upserts_same_platform_handle(ops):
    creator = create_creator(ops, "Jane Doe")
    add_profile(ops, creator_id=creator["id"], platform="instagram", handle="jane", results_limit=3)
    add_profile(ops, creator_id=creator["id"], platform="instagram", handle="jane", results_limit=5)

    got = get_creator(ops, creator["id"])
    assert len(got["profiles"]) == 1
    assert got["profiles"][0]["results_limit"] == 5


def test_same_handle_two_platforms_distinct(ops):
    creator = create_creator(ops, "Jane Doe")
    add_profile(ops, creator_id=creator["id"], platform="instagram", handle="jane")
    add_profile(ops, creator_id=creator["id"], platform="tiktok", handle="jane")

    got = get_creator(ops, creator["id"])
    assert {p["platform"] for p in got["profiles"]} == {"instagram", "tiktok"}


def test_add_profile_rejects_bad_depth(ops):
    creator = create_creator(ops, "Jane Doe")
    with pytest.raises(ValueError):
        add_profile(
            ops,
            creator_id=creator["id"],
            platform="instagram",
            handle="jane",
            results_limit=0,
        )


def test_batch_add_profiles(ops):
    creator = create_creator(ops, "Jane Doe")
    profiles = batch_add_profiles(
        ops, creator_id=creator["id"], platform="instagram", handles=["a", "b", "c"]
    )
    assert len(profiles) == 3
    assert {p["handle"] for p in profiles} == {"a", "b", "c"}


def test_batch_add_skips_blanks(ops):
    creator = create_creator(ops, "Jane Doe")
    profiles = batch_add_profiles(
        ops, creator_id=creator["id"], platform="instagram", handles=["a", " ", ""]
    )
    assert len(profiles) == 1


def test_edit_depth_changes_and_persists(ops):
    creator = create_creator(ops, "Jane Doe")
    add_profile(ops, creator_id=creator["id"], platform="instagram", handle="jane", results_limit=1)

    updated = edit_depth(ops, platform="instagram", handle="jane", results_limit=7)
    assert updated["results_limit"] == 7

    got = get_creator(ops, creator["id"])
    assert got["profiles"][0]["results_limit"] == 7


def test_edit_depth_rejects_below_one(ops):
    with pytest.raises(ValueError):
        edit_depth(ops, platform="instagram", handle="jane", results_limit=0)


def test_edit_depth_absent_returns_none(ops):
    ensure_schema(ops)
    assert edit_depth(ops, platform="instagram", handle="nobody", results_limit=3) is None


def test_rename_creator(ops):
    creator = create_creator(ops, "Old Name")
    renamed = rename_creator(ops, creator["id"], "New Name")
    assert renamed["name"] == "New Name"
    assert get_creator(ops, creator["id"])["name"] == "New Name"


def test_rename_does_not_affect_profiles(ops):
    creator = create_creator(ops, "Old Name")
    add_profile(ops, creator_id=creator["id"], platform="instagram", handle="jane", results_limit=4)
    rename_creator(ops, creator["id"], "New Name")

    got = get_creator(ops, creator["id"])
    assert got["name"] == "New Name"
    assert got["profiles"][0]["results_limit"] == 4


def test_remove_profile_keeps_creator(ops):
    creator = create_creator(ops, "Jane Doe")
    add_profile(ops, creator_id=creator["id"], platform="instagram", handle="jane")
    remove_profile(ops, platform="instagram", handle="jane")

    got = get_creator(ops, creator["id"])
    assert got is not None
    assert got["profiles"] == []


def test_remove_creator_cascades_profiles(ops):
    creator = create_creator(ops, "Jane Doe")
    add_profile(ops, creator_id=creator["id"], platform="instagram", handle="jane")
    remove_creator(ops, creator["id"])

    assert get_creator(ops, creator["id"]) is None
    conn = ops.get_connection()
    try:
        count = conn.execute(
            "SELECT COUNT(*) FROM profiles WHERE creator_id = ?", [creator["id"]]
        ).fetchone()[0]
    finally:
        conn.close()
    assert count == 0


def test_enabled_profiles_filters_disabled(ops):
    creator = create_creator(ops, "Jane Doe")
    add_profile(ops, creator_id=creator["id"], platform="instagram", handle="alice")
    add_profile(ops, creator_id=creator["id"], platform="instagram", handle="bob", enabled=False)

    assert [p["handle"] for p in enabled_profiles(ops)] == ["alice"]


def test_creator_for_handle_and_map(ops):
    creator = create_creator(ops, "Jane Doe")
    add_profile(ops, creator_id=creator["id"], platform="instagram", handle="jane")

    link = creator_for_handle(ops, platform="instagram", handle="jane")
    assert link == {"creator_id": creator["id"], "creator_name": "Jane Doe"}

    assert creator_map(ops) == {"jane": link}


def test_creator_for_handle_absent(ops):
    ensure_schema(ops)
    assert creator_for_handle(ops, platform="instagram", handle="nobody") is None


def test_creator_with_zero_profiles_listed(ops):
    create_creator(ops, "Empty")
    rows = list_creators(ops)
    assert len(rows) == 1
    assert rows[0]["profile_count"] == 0


def test_schema_catalog_has_creators_and_profiles():
    assert "creators" in SQLITE_TABLES
    assert "profiles" in SQLITE_TABLES
    assert "scrape_targets" not in SQLITE_TABLES

    assert set(SQLITE_TABLES["profiles"]) >= {
        "platform",
        "handle",
        "profile_url",
        "results_type",
        "results_limit",
        "enabled",
        "tier",
        "creator_id",
        "updated_at",
    }
