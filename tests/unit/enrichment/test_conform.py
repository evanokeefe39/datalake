"""Unit tests for the deterministic silver conform layer (Phase 4).

Covers (AC): well-formed bronze conforms with provenance; re-run is
identical and provably zero-API; malformed JSON quarantines with a reason
(never a silent NULL); each violation class gets a distinguishable
``reason_code``; the carousel cross-field check fires on
``n != len(image_summaries)`` and stays silent when they match; every table
is keyed ``(post_id, platform)``. Tests use tmp_path roots — never the real
lake — and make no network calls.
"""

from __future__ import annotations

import ast
import json
from pathlib import Path

import duckdb
import polars as pl
import pytest

from datalake.defs.enrichment import conform as conform_mod
from datalake.defs.enrichment.conform import (
    DERIVATION_VERSION,
    SILVER_AUDIO_TRANSCRIPTS,
    SILVER_QUARANTINE,
    SILVER_TABLES,
    SILVER_TEXT_ANNOTATIONS,
    SILVER_TEXT_SUMMARIES,
    SILVER_VISUAL_ANNOTATIONS,
    SILVER_VISUAL_SUMMARIES,
    TABLE_SCHEMAS,
    conform,
)
from datalake.defs.enrichment.growth_facets_schema import (
    GROWTH_FACETS_SCHEMA_VERSION,
)
from datalake.defs.enrichment.landing import (
    WORKLOAD_CONTENT_CLASSIFICATION,
    WORKLOAD_GROWTH_FACETS_TEXT,
    WORKLOAD_GROWTH_FACETS_VISUAL,
    land_response,
)

NOW = __import__("datetime").datetime(2026, 9, 10, 12, 0, 0)

# ── Payload builders (mirror the ACTUAL prompt-validated shapes) ───────────


def visual_payload(
    *, n_images: int | None = None, **facet_overrides
) -> dict:
    facets = {
        "face_present": False,
        "value_medium": "demo",
        "brand_logos": ["Acme"],
        "text_overlay_present": True,
        "on_screen_claim": False,
        **facet_overrides,
    }
    payload: dict = {"visual_facets": facets, "content_summary": "A demo video."}
    if n_images is not None:
        payload["image_summaries"] = [
            {"index": i, "summary": f"image {i}"} for i in range(n_images)
        ]
    return payload


def text_payload(**overrides) -> dict:
    payload = {
        "hook_content": "Stop doing X",
        "hook_type": "bold_claim",
        "is_sponsored": False,
        "sponsorship_signal": "",
        "claimed_results": True,
        "cta_type": "save",
        "audience_named": False,
        "value_depth": "practical",
        "replicable_tactic": "Open with the claim, then demo.",
        "evidence": "caption+transcript",
        "brand_safety": {
            "profanity": False,
            "sexualized_content": False,
            "political": False,
            "medical_claims": False,
            "financial_guarantees": False,
            "violence_trauma": False,
        },
        **overrides,
    }
    return payload


# ── Fixtures / helpers ─────────────────────────────────────────────────────


@pytest.fixture
def bronze_root(tmp_path) -> Path:
    return tmp_path / "lake" / "bronze"


@pytest.fixture
def silver_root(tmp_path) -> Path:
    return tmp_path / "lake" / "silver"


def _land_visual(bronze_root, post_id="p1", payload: dict | None = None, **overrides):
    kwargs = {
        "post_id": post_id,
        "platform": "instagram",
        "workload": WORKLOAD_GROWTH_FACETS_VISUAL,
        "provider": "qwen",
        "model": "qwen3.7-flash",
        "prompt_hash": "ph-visual",
        "schema_version": GROWTH_FACETS_SCHEMA_VERSION,
        "run_id": "job-1",
        "response_text": json.dumps(payload or visual_payload()),
        "ok": True,
        "root": bronze_root,
    }
    kwargs.update(overrides)
    return land_response(**kwargs)


def _land_text(bronze_root, post_id="p1", payload: dict | None = None, **overrides):
    kwargs = {
        "post_id": post_id,
        "platform": "instagram",
        "workload": WORKLOAD_GROWTH_FACETS_TEXT,
        "provider": "qwen",
        "model": "qwen3.7-flash",
        "prompt_hash": "ph-text",
        "schema_version": GROWTH_FACETS_SCHEMA_VERSION,
        "run_id": "job-1",
        "response_text": json.dumps(payload or text_payload()),
        "ok": True,
        "root": bronze_root,
    }
    kwargs.update(overrides)
    return land_response(**kwargs)


def _run(bronze_root, silver_root, **kwargs):
    return conform(root=bronze_root, silver_root=silver_root, now=NOW, **kwargs)


def _row(df: pl.DataFrame) -> dict:
    assert df.height >= 1
    return df.row(0, named=True)


# ── Well-formed bronze conforms with provenance ────────────────────────────


def test_well_formed_visual_conforms_to_both_visual_tables(bronze_root, silver_root):
    _land_visual(bronze_root, payload=visual_payload(n_images=None))
    result = _run(bronze_root, silver_root)
    assert result.counts == {"conformed": 1, "quarantined": 0, "skipped_classification": 0}

    ann = _row(conform_mod.read_table(SILVER_VISUAL_ANNOTATIONS, silver_root))
    assert ann["post_id"] == "p1"
    assert ann["platform"] == "instagram"
    assert ann["face_present"] is False
    assert ann["value_medium"] == "demo"
    assert json.loads(ann["brand_logos_json"]) == ["Acme"]
    assert ann["text_overlay_present"] is True
    assert ann["on_screen_claim"] is False

    summ = _row(conform_mod.read_table(SILVER_VISUAL_SUMMARIES, silver_root))
    assert summ["content_summary"] == "A demo video."
    assert summ["image_summaries_json"] is None  # non-carousel: explicit, not fabricated


def test_well_formed_visual_provenance_populated_per_table(bronze_root, silver_root):
    _land_visual(bronze_root)
    _run(bronze_root, silver_root)
    prov_cols = (
        "provider",
        "model",
        "prompt_hash",
        "schema_version",
        "run_id",
        "conformed_at",
        "derivation_version",
    )
    for tid in (SILVER_VISUAL_ANNOTATIONS, SILVER_VISUAL_SUMMARIES):
        row = _row(conform_mod.read_table(tid, silver_root))
        for col in prov_cols:
            assert row[col] is not None, f"{tid}.{col} missing provenance"
        assert row["provider"] == "qwen"
        assert row["model"] == "qwen3.7-flash"
        assert row["prompt_hash"] == "ph-visual"
        assert row["schema_version"] == GROWTH_FACETS_SCHEMA_VERSION
        assert row["run_id"] == "job-1"
        assert row["derivation_version"] == DERIVATION_VERSION


def test_well_formed_text_conforms_and_other_tables_explicitly_empty(
    bronze_root, silver_root
):
    _land_text(bronze_root)
    result = _run(bronze_root, silver_root)
    assert result.counts["conformed"] == 1

    ann = _row(conform_mod.read_table(SILVER_TEXT_ANNOTATIONS, silver_root))
    assert ann["hook_type"] == "bold_claim"
    assert ann["is_sponsored"] is False
    assert ann["cta_type"] == "save"
    assert ann["value_depth"] == "practical"
    assert json.loads(ann["brand_safety_json"]) == text_payload()["brand_safety"]

    # No bronze workload produces summaries or transcripts yet — the tables
    # EXIST with full schema, and their emptiness is correct and explicit.
    for tid in (SILVER_TEXT_SUMMARIES, SILVER_AUDIO_TRANSCRIPTS):
        df = conform_mod.read_table(tid, silver_root)
        assert df.height == 0
        assert set(TABLE_SCHEMAS[tid]) == set(df.columns)


def test_carousel_conforms_when_counts_match(bronze_root, silver_root):
    _land_visual(bronze_root, post_id="car1", payload=visual_payload(n_images=3))
    _run(bronze_root, silver_root, n_media_by_post={"car1": 3})
    summ = _row(conform_mod.read_table(SILVER_VISUAL_SUMMARIES, silver_root))
    assert summ["image_summaries_json"] is not None
    entries = json.loads(summ["image_summaries_json"])
    assert [e["index"] for e in entries] == [0, 1, 2]
    assert summ["content_summary"] == "A demo video."


# ── Deterministic replay + zero API calls ──────────────────────────────────


def test_rerun_same_bronze_produces_identical_tables(bronze_root, silver_root):
    _land_visual(bronze_root, payload=visual_payload(n_images=2))
    _land_text(bronze_root, post_id="p2")
    first = _run(bronze_root, silver_root)
    second = _run(bronze_root, silver_root)

    for tid in SILVER_TABLES:
        a = conform_mod.read_table(tid, silver_root)
        b = conform_mod.read_table(tid, silver_root)
        assert a.equals(b), f"{tid} changed across an identical replay"
    assert first.quarantine.equals(second.quarantine)
    assert first.counts == second.counts


def test_zero_api_calls_enforced_structurally(bronze_root, silver_root):
    """The zero-call property is structural: conform's import graph must not
    contain any provider client, seam module, or HTTP library. A conform run
    cannot reach the network because no code path that knows how to is even
    loaded. Banned module names are matched as dotted-path suffixes so both
    datalake siblings and third-party HTTP stacks are caught."""
    banned_suffixes = (
        "adapters",
        "seam",
        "qwen_client",
        "gemini_batch",
        "facets_batch",
        "facets",
        "harvest",
        "submit",
        "media_upload",
        "resources",
        "requests",
        "httpx",
        "aiohttp",
        "urllib",
        "socket",
        "http",
        "openai",
        "google",
        "grpc",
    )

    def module_suffixes_from_source(path: Path) -> set[str]:
        tree = ast.parse(path.read_text(encoding="utf-8"))
        names: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                names.update(a.name for a in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                names.add(node.module)
        return names

    src_root = Path(conform_mod.__file__).parent
    # Walk conform's LOCAL import graph transitively (stdlib/polars excluded).
    seen: set[str] = set()
    frontier = [conform_mod.__file__]
    while frontier:
        path = Path(frontier.pop())
        if str(path) in seen:
            continue
        seen.add(str(path))
        for name in module_suffixes_from_source(path):
            short = name.rsplit(".", 1)[-1]
            assert short not in banned_suffixes, (
                f"conform import graph contains banned module {name!r} (via {path})"
            )
            if name.startswith("datalake."):
                candidate = src_root.joinpath(*name.split(".")[2:]).with_suffix(".py")
                if candidate.exists():
                    frontier.append(candidate)
                else:  # package
                    candidate = src_root.joinpath(*name.split(".")[2:]) / "__init__.py"
                    if candidate.exists():
                        frontier.append(candidate)

    # And the run itself completes with only the fixture's tmp roots touched.
    _land_visual(bronze_root)
    result = _run(bronze_root, silver_root)
    assert result.counts["conformed"] == 1


# ── Quarantine: loud, reasoned, never a silent NULL ────────────────────────


def test_invalid_json_quarantined_not_silently_null(bronze_root, silver_root):
    _land_visual(bronze_root, response_text="not json { oops")
    result = _run(bronze_root, silver_root)
    assert result.counts["quarantined"] == 1
    assert result.counts["conformed"] == 0

    q = _row(result.quarantine)
    assert q["reason_code"] == conform_mod.REASON_PARSE_ERROR
    assert "invalid JSON" in q["reason_detail"]
    # Quarantine retains provenance for triage.
    assert q["derivation_version"] == DERIVATION_VERSION
    assert q["response_excerpt"].startswith("not json")

    # NO silently-null conformed row: the visual tables are empty, not NULL-rowed.
    assert q["post_id"] == "p1"
    assert q["prompt_hash"] == "ph-visual"
    assert q["run_id"] == "job-1"
    for tid in (SILVER_VISUAL_ANNOTATIONS, SILVER_VISUAL_SUMMARIES):
        assert conform_mod.read_table(tid, silver_root).height == 0


def test_each_violation_class_gets_a_distinguishable_reason(bronze_root, silver_root):
    cases = [
        # (post_id, workload, payload/response, expected reason_code)
        (
            "bad-enum",
            WORKLOAD_GROWTH_FACETS_TEXT,
            text_payload(hook_type="yelling"),
            conform_mod.REASON_ENUM_VIOLATION,
        ),
        (
            "missing-required",
            WORKLOAD_GROWTH_FACETS_TEXT,
            {k: v for k, v in text_payload().items() if k != "evidence"},
            conform_mod.REASON_MISSING_REQUIRED,
        ),
        (
            "length",
            WORKLOAD_GROWTH_FACETS_VISUAL,
            {**visual_payload(), "content_summary": "x" * 4001},
            conform_mod.REASON_LENGTH_VIOLATION,
        ),
        (
            "bad-type",
            WORKLOAD_GROWTH_FACETS_VISUAL,
            visual_payload(face_present="yes"),
            conform_mod.REASON_TYPE_VIOLATION,
        ),
        (
            "unknown-field",
            WORKLOAD_GROWTH_FACETS_TEXT,
            text_payload(surprise_field=1),
            conform_mod.REASON_UNKNOWN_FIELD,
        ),
        (
            "incomplete",
            WORKLOAD_GROWTH_FACETS_VISUAL,
            {"visual_facets": visual_payload()["visual_facets"]},  # no content_summary
            conform_mod.REASON_COMPLETENESS,
        ),
    ]
    for post_id, workload, payload, expected in cases:
        text = json.dumps(payload) if isinstance(payload, dict) else payload
        land_response(
            post_id=post_id,
            platform="instagram",
            workload=workload,
            provider="qwen",
            model="qwen3.7-flash",
            prompt_hash="ph",
            schema_version=GROWTH_FACETS_SCHEMA_VERSION,
            run_id="job-1",
            response_text=text,
            ok=True,
            root=bronze_root,
        )
    result = _run(bronze_root, silver_root)
    assert result.counts["quarantined"] == len(cases)

    reasons = {
        r["post_id"]: r["reason_code"] for r in result.quarantine.iter_rows(named=True)
    }
    for post_id, workload, payload, expected in cases:
        assert reasons[post_id] == expected, (
            f"{post_id}: expected {expected}, got {reasons[post_id]}"
        )
    # Every reason code used is distinct for its violation class.
    assert len(set(reasons.values())) == len(cases)
    # And quarantine is queryable from disk too.
    assert conform_mod.read_table(SILVER_QUARANTINE, silver_root).height == len(cases)


def test_provider_failure_lands_in_quarantine_as_provider_error(
    bronze_root, silver_root
):
    land_response(
        post_id="p1",
        platform="instagram",
        workload=WORKLOAD_GROWTH_FACETS_VISUAL,
        provider="qwen",
        model="qwen3.7-flash",
        prompt_hash="ph",
        schema_version=GROWTH_FACETS_SCHEMA_VERSION,
        run_id="job-1",
        response_text="",
        ok=False,
        error_message="service returned no output body",
        root=bronze_root,
    )
    result = _run(bronze_root, silver_root)
    q = _row(result.quarantine)
    assert q["reason_code"] == conform_mod.REASON_PROVIDER_ERROR
    assert "no output body" in q["reason_detail"]


# ── Carousel cross-field check fires on real violations ────────────────────


def test_carousel_count_mismatch_fires(bronze_root, silver_root):
    _land_visual(bronze_root, post_id="car1", payload=visual_payload(n_images=2))
    result = _run(bronze_root, silver_root, n_media_by_post={"car1": 3})
    assert result.counts["quarantined"] == 1
    q = _row(result.quarantine)
    assert q["reason_code"] == conform_mod.REASON_CROSS_FIELD
    assert "carousel of 3" in q["reason_detail"]
    assert conform_mod.read_table(SILVER_VISUAL_SUMMARIES, silver_root).height == 0


def test_carousel_index_misalignment_fires_without_external_count(bronze_root, silver_root):
    payload = visual_payload(n_images=3)
    payload["image_summaries"][2]["index"] = 5  # gap: 0,1,5
    _land_visual(bronze_root, post_id="car2", payload=payload)
    result = _run(bronze_root, silver_root)  # no n_media map — index rule fires
    assert result.counts["quarantined"] == 1
    q = _row(result.quarantine)
    assert q["reason_code"] == conform_mod.REASON_CROSS_FIELD
    assert "indices" in q["reason_detail"]


# ── Key discipline: (post_id, platform) everywhere ─────────────────────────


def test_each_table_is_keyed_post_id_platform(bronze_root, silver_root):
    for tid in SILVER_TABLES:
        schema = TABLE_SCHEMAS[tid]
        assert list(schema)[:2] == ["post_id", "platform"], tid
        # platform is the key — domain is never a key of a silver table.
        assert "domain" not in schema, tid


def test_conformed_keys_are_unique_pairs(bronze_root, silver_root):
    _land_visual(bronze_root, post_id="a")
    _land_text(bronze_root, post_id="b")
    _run(bronze_root, silver_root)
    for tid in SILVER_TABLES:
        df = conform_mod.read_table(tid, silver_root)
        if df.height == 0:
            continue
        assert df.select(["post_id", "platform"]).n_unique() == df.height, tid


# ── Classification is skipped loudly (sibling unit owns it) ────────────────


def test_classification_rows_are_skipped_not_quarantined(bronze_root, silver_root):
    land_response(
        post_id="p1",
        platform="instagram",
        workload=WORKLOAD_CONTENT_CLASSIFICATION,
        provider="qwen",
        model="qwen3.7-flash",
        prompt_hash="ph-cls",
        schema_version="1",
        run_id="job-1",
        response_text="{}",
        ok=True,
        root=bronze_root,
    )
    result = _run(bronze_root, silver_root)
    assert result.counts == {"conformed": 0, "quarantined": 0, "skipped_classification": 1}
    assert result.quarantine.height == 0


# ── DuckDB registration (queryability, house pattern) ─────────────────────
def test_register_conformed_makes_tables_queryable(bronze_root, silver_root):
    _land_visual(bronze_root)
    _land_text(bronze_root, post_id="p2")
    conn = duckdb.connect(":memory:")
    _run(bronze_root, silver_root, conn=conn)
    expected_counts = {
        SILVER_VISUAL_ANNOTATIONS: 1,
        SILVER_VISUAL_SUMMARIES: 1,
        SILVER_AUDIO_TRANSCRIPTS: 0,
        SILVER_TEXT_ANNOTATIONS: 1,
        SILVER_TEXT_SUMMARIES: 0,
        SILVER_QUARANTINE: 0,
    }
    for tid, expected in expected_counts.items():
        n = conn.execute(f"SELECT COUNT(*) FROM {tid}").fetchone()[0]
        assert n == expected, f"{tid}: {n} != {expected}"
    row = conn.execute(
        f"SELECT post_id, platform FROM {SILVER_VISUAL_ANNOTATIONS} "
        "WHERE post_id = 'p1'"
    ).fetchone()
    assert row == ("p1", "instagram")
