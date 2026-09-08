"""Universal video-call visual sub-schema + parsing tests (US-EFAC-3/ESUM-1)."""

import json

import pytest

from datalake.defs.enrichment.facets import parse_universal_response
from datalake.defs.enrichment.growth_facets_schema import (
    GROWTH_FACETS_JSON_SCHEMA,
    RESERVED_GOLD_KEYS,
    VISUAL_FACET_FIELDS,
    validate_growth_facets,
    validate_visual_facets,
)
from datalake.defs.enrichment.prompts import (
    build_growth_facets_prompt,
    compute_facets_prompt_hash,
)

GOOD_FACETS = {
    "face_present": True,
    "value_medium": "talking head with b-roll cuts",
    "brand_logos": ["Nike"],
    "text_overlay_present": False,
    "on_screen_claim": False,
}


def _full_v3(visual: dict) -> dict:
    return dict(
        visual,
        hook_content="",
        hook_type="none_clear",
        is_sponsored=False,
        sponsorship_signal="",
        claimed_results=False,
        cta_type="none",
        audience_named=False,
        value_depth="practical",
        replicable_tactic="",
        evidence="imagery",
        brand_safety={flag: False for flag in (
            "profanity", "sexualized_content", "political",
            "medical_claims", "financial_guarantees", "violence_trauma",
        )},
    )


class TestValidateVisualFacets:
    def test_valid_visual_payload_passes(self):
        assert validate_visual_facets(GOOD_FACETS) == []

    def test_all_visual_fields_are_in_locked_v3_schema(self):
        for field in VISUAL_FACET_FIELDS:
            assert field in GROWTH_FACETS_JSON_SCHEMA["properties"]

    def test_text_layer_field_is_unknown_in_visual_subschema(self):
        errors = validate_visual_facets({**GOOD_FACETS, "hook_type": "question"})
        assert any("hook_type" in e for e in errors)

    def test_summary_keys_rejected_inside_facet_json(self):
        errors = validate_visual_facets({**GOOD_FACETS, "content_summary": "x"})
        assert any("content_summary" in e for e in errors)

    def test_reserved_gold_key_rejected(self):
        errors = validate_visual_facets({**GOOD_FACETS, "domain": "Business"})
        assert any("domain" in e and "reserved" in e for e in errors)

    def test_missing_visual_fields_reported(self):
        errors = validate_visual_facets({"face_present": True})
        assert "value_medium: missing required visual field" in errors
        assert "brand_logos: missing required visual field" in errors

    def test_type_enforcement(self):
        errors = validate_visual_facets({**GOOD_FACETS, "face_present": "yes",
                                         "brand_logos": [1]})
        assert any("face_present" in e for e in errors)
        assert any("brand_logos[0]" in e for e in errors)

    def test_non_object_payload(self):
        assert validate_visual_facets([]) == [
            "payload: expected object, got list"
        ]


class TestFullSchemaUnchanged:
    def test_visual_core_merges_into_valid_v3_payload(self):
        assert validate_growth_facets(_full_v3(GOOD_FACETS)) == []

    def test_reserved_keys_unchanged(self):
        assert "is_educational" in RESERVED_GOLD_KEYS


class TestParseUniversalResponse:
    def test_happy_video(self):
        payload = {"visual_facets": GOOD_FACETS, "content_summary": "a demo"}
        rec = parse_universal_response(json.dumps(payload), 1)
        assert rec["errors"] == []
        assert rec["visual_facets"] == GOOD_FACETS
        assert rec["content_summary"] == "a demo"
        assert rec["image_summaries"] is None

    def test_happy_carousel(self):
        payload = {
            "visual_facets": GOOD_FACETS,
            "content_summary": "carousel",
            "image_summaries": [
                {"index": 0, "summary": "slide one"},
                {"index": 1, "summary": "slide two"},
            ],
        }
        rec = parse_universal_response(json.dumps(payload), 2)
        assert rec["errors"] == []
        assert len(rec["image_summaries"]) == 2

    def test_carousel_misalignment_detected(self):
        payload = {
            "visual_facets": GOOD_FACETS,
            "content_summary": "c",
            "image_summaries": [{"index": 0, "summary": "only one"}],
        }
        rec = parse_universal_response(json.dumps(payload), 2)
        assert any("mis-alignment" in e for e in rec["errors"])

    def test_invalid_json(self):
        rec = parse_universal_response("not json", 1)
        assert rec["visual_facets"] is None
        assert any("invalid JSON" in e for e in rec["errors"])

    def test_missing_visual_facets(self):
        rec = parse_universal_response('{"content_summary": "x"}', 1)
        assert any("visual_facets" in e for e in rec["errors"])

    def test_facet_schema_violation_surfaces_prefixed(self):
        bad = {**GOOD_FACETS, "hook_type": "question"}
        rec = parse_universal_response(
            json.dumps({"visual_facets": bad, "content_summary": "x"}), 1
        )
        assert any("visual_facets.hook_type" in e for e in rec["errors"])


class TestPrompt:
    def test_constrains_to_visual_keys(self):
        prompt = build_growth_facets_prompt("cap", 1)
        for key in VISUAL_FACET_FIELDS:
            assert key in prompt
        assert "visual_facets" in prompt
        assert "content_summary" in prompt

    def test_carousel_gets_image_summaries_contract(self):
        prompt = build_growth_facets_prompt("cap", 4)
        assert "EXACTLY 4 entries" in prompt
        assert "image_summaries" in prompt

    def test_text_layer_fields_explicitly_forbidden(self):
        prompt = build_growth_facets_prompt("cap", 1)
        assert "do NOT emit" in prompt
        assert "hook_content" in prompt and "brand_safety" in prompt

    def test_prompt_hash_is_stable_and_schema_versioned(self):
        h1 = compute_facets_prompt_hash("m")
        h2 = compute_facets_prompt_hash("m")
        assert h1 == h2 and len(h1) == 16
        assert compute_facets_prompt_hash("other") != h1
