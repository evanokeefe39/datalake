"""Text-layer sub-schema + text-call parsing tests (US-EFAC-4)."""

import json

import pytest

from datalake.defs.enrichment.growth_facets_schema import (
    BRAND_SAFETY_FLAGS,
    GROWTH_FACETS_JSON_SCHEMA,
    TEXT_FACET_FIELDS,
    validate_growth_facets,
    validate_text_facets,
    validate_visual_facets,
)
from datalake.defs.enrichment.prompts import (
    build_text_facets_prompt,
    compute_text_facets_prompt_hash,
)
from datalake.defs.enrichment.text_facets import (
    parse_text_response,
    run_text_call,
)

BS_FALSE = {flag: False for flag in BRAND_SAFETY_FLAGS}

GOOD_TEXT = {
    "hook_content": "Most people get this wrong",
    "hook_type": "bold_claim",
    "is_sponsored": False,
    "sponsorship_signal": "",
    "claimed_results": True,
    "cta_type": "comment",
    "audience_named": True,
    "value_depth": "practical",
    "replicable_tactic": "show a before/after metric",
    "evidence": "caption claims '+4k followers in 30 days'",
    "brand_safety": dict(BS_FALSE),
}


class TestValidateTextFacets:
    def test_valid_payload_passes(self):
        assert validate_text_facets(GOOD_TEXT) == []

    def test_hashtag_strategy_optional_but_string_when_present(self):
        with_hs = dict(GOOD_TEXT, hashtag_strategy="3 niche tags, no generic")
        assert validate_text_facets(with_hs) == []
        bad = dict(GOOD_TEXT, hashtag_strategy=42)
        assert any("hashtag_strategy" in e for e in validate_text_facets(bad))

    def test_visual_keys_rejected_in_text_payload(self):
        for key, value in [
            ("face_present", True),
            ("value_medium", "talking_head"),
            ("brand_logos", []),
            ("text_overlay_present", False),
            ("on_screen_claim", False),
        ]:
            assert any(key in e for e in validate_text_facets(dict(GOOD_TEXT, **{key: value})))

    def test_summary_keys_rejected_in_text_payload(self):
        for key, value in [("content_summary", "s"), ("image_summaries", [])]:
            assert any(key in e for e in validate_text_facets(dict(GOOD_TEXT, **{key: value})))

    def test_bad_enum_rejected(self):
        bad = dict(GOOD_TEXT, hook_type="shock", cta_type="buy_now",
                   value_depth="medium")
        errors = validate_text_facets(bad)
        assert any("hook_type" in e and "not in enum" in e for e in errors)
        assert any("cta_type" in e for e in errors)
        assert any("value_depth" in e for e in errors)

    def test_brand_safety_exact_six_flags(self):
        missing = dict(GOOD_TEXT, brand_safety={f: False for f in BRAND_SAFETY_FLAGS if f != "profanity"})
        assert any("brand_safety.profanity: missing" in e
                   for e in validate_text_facets(missing))
        extra = dict(GOOD_TEXT, brand_safety={**BS_FALSE, "spammy": True})
        assert any("brand_safety.spammy: unknown" in e
                   for e in validate_text_facets(extra))
        nonbool = dict(GOOD_TEXT, brand_safety={**BS_FALSE, "political": "no"})
        assert any("brand_safety.political" in e
                   for e in validate_text_facets(nonbool))

    def test_missing_required_text_field(self):
        bad = {k: v for k, v in GOOD_TEXT.items() if k != "hook_content"}
        assert any("hook_content: missing required text field" in e
                   for e in validate_text_facets(bad))

    def test_non_object(self):
        assert validate_text_facets([]) == ["payload: expected object, got list"]


class TestSubSchemaSplit:
    """The split is symmetric: each sub-validator rejects the other's keys."""

    def test_text_keys_rejected_by_visual_validator(self):
        visual_only = {
            "face_present": False,
            "value_medium": "demo",
            "brand_logos": [],
            "text_overlay_present": False,
            "on_screen_claim": False,
        }
        assert any("hook_content" in e
                   for e in validate_visual_facets(dict(visual_only, hook_content="x")))

    def test_text_facet_fields_declared(self):
        assert set(TEXT_FACET_FIELDS) == {
            "hook_content", "hook_type", "is_sponsored", "sponsorship_signal",
            "claimed_results", "cta_type", "audience_named", "value_depth",
            "replicable_tactic", "hashtag_strategy", "evidence",
        }

    def test_full_v3_unchanged_and_accepts_union(self):
        visual = {
            "face_present": False,
            "value_medium": "demo",
            "brand_logos": [],
            "text_overlay_present": False,
            "on_screen_claim": False,
        }
        union = dict(GOOD_TEXT, **visual)
        assert validate_growth_facets(union) == []
        assert GROWTH_FACETS_JSON_SCHEMA["required"].index("hook_content") >= 0


class TestParseTextResponse:
    def test_happy(self):
        text = json.dumps({"text_facets": GOOD_TEXT})
        rec = parse_text_response(text)
        assert rec["errors"] == []
        assert rec["text_facets"] == GOOD_TEXT

    def test_empty_and_bad_json(self):
        assert parse_text_response(None)["errors"] == ["empty response"]
        assert parse_text_response("")[ "errors"] == ["empty response"]
        rec = parse_text_response("{not json")
        assert rec["text_facets"] is None
        assert any("invalid JSON" in e for e in rec["errors"])

    def test_missing_facets_object(self):
        rec = parse_text_response(json.dumps({"brand_safety": BS_FALSE}))
        assert rec["text_facets"] is None
        assert any("text_facets" in e for e in rec["errors"])

    def test_validation_error_prefixed(self):
        bad = dict(GOOD_TEXT, face_present=True)
        rec = parse_text_response(json.dumps({"text_facets": bad}))
        assert rec["text_facets"] is None
        assert any("text_facets.face_present" in e for e in rec["errors"])

    def test_non_object_top_level(self):
        rec = parse_text_response(json.dumps([1]))
        assert any("expected object, got list" in e for e in rec["errors"])


class TestPrompt:
    def test_no_media_and_forbids_visual_keys(self):
        prompt = build_text_facets_prompt("caption text")
        assert "face_present" in prompt  # listed in the DO-NOT-EMIT clause
        assert "Transcript:" not in prompt
        assert "Caption:\ncaption text" in prompt
        assert "text_facets" in prompt and "brand_safety" in prompt

    def test_transcript_channel_present_when_given(self):
        prompt = build_text_facets_prompt("cap", "transcribed words")
        assert "Transcript:\ntranscribed words" in prompt

    def test_prompt_hash_stable_and_schema_folded(self):
        h1 = compute_text_facets_prompt_hash("m")
        assert h1 == compute_text_facets_prompt_hash("m")
        assert h1 != compute_text_facets_prompt_hash("other")


class TestRunTextCall:
    def test_media_free_call_and_cost(self, monkeypatch):
        captured = {}

        class FakeGemini:
            def analyze_with_usage(self, prompt, *, max_output_tokens=None, **kw):
                captured["prompt"] = prompt
                captured["max_output_tokens"] = max_output_tokens
                assert "media_files" not in kw
                return json.dumps({"text_facets": GOOD_TEXT}), {
                    "prompt_token_count": 500, "candidates_token_count": 300}

        rec = run_text_call(FakeGemini(), "p1", "caption text")
        assert rec["ok"] is True and rec["errors"] == []
        assert rec["text_facets"] == GOOD_TEXT
        assert rec["cost_usd"] == pytest.approx(
            500 / 1e6 * 0.10 + 300 / 1e6 * 0.40)
        assert captured["max_output_tokens"] == 2048
        # Caption-only degradation (US-EFAC-4 AC4): required fields still
        # produced with no transcript.
        assert rec["text_facets"]["hook_type"] == "bold_claim"

    def test_invalid_output_not_ok(self):
        class FakeGemini:
            def analyze_with_usage(self, prompt, **kw):
                return "garbage", None

        rec = run_text_call(FakeGemini(), "p1", "cap")
        assert rec["ok"] is False and rec["cost_usd"] == 0.0
