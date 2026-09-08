"""US-EFAC-1: V3 growth-facet schema lock — validator unit tests.

Proves the canonical JSON-Schema module
(``datalake.defs.enrichment.growth_facets_schema``):

- a fully valid assembled payload passes;
- reserved gold keys (``domain`` / ``format`` / a ``*_json`` key) are
  rejected;
- missing required fields are rejected;
- out-of-enum values are rejected;
- ``brand_safety`` must carry exactly the 6 flags, all bools;
- the module is hermetic under the ADR-0008 seam.
"""

from __future__ import annotations

from datalake.defs.enrichment.growth_facets_schema import (
    BRAND_SAFETY_FLAGS,
    GROWTH_FACETS_JSON_SCHEMA,
    GROWTH_FACETS_SCHEMA_VERSION,
    RESERVED_GOLD_KEYS,
    validate_growth_facets,
)


def _valid_payload() -> dict:
    return {
        # text-layer / cross-modal
        "hook_content": "Stop doing X — here's what actually works",
        "hook_type": "pattern_interrupt",
        "is_sponsored": False,
        "sponsorship_signal": "",
        "claimed_results": True,
        "cta_type": "save",
        "audience_named": True,
        "value_depth": "practical",
        "replicable_tactic": "3-step framework shown on screen",
        "evidence": "caption + transcript",
        # visual core
        "face_present": True,
        "value_medium": "talking_head",
        "brand_logos": [],
        "text_overlay_present": True,
        "on_screen_claim": False,
        # brand safety (all 6)
        "brand_safety": {flag: False for flag in BRAND_SAFETY_FLAGS},
    }


class TestValidPayload:
    def test_fully_valid_payload_passes(self):
        assert validate_growth_facets(_valid_payload()) == []

    def test_empty_free_text_allowed(self):
        payload = _valid_payload()
        for field in ("hook_content", "sponsorship_signal", "replicable_tactic", "evidence"):
            payload[field] = ""
        assert validate_growth_facets(payload) == []

    def test_hashtag_strategy_optional_but_string(self):
        payload = _valid_payload()
        payload["hashtag_strategy"] = "#growth #marketing"
        assert validate_growth_facets(payload) == []
        payload["hashtag_strategy"] = 42
        assert validate_growth_facets(payload) != []


class TestReservedKeys:
    def test_domain_rejected(self):
        payload = _valid_payload()
        payload["domain"] = "marketing"
        errors = validate_growth_facets(payload)
        assert any("domain" in e and "reserved" in e for e in errors)

    def test_format_rejected(self):
        payload = _valid_payload()
        payload["format"] = "carousel"
        errors = validate_growth_facets(payload)
        assert any("format" in e and "reserved" in e for e in errors)

    def test_actionable_json_rejected(self):
        payload = _valid_payload()
        payload["actionable_json"] = {"steps": []}
        errors = validate_growth_facets(payload)
        assert any("actionable_json" in e and "reserved" in e for e in errors)

    def test_every_reserved_constant_matches_story(self):
        assert RESERVED_GOLD_KEYS == frozenset(
            {
                "is_educational",
                "is_actionable",
                "admiralty",
                "domain",
                "subdomain",
                "content_type",
                "format",
                "educational_json",
                "actionable_json",
            }
        )


class TestRequiredFields:
    def test_missing_required_field_rejected(self):
        payload = _valid_payload()
        del payload["hook_type"]
        errors = validate_growth_facets(payload)
        assert any("hook_type" in e and "missing required" in e for e in errors)

    def test_brand_safety_missing_flag_rejected(self):
        payload = _valid_payload()
        del payload["brand_safety"]["medical_claims"]
        errors = validate_growth_facets(payload)
        assert any("medical_claims" in e for e in errors)

    def test_brand_safety_extra_flag_rejected(self):
        payload = _valid_payload()
        payload["brand_safety"]["sensitive_adjacency"] = True
        errors = validate_growth_facets(payload)
        assert any("sensitive_adjacency" in e for e in errors)

    def test_brand_safety_non_bool_rejected(self):
        payload = _valid_payload()
        payload["brand_safety"]["political"] = "no"
        errors = validate_growth_facets(payload)
        assert any("brand_safety.political" in e for e in errors)

    def test_brand_safety_exact_six_flags(self):
        assert len(BRAND_SAFETY_FLAGS) == 6


class TestEnums:
    def test_bad_enum_value_rejected(self):
        payload = _valid_payload()
        payload["hook_type"] = "shouty"
        errors = validate_growth_facets(payload)
        assert any("hook_type" in e and "not in enum" in e for e in errors)

    def test_value_medium_open_with_other_accepts_descriptive(self):
        # value_medium demoted to open-with-other free text at lock (US-EFAC-1);
        # any descriptive string is valid, not just the example vocabulary.
        payload = _valid_payload()
        payload["value_medium"] = "documentary"
        assert validate_growth_facets(payload) == []

    def test_value_medium_non_string_rejected(self):
        payload = _valid_payload()
        payload["value_medium"] = 42
        errors = validate_growth_facets(payload)
        assert any("value_medium" in e and "expected string" in e for e in errors)

    def test_non_bool_field_rejected(self):
        payload = _valid_payload()
        payload["is_sponsored"] = "yes"
        errors = validate_growth_facets(payload)
        assert any("is_sponsored" in e and "boolean" in e for e in errors)

    def test_brand_logos_non_string_element_rejected(self):
        payload = _valid_payload()
        payload["brand_logos"] = ["Nike", 7]
        errors = validate_growth_facets(payload)
        assert any("brand_logos[1]" in e for e in errors)

    def test_unknown_field_rejected(self):
        payload = _valid_payload()
        payload["content_summary"] = "a summary"
        errors = validate_growth_facets(payload)
        assert any("unknown field" in e for e in errors)


class TestSchemaShape:
    def test_payload_matches_json_schema_enum_lists(self):
        props = GROWTH_FACETS_JSON_SCHEMA["properties"]
        assert props["hook_type"]["enum"] == [
            "pattern_interrupt",
            "bold_claim",
            "question",
            "benefit_promise",
            "curiosity_gap",
            "story_open",
            "visual_hook",
            "demonstration",
            "none_clear",
            "other",
        ]
        assert props["brand_safety"]["required"] == list(BRAND_SAFETY_FLAGS)
        assert GROWTH_FACETS_JSON_SCHEMA["additionalProperties"] is False

    def test_version_constant_present(self):
        assert GROWTH_FACETS_SCHEMA_VERSION == "3"


class TestHermetic:
    def test_seam_violations_empty(self):
        from datalake.defs.enrichment.media_upload import seam_violations

        assert seam_violations() == []
