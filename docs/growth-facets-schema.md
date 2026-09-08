# Growth facets schema (V3) — canonical lock (US-EFAC-1)

**Canonical module:** `src/datalake/defs/enrichment/growth_facets_schema.py`
**Validator:** `validate_growth_facets(obj) -> list[str]` (empty list = valid)
**Version constant:** `GROWTH_FACETS_SCHEMA_VERSION = "3"` (AC6: fold into the
owning pass's `prompt_hash` so facet JSON written under an older schema is
detectably stale).
**Reserved keys:** `RESERVED_GOLD_KEYS` — structurally excluded; the validator
rejects any of them by name.

Source: `docs/enrichment-enhancement-design.md` §4,
`tasks/epics/enrich-facets/user-stories/us-efac-1-lock-v3-facet-schema.md`.
This locks the schema only — extraction/enrichment wiring lands in later PRs.

## Field inventory

### Cross-modal (evidence may come from caption, transcript, or imagery)

Facets are **modality-agnostic** (design §3): a facet is judged across every
channel that can carry its evidence. The schema does not encode channels;
`evidence` records which channels produced the judgment.

| Field | Type / enum | Meaning | Required |
|---|---|---|---|
| `hook_content` | string (may be `""`) | Verbatim opening hook text (spoken or on-screen) | yes |
| `hook_type` | enum: `pattern_interrupt`, `bold_claim`, `question`, `benefit_promise`, `curiosity_gap`, `story_open`, `visual_hook`, `demonstration`, `none_clear`, `other` | Hook rhetorical category. Directional-grade only (AC1 0.84) — excluded from the ≥0.8 decision gate | yes |
| `is_sponsored` | bool | Post is sponsored/paid partnership per any disclosure channel | yes |
| `sponsorship_signal` | string (may be `""`) | Verbatim disclosure text ("use code…", "sponsored by…") | yes |
| `claimed_results` | bool | Any concrete result claim ("I made $X", "grew 10k in 30 days") | yes |
| `cta_type` | enum: `comment`, `save`, `share`, `follow`, `like`, `link_click`, `dm`, `none`, `other` | Primary call-to-action | yes |
| `audience_named` | bool | Creator names their target audience explicitly | yes |
| `value_depth` | enum: `shallow`, `practical`, `deep` | Depth of the delivered value | yes |
| `replicable_tactic` | string (may be `""`) | The concrete tactic a viewer could copy, if any | yes |
| `hashtag_strategy` | string | Notable hashtag pattern; **the only optional field** | no |
| `evidence` | string (may be `""`) | Which channels (caption/transcript/on-screen/imagery) ground the judgments | yes |
| `brand_safety` | object, exactly 6 bool flags (below) | Brand-selectable risk set | yes |

### Visual-necessary (imagery required)

| Field | Type / enum | Meaning | Required |
|---|---|---|---|
| `face_present` | bool | A human face is visible | yes |
| `value_medium` | enum: `demo`, `talking_head`, `screenshare`, `broll_voiceover`, `slideshow_carousel`, `text_graphic`, `other` | Primary delivery format of the value | yes |
| `brand_logos` | array of strings (may be `[]`) | Objective list of visible brand marks/logos (see firmed definition) | yes |
| `text_overlay_present` | bool | Text is overlaid on the imagery | yes |
| `on_screen_claim` | bool | A result/claim appears ONLY on-screen (not spoken/caption) | yes |

### `brand_safety` — exact 6-flag set (AC4)

`profanity`, `sexualized_content`, `political`, `medical_claims`,
`financial_guarantees`, `violence_trauma` — all bool, all required, no extra
keys (the vague `sensitive_adjacency` is gone). Each flag may be set from any
channel; a sponsor picks its own risk subset downstream.

### Structurally excluded

- **Reserved gold keys** (gold pass owns them; validator rejects):
  `is_educational`, `is_actionable`, `admiralty`, `domain`, `subdomain`,
  `content_type`, `format`, `educational_json`, `actionable_json`.
- **`content_summary` / `image_summaries`**: NOT facets — separate additive
  columns riding the same universal video call (design §6). The validator
  rejects them here as unknown fields.

## Validator behavior

Hand-rolled (no `jsonschema` dependency — checked `pyproject.toml`;
flagged per brief). Checks in order:

1. payload is an object;
2. reserved gold keys → specific rejection naming the key;
3. every required field present;
4. per-field types (string/bool/array-of-strings) and enum membership;
5. `brand_safety`: exactly the 6 flags, all bool, no extras;
6. unknown keys rejected (schema is locked — catches typos and cross-pass
   bleed such as `content_summary`).

The canonical JSON-Schema dict `GROWTH_FACETS_JSON_SCHEMA` (draft 2020-12
syntax, `additionalProperties: false`) is exported for tooling and mirrors
every rule above.

## Required-vs-optional decision

**Required = everything except `hashtag_strategy`.**

Rationale (keep-bias): the field set is broad, but the required/optional
split is a contract question, not a volume question. Free-text fields
(`hook_content`, `sponsorship_signal`, `replicable_tactic`, `evidence`) must
be PRESENT but may be empty (`""`): absence means malformed output, `""`
means "no evidence found" — conflating them would make staleness and
extraction failures indistinguishable. `hashtag_strategy` is optional because
many posts carry no notable hashtag pattern and the field is descriptive
padding, not decision-grade. Enums always require a value — `none_clear`,
`none`, and `other` are the explicit "nothing" escapes, so there is no need
for nullability.

## Resolved design points (flagged for review, not silently guessed)

### (a) `value_medium` granularity — RECOMMENDATION: keep the enum for V3.1, demote only if the re-run still scores <0.85

Keep the 7-value enum as locked above; do **not** demote to open-with-`other`
now. Rationale:

- The 0.82 came from the 95-post fold spike, within noise of
  `format_structure`'s 0.86; the enum has a clear `other` escape already, so
  the main failure mode (boundary cases demo vs talking_head) is
  disagreement *between two correct-ish enums*, which open-text would not
  fix — it would just make the disagreement unverifiable and break
  analytics grouping.
- The universal call has not shipped yet (this PR locks the schema only).
  Re-validate at larger n per the keep-bias decision (design §4): if the
  V3 re-run still holds <0.85, demote to open-text-with-`other` in a v4
  schema bump (version constant exists for exactly this), not by silent edit.

### (b) Firmed codebook wording (was 68% `brand_logos`, 73% `on_screen_claim`)

**`brand_logos`** (codebook): "List every **legible brand mark** visible in
the imagery: logos, wordmarks, or distinctive brand packaging/labels that a
viewer could name. Include the creator's own brand and watermark-style
handles ONLY if they are a registered brand mark. EXCLUDE: generic products
without a readable mark, UI chrome (app icons of the recording platform),
and text that is merely descriptive. If no mark is legible, return `[]` —
do not guess."

**`on_screen_claim`** (codebook): "TRUE only when a **specific result or
performance claim appears as on-screen text** (overlay, caption burned into
the image, or slide text) — e.g. numbers ('+12,400 followers'), outcomes
('doubled revenue'), or before/after figures. The claim must NOT appear in
the spoken audio or the post caption for this flag to be TRUE (that is
`claimed_results`' channel). Use FALSE when on-screen text is merely
descriptive (step labels, titles) or when the same claim is also spoken/
captioned — this flag isolates the visual-only claim channel."

Rationale for the wording: both soft fields failed on *boundary* cases, not
on the concept. `brand_logos` needed an objective inclusion rule (legible =
nameable) and an explicit empty-list escape; `on_screen_claim` needed its
channel exclusivity made explicit (visual-only vs `claimed_results`) and
descriptive text excluded.

## Tests

`tests/unit/enrichment/test_growth_facets_schema.py` (20 tests): valid
payload passes; each reserved key class (`domain`, `format`, `*_json`)
rejected; missing required field rejected; bad enums rejected;
`brand_safety` exact-6-flag shape enforced; JSON-Schema dict consistency;
seam purity (`seam_violations() == []`).
