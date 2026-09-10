# Growth facets schema (V3) — canonical lock (US-EFAC-1)

**Canonical module:** `src/datalake/defs/enrichment/growth_facets_schema.py`
**Validator:** `validate_growth_facets(obj) -> list[str]` (empty list = valid)
**Version constant:** `GROWTH_FACETS_SCHEMA_VERSION = "3"` (AC6: fold into the
owning pass's `prompt_hash` so facet JSON written under an older schema is
detectably stale).
**Reserved keys:** `RESERVED_GOLD_KEYS` — structurally excluded; the validator
rejects any of them by name.

Source: `docs/architecture/pipelines/enrichment.md` (§2 naming rules, §4 silver tables) — the
canonical v3 spec; `docs/architecture/enrichment-design-v1-superseded.md` §4 is retained as the
rationale record only,
`tasks/epics/enrich-facets/user-stories/us-efac-1-lock-v3-facet-schema.md`.
This locks the schema only — extraction/enrichment wiring lands in later PRs.

> **Settled mapping — UPDATED 2026-09-10 for the v3 layer model (ADR-0011).**
> The locked V3 field set is unchanged; its storage homes are final:
> cross-modal fields → **`silver_text_annotations`** (with `brand_safety` stored
> as the `brand_safety_json` column), visual-necessary fields →
> **`silver_visual_annotations`**, and the non-facet summary outputs →
> **`silver_visual_summaries`**.
>
> **The `gold_*` names above are superseded.** [ADR-0011](adr/0011-enrichment-layered-model.md)
> supersedes ADR-0010's naming and layer scope: these are **silver** conformed
> tables, not gold, and the join key is **`platform`**, never `domain`. Gold is
> reserved for the four analytic marts. The legacy single-table store
> `gold_growth_facets` is replaced by the split above (see §Channel availability
> & provenance below).

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
| `value_medium` | string (open-with-`other` free text; example vocab: `demo`, `talking_head`, `screenshare`, `broll_voiceover`, `slideshow_carousel`, `text_graphic`, `other`) | Primary delivery format of the value, described in the creator's own terms | yes |
| `brand_logos` | array of strings (may be `[]`) | Objective list of visible brand marks/logos (see firmed definition) | yes |
| `text_overlay_present` | bool | Text is overlaid on the imagery | yes |
| `on_screen_claim` | bool | A result/claim appears ONLY on-screen (not spoken/caption) | yes |

### `brand_safety` — exact 6-flag set (AC4)

`profanity`, `sexualized_content`, `political`, `medical_claims`,
`financial_guarantees`, `violence_trauma` — all bool, all required, no extra
keys (the vague `sensitive_adjacency` is gone). Each flag may be set from any
channel; a sponsor picks its own risk subset downstream.

### Structurally excluded

- **Reserved classification keys** (the classification pass owns them — its
  table is `silver_content_classification`, renamed from legacy `gold_analyses`;
  the validator rejects):
  `is_educational`, `is_actionable`, `admiralty`, `domain`, `subdomain`,
  `content_type`, `format`, `educational_json`, `actionable_json`.
- **`content_summary` / `image_summaries`**: NOT facets — they live in
  `silver_visual_summaries` (produced by the same visual submit, separate
  table; design §6, `docs/architecture/pipelines/enrichment.md`). The validator rejects them
  here as unknown fields.

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

## Channel availability & provenance (audit 2026-09-09, no schema change)

The locked V3 field set needs NO change for the transcript/audio-absent
product intent:
- The schema is deliberately channel-blind; `evidence` records which channels
  grounded each judgment. When transcripts land, the text call feeds the same
  fields with more evidence — no field, enum, or validator change.
- **Audio-absent is a data condition, not a schema condition.** Image/carousel
  posts are marked in the transcript store
  (`silver_audio_transcripts.transcript_status = no_audio_source`,
  E-ENRICH-TRANSCRIPTS), never by nulling facet fields.
- **Per-pass provenance is a storage concern, not this schema's:** the facet
  JSON carries `GROWTH_FACETS_SCHEMA_VERSION`. SETTLED (2026-09-09; layer and
  naming updated 2026-09-10 for ADR-0011): the fix is structural — the legacy
  single-table store (`gold_growth_facets`, where the text pass overwrote the
  visual pass's hash, `defs/enrichment/facets_batch.py:250-256`) is replaced by
  the four-table split (`silver_visual_annotations` /
  `silver_visual_summaries` / `silver_text_annotations` /
  `silver_text_summaries`), each row carrying its own full provenance metadata
  set — per-pass provenance by construction (ADR-0010, re-homed into silver by
  ADR-0011).

## Resolved design points (flagged for review, not silently guessed)

### (a) `value_medium` granularity — DECISION (2026-09-08): demote to open-with-`other` free text

Locked as **free text**, not the 7-value enum. The 0.82 agreement (95-post
fold spike) is within noise of `format_structure`'s 0.86, and the enum's main
failure mode (boundary demo vs talking_head) is disagreement *between two
correct-ish enums*. Rather than ship a weak enum and demote later via a v4
bump, the user chose to demote **now**, before the schema's first release:
`value_medium` takes a free-text description of the delivery format, with the
old enum values retained as model-facing example vocabulary
(`VALUE_MEDIUM_EXAMPLES` in the module) — never enforced.

Tradeoff accepted: no fixed analytics grouping on this field (use free-text
grouping or embeddings); the cost is small and it removes a guaranteed-noise
enum before any data is written under it. If a stable grouping later proves
decision-useful, reintroduce a tight enum in a v4 bump with agreement evidence.

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
