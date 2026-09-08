"""Enrichment prompts — domain-specific Gemini analysis prompts.

Prompt hashes use ``hashlib.sha256``, NOT Python's built-in ``hash()``.
Python 3.3+ randomizes string hashing via ``PYTHONHASHSEED``, making
``hash()`` non-deterministic across process restarts.
"""

from __future__ import annotations

import hashlib


def compute_prompt_hash(prompt: str, model: str) -> str:
    """Compute a deterministic hash of (prompt + model) for staleness detection."""
    return hashlib.sha256(f"{prompt}:{model}".encode()).hexdigest()[:16]


IG_GOLD_PROMPT = """\
You are a social media classifier. Analyze the Instagram post and any attached
media below. Classify it using these fields:

- is_educational: true if the post teaches or informs, false if purely entertaining
- is_actionable: true if the post gives steps or actions the viewer can take
- admiralty: Information quality — A1 (primary source) through C2 (entertainment).
  A1=original research/data, A2=expert opinion, A3=synthesis/review,
  A4=professional summary, A5=official documentation, A6=archive/historical,
  B1=industry journalism, B2=curated collection, B3=credentialed analysis,
  B4=experience report, B5=authoritative reference, B6=peer-reviewed summary,
  C1=user-generated insight, C2=entertainment/meme
- domain: Primary category (e.g. Business, Tech, Science, Health, Education,
  Lifestyle, Finance, Legal, Creative)
- subdomain: More specific subcategory within the domain
- topic: The main subject of the post
- subtopic: More specific aspect of the topic
- content_type: tutorial, review, commentary, news, case_study, opinion,
  demonstration, comparison, interview, announcement, personal_story, other
- style: casual, professional, academic, conversational, humorous,
  inspirational, provocative, technical
- format: talking_head, text_overlay, slideshow, screen_recording,
  broll, animation, interview, other
- educational_json: If is_educational, provide:
  { "summary": "One paragraph summary",
    "workflow": [{"step": "Step name", "tool": "Tool used or None",
                  "detail": "What to do"}],
    "concepts": [{"term": "Key term", "explanation": "What it means"}],
    "principles": ["Key principle 1", "Key principle 2"],
    "techniques": ["Technique 1", "Technique 2"] }
- actionable_json: If is_actionable, provide:
  { "summary": "One paragraph summary",
    "resources": ["Resource link or description"],
    "tools": ["Tool name"],
    "guides": ["Step by step guide point"],
    "downloads": ["Download link or description"] }

Return ONLY valid JSON with these fields. No markdown, no explanation.

Caption:"""  # no trailing whitespace needed

# ── Model ───────────────────────────────────────────────────────────────────

_DEFAULT_GEMINI_MODEL = "gemini-3.5-flash-lite"

CURRENT_PROMPT_HASH = compute_prompt_hash(IG_GOLD_PROMPT, _DEFAULT_GEMINI_MODEL)


# ── Universal video→Gemini call (US-EFAC-3 + US-ESUM-1) ──────────────────────

from datalake.defs.enrichment.growth_facets_schema import (  # noqa: E402
    BRAND_SAFETY_FLAGS,
    GROWTH_FACETS_SCHEMA_VERSION,
    HOOK_TYPES,
    VALUE_MEDIUM_EXAMPLES,
)

_VISUAL_CODEBOOK = """\
Codebook (apply strictly):
- face_present=true ONLY if a human face is visible.
- brand_logos: list every LEGIBLE brand mark in the imagery — logos, wordmarks,
  or distinctive brand packaging/labels a viewer could name. Include the
  creator's own brand only if it is a registered brand mark. EXCLUDE: generic
  products without a readable mark, UI chrome (the recording platform's app
  icon), and merely descriptive text. If no mark is legible, return [] — do
  not guess.
- text_overlay_present=true only if text is rendered ON the imagery.
- on_screen_claim=true only when a SPECIFIC result or performance claim
  appears as on-screen text (overlay, burned-in caption, slide text) — e.g.
  numbers ('+12,400 followers'), outcomes ('doubled revenue'), before/after
  figures. The claim must NOT appear in the spoken audio or the caption for
  this flag to be TRUE (that is claimed_results' channel — do not emit it).
  FALSE when on-screen text is merely descriptive (step labels, titles) or
  when the same claim is also spoken/captioned.
- value_medium: free text describing HOW the content is primarily delivered,
  in the creator's own terms. Example vocabulary (guidance only, not an enum):
  demo, talking_head, screenshare, broll_voiceover, slideshow_carousel,
  text_graphic, other.
- content_summary / per-image summaries: describe only what is visible; never
  infer the unseen. Do not rest on the caption for visual claims."""


def build_growth_facets_prompt(caption: str, n_media: int) -> str:
    """Build the ONE universal media-call prompt (visual facets + summaries).

    ``n_media`` = number of media files sent. n_media > 1 means a carousel:
    the model additionally folds per-image summaries aligned to display order.
    Text-layer facets (hook_*, is_sponsored, claimed_results, cta_type,
    audience_named, value_depth, replicable_tactic, brand_safety) are
    explicitly out of scope (US-EFAC-4) — the prompt forbids them.
    """
    visual_keys = "face_present, value_medium, brand_logos, text_overlay_present, on_screen_claim"
    if n_media > 1:
        summary_task = (
            f"image_summaries: an array with EXACTLY {n_media} entries, one "
            "per image in display order; each entry {\"index\": int, "
            "\"summary\": string} — one short sentence per image."
        )
    else:
        summary_task = "content_summary: string (2-3 short sentences)."
    return (
        "You are a meticulous content analyst. You will see the post's media "
        "(video, carousel, or image). Caption is CONTEXT ONLY, not grounds for "
        "visual claims.\n\n"
        "TASK — return ONE JSON object with EXACTLY these keys:\n"
        "  visual_facets: object with EXACTLY these keys: " + visual_keys + "\n"
        "  " + summary_task + "\n"
        "Nothing else. No extra keys — especially do NOT emit hook_content, "
        "hook_type, is_sponsored, sponsorship_signal, claimed_results, "
        "cta_type, audience_named, value_depth, replicable_tactic, "
        "hashtag_strategy, brand_safety, evidence, or any brand_safety flags.\n\n"
        "Codebook:\n" + _VISUAL_CODEBOOK + "\n\n"
        "Return ONLY valid JSON. No markdown, no explanation.\n\n"
        "Caption:\n" + caption
    )


def facets_instruction_skeleton() -> str:
    """Static instruction text — hashed with model + schema version below."""
    return (
        "universal-video-call v1 | "
        "growth_facets_schema_v" + GROWTH_FACETS_SCHEMA_VERSION + " | "
        "visual core: face_present,value_medium,brand_logos,"
        "text_overlay_present,on_screen_claim | summaries: content_summary,"
        "image_summaries"
    )


def compute_facets_prompt_hash(model: str = _DEFAULT_GEMINI_MODEL) -> str:
    """Own prompt_hash for the visual pass (schema version folded in, AC6)."""
    return compute_prompt_hash(
        facets_instruction_skeleton() + ":" + model, model
    )


CURRENT_FACETS_PROMPT_HASH = compute_facets_prompt_hash(_DEFAULT_GEMINI_MODEL)


# ── Text-layer text call (US-EFAC-4) ─────────────────────────────────────────

_TEXT_CODEBOOK = """\
Codebook (apply strictly):
- Read ONLY the caption (and transcript, when provided). Do not guess at
  visuals, audio, or anything not present in the text.
- hook_content: the actual opening line/phrase verbatim or near-verbatim (may
  be "" if the text carries no distinct hook).
- hook_type: one of %s.
- is_sponsored: true only when the text discloses sponsorship/paid
  partnership (e.g. #ad, #sponsored, 'paid partnership', disclosure language).
- sponsorship_signal: the exact disclosure phrase found ("" when none).
- claimed_results: true when the text claims a SPECIFIC result or performance
  outcome (numbers, outcomes, before/after figures).
- cta_type: the primary call-to-action present in the text; "none" when there
  is no CTA.
- audience_named: true when the text explicitly names or addresses its target
  audience (e.g. 'for data engineers').
- value_depth: shallow (entertainment/surface), practical (usable steps or
  tips), or deep (substantial teaching or original insight).
- replicable_tactic: the concrete repeatable tactic a creator could copy (may
  be "").
- hashtag_strategy: how hashtags are used, if discernible; omit the key when
  there is nothing to say.
- evidence: short justification quoting the deciding text.
- brand_safety: judge ONLY what the text itself carries (profanity,
  sexualized content, political content, medical claims, financial
  guarantees, violence/trauma). All six flags are required booleans.""" % (
    list(HOOK_TYPES),
)


def build_text_facets_prompt(caption: str, transcript: str | None = None) -> str:
    """Build the cheap media-free TEXT-call prompt (US-EFAC-4).

    Caption (+ transcript when present) is the ONLY input — no media is sent.
    Visual-core facets and summaries are explicitly out of scope.
    """
    text_keys = (
        "hook_content, hook_type, is_sponsored, sponsorship_signal, "
        "claimed_results, cta_type, audience_named, value_depth, "
        "replicable_tactic, evidence"
    )
    channels = "Caption (and transcript, when provided) below."
    transcript_block = ""
    if transcript and transcript.strip():
        channels = "Transcript (primary) and Caption below."
        transcript_block = "\n\nTranscript:\n" + transcript
    return (
        "You are a meticulous content analyst. " + channels + " You see NO "
        "media.\n\n"
        "TASK — return ONE JSON object with EXACTLY these keys:\n"
        "  text_facets: object with EXACTLY these keys: " + text_keys + "\n"
        "  brand_safety: object with EXACTLY these keys: "
        + ", ".join(BRAND_SAFETY_FLAGS) + "\n"
        "hashtag_strategy is OPTIONAL — include it only when the text shows "
        "a hashtag strategy. Nothing else. No extra keys — especially do NOT "
        "emit face_present, value_medium, brand_logos, text_overlay_present, "
        "on_screen_claim, content_summary, or image_summaries.\n\n"
        "Codebook:\n" + _TEXT_CODEBOOK + "\n\n"
        "Return ONLY valid JSON. No markdown, no explanation."
        + transcript_block + "\n\nCaption:\n" + caption
    )


def compute_text_facets_prompt_hash(model: str = _DEFAULT_GEMINI_MODEL) -> str:
    """Own prompt_hash for the text pass (schema version folded in, AC6)."""
    return compute_prompt_hash(
        build_text_facets_prompt("[canary]")
        + f"|schema_v{GROWTH_FACETS_SCHEMA_VERSION}",
        model,
    )


CURRENT_TEXT_FACETS_PROMPT_HASH = compute_text_facets_prompt_hash(
    _DEFAULT_GEMINI_MODEL
)
