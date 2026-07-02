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
You are a social media classifier. Analyze the Instagram post caption below
and classify it using these fields:

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

_DEFAULT_GEMINI_MODEL = "gemini-3.1-flash-lite"

CURRENT_PROMPT_HASH = compute_prompt_hash(IG_GOLD_PROMPT, _DEFAULT_GEMINI_MODEL)
