"""Gold-layer factory helpers for tests."""

from __future__ import annotations

FAKE_ANALYSIS = {
    "is_educational": True,
    "is_actionable": True,
    "admirality": "B1",
    "domain": "Business",
    "subdomain": "Marketing",
    "topic": "AI Content",
    "subtopic": "Prompt Engineering",
    "content_type": "tutorial",
    "style": "casual",
    "format": "talking head",
    "educational_json": {
        "summary": "How to write better AI prompts.",
        "workflow": [
            {"step": "Define goal", "tool": "None", "detail": "Know what you want"},
        ],
        "concepts": [{"term": "Prompt Engineering", "explanation": "Crafting inputs"}],
        "principles": ["Be specific"],
        "techniques": ["Iterative refinement"],
    },
    "actionable_json": {
        "summary": "You can improve your prompts.",
        "resources": [],
        "tools": ["ChatGPT"],
        "guides": ["Start simple, add context"],
        "downloads": [],
    },
}
