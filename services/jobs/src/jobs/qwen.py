"""OpenRouter chat/completions caller for qwen-batch-service.

One function, no state: given (prompt, image file paths, model, max_tokens),
return the model's text output. Images are read from disk and inlined as
base64 data URLs. Video paths are sampled server-side into JPEG frames
first. Fails loudly on missing key, non-200, or empty content
(qwen reasoning models occasionally return empty content — treat as retryable
flake, not success).
"""
from __future__ import annotations

import base64
import logging
import mimetypes
import os
from pathlib import Path

import httpx

API_URL = "https://openrouter.ai/api/v1/chat/completions"
TIMEOUT_S = 300.0

_MIME_OVERRIDES = {
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png": "image/png",
    ".webp": "image/webp",
    ".gif": "image/gif",
}


_log = logging.getLogger("jobs.qwen")


class QwenError(RuntimeError):
    """Raised on any OpenRouter call failure (missing key, HTTP, empty)."""


class TerminalQwenError(QwenError):
    """QwenError that is deterministic (missing file, failed video sampling):
    retrying with backoff cannot help, so the item fails terminally."""


def _image_part(path: str) -> dict:
    p = Path(path)
    try:
        data = p.read_bytes()
    except OSError as exc:
        raise QwenError(f"cannot read image {path!r}: {exc}") from exc
    mime = _MIME_OVERRIDES.get(p.suffix.lower()) or mimetypes.guess_type(str(p))[0] or "image/jpeg"
    b64 = base64.b64encode(data).decode("ascii")
    return {"type": "image_url", "image_url": {"url": f"data:{mime};base64,{b64}"}}
def _bytes_part(data: bytes, mime: str) -> dict:
    """Image part from raw in-memory bytes (video frames sampled server-side)."""
    b64 = base64.b64encode(data).decode("ascii")
    return {"type": "image_url", "image_url": {"url": f"data:{mime};base64,{b64}"}}



def chat(
    prompt: str,
    images: list[str] | None = None,
    *,
    model: str,
    max_tokens: int = 2500,
) -> str:
    """Run one chat completion; return the assistant text output.

    Raises QwenError on missing API key, non-200 response, or empty content.
    """
    key = os.environ.get("OPENROUTER_API_KEY")
    if not key:
        raise QwenError(
            "OPENROUTER_API_KEY is not set — cannot call OpenRouter. "
            "Export it (e.g. in .env or shell) and retry."
        )

    content: list[dict] | str = prompt
    if images:
        from . import video as video_mod  # local import: ffmpeg only needed for videos

        parts: list[dict] = []
        for p in images:
            if video_mod.is_video_path(p):
                _log.info("sampling video %s into %d frame(s)", p, video_mod.frames_per_video())
                frames = video_mod.sample_video(p)
                parts.extend(_bytes_part(f, "image/jpeg") for f in frames)
            else:
                parts.append(_image_part(p))
        content = [{"type": "text", "text": prompt}, *parts]

    payload = {
        "model": model,
        "max_tokens": max_tokens,
        "response_format": {"type": "json_object"},
        "messages": [{"role": "user", "content": content}],
    }

    try:
        resp = httpx.post(
            API_URL,
            headers={"Authorization": f"Bearer {key}"},
            json=payload,
            timeout=TIMEOUT_S,
        )
    except httpx.HTTPError as exc:
        raise QwenError(f"OpenRouter request failed: {exc}") from exc

    if resp.status_code != 200:
        raise QwenError(f"OpenRouter returned HTTP {resp.status_code}: {resp.text[:500]}")

    try:
        body = resp.json()
    except ValueError as exc:
        raise QwenError(f"OpenRouter returned non-JSON body: {resp.text[:500]}") from exc

    if "error" in body:
        raise QwenError(f"OpenRouter error: {body['error']}")

    choices = body.get("choices") or []
    text = (choices[0].get("message") or {}).get("content") if choices else None
    if not text:
        raise QwenError("OpenRouter returned empty content (qwen reasoning flake)")
    return text
