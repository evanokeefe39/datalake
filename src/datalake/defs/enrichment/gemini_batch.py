"""Gemini BATCH API verbs — worker-owned, never called from the Dagster graph.

Three verbs only (ADR-0001): ``submit``, ``poll``, ``retrieve``. The worker
(``scripts/enrichment_worker.py --mode gemini-batch``) drives them against the
existing ops.sqlite queue; Dagster assets never touch this module (ADR-0003 —
submission/polling must not become a blocking transform asset).

Batch API facts (KB spike + google-genai 2.10):
- Paid-tier only (Tier 1+); 50% discount on token cost.
- Token caps are model-specific and IN-FLIGHT (across active jobs), not
  cumulative: Flash-Lite generation is 10M (Tier 1) / 128M (Tier 2). Chunk a
  large queue batch into several Gemini batch jobs to stay under the cap.
- ``InlinedRequest.metadata={"custom_key": post_id}`` round-trips to
  ``InlinedResponse.metadata`` — the correlation key. Never rely on response
  order.
- Results arrive inline (``job.dest.inlined_responses``) for smaller jobs;
  larger jobs land in a JSONL file (``job.dest.file_name``) — handle both.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile

from datalake.defs.common.resources import GeminiResource
from datalake.defs.instagram.config import GeminiTierConfig

logger = logging.getLogger("enrichment_worker.gemini_batch")

_TERMINAL_OK = {"SUCCEEDED"}
_TERMINAL_FAIL = {"FAILED", "CANCELLED", "EXPIRED"}
_ACTIVE = {"STATE_UNSPECIFIED", "PENDING", "QUEUED", "RUNNING", "PAUSED", "SUBMITTED"}


def _client(gemini: GeminiResource):
    from google.genai import Client

    return Client(api_key=gemini.api_key)


def _generation_config():
    """Same sampling contract as interactive: JSON mode, 0.2 temp, 2048 out."""
    from google.genai.types import GenerateContentConfig

    return GenerateContentConfig(
        response_mime_type="application/json",
        temperature=0.2,
        max_output_tokens=2048,
    )


# ── Submit ───────────────────────────────────────────────────────────────────


def estimate_tokens(text: str) -> int:
    """Cheap token estimate (~4 chars/token) — chunk sizing only, not billing."""
    return max(1, (len(text) + 3) // 4)


def chunk_requests(
    requests: list[dict], max_tokens: int
) -> list[list[dict]]:
    """Split requests into chunks whose estimated token count fits ``max_tokens``.

    Each request is ``{"custom_key": ..., "prompt": ...}``. Single requests
    larger than ``max_tokens`` get their own chunk (the API will reject them;
    per-request size is bounded by the model context).
    """
    chunks: list[list[dict]] = []
    current: list[dict] = []
    current_tokens = 0
    for req in requests:
        est = estimate_tokens(req["prompt"])
        if current and current_tokens + est > max_tokens:
            chunks.append(current)
            current = []
            current_tokens = 0
        current.append(req)
        current_tokens += est
    if current:
        chunks.append(current)
    return chunks


def submit(
    gemini: GeminiResource,
    model: str,
    requests: list[dict],
    display_name: str,
    max_tokens: int | None = None,
) -> list[str]:
    """Submit requests to the Gemini batch API.

    Args:
        gemini: resource carrying the API key (paid tier required — verify
            with ``GeminiTierConfig.detect().supports_batch`` BEFORE calling).
        model: model name (e.g. ``gemini-3.5-flash-lite``).
        requests: ``{"custom_key": str, "prompt": str}`` dicts (text-only;
            media wiring is a later concern per the ratified scope).
        display_name: base display name; multi-chunk submissions get ``-segN``.
        max_tokens: in-flight enqueued-token cap per job
            (default ``GeminiTierConfig.max_batch_tokens``).

    Returns:
        The Gemini batch job names, one per chunk, in submission order.
    """
    if not requests:
        raise ValueError("requests must not be empty")

    tier = GeminiTierConfig.detect()
    if not tier.supports_batch:
        raise RuntimeError(
            f"Gemini batch API requires Tier 1+ (active tier: {tier.tier.value}). "
            "Set GEMINI_TIER=tier1 with a paid key."
        )

    cap = max_tokens or tier.max_batch_tokens
    chunks = chunk_requests(requests, cap)

    client = _client(gemini)
    names: list[str] = []
    for i, chunk in enumerate(chunks):
        inlined = [
            _to_inlined_request(req) for req in chunk
        ]
        display = display_name if len(chunks) == 1 else f"{display_name}-seg{i}"
        job = client.batches.create(
            model=model,
            src=inlined,
            config={"display_name": display},
        )
        if not job.name:
            raise RuntimeError(f"Batch job submission returned no name ({display})")
        names.append(job.name)
        logger.info(
            "Submitted Gemini batch job %s (%d requests, ~%d est. tokens)",
            job.name, len(chunk),
            sum(estimate_tokens(r["prompt"]) for r in chunk),
        )
    return names


def _to_inlined_request(req: dict):
    from google.genai.types import InlinedRequest

    return InlinedRequest(
        contents=req["prompt"],
        config=_generation_config(),
        metadata={"custom_key": req["custom_key"]},
    )


# ── Poll ─────────────────────────────────────────────────────────────────────


def poll(gemini: GeminiResource, job_name: str):
    """Fetch current batch job state."""
    client = _client(gemini)
    return client.batches.get(name=job_name)


def job_state(job) -> str:
    """Normalize a BatchJob.state into our status string.

    The proto enum string is ``JOB_STATE_SUCCEEDED`` etc.; strip the
    ``JOB_STATE_`` prefix so terminal checks (``_TERMINAL_OK``/``_TERMINAL_FAIL``,
    ``is_terminal``, ``retrieve``'s guard) match the bare tokens they expect.
    """
    raw = str(job.state.value) if getattr(job, "state", None) else "UNKNOWN"
    return raw[10:] if raw.startswith("JOB_STATE_") else raw


def is_terminal(state: str) -> bool:
    return state in _TERMINAL_OK or state in _TERMINAL_FAIL


# ── Retrieve ─────────────────────────────────────────────────────────────────


def retrieve(gemini: GeminiResource, job_name: str) -> dict[str, dict]:
    """Retrieve finished responses, keyed by ``custom_key``.

    Returns ``{custom_key: {"ok": bool, "text": str|None, "error": str|None}}``.
    Handles both inline responses and JSONL-file results (Gemini Developer API
    moves larger jobs to a file destination).
    """
    job = poll(gemini, job_name)
    state = job_state(job)
    if state not in _TERMINAL_OK:
        raise RuntimeError(f"Batch job {job_name} not complete (state={state})")

    dest = getattr(job, "dest", None)
    inlined = getattr(dest, "inlined_responses", None) if dest else None
    if inlined:
        return _parse_inlined(inlined)

    file_name = getattr(dest, "file_name", None) if dest else None
    if file_name:
        return _parse_result_file(gemini, file_name)

    raise RuntimeError(
        f"Batch job {job_name} succeeded but carried no inlined responses "
        "or result file name"
    )


def _parse_inlined(inlined_responses) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for resp in inlined_responses:
        custom_key = None
        if getattr(resp, "metadata", None):
            custom_key = resp.metadata.get("custom_key")
        if resp.error is not None:
            out[custom_key or "?"] = {
                "ok": False,
                "error": str(getattr(resp.error, "message", resp.error)),
            }
            continue
        text = resp.response.text if resp.response else None
        out[custom_key or "?"] = {"ok": text is not None, "text": text,
                                  "error": None if text else "empty response"}
    return out


def _parse_result_file(gemini: GeminiResource, file_name: str) -> dict[str, dict]:
    """Download + parse a JSONL result file (custom_key in each line)."""
    client = _client(gemini)
    file = client.files.download(file=file_name)
    out: dict[str, dict] = {}
    tmp_path = os.path.join(tempfile.gettempdir(), f"batch-results-{os.getpid()}.jsonl")
    file.download_to(tmp_path)
    with open(tmp_path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            meta = rec.get("metadata") or rec.get("custom_metadata") or {}
            key = meta.get("custom_key")
            if key is None:
                continue
            if rec.get("error"):
                out[key] = {"ok": False, "error": json.dumps(rec["error"])}
                continue
            text = (rec.get("response") or {}).get("text")
            out[key] = {"ok": text is not None, "text": text,
                        "error": None if text else "empty response"}
    os.unlink(tmp_path)
    return out
