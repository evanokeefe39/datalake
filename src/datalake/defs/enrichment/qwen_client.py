"""HTTP client for the standalone qwen-batch service.

Thin sync-httpx client against the qwen-batch service contract:

- POST /jobs               {"items": [...], "model": str, "max_tokens": int|omit}
- GET  /jobs/{job_id}      -> {"job_id", "state", "total", "completed", "failed", "error"}
- GET  /jobs/{job_id}/results -> {"items": [{"custom_key", "ok", "output", "error"}]}
- GET  /health             -> {"status": "ok", "model": str, "version": str}

The service (~/repos/qwen-batch-service) does ALL media handling server-side,
including ffmpeg frame-sampling of video files. This client never runs ffmpeg
and never produces video frames — it only submits absolute local file paths.

httpx is imported lazily inside functions so importing this module has no
service/network dependency and no import-time side effects.
"""

from __future__ import annotations

import os
from typing import Any

DEFAULT_QWEN_SERVICE_URL = "http://127.0.0.1:8462"

_TERMINAL_STATES = frozenset({"completed", "failed"})

_SERVICE_REMEDY = (
    "start it: uv run qwen-batch in ~/repos/qwen-batch-service, "
    "or docker compose up"
)


class QwenServiceError(RuntimeError):
    """Raised when the qwen-batch service is unreachable or returns an error."""

    def __init__(self, message: str, *, base_url: str) -> None:
        super().__init__(message)
        self.base_url = base_url


def _resolve_base_url(base_url: str | None) -> str:
    if base_url:
        return base_url.rstrip("/")
    return os.environ.get("QWEN_SERVICE_URL", DEFAULT_QWEN_SERVICE_URL).rstrip("/")


def _service_unavailable(base_url: str, detail: str) -> QwenServiceError:
    return QwenServiceError(
        f"qwen-batch service at {base_url} is unreachable ({detail}) — {_SERVICE_REMEDY}",
        base_url=base_url,
    )


def check_health(
    base_url: str | None = None, timeout_s: float = 5
) -> dict[str, Any]:
    """GET /health. Raises :class:`QwenServiceError` loudly when down."""
    import httpx

    url = _resolve_base_url(base_url)
    try:
        resp = httpx.get(f"{url}/health", timeout=timeout_s)
    except httpx.HTTPError as exc:
        raise _service_unavailable(url, type(exc).__name__) from exc
    if resp.status_code != 200:
        raise _service_unavailable(url, f"HTTP {resp.status_code}")
    return resp.json()


def submit_job(
    base_url: str | None = None,
    items: list[dict[str, Any]] | None = None,
    *,
    model: str,
    max_tokens: int | None = None,
    timeout_s: float = 60,
) -> str:
    """POST /jobs with ``items``; return the job_id.

    Each item is ``{"custom_key": str, "prompt": str, "images": [abs paths]}``.
    Images may be native image paths OR video file paths — the service frames
    videos itself; this client never runs ffmpeg.
    """
    import httpx

    url = _resolve_base_url(base_url)
    body: dict[str, Any] = {"items": items or [], "model": model}
    if max_tokens is not None:
        body["max_tokens"] = max_tokens
    try:
        resp = httpx.post(f"{url}/jobs", json=body, timeout=timeout_s)
    except httpx.HTTPError as exc:
        raise _service_unavailable(url, type(exc).__name__) from exc
    if not (200 <= resp.status_code < 300):
        raise QwenServiceError(
            f"POST /jobs failed: HTTP {resp.status_code} from {url}: "
            f"{resp.text[:500]}",
            base_url=url,
        )
    job_id = resp.json().get("job_id")
    if not job_id:
        raise QwenServiceError(
            f"POST /jobs returned no job_id: {resp.text[:500]}",
            base_url=url,
        )
    return str(job_id)


def get_job(
    base_url: str | None = None, *, job_id: str, timeout_s: float = 60
) -> dict[str, Any]:
    """GET /jobs/{job_id}; return the status document."""
    import httpx

    url = _resolve_base_url(base_url)
    try:
        resp = httpx.get(f"{url}/jobs/{job_id}", timeout=timeout_s)
    except httpx.HTTPError as exc:
        raise _service_unavailable(url, type(exc).__name__) from exc
    if not (200 <= resp.status_code < 300):
        raise QwenServiceError(
            f"GET /jobs/{job_id} failed: HTTP {resp.status_code} from {url}",
            base_url=url,
        )
    return resp.json()


def get_results(
    base_url: str | None = None, *, job_id: str, timeout_s: float = 60
) -> list[dict[str, Any]]:
    """GET /jobs/{job_id}/results; return the items list."""
    import httpx

    url = _resolve_base_url(base_url)
    try:
        resp = httpx.get(f"{url}/jobs/{job_id}/results", timeout=timeout_s)
    except httpx.HTTPError as exc:
        raise _service_unavailable(url, type(exc).__name__) from exc
    if not (200 <= resp.status_code < 300):
        raise QwenServiceError(
            f"GET /jobs/{job_id}/results failed: HTTP {resp.status_code} "
            f"from {url}",
            base_url=url,
        )
    return resp.json().get("items", [])


def job_is_terminal(state: str) -> bool:
    """True when ``state`` is a terminal job state (completed or failed)."""
    return state in _TERMINAL_STATES
