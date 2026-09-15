"""Minimal Apify API client — trigger, poll, stream.

Extracted from the legacy ``ig_pipeline`` repo so the datalake has no
dependency on a local checkout. Only the three functions the pipeline uses
are kept; actor discovery/inspection were not ported.

All functions take an explicit ``token`` parameter (never a global).
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

log = logging.getLogger(__name__)

API_BASE = "https://api.apify.com/v2"
DEFAULT_TIMEOUT = 30


@dataclass
class RunInfo:
    """Result of triggering an Apify actor run."""

    run_id: str
    dataset_id: str | None = None
    actor: str = ""
    estimated_cost_usd: float = 0.0


def _is_retryable(exception: BaseException) -> bool:
    """True for transient errors that should be retried.

    Retry on 429, 5xx, and network/connect/timeout errors. Never on 4xx.
    """
    if isinstance(exception, httpx.TimeoutException | httpx.NetworkError):
        return True
    if isinstance(exception, httpx.HTTPStatusError):
        status = exception.response.status_code
        return status == 429 or 500 <= status < 600
    return False


@retry(
    retry=retry_if_exception(_is_retryable),
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    reraise=True,
)
def _get(path: str, token: str, **params: Any) -> dict[str, Any]:
    url = f"{API_BASE}/{path.lstrip('/')}"
    params["token"] = token
    resp = httpx.get(url, params=params, timeout=DEFAULT_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()
    if data.get("error"):
        raise RuntimeError(f"Apify API error: {data['error']}")
    return data.get("data", data)


@retry(
    retry=retry_if_exception(_is_retryable),
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    reraise=True,
)
def _post(path: str, token: str, body: Any = None, **params: Any) -> dict[str, Any]:
    url = f"{API_BASE}/{path.lstrip('/')}"
    params["token"] = token
    resp = httpx.post(url, params=params, json=body, timeout=DEFAULT_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()
    if data.get("error"):
        raise RuntimeError(f"Apify API error: {data['error']}")
    return data.get("data", data)


@retry(
    retry=retry_if_exception(_is_retryable),
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    reraise=True,
)
def _stream_get(url: str, **params: Any) -> httpx.Response:
    """GET with retries, returning the raw response for streaming."""
    resp = httpx.get(url, params=params, timeout=60)
    resp.raise_for_status()
    return resp


def trigger_run(
    actor: str,
    urls: list[str],
    *,
    token: str,
    results_limit: int = 1,
    results_type: str = "posts",
    max_charge_usd: float | None = None,
) -> RunInfo:
    """Start an actor run. Returns immediately with run_id and dataset_id.

    The dataset_id is available before the run finishes — Apify creates it
    up front. Use ``poll_run()`` to wait for completion.
    """
    body = {
        "directUrls": urls,
        "resultsType": results_type,
        "resultsLimit": results_limit,
        "proxy": {"useApifyProxy": True},
    }
    query: dict[str, Any] = {}
    if max_charge_usd is not None:
        query["maxTotalChargeUsd"] = max_charge_usd
    result = _post(f"acts/{actor}/runs", token, body=body, **query)
    run_id = result["id"]
    dataset_id = result.get("defaultDatasetId")
    cost = result.get("stats", {}).get("estimatedTotalPriceUsd", 0.0)
    log.info("Triggered run %s (dataset %s, est $%.4f)", run_id, dataset_id, cost)
    return RunInfo(
        run_id=run_id,
        dataset_id=dataset_id,
        actor=actor,
        estimated_cost_usd=cost,
    )


def poll_run(run_id: str, *, token: str, poll_secs: int = 5, timeout: int = 600) -> str:
    """Poll until run completes. Returns dataset_id on SUCCEEDED.

    Raises RuntimeError on failure or timeout.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        result = _get(f"actor-runs/{run_id}", token)
        status = result.get("status")
        dataset_id = result.get("defaultDatasetId") or result.get("dataset", {}).get("id")
        if status == "SUCCEEDED":
            log.info("Run %s succeeded, dataset %s", run_id, dataset_id)
            return dataset_id
        if status in ("FAILED", "ABORTED", "TIMED-OUT"):
            raise RuntimeError(f"Run {run_id} {status}: {result.get('errorMessage', '')}")
        log.debug("Run %s status: %s", run_id, status)
        time.sleep(poll_secs)
    raise RuntimeError(f"Run {run_id} timed out after {timeout}s")


def stream_dataset(dataset_id: str, dest: Path, *, token: str) -> int:
    """Download dataset to ``dest`` as NDJSON. Single request, no pagination.

    Uses ``format=json`` (JSON array) to avoid Apify's NDJSON newline bug.
    Parses the array, writes one JSON object per line.
    """
    import json as _json

    url = f"{API_BASE}/datasets/{dataset_id}/items"
    resp = _stream_get(url, format="json", token=token)
    items = _json.loads(resp.text)
    with open(dest, "w", encoding="utf-8") as f:
        for item in items:
            f.write(_json.dumps(item, ensure_ascii=False) + "\n")
    log.info("Streamed %d items to %s", len(items), dest)
    return len(items)
