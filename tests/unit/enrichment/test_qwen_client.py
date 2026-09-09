"""Unit tests for the qwen-batch HTTP client.

All httpx calls are mocked — no service is needed. Asserts exact methods,
URLs, and JSON bodies against the qwen-batch service contract.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from datalake.defs.enrichment import qwen_client

DEFAULT_QWEN_SERVICE_URL = qwen_client.DEFAULT_QWEN_SERVICE_URL
QwenServiceError = qwen_client.QwenServiceError  # noqa: kept for doc clarity
check_health = qwen_client.check_health
get_job = qwen_client.get_job
get_results = qwen_client.get_results
job_is_terminal = qwen_client.job_is_terminal
submit_job = qwen_client.submit_job


def _resp(status_code: int, json_data=None, text: str = "") -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data if json_data is not None else {}
    resp.text = text
    return resp


def test_module_import_does_not_import_httpx():
    import importlib
    import sys

    assert "httpx" not in sys.modules or sys.modules.pop("httpx") is not None
    importlib.reload(qwen_client)

    assert "httpx" not in sys.modules
    assert qwen_client.DEFAULT_QWEN_SERVICE_URL == "http://127.0.0.1:8462"


def test_check_health_ok():
    with patch("httpx.get") as get:
        get.return_value = _resp(200, {"status": "ok", "model": "qwen-vl", "version": "1"})
        out = check_health()
    get.assert_called_once_with(
        f"{DEFAULT_QWEN_SERVICE_URL}/health", timeout=5
    )
    assert out == {"status": "ok", "model": "qwen-vl", "version": "1"}


def test_check_health_connection_error_raises_loudly():
    import httpx

    with patch("httpx.get", side_effect=httpx.ConnectError("refused")):
        with pytest.raises(qwen_client.QwenServiceError) as exc:
            check_health()
    msg = str(exc.value)
    assert "qwen-batch service" in msg
    assert "uv run qwen-batch" in msg
    assert "docker compose up" in msg
    assert exc.value.base_url == DEFAULT_QWEN_SERVICE_URL


def test_check_health_non_200_raises():
    with patch("httpx.get", return_value=_resp(502, text="bad gateway")):
        with pytest.raises(qwen_client.QwenServiceError) as exc:
            check_health()
    assert "HTTP 502" in str(exc.value)
    assert exc.value.base_url == DEFAULT_QWEN_SERVICE_URL


def test_submit_job_exact_url_and_body():
    with patch("httpx.post") as post:
        post.return_value = _resp(200, {"job_id": "abc123", "total": 1})
        items = [
            {
                "custom_key": "post-1",
                "prompt": "describe",
                "images": ["/abs/path/a.jpg"],
            }
        ]
        job_id = submit_job(items=items, model="qwen2.5-vl", max_tokens=512)

    post.assert_called_once_with(
        f"{DEFAULT_QWEN_SERVICE_URL}/jobs",
        json={"items": items, "model": "qwen2.5-vl", "max_tokens": 512},
        timeout=60,
    )
    assert job_id == "abc123"


def test_submit_job_omits_max_tokens_when_none():
    with patch("httpx.post") as post:
        post.return_value = _resp(200, {"job_id": "j", "total": 0})
        submit_job([], model="qwen2.5-vl")
    assert "max_tokens" not in post.call_args.kwargs["json"]


def test_submit_job_non_2xx_raises():
    with patch("httpx.post", return_value=_resp(422, text="bad item")):
        with pytest.raises(qwen_client.QwenServiceError) as exc:
            submit_job([], model="m")
    assert "HTTP 422" in str(exc.value)
    assert exc.value.base_url == DEFAULT_QWEN_SERVICE_URL


def test_get_job_exact_url():
    with patch("httpx.get") as get:
        get.return_value = _resp(
            200,
            {"job_id": "j1", "state": "processing", "total": 5,
             "completed": 2, "failed": 0, "error": None},
        )
        out = get_job(job_id="j1")
    get.assert_called_once_with(
        f"{DEFAULT_QWEN_SERVICE_URL}/jobs/j1", timeout=60
    )
    assert out["state"] == "processing"


def test_get_job_non_2xx_raises():
    with patch("httpx.get", return_value=_resp(404, text="no job")):
        with pytest.raises(qwen_client.QwenServiceError):
            get_job(job_id="missing")


def test_get_results_returns_items_list():
    items = [{"custom_key": "k", "ok": True, "output": "text", "error": None}]
    with patch("httpx.get") as get:
        get.return_value = _resp(200, {"items": items})
        out = get_results(job_id="j2")
    get.assert_called_once_with(
        f"{DEFAULT_QWEN_SERVICE_URL}/jobs/j2/results", timeout=60
    )
    assert out == items


def test_base_url_env_override(monkeypatch):
    monkeypatch.setenv("QWEN_SERVICE_URL", "http://127.0.0.1:9999")
    with patch("httpx.get") as get:
        get.return_value = _resp(200, {"status": "ok", "model": "m", "version": "1"})
        check_health()
    assert get.call_args.args[0] == "http://127.0.0.1:9999/health"


def test_base_url_arg_beats_env(monkeypatch):
    monkeypatch.setenv("QWEN_SERVICE_URL", "http://127.0.0.1:9999")
    with patch("httpx.get") as get:
        get.return_value = _resp(200, {"status": "ok", "model": "m", "version": "1"})
        check_health("http://127.0.0.1:7000")
    assert get.call_args.args[0] == "http://127.0.0.1:7000/health"


def test_job_is_terminal_truth_table():
    assert job_is_terminal("completed") is True
    assert job_is_terminal("failed") is True
    assert job_is_terminal("pending") is False
    assert job_is_terminal("processing") is False
    assert job_is_terminal("") is False
