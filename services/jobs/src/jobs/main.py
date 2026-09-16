"""uvicorn entrypoint: `uv run jobs.main` or `python -m jobs.main`."""
from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

import uvicorn


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    if not os.environ.get("OPENROUTER_API_KEY"):
        print(
            "FATAL: OPENROUTER_API_KEY is not set. Export it before starting "
            "qwen-batch-service (the worker will fail every item without it).",
            file=sys.stderr,
        )
        raise SystemExit(2)

    db = os.environ.get("QWEN_BATCH_DB") or str(Path.home() / ".qwen-batch" / "state.sqlite")
    os.environ["QWEN_BATCH_DB"] = db  # pin it so app.py's Store uses the same path

    uvicorn.run(
        "jobs.app:app",
        host=os.environ.get("QWEN_BATCH_HOST", "127.0.0.1"),
        port=int(os.environ.get("QWEN_BATCH_PORT", "8462")),
        log_level="info",
    )


if __name__ == "__main__":
    main()
