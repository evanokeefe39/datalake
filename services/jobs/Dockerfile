# The jobs (inference) service image.
#
# Built from the REPO ROOT context: this service is a member of the repo uv
# workspace, so the image copies the workspace manifests and the member trees,
# then syncs just this member. `pip install .` cannot consume a uv workspace,
# which is why the previous standalone Dockerfile is not the shape here.
#
# ffmpeg is a runtime dependency: the service frame-samples video locally
# (client-side sampling replaced the File-API upload path — ADR-0009).
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml uv.lock ./
COPY packages/ packages/
COPY services/ services/

RUN uv sync --frozen --no-dev --package jobs

# Pin the store inside the mounted volume so state survives a container
# recreate; bind to all interfaces or the published port cannot reach it.
ENV QWEN_BATCH_DB=/jobs-data/state.sqlite \
    QWEN_BATCH_HOST=0.0.0.0

EXPOSE 8462

# Through `jobs.main`, never uvicorn directly: main runs the loud
# OPENROUTER_API_KEY startup check that a direct uvicorn launch would skip.
CMD ["uv", "run", "--no-sync", "python", "-m", "jobs.main"]
