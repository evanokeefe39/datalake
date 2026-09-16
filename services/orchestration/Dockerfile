# The orchestration (Dagster) image.
#
# Built from the REPO ROOT context: orchestration is a member of the repo uv
# workspace and the code location imports `opsdb`, so the image copies the
# workspace manifests plus the member trees, then syncs just this member.
#
# `dagster dev` starts the webserver AND the daemon in one process, which is
# what makes interval sensors actually tick in a container. A webserver-only
# deployment would expose the UI while nothing scheduled ever fires.
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /app

COPY pyproject.toml uv.lock ./
COPY packages/ packages/
COPY services/ services/

RUN uv sync --frozen --no-dev --package orchestration

EXPOSE 3000

# DAGSTER_HOME / IG_* are supplied by compose: this image carries no runtime
# configuration, so one image serves host-mounted and container-mounted data.
CMD ["uv", "run", "--no-sync", "dagster", "dev", "-m", "orchestration.definitions", "-h", "0.0.0.0", "-p", "3000"]
