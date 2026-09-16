# The dashboard image: a Vite/React build served by the FastAPI app.
#
# Two stages. The node stage builds the SPA; the python stage runs the API and
# serves the built assets. Built from the REPO ROOT context because the
# dashboard is a member of the repo uv workspace (it needs `opsdb`, and the
# workspace manifests live at the root).

# ── Stage 1: the frontend ───────────────────────────────────────────────────
FROM node:22-bookworm-slim AS frontend

WORKDIR /ui
COPY services/dashboard/package.json services/dashboard/package-lock.json ./
RUN npm ci
COPY services/dashboard/ ./
RUN npm run build

# ── Stage 2: the API + the built assets ─────────────────────────────────────
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /app

COPY pyproject.toml uv.lock ./
COPY packages/ packages/
COPY services/ services/

RUN uv sync --frozen --no-dev --package dashboard

# The SPA build served by the API. Gitignored in the source tree, so it must
# come from the node stage rather than the build context.
COPY --from=frontend /ui/dist /app/services/dashboard/dist

EXPOSE 3002

# `server.py` is a script, not a module in an installed package: run it by path.
CMD ["uv", "run", "--no-sync", "python", "services/dashboard/server.py"]
