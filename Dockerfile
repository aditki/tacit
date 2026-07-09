# syntax=docker/dockerfile:1.7

FROM ghcr.io/astral-sh/uv:0.5.31 AS uv

FROM python:3.12.13-alpine3.22 AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_PROJECT_ENVIRONMENT=/app/.venv \
    PATH="/app/.venv/bin:$PATH"

WORKDIR /app

COPY --from=uv /uv /uvx /usr/local/bin/

RUN apk upgrade --no-cache

ARG TACIT_UID=999
ARG TACIT_GID=10001

# Preserve the legacy volume-owning UID while avoiding Alpine's reserved GID 999.
RUN addgroup -S -g "${TACIT_GID}" tacit \
    && adduser -S -u "${TACIT_UID}" -G tacit -h /app -s /sbin/nologin tacit \
    && mkdir -p /app/data \
    && chown -R "${TACIT_UID}:${TACIT_GID}" /app

COPY --chown=${TACIT_UID}:${TACIT_GID} pyproject.toml uv.lock* ./
RUN uv sync --frozen --no-dev --no-install-project

COPY --chown=${TACIT_UID}:${TACIT_GID} . .
RUN uv sync --frozen --no-dev \
    && find /app -type d -name __pycache__ -prune -exec rm -rf {} +

USER tacit

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/healthz', timeout=3).read()"

CMD ["tacit", "serve", "--host", "0.0.0.0", "--port", "8000", "--no-slack"]
