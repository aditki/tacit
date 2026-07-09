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

RUN addgroup -S tacit \
    && adduser -S -G tacit -h /app -s /sbin/nologin tacit \
    && mkdir -p /app/data \
    && chown -R tacit:tacit /app

COPY --chown=tacit:tacit pyproject.toml uv.lock* ./
RUN uv sync --frozen --no-dev --no-install-project

COPY --chown=tacit:tacit . .
RUN uv sync --frozen --no-dev \
    && find /app -type d -name __pycache__ -prune -exec rm -rf {} +

USER tacit

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/healthz', timeout=3).read()"

CMD ["tacit", "serve", "--host", "0.0.0.0", "--port", "8000", "--no-slack"]
