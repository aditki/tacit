# syntax=docker/dockerfile:1.7

FROM ghcr.io/astral-sh/uv:0.5.31 AS uv

FROM python:3.12.8-slim-bookworm AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_PROJECT_ENVIRONMENT=/app/.venv \
    PATH="/app/.venv/bin:$PATH"

WORKDIR /app

COPY --from=uv /uv /uvx /usr/local/bin/

RUN groupadd --system dashforge \
    && useradd --system --gid dashforge --home-dir /app --shell /usr/sbin/nologin dashforge \
    && mkdir -p /app/data \
    && chown -R dashforge:dashforge /app

COPY --chown=dashforge:dashforge pyproject.toml uv.lock* ./
RUN uv sync --frozen --no-dev --no-install-project

COPY --chown=dashforge:dashforge . .
RUN uv sync --frozen --no-dev \
    && find /app -type d -name __pycache__ -prune -exec rm -rf {} +

USER dashforge

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/healthz', timeout=3).read()"

CMD ["dashforge", "serve", "--host", "0.0.0.0", "--port", "8000", "--no-slack"]
