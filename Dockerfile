# syntax=docker/dockerfile:1.7@sha256:a57df69d0ea827fb7266491f2813635de6f17269be881f696fbfdf2d83dda33e

FROM ghcr.io/astral-sh/uv:0.5.31@sha256:7bff3c3776ec467fc1437960f2c469d8beb30f536a6465a3350c647ccd260ec2 AS uv

FROM python:3.12.13-alpine3.22@sha256:a190708a2dec1bd18b1decb539f8e8f5407abaa9bf39cacda583f7f8c11db322 AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_PROJECT_ENVIRONMENT=/app/.venv \
    PATH="/app/.venv/bin:$PATH"

WORKDIR /app

COPY --from=uv /uv /uvx /usr/local/bin/

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
