# syntax=docker/dockerfile:1.7@sha256:a57df69d0ea827fb7266491f2813635de6f17269be881f696fbfdf2d83dda33e

FROM ghcr.io/astral-sh/uv:0.5.31@sha256:7bff3c3776ec467fc1437960f2c469d8beb30f536a6465a3350c647ccd260ec2 AS uv

FROM python:3.12.14-alpine3.24@sha256:1887c114801a8c82a4ec01daa52cfe7fc3f63573640e2247320289807ac1c3bb AS runtime

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
RUN --mount=type=cache,target=/root/.cache/uv,sharing=locked \
    uv sync --locked --link-mode=copy --no-dev --extra bedrock --no-install-project

COPY --chown=${TACIT_UID}:${TACIT_GID} . .
RUN --mount=type=cache,target=/root/.cache/uv,sharing=locked \
    uv sync --locked --link-mode=copy --no-dev --extra bedrock \
    && find /app/.venv -type f -path '*/tacit_ai-*.dist-info/RECORD' \
        -exec sed -i '/uv_cache\.json,/d' {} + \
    && find /app/.venv -type f -path '*/tacit_ai-*.dist-info/uv_cache.json' -delete \
    && find /app -type d -name __pycache__ -prune -exec rm -rf {} +

USER tacit

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD ["python", "/app/tacit/api/routes/system.py"]

CMD ["tacit", "serve", "--host", "0.0.0.0", "--port", "8000", "--no-slack"]
