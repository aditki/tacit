# Contributing

Thanks for taking a look at Tacit. This repository is in public beta, so the
bar for contributions is practical: keep changes small, testable, and honest
about what is supported versus experimental.

## Setup

```bash
uv sync --all-extras --dev
uv run tacit --version
```

For local demos:

```bash
cp .env.example .env
docker compose -f docker-compose.dev.yml up -d
```

The dev Compose stack is local-only and intentionally uses unsafe Grafana demo
defaults.

## Checks

Run these before opening a PR:

```bash
uv run ruff check .
uv run black --check .
uv run mypy tacit
uv run pytest -q
docker build -t tacit:local .
```

Live vendor scripts under `tests/live/` are not part of the hermetic test suite.
Run them only against accounts and dashboards you are allowed to mutate.

## Security

- Do not commit API keys, tokens, `.env` files, or generated credentials.
- Do not weaken API auth or Docker hardening without calling it out in the PR.
- Keep production guidance separate from local-demo shortcuts.
- For vulnerability reporting, see `SECURITY.md`.

## Pull Requests

- Prefer focused PRs over broad rewrites.
- Include tests for behavior changes.
- Label new vendor features as supported beta or experimental in docs.
- If a change touches generated dashboards, include the user-visible behavior in
  the PR description.
