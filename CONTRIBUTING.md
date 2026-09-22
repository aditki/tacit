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
export API_AUTH_KEY="$(python -c 'import secrets; print(secrets.token_hex(32))')"
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

### Release quality evidence

Public releases require clean-state and representative long-lived-state
100-prompt evidence for the exact current `origin/main` tip. Configure an
ephemeral, isolated self-hosted Linux x64 runner with the `release-quality`
label and a protected `release-quality` environment. The runner must have no
ambient cloud credentials and may expose only the intended loopback LLM and
Grafana fixtures plus sanitized representative state.

Configure these environment variables:

- `RELEASE_QUALITY_LLM_URL`
- `RELEASE_QUALITY_LLM_MODEL`
- `RELEASE_QUALITY_GRAFANA_URL`
- `RELEASE_QUALITY_LONG_LIVED_STATE_DIR`
- `RELEASE_QUALITY_TENANT`

Require environment reviewers, disable self-review and administrator bypass,
and restrict deployment to `main`. Dispatch `Release Quality Evidence` from
`main` and enter `AUTHORIZE UP TO 500 LLM REQUESTS`. Both modes need at least
400 provider requests; one shared counter includes retries, repairs, and
freeform calls, and refuses request 501 before external spend. The resulting
90-day artifact records corpus, the concrete tenant, the exact logical SQLite
snapshot and fixed size limits, model, endpoint, run, request-budget, and report
digests. If `main` advances before every registry publication finishes,
discard that evidence and tag and repeat from the new tip.

## Design Guidance

Before cross-cutting work, read the relevant records in `docs/adr/` and the
[foundation invariant matrix](docs/foundation-invariant-matrix.md), then the
[living engineering design notes](docs/engineering-design-notes.md). The ADRs
capture accepted decisions; the matrix defines mandatory test-first coverage;
the living notes capture recurring invariants, refactor triggers, and
observability expectations found during implementation.

For every cross-cutting change, list the foundations and matrix rows touched in
the PR description. Write failing matrix and no-side-effect tests before the
implementation. If the same missing invariant appears in two paths, stop local
patching and introduce a shared boundary instead.

Changes involving event loops, workers, subprocesses, or runtime-owned resources
must complete the matrix's cross-runtime lifecycle design gate first. Production
implementation starts only after the owner, admission controller, permit release,
resource adoption, cleanup, cancellation, and scaling contracts have failing
tests.

When a change exposes reusable design pressure, update the living notes. When it
selects a durable product or architecture direction, create or amend an ADR.

## Security

- Do not commit API keys, tokens, `.env` files, or generated credentials.
- Do not weaken API auth or Docker hardening without calling it out in the PR.
- Keep production guidance separate from local-demo shortcuts.
- For vulnerability reporting, see `SECURITY.md`.

## Pull Requests

- Prefer focused PRs over broad rewrites.
- Include tests for behavior changes.
- Complete the foundation-matrix evidence section for cross-cutting changes.
- Label new vendor features as supported beta or experimental in docs.
- If a change touches generated dashboards, include the user-visible behavior in
  the PR description.
