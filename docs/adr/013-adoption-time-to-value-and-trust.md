# ADR-013: Adoption depends on time-to-value and trust, not more agents

## Status

Accepted

## Context

Tacit is public-beta/early-alpha. Adoption risk is dominated by whether users understand the problem, can run a demo,
trust the output, and see value before heavy configuration. The repo already has a dev compose stack, fake metrics app,
doctor/test/serve CLI commands, API docs, evaluation docs, and public-beta warnings.

The repo does not yet have a single `tacit demo` command or README screenshots/GIFs, and functional demo hardening is
still listed as current focus.

## Decision

Near-term adoption should prioritize time-to-value and trust: local demo setup, clear README, visible evaluation results,
explainable learning/approval flows, query validation, feedback, and screenshots or repeatable demo evidence. Avoid
spending immediate cycles on multi-agent swarms, autonomous remediation, or complex incident-management integrations.

## Consequences

- Demo flows should be boring to run and honest about credentials/safety.
- Docs should keep explicit public-beta warnings.
- Quality gates should include E2E demo and learning-loop tests.
- New agent complexity should justify itself by improving trust or time-to-value.

## Implementation Notes

Implementation status: partially implemented.

Validated against:

- `README.md`: includes early project warnings, quickstart, Docker dev stack, support matrix, and evaluation/doc links.
- `docker-compose.dev.yml`: provides a local dev/demo stack with intentionally unsafe local Grafana defaults.
- `tacit/cli.py`: supports setup/doctor/test/serve/history commands.
- `docs/evaluation.md`: provides a public validation report.
- `tests/e2e/`: contains opt-in E2E scenarios for learning and API surface.
- `README.md` current focus still includes functional demo hardening.

TODO:

- Add a true `tacit demo` path or document the current local demo as a single repeatable recording flow.
- Add README screenshots/GIFs for generated dashboard, learning/approval output, and evaluation results.

