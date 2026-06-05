# CONTRIBUTING.md

# Contributing to DashForge

Thanks for your interest in DashForge.

This repository is currently an early-stage infrastructure/LLM tooling project focused on experimentation, architecture exploration, and learning.

## Project Goals

DashForge exists primarily to explore:

* LLM-assisted observability workflows
* Grafana dashboard automation
* Multi-agent orchestration patterns
* Infrastructure developer tooling
* Metrics/query generation systems
* Human-in-the-loop feedback pipelines

The project is also intended as a public engineering portfolio project.

## Current Status

DashForge is **not production-ready**.

Expect:

* breaking changes
* incomplete integrations
* evolving APIs
* changing configuration formats
* rough edges in UX and operational reliability

Contributions are welcome, but stability guarantees do not yet exist.

## Ways to Contribute

Helpful contributions include:

* bug reports
* datasource integration improvements
* documentation fixes
* CLI usability improvements
* testing and validation
* query validation improvements
* architecture discussions
* performance profiling
* security hardening ideas

## Development Setup

```bash
git clone https://github.com/aditki/dashforge.git
cd dashforge

uv sync
cp .env.example .env

uv run -m dashforge.cli init
uv run -m dashforge.cli doctor
```

Run the API locally:

```bash
uv run -m dashforge.main
```

Run tests:

```bash
uv run pytest
```

## Contribution Style

Please try to:

* keep changes focused and well-scoped
* include clear commit messages
* avoid unrelated refactors in feature PRs
* add/update tests where practical
* document new configuration fields

## Security

If you discover a security issue, please avoid filing a public issue.

See `SECURITY.md` for disclosure guidance.

## Philosophy

The repository intentionally prioritizes:

* clarity over abstraction
* experimentation over perfection
* fast iteration over long-term API stability

Over time, parts of the project may stabilize into more production-oriented patterns.
