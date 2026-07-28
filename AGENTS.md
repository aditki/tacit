# Repository Guidance

Before cross-cutting implementation work, read:

- `CONTRIBUTING.md`
- `docs/engineering-design-notes.md`
- the relevant records in `docs/adr/`

Treat the invariants and validation matrices in the engineering design notes as
review requirements, not optional background. ADRs remain authoritative when a
living note and an accepted decision overlap.

When implementation or debugging exposes a recurring workaround, unclear
ownership boundary, missing invariant, or observability gap:

1. Tell the user while the work is in progress.
2. Add focused instrumentation when it fits the requested change.
3. Update `docs/engineering-design-notes.md` when the lesson should guide future
   features.
4. Create or amend an ADR when the team is making a durable product or
   architecture decision rather than recording implementation pressure.

