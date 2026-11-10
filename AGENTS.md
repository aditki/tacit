# Repository Guidance

Before cross-cutting implementation work, read:

- `CONTRIBUTING.md`
- `docs/foundation-invariant-matrix.md`
- `docs/engineering-design-notes.md`
- the relevant records in `docs/adr/`

Select the applicable rows in the foundation invariant matrix and write the
matrix tests before implementation. Treat the invariants and validation matrices
as review requirements, not optional background. ADRs remain authoritative when
a living note and an accepted decision overlap.

When the same missing invariant appears in two paths, stop local patching. Update
the matrix, enumerate the complete owner/entry-point set, and implement one
shared boundary before resuming feature work.

For work that crosses event loops, worker threads, subprocesses, or runtime
instances, complete the cross-runtime lifecycle design gate in
`docs/foundation-invariant-matrix.md` before editing production code. Name the
single runtime owner, admission authority, capacity-release authority, cleanup
owner, and behavior for cancellation and loop loss. A caller-owned event loop
may transport a result, but it may not own worker capacity or be required for
cleanup. If any of those owners are ambiguous or duplicated, stop and resolve
the architecture first.

When implementation or debugging exposes a recurring workaround, unclear
ownership boundary, missing invariant, or observability gap:

1. Tell the user while the work is in progress.
2. Add focused instrumentation when it fits the requested change.
3. Update `docs/engineering-design-notes.md` when the lesson should guide future
   features.
4. Create or amend an ADR when the team is making a durable product or
   architecture decision rather than recording implementation pressure.
