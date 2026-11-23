# Local Tev evaluation, 2026-11-18

Actual local Tev inference completed: **36 of 36 planned decisions returned
valid responses, with no retries**. This establishes a working local collection,
wire-conformance and file-only scoring path. It does not establish improved
Tacit investigation quality or justify production classifier adoption.

## Scope and identity

The frozen development corpus contains 14 author-written, author-labeled
fictional investigations: 12 model-eligible cases, one captured policy abstention
and one deterministic single-candidate outcome. Primary, rotated and reworded
packets each produced 12 calls. Labels and deliberately varied stored baseline
orders were frozen before collection. The stored baseline's 6/14 result is an
authored fixture property, **not measured Tacit performance**. These are neither
independently adjudicated incidents nor the planned 100-incident held-out set.

The source-format 1.0 captures use legacy `runtime_evidence` strings with empty
requirement/resolution/observation arrays. The readings are fiction; no live
query, acquisition or production grounding verification is claimed.

Collection used official `tev1:4b` on Ollama **0.40.15**, MLX/safetensors,
`mxfp8`, `num_ctx=2048`, temperature 0. Requested and returned model tags agreed.
The pinned API artifact identity is
`4d24c6f6d61a48d9b72902f4805c5f1da53010736b834b0372377fb4bb7e53d3`;
the selected local runner manifest is
`8521f91c5839fb00aa5ab10bea2a4f0dd45d492220c394891850fed85779ad73`.
All 436 selected blobs, totaling 4,361,661,088 bytes, passed SHA-256 verification.
The distribution's Apache-2.0/MIT texts and upstream license wording discrepancy
are recorded in `collection-identity.json`. Weights were downloaded for this run;
the model loaded by collection was unloaded afterward, and `ollama ps` was empty.

One additional generic conformance probe preceded the frozen quality run:
**37 actual local calls total, 36 included in the experiment**. No hosted Jev
call occurred. The manifest's `not-collected-jev-v1` identity is an explicit
unattested placeholder, not a claim about a real hosted version.

## Results

| Measure | Primary | Rotated options | Reworded instructions |
| --- | ---: | ---: | ---: |
| Trustworthy outcomes, including common no-calls | 13/14 | 12/14 | 13/14 |
| Trustworthy outcomes among actual model calls | 11/12 | 10/12 | 11/12 |
| Required-abstention recall, including common policy abstention | 3/3 | 3/3 | 3/3 |
| Unsafe choices / unsafe attempts | 0 / 0 | 0 / 0 | 0 / 0 |
| Singleton multiclass Brier score | 0.159676 | 0.196136 | 0.186200 |

Each calibration score covers 11 eligible singleton-label cases, excluding the
two common no-calls and one multi-acceptable case. Primary abstention precision
was 3/4: Tev over-abstained on `case_08_contradicted_deploy`, where the author
label preferred inspecting the recorded database lock waits over deployment
coincidence. All three variants made that error. Rotation additionally abstained
on `case_12_two_observed_paths`, where either observed path was acceptable.
Rotation changed the choice on 1/12 comparable cases; wording changed 0/12.
Variants are paired robustness views, not additional independent incidents.

The 36 decisions reported 22,512 input tokens and zero generated output tokens;
the largest reported input was 647 tokens. Summed request elapsed time was
8,819 ms; primary mean latency was 284.67 ms over 12 calls. These are local-run
measurements, not a hardware-independent latency guarantee.

The descriptive primary difference from the authored baseline was +0.500000,
with the scorer's paired interval [0.214286, 0.785714]. This comparison cannot
establish superiority over Tacit because its baseline and labels were authored.
`comparisons.tev.gate_passed` is **false**: this is 14 development cases, not the
required independent held-out evaluation. No deployment or code-removal decision
follows from this run.

## Conformance and verification

The real response included optional `prompt_eval_cached_count`, which the old
closed envelope correctly rejected. The explicit
`choice-wire-ollama-0.40.15-v1` profile admits only that nonnegative integer
metadata for the pinned Tev server; it preserves actual response bytes and keeps
the original `choice-wire-v1` goldens and Jev envelope unchanged. Fourteen new
tests ran red before the repair; all **79 focused tests pass**.

The final repository run passed **4,531 tests**, with 12 skipped and 72
deselected. Ruff, Black (350 Python files), mypy (169 production files) and diff
checks passed. No production runtime, grounding policy or ranking arithmetic was
changed. Deterministic Grounding (40 cases) and Operational Learning (18/18)
also passed. File-only scoring was repeated with socket, database and subprocess
creation trapped; the reports were byte-identical, including semantic SHA-256
`0cd20742b52cf582e4767dd32deb70371593819f1a30aeae407f867878870222`.

Tev collection is complete at 36/36. The two-arm experiment remains incomplete:
all 36 Jev receipts are intentionally missing, so scoring returns **exit 1**.
The emitted `complete.json` markers mean the file-only artifact computation
finished; they do not certify receipt coverage, quality or paired collection.
No fake Jev receipts or Tev answers were substituted for the missing arm.

## Portable evidence and replay

The accompanying
[`tev_ollama_20261118.tar.gz`](../../tests/eval/fixtures/tev_ollama_20261118.tar.gz)
contains fictional bundles, author labels, frozen manifest and requests, exact
raw responses, receipts, identities and score artifacts. Its 206 regular files
expand to 626,826 bytes; the compressed artifact is 121,234 bytes, SHA-256
`bb297f0cb340b35e78fcdb1e293be88249c7631701e08fd49745cf8354866403`.
It contains no model weights or production data. Its frozen corpus digest is
`d38840d2b4229e6ebe43aaae031a18b62b1ebc00038bf8681eea7dd4ef87e590`.

From the repository root, extract the reviewed fixture into a fresh directory
and run the existing file-only driver; no Ollama process or new inference is
needed for replay:

```bash
tev_replay_root="$(uv run python -c 'from pathlib import Path; import tempfile; print(Path(tempfile.mkdtemp(prefix="tacit-tev-replay-")).resolve())')"
tar -xzf tests/eval/fixtures/tev_ollama_20261118.tar.gz -C "$tev_replay_root"
uv run python tests/eval/evidence_decisions.py prepare \
  --manifest "$tev_replay_root/tev_ollama_20261118/experiment/manifest.json" \
  --out "$tev_replay_root/prepared_again"
uv run python tests/eval/evidence_decisions.py score \
  --manifest "$tev_replay_root/tev_ollama_20261118/experiment/manifest.json" \
  --responses "$tev_replay_root/tev_ollama_20261118/responses" \
  --out "$tev_replay_root/scored_again"
```

The score command's expected exit status is 1 for the missing Jev arm; inspect
the Tev counts and the semantic digest above. Bounded extraction into a fresh
directory and replay of this portable artifact reproduced the identical report
under the same external-capability traps.

The updated Linux ARM64 container build passed as `tacit:offline-tev-local`.
Its archived run replayed to the same report hash as UID 999 in a read-only,
network-disabled container with socket, database and subprocess creation trapped.
An independent audit verified archive paths/bounds, frozen labels, exact raw and
receipt hashes, model identity, missing Jev recordings and publishable contents.
