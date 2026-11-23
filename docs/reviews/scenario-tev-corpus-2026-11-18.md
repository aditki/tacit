# Recorded scenario baseline and local Tev evaluation, 2026-11-18

The existing ClickStack/GAMMA scenarios yield **nine actual assessment captures
and four failed investigations across 13 distinct source recordings**. All nine
captures have exactly one admissible candidate. The frozen paired ranking
protocol therefore makes **zero model calls**. This corpus does not establish
that Tev improves investigation ranking or justify production adoption.

## Corpus and baseline

Source selection, family grouping, windows, metric transformations and label
rules froze before inference. The source plan's SHA-256 is
`ab0b016ffe90185694e34e9fb806d24942bda689d1eb1b01721f996f0502058f`.
The 13 recordings form five related families: one ClickStack cache scenario and
CPU, memory, network and mixed CPU/memory GAMMA families. Each GAMMA family has
a fault, pre-interference healthy and symptom-only recording. These are distinct
source recordings, not independent production incidents or repeated prompt
trials counted as new incidents. All data is controlled demo/dataset material.

Tacit's actual baseline used acquisition branch commit
`1af49fd1ef552446ff9856164e0d9811f5d76e85` and local Ollama
`qwen3-coder:30b-a3b-q4_K_M`, artifact
`06c1097efce0431c2045fe7b2e5108366e43bee1b4603a7aded8f21689e90bca`,
server `0.40.15`. Every recording used the same neutral prompt, fresh cold
runtime state and freshly recreated disposable telemetry volumes. The setup
did not teach or approve a dashboard containing an expected answer. The offline
evaluation branch does not import the acquisition implementation.

| Cohort | Recordings | Actual captures | Ranking choices with 2+ candidates |
| --- | ---: | ---: | ---: |
| ClickStack | 1 | 1 | 0 |
| GAMMA injected-resource windows | 4 | 4 | 0 |
| GAMMA pre-interference healthy | 4 | 4 | 0 |
| GAMMA application-only/resource-absent | 4 | 0 | 0 |
| Total | 13 | 9 | 0 |

All four application-only investigations returned failed status and no public
assessment bundle after fresh data passed import/readiness probes. They stay in
the coverage denominator. Their authoritative candidate labels are `null`; no
contract, candidate or successful abstention is manufactured. The paired
manifest's required empty label object and all-zero missing-bundle digest are
explicit schema sentinels. The missing file is scored as failed coverage.

## Replay clock and ownership

The first attempt incorrectly rebased every recording to one common clock.
Later recordings aged past the production validator's five-minute window. The
entire 13-recording timing-invalid cohort is retained locally and excluded,
including its early successful captures. It made no Tev calls. It is not counted
as a product failure or pooled with the corrected cohort.

The corrected cohort used `per-case-admission-clock/1`: source windows, values
and label rules remained fixed; immediately before each case, one constant
timestamp shift put the latest sample five seconds before admission. Each
resolved input and its hash froze before import and the baseline call. Instant
and five-minute readiness probes passed for all 13 cases; baseline starts were
less than 60 seconds after the newest sample. Tests verify timestamp-only
changes and reject stale admission. Production query validation was unchanged.

The existing cold-isolation runtime remained the sole runtime owner. Existing
admission, capacity-release and cleanup authorities stayed in place. All 13
runtime roots closed with every tracked owner/in-flight/retained count at zero
before the next reset. Final teardown verified the exact experiment ownership
nonce, container IDs, images, volumes and network, then removed only the
experiment's resources. Other Docker projects and the pre-existing Qwen model
were untouched.

All nine bundles were exported through the public assessment writer and replayed
with the exact acquisition implementation. Input/output fingerprints, decision
logs and provenance matched with network, database and subprocess capabilities
denied. The portable file-only experiment re-derives choices from these bundles,
without requiring the original stores or a model server.

## Labels and model-visible evidence

Labels froze after baseline capture and before any Tev response. They follow the
predeclared next-inspection rubric and actual captured observation links. Neither
hidden injected faults nor baseline rank, score or confidence supply label
truth. This is author/agent adjudication, not independent human adjudication.

All nine captures require `insufficient` under that rubric: they expose query
validity, nonemptiness, survival and metric/query identities, but no measured
abnormal condition or comparative fact justifying priority. ClickStack captured
generic Redis observations without the predeclared Visa-cache observation.
GAMMA collapsed CPU/memory availability into a single “Runtime Resources”
candidate, including the network cases. All eight GAMMA fault/healthy visible
states are byte-identical. The corpus therefore has only **two unique visible
states** despite its nine valid captures.

The unchanged paired policy carries the real one-candidate baseline forward for
both model arms without calling either model. Those common deterministic rows
are not model successes. All failed captures earn zero quality. The development
recommendation gate remains false; no Jev inference was collected.

The paired report records 9/13 coverage and 0/13 trustworthy priority choices
for the baseline and both carried-through arms. Its `unsafe: 9` field means nine
candidate selections conflict with required priority abstention under this
rubric; it is not nine Tev calls or nine proven causal assertions. The four
missing-capture sentinels remain failed rows, including in the driver's
13-case abstention denominator. `prepare` and `score` both return exit 1 for
these retained capture failures while finalizing bounded audit reports.

## Separate abstention diagnostic

To fulfill the local Tev run without changing the agreed no-call policy, a
separately frozen `scenario-abstention-diagnostic/1` protocol asks whether the
one real captured candidate is justified or the evidence is insufficient.
This is an offline probe of abstention behavior, not a paired ranking study.
It adds no distractor candidates and never changes production grounding,
publication, replay or safety policy.

The probe uses native Ollama `/v1/systemone`, exact frozen request bytes and
`tev1:4b`, artifact
`4d24c6f6d61a48d9b72902f4805c5f1da53010736b834b0372377fb4bb7e53d3`,
server `0.40.15`. The selected local MLX manifest and serving metadata are
recorded separately; the existing wire profile admits only this server's
optional nonnegative integer cache-token counter. No generic chat substitute,
response-field stripping or production integration is used.

Collection made **nine HTTP submissions, no retries**. Eight GAMMA responses
were valid; all eight selected the resource candidate instead of the frozen
`insufficient` label. The ClickStack submission was rejected with HTTP 400:
2,462 input tokens exceeded the model's 2,048-token limit. Its original raw
error and receipt remain recorded; input was not truncated or retried. The
diagnostic scoring command returns exit 1 for that external failure.

| Separate diagnostic outcome | Count |
| --- | ---: |
| HTTP submissions | 9 |
| Valid model responses | 8 |
| Correct insufficient choices | 0 |
| Unsupported-priority label errors | 8 |
| Rejected before model response | 1 |
| Unique submitted inputs / inputs with valid responses | 2 / 1 |

The frozen diagnostic digest is
`95518dfe39afe4a306835000509fd809588c379ec07ff70e3615f72a16efecaa`;
report semantic SHA-256 is
`346c3fb5c12ec0b6bb9d0e39b35e3f2498f394dd7d34aeaf58ef61eea7ea3e35`,
and exact report bytes hash to
`de53684f7b1d775802fd70107d325192bf7aada9fdcc4beccfbe5dd98b992c78`.
The original collector's `actual_inference_calls: 9` field counts HTTP
submissions, not nine completed model executions. A separate accounting record
states the eight successful responses and one pre-response rejection.
Seven responses report nonzero cached-prompt tokens. Those serving counters do
not turn repeated identical inputs into independent evidence. Total reported
usage for valid responses is 11,344 input tokens and zero output tokens.

The collection-start ledger binds the frozen labels, protocol, model, manifest
and source/index hashes before the first submission. The collector rechecked
those bindings after the final response. Its 600-second budget controls new
request admission, and its 60-second timeout controls socket operations; neither
claims authority to cancel or retire an Ollama worker. The existing server
retains that authority. No timeout or uncertain settlement occurred.

There are no positive labels, only two unique states, related dataset families
and no independent held-out incidents. Probe calls cannot establish ranking
improvement, calibration, root-cause accuracy or production readiness. An
unsupported candidate choice under this probe is a priority-label error, not
automatically a causal assertion.

## Published evidence and source terms

The bounded portable artifact retains immutable manifests, labels, real bundles,
failed-run audits, receipts, clock admissions, scores and operator source. It
contains no model weights, runtime databases, private credentials or unrelated
host/model inventories. The full frozen replay inputs remain in the local
experiment directory.

GAMMA excerpts are attributed to Gagan Somashekar's version-1 dataset, whose
[publisher metadata](https://www.kaggle.com/api/v1/datasets/view/gagansomashekar/microservices-bottleneck-detection-dataset)
declares Apache 2.0. The artifact includes the license, source hashes, requested
[GAMMA paper citation](https://doi.org/10.1145/3589334.3645665) and transformation
notices. The [ClickStack guide](https://clickhouse.com/docs/clickstack/example-datasets/sample-data)
offers its sample for local loading but does not establish a dataset
redistribution license. Raw ClickStack metric replays are therefore retained
locally; published metadata includes hashes, a reconstruction recipe and an
explicitly local transform attestation. Portable decision/bundle replay needs
neither raw source dataset. ClickStack timestamp transformation cannot be
independently rechecked from omitted bytes in the portable artifact.

Artifact: `tests/eval/fixtures/scenario_tev_corpus_v1.tar.gz`, 1,241,133 bytes,
338 regular USTAR members, SHA-256
`31a8252ded5464f7fb4733b615d89012fff424e796cd22b881bdbafbb805ccc6`.
The paired report's semantic SHA-256 is
`84a94a9cc5f5efb5d07b87c09d8e965883b2de7ba450e90f002fb62195f1e07d`;
exact report bytes hash to
`e9c57cdbb7f027e3ec2dea74cd094fd0561278e2cb6775d9987a757f54c4eb08`.

Recheck the bounded archive, source/freeze bindings, exact bundle identities and
two independent file-only score replays without starting a model:

```bash
uv run pytest -q tests/unit/test_scenario_evidence_corpus.py
```

To inspect the frozen reports directly, extract the checked-in artifact into a
fresh directory and run the file-only drivers from the repository root:

```bash
scenario_replay_dir="$(uv run python -c 'import tempfile; print(tempfile.mkdtemp(prefix="tacit-scenario-replay-"))')"
tar -xzf tests/eval/fixtures/scenario_tev_corpus_v1.tar.gz -C "$scenario_replay_dir"
uv run python tests/eval/evidence_decisions.py score \
  --manifest "$scenario_replay_dir/scenario_tev_corpus_v1/experiment/manifest.json" \
  --responses "$scenario_replay_dir/scenario_tev_corpus_v1/responses" \
  --out "$scenario_replay_dir/paired_again"
uv run python "$scenario_replay_dir/scenario_tev_corpus_v1/shadow_operator/shadow_diagnostic.py" score \
  --manifest "$scenario_replay_dir/scenario_tev_corpus_v1/shadow/prepared/manifest.json" \
  --responses "$scenario_replay_dir/scenario_tev_corpus_v1/shadow/responses" \
  --out "$scenario_replay_dir/shadow_again"
```

The paired command's exit 1 retains the four missing captures; it is expected.
The separate diagnostic's exit 1 preserves the rejected ClickStack request.
Its status describes receipt validity, not adoption.
Neither scoring command contacts Ollama or regenerates a baseline.

## Verification

The corpus gates were written red before artifact creation, and clock guards
were written red before the per-case operator repair. Final checks passed:

- Existing unit suite: 4,531 passed, 12 skipped, 72 integration/e2e cases
  deselected. The eight new frozen-corpus gates passed separately; combined
  unit coverage is 4,539 passing tests. Six separate pre-inference operator
  boundary checks passed. Existing production integration/e2e evidence remains
  recorded with each implementation branch; this exercise changes no production
  code.
- Ruff, Black (351 Python files), mypy (169 production files), Grounding (40
  cases) and Operational Learning (18 cases) passed.
- Both paired and diagnostic reports replayed twice from the published artifact
  with socket, SQLite and subprocess creation denied. Exact hashes above match.
- Linux ARM64 container build passed as `tacit:offline-scenario-tev`. The same
  artifact reproduced both reports as UID 999 with a read-only filesystem,
  networking disabled, dropped capabilities and the same external-access traps.
- A final independent audit found no actionable issue in source/label ordering,
  packet/receipt identity, failure accounting, report claims or publishable
  artifact contents.

No runtime, provider, ranker, grounding rule, publication policy or dependency
changed for this corpus exercise. No LangGraph code was introduced.

## Implication

The next investigation improvement should preserve measured conditions and
distinct, grounded candidate evidence before further classifier comparison.
These results supply a reproducible baseline and expose missing acquisition/
representation coverage; they do not select a replacement ranker. LangGraph
remains outside both workstreams.
