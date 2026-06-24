# ADR-009: Evaluation should prioritize operational usefulness over label purity

## Status

Accepted

## Context

Archetype classification accuracy is useful, but a dashboard can choose the right label and still omit critical evidence.
The repo includes a synthetic 100-prompt benchmark, pipeline validation, metric recall, critical recall, signal-to-noise,
and feedback storage.

## Decision

Evaluation should prioritize operational usefulness: metric recall, critical metric recall, query validity, dashboard
density, missing evidence, signal-to-noise, and human feedback. Archetype label accuracy should remain one metric, not
the primary product goal.

## Consequences

- Benchmarks should include both classification and generated dashboard quality.
- Synthetic results should be labeled as beta/demo-oriented, not production guarantees.
- Human usefulness feedback should eventually close the loop with ranking and mapping quality.

## Implementation Notes

Implementation status: partially implemented.

Validated against:

- `docs/evaluation.md`: reports archetype accuracy, recall, critical recall, weighted recall, SNR, pipeline success, and
  known weaknesses.
- `tests/validate.py` and `tests/tacit_validation_prompts.csv`: implement the public validation dataset/runner.
- `tacit/feedback.py`: stores dimensional feedback and analysis.
- `tacit/main.py`: exposes feedback endpoints.
- `tests/README.md`: describes usefulness-oriented E2E scenarios.

TODO:

- The benchmark does not yet measure real human usefulness during incidents.
- Expand datasource-specific and real-world scenario coverage before treating the metrics as launch-quality.

