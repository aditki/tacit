"""Repeated isolated LLM evaluation for the ClickStack prompt corpus.

The harness intentionally bypasses the pipeline cache between trials. A result
is useful when the structured intent retains both the Redis/cache hypothesis
and a latency/request investigation path; misleading prompts must not displace
the cache hypothesis with the distractor.

Run against the configured local provider:
    python -m tests.eval.prompt_variation_harness --trials 5 --json report.json
"""

from __future__ import annotations

import argparse
import asyncio
import json
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any

from tacit.agents.intent import classify_intent
from tests.eval.cold_isolation import LocalEvaluationEndpoints, cold_isolation, require_local_endpoint
from tests.eval.prompt_scoring import evaluate as _evaluate
from tests.eval.prompt_scoring import is_negative as _is_negative

FIXTURES_DIR = Path(__file__).parent / "fixtures"
DEFAULT_CORPUS = FIXTURES_DIR / "clickstack_prompts.json"
DEFAULT_LOCAL_LLM_URL = "http://127.0.0.1:11434"
DEFAULT_LOCAL_MODEL = "qwen3-coder:30b"


async def run(
    trials: int,
    corpus_path: Path,
    *,
    endpoints: LocalEvaluationEndpoints | None = None,
) -> dict[str, Any]:
    if endpoints is None:
        selected = _local_llm_endpoints(
            provider="ollama",
            model=DEFAULT_LOCAL_MODEL,
            api_key="",
            api_base=DEFAULT_LOCAL_LLM_URL,
        )
    else:
        selected = _local_llm_endpoints(
            provider="ollama",
            model=endpoints.llm_model,
            api_key="",
            api_base=endpoints.llm_api_base or "",
        )
    if trials < 1:
        raise ValueError("prompt variation evaluation requires at least one trial")
    fixture = json.loads(corpus_path.read_text())
    prompts = fixture.get("prompts") if isinstance(fixture, dict) else None
    if not isinstance(prompts, list):
        raise ValueError("prompt variation corpus must contain a prompts list")
    positive_count = sum(1 for item in prompts if isinstance(item, dict) and not _is_negative(item))
    negative_count = sum(1 for item in prompts if isinstance(item, dict) and _is_negative(item))
    if positive_count == 0 or negative_count == 0:
        raise ValueError("prompt variation corpus requires nonempty positive and negative populations")
    rows: list[dict[str, Any]] = []
    with cold_isolation(endpoints=selected) as state:
        if state.dependencies.llm_provider_factory is None:
            raise RuntimeError("isolated prompt evaluation has no LLM provider factory")
        provider = state.dependencies.llm_provider_factory()
        try:
            for prompt_index, item in enumerate(prompts):
                outcomes: list[bool] = []
                failures: list[dict[str, Any]] = []
                for trial in range(trials):
                    state.dependencies.llm_cache.invalidate()
                    try:
                        intent, _ = await classify_intent(
                            item["text"],
                            provider=provider,
                            runtime_settings=state.settings,
                        )
                        useful, evidence = _evaluate(intent, item)
                    except Exception as exc:
                        useful = False
                        evidence = {"error": f"{type(exc).__name__}: {exc}"}
                    outcomes.append(useful)
                    if not useful:
                        failures.append({"trial": trial + 1, **evidence})
                rows.append(
                    {
                        "prompt_index": prompt_index + 1,
                        "class": item["class"],
                        "polarity": "negative" if _is_negative(item) else "positive",
                        "prompt": item["text"],
                        "passed": sum(outcomes),
                        "trials": trials,
                        "rate": round(sum(outcomes) / trials, 4),
                        "failures": failures,
                    }
                )
            provider_name = state.settings.llm_provider
            model_name = state.settings.llm_model
        finally:
            await state.dependencies.close_resources()

    by_class: dict[str, list[bool]] = defaultdict(list)
    for row in rows:
        by_class[row["class"]].extend([True] * row["passed"])
        by_class[row["class"]].extend([False] * (row["trials"] - row["passed"]))
    class_rates = {name: round(sum(values) / len(values), 4) for name, values in sorted(by_class.items())}

    # Split positive useful-rate (the gate) from negative false-positive rate.
    pos = [r for r in rows if r["polarity"] == "positive"]
    neg = [r for r in rows if r["polarity"] == "negative"]
    pos_overall = sum(r["passed"] for r in pos) / sum(r["trials"] for r in pos)
    neg_correct = sum(r["passed"] for r in neg) / sum(r["trials"] for r in neg)
    rates = [row["rate"] for row in rows]
    return {
        "corpus": corpus_path.name,
        "role": fixture.get("role", "unspecified"),
        "provider": provider_name,
        "model": model_name,
        "prompts": len(rows),
        "trials_per_prompt": trials,
        "positive_useful_rate": round(pos_overall, 4),
        "negative_correct_rate": round(neg_correct, 4),
        "prompt_rate_mean": round(statistics.mean(rates), 4),
        "prompt_rate_stddev": round(statistics.pstdev(rates), 4) if rates else 0.0,
        "worst_prompt_rate": round(min(rates), 4),
        "class_rates": class_rates,
        "gate": 0.85,
        "passed": pos_overall >= 0.85 and neg_correct >= 0.85,
        "results": rows,
    }


def _resolve_corpus(name: str) -> Path:
    """Accept a fixture name, a stem ('holdout'/'dev'), or a path."""
    if not name:
        return DEFAULT_CORPUS
    aliases = {
        "dev": FIXTURES_DIR / "clickstack_prompts.json",
        "development": FIXTURES_DIR / "clickstack_prompts.json",
        "holdout": FIXTURES_DIR / "clickstack_prompts_holdout.json",
    }
    if name in aliases:
        return aliases[name]
    p = Path(name)
    if p.is_file():
        return p
    candidate = FIXTURES_DIR / name
    if candidate.is_file():
        return candidate
    raise SystemExit(f"corpus not found: {name}")


def _local_llm_endpoints(
    *,
    provider: str,
    model: str,
    api_key: str,
    api_base: str,
) -> LocalEvaluationEndpoints:
    """Validate the sole provider shape allowed by the isolated harness."""
    if provider.casefold() != "ollama":
        raise ValueError("prompt variation evaluation requires the local Ollama provider")
    if api_key:
        raise ValueError("prompt variation evaluation does not accept API credentials")
    if not model:
        raise ValueError("prompt variation evaluation requires an explicit local model")
    return LocalEvaluationEndpoints(
        llm_api_base=require_local_endpoint(api_base, "Prompt variation LLM URL"),
        llm_model=model,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Repeated prompt-variation gate (expectation-aware).")
    parser.add_argument("--trials", type=int, default=5)
    parser.add_argument("--corpus", default="dev", help="'dev', 'holdout', a fixture name, or a path.")
    parser.add_argument("--json", default="")
    parser.add_argument("--provider", default="ollama", help="Local provider (only ollama is accepted).")
    parser.add_argument("--model", default=DEFAULT_LOCAL_MODEL, help="Local Ollama model name.")
    parser.add_argument("--api-key", default="", help="Rejected: isolated evaluations do not accept credentials.")
    parser.add_argument("--api-base", default=DEFAULT_LOCAL_LLM_URL, help="Explicit local Ollama endpoint.")
    args = parser.parse_args()
    if args.trials < 1:
        parser.error("--trials must be at least 1")
    try:
        endpoints = _local_llm_endpoints(
            provider=args.provider,
            model=args.model,
            api_key=args.api_key,
            api_base=args.api_base,
        )
    except ValueError as exc:
        parser.error(str(exc))
    corpus_path = _resolve_corpus(args.corpus)
    report = asyncio.run(run(args.trials, corpus_path, endpoints=endpoints))
    if report.get("role") == "holdout":
        print("NOTE: holdout — run ONCE and do not tune the synonym layer from its failures.\n")
    print(json.dumps({key: value for key, value in report.items() if key != "results"}, indent=2))
    failures = [row for row in report["results"] if row["rate"] < 1.0]
    if failures:
        print("\nPrompt failures:")
        for row in failures:
            print(
                f"  #{row['prompt_index']:02d} {row['class']:11s} "
                f"{row['polarity']:8s} rate={row['rate']:.2f} {row['prompt']}"
            )
    if args.json:
        Path(args.json).write_text(json.dumps(report, indent=2))
        print(f"\nwrote {args.json}")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
