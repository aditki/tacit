"""Standalone cold-isolation runtime for evaluation runs.

Lifts the ``isolated_learning_runtime`` pytest fixture into a plain context
manager that any evaluation harness (or a manual run) can use to guarantee a
*cold* baseline: fresh signal/history/feedback stores seeded only from the
packaged ``signals.yaml``, no learned archetypes, and cleared in-memory caches.

Without this, learned mappings, runtime archetypes, feedback-driven metric
quality, and the LLM/metric caches accumulate across runs and silently
contaminate cold-recall measurements (as observed during ClickStack testing).

Usage
-----
    from tests.eval.cold_isolation import cold_isolation

    with cold_isolation() as state:
        # run the pipeline here — every get_*_store() returns the fresh stores
        ...

CLI
---
    python -m tests.eval.cold_isolation --verify
        Enters an isolated runtime and prints the baseline (mappings loaded,
        archetypes registered, cache sizes) so you can confirm a clean slate.
"""

from __future__ import annotations

import argparse
import contextlib
import importlib
import os
import tempfile
from collections.abc import Iterator
from dataclasses import dataclass
from importlib.resources import files
from pathlib import Path
from typing import Any

# Modules whose ``get_*_store`` accessor must be redirected to the fresh store.
# Listed as (module_path, attribute) so they can be patched lazily — a module
# that fails to import in a constrained interpreter is skipped rather than
# breaking the whole reset.
_SIGNAL_STORE_TARGETS = [("dashforge.signals", "get_signal_store"), ("dashforge.dashboard_ingest", "get_signal_store")]
_HISTORY_STORE_TARGETS = [
    ("dashforge.history", "get_investigation_store"),
    ("dashforge.pipeline", "get_investigation_store"),
]
_FEEDBACK_STORE_TARGETS = [("dashforge.feedback", "get_feedback_store"), ("dashforge.main", "get_feedback_store")]


@dataclass
class IsolatedState:
    """Handles to the fresh stores backing an isolated run."""

    workdir: Path
    signal_store: Any
    history_store: Any
    feedback_store: Any
    archetypes_path: Path
    signal_mappings_loaded: int


def _patch_all(targets: list[tuple[str, str]], value: Any) -> list[tuple[Any, str, Any]]:
    """Patch ``module.attr = lambda: value`` for each importable target.

    Returns a list of (module, attr, original) so the caller can restore.
    """
    restores: list[tuple[Any, str, Any]] = []
    for module_path, attr in targets:
        try:
            module = importlib.import_module(module_path)
        except Exception:
            # Module not importable in this interpreter — skip; in the full
            # runtime every target imports cleanly.
            continue
        if hasattr(module, attr):
            restores.append((module, attr, getattr(module, attr)))
            setattr(module, attr, lambda value=value: value)
    return restores


def _reset_caches() -> None:
    """Clear the in-memory metric + LLM caches and the feedback quality cache."""
    try:
        from dashforge.cache import llm_cache, metric_cache

        metric_cache.invalidate()
        llm_cache.invalidate()
    except Exception:
        pass
    # Ranking memoizes feedback-derived metric quality; force a reload.
    try:
        import dashforge.ranking as ranking

        ranking._metric_quality_cache = {}
        ranking._metric_quality_expires = 0.0
    except Exception:
        pass


@contextlib.contextmanager
def cold_isolation(workdir: str | os.PathLike[str] | None = None) -> Iterator[IsolatedState]:
    """Context manager yielding an isolated, cold runtime.

    On enter: builds fresh stores in ``workdir`` (a temp dir if omitted), loads
    only the packaged signal taxonomy, points archetype loading at an empty
    learned-archetypes file, clears caches, and redirects every ``get_*_store``
    accessor to the fresh instances. On exit: restores the accessors, the
    ``DASHFORGE_ARCHETYPES_PATH`` env var, and reloads archetypes.
    """
    from dashforge.feedback import FeedbackStore
    from dashforge.history import InvestigationStore
    from dashforge.signals import SignalStore

    tmp_ctx: tempfile.TemporaryDirectory[str] | None = None
    if workdir is None:
        tmp_ctx = tempfile.TemporaryDirectory(prefix="dashforge-cold-")
        base = Path(tmp_ctx.name)
    else:
        base = Path(workdir)
        base.mkdir(parents=True, exist_ok=True)

    signal_store = SignalStore(db_path=base / "signals.db")
    packaged_signals = base / "packaged_signals.yaml"
    resource = files("dashforge.data").joinpath("signals.yaml")
    with resource.open() as source:
        packaged_signals.write_text(source.read())
    mappings_loaded = signal_store.load_from_yaml(packaged_signals)
    history_store = InvestigationStore(db_path=base / "history.db")
    feedback_store = FeedbackStore(db_path=base / "feedback.db")
    archetypes_path = base / "learned_archetypes.yaml"

    restores: list[tuple[Any, str, Any]] = []
    restores += _patch_all(_SIGNAL_STORE_TARGETS, signal_store)
    restores += _patch_all(_HISTORY_STORE_TARGETS, history_store)
    restores += _patch_all(_FEEDBACK_STORE_TARGETS, feedback_store)

    prior_arch_path = os.environ.get("DASHFORGE_ARCHETYPES_PATH")
    os.environ["DASHFORGE_ARCHETYPES_PATH"] = str(archetypes_path)

    import dashforge.archetypes.templates as templates

    templates.reload_archetypes()
    _reset_caches()

    state = IsolatedState(
        workdir=base,
        signal_store=signal_store,
        history_store=history_store,
        feedback_store=feedback_store,
        archetypes_path=archetypes_path,
        signal_mappings_loaded=mappings_loaded,
    )
    try:
        yield state
    finally:
        for module, attr, original in restores:
            setattr(module, attr, original)
        if prior_arch_path is None:
            os.environ.pop("DASHFORGE_ARCHETYPES_PATH", None)
        else:
            os.environ["DASHFORGE_ARCHETYPES_PATH"] = prior_arch_path
        templates.reload_archetypes()
        _reset_caches()
        if tmp_ctx is not None:
            tmp_ctx.cleanup()


def _verify() -> int:
    """Enter an isolated runtime and print the baseline; non-zero on a dirty slate."""
    from dashforge.cache import llm_cache, metric_cache

    with cold_isolation() as state:
        import dashforge.archetypes.templates as templates

        archetype_count = len(getattr(templates, "ALL_ARCHETYPES", []))
        learned = [a for a in getattr(templates, "ALL_ARCHETYPES", []) if "learned" in getattr(a, "tags", [])]
        print("cold-isolation baseline:")
        print(f"  workdir              : {state.workdir}")
        print(f"  signal mappings      : {state.signal_mappings_loaded} (packaged taxonomy)")
        print(f"  archetypes loaded    : {archetype_count}")
        print(f"  learned archetypes   : {len(learned)} (expect 0)")
        print(f"  metric cache size    : {metric_cache.size} (expect 0)")
        print(f"  llm cache size       : {llm_cache.size} (expect 0)")
        ok = (
            state.signal_mappings_loaded > 0
            and len(learned) == 0
            and metric_cache.size == 0
            and llm_cache.size == 0
        )
        print(f"  CLEAN BASELINE       : {ok}")
        return 0 if ok else 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Cold-isolation runtime for DashForge eval runs.")
    parser.add_argument("--verify", action="store_true", help="Print the baseline and exit non-zero if not clean.")
    args = parser.parse_args()
    if args.verify:
        return _verify()
    parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
