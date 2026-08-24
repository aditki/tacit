"""Reproducible stdlib-versus-Tacit SQLite storage benchmark.

Run with:

    uv run python -m tests.eval.sqlite_storage_benchmark --samples 20
"""

from __future__ import annotations

import argparse
import gc
import json
import multiprocessing
import platform
import queue
import sqlite3
import statistics
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor
from contextlib import closing
from pathlib import Path
from typing import Any

from tacit.sqlite_identity import SQLiteDatabaseTarget, activate_sqlite_wal

_TIMEOUT_MS = 30_000


def _control_connect(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path, timeout=_TIMEOUT_MS / 1_000)
    connection.execute(f"PRAGMA busy_timeout={_TIMEOUT_MS}")
    deadline = time.monotonic() + _TIMEOUT_MS / 1_000
    delay = 0.005
    try:
        while True:
            try:
                row = connection.execute("PRAGMA journal_mode").fetchone()
                if not row or str(row[0]).casefold() != "wal":
                    row = connection.execute("PRAGMA journal_mode=WAL").fetchone()
                if row and str(row[0]).casefold() == "wal":
                    return connection
                raise RuntimeError("stdlib control did not enter WAL mode")
            except sqlite3.OperationalError as exc:
                if "locked" not in str(exc).casefold() or time.monotonic() >= deadline:
                    raise
                time.sleep(delay)
                delay = min(delay * 2, 0.1)
    except Exception:
        connection.close()
        raise


def _tacit_connect(path: Path) -> sqlite3.Connection:
    connection = SQLiteDatabaseTarget(path).connect(timeout_ms=_TIMEOUT_MS)
    activate_sqlite_wal(connection, timeout_ms=_TIMEOUT_MS)
    return connection


def _subprocess_writer(kind: str, path: str, worker: int, writes: int) -> int:
    connector = _control_connect if kind == "control" else _tacit_connect
    with closing(connector(Path(path))) as connection:
        for offset in range(writes):
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                "INSERT INTO rows (worker, offset) VALUES (?, ?)",
                (worker, offset),
            )
            connection.commit()
    return writes


def _coordinated_wal_worker(
    kind: str,
    path: str,
    worker: int,
    start_event: Any,
    reopen_event: Any,
    checkpoint_event: Any,
    close_event: Any,
    messages: Any,
) -> None:
    connector = _control_connect if kind == "control" else _tacit_connect
    connection: sqlite3.Connection | None = None
    try:
        if not start_event.wait(15):
            raise TimeoutError("start signal not received")
        connection = connector(Path(path))
        connection.execute("BEGIN IMMEDIATE")
        connection.execute(
            "CREATE TABLE IF NOT EXISTS lifecycle_rows (worker INTEGER PRIMARY KEY, value TEXT NOT NULL)"
        )
        connection.execute(
            "INSERT INTO lifecycle_rows (worker, value) VALUES (?, ?)",
            (worker, f"worker-{worker}"),
        )
        connection.commit()
        messages.put(("ready", worker, None))

        if worker == 1:
            if not reopen_event.wait(15):
                raise TimeoutError("reopen signal not received")
            connection.close()
            connection = connector(Path(path))
            row_count = connection.execute("SELECT COUNT(*) FROM lifecycle_rows").fetchone()[0]
            messages.put(("reopened", worker, row_count))

        if worker == 0:
            if not checkpoint_event.wait(15):
                raise TimeoutError("checkpoint signal not received")
            checkpoint = connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
            messages.put(("checkpointed", worker, checkpoint))

        if not close_event.wait(15):
            raise TimeoutError("close signal not received")
        connection.close()
        connection = None
        messages.put(("closed", worker, None))
    except BaseException as exc:
        messages.put(("error", worker, type(exc).__name__))
        raise
    finally:
        if connection is not None:
            connection.close()


def _coordinated_fresh_worker(kind: str, path: str, messages: Any) -> None:
    connector = _control_connect if kind == "control" else _tacit_connect
    try:
        with closing(connector(Path(path))) as connection:
            prior_count = connection.execute("SELECT COUNT(*) FROM lifecycle_rows").fetchone()[0]
            connection.execute("INSERT INTO lifecycle_rows (worker, value) VALUES (2, 'fresh-process')")
            connection.commit()
            checkpoint = connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
            final_count = connection.execute("SELECT COUNT(*) FROM lifecycle_rows").fetchone()[0]
        messages.put(("fresh", prior_count, final_count, checkpoint))
    except BaseException as exc:
        messages.put(("error", 2, type(exc).__name__))
        raise


def _next_lifecycle_message(messages: Any, expected: str) -> tuple[Any, ...]:
    try:
        message = messages.get(timeout=20)
    except queue.Empty as exc:
        raise RuntimeError(f"timed out waiting for coordinated WAL event {expected}") from exc
    if message[0] == "error":
        raise RuntimeError(f"coordinated WAL worker {message[1]} failed with {message[2]}")
    if message[0] != expected:
        raise RuntimeError(f"expected coordinated WAL event {expected}, received {message[0]}")
    return message


def _run_coordinated_wal_lifecycle(kind: str, path: Path, *, last_closer: int) -> None:
    context = multiprocessing.get_context("spawn")
    start_event = context.Event()
    reopen_event = context.Event()
    checkpoint_event = context.Event()
    close_events = [context.Event(), context.Event()]
    messages = context.Queue()
    processes = [
        context.Process(
            target=_coordinated_wal_worker,
            args=(
                kind,
                str(path),
                worker,
                start_event,
                reopen_event,
                checkpoint_event,
                close_events[worker],
                messages,
            ),
        )
        for worker in range(2)
    ]
    fresh: multiprocessing.Process | None = None
    try:
        for process in processes:
            process.start()
        start_event.set()
        ready = {_next_lifecycle_message(messages, "ready")[1] for _ in processes}
        if ready != {0, 1}:
            raise RuntimeError("coordinated WAL workers did not both reach first open")

        reopen_event.set()
        if _next_lifecycle_message(messages, "reopened")[2] != 2:
            raise RuntimeError("coordinated WAL reopen did not observe both committed rows")
        checkpoint_event.set()
        if _next_lifecycle_message(messages, "checkpointed")[2] is None:
            raise RuntimeError("coordinated WAL checkpoint returned no result")

        for worker in (1 - last_closer, last_closer):
            close_events[worker].set()
            if _next_lifecycle_message(messages, "closed")[1] != worker:
                raise RuntimeError("coordinated WAL workers closed out of order")
            processes[worker].join(timeout=20)
            if processes[worker].exitcode != 0:
                raise RuntimeError(f"coordinated WAL worker {worker} did not exit cleanly")

        fresh = context.Process(
            target=_coordinated_fresh_worker,
            args=(kind, str(path), messages),
        )
        fresh.start()
        fresh_message = _next_lifecycle_message(messages, "fresh")
        if fresh_message[1:3] != (2, 3):
            raise RuntimeError("fresh WAL owner did not verify and extend committed state")
        fresh.join(timeout=20)
        if fresh.exitcode != 0:
            raise RuntimeError("fresh WAL owner did not exit cleanly")
    finally:
        for process in (*processes, fresh):
            if process is None:
                continue
            if process.is_alive():
                process.terminate()
            process.join(timeout=5)
        messages.close()
        messages.join_thread()


def _percentile(values: list[float], quantile: float) -> float:
    ordered = sorted(values)
    if not ordered:
        raise ValueError("benchmark sample set is empty")
    position = (len(ordered) - 1) * quantile
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def _summary(samples_ns: list[int], *, operations_per_sample: int) -> dict[str, int | float]:
    if not samples_ns:
        raise ValueError("benchmark sample set is empty")
    samples_ms = [sample / 1_000_000 for sample in samples_ns]
    elapsed_seconds = sum(samples_ns) / 1_000_000_000
    return {
        "sample_count": len(samples_ns),
        "operations_per_sample": operations_per_sample,
        "mean_ms": round(statistics.fmean(samples_ms), 6),
        "p50_ms": round(_percentile(samples_ms, 0.50), 6),
        "p95_ms": round(_percentile(samples_ms, 0.95), 6),
        "operations_per_second": round(
            len(samples_ns) * operations_per_sample / elapsed_seconds,
            3,
        ),
    }


def _measure(
    operation: Callable[[int], None],
    *,
    samples: int,
    warmups: int,
) -> list[int]:
    for index in range(warmups):
        operation(-index - 1)
    measured: list[int] = []
    for index in range(samples):
        started = time.perf_counter_ns()
        operation(index)
        measured.append(time.perf_counter_ns() - started)
    return measured


def _connect_workload(
    root: Path,
    connector: Callable[[Path], sqlite3.Connection],
    label: str,
) -> Callable[[int], None]:
    def run(index: int) -> None:
        with closing(connector(root / f"{label}-connect-{index}.db")) as connection:
            connection.execute("SELECT 1").fetchone()

    return run


def _commit_workload(
    root: Path,
    connector: Callable[[Path], sqlite3.Connection],
    label: str,
    *,
    batch_size: int,
) -> Callable[[int], None]:
    def run(index: int) -> None:
        with closing(connector(root / f"{label}-commit-{batch_size}-{index}.db")) as connection:
            connection.execute("CREATE TABLE rows (value INTEGER NOT NULL)")
            connection.commit()
            connection.execute("BEGIN IMMEDIATE")
            connection.executemany(
                "INSERT INTO rows VALUES (?)",
                [(value,) for value in range(batch_size)],
            )
            connection.commit()

    return run


def _checkpoint_workload(
    root: Path,
    connector: Callable[[Path], sqlite3.Connection],
    label: str,
) -> Callable[[int], None]:
    def run(index: int) -> None:
        path = root / f"{label}-checkpoint-{index}.db"
        with closing(connector(path)) as connection:
            connection.execute("CREATE TABLE rows (value INTEGER NOT NULL)")
            connection.execute("INSERT INTO rows VALUES (1)")
            connection.commit()
            connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        with closing(connector(path)) as reopened:
            if reopened.execute("SELECT value FROM rows").fetchone() != (1,):
                raise RuntimeError("checkpoint/reopen verification failed")

    return run


def _subprocess_workload(
    root: Path,
    connector: Callable[[Path], sqlite3.Connection],
    label: str,
    *,
    kind: str,
    workers: int,
    writes: int,
) -> Callable[[int], None]:
    def run(index: int) -> None:
        path = root / f"{label}-subprocess-{index}.db"
        with closing(connector(path)) as connection:
            connection.execute(
                "CREATE TABLE rows (worker INTEGER NOT NULL, offset INTEGER NOT NULL, PRIMARY KEY(worker, offset))"
            )
            connection.commit()
        context = multiprocessing.get_context("spawn")
        with ProcessPoolExecutor(max_workers=workers, mp_context=context) as executor:
            completed = list(
                executor.map(
                    _subprocess_writer,
                    [kind] * workers,
                    [str(path)] * workers,
                    range(workers),
                    [writes] * workers,
                )
            )
        if completed != [writes] * workers:
            raise RuntimeError("subprocess write count mismatch")
        with sqlite3.connect(path) as connection:
            row_count = connection.execute("SELECT COUNT(*) FROM rows").fetchone()
        if row_count != (workers * writes,):
            raise RuntimeError("subprocess committed row count mismatch")

    return run


def _coordinated_lifecycle_workload(root: Path, label: str, *, kind: str) -> Callable[[int], None]:
    def run(index: int) -> None:
        for last_closer in (0, 1):
            _run_coordinated_wal_lifecycle(
                kind,
                root / f"{label}-coordinated-{index}-{last_closer}.db",
                last_closer=last_closer,
            )

    return run


def _revision() -> str:
    try:
        completed = subprocess.run(
            ["git", "rev-parse", "--short=12", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        )
        revision = completed.stdout.strip() or "unknown"
        status = subprocess.run(
            ["git", "status", "--porcelain=v1", "--untracked-files=normal", "--"],
            check=True,
            capture_output=True,
            text=True,
        )
        return f"{revision}-dirty" if status.stdout.strip() else revision
    except (OSError, subprocess.SubprocessError):
        return "unknown"


def _descriptor_count() -> int | None:
    for directory in (Path("/proc/self/fd"), Path("/dev/fd")):
        try:
            return len(tuple(directory.iterdir()))
        except OSError:
            continue
    return None


def run_benchmark(
    *,
    root: Path,
    samples: int,
    warmups: int,
    batch_size: int,
    subprocess_workers: int,
    subprocess_writes: int,
) -> dict[str, Any]:
    """Run every benchmark workload and return a JSON-serializable report."""
    if min(samples, batch_size, subprocess_workers, subprocess_writes) <= 0 or warmups < 0:
        raise ValueError("benchmark parameters must be positive and warmups non-negative")
    root.mkdir(parents=True, mode=0o700, exist_ok=True)
    start_descriptors = _descriptor_count()
    failures: list[dict[str, str]] = []
    workloads: dict[str, dict[str, dict[str, int | float]]] = {}
    definitions = {
        "connect_wal_close": (
            lambda connector, label: _connect_workload(root, connector, label),
            1,
        ),
        "single_row_commit": (
            lambda connector, label: _commit_workload(
                root,
                connector,
                label,
                batch_size=1,
            ),
            1,
        ),
        "batched_statements": (
            lambda connector, label: _commit_workload(
                root,
                connector,
                label,
                batch_size=batch_size,
            ),
            batch_size,
        ),
        "checkpoint_reopen": (
            lambda connector, label: _checkpoint_workload(root, connector, label),
            1,
        ),
        "subprocess_wal": (
            lambda connector, label: _subprocess_workload(
                root,
                connector,
                label,
                kind=label,
                workers=subprocess_workers,
                writes=subprocess_writes,
            ),
            subprocess_workers * subprocess_writes,
        ),
        "coordinated_wal_lifecycle": (
            lambda _connector, label: _coordinated_lifecycle_workload(
                root,
                label,
                kind=label,
            ),
            6,
        ),
    }
    for workload_name, (factory, operations_per_sample) in definitions.items():
        workloads[workload_name] = {}
        for variant, connector in (("control", _control_connect), ("tacit", _tacit_connect)):
            try:
                measurements = _measure(
                    factory(connector, variant),
                    samples=samples,
                    warmups=warmups,
                )
                workloads[workload_name][variant] = _summary(
                    measurements,
                    operations_per_sample=operations_per_sample,
                )
            except Exception as exc:
                failures.append(
                    {
                        "workload": workload_name,
                        "variant": variant,
                        "error_type": type(exc).__name__,
                    }
                )
                workloads[workload_name][variant] = {
                    "sample_count": 0,
                    "operations_per_sample": operations_per_sample,
                    "mean_ms": 0.0,
                    "p50_ms": 0.0,
                    "p95_ms": 0.0,
                    "operations_per_second": 0.0,
                }
    gc.collect()
    end_descriptors = _descriptor_count()
    descriptor_delta = (
        None if start_descriptors is None or end_descriptors is None else end_descriptors - start_descriptors
    )
    return {
        "schema_version": 1,
        "runtime": {
            "revision": _revision(),
            "python": platform.python_version(),
            "python_implementation": platform.python_implementation(),
            "sqlite": sqlite3.sqlite_version,
            "platform": platform.platform(),
            "filesystem_root": str(root.resolve(strict=True)),
            "filesystem_device": root.stat().st_dev,
        },
        "pragmas": {
            "journal_mode": "wal",
            "busy_timeout_ms": _TIMEOUT_MS,
            "synchronous": "sqlite_default",
        },
        "parameters": {
            "samples": samples,
            "warmups": warmups,
            "batch_size": batch_size,
            "subprocess_workers": subprocess_workers,
            "subprocess_writes": subprocess_writes,
            "coordinated_last_close_orders": [0, 1],
        },
        "descriptor_delta": descriptor_delta,
        "failures": failures,
        "workloads": workloads,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path)
    parser.add_argument("--samples", type=int, default=20)
    parser.add_argument("--warmups", type=int, default=3)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--subprocess-workers", type=int, default=4)
    parser.add_argument("--subprocess-writes", type=int, default=8)
    parser.add_argument("--output", type=Path)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.root is not None:
        report = run_benchmark(
            root=args.root,
            samples=args.samples,
            warmups=args.warmups,
            batch_size=args.batch_size,
            subprocess_workers=args.subprocess_workers,
            subprocess_writes=args.subprocess_writes,
        )
    else:
        with tempfile.TemporaryDirectory(prefix="tacit-sqlite-benchmark-") as directory:
            report = run_benchmark(
                root=Path(directory),
                samples=args.samples,
                warmups=args.warmups,
                batch_size=args.batch_size,
                subprocess_workers=args.subprocess_workers,
                subprocess_writes=args.subprocess_writes,
            )
    rendered = json.dumps(report, indent=2, sort_keys=True)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(f"{rendered}\n", encoding="utf-8")
    print(rendered)
    return 1 if report["failures"] else 0


if __name__ == "__main__":
    sys.exit(main())
