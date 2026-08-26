#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

SHA_PATTERN = re.compile(r"[0-9a-f]{40}(?:[0-9a-f]{24})?\Z")


class RangeSelectionError(ValueError):
    pass


def _validated_sha(value: str, label: str) -> str:
    normalized = value.strip().lower()
    if SHA_PATTERN.fullmatch(normalized) is None:
        raise RangeSelectionError(f"{label} is not a complete Git object ID")
    return normalized


def _all_zero_sha(value: str) -> bool:
    normalized = value.strip()
    return len(normalized) in (40, 64) and set(normalized) == {"0"}


def _commit_exists(repository: Path, revision: str) -> bool:
    return (
        subprocess.run(
            ["git", "cat-file", "-e", f"{revision}^{{commit}}"],
            cwd=repository,
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        ).returncode
        == 0
    )


def _is_ancestor(repository: Path, ancestor: str, descendant: str) -> bool:
    return (
        subprocess.run(
            ["git", "merge-base", "--is-ancestor", ancestor, descendant],
            cwd=repository,
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        ).returncode
        == 0
    )


def select_log_range(
    repository: Path,
    *,
    event_name: str,
    head_sha: str,
    base_sha: str,
    before_sha: str,
    baseline_sha: str,
) -> str:
    head = _validated_sha(head_sha, "head SHA")
    if not _commit_exists(repository, head):
        raise RangeSelectionError("head SHA is not present in the full checkout")
    if event_name not in {"pull_request", "push"}:
        raise RangeSelectionError(f"unsupported CI event: {event_name!r}")

    if not baseline_sha:
        print(
            "No trusted Gitleaks baseline is configured; scanning full reachable history",
            file=sys.stderr,
        )
        return head

    baseline = _validated_sha(baseline_sha, "Gitleaks baseline SHA")
    if not _commit_exists(repository, baseline):
        raise RangeSelectionError("Gitleaks baseline SHA is not present in the full checkout")
    if not _is_ancestor(repository, baseline, head):
        print(
            "Gitleaks baseline is not reachable from the event head; scanning full reachable history",
            file=sys.stderr,
        )
        return head
    if baseline == head:
        return head

    if event_name == "pull_request":
        base = _validated_sha(base_sha, "pull-request base SHA")
        if not _commit_exists(repository, base):
            raise RangeSelectionError("pull-request base SHA is not present in the full checkout")
        if base == head:
            return head
        if _is_ancestor(repository, baseline, base):
            return f"{base}..{head}"
        return f"{baseline}..{head}"

    if not before_sha or _all_zero_sha(before_sha):
        return f"{baseline}..{head}"
    before = _validated_sha(before_sha, "push before SHA")
    if not _commit_exists(repository, before):
        print(
            "Push before SHA is unavailable; scanning from the trusted Gitleaks baseline",
            file=sys.stderr,
        )
        return f"{baseline}..{head}"
    if before == head:
        return head
    if _is_ancestor(repository, baseline, before) and _is_ancestor(repository, before, head):
        return f"{before}..{head}"
    return f"{baseline}..{head}"


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Select the reachable Gitleaks commit range")
    parser.add_argument("--repository", type=Path, required=True)
    parser.add_argument("--event-name", required=True)
    parser.add_argument("--head-sha", required=True)
    parser.add_argument("--base-sha", default="")
    parser.add_argument("--before-sha", default="")
    parser.add_argument("--baseline-sha", default="")
    parser.add_argument("--output", type=Path, required=True)
    return parser


def main() -> int:
    arguments = _parser().parse_args()
    try:
        selected = select_log_range(
            arguments.repository,
            event_name=arguments.event_name,
            head_sha=arguments.head_sha,
            base_sha=arguments.base_sha,
            before_sha=arguments.before_sha,
            baseline_sha=arguments.baseline_sha,
        )
        with arguments.output.open("a", encoding="utf-8") as output:
            output.write(f"log_opts={selected}\n")
    except (OSError, RangeSelectionError) as exc:
        print(f"Gitleaks range selection failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
