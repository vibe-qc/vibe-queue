#!/usr/bin/env python3
"""Run a small smoke suite by default; request the full suite explicitly."""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FAST_TESTS = (
    "tests/test_no_maintainer_paths.py",
    "tests/test_ci_scope.py",
    "tests/test_spec.py::TestRoundTrip",
    "tests/test_spec.py::TestValidation::test_cpus_must_be_at_least_one",
    "tests/test_spec.py::TestValidation::test_command_cannot_be_empty",
    "tests/test_spec.py::TestValidation::test_newer_spec_version_rejected",
    "tests/test_auth.py::TestWriteAndLoadToken::test_write_creates_file_with_mode_0600",
    "tests/test_auth.py::TestWriteAndLoadToken::test_load_returns_none_if_mode_too_permissive",
    "tests/test_auth.py::TestConstantTimeEq",
    "tests/test_dispatch.py",
    "tests/test_dependency_cascade_contract.py::test_combined_dependency_modes_are_additive",
)


def full_suite_shards(count: int) -> list[list[str]]:
    """Balance whole modules by source size without collecting tests repeatedly.

    Whole modules preserve module fixtures and ordering. Each CI job supplies
    its own sandbox; parallel workers never share queue or process state.
    """
    shards: list[list[str]] = [[] for _ in range(count)]
    sizes = [0] * count
    files = sorted(set((ROOT / "tests").rglob("test_*.py"))
                   | set((ROOT / "tests").rglob("*_test.py")),
                   key=lambda path: (-path.stat().st_size, path.as_posix()))
    for path in files:
        index = min(range(count), key=lambda slot: (sizes[slot], slot))
        shards[index].append(path.relative_to(ROOT).as_posix())
        sizes[index] += path.stat().st_size
    return [sorted(shard) for shard in shards]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--junitxml")
    parser.add_argument("--scope", choices=("fast", "full"),
                        default=os.environ.get("VQ_TEST_SCOPE", "fast"))
    parser.add_argument("--list-targets", action="store_true")
    parser.add_argument("--shard-count", type=int, default=1)
    parser.add_argument("--shard-index", type=int, default=1,
                        help="one-based shard number (full scope only)")
    args = parser.parse_args()
    if args.scope not in {"fast", "full"}:
        parser.error("VQ_TEST_SCOPE must be fast or full")
    if not 1 <= args.shard_index <= args.shard_count:
        parser.error("require 1 <= shard-index <= shard-count")
    if args.shard_count != 1 and args.scope != "full":
        parser.error("sharding requires --scope full")
    targets = ("tests",) if args.scope == "full" else FAST_TESTS
    if args.shard_count != 1:
        targets = full_suite_shards(args.shard_count)[args.shard_index - 1]
        if not targets:
            parser.error("empty shard; reduce --shard-count")
    print("CI pytest targets: " + " ".join(targets), flush=True)
    if args.list_targets:
        return 0
    command = [sys.executable, "-m", "pytest", *targets, "-q", "-p", "no:cacheprovider"]
    if args.junitxml:
        command.append(f"--junitxml={args.junitxml}")
    env = dict(os.environ, PYTHONPATH=str(ROOT / "src"))
    env.pop("PYTEST_ADDOPTS", None)
    return subprocess.run(command, cwd=ROOT, env=env, check=False).returncode


if __name__ == "__main__":
    raise SystemExit(main())
