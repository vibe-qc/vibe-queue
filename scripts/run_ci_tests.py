#!/usr/bin/env python3
"""Fast routine feedback; full validation is explicit or release-candidate CI.

Routine runs use a fixed sample, without Git history or changed-file expansion.
Use --scope changed for affected test files, or --scope full for broad validation.
See docs/testing.md for the deliberately omitted coverage.
"""
from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path, PurePosixPath

ROOT = Path(__file__).resolve().parents[1]
# Documentation has executable contract assertions as well as privacy checks.
# Keep new documentation assertions in this list when adding them to the suite.
PROSE_TESTS = (
    "tests/test_no_maintainer_paths.py",
    "tests/test_ci_scope.py",
    "tests/test_vibeqc_site_claims.py",
    "tests/test_docs_relative_links.py",
    "tests/test_scheduler_dispatch.py::test_workdir_docs_match_scheduler_scratch_runtime_contract",
    "tests/test_admin_outcomes.py::TestTheDocumentedTableMatchesTheCode",
    "tests/test_admin_update_extras_issue85.py::"
    "test_the_pages_that_described_the_old_rule_describe_this_one",
    "tests/test_pbs_lane_authority_v0_26_10.py::test_the_documented_contract_names_the_same_schema",
    "tests/test_doctor_declared_extras_routes.py::"
    "test_the_check_is_documented_where_operators_look",
    "tests/test_admin_install_extras_v0_26_10.py::"
    "test_the_documented_remedy_restores_the_declaration",
    "tests/test_admin_install_extras_v0_26_10.py::"
    "test_the_install_refusal_is_documented_where_operators_look",
)
FAST_TESTS = (
    "tests/test_no_maintainer_paths.py",
    "tests/test_spec.py",
    "tests/test_auth.py",
    "tests/test_dispatch.py",
    "tests/test_dependency_cascade_contract.py",
)

ROOT_PROSE = frozenset({
    "AGENTS.md", "CLAUDE.md", "CODEX.md", "CONTRIBUTING.md", "README.md",
    "CHANGELOG.md", "SECURITY.md", "PUBLICATION.md",
})


def prose_only(paths: Sequence[str]) -> bool:
    def is_prose(path: str) -> bool:
        item = PurePosixPath(path)
        return path in ROOT_PROSE or (
            item.parts[0] in {"docs", "changelog.d"}
            and item.suffix in {".md", ".rst"}
        )

    return bool(paths) and all(is_prose(path) for path in paths)


def select_targets(
    env: Mapping[str, str], root: Path = ROOT, scope: str = "auto",
) -> tuple[str, ...]:
    source = env.get("CI_PIPELINE_SOURCE")
    branch = env.get("CI_COMMIT_BRANCH", "")
    if scope == "full" or (scope == "auto" and (
        branch.startswith("release-candidate/")
        or source in {"web", "schedule", "api"}
    )):
        return ("tests",)
    if scope != "changed":
        return FAST_TESTS
    if source == "merge_request_event":
        base = env.get("CI_MERGE_REQUEST_DIFF_BASE_SHA", "")
    elif source == "push" and branch == "main":
        base = env.get("CI_COMMIT_BEFORE_SHA", "")
    else:
        return FAST_TESTS
    if not re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", base) or set(base) == {"0"}:
        return FAST_TESTS
    try:
        diff = subprocess.run(
            ["git", "diff", "--name-only", "--no-renames", "-z", base, "HEAD", "--"],
            cwd=root, capture_output=True, check=True, timeout=30,
        )
        paths = diff.stdout.decode("utf-8").rstrip("\0").split("\0") if diff.stdout else []
    except (OSError, UnicodeError, subprocess.SubprocessError):
        return FAST_TESTS
    if prose_only(paths):
        return PROSE_TESTS
    changed_tests = [
        path for path in paths if path.startswith("tests/")
        and PurePosixPath(path).name.startswith("test_")
        and path.endswith(".py") and (root / path).is_file()
    ]
    return tuple(dict.fromkeys((*FAST_TESTS, *changed_tests)))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--junitxml")
    parser.add_argument("--scope", choices=("auto", "fast", "changed", "full"), default="auto")
    parser.add_argument("--list-targets", action="store_true")
    args = parser.parse_args()
    targets = select_targets(os.environ, scope=args.scope)
    print("CI pytest targets: " + " ".join(targets), flush=True)
    if args.list_targets:
        return 0
    command = [
        sys.executable, "-m", "pytest", *targets, "-q", "-p", "no:cacheprovider",
        "--durations=30",
    ]
    if args.junitxml:
        command.append(f"--junitxml={args.junitxml}")
    return subprocess.run(command, cwd=ROOT, check=False).returncode


if __name__ == "__main__":
    raise SystemExit(main())
