"""Routine smoke is bounded; full validation remains explicitly selectable."""
from __future__ import annotations

import importlib.util
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts/run_ci_tests.py"
spec = importlib.util.spec_from_file_location("ci_scope", SCRIPT)
assert spec is not None and spec.loader is not None
scope = importlib.util.module_from_spec(spec)
spec.loader.exec_module(scope)


@pytest.mark.parametrize("paths,expected", [
    (["docs/usage.md"], True),
    (["README.md", "docs/nested/page.rst", "changelog.d/test.fixed.md"], True),
    (["docs/conf.py"], False),
    (["docs/usage.md", "src/vq/daemon.py"], False),
    (["tests/test_paths.py"], False),
    ([".gitlab-ci.yml"], False),
    (["pyproject.toml"], False),
    (["config/queue.toml"], False),
    (["new-runtime/module.py"], False),
    ([], False),
])
def test_only_known_prose_is_eligible(paths, expected):
    assert scope.prose_only(paths) is expected


@pytest.fixture
def history(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()

    def git(*args):
        return subprocess.check_output(["git", "-C", str(repo), *args], text=True).strip()

    git("init", "-q")
    git("config", "user.name", "Test")
    git("config", "user.email", "test@example.invalid")
    (repo / "README.md").write_text("before\n")
    git("add", "README.md")
    git("commit", "-qm", "initial")
    base = git("rev-parse", "HEAD")
    (repo / "README.md").write_text("after\n")
    git("commit", "-qam", "docs")
    return repo, base, git


def test_mr_and_main_prose_diff_keep_privacy_checks(history):
    repo, base, _ = history
    for env in (
        {"CI_PIPELINE_SOURCE": "merge_request_event", "CI_MERGE_REQUEST_DIFF_BASE_SHA": base},
        {"CI_PIPELINE_SOURCE": "push", "CI_COMMIT_BRANCH": "main", "CI_COMMIT_BEFORE_SHA": base},
    ):
        assert scope.select_targets(env, repo, scope="changed") == scope.PROSE_TESTS


@pytest.mark.parametrize("source,branch", [
    ("web", "main"), ("schedule", "main"), ("api", "main"),
    ("push", "release-candidate/v1.0.0"),
])
def test_explicit_validation_never_uses_prose_shortcut(history, source, branch):
    repo, base, _ = history
    assert scope.select_targets({
        "CI_PIPELINE_SOURCE": source, "CI_COMMIT_BRANCH": branch,
        "CI_COMMIT_BEFORE_SHA": base, "CI_MERGE_REQUEST_DIFF_BASE_SHA": base,
    }, repo) == ("tests",)


@pytest.mark.parametrize("base", ["", "0" * 40, "f" * 40, "--output=unexpected"])
def test_missing_or_invalid_history_keeps_fast_feedback(history, base):
    repo, _, _ = history
    assert scope.select_targets({
        "CI_PIPELINE_SOURCE": "merge_request_event", "CI_MERGE_REQUEST_DIFF_BASE_SHA": base,
    }, repo, scope="changed") == scope.FAST_TESTS


def test_renaming_prose_to_code_keeps_smoke(history):
    repo, base, git = history
    git("mv", "README.md", "runtime.py")
    git("commit", "-qm", "rename")
    assert scope.select_targets({
        "CI_PIPELINE_SOURCE": "merge_request_event", "CI_MERGE_REQUEST_DIFF_BASE_SHA": base,
    }, repo, scope="changed") == scope.FAST_TESTS


def test_empty_diff_keeps_smoke(history):
    repo, _, git = history
    assert scope.select_targets({
        "CI_PIPELINE_SOURCE": "merge_request_event",
        "CI_MERGE_REQUEST_DIFF_BASE_SHA": git("rev-parse", "HEAD"),
    }, repo, scope="changed") == scope.FAST_TESTS


def test_runner_forwards_targets_report_and_failure(monkeypatch):
    import sys

    calls = []
    monkeypatch.setattr(scope, "select_targets", lambda env, **kwargs: scope.PROSE_TESTS)
    monkeypatch.setattr(sys, "argv", [str(SCRIPT), "--junitxml=reports/results.xml"])

    def run(command, **kwargs):
        calls.append((command, kwargs))
        return subprocess.CompletedProcess(command, 1)

    monkeypatch.setattr(scope.subprocess, "run", run)
    assert scope.main() == 1
    command, options = calls[0]
    assert command[:3] == [sys.executable, "-m", "pytest"]
    assert command[3:3 + len(scope.PROSE_TESTS)] == list(scope.PROSE_TESTS)
    assert command[-1] == "--junitxml=reports/results.xml"
    assert options == {"cwd": scope.ROOT, "check": False}


def test_full_scope_is_available_without_ci(history):
    repo, _, _ = history
    assert scope.select_targets({}, repo, scope="full") == ("tests",)
    assert scope.select_targets({}, repo, scope="changed") == scope.FAST_TESTS


def test_opt_in_changed_files_join_smoke_but_deleted_tests_do_not(history):
    repo, base, git = history
    tests = repo / "tests"
    tests.mkdir()
    (tests / "test_new.py").write_text("def test_new(): pass\n")
    git("add", "tests/test_new.py")
    git("commit", "-qm", "new test")
    env = {"CI_PIPELINE_SOURCE": "merge_request_event", "CI_MERGE_REQUEST_DIFF_BASE_SHA": base}
    assert scope.select_targets(env, repo, scope="changed") == (
        *scope.FAST_TESTS, "tests/test_new.py",
    )
    git("rm", "tests/test_new.py")
    git("commit", "-qm", "remove test")
    assert scope.select_targets(env, repo, scope="changed") == scope.PROSE_TESTS


def test_every_smoke_and_documentation_target_exists():
    for target in (*scope.FAST_TESTS, *scope.PROSE_TESTS):
        assert (scope.ROOT / target.split("::", 1)[0]).is_file(), target


def test_routine_smoke_does_not_inspect_history_or_expand_changed_files(monkeypatch):
    def unexpected(*args, **kwargs):
        raise AssertionError("routine smoke must not query Git history")
    monkeypatch.setattr(scope.subprocess, "run", unexpected)
    env = {"CI_PIPELINE_SOURCE": "merge_request_event", "CI_MERGE_REQUEST_DIFF_BASE_SHA": "a" * 40}
    assert scope.select_targets(env) == scope.FAST_TESTS
    assert scope.select_targets(env, scope="fast") == scope.FAST_TESTS
