"""One default smoke path and one explicit full-suite opt-in."""
from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts/run_ci_tests.py"
spec = importlib.util.spec_from_file_location("ci_scope", SCRIPT)
assert spec is not None and spec.loader is not None
scope = importlib.util.module_from_spec(spec)
spec.loader.exec_module(scope)


def test_ci_context_does_not_expand_default_smoke(monkeypatch, capsys):
    monkeypatch.delenv("VQ_TEST_SCOPE", raising=False)
    monkeypatch.setenv("CI_PIPELINE_SOURCE", "schedule")
    monkeypatch.setenv("CI_COMMIT_BRANCH", "release-candidate/v1.0.0")
    monkeypatch.setattr(sys, "argv", [str(SCRIPT), "--list-targets"])
    assert scope.main() == 0
    assert capsys.readouterr().out.strip() == "CI pytest targets: " + " ".join(scope.FAST_TESTS)
    for target in scope.FAST_TESTS:
        assert (scope.ROOT / target.split("::", 1)[0]).is_file()


@pytest.mark.parametrize("via_env", [False, True])
def test_full_opt_in_preserves_report_failure_and_sandbox_environment(monkeypatch, via_env):
    monkeypatch.setenv("VQ_TEST_SCOPE", "full" if via_env else "fast")
    monkeypatch.setenv("PYTEST_ADDOPTS", "--noconftest")
    argv = [str(SCRIPT), "--junitxml=reports/results.xml"]
    if not via_env:
        argv += ["--scope", "full"]
    monkeypatch.setattr(sys, "argv", argv)
    calls = []

    def run(command, **kwargs):
        calls.append((command, kwargs))
        return subprocess.CompletedProcess(command, 1)

    monkeypatch.setattr(scope.subprocess, "run", run)
    assert scope.main() == 1
    command, options = calls[0]
    assert command == [sys.executable, "-m", "pytest", "tests", "-q", "-p",
                       "no:cacheprovider", "--junitxml=reports/results.xml"]
    assert options["env"]["PYTHONPATH"] == str(scope.ROOT / "src")
    assert "PYTEST_ADDOPTS" not in options["env"]
    assert options["cwd"] == scope.ROOT


@pytest.mark.parametrize("options", [
    ["--scope", "full", "--shard-count", "0"],
    ["--scope", "full", "--shard-count", "4", "--shard-index", "5"],
    ["--scope", "fast", "--shard-count", "4"],
])
def test_invalid_partition_fails_before_pytest(monkeypatch, options):
    monkeypatch.setattr(sys, "argv", [str(SCRIPT), *options, "--list-targets"])
    with pytest.raises(SystemExit) as error:
        scope.main()
    assert error.value.code == 2
