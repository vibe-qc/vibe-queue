"""Development never creates CI; candidate qualification always runs in full."""
from __future__ import annotations

import os
import re
import shlex
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
CI = yaml.safe_load((ROOT / ".gitlab-ci.yml").read_text())


def _matches(expression, variables):
    if " || " in expression:
        return any(_matches(part, variables) for part in expression.split(" || "))
    if match := re.fullmatch(r'\$(\w+) == "([^"]*)"', expression):
        return variables.get(match[1], "") == match[2]
    if match := re.fullmatch(r"\$(\w+) =~ /(.*)/", expression):
        return re.search(match[2], variables.get(match[1], "")) is not None
    if re.fullmatch(r"\$\w+", expression):
        return bool(variables.get(expression[1:], ""))
    raise AssertionError(f"Unsupported CI rule: {expression}")


def _selected(rules, variables):
    for rule in rules:
        if "if" not in rule or _matches(rule["if"], variables):
            return None if rule.get("when") == "never" else rule
    return None


@pytest.mark.parametrize("branch", ["main", "dev", "feature/topic"])
@pytest.mark.parametrize("source", ["push", "web", "schedule", "api", "trigger", "parent_pipeline"])
def test_development_never_creates_a_pipeline(branch, source):
    variables = {"CI_COMMIT_BRANCH": branch, "CI_PIPELINE_SOURCE": source}
    assert _selected(CI["workflow"]["rules"], variables) is None


def test_merge_requests_never_create_a_pipeline():
    assert _selected(CI["workflow"]["rules"], {"CI_PIPELINE_SOURCE": "merge_request_event"}) is None


@pytest.mark.parametrize("source", ["push", "web", "schedule", "api"])
@pytest.mark.parametrize("branch,expected", [
    ("release-candidate/v1.0.0", {"ruff", "test", "docs-build"}),
    ("release", {"docs-build"}),
])
def test_release_jobs_cover_qualification_and_publication(branch, source, expected):
    variables = {"CI_COMMIT_BRANCH": branch, "CI_PIPELINE_SOURCE": source}
    assert _selected(CI["workflow"]["rules"], variables) is not None
    assert {
        name for name in ("ruff", "test", "docs-build")
        if _selected(CI[name]["rules"], variables) is not None
    } == expected


def test_candidate_invocation_cannot_be_downgraded_to_smoke():
    command = next(line for line in CI["test"]["script"] if "scripts/run_ci_tests.py" in line)
    arguments = shlex.split(command.split("python scripts/run_ci_tests.py", 1)[1])
    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts/run_ci_tests.py"), *arguments, "--list-targets"],
        env=dict(os.environ, VQ_TEST_SCOPE="fast"), text=True, capture_output=True, check=True,
    )
    assert result.stdout.strip() == "CI pytest targets: tests"
    assert "pip install -e '.[test,web]'" in CI["test"]["script"]


@pytest.mark.parametrize("variables,enabled", [
    ({}, False),
    ({"VIBE_QUEUE_CI_PROJECT": "example/deployment"}, True),
    ({"VIBE_QUEUE_CI_REF": "main"}, True),
    ({"VIBE_QUEUE_CI_FILE": "/queue.yml"}, True),
])
def test_deployment_override_is_loaded_only_with_external_job(variables, enabled):
    external, local = CI["include"]
    assert "project" in external
    assert local["local"] == "/.gitlab/ci/deploy-rules.yml"
    assert (_selected(external["rules"], variables) is not None) is enabled
    assert (_selected(local["rules"], variables) is not None) is enabled
    assert "docs-deploy" not in CI


@pytest.mark.parametrize("branch", ["main", "dev", "release-candidate/v1.0.0", "release"])
def test_optional_publishing_only_accepts_release(branch):
    override = yaml.safe_load((ROOT / ".gitlab/ci/deploy-rules.yml").read_text())
    assert set(override) == {"docs-deploy"}
    assert set(override["docs-deploy"]) == {"rules"}
    selected = _selected(override["docs-deploy"]["rules"], {
        "CI_COMMIT_BRANCH": branch, "CI_PIPELINE_SOURCE": "web",
    })
    if branch == "release":
        assert selected["when"] == "manual"
        assert selected["allow_failure"] is True
    else:
        assert selected is None
