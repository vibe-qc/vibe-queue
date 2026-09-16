"""Schema-/3 release-tree evidence for make_release_report.py.

Under the release-only CI policy (2026-07-26) every component gate is
proven by the single release-gate pipeline on the exact release tree, so
all four pins carry the release commit and rule A evidence at that SHA.
Ordinary main pipelines no longer exist and must never be consulted.
"""

from __future__ import annotations

import importlib.util
import json
import os
import stat
import sys
from pathlib import Path

import jsonschema
import pytest


@pytest.fixture(scope="module")
def report_script():
    path = Path(__file__).parents[1] / "scripts" / "make_release_report.py"
    spec = importlib.util.spec_from_file_location("make_release_report", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


RELEASE_SHA = "a" * 40
TAG_OBJECT = "e" * 40


def test_report_output_refuses_product_paths_and_symlink_escape(report_script, tmp_path):
    source = tmp_path / "source"
    outside = tmp_path / "private"
    source.mkdir()
    outside.mkdir()
    (source / "external").symlink_to(outside, target_is_directory=True)
    for candidate in (source / "report.json", source / "external" / "report.json"):
        with pytest.raises(ValueError, match="outside product"):
            report_script._output_path(str(candidate), source_repos=[source])
    accepted = report_script._output_path(str(outside / "report.json"), source_repos=[source])
    assert accepted == outside / "report.json"


def test_report_output_refuses_git_metadata_and_bare_databases(report_script, tmp_path):
    with pytest.raises(ValueError, match="Git metadata"):
        report_script._output_path(str(tmp_path / ".git" / "report.json"), source_repos=[])
    bare = tmp_path / "archive.git"
    (bare / "objects").mkdir(parents=True)
    (bare / "refs").mkdir()
    (bare / "HEAD").write_text("ref: refs/heads/main\n")
    with pytest.raises(ValueError, match="Git object stores"):
        report_script._output_path(str(bare / "report.json"), source_repos=[])


def test_report_records_are_private_and_do_not_truncate_hardlinks(report_script, tmp_path):
    path = tmp_path / "report.json"
    report_script._write_private_report(path, '{"accepted":true}')
    assert path.read_text() == '{"accepted":true}\n'
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    alias = tmp_path / "alias.json"
    os.link(path, alias)
    with pytest.raises(ValueError, match="one link"):
        report_script._write_private_report(path, "changed")
    assert alias.read_text() == '{"accepted":true}\n'

VERSIONS = {
    (RELEASE_SHA, "pyproject.toml"): "0.15.62",
    (RELEASE_SHA, "vibe-queue/pyproject.toml"): "0.19.0",
    (RELEASE_SHA, "vibe-view/pyproject.toml"): "2.6.0",
    (RELEASE_SHA, "vibe-basis/pyproject.toml"): "0.4.0",
}


def _fake_git(*args: str) -> str:
    if args == ("rev-parse", "v0.15.62^{commit}"):
        return RELEASE_SHA
    if args == ("rev-parse", "v0.15.62"):
        return TAG_OBJECT
    raise AssertionError(args)


def _release_tree_evidence(sha: str, *, gating_job: str = "build-test", **_):
    """Evidence from the one release-gate pipeline at the exact tree."""
    assert sha == RELEASE_SHA, (
        "component evidence must be requested at the exact release tree, "
        f"got {sha}"
    )
    return {
        "pipeline_id": 4700,
        "pipeline_status": "success",
        "ref": "v0.15.62",
        "sha": sha,
        "gating_job": gating_job,
        "gating_job_status": "success",
        "web_url": "https://gitlab.example/pipelines/4700",
    }


def test_main_pins_every_component_to_the_release_tree(
    report_script,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    output = tmp_path / "report.json"
    monkeypatch.setattr(report_script, "git", _fake_git)
    # These two exercise main() as it behaved in the monorepo.
    monkeypatch.setattr(report_script, "_tracked_at_head", lambda path: True)
    monkeypatch.setattr(report_script, "gating_evidence", _release_tree_evidence)
    monkeypatch.setattr(
        report_script,
        "classify_release_pin",
        lambda sha: {
            "rule": "A",
            "accepted": True,
            "ci_evidence": _release_tree_evidence(sha),
        },
    )
    monkeypatch.setattr(
        report_script,
        "version_at",
        lambda sha, path: VERSIONS[(sha, path)],
    )
    monkeypatch.setattr(
        report_script.sys,
        "argv",
        [
            "make_release_report.py",
            "--tag",
            "v0.15.62",
            "--generated-at",
            "2026-07-26T20:00:00Z",
            "-o",
            str(output),
        ],
    )

    assert report_script.main() == 0
    payload = json.loads(output.read_text())
    assert payload["schema"] == "vq.fleet.release_report/3"
    assert payload["all_pins_accepted"] is True
    for name in ("release", "dev", "vq", "vibe_view"):
        pin = payload["pins"][name]
        assert pin["sha"] == RELEASE_SHA
        assert pin["acceptance_rule"] == "A"
        assert pin["ci_evidence"]["sha"] == RELEASE_SHA
        assert pin["ci_evidence"]["pipeline_id"] == 4700
    assert (
        payload["pins"]["vq"]["ci_evidence"]["gating_job"]
        == report_script.GATING_JOBS["vq"]
    )
    assert (
        payload["pins"]["vibe_view"]["ci_evidence"]["gating_job"]
        == report_script.GATING_JOBS["vibe_view"]
    )
    assert payload["sibling_versions_at_release"] == {
        "vq": "0.19.0",
        "vibe_view": "2.6.0",
        "vibe_basis": "0.4.0",
    }
    schema = json.loads(
        (
            Path(__file__).parents[1]
            / "docs"
            / "fleet_release_report.schema.json"
        ).read_text()
    )
    jsonschema.validate(payload, schema)


def test_missing_component_gate_at_release_tree_rejects(
    report_script,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A gate absent from the release-tree pipeline is rule C, not a
    license to walk main pipelines for older evidence."""
    output = tmp_path / "report.json"

    def evidence_without_vq(sha: str, *, gating_job: str = "build-test", **_):
        if gating_job == report_script.GATING_JOBS["vq"]:
            return None
        return _release_tree_evidence(sha, gating_job=gating_job)

    monkeypatch.setattr(report_script, "git", _fake_git)
    # These two exercise main() as it behaved in the monorepo.
    monkeypatch.setattr(report_script, "_tracked_at_head", lambda path: True)
    monkeypatch.setattr(report_script, "gating_evidence", evidence_without_vq)
    monkeypatch.setattr(
        report_script,
        "classify_release_pin",
        lambda sha: {
            "rule": "A",
            "accepted": True,
            "ci_evidence": _release_tree_evidence(sha),
        },
    )
    monkeypatch.setattr(
        report_script,
        "version_at",
        lambda sha, path: VERSIONS[(sha, path)],
    )
    monkeypatch.setattr(
        report_script.sys,
        "argv",
        ["make_release_report.py", "--tag", "v0.15.62", "-o", str(output)],
    )

    assert report_script.main() == 1
    payload = json.loads(output.read_text())
    assert payload["all_pins_accepted"] is False
    vq_pin = payload["pins"]["vq"]
    assert vq_pin["accepted"] is False
    assert vq_pin["acceptance_rule"] == "C"
    assert vq_pin["sha"] == RELEASE_SHA
    assert "exact release tree" in vq_pin["remediation"]


# --- ref filtering -------------------------------------------------------
#
# The tests above stub out gating_evidence, so they never exercise which
# pipeline it is willing to believe. These do. The regression they pin is
# real: v0.15.62's report was generated with main pipeline 4630 as the
# release-pin evidence while release-gate pipeline 4631 had failed
# build-test on the identical tree.


@pytest.mark.parametrize(
    "ref",
    ["v0.15.62", "v1.0.0", "release-candidate/v0.15.63", "release-candidate/fix"],
)
def test_release_gate_refs_are_accepted(report_script, ref: str) -> None:
    assert report_script.is_release_gate_ref(ref) is True


@pytest.mark.parametrize(
    "ref",
    ["main", "release", "v0.15", "v0.15.62-rollout", "feature/v1.0.0", None, 4630],
)
def test_non_release_gate_refs_are_rejected(report_script, ref) -> None:
    assert report_script.is_release_gate_ref(ref) is False


def _pipeline(pid: int, ref: str, status: str = "success") -> dict:
    return {
        "id": pid,
        "status": status,
        "ref": ref,
        "sha": RELEASE_SHA,
        "web_url": f"https://gitlab.example/pipelines/{pid}",
    }


def test_gating_evidence_ignores_main_pipeline_and_reports_none(
    report_script,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The v0.15.62 shape: a green main pipeline alongside a release-gate
    pipeline that failed the very gate being asked about. The green main
    job must not be usable as evidence, and no evidence must be returned."""
    monkeypatch.setattr(
        report_script,
        "glab_pipelines",
        lambda **_: [
            _pipeline(4632, "release"),
            _pipeline(4631, "v0.15.62", status="failed"),
            _pipeline(4630, "main"),
        ],
    )
    jobs = {
        4632: [{"name": "docs-build", "status": "success"}],
        4631: [
            {"name": "build-test", "status": "failed"},
            {"name": "test-vq", "status": "success"},
        ],
        4630: [{"name": "build-test", "status": "success"}],
    }
    monkeypatch.setattr(report_script, "glab_jobs", lambda pid, **_: jobs[pid])

    assert report_script.gating_evidence(RELEASE_SHA, gating_job="build-test") is None


def test_gating_evidence_takes_a_green_gate_from_a_failed_release_gate_run(
    report_script,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Each component is gated by its own job, so a green test-vq inside a
    release-gate pipeline that failed build-test is genuine vq evidence."""
    monkeypatch.setattr(
        report_script,
        "glab_pipelines",
        lambda **_: [_pipeline(4631, "v0.15.62", status="failed")],
    )
    monkeypatch.setattr(
        report_script,
        "glab_jobs",
        lambda pid, **_: [
            {"name": "build-test", "status": "failed"},
            {"name": "test-vq", "status": "success"},
        ],
    )

    evidence = report_script.gating_evidence(RELEASE_SHA, gating_job="test-vq")
    assert evidence is not None
    assert evidence["pipeline_id"] == 4631
    assert evidence["ref"] == "v0.15.62"
    assert evidence["gating_job_status"] == "success"


def test_gating_evidence_accepts_release_candidate_branch(
    report_script,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Evidence before the tag exists comes from release-candidate/*."""
    monkeypatch.setattr(
        report_script,
        "glab_pipelines",
        lambda **_: [
            _pipeline(4630, "main"),
            _pipeline(4636, "release-candidate/v0.15.63"),
        ],
    )
    monkeypatch.setattr(
        report_script,
        "glab_jobs",
        lambda pid, **_: [{"name": "build-test", "status": "success"}],
    )

    evidence = report_script.gating_evidence(RELEASE_SHA, gating_job="build-test")
    assert evidence is not None
    assert evidence["pipeline_id"] == 4636
    assert evidence["ref"] == "release-candidate/v0.15.63"


def test_pin_sources_cover_every_pin_and_span_three_projects(report_script) -> None:
    """The generator and the reader must agree on where each pin resolves."""
    from vq import fleet_release

    assert report_script.PIN_SOURCES == fleet_release.PIN_SOURCES
    assert set(report_script.PIN_SOURCES) == set(fleet_release.PIN_NAMES)
    assert {pid for _, pid in report_script.PIN_SOURCES.values()} == {34, 35, 36}


def test_split_layout_is_detected_not_assumed(
    report_script,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`vibe-queue/` and `vibe-view/` present means the monorepo shape."""
    monkeypatch.setattr(report_script, "_tracked_at_head", lambda path: True)
    assert report_script.monorepo_layout() is True
    monkeypatch.setattr(report_script, "_tracked_at_head", lambda path: False)
    assert report_script.monorepo_layout() is False


def test_split_layout_requires_the_sibling_checkouts(
    report_script,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Refuse rather than invent a sibling SHA that cannot exist.

    The release SHA is a vibe-qc commit; it is not in the vibe-queue or
    vibe-view repositories at all, so those pins must be resolved in their
    own checkouts or not at all.
    """
    monkeypatch.setattr(report_script, "_tracked_at_head", lambda path: False)
    monkeypatch.setattr(sys, "argv", ["make_release_report.py", "--tag", "v0.16.0"])
    with pytest.raises(SystemExit) as excinfo:
        report_script.main()
    message = str(excinfo.value)
    assert "--vq-repo" in message and "--vibe-view-repo" in message
    assert "does not exist in theirs" in message


def test_sibling_pin_resolves_in_its_own_repository(
    report_script,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A sibling pins its OWN tag, version and project -- never the anchor's."""
    sibling_sha = "c" * 40
    calls: list[tuple] = []

    def fake_git_in(repo, *args):
        calls.append((repo, args))
        if args[:1] == ("tag",):
            return "v0.25.6\nv0.25.7\nv0.25.10\nnot-a-tag\n"
        if args[:1] == ("rev-parse",):
            return sibling_sha
        if args[:1] == ("show",):
            return 'version = "0.25.10"\n'
        raise AssertionError(args)

    def fake_evidence(sha, *, gating_job, project_id):
        assert sha == sibling_sha
        assert project_id == 36, "vq evidence must be looked up in project 36"
        assert gating_job == "test", "the split repo's suite is named `test`"
        return {"pipeline_id": 77, "gating_job": gating_job}

    monkeypatch.setattr(report_script, "git_in", fake_git_in)
    monkeypatch.setattr(report_script, "gating_evidence", fake_evidence)

    pin = report_script.sibling_pin("vq", "/somewhere/vibe-queue")

    assert pin["repo"] == "mpei/vibe-queue"
    assert pin["project_id"] == 36
    assert pin["sha"] == sibling_sha
    assert pin["version"] == "0.25.10"
    # Version order, not lexicographic: v0.25.10 is newer than v0.25.7.
    assert pin["ref_resolved_from"] == "v0.25.10"
    assert pin["accepted"] is True
    assert pin["acceptance_rule"] == "A"
    assert pin["deploy_flags"] == ["--expected-sha", sibling_sha]
    assert all(repo == "/somewhere/vibe-queue" for repo, _ in calls)


def test_sibling_pin_without_evidence_is_rejected_and_says_where_to_look(
    report_script,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_git_in(repo, *args):
        if args[:1] == ("tag",):
            return "v2.15.2\n"
        if args[:1] == ("rev-parse",):
            return "d" * 40
        return 'version = "2.15.2"\n'

    monkeypatch.setattr(report_script, "git_in", fake_git_in)
    monkeypatch.setattr(report_script, "gating_evidence", lambda *a, **k: None)

    pin = report_script.sibling_pin("vibe_view", "/somewhere/vibe-view")

    assert pin["accepted"] is False
    assert pin["acceptance_rule"] == "C"
    assert pin["ci_evidence"] is None
    # Point at the project that actually runs the gate, not at vibe-qc.
    assert "project 35" in pin["remediation"]
