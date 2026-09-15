"""Accepted-report discovery for ``vq admin rollout-latest``."""

from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path

import pytest

from vq import fleet_release


def _report(tag: str, sha: str) -> dict[str, object]:
    version = tag.removeprefix("v")

    def pin(name: str, *, pin_version: str) -> dict[str, object]:
        gating_job = fleet_release.PIN_GATING_JOBS[name]
        flags = (
            ["--tag", tag, "--expected-sha", sha]
            if name == "release"
            else ["--expected-sha", sha]
        )
        payload: dict[str, object] = {
            "accepted": True,
            "acceptance_rule": "A",
            "sha": sha,
            "version": pin_version,
            "deploy_flags": flags,
            "ci_evidence": {
                "pipeline_id": 4400,
                "pipeline_status": "success",
                # The release-gate ref for this tag. This fixture said "main"
                # until 2026-07-26, which is precisely why nothing on the
                # deploy side ever exercised evidence provenance.
                "ref": tag,
                "sha": sha,
                "gating_job": gating_job,
                "gating_job_status": "success",
                "web_url": "https://gitlab.example/pipelines/4400",
            },
        }
        repo, project_id = fleet_release.PIN_SOURCES[name]
        payload["repo"] = repo
        payload["project_id"] = project_id
        if name == "release":
            payload["tag"] = tag
            payload["tag_object"] = sha
        return payload

    return {
        "schema": fleet_release.REPORT_SCHEMA,
        "generated_at": "2026-07-25T20:00:00Z",
        "all_pins_accepted": True,
        "pins": {
            "release": pin("release", pin_version=version),
            "dev": pin("dev", pin_version=version),
            "vq": pin("vq", pin_version="0.17.0"),
            "vibe_view": pin("vibe_view", pin_version="2.5.0"),
        },
    }


def _pin_repos(repo: Path) -> dict[str, Path]:
    """Point every /3 pin slug at one temp repo.

    Production splits these across three checkouts; the fixtures keep one, so
    the mapping is what makes a /3 report verifiable here at all.
    """
    return {slug: repo for slug, _ in fleet_release.PIN_SOURCES.values()}


def _git(repo: Path, *args: str) -> str:
    proc = subprocess.run(
        ["git", "-C", str(repo), *args],
        capture_output=True,
        text=True,
        check=True,
    )
    return proc.stdout.strip()


def _commit(repo: Path, message: str) -> str:
    _git(repo, "add", ".")
    _git(repo, "commit", "-m", message)
    return _git(repo, "rev-parse", "HEAD")


def _init_repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-b", "main")
    _git(repo, "config", "user.name", "Test")
    _git(repo, "config", "user.email", "test@example.com")
    (repo / "README").write_text("base\n")
    _commit(repo, "base")
    return repo


def _write_report(repo: Path, tag: str, sha: str) -> Path:
    path = repo / fleet_release.REPORT_DIRECTORY / f"{tag}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_report(tag, sha), indent=2, sort_keys=True) + "\n")
    return path


def test_parse_report_requires_a_supported_schema_and_every_component() -> None:
    sha = "a" * 40
    payload = _report("v0.15.60", sha)
    payload["schema"] = "vq.fleet.release_report/1"
    with pytest.raises(fleet_release.FleetReleaseError, match="schema must be"):
        fleet_release.parse_report(
            payload,
            source_ref="origin/main",
            source_path="vibe-queue/releases/v0.15.60.json",
            raw_bytes=b"{}",
        )

    payload = _report("v0.15.60", sha)
    del payload["pins"]["vibe_view"]  # type: ignore[index]
    with pytest.raises(fleet_release.FleetReleaseError, match="pins mismatch"):
        fleet_release.parse_report(
            payload,
            source_ref="origin/main",
            source_path="vibe-queue/releases/v0.15.60.json",
            raw_bytes=b"{}",
        )


def _as_v2(payload: dict) -> dict:
    """Rewrite a /3 fixture into the /2 shape a committed report still has.

    That means the monorepo's gate names too: one pipeline ran every
    component, so the jobs needed distinct names.
    """
    payload["schema"] = "vq.fleet.release_report/2"
    payload["project_id"] = 19
    for name, pin in payload["pins"].items():
        pin.pop("repo", None)
        pin.pop("project_id", None)
        pin["ci_evidence"]["gating_job"] = fleet_release.LEGACY_PIN_GATING_JOBS[name]
    return payload


def test_parse_report_still_reads_committed_v2_reports() -> None:
    """The compatibility window is load-bearing, not politeness.

    ``rollout-latest`` reads previously committed reports to prove a
    supersession chain, so a vq that refused /2 could not read its own
    history. Every report on the fleet today is /2.
    """
    sha = "a" * 40
    report = fleet_release.parse_report(
        _as_v2(_report("v0.15.60", sha)),
        source_ref="origin/main",
        source_path="vibe-queue/releases/v0.15.60.json",
        raw_bytes=b"{}",
    )
    assert report.pins["vq"].sha == sha
    # A /2 pin states no origin, so it is attributed to the monorepo it
    # actually came from -- not guessed from the pin name. project 36 did
    # not exist when this report was written.
    for name in fleet_release.PIN_NAMES:
        assert report.pins[name].repo == "mpei/vibeqc"
        assert report.pins[name].project_id == 19


def test_parse_report_records_each_v3_pin_origin() -> None:
    sha = "c" * 40
    report = fleet_release.parse_report(
        _report("v0.15.60", sha),
        source_ref="origin/main",
        source_path="vibe-queue/releases/v0.15.60.json",
        raw_bytes=b"{}",
    )
    # The split's whole point: these four pins no longer share one project.
    assert report.pins["release"].project_id == 34
    assert report.pins["dev"].project_id == 34
    assert report.pins["vq"].project_id == 36
    assert report.pins["vibe_view"].project_id == 35
    assert {p.project_id for p in report.pins.values()} == {34, 35, 36}


def test_parse_report_requires_pin_origin_under_v3() -> None:
    payload = _report("v0.15.60", "d" * 40)
    del payload["pins"]["vq"]["repo"]  # type: ignore[index]
    with pytest.raises(fleet_release.FleetReleaseError, match="pins.vq.repo"):
        fleet_release.parse_report(
            payload,
            source_ref="origin/main",
            source_path="vibe-queue/releases/v0.15.60.json",
            raw_bytes=b"{}",
        )


def test_parse_report_rejects_a_pin_pointing_at_the_wrong_project() -> None:
    """A misattributed pin sends evidence lookup to the wrong repository.

    There the SHA either does not resolve or -- far worse -- names an
    unrelated commit that happens to exist.
    """
    payload = _report("v0.15.60", "e" * 40)
    payload["pins"]["vq"]["project_id"] = 34  # type: ignore[index]
    payload["pins"]["vq"]["repo"] = "mpei/vibe-qc"  # type: ignore[index]
    with pytest.raises(fleet_release.FleetReleaseError, match="resolves in 'mpei/vibe-queue'"):
        fleet_release.parse_report(
            payload,
            source_ref="origin/main",
            source_path="vibe-queue/releases/v0.15.60.json",
            raw_bytes=b"{}",
        )


def test_parse_report_rejects_wrong_component_gate_and_flags() -> None:
    sha = "b" * 40
    payload = _report("v0.15.60", sha)
    payload["pins"]["vq"]["ci_evidence"]["gating_job"] = "build-test"  # type: ignore[index]
    expected = fleet_release.PIN_GATING_JOBS["vq"]
    with pytest.raises(
        fleet_release.FleetReleaseError, match=f"requires {expected!r}"
    ):
        fleet_release.parse_report(
            payload,
            source_ref="origin/main",
            source_path="vibe-queue/releases/v0.15.60.json",
            raw_bytes=b"{}",
        )

    payload = _report("v0.15.60", sha)
    payload["pins"]["release"]["deploy_flags"] = ["--expected-sha", sha]  # type: ignore[index]
    with pytest.raises(fleet_release.FleetReleaseError, match="deploy_flags"):
        fleet_release.parse_report(
            payload,
            source_ref="origin/main",
            source_path="vibe-queue/releases/v0.15.60.json",
            raw_bytes=b"{}",
        )

    payload = _report("v0.15.60", sha)
    payload["pins"]["vq"]["ci_evidence"]["sha"] = "c" * 40  # type: ignore[index]
    with pytest.raises(
        fleet_release.FleetReleaseError,
        match="rule A evidence SHA must equal pin SHA",
    ):
        fleet_release.parse_report(
            payload,
            source_ref="origin/main",
            source_path="vibe-queue/releases/v0.15.60.json",
            raw_bytes=b"{}",
        )


@pytest.mark.parametrize(
    "pipeline_status",
    ["failed", "canceled", "waiting_for_resource"],
)
def test_parse_report_uses_component_job_not_aggregate_pipeline_status(
    pipeline_status: str,
) -> None:
    sha = "d" * 40
    payload = _report("v0.15.60", sha)
    payload["pins"]["vq"]["ci_evidence"]["pipeline_status"] = pipeline_status  # type: ignore[index]

    parsed = fleet_release.parse_report(
        payload,
        source_ref="origin/main",
        source_path="vibe-queue/releases/v0.15.60.json",
        raw_bytes=b"{}",
    )

    assert parsed.pins["vq"].gating_job == fleet_release.PIN_GATING_JOBS["vq"]
    assert parsed.pins["vq"].pipeline_id == 4400


def test_discovery_selects_newest_accepted_report(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path)
    sha_59 = _git(repo, "rev-parse", "HEAD")
    _git(repo, "tag", "v0.15.59", sha_59)
    report_59 = _write_report(repo, "v0.15.59", sha_59)
    _commit(repo, "report 59")

    (repo / "README").write_text("release 60\n")
    sha_60 = _commit(repo, "release 60")
    _git(repo, "tag", "v0.15.60", sha_60)
    report_60 = _write_report(repo, "v0.15.60", sha_60)
    _commit(repo, "report 60")
    _git(repo, "update-ref", "refs/remotes/origin/main", "HEAD")

    selected = fleet_release.discover_latest_report(repo, fetch=False, pin_repos=_pin_repos(repo))

    assert selected.release.tag == "v0.15.60"
    assert selected.release.sha == sha_60
    assert selected.source_path.endswith("v0.15.60.json")
    assert selected.digest_sha256 == hashlib.sha256(report_60.read_bytes()).hexdigest()
    assert report_59.exists()


def test_discovery_selects_newest_accepted_report_within_series(
    tmp_path: Path,
) -> None:
    repo = _init_repo(tmp_path)
    sha_15 = _git(repo, "rev-parse", "HEAD")
    _git(repo, "tag", "v0.15.60", sha_15)
    _write_report(repo, "v0.15.60", sha_15)
    _commit(repo, "report 0.15.60")

    (repo / "README").write_text("release 0.16.0\n")
    sha_16 = _commit(repo, "release 0.16.0")
    _git(repo, "tag", "v0.16.0", sha_16)
    _write_report(repo, "v0.16.0", sha_16)
    _commit(repo, "report 0.16.0")
    _git(repo, "update-ref", "refs/remotes/origin/main", "HEAD")

    selected = fleet_release.discover_latest_report(
        repo,
        fetch=False,
        series=(0, 15),
        pin_repos=_pin_repos(repo),
    )

    assert selected.release.tag == "v0.15.60"
    assert selected.release.sha == sha_15


def test_discovery_skips_rejected_newer_candidate(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path)
    sha_59 = _git(repo, "rev-parse", "HEAD")
    _git(repo, "tag", "v0.15.59", sha_59)
    _write_report(repo, "v0.15.59", sha_59)
    _commit(repo, "report 59")

    (repo / "README").write_text("release 60\n")
    sha_60 = _commit(repo, "release 60")
    _git(repo, "tag", "v0.15.60", sha_60)
    bad = _report("v0.15.60", sha_60)
    bad["all_pins_accepted"] = False
    path = repo / fleet_release.REPORT_DIRECTORY / "v0.15.60.json"
    path.write_text(json.dumps(bad) + "\n")
    _commit(repo, "rejected report 60")
    _git(repo, "update-ref", "refs/remotes/origin/main", "HEAD")

    selected = fleet_release.discover_latest_report(repo, fetch=False, pin_repos=_pin_repos(repo))

    assert selected.release.tag == "v0.15.59"


def test_explicit_report_discovery_selects_only_named_identity(
    tmp_path: Path,
) -> None:
    repo = _init_repo(tmp_path)
    sha_59 = _git(repo, "rev-parse", "HEAD")
    _git(repo, "tag", "v0.15.59", sha_59)
    _write_report(repo, "v0.15.59", sha_59)
    _commit(repo, "report 59")

    (repo / "README").write_text("release 60\n")
    sha_60 = _commit(repo, "release 60")
    _git(repo, "tag", "v0.15.60", sha_60)
    _write_report(repo, "v0.15.60", sha_60)
    _commit(repo, "report 60")
    _git(repo, "update-ref", "refs/remotes/origin/main", "HEAD")

    selected = fleet_release.discover_report(
        "v0.15.59", repo, fetch=False,
        pin_repos=_pin_repos(repo),
    )

    assert selected.release.tag == "v0.15.59"
    assert selected.release.sha == sha_59


@pytest.mark.parametrize("selected_state", ["missing", "damaged"])
def test_explicit_report_discovery_never_falls_back(
    selected_state: str,
    tmp_path: Path,
) -> None:
    repo = _init_repo(tmp_path)
    sha_59 = _git(repo, "rev-parse", "HEAD")
    _git(repo, "tag", "v0.15.59", sha_59)
    _write_report(repo, "v0.15.59", sha_59)
    _commit(repo, "accepted fallback candidate")

    (repo / "README").write_text("release 60\n")
    sha_60 = _commit(repo, "release 60")
    _git(repo, "tag", "v0.15.60", sha_60)
    if selected_state == "damaged":
        path = repo / fleet_release.REPORT_DIRECTORY / "v0.15.60.json"
        path.write_text("{not-json\n")
        _commit(repo, "damaged selected report")
    _git(repo, "update-ref", "refs/remotes/origin/main", "HEAD")

    with pytest.raises(
        fleet_release.FleetReleaseError,
        match="accepted report 'v0.15.60' could not be selected",
    ):
        fleet_release.discover_report("v0.15.60", repo, fetch=False, pin_repos=_pin_repos(repo))


def test_explicit_report_discovery_rejects_invalid_main_evidence(
    tmp_path: Path,
) -> None:
    repo = _init_repo(tmp_path)
    sha = _git(repo, "rev-parse", "HEAD")
    _git(repo, "tag", "v0.15.60", sha)
    payload = _report("v0.15.60", sha)
    payload["pins"]["vq"]["ci_evidence"]["ref"] = "main"  # type: ignore[index]
    path = repo / fleet_release.REPORT_DIRECTORY / "v0.15.60.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(payload) + "\n")
    _commit(repo, "invalid selected report")
    _git(repo, "update-ref", "refs/remotes/origin/main", "HEAD")

    with pytest.raises(fleet_release.FleetReleaseError, match="release-gate ref"):
        fleet_release.discover_report("v0.15.60", repo, fetch=False, pin_repos=_pin_repos(repo))


def test_discovery_rejects_moved_tag_and_non_ancestor_pin(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path)
    sha = _git(repo, "rev-parse", "HEAD")
    _git(repo, "tag", "v0.15.60", sha)
    payload = _report("v0.15.60", sha)
    payload["pins"]["dev"]["sha"] = "f" * 40  # type: ignore[index]
    payload["pins"]["dev"]["deploy_flags"] = [  # type: ignore[index]
        "--expected-sha",
        "f" * 40,
    ]
    path = repo / fleet_release.REPORT_DIRECTORY / "v0.15.60.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(payload) + "\n")
    _commit(repo, "bad ancestry")
    _git(repo, "update-ref", "refs/remotes/origin/main", "HEAD")

    with pytest.raises(
        fleet_release.FleetReleaseError,
        match="no accepted fleet release report",
    ):
        fleet_release.discover_latest_report(repo, fetch=False, pin_repos=_pin_repos(repo))


def test_discovery_rechecks_historical_b_prime_claim(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path)
    evidence_sha = _git(repo, "rev-parse", "HEAD")
    (repo / "docs").mkdir()
    (repo / "docs" / "release.md").write_text("release note\n")
    release_sha = _commit(repo, "release docs")
    _git(repo, "tag", "v0.15.60", release_sha)
    payload = _report("v0.15.60", release_sha)
    release_pin = payload["pins"]["release"]  # type: ignore[index]
    release_pin["acceptance_rule"] = "B-prime"
    release_pin["ci_evidence"]["sha"] = evidence_sha
    path = repo / fleet_release.REPORT_DIRECTORY / "v0.15.60.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(payload) + "\n")
    _commit(repo, "accepted B-prime report")
    _git(repo, "update-ref", "refs/remotes/origin/main", "HEAD")

    selected = fleet_release.discover_latest_report(repo, fetch=False, pin_repos=_pin_repos(repo))
    assert selected.release.sha == release_sha
    assert selected.release.acceptance_rule == "B-prime"

    bad_root = tmp_path / "bad"
    bad_root.mkdir()
    bad_repo = _init_repo(bad_root)
    bad_evidence = _git(bad_repo, "rev-parse", "HEAD")
    (bad_repo / "python").mkdir()
    (bad_repo / "python" / "core.py").write_text("changed = True\n")
    bad_release = _commit(bad_repo, "native change")
    _git(bad_repo, "tag", "v0.15.60", bad_release)
    bad_payload = _report("v0.15.60", bad_release)
    bad_pin = bad_payload["pins"]["release"]  # type: ignore[index]
    bad_pin["acceptance_rule"] = "B-prime"
    bad_pin["ci_evidence"]["sha"] = bad_evidence
    bad_path = (
        bad_repo / fleet_release.REPORT_DIRECTORY / "v0.15.60.json"
    )
    bad_path.parent.mkdir(parents=True)
    bad_path.write_text(json.dumps(bad_payload) + "\n")
    _commit(bad_repo, "false B-prime report")
    _git(bad_repo, "update-ref", "refs/remotes/origin/main", "HEAD")

    with pytest.raises(
        fleet_release.FleetReleaseError,
        match="no accepted fleet release report",
    ):
        fleet_release.discover_latest_report(bad_repo, fetch=False, pin_repos=_pin_repos(bad_repo))


def test_parse_report_refuses_main_pipeline_evidence() -> None:
    """Regression: a green gate on an ordinary main pipeline is not proof.

    v0.15.62 was the demonstration -- release and dev accepted from main
    pipeline 4630 while release-gate pipeline 4631 had FAILED build-test on
    the identical tree. ``make_release_report.py`` stopped generating such a
    report in 95f12a08; this is the deploy-side half, because a report is a
    committed file that an older vq (or a hand edit) can still supply.
    """
    sha = "a" * 40
    payload = _report("v0.15.60", sha)
    payload["pins"]["dev"]["ci_evidence"]["ref"] = "main"  # type: ignore[index]

    with pytest.raises(
        fleet_release.FleetReleaseError,
        match=r"pins\.dev\.ci_evidence\.ref 'main' is not a release-gate ref",
    ):
        fleet_release.parse_report(
            payload,
            source_ref="origin/main",
            source_path="vibe-queue/releases/v0.15.60.json",
            raw_bytes=b"{}",
        )


@pytest.mark.parametrize(
    "ref",
    ["release", "release-candidate", "", None, 4631, "v0.15", "main"],
)
def test_parse_report_refuses_every_non_release_gate_ref(ref: object) -> None:
    sha = "a" * 40
    payload = _report("v0.15.60", sha)
    payload["pins"]["vq"]["ci_evidence"]["ref"] = ref  # type: ignore[index]

    with pytest.raises(fleet_release.FleetReleaseError, match="release-gate ref"):
        fleet_release.parse_report(
            payload,
            source_ref="origin/main",
            source_path="vibe-queue/releases/v0.15.60.json",
            raw_bytes=b"{}",
        )


def test_parse_report_accepts_release_candidate_evidence() -> None:
    """The pre-tag half of the release gate stays deployable: v0.15.60's own
    release pin was evidenced by ``release-candidate/v0.15.60-rollout``."""
    sha = "a" * 40
    payload = _report("v0.15.60", sha)
    for name in fleet_release.PIN_NAMES:
        payload["pins"][name]["ci_evidence"]["ref"] = (  # type: ignore[index]
            "release-candidate/v0.15.60-rollout"
        )

    parsed = fleet_release.parse_report(
        payload,
        source_ref="origin/main",
        source_path="vibe-queue/releases/v0.15.60.json",
        raw_bytes=b"{}",
    )

    assert parsed.release.tag == "v0.15.60"


def test_discovery_skips_a_main_evidenced_newer_report(tmp_path: Path) -> None:
    """The exact v0.15.62 shape: the newest report is unevidencable, so
    discovery must fall back rather than deploy it."""
    repo = _init_repo(tmp_path)
    sha_59 = _git(repo, "rev-parse", "HEAD")
    _git(repo, "tag", "v0.15.59", sha_59)
    _write_report(repo, "v0.15.59", sha_59)
    _commit(repo, "report 59")

    (repo / "README").write_text("release 60\n")
    sha_60 = _commit(repo, "release 60")
    _git(repo, "tag", "v0.15.60", sha_60)
    payload = _report("v0.15.60", sha_60)
    payload["pins"]["release"]["ci_evidence"]["ref"] = "main"  # type: ignore[index]
    path = repo / fleet_release.REPORT_DIRECTORY / "v0.15.60.json"
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    _commit(repo, "report 60 on main-pipeline evidence")
    _git(repo, "update-ref", "refs/remotes/origin/main", "HEAD")

    selected = fleet_release.discover_latest_report(repo, fetch=False, pin_repos=_pin_repos(repo))

    assert selected.release.tag == "v0.15.59"


def test_historical_discovery_authenticates_exact_retracted_blob(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = _init_repo(tmp_path)
    release_sha = _git(repo, "rev-parse", "HEAD")
    _git(repo, "tag", "v0.15.60", release_sha)
    payload = _report("v0.15.60", release_sha)
    for name in fleet_release.PIN_NAMES:
        payload["pins"][name]["ci_evidence"]["ref"] = "main"  # type: ignore[index]
    path = repo / fleet_release.REPORT_DIRECTORY / "v0.15.60.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    historical_bytes = path.read_bytes()
    historical_digest = hashlib.sha256(historical_bytes).hexdigest()
    historical_commit = _commit(repo, "legacy accepted report")

    path.unlink()
    _commit(repo, "retract legacy report")
    (repo / "HARDENED").write_text("release-gate refs required\n")
    cutoff = _commit(repo, "harden deploy evidence")
    _git(repo, "update-ref", "refs/remotes/origin/main", "HEAD")
    monkeypatch.setattr(
        fleet_release,
        "LEGACY_MAIN_EVIDENCE_CUTOFF",
        cutoff,
    )

    selected = fleet_release.discover_historical_report_by_digest(
        "vibe-queue/releases/v0.15.60.json",
        historical_digest,
        repo,
        fetch=False,
        pin_repos=_pin_repos(repo),
    )

    assert selected.source_ref == historical_commit
    assert selected.digest_sha256 == historical_digest
    assert selected.release.sha == release_sha


def test_historical_discovery_never_relaxes_post_hardening_report(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = _init_repo(tmp_path)
    release_sha = _git(repo, "rev-parse", "HEAD")
    _git(repo, "tag", "v0.15.60", release_sha)
    (repo / "HARDENED").write_text("release-gate refs required\n")
    cutoff = _commit(repo, "harden deploy evidence")
    payload = _report("v0.15.60", release_sha)
    for name in fleet_release.PIN_NAMES:
        payload["pins"][name]["ci_evidence"]["ref"] = "main"  # type: ignore[index]
    path = repo / fleet_release.REPORT_DIRECTORY / "v0.15.60.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    _commit(repo, "invalid post-hardening report")
    _git(repo, "update-ref", "refs/remotes/origin/main", "HEAD")
    monkeypatch.setattr(
        fleet_release,
        "LEGACY_MAIN_EVIDENCE_CUTOFF",
        cutoff,
    )

    with pytest.raises(
        fleet_release.FleetReleaseError,
        match="not proven to predate",
    ):
        fleet_release.discover_historical_report_by_digest(
            "vibe-queue/releases/v0.15.60.json",
            digest,
            repo,
            fetch=False,
        )


def test_release_gate_ref_predicate_matches_the_report_generator() -> None:
    """Drift guard.

    ``make_release_report.py`` is deliberately stdlib-only (its tests load it
    by path), so it cannot import ``vq`` and the predicate necessarily exists
    twice. Pin the two to one behaviour instead of hoping they stay in step --
    a generator that accepted a ref the deploy side rejects would produce
    reports that can never be rolled out, and the reverse re-opens this bug.
    """
    import importlib.util

    script = Path(__file__).parents[1] / "scripts" / "make_release_report.py"
    spec = importlib.util.spec_from_file_location("make_release_report", script)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    refs = [
        "v0.15.63",
        "v1.2.3",
        "release-candidate/v0.15.63",
        "release-candidate/anything",
        "main",
        "release",
        "v0.15",
        "vX.Y.Z",
        "",
        "feature/v1.2.3",
        None,
        4631,
    ]
    for ref in refs:
        assert fleet_release.is_release_gate_ref(ref) is module.is_release_gate_ref(
            ref
        ), ref


def test_semver_and_host_order_helpers() -> None:
    assert fleet_release.semver_from_text("v0.15.60") == (0, 15, 60)
    assert fleet_release.semver_from_text("2.5.0") == (2, 5, 0)
    assert fleet_release.semver_from_text("2.5.0.dev0") is None
    assert fleet_release.ordered_hosts(
        ["host_a", "host_f", "host_d", "host_c"],
        ["host_f", "host_c"],
    ) == ["host_f", "host_c", "host_a", "host_d"]


def test_both_split_transition_report_prefixes_are_accepted() -> None:
    """Receipts persisted on the fleet and in vibe-queue's own repo differ.

    Every receipt already on host_f and host_c records
    ``vibe-queue/releases/...``; vibe-queue's own repository holds reports at
    ``releases/...``. Accepting only one prefix is what would make vq reject
    its own history during the repointing.
    """
    assert fleet_release.is_report_path("vibe-queue/releases/v0.15.60.json")
    assert fleet_release.is_report_path("releases/v0.15.60.json")
    assert fleet_release.REPORT_DIRECTORY in fleet_release.REPORT_DIRECTORIES
    # Reports are still WRITTEN under the monorepo-relative path; only reads
    # widened. Flipping this constant is the change that breaks the fleet.
    assert fleet_release.REPORT_DIRECTORY == "vibe-queue/releases"


def test_report_path_rejects_unrelated_and_lookalike_paths() -> None:
    for bad in (
        "releases-old/v0.15.60.json",
        "vibe-queue/releases-archive/v0.15.60.json",
        "docs/releases/v0.15.60.json",
        "releases",
        "",
        None,
        123,
    ):
        assert not fleet_release.is_report_path(bad), bad


def test_discovery_finds_reports_in_the_split_layout(tmp_path: Path) -> None:
    """Discovery searches BOTH layouts, not the installed vq's own layout.

    vibe-queue's own repository holds reports at ``releases/``. Deriving one
    directory from the running vq would be the wrong question: the repository
    being searched need not be the one vq runs from.
    """
    repo = _init_repo(tmp_path)
    sha = _git(repo, "rev-parse", "HEAD")
    _git(repo, "tag", "v0.15.60", sha)
    path = repo / "releases" / "v0.15.60.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_report("v0.15.60", sha), indent=2, sort_keys=True) + "\n")
    _commit(repo, "report 60 in the split layout")
    _git(repo, "update-ref", "refs/remotes/origin/main", "HEAD")

    selected = fleet_release.discover_latest_report(repo, fetch=False, pin_repos=_pin_repos(repo))

    assert selected.source_path == "releases/v0.15.60.json"
    assert selected.release.tag == "v0.15.60"


def test_discovery_failure_quotes_a_bounded_number_of_rejections(
    tmp_path: Path,
) -> None:
    """A vibe-queue checkout sees every monorepo-era report and rejects all.

    Those tags are not in its fresh history, so without a cap the failure
    concatenates 100+ multi-line git errors and buries the newest one.
    """
    repo = _init_repo(tmp_path)
    sha = _git(repo, "rev-parse", "HEAD")
    for patch in range(40, 50):
        tag = f"v0.15.{patch}"
        path = repo / "releases" / f"{tag}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(_report(tag, sha), indent=2, sort_keys=True) + "\n"
        )
    _commit(repo, "ten reports whose tags do not exist here")
    _git(repo, "update-ref", "refs/remotes/origin/main", "HEAD")

    with pytest.raises(fleet_release.FleetReleaseError) as excinfo:
        fleet_release.discover_latest_report(repo, fetch=False, pin_repos=_pin_repos(repo))

    message = str(excinfo.value)
    # Newest-first, so the newest rejected candidate must be named.
    assert "v0.15.49" in message
    assert "+7 older candidate(s) also rejected" in message
    assert "v0.15.40" not in message


def test_gate_names_are_schema_aware() -> None:
    """A /2 report is gated by the monorepo's job names, a /3 by its own.

    The monorepo ran every component's gate in one pipeline, so the jobs
    needed distinct names. After the split each component's suite is just
    `test` in its own project, and `test-vq` / `vibe-view-test` exist
    nowhere. Applying the /3 names to a /2 report would reject the fleet's
    entire committed history.
    """
    legacy = fleet_release.pin_gating_jobs("vq.fleet.release_report/2")
    current = fleet_release.pin_gating_jobs(fleet_release.REPORT_SCHEMA)

    assert legacy["vq"] == "test-vq"
    assert legacy["vibe_view"] == "vibe-view-test"
    assert current["vq"] == "test"
    assert current["vibe_view"] == "test"
    # vibe-basis stayed inside vibe-qc, so the release gate is unchanged.
    assert legacy["release"] == current["release"] == "build-test"


def test_committed_v2_report_still_validates_against_legacy_gates() -> None:
    """The real shape on the fleet: /2 schema AND /2 gate names together."""
    report = fleet_release.parse_report(
        _as_v2(_report("v0.15.60", "a" * 40)),
        source_ref="origin/main",
        source_path="vibe-queue/releases/v0.15.60.json",
        raw_bytes=b"{}",
    )
    assert report.pins["vq"].gating_job == "test-vq"
    assert report.pins["vibe_view"].gating_job == "vibe-view-test"


def test_v3_pins_are_verified_in_their_own_repositories(tmp_path: Path) -> None:
    """The whole point of /3: each pin's provenance is checked where it lives.

    Before the split one checkout held every component, so the loader used the
    runtime clone for all four pins. Post-split the anchor tag is a vibe-qc
    tag and the reports live in vibe-queue, so resolving it in the runtime
    clone fails outright -- that is what blocked rollout-latest.
    """
    anchor = _init_repo(tmp_path)
    sha = _git(anchor, "rev-parse", "HEAD")
    _git(anchor, "tag", "v0.15.60", sha)
    _git(anchor, "update-ref", "refs/remotes/origin/main", "HEAD")

    # Reports live in a DIFFERENT repository that has no such tag.
    reports = tmp_path / "reports"
    reports.mkdir()
    _git(reports, "init", "-b", "main")
    _git(reports, "config", "user.name", "Test")
    _git(reports, "config", "user.email", "test@example.com")
    path = reports / "releases" / "v0.15.60.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(_report("v0.15.60", sha), indent=2, sort_keys=True) + "\n")
    _commit(reports, "report")
    _git(reports, "update-ref", "refs/remotes/origin/main", "HEAD")

    # Without a mapping the anchor tag cannot be resolved, and it says so.
    with pytest.raises(fleet_release.FleetReleaseError, match="no local checkout"):
        fleet_release.discover_latest_report(reports, fetch=False)

    # With each slug pointed at the repository that actually holds the commit,
    # the same report verifies.
    selected = fleet_release.discover_latest_report(
        reports, fetch=False, pin_repos=_pin_repos(anchor)
    )
    assert selected.source_path == "releases/v0.15.60.json"
    assert selected.release.sha == sha


def test_a_pin_repo_that_lacks_the_commit_is_rejected(tmp_path: Path) -> None:
    """Validating a pin in the wrong repository must fail, not pass silently.

    This is the failure the mapping exists to prevent: a SHA checked against a
    repository it does not belong to either will not resolve or, worse, names
    an unrelated commit that happens to exist there.
    """
    anchor = _init_repo(tmp_path)
    sha = _git(anchor, "rev-parse", "HEAD")
    _git(anchor, "tag", "v0.15.60", sha)
    _git(anchor, "update-ref", "refs/remotes/origin/main", "HEAD")
    path = anchor / "releases" / "v0.15.60.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(_report("v0.15.60", sha), indent=2, sort_keys=True) + "\n")
    _commit(anchor, "report")
    _git(anchor, "update-ref", "refs/remotes/origin/main", "HEAD")

    unrelated = tmp_path / "unrelated"
    unrelated.mkdir()
    _git(unrelated, "init", "-b", "main")
    _git(unrelated, "config", "user.name", "Test")
    _git(unrelated, "config", "user.email", "test@example.com")
    (unrelated / "README").write_text("different history\n")
    _commit(unrelated, "unrelated")
    _git(unrelated, "update-ref", "refs/remotes/origin/main", "HEAD")

    mapping = dict(_pin_repos(anchor))
    mapping["mpei/vibe-queue"] = unrelated  # the vq pin now points somewhere wrong

    with pytest.raises(fleet_release.FleetReleaseError, match="not an ancestor"):
        fleet_release.discover_latest_report(anchor, fetch=False, pin_repos=mapping)


def test_missing_pin_repo_names_the_slug_and_how_to_fix_it(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path)
    sha = _git(repo, "rev-parse", "HEAD")
    pin = fleet_release.FleetPin(
        name="vq", sha=sha, version="0.26.0", deploy_flags=(),
        gating_job="test", pipeline_id=1, evidence_sha=sha,
        acceptance_rule="A", repo="mpei/vibe-queue", project_id=36,
    )
    with pytest.raises(fleet_release.FleetReleaseError) as excinfo:
        fleet_release._repo_for_pin(
            pin, schema=fleet_release.REPORT_SCHEMA, runtime_repo_path=repo,
            pin_repos={}, source_path="releases/v0.17.0.json",
        )
    msg = str(excinfo.value)
    assert "mpei/vibe-queue" in msg
    assert "pin_source_repos" in msg


def test_v2_reports_still_validate_against_the_runtime_clone(tmp_path: Path) -> None:
    """/2 needs no mapping at all: its pins really were all one repository."""
    repo = _init_repo(tmp_path)
    sha = _git(repo, "rev-parse", "HEAD")
    _git(repo, "tag", "v0.15.60", sha)
    path = repo / fleet_release.REPORT_DIRECTORY / "v0.15.60.json"
    path.parent.mkdir(parents=True)
    path.write_text(
        json.dumps(_as_v2(_report("v0.15.60", sha)), indent=2, sort_keys=True) + "\n"
    )
    _commit(repo, "v2 report")
    _git(repo, "update-ref", "refs/remotes/origin/main", "HEAD")

    selected = fleet_release.discover_latest_report(repo, fetch=False)

    assert selected.source_path.endswith("v0.15.60.json")
    assert all(p.repo == "mpei/vibeqc" for p in selected.pins.values())


def test_historical_blob_is_found_under_either_layout(tmp_path: Path) -> None:
    """A pre-split receipt names the monorepo path; the blob lives elsewhere.

    Receipts persisted on the fleet record "vibe-queue/releases/<tag>.json".
    vibe-queue's own repository has fresh history and never contained that
    path -- the byte-identical report sits at "releases/<tag>.json". The
    receipt attests to the blob's CONTENT, so authentication must follow the
    digest, not the location it was written down as.
    """
    repo = _init_repo(tmp_path)
    sha = _git(repo, "rev-parse", "HEAD")
    _git(repo, "tag", "v0.15.60", sha)
    # Stored under the SPLIT path only.
    path = repo / "releases" / "v0.15.60.json"
    path.parent.mkdir(parents=True)
    payload = json.dumps(_report("v0.15.60", sha), indent=2, sort_keys=True) + "\n"
    path.write_text(payload)
    _commit(repo, "report at the split path")
    _git(repo, "update-ref", "refs/remotes/origin/main", "HEAD")
    digest = hashlib.sha256(path.read_bytes()).hexdigest()

    # The receipt names the MONOREPO path, as every fleet receipt does.
    selected = fleet_release.discover_historical_report_by_digest(
        source_path="vibe-queue/releases/v0.15.60.json",
        digest_sha256=digest,
        repo=repo,
        fetch=False,
        pin_repos=_pin_repos(repo),
    )
    assert selected.digest_sha256 == digest
    assert selected.release.tag == "v0.15.60"


def test_historical_blob_failure_names_every_path_it_tried(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path)
    _git(repo, "update-ref", "refs/remotes/origin/main", "HEAD")
    with pytest.raises(fleet_release.FleetReleaseError) as excinfo:
        fleet_release.discover_historical_report_by_digest(
            source_path="vibe-queue/releases/v0.15.60.json",
            digest_sha256="a" * 64,
            repo=repo,
            fetch=False,
            pin_repos=_pin_repos(repo),
        )
    message = str(excinfo.value)
    assert "vibe-queue/releases/v0.15.60.json" in message
    assert "releases/v0.15.60.json" in message


def test_v2_pins_resolve_in_a_retained_monorepo_checkout(tmp_path: Path) -> None:
    """Authenticating fleet history needs the pre-split checkout.

    A /2 pin is a monorepo commit and its tag is a monorepo tag. The runtime
    clone answered both before the split because it WAS the monorepo;
    vibe-queue's fresh history carries neither. Pointing "mpei/vibeqc" at a
    retained checkout restores that, which is why the rollback trees matter
    beyond rollback.
    """
    monorepo = _init_repo(tmp_path)
    sha = _git(monorepo, "rev-parse", "HEAD")
    _git(monorepo, "tag", "v0.15.60", sha)
    _git(monorepo, "update-ref", "refs/remotes/origin/main", "HEAD")

    # Reports live in vibe-queue, which has none of that history.
    reports = tmp_path / "vibe-queue"
    reports.mkdir()
    _git(reports, "init", "-b", "main")
    _git(reports, "config", "user.name", "Test")
    _git(reports, "config", "user.email", "test@example.com")
    path = reports / fleet_release.REPORT_DIRECTORY / "v0.15.60.json"
    path.parent.mkdir(parents=True)
    path.write_text(
        json.dumps(_as_v2(_report("v0.15.60", sha)), indent=2, sort_keys=True) + "\n"
    )
    _commit(reports, "v2 report")
    _git(reports, "update-ref", "refs/remotes/origin/main", "HEAD")

    # Unconfigured: the /2 pins are checked in the runtime clone, which cannot
    # resolve a monorepo tag, so it fails rather than silently accepting.
    with pytest.raises(fleet_release.FleetReleaseError):
        fleet_release.discover_latest_report(reports, fetch=False)

    selected = fleet_release.discover_latest_report(
        reports, fetch=False, pin_repos={"mpei/vibeqc": monorepo}
    )
    assert selected.release.sha == sha
    assert all(p.repo == "mpei/vibeqc" for p in selected.pins.values())


class TestRuntimeRepoRefusal:
    """A copied install disables rollout, and must say that it did.

    `runtime_repo()` resolves the controller checkout from where vq is
    imported, so a copied install lands in site-packages and rollout refuses.
    Naming the directory alone left the operator holding a path nobody chose:
    the thing to change is the install mode.
    """

    def _as_copied_install(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        module = (
            tmp_path / "venv" / "lib" / "python3.14"
            / "site-packages" / "vq" / "fleet_release.py"
        )
        module.parent.mkdir(parents=True)
        monkeypatch.setattr(fleet_release, "__file__", str(module))

    def test_a_copied_install_is_refused_by_naming_the_install_mode(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        self._as_copied_install(tmp_path, monkeypatch)

        with pytest.raises(fleet_release.FleetReleaseError) as excinfo:
            fleet_release.runtime_repo()

        message = str(excinfo.value)
        assert "COPIED" in message
        assert "--editable" in message
        # The path stays, because it is what the operator will have seen.
        assert "lib/python3.14" in message

    def test_a_non_venv_directory_keeps_the_original_refusal(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Not every failure is an install-mode problem; a checkout that
        simply is not a git repository must not be misdiagnosed as one."""
        module = tmp_path / "somewhere" / "src" / "vq" / "fleet_release.py"
        module.parent.mkdir(parents=True)
        monkeypatch.setattr(fleet_release, "__file__", str(module))

        with pytest.raises(fleet_release.FleetReleaseError) as excinfo:
            fleet_release.runtime_repo()

        message = str(excinfo.value)
        assert "requires the managed runtime clone" in message
        assert "COPIED" not in message

    def test_an_editable_install_resolves_its_checkout(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        checkout = tmp_path / "vibe-queue"
        module = checkout / "src" / "vq" / "fleet_release.py"
        module.parent.mkdir(parents=True)
        (checkout / ".git").mkdir()
        monkeypatch.setattr(fleet_release, "__file__", str(module))

        assert fleet_release.runtime_repo() == checkout
