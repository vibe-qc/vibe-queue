"""Report-pinned scheduler-helper staging, canonical recording, metrics.

The accepted release report is the sole source of every deployed vq
identity. These tests pin the three mechanisms that enforce it for the
scheduler helpers:

* ``--expected-sha`` staging archives the exact pinned commit from the
  managed runtime repository, never the live driver checkout;
* the helper's verified identity is recorded canonically (same store as
  runtime lanes) so a second rollout proves "already at the pin" without
  comparing against the driver tree;
* ``VQ-DEPLOY-METRIC`` transcript lines become structured metrics on
  results and records.
"""

from __future__ import annotations

import json
import subprocess
import tarfile
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from vq import admin, config, paths
from vq.cli import main

PIN_TREE_VALUE = "VALUE = 'pinned'\n"


def _git(repo: Path, *args: str) -> str:
    proc = subprocess.run(
        ["git", "-C", str(repo), *args],
        capture_output=True,
        text=True,
        check=True,
    )
    return proc.stdout.strip()


@pytest.fixture
def pinned_repo(tmp_path: Path) -> tuple[Path, str]:
    """A tiny monorepo-shaped git checkout with one committed helper tree."""
    repo = tmp_path / "monorepo"
    project = repo / "vibe-queue"
    package = project / "src" / "vq"
    package.mkdir(parents=True)
    (project / "pyproject.toml").write_text(
        '[project]\nname = "vq"\nversion = "0.19.0"\n', encoding="utf-8"
    )
    (package / "__init__.py").write_text(PIN_TREE_VALUE, encoding="utf-8")
    _git(repo, "init", "--quiet")
    _git(repo, "config", "user.email", "test@example.invalid")
    _git(repo, "config", "user.name", "Test")
    _git(repo, "add", "-A")
    _git(repo, "-c", "commit.gpgsign=false", "commit", "--quiet", "-m", "pin")
    return project, _git(repo, "rev-parse", "HEAD")


def test_pinned_staging_archives_the_pin_not_the_worktree(
    pinned_repo: tuple[Path, str],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    project, pin = pinned_repo
    package = project / "src" / "vq"
    # The live checkout moves ahead AND is dirty: neither may leak into a
    # pinned stage, and a dirty worktree must not block it (git archive of
    # an exact SHA reads immutable objects).
    (package / "__init__.py").write_text("VALUE = 'ahead'\n", encoding="utf-8")

    uploaded: dict[str, bytes] = {}

    def fake_upload(host_cfg, local_path, remote_path, **kwargs):  # type: ignore[no-untyped-def]
        uploaded[Path(remote_path).name] = Path(local_path).read_bytes()

    def fake_remote(host_cfg, *argv, **kwargs):  # type: ignore[no-untyped-def]
        return subprocess.CompletedProcess(
            args=list(argv), returncode=0, stdout="ok\n", stderr=""
        )

    monkeypatch.setattr("vq.admin._scheduler_helper_project_root", lambda: project)
    monkeypatch.setattr("vq.admin.transport.upload_file", fake_upload)
    monkeypatch.setattr("vq.admin.transport.run_remote_shell", fake_remote)
    host_cfg = config.HostConfig(
        ssh="cluster",
        scheduler="slurm",
        scheduler_dialect="slurm",
        scheduler_driver="localhost",
        scratch_root="/shared/jobs",
        scheduler_update_stage="/shared/vq-admin/cluster",
    )
    result = admin.SchedulerHostUpdateResult(
        host="cluster",
        ssh="cluster",
        scheduler="slurm",
        mode="update",
        command="update-helper",
    )

    stage = admin._stage_scheduler_helper_source(
        "cluster", host_cfg, host_cfg, result, expected_sha=pin
    )

    assert stage.startswith(f"/shared/vq-admin/cluster/generations/{pin}-")
    assert result.stage_source == "report-pin"
    assert result.expected_source_sha == pin
    assert uploaded["SOURCE-SHA"].decode().strip() == pin

    # The archived tree is the committed pin, not the dirty/ahead worktree,
    # and the recorded tree digest describes exactly that archive.
    extract = tmp_path / "extract"
    extract.mkdir()
    archive = tmp_path / "stage.tar.gz"
    archive.write_bytes(uploaded["vibe-queue-src.tar.gz"])
    with tarfile.open(archive, "r:gz") as tar:
        tar.extractall(extract, filter="data")
    staged_init = extract / "vibe-queue" / "src" / "vq" / "__init__.py"
    assert staged_init.read_text(encoding="utf-8") == PIN_TREE_VALUE
    expected_digest = admin.source_tree_sha256(extract / "vibe-queue" / "src" / "vq")
    assert result.expected_source_tree_sha256 == expected_digest
    assert uploaded["SOURCE-TREE-SHA256"].decode().strip() == expected_digest


def test_accepted_commit_tree_digest_ignores_the_live_worktree(
    pinned_repo: tuple[Path, str],
) -> None:
    project, pin = pinned_repo
    package = project / "src" / "vq"
    expected = admin.source_tree_sha256(package)

    (package / "__init__.py").write_text("VALUE = 'dirty'\n", encoding="utf-8")
    (package / "untracked.py").write_text("UNTRACKED = True\n", encoding="utf-8")

    assert admin.source_tree_sha256_at_git_commit(project, pin) == expected
    assert admin.source_tree_sha256(package) != expected

    _git(project.parent, "add", "-A")
    _git(
        project.parent,
        "-c",
        "commit.gpgsign=false",
        "commit",
        "--quiet",
        "-m",
        "newer",
    )
    newer = _git(project.parent, "rev-parse", "HEAD")
    assert admin.source_tree_sha256_at_git_commit(project, newer) != expected


@pytest.mark.parametrize("source_sha", ["HEAD", "a" * 12, "g" * 40])
def test_accepted_commit_tree_digest_rejects_a_non_full_sha_before_git(
    pinned_repo: tuple[Path, str],
    monkeypatch: pytest.MonkeyPatch,
    source_sha: str,
) -> None:
    project, _pin = pinned_repo

    def fail_run(*args: object, **kwargs: object) -> None:
        raise AssertionError("git must not run for a malformed SHA")

    monkeypatch.setattr(admin.subprocess, "run", fail_run)

    with pytest.raises(admin.AdminError, match="full 40-character"):
        admin.source_tree_sha256_at_git_commit(project, source_sha)


def test_accepted_commit_tree_digest_rejects_non_commit_and_unknown_objects(
    pinned_repo: tuple[Path, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project, pin = pinned_repo
    tree = _git(project.parent, "rev-parse", f"{pin}^{{tree}}")
    calls: list[list[str]] = []
    real_run = admin.subprocess.run

    def record_run(argv: list[str], **kwargs: object):
        calls.append(list(argv))
        return real_run(argv, **kwargs)

    monkeypatch.setattr(admin.subprocess, "run", record_run)

    with pytest.raises(admin.AdminError, match="commit|archive"):
        admin.source_tree_sha256_at_git_commit(project, tree)
    with pytest.raises(admin.AdminError, match="commit|archive"):
        admin.source_tree_sha256_at_git_commit(project, "f" * 40)
    assert not any(
        command in {"fetch", "checkout", "status"}
        for argv in calls
        for command in argv
    )


def test_accepted_commit_tree_digest_ignores_local_git_replace_refs(
    pinned_repo: tuple[Path, str],
) -> None:
    project, pin = pinned_repo
    expected = admin.source_tree_sha256(project / "src" / "vq")
    (project / "src" / "vq" / "__init__.py").write_text(
        "VALUE = 'replacement'\n",
        encoding="utf-8",
    )
    _git(project.parent, "add", "-A")
    _git(
        project.parent,
        "-c",
        "commit.gpgsign=false",
        "commit",
        "--quiet",
        "-m",
        "replacement",
    )
    replacement = _git(project.parent, "rev-parse", "HEAD")
    _git(project.parent, "replace", pin, replacement)

    assert admin.source_tree_sha256_at_git_commit(project, pin) == expected


def test_pinned_staging_rejects_an_unknown_commit(
    pinned_repo: tuple[Path, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project, _pin = pinned_repo
    monkeypatch.setattr("vq.admin._scheduler_helper_project_root", lambda: project)
    host_cfg = config.HostConfig(
        ssh="cluster",
        scheduler="slurm",
        scheduler_dialect="slurm",
        scheduler_driver="localhost",
        scratch_root="/shared/jobs",
        scheduler_update_stage="/shared/vq-admin/cluster",
    )
    result = admin.SchedulerHostUpdateResult(
        host="cluster",
        ssh="cluster",
        scheduler="slurm",
        mode="update",
        command="update-helper",
    )
    with pytest.raises(
        admin.AdminError, match="does not exist|is not local"
    ):
        admin._stage_scheduler_helper_source(
            "cluster", host_cfg, host_cfg, result, expected_sha="f" * 40
        )


def test_parse_deploy_metrics_extracts_key_values() -> None:
    transcript = (
        "native cache: reused compatible installed dependencies\n"
        "VQ-DEPLOY-METRIC dependency_cache=reused\n"
        "VQ-DEPLOY-METRIC native_deps_rebuilt=no\n"
        "noise VQ-DEPLOY-METRIC not_at_line_start=1\n"
        "VQ-DEPLOY-METRIC compiler_cache_hit_rate_percent=97.3\n"
        "VQ-DEPLOY-METRIC dependency_cache_reason=toolchain fingerprint "
        "mismatch (cache aa vs current bb)\n"
        "VQ-DEPLOY-METRIC dependency_cache=cold\n"
    )
    metrics = admin.parse_deploy_metrics(transcript)
    assert metrics["native_deps_rebuilt"] == "no"
    assert metrics["compiler_cache_hit_rate_percent"] == "97.3"
    assert metrics["dependency_cache_reason"].startswith("toolchain fingerprint")
    # Later refinements win.
    assert metrics["dependency_cache"] == "cold"
    assert "not_at_line_start" not in metrics


@pytest.fixture
def scheduler_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    (tmp_path / "cfg" / "config.toml").write_text(
        "\n".join(
            [
                "[hosts.localhost]",
                'ssh = "localhost"',
                "",
                "[hosts.host_f]",
                'ssh = "host_f-login"',
                'fleet_role = "managed"',
                'scheduler = "pbs"',
                'scheduler_dialect = "torque"',
                'scratch_root = "/home/USER"',
                'scheduler_driver = "localhost"',
                'scheduler_update_command = "/site/update-helper"',
                "",
            ]
        )
    )
    return tmp_path


def _helper_result(**overrides: object) -> admin.SchedulerHostUpdateResult:
    result = admin.SchedulerHostUpdateResult(
        host="host_f",
        ssh="host_f-login",
        scheduler="pbs",
        mode="update",
        command="update-helper",
    )
    result.command_rc = 0
    result.stage_source = "report-pin"
    result.expected_source_sha = "4" * 40
    result.expected_source_tree_sha256 = "5" * 64
    result.remote_source_sha = "4" * 40
    result.remote_source_tree_sha256 = "5" * 64
    result.metrics = {"dependency_cache": "not-applicable"}
    for key, value in overrides.items():
        setattr(result, key, value)
    return result


def test_helper_outcome_is_recorded_canonically(scheduler_state: Path) -> None:
    cfg = config.load_config()
    admin.record_scheduler_helper_outcome(_helper_result())

    records = admin.load_scheduler_runtime_status()
    record = records["host_f:vq-helper"]
    assert record.last_success is True
    assert record.actual_sha == "4" * 40
    assert record.last_ok_sha == "4" * 40
    assert record.metrics == {"dependency_cache": "not-applicable"}
    assert "report-pin" in (record.health_detail or "")

    payload = json.loads(admin.format_scheduler_runtime_status_json("host_f", cfg))
    assert payload["helper"]["configured"] is True
    assert payload["helper"]["last"]["actual_sha"] == "4" * 40
    assert payload["helper"]["last"]["last_success"] is True


def test_failed_helper_outcome_keeps_the_rollback_identity(
    scheduler_state: Path,
) -> None:
    admin.record_scheduler_helper_outcome(_helper_result())
    failed = _helper_result(
        remote_source_sha=None,
        work_errors=["helper source-tree digest mismatch"],
    )
    admin.record_scheduler_helper_outcome(failed)

    record = admin.load_scheduler_runtime_status()["host_f:vq-helper"]
    assert record.last_success is False
    assert record.last_ok_sha == "4" * 40
    assert "mismatch" in (record.health_detail or "")


def test_helper_outcome_without_identity_is_not_recorded(
    scheduler_state: Path,
) -> None:
    result = _helper_result(
        expected_source_sha=None,
        work_errors=["refused: active jobs"],
    )
    admin.record_scheduler_helper_outcome(result)
    assert "host_f:vq-helper" not in admin.load_scheduler_runtime_status()


def test_cli_forwards_expected_sha_to_the_helper_update(
    scheduler_state: Path,
) -> None:
    captured: dict[str, object] = {}

    def fake_update(host, cfg, **kwargs):  # type: ignore[no-untyped-def]
        captured["host"] = host
        captured.update(kwargs)
        result = _helper_result()
        return result

    with patch("vq.cli.admin_module.update_scheduler_host", side_effect=fake_update):
        outcome = CliRunner().invoke(
            main,
            ["admin", "update", "host_f", "--expected-sha", "4" * 40],
        )
    assert outcome.exit_code == 0, outcome.output
    assert captured["host"] == "host_f"
    assert captured["expected_sha"] == "4" * 40
