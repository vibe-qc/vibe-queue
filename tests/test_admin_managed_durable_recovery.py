"""Managed self-update rollback, commit, and crash-boundary coverage."""

from __future__ import annotations

import io
import json
import os
import plistlib
import shutil
import stat
import subprocess
import sys
import tarfile
import time
import uuid
import venv as stdlib_venv
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from click.testing import CliRunner

from vq import admin, cli, config, paths
from vq.pause_resume import PauseError

pytestmark = [
    pytest.mark.no_autopatch_branch_check,
    pytest.mark.no_autopatch_lifecycle_lock,
]

OLD_TREE = "12" * 32
NEW_TREE = "34" * 32
BAD_TREE = "56" * 32


class _SimulatedProcessDeath(BaseException):
    """Model SIGKILL at a filesystem boundary without running cleanup."""


def _git(cwd: Path, *args: str) -> str:
    environment = {
        **os.environ,
        "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_CONFIG_SYSTEM": "/dev/null",
        "GIT_AUTHOR_NAME": "vq-test",
        "GIT_AUTHOR_EMAIL": "vq-test@example.invalid",
        "GIT_COMMITTER_NAME": "vq-test",
        "GIT_COMMITTER_EMAIL": "vq-test@example.invalid",
    }
    completed = subprocess.run(
        ["git", "-C", str(cwd), *args],
        capture_output=True,
        text=True,
        check=False,
        env=environment,
    )
    assert completed.returncode == 0, completed.stderr
    return completed.stdout.strip()


def _write_venv(path: Path, identity: str) -> None:
    (path / "bin").mkdir(parents=True)
    (path / "bin" / "python").write_text(
        f"python placeholder: {identity}\n",
        encoding="utf-8",
    )
    (path / "bin" / "vq").write_text(
        f"vq placeholder: {identity}\n",
        encoding="utf-8",
    )
    (path / "pyvenv.cfg").write_text("home = /usr/bin\n", encoding="utf-8")
    (path / "identity.txt").write_text(identity + "\n", encoding="utf-8")


def _installed_digest(python: str) -> str | None:
    identity = Path(python).parent.parent / "identity.txt"
    try:
        value = identity.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    return {"old": OLD_TREE, "new": NEW_TREE, "bad": BAD_TREE}.get(value)


def _fixture(tmp_path: Path) -> tuple[
    config.VenvProgram,
    str,
    str,
    Path,
    Path,
]:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "--initial-branch", "main")
    (repo / "payload.txt").write_text("old\n", encoding="utf-8")
    _git(repo, "add", ".")
    _git(repo, "commit", "-m", "old")
    old_sha = _git(repo, "rev-parse", "HEAD")
    (repo / "payload.txt").write_text("new\n", encoding="utf-8")
    _git(repo, "add", ".")
    _git(repo, "commit", "-m", "new")
    new_sha = _git(repo, "rev-parse", "HEAD")
    _git(repo, "tag", "new-target", new_sha)
    _git(repo, "checkout", "--detach", new_sha)
    _git(repo, "branch", "-f", "main", old_sha)
    _git(repo, "checkout", "main")
    origin = tmp_path / "origin.git"
    origin.mkdir()
    _git(origin, "init", "--bare")
    _git(repo, "remote", "add", "origin", str(origin))
    _git(repo, "push", "--set-upstream", "origin", "main")
    _git(repo, "push", "origin", "new-target")

    venv = tmp_path / "managed-venv"
    backup = tmp_path / (
        ".managed-venv.vq-admin-backup-"
        "0123456789abcdef0123456789abcdef"
    )
    _write_venv(backup, "old")
    _write_venv(venv, "new")
    prog = config.VenvProgram(
        kind="venv",
        python=str(venv / "bin" / "python"),
        git_dir=str(repo),
        branch="main",
        update_script="vibe-queue/scripts/update.sh",
    )
    return prog, old_sha, new_sha, venv, backup


def _systemd_execstart(
    executable: Path,
    *,
    arguments: str = "daemon run",
    ignore_errors: str = "no",
    start_time: str = "[n/a]",
    stop_time: str = "[n/a]",
    pid: int = 0,
    code: str = "(null)",
    status: str = "0/0",
) -> str:
    """Return the real field shape printed by ``systemctl show``."""
    return (
        f"{{ path={executable} ; argv[]={executable} {arguments} ; "
        f"ignore_errors={ignore_errors} ; start_time={start_time} ; "
        f"stop_time={stop_time} ; pid={pid} ; code={code} ; "
        f"status={status} }}"
    )


@pytest.mark.parametrize(
    "malformed",
    [
        pytest.param("missing-ignore-errors", id="missing-definition-field"),
        pytest.param("duplicate-path", id="duplicate-definition-field"),
        pytest.param("multiline", id="multiline"),
        pytest.param("separator-newline", id="separator-newline"),
        pytest.param("multiple-structs", id="multiple-structs"),
    ],
)
def test_systemd_identity_normalizer_rejects_ambiguous_shapes(
    tmp_path: Path,
    malformed: str,
) -> None:
    executable = tmp_path / "venv" / "bin" / "vq"
    raw = _systemd_execstart(executable)
    if malformed == "missing-ignore-errors":
        raw = raw.replace("ignore_errors=no ; ", "")
    elif malformed == "duplicate-path":
        raw = raw.replace(
            "ignore_errors=no ; ",
            f"ignore_errors=no ; path={executable} ; ",
        )
    elif malformed == "multiline":
        raw = raw.replace(" daemon run ;", " daemon\nrun ;")
    elif malformed == "separator-newline":
        raw = raw.replace(" ; argv[]=", " ;\nargv[]=", 1)
    else:
        raw = f"{raw} {raw}"

    identity = ("systemd-execstart", raw)
    assert admin._normalize_systemd_command_identity(identity) is None
    assert not admin._service_command_identities_match(
        admin._DaemonServiceManager.SYSTEMD,
        identity,
        identity,
    )


def test_service_identity_matcher_keeps_definition_fields_exact(
    tmp_path: Path,
) -> None:
    executable = tmp_path / "venv" / "bin" / "vq"
    recorded = ("systemd-execstart", _systemd_execstart(executable))
    path_rewrite = (
        "systemd-execstart",
        _systemd_execstart(tmp_path / "other-venv" / "bin" / "vq"),
    )
    ignore_rewrite = (
        "systemd-execstart",
        _systemd_execstart(executable, ignore_errors="yes"),
    )
    trailing_argv_space = (
        "systemd-execstart",
        _systemd_execstart(executable).replace(" daemon run ;", " daemon run  ;"),
    )

    assert not admin._service_command_identities_match(
        admin._DaemonServiceManager.SYSTEMD,
        path_rewrite,
        recorded,
    )
    assert not admin._service_command_identities_match(
        admin._DaemonServiceManager.SYSTEMD,
        ignore_rewrite,
        recorded,
    )
    assert not admin._service_command_identities_match(
        admin._DaemonServiceManager.SYSTEMD,
        trailing_argv_space,
        recorded,
    )
    assert not admin._service_command_identities_match(
        admin._DaemonServiceManager.LAUNCHD,
        ("python", "-m", "vq", "daemon", "run", "--quiet"),
        ("python", "-m", "vq", "daemon", "run"),
    )
    assert not admin._service_command_identities_match(
        admin._DaemonServiceManager.SYSTEMD,
        None,
        None,
    )
    assert admin._service_command_identities_match(
        admin._DaemonServiceManager.LAUNCHD,
        None,
        None,
    )


@pytest.mark.parametrize(
    ("arguments", "expected"),
    [
        pytest.param("daemon run", True, id="daemon-run"),
        pytest.param(
            "daemon run --max-cpus 2 --max-jobs 2",
            True,
            id="daemon-run-options",
        ),
        pytest.param("daemon runner", False, id="subcommand-boundary"),
        pytest.param("admin daemon run", False, id="embedded-daemon-run"),
        pytest.param("queue run", False, id="different-command"),
    ],
)
def test_systemd_daemon_identity_requires_canonical_command_boundary(
    tmp_path: Path,
    arguments: str,
    expected: bool,
) -> None:
    executable = tmp_path / "venv" / "bin" / "vq"
    identity = (
        "systemd-execstart",
        _systemd_execstart(executable, arguments=arguments),
    )

    assert admin._systemd_command_identity_is_vq_daemon(identity) is expected


def _lifecycle(
    prog: config.VenvProgram,
    old_sha: str,
    venv: Path,
    backup: Path,
) -> admin._ManagedDaemonUpdate:
    return admin._ManagedDaemonUpdate(
        manager=admin._DaemonServiceManager.SYSTEMD,
        env="vibeqc-queue",
        pre_pid=41,
        was_running=True,
        was_stopped=True,
        pre_source_sha=old_sha,
        pre_source_tree_sha256=OLD_TREE,
        pre_checkout_branch="main",
        venv_path=venv,
        venv_backup=backup,
        service_executable=str(venv / "bin" / "vq"),
        service_command=(
            "systemd-execstart",
            _systemd_execstart(venv / "bin" / "vq"),
        ),
        transaction_id="0123456789abcdef0123456789abcdef",
        backup_moved=True,
        receipt_phase="backup_moved",
    )


def _target_result(
    prog: config.VenvProgram,
    new_sha: str,
) -> admin.UpdateResult:
    return admin.UpdateResult(
        env="vibeqc-queue",
        git_dir=prog.git_dir,
        branch="main",
        update_script=None,
        git_pull_rc=0,
        expected_sha=new_sha,
        actual_sha=new_sha,
        sha_check_rc=0,
    )


def _patch_common_completion(
    monkeypatch: pytest.MonkeyPatch,
    prog: config.VenvProgram,
    old_sha: str,
    new_sha: str,
) -> None:
    monkeypatch.setattr(admin, "transition_admin_update_state", lambda *args: None)
    monkeypatch.setattr(
        admin,
        "_vq_project_root_for_program",
        lambda unused: Path(prog.git_dir),
    )
    monkeypatch.setattr(
        admin,
        "source_tree_sha256_at_git_commit",
        lambda project, sha: NEW_TREE if sha == new_sha else OLD_TREE,
    )
    monkeypatch.setattr(admin, "_installed_tree_digest", _installed_digest)
    monkeypatch.setattr(
        admin,
        "_stop_managed_daemon_for_restore",
        lambda *args: (True, "service is quiescent"),
    )
    monkeypatch.setattr(
        admin,
        "_reattest_service_before_start",
        lambda lifecycle: (True, "exact service command retained"),
    )
    monkeypatch.setattr(
        admin,
        "_query_daemon_service_state",
        lambda manager: admin._DaemonServiceState(
            manager=manager,
            running=False,
            pid=None,
            executable=str(Path(prog.python).parent / "vq"),
            diagnostic="test exact service state",
            command_identity=(
                "systemd-execstart",
                _systemd_execstart(Path(prog.python).parent / "vq"),
            ),
        ),
    )


def _arm_receipt(
    prog: config.VenvProgram,
    lifecycle: admin._ManagedDaemonUpdate,
    *,
    multi_user: bool = False,
) -> Path:
    """Create the real owned marker and seed its durable transaction."""
    admin._set_owned_admin_update_marker_path(None)
    admin.acquire_admin_update_marker(
        envs=[lifecycle.env],
        host="localhost",
    )
    admin._record_admin_update_pause_scope(
        pause_token="admin-update-0123456789ab",
        paused_jobids=[],
        surgical=False,
        multi_user=multi_user,
    )
    admin._persist_managed_update_receipt(prog, lifecycle)
    return admin.admin_update_marker_path()


def _managed_receipt(marker_path: Path) -> dict[str, Any] | None:
    payload = json.loads(marker_path.read_text(encoding="utf-8"))
    receipt = payload.get("managed_transaction")
    return receipt if isinstance(receipt, dict) else None


def _mark_receipt_stale(monkeypatch: pytest.MonkeyPatch) -> admin.AdminUpdateMarker:
    marker = admin.read_admin_update_marker()
    assert marker is not None
    marker.pid = 999_999
    marker.pid_start_time = 0
    admin._write_admin_update_marker_atomic(marker)
    admin._set_owned_admin_update_marker_path(None)
    monkeypatch.setattr(admin, "_pid_liveness", lambda unused: False)
    return marker


def _patch_clear_resume(monkeypatch: pytest.MonkeyPatch) -> None:
    class ClearResumeProof:
        summary = "durable token scope is clear"

        @staticmethod
        def require_clear() -> None:
            return None

    monkeypatch.setattr(
        admin,
        "resume_token_scope_with_proof",
        lambda *args, **kwargs: ClearResumeProof(),
    )


def _assert_old_state(
    prog: config.VenvProgram,
    old_sha: str,
    venv: Path,
    lifecycle: admin._ManagedDaemonUpdate,
) -> None:
    assert (venv / "identity.txt").read_text(encoding="utf-8") == "old\n"
    assert _git(Path(prog.git_dir), "rev-parse", "HEAD") == old_sha
    assert _git(Path(prog.git_dir), "rev-parse", "--abbrev-ref", "HEAD") == "main"
    assert _git(Path(prog.git_dir), "status", "--porcelain") == ""
    assert lifecycle.backup_moved is False
    assert lifecycle.venv_backup is not None
    assert not lifecycle.venv_backup.exists()
    assert lifecycle.terminal_verified is True


def test_verified_success_commits_and_removes_real_backup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prog, old_sha, new_sha, venv, backup = _fixture(tmp_path)
    lifecycle = _lifecycle(prog, old_sha, venv, backup)
    marker_path = _arm_receipt(prog, lifecycle)
    _git(Path(prog.git_dir), "checkout", "--detach", new_sha)
    result = _target_result(prog, new_sha)
    _patch_common_completion(monkeypatch, prog, old_sha, new_sha)
    monkeypatch.setattr(
        admin,
        "_start_managed_daemon_update",
        lambda unused: (True, "target service started"),
    )
    monkeypatch.setattr(
        admin,
        "_verify_restarted_daemon",
        lambda *args, **kwargs: admin.DaemonProvenance(
            verified=True,
            actual_sha=new_sha,
            actual_tree_sha256=NEW_TREE,
            detail="target RPC identity verified",
        ),
    )

    admin._complete_managed_daemon_update(prog, result, lifecycle)

    assert result.success is True
    assert lifecycle.terminal_verified is True
    assert lifecycle.backup_moved is False
    assert not backup.exists()
    assert (venv / "identity.txt").read_text(encoding="utf-8") == "new\n"
    assert not list(tmp_path.glob(".managed-venv.vq-admin-committed-*"))
    receipt = _managed_receipt(marker_path)
    assert receipt is not None
    assert receipt["phase"] == "target_committed"


@pytest.mark.parametrize(
    ("spec_count", "ready_at", "identity", "override", "succeeds"),
    [
        pytest.param(0, 45.0, "exact", None, True, id="idle-startup"),
        pytest.param(21_683, 300.0, "exact", None, True, id="loaded-startup"),
        pytest.param(21_683, 550.0, "exact", None, True, id="loaded-headroom"),
        pytest.param(21_683, 650.0, "exact", None, False, id="bounded-timeout"),
        pytest.param(21_683, 650.0, "exact", "900", True, id="operator-override"),
        pytest.param(21_683, 300.0, "wrong-sha", None, False, id="wrong-sha"),
        pytest.param(21_683, 300.0, "wrong-tree", None, False, id="wrong-tree"),
    ],
)
def test_managed_completion_waits_for_loaded_queue_without_weakening_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    spec_count: int,
    ready_at: float,
    identity: str,
    override: str | None,
    succeeds: bool,
) -> None:
    """#53: delayed exact readiness commits; timeout/stale identity restores.

    Model the directory entry count and elapsed startup time, not a claim
    about this machine's filesystem speed. Service operations are stubbed;
    the provenance poll, receipt transitions and file rollback are real.
    """
    from vq import rpc

    prog, old_sha, new_sha, venv, backup = _fixture(tmp_path)
    lifecycle = _lifecycle(prog, old_sha, venv, backup)
    marker_path = _arm_receipt(prog, lifecycle)
    _git(Path(prog.git_dir), "checkout", "--detach", new_sha)
    result = _target_result(prog, new_sha)
    _patch_common_completion(monkeypatch, prog, old_sha, new_sha)
    if override is None:
        monkeypatch.delenv("VQ_DAEMON_HEALTH_TIMEOUT", raising=False)
    else:
        monkeypatch.setenv("VQ_DAEMON_HEALTH_TIMEOUT", override)
    queue = paths.queue_dir()
    original_glob = Path.glob
    monkeypatch.setattr(
        Path, "glob",
        lambda path, pattern, **kwargs: iter(range(spec_count))
        if path == queue and pattern == "*.json" else original_glob(path, pattern, **kwargs),
    )
    elapsed = [0.0]
    # Replace only admin's clock; do not alter subprocess/test harness timing.
    monkeypatch.setattr(admin, "time", SimpleNamespace(
        **{name: getattr(time, name) for name in dir(time) if not name.startswith("_")},
    ))
    monkeypatch.setattr(admin.time, "monotonic", lambda: elapsed[0])
    monkeypatch.setattr(admin.time, "sleep", lambda s: elapsed.__setitem__(0, elapsed[0] + s))
    starts: list[str] = []

    def start(unused: admin._ManagedDaemonUpdate) -> tuple[bool, str]:
        starts.append((venv / "identity.txt").read_text().strip())
        return True, "service started"

    def ping() -> dict[str, str] | None:
        if starts[-1] == "old":
            return {"source_sha": old_sha, "source_tree_sha256": OLD_TREE}
        if elapsed[0] < ready_at:
            return None
        return {
            "source_sha": old_sha if identity == "wrong-sha" else new_sha,
            "source_tree_sha256": BAD_TREE if identity == "wrong-tree" else NEW_TREE,
        }

    monkeypatch.setattr(admin, "_start_managed_daemon_update", start)
    monkeypatch.setattr(rpc, "ping_user_daemon", ping)

    admin._complete_managed_daemon_update(prog, result, lifecycle)

    assert result.success is succeeds, result.daemon_restart_message
    receipt = _managed_receipt(marker_path)
    assert receipt is not None
    if succeeds:
        assert starts == ["new"]
        assert elapsed[0] == pytest.approx(ready_at, abs=0.11)
        assert receipt["phase"] == "target_committed"
        assert not backup.exists()
        assert (venv / "identity.txt").read_text() == "new\n"
        assert _git(Path(prog.git_dir), "rev-parse", "HEAD") == new_sha
    else:
        assert starts == ["new", "old"]
        assert elapsed[0] == pytest.approx(600.0, abs=0.11)
        assert receipt["phase"] == "old_restored"
        _assert_old_state(prog, old_sha, venv, lifecycle)
        expected = "did not respond" if ready_at > 600 else "strict identity mismatch"
        assert expected in result.daemon_restart_message


def test_target_digest_failure_restores_real_backup_and_attached_checkout(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prog, old_sha, new_sha, venv, backup = _fixture(tmp_path)
    lifecycle = _lifecycle(prog, old_sha, venv, backup)
    marker_path = _arm_receipt(prog, lifecycle)
    (venv / "identity.txt").write_text("bad\n", encoding="utf-8")
    _git(Path(prog.git_dir), "checkout", "--detach", new_sha)
    (Path(prog.git_dir) / "generated.tmp").write_text("remove me\n", encoding="utf-8")
    result = _target_result(prog, new_sha)
    _patch_common_completion(monkeypatch, prog, old_sha, new_sha)
    starts: list[str] = []

    def start(unused: admin._ManagedDaemonUpdate) -> tuple[bool, str]:
        starts.append((venv / "identity.txt").read_text(encoding="utf-8").strip())
        return True, "old service started"

    monkeypatch.setattr(admin, "_start_managed_daemon_update", start)
    monkeypatch.setattr(
        admin,
        "_verify_restarted_daemon",
        lambda *args, **kwargs: admin.DaemonProvenance(
            verified=True,
            actual_sha=old_sha,
            actual_tree_sha256=OLD_TREE,
            detail="old RPC identity verified",
        ),
    )

    admin._complete_managed_daemon_update(prog, result, lifecycle)

    assert result.success is False
    assert starts == ["old"]
    assert any("installed vq tree" in error for error in result.work_errors)
    _assert_old_state(prog, old_sha, venv, lifecycle)
    receipt = _managed_receipt(marker_path)
    assert receipt is not None
    assert receipt["phase"] == "old_restored"


def test_target_start_failure_restores_old_venv_checkout_and_daemon(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prog, old_sha, new_sha, venv, backup = _fixture(tmp_path)
    lifecycle = _lifecycle(prog, old_sha, venv, backup)
    marker_path = _arm_receipt(prog, lifecycle)
    _git(Path(prog.git_dir), "checkout", "--detach", new_sha)
    result = _target_result(prog, new_sha)
    _patch_common_completion(monkeypatch, prog, old_sha, new_sha)
    starts: list[str] = []

    def start(unused: admin._ManagedDaemonUpdate) -> tuple[bool, str]:
        identity = (venv / "identity.txt").read_text(encoding="utf-8").strip()
        starts.append(identity)
        if identity == "new":
            return False, "target start refused"
        return True, "old service started"

    monkeypatch.setattr(admin, "_start_managed_daemon_update", start)
    monkeypatch.setattr(
        admin,
        "_verify_restarted_daemon",
        lambda *args, **kwargs: admin.DaemonProvenance(
            verified=True,
            actual_sha=old_sha,
            actual_tree_sha256=OLD_TREE,
            detail="old RPC identity verified",
        ),
    )

    admin._complete_managed_daemon_update(prog, result, lifecycle)

    assert result.success is False
    assert starts == ["new", "old"], result.daemon_restart_message
    _assert_old_state(prog, old_sha, venv, lifecycle)
    receipt = _managed_receipt(marker_path)
    assert receipt is not None
    assert receipt["phase"] == "old_restored"


def test_target_rpc_failure_restores_old_venv_checkout_and_rpc_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prog, old_sha, new_sha, venv, backup = _fixture(tmp_path)
    lifecycle = _lifecycle(prog, old_sha, venv, backup)
    marker_path = _arm_receipt(prog, lifecycle)
    _git(Path(prog.git_dir), "checkout", "--detach", new_sha)
    result = _target_result(prog, new_sha)
    _patch_common_completion(monkeypatch, prog, old_sha, new_sha)
    monkeypatch.setattr(
        admin,
        "_start_managed_daemon_update",
        lambda unused: (True, "service started"),
    )
    verified: list[str] = []

    def verify(expected_sha: str, **kwargs: object) -> admin.DaemonProvenance:
        verified.append(expected_sha)
        if expected_sha == new_sha:
            return admin.DaemonProvenance(
                verified=False,
                actual_sha=old_sha,
                actual_tree_sha256=OLD_TREE,
                detail="target RPC still reports old bytes",
            )
        return admin.DaemonProvenance(
            verified=True,
            actual_sha=old_sha,
            actual_tree_sha256=OLD_TREE,
            detail="old RPC identity verified",
        )

    monkeypatch.setattr(admin, "_verify_restarted_daemon", verify)

    admin._complete_managed_daemon_update(prog, result, lifecycle)

    assert result.success is False
    assert verified == [new_sha, old_sha]
    _assert_old_state(prog, old_sha, venv, lifecycle)
    receipt = _managed_receipt(marker_path)
    assert receipt is not None
    assert receipt["phase"] == "old_restored"


def test_service_command_drift_after_target_rpc_restores_old_transaction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prog, old_sha, new_sha, venv, backup = _fixture(tmp_path)
    lifecycle = _lifecycle(prog, old_sha, venv, backup)
    marker_path = _arm_receipt(prog, lifecycle)
    _git(Path(prog.git_dir), "checkout", "--detach", new_sha)
    result = _target_result(prog, new_sha)
    _patch_common_completion(monkeypatch, prog, old_sha, new_sha)
    monkeypatch.setattr(
        admin,
        "_start_managed_daemon_update",
        lambda unused: (True, "service started"),
    )
    monkeypatch.setattr(
        admin,
        "_verify_restarted_daemon",
        lambda expected_sha, **kwargs: admin.DaemonProvenance(
            verified=True,
            actual_sha=expected_sha,
            actual_tree_sha256=NEW_TREE if expected_sha == new_sha else OLD_TREE,
            detail="RPC identity verified",
        ),
    )
    attestations = iter(
        [
            (False, "systemd service command changed during managed update"),
            (True, "old service command restored"),
        ]
    )
    monkeypatch.setattr(
        admin,
        "_reattest_service_before_start",
        lambda unused: next(attestations),
    )

    admin._complete_managed_daemon_update(prog, result, lifecycle)

    assert result.success is False
    assert "service definition changed" in result.daemon_restart_message
    _assert_old_state(prog, old_sha, venv, lifecycle)
    receipt = _managed_receipt(marker_path)
    assert receipt is not None
    assert receipt["phase"] == "old_restored"


def _begin_patches(
    monkeypatch: pytest.MonkeyPatch,
    prog: config.VenvProgram,
    old_sha: str,
) -> None:
    state = admin._DaemonServiceState(
        manager=admin._DaemonServiceManager.SYSTEMD,
        running=False,
        pid=None,
        executable=str(Path(prog.python).parent / "vq"),
        diagnostic="exact inactive service",
        command_identity=(
            "systemd-execstart",
            _systemd_execstart(Path(prog.python).parent / "vq"),
        ),
    )
    monkeypatch.setattr(admin, "_query_daemon_service_state", lambda manager: state)
    monkeypatch.setattr(
        admin,
        "_run_daemon_service_command",
        lambda *args, **kwargs: (True, "service stopped"),
    )
    monkeypatch.setattr(
        admin,
        "_wait_for_managed_daemon_quiescence",
        lambda *args: (True, "service is inactive"),
    )
    monkeypatch.setattr(admin, "_installed_tree_digest", _installed_digest)
    monkeypatch.setattr(
        admin,
        "source_tree_sha256_at_git_commit",
        lambda project, sha: OLD_TREE,
    )
    monkeypatch.setattr(
        admin,
        "_vq_project_root_for_program",
        lambda unused: Path(prog.git_dir),
    )
    monkeypatch.setattr(
        admin,
        "_start_managed_daemon_update",
        lambda unused: (True, "old service started"),
    )
    monkeypatch.setattr(
        admin,
        "_verify_restarted_daemon",
        lambda *args, **kwargs: admin.DaemonProvenance(
            verified=True,
            actual_sha=old_sha,
            actual_tree_sha256=OLD_TREE,
            detail="old RPC identity verified",
        ),
    )
    monkeypatch.setattr(
        admin,
        "_reattest_service_before_start",
        lambda unused: (True, "old service command retained"),
    )


def _probe() -> admin._SelfUpdateProbe:
    return admin._SelfUpdateProbe(
        is_self_update=True,
        daemon_running=False,
        service_manager="systemd",
        manager_available=True,
        diagnostic="exact test service",
    )


def _receipt_payloads(*roots: Path) -> list[tuple[Path, dict[str, Any]]]:
    payloads: list[tuple[Path, dict[str, Any]]] = []
    for root in roots:
        if not root.exists():
            continue
        for candidate in root.rglob("*"):
            if not candidate.is_file():
                continue
            try:
                info = candidate.lstat()
                payload = json.loads(candidate.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                continue
            transaction = (
                payload.get("managed_transaction")
                if isinstance(payload, dict)
                else None
            )
            if (
                stat.S_ISREG(info.st_mode)
                and info.st_nlink == 1
                and isinstance(transaction, dict)
            ):
                payloads.append((candidate, transaction))
    return payloads


def test_begin_persists_recovery_receipt_before_atomic_venv_rename(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A SIGKILL after rename must leave enough durable recovery authority."""
    prog, old_sha, _new_sha, venv, stale_backup = _fixture(tmp_path)
    # Begin operates on the old serving venv, not the synthetic completion pair.
    stale_backup.rename(tmp_path / "discarded-old")
    (venv / "identity.txt").write_text("old\n", encoding="utf-8")
    monkeypatch.setenv("VQ_STATE_DIR", str(tmp_path / "state"))
    _begin_patches(monkeypatch, prog, old_sha)
    admin._set_owned_admin_update_marker_path(None)
    admin.acquire_admin_update_marker(
        envs=["vibeqc-queue"],
        host="localhost",
    )
    real_replace = admin.os.replace
    observed: list[tuple[Path, list[tuple[Path, dict[str, Any]]]]] = []

    def replace(source: str | os.PathLike[str], target: str | os.PathLike[str]) -> None:
        source_path = Path(source)
        target_path = Path(target)
        if source_path == venv:
            observed.append(
                (
                    target_path,
                    _receipt_payloads(venv.parent, paths.state_root()),
                )
            )
        real_replace(source, target)

    monkeypatch.setattr(admin.os, "replace", replace)

    lifecycle = admin._begin_managed_daemon_update(
        prog,
        _probe(),
        env="vibeqc-queue",
    )

    assert lifecycle.venv_backup is not None
    assert observed, "the atomic serving-venv rename was not observed"
    planned_backup, receipts = observed[0]
    assert receipts, (
        "no durable managed-update recovery receipt existed before the old "
        "serving venv became reachable only by its randomized backup path"
    )
    receipt_text = json.dumps(receipts[0][1], sort_keys=True)
    for required in (
        old_sha,
        OLD_TREE,
        str(venv),
        str(planned_backup),
        "systemd",
    ):
        assert required in receipt_text


def test_stale_crash_receipt_restores_old_checkout_venv_and_daemon(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A fresh process recovers exact old state after the unsafe boundary."""
    prog, old_sha, _new_sha, venv, stale_backup = _fixture(tmp_path)
    stale_backup.rename(tmp_path / "discarded-old")
    (venv / "identity.txt").write_text("old\n", encoding="utf-8")
    monkeypatch.setenv("VQ_STATE_DIR", str(tmp_path / "state"))
    _begin_patches(monkeypatch, prog, old_sha)
    admin._set_owned_admin_update_marker_path(None)
    marker = admin.acquire_admin_update_marker(
        envs=["vibeqc-queue"],
        host="localhost",
    )
    admin._record_admin_update_pause_scope(
        pause_token="admin-update-fedcba987654",
        paused_jobids=[],
        surgical=False,
        multi_user=False,
    )

    first = admin._begin_managed_daemon_update(
        prog,
        _probe(),
        env="vibeqc-queue",
    )
    assert first.venv_backup is not None and first.venv_backup.exists()
    first_backup = first.venv_backup
    _git(Path(prog.git_dir), "checkout", "--detach", "new-target")
    _write_venv(venv, "new")

    cfg = config.Config(programs={"vibeqc-queue": prog})
    with pytest.raises(admin.AdminUpdateInProgress, match="still appears live"):
        admin.recover_managed_update(cfg, marker_id=marker.marker_id)

    # Model the next process observing that the receipt writer died. The
    # durable transaction identity remains intact, but recovery is a distinct
    # PID and must securely adopt the exact stale lease under both locks.
    stale = admin.read_admin_update_marker()
    assert stale is not None
    stale.pid = 999_999
    stale.pid_start_time = 0
    admin._write_admin_update_marker_atomic(stale)
    admin._set_owned_admin_update_marker_path(None)
    monkeypatch.setattr(admin, "_pid_liveness", lambda unused: False)
    recovered = admin.recover_managed_update(cfg, marker_id=marker.marker_id)

    assert recovered.recovered is True
    assert recovered.env == "vibeqc-queue"
    assert "previous virtualenv restored" in recovered.detail
    assert (venv / "identity.txt").read_text(encoding="utf-8") == "old\n"
    assert _git(Path(prog.git_dir), "rev-parse", "HEAD") == old_sha
    assert _git(Path(prog.git_dir), "rev-parse", "--abbrev-ref", "HEAD") == "main"
    assert not first_backup.exists()
    assert not admin.admin_update_marker_exists()


def test_systemd_wrapper_receipt_recovers_after_restart_runtime_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A restart must not make a definition-identical v1 receipt foreign."""
    prog, old_sha, _new_sha, venv, backup = _fixture(tmp_path)
    (venv / "identity.txt").write_text("old\n", encoding="utf-8")
    real_vq = venv / "libexec" / "vq-real"
    real_vq.parent.mkdir()
    (venv / "bin" / "vq").replace(real_vq)
    (venv / "bin" / "vq").symlink_to(real_vq)
    wrapper = tmp_path / "stable-bin" / "vq"
    wrapper.parent.mkdir()
    wrapper.symlink_to(venv / "bin" / "vq")
    receipt_command = _systemd_execstart(
        wrapper,
        arguments="daemon run --max-cpus 2 --max-jobs 2",
        start_time="[Sat 2026-08-15 07:45:22 UTC]",
        pid=3_612_344,
    )
    live_command = _systemd_execstart(
        wrapper,
        arguments="daemon run --max-cpus 2 --max-jobs 2",
        start_time="[Mon 2026-08-17 16:54:09 UTC]",
        pid=1_819,
    )
    lifecycle = _lifecycle(prog, old_sha, venv, backup)
    lifecycle.service_executable = str(wrapper)
    lifecycle.service_command = ("systemd-execstart", receipt_command)
    lifecycle.receipt_phase = "old_restored"
    lifecycle.backup_moved = False
    admin.shutil.rmtree(backup)
    _arm_receipt(prog, lifecycle)
    state = admin._DaemonServiceState(
        manager=admin._DaemonServiceManager.SYSTEMD,
        running=True,
        pid=1_819,
        executable=str(wrapper),
        diagnostic="systemd stable wrapper",
        command_identity=("systemd-execstart", live_command),
    )
    monkeypatch.setattr(admin, "_query_daemon_service_state", lambda unused: state)
    monkeypatch.setattr(
        admin,
        "_verify_restarted_daemon",
        lambda *args, **kwargs: admin.DaemonProvenance(
            verified=True,
            actual_sha=old_sha,
            actual_tree_sha256=OLD_TREE,
            detail="old RPC identity verified",
        ),
    )
    _patch_clear_resume(monkeypatch)
    marker = _mark_receipt_stale(monkeypatch)

    recovered = admin.recover_managed_update(
        config.Config(programs={"vibeqc-queue": prog}),
        marker_id=marker.marker_id,
    )

    assert recovered.recovered is True
    assert "stable wrapper" in recovered.detail
    assert not admin.admin_update_marker_exists()


def test_systemd_never_started_receipt_matches_running_definition(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The pid=0/[n/a] form binds the same definition after first start."""
    prog, old_sha, _new_sha, venv, backup = _fixture(tmp_path)
    executable = venv / "bin" / "vq"
    lifecycle = _lifecycle(prog, old_sha, venv, backup)
    lifecycle.service_command = (
        "systemd-execstart",
        _systemd_execstart(executable),
    )
    _arm_receipt(prog, lifecycle)
    live_command = _systemd_execstart(
        executable,
        start_time="[Mon 2026-08-17 16:54:09 UTC]",
        pid=1_819,
    )
    monkeypatch.setattr(
        admin,
        "_query_daemon_service_state",
        lambda unused: admin._DaemonServiceState(
            manager=admin._DaemonServiceManager.SYSTEMD,
            running=True,
            pid=1_819,
            executable=str(executable),
            diagnostic="first systemd start",
            command_identity=("systemd-execstart", live_command),
        ),
    )
    marker = admin.read_admin_update_marker()
    assert marker is not None

    _prog, parsed = admin._parse_managed_update_receipt(
        marker,
        config.Config(programs={"vibeqc-queue": prog}),
    )

    assert parsed.service_command == lifecycle.service_command


@pytest.mark.parametrize(
    "arguments",
    [
        pytest.param("daemon runner", id="subcommand-boundary"),
        pytest.param("admin daemon run", id="embedded-daemon-run"),
    ],
)
def test_systemd_receipt_rejects_non_daemon_command(
    tmp_path: Path,
    arguments: str,
) -> None:
    prog, old_sha, _new_sha, venv, backup = _fixture(tmp_path)
    executable = venv / "bin" / "vq"
    lifecycle = _lifecycle(prog, old_sha, venv, backup)
    lifecycle.service_command = (
        "systemd-execstart",
        _systemd_execstart(executable, arguments=arguments),
    )
    _arm_receipt(prog, lifecycle)
    marker = admin.read_admin_update_marker()
    assert marker is not None

    with pytest.raises(admin.AdminError, match="systemd command is not canonical"):
        admin._parse_managed_update_receipt(
            marker,
            config.Config(programs={"vibeqc-queue": prog}),
        )


def test_systemd_reattest_ignores_runtime_fields(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pre-start reattestation binds the unit definition, not its last run."""
    prog, old_sha, _new_sha, venv, backup = _fixture(tmp_path)
    executable = venv / "bin" / "vq"
    lifecycle = _lifecycle(prog, old_sha, venv, backup)
    lifecycle.service_command = (
        "systemd-execstart",
        _systemd_execstart(
            executable,
            start_time="[Sat 2026-08-15 07:45:22 UTC]",
            pid=3_612_344,
        ),
    )
    stopped_command = _systemd_execstart(
        executable,
        start_time="[Sat 2026-08-15 07:45:22 UTC]",
        stop_time="[Sat 2026-08-15 07:45:45 UTC]",
        pid=3_612_344,
        code="exited",
        status="0",
    )
    monkeypatch.setattr(
        admin,
        "_query_daemon_service_state",
        lambda unused: admin._DaemonServiceState(
            manager=admin._DaemonServiceManager.SYSTEMD,
            running=False,
            pid=None,
            executable=str(executable),
            diagnostic="stopped systemd service",
            command_identity=("systemd-execstart", stopped_command),
        ),
    )

    matches, detail = admin._reattest_service_before_start(lifecycle)

    assert matches is True
    assert detail == "stopped systemd service"


def test_systemd_receipt_rejects_wrapper_retargeted_outside_venv(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unchanged raw unit path is insufficient after its symlink retargets."""
    prog, old_sha, _new_sha, venv, backup = _fixture(tmp_path)
    wrapper = tmp_path / "stable-bin" / "vq"
    wrapper.parent.mkdir()
    wrapper.symlink_to(venv / "bin" / "vq")
    raw_command = _systemd_execstart(wrapper)
    lifecycle = _lifecycle(prog, old_sha, venv, backup)
    lifecycle.service_executable = str(wrapper)
    lifecycle.service_command = ("systemd-execstart", raw_command)
    _arm_receipt(prog, lifecycle)
    unrelated = tmp_path / "unrelated-vq"
    unrelated.write_text("not the managed executable\n", encoding="utf-8")
    wrapper.unlink()
    wrapper.symlink_to(unrelated)
    state = admin._DaemonServiceState(
        manager=admin._DaemonServiceManager.SYSTEMD,
        running=False,
        pid=None,
        executable=str(wrapper),
        diagnostic="retargeted wrapper",
        command_identity=("systemd-execstart", raw_command),
    )
    monkeypatch.setattr(admin, "_query_daemon_service_state", lambda unused: state)
    marker = admin.read_admin_update_marker()
    assert marker is not None

    with pytest.raises(admin.AdminError, match="outside the configured venv"):
        admin._parse_managed_update_receipt(
            marker,
            config.Config(programs={"vibeqc-queue": prog}),
        )


def test_systemd_receipt_rejects_same_target_raw_command_rewrite(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Resolved identity does not weaken exact authoritative command binding."""
    prog, old_sha, _new_sha, venv, backup = _fixture(tmp_path)
    wrapper = tmp_path / "stable-bin" / "vq"
    wrapper.parent.mkdir()
    wrapper.symlink_to(venv / "bin" / "vq")
    raw_command = _systemd_execstart(wrapper)
    lifecycle = _lifecycle(prog, old_sha, venv, backup)
    lifecycle.service_executable = str(wrapper)
    lifecycle.service_command = ("systemd-execstart", raw_command)
    _arm_receipt(prog, lifecycle)
    rewritten_command = raw_command.replace(
        " daemon run ;",
        " daemon run --quiet ;",
    )
    state = admin._DaemonServiceState(
        manager=admin._DaemonServiceManager.SYSTEMD,
        running=False,
        pid=None,
        executable=str(wrapper),
        diagnostic="rewritten manager command",
        command_identity=("systemd-execstart", rewritten_command),
    )
    monkeypatch.setattr(admin, "_query_daemon_service_state", lambda unused: state)
    marker = admin.read_admin_update_marker()
    assert marker is not None

    with pytest.raises(admin.AdminError, match="current authoritative manager"):
        admin._parse_managed_update_receipt(
            marker,
            config.Config(programs={"vibeqc-queue": prog}),
        )


@pytest.mark.parametrize("value", [pytest.param(True, id="bool"), [], "missing"])
def test_receipt_rejects_malformed_pre_checkout_branch_without_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    value: object,
) -> None:
    prog, old_sha, _new_sha, venv, backup = _fixture(tmp_path)
    lifecycle = _lifecycle(prog, old_sha, venv, backup)
    marker_path = _arm_receipt(prog, lifecycle)
    payload = json.loads(marker_path.read_text(encoding="utf-8"))
    transaction = payload["managed_transaction"]
    if value == "missing":
        transaction.pop("pre_checkout_branch")
    else:
        transaction["pre_checkout_branch"] = value
    marker_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    before = marker_path.read_bytes()
    state = admin._DaemonServiceState(
        manager=admin._DaemonServiceManager.SYSTEMD,
        running=False,
        pid=None,
        executable=str(venv / "bin" / "vq"),
        diagnostic="unchanged manager",
        command_identity=lifecycle.service_command,
    )
    monkeypatch.setattr(admin, "_query_daemon_service_state", lambda unused: state)
    marker = admin.read_admin_update_marker()
    assert marker is not None

    with pytest.raises(admin.AdminError, match="pre_checkout_branch"):
        admin._parse_managed_update_receipt(
            marker,
            config.Config(programs={"vibeqc-queue": prog}),
        )

    assert marker_path.read_bytes() == before
    assert venv.is_dir()
    assert backup.is_dir()


@pytest.mark.parametrize('form', ['console', 'module'])
@pytest.mark.parametrize('venv_absent', [False, True])
def test_launchd_emitted_receipt_round_trips_both_entry_points(
    tmp_path, monkeypatch, form, venv_absent,
):
    prog, old_sha, _, venv, backup = _fixture(tmp_path)
    lifecycle = _lifecycle(prog, old_sha, venv, backup)
    lifecycle.manager = admin._DaemonServiceManager.LAUNCHD
    command = ([str(venv / 'bin/vq'), 'daemon', 'run'] if form == 'console'
               else [str(venv / 'bin/python'), '-m', 'vq', 'daemon', 'run'])
    lifecycle.service_executable = command[0]
    lifecycle.service_command = tuple(command)
    marker_path = _arm_receipt(prog, lifecycle)
    before = marker_path.read_bytes()
    if venv_absent:
        shutil.rmtree(venv)
    plist = Path.home() / 'Library/LaunchAgents' / f'{admin.LAUNCHD_DAEMON_LABEL}.plist'
    plist.parent.mkdir(parents=True, exist_ok=True)
    plist.write_bytes(plistlib.dumps({'ProgramArguments': command}))
    monkeypatch.setattr(admin, '_query_launchd_daemon',
                        lambda: (113, 'Could not find service'))
    marker = admin.read_admin_update_marker()
    assert marker is not None

    parsed_prog, parsed = admin._parse_managed_update_receipt(
        marker, config.Config(programs={'vibeqc-queue': prog}),
    )

    assert parsed_prog == prog
    assert parsed.service_executable == lifecycle.service_executable
    assert parsed.service_command == lifecycle.service_command
    assert parsed.venv_path == venv and parsed.venv_backup == backup
    assert marker_path.read_bytes() == before
    assert venv.exists() is not venv_absent


@pytest.mark.parametrize('form', ['console', 'module'])
@pytest.mark.parametrize('damage', [
    'foreign-venv', 'wrong-command', 'changed-plist', 'symlinked-plist',
])
def test_launchd_receipt_entry_points_reject_changed_identity_without_mutation(
    tmp_path, monkeypatch, form, damage,
):
    prog, old_sha, _, venv, backup = _fixture(tmp_path)
    lifecycle = _lifecycle(prog, old_sha, venv, backup)
    lifecycle.manager = admin._DaemonServiceManager.LAUNCHD
    command = ([str(venv / 'bin/vq'), 'daemon', 'run'] if form == 'console'
               else [str(venv / 'bin/python'), '-m', 'vq', 'daemon', 'run'])
    if damage == 'foreign-venv':
        command[0] = str(tmp_path / 'foreign/bin' / Path(command[0]).name)
    elif damage == 'wrong-command':
        command[-2] = 'web'
    lifecycle.service_executable = command[0]
    lifecycle.service_command = tuple(command)
    marker_path = _arm_receipt(prog, lifecycle)
    before = marker_path.read_bytes()
    plist_command = [*command, '--max-cpus', '2'] if damage == 'changed-plist' else command
    plist = Path.home() / 'Library/LaunchAgents' / f'{admin.LAUNCHD_DAEMON_LABEL}.plist'
    plist.parent.mkdir(parents=True, exist_ok=True)
    plist.write_bytes(plistlib.dumps({'ProgramArguments': plist_command}))
    if damage == 'symlinked-plist':
        other = plist.with_suffix('.backup')
        plist.rename(other)
        plist.symlink_to(other)
    monkeypatch.setattr(admin, '_query_launchd_daemon',
                        lambda: (113, 'Could not find service'))
    marker = admin.read_admin_update_marker()
    assert marker is not None

    with pytest.raises(admin.AdminError, match='managed receipt.*(executable|command)'):
        admin._parse_managed_update_receipt(
            marker, config.Config(programs={'vibeqc-queue': prog}),
        )

    assert marker_path.read_bytes() == before
    assert venv.is_dir() and backup.is_dir()


def test_launchd_real_venv_python_symlink_receipt_recovers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Normal venv bin/python symlinks remain valid launchd identities."""
    prog, old_sha, _new_sha, venv, backup = _fixture(tmp_path)
    admin.shutil.rmtree(venv)
    stdlib_venv.EnvBuilder(with_pip=False, symlinks=True).create(venv)
    (venv / "identity.txt").write_text("old\n", encoding="utf-8")
    python = venv / "bin" / "python"
    assert python.is_symlink(), "test requires normal POSIX venv symlink layout"
    lifecycle = _lifecycle(prog, old_sha, venv, backup)
    lifecycle.manager = admin._DaemonServiceManager.LAUNCHD
    lifecycle.service_executable = str(python)
    lifecycle.service_command = (
        str(python), "-m", "vq", "daemon", "run",
    )
    lifecycle.receipt_phase = "old_restored"
    lifecycle.backup_moved = False
    admin.shutil.rmtree(backup)
    _arm_receipt(prog, lifecycle)
    state = admin._DaemonServiceState(
        manager=admin._DaemonServiceManager.LAUNCHD,
        running=True,
        pid=61,
        executable=str(python),
        diagnostic="launchd real venv python symlink",
        command_identity=lifecycle.service_command,
    )
    monkeypatch.setattr(admin, "_query_daemon_service_state", lambda unused: state)
    monkeypatch.setattr(admin, "_launchd_plist_executable", lambda: str(python))
    monkeypatch.setattr(admin, "_launchd_plist_argv",
                        lambda: [str(python), "-m", "vq", "daemon", "run"])
    monkeypatch.setattr(admin, "_launchd_plist_matches_venv", lambda unused: True)
    monkeypatch.setattr(
        admin,
        "_verify_restarted_daemon",
        lambda *args, **kwargs: admin.DaemonProvenance(
            verified=True,
            actual_sha=old_sha,
            actual_tree_sha256=OLD_TREE,
            detail="old RPC identity verified",
        ),
    )
    _patch_clear_resume(monkeypatch)
    marker = _mark_receipt_stale(monkeypatch)

    recovered = admin.recover_managed_update(
        config.Config(programs={"vibeqc-queue": prog}),
        marker_id=marker.marker_id,
    )

    assert recovered.recovered is True
    assert "real venv python symlink" in recovered.detail
    assert not admin.admin_update_marker_exists()


def test_launchd_crash_after_venv_rename_recovers_from_backup_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Receipt parsing remains possible while the configured venv is absent."""
    prog, old_sha, _new_sha, venv, stale_backup = _fixture(tmp_path)
    admin.shutil.rmtree(venv)
    admin.shutil.rmtree(stale_backup)
    stdlib_venv.EnvBuilder(with_pip=False, symlinks=True).create(venv)
    (venv / "identity.txt").write_text("old\n", encoding="utf-8")
    python = venv / "bin" / "python"
    assert python.is_symlink(), "test requires normal POSIX venv symlink layout"
    lifecycle = _lifecycle(
        prog,
        old_sha,
        venv,
        tmp_path / (
            ".managed-venv.vq-admin-backup-"
            "0123456789abcdef0123456789abcdef"
        ),
    )
    lifecycle.manager = admin._DaemonServiceManager.LAUNCHD
    lifecycle.service_executable = str(python)
    lifecycle.service_command = (
        str(python), "-m", "vq", "daemon", "run",
    )
    _arm_receipt(prog, lifecycle)
    assert lifecycle.venv_backup is not None
    venv.rename(lifecycle.venv_backup)
    state = admin._DaemonServiceState(
        manager=admin._DaemonServiceManager.LAUNCHD,
        running=False,
        pid=None,
        executable=None,
        diagnostic="launchd unloaded after atomic venv rename",
        command_identity=lifecycle.service_command,
    )
    monkeypatch.setattr(admin, "_query_daemon_service_state", lambda unused: state)
    monkeypatch.setattr(admin, "_launchd_plist_executable", lambda: str(python))
    monkeypatch.setattr(admin, "_launchd_plist_argv",
                        lambda: [str(python), "-m", "vq", "daemon", "run"])
    monkeypatch.setattr(admin, "_launchd_plist_matches_venv", lambda unused: True)
    monkeypatch.setattr(
        admin,
        "_stop_managed_daemon_for_restore",
        lambda *args: (True, "launchd is quiescent"),
    )
    monkeypatch.setattr(admin, "_installed_tree_digest", _installed_digest)
    monkeypatch.setattr(
        admin,
        "_start_managed_daemon_update",
        lambda unused: (True, "old launchd service started"),
    )
    monkeypatch.setattr(
        admin,
        "_verify_restarted_daemon",
        lambda *args, **kwargs: admin.DaemonProvenance(
            verified=True,
            actual_sha=old_sha,
            actual_tree_sha256=OLD_TREE,
            detail="old RPC identity verified",
        ),
    )
    _patch_clear_resume(monkeypatch)
    marker = _mark_receipt_stale(monkeypatch)

    recovered = admin.recover_managed_update(
        config.Config(programs={"vibeqc-queue": prog}),
        marker_id=marker.marker_id,
    )

    assert recovered.recovered is True
    assert (venv / "identity.txt").read_text(encoding="utf-8") == "old\n"
    assert not lifecycle.venv_backup.exists()
    assert not admin.admin_update_marker_exists()


def _orphan_quarantine_fixture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[config.Config, config.VenvProgram, str, Path, bytes, str]:
    """Create an exact stale terminal-v1 marker whose assets are all gone."""
    prog, current_sha, _new_sha, _venv, backup = _fixture(tmp_path)
    admin.shutil.rmtree(backup)
    state_root = tmp_path / "state"
    monkeypatch.setenv("VQ_STATE_DIR", str(state_root))
    admin._set_owned_admin_update_marker_path(None)
    marker = admin.acquire_admin_update_marker(
        envs=["vibeqc-queue"], host="localhost",
    )
    admin._record_admin_update_pause_scope(
        pause_token="admin-update-abcdef012345",
        paused_jobids=[],
        surgical=False,
        multi_user=False,
    )
    foreign_git = tmp_path / "deleted-pytest-checkout"
    foreign_venv = tmp_path / "deleted-pytest-venv"
    transaction_id = "fedcba9876543210fedcba9876543210"
    foreign_backup = tmp_path / (
        f".{foreign_venv.name}.vq-admin-backup-{transaction_id}"
    )
    foreign_vq = foreign_venv / "bin" / "vq"
    marker = admin.read_admin_update_marker()
    assert marker is not None
    marker.pid = 999_999
    marker.pid_start_time = 0
    marker.state = admin.ADMIN_UPDATE_STATE_RESUMING
    marker.managed_transaction = {
        "schema": admin.MANAGED_UPDATE_RECEIPT_SCHEMA,
        "transaction_id": transaction_id,
        "owner_uid": os.geteuid(),
        "env": "vibeqc-queue",
        "phase": "target_committed",
        "manager": "systemd",
        "pre_pid": 1234,
        "was_running": True,
        "pre_source_sha": "12" * 20,
        "pre_source_tree_sha256": OLD_TREE,
        "pre_checkout_branch": "main",
        "git_dir": str(foreign_git),
        "venv_path": str(foreign_venv),
        "venv_backup": str(foreign_backup),
        "backup_moved": False,
        "service_executable": str(foreign_vq),
        "service_command": [
            "systemd-execstart",
            _systemd_execstart(foreign_vq),
        ],
        "target_source_sha": "34" * 20,
        "target_source_tree_sha256": NEW_TREE,
        "updated_at": "2026-08-30T12:00:00+00:00",
    }
    admin._write_admin_update_marker_atomic(marker)
    admin._set_owned_admin_update_marker_path(None)
    marker_path = admin.admin_update_marker_path()
    marker_bytes = marker_path.read_bytes()
    marker_sha = admin.hashlib.sha256(marker_bytes).hexdigest()
    status_path = admin.admin_status_path()
    status_path.write_text(
        json.dumps({
            "vibeqc-queue": {
                "last_updated_at": "2026-08-30T12:30:00+00:00",
                "last_success": True,
                "last_sha": current_sha,
            }
        }),
        encoding="utf-8",
    )
    status_path.chmod(0o600)
    monkeypatch.setattr(admin, "_pid_liveness", lambda unused: False)
    def current_runtime(
        unused: object, *, expected_source_sha: str,
    ) -> dict[str, object]:
        if expected_source_sha != current_sha:
            raise admin.AdminError("accepted current SHA does not match")
        return {
            "source_sha": expected_source_sha,
            "source_tree_sha256": "56" * 32,
            "pid": 4321,
            "euid": os.geteuid(),
            "version": "0.25.7",
            "manager": "systemd",
        }

    monkeypatch.setattr(admin, "_current_runtime_identity", current_runtime)
    return (
        config.Config(programs={"vibeqc-queue": prog}),
        prog,
        current_sha,
        marker_path,
        marker_bytes,
        marker_sha,
    )


def _quarantine(
    cfg: config.Config,
    marker: admin.AdminUpdateMarker,
    current_sha: str,
    marker_sha: str,
    *,
    dry_run: bool = False,
) -> admin.OrphanReceiptQuarantineResult:
    return admin.quarantine_orphaned_managed_receipt(
        cfg,
        marker_id=marker.marker_id,
        expected_marker_sha256=marker_sha,
        expected_current_source_sha=current_sha,
        reason="retire deleted pytest receipt after exact source review",
        dry_run=dry_run,
    )


def test_orphan_quarantine_dry_run_apply_and_restart_are_exact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg, _prog, current_sha, marker_path, marker_bytes, marker_sha = (
        _orphan_quarantine_fixture(tmp_path, monkeypatch)
    )
    marker = admin.read_admin_update_marker()
    assert marker is not None
    original = marker_path.lstat()

    planned = _quarantine(
        cfg, marker, current_sha, marker_sha, dry_run=True,
    )

    assert planned.dry_run is True
    assert planned.quarantined is False
    assert marker_path.read_bytes() == marker_bytes
    assert marker_path.lstat().st_ino == original.st_ino
    assert not (paths.state_root() / admin.ADMIN_UPDATE_QUARANTINE_DIRNAME).exists()

    applied = _quarantine(cfg, marker, current_sha, marker_sha)
    final = Path(applied.quarantine_path)
    assert applied.quarantined is True
    assert applied.plan_sha256 == planned.plan_sha256
    assert not marker_path.exists()
    assert stat.S_IMODE(final.lstat().st_mode) == 0o700
    for name in admin._QUARANTINE_FILE_NAMES:
        assert stat.S_IMODE((final / name).lstat().st_mode) == 0o600
    moved = final / "marker.json"
    assert moved.read_bytes() == marker_bytes
    assert moved.lstat().st_ino == original.st_ino
    receipt = json.loads((final / "quarantine-receipt.json").read_text())
    assert receipt["retention"] == "manual-cleanup-only"
    assert receipt["plan"]["pause"]["proven_clear"] is True

    repeated = _quarantine(cfg, marker, current_sha, marker_sha)
    assert repeated.quarantined is True
    assert repeated.plan_sha256 == applied.plan_sha256
    assert repeated.detail.startswith("quarantine already complete")


@pytest.mark.parametrize(
    "mutation",
    [
        pytest.param("live-writer", id="live-writer"),
        pytest.param("surviving-asset", id="surviving-asset"),
        pytest.param("pause-token", id="pause-token-not-clear"),
        pytest.param("runtime", id="runtime-unhealthy"),
        pytest.param("status-missing", id="status-missing"),
        pytest.param("status-corrupt", id="status-corrupt"),
        pytest.param("status-writeable", id="status-writeable"),
        pytest.param("phase", id="nonterminal-phase"),
        pytest.param("backup", id="backup-still-moved"),
    ],
)
def test_orphan_quarantine_failed_proof_preserves_marker_bytes_and_inode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
) -> None:
    cfg, _prog, current_sha, marker_path, marker_bytes, marker_sha = (
        _orphan_quarantine_fixture(tmp_path, monkeypatch)
    )
    marker = admin.read_admin_update_marker()
    assert marker is not None
    original_inode = marker_path.lstat().st_ino
    if mutation == "live-writer":
        monkeypatch.setattr(admin, "_pid_liveness", lambda unused: True)
    elif mutation == "surviving-asset":
        raw = marker.managed_transaction
        assert isinstance(raw, dict)
        Path(str(raw["git_dir"])).mkdir()
    elif mutation == "pause-token":
        class NotClear:
            proven_clear = False
            summary = "read-only token scan; NOT clear"

            @staticmethod
            def require_clear() -> None:
                raise PauseError("token remains")

        monkeypatch.setattr(
            admin, "prove_pause_token_absent", lambda *args, **kwargs: NotClear(),
        )
    elif mutation == "runtime":
        monkeypatch.setattr(
            admin,
            "_current_runtime_identity",
            lambda *args, **kwargs: (_ for _ in ()).throw(
                admin.AdminError("runtime mismatch")
            ),
        )
    elif mutation == "status-missing":
        admin.admin_status_path().unlink()
    elif mutation == "status-corrupt":
        admin.admin_status_path().write_text("{", encoding="utf-8")
        admin.admin_status_path().chmod(0o600)
    elif mutation == "status-writeable":
        admin.admin_status_path().chmod(0o666)
    else:
        raw = marker.managed_transaction
        assert isinstance(raw, dict)
        raw["phase" if mutation == "phase" else "backup_moved"] = (
            "armed" if mutation == "phase" else True
        )
        admin._write_admin_update_marker_atomic(marker)
        marker_bytes = marker_path.read_bytes()
        marker_sha = admin.hashlib.sha256(marker_bytes).hexdigest()
        original_inode = marker_path.lstat().st_ino
        admin._set_owned_admin_update_marker_path(None)

    with pytest.raises((admin.AdminError, admin.AdminUpdateInProgress, PauseError)):
        _quarantine(cfg, marker, current_sha, marker_sha)

    assert marker_path.read_bytes() == marker_bytes
    assert marker_path.lstat().st_ino == original_inode
    assert not (paths.state_root() / admin.ADMIN_UPDATE_QUARANTINE_DIRNAME).exists()


@pytest.mark.parametrize("field", ["marker-id", "marker-hash", "current-sha"])
def test_orphan_quarantine_wrong_selector_is_side_effect_free(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
) -> None:
    cfg, _prog, current_sha, marker_path, marker_bytes, marker_sha = (
        _orphan_quarantine_fixture(tmp_path, monkeypatch)
    )
    marker = admin.read_admin_update_marker()
    assert marker is not None
    inode = marker_path.lstat().st_ino
    kwargs = {
        "marker_id": marker.marker_id,
        "expected_marker_sha256": marker_sha,
        "expected_current_source_sha": current_sha,
        "reason": "selector discriminator",
    }
    if field == "marker-id":
        kwargs["marker_id"] = "0" * 32
    elif field == "marker-hash":
        kwargs["expected_marker_sha256"] = "0" * 64
    else:
        kwargs["expected_current_source_sha"] = "0" * 40
    with pytest.raises(admin.AdminError):
        admin.quarantine_orphaned_managed_receipt(cfg, **kwargs)
    assert marker_path.read_bytes() == marker_bytes
    assert marker_path.lstat().st_ino == inode


def test_orphan_quarantine_recovers_after_terminal_move_crash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg, _prog, current_sha, marker_path, marker_bytes, marker_sha = (
        _orphan_quarantine_fixture(tmp_path, monkeypatch)
    )
    marker = admin.read_admin_update_marker()
    assert marker is not None
    real_replace = admin.os.replace

    def crash_after_marker_move(source: object, target: object) -> None:
        real_replace(source, target)
        if Path(source) == marker_path and Path(target).name == "marker.json":
            raise _SimulatedProcessDeath()

    monkeypatch.setattr(admin.os, "replace", crash_after_marker_move)
    with pytest.raises(_SimulatedProcessDeath):
        _quarantine(cfg, marker, current_sha, marker_sha)
    assert not marker_path.exists()
    pending = next(
        (paths.state_root() / admin.ADMIN_UPDATE_QUARANTINE_DIRNAME).glob(
            ".*.pending"
        )
    )
    assert (pending / "marker.json").read_bytes() == marker_bytes

    monkeypatch.setattr(admin.os, "replace", real_replace)
    recovered = _quarantine(cfg, marker, current_sha, marker_sha)
    assert recovered.quarantined is True
    assert Path(recovered.quarantine_path, "marker.json").read_bytes() == marker_bytes


@pytest.mark.parametrize(
    ("evidence_name", "crash_write_call"),
    [
        pytest.param("admin-status.json", 1, id="status-write"),
        pytest.param("quarantine-receipt.json", 2, id="receipt-write"),
    ],
)
def test_orphan_quarantine_recovers_after_partial_evidence_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    evidence_name: str,
    crash_write_call: int,
) -> None:
    cfg, _prog, current_sha, marker_path, marker_bytes, marker_sha = (
        _orphan_quarantine_fixture(tmp_path, monkeypatch)
    )
    marker = admin.read_admin_update_marker()
    assert marker is not None
    marker_inode = marker_path.lstat().st_ino
    real_write = admin.os.write
    calls = 0

    def short_write_then_die(fd: int, payload: object) -> int:
        nonlocal calls
        calls += 1
        if calls == crash_write_call:
            view = memoryview(payload)  # type: ignore[arg-type]
            real_write(fd, view[:7])
            raise _SimulatedProcessDeath()
        return real_write(fd, payload)  # type: ignore[arg-type]

    monkeypatch.setattr(admin.os, "write", short_write_then_die)
    with pytest.raises(_SimulatedProcessDeath):
        _quarantine(cfg, marker, current_sha, marker_sha)

    assert marker_path.read_bytes() == marker_bytes
    assert marker_path.lstat().st_ino == marker_inode
    pending = next(
        (paths.state_root() / admin.ADMIN_UPDATE_QUARANTINE_DIRNAME).glob(
            ".*.pending"
        )
    )
    assert not (pending / evidence_name).exists()
    staging = pending / f".{evidence_name}.tmp"
    assert staging.read_bytes() == b"" or len(staging.read_bytes()) == 7
    assert stat.S_IMODE(staging.lstat().st_mode) == 0o600

    monkeypatch.setattr(admin.os, "write", real_write)
    recovered = _quarantine(cfg, marker, current_sha, marker_sha)
    final = Path(recovered.quarantine_path)
    assert recovered.quarantined is True
    assert not staging.exists()
    assert set(item.name for item in final.iterdir()) == admin._QUARANTINE_FILE_NAMES
    assert (final / "marker.json").read_bytes() == marker_bytes


@pytest.mark.parametrize(
    "evidence_name",
    ["admin-status.json", "quarantine-receipt.json"],
)
def test_orphan_quarantine_rejects_duplicate_final_and_staging_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    evidence_name: str,
) -> None:
    cfg, _prog, current_sha, marker_path, marker_bytes, marker_sha = (
        _orphan_quarantine_fixture(tmp_path, monkeypatch)
    )
    marker = admin.read_admin_update_marker()
    assert marker is not None
    marker_inode = marker_path.lstat().st_ino
    real_read = admin._read_secure_owner_file

    def die_after_preparing_evidence(
        path: Path, *, label: str, max_bytes: int = 8 * 1024 * 1024,
    ) -> admin._SecureBytes:
        quarantine_root = paths.state_root() / admin.ADMIN_UPDATE_QUARANTINE_DIRNAME
        pending = next(quarantine_root.glob(".*.pending"), None)
        if (
            path == marker_path
            and pending is not None
            and (pending / "admin-status.json").exists()
            and (pending / "quarantine-receipt.json").exists()
        ):
            raise _SimulatedProcessDeath()
        return real_read(path, label=label, max_bytes=max_bytes)

    monkeypatch.setattr(admin, "_read_secure_owner_file", die_after_preparing_evidence)
    with pytest.raises(_SimulatedProcessDeath):
        _quarantine(cfg, marker, current_sha, marker_sha)
    monkeypatch.setattr(admin, "_read_secure_owner_file", real_read)

    pending = next(
        (paths.state_root() / admin.ADMIN_UPDATE_QUARANTINE_DIRNAME).glob(
            ".*.pending"
        )
    )
    staging = pending / f".{evidence_name}.tmp"
    staging.write_bytes(b"stale duplicate staging bytes")
    staging.chmod(0o600)

    for dry_run in (True, False):
        with pytest.raises(admin.AdminError, match="duplicate final and staging"):
            _quarantine(
                cfg, marker, current_sha, marker_sha, dry_run=dry_run,
            )
        assert marker_path.read_bytes() == marker_bytes
        assert marker_path.lstat().st_ino == marker_inode


@pytest.mark.parametrize(
    "evidence_name",
    ["admin-status.json", "quarantine-receipt.json"],
)
def test_orphan_quarantine_rejects_unsafe_lone_staging_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    evidence_name: str,
) -> None:
    cfg, _prog, current_sha, marker_path, marker_bytes, marker_sha = (
        _orphan_quarantine_fixture(tmp_path, monkeypatch)
    )
    marker = admin.read_admin_update_marker()
    assert marker is not None
    marker_inode = marker_path.lstat().st_ino
    root = paths.state_root() / admin.ADMIN_UPDATE_QUARANTINE_DIRNAME
    pending = root / f".{marker.marker_id}-{marker_sha}.pending"
    root.mkdir(mode=0o700)
    pending.mkdir(mode=0o700)
    staging = pending / f".{evidence_name}.tmp"
    staging.write_bytes(b"partial")
    staging.chmod(0o644)

    for dry_run in (True, False):
        with pytest.raises(admin.AdminError, match="unsafe prepared quarantine"):
            _quarantine(
                cfg, marker, current_sha, marker_sha, dry_run=dry_run,
            )
        assert marker_path.read_bytes() == marker_bytes
        assert marker_path.lstat().st_ino == marker_inode


@pytest.mark.parametrize(
    "evidence_name",
    ["admin-status.json", "quarantine-receipt.json"],
)
def test_orphan_quarantine_rechecks_final_evidence_before_marker_move(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    evidence_name: str,
) -> None:
    cfg, _prog, current_sha, marker_path, marker_bytes, marker_sha = (
        _orphan_quarantine_fixture(tmp_path, monkeypatch)
    )
    marker = admin.read_admin_update_marker()
    assert marker is not None
    marker_inode = marker_path.lstat().st_ino
    real_read = admin._read_secure_owner_file
    replaced = False

    def replace_evidence_at_final_marker_reread(
        path: Path, *, label: str, max_bytes: int = 8 * 1024 * 1024,
    ) -> admin._SecureBytes:
        nonlocal replaced
        quarantine_root = paths.state_root() / admin.ADMIN_UPDATE_QUARANTINE_DIRNAME
        pending = next(quarantine_root.glob(".*.pending"), None)
        if (
            not replaced
            and path == marker_path
            and pending is not None
            and (pending / "admin-status.json").exists()
            and (pending / "quarantine-receipt.json").exists()
        ):
            replacement = pending / f".race-{evidence_name}"
            replacement.write_bytes(b"same-owner replacement")
            replacement.chmod(0o600)
            os.replace(replacement, pending / evidence_name)
            replaced = True
        return real_read(path, label=label, max_bytes=max_bytes)

    monkeypatch.setattr(
        admin,
        "_read_secure_owner_file",
        replace_evidence_at_final_marker_reread,
    )
    with pytest.raises(admin.AdminUpdateInProgress, match="changed before marker"):
        _quarantine(cfg, marker, current_sha, marker_sha)

    assert replaced is True
    assert marker_path.read_bytes() == marker_bytes
    assert marker_path.lstat().st_ino == marker_inode
    final = (
        paths.state_root()
        / admin.ADMIN_UPDATE_QUARANTINE_DIRNAME
        / f"{marker.marker_id}-{marker_sha}"
    )
    assert not final.exists()


def test_orphan_quarantine_rejects_mismatched_prepared_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg, _prog, current_sha, marker_path, marker_bytes, marker_sha = (
        _orphan_quarantine_fixture(tmp_path, monkeypatch)
    )
    marker = admin.read_admin_update_marker()
    assert marker is not None
    root = paths.state_root() / admin.ADMIN_UPDATE_QUARANTINE_DIRNAME
    pending = root / f".{marker.marker_id}-{marker_sha}.pending"
    root.mkdir(mode=0o700)
    pending.mkdir(mode=0o700)
    evidence = pending / "admin-status.json"
    evidence.write_bytes(b"different status bytes")
    evidence.chmod(0o600)
    inode = marker_path.lstat().st_ino

    for dry_run in (True, False):
        with pytest.raises(admin.AdminError, match="conflicts"):
            _quarantine(
                cfg, marker, current_sha, marker_sha, dry_run=dry_run,
            )
        assert marker_path.read_bytes() == marker_bytes
        assert marker_path.lstat().st_ino == inode


def test_orphan_quarantine_refuses_state_root_rebinding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg, _prog, current_sha, marker_path, marker_bytes, marker_sha = (
        _orphan_quarantine_fixture(tmp_path, monkeypatch)
    )
    marker = admin.read_admin_update_marker()
    assert marker is not None

    def rebind(unused: object, *, expected_source_sha: str) -> dict[str, object]:
        monkeypatch.setenv("VQ_STATE_DIR", str(tmp_path / "different-state"))
        return {"source_sha": expected_source_sha}

    monkeypatch.setattr(admin, "_current_runtime_identity", rebind)
    with pytest.raises(admin.AdminUpdateInProgress, match="binding changed"):
        _quarantine(cfg, marker, current_sha, marker_sha)
    assert marker_path.read_bytes() == marker_bytes


def _patch_current_runtime_proof(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[config.VenvProgram, str, dict[str, object]]:
    prog, current_sha, _new_sha, venv, backup = _fixture(tmp_path)
    admin.shutil.rmtree(backup)
    executable = venv / "bin" / "vq"
    python = Path(prog.python).resolve()
    socket = tmp_path / "user-daemon.sock"
    tree = "78" * 32
    version = "0.25.7"
    command = (
        "systemd-execstart",
        _systemd_execstart(executable),
    )
    monkeypatch.setattr(
        admin, "_canonical_lifecycle_checkout", lambda unused: Path(prog.git_dir),
    )
    monkeypatch.setattr(admin, "_guard_git_index_unlocked", lambda unused: None)
    monkeypatch.setattr(admin, "_git_head_sha", lambda unused: current_sha)
    monkeypatch.setattr(admin, "_run_git_status_porcelain", lambda unused: (0, ""))
    monkeypatch.setattr(
        admin, "source_tree_sha256_at_git_commit", lambda *args: tree,
    )
    monkeypatch.setattr(admin, "_installed_tree_digest", lambda unused: tree)
    monkeypatch.setattr(
        admin,
        "_detect_vq_self_update",
        lambda unused: admin._SelfUpdateProbe(
            is_self_update=True,
            daemon_running=True,
            service_manager="systemd",
            manager_available=True,
            diagnostic="exact running test service",
        ),
    )
    monkeypatch.setattr(
        admin,
        "_query_daemon_service_state",
        lambda unused: admin._DaemonServiceState(
            manager=admin._DaemonServiceManager.SYSTEMD,
            running=True,
            pid=4321,
            executable=str(executable),
            diagnostic="exact service",
            command_identity=command,
        ),
    )
    monkeypatch.setattr(admin, "_service_executable_matches", lambda *a, **k: True)
    monkeypatch.setattr(
        admin,
        "_canonical_lifecycle_target",
        lambda unused: venv,
    )
    real_run = admin.subprocess.run

    def version_run(argv: list[str], **kwargs: object) -> subprocess.CompletedProcess:
        if argv[:2] == [prog.python, "-I"]:
            return subprocess.CompletedProcess(argv, 0, version + "\n", "")
        return real_run(argv, **kwargs)

    monkeypatch.setattr(admin.subprocess, "run", version_run)
    from vq import rpc

    monkeypatch.setattr(rpc, "user_socket_path", lambda: socket)
    identity: dict[str, object] = {
        "pid": 4321,
        "euid": os.geteuid(),
        "python_executable": str(python),
        "argv": [str(executable), "daemon", "run"],
        "version": version,
        "source_sha": current_sha,
        "source_tree_sha256": tree,
        "multi_user": False,
        "socket_path": str(socket),
    }
    ping = {
        "version": version,
        "source_sha": current_sha,
        "source_tree_sha256": tree,
        "multi_user": False,
    }

    def rpc_call(method: str, **kwargs: object) -> object:
        assert kwargs["socket_override"] == socket
        return {
            "ping": ping,
            "get_methods": {"methods": ["get_process_identity"]},
            "get_process_identity": identity,
        }[method]

    monkeypatch.setattr(rpc, "call", rpc_call)
    return prog, current_sha, identity


def test_current_runtime_orphan_attestation_accepts_exact_mutual_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prog, current_sha, _identity = _patch_current_runtime_proof(
        tmp_path, monkeypatch,
    )

    evidence = admin._current_runtime_identity(
        prog, expected_source_sha=current_sha,
    )

    assert evidence["source_sha"] == current_sha
    assert evidence["pid"] == 4321
    assert evidence["installed_tree_sha256"] == "78" * 32


@pytest.mark.parametrize(
    ("field", "bad"),
    [
        pytest.param("pid", 9999, id="pid"),
        pytest.param("euid", 9999, id="euid"),
        pytest.param("version", "0.0.0", id="version"),
        pytest.param("source_sha", "0" * 40, id="source-sha"),
        pytest.param("source_tree_sha256", "0" * 64, id="source-tree"),
        pytest.param("multi_user", True, id="multi-user"),
        pytest.param("socket_path", "/different/socket", id="socket"),
        pytest.param("python_executable", "/different/python", id="python"),
        pytest.param("argv", ["vq", "daemon", "runner"], id="argv"),
    ],
)
def test_current_runtime_orphan_attestation_rejects_each_process_mismatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    bad: object,
) -> None:
    prog, current_sha, identity = _patch_current_runtime_proof(
        tmp_path, monkeypatch,
    )
    identity[field] = bad

    with pytest.raises(admin.AdminError, match="identity|Python|argv"):
        admin._current_runtime_identity(
            prog, expected_source_sha=current_sha,
        )


def test_marker_admission_refuses_another_thread(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("VQ_STATE_DIR", str(tmp_path / "state"))
    results: list[str] = []
    release = admin.threading.Event()

    def contend() -> None:
        try:
            with admin._admin_update_marker_lock():
                results.append("acquired")
        except admin.AdminUpdateInProgress:
            results.append("refused")
        finally:
            release.set()

    with admin._admin_update_marker_lock():
        thread = admin.threading.Thread(target=contend)
        thread.start()
        assert release.wait(timeout=2)
    thread.join(timeout=2)
    assert results == ["refused"]


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires POSIX fork")
def test_admin_lock_descriptors_and_reentry_are_invalidated_after_fork(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("VQ_STATE_DIR", str(tmp_path / "state"))
    read_fd, write_fd = os.pipe()
    with admin._admin_update_marker_lock():
        assert admin._admin_lock_fds
        pid = os.fork()
        if pid == 0:
            try:
                os.close(read_fd)
                payload = json.dumps({
                    "fds": len(admin._admin_lock_fds),
                    "depth": getattr(admin._admin_update_marker_local, "depth", 0),
                    "binding": admin._active_admin_state_binding() is not None,
                }).encode()
                os.write(write_fd, payload)
            finally:
                os._exit(0)
        os.close(write_fd)
        child_payload = os.read(read_fd, 4096)
        _, status = os.waitpid(pid, 0)
    os.close(read_fd)

    assert os.waitstatus_to_exitcode(status) == 0
    assert json.loads(child_payload) == {
        "fds": 0,
        "depth": 0,
        "binding": False,
    }


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires POSIX fork")
def test_fork_child_cannot_inherit_marker_owner_or_implicitly_clear(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("VQ_STATE_DIR", str(tmp_path / "state"))
    marker = admin.write_admin_update_marker(["parent"], "localhost")
    marker_path = admin.admin_update_marker_path()
    child_marker_path = paths.state_root() / "fork-child-write.json"
    marker_bytes = marker_path.read_bytes()
    read_fd, write_fd = os.pipe()
    pid = os.fork()
    if pid == 0:
        try:
            os.close(read_fd)
            payload: dict[str, object] = {
                "owned": (
                    str(admin._owned_admin_update_marker_path())
                    if admin._owned_admin_update_marker_path() is not None
                    else None
                ),
                "clear": "not-attempted",
                "fresh_reentry": False,
                "write": False,
            }
            try:
                admin.clear_admin_update_marker()
            except admin.AdminUpdateInProgress:
                payload["clear"] = "refused"
            else:
                payload["clear"] = "unexpected-success"
            with admin._admin_update_marker_lock():
                payload["fresh_reentry"] = (
                    getattr(admin._admin_update_marker_local, "depth", 0) == 1
                    and getattr(admin._admin_update_marker_local, "pid", None)
                    == os.getpid()
                )
            marker.pid = os.getpid()
            marker.marker_id = "1" * 32
            admin._write_admin_update_marker_atomic(
                marker, path=child_marker_path,
            )
            payload["write"] = (
                child_marker_path.exists()
                and marker_path.read_bytes() == marker_bytes
            )
            os.write(write_fd, json.dumps(payload).encode())
        finally:
            os._exit(0)
    os.close(write_fd)
    child_payload = os.read(read_fd, 4096)
    _, status = os.waitpid(pid, 0)
    os.close(read_fd)

    assert os.waitstatus_to_exitcode(status) == 0
    assert json.loads(child_payload) == {
        "owned": None,
        "clear": "refused",
        "fresh_reentry": True,
        "write": True,
    }
    assert marker_path.read_bytes() == marker_bytes
    child_marker_path.unlink()
    assert admin._owned_admin_update_marker_path() == marker_path
    assert admin.clear_admin_update_marker(marker) == marker


def test_recover_update_cli_forwards_exact_quarantine_contract(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[tuple[str, ...]] = []
    cfg = config.Config(
        hosts={
            "remote": config.HostConfig(
                ssh="remote.example.invalid",
            )
        }
    )
    monkeypatch.setattr(cli.config, "load_config", lambda: cfg)
    monkeypatch.setattr(cli, "_resolve_admin_token", lambda *args, **kwargs: None)
    monkeypatch.setattr(cli, "_remote_admin_auth", lambda *args: ([], None))
    monkeypatch.setattr(
        cli,
        "_delegate_to_remote",
        lambda owner, loaded, *args, **kwargs: captured.append(args) or "ok\n",
    )
    result = CliRunner().invoke(
        cli.main,
        [
            "admin", "recover-update", "remote",
            "--quarantine-orphaned-receipt",
            "--marker-id", "a" * 32,
            "--expected-marker-sha256", "b" * 64,
            "--expected-current-source-sha", "c" * 40,
            "--reason", "reviewed orphan",
            "--dry-run", "--json",
        ],
    )
    assert result.exit_code == 0, result.output
    assert captured == [(
        "admin", "recover-update", "localhost",
        "--marker-id", "a" * 32,
        "--quarantine-orphaned-receipt",
        "--expected-marker-sha256", "b" * 64,
        "--expected-current-source-sha", "c" * 40,
        "--reason", "reviewed orphan",
        "--dry-run", "--json",
    )]


def _driver_runtime_config() -> config.Config:
    return config.Config(
        hosts={
            "remote": config.HostConfig(
                ssh="remote.example.invalid",
                remote_vq="/home/USER/vq/.venv/bin/vq",
            )
        }
    )


def test_recover_update_cli_can_use_integrity_checked_driver_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[dict[str, object]] = []
    cfg = _driver_runtime_config()
    monkeypatch.setattr(cli.config, "load_config", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "_resolve_admin_token",
        lambda *args, **kwargs: "driver-secret",
    )
    monkeypatch.setattr(
        cli,
        "_remote_admin_auth",
        lambda *args: (["--token-stdin"], "driver-secret\n"),
    )
    monkeypatch.setattr(
        cli,
        "_delegate_to_remote",
        lambda *args, **kwargs: pytest.fail("old remote vq must not parse recovery"),
    )

    def run_driver_runtime(*args: object, **kwargs: object) -> str:
        captured.append({"args": args, "kwargs": kwargs})
        return "driver recovery complete\n"

    monkeypatch.setattr(
        admin,
        "recover_remote_managed_update_with_driver_runtime",
        run_driver_runtime,
    )
    result = CliRunner().invoke(
        cli.main,
        [
            "admin",
            "recover-update",
            "remote",
            "--marker-id",
            "a" * 32,
            "--with-driver-runtime",
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    assert result.output == "driver recovery complete\n"
    assert captured == [
        {
            "args": (cfg, "remote"),
            "kwargs": {
                "marker_id": "a" * 32,
                "remote_auth_args": ("--token-stdin",),
                "stdin_data": "driver-secret\n",
                "as_json": True,
            },
        }
    ]


def test_staged_driver_identity_refuses_an_unstaged_runtime() -> None:
    result = CliRunner().invoke(
        cli.main,
        [
            "admin",
            "recover-update",
            "localhost",
            "--staged-driver-archive-sha256",
            "a" * 64,
        ],
    )

    assert result.exit_code == 2
    assert "integrity-checked zip-imported vq runtime" in result.output


def test_driver_runtime_recovery_verifies_archive_before_remote_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = _driver_runtime_config()
    archive_sha = "a" * 64
    calls: list[tuple[str, tuple[str, ...], dict[str, object]]] = []

    def write_archive(path: Path) -> str:
        path.write_bytes(b"exact current vq archive")
        return archive_sha

    def upload(
        host_cfg: config.HostConfig,
        local_path: Path,
        remote_path: str,
        **kwargs: object,
    ) -> None:
        calls.append(("upload", (str(local_path), remote_path), kwargs))

    def run_shell(
        host_cfg: config.HostConfig,
        *args: str,
        **kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        calls.append(("shell", args, kwargs))
        if args[0] in {"mkdir", "rmdir", "rm"}:
            return subprocess.CompletedProcess(args, 0, "", "")
        if args[0] == "sha256sum":
            return subprocess.CompletedProcess(
                args,
                0,
                f"{'c' * 64}  {args[1]}\n",
                "",
            )
        pytest.fail("recovery mutation must not run after a digest mismatch")

    monkeypatch.setattr(admin, "_write_driver_recovery_archive", write_archive)
    monkeypatch.setattr(admin.transport, "upload_file", upload)
    monkeypatch.setattr(admin.transport, "run_remote_shell", run_shell)

    with pytest.raises(admin.AdminError, match="archive digest mismatch"):
        admin.recover_remote_managed_update_with_driver_runtime(
            cfg,
            "remote",
            marker_id="a" * 32,
            remote_auth_args=("--token-stdin",),
            stdin_data="driver-secret\n",
            as_json=True,
        )

    assert [call[0] for call in calls] == [
        "shell",
        "upload",
        "shell",
        "shell",
        "shell",
    ]
    assert all(call[1][0] != "/usr/bin/env" for call in calls)


def test_driver_runtime_recovery_runs_exact_archive_then_cleans_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = _driver_runtime_config()
    archive_sha = "a" * 64
    calls: list[tuple[str, tuple[str, ...], dict[str, object]]] = []

    def write_archive(path: Path) -> str:
        path.write_bytes(b"exact current vq archive")
        return archive_sha

    def upload(
        host_cfg: config.HostConfig,
        local_path: Path,
        remote_path: str,
        **kwargs: object,
    ) -> None:
        calls.append(("upload", (str(local_path), remote_path), kwargs))

    def run_shell(
        host_cfg: config.HostConfig,
        *args: str,
        **kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        calls.append(("shell", args, kwargs))
        if args[0] in {"mkdir", "rmdir"}:
            return subprocess.CompletedProcess(args, 0, "", "")
        if args[0] == "sha256sum":
            return subprocess.CompletedProcess(
                args,
                0,
                f"{archive_sha}  {args[1]}\n",
                "",
            )
        if args[0] == "/usr/bin/env":
            return subprocess.CompletedProcess(args, 0, "recovered\n", "")
        assert args[:3] == ("rm", "-f", "--")
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(admin, "_write_driver_recovery_archive", write_archive)
    monkeypatch.setattr(admin.transport, "upload_file", upload)
    monkeypatch.setattr(admin.transport, "run_remote_shell", run_shell)

    result = admin.recover_remote_managed_update_with_driver_runtime(
        cfg,
        "remote",
        marker_id="a" * 32,
        remote_auth_args=("--token-stdin",),
        stdin_data="driver-secret\n",
        as_json=True,
    )

    assert result == "recovered\n"
    assert [call[0] for call in calls] == [
        "shell",
        "upload",
        "shell",
        "shell",
        "shell",
        "shell",
    ]
    remote_archive = calls[2][1][1]
    assert remote_archive.startswith("vqscratch/vq-driver-recovery/")
    recovery_args = calls[3][1]
    assert recovery_args == (
        "/usr/bin/env",
        "PYTHONDONTWRITEBYTECODE=1",
        "PYTHONNOUSERSITE=1",
        f"PYTHONPATH={remote_archive}",
        "/home/USER/vq/.venv/bin/vq",
        "admin",
        "recover-update",
        "localhost",
        "--marker-id",
        "a" * 32,
        "--staged-driver-archive-sha256",
        archive_sha,
        "--json",
        "--token-stdin",
    )
    assert calls[3][2]["stdin_data"] == "driver-secret\n"
    assert calls[3][2]["retry_transient"] == 0
    assert calls[4][1] == ("rm", "-f", "--", remote_archive)
    assert calls[5][1] == (
        "rmdir",
        "--",
        remote_archive.rsplit("/", 1)[0],
    )


def test_driver_runtime_recovery_retains_archive_on_ambiguous_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = _driver_runtime_config()
    archive_sha = "a" * 64
    calls: list[tuple[str, tuple[str, ...]]] = []

    def write_archive(path: Path) -> str:
        path.write_bytes(b"exact current vq archive")
        return archive_sha

    def upload(
        host_cfg: config.HostConfig,
        local_path: Path,
        remote_path: str,
        **kwargs: object,
    ) -> None:
        calls.append(("upload", (str(local_path), remote_path)))

    def run_shell(
        host_cfg: config.HostConfig,
        *args: str,
        **kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        calls.append(("shell", args))
        if args[0] == "mkdir":
            return subprocess.CompletedProcess(args, 0, "", "")
        if args[0] == "sha256sum":
            return subprocess.CompletedProcess(
                args,
                0,
                f"{archive_sha}  {args[1]}\n",
                "",
            )
        if args[0] == "/usr/bin/env":
            raise admin.transport.RemoteOutcomeUnknown("observer lost")
        pytest.fail("ambiguous recovery archive must be retained")

    monkeypatch.setattr(admin, "_write_driver_recovery_archive", write_archive)
    monkeypatch.setattr(admin.transport, "upload_file", upload)
    monkeypatch.setattr(admin.transport, "run_remote_shell", run_shell)

    with pytest.raises(
        admin.transport.RemoteOutcomeUnknown,
        match=r"retained at remote:vqscratch/vq-driver-recovery/",
    ):
        admin.recover_remote_managed_update_with_driver_runtime(
            cfg,
            "remote",
            marker_id="a" * 32,
            remote_auth_args=(),
            stdin_data=None,
            as_json=False,
        )

    assert [call[0] for call in calls] == ["shell", "upload", "shell", "shell"]


def test_driver_recovery_archive_is_the_imported_runtime(
    tmp_path: Path,
) -> None:
    archive = tmp_path / "driver.pyz"
    archive_sha256 = admin._write_driver_recovery_archive(archive)
    environment = {
        **os.environ,
        "PYTHONPATH": str(archive),
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONNOUSERSITE": "1",
        "VQ_CONFIG_DIR": str(tmp_path / "config"),
        "VQ_STATE_DIR": str(tmp_path / "state"),
    }

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "vq.cli",
            "admin",
            "recover-update",
            "localhost",
            "--staged-driver-archive-sha256",
            archive_sha256,
            "--json",
        ],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 2
    assert "transaction receipt; found 0" in result.stderr
    assert "staged driver recovery" not in result.stderr


def _driver_update_runtime_config(
    *,
    remote_token_file: str | None = None,
    scheduler_topology: bool = False,
    remote_vq: str = "/home/USER/vq/.venv/bin/vq",
) -> config.Config:
    hosts = {
        "localhost": config.HostConfig(
            ssh="localhost",
            remote_vq="/local/vq/.venv/bin/vq",
        ),
        "remote": config.HostConfig(
            ssh="remote.example.invalid",
            remote_vq=remote_vq,
            admin_token_file=remote_token_file,
            fleet_role="managed",
        ),
        "other": config.HostConfig(
            ssh="other.example.invalid",
            # Synthetic foreign-owner path; preserve the runtime fixture value.
            remote_vq="/" "home/OTHER/vq/.venv/bin/vq",
            admin_token_file="/etc/vq/other-admin-token",
            fleet_role="managed",
        ),
    }
    if scheduler_topology:
        hosts["host_f"] = config.HostConfig(
            ssh="host_f.example.invalid",
            remote_vq="/home/USER/vq/.venv/bin/vq",
            fleet_role="managed",
            scheduler="pbs",
            scheduler_dialect="torque",
            scratch_root="/scratch/USER",
            scheduler_driver="remote",
            scheduler_update_command="/site/update-vq-helper",
            scheduler_install_command="/site/install-vq-helper",
            scheduler_runtime_deployments={
                "vibeqc-queue": config.SchedulerRuntimeDeployment(
                    update_command="/site/update-runtime",
                    verify_command="/site/verify-runtime",
                )
            },
        )
    return config.Config(default_host="remote", hosts=hosts)


@pytest.mark.parametrize(
    ("remote_token_file", "expected_auth", "expected_stdin"),
    [
        pytest.param(
            None,
            ("--token-stdin",),
            "driver-secret\n",
            id="driver-token-via-stdin",
        ),
        pytest.param(
            "/etc/vq/remote-admin-token",
            ("--token-file", "/etc/vq/remote-admin-token"),
            None,
            id="target-specific-token-file",
        ),
    ],
)
def test_update_cli_can_use_integrity_checked_driver_runtime_and_valid_json(
    monkeypatch: pytest.MonkeyPatch,
    remote_token_file: str | None,
    expected_auth: tuple[str, ...],
    expected_stdin: str | None,
) -> None:
    """A current driver must bypass the old remote update parser exactly once."""
    captured: list[dict[str, object]] = []
    cfg = _driver_update_runtime_config(remote_token_file=remote_token_file)
    monkeypatch.delenv("VQ_UPDATE_SCRIPT_TIMEOUT", raising=False)
    monkeypatch.delenv("VQ_BUILD_STALL_TIMEOUT", raising=False)
    monkeypatch.delenv("VQ_REMOTE_ADMIN_UPDATE_TIMEOUT", raising=False)
    monkeypatch.setattr(cli.config, "load_config", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "_resolve_admin_token",
        lambda *args, **kwargs: "driver-secret",
    )
    monkeypatch.setattr(
        cli,
        "_emit_remote_admin_timeout_summary",
        lambda *args: None,
    )
    monkeypatch.setattr(
        cli,
        "_forward_admin_command",
        lambda *args, **kwargs: pytest.fail("old remote vq must not parse the guarded update"),
    )

    def run_driver_runtime(*args: object, **kwargs: object) -> str:
        captured.append({"args": args, "kwargs": kwargs})
        return (
            json.dumps(
                {
                    "env": "vibeqc-queue",
                    "last_sha": "b" * 40,
                    "success": True,
                },
                sort_keys=True,
            )
            + "\n"
        )

    monkeypatch.setattr(
        admin,
        "update_remote_managed_env_with_driver_runtime",
        run_driver_runtime,
        raising=False,
    )
    result = CliRunner().invoke(
        cli.main,
        [
            "admin",
            "update",
            "vibeqc-queue",
            "remote",
            "--with-driver-runtime",
            "--tag",
            "v0.15.160",
            "--expected-sha",
            "b" * 40,
            "--update-script-arg",
            "--recreate-venv",
            "--show-output",
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == {
        "env": "vibeqc-queue",
        "last_sha": "b" * 40,
        "success": True,
    }
    assert captured == [
        {
            "args": (cfg, "remote"),
            "kwargs": {
                "env": "vibeqc-queue",
                "expected_sha": "b" * 40,
                "expected_tag": "v0.15.160",
                "remote_auth_args": expected_auth,
                "stdin_data": expected_stdin,
                "as_json": True,
                "update_script_args": ("--recreate-venv",),
                "show_output": True,
                "remote_timeout_env": {
                    "VQ_UPDATE_SCRIPT_TIMEOUT": "14400.0",
                    "VQ_BUILD_STALL_TIMEOUT": "3600.0",
                },
                "timeout": 15000.0,
            },
        }
    ]


@pytest.mark.parametrize(
    ("scheduler_topology", "argv", "error"),
    [
        pytest.param(
            False,
            ["vibeqc-queue", "remote", "--tag", "v0.15.160"],
            "--with-driver-runtime requires --expected-sha FULL_SHA",
            id="missing-expected-sha",
        ),
        pytest.param(
            False,
            ["vibeqc-queue", "localhost", "--expected-sha", "b" * 40],
            "--with-driver-runtime requires a non-local direct host",
            id="local-target",
        ),
        pytest.param(
            False,
            ["vibeqc-queue", "--all-hosts", "--expected-sha", "b" * 40],
            "--with-driver-runtime targets exactly one host",
            id="all-hosts",
        ),
        pytest.param(
            False,
            ["--all", "remote", "--expected-sha", "b" * 40],
            "--with-driver-runtime targets one managed environment",
            id="batch",
        ),
        pytest.param(
            True,
            ["host_f", "--expected-sha", "b" * 40],
            "--with-driver-runtime is not a scheduler-helper update",
            id="scheduler-helper",
        ),
        pytest.param(
            True,
            ["vibeqc-queue", "host_f", "--expected-sha", "b" * 40],
            "--with-driver-runtime is not a scheduler-runtime update",
            id="scheduler-runtime",
        ),
        pytest.param(
            True,
            ["vibeqc-queue", "remote", "--expected-sha", "b" * 40],
            "--with-driver-runtime refuses a scheduler_driver",
            id="scheduler-driver",
        ),
        pytest.param(
            False,
            ["vibeqc-queue", "remote", "--expected-sha", "b" * 40, "--force"],
            "--with-driver-runtime cannot be combined with --force",
            id="force",
        ),
        pytest.param(
            False,
            [
                "vibeqc-queue",
                "remote",
                "--expected-sha",
                "b" * 40,
                "--no-restart-daemon",
            ],
            "--with-driver-runtime cannot be combined with --no-restart-daemon",
            id="no-restart-daemon",
        ),
        pytest.param(
            False,
            [
                "vibeqc-queue",
                "remote",
                "--expected-sha",
                "b" * 40,
                "--cluster-install",
            ],
            "--with-driver-runtime cannot be combined with --cluster-install",
            id="cluster-install",
        ),
        pytest.param(
            False,
            [
                "vibeqc-queue",
                "remote",
                "--expected-sha",
                "b" * 40,
                "--drain-wait",
                "5m",
            ],
            "--with-driver-runtime cannot be combined with --drain-wait",
            id="drain-wait",
        ),
        pytest.param(
            False,
            [
                "vibeqc-queue",
                "remote",
                "--expected-sha",
                "b" * 40,
                "--staged-driver-archive-sha256",
                "a" * 64,
            ],
            "internal staged-driver identity cannot be combined",
            id="mixed-hidden-staged-identity",
        ),
    ],
)
def test_update_cli_rejects_unsafe_driver_runtime_modes_before_transport(
    monkeypatch: pytest.MonkeyPatch,
    scheduler_topology: bool,
    argv: list[str],
    error: str,
) -> None:
    cfg = _driver_update_runtime_config(scheduler_topology=scheduler_topology)
    monkeypatch.setattr(cli.config, "load_config", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "_resolve_admin_token",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        cli,
        "_forward_admin_command",
        lambda *args, **kwargs: pytest.fail("unsafe mode reached remote vq"),
    )
    monkeypatch.setattr(
        admin,
        "update_remote_managed_env_with_driver_runtime",
        lambda *args, **kwargs: pytest.fail("unsafe mode reached current driver"),
        raising=False,
    )

    result = CliRunner().invoke(
        cli.main,
        ["admin", "update", *argv, "--with-driver-runtime"],
    )

    assert result.exit_code == 2
    assert error in result.output
    assert "No such option" not in result.output


def _call_driver_runtime_update(
    cfg: config.Config,
    *,
    host: str = "remote",
    env: str = "vibeqc-queue",
    expected_sha: str = "b" * 40,
    expected_tag: str | None = "v0.15.160",
    remote_auth_args: tuple[str, ...] = ("--token-stdin",),
    stdin_data: str | None = "driver-secret\n",
    as_json: bool = True,
    update_script_args: tuple[str, ...] = ("--recreate-venv",),
    show_output: bool = True,
) -> str:
    return admin.update_remote_managed_env_with_driver_runtime(
        cfg,
        host,
        env=env,
        expected_sha=expected_sha,
        expected_tag=expected_tag,
        remote_auth_args=remote_auth_args,
        stdin_data=stdin_data,
        as_json=as_json,
        update_script_args=update_script_args,
        show_output=show_output,
        remote_timeout_env={
            "VQ_UPDATE_SCRIPT_TIMEOUT": "14400.0",
            "VQ_BUILD_STALL_TIMEOUT": "3600.0",
        },
        timeout=15000.0,
    )


@pytest.mark.parametrize(
    ("remote_auth_args", "stdin_data"),
    [
        pytest.param((), "secret\n", id="stdin-without-mode"),
        pytest.param(("--token-stdin",), None, id="token-stdin-without-input"),
        pytest.param(("--token-stdin",), "two\nlines\n", id="multi-line-token"),
        pytest.param(("--token-file", "relative-token"), None, id="relative-file"),
        pytest.param(
            ("--token-file", "/etc/vq/token"),
            "secret\n",
            id="file-with-stdin",
        ),
        pytest.param(("--token", "secret"), None, id="argv-token"),
        pytest.param(("--json",), None, id="foreign-option"),
    ],
)
def test_driver_runtime_update_rejects_malformed_auth_before_staging(
    monkeypatch: pytest.MonkeyPatch,
    remote_auth_args: tuple[str, ...],
    stdin_data: str | None,
) -> None:
    cfg = _driver_update_runtime_config()
    monkeypatch.setattr(
        admin,
        "_write_driver_recovery_archive",
        lambda path: pytest.fail("invalid auth reached archive staging"),
    )
    monkeypatch.setattr(
        admin.transport,
        "run_remote_shell",
        lambda *args, **kwargs: pytest.fail("invalid auth reached transport"),
    )

    with pytest.raises(admin.AdminError, match="auth|token|stdin"):
        _call_driver_runtime_update(
            cfg,
            remote_auth_args=remote_auth_args,
            stdin_data=stdin_data,
        )


def test_driver_runtime_update_requires_absolute_target_console_before_staging(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = _driver_update_runtime_config(remote_vq="vq")
    monkeypatch.setattr(
        admin,
        "_write_driver_recovery_archive",
        lambda path: pytest.fail("relative remote_vq reached archive staging"),
    )

    with pytest.raises(admin.AdminError, match="absolute configured remote_vq"):
        _call_driver_runtime_update(cfg)


def _patch_driver_runtime_transport(
    monkeypatch: pytest.MonkeyPatch,
    *,
    verified_sha: str = "a" * 64,
    mutation: subprocess.CompletedProcess[str] | BaseException | None = None,
) -> list[tuple[str, tuple[str, ...], dict[str, object]]]:
    archive_sha = "a" * 64
    calls: list[tuple[str, tuple[str, ...], dict[str, object]]] = []

    def write_archive(path: Path) -> str:
        path.write_bytes(b"exact current vq archive")
        return archive_sha

    def upload(
        host_cfg: config.HostConfig,
        local_path: Path,
        remote_path: str,
        **kwargs: object,
    ) -> None:
        calls.append(("upload", (str(local_path), remote_path), kwargs))

    def run_shell(
        host_cfg: config.HostConfig,
        *args: str,
        **kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        calls.append(("shell", args, kwargs))
        if args[0] in {"mkdir", "rmdir", "rm"}:
            return subprocess.CompletedProcess(args, 0, "", "")
        if args[0] == "sha256sum":
            return subprocess.CompletedProcess(
                args,
                0,
                f"{verified_sha}  {args[1]}\n",
                "",
            )
        assert args[0] == "/usr/bin/env"
        if isinstance(mutation, BaseException):
            raise mutation
        return mutation or subprocess.CompletedProcess(args, 0, "updated\n", "")

    monkeypatch.setattr(admin, "_write_driver_recovery_archive", write_archive)
    monkeypatch.setattr(admin.transport, "upload_file", upload)
    monkeypatch.setattr(admin.transport, "run_remote_shell", run_shell)
    return calls


def test_driver_runtime_update_verifies_archive_before_remote_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = _driver_update_runtime_config()
    calls = _patch_driver_runtime_transport(monkeypatch, verified_sha="c" * 64)

    with pytest.raises(admin.AdminError, match="archive digest mismatch"):
        _call_driver_runtime_update(cfg)

    assert [call[0] for call in calls] == [
        "shell",
        "upload",
        "shell",
        "shell",
        "shell",
    ]
    assert all(call[1][0] != "/usr/bin/env" for call in calls)


def test_driver_runtime_update_runs_exact_archive_then_cleans_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = _driver_update_runtime_config()
    archive_sha = "a" * 64
    calls = _patch_driver_runtime_transport(monkeypatch)

    result = _call_driver_runtime_update(cfg)

    assert result == "updated\n"
    assert [call[0] for call in calls] == [
        "shell",
        "upload",
        "shell",
        "shell",
        "shell",
        "shell",
    ]
    remote_archive = calls[2][1][1]
    assert remote_archive.startswith("vqscratch/vq-driver-recovery/")
    assert calls[3][1] == (
        "/usr/bin/env",
        "PYTHONDONTWRITEBYTECODE=1",
        "PYTHONNOUSERSITE=1",
        f"PYTHONPATH={remote_archive}",
        "VQ_BUILD_STALL_TIMEOUT=3600.0",
        "VQ_UPDATE_SCRIPT_TIMEOUT=14400.0",
        "/home/USER/vq/.venv/bin/vq",
        "admin",
        "update",
        "vibeqc-queue",
        "localhost",
        "--tag",
        "v0.15.160",
        "--expected-sha",
        "b" * 40,
        "--staged-driver-archive-sha256",
        archive_sha,
        "--update-script-arg",
        "--recreate-venv",
        "--show-output",
        "--json",
        "--token-stdin",
    )
    assert calls[3][2] == {
        "stdin_data": "driver-secret\n",
        "retry_transient": 0,
        "timeout": 15000.0,
    }
    assert calls[4][1] == ("rm", "-f", "--", remote_archive)
    assert calls[5][1] == (
        "rmdir",
        "--",
        remote_archive.rsplit("/", 1)[0],
    )


def test_driver_runtime_update_retains_archive_on_ambiguous_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = _driver_update_runtime_config()
    calls = _patch_driver_runtime_transport(
        monkeypatch,
        mutation=admin.transport.RemoteOutcomeUnknown("observer lost"),
    )

    with pytest.raises(
        admin.transport.RemoteOutcomeUnknown,
        match=r"retained at remote:vqscratch/vq-driver-recovery/",
    ):
        _call_driver_runtime_update(
            cfg,
            expected_tag=None,
            remote_auth_args=(),
            stdin_data=None,
            as_json=False,
        )

    assert [call[0] for call in calls] == ["shell", "upload", "shell", "shell"]


def test_driver_runtime_update_cleans_stage_after_known_remote_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = _driver_update_runtime_config()
    calls = _patch_driver_runtime_transport(
        monkeypatch,
        mutation=admin.transport.RemoteError("remote update rejected"),
    )

    with pytest.raises(admin.transport.RemoteError, match="remote update rejected"):
        _call_driver_runtime_update(cfg)

    assert [call[0] for call in calls] == [
        "shell",
        "upload",
        "shell",
        "shell",
        "shell",
        "shell",
    ]
    remote_archive = calls[2][1][1]
    assert calls[4][1] == ("rm", "-f", "--", remote_archive)
    assert calls[5][1] == ("rmdir", "--", remote_archive.rsplit("/", 1)[0])


def test_driver_runtime_update_uses_one_distinct_uuid_stage_per_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = _driver_update_runtime_config()
    calls = _patch_driver_runtime_transport(monkeypatch)
    stage_ids = iter([uuid.UUID(int=1), uuid.UUID(int=2)])
    monkeypatch.setattr(admin.uuid, "uuid4", lambda: next(stage_ids))

    assert _call_driver_runtime_update(cfg) == "updated\n"
    assert _call_driver_runtime_update(cfg) == "updated\n"

    uploads = [call for call in calls if call[0] == "upload"]
    assert [call[1][1] for call in uploads] == [
        "vqscratch/vq-driver-recovery/00000000000000000000000000000001/vq-driver.pyz",
        "vqscratch/vq-driver-recovery/00000000000000000000000000000002/vq-driver.pyz",
    ]


@pytest.mark.parametrize(
    ("field", "changed_value"),
    [
        pytest.param(
            "remote_vq",
            "/home/USER/replaced-vq/.venv/bin/vq",
            id="console-path",
        ),
        pytest.param(
            "admin_token_file",
            "/etc/vq/replaced-admin-token",
            id="target-token-file",
        ),
    ],
)
def test_driver_runtime_update_rejects_target_config_drift_before_mutation(
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    changed_value: str,
) -> None:
    cfg = _driver_update_runtime_config()
    calls = _patch_driver_runtime_transport(monkeypatch)

    def write_archive(path: Path) -> str:
        path.write_bytes(b"exact current vq archive")
        setattr(cfg.hosts["remote"], field, changed_value)
        return "a" * 64

    monkeypatch.setattr(admin, "_write_driver_recovery_archive", write_archive)

    with pytest.raises(admin.AdminError, match="config|target.*changed"):
        _call_driver_runtime_update(cfg)

    assert all(call[1][0] != "/usr/bin/env" for call in calls)


def test_staged_driver_update_identity_refuses_an_unstaged_runtime() -> None:
    result = CliRunner().invoke(
        cli.main,
        [
            "admin",
            "update",
            "missing-env",
            "localhost",
            "--expected-sha",
            "b" * 40,
            "--staged-driver-archive-sha256",
            "a" * 64,
        ],
    )

    assert result.exit_code == 2
    assert "integrity-checked zip-imported vq runtime" in result.output


@pytest.mark.parametrize(
    "mode",
    [
        pytest.param("wrong-identity", id="wrong-imported-archive-identity"),
        pytest.param("tampered-after-hash", id="archive-tampered-before-admission"),
    ],
)
def test_staged_driver_update_rejects_archive_identity_drift(
    tmp_path: Path,
    mode: str,
) -> None:
    archive = tmp_path / "driver.pyz"
    archive_sha256 = admin._write_driver_recovery_archive(archive)
    expected_sha256 = archive_sha256
    if mode == "wrong-identity":
        expected_sha256 = "c" * 64
    else:
        archive.write_bytes(archive.read_bytes() + b"post-hash tamper")
    environment = {
        **os.environ,
        "PYTHONPATH": str(archive),
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONNOUSERSITE": "1",
        "VQ_CONFIG_DIR": str(tmp_path / "config"),
        "VQ_STATE_DIR": str(tmp_path / "state"),
    }

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "vq.cli",
            "admin",
            "update",
            "missing-env",
            "localhost",
            "--expected-sha",
            "b" * 40,
            "--staged-driver-archive-sha256",
            expected_sha256,
            "--json",
        ],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 2
    assert "staged driver recovery archive changed" in result.stderr
    assert "unknown env" not in result.stderr


def test_driver_update_archive_is_the_imported_runtime(tmp_path: Path) -> None:
    archive = tmp_path / "driver.pyz"
    archive_sha256 = admin._write_driver_recovery_archive(archive)
    environment = {
        **os.environ,
        "PYTHONPATH": str(archive),
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONNOUSERSITE": "1",
        "VQ_CONFIG_DIR": str(tmp_path / "config"),
        "VQ_STATE_DIR": str(tmp_path / "state"),
    }

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "vq.cli",
            "admin",
            "update",
            "missing-env",
            "localhost",
            "--expected-sha",
            "b" * 40,
            "--staged-driver-archive-sha256",
            archive_sha256,
            "--json",
        ],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 2
    assert "unknown env 'missing-env'" in result.stderr
    assert "integrity-checked zip-imported vq runtime" not in result.stderr


# Exact deployed pre-#309 source recorded on #677. Missing history is a
# visible integration skip in shallow clones, never a synthetic legacy error.
LEGACY_DRIVER_SHA = "4786970dd34ee42faa974079ad0fd748bb48384f"


def _driver_update_process_fixture(
    tmp_path: Path,
) -> tuple[config.Config, Path, Path, Path, str, str, dict[str, str]]:
    """Install historical vq; simulate only the host service and build tools.

    Both consoles run unchanged production CLI/update/recovery code. Git,
    receipt writes, venv backup/replace, loader verification and installed
    package hashing are real. No fixture manufactures the legacy exception.
    """
    # This archives a legacy driver out of the monorepo's history, which
    # this repository does not have: it starts at the split commit.
    source_root = Path(__file__).resolve().parents[1]
    if subprocess.run(["git", "-C", str(source_root), "cat-file", "-e",
                       LEGACY_DRIVER_SHA], capture_output=True).returncode:
        pytest.skip("legacy driver SHA predates this repository's history")
    archived = subprocess.run(
        ["git", "-C", str(source_root), "archive", LEGACY_DRIVER_SHA,
         "vibe-queue/src", "vibe-queue/scripts", "vibe-queue/pyproject.toml"],
        capture_output=True,
    )
    if archived.returncode:
        pytest.skip(f"historical integration requires git object {LEGACY_DRIVER_SHA}")
    repo = tmp_path / "repo"
    repo.mkdir()
    with tarfile.open(fileobj=io.BytesIO(archived.stdout)) as archive:
        archive.extractall(repo, filter="data")
    _git(repo, "init", "--initial-branch", "main")
    _git(repo, "add", "vibe-queue")
    _git(repo, "commit", "-m", "historical vq 0.25.3")
    old_sha = _git(repo, "rev-parse", "HEAD")
    for folder in ("src", "scripts"):
        shutil.rmtree(repo / "vibe-queue" / folder)
        shutil.copytree(
            source_root / "vibe-queue" / folder, repo / "vibe-queue" / folder,
            ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
        )
    _git(repo, "add", "vibe-queue")
    _git(repo, "commit", "-m", "current vq")
    new_sha = _git(repo, "rev-parse", "HEAD")
    _git(repo, "tag", "new-target")
    origin = tmp_path / "origin.git"
    origin.mkdir()
    _git(origin, "init", "--bare")
    _git(repo, "remote", "add", "origin", str(origin))
    _git(repo, "push", "--set-upstream", "origin", "main", "--tags")
    _git(repo, "reset", "--hard", old_sha)
    # #677's live driver also had a lagging mirror upstream. The accepted tag
    # exists only at canonical origin: the staged driver must fetch it there,
    # independently of the old console's service-identity failure.
    mirror = tmp_path / "lagging-mirror.git"
    mirror.mkdir()
    _git(mirror, "init", "--bare")
    _git(repo, "remote", "add", "mirror", str(mirror))
    _git(repo, "push", "mirror", f"{old_sha}:refs/heads/main")
    _git(repo, "config", "branch.main.remote", "mirror")
    _git(repo, "tag", "--delete", "new-target")
    assert _git(repo, "ls-remote", "mirror", "refs/tags/new-target") == ""
    assert _git(repo, "ls-remote", "origin", "refs/tags/new-target").startswith(new_sha)
    venv = tmp_path / "managed-venv"
    config_root = tmp_path / "remote-config"
    state_root = tmp_path / "remote-state"
    config_root.mkdir()
    state_root.mkdir()
    console = venv / "bin" / "vq"
    (config_root / "config.toml").write_text(
        '\n'.join([
            'default_host = "localhost"', '[hosts.localhost]', 'ssh = "localhost"',
            f'remote_vq = {json.dumps(str(console))}', '[programs.vibeqc-queue]',
            'kind = "venv"', f'python = {json.dumps(str(venv / "bin/python"))}',
            f'git_dir = {json.dumps(str(repo))}', 'branch = "main"',
            'update_script = "vibe-queue/scripts/update.sh"',
        ]), encoding="utf-8",
    )
    # The installer is a test build-tool boundary. It copies actual historical
    # or selected-current package bytes into a real venv at its final path.
    installer = tmp_path / "install_runtime.py"
    service_hook = f"import runpy; runpy.run_path({str(tmp_path / 'host_tools.py')!r})\n"
    installer.write_text(
        "import pathlib, shutil, sys, venv\n"
        f"root = pathlib.Path({str(tmp_path)!r})\n"
        "target = root / 'managed-venv'\n"
        "venv.EnvBuilder(with_pip=False).create(target)\n"
        "site = next(target.glob('lib/python*/site-packages'))\n"
        f"deps = {[str(x) for x in sys.path if 'site-packages' in str(x)]!r}\n"
        "(site / 'test-dependencies.pth').write_text('\\n'.join(deps) + '\\n')\n"
        "shutil.copytree(root / 'repo/vibe-queue/src/vq', site / 'vq')\n"
        "(target / '.vq-install-metadata').write_text('version=1\\nextras=core\\neditable=0\\n')\n"
        "(target / 'bin/vq').write_text('#!' + str(target / 'bin/python') + "
        "'\\nfrom vq.cli import main\\nmain()\\n')\n"
        "(target / 'bin/vq').chmod(0o755)\n"
        f"(site / 'z-test-service.pth').write_text({service_hook!r})\n",
        encoding="utf-8",
    )
    host_tools = tmp_path / "host_tools.py"
    host_tools.write_text(
        "import json, os, pathlib, subprocess, sys\n"
        "from vq import admin, config, rpc\n"
        f"root = pathlib.Path({str(tmp_path)!r})\n"
        "serving = root / 'service-running'\n"
        "console = root / 'managed-venv/bin/vq'\n"
        "def query(manager):\n"
        "    running = serving.exists()\n"
        "    raw = ('{ path=' + str(console) + ' ; argv[]=' + str(console) + "
        "' daemon run ; ignore_errors=no ; start_time=[start] ; stop_time=' + "
        "('[n/a]' if running else '[stop]') + ' ; pid=1819 ; code=exited ; status=0 }')\n"
        "    return admin._DaemonServiceState(manager=manager, running=running, "
        "pid=1819 if running else None, executable=str(console), "
        "diagnostic='test systemd adapter', command_identity=('systemd-execstart', raw))\n"
        "def service(argv, **kwargs):\n"
        "    if 'stop' in argv: serving.unlink(missing_ok=True)\n"
        "    else: serving.write_text('running')\n"
        "    return True, 'test service command completed'\n"
        "def build(*args, **kwargs):\n"
        f"    p = subprocess.run([{sys.executable!r}, str(root / 'install_runtime.py')], "
        "capture_output=True, text=True)\n"
        "    return p.returncode, p.stdout + p.stderr\n"
        "def ping():\n"
        "    if not serving.exists(): return None\n"
        "    sha = subprocess.check_output(['git', '-C', str(root / 'repo'), "
        "'rev-parse', 'HEAD'], text=True).strip()\n"
        "    return {'source_sha': sha, 'source_tree_sha256': "
        "admin._installed_tree_digest(str(root / 'managed-venv/bin/python')), "
        "'multi_user': False}\n"
        "admin._query_daemon_service_state = query\n"
        "admin._run_daemon_service_command = service\n"
        "admin._wait_for_managed_daemon_quiescence = lambda *a: "
        "(not serving.exists(), 'test service stopped')\n"
        "admin._detect_vq_self_update = lambda prog: admin._SelfUpdateProbe("
        "is_self_update=True, daemon_running=serving.exists(), service_manager='systemd', "
        "manager_available=True, diagnostic='test systemd adapter')\n"
        "admin._run_update_script = build\n"
        "rpc.ping_user_daemon = ping\n",
        encoding="utf-8",
    )
    installed = subprocess.run([sys.executable, str(installer)], capture_output=True, text=True)
    assert installed.returncode == 0, installed.stderr
    (tmp_path / "service-running").write_text("running")
    remote_env = {
        **os.environ,
        "VQ_CONFIG_DIR": str(config_root), "VQ_STATE_DIR": str(state_root),
        "PYTHONDONTWRITEBYTECODE": "1", "VQ_DAEMON_HEALTH_TIMEOUT": "1",
    }
    remote_env.pop("PYTHONPATH", None)
    probe = subprocess.run(
        [str(venv / "bin/python"), "-c",
         "from vq import admin; print(admin._detect_vq_self_update.__name__)"],
        env=remote_env, capture_output=True, text=True,
    )
    assert "<lambda>" in probe.stdout, probe.stdout + probe.stderr
    cfg = _driver_update_runtime_config(remote_vq=str(console))
    return cfg, repo, console, state_root, old_sha, new_sha, remote_env


def _patch_local_driver_update_transport(
    monkeypatch: pytest.MonkeyPatch,
    *,
    remote_root: Path,
    remote_env: dict[str, str],
) -> list[tuple[str, tuple[str, ...]]]:
    calls: list[tuple[str, tuple[str, ...]]] = []

    def remote_path(raw: str) -> Path:
        path = Path(raw)
        return path if path.is_absolute() else remote_root / path

    def upload(
        host_cfg: config.HostConfig,
        local_path: Path,
        destination: str,
        **kwargs: object,
    ) -> None:
        del host_cfg, kwargs
        calls.append(("upload", (str(local_path), destination)))
        target = remote_path(destination)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(local_path, target)

    def run_shell(
        host_cfg: config.HostConfig,
        *args: str,
        **kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        del host_cfg
        calls.append(("shell", args))
        if args[0] == "mkdir":
            remote_path(args[-1]).mkdir(parents=True, exist_ok=True)
            return subprocess.CompletedProcess(args, 0, "", "")
        if args[0] == "sha256sum":
            path = remote_path(args[1])
            return subprocess.CompletedProcess(
                args,
                0,
                f"{admin._sha256_file(path)}  {args[1]}\n",
                "",
            )
        if args[0] == "rm":
            remote_path(args[-1]).unlink(missing_ok=True)
            return subprocess.CompletedProcess(args, 0, "", "")
        if args[0] == "rmdir":
            remote_path(args[-1]).rmdir()
            return subprocess.CompletedProcess(args, 0, "", "")
        assert args[0] == "/usr/bin/env"
        completed = subprocess.run(
            list(args),
            cwd=remote_root,
            env=remote_env,
            input=kwargs.get("stdin_data"),
            capture_output=True,
            text=True,
            check=False,
        )
        if completed.returncode != 0:
            raise admin.transport.RemoteError(completed.stderr.strip() or "remote update failed")
        return completed

    monkeypatch.setattr(admin.transport, "upload_file", upload)
    monkeypatch.setattr(admin.transport, "run_remote_shell", run_shell)
    return calls


def test_staged_driver_update_preserves_preexisting_marker_before_git_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (
        _cfg,
        repo,
        console,
        state_root,
        old_sha,
        new_sha,
        remote_env,
    ) = _driver_update_process_fixture(tmp_path)
    marker_path = state_root / admin.ADMIN_UPDATE_MARKER_FILENAME
    monkeypatch.setenv("VQ_STATE_DIR", str(state_root))
    admin._set_owned_admin_update_marker_path(None)
    admin.acquire_admin_update_marker(
        envs=["vibeqc-queue"],
        host="localhost",
    )
    marker_bytes = marker_path.read_bytes()
    admin._set_owned_admin_update_marker_path(None)
    archive = tmp_path / "current-driver.pyz"
    archive_sha256 = admin._write_driver_recovery_archive(archive)
    environment = {
        **remote_env,
        "PYTHONPATH": str(archive),
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONNOUSERSITE": "1",
    }
    # The driver executes the staged archive through the installed console:
    # the managed venv's interpreter, and with it the fixture's site hook,
    # supplies the host service adapter while PYTHONPATH supplies vq. Running
    # pytest's own interpreter here would skip that hook, so the real service
    # probe would run and, on a host with neither launchd nor user systemd
    # (the CI container), fail closed with a usage error before the marker
    # check this test is about. Prove the hook binds the archive-imported
    # module before trusting the refusal below.
    hooked = subprocess.run(
        [
            str(console.parent / "python"),
            "-c",
            "import vq; from vq import admin; "
            "print(vq.__file__); print(admin._detect_vq_self_update.__name__)",
        ],
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    assert hooked.returncode == 0, hooked.stderr
    assert hooked.stdout.splitlines() == [
        str(archive / "vq" / "__init__.py"),
        "<lambda>",
    ], hooked.stdout + hooked.stderr

    result = subprocess.run(
        [
            str(console),
            "admin",
            "update",
            "vibeqc-queue",
            "localhost",
            "--tag",
            "new-target",
            "--expected-sha",
            new_sha,
            "--staged-driver-archive-sha256",
            archive_sha256,
            "--json",
        ],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )

    # A pre-existing marker is a state refusal: exit 1 with a plain
    # ``Error:`` line, never the exit-2 usage banner the service-provenance
    # or argv failures produce. Only the marker guard emits this sentence.
    assert result.returncode == 1, result.stdout + result.stderr
    assert "Usage:" not in result.stderr
    assert (
        "admin-update-in-progress marker present and conflicts with "
        "envs=['vibeqc-queue'], host=localhost" in result.stderr
    ), result.stderr
    assert result.stdout == ""
    assert marker_path.read_bytes() == marker_bytes
    assert _git(repo, "rev-parse", "HEAD") == old_sha
    assert (tmp_path / "service-running").exists()
    assert not list(tmp_path.glob(".managed-venv.vq-admin-backup-*"))


def test_current_driver_converges_after_legacy_console_managed_update_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (
        cfg,
        repo,
        old_console,
        state_root,
        old_sha,
        new_sha,
        remote_env,
    ) = _driver_update_process_fixture(tmp_path)
    marker_path = state_root / admin.ADMIN_UPDATE_MARKER_FILENAME
    legacy_env = dict(remote_env)
    legacy_env.pop("PYTHONPATH", None)
    legacy = subprocess.run(
        [
            str(old_console),
            "admin",
            "update",
            "vibeqc-queue",
            "localhost",
            "--tag",
            "new-target",
            "--expected-sha",
            new_sha,
            "--json",
        ],
        cwd=tmp_path,
        env=legacy_env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert legacy.returncode == 2, legacy.stdout + legacy.stderr
    assert "serving daemon command changed during stop" in legacy.stderr
    assert marker_path.exists()
    assert _git(repo, "rev-parse", "HEAD") == old_sha

    calls = _patch_local_driver_update_transport(
        monkeypatch,
        remote_root=tmp_path,
        remote_env=remote_env,
    )

    marker_id = json.loads(marker_path.read_text())["marker_id"]
    admin.recover_remote_managed_update_with_driver_runtime(
        cfg, "remote", marker_id=marker_id, remote_auth_args=(),
        stdin_data=None, as_json=True,
    )
    assert not marker_path.exists()
    calls.clear()
    output = _call_driver_runtime_update(
        cfg,
        expected_sha=new_sha,
        expected_tag="new-target",
        remote_auth_args=(),
        stdin_data=None,
        as_json=True,
        update_script_args=(),
        show_output=False,
    )

    payload = json.loads(output)
    assert payload["success"] is True
    assert payload["actual_sha"] == new_sha
    assert _git(repo, "rev-parse", "HEAD") == new_sha
    assert not marker_path.exists(), "current-driver update re-armed the receipt"
    mutating_calls = [args for kind, args in calls if kind == "shell" and args[0] == "/usr/bin/env"]
    assert len(mutating_calls) == 1
    assert not list((tmp_path / "vqscratch" / "vq-driver-recovery").glob("*/vq-driver.pyz"))


@pytest.mark.parametrize(
    "argv",
    [
        pytest.param(
            ["--quarantine-orphaned-receipt", "--marker-id", "a" * 32],
            id="missing-bound-fields",
        ),
        pytest.param(["--dry-run"], id="dry-run-without-quarantine"),
        pytest.param(["--reason", "x"], id="reason-without-quarantine"),
    ],
)
def test_recover_update_cli_rejects_mixed_quarantine_modes(argv: list[str]) -> None:
    result = CliRunner().invoke(
        cli.main, ["admin", "recover-update", "localhost", *argv],
    )
    assert result.exit_code == 2


def test_recovery_retains_terminal_receipt_when_resume_proof_is_partial(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Recovered files/service are not enough to disarm paused-job evidence."""
    prog, old_sha, new_sha, venv, backup = _fixture(tmp_path)
    lifecycle = _lifecycle(prog, old_sha, venv, backup)
    lifecycle.receipt_phase = "old_restored"
    lifecycle.backup_moved = False
    admin.shutil.rmtree(backup)
    (venv / "identity.txt").write_text("old\n", encoding="utf-8")
    # Receipt was armed while the host used the multi-user queue namespace;
    # recovery below intentionally receives today's default single-user config.
    marker_path = _arm_receipt(prog, lifecycle, multi_user=True)
    _patch_common_completion(monkeypatch, prog, old_sha, new_sha)
    monkeypatch.setattr(
        admin,
        "_verify_restarted_daemon",
        lambda *args, **kwargs: admin.DaemonProvenance(
            verified=True,
            actual_sha=old_sha,
            actual_tree_sha256=OLD_TREE,
            detail="old RPC identity verified",
        ),
    )

    class PartialResumeProof:
        summary = "resumed 1 job; durable token scope NOT clear (1 unresolved)"

        @staticmethod
        def require_clear() -> None:
            raise PauseError("job two still SUSPENDED")

    resume_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    def partial_resume(*args: object, **kwargs: object) -> PartialResumeProof:
        resume_calls.append((args, kwargs))
        return PartialResumeProof()

    monkeypatch.setattr(admin, "resume_token_scope_with_proof", partial_resume)
    marker = admin.read_admin_update_marker()
    assert marker is not None
    marker.pid = 999_999
    marker.pid_start_time = 0
    admin._write_admin_update_marker_atomic(marker)
    admin._set_owned_admin_update_marker_path(None)
    monkeypatch.setattr(admin, "_pid_liveness", lambda unused: False)

    recovered = admin.recover_managed_update(
        config.Config(programs={"vibeqc-queue": prog}),
        marker_id=marker.marker_id,
    )

    assert recovered.recovered is False
    assert "NOT clear" in recovered.resumed_summary
    assert resume_calls == [
        (
            ("localhost", "admin-update-0123456789ab"),
            {
                "multi_user": True,
                "queue_root": paths.multi_user_root().resolve(),
            },
        )
    ]
    retained = admin.read_admin_update_marker()
    assert retained is not None
    assert retained.state == admin.ADMIN_UPDATE_STATE_FAILED
    assert "paused jobs remain" in (retained.failure_reason or "")
    receipt = _managed_receipt(marker_path)
    assert receipt is not None
    assert receipt["phase"] == "old_restored"


def test_managed_legacy_pause_scope_without_namespace_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An old token-bearing receipt is retained; recovery never guesses mode."""
    prog, old_sha, _new_sha, venv, backup = _fixture(tmp_path)
    lifecycle = _lifecycle(prog, old_sha, venv, backup)
    marker_path = _arm_receipt(prog, lifecycle)
    payload = json.loads(marker_path.read_text(encoding="utf-8"))
    payload.pop("pause_multi_user")
    payload.pop("pause_queue_root")
    payload["pid"] = 999_999
    payload["pid_start_time"] = 0
    marker_path.write_text(json.dumps(payload), encoding="utf-8")
    marker = admin.read_admin_update_marker()
    assert marker is not None
    admin._set_owned_admin_update_marker_path(None)
    monkeypatch.setattr(admin, "_pid_liveness", lambda unused: False)

    with pytest.raises(admin.AdminError, match="queue namespace"):
        admin.recover_managed_update(
            config.Config(programs={"vibeqc-queue": prog}),
            marker_id=marker.marker_id,
        )

    assert marker_path.exists()
    retained = admin.read_admin_update_marker()
    assert retained is not None
    assert retained.managed_transaction is not None


def test_failed_recovery_retains_latest_phase_and_retry_succeeds(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A post-restore failure may not rewrite the receipt to its entry phase.

    Recovery persists ``restoring_old``/``files_restored`` around the
    destructive backup swap.  Rewriting the pre-recovery ``backup_moved``
    snapshot after a later start failure makes the absent backup impossible to
    reconcile on retry.  Pin both the newest checkpoint and idempotent retry.
    """
    prog, old_sha, new_sha, venv, backup = _fixture(tmp_path)
    lifecycle = _lifecycle(prog, old_sha, venv, backup)
    marker_path = _arm_receipt(prog, lifecycle)
    _git(Path(prog.git_dir), "checkout", "--detach", new_sha)
    _patch_common_completion(monkeypatch, prog, old_sha, new_sha)
    _patch_clear_resume(monkeypatch)
    starts = 0

    def start_old(unused: admin._ManagedDaemonUpdate) -> tuple[bool, str]:
        nonlocal starts
        starts += 1
        if starts == 1:
            return False, "injected old-service start failure"
        return True, "old service started on retry"

    monkeypatch.setattr(admin, "_start_managed_daemon_update", start_old)
    monkeypatch.setattr(
        admin,
        "_verify_restarted_daemon",
        lambda *args, **kwargs: admin.DaemonProvenance(
            verified=True,
            actual_sha=old_sha,
            actual_tree_sha256=OLD_TREE,
            detail="old RPC identity verified",
        ),
    )
    marker = _mark_receipt_stale(monkeypatch)

    first = admin.recover_managed_update(
        config.Config(programs={"vibeqc-queue": prog}),
        marker_id=marker.marker_id,
    )

    assert first.recovered is False
    receipt = _managed_receipt(marker_path)
    assert receipt is not None
    assert receipt["phase"] == "files_restored"
    assert receipt["backup_moved"] is False
    assert not backup.exists()
    assert (venv / "identity.txt").read_text(encoding="utf-8") == "old\n"

    retried_marker = _mark_receipt_stale(monkeypatch)
    second = admin.recover_managed_update(
        config.Config(programs={"vibeqc-queue": prog}),
        marker_id=retried_marker.marker_id,
    )

    assert second.recovered is True
    assert starts == 2
    assert not admin.admin_update_marker_exists()


def test_crash_after_old_venv_restore_rename_is_durably_recoverable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Death after backup->venv must not make the receipt unrecoverable."""
    prog, old_sha, new_sha, venv, backup = _fixture(tmp_path)
    lifecycle = _lifecycle(prog, old_sha, venv, backup)
    marker_path = _arm_receipt(prog, lifecycle)
    (venv / "identity.txt").write_text("bad\n", encoding="utf-8")
    _git(Path(prog.git_dir), "checkout", "--detach", new_sha)
    result = _target_result(prog, new_sha)
    _patch_common_completion(monkeypatch, prog, old_sha, new_sha)
    starts: list[str] = []

    def start_old(unused: admin._ManagedDaemonUpdate) -> tuple[bool, str]:
        starts.append((venv / "identity.txt").read_text(encoding="utf-8").strip())
        return True, "old service started"

    monkeypatch.setattr(admin, "_start_managed_daemon_update", start_old)

    def verify_old(*args: object, **kwargs: object) -> admin.DaemonProvenance:
        running = starts == ["old"]
        return admin.DaemonProvenance(
            verified=running,
            actual_sha=old_sha if running else None,
            actual_tree_sha256=OLD_TREE if running else None,
            detail="old RPC identity verified" if running else "daemon is stopped",
        )

    monkeypatch.setattr(
        admin,
        "_verify_restarted_daemon",
        verify_old,
    )
    real_replace = admin.os.replace

    def die_after_restore(
        source: str | os.PathLike[str],
        target: str | os.PathLike[str],
    ) -> None:
        real_replace(source, target)
        if Path(source) == backup and Path(target) == venv:
            raise _SimulatedProcessDeath

    monkeypatch.setattr(admin.os, "replace", die_after_restore)
    with pytest.raises(_SimulatedProcessDeath):
        admin._complete_managed_daemon_update(prog, result, lifecycle)

    assert (venv / "identity.txt").read_text(encoding="utf-8") == "old\n"
    assert not backup.exists()
    receipt = _managed_receipt(marker_path)
    assert receipt is not None
    monkeypatch.setattr(admin.os, "replace", real_replace)
    monkeypatch.setattr(admin, "_pid_liveness", lambda unused: False)

    recovered = admin.recover_managed_update(
        config.Config(programs={"vibeqc-queue": prog}),
    )

    assert recovered.recovered is True
    assert starts == ["old"]
    assert _git(Path(prog.git_dir), "rev-parse", "HEAD") == old_sha
    assert _git(Path(prog.git_dir), "rev-parse", "--abbrev-ref", "HEAD") == "main"
    assert not list(tmp_path.glob(".managed-venv.vq-admin-failed-*"))
    assert not admin.admin_update_marker_exists()


def test_crash_after_target_commit_receipt_finishes_backup_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Death after commit intent but before disarm must not orphan backup."""
    prog, old_sha, new_sha, venv, backup = _fixture(tmp_path)
    lifecycle = _lifecycle(prog, old_sha, venv, backup)
    marker_path = _arm_receipt(prog, lifecycle)
    _git(Path(prog.git_dir), "checkout", "--detach", new_sha)
    result = _target_result(prog, new_sha)
    _patch_common_completion(monkeypatch, prog, old_sha, new_sha)
    monkeypatch.setattr(
        admin,
        "_start_managed_daemon_update",
        lambda unused: (True, "target service started"),
    )
    monkeypatch.setattr(
        admin,
        "_verify_restarted_daemon",
        lambda *args, **kwargs: admin.DaemonProvenance(
            verified=True,
            actual_sha=new_sha,
            actual_tree_sha256=NEW_TREE,
            detail="target RPC identity verified",
        ),
    )
    real_replace = admin.os.replace

    def die_before_commit_rename(
        source: str | os.PathLike[str],
        target: str | os.PathLike[str],
    ) -> None:
        if Path(source) == backup:
            raise _SimulatedProcessDeath
        real_replace(source, target)

    monkeypatch.setattr(admin.os, "replace", die_before_commit_rename)
    with pytest.raises(_SimulatedProcessDeath):
        admin._complete_managed_daemon_update(prog, result, lifecycle)

    receipt = _managed_receipt(marker_path)
    assert receipt is not None
    assert receipt["phase"] == "target_committed"
    assert backup.exists()
    monkeypatch.setattr(admin.os, "replace", real_replace)
    monkeypatch.setattr(admin, "_pid_liveness", lambda unused: False)

    recovered = admin.recover_managed_update(
        config.Config(programs={"vibeqc-queue": prog}),
    )

    assert recovered.recovered is True
    assert (venv / "identity.txt").read_text(encoding="utf-8") == "new\n"
    assert not backup.exists()
    assert not list(tmp_path.glob(".managed-venv.vq-admin-committed-*"))
    assert not admin.admin_update_marker_exists()


def test_crash_after_target_backup_disarm_cleans_exact_committed_tree(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The commit rename may not create an anonymous crash orphan."""
    prog, old_sha, new_sha, venv, backup = _fixture(tmp_path)
    lifecycle = _lifecycle(prog, old_sha, venv, backup)
    marker_path = _arm_receipt(prog, lifecycle)
    _git(Path(prog.git_dir), "checkout", "--detach", new_sha)
    result = _target_result(prog, new_sha)
    _patch_common_completion(monkeypatch, prog, old_sha, new_sha)
    monkeypatch.setattr(
        admin,
        "_start_managed_daemon_update",
        lambda unused: (True, "target service started"),
    )
    monkeypatch.setattr(
        admin,
        "_verify_restarted_daemon",
        lambda *args, **kwargs: admin.DaemonProvenance(
            verified=True,
            actual_sha=new_sha,
            actual_tree_sha256=NEW_TREE,
            detail="target RPC identity verified",
        ),
    )
    real_replace = admin.os.replace

    def die_after_commit_rename(
        source: str | os.PathLike[str],
        target: str | os.PathLike[str],
    ) -> None:
        real_replace(source, target)
        if Path(source) == backup:
            raise _SimulatedProcessDeath

    monkeypatch.setattr(admin.os, "replace", die_after_commit_rename)
    with pytest.raises(_SimulatedProcessDeath):
        admin._complete_managed_daemon_update(prog, result, lifecycle)

    receipt = _managed_receipt(marker_path)
    assert receipt is not None
    assert receipt["phase"] == "target_committed"
    assert not backup.exists()
    assert list(tmp_path.glob(".managed-venv.vq-admin-committed-*"))
    monkeypatch.setattr(admin.os, "replace", real_replace)
    monkeypatch.setattr(admin, "_pid_liveness", lambda unused: False)

    recovered = admin.recover_managed_update(
        config.Config(programs={"vibeqc-queue": prog}),
    )

    assert recovered.recovered is True
    assert (venv / "identity.txt").read_text(encoding="utf-8") == "new\n"
    assert not list(tmp_path.glob(".managed-venv.vq-admin-committed-*"))
    assert not admin.admin_update_marker_exists()


@pytest.mark.parametrize("attached_branch", ["main", None])
def test_failed_immutable_selector_restores_checkout_attachment_and_cleans(
    attached_branch: str | None,
    tmp_path: Path,
) -> None:
    """Rollback preserves whether the old serving checkout was attached."""
    prog, old_sha, new_sha, _venv, _backup = _fixture(tmp_path)
    repo = Path(prog.git_dir)
    if attached_branch is None:
        _git(repo, "checkout", "--detach", old_sha)
    _git(repo, "checkout", "--detach", new_sha)
    (repo / "generated-after-selector.tmp").write_text(
        "partial build output\n",
        encoding="utf-8",
    )
    result = _target_result(prog, new_sha)
    result.pre_update_sha = old_sha
    result.pre_update_branch = attached_branch
    result.work_errors.append("synthetic immutable build failure")

    admin._finalize_immutable_checkout(result, prog, repo, None)

    assert result.rolled_back is True
    assert _git(repo, "rev-parse", "HEAD") == old_sha
    expected_branch = attached_branch if attached_branch is not None else "HEAD"
    assert _git(repo, "rev-parse", "--abbrev-ref", "HEAD") == expected_branch
    assert _git(repo, "status", "--porcelain") == ""
    assert not (repo / "generated-after-selector.tmp").exists()


def test_checkout_restore_preserves_custom_branch_upstream(
    tmp_path: Path,
) -> None:
    """Rollback must not rewrite a branch's custom tracking destination."""
    prog, old_sha, new_sha, _venv, _backup = _fixture(tmp_path)
    repo = Path(prog.git_dir)
    _git(repo, "push", "origin", f"{new_sha}:refs/heads/release-track")
    _git(
        repo,
        "branch",
        "--set-upstream-to",
        "origin/release-track",
        "main",
    )
    upstream_before = _git(
        repo,
        "for-each-ref",
        "--format=%(upstream:short)",
        "refs/heads/main",
    )
    assert upstream_before == "origin/release-track"
    _git(repo, "checkout", "--detach", new_sha)
    _git(repo, "branch", "-f", "main", new_sha)

    rc, _output = admin._restore_checkout_state(
        repo,
        sha=old_sha,
        branch="main",
    )

    assert rc == 0
    assert _git(repo, "rev-parse", "HEAD") == old_sha
    assert _git(repo, "rev-parse", "--abbrev-ref", "HEAD") == "main"
    assert _git(
        repo,
        "for-each-ref",
        "--format=%(upstream:short)",
        "refs/heads/main",
    ) == upstream_before


def test_checkout_restore_supports_local_only_branch_without_upstream(
    tmp_path: Path,
) -> None:
    """A rollback branch need not exist on origin or acquire an upstream."""
    prog, old_sha, new_sha, _venv, _backup = _fixture(tmp_path)
    repo = Path(prog.git_dir)
    _git(repo, "checkout", "-b", "local-rollback", old_sha)
    assert _git(
        repo,
        "for-each-ref",
        "--format=%(upstream:short)",
        "refs/heads/local-rollback",
    ) == ""
    assert _git(repo, "ls-remote", "--heads", "origin", "local-rollback") == ""
    _git(repo, "checkout", "--detach", new_sha)

    rc, _output = admin._restore_checkout_state(
        repo,
        sha=old_sha,
        branch="local-rollback",
    )

    assert rc == 0
    assert _git(repo, "rev-parse", "HEAD") == old_sha
    assert _git(repo, "rev-parse", "--abbrev-ref", "HEAD") == "local-rollback"
    assert _git(
        repo,
        "for-each-ref",
        "--format=%(upstream:short)",
        "refs/heads/local-rollback",
    ) == ""
    assert _git(repo, "ls-remote", "--heads", "origin", "local-rollback") == ""


def test_checkout_restore_cleans_untracked_collision_before_reattach(
    tmp_path: Path,
) -> None:
    """Target output may collide with a path tracked on the old branch."""
    prog, old_sha, _new_sha, _venv, _backup = _fixture(tmp_path)
    repo = Path(prog.git_dir)
    collision = repo / "generated" / "tracked-on-old.txt"
    collision.parent.mkdir()
    collision.write_text("baseline tracked content\n", encoding="utf-8")
    _git(repo, "add", str(collision.relative_to(repo)))
    _git(repo, "commit", "-m", "track baseline collision path")
    baseline_sha = _git(repo, "rev-parse", "HEAD")
    _git(repo, "checkout", "--detach", old_sha)
    collision.parent.mkdir()
    collision.write_text("untracked target build output\n", encoding="utf-8")

    rc, _output = admin._restore_checkout_state(
        repo,
        sha=baseline_sha,
        branch="main",
    )

    assert rc == 0
    assert _git(repo, "rev-parse", "HEAD") == baseline_sha
    assert _git(repo, "rev-parse", "--abbrev-ref", "HEAD") == "main"
    assert collision.read_text(encoding="utf-8") == "baseline tracked content\n"
    assert _git(repo, "status", "--porcelain") == ""


def test_checkout_restore_cleans_only_after_restoring_baseline_ignore_rules(
    tmp_path: Path,
) -> None:
    """Target ignore drift cannot delete a baseline-ignored restored venv."""
    prog, _old_sha, _new_sha, _venv, _backup = _fixture(tmp_path)
    repo = Path(prog.git_dir)
    ignore = repo / ".gitignore"
    ignore.write_text("/.restored-venv/\n", encoding="utf-8")
    _git(repo, "add", ".gitignore")
    _git(repo, "commit", "-m", "ignore managed serving venv")
    baseline_sha = _git(repo, "rev-parse", "HEAD")
    ignore.unlink()
    _git(repo, "add", "-u")
    _git(repo, "commit", "-m", "target removes old venv ignore")
    target_sha = _git(repo, "rev-parse", "HEAD")
    _git(repo, "checkout", "--detach", target_sha)
    _git(repo, "branch", "-f", "main", baseline_sha)
    restored_venv = repo / ".restored-venv"
    restored_venv.mkdir()
    (restored_venv / "identity.txt").write_text("sole old venv\n", encoding="utf-8")

    rc, output = admin._restore_checkout_state(
        repo,
        sha=baseline_sha,
        branch="main",
    )

    assert rc == 0, output
    assert _git(repo, "rev-parse", "HEAD") == baseline_sha
    assert _git(repo, "rev-parse", "--abbrev-ref", "HEAD") == "main"
    assert (restored_venv / "identity.txt").read_text(
        encoding="utf-8",
    ) == "sole old venv\n"
    assert _git(repo, "status", "--porcelain") == ""


def test_durable_restore_cleans_collision_before_original_branch_attachment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Managed crash rollback uses the same collision-safe restoration order."""
    prog, _old_sha, new_sha, venv, backup = _fixture(tmp_path)
    repo = Path(prog.git_dir)
    collision = repo / "generated" / "tracked-on-old.txt"
    collision.parent.mkdir()
    collision.write_text("baseline tracked content\n", encoding="utf-8")
    _git(repo, "add", str(collision.relative_to(repo)))
    _git(repo, "commit", "-m", "track durable collision path")
    baseline_sha = _git(repo, "rev-parse", "HEAD")
    lifecycle = _lifecycle(prog, baseline_sha, venv, backup)
    _arm_receipt(prog, lifecycle)
    (venv / "identity.txt").write_text("bad\n", encoding="utf-8")
    _git(repo, "checkout", "--detach", new_sha)
    collision.parent.mkdir()
    collision.write_text("untracked failed-target output\n", encoding="utf-8")
    _patch_common_completion(monkeypatch, prog, baseline_sha, new_sha)
    monkeypatch.setattr(
        admin,
        "_start_managed_daemon_update",
        lambda unused: (True, "old service started"),
    )
    monkeypatch.setattr(
        admin,
        "_verify_restarted_daemon",
        lambda *args, **kwargs: admin.DaemonProvenance(
            verified=True,
            actual_sha=baseline_sha,
            actual_tree_sha256=OLD_TREE,
            detail="old RPC identity verified",
        ),
    )

    recovered, detail = admin._recover_managed_daemon_after_exception(
        prog,
        lifecycle,
        clear_receipt=False,
    )

    assert recovered is True, detail
    assert _git(repo, "rev-parse", "HEAD") == baseline_sha
    assert _git(repo, "rev-parse", "--abbrev-ref", "HEAD") == "main"
    assert collision.read_text(encoding="utf-8") == "baseline tracked content\n"
    assert _git(repo, "status", "--porcelain") == ""


def test_durable_restore_preserves_in_checkout_venv_after_target_ignore_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Managed recovery never cleans the sole old venv under target ignores."""
    original, _old_sha, _new_sha, external_venv, external_backup = _fixture(tmp_path)
    repo = Path(original.git_dir)
    admin.shutil.rmtree(external_venv)
    admin.shutil.rmtree(external_backup)
    ignore = repo / ".gitignore"
    ignore.write_text("/.managed-venv/\n", encoding="utf-8")
    _git(repo, "add", ".gitignore")
    _git(repo, "commit", "-m", "ignore in-checkout serving venv")
    baseline_sha = _git(repo, "rev-parse", "HEAD")
    ignore.unlink()
    _git(repo, "add", "-u")
    _git(repo, "commit", "-m", "failed target removes serving venv ignore")
    target_sha = _git(repo, "rev-parse", "HEAD")
    _git(repo, "checkout", "--detach", target_sha)
    _git(repo, "branch", "-f", "main", baseline_sha)
    venv = repo / ".managed-venv"
    backup = repo.parent / (
        ".managed-venv.vq-admin-backup-"
        "0123456789abcdef0123456789abcdef"
    )
    _write_venv(venv, "bad")
    _write_venv(backup, "old")
    prog = config.VenvProgram(
        kind="venv",
        python=str(venv / "bin" / "python"),
        git_dir=str(repo),
        branch="main",
        update_script="vibe-queue/scripts/update.sh",
    )
    lifecycle = _lifecycle(prog, baseline_sha, venv, backup)
    _arm_receipt(prog, lifecycle)
    _patch_common_completion(monkeypatch, prog, baseline_sha, target_sha)
    monkeypatch.setattr(
        admin,
        "_start_managed_daemon_update",
        lambda unused: (True, "old service started"),
    )
    monkeypatch.setattr(
        admin,
        "_verify_restarted_daemon",
        lambda *args, **kwargs: admin.DaemonProvenance(
            verified=True,
            actual_sha=baseline_sha,
            actual_tree_sha256=OLD_TREE,
            detail="old RPC identity verified",
        ),
    )

    recovered, detail = admin._recover_managed_daemon_after_exception(
        prog,
        lifecycle,
        clear_receipt=False,
    )

    assert recovered is True, detail
    assert (venv / "identity.txt").read_text(encoding="utf-8") == "old\n"
    assert _git(repo, "rev-parse", "HEAD") == baseline_sha
    assert _git(repo, "rev-parse", "--abbrev-ref", "HEAD") == "main"
    assert _git(repo, "status", "--porcelain") == ""


@pytest.mark.parametrize('failure', [KeyboardInterrupt(), SystemExit(130)])
def test_driver_update_interruption_retains_executing_archive(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
    failure: BaseException,
) -> None:
    calls = _patch_driver_runtime_transport(monkeypatch, mutation=failure)
    with pytest.raises(type(failure)):
        _call_driver_runtime_update(_driver_update_runtime_config())
    assert len([c for c in calls if c[1][0] == '/usr/bin/env']) == 1
    assert not any(c[1][0] in {'rm', 'rmdir'} for c in calls)
    assert 'retained at remote:vqscratch/' in caplog.text


@pytest.mark.parametrize('boundary', ['upload', 'hash'])
def test_driver_update_rechecks_config_after_transport(
    monkeypatch: pytest.MonkeyPatch, boundary: str,
) -> None:
    cfg = _driver_update_runtime_config()
    calls = _patch_driver_runtime_transport(monkeypatch)
    upload = admin.transport.upload_file
    shell = admin.transport.run_remote_shell

    def changed_upload(*args: Any, **kwargs: Any) -> None:
        upload(*args, **kwargs)
        if boundary == 'upload':
            cfg.hosts['remote'].ssh = 'changed.example.invalid'

    def changed_shell(*args: Any, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        result = shell(*args, **kwargs)
        if boundary == 'hash' and args[1] == 'sha256sum':
            cfg.hosts['remote'].admin_token_file = '/etc/vq/changed-token'
        return result

    monkeypatch.setattr(admin.transport, 'upload_file', changed_upload)
    monkeypatch.setattr(admin.transport, 'run_remote_shell', changed_shell)
    with pytest.raises(admin.AdminError, match='config changed'):
        _call_driver_runtime_update(cfg)
    assert not any(c[1][0] == '/usr/bin/env' for c in calls)
    assert calls[-1][1][0] == 'rmdir'


def test_driver_update_vq_only_host_accepts_queue_and_refuses_chemistry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = _driver_update_runtime_config()
    cfg.hosts['remote'].fleet_role = 'vq-only'
    calls = _patch_driver_runtime_transport(monkeypatch)
    assert _call_driver_runtime_update(cfg) == 'updated\n'
    calls.clear()
    with pytest.raises(admin.AdminError, match='vq-only'):
        _call_driver_runtime_update(cfg, env='vibeqc-release')
    assert calls == []


def test_driver_update_refuses_another_name_for_scheduler_driver(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = _driver_update_runtime_config(scheduler_topology=True)
    cfg.hosts['same-driver'] = cfg.hosts['remote'].model_copy(deep=True)
    calls = _patch_driver_runtime_transport(monkeypatch)
    with pytest.raises(admin.AdminError, match='scheduler_driver'):
        _call_driver_runtime_update(cfg, host='same-driver')
    assert calls == []


def test_driver_update_rechecks_config_file_after_upload(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    cfg = _driver_update_runtime_config()
    config_file = tmp_path / 'config.toml'
    config_file.write_text('default_host = "remote"\n')
    monkeypatch.setattr(config, 'config_path', lambda: config_file)
    calls = _patch_driver_runtime_transport(monkeypatch)
    upload = admin.transport.upload_file

    def changed_upload(*args: Any, **kwargs: Any) -> None:
        upload(*args, **kwargs)
        config_file.write_text('default_host = "other"\n')

    monkeypatch.setattr(admin.transport, 'upload_file', changed_upload)
    with pytest.raises(admin.AdminError, match='config changed'):
        _call_driver_runtime_update(cfg)
    assert not any(c[1][0] == '/usr/bin/env' for c in calls)
    assert calls[-1][1][0] == 'rmdir'


def _bootstrap_driver(cfg: config.Config, sha: str) -> str:
    return admin.update_remote_managed_env_with_driver_runtime(
        cfg, "remote", env="vibeqc-queue", expected_sha=sha, expected_tag=None,
        remote_auth_args=(), stdin_data=None, as_json=True,
        update_script_args=(), show_output=False, remote_timeout_env={},
        timeout=120, self_update=True,
    )


@pytest.mark.parametrize("hold", [False, True], ids=["converges", "rollout-fenced"])
def test_bootstrap_historical_scheduler_driver_uses_self_update(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, hold: bool,
) -> None:
    from contextlib import nullcontext

    from vq import fleet_rollout

    _, repo, console, state_root, old_sha, new_sha, remote_env = (
        _driver_update_process_fixture(tmp_path)
    )
    cfg = _driver_update_runtime_config(
        remote_vq=str(console), scheduler_topology=True,
    )
    remote_config = Path(remote_env["VQ_CONFIG_DIR"]) / "config.toml"
    with remote_config.open("a") as stream:
        stream.write(
            '\n[hosts.host_f]\nssh = "host_f.example.invalid"\n'
            'scheduler = "pbs"\nscheduler_dialect = "torque"\n'
            'scratch_root = "/scratch/USER"\nscheduler_driver = "localhost"\n'
        )
    calls = _patch_local_driver_update_transport(
        monkeypatch, remote_root=tmp_path, remote_env=remote_env,
    )
    monkeypatch.setenv("VQ_STATE_DIR", str(state_root))
    lock = fleet_rollout.rollout_execution_lock("other-rollout") if hold else nullcontext()
    with lock:
        if hold:
            with pytest.raises(
                admin.transport.RemoteError, match="another fleet rollout is already",
            ):
                _bootstrap_driver(cfg, new_sha)
            assert _git(repo, "rev-parse", "HEAD") == old_sha
        else:
            payload = json.loads(_bootstrap_driver(cfg, new_sha))
            assert payload["success"] is True
            assert payload["actual_sha"] == new_sha
            assert payload["daemon_health_verified"] is True
            assert payload["self_update_selector"] == {
                "kind": "expected-sha", "identity": new_sha,
            }
            assert _git(repo, "rev-parse", "HEAD") == new_sha
    assert not (state_root / admin.ADMIN_UPDATE_MARKER_FILENAME).exists()
    assert (tmp_path / "service-running").exists()
    mutations = [args for kind, args in calls if kind == "shell" and args[0] == "/usr/bin/env"]
    assert len(mutations) == 1
    assert "self-update" in mutations[0]
    assert not list((tmp_path / "vqscratch/vq-driver-recovery").glob("*/vq-driver.pyz"))


@pytest.mark.parametrize("failure", ["digest", "interruption", "drift"])
def test_bootstrap_driver_retains_staging_safety(
    monkeypatch: pytest.MonkeyPatch, failure: str,
) -> None:
    cfg = _driver_update_runtime_config(scheduler_topology=True)
    calls = _patch_driver_runtime_transport(
        monkeypatch, verified_sha="b" * 64 if failure == "digest" else "a" * 64,
        mutation=KeyboardInterrupt() if failure == "interruption" else None,
    )
    if failure == "drift":
        original_upload = admin.transport.upload_file

        def upload(*args: Any, **kwargs: Any) -> None:
            original_upload(*args, **kwargs)
            cfg.hosts["remote"].ssh = "changed.example.invalid"

        monkeypatch.setattr(admin.transport, "upload_file", upload)
    error = KeyboardInterrupt if failure == "interruption" else admin.AdminError
    with pytest.raises(error):
        _bootstrap_driver(cfg, "a" * 40)
    mutations = [args for kind, args, _ in calls if kind == "shell" and args[0] == "/usr/bin/env"]
    if failure == "interruption":
        assert len(mutations) == 1
        assert not any(args[0] in {"rm", "rmdir"} for kind, args, _ in calls if kind == "shell")
        assert next(options["retry_transient"] for kind, args, options in calls
                    if kind == "shell" and args[0] == "/usr/bin/env") == 0
    else:
        assert mutations == []


def test_bootstrap_cli_routes_scheduler_driver_to_self_update(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = _driver_update_runtime_config(scheduler_topology=True)
    monkeypatch.setattr(config, "load_config", lambda: cfg)
    calls: list[dict[str, Any]] = []

    def bootstrap(*args: Any, **kwargs: Any) -> str:
        calls.append(kwargs)
        return '{"success": true}\n'

    monkeypatch.setattr(admin, "update_remote_managed_env_with_driver_runtime", bootstrap)
    result = CliRunner().invoke(cli.main, [
        "admin", "bootstrap-self-update", "remote", "--expected-sha", "a" * 40, "--json",
    ])
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["success"] is True
    assert calls[0]["self_update"] is True
    assert calls[0]["expected_tag"] is None
    assert calls[0]["update_script_args"] == ()


def test_staged_self_update_rejects_unstaged_runtime_before_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        config, "load_config", lambda: pytest.fail("config reached before authentication"),
    )
    result = CliRunner().invoke(cli.main, [
        "self-update", "--expected-sha", "a" * 40,
        "--staged-driver-archive-sha256", "b" * 64,
    ])
    assert result.exit_code == 2
    assert "integrity-checked zip-imported vq runtime" in result.output


@pytest.mark.parametrize("missing", [False, True], ids=["main-pin", "absent-pin"])
def test_exact_sha_fetch_when_managed_environment_tracks_release(
    tmp_path: Path, missing: bool,
) -> None:
    origin = tmp_path / "origin"
    origin.mkdir()
    _git(origin, "init", "--initial-branch", "release")
    (origin / "version").write_text("old")
    _git(origin, "add", "version")
    _git(origin, "commit", "-m", "release source")
    old_sha = _git(origin, "rev-parse", "HEAD")
    target = tmp_path / "managed"
    _git(tmp_path, "clone", "--single-branch", "--branch", "release", str(origin), str(target))
    _git(origin, "checkout", "-b", "main")
    (origin / "version").write_text("current")
    _git(origin, "commit", "-am", "main source")
    wanted = "a" * 40 if missing else _git(origin, "rev-parse", "HEAD")
    assert subprocess.run(
        ["git", "-C", str(target), "cat-file", "-e", wanted], capture_output=True,
    ).returncode != 0
    code, output = admin._run_git_fetch_sha(target, wanted, "release")
    if missing:
        assert code != 0
    else:
        assert code == 0, output
        assert _git(target, "rev-parse", wanted + "^{commit}") == wanted
    assert _git(target, "rev-parse", "HEAD") == old_sha
    assert _git(target, "rev-parse", "origin/release") == old_sha
    assert (target / "version").read_text() == "old"


@pytest.mark.parametrize("outcome", ["success", "build-failure", "proof-drift"])
def test_current_driver_adopts_unmarked_historical_install(tmp_path, monkeypatch, outcome):
    (cfg, repo, console, state_root, _old_sha, new_sha,
     remote_env) = _driver_update_process_fixture(tmp_path)
    venv = console.parent.parent
    (venv / '.vq-install-metadata').unlink()
    site = next(venv.glob('lib/python*/site-packages'))
    metadata = site / 'vq-0.11.0.dist-info/direct_url.json'
    metadata.parent.mkdir()
    metadata.write_text(json.dumps({'url': (repo / 'vibe-queue').as_uri(), 'dir_info': {}}))
    if not (venv / 'lib64').exists():
        (venv / 'lib64').symlink_to('lib', target_is_directory=True)
    _patch_local_driver_update_transport(monkeypatch, remote_root=tmp_path, remote_env=remote_env)
    if outcome != "success":
        with (tmp_path / 'host_tools.py').open('a') as out:
            if outcome == "build-failure":
                out.write(
                    "\nadmin._run_update_script = lambda *a, **k: "
                    "(23, 'injected build failure')\n"
                )
            else:
                out.write(
                    "\n_original_pause = admin.pause_token_scope_with_proof\n"
                    "def drift(*a, **k):\n"
                    "    result = _original_pause(*a, **k)\n"
                    f"    pathlib.Path({str(metadata)!r}).write_text('{{}}')\n"
                    "    return result\n"
                    "admin.pause_token_scope_with_proof = drift\n"
                )
        with pytest.raises(admin.transport.RemoteError):
            _call_driver_runtime_update(
                cfg, expected_sha=new_sha, expected_tag='new-target', remote_auth_args=(),
                stdin_data=None, as_json=True,
                update_script_args=('--adopt-legacy', '--extras', 'core', '--copied'),
                show_output=False,
            )
        assert _git(repo, 'rev-parse', 'HEAD') == _old_sha
        assert not (venv / '.vq-install-metadata').exists()
        assert metadata.is_file()
        assert (tmp_path / 'service-running').exists()
        return
    output = _call_driver_runtime_update(
        cfg, expected_sha=new_sha, expected_tag='new-target', remote_auth_args=(),
        stdin_data=None, as_json=True,
        update_script_args=('--adopt-legacy', '--extras', 'core', '--copied'),
        show_output=False,
    )
    payload = json.loads(output)
    assert payload['success'] is True
    assert payload['actual_sha'] == new_sha
    assert _git(repo, 'rev-parse', 'HEAD') == new_sha
    assert (venv / '.vq-install-metadata').is_file()
    assert not (state_root / admin.ADMIN_UPDATE_MARKER_FILENAME).exists()
