"""v0.6.39: the daemon's auto-cleanup sweeps the per-user state trees.

`run_auto_cleanup_pass` resolved terminal jobs from the single-user
`paths.queue_dir()`. In multi-user mode job state lives under
`/var/lib/vq/users/<uid>/`, so the daemon's auto-cleanup pass
silently never touched it — terminal jobs would accumulate on disk
forever once auto-cleanup was enabled on a multi-user host.

With `multi_user=True` the pass now sweeps every per-user state
tree; each user's workspaces archive into their own
`user_archive_dir`. `auto_cleanup_policy_path()` is also
multi-user-aware (the policy file is daemon-wide, at the system
root).
"""
from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from vq import cleanup, paths
from vq.spec import JobSpec, JobState


@pytest.fixture
def mu_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Multi-user state root + a provisioned dir for the test's uid.
    VQ_STATE_DIR is also pointed at tmp as a backstop so a policy
    write can never touch the real ~/.local/share/vq."""
    monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(tmp_path / "mu"))
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "su"))
    monkeypatch.setattr(paths, "is_multi_user", lambda: True)
    paths.provision_user_state(os.getuid(), os.getgid())
    return tmp_path


def _write_user_terminal_spec(
    uid: int, jobid: str, *, finished_at: str,
    state_val: JobState = JobState.COMPLETED,
) -> None:
    """Drop a terminal-state spec + workspace into the user's tree."""
    ws = paths.user_workspace_dir(uid, jobid)
    ws.mkdir(parents=True, exist_ok=True)
    (ws / "out.txt").write_text("result data\n")
    JobSpec(
        id=jobid,
        command=["true"],
        cwd=str(ws),
        cpus=1,
        state=state_val,
        finished_at=finished_at,
        exit_code=0 if state_val == JobState.COMPLETED else 1,
        submitter=str(uid),
    ).write(paths.user_spec_path(uid, jobid))


def _ago(*, hours: int = 0, days: int = 0) -> str:
    return (
        datetime.now(UTC) - timedelta(hours=hours, days=days)
    ).isoformat()


class TestPolicyPathMultiUser:
    def test_policy_path_follows_mode(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(tmp_path / "mu"))
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "su"))
        monkeypatch.setattr(paths, "is_multi_user", lambda: True)
        assert cleanup.auto_cleanup_policy_path() == (
            tmp_path / "mu" / cleanup.AUTO_CLEANUP_FILENAME
        )
        monkeypatch.setattr(paths, "is_multi_user", lambda: False)
        assert cleanup.auto_cleanup_policy_path() == (
            tmp_path / "su" / cleanup.AUTO_CLEANUP_FILENAME
        )


class TestAutoCleanupSweepsPerUserDirs:
    def test_archives_a_per_user_job(self, mu_state: Path) -> None:
        uid = os.getuid()
        _write_user_terminal_spec(uid, "mujob-arch-1", finished_at=_ago(hours=2))
        policy = cleanup.AutoCleanupPolicy(archive_after_seconds=60)
        counts = cleanup.run_auto_cleanup_pass(policy, multi_user=True)
        assert counts["archived"] == 1
        # Archived into the *user's own* archive dir.
        assert (
            paths.user_archive_dir(uid) / "mujob-arch-1.tar.bz2"
        ).exists()

    def test_deletes_a_per_user_job(self, mu_state: Path) -> None:
        uid = os.getuid()
        _write_user_terminal_spec(uid, "mujob-del-1", finished_at=_ago(days=200))
        policy = cleanup.AutoCleanupPolicy(delete_after_seconds=86400)
        counts = cleanup.run_auto_cleanup_pass(policy, multi_user=True)
        assert counts["deleted"] == 1
        assert not paths.user_spec_path(uid, "mujob-del-1").exists()

    def test_recent_per_user_job_left_alone(self, mu_state: Path) -> None:
        uid = os.getuid()
        _write_user_terminal_spec(uid, "mujob-new-1", finished_at=_ago(hours=1))
        policy = cleanup.AutoCleanupPolicy(archive_after_seconds=86400)
        counts = cleanup.run_auto_cleanup_pass(policy, multi_user=True)
        assert counts["archived"] == 0
        assert paths.user_spec_path(uid, "mujob-new-1").exists()


class TestSingleUserUnaffected:
    def test_single_user_pass_ignores_per_user_dirs(
        self, mu_state: Path
    ) -> None:
        # An old terminal job exists ONLY in a per-user dir. A
        # single-user pass (multi_user defaults False) must not see
        # it — proving the per-user sweep is gated on the flag.
        uid = os.getuid()
        _write_user_terminal_spec(uid, "mujob-ctl-1", finished_at=_ago(hours=2))
        policy = cleanup.AutoCleanupPolicy(archive_after_seconds=60)
        counts = cleanup.run_auto_cleanup_pass(policy)
        assert counts["archived"] == 0
        assert paths.user_spec_path(uid, "mujob-ctl-1").exists()
