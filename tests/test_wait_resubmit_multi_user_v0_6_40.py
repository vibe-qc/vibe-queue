"""v0.6.40: `vq wait` and `vq resubmit` work in multi-user mode.

Both resolved job specs from the single-user `paths.queue_dir()`.
In multi-user mode specs live under `/var/lib/vq/users/<uid>/
queue/`, so on a multi-user host:

* `vq wait JOBID` failed with "no such job"; and
* `vq resubmit JOBID` couldn't find the source — and even if it
  had, it wrote the new spec with a `user@host` submitter, which
  the v0.6.35 dispatch gate rejects in a per-user queue dir.

`wait_for_terminal_local` / `resubmit_local` / `resubmit_state`
now take a `multi_user` flag: they resolve from the per-user
dirs, and resubmit lands the new job (spec + workspace +
numeric-uid submitter) in the source's own user tree.
"""
from __future__ import annotations

import os
from pathlib import Path

import pytest

from vq import config, paths, resubmit, wait
from vq.spec import JobSpec, JobState


@pytest.fixture
def mu_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Multi-user state root + a provisioned dir for the test's uid.
    VQ_STATE_DIR also points at tmp as a backstop."""
    monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(tmp_path / "mu"))
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "su"))
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "config.toml").write_text(
        "[multi_user]\nenabled = true\n"
    )
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(config_dir))
    monkeypatch.setattr(
        config, "SYSTEM_CONFIG_PATH", tmp_path / "absent-system.toml"
    )
    paths.provision_user_state(os.getuid(), os.getgid())
    return tmp_path


def _write_user_terminal_job(
    uid: int, jobid: str, *, state: JobState = JobState.COMPLETED
) -> None:
    """A terminal-state job (spec + workspace) in the user's tree."""
    ws = paths.user_workspace_dir(uid, jobid)
    ws.mkdir(parents=True, exist_ok=True)
    (ws / "input.txt").write_text("job input\n")
    JobSpec(
        id=jobid,
        command=["true"],
        cwd=str(ws),
        cpus=1,
        state=state,
        finished_at="2026-05-21T12:00:00+00:00",
        exit_code=0 if state == JobState.COMPLETED else 1,
        submitter=str(uid),
    ).write(paths.user_spec_path(uid, jobid))


class TestWaitMultiUser:
    def test_wait_resolves_per_user_spec(self, mu_state: Path) -> None:
        uid = os.getuid()
        _write_user_terminal_job(uid, "wait-job-1")
        # Single-user resolution can't see the per-user spec.
        with pytest.raises(FileNotFoundError):
            wait.wait_for_terminal_local("wait-job-1")
        # Multi-user resolves it; already terminal → returns at once.
        result = wait.wait_for_terminal_local(
            "wait-job-1", multi_user=True
        )
        assert result.state == JobState.COMPLETED
        assert result.jobid == "wait-job-1"


class TestResubmitMultiUser:
    def test_resubmit_local_lands_in_owner_tree(
        self, mu_state: Path
    ) -> None:
        uid = os.getuid()
        _write_user_terminal_job(uid, "src-job-1")
        new_id = resubmit.resubmit_local("src-job-1", multi_user=True)
        # New spec lands in the same user's per-user queue dir.
        new_spec_path = paths.user_spec_path(uid, new_id)
        assert new_spec_path.exists()
        new_spec = JobSpec.read(new_spec_path)
        # submitter is the numeric owning uid — the v0.6.35 dispatch
        # gate rejects a user@host submitter in a per-user dir.
        assert new_spec.submitter == str(uid)
        assert new_spec.parent_jobid == "src-job-1"
        # Workspace deep-copied under the user's own jobs dir.
        assert Path(new_spec.cwd) == paths.user_workspace_dir(uid, new_id)
        assert (Path(new_spec.cwd) / "input.txt").is_file()

    def test_resubmit_local_single_user_cannot_find_per_user_job(
        self, mu_state: Path
    ) -> None:
        _write_user_terminal_job(os.getuid(), "src-job-2")
        with pytest.raises(FileNotFoundError):
            resubmit.resubmit_local("src-job-2")

    def test_resubmit_state_sweeps_per_user_dirs(
        self, mu_state: Path
    ) -> None:
        uid = os.getuid()
        _write_user_terminal_job(
            uid, "rs-job-1", state=JobState.ABORTED_BY_QUEUE
        )
        res = resubmit.resubmit_state(
            [JobState.ABORTED_BY_QUEUE], multi_user=True
        )
        assert len(res.pairs) == 1
        src, new_id = res.pairs[0]
        assert src == "rs-job-1"
        assert paths.user_spec_path(uid, new_id).exists()

    def test_resubmit_state_single_user_ignores_per_user(
        self, mu_state: Path
    ) -> None:
        # A per-user-only job; a single-user bulk resubmit (flag
        # defaults False) must not see it.
        _write_user_terminal_job(
            os.getuid(), "ctl-job-1", state=JobState.ABORTED_BY_QUEUE
        )
        res = resubmit.resubmit_state([JobState.ABORTED_BY_QUEUE])
        assert res.pairs == []
        assert res.errors == []
