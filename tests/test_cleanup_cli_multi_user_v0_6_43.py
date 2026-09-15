"""v0.6.43: the manual `vq cleanup` CLI verb works in multi-user mode.

`find_candidates` / `find_candidates_by_jobid` resolved terminal
jobs from the single-user `paths.queue_dir()`. In multi-user mode
jobs live under `/var/lib/vq/users/<uid>/queue/`, so `vq cleanup`
on a multi-user host listed nothing and could archive/delete
nothing. (The daemon's *auto*-cleanup was already fixed in
v0.6.39 — this is the operator-facing manual verb.)

Both discovery functions now take a `multi_user` flag: they sweep
/ resolve the per-user dirs and stamp each `Candidate` with its
owning `uid`, so the CLI can archive/delete using that user's own
queue + archive dirs.
"""
from __future__ import annotations

import os
from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import cleanup, config, paths
from vq.cli import main
from vq.spec import JobSpec, JobState


@pytest.fixture
def mu_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Multi-user root + a [multi_user] config (so the CLI autodetect
    fires) + a provisioned dir for the test's uid. VQ_STATE_DIR is a
    tmp backstop."""
    monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(tmp_path / "mu"))
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "su"))
    cfgdir = tmp_path / "cfg"
    cfgdir.mkdir()
    (cfgdir / "config.toml").write_text("[multi_user]\nenabled = true\n")
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(cfgdir))
    paths.provision_user_state(os.getuid(), os.getgid())
    return tmp_path


def _write_user_terminal_job(uid: int, jobid: str) -> None:
    ws = paths.user_workspace_dir(uid, jobid)
    ws.mkdir(parents=True, exist_ok=True)
    (ws / "out.txt").write_text("result\n")
    JobSpec(
        id=jobid,
        command=["true"],
        cwd=str(ws),
        cpus=1,
        state=JobState.COMPLETED,
        finished_at="2026-05-21T12:00:00+00:00",
        exit_code=0,
        submitter=str(uid),
    ).write(paths.user_spec_path(uid, jobid))


class TestFindCandidatesMultiUser:
    def test_find_candidates_sweeps_per_user_dirs(
        self, mu_state: Path
    ) -> None:
        uid = os.getuid()
        _write_user_terminal_job(uid, "cljob111111")
        cands = cleanup.find_candidates(multi_user=True)
        assert [c.spec.id for c in cands] == ["cljob111111"]
        # The Candidate is stamped with its owning uid.
        assert cands[0].uid == str(uid)
        # Single-user mode (default) cannot see the per-user job.
        assert cleanup.find_candidates() == []

    def test_find_candidates_by_jobid_resolves_per_user(
        self, mu_state: Path
    ) -> None:
        uid = os.getuid()
        _write_user_terminal_job(uid, "cljob222222")
        cands, errs = cleanup.find_candidates_by_jobid(
            ["cljob222222"], multi_user=True
        )
        assert len(cands) == 1
        assert cands[0].uid == str(uid)
        assert errs == []
        # Single-user can't resolve it → reported as "no such job".
        cands_su, errs_su = cleanup.find_candidates_by_jobid(["cljob222222"])
        assert cands_su == []
        assert errs_su == [("cljob222222", "no such job")]


class TestCleanupCLIMultiUser:
    def test_cli_cleanup_lists_per_user_jobs(self, mu_state: Path) -> None:
        uid = os.getuid()
        _write_user_terminal_job(uid, "cljob333333")
        result = CliRunner().invoke(main, ["cleanup", "localhost"])
        assert result.exit_code == 0
        assert "cljob333333" in result.output

    def test_cli_cleanup_delete_removes_per_user_job(
        self, mu_state: Path
    ) -> None:
        uid = os.getuid()
        _write_user_terminal_job(uid, "cljob444444")
        result = CliRunner().invoke(
            main,
            ["cleanup", "localhost", "--delete", "--jobid", "cljob444444",
             "-x"],
        )
        assert result.exit_code == 0
        # The spec was deleted from the user's own queue dir.
        assert not paths.user_spec_path(uid, "cljob444444").exists()
