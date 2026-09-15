"""v0.6.41: the web dashboard + `vq overview` work in multi-user mode.

Both read job state from the single-user `paths.queue_dir()`. In
multi-user mode jobs live under `/var/lib/vq/users/<uid>/queue/`,
so on a multi-user host the web `/queue` page rendered empty,
`/jobs/<id>` 404'd, and `vq overview` / `vq summary` reported 0
jobs.

`create_app()` now detects multi-user mode once and threads it
into every queue read; `gather_overview_local` takes a
`multi_user` flag forwarded to `list_jobs`.
"""
from __future__ import annotations

import os
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from vq import config, overview, paths
from vq.spec import JobSpec, JobState
from vq.web import create_app


@pytest.fixture
def mu_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Multi-user root + a config with [multi_user] enabled so
    create_app() detects the mode. VQ_STATE_DIR is a tmp backstop."""
    monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(tmp_path / "mu"))
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "su"))
    cfgdir = tmp_path / "cfg"
    cfgdir.mkdir()
    (cfgdir / "config.toml").write_text("[multi_user]\nenabled = true\n")
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(cfgdir))
    paths.provision_user_state(os.getuid(), os.getgid())
    return tmp_path


def _write_user_spec(
    uid: int, jobid: str, *, state: JobState = JobState.COMPLETED
) -> None:
    ws = paths.user_workspace_dir(uid, jobid)
    ws.mkdir(parents=True, exist_ok=True)
    JobSpec(
        id=jobid,
        command=["true"],
        cwd=str(ws),
        cpus=1,
        state=state,
        submitter=str(uid),
    ).write(paths.user_spec_path(uid, jobid))


class TestWebDashboardMultiUser:
    def test_queue_page_shows_per_user_jobs(self, mu_state: Path) -> None:
        _write_user_spec(os.getuid(), "webjob111111")
        client = TestClient(create_app())
        r = client.get("/queue")
        assert r.status_code == 200
        assert "webjob111111" in r.text

    def test_job_detail_resolves_per_user_spec(
        self, mu_state: Path
    ) -> None:
        _write_user_spec(os.getuid(), "webjob222222")
        client = TestClient(create_app())
        r = client.get("/jobs/webjob222222")
        assert r.status_code == 200

    def test_job_detail_unknown_job_404s(self, mu_state: Path) -> None:
        client = TestClient(create_app())
        r = client.get("/jobs/nosuchjob9999")
        assert r.status_code == 404

    def test_health_ready_uses_multi_user_daemon_scope(
        self,
        mu_state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        scopes: list[bool] = []

        def daemon_running(*, multi_user: bool = False) -> bool:
            scopes.append(multi_user)
            return True

        monkeypatch.setattr("vq.web.is_daemon_running", daemon_running)
        client = TestClient(create_app())

        r = client.get("/health/ready")

        assert r.status_code == 200
        assert scopes == [True]


class TestOverviewMultiUser:
    def test_overview_counts_per_user_jobs(self, mu_state: Path) -> None:
        _write_user_spec(
            os.getuid(), "ovjob111111", state=JobState.RUNNING
        )
        cfg = config.load_config()
        ov = overview.gather_overview_local(
            "localhost", cfg, multi_user=True
        )
        assert ov.queue_counts.get("running", 0) == 1

    def test_overview_single_user_ignores_per_user(
        self, mu_state: Path
    ) -> None:
        # multi_user defaults False → the per-user job is invisible.
        _write_user_spec(
            os.getuid(), "ovjob222222", state=JobState.RUNNING
        )
        cfg = config.load_config()
        ov = overview.gather_overview_local("localhost", cfg)
        assert ov.queue_counts.get("running", 0) == 0
