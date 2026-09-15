"""Tests for the v0.6.23 polish bundle.

Five items:
1. Watchdog auto-pause marks paused_by="watchdog_host_pressure".
2. vq overview surfaces drain + throttle state.
3. vq daemon health --json exposes drain_active + drain_reason.
4. Web dashboard surfaces host_pressure_active.
5. vq cleanup --jobid X for single-job archive/delete.
"""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from vq import (
    cleanup as cleanup_module,
)
from vq import (
    config,
    drain,
    lifecycle,
    overview,
    paths,
    throttle,
)
from vq.cli import main
from vq.spec import JobSpec, JobState


@pytest.fixture
def state_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    paths.jobs_dir().mkdir(parents=True, exist_ok=True)
    (tmp_path / "cfg" / "config.toml").write_text(
        'default_host = "localhost"\n'
    )
    return tmp_path


def _make_spec(
    jobid: str,
    state: JobState = JobState.COMPLETED,
    *,
    finished_at: str | None = "2026-05-18T10:00:00+00:00",
    archived: bool = False,
    workspace_files: dict[str, str] | None = None,
    **kwargs: object,
) -> JobSpec:
    queue = paths.queue_dir()
    jobs = paths.jobs_dir()
    workspace = jobs / jobid
    workspace.mkdir(exist_ok=True)
    for name, content in (workspace_files or {}).items():
        (workspace / name).write_text(content)
    base = {
        "id": jobid,
        "command": ["echo", "x"],
        "cwd": str(workspace),
        "cpus": 1,
        "state": state,
        "finished_at": finished_at,
    }
    base.update(kwargs)  # type: ignore[arg-type]
    spec = JobSpec(**base)  # type: ignore[arg-type]
    if archived:
        # Stamp an archive marker; the file itself need not exist
        # for the find_candidates filter to pick this up.
        spec.archived_at = "2026-05-18T11:00:00+00:00"
        spec.archive_path = str(workspace) + ".tar.bz2"
    spec.write(queue / f"{jobid}.json")
    return spec


# ----------------------------------------------------------------------
# Item 3: daemon health --json exposes drain_active + drain_reason
# ----------------------------------------------------------------------


class TestDaemonHealthDrainFields:
    def test_no_drain_means_inactive_no_reason(self, state_dir: Path) -> None:
        verdict = lifecycle.verify_user_systemd_contract()
        assert verdict.drain_active is False
        assert verdict.drain_reason is None

    def test_active_drain_surfaces_in_verdict(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Inject a drain state via the read function so the verdict
        # picks it up. Patching read_drain_state in vq.lifecycle's
        # import scope.
        fake = drain.DrainState(
            enabled=True, max_jobs=None, max_cpus=None,
            reason="kids gaming",
        )
        with patch("vq.drain.read_drain_state", return_value=fake):
            verdict = lifecycle.verify_user_systemd_contract()
        assert verdict.drain_active is True
        assert verdict.drain_reason == "kids gaming"

    def test_json_serialization_includes_drain_fields(
        self, state_dir: Path
    ) -> None:
        verdict = lifecycle.verify_user_systemd_contract()
        payload = json.loads(lifecycle.format_contract_verdict_json(verdict))
        assert "drain_active" in payload
        assert "drain_reason" in payload


# ----------------------------------------------------------------------
# Item 2: vq overview surfaces drain + throttle
# ----------------------------------------------------------------------


class TestOverviewDrainThrottle:
    def test_no_drain_throttle_means_none_fields(self, state_dir: Path) -> None:
        cfg = config.load_config()
        ov = overview.gather_overview_local("localhost", cfg)
        assert ov.drain_state is None
        assert ov.throttle_state is None

    def test_drain_state_populated_when_set(
        self, state_dir: Path
    ) -> None:
        # Write a real drain.json the overview gather will read.
        from vq.drain import DrainState, drain_state_path
        d = DrainState(enabled=True, reason="overview-test")
        drain_state_path().write_text(d.model_dump_json())

        cfg = config.load_config()
        ov = overview.gather_overview_local("localhost", cfg)
        assert ov.drain_state is not None
        assert ov.drain_state.reason == "overview-test"

    def test_text_format_shows_drain_line(self) -> None:
        d = drain.DrainState(
            enabled=True, max_jobs=None, max_cpus=None, reason="kids-gaming",
        )
        ov = overview.HostOverview(
            host="host_d",
            vq_version="0.6.23",
            drain_state=d,
        )
        text = overview.format_overview_text(ov)
        assert "drain:" in text
        assert "kids-gaming" in text

    def test_text_format_shows_throttle_line(self) -> None:
        t = throttle.ThrottleState(weight=20, reason="background")
        ov = overview.HostOverview(
            host="host_d",
            vq_version="0.6.23",
            throttle_state=t,
        )
        text = overview.format_overview_text(ov)
        assert "throttle:" in text
        assert "CPUWeight=20" in text
        assert "background" in text

    def test_json_format_includes_drain_throttle_keys(
        self, state_dir: Path
    ) -> None:
        cfg = config.load_config()
        ov = overview.gather_overview_local("localhost", cfg)
        payload = overview.format_overview_json(ov)
        assert "drain_state" in payload
        assert "throttle_state" in payload


# ----------------------------------------------------------------------
# Item 5: vq cleanup --jobid X
# ----------------------------------------------------------------------


class TestCleanupByJobid:
    def test_find_candidates_by_jobid_picks_terminal_specs(
        self, state_dir: Path
    ) -> None:
        _make_spec("term00000001", JobState.COMPLETED)
        _make_spec("term00000002", JobState.FAILED)
        candidates, errors = cleanup_module.find_candidates_by_jobid(
            ["term00000001", "term00000002"]
        )
        assert len(candidates) == 2
        assert errors == []
        assert {c.spec.id for c in candidates} == {
            "term00000001", "term00000002",
        }

    def test_find_candidates_by_jobid_rejects_running(
        self, state_dir: Path
    ) -> None:
        _make_spec("run000000001", JobState.RUNNING, finished_at=None)
        candidates, errors = cleanup_module.find_candidates_by_jobid(
            ["run000000001"]
        )
        assert candidates == []
        assert len(errors) == 1
        assert errors[0][0] == "run000000001"
        assert "not terminal" in errors[0][1]

    def test_find_candidates_by_jobid_rejects_unknown(
        self, state_dir: Path
    ) -> None:
        candidates, errors = cleanup_module.find_candidates_by_jobid(
            ["nonexistent1"]
        )
        assert candidates == []
        assert errors[0] == ("nonexistent1", "no such job")

    def test_find_candidates_by_jobid_require_archived_false_rejects_archived(
        self, state_dir: Path
    ) -> None:
        """--archive (require_archived=False) refuses already-archived
        jobs so the operator doesn't accidentally double-archive."""
        _make_spec("arc000000001", JobState.COMPLETED, archived=True)
        candidates, errors = cleanup_module.find_candidates_by_jobid(
            ["arc000000001"], require_archived=False,
        )
        assert candidates == []
        assert "already archived" in errors[0][1]

    def test_cli_jobid_and_older_than_mutually_exclusive(
        self, state_dir: Path
    ) -> None:
        result = CliRunner().invoke(
            main,
            [
                "cleanup", "localhost",
                "--archive",
                "--jobid", "deadbeef0001",
                "--older-than", "30d",
            ],
        )
        assert result.exit_code != 0
        assert "mutually exclusive" in result.output

    def test_cli_archive_without_older_or_jobid_errors(
        self, state_dir: Path
    ) -> None:
        result = CliRunner().invoke(
            main, ["cleanup", "localhost", "--archive"],
        )
        assert result.exit_code != 0
        assert "--older-than or --jobid" in result.output

    def test_cli_jobid_dryrun_lists_candidate(self, state_dir: Path) -> None:
        _make_spec("jcli00000001", JobState.COMPLETED)
        result = CliRunner().invoke(
            main,
            [
                "cleanup", "localhost",
                "--archive",
                "--jobid", "jcli00000001",
            ],
        )
        assert result.exit_code == 0, result.output
        assert "jcli00000001" in result.output

    def test_cli_jobid_with_unknown_id_surfaces_error_continues(
        self, state_dir: Path
    ) -> None:
        _make_spec("real00000001", JobState.COMPLETED)
        result = CliRunner().invoke(
            main,
            [
                "cleanup", "localhost",
                "--archive",
                "--jobid", "real00000001",
                "--jobid", "nope00000002",
            ],
        )
        # Dry-run; the real one shows in the table, the unknown one
        # is surfaced on stderr but doesn't fail the whole run.
        assert result.exit_code == 0, result.output
        assert "real00000001" in result.output

    def test_cli_jobid_rejected_with_restore(self, state_dir: Path) -> None:
        result = CliRunner().invoke(
            main,
            [
                "cleanup", "localhost",
                "--restore", "deadbeef0001",
                "--jobid", "other00000001",
            ],
        )
        assert result.exit_code != 0
        assert "--jobid only applies to --archive / --delete" in result.output


# ----------------------------------------------------------------------
# Item 1: watchdog auto-pause uses paused_by="watchdog_host_pressure"
# ----------------------------------------------------------------------


class TestWatchdogPausedByTag:
    def test_pause_call_passes_paused_by(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify Daemon._host_pressure_pass passes the
        paused_by='watchdog_host_pressure' kwarg to pause_job."""
        from vq import daemon as daemon_module
        from vq.watchdog import HostPressureAction, HostPressureVerdict

        captured: list[dict] = []

        def fake_pause_job(host, jid, **kwargs):  # type: ignore[no-untyped-def]
            captured.append({"host": host, "jid": jid, **kwargs})

        # Build a minimal Daemon-like surface for the pass test.
        # The _host_pressure_pass method only reads self.watchdog +
        # self._running + self._read_active_spec + self._multi_user, so a
        # stub is enough.
        class _StubDaemon:
            def __init__(self) -> None:
                self._running = {"jjj000000001": object()}
                # HP-3 (v0.8.24): _host_pressure_pass now also scans
                # reattached orphans as auto-pause candidates, so the stub
                # needs an (empty) _orphans dict.
                self._orphans: dict[str, object] = {}
                # v0.6.38: _host_pressure_pass now threads _multi_user
                # into pause_job/resume_job so the host-pressure
                # auto-pause finds per-user specs on a multi-user host.
                self._multi_user = False

                class _Wd:
                    # #563: the daemon passes _pressure_reader as a keyword.
                    def check_host_pressure(self, jobids, **kwargs):
                        return HostPressureVerdict(
                            action=HostPressureAction.PAUSE,
                            jobids=jobids,
                            pressure_pct=95.0,
                            reason="test",
                        )

                self.watchdog = _Wd()

            def _spec_path(self, jid):  # type: ignore[no-untyped-def]
                return paths.spec_path(jid)

            def _read_active_spec(  # type: ignore[no-untyped-def]
                self, jid, runtime=None
            ):
                spec_path = self._spec_path(jid)
                return JobSpec.read(spec_path), spec_path

        _make_spec("jjj000000001", JobState.RUNNING, finished_at=None)
        monkeypatch.setattr(
            "vq.pause_resume.pause_job", fake_pause_job
        )

        daemon_module.Daemon._host_pressure_pass(_StubDaemon())  # type: ignore[arg-type]

        assert len(captured) == 1
        assert captured[0]["paused_by"] == "watchdog_host_pressure"


# ----------------------------------------------------------------------
# Item 4: web dashboard surfaces host_pressure_active
# ----------------------------------------------------------------------


class TestWebHostPressureBanner:
    """Lightweight render test — TestClient on the FastAPI app."""

    def test_no_pressure_no_banner(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        try:
            from fastapi.testclient import TestClient
        except ImportError:
            pytest.skip("fastapi[testclient] not installed in this env")
        from vq.web import create_app

        # Force pressure to a low value so no banner renders.
        monkeypatch.setattr(
            "vq.watchdog.read_host_memory_pressure_pct",
            lambda: 25.0,
        )
        app = create_app()
        client = TestClient(app)
        r = client.get("/queue")
        assert r.status_code == 200
        # Banner phrase should NOT appear when below the threshold
        # and no auto-paused jobs.
        assert "Host memory pressure" not in r.text

    def test_high_pressure_renders_banner(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        try:
            from fastapi.testclient import TestClient
        except ImportError:
            pytest.skip("fastapi[testclient] not installed in this env")
        from vq.web import create_app

        monkeypatch.setattr(
            "vq.watchdog.read_host_memory_pressure_pct",
            lambda: 92.0,
        )
        app = create_app()
        client = TestClient(app)
        r = client.get("/queue")
        assert r.status_code == 200
        assert "Host memory pressure" in r.text
        assert "92.0%" in r.text

    def test_watchdog_paused_job_counted(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        try:
            from fastapi.testclient import TestClient
        except ImportError:
            pytest.skip("fastapi[testclient] not installed in this env")
        from vq.web import create_app

        _make_spec(
            "wpaused00001",
            JobState.SUSPENDED,
            paused_by="watchdog_host_pressure",
            paused_at="2026-05-18T12:00:00+00:00",
            finished_at=None,
        )
        # Make pressure low so the ONLY reason the banner renders is
        # the auto-paused job.
        monkeypatch.setattr(
            "vq.watchdog.read_host_memory_pressure_pct",
            lambda: 30.0,
        )
        app = create_app()
        client = TestClient(app)
        r = client.get("/queue")
        assert r.status_code == 200
        assert "auto-paused" in r.text
        assert "1" in r.text  # one paused job
