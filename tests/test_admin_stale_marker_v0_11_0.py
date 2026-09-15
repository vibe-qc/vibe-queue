"""v0.11.0+: stale admin-update markers need truthful dispatch handling.

Regression for the host_e / host_b 2026-06-18 incident: a marker left by a
killed `vq admin update` (pid long dead, started_at 5 days stale) survived
a reboot — it persists in the state dir — and held the queue idle. Every
submitted job sat `pending` while the daemon was otherwise healthy and the
host reported plain `up`/`OK`. `vq admin clear-update-marker -y` un-wedged
it instantly, which is exactly what the daemon now does on its own.

Coverage:
  * admin.admin_update_marker_stale_reason — the pure staleness predicate
    (pid gone / pid recycled / age backstop / live → None).
  * write_admin_update_marker records the pid_start_time fingerprint.
  * Daemon dispatch gate: an ordinary stale marker is reaped and dispatch
    proceeds; durable stale receipts remain scoped for explicit recovery;
    a *live* marker still pauses and is kept.
  * `vq overview` surfaces a loud STALE line + a JSON field, host-side
    and through the JSON round-trip.
"""
from __future__ import annotations

import subprocess
import time
from collections.abc import Iterator
from pathlib import Path

import pytest

from vq import admin, config, overview, paths
from vq.admin import AdminUpdateMarker
from vq.daemon import Daemon
from vq.spec import JobSpec, JobState, utcnow_iso


@pytest.fixture
def state_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Redirect VQ state + config dirs into tmp_path so the marker the
    test writes is the one the daemon / overview read. Returns tmp_path."""
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    (tmp_path / "cfg" / "config.toml").write_text('default_host = "localhost"\n')
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    paths.jobs_dir().mkdir(parents=True, exist_ok=True)
    return tmp_path


def _dead_pid() -> int:
    """A pid that is definitely not in the process table: spawn a trivial
    child, reap it, return its (now-dead) pid. Reuse within the test is
    vanishingly unlikely on the timescales here."""
    p = subprocess.Popen(["true"])
    p.wait()
    return p.pid


# ----------------------------------------------------------------------
# The staleness predicate
# ----------------------------------------------------------------------


class TestStaleReasonPredicate:
    def _marker(self, **over: object) -> AdminUpdateMarker:
        base: dict = dict(
            envs=["vibeqc-dev"],
            host="host_e",
            started_at=utcnow_iso(),
            pid=1,
            vq_version="0.10.0",
            pid_start_time=0,
        )
        base.update(over)
        return AdminUpdateMarker(**base)  # type: ignore[arg-type]

    def test_none_marker_is_not_stale(self) -> None:
        assert admin.admin_update_marker_stale_reason(None) is None

    def test_live_marker_is_not_stale(self) -> None:
        import os

        m = self._marker(
            pid=os.getpid(),
            started_at=utcnow_iso(),
            pid_start_time=admin._pid_start_time(os.getpid()) or 0,
        )
        assert admin.admin_update_marker_stale_reason(m) is None

    def test_dead_pid_is_stale(self) -> None:
        m = self._marker(pid=_dead_pid(), started_at=utcnow_iso())
        reason = admin.admin_update_marker_stale_reason(m)
        assert reason is not None
        assert "no longer running" in reason

    def test_dead_pid_via_probe_is_stale(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Deterministic: simulate "writer gone" without relying on a real
        # dead pid. This is the probe the daemon path exercises.
        monkeypatch.setattr(admin, "_pid_liveness", lambda pid: False)
        m = self._marker(pid=4242, started_at=utcnow_iso())
        assert admin.admin_update_marker_stale_reason(m) is not None

    def test_recycled_pid_is_stale(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # pid reads alive, but its /proc start-time differs from the one
        # recorded at write → the kernel reused the slot for a stranger.
        monkeypatch.setattr(admin, "_pid_liveness", lambda pid: True)
        monkeypatch.setattr(admin, "_pid_start_time", lambda pid: 999_999)
        m = self._marker(pid=4242, pid_start_time=111_111)
        reason = admin.admin_update_marker_stale_reason(m)
        assert reason is not None
        assert "recycled" in reason

    def test_matching_fingerprint_live_pid_is_not_stale(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(admin, "_pid_liveness", lambda pid: True)
        monkeypatch.setattr(admin, "_pid_start_time", lambda pid: 111_111)
        m = self._marker(pid=4242, pid_start_time=111_111)
        assert admin.admin_update_marker_stale_reason(m) is None

    def test_age_backstop_marks_stale(self) -> None:
        # The host_e shape: pid liveness indeterminate (pid<=0 → None),
        # but the marker is days old. Age is the backstop.
        m = self._marker(pid=0, started_at="2026-06-13T19:53:00+00:00")
        reason = admin.admin_update_marker_stale_reason(
            m, now_iso="2026-06-18T12:00:00+00:00"
        )
        assert reason is not None
        assert "old" in reason

    def test_young_indeterminate_pid_is_not_stale(self) -> None:
        # pid<=0 (liveness None), but fresh and within the age bound → we
        # can't prove it's a corpse, so respect it (conservative).
        m = self._marker(pid=0, started_at="2026-06-18T11:59:00+00:00")
        assert (
            admin.admin_update_marker_stale_reason(
                m, now_iso="2026-06-18T12:00:00+00:00"
            )
            is None
        )

    def test_age_bound_is_configurable(self) -> None:
        m = self._marker(pid=0, started_at="2026-06-18T10:00:00+00:00")
        now = "2026-06-18T12:00:00+00:00"  # 2h old
        assert admin.admin_update_marker_stale_reason(m, now_iso=now) is None
        # Tighten the bound to 1h → the same marker is now stale.
        assert (
            admin.admin_update_marker_stale_reason(
                m, now_iso=now, max_age_seconds=3600
            )
            is not None
        )

    def test_unparseable_started_at_skips_age_backstop(self) -> None:
        # A garbage timestamp shouldn't crash or false-trigger; with a
        # live-reading pid and no age signal the verdict is "not stale".
        import os

        m = self._marker(pid=os.getpid(), started_at="not-a-timestamp")
        assert admin.admin_update_marker_stale_reason(m) is None


class TestMarkerRecordsPidStartTime:
    def test_write_records_fingerprint_field(self, state_dir: Path) -> None:
        m = admin.write_admin_update_marker(envs=["vibeqc-dev"], host="host_e")
        loaded = admin.read_admin_update_marker()
        assert loaded is not None
        # int field present and round-trips. >0 on Linux (/proc), 0 on
        # macOS (no /proc) — both are valid; just assert the type + parity.
        assert isinstance(loaded.pid_start_time, int)
        assert loaded.pid_start_time == m.pid_start_time

    def test_acquire_also_records_fingerprint(self, state_dir: Path) -> None:
        # acquire_admin_update_marker is the *production* writer (the
        # O_EXCL claim used by `vq admin update`); it must record the
        # fingerprint too, not just write_admin_update_marker.
        import os

        m = admin.acquire_admin_update_marker(
            envs=["vibeqc-dev"], host="localhost"
        )
        assert m.pid == os.getpid()
        assert isinstance(m.pid_start_time, int)
        loaded = admin.read_admin_update_marker()
        assert loaded is not None
        assert loaded.pid_start_time == m.pid_start_time

    def test_legacy_marker_without_field_defaults_zero(
        self, state_dir: Path
    ) -> None:
        # A pre-v0.11.0 marker on disk (no pid_start_time key) parses with
        # the field defaulted to 0 — the staleness check then leans on the
        # age backstop rather than the fingerprint.
        path = admin.admin_update_marker_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            '{"envs": ["x"], "host": "h", "started_at": "2020-01-01T00:00:00+00:00",'
            ' "pid": 1, "vq_version": "0.5.44"}'
        )
        loaded = admin.read_admin_update_marker()
        assert loaded is not None
        assert loaded.pid_start_time == 0


# ----------------------------------------------------------------------
# Daemon dispatch gate
# ----------------------------------------------------------------------


@pytest.fixture
def daemon(state_dir: Path) -> Iterator[Daemon]:
    d = Daemon(
        max_cpus=4,
        poll_interval=0.05,
        queue_dir=paths.queue_dir(),
        jobs_dir=paths.jobs_dir(),
    )
    yield d
    for rj in list(d._running.values()):
        try:
            rj.popen.kill()
            rj.popen.wait(timeout=1)
        except Exception:
            pass
        rj.close_logs()


def _submit(daemon: Daemon, jobid: str, command: list[str]) -> JobSpec:
    workspace = daemon.jobs_dir / jobid
    workspace.mkdir(parents=True, exist_ok=True)
    spec = JobSpec(id=jobid, command=command, cwd=str(workspace), cpus=1)
    spec.write(daemon._spec_path(jobid))
    return spec


class TestDaemonReapsStaleMarker:
    def test_stale_marker_is_reaped_and_unblocks(
        self, daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Startup-equivalent: a corpse marker present before the first
        # poll. The daemon must reap it (delete the file) and NOT pause.
        admin.write_admin_update_marker(envs=["vibeqc-dev"], host="localhost")
        monkeypatch.setattr(admin, "_pid_liveness", lambda pid: False)
        assert admin.admin_update_marker_exists() is True

        paused = daemon._poll_admin_update_marker()

        assert paused is False  # dispatch proceeds
        assert admin.admin_update_marker_exists() is False  # reaped

    def test_pending_job_dispatches_despite_stale_marker(
        self, daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The headline regression: job sat `pending` behind a dead marker.
        admin.write_admin_update_marker(envs=["vibeqc-dev"], host="localhost")
        monkeypatch.setattr(admin, "_pid_liveness", lambda pid: False)
        _submit(daemon, "j1", ["true"])

        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            daemon.iterate()
            spec = JobSpec.read(daemon._spec_path("j1"))
            if spec.state == JobState.COMPLETED:
                break
            time.sleep(0.02)

        spec = JobSpec.read(daemon._spec_path("j1"))
        assert spec.state == JobState.COMPLETED
        assert admin.admin_update_marker_exists() is False

    def test_live_marker_still_pauses_and_is_kept(
        self, daemon: Daemon
    ) -> None:
        # A genuinely in-flight update (our own live pid, fresh) must NOT
        # be reaped — that would race dispatch against a venv rebuild.
        admin.write_admin_update_marker(envs=["vibeqc-dev"], host="localhost")
        _submit(daemon, "j1", ["true"])

        assert daemon._poll_admin_update_marker() is True  # pauses
        # Drive a few ticks; the job must stay pending and the marker stay.
        for _ in range(3):
            daemon.iterate()
            time.sleep(0.02)
        spec = JobSpec.read(daemon._spec_path("j1"))
        assert spec.state == JobState.PENDING
        assert admin.admin_update_marker_exists() is True

    def test_aged_marker_is_reaped(self, daemon: Daemon) -> None:
        # No mocking: a marker with a days-old started_at and a bogus pid
        # is reaped via the age backstop on a real poll.
        path = admin.admin_update_marker_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            '{"envs": ["vibeqc-dev"], "host": "localhost",'
            ' "started_at": "2020-01-01T00:00:00+00:00",'
            ' "pid": 0, "vq_version": "0.5.44"}'
        )
        assert daemon._poll_admin_update_marker() is False
        assert admin.admin_update_marker_exists() is False


# ----------------------------------------------------------------------
# vq overview surfaces the stale marker
# ----------------------------------------------------------------------


class TestOverviewSurfacesStaleMarker:
    def _marker(self, **overrides: object) -> AdminUpdateMarker:
        fields: dict[str, object] = dict(
            envs=["vibeqc-dev"],
            host="host_e",
            started_at="2026-06-13T19:53:00+00:00",
            pid=66408,
            vq_version="0.10.0",
        )
        fields.update(overrides)
        return AdminUpdateMarker(**fields)  # type: ignore[arg-type]

    def test_text_render_flags_stale_loudly(self) -> None:
        ov = overview.HostOverview(
            host="host_e",
            vq_version="0.10.0",
            admin_marker=self._marker(),
            admin_marker_stale_reason="the `vq admin update` process is gone",
        )
        text = overview.format_overview_text(ov)
        assert "STALE" in text
        assert "Not gating dispatch" in text
        assert "auto-reaps" in text
        assert "vq admin clear-update-marker host_e" in text
        assert "Dispatch remains scoped" not in text
        assert "recover-update" not in text

    @pytest.mark.parametrize(
        ("durable_overrides", "expected_command", "excluded_command"),
        [
            pytest.param(
                {"managed_transaction": {"schema": "test-managed-receipt"}},
                "`vq admin recover-update host_e`",
                "`vq admin clear-update-marker host_e`",
                id="managed-transaction",
            ),
            pytest.param(
                {"pause_token": "admin-update-0123456789ab"},
                "`vq admin clear-update-marker host_e`",
                "`vq admin recover-update host_e`",
                id="pause-only",
            ),
        ],
    )
    def test_text_render_derives_durable_recovery_action(
        self,
        durable_overrides: dict[str, object],
        expected_command: str,
        excluded_command: str,
    ) -> None:
        marker = self._marker(**durable_overrides)
        ov = overview.HostOverview(
            host="host_e",
            vq_version="0.10.0",
            admin_marker=marker,
            admin_marker_stale_reason="the updater process is gone",
            admin_marker_action="Run `vq admin recover-update wrong-host`.",
        )

        text = overview.format_overview_text(ov)

        assert "STALE" in text
        assert expected_command in text
        assert excluded_command not in text
        assert "wrong-host" not in text
        assert "Never use --force" in text
        assert "Not gating dispatch" not in text
        assert "auto-reaps" not in text
        assert "Clear by hand:" not in text

    @pytest.mark.parametrize(
        ("durable_overrides", "expected_command", "excluded_command"),
        [
            pytest.param(
                {
                    "managed_transaction": {"schema": "test-managed-receipt"},
                    "pause_token": "admin-update-0123456789ab",
                },
                "`vq admin recover-update host_e`",
                "`vq admin clear-update-marker host_e`",
                id="managed-precedes-pause-scope",
            ),
            pytest.param(
                {"pause_token": "admin-update-0123456789ab"},
                "`vq admin clear-update-marker host_e`",
                "`vq admin recover-update host_e`",
                id="pause-only",
            ),
        ],
    )
    def test_durable_render_without_carried_action_fails_safe(
        self,
        durable_overrides: dict[str, object],
        expected_command: str,
        excluded_command: str,
    ) -> None:
        ov = overview.HostOverview(
            host="host_e",
            admin_marker=self._marker(**durable_overrides),
            admin_marker_stale_reason="the updater process is gone",
        )

        text = overview.format_overview_text(ov)

        assert "Dispatch remains scoped" in text
        assert expected_command in text
        assert excluded_command not in text
        assert "Never use --force" in text
        assert "Not gating dispatch" not in text

    @pytest.mark.parametrize(
        (
            "overview_host",
            "is_scheduler_host",
            "marker_host",
            "marker_envs",
            "expected_host",
        ),
        [
            pytest.param(
                "host_0",
                False,
                "host_f",
                ["scheduler:host_f"],
                "host_f",
                id="scheduler-scope-on-physical-driver",
            ),
            pytest.param(
                "host_f",
                True,
                "host_f",
                ["scheduler:host_f"],
                "host_f",
                id="scheduler-scope-on-scheduler-card",
            ),
            pytest.param(
                "host_0",
                False,
                "localhost",
                ["vibeqc-queue"],
                "host_0",
                id="local-marker-on-physical-driver",
            ),
        ],
    )
    def test_marker_action_targets_cli_route(
        self,
        overview_host: str,
        is_scheduler_host: bool,
        marker_host: str,
        marker_envs: list[str],
        expected_host: str,
    ) -> None:
        marker = self._marker(
            envs=marker_envs,
            host=marker_host,
            pause_token="admin-update-0123456789ab",
        )
        ov = overview.HostOverview(
            host=overview_host,
            is_scheduler_host=is_scheduler_host,
            admin_marker=marker,
            admin_marker_stale_reason="the driver updater process is gone",
        )

        text = overview.format_overview_text(ov)

        assert f"`vq admin clear-update-marker {expected_host}`" in text
        wrong_host = "host_f" if expected_host == "host_0" else "host_0"
        assert f"clear-update-marker {wrong_host}" not in text

    @pytest.mark.parametrize(
        ("marker_envs", "marker_host", "projected"),
        [
            pytest.param(
                ["scheduler:host_f"],
                "host_f",
                True,
                id="matching-scheduler-scope",
            ),
            pytest.param(
                ["vibeqc-queue"],
                "localhost",
                False,
                id="driver-local-scope",
            ),
            pytest.param(
                ["scheduler:host_c"],
                "host_c",
                False,
                id="foreign-scheduler-scope",
            ),
        ],
    )
    def test_scheduler_projection_only_carries_applicable_marker(
        self,
        monkeypatch: pytest.MonkeyPatch,
        marker_envs: list[str],
        marker_host: str,
        projected: bool,
    ) -> None:
        marker = self._marker(
            envs=marker_envs,
            host=marker_host,
            pause_token="admin-update-0123456789ab",
        )
        driver_overview = overview.HostOverview(
            host="host_0",
            admin_marker=marker,
            admin_markers=[
                overview.AdminMarkerSnapshot(
                    marker=marker,
                    stale_reason="the driver updater process is gone",
                    status="stale",
                    summary="durable admin update needs recovery",
                    action=admin.diagnose_admin_update_marker(marker).action,
                )
            ],
            admin_marker_stale_reason="the driver updater process is gone",
            admin_marker_status="stale",
            admin_marker_summary="durable admin update needs recovery",
            admin_marker_action=admin.diagnose_admin_update_marker(marker).action,
        )
        cfg = config.Config(
            hosts={
                "host_f": config.HostConfig(
                    ssh="host_f",
                    scheduler="pbs",
                    scheduler_dialect="torque",
                    scheduler_driver="host_0",
                    scratch_root="/scratch/USER",
                ),
                "host_0": config.HostConfig(ssh="host_0"),
            }
        )
        monkeypatch.setattr(
            overview,
            "is_local_host",
            lambda host: host == "host_0",
        )
        monkeypatch.setattr(
            overview,
            "gather_overview_local",
            lambda *args, **kwargs: driver_overview,
        )
        monkeypatch.setattr(overview, "list_jobs", lambda *args, **kwargs: [])
        monkeypatch.setattr(overview, "_helper_source_sha", lambda host: None)

        projected_overview = overview.gather_scheduler_overview(
            "host_f",
            cfg.host("host_f"),
            cfg,
        )

        assert (projected_overview.admin_marker is marker) is projected
        assert (
            projected_overview.admin_marker_stale_reason is not None
        ) is projected
        text = overview.format_overview_text(projected_overview)
        assert ("admin update marker" in text) is projected
        assert ("`vq admin clear-update-marker host_f`" in text) is projected

    def test_scheduler_projection_keeps_later_unreadable_global_lease(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A foreign first lease cannot hide a later fail-closed marker."""
        foreign = self._marker(
            envs=["scheduler:host_c"],
            host="host_c",
        )
        source = overview.HostOverview(
            host="host_0",
            admin_marker=foreign,
            admin_markers=[
                overview.AdminMarkerSnapshot(
                    marker=foreign,
                    status="running",
                    summary="host_c update is running",
                ),
                overview.AdminMarkerSnapshot(
                    marker=None,
                    status="unreadable",
                    summary="marker file is present but cannot be parsed",
                ),
            ],
        )
        driver_overview = overview._overview_from_json(
            "host_0",
            overview.format_overview_json(source),
        )
        cfg = config.Config(
            hosts={
                "host_f": config.HostConfig(
                    ssh="host_f",
                    scheduler="pbs",
                    scheduler_dialect="torque",
                    scheduler_driver="host_0",
                    scratch_root="/scratch/USER",
                ),
                "host_0": config.HostConfig(ssh="host_0"),
            }
        )
        monkeypatch.setattr(
            overview,
            "is_local_host",
            lambda host: host == "host_0",
        )
        monkeypatch.setattr(
            overview,
            "gather_overview_local",
            lambda *args, **kwargs: driver_overview,
        )
        monkeypatch.setattr(overview, "list_jobs", lambda *args, **kwargs: [])
        monkeypatch.setattr(overview, "_helper_source_sha", lambda host: None)

        projected = overview.gather_scheduler_overview(
            "host_f",
            cfg.host("host_f"),
            cfg,
        )
        text = overview.format_overview_text(projected)

        assert projected.admin_marker is None
        assert projected.admin_marker_status == "unreadable"
        assert "admin update marker" in text
        assert "dispatch impact is unknown" in text
        assert "scheduler:host_c" not in text

    def test_scheduler_projection_prioritises_global_over_matching_lease(
        self,
    ) -> None:
        """A narrower true hold cannot hide a later fail-closed global one."""
        matching = self._marker(envs=["scheduler:host_f"], host="host_f")
        source = overview.HostOverview(
            host="driver",
            admin_marker=matching,
            admin_markers=[
                overview.AdminMarkerSnapshot(
                    marker=matching,
                    status="running",
                    summary="host_f update is running",
                ),
                overview.AdminMarkerSnapshot(
                    marker=None,
                    status="unreadable",
                    summary="marker file is present but cannot be parsed",
                ),
            ],
        )

        selected = overview._select_admin_marker_snapshot_for_target(
            source,
            "host_f",
        )

        assert selected is not None
        assert selected.marker is None
        assert selected.status == "unreadable"

    @pytest.mark.parametrize(
        "bad_registry",
        [
            pytest.param(["bad-member"], id="invalid-list-member"),
            pytest.param({"not": "a-list"}, id="invalid-top-level-type"),
        ],
    )
    def test_remote_malformed_marker_registry_fails_closed(
        self,
        bad_registry: object,
    ) -> None:
        """A corrupt new-peer registry cannot fall back to a foreign lease."""
        foreign = self._marker(envs=["scheduler:host_c"], host="host_c")
        payload = overview.format_overview_json(
            overview.HostOverview(host="driver", admin_marker=foreign)
        )
        payload["admin_markers"] = bad_registry

        rebuilt = overview._overview_from_json("driver", payload)
        selected = overview._select_admin_marker_snapshot_for_target(
            rebuilt,
            "host_f",
        )

        assert selected is not None
        assert selected.marker is None
        assert selected.status == "unreadable"

    def test_scheduler_projection_labels_unknown_scope_stale_corpse_informational(
        self,
    ) -> None:
        """A marker the daemon auto-reaps must never become a global hold."""
        marker = self._marker(envs=["future:scope"], host="driver")
        snapshot = overview.AdminMarkerSnapshot(
            marker=marker,
            stale_reason="the updater process is gone",
            status="stale",
            summary="ordinary stale marker awaits auto-reap",
        )
        source = overview.HostOverview(
            host="driver",
            admin_marker=marker,
            admin_markers=[snapshot],
        )

        selected = overview._select_admin_marker_snapshot_for_target(
            source,
            "host_f",
        )

        assert selected is snapshot
        projected = overview.HostOverview(host="host_f", is_scheduler_host=True)
        overview._apply_admin_marker_snapshot(projected, selected)
        text = overview.format_overview_text(projected)
        assert "STALE" in text
        assert "Not gating dispatch" in text
        assert "dispatch impact is unknown" not in text

    def test_text_render_quiet_when_marker_live(self) -> None:
        ov = overview.HostOverview(
            host="host_e",
            vq_version="0.10.0",
            admin_marker=self._marker(),
            admin_marker_stale_reason=None,
            admin_marker_status="running",
            admin_marker_pid_status="pid=66408 appears alive",
            admin_marker_heartbeat_status=(
                "last heartbeat 2m ago: scheduler update command still running"
            ),
            admin_marker_summary="admin update is already running on this host",
        )
        text = overview.format_overview_text(ov)
        assert "admin update marker" in text
        assert "marker_status=running" in text
        assert "pid=66408 appears alive" in text
        assert "scheduler update command still running" in text
        assert "STALE" not in text

    def test_json_round_trip_preserves_reason(self) -> None:
        ov = overview.HostOverview(
            host="host_e",
            vq_version="0.10.0",
            admin_marker=self._marker(),
            admin_marker_stale_reason="pid=66408 is no longer running",
            admin_marker_status="stale",
            admin_marker_summary="admin-update marker is stale",
            admin_marker_action="clear it after inspection",
            admin_marker_pid_status="pid=66408 is not running",
            admin_marker_heartbeat_status="last heartbeat 1.0h ago",
            admin_marker_heartbeat_age_seconds=3600.0,
        )
        payload = overview.format_overview_json(ov)
        assert payload["admin_marker_stale_reason"] == (
            "pid=66408 is no longer running"
        )
        assert payload["admin_marker_status"] == "stale"
        assert payload["admin_marker_heartbeat_status"] == "last heartbeat 1.0h ago"
        back = overview._overview_from_json("host_e", payload)
        assert back.admin_marker_stale_reason == "pid=66408 is no longer running"
        assert back.admin_marker_status == "stale"
        assert back.admin_marker_pid_status == "pid=66408 is not running"
        assert back.admin_marker_heartbeat_age_seconds == 3600.0

    @pytest.mark.parametrize(
        ("durable_overrides", "expected_command", "excluded_command"),
        [
            pytest.param(
                {"managed_transaction": {"schema": "test-managed-receipt"}},
                "`vq admin recover-update host_e`",
                "`vq admin clear-update-marker host_e`",
                id="managed-transaction",
            ),
            pytest.param(
                {"pause_token": "admin-update-0123456789ab"},
                "`vq admin clear-update-marker host_e`",
                "`vq admin recover-update host_e`",
                id="pause-token",
            ),
            pytest.param(
                {"paused_jobids": ["job-1"], "surgical_pause": True},
                "`vq admin clear-update-marker host_e`",
                "`vq admin recover-update host_e`",
                id="paused-jobids",
            ),
        ],
    )
    def test_json_round_trip_preserves_durable_recovery_rendering(
        self,
        durable_overrides: dict[str, object],
        expected_command: str,
        excluded_command: str,
    ) -> None:
        marker = self._marker(**durable_overrides)
        ov = overview.HostOverview(
            host="host_e",
            admin_marker=marker,
            admin_marker_stale_reason="the updater process is gone",
            admin_marker_action="Run `vq admin recover-update wrong-host`.",
        )

        rebuilt = overview._overview_from_json(
            "host_e",
            overview.format_overview_json(ov),
        )
        text = overview.format_overview_text(rebuilt)

        assert rebuilt.admin_marker is not None
        assert (
            rebuilt.admin_marker.managed_transaction is not None
            or rebuilt.admin_marker.owns_pause_scope
        )
        assert expected_command in text
        assert excluded_command not in text
        assert "wrong-host" not in text
        assert "Never use --force" in text
        assert "Not gating dispatch" not in text
        assert "auto-reaps" not in text
        assert "Clear by hand:" not in text

    @pytest.mark.parametrize(
        "bad_marker",
        [
            pytest.param([], id="non-mapping"),
            pytest.param({"envs": ["vibeqc-dev"]}, id="incomplete-mapping"),
        ],
    )
    def test_json_reconstruction_failure_ignores_carried_action(
        self,
        bad_marker: object,
    ) -> None:
        payload = overview.format_overview_json(
            overview.HostOverview(host="host_e")
        )
        payload["admin_marker"] = bad_marker
        payload["admin_marker_stale_reason"] = "remote marker is stale"
        payload["admin_marker_action"] = (
            "Run `vq admin recover-update`, then use "
            "`vq admin clear-update-marker` only if instructed."
        )

        rebuilt = overview._overview_from_json("host_e", payload)
        text = overview.format_overview_text(rebuilt)

        assert rebuilt.admin_marker is None
        assert "remote marker is stale" in text
        assert "`vq admin status host_e --verbose`" in text
        assert "recover-update" not in text
        assert "clear-update-marker" not in text
        assert "Never use --force" in text
        assert "Not gating dispatch" not in text
        assert "auto-reaps" not in text

    @pytest.mark.parametrize(
        "bad_action",
        [
            pytest.param(["bad"], id="list"),
            pytest.param({"command": "bad"}, id="mapping"),
            pytest.param(7, id="integer"),
            pytest.param(True, id="boolean"),
        ],
    )
    def test_json_malformed_marker_action_uses_safe_fallback(
        self,
        bad_action: object,
    ) -> None:
        marker = self._marker(
            managed_transaction={"schema": "test-managed-receipt"},
        )
        payload = overview.format_overview_json(
            overview.HostOverview(
                host="host_e",
                admin_marker=marker,
                admin_marker_stale_reason="remote marker is stale",
            )
        )
        payload["admin_marker_action"] = bad_action

        rebuilt = overview._overview_from_json("host_e", payload)
        text = overview.format_overview_text(rebuilt)

        assert rebuilt.admin_marker_action is None
        assert "`vq admin recover-update host_e`" in text
        assert "Never use --force" in text
        assert "auto-reaps" not in text

    @pytest.mark.parametrize(
        "bad_envs",
        [
            pytest.param(7, id="not-a-list"),
            pytest.param([7], id="non-string-entry"),
        ],
    )
    def test_json_wrong_typed_marker_scope_degrades_to_unavailable(
        self,
        bad_envs: object,
    ) -> None:
        marker = self._marker(
            managed_transaction={"schema": "test-managed-receipt"},
        )
        payload = overview.format_overview_json(
            overview.HostOverview(
                host="host_e",
                admin_marker=marker,
                admin_marker_stale_reason="remote marker is stale",
                admin_marker_status="stale",
            )
        )
        assert isinstance(payload["admin_marker"], dict)
        payload["admin_marker"]["envs"] = bad_envs

        rebuilt = overview._overview_from_json("host_e", payload)
        text = overview.format_overview_text(rebuilt)

        assert rebuilt.admin_marker is None
        assert "details=unavailable" in text
        assert "dispatch impact is unknown" in text
        assert "`vq admin status host_e --verbose`" in text
        assert "auto-reaps" not in text

    def test_untrusted_scheduler_host_is_not_used_in_action(self) -> None:
        untrusted_host = "host_f --force"
        marker = self._marker(
            envs=[f"scheduler:{untrusted_host}"],
            host=untrusted_host,
            pause_token="admin-update-0123456789ab",
        )
        ov = overview.HostOverview(
            host="host_0",
            admin_marker=marker,
            admin_marker_stale_reason="the updater process is gone",
        )

        text = overview.format_overview_text(ov)

        assert overview._admin_marker_action_target(ov) == "host_0"
        assert "`vq admin clear-update-marker host_0`" in text
        assert "`vq admin clear-update-marker host_f --force`" not in text

    def test_remote_unreadable_marker_round_trip_stays_visible(self) -> None:
        source = overview.HostOverview(
            host="host_e",
            admin_marker_status="unreadable",
            admin_marker_summary="marker file is present but cannot be parsed",
            admin_marker_action=(
                "Inspect the marker file, then run "
                "`vq admin clear-update-marker` only after verification."
            ),
        )

        rebuilt = overview._overview_from_json(
            "host_e",
            overview.format_overview_json(source),
        )
        text = overview.format_overview_text(rebuilt)

        assert rebuilt.admin_marker is None
        assert rebuilt.admin_marker_stale_reason is None
        assert "admin update marker: marker_status=unreadable" in text
        assert "`vq admin status host_e --verbose`" in text
        assert "clear-update-marker" not in text
        assert "Never use --force" in text
        assert "auto-reaps" not in text

    def test_gather_and_render_unreadable_marker_without_stale_reason(
        self,
        state_dir: Path,
    ) -> None:
        marker_path = admin.admin_update_marker_path()
        marker_path.parent.mkdir(parents=True, exist_ok=True)
        marker_path.write_text("{not-json", encoding="utf-8")

        ov = overview.gather_overview_local("localhost", config.load_config())
        text = overview.format_overview_text(ov)

        assert ov.admin_marker is None
        assert ov.admin_marker_status == "unreadable"
        assert ov.admin_marker_stale_reason is None
        assert "admin update marker: marker_status=unreadable" in text
        assert "marker file is present but cannot be parsed" in text
        assert "MARKER DETAILS UNAVAILABLE" in text
        assert "`vq admin status localhost --verbose`" in text
        assert "clear-update-marker" not in text
        assert "Never use --force" in text
        assert "auto-reaps" not in text

    def test_gather_computes_stale_reason_host_side(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        admin.write_admin_update_marker(envs=["vibeqc-dev"], host="localhost")
        monkeypatch.setattr(admin, "_pid_liveness", lambda pid: False)
        cfg = config.load_config()
        ov = overview.gather_overview_local("localhost", cfg)
        assert ov.admin_marker is not None
        assert ov.admin_marker_stale_reason is not None
        assert ov.admin_marker_status == "stale"
        assert ov.admin_marker_pid_status is not None
        assert "is not running" in ov.admin_marker_pid_status
        assert ov.admin_marker_heartbeat_status is not None
