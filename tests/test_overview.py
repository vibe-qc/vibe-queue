"""Tests for vq.overview (v0.6.21 fleet summary verb)."""
from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import config, lifecycle, overview, paths
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
    state: JobState,
    *,
    finished_at: str | None = None,
) -> JobSpec:
    queue = paths.queue_dir()
    jobs = paths.jobs_dir()
    workspace = jobs / jobid
    workspace.mkdir(exist_ok=True)
    spec = JobSpec(
        id=jobid,
        command=["echo", "hi"],
        cwd=str(workspace),
        cpus=1,
        state=state,
        finished_at=finished_at,
    )
    spec.write(queue / f"{jobid}.json")
    return spec


class TestCountSpecs:
    """Pure-function spec partitioning into current vs recent-terminal."""

    def test_queue_counts_includes_every_state(self) -> None:
        now = datetime(2026, 5, 18, 12, 0, tzinfo=UTC)
        specs = [
            JobSpec(id="a", command=["x"], cwd=".", cpus=1, state=JobState.RUNNING),
            JobSpec(id="b", command=["x"], cwd=".", cpus=1, state=JobState.PENDING),
            JobSpec(id="c", command=["x"], cwd=".", cpus=1, state=JobState.COMPLETED),
            JobSpec(id="d", command=["x"], cwd=".", cpus=1, state=JobState.RUNNING),
        ]
        queue, recent, last_terminal, running_cpus, pending_cpus = (
            overview._count_specs(
                specs, recent_window=timedelta(hours=24), now=now,
            )
        )
        assert queue == {"running": 2, "pending": 1, "completed": 1}
        # v0.7.18: load fields too — 2 RUNNING × 1 cpu = 2, 1 PENDING × 1.
        assert running_cpus == 2
        assert pending_cpus == 1

    def test_recent_terminal_only_within_window(self) -> None:
        now = datetime(2026, 5, 18, 12, 0, tzinfo=UTC)
        # 12 hours ago — inside the 24h window
        recent = (now - timedelta(hours=12)).isoformat()
        # 36 hours ago — outside the 24h window
        old = (now - timedelta(hours=36)).isoformat()
        specs = [
            JobSpec(
                id="a", command=["x"], cwd=".", cpus=1,
                state=JobState.COMPLETED, finished_at=recent,
            ),
            JobSpec(
                id="b", command=["x"], cwd=".", cpus=1,
                state=JobState.FAILED, finished_at=old,
            ),
            JobSpec(
                id="c", command=["x"], cwd=".", cpus=1,
                state=JobState.RUNNING,
            ),
        ]
        _, recent_counts, _, _, _ = overview._count_specs(
            specs, recent_window=timedelta(hours=24), now=now,
        )
        assert recent_counts == {"completed": 1}

    def test_terminal_without_finished_at_is_skipped(self) -> None:
        """ABORTED_BY_QUEUE often has no finished_at (daemon never
        saw the exit). Such specs aren't in 'recent' counts since
        we can't bound them to the window."""
        now = datetime(2026, 5, 18, 12, 0, tzinfo=UTC)
        specs = [
            JobSpec(
                id="a", command=["x"], cwd=".", cpus=1,
                state=JobState.ABORTED_BY_QUEUE, finished_at=None,
            ),
        ]
        _, recent, _, _, _ = overview._count_specs(
            specs, recent_window=timedelta(hours=24), now=now,
        )
        assert recent == {}

    def test_unparseable_finished_at_is_skipped(self) -> None:
        now = datetime(2026, 5, 18, 12, 0, tzinfo=UTC)
        specs = [
            JobSpec(
                id="a", command=["x"], cwd=".", cpus=1,
                state=JobState.COMPLETED, finished_at="not iso 8601",
            ),
        ]
        _, recent, _, _, _ = overview._count_specs(
            specs, recent_window=timedelta(hours=24), now=now,
        )
        assert recent == {}


class TestGatherOverviewLocal:
    def test_smoke_minimal_overview(self, state_dir: Path) -> None:
        cfg = config.load_config()
        ov = overview.gather_overview_local("localhost", cfg)
        # Always-set fields
        assert ov.host == "localhost"
        assert ov.reachable is True
        assert ov.vq_version is not None  # from vq.__version__
        # Empty queue + no envs configured → zero counts + empty list
        assert ov.queue_counts == {}
        assert ov.recent_terminal_counts == {}
        assert ov.envs == []
        # daemon_health is a real ContractVerdict (always present
        # post-call, even when systemctl is unavailable)
        assert ov.daemon_health is not None

    def test_counts_specs_from_queue_dir(self, state_dir: Path) -> None:
        _make_spec("aaa00000aaaa", JobState.RUNNING)
        _make_spec("bbb00000bbbb", JobState.PENDING)
        now = datetime.now(UTC)
        completed_at = (now - timedelta(hours=1)).isoformat()
        _make_spec("ccc00000cccc", JobState.COMPLETED, finished_at=completed_at)

        cfg = config.load_config()
        ov = overview.gather_overview_local("localhost", cfg)
        assert ov.queue_counts.get("running") == 1
        assert ov.queue_counts.get("pending") == 1
        assert ov.queue_counts.get("completed") == 1
        assert ov.recent_terminal_counts.get("completed") == 1


class TestFormatOverviewText:
    def test_unreachable_host_one_liner(self) -> None:
        ov = overview.HostOverview(
            host="host_d", reachable=False, error="ssh: timeout",
        )
        text = overview.format_overview_text(ov)
        assert "ERROR" in text
        assert "ssh: timeout" in text
        assert "host_d" in text

    def test_reachable_host_renders_sections(self) -> None:
        ov = overview.HostOverview(
            host="host_d",
            vq_version="0.6.21",
            queue_counts={"running": 2, "pending": 1, "completed": 5},
        )
        text = overview.format_overview_text(ov)
        assert "host_d (vq 0.6.21)" in text
        assert "running" in text
        assert "pending" in text
        # Counts must appear
        assert "2" in text
        assert "1" in text

    def test_includes_memory_pressure_when_available(self) -> None:
        verdict = lifecycle.ContractVerdict(
            ok=True, manager_pid=1234,
            loginctl_state="active", loginctl_runtime_path="/run/user/1000",
            systemctl_user_reachable=True,
            vq_daemon_state="active", vq_daemon_main_pid=4321,
            daemon_pidfile_pid=4321, daemon_process_alive=True,
            memory_pressure_pct=42.5,
        )
        ov = overview.HostOverview(
            host="host_d", vq_version="0.6.21", daemon_health=verdict,
        )
        text = overview.format_overview_text(ov)
        assert "memory pressure" in text
        assert "42.5%" in text

    def test_macos_rpc_liveness_verdict_renders_ok_with_pidfile(self) -> None:
        """Regression: on a macOS guarded node the daemon has no systemd
        MainPID, so the verdict carries vq_daemon_main_pid=None +
        daemon_pidfile_pid=<pid>. A healthy (ok=True) such verdict must
        render `daemon: OK ... (pidfile)`, NOT the old false-negative
        `daemon: FAIL ... (pidfile)`. Mirrors the shape
        lifecycle._verify_via_rpc_liveness() produces for a responsive
        manually-started daemon."""
        verdict = lifecycle.ContractVerdict(
            ok=True, manager_pid=None,
            loginctl_state=None, loginctl_runtime_path=None,
            systemctl_user_reachable=False,
            vq_daemon_state=None, vq_daemon_main_pid=None,
            daemon_pidfile_pid=4321, daemon_process_alive=True,
        )
        ov = overview.HostOverview(
            host="localhost", vq_version="0.11.0", daemon_health=verdict,
        )
        text = overview.format_overview_text(ov)
        assert "daemon:        OK, daemon pid=4321 (pidfile)" in text
        assert "FAIL" not in text


class TestFormatOverviewJson:
    def test_schema_includes_all_top_level_fields(self, state_dir: Path) -> None:
        cfg = config.load_config()
        ov = overview.gather_overview_local("localhost", cfg)
        payload = overview.format_overview_json(ov)
        for key in (
            "host", "reachable", "error", "vq_version",
            "queue_counts", "recent_terminal_counts",
            "daemon_health", "envs", "admin_marker",
        ):
            assert key in payload, f"missing key {key}"

    def test_fleet_json_wraps_in_hosts_array(self) -> None:
        ovs = [
            overview.HostOverview(host="a", vq_version="0.6.21"),
            overview.HostOverview(host="b", reachable=False, error="down"),
        ]
        text = overview.format_fleet_overview_json(ovs)
        payload = json.loads(text)
        assert "hosts" in payload
        assert len(payload["hosts"]) == 2
        assert payload["hosts"][0]["host"] == "a"
        assert payload["hosts"][1]["reachable"] is False


class TestOverviewCLI:
    def test_local_overview_text(self, state_dir: Path) -> None:
        _make_spec("clicli000001", JobState.RUNNING)
        result = CliRunner().invoke(main, ["overview", "localhost"])
        assert result.exit_code == 0, result.output
        assert "localhost" in result.output
        assert "running" in result.output

    def test_local_overview_json(self, state_dir: Path) -> None:
        _make_spec("clicli000002", JobState.PENDING)
        result = CliRunner().invoke(main, ["overview", "localhost", "--json"])
        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert "hosts" in payload
        assert payload["hosts"][0]["queue_counts"]["pending"] == 1

    def test_help_mentions_key_capabilities(self) -> None:
        result = CliRunner().invoke(main, ["overview", "--help"])
        assert result.exit_code == 0
        output_lower = result.output.lower()
        assert "health" in output_lower
        assert "running" in output_lower
        assert "queued" in output_lower


class TestQueueJsonFlag:
    """v0.6.21 prerequisite: --json on vq queue."""

    def test_queue_json_returns_array(self, state_dir: Path) -> None:
        _make_spec("queuejson001", JobState.PENDING)
        result = CliRunner().invoke(
            main, ["queue", "localhost", "--json"]
        )
        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert isinstance(payload, list)
        assert len(payload) == 1
        assert payload[0]["id"] == "queuejson001"
        assert payload[0]["state"] == "pending"
        assert payload[0]["queue_handle"] == {
            "job_id": "queuejson001",
            "host": "localhost",
            "submitted_at": payload[0]["submitted_at"],
        }
        assert payload[0]["terminal_diagnosis"] is None

    def test_queue_json_includes_terminal_diagnosis(
        self, state_dir: Path
    ) -> None:
        spec = _make_spec("queuejson004", JobState.FAILED)
        spec.exit_code = 137
        spec.write(paths.queue_dir() / f"{spec.id}.json")
        result = CliRunner().invoke(
            main, ["queue", "localhost", "--json"]
        )
        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload[0]["id"] == "queuejson004"
        assert payload[0]["terminal_diagnosis"]["category"] == "sigkill"
        assert (
            payload[0]["terminal_diagnosis"]["action_hint"]
            == "increase_memory_or_check_external_kill"
        )

    def test_queue_json_respects_state_filter(self, state_dir: Path) -> None:
        _make_spec("queuejson002", JobState.RUNNING)
        _make_spec("queuejson003", JobState.COMPLETED)
        result = CliRunner().invoke(
            main, ["queue", "localhost", "--json", "-s", "running"]
        )
        payload = json.loads(result.output)
        assert len(payload) == 1
        assert payload[0]["id"] == "queuejson002"

    def test_queue_json_empty_array_when_no_jobs(
        self, state_dir: Path
    ) -> None:
        result = CliRunner().invoke(
            main, ["queue", "localhost", "--json"]
        )
        payload = json.loads(result.output)
        assert payload == []


class TestDaemonHealthMemoryPressure:
    """v0.6.21: ContractVerdict now carries memory_pressure_pct;
    the text format surfaces it; the JSON format exposes it under
    the same key."""

    def test_verdict_has_pressure_field(self) -> None:
        verdict = lifecycle.verify_user_systemd_contract()
        # The field exists on the dataclass; value may be None on
        # macOS dev box (no /proc/meminfo) — both are valid.
        assert hasattr(verdict, "memory_pressure_pct")

    def test_json_includes_pressure_key(self) -> None:
        verdict = lifecycle.verify_user_systemd_contract()
        payload = json.loads(lifecycle.format_contract_verdict_json(verdict))
        assert "memory_pressure_pct" in payload


class TestQuotaIsVisibleInOverview:
    """The per-submitter quota gates dispatch and was invisible.

    host_d on 2026-07-27: 32 physical cores, daemon `max_cpus` 32, and a
    `[quotas] default_max_concurrent_cpus = 16` that nothing surfaced. A pending
    24-cpu job sat for hours against an idle queue with no way to see the 16
    that explained it, and the investigation went to daemon health instead.
    """

    def test_quota_is_read_and_serialized(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
        (tmp_path / "cfg").mkdir()
        (tmp_path / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n\n[quotas]\n'
            "default_max_concurrent_cpus = 16\n"
        )

        ov = overview.gather_overview_local("localhost", config.load_config())

        assert ov.quota_max_concurrent_cpus == 16
        payload = overview.format_overview_json(ov)
        assert payload["quota_max_concurrent_cpus"] == 16

    def test_quota_survives_the_remote_forwarding_round_trip(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A fleet operator reads this for a remote host, so it has to make it
        back through the JSON the forwarder rebuilds from."""
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
        (tmp_path / "cfg").mkdir()
        (tmp_path / "cfg" / "config.toml").write_text('default_host = "localhost"\n')

        source = overview.HostOverview(host="host_d")
        source.quota_max_concurrent_cpus = 16
        rebuilt = overview._overview_from_json(
            "host_d", overview.format_overview_json(source)
        )

        assert rebuilt.quota_max_concurrent_cpus == 16

    def test_no_quota_configured_stays_none(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Unlimited must read as unlimited, not as zero."""
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
        (tmp_path / "cfg").mkdir()
        (tmp_path / "cfg" / "config.toml").write_text('default_host = "localhost"\n')

        ov = overview.gather_overview_local("localhost", config.load_config())

        assert ov.quota_max_concurrent_cpus is None


class TestSchedulerHostReportsItsOwnHelper:
    """A scheduler host must not be labelled with the driver's vq version.

    Scheduler hosts are daemonless, so `gather_scheduler_overview` used to copy
    `driver_overview.vq_version` onto them. But host_f and host_c each run their
    own helper vq -- the one that polls, reconciles and dispatches -- and its
    skew from the driver was a whole incident. Showing the driver's number hid
    exactly the disagreement an operator needed to see.
    """

    def test_helper_sha_is_reported_and_driver_version_is_not_borrowed(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        sha = "1" * 40
        record = type("R", (), {"actual_sha": sha})()
        monkeypatch.setattr(
            overview.admin,
            "load_scheduler_runtime_status",
            lambda: {"host_f:vq-helper": record},
        )

        assert overview._helper_source_sha("host_f") == sha[:12]

        ov = overview.HostOverview(host="host_f")
        ov.helper_source_sha = sha[:12]
        text = overview.format_overview_text(ov)

        assert "vq helper 111111111111" in text
        # The driver's version is not presented as this host's.
        assert ov.vq_version is None

    def test_absent_record_is_not_invented(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A helper deployed before canonical bookkeeping has no record; that
        is a legitimate state, not something to fill in with a guess."""
        monkeypatch.setattr(
            overview.admin, "load_scheduler_runtime_status", lambda: {}
        )

        assert overview._helper_source_sha("host_f") is None

        ov = overview.HostOverview(host="host_f")
        assert "vq ?" in overview.format_overview_text(ov)

    def test_a_short_or_malformed_sha_is_rejected(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        record = type("R", (), {"actual_sha": "abc123"})()
        monkeypatch.setattr(
            overview.admin,
            "load_scheduler_runtime_status",
            lambda: {"host_f:vq-helper": record},
        )

        assert overview._helper_source_sha("host_f") is None

    def test_helper_sha_survives_the_json_round_trip(self) -> None:
        source = overview.HostOverview(host="host_f")
        source.helper_source_sha = "1" * 12

        rebuilt = overview._overview_from_json(
            "host_f", overview.format_overview_json(source)
        )

        assert rebuilt.helper_source_sha == "1" * 12

    def test_a_local_host_still_shows_its_own_vq_version(self) -> None:
        """Unchanged for every non-scheduler host."""
        ov = overview.HostOverview(host="localhost")
        ov.vq_version = "0.21.0"

        assert "(vq 0.21.0)" in overview.format_overview_text(ov)
