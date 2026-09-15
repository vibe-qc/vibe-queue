"""Tests for `vq usage` CPU-hour accounting."""
from __future__ import annotations

import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import config, host_status, paths, transport, usage
from vq.cli import main
from vq.spec import JobSpec, JobState

_NOW = datetime(2026, 6, 30, 12, 0, 0, tzinfo=UTC)


def _spec(
    jobid: str,
    *,
    cpus: int = 1,
    state: JobState = JobState.COMPLETED,
    started_at: str | None = "2026-06-30T10:00:00+00:00",
    finished_at: str | None = "2026-06-30T11:00:00+00:00",
    **fields: object,
) -> JobSpec:
    base: dict[str, object] = {
        "id": jobid,
        "command": ["python", "job.py"],
        "cwd": f"/tmp/{jobid}",
        "cpus": cpus,
        "state": state,
        "started_at": started_at,
        "finished_at": finished_at,
    }
    base.update(fields)
    return JobSpec(**base)


def _write_spec(spec: JobSpec) -> None:
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    spec.write(paths.spec_path(spec.id))


class TestUsageAccounting:
    def test_groups_by_tag_and_total_counts_jobs_once(self) -> None:
        specs = [
            _spec("job-a", cpus=2, tags=["foo", "bar"]),
            _spec(
                "job-b",
                cpus=1,
                started_at="2026-06-30T10:00:00+00:00",
                finished_at="2026-06-30T10:30:00+00:00",
                tags=["foo"],
            ),
        ]

        report = usage.build_usage_report(specs, host="localhost", now=_NOW)

        rows = {row.group: row for row in report.rows}
        assert rows["foo"].jobs == 2
        assert rows["foo"].cpu_hours == pytest.approx(2.5)
        assert rows["bar"].jobs == 1
        assert rows["bar"].cpu_hours == pytest.approx(2.0)
        assert report.total.jobs == 2
        assert report.total.cpu_hours == pytest.approx(2.5)

    def test_scheduler_walltime_used_preferred_for_cluster_jobs(self) -> None:
        spec = _spec(
            "host_f-job",
            cpus=4,
            started_at="2026-06-30T00:00:00+00:00",
            finished_at="2026-06-30T10:00:00+00:00",
            scheduler_target="host_f",
            scheduler_walltime_used="02:00:00",
        )

        report = usage.build_usage_report(
            [spec],
            host="host_f",
            group_by="host",
            now=_NOW,
        )

        assert report.total.wall_hours == pytest.approx(2.0)
        assert report.total.cpu_hours == pytest.approx(8.0)
        assert report.rows[0].group == "host_f"

    def test_include_active_counts_running_and_current_pause(self) -> None:
        running = _spec(
            "running",
            state=JobState.RUNNING,
            started_at="2026-06-30T11:00:00+00:00",
            finished_at=None,
            cpus=2,
        )
        suspended = _spec(
            "suspended",
            state=JobState.SUSPENDED,
            started_at="2026-06-30T10:00:00+00:00",
            finished_at=None,
            cpus=1,
            paused_at="2026-06-30T11:00:00+00:00",
        )

        without_active = usage.build_usage_report(
            [running, suspended],
            host="localhost",
            include_active=False,
            now=_NOW,
        )
        with_active = usage.build_usage_report(
            [running, suspended],
            host="localhost",
            include_active=True,
            now=_NOW,
        )

        assert without_active.total.jobs == 0
        assert without_active.skipped_jobs == 2
        assert with_active.total.jobs == 2
        assert with_active.total.wall_hours == pytest.approx(2.0)
        assert with_active.total.cpu_hours == pytest.approx(3.0)

    def test_json_contains_seconds_and_hours(self) -> None:
        report = usage.build_usage_report(
            [_spec("job-a", cpus=2, submitter="alice")],
            host="localhost",
            group_by="submitter",
            now=_NOW,
        )

        payload = json.loads(usage.format_usage_json(report))

        assert payload["host"] == "localhost"
        assert payload["group_by"] == "submitter"
        assert payload["rows"][0]["group"] == "alice"
        assert payload["rows"][0]["wall_seconds"] == 3600.0
        assert payload["rows"][0]["cpu_hours"] == 2.0

    def test_table_mentions_skipped_jobs(self) -> None:
        report = usage.build_usage_report(
            [_spec("bad", started_at=None, finished_at=None)],
            host="localhost",
            now=_NOW,
        )

        out = usage.format_usage_table(report)

        assert "(no usage records)" in out
        assert "skipped jobs without usable timing: 1" in out


class TestUsageCLI:
    def test_local_usage_json(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
        cfg_dir = tmp_path / "cfg"
        cfg_dir.mkdir()
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(cfg_dir))
        _write_spec(_spec("job-a", cpus=2, submitter="alice"))

        result = CliRunner().invoke(
            main,
            ["usage", "localhost", "--by", "submitter", "--json"],
        )

        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["rows"][0]["group"] == "alice"
        assert payload["total"]["cpu_hours"] == 2.0

    def test_scheduler_host_filters_driver_specs(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
        cfg_dir = tmp_path / "cfg"
        cfg_dir.mkdir()
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(cfg_dir))
        (cfg_dir / "config.toml").write_text(
            "[hosts.localhost]\n"
            'ssh = "localhost"\n'
            "\n"
            "[hosts.host_f]\n"
            'ssh = "host_f.invalid"\n'
            'scheduler = "pbs"\n'
            'scheduler_dialect = "torque"\n'
            'scratch_root = "/home/USER"\n'
            'scheduler_driver = "localhost"\n'
        )
        _write_spec(
            _spec(
                "host_f-job",
                cpus=4,
                scheduler_target="host_f",
                scheduler_walltime_used="02:00:00",
            )
        )
        _write_spec(_spec("local-job", cpus=8))

        result = CliRunner().invoke(main, ["usage", "host_f", "--by", "host", "--json"])

        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["host"] == "host_f"
        assert payload["total"]["jobs"] == 1
        assert payload["total"]["cpu_hours"] == 8.0
        assert payload["rows"][0]["group"] == "host_f"

    def test_remote_host_delegates_to_remote_vq(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        cfg_dir = tmp_path / "cfg"
        cfg_dir.mkdir()
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(cfg_dir))
        (cfg_dir / "config.toml").write_text(
            "[hosts.remote-test]\n"
            'ssh = "remote.example.com"\n'
            'remote_vq = "vq"\n'
        )
        captured: list[tuple[str, ...]] = []

        def fake_run_remote_vq(host_cfg, *args, **kwargs):
            captured.append(tuple(args))
            return subprocess.CompletedProcess(
                args=["vq", *args],
                returncode=0,
                stdout='{"rows": [], "total": {"jobs": 0}}\n',
                stderr="",
            )

        monkeypatch.setattr(transport, "run_remote_vq", fake_run_remote_vq)

        result = CliRunner().invoke(
            main,
            ["usage", "remote-test", "--by", "submitter", "--include-active", "--json"],
        )

        assert result.exit_code == 0, result.output
        assert captured == [
            ("usage", "localhost", "--by", "submitter", "--include-active", "--json")
        ]

    def test_default_remote_host_delegates_to_remote_vq(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        cfg_dir = tmp_path / "cfg"
        cfg_dir.mkdir()
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(cfg_dir))
        (cfg_dir / "config.toml").write_text(
            'default_host = "remote-test"\n'
            "\n"
            "[hosts.remote-test]\n"
            'ssh = "remote.example.com"\n'
            'remote_vq = "vq"\n'
        )
        captured: list[tuple[str, ...]] = []

        def fake_run_remote_vq(host_cfg, *args, **kwargs):
            captured.append(tuple(args))
            return subprocess.CompletedProcess(
                args=["vq", *args],
                returncode=0,
                stdout="remote usage\n",
                stderr="",
            )

        monkeypatch.setattr(transport, "run_remote_vq", fake_run_remote_vq)

        result = CliRunner().invoke(main, ["usage"])

        assert result.exit_code == 0, result.output
        assert result.output.strip() == "remote usage"
        assert captured == [("usage", "localhost", "--by", "tag")]

    def test_default_remote_transport_failure_falls_back_to_localhost(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
        cfg_dir = tmp_path / "cfg"
        cfg_dir.mkdir()
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(cfg_dir))
        (cfg_dir / "config.toml").write_text(
            'default_host = "remote-test"\n'
            "\n"
            "[hosts.remote-test]\n"
            'ssh = "remote.example.com"\n'
            'remote_vq = "vq"\n'
        )
        captured: list[tuple[str, ...]] = []

        def fake_run_remote_vq(host_cfg, *args, **kwargs):
            captured.append(tuple(args))
            raise transport.RemoteError(
                "remote vq failed (exit 255) on remote-test: "
                "ssh: Network is unreachable"
            )

        monkeypatch.setattr(transport, "run_remote_vq", fake_run_remote_vq)

        result = CliRunner().invoke(main, ["usage"])

        assert result.exit_code == 0, result.output
        assert "(no usage records)" in result.output
        combined = result.output + result.stderr
        assert "default_host 'remote-test' is unreachable" in combined
        assert "showing localhost usage" in combined
        assert captured == [("usage", "localhost", "--by", "tag")]

    def test_default_remote_marked_down_skips_remote_probe(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
        cfg_dir = tmp_path / "cfg"
        cfg_dir.mkdir()
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(cfg_dir))
        (cfg_dir / "config.toml").write_text(
            'default_host = "remote-test"\n'
            "\n"
            "[hosts.remote-test]\n"
            'ssh = "remote.example.com"\n'
            'remote_vq = "vq"\n'
        )
        captured: list[tuple[str, ...]] = []

        def fake_run_remote_vq(host_cfg, *args, **kwargs):
            captured.append(tuple(args))
            raise AssertionError("down default_host should not be probed")

        monkeypatch.setattr(transport, "run_remote_vq", fake_run_remote_vq)
        host_status.mark_down("remote-test", "off network")

        result = CliRunner().invoke(main, ["usage"])

        assert result.exit_code == 0, result.output
        assert "(no usage records)" in result.output
        combined = result.output + result.stderr
        assert "default_host 'remote-test' is marked down" in combined
        assert "showing localhost usage" in combined
        assert captured == []

    def test_default_remote_json_stays_strict(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
        cfg_dir = tmp_path / "cfg"
        cfg_dir.mkdir()
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(cfg_dir))
        (cfg_dir / "config.toml").write_text(
            'default_host = "remote-test"\n'
            "\n"
            "[hosts.remote-test]\n"
            'ssh = "remote.example.com"\n'
            'remote_vq = "vq"\n'
        )
        captured: list[tuple[str, ...]] = []

        def fake_run_remote_vq(host_cfg, *args, **kwargs):
            captured.append(tuple(args))
            raise transport.RemoteError(
                "remote vq failed (exit 255) on remote-test: "
                "ssh: Network is unreachable"
            )

        monkeypatch.setattr(transport, "run_remote_vq", fake_run_remote_vq)

        result = CliRunner().invoke(main, ["usage", "--json"])

        assert result.exit_code != 0
        assert "remote vq failed" in result.output
        assert captured == [("usage", "localhost", "--by", "tag", "--json")]
