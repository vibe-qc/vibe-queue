"""Tests for `vq top` — the live per-job resource snapshot (v0.9.0)."""
from __future__ import annotations

import json
import math
from datetime import UTC, datetime
from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import config, host_status, paths, top, transport
from vq.cli import main
from vq.spec import JobSpec, JobState

_NOW = datetime(2026, 6, 5, 12, 0, 5, tzinfo=UTC)


def _running_spec(
    root: Path,
    jobid: str,
    *,
    cpu: float | None = None,
    rss: float | None = None,
    ts: str | None = None,
    **fields: object,
) -> JobSpec:
    ws = root / jobid
    (ws / "_vq").mkdir(parents=True, exist_ok=True)
    if cpu is not None or rss is not None or ts is not None:
        rec = {"ts": ts, "cpu_percent": cpu, "rss_mb": rss}
        (ws / "_vq" / "samples.jsonl").write_text(json.dumps(rec) + "\n")
    base: dict[str, object] = {
        "id": jobid, "command": ["crystal"], "cwd": str(ws), "cpus": 1,
        "state": JobState.RUNNING, "started_at": "2026-06-05T11:00:00+00:00",
    }
    base.update(fields)
    return JobSpec(**base)


class TestGatherTopRows:
    def test_reads_latest_sample(self, tmp_path: Path) -> None:
        spec = _running_spec(
            tmp_path, "job000000001", cpu=250.0, rss=4096,
            ts="2026-06-05T12:00:00+00:00",
        )
        rows = top.gather_top_rows([spec], now=_NOW)
        assert len(rows) == 1
        assert rows[0].cpu_percent == 250.0
        assert rows[0].rss_mb == 4096
        assert rows[0].mem_percent is None
        assert rows[0].elapsed_seconds == pytest.approx(3605, abs=2)
        assert rows[0].active_elapsed_seconds == pytest.approx(3605, abs=2)
        assert rows[0].wall_percent is None
        assert rows[0].sample_stale is False

    def test_takes_the_LAST_sample_line(self, tmp_path: Path) -> None:
        spec = _running_spec(tmp_path, "job000000001")
        (tmp_path / "job000000001" / "_vq" / "samples.jsonl").write_text(
            json.dumps({"ts": "2026-06-05T11:59:00+00:00", "cpu_percent": 10.0, "rss_mb": 1})
            + "\n"
            + json.dumps({"ts": "2026-06-05T12:00:00+00:00", "cpu_percent": 99.0, "rss_mb": 2})
            + "\n"
        )
        rows = top.gather_top_rows([spec], now=_NOW)
        assert rows[0].cpu_percent == 99.0  # the latest, not the first

    def test_only_running_jobs(self, tmp_path: Path) -> None:
        running = _running_spec(tmp_path, "run000000001")
        done = JobSpec(id="done00000001", command=["x"], cwd=str(tmp_path), cpus=1,
                       state=JobState.COMPLETED)
        pending = JobSpec(id="pend00000001", command=["x"], cwd=str(tmp_path), cpus=1,
                          state=JobState.PENDING)
        rows = top.gather_top_rows([running, done, pending], now=_NOW)
        assert [r.jobid for r in rows] == ["run000000001"]

    def test_missing_sample_yields_none_resources_but_elapsed(self, tmp_path: Path) -> None:
        spec = _running_spec(tmp_path, "job000000001")  # no samples.jsonl
        rows = top.gather_top_rows([spec], now=_NOW)
        assert rows[0].cpu_percent is None
        assert rows[0].rss_mb is None
        assert rows[0].elapsed_seconds is not None  # from started_at
        assert rows[0].active_elapsed_seconds is not None
        assert rows[0].mem_percent is None
        assert rows[0].wall_percent is None

    def test_resource_pressure_percentages(self, tmp_path: Path) -> None:
        spec = _running_spec(
            tmp_path,
            "job000000001",
            cpu=50.0,
            rss=2048,
            ts="2026-06-05T12:00:00+00:00",
            mem_mb=8192,
            wall_time_seconds=7210,
            paused_seconds_total=600.0,
        )
        rows = top.gather_top_rows([spec], now=_NOW)
        assert rows[0].mem_percent == pytest.approx(25.0)
        assert rows[0].wall_percent == pytest.approx(41.678, abs=0.001)

    def test_active_elapsed_subtracts_paused_seconds(self, tmp_path: Path) -> None:
        spec = _running_spec(
            tmp_path,
            "job000000001",
            paused_seconds_total=600.0,
        )
        rows = top.gather_top_rows([spec], now=_NOW)
        assert rows[0].elapsed_seconds == pytest.approx(3605, abs=2)
        assert rows[0].active_elapsed_seconds == pytest.approx(3005, abs=2)

    def test_active_elapsed_never_goes_negative(self, tmp_path: Path) -> None:
        spec = _running_spec(
            tmp_path,
            "job000000001",
            paused_seconds_total=7200.0,
        )
        rows = top.gather_top_rows([spec], now=_NOW)
        assert rows[0].active_elapsed_seconds == 0.0

    def test_sorted_by_cpu_descending(self, tmp_path: Path) -> None:
        a = _running_spec(tmp_path, "aaa00000000a", cpu=100.0, ts="2026-06-05T12:00:00+00:00")
        b = _running_spec(tmp_path, "bbb00000000b", cpu=800.0, ts="2026-06-05T12:00:00+00:00")
        rows = top.gather_top_rows([a, b], now=_NOW)
        assert [r.jobid for r in rows] == ["bbb00000000b", "aaa00000000a"]

    def test_stale_sample_flagged(self, tmp_path: Path) -> None:
        spec = _running_spec(tmp_path, "job000000001", cpu=50.0,
                             ts="2026-06-05T12:00:00+00:00")
        # 60s after the sample -> stale (> 30s)
        later = datetime(2026, 6, 5, 12, 1, 0, tzinfo=UTC)
        rows = top.gather_top_rows([spec], now=later)
        assert rows[0].sample_stale is True

    @pytest.mark.parametrize(
        "value", ["busy", True, -1, math.nan, math.inf, -math.inf, 10**1000]
    )
    def test_invalid_resource_sample_values_are_unavailable(
        self, tmp_path: Path, value: object
    ) -> None:
        spec = _running_spec(tmp_path, "job000000001")
        sample_path = tmp_path / spec.id / "_vq" / "samples.jsonl"
        sample_path.write_text(
            json.dumps(
                {
                    "ts": "2026-06-05T12:00:00+00:00",
                    "cpu_percent": value,
                    "rss_mb": value,
                }
            )
            + "\n"
        )

        rows = top.gather_top_rows([spec], now=_NOW)

        assert rows[0].cpu_percent is None
        assert rows[0].rss_mb is None
        assert rows[0].elapsed_seconds == pytest.approx(3605, abs=2)
        rendered_json = top.format_top_json(rows)
        assert "NaN" not in rendered_json and "Infinity" not in rendered_json
        assert json.loads(rendered_json)[0]["cpu_percent"] is None
        assert "job000000001" in top.format_top_table(rows)

    @pytest.mark.parametrize(
        "timestamp", [7, True, math.nan, "not-a-timestamp", "2026-06-05T12:00:00"]
    )
    def test_invalid_sample_timestamp_preserves_resources(
        self, tmp_path: Path, timestamp: object
    ) -> None:
        spec = _running_spec(tmp_path, "job000000001")
        sample_path = tmp_path / spec.id / "_vq" / "samples.jsonl"
        sample_path.write_text(
            json.dumps(
                {"ts": timestamp, "cpu_percent": 250.0, "rss_mb": 4096}
            )
            + "\n"
        )

        rows = top.gather_top_rows([spec], now=_NOW)

        assert rows[0].cpu_percent == 250.0
        assert rows[0].rss_mb == 4096.0
        assert rows[0].sample_stale is False


class TestFormatTopTable:
    def test_empty_is_friendly(self) -> None:
        assert top.format_top_table([]) == "(no running jobs)"

    def test_renders_columns_and_flags_stale(self, tmp_path: Path) -> None:
        spec = _running_spec(tmp_path, "job000000001", cpu=50.0, rss=2048,
                             ts="2026-06-05T12:00:00+00:00", mem_mb=8000)
        later = datetime(2026, 6, 5, 12, 1, 0, tzinfo=UTC)
        out = top.format_top_table(top.gather_top_rows([spec], now=later))
        assert "JOBID" in out and "job000000001" in out
        assert "50%*" in out  # the stale marker
        assert "stale" in out  # the footer explanation
        assert "2.0G" in out  # rss 2048 MB -> 2.0G
        assert "ACTIVE" in out
        assert "MEM%" in out
        assert "WALL%" in out

    def test_renders_active_elapsed_separately(self, tmp_path: Path) -> None:
        spec = _running_spec(
            tmp_path,
            "job000000001",
            cpu=50.0,
            ts="2026-06-05T12:00:00+00:00",
            paused_seconds_total=600.0,
        )
        out = top.format_top_table(top.gather_top_rows([spec], now=_NOW))
        assert "0:50:05" in out
        assert "1:00:05" in out

    def test_renders_resource_pressure_percentages(self, tmp_path: Path) -> None:
        spec = _running_spec(
            tmp_path,
            "job000000001",
            cpu=50.0,
            rss=2048,
            ts="2026-06-05T12:00:00+00:00",
            mem_mb=8192,
            wall_time_seconds=7210,
            paused_seconds_total=600.0,
        )
        out = top.format_top_table(top.gather_top_rows([spec], now=_NOW))
        assert "25%" in out
        assert "42%" in out


class TestShowTopLocal:
    def test_remote_host_raises(self) -> None:
        with pytest.raises(NotImplementedError):
            top.show_top_local("some-remote-host")


class TestTopCLI:
    def test_help(self) -> None:
        result = CliRunner().invoke(main, ["top", "--help"])
        assert result.exit_code == 0
        assert "resource snapshot" in result.output

    def test_local_no_running_jobs(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
        paths.queue_dir().mkdir(parents=True, exist_ok=True)
        result = CliRunner().invoke(main, ["top", "localhost"])
        assert result.exit_code == 0
        assert "(no running jobs)" in result.output

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
        paths.queue_dir().mkdir(parents=True, exist_ok=True)
        host_f = _running_spec(
            tmp_path,
            "twintop00001",
            cpu=125.0,
            ts="2026-06-05T12:00:00+00:00",
            scheduler_target="host_f",
        )
        local = _running_spec(
            tmp_path,
            "localtop0001",
            cpu=500.0,
            ts="2026-06-05T12:00:00+00:00",
        )
        host_f.write(paths.spec_path(host_f.id))
        local.write(paths.spec_path(local.id))

        result = CliRunner().invoke(main, ["top", "host_f", "--json"])

        assert result.exit_code == 0, result.output
        rows = json.loads(result.output)
        assert [row["jobid"] for row in rows] == ["twintop00001"]
        assert rows[0]["queue_handle"] == {
            "job_id": "twintop00001",
            "host": "host_f",
            "submitted_at": rows[0]["queue_handle"]["submitted_at"],
        }

    def test_remote_host_delegates_to_remote_vq(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
        cfg_dir = tmp_path / "cfg"
        cfg_dir.mkdir()
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(cfg_dir))
        (cfg_dir / "config.toml").write_text(
            "[hosts.remote-test]\n"
            'ssh = "remote.example.com"\n'
            'remote_vq = "vq"\n'
        )
        captured: list[tuple[str, ...]] = []

        class FakeProc:
            stdout = json.dumps(
                [
                    {
                        "jobid": "remtop000001",
                        "queue_handle": {
                            "job_id": "remtop000001",
                            "host": "localhost",
                            "submitted_at": "2026-07-15T10:00:00+00:00",
                        },
                    },
                    {"jobid": "oldtop000002"},
                ]
            ) + "\n"

        def fake_run_remote_vq(host_cfg, *args, **kwargs):
            captured.append(tuple(args))
            return FakeProc()

        monkeypatch.setattr(transport, "run_remote_vq", fake_run_remote_vq)

        result = CliRunner().invoke(main, ["top", "remote-test", "--json"])

        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload[0]["queue_handle"]["host"] == "remote-test"
        assert payload[1]["queue_handle"] == {
            "job_id": "oldtop000002",
            "host": "remote-test",
            "submitted_at": None,
        }
        assert captured == [("top", "localhost", "--json")]

    def test_default_remote_host_delegates_to_remote_vq(
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

        class FakeProc:
            stdout = "(no running jobs)\n"

        def fake_run_remote_vq(host_cfg, *args, **kwargs):
            captured.append(tuple(args))
            return FakeProc()

        monkeypatch.setattr(transport, "run_remote_vq", fake_run_remote_vq)

        result = CliRunner().invoke(main, ["top"])

        assert result.exit_code == 0, result.output
        assert result.output.strip() == "(no running jobs)"
        assert captured == [("top", "localhost")]

    def test_default_remote_transport_failure_falls_back_to_localhost(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
        paths.queue_dir().mkdir(parents=True, exist_ok=True)
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

        result = CliRunner().invoke(main, ["top"])

        assert result.exit_code == 0, result.output
        assert "(no running jobs)" in result.output
        combined = result.output + result.stderr
        assert "default_host 'remote-test' is unreachable" in combined
        assert "showing localhost" in combined
        assert captured == [("top", "localhost")]

    def test_default_remote_marked_down_skips_remote_probe(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
        paths.queue_dir().mkdir(parents=True, exist_ok=True)
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

        result = CliRunner().invoke(main, ["top"])

        assert result.exit_code == 0, result.output
        assert "(no running jobs)" in result.output
        combined = result.output + result.stderr
        assert "default_host 'remote-test' is marked down" in combined
        assert "showing localhost" in combined
        assert captured == []


class TestJsonOutput:
    def test_format_top_json_fields(self, tmp_path: Path) -> None:
        spec = _running_spec(
            tmp_path, "job000000001", cpu=250.0, rss=4096,
            ts="2026-06-05T12:00:00+00:00", mem_mb=8000, wall_time_seconds=3600,
        )
        data = json.loads(top.format_top_json(top.gather_top_rows([spec], now=_NOW)))
        assert len(data) == 1
        r = data[0]
        assert r["jobid"] == "job000000001"
        assert r["queue_handle"] == {
            "job_id": "job000000001",
            "host": "localhost",
            "submitted_at": r["queue_handle"]["submitted_at"],
        }
        assert r["cpu_percent"] == 250.0
        assert r["rss_mb"] == 4096
        assert r["mem_mb"] == 8000
        assert r["mem_percent"] == pytest.approx(51.2)
        assert r["active_elapsed_seconds"] == pytest.approx(3605, abs=2)
        assert r["elapsed_seconds"] == pytest.approx(3605, abs=2)
        assert r["wall_time_seconds"] == 3600
        assert r["wall_percent"] == pytest.approx(100.139, abs=0.001)
        assert r["sample_stale"] is False

    def test_show_top_local_json_empty(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
        paths.queue_dir().mkdir(parents=True, exist_ok=True)
        assert json.loads(top.show_top_local("localhost", as_json=True)) == []


class TestWatchLoop:
    def test_renders_one_frame_then_exits_on_interrupt(self) -> None:
        frames: list[str] = []
        sleeps: list[float] = []

        def _sleep(n: float) -> None:
            sleeps.append(n)
            raise KeyboardInterrupt  # exit after the first frame

        top.watch_loop(
            lambda: "BODY-TEXT", interval=3.0, host="host_d",
            sleep=_sleep, write=frames.append, clock=lambda: "TS",
        )
        assert len(frames) == 1
        assert "BODY-TEXT" in frames[0]
        assert "host_d" in frames[0]
        assert "every 3s" in frames[0]
        assert "\033[2J" in frames[0]  # screen-clear escape
        assert sleeps == [3.0]


class TestTopCLIOptions:
    def test_json_flag_empty(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
        paths.queue_dir().mkdir(parents=True, exist_ok=True)
        result = CliRunner().invoke(main, ["top", "localhost", "--json"])
        assert result.exit_code == 0
        assert result.output.strip() == "[]"

    def test_watch_and_json_mutually_exclusive(self) -> None:
        result = CliRunner().invoke(main, ["top", "localhost", "--watch", "--json"])
        assert result.exit_code != 0
        assert "mutually exclusive" in result.output

    @pytest.mark.parametrize(
        ("interval", "message"),
        [
            ("0", "must be a finite number greater than zero"),
            ("-1", "Error:"),
            ("nan", "must be a finite number greater than zero"),
            ("inf", "must be a finite number greater than zero"),
            ("-inf", "Error:"),
            ("1e308", "must not exceed 86400 seconds"),
        ],
    )
    def test_watch_rejects_nonpositive_or_nonfinite_intervals(
        self,
        interval: str,
        message: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        def _unexpected(*args: object, **kwargs: object) -> None:
            del args, kwargs
            pytest.fail("invalid watch interval reached command work")

        monkeypatch.setattr(top, "show_top_local", _unexpected)
        monkeypatch.setattr(top, "watch_loop", _unexpected)

        result = CliRunner().invoke(
            main,
            ["top", "localhost", f"--watch={interval}"],
        )

        assert result.exit_code == 2
        assert message in result.output

    @pytest.mark.parametrize(
        ("option", "expected"),
        [
            (["--watch"], 2.0),
            (["--watch=0.25"], 0.25),
            (["--watch=86400"], 86_400.0),
        ],
    )
    def test_watch_accepts_positive_finite_intervals(
        self,
        option: list[str],
        expected: float,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        captured: dict[str, float] = {}

        def _watch_loop(*args: object, interval: float, **kwargs: object) -> None:
            del args, kwargs
            captured["interval"] = interval

        monkeypatch.setattr(top, "watch_loop", _watch_loop)

        result = CliRunner().invoke(main, ["top", "localhost", *option])

        assert result.exit_code == 0, result.output
        assert captured["interval"] == expected
