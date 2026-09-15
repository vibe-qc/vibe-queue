"""v0.6.24: idle_seconds field on HostOverview + `vq summary` alias.

Tests cover:
  * _format_duration helper rendering across magnitudes
  * _count_specs returns last_terminal_at (None when no terminal
    finished_at; max(finished_at) otherwise)
  * gather_overview_local populates idle_seconds when host is quiet
    and has terminal history; leaves None when running > 0; None
    when no terminal finishes recorded
  * format_overview_text renders "idle: X" when idle and "running:
    N job(s)" when busy
  * format_overview_json includes idle_seconds (None or int)
  * _overview_from_json reads idle_seconds back faithfully + tolerates
    pre-v0.6.24 payloads (key absent)
  * `vq summary` alias resolves to the same command path as
    `vq overview`
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import config, overview, paths
from vq.cli import main as cli_main
from vq.spec import JobSpec, JobState


@pytest.fixture
def state_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Mirror tests/test_overview.py's fixture: redirect VQ state +
    config dirs into a tmp_path so the test can write specs without
    touching real queue state. Returns the tmp_path root."""
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    paths.jobs_dir().mkdir(parents=True, exist_ok=True)
    (tmp_path / "cfg" / "config.toml").write_text(
        'default_host = "localhost"\n'
    )
    return tmp_path


# ----------------------------------------------------------------------
# _format_duration
# ----------------------------------------------------------------------


class TestFormatDuration:
    def test_zero_or_negative_clamps_to_zero(self) -> None:
        assert overview._format_duration(0) == "0s"
        assert overview._format_duration(-5) == "0s"

    def test_seconds_only(self) -> None:
        assert overview._format_duration(12) == "12s"
        assert overview._format_duration(59) == "59s"

    def test_minutes_and_seconds(self) -> None:
        assert overview._format_duration(60) == "1m"
        assert overview._format_duration(90) == "1m 30s"
        assert overview._format_duration(3599) == "59m 59s"

    def test_hours_and_minutes(self) -> None:
        assert overview._format_duration(3600) == "1h"
        assert overview._format_duration(3725) == "1h 2m"
        assert overview._format_duration(7200) == "2h"

    def test_days_and_hours_compact(self) -> None:
        assert overview._format_duration(86400) == "1d"
        assert overview._format_duration(90000) == "1d 1h"
        # 2d 5h: don't drop into minutes/seconds — keep to top 2 units.
        assert overview._format_duration(2 * 86400 + 5 * 3600 + 30) == "2d 5h"


# ----------------------------------------------------------------------
# _count_specs returns last_terminal_at
# ----------------------------------------------------------------------


class TestCountSpecsLastTerminalAt:
    def test_no_terminal_specs_returns_none(self) -> None:
        now = datetime(2026, 5, 18, 12, 0, tzinfo=UTC)
        specs = [
            JobSpec(id="a", command=["x"], cwd=".", cpus=1, state=JobState.RUNNING),
            JobSpec(id="b", command=["x"], cwd=".", cpus=1, state=JobState.PENDING),
        ]
        _, _, last, _, _ = overview._count_specs(
            specs, recent_window=timedelta(hours=24), now=now,
        )
        assert last is None

    def test_terminal_without_finished_at_does_not_set_last(self) -> None:
        now = datetime(2026, 5, 18, 12, 0, tzinfo=UTC)
        specs = [
            JobSpec(
                id="a", command=["x"], cwd=".", cpus=1,
                state=JobState.ABORTED_BY_QUEUE, finished_at=None,
            ),
        ]
        _, _, last, _, _ = overview._count_specs(
            specs, recent_window=timedelta(hours=24), now=now,
        )
        assert last is None

    def test_picks_max_across_all_history_not_just_window(self) -> None:
        """The 'recent window' is for the recent-counts section; idle
        time wants the absolute most-recent finish even if outside
        the window. A host that ran nothing in 24h but finished a
        long job 36h ago should idle-report as ~36h, not 'unknown'.
        """
        now = datetime(2026, 5, 18, 12, 0, tzinfo=UTC)
        in_window = (now - timedelta(hours=12)).isoformat()
        out_of_window = (now - timedelta(hours=36)).isoformat()
        specs = [
            JobSpec(
                id="a", command=["x"], cwd=".", cpus=1,
                state=JobState.COMPLETED, finished_at=out_of_window,
            ),
            JobSpec(
                id="b", command=["x"], cwd=".", cpus=1,
                state=JobState.FAILED, finished_at=in_window,
            ),
        ]
        _, _, last, _, _ = overview._count_specs(
            specs, recent_window=timedelta(hours=24), now=now,
        )
        assert last is not None
        assert last == datetime.fromisoformat(in_window)

    def test_unparseable_finished_at_skipped_for_last(self) -> None:
        now = datetime(2026, 5, 18, 12, 0, tzinfo=UTC)
        good = (now - timedelta(hours=2)).isoformat()
        specs = [
            JobSpec(
                id="a", command=["x"], cwd=".", cpus=1,
                state=JobState.COMPLETED, finished_at="not iso",
            ),
            JobSpec(
                id="b", command=["x"], cwd=".", cpus=1,
                state=JobState.COMPLETED, finished_at=good,
            ),
        ]
        _, _, last, _, _ = overview._count_specs(
            specs, recent_window=timedelta(hours=24), now=now,
        )
        assert last == datetime.fromisoformat(good)


# ----------------------------------------------------------------------
# gather_overview_local: idle_seconds population
# ----------------------------------------------------------------------


class TestGatherOverviewIdle:
    def _seed_specs(
        self, state_dir: Path, specs: list[JobSpec],
    ) -> None:
        """Persist specs into the queue dir so gather_overview_local
        sees them via list_jobs(localhost)."""
        from vq import paths
        qd = paths.queue_dir()
        qd.mkdir(parents=True, exist_ok=True)
        for s in specs:
            s.write(qd / f"{s.id}.json")

    def test_quiet_host_with_terminal_history_sets_idle(
        self, state_dir: Path,
    ) -> None:
        now = datetime(2026, 5, 18, 12, 0, tzinfo=UTC)
        finished = (now - timedelta(seconds=192)).isoformat()
        self._seed_specs(state_dir, [
            JobSpec(
                id="abc", command=["x"], cwd=".", cpus=1,
                state=JobState.COMPLETED, finished_at=finished,
            ),
        ])
        cfg = config.load_config()
        ov = overview.gather_overview_local("localhost", cfg, now=now)
        # idle should be ~192s; allow a small slop for the read path.
        assert ov.idle_seconds is not None
        assert 190 <= ov.idle_seconds <= 200

    def test_busy_host_idle_is_none(self, state_dir: Path) -> None:
        now = datetime(2026, 5, 18, 12, 0, tzinfo=UTC)
        finished = (now - timedelta(seconds=200)).isoformat()
        self._seed_specs(state_dir, [
            JobSpec(
                id="aaa", command=["x"], cwd=".", cpus=1,
                state=JobState.COMPLETED, finished_at=finished,
            ),
            JobSpec(
                id="bbb", command=["x"], cwd=".", cpus=1,
                state=JobState.RUNNING,
            ),
        ])
        cfg = config.load_config()
        ov = overview.gather_overview_local("localhost", cfg, now=now)
        # A running job is in flight — host is busy regardless of when
        # the last terminal finished. idle_seconds must be None so
        # consumers don't mistakenly read it as "this host is quiet".
        assert ov.idle_seconds is None
        assert ov.queue_counts.get("running") == 1

    def test_no_terminal_history_idle_is_none(self, state_dir: Path) -> None:
        now = datetime(2026, 5, 18, 12, 0, tzinfo=UTC)
        self._seed_specs(state_dir, [
            JobSpec(
                id="ccc", command=["x"], cwd=".", cpus=1,
                state=JobState.PENDING,
            ),
        ])
        cfg = config.load_config()
        ov = overview.gather_overview_local("localhost", cfg, now=now)
        assert ov.idle_seconds is None


# ----------------------------------------------------------------------
# Text + JSON formatting
# ----------------------------------------------------------------------


class TestFormatOverviewIdle:
    def test_text_renders_idle_line_when_quiet(self) -> None:
        ov = overview.HostOverview(
            host="testhost",
            reachable=True,
            vq_version="0.6.24",
            queue_counts={"completed": 5},
            idle_seconds=192,
        )
        text = overview.format_overview_text(ov)
        # "idle:" with formatted duration appears; no "running:" line.
        assert "idle:" in text
        assert "3m 12s" in text
        assert "running:" not in text

    def test_text_renders_running_line_when_busy(self) -> None:
        ov = overview.HostOverview(
            host="testhost",
            reachable=True,
            vq_version="0.6.24",
            queue_counts={"running": 2, "pending": 3},
            idle_seconds=None,
        )
        text = overview.format_overview_text(ov)
        # "running: 2 job(s)" line appears; no "idle:" line.
        assert "running:" in text
        assert "2 job(s)" in text
        assert "idle:" not in text

    def test_text_omits_both_lines_when_quiet_no_history(self) -> None:
        ov = overview.HostOverview(
            host="testhost",
            reachable=True,
            vq_version="0.6.24",
            queue_counts={"pending": 1},
            idle_seconds=None,
        )
        text = overview.format_overview_text(ov)
        # Neither idle nor running line — the host is quiet but has
        # no terminal history to report idle against.
        assert "idle:" not in text
        assert "running:" not in text

    def test_json_includes_idle_seconds(self) -> None:
        ov = overview.HostOverview(
            host="testhost",
            reachable=True,
            vq_version="0.6.24",
            idle_seconds=42,
        )
        payload = overview.format_overview_json(ov)
        assert payload["idle_seconds"] == 42

    def test_json_none_idle_serializes_as_null(self) -> None:
        ov = overview.HostOverview(host="testhost", idle_seconds=None)
        payload = overview.format_overview_json(ov)
        # The key must be present so the JSON schema stays stable.
        assert "idle_seconds" in payload
        assert payload["idle_seconds"] is None


# ----------------------------------------------------------------------
# Round-trip through _overview_from_json (remote-forwarding path)
# ----------------------------------------------------------------------


class TestOverviewJsonRoundtrip:
    def test_idle_seconds_roundtrip(self) -> None:
        src = overview.HostOverview(
            host="testhost",
            reachable=True,
            vq_version="0.6.24",
            idle_seconds=900,
        )
        payload = overview.format_overview_json(src)
        rebuilt = overview._overview_from_json("testhost", payload)
        assert rebuilt.idle_seconds == 900

    def test_missing_idle_key_tolerated(self) -> None:
        """Pre-v0.6.24 remote omits idle_seconds; consumer treats as
        None (not idle data) rather than erroring."""
        payload = {
            "vq_version": "0.6.23",
            "queue_counts": {"completed": 1},
            "recent_terminal_counts": {},
            "envs": [],
        }
        rebuilt = overview._overview_from_json("testhost", payload)
        assert rebuilt.idle_seconds is None

    def test_negative_idle_in_payload_ignored(self) -> None:
        """A misbehaving remote sending a negative idle_seconds (clock
        skew, schema bug) must not propagate; treat as None."""
        payload = {"idle_seconds": -3, "queue_counts": {}, "envs": []}
        rebuilt = overview._overview_from_json("testhost", payload)
        assert rebuilt.idle_seconds is None


# ----------------------------------------------------------------------
# `vq summary` Click alias
# ----------------------------------------------------------------------


class TestSummaryAlias:
    def test_summary_alias_registered(self) -> None:
        """`vq summary` is registered as a name-level alias for the
        same command object as `vq overview`. Click looks both up via
        get_command()."""
        runner = CliRunner()
        # Both must produce help output (smoke — same command behind
        # both names).
        overview_help = runner.invoke(cli_main, ["overview", "--help"])
        summary_help = runner.invoke(cli_main, ["summary", "--help"])
        assert overview_help.exit_code == 0
        assert summary_help.exit_code == 0
        # The body of the help text is identical (same callback);
        # the Usage line differs only in the verb name. Strip those.
        def _body(text: str) -> str:
            lines = [
                ln for ln in text.splitlines() if not ln.startswith("Usage:")
            ]
            return "\n".join(lines)
        assert _body(overview_help.output) == _body(summary_help.output)
