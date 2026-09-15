"""`vq events JOBID` — the per-job lifecycle timeline.

The daemon has always written a structured event per lifecycle transition to
`<workspace>/_vq/events.jsonl`, but nothing read it: `vq status` pointed the
user at the file path and left them to ssh in and cat it. This surfaces it as
a verb, routed like `vq logs`.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import config, events, paths
from vq.cli import main
from vq.logs import _format_event
from vq.spec import JobSpec, JobState


@pytest.fixture
def state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    (tmp_path / "cfg" / "config.toml").write_text(
        'default_host = "localhost"\n[hosts.localhost]\nssh = "localhost"\n'
    )
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    return tmp_path


def _job_with_events(state_dir: Path) -> Path:
    ws = state_dir / "ws"
    ws.mkdir()
    JobSpec(
        id="abc123456789", command=["true"], cwd=str(ws), cpus=1, state=JobState.COMPLETED
    ).write(paths.queue_dir() / "abc123456789.json")
    events.append_event(ws, events.EventKind.SUBMITTED, "abc123456789")
    events.state_transition(
        ws, "abc123456789", from_state="pending", to_state="running"
    )
    events.state_transition(
        ws, "abc123456789", from_state="running", to_state="completed", exit_code=0
    )
    return ws


def test_events_renders_the_timeline(state: Path) -> None:
    _job_with_events(state)

    result = CliRunner().invoke(main, ["events", "abc123456789"])

    assert result.exit_code == 0, result.output
    assert "submitted" in result.output
    assert "pending -> running" in result.output
    assert "running -> completed (exit 0)" in result.output


def test_events_json_is_the_raw_records(state: Path) -> None:
    _job_with_events(state)

    result = CliRunner().invoke(main, ["events", "abc123456789", "--json"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["jobid"] == "abc123456789"
    assert [e["kind"] for e in payload["events"]] == [
        "submitted",
        "state_transition",
        "state_transition",
    ]


def test_events_for_a_job_with_none_says_so(state: Path) -> None:
    ws = state / "ws2"
    ws.mkdir()
    JobSpec(
        id="noevents1234", command=["true"], cwd=str(ws), cpus=1, state=JobState.PENDING
    ).write(paths.queue_dir() / "noevents1234.json")

    result = CliRunner().invoke(main, ["events", "noevents1234"])

    assert result.exit_code == 0, result.output
    assert "no events recorded" in result.output


def test_events_unknown_job_errors(state: Path) -> None:
    result = CliRunner().invoke(main, ["events", "deadbeef0000"])

    assert result.exit_code != 0
    assert "no such job" in result.output


def test_events_delegates_a_remote_host(
    state: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (state / "cfg" / "config.toml").write_text(
        'default_host = "localhost"\n[hosts.localhost]\nssh = "localhost"\n'
        "\n[hosts.host_a]\nssh = \"host_a\"\n"
    )
    captured: dict[str, object] = {}

    def fake_delegate(host, cfg, *args, **kwargs):  # type: ignore[no-untyped-def]
        captured["host"] = host
        captured["argv"] = list(args)
        return "timeline\n"

    monkeypatch.setattr("vq.cli._delegate_to_remote", fake_delegate)

    result = CliRunner().invoke(main, ["events", "host_a", "abc123456789"])

    assert result.exit_code == 0, result.output
    assert captured["host"] == "host_a"
    assert captured["argv"] == ["events", "localhost", "abc123456789"]


def test_events_follows_the_scheduler_driver(
    state: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A scheduler job's events live on its driver, where the workspace is."""
    (state / "cfg" / "config.toml").write_text(
        "\n".join(
            [
                'default_host = "localhost"',
                "[hosts.localhost]",
                'ssh = "localhost"',
                "",
                "[hosts.driver]",
                'ssh = "driver"',
                "",
                "[hosts.host_f]",
                'ssh = "host_f-login"',
                'scheduler = "pbs"',
                'scheduler_dialect = "torque"',
                'scratch_root = "/home/USER"',
                'scheduler_driver = "driver"',
                "",
            ]
        )
    )
    captured: dict[str, object] = {}

    def fake_delegate(host, cfg, *args, **kwargs):  # type: ignore[no-untyped-def]
        captured["host"] = host
        return "timeline\n"

    monkeypatch.setattr("vq.cli._delegate_to_remote", fake_delegate)

    result = CliRunner().invoke(main, ["events", "host_f", "abc123456789"])

    assert result.exit_code == 0, result.output
    assert captured["host"] == "driver"


def test_format_event_shows_unknown_kinds_without_dropping_data() -> None:
    """A future EventKind must not silently render as a bare timestamp."""
    line = _format_event(
        {"ts": "t", "kind": "future_kind", "jobid": "j", "widget": "frob", "n": 3}
    )
    assert "future_kind" in line
    assert "widget=frob" in line
    assert "n=3" in line
