"""A bare per-job verb must name the host it searched when the job is absent.

`vq status JOBID` resolves to default_host and searches only there. When the
job lives elsewhere the error was a flat "no such job" that never said the
search had been scoped — so a polling loop reported a healthy job as missing
(the 2026-07 host_a/host_c case: 890 s of blank status for two live jobs).
Applied to the read/poll verbs an agent gets stuck on; explicit HOST forms and
every other error are untouched.
"""
from __future__ import annotations

import subprocess
from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import config, paths
from vq.cli import _note_searched_host, main
from vq.spec import JobSpec, JobState


@pytest.fixture
def local_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    (tmp_path / "cfg" / "config.toml").write_text(
        'default_host = "localhost"\n[hosts.localhost]\nssh = "localhost"\n'
    )
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    return tmp_path


def test_helper_only_fires_on_inferred_not_found() -> None:
    base = "no such job: abc"
    # explicit host: untouched
    assert _note_searched_host(
        base, verb="status", jobid="abc", searched_host="host_a", inferred=False
    ) == base
    # unrelated error: untouched
    assert _note_searched_host(
        "permission denied", verb="status", jobid="abc",
        searched_host="host_a", inferred=True,
    ) == "permission denied"
    # inferred + not found: enriched, substring preserved
    out = _note_searched_host(
        base, verb="status", jobid="abc", searched_host="host_a", inferred=True
    )
    assert "no such job: abc" in out
    assert "searched only 'host_a'" in out
    assert "vq status HOST abc" in out


def test_bare_status_names_the_searched_host(local_state: Path) -> None:
    result = CliRunner().invoke(main, ["status", "deadbeef0000"])
    assert result.exit_code != 0
    assert "no such job" in result.output  # substring preserved for vq fetch
    assert "searched only 'localhost'" in result.output
    assert "vq status HOST deadbeef0000" in result.output


def test_explicit_host_status_is_not_enriched(local_state: Path) -> None:
    result = CliRunner().invoke(main, ["status", "localhost", "deadbeef0000"])
    assert result.exit_code != 0
    assert "no such job" in result.output
    assert "searched only" not in result.output


def test_bare_logs_names_the_searched_host(local_state: Path) -> None:
    result = CliRunner().invoke(main, ["logs", "deadbeef0000"])
    assert result.exit_code != 0
    assert "searched only 'localhost'" in result.output
    assert "vq logs HOST deadbeef0000" in result.output


def test_a_found_job_is_unaffected(local_state: Path) -> None:
    """The hint must never appear for a job that exists."""
    ws = local_state / "ws"
    ws.mkdir()
    JobSpec(
        id="abc123456789", command=["true"], cwd=str(ws), cpus=1, state=JobState.PENDING
    ).write(paths.queue_dir() / "abc123456789.json")

    result = CliRunner().invoke(main, ["status", "abc123456789"])

    assert result.exit_code == 0, result.output
    assert "searched only" not in result.output


def test_bare_remote_status_not_found_reports_durable_fleet_search(
    local_state: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Bare status no longer trusts or describes a single remote default."""
    (local_state / "cfg" / "config.toml").write_text(
        'default_host = "host_a"\n[hosts.localhost]\nssh = "localhost"\n'
        "\n[hosts.host_a]\nssh = \"host_a\"\n"
    )

    def fake_queue(_host_cfg, *args, **kwargs):  # type: ignore[no-untyped-def]
        assert args == ("queue", "localhost", "--show-archived", "--json")
        return subprocess.CompletedProcess([], 0, "[]", "")

    monkeypatch.setattr("vq.cli.transport.run_remote_vq", fake_queue)

    result = CliRunner().invoke(main, ["status", "deadbeef0000"])

    assert result.exit_code != 0
    assert "not found on any reachable host" in result.output
    assert "searched only 'host_a'" not in result.output
