"""Remote per-job follow commands must stream instead of buffering output."""

from __future__ import annotations

import contextlib
import io
from collections.abc import Iterator
from typing import Any

import pytest
from click.testing import CliRunner

from vq import cli, config, transport

_JOB_ID = "deadbeef0000"


@pytest.fixture
def remote_config(monkeypatch: pytest.MonkeyPatch) -> config.Config:
    cfg = config.Config(
        default_host="host_a",
        hosts={"host_a": config.HostConfig(ssh="host_a")},
    )
    monkeypatch.setattr(cli.config, "load_config", lambda: cfg)
    return cfg


class _ChunkReader:
    def __init__(self, chunks: list[bytes]) -> None:
        self._chunks = iter(chunks)

    def read(self, size: int = -1) -> bytes:
        assert size == 65536
        return next(self._chunks, b"")


@pytest.mark.parametrize(
    ("argv", "expected_remote_args"),
    [
        (
            ["logs", "host_a", _JOB_ID, "--follow", "-n", "7", "--stdout"],
            ("logs", "localhost", _JOB_ID, "-n", "7", "-f", "--stdout"),
        ),
        (
            ["logs", "host_a", _JOB_ID, "--follow", "-n", "0", "--stdout"],
            ("logs", "localhost", _JOB_ID, "-n", "0", "-f", "--stdout"),
        ),
        (
            ["output", "host_a", _JOB_ID, "--follow", "-n", "8"],
            ("output", "localhost", _JOB_ID, "-n", "8", "-f"),
        ),
        (
            ["progress", "host_a", _JOB_ID, "--follow", "-n", "9"],
            ("progress", "localhost", _JOB_ID, "-n", "9", "-f"),
        ),
    ],
)
def test_remote_follow_streams_binary_chunks_without_captured_delegate(
    remote_config: config.Config,
    monkeypatch: pytest.MonkeyPatch,
    argv: list[str],
    expected_remote_args: tuple[str, ...],
) -> None:
    calls: list[tuple[config.HostConfig, tuple[str, ...]]] = []

    @contextlib.contextmanager
    def fake_stream(
        host_cfg: config.HostConfig, *remote_args: str
    ) -> Iterator[transport.RemoteStream]:
        calls.append((host_cfg, remote_args))
        yield transport.RemoteStream(_ChunkReader([b"first\n", b"second\n"]))

    def captured_delegate_must_not_run(*args: Any, **kwargs: Any) -> str:
        raise AssertionError("remote --follow must not use captured delegation")

    monkeypatch.setattr(cli.transport, "stream_remote_vq", fake_stream)
    monkeypatch.setattr(cli, "_delegate_job_lookup", captured_delegate_must_not_run)

    result = CliRunner().invoke(cli.main, argv)

    assert result.exit_code == 0, result.output
    assert result.stdout_bytes == b"first\nsecond\n"
    assert calls == [(remote_config.host("host_a"), expected_remote_args)]


@pytest.mark.parametrize(
    ("argv", "expected_remote_args", "verb"),
    [
        (
            ["logs", "host_a", _JOB_ID, "-n", "7", "--stderr"],
            ("logs", "localhost", _JOB_ID, "-n", "7", "--stderr"),
            "logs",
        ),
        (
            ["output", "host_a", _JOB_ID, "-n", "8"],
            ("output", "localhost", _JOB_ID, "-n", "8"),
            "output",
        ),
        (
            ["progress", "host_a", _JOB_ID, "-n", "9"],
            ("progress", "localhost", _JOB_ID, "-n", "9"),
            "progress",
        ),
    ],
)
def test_remote_snapshot_keeps_captured_job_lookup_delegation(
    remote_config: config.Config,
    monkeypatch: pytest.MonkeyPatch,
    argv: list[str],
    expected_remote_args: tuple[str, ...],
    verb: str,
) -> None:
    calls: list[tuple[config.Config, str, tuple[str, ...], dict[str, Any]]] = []

    def fake_delegate(
        cfg: config.Config, host: str, *remote_args: str, **kwargs: Any
    ) -> str:
        calls.append((cfg, host, remote_args, kwargs))
        return "snapshot\n"

    def streaming_delegate_must_not_run(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("one-shot remote command must retain captured delegation")

    monkeypatch.setattr(cli, "_delegate_job_lookup", fake_delegate)
    monkeypatch.setattr(cli, "_stream_delegated_job_lookup", streaming_delegate_must_not_run)

    result = CliRunner().invoke(cli.main, argv)

    assert result.exit_code == 0, result.output
    assert result.stdout_bytes == b"snapshot\n"
    assert calls == [
        (
            remote_config,
            "host_a",
            expected_remote_args,
            {
                "verb": verb,
                "jobid": _JOB_ID,
                "searched_host": "host_a",
                "inferred": False,
            },
        )
    ]


@pytest.mark.parametrize("verb", ["logs", "output", "progress"])
def test_remote_follow_enriches_inferred_no_such_job_with_searched_host(
    remote_config: config.Config,
    monkeypatch: pytest.MonkeyPatch,
    verb: str,
) -> None:
    monkeypatch.setattr(cli, "_resolve_job_host", lambda *args: "host_a")

    @contextlib.contextmanager
    def missing_job(
        host_cfg: config.HostConfig, *remote_args: str
    ) -> Iterator[transport.RemoteStream]:
        yield transport.RemoteStream(io.BytesIO())
        raise transport.RemoteError(
            "remote vq failed (exit 2) on host_a:\n"
            f"  stderr: Error: no such job: {_JOB_ID}"
        )

    monkeypatch.setattr(cli.transport, "stream_remote_vq", missing_job)

    result = CliRunner().invoke(cli.main, [verb, _JOB_ID, "--follow"])

    assert result.exit_code != 0
    assert "searched only 'host_a'" in result.output
    assert f"vq {verb} HOST {_JOB_ID}" in result.output


@pytest.mark.parametrize("verb", ["logs", "output", "progress"])
def test_remote_follow_leaves_explicit_host_no_such_job_unenriched(
    remote_config: config.Config,
    monkeypatch: pytest.MonkeyPatch,
    verb: str,
) -> None:
    @contextlib.contextmanager
    def missing_job(
        host_cfg: config.HostConfig, *remote_args: str
    ) -> Iterator[transport.RemoteStream]:
        yield transport.RemoteStream(io.BytesIO())
        raise transport.RemoteError(
            "remote vq failed (exit 2) on host_a:\n"
            f"  stderr: Error: no such job: {_JOB_ID}"
        )

    monkeypatch.setattr(cli.transport, "stream_remote_vq", missing_job)

    result = CliRunner().invoke(cli.main, [verb, "host_a", _JOB_ID, "--follow"])

    assert result.exit_code != 0
    assert f"no such job: {_JOB_ID}" in result.output
    assert "searched only" not in result.output
