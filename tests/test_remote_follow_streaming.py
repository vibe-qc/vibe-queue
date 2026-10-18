"""Remote per-job follow commands must stream instead of buffering output."""

from __future__ import annotations

import contextlib
import io
import sys
from collections.abc import Iterator
from typing import Any

import click
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

    def read1(self, size: int = -1) -> bytes:
        assert size == 65536
        return next(self._chunks, b"")

    def read(self, size: int = -1) -> bytes:
        raise AssertionError("read(n) on a pipe waits for n bytes; follow must use read1")


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


# --- Click 9.0 removals (#71) ------------------------------------------------
#
# The streaming path used to reach for ``click.get_binary_stream("stdout")``,
# which Click 8.5 deprecates and Click 9.0 removes. These pin the behaviour
# under Click 9 semantics without needing Click 9 installed.


def _hide_click_9_removals(monkeypatch: pytest.MonkeyPatch, *names: str) -> None:
    """Make ``names`` unresolvable on ``click``, the way Click 9.0 does.

    ``monkeypatch.delattr(click, "get_binary_stream", raising=False)`` is the
    obvious spelling, and on Click 8.5 it is wrong twice over: there the
    deprecated names are served by a module-level ``__getattr__`` rather than
    living in ``click.__dict__``. ``raising=False`` only covers "no such
    attribute at all", and here ``hasattr`` is true -- so monkeypatch records an
    undo entry, *then* ``delattr`` raises ``AttributeError`` regardless. Worse,
    the undo it already recorded writes the deprecated name into
    ``click.__dict__`` at teardown, so the symbol becomes permanently resolvable
    and later tests in the same session quietly stop simulating anything.

    So: delete only a name that really is a module attribute (the Click 8.0-8.4
    shape), and otherwise shadow the lazy shim (the Click 8.5 shape). Names
    Click still provides are delegated, so only the removals disappear.
    """
    removed = frozenset(names)
    for name in removed:
        if name in click.__dict__:
            monkeypatch.delattr(click, name)
    lazy = click.__dict__.get("__getattr__")
    if lazy is None:
        return

    def click_9_getattr(name: str) -> object:
        if name in removed:
            raise AttributeError(f"module 'click' has no attribute {name!r}")
        return lazy(name)

    monkeypatch.setattr(click, "__getattr__", click_9_getattr)


@pytest.mark.parametrize(
    ("argv", "expected_remote_args"),
    [
        (
            ["logs", "host_a", _JOB_ID, "--follow"],
            ("logs", "localhost", _JOB_ID, "-f"),
        ),
        (
            ["output", "host_a", _JOB_ID, "--follow"],
            ("output", "localhost", _JOB_ID, "-f"),
        ),
        (
            ["progress", "host_a", _JOB_ID, "--follow"],
            ("progress", "localhost", _JOB_ID, "-f"),
        ),
    ],
)
def test_remote_follow_streams_without_the_stream_helper_click_9_removes(
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

    monkeypatch.setattr(cli.transport, "stream_remote_vq", fake_stream)
    _hide_click_9_removals(monkeypatch, "get_binary_stream", "get_text_stream")

    # Guard the simulation itself: if this stops biting, the assertions below
    # would pass against the very code they exist to reject.
    assert not hasattr(click, "get_binary_stream")

    result = CliRunner().invoke(cli.main, argv)

    assert result.exit_code == 0, result.output
    assert result.stdout_bytes == b"first\nsecond\n"
    assert calls == [(remote_config.host("host_a"), expected_remote_args)]


def test_remote_follow_preserves_bytes_split_across_a_multibyte_boundary(
    remote_config: config.Config,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Remote log bytes reach stdout verbatim -- no decode/encode round trip.

    The first chunk ends mid-UTF-8 and the second carries bytes that are not
    valid UTF-8 at all, so any text stream in the path corrupts or replaces
    them instead of passing them through.
    """
    chunks = [b"caf\xc3", b"\xa9 \x00\x01\x80\xfe\xff\n"]

    @contextlib.contextmanager
    def fake_stream(
        host_cfg: config.HostConfig, *remote_args: str
    ) -> Iterator[transport.RemoteStream]:
        yield transport.RemoteStream(_ChunkReader(list(chunks)))

    monkeypatch.setattr(cli.transport, "stream_remote_vq", fake_stream)
    _hide_click_9_removals(monkeypatch, "get_binary_stream", "get_text_stream")

    result = CliRunner().invoke(cli.main, ["logs", "host_a", _JOB_ID, "--follow"])

    assert result.exit_code == 0, result.output
    assert result.stdout_bytes == b"".join(chunks)


def test_binary_stdout_prefers_the_buffer_of_a_text_stdout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw = io.BytesIO()
    text = io.TextIOWrapper(raw, encoding="utf-8")
    monkeypatch.setattr(sys, "stdout", text)

    assert cli._binary_stdout() is raw


def test_binary_stdout_accepts_a_stdout_that_is_already_a_byte_sink(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A ``sys.stdout`` replaced by a bare binary object has no ``.buffer``.

    Click's wrapper handled that shape by returning the stream itself; keep it,
    so a stand-in stdout does not turn into an ``AttributeError``.
    """
    sink = io.BytesIO()
    monkeypatch.setattr(sys, "stdout", sink)

    assert cli._binary_stdout() is sink
    cli._binary_stdout().write(b"\xfe\xff")
    assert sink.getvalue() == b"\xfe\xff"
