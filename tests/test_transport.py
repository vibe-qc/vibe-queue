"""Tests for vq.transport: SSH/scp wrappers, all with mocked subprocess."""
from __future__ import annotations

import io
import os
import shlex
import signal
import subprocess
import sys
import time
import traceback
from pathlib import Path
from typing import Any

import pytest

from vq import transport
from vq.config import HostConfig

# Budget for a spin loop that is only waiting for the kernel to finish
# reaping a process group. A liveness guard: it exists so a genuine failure to
# reap fails the test instead of blocking the suite, and its value carries no
# meaning beyond "longer than this is certainly broken". It must be generous.
# At 1.0s it was not -- see issue #5 and the same constant in
# tests/test_daemon_scheduler.py, where a loaded shared CI runner failed twice
# in a row on tests that pass locally in milliseconds.
#
# **It also has a ceiling here, which that file does not have.** The process
# trees these tests build sleep 30s and then exit on their own. A budget at or
# above that would let a spin loop outlive the thing it is waiting for, and
# `assert not _process_exists(...)` would then pass because the process
# finished rather than because it was reaped -- a broken reap reporting
# success. Ten seconds is ten times the budget that flaked and a third of the
# self-exit, so it stays a liveness guard and keeps discriminating. Raising it
# means raising those sleeps first.
#
# Not for a timing assertion that is actually under test. The `timeout=1.0`
# arguments below are the timeouts being exercised, and the messages assert
# that exact value; they are not budgets and must not move.
_LIVENESS_SECONDS = 10.0


class _FakePopen:
    """Minimal stand-in for the streaming ssh Popen used by
    :func:`transport.stream_remote_vq`."""

    def __init__(
        self, *, stdout: bytes = b"", stderr: bytes = b"", returncode: int = 0
    ) -> None:
        self.stdout = io.BytesIO(stdout)
        self.stderr = io.BytesIO(stderr)
        self._rc = returncode
        self.killed = False

    def wait(self) -> int:
        return self._rc

    def kill(self) -> None:
        self.killed = True


@pytest.fixture
def host_cfg() -> HostConfig:
    return HostConfig(ssh="host_d", remote_vq="vq", remote_python=None)


def _fake_run_factory(
    captured: list[list[str]],
    returncode: int = 0,
    stdout: str = "",
    stderr: str = "",
):
    """Build a fake subprocess.run that records argv lists into ``captured``
    and always returns a CompletedProcess with the given outputs."""

    def fake(cmd: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        captured.append(cmd)
        return subprocess.CompletedProcess(
            args=cmd, returncode=returncode, stdout=stdout, stderr=stderr
        )

    return fake


def _fake_owned_factory(
    captured: list[list[str]],
    returncode: int = 0,
    stdout: str = "",
    stderr: str = "",
):
    """Fake the bounded owned subprocess seam used by scp transfers."""

    def fake(cmd: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        captured.append(cmd)
        return subprocess.CompletedProcess(
            args=cmd,
            returncode=returncode,
            stdout=stdout,
            stderr=stderr,
        )

    return fake


class TestRunRemoteVQ:
    @pytest.mark.parametrize("failure", ["timeout", "nonzero"])
    def test_idempotency_key_is_redacted_from_remote_failures(
        self,
        host_cfg: HostConfig,
        monkeypatch: pytest.MonkeyPatch,
        failure: str,
    ) -> None:
        raw_key = "private-campaign-key-493"
        if failure == "timeout":
            def fail(*args: object, **kwargs: object) -> object:
                raise subprocess.TimeoutExpired(args[0], kwargs["timeout"])
        else:
            def fail(*args: object, **kwargs: object) -> object:
                return subprocess.CompletedProcess(
                    args=args[0], returncode=2, stdout="", stderr="rejected"
                )
        monkeypatch.setattr(transport.subprocess, "run", fail)

        with pytest.raises(transport.RemoteError) as exc_info:
            transport.run_remote_vq(
                host_cfg,
                "submit",
                "localhost",
                "--idempotency-key",
                raw_key,
                "input.py",
            )

        message = str(exc_info.value)
        assert raw_key not in message
        assert "<redacted>" in message

    def test_idempotency_key_is_redacted_from_timeout_traceback(
        self,
        host_cfg: HostConfig,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        raw_key = "private-campaign-key-493"

        def timeout(cmd: list[str], **kwargs: object) -> object:
            raise subprocess.TimeoutExpired(cmd, kwargs["timeout"])

        monkeypatch.setattr(transport.subprocess, "run", timeout)

        with pytest.raises(transport.RemoteOutcomeUnknown) as exc_info:
            transport.run_remote_vq(
                host_cfg,
                "submit",
                "localhost",
                "--idempotency-key",
                raw_key,
                "input.py",
            )

        rendered = "".join(
            traceback.format_exception(
                type(exc_info.value),
                exc_info.value,
                exc_info.value.__traceback__,
            )
        )
        assert raw_key not in rendered
        assert "<redacted>" in rendered

    @pytest.mark.parametrize(
        "argv",
        [
            ["--idempotency-key", "private-campaign-key-493"],
            ["--idempotency-key=private-campaign-key-493"],
        ],
    )
    def test_echoed_idempotency_key_is_redacted_from_remote_stderr(
        self,
        host_cfg: HostConfig,
        monkeypatch: pytest.MonkeyPatch,
        argv: list[str],
    ) -> None:
        raw_key = "private-campaign-key-493"
        monkeypatch.setattr(
            transport.subprocess,
            "run",
            _fake_run_factory(
                [],
                returncode=2,
                stderr=f"rejected argument {raw_key}\n",
            ),
        )

        with pytest.raises(transport.RemoteError) as exc_info:
            transport.run_remote_vq(
                host_cfg,
                "submit",
                "localhost",
                *argv,
                "input.py",
            )

        assert raw_key not in str(exc_info.value)
        assert "<redacted>" in str(exc_info.value)
        assert isinstance(exc_info.value, transport.RemoteCommandError)
        assert exc_info.value.returncode == 2
        assert raw_key not in exc_info.value.stderr
        assert "<redacted>" in exc_info.value.stderr

    def test_signal_encoded_sigkill_exit_is_outcome_unknown(
        self,
        host_cfg: HostConfig,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(
            transport.subprocess,
            "run",
            _fake_run_factory([], returncode=137, stderr="Killed\n"),
        )

        with pytest.raises(transport.RemoteOutcomeUnknown, match="exit 137"):
            transport.run_remote_vq(host_cfg, "submit", "localhost")

    def test_builds_ssh_command_with_remote_vq_first(
        self, host_cfg: HostConfig, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """v0.5.32: the post-host arg to ssh is a single shlex-joined
        string (so the remote shell sees properly-quoted tokens, not
        re-interpreted metacharacters). shlex.split round-trips it."""
        captured: list[list[str]] = []
        monkeypatch.setattr(
            transport.subprocess, "run", _fake_run_factory(captured, stdout="ok\n")
        )
        proc = transport.run_remote_vq(host_cfg, "queue", "localhost")
        assert proc.stdout == "ok\n"
        assert len(captured) == 1
        cmd = captured[0]
        # v0.6.17: ssh argv now includes ConnectTimeout +
        # ServerAliveInterval/CountMax options inserted between
        # `ssh` and the host. Test asserts the shape ssh-then-host
        # rather than pinning exact positions.
        assert cmd[0] == "ssh"
        assert "host_d" in cmd
        assert cmd[-2] == "host_d", (
            f"host should be penultimate arg (right before remote-cmd); got {cmd!r}"
        )
        assert shlex.split(cmd[-1]) == ["vq", "queue", "localhost"]

    def test_uses_custom_remote_vq_path(self, monkeypatch: pytest.MonkeyPatch) -> None:
        cfg = HostConfig(ssh="x", remote_vq="/abs/path/vq")
        captured: list[list[str]] = []
        monkeypatch.setattr(
            transport.subprocess, "run", _fake_run_factory(captured)
        )
        transport.run_remote_vq(cfg, "queue", "localhost")
        cmd = captured[0]
        # v0.6.17: ssh argv now has ConnectTimeout + ServerAlive
        # options between `ssh` and the host; assert shape rather
        # than exact positions.
        assert cmd[0] == "ssh"
        assert cmd[-2] == "x"
        assert shlex.split(cmd[-1]) == ["/abs/path/vq", "queue", "localhost"]

    def test_empty_remote_environment_keeps_legacy_command_shape(
        self, host_cfg: HostConfig, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        captured: list[list[str]] = []
        monkeypatch.setattr(
            transport.subprocess, "run", _fake_run_factory(captured)
        )

        transport.run_remote_vq(
            host_cfg,
            "queue",
            "localhost",
            remote_env={},
        )

        assert shlex.split(captured[0][-1]) == ["vq", "queue", "localhost"]

    def test_shell_metacharacters_preserved_through_quoting(
        self, host_cfg: HostConfig, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """v0.5.32 regression guard for the two bugs the operator reported:
        argv containing shell metacharacters must reach the remote
        process intact, not be re-interpreted by the remote shell.

        Concretely: ``vq submit … -- bash -c 'echo X > /tmp/y'`` used
        to ship an unquoted ``>`` to the remote shell, which redirected
        the WHOLE remote ``vq submit`` command's stdout (the jobid!)
        into ``/tmp/y`` and left local stdout empty. The fix
        (shlex.join on the laptop) means the remote shell sees a
        properly-quoted argument string and hands it intact to bash -c.

        Test: round-trip the argv through shlex.split — every element
        the caller passed must come back out unchanged, including the
        one containing > and spaces."""
        captured: list[list[str]] = []
        monkeypatch.setattr(
            transport.subprocess, "run", _fake_run_factory(captured),
        )
        shell_token = "printf '%s\\n' \"$HOME value\" > result; *.dat"
        transport.run_remote_vq(
            host_cfg, "submit", "localhost", "--cpus", "1", "--",
            "bash", "-c", shell_token,
        )
        cmd = captured[0]
        # v0.6.17: ssh argv now has timeout opts between ssh and
        # host. Check shape: ssh-then-options-then-host-then-joined-cmd.
        assert cmd[0] == "ssh"
        assert cmd[-2] == "host_d"
        # The remote shell will run `sh -c <cmd[-1]>`. shlex.split is
        # the standard model of what the remote shell will parse to.
        # The bash -c argument MUST round-trip with metacharacters
        # intact — that's the whole bug fix.
        assert shlex.split(cmd[-1]) == [
            "vq", "submit", "localhost", "--cpus", "1", "--",
            "bash", "-c", shell_token,
        ]

    def test_remote_environment_prefix_is_shell_quoted_and_cli_compatible(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Environment forwarding is an ``env`` prefix, not a new vq flag.

        An older remote vq therefore receives its unchanged admin-update argv,
        while shell metacharacters in a transport-level value remain one token.
        """
        cfg = HostConfig(ssh="x", remote_vq="/old install/bin/vq")
        captured: list[list[str]] = []
        monkeypatch.setattr(
            transport.subprocess, "run", _fake_run_factory(captured)
        )
        transport.run_remote_vq(
            cfg,
            "admin",
            "update",
            "vibeqc-dev",
            "localhost",
            remote_env={
                "VQ_UPDATE_SCRIPT_TIMEOUT": "7200.5",
                "VQ_BUILD_STALL_TIMEOUT": "0.0",
            },
        )

        assert shlex.split(captured[0][-1]) == [
            "/usr/bin/env",
            "VQ_BUILD_STALL_TIMEOUT=0.0",
            "VQ_UPDATE_SCRIPT_TIMEOUT=7200.5",
            "/old install/bin/vq",
            "admin",
            "update",
            "vibeqc-dev",
            "localhost",
        ]

    def test_old_remote_process_observes_forwarded_initiator_defaults(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An unchanged old receiver must not retain its 1800/900 defaults."""
        monkeypatch.delenv("VQ_UPDATE_SCRIPT_TIMEOUT", raising=False)
        monkeypatch.delenv("VQ_BUILD_STALL_TIMEOUT", raising=False)
        monkeypatch.setattr(
            transport,
            "_ssh_base",
            lambda _host_cfg: ["/bin/sh", "-c"],
        )
        cfg = HostConfig(ssh="old-remote", remote_vq=sys.executable)
        receiver = (
            "import os; print("
            "os.getenv('VQ_UPDATE_SCRIPT_TIMEOUT', '1800') + '/' + "
            "os.getenv('VQ_BUILD_STALL_TIMEOUT', '900'))"
        )

        proc = transport.run_remote_vq(
            cfg,
            "-c",
            receiver,
            remote_env={
                "VQ_UPDATE_SCRIPT_TIMEOUT": "14400.0",
                "VQ_BUILD_STALL_TIMEOUT": "3600.0",
            },
        )

        assert proc.stdout.strip() == "14400.0/3600.0"

    @pytest.mark.parametrize(
        "remote_env",
        [
            {"VQ_UPDATE_SCRIPT_TIMEOUT": "7200"},
            {
                "VQ_UPDATE_SCRIPT_TIMEOUT": "7200",
                "VQ_BUILD_STALL_TIMEOUT": "900",
                "AWS_SECRET_ACCESS_KEY": "must-not-cross-ssh",
            },
            {
                "VQ_UPDATE_SCRIPT_TIMEOUT": "7200; printf unsafe",
                "VQ_BUILD_STALL_TIMEOUT": "900",
            },
            {
                "VQ_UPDATE_SCRIPT_TIMEOUT": "inf",
                "VQ_BUILD_STALL_TIMEOUT": "900",
            },
            {
                "VQ_UPDATE_SCRIPT_TIMEOUT": "-inf",
                "VQ_BUILD_STALL_TIMEOUT": "900",
            },
            {
                "VQ_UPDATE_SCRIPT_TIMEOUT": "-1",
                "VQ_BUILD_STALL_TIMEOUT": "900",
            },
            {
                "VQ_UPDATE_SCRIPT_TIMEOUT": "7200",
                "VQ_BUILD_STALL_TIMEOUT": "-1",
            },
            {
                "VQ_UPDATE_SCRIPT_TIMEOUT": "7200",
                "VQ_BUILD_STALL_TIMEOUT": "-inf",
            },
            {
                "VQ_UPDATE_SCRIPT_TIMEOUT": 7200,
                "VQ_BUILD_STALL_TIMEOUT": "900",
            },
            {
                "VQ_UPDATE_SCRIPT_TIMEOUT": "7200",
                "VQ_BUILD_STALL_TIMEOUT": True,
            },
        ],
    )
    def test_remote_environment_rejects_non_allowlisted_or_invalid_values(
        self,
        host_cfg: HostConfig,
        monkeypatch: pytest.MonkeyPatch,
        remote_env: dict[str, object],
    ) -> None:
        captured: list[list[str]] = []
        monkeypatch.setattr(
            transport.subprocess, "run", _fake_run_factory(captured)
        )

        with pytest.raises(ValueError, match="remote admin timeout environment"):
            transport.run_remote_vq(
                host_cfg,
                "admin",
                "update",
                "vibeqc-dev",
                "localhost",
                remote_env=remote_env,
            )

        assert captured == []

    def test_non_zero_exit_raises_remote_error_with_stderr(
        self, host_cfg: HostConfig, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        captured: list[list[str]] = []
        monkeypatch.setattr(
            transport.subprocess,
            "run",
            _fake_run_factory(captured, returncode=42, stderr="ssh: bad host\n"),
        )
        with pytest.raises(transport.RemoteError) as exc_info:
            transport.run_remote_vq(host_cfg, "queue", "localhost")
        msg = str(exc_info.value)
        assert "exit 42" in msg
        assert "ssh: bad host" in msg

    def test_signal_exit_raises_bounded_transport_error(
        self, host_cfg: HostConfig, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            transport.subprocess,
            "run",
            _fake_run_factory([], returncode=-15, stderr=""),
        )
        with pytest.raises(transport.RemoteError) as exc_info:
            transport.run_remote_vq(host_cfg, "queue", "localhost")
        msg = str(exc_info.value)
        assert "terminated by signal SIGTERM" in msg
        assert "transport/helper failure" in msg
        assert "vq doctor host_d --verbose" in msg

    def test_check_false_returns_completed_process_unchanged(
        self, host_cfg: HostConfig, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        captured: list[list[str]] = []
        monkeypatch.setattr(
            transport.subprocess,
            "run",
            _fake_run_factory(captured, returncode=1, stdout="x"),
        )
        proc = transport.run_remote_vq(host_cfg, "x", check=False)
        assert proc.returncode == 1
        assert proc.stdout == "x"

    def test_bounded_remote_vq_output_overflow_is_outcome_unknown(
        self,
        host_cfg: HostConfig,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        def overflow(*_args: object, **_kwargs: object) -> object:
            raise transport.SubprocessOutputLimitExceeded(
                "owned subprocess stdout exceeded 32 bytes"
            )

        monkeypatch.setattr(transport, "run_owned_subprocess", overflow)

        with pytest.raises(
            transport.RemoteOutcomeUnknown,
            match="output exceeded its bounded capture",
        ):
            transport.run_remote_vq(
                host_cfg,
                "submit",
                "localhost",
                owned_process_group=True,
                max_stdout_bytes=32,
                max_stderr_bytes=32,
            )


class TestRunRemoteShell:
    def test_passes_args_directly_to_ssh(
        self, host_cfg: HostConfig, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """v0.5.32: same quoting model as run_remote_vq."""
        captured: list[list[str]] = []
        monkeypatch.setattr(
            transport.subprocess, "run", _fake_run_factory(captured)
        )
        transport.run_remote_shell(host_cfg, "rm", "-f", "/tmp/x")
        cmd = captured[0]
        assert cmd[0] == "ssh"
        assert cmd[-2] == "host_d"
        assert shlex.split(cmd[-1]) == ["rm", "-f", "/tmp/x"]

    def test_paths_with_spaces_preserved(
        self, host_cfg: HostConfig, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A remote_tar path with a space (or any shell metacharacter)
        must survive the round trip — pre-v0.5.32 the remote shell
        would have re-tokenised it."""
        captured: list[list[str]] = []
        monkeypatch.setattr(
            transport.subprocess, "run", _fake_run_factory(captured),
        )
        transport.run_remote_shell(
            host_cfg, "rm", "-f", "/tmp/upload dir/x.tar",
        )
        cmd = captured[0]
        # v0.6.17: ssh argv now has timeout opts; cmd[-1] is the
        # joined remote command (the only thing we care about for
        # the round-trip property this test guards).
        assert shlex.split(cmd[-1]) == ["rm", "-f", "/tmp/upload dir/x.tar"]

    def test_non_zero_raises(
        self, host_cfg: HostConfig, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        captured: list[list[str]] = []
        monkeypatch.setattr(
            transport.subprocess,
            "run",
            _fake_run_factory(captured, returncode=2, stderr="permission denied"),
        )
        with pytest.raises(transport.RemoteError, match="permission denied"):
            transport.run_remote_shell(host_cfg, "rm", "-f", "/etc/passwd")

    def test_exit_127_reports_missing_remote_shell_hint(
        self, host_cfg: HostConfig, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        captured: list[list[str]] = []
        monkeypatch.setattr(
            transport.subprocess,
            "run",
            _fake_run_factory(
                captured,
                returncode=127,
                stderr="zsh:1: command not found: missing-helper\n",
            ),
        )

        with pytest.raises(transport.RemoteError) as excinfo:
            transport.run_remote_shell(host_cfg, "missing-helper", "--version")

        message = str(excinfo.value)
        assert "remote shell failed (exit 127) on host_d" in message
        assert "cmd: missing-helper --version" in message
        assert "remote shell command was not found" in message
        assert "stale scheduler, cleanup, or provisioning command paths" in message
        assert "vq doctor host_d --verbose" in message

    def test_check_false_swallows_failure(
        self, host_cfg: HostConfig, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        captured: list[list[str]] = []
        monkeypatch.setattr(
            transport.subprocess,
            "run",
            _fake_run_factory(captured, returncode=99),
        )
        proc = transport.run_remote_shell(host_cfg, "x", check=False)
        assert proc.returncode == 99

    def test_signal_exit_reports_transport_hint(
        self, host_cfg: HostConfig, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            transport.subprocess,
            "run",
            _fake_run_factory([], returncode=-15, stderr=""),
        )
        with pytest.raises(transport.RemoteOutcomeUnknown) as exc_info:
            transport.run_remote_shell(host_cfg, "queue", "localhost")
        msg = str(exc_info.value)
        assert "terminated by signal SIGTERM" in msg
        assert "transport/helper failure" in msg

    def test_ssh_255_is_outcome_unknown_when_checking_mutation(
        self, host_cfg: HostConfig, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            transport.subprocess,
            "run",
            _fake_run_factory([], returncode=255, stderr="connection lost"),
        )

        with pytest.raises(transport.RemoteOutcomeUnknown, match="exit 255"):
            transport.run_remote_shell(host_cfg, "mutate")

    def test_retries_transient_ssh_exit_255(
        self, host_cfg: HostConfig, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        calls = 0

        def fake(cmd: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
            nonlocal calls
            calls += 1
            rc = 255 if calls == 1 else 0
            return subprocess.CompletedProcess(
                args=cmd, returncode=rc, stdout="ok", stderr=""
            )

        monkeypatch.setattr(transport.subprocess, "run", fake)
        monkeypatch.setattr(transport.time, "sleep", lambda _seconds: None)

        proc = transport.run_remote_shell(
            host_cfg,
            "qstat",
            "123.cluster",
            check=False,
            retry_transient=1,
        )

        assert proc.returncode == 0
        assert calls == 2

    def test_retries_timeout_before_raising(
        self, host_cfg: HostConfig, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        calls = 0

        def fake(cmd: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
            nonlocal calls
            calls += 1
            if calls == 1:
                raise subprocess.TimeoutExpired(cmd, kwargs.get("timeout", 0))
            return subprocess.CompletedProcess(
                args=cmd, returncode=0, stdout="ok", stderr=""
            )

        monkeypatch.setattr(transport.subprocess, "run", fake)
        monkeypatch.setattr(transport.time, "sleep", lambda _seconds: None)

        proc = transport.run_remote_shell(
            host_cfg,
            "qstat",
            "123.cluster",
            check=False,
            timeout=1.0,
            retry_transient=1,
        )

        assert proc.returncode == 0
        assert calls == 2

    def test_rejects_negative_retry_count(self, host_cfg: HostConfig) -> None:
        with pytest.raises(ValueError, match="retry_transient"):
            transport.run_remote_shell(
                host_cfg,
                "qstat",
                "123.cluster",
                retry_transient=-1,
            )

    def test_bounded_output_overflow_is_an_unknown_remote_outcome(
        self,
        host_cfg: HostConfig,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        def overflow(*_args: object, **_kwargs: object) -> object:
            raise transport.SubprocessOutputLimitExceeded(
                "owned subprocess stdout exceeded 32 bytes"
            )

        monkeypatch.setattr(transport, "run_owned_subprocess", overflow)

        with pytest.raises(
            transport.RemoteOutcomeUnknown,
            match="output exceeded its bounded capture",
        ):
            transport.run_remote_shell(
                host_cfg,
                "mutating-command",
                owned_process_group=True,
                max_stdout_bytes=32,
                max_stderr_bytes=32,
            )


class TestUploadFile:
    def test_upload_capture_is_owned_and_bounded(
        self,
        host_cfg: HostConfig,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        observed: dict[str, object] = {}

        def owned(
            argv: list[str],
            **kwargs: object,
        ) -> subprocess.CompletedProcess[str]:
            observed["argv"] = argv
            observed.update(kwargs)
            return subprocess.CompletedProcess(argv, 0, "", "")

        monkeypatch.setattr(transport, "run_owned_subprocess", owned)
        monkeypatch.setattr(
            transport.subprocess,
            "run",
            lambda *_args, **_kwargs: pytest.fail(
                "upload must not use unbounded subprocess.run"
            ),
        )
        local = tmp_path / "x.tar"
        local.write_bytes(b"")

        transport.upload_file(host_cfg, local, "/tmp/remote.tar")

        assert observed["max_stdout_bytes"] == transport.SCP_STDOUT_MAX_BYTES
        assert observed["max_stderr_bytes"] == transport.SCP_STDERR_MAX_BYTES
        assert observed["timeout"] == transport.DEFAULT_UPLOAD_TIMEOUT_SECONDS

    def test_invokes_scp_with_quiet_flag(
        self, host_cfg: HostConfig, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        captured: list[list[str]] = []
        monkeypatch.setattr(
            transport, "run_owned_subprocess", _fake_owned_factory(captured)
        )
        local = tmp_path / "x.tar"
        local.write_bytes(b"")
        transport.upload_file(host_cfg, local, "/tmp/remote.tar")
        # v0.6.17: scp argv now has ConnectTimeout + ServerAlive
        # options between `-q` and the source path. Verify shape
        # rather than exact argv: scp prefix + the two path args
        # at the end.
        assert len(captured) == 1
        cmd = captured[0]
        assert cmd[0] == "scp"
        assert "-q" in cmd
        assert cmd[-2] == str(local)
        assert cmd[-1] == "host_d:/tmp/remote.tar"

    def test_failure_raises_remote_error(
        self, host_cfg: HostConfig, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            transport,
            "run_owned_subprocess",
            _fake_owned_factory([], returncode=1, stderr="connection refused"),
        )
        local = tmp_path / "x.tar"
        local.write_bytes(b"")
        with pytest.raises(transport.RemoteError, match="connection refused"):
            transport.upload_file(host_cfg, local, "/tmp/remote.tar")

    def test_ssh_transport_failure_reports_hint(
        self, host_cfg: HostConfig, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            transport,
            "run_owned_subprocess",
            _fake_owned_factory(
                [],
                returncode=255,
                stderr="Permission denied (publickey).\n",
            ),
        )
        local = tmp_path / "x.tar"
        local.write_bytes(b"")

        with pytest.raises(transport.RemoteOutcomeUnknown) as excinfo:
            transport.upload_file(host_cfg, local, "/tmp/remote.tar")

        message = str(excinfo.value)
        assert "scp upload failed (exit 255) to host_d:/tmp/remote.tar" in message
        assert "SSH transport/auth layer" in message
        assert "vq doctor host_d --verbose" in message

    def test_remote_disk_full_reports_cleanup_hint(
        self, host_cfg: HostConfig, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            transport,
            "run_owned_subprocess",
            _fake_owned_factory(
                [],
                returncode=1,
                stderr="scp: /scratch/vq-upload.tar: No space left on device\n",
            ),
        )
        local = tmp_path / "x.tar"
        local.write_bytes(b"")

        with pytest.raises(transport.RemoteError) as excinfo:
            transport.upload_file(host_cfg, local, "/scratch/vq-upload.tar")

        message = str(excinfo.value)
        assert "remote filesystem appears full or over quota" in message
        assert "vq cleanup host_d --auto-status" in message


class TestDownloadFile:
    def test_remote_path_failure_reports_hint(
        self, host_cfg: HostConfig, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            transport,
            "run_owned_subprocess",
            _fake_owned_factory(
                [],
                returncode=1,
                stderr="scp: /tmp/missing.tar: No such file or directory\n",
            ),
        )
        local = tmp_path / "x.tar"

        with pytest.raises(transport.RemoteError) as excinfo:
            transport.download_file(host_cfg, "/tmp/missing.tar", local)

        message = str(excinfo.value)
        assert "scp download failed (exit 1) from host_d:/tmp/missing.tar" in message
        assert "source/destination path or permissions look wrong" in message
        assert "scratch/workspace path" in message


class TestRemoteTempPath:
    def test_unique_home_relative_shared_fs_tar_suffix(self) -> None:
        # Regression (2026-07-16 host_c multi-login-node wedge): the staging
        # tarball must be home-relative (shared home FS, visible from any login
        # node), NOT under node-local /tmp — the scp and the ssh that consumes
        # the tarball can land on different login nodes.
        a = transport.remote_temp_tar_path()
        b = transport.remote_temp_tar_path()
        assert a != b
        for p in (a, b):
            assert p.startswith(".vq-upload-")
            assert p.endswith(".tar")
            # Home-relative: no leading slash, and never node-local /tmp.
            assert not p.startswith("/")
            assert "/tmp/" not in p


class TestSshTimeoutOptions:
    """v0.6.17: every ssh / scp invocation carries explicit
    ConnectTimeout + ServerAlive options so the daemon main loop
    can't be hung by a momentary network glitch or a half-broken
    remote sshd."""

    def test_run_remote_vq_includes_ssh_timeout_options(
        self, host_cfg: HostConfig, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        captured: list[list[str]] = []
        monkeypatch.setattr(
            transport.subprocess, "run", _fake_run_factory(captured)
        )
        transport.run_remote_vq(host_cfg, "queue", "localhost")
        cmd = captured[0]
        # ConnectTimeout, ServerAliveInterval, ServerAliveCountMax
        # all appear as `-o KEY=VAL` pairs
        joined = " ".join(cmd)
        assert "ConnectTimeout=" in joined
        assert "ServerAliveInterval=" in joined
        assert "ServerAliveCountMax=" in joined

    def test_run_remote_shell_includes_ssh_timeout_options(
        self, host_cfg: HostConfig, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        captured: list[list[str]] = []
        monkeypatch.setattr(
            transport.subprocess, "run", _fake_run_factory(captured)
        )
        transport.run_remote_shell(host_cfg, "echo", "hi")
        joined = " ".join(captured[0])
        assert "ConnectTimeout=" in joined
        assert "ServerAliveInterval=" in joined

    def test_upload_file_includes_scp_timeout_options(
        self, host_cfg: HostConfig, tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        captured: list[list[str]] = []
        monkeypatch.setattr(
            transport, "run_owned_subprocess", _fake_owned_factory(captured)
        )
        local = tmp_path / "x.tar"
        local.write_bytes(b"")
        transport.upload_file(host_cfg, local, "/tmp/remote.tar")
        joined = " ".join(captured[0])
        assert "ConnectTimeout=" in joined
        assert "ServerAliveInterval=" in joined


class TestSshBatchMode:
    """v0.7.3 *Dijkstra's Semaphore*: ``BatchMode=yes`` on every ssh /
    scp invocation so a host whose authorized_keys lost the laptop's
    key fails fast (auth error) instead of hanging on a password
    prompt waiting on the inherited tty stdin.

    The 2026-05-26 host_d lockout exposed this: ConnectTimeout
    passed, the TCP+sshd handshake succeeded, key auth failed, and
    ssh then sat at the password prompt indefinitely. ``vq admin
    status --all`` hung on the bad host instead of erroring inline.
    BatchMode=yes makes ssh return non-zero immediately on auth
    failure, which the per-host aggregation already renders inline."""

    def test_run_remote_vq_includes_batchmode(
        self, host_cfg: HostConfig, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        captured: list[list[str]] = []
        monkeypatch.setattr(
            transport.subprocess, "run", _fake_run_factory(captured)
        )
        transport.run_remote_vq(host_cfg, "queue", "localhost")
        joined = " ".join(captured[0])
        assert "BatchMode=yes" in joined

    def test_run_remote_shell_includes_batchmode(
        self, host_cfg: HostConfig, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        captured: list[list[str]] = []
        monkeypatch.setattr(
            transport.subprocess, "run", _fake_run_factory(captured)
        )
        transport.run_remote_shell(host_cfg, "echo", "hi")
        joined = " ".join(captured[0])
        assert "BatchMode=yes" in joined

    def test_upload_file_includes_batchmode(
        self, host_cfg: HostConfig, tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        captured: list[list[str]] = []
        monkeypatch.setattr(
            transport, "run_owned_subprocess", _fake_owned_factory(captured)
        )
        local = tmp_path / "x.tar"
        local.write_bytes(b"")
        transport.upload_file(host_cfg, local, "/tmp/remote.tar")
        joined = " ".join(captured[0])
        assert "BatchMode=yes" in joined


class TestSubprocessTimeoutTranslation:
    """v0.6.17: subprocess.TimeoutExpired → RemoteError. Without
    the translation, the caller would have to import subprocess
    just to except the timeout case."""

    def test_run_remote_vq_timeout_raises_remote_error(
        self, host_cfg: HostConfig, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def fake(cmd: list[str], **kw: Any) -> subprocess.CompletedProcess[str]:
            raise subprocess.TimeoutExpired(cmd, kw.get("timeout", 0))

        monkeypatch.setattr(transport.subprocess, "run", fake)
        with pytest.raises(transport.RemoteError, match="timed out"):
            transport.run_remote_vq(host_cfg, "queue", "localhost", timeout=1.0)

    def test_run_remote_shell_timeout_raises_remote_error(
        self, host_cfg: HostConfig, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def fake(cmd: list[str], **kw: Any) -> subprocess.CompletedProcess[str]:
            raise subprocess.TimeoutExpired(cmd, kw.get("timeout", 0))

        monkeypatch.setattr(transport.subprocess, "run", fake)
        with pytest.raises(transport.RemoteOutcomeUnknown, match="timed out"):
            transport.run_remote_shell(host_cfg, "echo", "hi", timeout=1.0)

    def test_upload_file_timeout_raises_remote_error(
        self, host_cfg: HostConfig, tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        def fake(cmd: list[str], **kw: Any) -> subprocess.CompletedProcess[str]:
            raise subprocess.TimeoutExpired(cmd, kw.get("timeout", 0))

        monkeypatch.setattr(transport, "run_owned_subprocess", fake)
        local = tmp_path / "x.tar"
        local.write_bytes(b"")
        with pytest.raises(transport.RemoteOutcomeUnknown) as excinfo:
            transport.upload_file(
                host_cfg, local, "/tmp/remote.tar", timeout=1.0
            )
        message = str(excinfo.value)
        assert "scp upload timed out after 1.0s to host_d:/tmp/remote.tar" in message
        assert f"local: {local}" in message
        assert "vq doctor host_d --verbose" in message

    def test_download_file_timeout_raises_remote_error(
        self, host_cfg: HostConfig, tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        def fake(cmd: list[str], **kw: Any) -> subprocess.CompletedProcess[str]:
            raise subprocess.TimeoutExpired(cmd, kw.get("timeout", 0))

        monkeypatch.setattr(transport, "run_owned_subprocess", fake)
        local = tmp_path / "x.tar"
        with pytest.raises(transport.RemoteError) as excinfo:
            transport.download_file(
                host_cfg, "/tmp/remote.tar", local, timeout=1.0
            )
        message = str(excinfo.value)
        assert "scp download timed out after 1.0s from host_d:/tmp/remote.tar" in message
        assert f"local: {local}" in message
        assert "vq doctor host_d --verbose" in message

    def test_timeout_is_passed_to_subprocess_run(
        self, host_cfg: HostConfig, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Timeout and process-session kwargs flow through to subprocess.run."""
        captured_kwargs: list[dict] = []

        def fake(cmd: list[str], **kw: Any) -> subprocess.CompletedProcess[str]:
            captured_kwargs.append(kw)
            return subprocess.CompletedProcess(
                args=cmd, returncode=0, stdout="ok\n", stderr=""
            )

        monkeypatch.setattr(transport.subprocess, "run", fake)
        transport.run_remote_vq(host_cfg, "queue", "localhost", timeout=42.0)
        assert captured_kwargs[0].get("timeout") == 42.0
        assert captured_kwargs[0].get("start_new_session") is True


class TestOwnedSubprocessTimeout:
    @staticmethod
    def _wait_for_tree_before_timeout(monkeypatch, identities, child_ready):
        """Arrange the real tree before timing its cleanup, not Python startup.

        On a busy driver, importing the fixture's stdlib can exceed 0.2 s.
        Both processes must have installed their signal policy before the
        existing timeout can meaningfully exercise descendant cleanup (#677).
        """
        real_popen = transport.subprocess.Popen
        ready_at = []

        def ready_popen(*args, **kwargs):
            proc = real_popen(*args, **kwargs)
            deadline = time.monotonic() + 10.0
            while not (identities.exists() and child_ready.exists()):
                if proc.poll() is not None:
                    pytest.fail(f"process-tree fixture exited early: {proc.communicate()}")
                if time.monotonic() >= deadline:
                    os.killpg(proc.pid, signal.SIGKILL)
                    proc.communicate(timeout=1.0)
                    pytest.fail("process-tree fixture never became ready")
                time.sleep(0.01)
            ready_at.append(time.monotonic())
            return proc

        monkeypatch.setattr(transport.subprocess, "Popen", ready_popen)
        return ready_at

    def test_bounded_capture_kills_and_reaps_on_stdout_limit(self) -> None:
        started = time.monotonic()
        with pytest.raises(
            transport.SubprocessOutputLimitExceeded,
            match=r"stdout exceeded 32 bytes",
        ):
            transport.run_owned_subprocess(
                [
                    sys.executable,
                    "-c",
                    "import sys,time; sys.stdout.write('x'*4096); "
                    "sys.stdout.flush(); time.sleep(30)",
                ],
                timeout=2.0,
                max_stdout_bytes=32,
                max_stderr_bytes=32,
                kill_grace_seconds=0.5,
            )
        assert time.monotonic() - started < 2.0

    @pytest.mark.parametrize(
        ("stdout_limit", "stderr_limit"),
        [(None, 1), (1, None), (0, 1), (1, 0)],
    )
    def test_bounded_capture_requires_two_positive_limits(
        self,
        monkeypatch: pytest.MonkeyPatch,
        stdout_limit: int | None,
        stderr_limit: int | None,
    ) -> None:
        monkeypatch.setattr(
            transport.subprocess,
            "Popen",
            lambda *_a, **_k: pytest.fail("invalid limit must fail before Popen"),
        )

        with pytest.raises(ValueError, match=r"max_(?:stdout|stderr)_bytes"):
            transport.run_owned_subprocess(
                ["ignored"],
                timeout=1.0,
                max_stdout_bytes=stdout_limit,
                max_stderr_bytes=stderr_limit,
            )

    @pytest.mark.parametrize(
        "grace_kwargs",
        [
            {"terminate_grace_seconds": float("nan")},
            {"terminate_grace_seconds": float("inf")},
            {"kill_grace_seconds": float("nan")},
            {"kill_grace_seconds": float("inf")},
        ],
    )
    def test_nonfinite_cleanup_grace_is_rejected_before_popen(
        self,
        monkeypatch: pytest.MonkeyPatch,
        grace_kwargs: dict[str, float],
    ) -> None:
        monkeypatch.setattr(
            transport.subprocess,
            "Popen",
            lambda *_a, **_k: pytest.fail("invalid grace must fail before Popen"),
        )

        with pytest.raises(ValueError, match="grace"):
            transport.run_owned_subprocess(
                ["ignored"],
                timeout=1.0,
                **grace_kwargs,
            )

    def test_timeout_kills_and_reaps_descendant_holding_captured_pipes(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        identities = tmp_path / "processes.txt"
        child_ready = tmp_path / "child-ready"
        child_code = (
            "import signal,time; "
            "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
            f"open({str(child_ready)!r},'w').close(); "
            "time.sleep(30)"
        )
        parent_code = (
            "import os,signal,subprocess,sys,time; "
            f"child=subprocess.Popen([sys.executable,'-c',{child_code!r}]); "
            "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
            f"open({str(identities)!r},'w').write("
            "f'{os.getpid()} {child.pid}'); "
            "print('ready', flush=True); "
            "time.sleep(30)"
        )

        ready_at = self._wait_for_tree_before_timeout(monkeypatch, identities, child_ready)
        with pytest.raises(subprocess.TimeoutExpired):
            transport.run_owned_subprocess(
                [sys.executable, "-c", parent_code],
                timeout=0.2,
                terminate_grace_seconds=0.05,
                kill_grace_seconds=0.5,
            )
        elapsed = time.monotonic() - ready_at[0]

        assert elapsed < 2.0
        parent_pid, child_pid = [
            int(value) for value in identities.read_text(encoding="utf-8").split()
        ]
        deadline = time.monotonic() + _LIVENESS_SECONDS
        while time.monotonic() < deadline and any(
            _process_exists(pid) for pid in (parent_pid, child_pid)
        ):
            time.sleep(0.01)
        assert not _process_exists(parent_pid), "owned parent was not reaped"
        assert not _process_exists(child_pid), "pipe-holding descendant survived"

    def test_timeout_kills_same_group_descendant_after_leader_closes_pipes(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        identities = tmp_path / "processes-no-pipes.txt"
        child_ready = tmp_path / "child-no-pipes-ready"
        child_code = (
            "import os,signal,time; "
            "os.close(1); os.close(2); "
            "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
            f"open({str(child_ready)!r},'w').close(); "
            "time.sleep(30)"
        )
        parent_code = (
            "import os,subprocess,sys,time; "
            f"child=subprocess.Popen([sys.executable,'-c',{child_code!r}]); "
            f"open({str(identities)!r},'w').write("
            "f'{os.getpid()} {os.getpgrp()} {child.pid} {os.getpgid(child.pid)}'); "
            "print('ready', flush=True); "
            "time.sleep(30)"
        )
        self._wait_for_tree_before_timeout(monkeypatch, identities, child_ready)
        child_pid: int | None = None
        try:
            with pytest.raises(subprocess.TimeoutExpired):
                transport.run_owned_subprocess(
                    [sys.executable, "-c", parent_code],
                    timeout=0.2,
                    terminate_grace_seconds=0.05,
                    kill_grace_seconds=0.5,
                )
            parent_pid, parent_pgid, child_pid, child_pgid = [
                int(value)
                for value in identities.read_text(encoding="utf-8").split()
            ]
            assert parent_pgid == parent_pid
            assert child_pgid == parent_pgid
            deadline = time.monotonic() + _LIVENESS_SECONDS
            while time.monotonic() < deadline and _process_exists(child_pid):
                time.sleep(0.01)
            assert not _process_exists(child_pid), "same-group descendant survived"
        finally:
            if child_pid is not None and _process_exists(child_pid):
                os.kill(child_pid, signal.SIGKILL)

    def test_post_popen_base_exception_kills_and_reaps_owned_process_group(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        class ProbeAbort(BaseException):
            pass

        real_popen = transport.subprocess.Popen
        proc = real_popen(
            [sys.executable, "-c", "import time; time.sleep(30)"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
        original_communicate = proc.communicate
        calls = 0

        def interrupted_communicate(*args: Any, **kwargs: Any):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise ProbeAbort("stop probe")
            return original_communicate(*args, **kwargs)

        proc.communicate = interrupted_communicate  # type: ignore[method-assign]
        monkeypatch.setattr(transport.subprocess, "Popen", lambda *_a, **_k: proc)
        try:
            with pytest.raises(ProbeAbort, match="stop probe"):
                transport.run_owned_subprocess(
                    ["ignored"],
                    timeout=1.0,
                    kill_grace_seconds=0.5,
                )
            assert calls >= 2, "cleanup must communicate after killing the group"
            assert proc.returncode is not None, "owned leader was not reaped"
            assert not _process_exists(proc.pid)
        finally:
            if proc.poll() is None:
                os.killpg(proc.pid, signal.SIGKILL)
                proc.wait(timeout=1.0)


def _process_exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


class TestStreamRemoteVQ:
    """v0.8.17: the streaming transport used by fetch (REMOTE-2/3/5/6)."""

    def test_hardened_argv_and_streams_stdout(
        self, host_cfg: HostConfig, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        captured: list[list[str]] = []
        captured_kwargs: list[dict] = []

        def fake_popen(cmd: list[str], **kw: Any) -> _FakePopen:
            captured.append(cmd)
            captured_kwargs.append(kw)
            return _FakePopen(stdout=b"TARBYTES", returncode=0)

        monkeypatch.setattr(transport.subprocess, "Popen", fake_popen)
        with transport.stream_remote_vq(host_cfg, "tar-workspace", "job1") as s:
            data = s.stdout.read()
        assert data == b"TARBYTES"
        argv = captured[0]
        # REMOTE-2: hardened _ssh_base prefix. REMOTE-3: the remote argv is
        # ONE shell-quoted string after the host, not separate ssh args.
        assert argv[0] == "ssh"
        assert "BatchMode=yes" in argv
        assert f"ConnectTimeout={transport._SSH_CONNECT_TIMEOUT_SECONDS}" in argv
        assert argv[-2:] == ["host_d", "vq tar-workspace job1"]
        assert captured_kwargs[0].get("start_new_session") is True

    def test_ssh_exit_255_is_a_distinct_transport_error(
        self, host_cfg: HostConfig, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """REMOTE-6: ssh's own 255 (connection refused / host unreachable /
        key rejected) is surfaced distinctly from a real remote rc."""
        monkeypatch.setattr(
            transport.subprocess,
            "Popen",
            lambda *a, **k: _FakePopen(
                stdout=b"x", stderr=b"ssh: connect: Connection refused", returncode=255
            ),
        )
        with pytest.raises(transport.RemoteError, match="255"):  # noqa: SIM117
            with transport.stream_remote_vq(host_cfg, "tar-workspace", "j") as s:
                s.stdout.read()

    def test_remote_nonzero_exit_raises_with_stderr(
        self, host_cfg: HostConfig, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            transport.subprocess,
            "Popen",
            lambda *a, **k: _FakePopen(stdout=b"x", stderr=b"boom", returncode=3),
        )
        with pytest.raises(transport.RemoteError, match="exit 3") as ei:  # noqa: SIM117
            with transport.stream_remote_vq(host_cfg, "tar-workspace", "j") as s:
                s.stdout.read()
        assert "boom" in str(ei.value)

    def test_signal_exit_is_distinct_transport_error(
        self, host_cfg: HostConfig, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            transport.subprocess,
            "Popen",
            lambda *a, **k: _FakePopen(stdout=b"", stderr=b"", returncode=-15),
        )
        with pytest.raises(transport.RemoteError) as excinfo:  # noqa: SIM117
            with transport.stream_remote_vq(host_cfg, "tar-workspace", "j") as s:
                s.stdout.read()
        assert "terminated by signal SIGTERM" in str(excinfo.value)

    def test_remote_exit_127_reports_missing_remote_vq_hint(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        cfg = HostConfig(ssh="host_e", remote_vq="/opt/vq/missing/bin/vq")
        monkeypatch.setattr(
            transport.subprocess,
            "Popen",
            lambda *a, **k: _FakePopen(
                stdout=b"",
                stderr=b"zsh:1: no such file or directory: /opt/vq/missing/bin/vq",
                returncode=127,
            ),
        )

        with pytest.raises(transport.RemoteError) as excinfo:  # noqa: SIM117
            with transport.stream_remote_vq(cfg, "tar-workspace", "j") as s:
                s.stdout.read()

        message = str(excinfo.value)
        assert "remote vq failed (exit 127) on host_e" in message
        assert "configured remote_vq command was not found" in message
        assert "configured remote_vq: /opt/vq/missing/bin/vq" in message
        assert "vq doctor host_e --verbose" in message
        assert "vq host down host_e --reason \"remote_vq missing\"" in message
        assert "vq host up host_e" in message

    def test_caller_exception_not_masked_by_rc(
        self, host_cfg: HostConfig, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """If the caller's block raises, the remote is killed and the
        caller's exception propagates — NOT an rc-based RemoteError (the rc
        is meaningless once we kill the remote)."""
        holder: dict[str, _FakePopen] = {}

        def fake_popen(*a: Any, **k: Any) -> _FakePopen:
            # rc 255 would become a RemoteError IF we checked it on a clean
            # exit — but the caller raises, so it must not.
            p = _FakePopen(stdout=b"x", returncode=255)
            holder["p"] = p
            return p

        monkeypatch.setattr(transport.subprocess, "Popen", fake_popen)
        with pytest.raises(ValueError, match="boom"):  # noqa: SIM117
            with transport.stream_remote_vq(host_cfg, "tar-workspace", "j"):
                raise ValueError("boom")
        assert holder["p"].killed, "the remote must be killed on caller error"

    def test_stderr_is_fully_drained_into_the_handle(
        self, host_cfg: HostConfig, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """REMOTE-5: stderr is drained concurrently (a daemon thread), so a
        chatty remote can't deadlock against our stdout read. Here we just
        assert the full stderr is captured on the handle after exit."""
        big = b"E" * 1_000_000
        monkeypatch.setattr(
            transport.subprocess,
            "Popen",
            lambda *a, **k: _FakePopen(stdout=b"x", stderr=big, returncode=0),
        )
        with transport.stream_remote_vq(host_cfg, "tar-workspace", "j") as s:
            s.stdout.read()
        assert len(s.stderr_text) == 1_000_000
