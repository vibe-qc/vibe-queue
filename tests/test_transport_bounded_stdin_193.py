"""Real pipes cover the bounded registry request transport, not an endpoint mock."""
from __future__ import annotations

import hashlib
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from tests.test_transport import TestOwnedSubprocessTimeout as _OwnedTimeoutTests
from tests.test_transport import _process_exists
from vq import config, runtime_registry_remote, transport


def test_large_input_and_output_make_progress_together() -> None:
    payload = "αβ\n" * 200_000
    # Emit more than pipe capacity before reading. Writing all stdin first
    # deadlocks here; reading only output leaves the child waiting for EOF.
    code = (
        "import hashlib,sys; "
        "sys.stdout.buffer.write(b'o'*262144); sys.stdout.flush(); "
        "sys.stderr.buffer.write(b'e'*262144); sys.stderr.flush(); "
        "print(hashlib.sha256(sys.stdin.buffer.read()).hexdigest())"
    )
    result = transport.run_owned_subprocess(
        [sys.executable, "-c", code], input=payload, timeout=10,
        max_stdout_bytes=262144 + 65, max_stderr_bytes=262144, check=True,
    )
    assert result.stdout == "o" * 262144 + hashlib.sha256(payload.encode()).hexdigest() + "\n"
    assert result.stderr == "e" * 262144


@pytest.mark.parametrize("payload", [None, ""])
def test_absent_or_empty_input_is_eof(payload: str | None) -> None:
    result = transport.run_owned_subprocess(
        [sys.executable, "-c", "import sys; print(repr(sys.stdin.buffer.read()))"],
        input=payload, timeout=10, max_stdout_bytes=32, max_stderr_bytes=32, check=True,
    )
    assert result.stdout == "b''\n"


def test_early_stdin_close_still_collects_child_error() -> None:
    result = transport.run_owned_subprocess(
        [sys.executable, "-c", "import os,sys; os.close(0); print('refused'); sys.exit(7)"],
        input="x" * 1_000_000, timeout=10, max_stdout_bytes=32, max_stderr_bytes=32,
    )
    assert result.returncode == 7
    assert result.stdout == "refused\n"


def test_blocked_input_deadline_kills_the_owned_tree(tmp_path, monkeypatch) -> None:
    identities = tmp_path / "processes"
    child_ready = tmp_path / "ready"
    child_code = (
        "import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); "
        f"open({str(child_ready)!r},'w').close(); time.sleep(30)"
    )
    parent_code = (
        "import os,signal,subprocess,sys,time; "
        f"child=subprocess.Popen([sys.executable,'-c',{child_code!r}]); "
        "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
        f"open({str(identities)!r},'w').write(f'{{os.getpid()}} {{child.pid}}'); "
        "print('ready',flush=True); time.sleep(30)"
    )
    ready_at = _OwnedTimeoutTests._wait_for_tree_before_timeout(
        monkeypatch, identities, child_ready,
    )
    with pytest.raises(subprocess.TimeoutExpired) as caught:
        transport.run_owned_subprocess(
            [sys.executable, "-c", parent_code], input="x" * 1_000_000,
            timeout=0.2, max_stdout_bytes=64, max_stderr_bytes=64,
            terminate_grace_seconds=0.05, kill_grace_seconds=0.5,
        )
    assert time.monotonic() - ready_at[0] < 2
    assert caught.value.output == "ready\n"
    pids = [int(value) for value in identities.read_text().split()]
    deadline = time.monotonic() + 10
    try:
        while time.monotonic() < deadline and any(_process_exists(pid) for pid in pids):
            time.sleep(0.01)
        assert not any(_process_exists(pid) for pid in pids)
    finally:
        for pid in pids:
            if _process_exists(pid):
                os.kill(pid, signal.SIGKILL)


@pytest.mark.parametrize("stream", ["stdout", "stderr"])
def test_output_overflow_with_input_never_uses_unbounded_communicate(monkeypatch, stream):
    processes = []
    communicates = []
    real_popen = transport.subprocess.Popen

    def popen(*args, **kwargs):
        process = real_popen(*args, **kwargs)
        processes.append(process)

        def no_communicate(*args, **kwargs):
            communicates.append(True)
            pytest.fail("bounded cleanup must not recapture unlimited output")

        monkeypatch.setattr(process, "communicate", no_communicate)
        return process

    monkeypatch.setattr(transport.subprocess, "Popen", popen)
    with pytest.raises(transport.SubprocessOutputLimitExceeded, match=stream):
        transport.run_owned_subprocess(
            [sys.executable, "-c", f"import sys,time; sys.{stream}.write('x'*4096); "
             f"sys.{stream}.flush(); time.sleep(30)"],
            input="x" * 1_000_000, timeout=10,
            max_stdout_bytes=32, max_stderr_bytes=32,
        )
    assert len(processes) == 1
    assert not communicates
    assert processes[0].returncode is not None
    assert all(pipe.closed for pipe in (processes[0].stdin,
                                       processes[0].stdout, processes[0].stderr))


def test_registry_snapshot_stage_replace_and_rollback_through_real_pipes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = config.Config(hosts={"driver": config.HostConfig(ssh="driver-fixture")})
    monkeypatch.setattr(config, "load_config", lambda: cfg)
    # Replace only SSH routing; preserve run_remote_vq, its owned subprocess,
    # stdin limits, real CLI entry point and response digest checks.
    shim = tmp_path / "ssh.py"
    shim.write_text(
        "import os,shlex,sys\n"
        "args=shlex.split(sys.argv[1])\n"
        "assert args[1:]==['admin','runtime-registry']\n"
        "os.execv(sys.executable,[sys.executable,'-m','vq.cli',*args[1:]])\n"
    )
    monkeypatch.setattr(transport, "_ssh_base", lambda *_a, **_k: [sys.executable, str(shim)])
    peer = runtime_registry_remote.authority("driver")
    registry, candidate = tmp_path / "registry.toml", tmp_path / "candidate.toml"
    before_bytes = b"# " + b"original registry\n# " * 20_000
    after_bytes = b"# " + b"replacement registry\n# " * 20_000
    registry.write_bytes(before_bytes)
    candidate.write_bytes(after_bytes)
    before = runtime_registry_remote.snapshot(peer, str(registry))
    after = runtime_registry_remote.snapshot(peer, str(candidate))
    action = {
        "before": before, "after": after["entry"],
        "forward_stage": ".vq-migrate-" + "1" * 32,
        "rollback_stage": ".vq-migrate-" + "2" * 32,
    }
    runtime_registry_remote.stage(action)
    assert registry.read_bytes() == before_bytes
    runtime_registry_remote.replace(action, rollback=False)
    assert registry.read_bytes() == after_bytes
    runtime_registry_remote.replace(action, rollback=True)
    assert registry.read_bytes() == before_bytes
