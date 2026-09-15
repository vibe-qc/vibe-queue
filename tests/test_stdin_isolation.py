"""Regression tests for the stdin-isolation contract (#118).

``ssh`` reads its stdin greedily and forwards it to the remote command. Every
vq subcommand that shells out therefore used to *consume the caller's stdin*.
The operator-visible symptom is silent truncation of a fleet poll::

    while IFS=$'\\t' read -r a id c; do vq status "$host" "$id"; done < list.tsv

processed the FIRST row and exited 0. A measured 28-row rp218 poll produced
9 status files and looked complete; re-running with ``< /dev/null`` on the vq
call produced all 28.

Two layers are pinned here:

1. a **behavioural** test that runs a real ``while read`` loop over a real
   file, with a fake ``ssh`` on PATH that drains stdin the way ssh does — it
   counts iterations, exactly as the operator did;
2. an **AST guard** over ``src/vq`` so a new ``subprocess`` call site cannot
   silently reintroduce an inherited stdin.
"""
from __future__ import annotations

import ast
import os
import pathlib
import stat
import subprocess
import sys
import textwrap

import pytest

from vq import transport
from vq.config import HostConfig

SRC_ROOT = pathlib.Path(transport.__file__).resolve().parent

HOST = HostConfig(ssh="fakehost", remote_vq="vq")


# ----------------------------------------------------------------------
# 1. behavioural — a real read-loop over a real file
# ----------------------------------------------------------------------


def _install_fake_ssh(tmp_path: pathlib.Path) -> pathlib.Path:
    """A stand-in for ssh that drains stdin, like the real thing."""
    bindir = tmp_path / "bin"
    bindir.mkdir(parents=True, exist_ok=True)
    fake = bindir / "ssh"
    fake.write_text("#!/bin/sh\ncat > /dev/null\nexit 0\n")
    fake.chmod(fake.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    return bindir


@pytest.mark.parametrize(
    "entrypoint",
    [
        "transport.run_remote_vq(HOST, 'status', 'localhost', 'JOB', timeout=30,"
        " check=False)",
        "transport.run_remote_shell(HOST, 'true', check=False)",
    ],
    ids=["run_remote_vq", "run_remote_shell"],
)
def test_read_loop_over_a_job_list_processes_every_row(
    tmp_path: pathlib.Path, entrypoint: str
) -> None:
    """The defect, reproduced at operator altitude: N rows in, N rows out."""
    rows = [f"job{n:04d}" for n in range(8)]
    listing = tmp_path / "list.tsv"
    listing.write_text("".join(f"{r}\n" for r in rows))

    caller = tmp_path / "poll_one.py"
    caller.write_text(
        textwrap.dedent(
            f"""
            import sys
            from vq import transport
            from vq.config import HostConfig
            HOST = HostConfig(ssh="fakehost", remote_vq="vq")
            {entrypoint}
            """
        ).strip()
        + "\n"
    )
    counter = tmp_path / "iterations"
    loop = (
        f'while IFS= read -r id; do '
        f'  echo "$id" >> "{counter}"; '
        f'  "{sys.executable}" "{caller}"; '
        f'done < "{listing}"'
    )
    env = dict(os.environ)
    env["PATH"] = f"{_install_fake_ssh(tmp_path)}{os.pathsep}{env['PATH']}"
    env["PYTHONPATH"] = str(SRC_ROOT.parent)

    proc = subprocess.run(
        ["sh", "-c", loop],
        capture_output=True,
        text=True,
        timeout=120,
        stdin=subprocess.DEVNULL,
        env=env,
    )

    assert proc.returncode == 0, proc.stderr
    processed = counter.read_text().split()
    assert processed == rows, (
        f"the read-loop processed {len(processed)} of {len(rows)} rows; "
        "a vq subprocess consumed the caller's stdin"
    )


def test_streamed_fetch_does_not_consume_the_callers_stdin(
    tmp_path: pathlib.Path,
) -> None:
    """``vq fetch`` streams through Popen rather than run(); same contract."""
    rows = [f"job{n:04d}" for n in range(6)]
    listing = tmp_path / "list.tsv"
    listing.write_text("".join(f"{r}\n" for r in rows))

    caller = tmp_path / "fetch_one.py"
    caller.write_text(
        textwrap.dedent(
            """
            import contextlib
            from vq import transport
            from vq.config import HostConfig
            HOST = HostConfig(ssh="fakehost", remote_vq="vq")
            with contextlib.suppress(transport.RemoteError):
                with transport.stream_remote_vq(HOST, "tar-workspace", "x") as s:
                    s.stdout.read()
            """
        ).strip()
        + "\n"
    )
    counter = tmp_path / "iterations"
    loop = (
        f'while IFS= read -r id; do '
        f'  echo "$id" >> "{counter}"; '
        f'  "{sys.executable}" "{caller}"; '
        f'done < "{listing}"'
    )
    env = dict(os.environ)
    env["PATH"] = f"{_install_fake_ssh(tmp_path)}{os.pathsep}{env['PATH']}"
    env["PYTHONPATH"] = str(SRC_ROOT.parent)

    proc = subprocess.run(
        ["sh", "-c", loop],
        capture_output=True,
        text=True,
        timeout=120,
        stdin=subprocess.DEVNULL,
        env=env,
    )

    assert proc.returncode == 0, proc.stderr
    assert counter.read_text().split() == rows


# ----------------------------------------------------------------------
# 2. unit — the transport layer hands DEVNULL to its children
# ----------------------------------------------------------------------


class TestTransportWithholdsStdin:
    def test_run_remote_vq_passes_devnull(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        seen: dict[str, object] = {}

        def fake_run(cmd, **kwargs):  # noqa: ANN001, ANN003
            seen.update(kwargs)
            return subprocess.CompletedProcess(cmd, 0, "", "")

        monkeypatch.setattr(transport.subprocess, "run", fake_run)
        transport.run_remote_vq(HOST, "status", check=False, timeout=5)

        assert seen["stdin"] is subprocess.DEVNULL

    def test_run_remote_vq_with_stdin_data_still_pipes_it(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Withholding stdin must not break the token-forwarding path."""
        seen: dict[str, object] = {}

        def fake_run(cmd, **kwargs):  # noqa: ANN001, ANN003
            seen.update(kwargs)
            return subprocess.CompletedProcess(cmd, 0, "", "")

        monkeypatch.setattr(transport.subprocess, "run", fake_run)
        transport.run_remote_vq(
            HOST, "admin", check=False, timeout=5, stdin_data="secret-token"
        )

        assert "stdin" not in seen
        assert seen["input"] == "secret-token"

    def test_run_remote_shell_passes_devnull(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        seen: dict[str, object] = {}

        def fake_run(cmd, **kwargs):  # noqa: ANN001, ANN003
            seen.update(kwargs)
            return subprocess.CompletedProcess(cmd, 0, "", "")

        monkeypatch.setattr(transport.subprocess, "run", fake_run)
        transport.run_remote_shell(HOST, "true", check=False)

        assert seen["stdin"] is subprocess.DEVNULL

    def test_stream_remote_vq_passes_devnull(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import io

        seen: dict[str, object] = {}

        class FakeProc:
            def __init__(self) -> None:
                self.stdout = io.BytesIO(b"")
                self.stderr = io.BytesIO(b"")
                self.returncode: int | None = None

            def wait(self) -> int:
                self.returncode = 0
                return 0

            def kill(self) -> None:
                return None

        def fake_popen(cmd, **kwargs):  # noqa: ANN001, ANN003
            seen.update(kwargs)
            return FakeProc()

        monkeypatch.setattr(transport.subprocess, "Popen", fake_popen)
        with transport.stream_remote_vq(HOST, "tar-workspace", "job") as stream:
            stream.stdout.read()

        assert seen["stdin"] is subprocess.DEVNULL

    def test_owned_subprocess_passes_devnull(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        proc = transport.run_owned_subprocess(
            [sys.executable, "-c", "import sys; print(len(sys.stdin.read()))"],
            timeout=30,
        )
        assert proc.stdout.strip() == "0"


# ----------------------------------------------------------------------
# 3. AST guard — no new call site may inherit stdin
# ----------------------------------------------------------------------

#: Functions that hand stdin to their child through a helper or a
#: ``setdefault`` on forwarded ``**kwargs`` rather than a literal keyword.
#: Each is asserted separately by the unit tests above or by inspection here.
_INDIRECT_STDIN_FUNCTIONS = {
    # transport.py — routes through _subprocess_session_kwargs()
    "run_remote_vq",
    "run_remote_shell",
    "stream_remote_vq",
    # **kwargs pass-through wrappers that setdefault stdin
    "_mutating_git_run",
    "_rollout_update_runner",
}

_SUBPROCESS_SPAWNERS = {"run", "Popen", "check_output", "check_call", "call"}


def _enclosing_function(tree: ast.AST, node: ast.Call) -> str | None:
    best: tuple[int, str] | None = None
    for candidate in ast.walk(tree):
        if not isinstance(candidate, ast.FunctionDef | ast.AsyncFunctionDef):
            continue
        encloses = candidate.lineno <= node.lineno <= (
            candidate.end_lineno or node.lineno
        )
        if encloses and (best is None or candidate.lineno > best[0]):
            best = (candidate.lineno, candidate.name)
    return best[1] if best else None


def test_no_vq_subprocess_call_inherits_stdin() -> None:
    """Every ``subprocess`` spawn in ``src/vq`` declares its stdin.

    A call with no ``stdin=`` (and no ``input=``) inherits the operator's
    stdin, which is #118. New code must be explicit; the small set of
    indirect sites is enumerated above and covered by its own tests.
    """
    offenders: list[str] = []
    for path in sorted(SRC_ROOT.rglob("*.py")):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            if not (
                isinstance(func, ast.Attribute)
                and isinstance(func.value, ast.Name)
                and func.value.id == "subprocess"
                and func.attr in _SUBPROCESS_SPAWNERS
            ):
                continue
            keywords = {kw.arg for kw in node.keywords if kw.arg}
            if "stdin" in keywords or "input" in keywords:
                continue
            enclosing = _enclosing_function(tree, node)
            if enclosing in _INDIRECT_STDIN_FUNCTIONS:
                continue
            offenders.append(
                f"{path.relative_to(SRC_ROOT)}:{node.lineno} "
                f"subprocess.{func.attr} in {enclosing}()"
            )
    assert not offenders, (
        "these subprocess call sites inherit the caller's stdin (#118); pass "
        "stdin=subprocess.DEVNULL:\n  " + "\n  ".join(offenders)
    )
