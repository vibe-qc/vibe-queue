"""Old-code contracts for managed-venv healthcheck execution.

``VenvProgram`` owns when a healthcheck runs.  The command parser, subprocess
envelope, environment, and result reduction form a model-free probe leaf that
can move independently once these results are pinned.
"""
from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

from vq import config


def test_healthcheck_probe_uses_config_free_compatibility_boundary() -> None:
    from vq import _program_probe

    assert config._last_nonempty_line is _program_probe._last_nonempty_line
    assert config._timeout_stream_text is _program_probe._timeout_stream_text


def test_healthcheck_model_adapter_forwards_explicit_probe_inputs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from vq import _program_probe

    calls: list[tuple[str | None, str, Path]] = []

    def run_healthcheck(
        healthcheck_command: str | None,
        *,
        python: str,
        git_dir: Path,
    ) -> tuple[bool, str | None]:
        calls.append((healthcheck_command, python, git_dir))
        return False, "sentinel healthcheck result"

    monkeypatch.setattr(
        _program_probe,
        "run_venv_healthcheck",
        run_healthcheck,
    )

    assert _program("probe --flag")._run_healthcheck(Path("/checkout")) == (
        False,
        "sentinel healthcheck result",
    )
    assert calls == [
        ("probe --flag", "/managed/venv/bin/python", Path("/checkout"))
    ]


def _program(command: str | None) -> config.VenvProgram:
    return config.VenvProgram(
        kind="venv",
        python="/managed/venv/bin/python",
        git_dir="/checkout",
        healthcheck_command=command,
    )


def _forbid_subprocess(monkeypatch: pytest.MonkeyPatch) -> None:
    def unexpected(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("this healthcheck result must not run a subprocess")

    monkeypatch.setattr(subprocess, "run", unexpected)


@pytest.mark.parametrize("command", (None, ""))
def test_healthcheck_absent_is_a_noop(
    command: str | None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _forbid_subprocess(monkeypatch)

    assert _program(command)._run_healthcheck(Path("/checkout")) == (True, None)


@pytest.mark.parametrize(
    ("command", "expected"),
    (
        ("   \t", "healthcheck command is empty"),
        ("probe 'unterminated", "healthcheck command parse failed: No closing quotation"),
    ),
)
def test_healthcheck_command_gate_is_exact_without_subprocess(
    command: str,
    expected: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _forbid_subprocess(monkeypatch)

    assert _program(command)._run_healthcheck(Path("/checkout")) == (
        False,
        expected,
    )


def test_healthcheck_subprocess_and_inherited_environment_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PATH", "/base/bin")
    monkeypatch.setenv("PYVISTA_OFF_SCREEN", "operator-value")
    monkeypatch.setenv("VQ_HEALTHCHECK_SENTINEL", "preserved")
    calls: list[tuple[list[str], dict[str, object]]] = []

    def run(argv: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append((argv, kwargs))
        return subprocess.CompletedProcess(
            argv,
            0,
            stdout="first line\nlast stdout\n",
            stderr="last stderr\n",
        )

    monkeypatch.setattr(subprocess, "run", run)

    assert _program("probe '' --label 'two words'")._run_healthcheck(
        Path("/checkout")
    ) == (True, "last stderr")
    assert len(calls) == 1
    argv, kwargs = calls[0]
    assert argv == ["probe", "", "--label", "two words"]
    env = kwargs.pop("env")
    assert kwargs == {
        "cwd": Path("/checkout"),
        "capture_output": True,
        "text": True,
        "timeout": 60.0,
        "stdin": subprocess.DEVNULL,
    }
    assert isinstance(env, dict)
    assert env is not os.environ
    assert env["PATH"] == f"/managed/venv/bin{os.pathsep}/base/bin"
    assert env["PYVISTA_OFF_SCREEN"] == "operator-value"
    assert env["VQ_HEALTHCHECK_SENTINEL"] == "preserved"
    assert os.environ["PATH"] == "/base/bin"
    assert os.environ["PYVISTA_OFF_SCREEN"] == "operator-value"


def test_healthcheck_defaults_offscreen_environment_and_empty_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("PYVISTA_OFF_SCREEN", raising=False)
    monkeypatch.delenv("PATH", raising=False)
    observed_env: dict[str, str] = {}

    def run(
        argv: list[str],
        **kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        env = kwargs["env"]
        assert isinstance(env, dict)
        observed_env.update(env)
        return subprocess.CompletedProcess(
            argv,
            0,
            stdout="",
            stderr="",
        )

    monkeypatch.setattr(subprocess, "run", run)

    assert _program("probe")._run_healthcheck(Path("/checkout")) == (
        True,
        "ok",
    )
    assert observed_env["PYVISTA_OFF_SCREEN"] == "True"
    assert observed_env["PATH"] == f"/managed/venv/bin{os.pathsep}"
    assert "PYVISTA_OFF_SCREEN" not in os.environ
    assert "PATH" not in os.environ


@pytest.mark.parametrize(
    ("returncode", "stdout", "stderr", "expected"),
    (
        (0, "first\nlast stdout\n", "", (True, "last stdout")),
        (0, " \n", "\n", (True, "ok")),
        (7, "", "", (False, "healthcheck rc=7")),
        (
            7,
            "stdout tail\n",
            "first error\nlast error\n",
            (False, "healthcheck rc=7: last error"),
        ),
    ),
)
def test_healthcheck_completed_process_result_matrix(
    returncode: int,
    stdout: str,
    stderr: str,
    expected: tuple[bool, str | None],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def run(argv: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            argv,
            returncode,
            stdout=stdout,
            stderr=stderr,
        )

    monkeypatch.setattr(subprocess, "run", run)

    assert _program("probe")._run_healthcheck(Path("/checkout")) == expected


@pytest.mark.parametrize(
    ("output", "expected"),
    (
        ("", None),
        (" \n\t", None),
        ("first\n  last value  \n\n", "last value"),
    ),
)
def test_last_nonempty_line_contract(
    output: str,
    expected: str | None,
) -> None:
    assert config._last_nonempty_line(output) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    (
        (None, ""),
        (b"plain bytes", "plain bytes"),
        (b"bad byte: \xff", "bad byte: \ufffd"),
        ("plain text", "plain text"),
        (7, "7"),
    ),
)
def test_timeout_stream_text_contract(value: object, expected: str) -> None:
    assert config._timeout_stream_text(value) == expected


@pytest.mark.parametrize(
    ("output", "stderr", "expected"),
    (
        (None, None, "healthcheck timed out after 60s"),
        (
            b"starting capture\n",
            b"waiting for offscreen GL: \xff\n",
            "healthcheck timed out after 60s: waiting for offscreen GL: \ufffd",
        ),
        (
            "string output tail\n",
            None,
            "healthcheck timed out after 60s: string output tail",
        ),
    ),
)
def test_healthcheck_timeout_result_is_exact(
    output: str | bytes | None,
    stderr: str | bytes | None,
    expected: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[list[str], dict[str, object]]] = []

    def run(argv: list[str], **kwargs: object) -> None:
        calls.append((argv, kwargs))
        raise subprocess.TimeoutExpired(
            argv,
            timeout=float(kwargs["timeout"]),
            output=output,
            stderr=stderr,
        )

    monkeypatch.setattr(subprocess, "run", run)

    assert _program("probe")._run_healthcheck(Path("/checkout")) == (
        False,
        expected,
    )
    assert len(calls) == 1
    assert calls[0][0] == ["probe"]
    assert calls[0][1]["timeout"] == 60.0


def test_healthcheck_start_error_result_is_exact(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def run(_argv: list[str], **_kwargs: object) -> None:
        raise OSError("injected healthcheck start failure")

    monkeypatch.setattr(subprocess, "run", run)

    # v0.26.1: the message names where vq looked. The commonest start error
    # here is errno 2 on a binary that used to live in the venv's own bin/,
    # and "No such file or directory: 'xvfb-run'" alone does not say that vq
    # searched the venv first.
    assert _program("probe")._run_healthcheck(Path("/checkout")) == (
        False,
        "healthcheck failed to start: injected healthcheck start failure "
        "(searched /managed/venv/bin then PATH)",
    )


def test_healthcheck_unexpected_exception_propagates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def run(_argv: list[str], **_kwargs: object) -> None:
        raise RuntimeError("unexpected healthcheck failure")

    monkeypatch.setattr(subprocess, "run", run)

    with pytest.raises(RuntimeError, match="unexpected healthcheck failure"):
        _program("probe")._run_healthcheck(Path("/checkout"))


class TestHealthcheckBinaryInsideTheVenv:
    """A healthcheck binary that only exists inside the venv is fragile.

    On host_b and host_e, `vibeview-dev`'s `healthcheck_command` ran `xvfb-run`,
    and no such binary existed anywhere on either system: the working one was
    a hand-written bash shim inside the old venv's `bin/`, present on no
    system path and reproduced by no reinstall. Migrating the venv broke every
    vibe-view healthcheck until the shim was copied across by hand.

    Not vq's bug, but vq's blast radius -- and invisible exactly while
    everything still works, which is when it can be fixed cheaply.
    """

    def _venv(self, tmp_path: Path, *, shim: bool) -> tuple[str, str]:
        venv_bin = tmp_path / "venv" / "bin"
        venv_bin.mkdir(parents=True)
        (venv_bin / "python").write_text("#!/bin/sh\n", encoding="utf-8")
        if shim:
            probe = venv_bin / "hc-probe"
            probe.write_text("#!/bin/sh\necho ok\n", encoding="utf-8")
            probe.chmod(0o755)
        return str(venv_bin / "python"), str(venv_bin)

    def test_a_venv_only_binary_is_flagged_while_it_still_works(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from vq import _program_probe

        python, venv_bin = self._venv(tmp_path, shim=True)
        monkeypatch.setenv("PATH", "/usr/bin:/bin")

        ok, detail = _program_probe.run_venv_healthcheck(
            "hc-probe", python=python, git_dir=tmp_path,
        )

        assert ok is True
        assert detail is not None
        assert "warning" in detail
        assert "resolves only inside the venv" in detail
        assert venv_bin in detail
        assert "a reprovision will not recreate it" in detail

    def test_a_binary_that_also_exists_on_the_system_is_not_flagged(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from vq import _program_probe

        python, _venv_bin = self._venv(tmp_path, shim=True)
        system_bin = tmp_path / "usr-bin"
        system_bin.mkdir()
        system = system_bin / "hc-probe"
        system.write_text("#!/bin/sh\necho ok\n", encoding="utf-8")
        system.chmod(0o755)
        monkeypatch.setenv("PATH", str(system_bin))

        ok, detail = _program_probe.run_venv_healthcheck(
            "hc-probe", python=python, git_dir=tmp_path,
        )

        assert ok is True
        assert detail == "ok"

    def test_an_ordinary_healthcheck_keeps_its_plain_result(
        self, tmp_path: Path,
    ) -> None:
        python, _venv_bin = self._venv(tmp_path, shim=False)

        ok, detail = _program_probe_module().run_venv_healthcheck(
            "/bin/echo healthy", python=python, git_dir=tmp_path,
        )

        assert (ok, detail) == (True, "healthy")

    def test_a_path_spelling_is_not_this_case(self, tmp_path: Path) -> None:
        """An argv[0] that names a path is not relying on the venv search."""
        from vq import _program_probe

        python, venv_bin = self._venv(tmp_path, shim=True)

        assert _program_probe._healthcheck_binary_is_venv_only(
            f"{venv_bin}/hc-probe", venv_bin=venv_bin, system_path="/usr/bin",
        ) is None


def _program_probe_module():  # type: ignore[no-untyped-def]
    from vq import _program_probe

    return _program_probe
