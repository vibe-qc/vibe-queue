"""Old-code result contracts for binary and ORCA/MPI program probes.

The matrix drives the public ``BinaryProgram.availability()`` method.  It pins
the behavior that must survive moving active filesystem/subprocess inspection
out of the Pydantic configuration model.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

from vq import config


def _write_file(path: Path, *, executable: bool = False) -> None:
    path.write_text("#!/bin/sh\n", encoding="utf-8")
    path.chmod(0o755 if executable else 0o644)


def _availability(path: Path) -> tuple[bool, str]:
    return config.BinaryProgram(kind="binary", binary=str(path)).availability()


def _orca_pair(tmp_path: Path) -> tuple[Path, Path]:
    orca = tmp_path / "orca"
    startup = tmp_path / "orca_startup_mpi"
    _write_file(orca, executable=True)
    _write_file(startup, executable=True)
    return orca, startup


def _crystal_binary(tmp_path: Path, name: str = "crystal23") -> Path:
    binary = tmp_path / name
    _write_file(binary, executable=True)
    return binary


def _forbid_subprocess(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unexpected(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("this binary result must not launch a subprocess")

    monkeypatch.setattr(subprocess, "run", unexpected)


def test_missing_binary_result_is_exact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _forbid_subprocess(monkeypatch)
    binary = tmp_path / "missing"
    assert _availability(binary) == (False, f"not found: {binary}")


def test_directory_binary_result_is_exact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _forbid_subprocess(monkeypatch)
    assert _availability(tmp_path) == (False, f"not a file: {tmp_path}")


def test_nonexecutable_binary_result_is_exact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _forbid_subprocess(monkeypatch)
    binary = tmp_path / "orca"
    _write_file(binary)
    _write_file(tmp_path / "orca_startup_mpi", executable=True)
    assert _availability(binary) == (False, f"not executable: {binary}")


@pytest.mark.parametrize(
    "binary_name", ("generic", "ORCA", "Pcrystal", "properties")
)
def test_generic_binary_ignores_mpi_sibling(
    binary_name: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _forbid_subprocess(monkeypatch)
    binary = tmp_path / binary_name
    _write_file(binary, executable=True)
    _write_file(tmp_path / "orca_startup_mpi", executable=True)
    assert _availability(binary) == (True, f"executable at {binary}")


def test_crystal_loader_failure_is_unavailable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    binary = _crystal_binary(tmp_path)
    calls: list[tuple[list[str], dict[str, object]]] = []

    def run(
        argv: list[str], **kwargs: object
    ) -> subprocess.CompletedProcess[str]:
        calls.append((argv, kwargs))
        return subprocess.CompletedProcess(
            argv,
            127,
            stdout="",
            stderr=(
                f"{binary}: error while loading shared libraries: "
                "libmpi_usempif08.so.40: cannot open shared object file\n"
            ),
        )

    monkeypatch.setattr(subprocess, "run", run)

    assert _availability(binary) == (
        False,
        "CRYSTAL startup cannot load runtime: "
        "libmpi_usempif08.so.40: cannot open shared object file",
    )
    assert len(calls) == 1
    argv, kwargs = calls[0]
    assert argv == [str(binary)]
    assert kwargs["capture_output"] is True
    assert kwargs["text"] is True
    assert kwargs["timeout"] == 10.0
    assert kwargs["stdin"] is subprocess.DEVNULL
    probe_cwd = kwargs["cwd"]
    assert isinstance(probe_cwd, str)
    assert Path(probe_cwd) != tmp_path
    assert not Path(probe_cwd).exists()


def test_crystal_expected_no_input_exit_proves_startup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    binary = _crystal_binary(tmp_path, "crystal")

    def run(
        argv: list[str], **_kwargs: object
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            argv,
            0,
            stdout=" ERROR **** INPUT ****  END OF DATA IN INPUT DECK\n",
            stderr="",
        )

    monkeypatch.setattr(subprocess, "run", run)
    assert _availability(binary) == (
        True,
        f"executable at {binary}; CRYSTAL startup loads",
    )


def test_crystal_spoofed_identity_without_no_input_signature_is_unavailable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    binary = _crystal_binary(tmp_path)

    def run(
        argv: list[str], **_kwargs: object
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            argv,
            0,
            stdout="CRYSTAL wrapper is installed but disabled\n",
            stderr="",
        )

    monkeypatch.setattr(subprocess, "run", run)
    assert _availability(binary) == (
        False,
        "CRYSTAL no-input diagnostic missing: "
        "CRYSTAL wrapper is installed but disabled",
    )


def test_crystal_nonzero_with_no_input_signature_is_unavailable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    binary = _crystal_binary(tmp_path)

    def run(
        argv: list[str], **_kwargs: object
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            argv,
            2,
            stdout=" ERROR **** INPUT ****  END OF DATA IN INPUT DECK\n",
            stderr="",
        )

    monkeypatch.setattr(subprocess, "run", run)
    assert _availability(binary) == (
        False,
        "CRYSTAL startup failed (exit 2): "
        "ERROR **** INPUT ****  END OF DATA IN INPUT DECK",
    )


def test_crystal_signal_exit_is_unavailable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    binary = _crystal_binary(tmp_path)

    def run(
        argv: list[str], **_kwargs: object
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            argv,
            -9,
            stdout="",
            stderr="killed during startup\n",
        )

    monkeypatch.setattr(subprocess, "run", run)
    assert _availability(binary) == (
        False,
        "CRYSTAL startup terminated by signal 9: killed during startup",
    )


def test_crystal_relative_config_path_is_resolved_before_probe_cwd(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    binary = _crystal_binary(tmp_path)
    monkeypatch.chdir(tmp_path)
    calls: list[tuple[list[str], str]] = []

    def run(
        argv: list[str], **kwargs: object
    ) -> subprocess.CompletedProcess[str]:
        cwd = kwargs.get("cwd")
        assert isinstance(cwd, str)
        calls.append((argv, cwd))
        return subprocess.CompletedProcess(
            argv,
            0,
            stdout=" ERROR **** INPUT ****  END OF DATA IN INPUT DECK\n",
            stderr="",
        )

    monkeypatch.setattr(subprocess, "run", run)
    assert config.BinaryProgram(
        kind="binary", binary="crystal23"
    ).availability() == (
        True,
        "executable at crystal23; CRYSTAL startup loads",
    )
    assert len(calls) == 1
    argv, cwd = calls[0]
    assert argv == [str(binary.resolve())]
    assert Path(cwd) != tmp_path


@pytest.mark.parametrize("returncode", (126, 127))
def test_crystal_wrapper_launch_failure_is_unavailable(
    returncode: int,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    binary = _crystal_binary(tmp_path)

    def run(
        argv: list[str], **_kwargs: object
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            argv,
            returncode,
            stdout="",
            stderr="exec: /missing/crystal23: No such file or directory\n",
        )

    monkeypatch.setattr(subprocess, "run", run)
    assert _availability(binary) == (
        False,
        "CRYSTAL startup failed (exit "
        f"{returncode}): exec: /missing/crystal23: No such file or directory",
    )


def test_crystal_probe_timeout_is_unavailable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    binary = _crystal_binary(tmp_path, "crystal23demo")

    def run(_argv: list[str], **_kwargs: object) -> None:
        raise subprocess.TimeoutExpired(
            [str(binary)], timeout=10.0, stderr="startup stalled\n"
        )

    monkeypatch.setattr(subprocess, "run", run)
    assert _availability(binary) == (
        False,
        f"CRYSTAL startup probe timed out: {binary}: startup stalled",
    )


def test_crystal_probe_start_error_is_unavailable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    binary = _crystal_binary(tmp_path)

    def run(_argv: list[str], **_kwargs: object) -> None:
        raise OSError("missing shell interpreter")

    monkeypatch.setattr(subprocess, "run", run)
    assert _availability(binary) == (
        False,
        "CRYSTAL startup probe failed: missing shell interpreter",
    )


def test_orca_without_mpi_sibling_keeps_generic_binary_result(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _forbid_subprocess(monkeypatch)
    orca = tmp_path / "orca"
    _write_file(orca, executable=True)
    assert _availability(orca) == (True, f"executable at {orca}")


def test_orca_with_directory_mpi_sibling_reports_serial_executable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _forbid_subprocess(monkeypatch)
    orca = tmp_path / "orca"
    _write_file(orca, executable=True)
    (tmp_path / "orca_startup_mpi").mkdir()
    assert _availability(orca) == (
        True,
        f"executable at {orca}; "
        "ORCA MPI startup not present; serial executable only",
    )


def test_orca_with_nonexecutable_mpi_sibling_remains_serially_available(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _forbid_subprocess(monkeypatch)
    orca = tmp_path / "orca"
    startup = tmp_path / "orca_startup_mpi"
    _write_file(orca, executable=True)
    _write_file(startup)
    assert _availability(orca) == (
        True,
        f"executable at {orca}; ORCA MPI startup not executable: {startup}; "
        "serial ORCA only until MPI runtime is fixed",
    )


@pytest.mark.parametrize("returncode", (1, 127))
def test_orca_mpi_nonloader_exit_code_is_ignored(
    returncode: int,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    orca, startup = _orca_pair(tmp_path)
    calls: list[tuple[list[str], dict[str, object]]] = []

    def run(argv: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append((argv, kwargs))
        return subprocess.CompletedProcess(
            argv,
            returncode,
            stdout="",
            stderr="Fatal Error (ORCA_StartUp): no input files\n",
        )

    monkeypatch.setattr(subprocess, "run", run)

    assert _availability(orca) == (
        True,
        f"executable at {orca}; ORCA MPI startup loads",
    )
    assert calls == [
        (
            [str(startup)],
            {
                "capture_output": True,
                "text": True,
                "timeout": 10.0,
                "stdin": subprocess.DEVNULL,
            },
        )
    ]


@pytest.mark.parametrize(
    ("stdout", "stderr", "detail"),
    (
        ("Library not loaded: libmpi.40.dylib\n", "", "Library not loaded: libmpi.40.dylib"),
        ("DYLD[42]: missing runtime\n", "", "DYLD[42]: missing runtime"),
        (
            "",
            "error while loading shared libraries: libmpi.so.40\n",
            "error while loading shared libraries: libmpi.so.40",
        ),
        (
            "",
            "cannot open shared object file: No such file or directory\n",
            "cannot open shared object file: No such file or directory",
        ),
    ),
)
def test_orca_mpi_loader_signatures_degrade_to_serial(
    stdout: str,
    stderr: str,
    detail: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    orca, startup = _orca_pair(tmp_path)

    def run(argv: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        assert argv == [str(startup)]
        return subprocess.CompletedProcess(
            argv,
            127,
            stdout=stdout,
            stderr=stderr,
        )

    monkeypatch.setattr(subprocess, "run", run)
    assert _availability(orca) == (
        True,
        f"executable at {orca}; "
        f"ORCA MPI startup cannot load runtime: {detail}; "
        "serial ORCA only until MPI runtime is fixed",
    )


def test_orca_mpi_failure_detail_uses_first_stdout_line(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    orca, startup = _orca_pair(tmp_path)

    def run(argv: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        assert argv == [str(startup)]
        return subprocess.CompletedProcess(
            argv,
            127,
            stdout="benign startup preamble\n",
            stderr="error while loading shared libraries: libmpi.so.40\n",
        )

    monkeypatch.setattr(subprocess, "run", run)
    assert _availability(orca) == (
        True,
        f"executable at {orca}; "
        "ORCA MPI startup cannot load runtime: benign startup preamble; "
        "serial ORCA only until MPI runtime is fixed",
    )


def test_orca_mpi_timeout_degrades_to_serial_without_partial_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    orca, startup = _orca_pair(tmp_path)

    def run(argv: list[str], **_kwargs: object) -> None:
        raise subprocess.TimeoutExpired(
            argv,
            timeout=10.0,
            output="partial loader output",
        )

    monkeypatch.setattr(subprocess, "run", run)
    assert _availability(orca) == (
        True,
        f"executable at {orca}; ORCA MPI startup probe timed out: {startup}; "
        "serial ORCA only until MPI runtime is fixed",
    )


def test_orca_mpi_start_error_degrades_to_serial(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    orca, _startup = _orca_pair(tmp_path)

    def run(_argv: list[str], **_kwargs: object) -> None:
        raise OSError("injected exec failure")

    monkeypatch.setattr(subprocess, "run", run)
    assert _availability(orca) == (
        True,
        f"executable at {orca}; ORCA MPI startup probe failed: "
        "injected exec failure; serial ORCA only until MPI runtime is fixed",
    )


def test_orca_mpi_unexpected_exception_propagates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    orca, _startup = _orca_pair(tmp_path)

    def run(_argv: list[str], **_kwargs: object) -> None:
        raise RuntimeError("unexpected probe failure")

    monkeypatch.setattr(subprocess, "run", run)
    with pytest.raises(RuntimeError, match="unexpected probe failure"):
        _availability(orca)


def test_program_probe_module_has_no_config_model_dependency(tmp_path: Path) -> None:
    code = (
        "import sys; before = set(sys.modules); import vq._program_probe; "
        "added = set(sys.modules) - before; "
        "unexpected = {'vq.config', 'pydantic'} & added; "
        "assert not unexpected, sorted(unexpected)"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
