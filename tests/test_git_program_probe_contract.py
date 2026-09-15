"""Old-code contracts for Git-backed venv runtime inspection.

The low-level Git probes are configuration-free subprocess leaves, while the
``VenvProgram`` model owns fallback order and user-visible detail assembly.
These cases land before moving only those leaves out of ``vq.config``.
"""
from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

from vq import config


def test_git_probe_leaves_use_config_free_compatibility_aliases() -> None:
    from vq import _program_probe

    assert config._query_git is _program_probe._query_git
    assert config._git_has_changes is _program_probe._git_has_changes


@pytest.mark.parametrize(
    ("returncode", "stdout", "stderr", "expected"),
    (
        (0, "  abc123\n", "ignored warning", "abc123"),
        (0, " \n\t", "ignored warning", None),
        (7, "value despite failure\n", "git failed", None),
    ),
)
def test_query_git_result_and_subprocess_contract(
    returncode: int,
    stdout: str,
    stderr: str,
    expected: str | None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[list[str], dict[str, object]]] = []

    def run(argv: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append((argv, kwargs))
        return subprocess.CompletedProcess(
            argv,
            returncode,
            stdout=stdout,
            stderr=stderr,
        )

    monkeypatch.setattr(subprocess, "run", run)

    assert config._query_git(
        "/checkout", "rev-parse", "--short=12", "HEAD"
    ) == expected
    assert calls == [
        (
            [
                "git",
                "-C",
                "/checkout",
                "rev-parse",
                "--short=12",
                "HEAD",
            ],
            {
                "capture_output": True,
                "text": True,
                "timeout": 5.0,
                "stdin": subprocess.DEVNULL,
            },
        )
    ]


@pytest.mark.parametrize("failure", ("timeout", "oserror"))
def test_query_git_expected_faults_return_none(
    failure: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[list[str], dict[str, object]]] = []

    def run(argv: list[str], **kwargs: object) -> None:
        calls.append((argv, kwargs))
        if failure == "timeout":
            raise subprocess.TimeoutExpired(
                argv,
                timeout=float(kwargs["timeout"]),
                output="partial sha",
            )
        raise OSError("injected git start failure")

    monkeypatch.setattr(subprocess, "run", run)

    assert config._query_git("/checkout", "describe", "--always") is None
    assert calls == [
        (
            ["git", "-C", "/checkout", "describe", "--always"],
            {
                "capture_output": True,
                "text": True,
                "timeout": 5.0,
                "stdin": subprocess.DEVNULL,
            },
        )
    ]


def test_query_git_unexpected_exception_propagates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def run(_argv: list[str], **_kwargs: object) -> None:
        raise RuntimeError("unexpected git query failure")

    monkeypatch.setattr(subprocess, "run", run)

    with pytest.raises(RuntimeError, match="unexpected git query failure"):
        config._query_git("/checkout", "rev-parse", "HEAD")


@pytest.mark.parametrize(
    ("returncode", "stdout", "expected"),
    (
        (0, "", False),
        (0, " \n\t", False),
        (0, " M tracked.py\n", True),
        (7, " M ignored-on-failure.py\n", None),
    ),
)
def test_git_has_changes_result_and_subprocess_contract(
    returncode: int,
    stdout: str,
    expected: bool | None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[list[str], dict[str, object]]] = []

    def run(argv: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append((argv, kwargs))
        return subprocess.CompletedProcess(
            argv,
            returncode,
            stdout=stdout,
            stderr="ignored status warning",
        )

    monkeypatch.setattr(subprocess, "run", run)

    assert config._git_has_changes("/checkout") is expected
    assert calls == [
        (
            ["git", "-C", "/checkout", "status", "--porcelain"],
            {
                "capture_output": True,
                "text": True,
                "timeout": 5.0,
                "stdin": subprocess.DEVNULL,
                "env": {**os.environ, "GIT_OPTIONAL_LOCKS": "0"},
            },
        )
    ]


@pytest.mark.parametrize("failure", ("timeout", "oserror"))
def test_git_has_changes_expected_faults_are_unknown(
    failure: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def run(argv: list[str], **kwargs: object) -> None:
        if failure == "timeout":
            raise subprocess.TimeoutExpired(
                argv,
                timeout=float(kwargs["timeout"]),
                output=" M partial.py\n",
            )
        raise OSError("injected git status start failure")

    monkeypatch.setattr(subprocess, "run", run)

    assert config._git_has_changes("/checkout") is None


def test_git_has_changes_unexpected_exception_propagates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def run(_argv: list[str], **_kwargs: object) -> None:
        raise RuntimeError("unexpected git status failure")

    monkeypatch.setattr(subprocess, "run", run)

    with pytest.raises(RuntimeError, match="unexpected git status failure"):
        config._git_has_changes("/checkout")


def test_venv_git_identity_methods_map_commands_and_results(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    full_sha = "a" * 40
    responses = {
        ("rev-parse", "--short=12", "HEAD"): (0, "abc123def456\n"),
        ("rev-parse", "HEAD"): (0, f"{full_sha}\n"),
        ("describe", "--tags", "--always"): (0, "v0.15.9-2-gabc123\n"),
        ("branch", "--show-current"): (0, "main\n"),
        ("status", "--porcelain"): (0, " M tracked.py\n"),
    }
    commands: list[tuple[str, ...]] = []

    def run(argv: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        command = tuple(argv[3:])
        commands.append(command)
        returncode, stdout = responses[command]
        return subprocess.CompletedProcess(
            argv,
            returncode,
            stdout=stdout,
            stderr="",
        )

    monkeypatch.setattr(subprocess, "run", run)
    program = config.VenvProgram(
        kind="venv",
        python="/venv/bin/python",
        git_dir="/checkout",
    )

    assert program.current_git_sha() == "abc123def456"
    assert program.current_git_sha(full=True) == full_sha
    assert program.current_git_describe() == "v0.15.9-2-gabc123"
    assert program.current_git_branch() == "main"
    assert program.current_git_dirty() is True
    assert commands == [
        ("rev-parse", "--short=12", "HEAD"),
        ("rev-parse", "HEAD"),
        ("describe", "--tags", "--always"),
        ("branch", "--show-current"),
        ("status", "--porcelain"),
    ]


@pytest.mark.parametrize(
    ("first_returncode", "first_stdout"),
    ((0, "\n"), (7, "ignored-branch\n")),
)
def test_venv_git_branch_falls_back_only_when_first_query_has_no_value(
    first_returncode: int,
    first_stdout: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[list[str]] = []
    responses = iter(
        (
            (first_returncode, first_stdout),
            (0, "detached-branch\n"),
        )
    )

    def run(argv: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(argv)
        returncode, stdout = next(responses)
        return subprocess.CompletedProcess(
            argv,
            returncode,
            stdout=stdout,
            stderr="",
        )

    monkeypatch.setattr(subprocess, "run", run)
    program = config.VenvProgram(
        kind="venv",
        python="/venv/bin/python",
        git_dir="/checkout",
    )

    assert program.current_git_branch() == "detached-branch"
    assert calls == [
        ["git", "-C", "/checkout", "branch", "--show-current"],
        [
            "git",
            "-C",
            "/checkout",
            "rev-parse",
            "--abbrev-ref",
            "HEAD",
        ],
    ]


def test_venv_availability_preserves_git_detail_order(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    python = tmp_path / "venv" / "bin" / "python"
    python.parent.mkdir(parents=True)
    python.touch()
    python.chmod(0o755)
    git_dir = tmp_path / "checkout"
    (git_dir / ".git").mkdir(parents=True)
    responses = {
        ("rev-parse", "--short=12", "HEAD"): (0, "abc123def456\n"),
        ("describe", "--tags", "--always"): (0, "v0.15.9\n"),
        ("branch", "--show-current"): (0, "main\n"),
        ("status", "--porcelain"): (0, " M tracked.py\n"),
    }
    commands: list[tuple[str, ...]] = []

    def run(argv: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        command = tuple(argv[3:])
        commands.append(command)
        returncode, stdout = responses[command]
        return subprocess.CompletedProcess(
            argv,
            returncode,
            stdout=stdout,
            stderr="",
        )

    monkeypatch.setattr(subprocess, "run", run)
    program = config.VenvProgram(
        kind="venv",
        python=str(python),
        git_dir=str(git_dir),
    )

    assert program.availability() == (
        True,
        f"venv ok (python={python}; git_dir={git_dir}; "
        "git_sha=abc123def456; git_describe=v0.15.9; "
        "git_branch=main; git_dirty=true)",
    )
    assert commands == [
        ("rev-parse", "--short=12", "HEAD"),
        ("describe", "--tags", "--always"),
        ("branch", "--show-current"),
        ("status", "--porcelain"),
    ]


def test_venv_availability_reports_unknown_dirty_when_git_queries_fail(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    python = tmp_path / "venv" / "bin" / "python"
    python.parent.mkdir(parents=True)
    python.touch()
    python.chmod(0o755)
    git_dir = tmp_path / "checkout"
    (git_dir / ".git").mkdir(parents=True)

    def run(argv: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            argv,
            7,
            stdout="ignored failure output\n",
            stderr="git failed",
        )

    monkeypatch.setattr(subprocess, "run", run)
    program = config.VenvProgram(
        kind="venv",
        python=str(python),
        git_dir=str(git_dir),
    )

    assert program.availability() == (
        True,
        f"venv ok (python={python}; git_dir={git_dir}; git_dirty=unknown)",
    )


def test_venv_expected_git_pin_fails_closed_when_query_is_unreadable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    python = tmp_path / "venv" / "bin" / "python"
    python.parent.mkdir(parents=True)
    python.touch()
    python.chmod(0o755)
    git_dir = tmp_path / "checkout"
    (git_dir / ".git").mkdir(parents=True)
    commands: list[tuple[str, ...]] = []

    def run(argv: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        commands.append(tuple(argv[3:]))
        return subprocess.CompletedProcess(
            argv,
            7,
            stdout="ignored failure output\n",
            stderr="git failed",
        )

    monkeypatch.setattr(subprocess, "run", run)
    program = config.VenvProgram(
        kind="venv",
        python=str(python),
        git_dir=str(git_dir),
        expected_git_sha="deadbeef",
    )

    assert program.availability() == (
        False,
        "runtime pin mismatch: git_sha expected deadbeef, "
        "but current git SHA could not be read",
    )
    assert commands == [
        ("rev-parse", "--short=12", "HEAD"),
        ("describe", "--tags", "--always"),
        ("branch", "--show-current"),
        ("rev-parse", "--abbrev-ref", "HEAD"),
        ("status", "--porcelain"),
        ("rev-parse", "--short=12", "HEAD"),
    ]
