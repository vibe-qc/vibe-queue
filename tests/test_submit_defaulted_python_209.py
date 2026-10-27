"""#209: a remote single-file --program submit runs the program's interpreter.

The client cannot see the remote's program registry, so it still sends its
default interpreter as the command but marks it with the hidden
``--defaulted-python``. The receiving vq binds a pinned venv program's bare
``python`` and keeps the default for any other program. Every other submit's
wire is unchanged, and an older receiving vq refuses the marked submit rather
than run it under the wrong interpreter.
"""
from __future__ import annotations

import subprocess
import sys
import tarfile
from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import config, paths, submit
from vq.cli import main
from vq.config import HostConfig
from vq.spec import JobSpec

sys.path.insert(0, str(Path(__file__).parent))
from test_submit_remote import TransportCalls, calls  # noqa: E402,F401


def _remote_argv(calls: TransportCalls) -> tuple[str, ...]:  # noqa: F811
    (argv,) = calls.remote_vq
    return argv


class TestClientWire:
    @pytest.mark.parametrize("remote_python", ["/remote/py3", None])
    def test_a_defaulted_interpreter_is_marked(
        self, tmp_path: Path, calls: TransportCalls, remote_python: str | None,  # noqa: F811
    ) -> None:
        src = tmp_path / "input.py"
        src.write_text("")
        host_cfg = HostConfig(ssh="h", remote_vq="vq", remote_python=remote_python)
        submit.submit_remote(host="h", host_cfg=host_cfg, input_file=str(src),
                             program="vibeqc-dev")
        argv = _remote_argv(calls)
        interp = remote_python or "python"
        sep = argv.index("--")
        assert argv[sep + 1:] == (interp, "input.py")
        flag = argv.index("--defaulted-python")
        assert flag < sep and argv[flag + 1] == interp

    @pytest.mark.parametrize("extra", [{}, {"python": "/explicit/py", "program": "p"}])
    def test_nothing_is_marked_without_a_program_or_with_python(
        self, tmp_path: Path, calls: TransportCalls, extra: dict[str, str],  # noqa: F811
    ) -> None:
        src = tmp_path / "input.py"
        src.write_text("")
        host_cfg = HostConfig(ssh="h", remote_vq="vq", remote_python="/remote/py3")
        submit.submit_remote(host="h", host_cfg=host_cfg, input_file=str(src), **extra)
        assert "--defaulted-python" not in _remote_argv(calls)

    def test_a_directory_submit_is_not_marked(
        self, tmp_path: Path, calls: TransportCalls,  # noqa: F811
    ) -> None:
        (tmp_path / "run.py").write_text("")
        host_cfg = HostConfig(ssh="h", remote_vq="vq", remote_python="/remote/py3")
        submit.submit_remote(host="h", host_cfg=host_cfg, directory=str(tmp_path),
                             command=["python", "run.py"], program="vibeqc-dev")
        assert "--defaulted-python" not in _remote_argv(calls)

    def test_a_scheduler_target_is_not_marked(
        self, tmp_path: Path, calls: TransportCalls,  # noqa: F811
    ) -> None:
        src = tmp_path / "input.py"
        src.write_text("")
        host_cfg = HostConfig(ssh="drv", remote_vq="vq", remote_python=None)
        submit.submit_remote(host="drv", host_cfg=host_cfg, input_file=str(src),
                             program="vibeqc-dev", scheduler_target="cluster")
        assert "--defaulted-python" not in _remote_argv(calls)


def _git_repo(path: Path) -> None:
    path.mkdir()
    run = ["git", "-C", str(path), "-c", "user.email=t@example.com", "-c", "user.name=t"]
    subprocess.run(["git", "init", "-q", "-b", "main", str(path)], check=True)
    (path / "README").write_text("x")
    subprocess.run([*run, "add", "README"], check=True)
    subprocess.run([*run, "commit", "-qm", "init"], check=True)


@pytest.fixture
def receiving(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A receiving host with a venv and a binary program, and a staged tar."""
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    _git_repo(tmp_path / "repo")
    (tmp_path / "cfg" / "config.toml").write_text(
        "[programs.toy]\n"
        'kind = "venv"\n'
        f'python = "{sys.executable}"\n'
        f'git_dir = "{tmp_path / "repo"}"\n'
        "\n[programs.tool]\n"
        'kind = "binary"\n'
        'binary = "/bin/echo"\n'
    )
    script = tmp_path / "input.py"
    script.write_text("print('hi')\n")
    with tarfile.open(tmp_path / "staged.tar", "w") as tf:
        tf.add(script, arcname="input.py")
    return tmp_path


def _receive(root: Path, program: str, interp: str, *, marked: bool = True) -> JobSpec:
    argv = ["submit", "localhost", "-c", str(root / "staged.tar"), "--program", program]
    if marked:
        argv += ["--defaulted-python", interp]
    result = CliRunner().invoke(main, [*argv, "--", interp, "input.py"])
    assert result.exit_code == 0, result.output
    jobid = result.stdout.strip().splitlines()[-1]
    return JobSpec.read(paths.queue_dir() / f"{jobid}.json")


class TestReceivingHost:
    @pytest.mark.parametrize("interp", ["/remote/py3", "python"])
    def test_a_venv_program_runs_its_own_interpreter(
        self, receiving: Path, interp: str,
    ) -> None:
        assert _receive(receiving, "toy", interp).command == ["python", "input.py"]

    def test_a_binary_program_keeps_the_default(self, receiving: Path) -> None:
        spec = _receive(receiving, "tool", "/remote/py3")
        assert spec.command == ["/remote/py3", "input.py"]

    def test_a_bare_default_becomes_this_vqs_interpreter(self, receiving: Path) -> None:
        # Bare `python` with a binary program is refused at dispatch, so the
        # receiving host uses its own interpreter, as a local submit would.
        spec = _receive(receiving, "tool", "python")
        assert spec.command == [sys.executable, "input.py"]

    def test_an_unmarked_command_is_used_as_given(self, receiving: Path) -> None:
        spec = _receive(receiving, "toy", "/remote/py3", marked=False)
        assert spec.command == ["/remote/py3", "input.py"]

    def test_the_marker_must_name_the_commands_interpreter(self, receiving: Path) -> None:
        result = CliRunner().invoke(main, [
            "submit", "localhost", "-c", str(receiving / "staged.tar"), "--program", "toy",
            "--defaulted-python", "/other/py", "--", "/remote/py3", "input.py"])
        assert result.exit_code == 2
        assert "must name the command's interpreter" in result.output

    def test_an_unknown_marker_is_refused_not_run(self, receiving: Path) -> None:
        # What an older vq does with the marker: an option it does not know
        # becomes the start of the command, which vq refuses.
        result = CliRunner().invoke(main, [
            "submit", "localhost", "-c", str(receiving / "staged.tar"), "--program", "toy",
            "--not-yet-known", "/remote/py3", "--", "/remote/py3", "input.py"])
        assert result.exit_code != 0
        assert "invalid command executable '--not-yet-known'" in result.output
        assert not list((receiving / "state").glob("**/queue/*.json"))
