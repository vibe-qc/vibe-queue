"""Standalone launchers select slots without importing an installed vq."""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
from click.testing import CliRunner

from tests.test_runtime_execution_binding import runtimes as runtimes
from vq import cli, config, runtime_slots


def _write_launcher(tmp_path, root, arguments=()):
    path = tmp_path / "queue launcher"
    path.write_text(runtime_slots.render_launcher(root, Path(sys.executable), list(arguments)))
    path.chmod(0o700)
    return path


def _run(path, *args, **kwargs):
    env = {k: v for k, v in os.environ.items() if k != "PYTHONPATH"}
    return subprocess.run([str(path), *args], env=env, text=True, capture_output=True,
                          timeout=15, **kwargs)


def test_export_runs_active_slot_after_legacy_retirement(runtimes, tmp_path):
    root, shas, program, _ = runtimes
    code = ("import json,os,pathlib,sys; print(json.dumps(["
            "pathlib.Path(os.environ['VQ_RUNTIME_SLOT_SOURCE'],'version.txt').read_text(), "
            "sys.executable,sys.argv[1:]]))")
    path = _write_launcher(tmp_path, root, ["-c", code])
    Path(program.git_dir).rename(tmp_path / "retired-legacy")
    for version, sha in enumerate(shas, 1):
        runtime_slots.activate(root, sha)
        result = _run(path, "a b", "$(literal)", "'quoted'")
        assert result.returncode == 0, result.stderr
        assert json.loads(result.stdout) == [
            str(version), str(runtime_slots.slot_python(root, sha)),
            ["a b", "$(literal)", "'quoted'"],
        ]


def test_export_cli_needs_no_config_or_client_log(runtimes, tmp_path, monkeypatch):
    root, _, _, _ = runtimes
    monkeypatch.setattr(config, "load_config", lambda: pytest.fail("loaded config"))
    monkeypatch.setattr(cli, "_setup_cli_invocation_logging", lambda: pytest.fail("client log"))
    result = CliRunner().invoke(cli.main, ["runtime-launcher", "--root", str(root),
                                         "--python", sys.executable, "--", "-c", "print('ready')"])
    assert result.exit_code == 0, result.output
    launcher = tmp_path / "export"
    launcher.write_text(result.output)
    launcher.chmod(0o700)
    assert _run(launcher).stdout == "ready\n"


@pytest.mark.parametrize("damage", ["missing", "escape", "unverified", "source-link"])
def test_export_refuses_damaged_current_at_execution(runtimes, tmp_path, damage):
    root, shas, _, _ = runtimes
    launcher = _write_launcher(tmp_path, root, ["-c", "print('unsafe execution')"])
    if damage == "missing":
        (root / "current").unlink()
    elif damage == "escape":
        (root / "current").unlink()
        (root / "current").symlink_to("../elsewhere")
    elif damage == "unverified":
        (runtime_slots.slot_path(root, shas[0]) / runtime_slots.SLOT_STATE_MARKER).unlink()
    else:
        source = runtime_slots.slot_source(root, shas[0])
        moved = source.with_name("moved")
        source.rename(moved)
        source.symlink_to(moved)
    result = _run(launcher)
    assert result.returncode == 1
    assert result.stdout == ""
    assert "Error:" in result.stderr and "Traceback" not in result.stderr


def test_bootstrap_cannot_be_a_retained_slot_file(runtimes):
    root, _, _, _ = runtimes
    binary = root / "base-python"
    shutil.copy2(Path(sys.executable).resolve(), binary)
    with pytest.raises(runtime_slots.RuntimeSlotError, match="outside the runtime store"):
        runtime_slots.render_launcher(root, binary, [])


def test_bootstrap_command_with_spaces_and_quotes_is_literal(runtimes, tmp_path):
    root, _, _, _ = runtimes
    # A small base-Python trampoline makes the command path hostile to shell
    # quoting without relying on a copied platform Python's library lookup.
    interpreter = tmp_path / "base ''' python $(touch injected)"
    import shlex

    interpreter.write_text("#!/bin/sh\nexec " + shlex.quote(str(Path(sys.executable).resolve()))
                           + ' "$@"\n')
    interpreter.chmod(0o700)
    launcher = tmp_path / "export"
    launcher.write_text(runtime_slots.render_launcher(root, interpreter, ["-c", "print('ready')"]))
    launcher.chmod(0o700)
    result = _run(launcher, cwd=tmp_path)
    assert result.returncode == 0, result.stderr
    assert result.stdout == "ready\n"
    assert not (tmp_path / "injected").exists()
