"""#207: ``vq submit HOST -- CMD`` with no -d/-c runs CMD in an empty workspace.

Without ``--`` the positionals still name one input file, and
``-- input.py`` keeps the single-file form. The empty directory goes
through the ordinary ``-d`` staging and is removed when the command ends.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import submit as submit_module
from vq.cli import main
from vq.spec import JobSpec

sys.path.insert(0, str(Path(__file__).parent))
from test_cli import cli_state  # noqa: E402,F401


def _local_spec(output: str, state: Path) -> JobSpec:
    jobid = output.strip().splitlines()[-1]
    return JobSpec.read(state / "state" / "queue" / f"{jobid}.json")


def test_a_bare_command_runs_in_an_empty_workspace(cli_state: Path) -> None:  # noqa: F811
    result = CliRunner().invoke(main, ["submit", "localhost", "--", "hostname"])
    assert result.exit_code == 0, result.output
    spec = _local_spec(result.stdout, cli_state)
    assert spec.command == ["hostname"]
    assert [p.name for p in Path(spec.cwd).iterdir() if p.name != "_vq"] == []
    assert spec.workspace_source is not None
    assert "vq-empty-workspace-" in spec.workspace_source
    # The staging directory is gone once the command has returned.
    assert not Path(spec.workspace_source).exists()


def test_options_before_the_separator_still_apply(cli_state: Path) -> None:  # noqa: F811
    result = CliRunner().invoke(main, ["submit", "localhost", "--cpus", "2", "--",
                                       "python3", "-c", "print(1)"])
    assert result.exit_code == 0, result.output
    spec = _local_spec(result.stdout, cli_state)
    assert spec.command == ["python3", "-c", "print(1)"]
    assert spec.cpus == 2


def test_a_python_input_after_the_separator_keeps_the_single_file_form(
    cli_state: Path, monkeypatch: pytest.MonkeyPatch,  # noqa: F811
) -> None:
    monkeypatch.chdir(cli_state)
    Path("input.py").write_text("print('hi')\n")
    result = CliRunner().invoke(main, ["submit", "localhost", "--", "input.py"])
    assert result.exit_code == 0, result.output
    spec = _local_spec(result.stdout, cli_state)
    assert spec.command[-1] == "input.py"
    assert spec.workspace_source == str((cli_state / "input.py").resolve())


def test_without_a_separator_the_positionals_name_one_input_file(
    cli_state: Path,  # noqa: F811
) -> None:
    result = CliRunner().invoke(main, ["submit", "localhost", "hostname"])
    assert result.exit_code != 0
    assert "input file not found" in result.output


@pytest.mark.parametrize("scheduler", [False, True])
def test_a_remote_or_scheduler_host_gets_the_empty_directory_as_its_payload(
    cli_state: Path, monkeypatch: pytest.MonkeyPatch, scheduler: bool,  # noqa: F811
) -> None:
    config = '[hosts.far]\nssh = "far"\n'
    if scheduler:
        config += ('scheduler = "pbs"\nscheduler_dialect = "torque"\n'
                   'scratch_root = "/home/USER"\n'
                   'scheduler_driver = "drv"\n\n[hosts.drv]\nssh = "drv-ssh"\n')
    (cli_state / "cfg" / "config.toml").write_text(config)
    seen: dict[str, object] = {}

    def fake_submit_remote(**kw: object) -> list[str]:
        directory = Path(str(kw["directory"]))
        seen.update(kw, listing=sorted(p.name for p in directory.iterdir()))
        return ["abc123def456"]

    monkeypatch.setattr(submit_module, "submit_remote", fake_submit_remote)
    result = CliRunner().invoke(main, ["submit", "far", "--json", "--", "nproc"])
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["jobids"] == ["abc123def456"]
    assert seen["command"] == ["nproc"]
    assert seen["listing"] == []
    assert seen["input_file"] is None
    assert not Path(str(seen["directory"])).exists()
    if scheduler:
        assert seen["host"] == "drv" and seen["scheduler_target"] == "far"
