"""Tests for v0.6.15 SLURM-style verb aliases.

Verb mapping:
  vq sbatch  → vq submit
  vq squeue  → vq queue
  vq scancel → vq kill
  vq sacct   → vq status

Each alias is the SAME Click command object registered under a
second name. The tests verify the alias is reachable + behaves
identically to the canonical verb on equivalent input.
"""
from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import paths
from vq.cli import main
from vq.spec import JobSpec, JobState


@pytest.fixture
def cli_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Path:
    """Local state + config dirs in tmp_path; minimal config with
    default_host=localhost so the CLI verbs don't error on host
    resolution. Mirrors the cli_state fixture in test_cli.py."""
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv("VQ_CONFIG_DIR", str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    paths.jobs_dir().mkdir(parents=True, exist_ok=True)
    (tmp_path / "cfg" / "config.toml").write_text(
        'default_host = "localhost"\n'
    )
    return tmp_path


class TestAliasRegistration:
    """The four aliases are all registered under main."""

    @pytest.mark.parametrize(
        "alias,canonical",
        [
            ("sbatch", "submit"),
            ("squeue", "queue"),
            ("scancel", "kill"),
            ("sacct", "status"),
        ],
    )
    def test_alias_is_registered(self, alias: str, canonical: str) -> None:
        assert alias in main.commands
        assert canonical in main.commands
        # Same Click command object under both names — that's the
        # registration shape we want, vs two separate definitions
        # that would drift over time.
        assert main.commands[alias] is main.commands[canonical]


class TestSbatchAlias:
    """vq sbatch behaves like vq submit."""

    def test_sbatch_submits_a_single_file_job(self, cli_state: Path) -> None:
        f = cli_state / "x.py"
        f.write_text("print('hi from sbatch')")
        result = CliRunner().invoke(main, ["sbatch", "localhost", str(f)])
        assert result.exit_code == 0, result.output
        jobid = result.output.strip()
        assert len(jobid) == 12
        # Spec written, same as if `vq submit` had been used
        spec_path = paths.queue_dir() / f"{jobid}.json"
        assert spec_path.exists()

    def test_sbatch_help_matches_submit_help(self) -> None:
        """The alias shares the canonical command object, so its
        help text is the same as `vq submit --help`. (Click renders
        the alias name in the Usage line; the body text is shared.)"""
        sbatch_help = CliRunner().invoke(main, ["sbatch", "--help"])
        submit_help = CliRunner().invoke(main, ["submit", "--help"])
        assert sbatch_help.exit_code == 0
        assert submit_help.exit_code == 0
        # The Usage: line differs by verb name. Strip that, then
        # rest should match.
        sbatch_body = "\n".join(sbatch_help.output.splitlines()[1:])
        submit_body = "\n".join(submit_help.output.splitlines()[1:])
        assert sbatch_body == submit_body

    def test_sbatch_accepts_vq_flags(self, cli_state: Path) -> None:
        """Flags stay vq-style: --cpus, --mem-mb, --wall-time-seconds.
        The alias inherits the canonical's full flag set."""
        f = cli_state / "x.py"
        f.write_text("")
        result = CliRunner().invoke(
            main,
            [
                "sbatch", "localhost", str(f),
                "--cpus", "4",
                "--mem-mb", "1024",
                "--wall-time-seconds", "300",
                "--tag", "alias-test",
            ],
        )
        assert result.exit_code == 0, result.output
        jobid = result.output.strip()
        spec = JobSpec.read(paths.queue_dir() / f"{jobid}.json")
        assert spec.cpus == 4
        assert spec.mem_mb == 1024
        assert spec.wall_time_seconds == 300
        assert spec.tags == ["alias-test"]


class TestSqueueAlias:
    """vq squeue behaves like vq queue."""

    def test_squeue_lists_jobs(self, cli_state: Path) -> None:
        f = cli_state / "x.py"
        f.write_text("")
        CliRunner().invoke(main, ["submit", "localhost", str(f)])
        result = CliRunner().invoke(main, ["squeue", "localhost"])
        assert result.exit_code == 0
        assert "STATE" in result.output  # header line

    def test_squeue_state_filter_works(self, cli_state: Path) -> None:
        """The --state filter from `vq queue` (v0.5.27) reaches
        through the alias."""
        # Build one spec each in pending + completed state
        for jobid, state in [
            ("pend00000001", JobState.PENDING),
            ("comp00000001", JobState.COMPLETED),
        ]:
            workspace = paths.jobs_dir() / jobid
            workspace.mkdir()
            spec = JobSpec(
                id=jobid,
                command=["echo", "hi"],
                cwd=str(workspace),
                cpus=1,
                state=state,
            )
            spec.write(paths.queue_dir() / f"{jobid}.json")

        result = CliRunner().invoke(
            main, ["squeue", "localhost", "--state", "pending"]
        )
        assert result.exit_code == 0
        assert "pend00000001" in result.output
        assert "comp00000001" not in result.output


class TestScancelAlias:
    """vq scancel behaves like vq kill."""

    def test_scancel_kills_pending_job(self, cli_state: Path) -> None:
        f = cli_state / "x.py"
        f.write_text("")
        sub = CliRunner().invoke(main, ["submit", "localhost", str(f)])
        jobid = sub.output.strip()

        result = CliRunner().invoke(main, ["scancel", "localhost", jobid])
        assert result.exit_code == 0, result.output
        # Spec should be in KILLED state after cancel (pending jobs
        # are marked KILLED immediately rather than getting SIGTERM).
        spec = JobSpec.read(paths.queue_dir() / f"{jobid}.json")
        assert spec.state == JobState.KILLED

    def test_scancel_unknown_jobid_errors(self, cli_state: Path) -> None:
        result = CliRunner().invoke(
            main, ["scancel", "localhost", "nonexistent1"]
        )
        assert result.exit_code != 0
        assert "no such" in result.output.lower() or "not found" in result.output.lower()


class TestSacctAlias:
    """vq sacct behaves like vq status."""

    def test_sacct_shows_status(self, cli_state: Path) -> None:
        f = cli_state / "x.py"
        f.write_text("print('hi')")
        sub = CliRunner().invoke(main, ["submit", "localhost", str(f)])
        jobid = sub.output.strip()

        result = CliRunner().invoke(main, ["sacct", "localhost", jobid])
        assert result.exit_code == 0, result.output
        assert f"id:           {jobid}" in result.output
        assert "state:" in result.output

    def test_sacct_json_flag_works(self, cli_state: Path) -> None:
        """The --json flag from `vq status` (v0.6.14) is reachable
        via the alias."""
        f = cli_state / "x.py"
        f.write_text("")
        sub = CliRunner().invoke(main, ["submit", "localhost", str(f)])
        jobid = sub.output.strip()

        result = CliRunner().invoke(main, ["sacct", "localhost", jobid, "--json"])
        assert result.exit_code == 0
        import json as _json
        payload = _json.loads(result.output)
        assert payload["id"] == jobid
        assert "state" in payload


class TestAliasVisibility:
    """All four aliases appear in `vq --help` so an operator looking
    for SLURM-style verbs can discover them."""

    def test_top_level_help_lists_all_aliases(self) -> None:
        result = CliRunner().invoke(main, ["--help"])
        assert result.exit_code == 0
        for alias in ("sbatch", "squeue", "scancel", "sacct"):
            assert alias in result.output, (
                f"{alias} missing from top-level help"
            )
