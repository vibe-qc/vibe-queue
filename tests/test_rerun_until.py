"""v0.8.8 *Turing's Halt* — `vq submit --rerun-until FILE` tests.

Pins the v0.8.8 contract:

1. **Spec gains rerun_until_file_exists / rerun_max / rerun_count.**
2. **Daemon's `_maybe_spawn_rerun` fires on COMPLETED.**
3. **Flag present → no rerun.**
4. **Flag absent + rerun_count < rerun_max → spawn a clone**
   with rerun_count++, depends_on=[original].
5. **Flag absent + rerun_count == rerun_max → log warning, no spawn.**
6. **FAILED jobs don't trigger rerun.**
7. **$VQ_WORKDIR substitution** in the flag path.
8. **CLI parses --rerun-until + --rerun-max.**
"""

from __future__ import annotations

from pathlib import Path

import pytest

from vq import paths
from vq.spec import JobSpec, JobState, ProgramRuntimePin

# ----------------------------------------------------------------------
# Spec fields
# ----------------------------------------------------------------------


class TestSpecRerunFields:
    def test_defaults(self) -> None:
        s = JobSpec(
            id="abc", command=["python", "x.py"], cwd="/tmp", cpus=1,
        )
        assert s.rerun_until_file_exists is None
        assert s.rerun_max == 10
        assert s.rerun_count == 0

    def test_roundtrip(self, tmp_path: Path) -> None:
        s = JobSpec(
            id="abc", command=["python", "x.py"], cwd="/tmp", cpus=1,
            rerun_until_file_exists="$VQ_WORKDIR/CONVERGED",
            rerun_max=20, rerun_count=3,
        )
        path = tmp_path / "spec.json"
        s.write(path)
        loaded = JobSpec.read(path)
        assert loaded.rerun_until_file_exists == "$VQ_WORKDIR/CONVERGED"
        assert loaded.rerun_max == 20
        assert loaded.rerun_count == 3

    def test_rerun_max_negative_rejected(self) -> None:
        with pytest.raises(ValueError):
            JobSpec(
                id="abc", command=["x"], cwd="/tmp", cpus=1,
                rerun_max=-1,
            )

    def test_rerun_count_negative_rejected(self) -> None:
        with pytest.raises(ValueError):
            JobSpec(
                id="abc", command=["x"], cwd="/tmp", cpus=1,
                rerun_count=-1,
            )


# ----------------------------------------------------------------------
# Daemon hook — _maybe_spawn_rerun
# ----------------------------------------------------------------------


@pytest.fixture
def daemon_with_dirs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
):
    """A bare Daemon-like object with the minimum state needed to
    drive `_maybe_spawn_rerun` and `_spawn_rerun_clone`. We bypass
    the full `Daemon.__init__` because that wants a queue dir + lots
    of other config — for testing the hook, a stub with queue_dir +
    jobs_dir + _multi_user is enough."""
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    queue_dir = tmp_path / "state" / "queue"
    jobs_dir = tmp_path / "state" / "jobs"
    queue_dir.mkdir(parents=True)
    jobs_dir.mkdir(parents=True)

    from vq.daemon import Daemon

    class _Stub(Daemon):
        # Override __init__ to skip the heavy startup work.
        def __init__(self) -> None:  # noqa: D401
            self.queue_dir = queue_dir
            self.jobs_dir = jobs_dir
            self._multi_user = False

    return _Stub()


def _spec_with_workspace(
    tmp_path: Path, **kwargs,
) -> JobSpec:
    """Build a spec whose cwd is a fresh workspace dir we can copy."""
    workspace = tmp_path / "ws"
    workspace.mkdir()
    (workspace / "script.py").write_text("print('hi')\n")
    return JobSpec(
        id="parent12345a", command=["python", "script.py"],
        cwd=str(workspace), cpus=1,
        state=JobState.COMPLETED,
        **kwargs,
    )


class TestMaybeSpawnRerun:
    def test_no_rerun_when_field_unset(
        self, daemon_with_dirs, tmp_path: Path,
    ) -> None:
        """A normal job (no --rerun-until) doesn't trigger a respawn."""
        spec = _spec_with_workspace(tmp_path)
        daemon_with_dirs._maybe_spawn_rerun(spec)
        # No spec written to queue.
        assert list(daemon_with_dirs.queue_dir.glob("*.json")) == []

    def test_no_rerun_when_flag_present(
        self, daemon_with_dirs, tmp_path: Path,
    ) -> None:
        """Flag file exists → loop is converged, no respawn."""
        flag = tmp_path / "CONVERGED"
        flag.write_text("done\n")
        spec = _spec_with_workspace(
            tmp_path,
            rerun_until_file_exists=str(flag),
        )
        daemon_with_dirs._maybe_spawn_rerun(spec)
        assert list(daemon_with_dirs.queue_dir.glob("*.json")) == []

    def test_no_rerun_when_state_failed(
        self, daemon_with_dirs, tmp_path: Path,
    ) -> None:
        """FAILED jobs don't respawn — failure is wrong-signal."""
        spec = _spec_with_workspace(
            tmp_path,
            rerun_until_file_exists=str(tmp_path / "missing.flag"),
        )
        spec.state = JobState.FAILED
        daemon_with_dirs._maybe_spawn_rerun(spec)
        assert list(daemon_with_dirs.queue_dir.glob("*.json")) == []

    def test_spawn_when_flag_absent_and_budget_left(
        self, daemon_with_dirs, tmp_path: Path,
    ) -> None:
        """Flag missing + budget left → fresh clone in queue."""
        spec = _spec_with_workspace(
            tmp_path,
            rerun_until_file_exists=str(tmp_path / "missing.flag"),
            rerun_max=5, rerun_count=0,
            program="vibeqc-dev",
            program_runtime_pin=ProgramRuntimePin(expected_git_sha="abc123"),
            idempotency_key_hash="a" * 64,
            submission_intent_digest="b" * 64,
            submission_owner_hash="c" * 64,
        )
        daemon_with_dirs._maybe_spawn_rerun(spec)
        specs = list(daemon_with_dirs.queue_dir.glob("*.json"))
        assert len(specs) == 1
        new_spec = JobSpec.read(specs[0])
        assert new_spec.rerun_count == 1
        assert new_spec.rerun_max == 5
        assert new_spec.depends_on == [spec.id]
        assert new_spec.state == JobState.PENDING
        assert new_spec.program == "vibeqc-dev"
        assert new_spec.program_runtime_pin is not None
        assert new_spec.program_runtime_pin.expected_git_sha == "abc123"
        assert new_spec.idempotency_key_hash is None
        assert new_spec.submission_intent_digest is None
        assert new_spec.submission_owner_hash is None
        # The new workspace was created and contains the script.
        assert Path(new_spec.cwd).exists()
        assert (Path(new_spec.cwd) / "script.py").exists()

    def test_no_spawn_when_max_reached(
        self, daemon_with_dirs, tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """rerun_count == rerun_max → log warning, no spawn."""
        import logging as _logging
        caplog.set_level(_logging.WARNING, logger="vq.daemon")
        spec = _spec_with_workspace(
            tmp_path,
            rerun_until_file_exists=str(tmp_path / "missing.flag"),
            rerun_max=3, rerun_count=3,
        )
        daemon_with_dirs._maybe_spawn_rerun(spec)
        assert list(daemon_with_dirs.queue_dir.glob("*.json")) == []
        # Warning surfaces the exhaustion.
        msgs = " ".join(r.message for r in caplog.records)
        assert "rerun_count=3" in msgs and "rerun_max=3" in msgs

    def test_workdir_substitution_in_flag_path(
        self, daemon_with_dirs, tmp_path: Path,
    ) -> None:
        """$VQ_WORKDIR in the flag path gets substituted from
        spec.workdir at check time."""
        workdir = tmp_path / "workdir"
        workdir.mkdir()
        (workdir / "CONVERGED").write_text("yes\n")
        spec = _spec_with_workspace(
            tmp_path,
            workdir=str(workdir),
            rerun_until_file_exists="$VQ_WORKDIR/CONVERGED",
        )
        daemon_with_dirs._maybe_spawn_rerun(spec)
        # Flag present after substitution → no respawn.
        assert list(daemon_with_dirs.queue_dir.glob("*.json")) == []

    def test_rerun_count_increments_across_chain(
        self, daemon_with_dirs, tmp_path: Path,
    ) -> None:
        """Spawning a clone at count=2 produces a spec with count=3."""
        spec = _spec_with_workspace(
            tmp_path,
            rerun_until_file_exists=str(tmp_path / "missing.flag"),
            rerun_max=10, rerun_count=2,
        )
        daemon_with_dirs._maybe_spawn_rerun(spec)
        new_spec_path = next(daemon_with_dirs.queue_dir.glob("*.json"))
        new_spec = JobSpec.read(new_spec_path)
        assert new_spec.rerun_count == 3


# ----------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------


class TestSubmitRerunCLI:
    def test_rerun_until_flag_parsed(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from click.testing import CliRunner

        from vq import config as _config
        from vq.cli import main

        state = tmp_path / "state"
        state.mkdir()
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(state))
        cfg = tmp_path / "cfg"
        cfg.mkdir()
        (cfg / "config.toml").write_text(
            "default_host = \"localhost\"\n"
            "[hosts.localhost]\nssh = \"localhost\"\n"
        )
        monkeypatch.setenv(_config.ENV_CONFIG_DIR, str(cfg))

        script = tmp_path / "iter.py"
        script.write_text("print('go')\n")

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "submit", "--rerun-until", "$VQ_WORKDIR/CONVERGED",
                "--rerun-max", "5", str(script),
            ],
        )
        assert result.exit_code == 0, result.output
        jobid = result.output.strip()
        spec = JobSpec.read(paths.queue_dir() / f"{jobid}.json")
        assert spec.rerun_until_file_exists == "$VQ_WORKDIR/CONVERGED"
        assert spec.rerun_max == 5
        assert spec.rerun_count == 0
