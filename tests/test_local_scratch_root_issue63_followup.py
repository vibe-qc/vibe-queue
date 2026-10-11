"""#63 follow-ups, host side: scratch that the split left unreachable.

Five defects were demonstrated against the merged #63 change.

* A workdir under a root the host had since been configured away from was
  invisible to every verb -- accounted as zero bytes, refused by
  ``--delete``, never swept -- while ``--delete`` removed the small
  workspace beside it, because that one has always been removed at its
  recorded path. ``Config.scratch_root`` claimed changing it strands nothing.
* One foreign directory under a workdir root raised out of the whole
  auto-cleanup pass, and the daemon stamped ``last_run_at`` anyway, so
  archive, delete and the sweep all stopped until a human removed it. #63 is
  what made that root operator-configurable, and therefore shareable.
* A refused workdir cleanup was appended to ``failure_reason``, so a job that
  COMPLETED reported ``failure:`` and a terminal ``reason``.
* The multi-user scratch root followed the *calling process's* config file,
  so the root daemon and a user's CLI could resolve different trees.
* ``vq daemon status`` printed one path with no space after its colon, which
  is the one line the documented ``awk`` idiom could not read.

Everything runs against real directories in a per-test temporary sandbox.
"""
from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import cleanup, paths
from vq.cli import main
from vq.config import ConfigError, load_config
from vq.daemon import Daemon
from vq.spec import JobSpec, JobState, utcnow_iso

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _use_scratch_root(monkeypatch: pytest.MonkeyPatch, root: Path) -> None:
    """Point this host's scratch root at ``root`` the way an operator would."""
    root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv(paths.ENV_SCRATCH_DIR, str(root))


def _terminal_spec(
    jobid: str,
    *,
    cwd: Path,
    workdir: Path | None,
    queue_dir: Path,
    finished_at: str | None = None,
) -> JobSpec:
    """A finished job whose recorded paths are exactly where its bytes are."""
    cwd.mkdir(parents=True, exist_ok=True)
    (cwd / "result.txt").write_bytes(b"r" * 32)
    if workdir is not None:
        workdir.mkdir(parents=True, exist_ok=True)
        (workdir / "scratch.bin").write_bytes(b"s" * 4096)
    spec = JobSpec(
        id=jobid,
        command=["true"],
        cwd=str(cwd),
        cpus=1,
        state=JobState.COMPLETED,
        exit_code=0,
        finished_at=finished_at or utcnow_iso(),
        workdir=str(workdir) if workdir is not None else None,
    )
    queue_dir.mkdir(parents=True, exist_ok=True)
    spec.write(queue_dir / f"{jobid}.json")
    return spec


def _stale() -> str:
    return (datetime.now(UTC) - timedelta(days=30)).isoformat()


# ===========================================================================
# D2. A previously configured root is still this host's to account for
# ===========================================================================


@pytest.fixture
def former(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> tuple[Path, JobSpec]:
    """A job dispatched under scratch root one, on a host now using two."""
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    _use_scratch_root(monkeypatch, tmp_path / "scratch-one")
    spec = _terminal_spec(
        "former0000001",
        cwd=paths.jobs_dir() / "former0000001",
        workdir=paths.workdir_root() / "former0000001",
        queue_dir=paths.queue_dir(),
        finished_at=_stale(),
    )
    _use_scratch_root(monkeypatch, tmp_path / "scratch-two")
    assert spec.workdir is not None
    assert Path(spec.workdir).parent not in paths.managed_workdir_roots()
    return tmp_path, spec


class TestAFormerScratchRoot:
    """``scratch_root`` claimed changing it strands nothing. It stranded.

    ``managed_workdir_roots()`` answers with the *current* root and the
    pre-#63 one under the state root. Configure a third and the second is in
    neither, so the job's own recorded workdir became invisible to every verb
    -- while ``--delete`` still removed the workspace beside it, because that
    one has always been removed at its recorded path.
    """

    def test_its_bytes_are_accounted_not_reported_as_zero(
        self, former: tuple[Path, JobSpec]
    ) -> None:
        _tmp, spec = former

        candidates = {c.spec.id: c for c in cleanup.find_candidates()}

        assert candidates[spec.id].workdir_size == 4096, (
            "a workdir on disk under a root this host used to be configured "
            "for is not zero bytes"
        )
        assert candidates[spec.id].reclaimable == (
            candidates[spec.id].workspace_size + 4096
        )

    def test_delete_takes_the_workdir_and_not_just_the_workspace(
        self, former: tuple[Path, JobSpec]
    ) -> None:
        _tmp, spec = former
        assert spec.workdir is not None

        cleanup.delete_job(spec)

        assert not Path(spec.cwd).exists()
        assert not Path(spec.workdir).exists(), (
            "--delete removed the small workspace and left the large workdir, "
            "which is exactly the accounting #63 was opened about"
        )

    def test_the_age_sweep_reaches_it(self, former: tuple[Path, JobSpec]) -> None:
        _tmp, spec = former
        assert spec.workdir is not None

        counts = cleanup.run_auto_cleanup_pass(
            cleanup.AutoCleanupPolicy(enabled=True, workdir_max_age_seconds=3600)
        )

        assert counts["workdirs_swept"] == 1
        assert counts["workdir_errors"] == 0
        assert not Path(spec.workdir).exists()

    def test_the_sweep_roots_are_derived_from_the_records_themselves(
        self, former: tuple[Path, JobSpec]
    ) -> None:
        _tmp, spec = former
        assert spec.workdir is not None

        configured, derived = cleanup._single_user_sweep_roots()

        assert configured == paths.managed_workdir_roots(), (
            "the configured roots are exactly what the operator points at now"
        )
        assert Path(spec.workdir).parent in derived
        assert Path(spec.workdir).parent not in configured


class TestADerivedRootIsNotAuthorityToDeleteWhatVqNeverRecorded:
    """A root this host used to write into may since have been repurposed.

    Deriving sweep roots from the records reaches the workdirs of jobs under
    a former root, which is the point. It must not also hand vq the run of
    that directory: `validate_job_id` accepts `notes`, `README` and
    `my-data`, and an orphan is aged by its own mtime, which for a directory
    nobody has added to in a month is old. Authority under a former root
    comes from a record naming the directory, never from the listing.
    """

    def test_an_operators_directory_under_a_former_root_is_left_alone(
        self, former: tuple[Path, JobSpec]
    ) -> None:
        tmp, spec = former
        assert spec.workdir is not None
        former_root = Path(spec.workdir).parent
        for name in ("notes", "README", "my-data"):
            victim = former_root / name
            victim.mkdir(parents=True, exist_ok=True)
            (victim / "keep.dat").write_bytes(b"keep")
            os.utime(victim, (0, 0))

        counts = cleanup.run_auto_cleanup_pass(
            cleanup.AutoCleanupPolicy(enabled=True, workdir_max_age_seconds=3600)
        )

        for name in ("notes", "README", "my-data"):
            assert (former_root / name / "keep.dat").read_bytes() == b"keep", (
                f"{name} is not vq's to remove"
            )
        # The recorded workdir under that same root is still swept: reaching
        # it is why the root is derived at all.
        assert not Path(spec.workdir).exists()
        assert counts["workdirs_swept"] == 1
        assert counts["workdir_errors"] == 0

    def test_a_spec_that_shares_the_name_is_not_a_record_of_the_directory(
        self, former: tuple[Path, JobSpec]
    ) -> None:
        """Authority is the record naming the directory, not the name.

        Legacy ids such as ``data`` are valid job ids. A finished job called
        ``data`` whose workdir is under the root configured now says nothing
        about an operator's ``data`` under the former root.
        """
        _tmp, spec = former
        assert spec.workdir is not None
        former_root = Path(spec.workdir).parent
        namesake = _terminal_spec(
            "data",
            cwd=paths.jobs_dir() / "data",
            workdir=paths.workdir_root() / "data",
            queue_dir=paths.queue_dir(),
            finished_at=_stale(),
        )
        assert namesake.workdir is not None
        assert Path(namesake.workdir).parent != former_root
        victim = former_root / "data"
        victim.mkdir(parents=True)
        (victim / "keep.dat").write_bytes(b"keep")
        os.utime(victim, (0, 0))

        counts = cleanup.run_auto_cleanup_pass(
            cleanup.AutoCleanupPolicy(enabled=True, workdir_max_age_seconds=3600)
        )

        assert (victim / "keep.dat").read_bytes() == b"keep"
        # Both recorded workdirs are still swept, each where it is recorded.
        assert not Path(namesake.workdir).exists()
        assert not Path(spec.workdir).exists()
        assert counts["workdirs_swept"] == 2
        assert counts["workdir_errors"] == 0

    def test_an_orphan_under_a_configured_root_is_still_swept(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """The long-standing behaviour, unchanged where the operator points now."""
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
        _use_scratch_root(monkeypatch, tmp_path / "scratch")
        orphan = paths.workdir_root() / "orphan000001"
        orphan.mkdir(parents=True)
        os.utime(orphan, (0, 0))

        counts = cleanup.run_auto_cleanup_pass(
            cleanup.AutoCleanupPolicy(enabled=True, workdir_max_age_seconds=3600)
        )

        assert not orphan.exists()
        assert counts["workdirs_swept"] == 1

    def test_a_record_that_does_not_name_its_own_job_is_still_refused(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Widening the roots must not widen what may be deleted.

        The authority is "this job's own directory", so a record pointing at
        a directory that is not named for the job is refused wherever it is.
        """
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
        _use_scratch_root(monkeypatch, tmp_path / "scratch")
        victim = tmp_path / "somewhere-else" / "valuable"
        victim.mkdir(parents=True)
        (victim / "keep.dat").write_bytes(b"keep")
        spec = _terminal_spec(
            "aimed00000001",
            cwd=paths.jobs_dir() / "aimed00000001",
            workdir=None,
            queue_dir=paths.queue_dir(),
        )
        spec.workdir = str(victim)
        spec.write(paths.spec_path(spec.id))

        assert paths.workdir_roots_for_job(spec.workdir, spec.id) == (
            paths.managed_workdir_roots()
        )
        cleanup.delete_job(spec)

        assert (victim / "keep.dat").is_file()


# ===========================================================================
# D5. One foreign directory must not stop the whole pass
# ===========================================================================


class TestAForeignDirectoryUnderAWorkdirRoot:
    def test_it_is_skipped_rather_than_aborting_the_sweep(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """#63 made the root operator-configurable, so it may be shared.

        ``spec_path_for(entry.name)`` was called outside every ``try``, and
        ``validate_job_id`` raises for a name that is not a job id -- so a
        directory somebody else created stopped archive, delete and the
        workdir sweep until a human removed it, while the daemon went on
        stamping ``last_run_at``.
        """
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
        _use_scratch_root(monkeypatch, tmp_path / "shared-scratch")
        foreign = paths.workdir_root() / "not a job id!"
        foreign.mkdir(parents=True)
        (foreign / "someone-elses.dat").write_bytes(b"x")
        spec = _terminal_spec(
            "sweepme000001",
            cwd=paths.jobs_dir() / "sweepme000001",
            workdir=paths.workdir_root() / "sweepme000001",
            queue_dir=paths.queue_dir(),
            finished_at=_stale(),
        )

        counts = cleanup.run_auto_cleanup_pass(
            cleanup.AutoCleanupPolicy(enabled=True, workdir_max_age_seconds=3600)
        )

        assert counts["workdirs_swept"] == 1
        assert counts["workdir_errors"] == 0
        assert spec.workdir is not None
        assert not Path(spec.workdir).exists()
        assert (foreign / "someone-elses.dat").is_file(), (
            "a directory vq did not create is left alone, not swept"
        )


# ===========================================================================
# D8. A COMPLETED job is not a failed one
# ===========================================================================


class TestARefusedWorkdirCleanup:
    def test_it_is_a_note_on_a_completed_job_not_a_failure(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """``--clean-tmp`` plus a refusal used to stamp a failure on success.

        ``spec.failure_reason`` is reserved for non-COMPLETED terminal
        transitions, ``vq status`` prints it as ``failure:`` and ``--json``
        reports it as the terminal ``reason``.
        """
        from vq import status as status_mod

        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
        _use_scratch_root(monkeypatch, tmp_path / "scratch")
        daemon = Daemon(
            max_cpus=2,
            poll_interval=0.05,
            queue_dir=paths.queue_dir(),
            jobs_dir=paths.jobs_dir(),
        )
        daemon.queue_dir.mkdir(parents=True, exist_ok=True)
        spec = _terminal_spec(
            "cleantmp00001",
            cwd=paths.jobs_dir() / "cleantmp00001",
            workdir=None,
            queue_dir=paths.queue_dir(),
        )
        # A record outside every root this job's workdir may sit under: the
        # refusal path, without needing a hostile filesystem.
        spec.workdir = str(tmp_path / "elsewhere" / "cleantmp00001")
        spec.clean_workdir_on_terminal = True
        spec_path = paths.spec_path(spec.id)
        spec.write(spec_path)

        daemon._maybe_cleanup_workdir(spec, spec_path=spec_path)

        persisted = JobSpec.read(spec_path)
        assert persisted.state == JobState.COMPLETED
        assert persisted.failure_reason is None, (
            "a job that completed did not fail because its workdir stayed"
        )
        assert persisted.workdir_cleanup_note is not None
        assert "workdir cleanup skipped" in persisted.workdir_cleanup_note

        rendered = status_mod.show_status("localhost", spec.id, tail=0)
        assert "failure:" not in rendered
        assert "workdir cleanup skipped" in rendered

    def test_the_note_is_not_the_terminal_reason_in_json(
        self, tmp_path: Path
    ) -> None:
        from vq import status as status_mod

        note = "workdir cleanup skipped: managed job path is a symlink"
        spec = JobSpec(
            id="cleantmp00002",
            command=["true"],
            cwd=str(tmp_path / "workspace"),
            cpus=1,
            state=JobState.COMPLETED,
            exit_code=0,
            finished_at=utcnow_iso(),
            workdir_cleanup_note=note,
        )

        # ``JobSpec`` ignores unknown fields on read, so assert the field is
        # really there rather than silently dropped -- that tolerance is what
        # lets an older vq read a newer spec, and it would otherwise let this
        # case pass against a vq that has no such field at all.
        assert spec.workdir_cleanup_note == note
        assert spec.model_dump(mode="json")["workdir_cleanup_note"] == note

        diagnosis = status_mod.terminal_diagnosis_for_spec(spec)

        assert diagnosis is not None
        assert "reason" not in diagnosis
        assert diagnosis["category"] == "completed"


# ===========================================================================
# D4. The multi-user scratch root is one deployment-wide decision
# ===========================================================================


class TestTheMultiUserScratchRoot:
    def test_it_does_not_follow_the_callers_config_file(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """The root daemon reads /etc/vq; a user's CLI reads their own file.

        ``vq submit`` resolves the submitter's jobs directory in the user's
        own process, so honouring a per-caller key meant the user staged into
        one tree while the daemon's cwd gate expected another.
        """
        monkeypatch.delenv(paths.ENV_MULTI_USER_SCRATCH_ROOT, raising=False)
        multi_user_root = tmp_path / "vq-system"
        monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(multi_user_root))
        users_cfg = tmp_path / "user-cfg"
        users_cfg.mkdir()
        (users_cfg / "config.toml").write_text(
            f'scratch_root = "{tmp_path / "somewhere-of-my-own"}"\n',
            encoding="utf-8",
        )
        monkeypatch.setenv(paths.ENV_CONFIG_DIR, str(users_cfg))

        assert paths.multi_user_scratch_root() == tmp_path / "vq-system-scratch"
        assert paths.user_jobs_dir(1234) == (
            tmp_path / "vq-system-scratch" / "users" / "1234" / "jobs"
        )

    def test_the_env_var_still_moves_it_for_the_whole_deployment(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        moved = tmp_path / "vq-scratch-volume"
        monkeypatch.setenv(paths.ENV_MULTI_USER_SCRATCH_ROOT, str(moved))

        assert paths.multi_user_scratch_root() == moved

    def test_a_multi_user_config_refuses_the_key_rather_than_ignoring_it(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        cfg_dir = tmp_path / "system-cfg"
        cfg_dir.mkdir()
        (cfg_dir / "config.toml").write_text(
            f'scratch_root = "{tmp_path / "shared"}"\n'
            "\n[multi_user]\n"
            "enabled = true\n",
            encoding="utf-8",
        )
        monkeypatch.setenv(paths.ENV_CONFIG_DIR, str(cfg_dir))

        with pytest.raises(ConfigError, match="scratch_root is single-user only"):
            load_config()

    def test_a_single_user_config_keeps_the_key(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        cfg_dir = tmp_path / "single-cfg"
        cfg_dir.mkdir()
        configured = tmp_path / "configured-scratch"
        (cfg_dir / "config.toml").write_text(
            f'scratch_root = "{configured}"\n', encoding="utf-8"
        )
        monkeypatch.setenv(paths.ENV_CONFIG_DIR, str(cfg_dir))
        monkeypatch.delenv(paths.ENV_SCRATCH_DIR, raising=False)

        assert load_config().scratch_root == str(configured)
        assert paths.scratch_root() == configured


# ===========================================================================
# D9. Every line of `vq daemon status` parses the same way
# ===========================================================================


class TestDaemonStatusColumns:
    def test_every_path_line_has_a_space_after_its_colon(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """``docs/operations.md`` teaches ``awk '/scratch:/{print $2}'``.

        ``workdirs:`` ran its path straight into the colon, so that idiom --
        and any other field-splitting reader -- got an empty string for the
        one line pointing at the bytes.
        """
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
        _use_scratch_root(monkeypatch, tmp_path / "scratch")

        result = CliRunner().invoke(main, ["daemon", "status"])

        assert result.exit_code == 0, result.output
        printed = {}
        for line in result.output.splitlines():
            if not line.startswith("  ") or ":" not in line:
                continue
            label, _, rest = line.strip().partition(":")
            fields = line.split()
            assert len(fields) >= 2, f"no second field in {line!r}"
            printed[label] = fields[1]
            assert rest.startswith(" "), f"no space after the colon in {line!r}"
        assert printed["scratch"] == str(paths.scratch_root())
        assert printed["workdirs"] == str(paths.workdir_root())
        assert printed["jobs"] == str(paths.jobs_dir())


def test_a_workdir_root_that_is_gone_is_simply_not_swept(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The honest limit of deriving roots from records, stated as a test.

    A root no remaining record names is not enumerated: an orphan directory
    left in it is an operator's to remove, and ``docs/operations.md`` says so
    rather than implying a sweep that cannot reach it.
    """
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    _use_scratch_root(monkeypatch, tmp_path / "scratch-one")
    orphan = paths.workdir_root() / "orphan000001"
    orphan.mkdir(parents=True)
    (orphan / "left.dat").write_bytes(b"x")
    os.utime(orphan, (0, 0))
    _use_scratch_root(monkeypatch, tmp_path / "scratch-two")

    counts = cleanup.run_auto_cleanup_pass(
        cleanup.AutoCleanupPolicy(enabled=True, workdir_max_age_seconds=3600)
    )

    assert counts["workdirs_swept"] == 0
    assert counts["workdir_errors"] == 0
    assert (orphan / "left.dat").is_file()
