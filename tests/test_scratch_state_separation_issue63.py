"""#63: disposable scratch lives apart from durable queue state.

The measurement that opened the issue: on two daemon hosts **over 99 % of the
state root's footprint was job workdirs** -- 132 GB of 138 on one, 81 GB of 84
on the other -- under a root the layout documents as durable and never pruned.
``vq cleanup`` reported about 1 GB on the second of them, because it accounted
for ``jobs/`` and could not see ``workdirs/`` at all.

An operator could not fix that by configuration. ``paths.workdir_root()`` was
``state_root() / "workdirs"`` with no override, and the one redirection a
filesystem offers -- a symlink -- made the daemon's ``O_NOFOLLOW`` open of that
root fail, which switched vq's own workdir cleanup off *silently*. On a
scheduler host ``scratch_root`` was configuration, but a job's workspace was
recomputed from its **current** value on every call, so changing it stranded
every in-flight workspace and made remote cleanup refuse them permanently.

What this file pins, by the acceptance shape the issue's comment asked for:

* the roots, their precedence and the derived default;
* old and new jobs coexisting across a root change -- accounting, deletion,
  the age sweep, daemon dispatch and restart, and result retrieval;
* the scheduler handle's recorded workspace surviving a disk round trip and a
  ``scratch_root`` change, and the transition for records that predate it;
* the deletion-authority guards on the new root: relative and symlink escapes,
  and multi-user ownership;
* the invariants that must NOT have changed -- an active job is retained, and
  durable state is never a scratch-sweep candidate.

Everything runs against real directories in a per-test temporary sandbox.
"""
from __future__ import annotations

import os
import stat as stat_mod
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import pytest

from vq import cleanup, paths
from vq.config import ConfigError, load_config
from vq.daemon import Daemon
from vq.fetch import fetch_local
from vq.scheduler_dialect import dialect_for
from vq.scheduler_dispatch import (
    SchedulerDispatcher,
    SchedulerError,
    SchedulerHandle,
    scheduler_handle_for_spec,
)

# ``bound_remote_workspace`` and ``unbound_staged_job_ids`` arrive with
# #63 and are imported inside the two tests that need them, so this module
# still collects against the parent commit and each case reports its own
# result there rather than one collection error standing in for all of them.
from vq.spec import JobSpec, JobState, utcnow_iso

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _resolve_production_default(expression: str, tmp_path: Path) -> Path:
    """Resolve a documented default outside the pytest boundary.

    The conftest declares every persistent root explicitly, which is exactly
    what must not be in force when the question is "what does a stock install
    do". Same technique as ``tests/test_multi_user.py``.
    """
    env = dict(os.environ)
    for name in (
        "VQ_STATE_DIR",
        "VQ_SCRATCH_DIR",
        "VQ_CONFIG_DIR",
        "VQ_ARCHIVE_DIR",
        "VQ_MULTI_USER_ROOT",
        "VQ_MULTI_USER_SCRATCH_ROOT",
        "VQ_TEST_SANDBOX_ROOT",
        "XDG_DATA_HOME",
        "XDG_CONFIG_HOME",
        "PYTEST_ADDOPTS",
        "PYTEST_CURRENT_TEST",
        "PYTEST_VERSION",
    ):
        env.pop(name, None)
    home = tmp_path / "default-home"
    home.mkdir(exist_ok=True)
    env["HOME"] = str(home)
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1] / "src")
    result = subprocess.run(
        [sys.executable, "-c", f"from vq import paths; print({expression})"],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    return Path(result.stdout.strip())


def _write_config(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, body: str) -> Path:
    """Point the config root at a fresh directory holding ``body``."""
    cfg_dir = tmp_path / f"cfg-{len(body)}-{abs(hash(body)) % 100000}"
    cfg_dir.mkdir(parents=True, exist_ok=True)
    (cfg_dir / "config.toml").write_text(body, encoding="utf-8")
    monkeypatch.setenv("VQ_CONFIG_DIR", str(cfg_dir))
    return cfg_dir


def _terminal_spec(
    jobid: str,
    *,
    cwd: Path,
    workdir: Path | None,
    queue_dir: Path,
    state: JobState = JobState.COMPLETED,
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
        state=state,
        exit_code=0,
        finished_at=finished_at or utcnow_iso(),
        workdir=str(workdir) if workdir is not None else None,
    )
    queue_dir.mkdir(parents=True, exist_ok=True)
    spec.write(queue_dir / f"{jobid}.json")
    return spec


class _RecordingDispatcher:
    """Just enough scheduler dispatcher to observe path selection."""

    def __init__(
        self, scratch_root: str, job_root: str = ".vibeqc-cluster/jobs"
    ) -> None:
        self.scratch_root = scratch_root.rstrip("/")
        self.job_root = job_root.strip("/")
        self.submitted: list[tuple[str, str | None]] = []
        self.receipt_reads: list[str | None] = []
        self.marker_reads: list[str | None] = []
        self.retry_rotations: list[str | None] = []

    def remote_workspace(self, job_id: str) -> str:
        return f"{self.scratch_root}/{self.job_root}/{job_id}"

    def submit(
        self, *, job_id: str, remote_workspace: str | None = None, **_kw: object
    ) -> SchedulerHandle:
        self.submitted.append((job_id, remote_workspace))
        return SchedulerHandle(
            job_id=f"{job_id}.cluster",
            remote_workspace=remote_workspace or self.remote_workspace(job_id),
        )

    def submit_receipt(
        self, job_id: str, *, remote_workspace: str | None = None
    ) -> None:
        self.receipt_reads.append(remote_workspace)
        return None

    def recorded_job_id(
        self, job_id: str, *, remote_workspace: str | None = None, **_kw: object
    ) -> None:
        self.marker_reads.append(remote_workspace)
        return None

    def prepare_retry_attempt(
        self, job_id: str, *, remote_workspace: str | None = None
    ) -> None:
        self.retry_rotations.append(remote_workspace)


def _split_roots(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """State and scratch on separate roots, by their public env names.

    Spelled as literals rather than through ``paths.ENV_*`` so this module
    still *runs* against the commit before #63 -- which has no such constant,
    and simply ignores the variable -- and each case can report its own
    result there instead of erroring in setup.
    """
    monkeypatch.setenv("VQ_STATE_DIR", str(tmp_path / "state"))
    monkeypatch.setenv("VQ_SCRATCH_DIR", str(tmp_path / "scratch"))


def _state_jobs_root() -> Path:
    """``<state_root>/jobs`` -- where workspaces lived before #63."""
    return paths.state_root() / "jobs"


def _state_workdir_root() -> Path:
    """``<state_root>/workdirs`` -- where workdirs lived before #63."""
    return paths.state_root() / "workdirs"


class _RefusingRunner:
    """A remote runner that fails the test if anything reaches the host."""

    def run(self, argv, *, stdin_data=None, check=False):  # type: ignore[no-untyped-def]
        raise AssertionError(f"must not run anything remotely: {argv!r}")


class _RecordingRunner:
    def __init__(self) -> None:
        self.runs: list[list[str]] = []

    def run(self, argv, *, stdin_data=None, check=False):  # type: ignore[no-untyped-def]
        self.runs.append(list(argv))
        return type("R", (), {"returncode": 0, "stdout": "", "stderr": ""})()


# ===========================================================================
# 1. The roots themselves
# ===========================================================================


class TestTheRoots:
    def test_default_scratch_root_is_a_derived_sibling_of_the_state_root(
        self, tmp_path: Path
    ) -> None:
        """A stock install puts scratch beside state, not inside it.

        Derived rather than written out as ``~/vqscratch`` or any other
        home-relative literal, so it follows whatever the state root follows.
        """
        home = tmp_path / "default-home"
        state = _resolve_production_default("paths.state_root()", tmp_path)
        scratch = _resolve_production_default("paths.scratch_root()", tmp_path)

        assert state == home / ".local" / "share" / "vq"
        assert scratch == home / ".local" / "share" / "vq-scratch"
        assert scratch != state
        assert state not in scratch.parents

        assert _resolve_production_default("paths.jobs_dir()", tmp_path) == (
            scratch / "jobs"
        )
        assert _resolve_production_default("paths.workdir_root()", tmp_path) == (
            scratch / "workdirs"
        )
        # The durable trees stay where they were.
        assert _resolve_production_default("paths.queue_dir()", tmp_path) == (
            state / "queue"
        )
        assert _resolve_production_default("paths.archive_dir()", tmp_path) == (
            state / "archive"
        )

    def test_the_default_follows_an_explicit_state_root(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        moved = tmp_path / "volume" / "vqstate"
        monkeypatch.setenv("VQ_STATE_DIR", str(moved))
        monkeypatch.delenv("VQ_SCRATCH_DIR", raising=False)
        assert paths.scratch_root() == tmp_path / "volume" / "vqstate-scratch"

    def test_precedence_is_env_then_config_then_default(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """$VQ_SCRATCH_DIR beats the config key, which beats the default."""
        monkeypatch.setenv("VQ_STATE_DIR", str(tmp_path / "state"))
        configured = tmp_path / "configured-scratch"
        _write_config(monkeypatch, tmp_path, f'scratch_root = "{configured}"\n')

        monkeypatch.delenv("VQ_SCRATCH_DIR", raising=False)
        assert paths.scratch_root() == configured
        assert paths.jobs_dir() == configured / "jobs"
        assert paths.workdir_root() == configured / "workdirs"

        from_env = tmp_path / "env-scratch"
        monkeypatch.setenv("VQ_SCRATCH_DIR", str(from_env))
        assert paths.scratch_root() == from_env

    def test_jobs_and_workdirs_can_be_split_onto_different_volumes(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """They default together, but a site may keep results on durable disk."""
        monkeypatch.setenv("VQ_STATE_DIR", str(tmp_path / "state"))
        monkeypatch.delenv("VQ_SCRATCH_DIR", raising=False)
        durable = tmp_path / "durable" / "results"
        fast = tmp_path / "fast" / "scratch"
        _write_config(
            monkeypatch,
            tmp_path,
            f'scratch_root = "{tmp_path / "base"}"\n'
            f'jobs_root = "{durable}"\n'
            f'workdir_root = "{fast}"\n',
        )
        assert paths.jobs_dir() == durable
        assert paths.workdir_root() == fast
        assert paths.workspace_dir("abc123") == durable / "abc123"

    def test_a_relative_root_is_a_load_error(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """A relative root resolves against the caller's cwd, and the daemon,
        `vq cleanup` and `vq fetch` are three different working directories."""
        _write_config(monkeypatch, tmp_path, 'scratch_root = "relative/scratch"\n')
        with pytest.raises(ConfigError, match="absolute path"):
            load_config()

    def test_both_managed_roots_are_reported_current_first(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        _split_roots(monkeypatch, tmp_path)
        assert paths.managed_jobs_roots() == (
            tmp_path / "scratch" / "jobs",
            tmp_path / "state" / "jobs",
        )
        assert paths.managed_workdir_roots() == (
            tmp_path / "scratch" / "workdirs",
            tmp_path / "state" / "workdirs",
        )

    def test_pointing_scratch_back_at_state_does_not_double_count_a_root(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """A site that declines the split gets one root, visited once."""
        state = tmp_path / "state"
        monkeypatch.setenv("VQ_STATE_DIR", str(state))
        monkeypatch.setenv("VQ_SCRATCH_DIR", str(state))
        assert paths.managed_jobs_roots() == (state / "jobs",)
        assert paths.managed_workdir_roots() == (state / "workdirs",)


# ===========================================================================
# 2. Old and new jobs coexisting across a root change
# ===========================================================================


class TestOldAndNewJobsCoexist:
    @pytest.fixture(autouse=True)
    def _roots(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        _split_roots(monkeypatch, tmp_path)

    def test_cleanup_accounts_for_workdir_bytes_in_both_roots(self) -> None:
        """The number #63 was opened about: workdirs were not counted at all."""
        old = _terminal_spec(
            "oldroot00001",
            cwd=_state_jobs_root() / "oldroot00001",
            workdir=_state_workdir_root() / "oldroot00001",
            queue_dir=paths.queue_dir(),
        )
        new = _terminal_spec(
            "newroot00001",
            cwd=paths.jobs_dir() / "newroot00001",
            workdir=paths.workdir_root() / "newroot00001",
            queue_dir=paths.queue_dir(),
        )

        assert _state_workdir_root() != paths.workdir_root()
        by_id = {c.spec.id: c for c in cleanup.find_candidates()}
        assert set(by_id) == {old.id, new.id}
        for candidate in by_id.values():
            assert candidate.workdir_size == 4096, (
                "#63: a job's workdir bytes must be accounted wherever the "
                "job recorded them"
            )
            assert candidate.reclaimable == (
                candidate.workspace_size + candidate.workdir_size
            )

        table = cleanup.format_table(
            list(by_id.values()), action="delete", dry_run=True
        )
        assert "WORKDIR" in table
        assert "workdirs:" in table

    def test_delete_removes_the_workdir_under_either_root(self) -> None:
        old = _terminal_spec(
            "oldroot00002",
            cwd=_state_jobs_root() / "oldroot00002",
            workdir=_state_workdir_root() / "oldroot00002",
            queue_dir=paths.queue_dir(),
        )
        new = _terminal_spec(
            "newroot00002",
            cwd=paths.jobs_dir() / "newroot00002",
            workdir=paths.workdir_root() / "newroot00002",
            queue_dir=paths.queue_dir(),
        )

        cleanup.delete_job(old)
        cleanup.delete_job(new)

        for spec in (old, new):
            assert not Path(spec.cwd).exists()
            assert spec.workdir is not None
            assert not Path(spec.workdir).exists(), (
                "#63: --delete must reclaim the workdir, which is where the "
                "bytes are"
            )

    def test_the_age_sweep_visits_both_roots(self) -> None:
        assert _state_workdir_root() != paths.workdir_root(), (
            "the roots must actually differ, or 'both' proves nothing"
        )
        stale = (datetime.now(UTC) - timedelta(days=30)).isoformat()
        old = _terminal_spec(
            "oldroot00003",
            cwd=_state_jobs_root() / "oldroot00003",
            workdir=_state_workdir_root() / "oldroot00003",
            queue_dir=paths.queue_dir(),
            finished_at=stale,
        )
        new = _terminal_spec(
            "newroot00003",
            cwd=paths.jobs_dir() / "newroot00003",
            workdir=paths.workdir_root() / "newroot00003",
            queue_dir=paths.queue_dir(),
            finished_at=stale,
        )

        counts = cleanup.run_auto_cleanup_pass(
            cleanup.AutoCleanupPolicy(enabled=True, workdir_max_age_seconds=3600)
        )

        assert counts["workdirs_swept"] == 2
        assert counts["workdir_errors"] == 0
        for spec in (old, new):
            assert spec.workdir is not None
            assert not Path(spec.workdir).exists()

    def test_result_retrieval_works_from_either_root(self, tmp_path: Path) -> None:
        """#69's lesson, restated: a fetch reads the path the record names.

        This already held before #63 -- it is pinned here because separating
        the roots is exactly the change that could break it.
        """
        old = _terminal_spec(
            "oldroot00004",
            cwd=_state_jobs_root() / "oldroot00004",
            workdir=_state_workdir_root() / "oldroot00004",
            queue_dir=paths.queue_dir(),
        )
        new = _terminal_spec(
            "newroot00004",
            cwd=paths.jobs_dir() / "newroot00004",
            workdir=paths.workdir_root() / "newroot00004",
            queue_dir=paths.queue_dir(),
        )
        assert _state_jobs_root() != paths.jobs_dir()
        for spec in (old, new):
            dest = tmp_path / f"fetched-{spec.id}"
            fetch_local(spec.id, dest)
            assert (dest / spec.dest_dirname / "result.txt").read_bytes() == (
                b"r" * 32
            )

    def test_a_new_dispatch_uses_the_new_root_while_old_records_keep_theirs(
        self,
    ) -> None:
        """Daemon restart with jobs under both roots.

        The existing job's recorded paths are not rewritten, not moved and not
        recomputed; the next job the daemon dispatches lands under the scratch
        root.
        """
        daemon = Daemon(
            max_cpus=4,
            poll_interval=0.05,
            queue_dir=paths.queue_dir(),
            jobs_dir=paths.jobs_dir(),
        )
        daemon.queue_dir.mkdir(parents=True, exist_ok=True)
        daemon.jobs_dir.mkdir(parents=True, exist_ok=True)
        assert _state_jobs_root() != paths.jobs_dir(), (
            "the roots must actually differ, or coexistence proves nothing"
        )

        legacy_cwd = _state_jobs_root() / "legacyjob001"
        legacy_workdir = _state_workdir_root() / "legacyjob001"
        legacy = _terminal_spec(
            "legacyjob001",
            cwd=legacy_cwd,
            workdir=legacy_workdir,
            queue_dir=paths.queue_dir(),
            state=JobState.RUNNING,
        )
        legacy_bytes = (legacy_workdir / "scratch.bin").read_bytes()

        fresh_cwd = paths.jobs_dir() / "freshjob0001"
        fresh_cwd.mkdir(parents=True, exist_ok=True)
        fresh = JobSpec(
            id="freshjob0001",
            command=["true"],
            cwd=str(fresh_cwd),
            cpus=1,
            state=JobState.PENDING,
        )
        fresh.write(daemon._spec_path("freshjob0001"))

        class FakePopen:
            def __init__(self, *args: object, **kwargs: object) -> None:
                self.pid = 4242

            def poll(self) -> int:
                return 0

        with patch("vq.daemon.subprocess.Popen", FakePopen), patch(
            "vq.daemon.os.getpgid", return_value=4242
        ), patch("vq.daemon._read_pid_start_time", return_value=None):
            daemon._start_job(fresh)

        dispatched = JobSpec.read(daemon._spec_path("freshjob0001"))
        assert dispatched.workdir is not None
        assert Path(dispatched.workdir).parent == paths.workdir_root()

        # Untouched: same strings, same bytes, still under the old root.
        reread = JobSpec.read(paths.queue_dir() / f"{legacy.id}.json")
        assert reread.cwd == str(legacy_cwd)
        assert reread.workdir == str(legacy_workdir)
        assert (legacy_workdir / "scratch.bin").read_bytes() == legacy_bytes


# ===========================================================================
# 3. Deletion authority on the new root
# ===========================================================================


class TestDeletionAuthority:
    @pytest.fixture(autouse=True)
    def _roots(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        _split_roots(monkeypatch, tmp_path)

    def test_a_recorded_path_outside_every_managed_root_is_refused(
        self, tmp_path: Path
    ) -> None:
        outside = tmp_path / "somebody-elses" / "job0000000001"
        outside.mkdir(parents=True)
        (outside / "precious").write_text("keep me")

        reason = paths.remove_managed_scratch_dir(
            outside, "job0000000001", roots=paths.managed_workdir_roots()
        )

        assert reason is not None
        assert "managed scratch root" in reason
        assert (outside / "precious").exists()

    @pytest.mark.parametrize(
        "recorded",
        [
            "workdirs/../../etc",
            "workdirs/./job0000000002",
            "../scratch/workdirs/job0000000002",
        ],
    )
    def test_a_relative_or_traversing_record_is_refused(
        self, recorded: str
    ) -> None:
        managed = paths.workdir_root() / "job0000000002"
        managed.mkdir(parents=True)

        reason = paths.remove_managed_scratch_dir(
            recorded, "job0000000002", roots=paths.managed_workdir_roots()
        )

        assert reason is not None
        assert managed.exists(), "an unnormalised record must not delete"

    def test_a_symlinked_entry_is_refused_without_following_it(
        self, tmp_path: Path
    ) -> None:
        target = tmp_path / "link-target"
        target.mkdir()
        (target / "precious").write_text("keep me")
        root = paths.workdir_root()
        root.mkdir(parents=True)
        link = root / "job0000000003"
        link.symlink_to(target, target_is_directory=True)

        reason = paths.remove_managed_scratch_dir(
            link, "job0000000003", roots=(root,)
        )

        assert reason == "managed job path is a symlink"
        assert (target / "precious").exists()
        assert link.is_symlink()

    def test_a_symlinked_root_cannot_redirect_deletion(
        self, tmp_path: Path
    ) -> None:
        """The O_NOFOLLOW guard, now applied to the *configured* root.

        That is the whole point of #63: the root may be anywhere the operator
        puts it, and is still required to be a real directory rather than a
        redirection into someone else's tree.
        """
        elsewhere = tmp_path / "elsewhere"
        (elsewhere / "job0000000004").mkdir(parents=True)
        (elsewhere / "job0000000004" / "precious").write_text("keep me")
        root = paths.workdir_root()
        root.parent.mkdir(parents=True, exist_ok=True)
        root.symlink_to(elsewhere, target_is_directory=True)

        reason = paths.remove_managed_scratch_dir(
            root / "job0000000004", "job0000000004", roots=(root,)
        )

        assert reason is not None
        assert "could not be opened safely" in reason
        assert (elsewhere / "job0000000004" / "precious").exists()

    def test_a_foreign_owner_is_refused_rather_than_removed(self) -> None:
        """Multi-user ownership on the new root.

        The sweep cannot ask the kernel to drop privileges per entry, so the
        owner check IS the boundary: a directory under the per-user scratch
        root that is not owned by that user is ambiguous, and ambiguity is a
        refusal.
        """
        root = paths.workdir_root()
        managed = root / "job0000000005"
        managed.mkdir(parents=True)
        (managed / "precious").write_text("keep me")

        reason = paths.remove_managed_scratch_dir(
            managed,
            "job0000000005",
            roots=(root,),
            expected_uid=os.geteuid() + 4242,
        )

        assert reason is not None
        assert "owned by uid" in reason
        assert (managed / "precious").exists()

    def test_the_multi_user_scratch_tree_is_provisioned_root_owned(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """The per-user scratch dir gets the state tree's structural policy."""
        monkeypatch.setenv("VQ_MULTI_USER_ROOT", str(tmp_path / "mu"))
        monkeypatch.setenv("VQ_MULTI_USER_SCRATCH_ROOT", str(tmp_path / "mu-scratch"))
        uid, gid = os.getuid(), os.getgid()

        paths.provision_user_state(uid, gid)

        structural = paths.user_scratch_dir(uid)
        assert structural.is_dir()
        assert structural.stat().st_uid == os.geteuid()
        assert stat_mod.S_IMODE(structural.stat().st_mode) == 0o750
        for child in (paths.user_jobs_dir(uid), paths.user_workdir_root(uid)):
            assert child.is_dir()
            assert child.stat().st_uid == uid
            assert stat_mod.S_IMODE(child.stat().st_mode) & 0o022 == 0


# ===========================================================================
# 4. The invariants that must NOT have moved
# ===========================================================================


class TestRetainedInvariants:
    @pytest.fixture(autouse=True)
    def _roots(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        _split_roots(monkeypatch, tmp_path)

    def test_an_active_jobs_workdir_is_never_swept(self) -> None:
        stale = (datetime.now(UTC) - timedelta(days=30)).isoformat()
        running = _terminal_spec(
            "activejob001",
            cwd=paths.jobs_dir() / "activejob001",
            workdir=paths.workdir_root() / "activejob001",
            queue_dir=paths.queue_dir(),
            state=JobState.RUNNING,
            finished_at=stale,
        )
        assert running.workdir is not None
        # A long SCF loop writes into files that already exist, so the
        # directory mtime stays frozen at dispatch: age alone must never be
        # enough to take live scratch away.
        os.utime(running.workdir, (0, 0))

        counts = cleanup.run_auto_cleanup_pass(
            cleanup.AutoCleanupPolicy(enabled=True, workdir_max_age_seconds=1)
        )

        assert counts["workdirs_swept"] == 0
        assert Path(running.workdir).exists()

    def test_durable_state_is_never_a_scratch_sweep_candidate(self) -> None:
        """The sweep reaches per-job directories under the workdir roots only."""
        stale = (datetime.now(UTC) - timedelta(days=30)).isoformat()
        spec = _terminal_spec(
            "durable00001",
            cwd=paths.jobs_dir() / "durable00001",
            workdir=paths.workdir_root() / "durable00001",
            queue_dir=paths.queue_dir(),
            finished_at=stale,
        )
        archive = paths.archive_dir() / f"{spec.id}.tar.bz2"
        archive.parent.mkdir(parents=True, exist_ok=True)
        archive.write_bytes(b"promoted evidence")
        staging = paths.state_root() / "runs-staging"
        staging.mkdir(parents=True, exist_ok=True)
        (staging / "kept.qvf").write_bytes(b"backed up, not scratch")

        cleanup.run_auto_cleanup_pass(
            cleanup.AutoCleanupPolicy(enabled=True, workdir_max_age_seconds=1)
        )

        assert (paths.queue_dir() / f"{spec.id}.json").exists()
        assert archive.exists()
        assert (staging / "kept.qvf").exists()
        assert Path(spec.cwd).exists(), "the workspace is not the workdir"


# ===========================================================================
# 5. Scheduler hosts: the workspace is bound before the submission boundary
# ===========================================================================


class TestSchedulerWorkspaceBinding:
    @pytest.fixture
    def daemon(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Daemon:
        _split_roots(monkeypatch, tmp_path)
        d = Daemon(
            max_cpus=4,
            poll_interval=0.05,
            queue_dir=paths.queue_dir(),
            jobs_dir=paths.jobs_dir(),
        )
        d.queue_dir.mkdir(parents=True, exist_ok=True)
        d.jobs_dir.mkdir(parents=True, exist_ok=True)
        return d

    def _scheduler_spec(self, daemon: Daemon, jobid: str) -> JobSpec:
        workspace = daemon.jobs_dir / jobid
        workspace.mkdir(parents=True, exist_ok=True)
        spec = JobSpec(
            id=jobid,
            command=["true"],
            cwd=str(workspace),
            cpus=1,
            scheduler_target="host_f",
        )
        spec.write(daemon._spec_path(jobid))
        return spec

    def test_the_workspace_is_recorded_before_submit_and_survives_a_round_trip(
        self, daemon: Daemon
    ) -> None:
        dispatcher = _RecordingDispatcher("/scratch/original")
        daemon._scheduler_dispatchers["host_f"] = dispatcher  # type: ignore[assignment]
        spec = self._scheduler_spec(daemon, "schedjob0001")

        assert daemon._start_scheduler_job(spec) is True

        expected = "/scratch/original/.vibeqc-cluster/jobs/schedjob0001"
        # Passed to submit, so the staging and the record cannot disagree.
        assert dispatcher.submitted == [("schedjob0001", expected)]
        # And durable: read back off disk, not from the object we submitted.
        on_disk = JobSpec.read(daemon._spec_path("schedjob0001"))
        assert on_disk.scheduler_remote_workspace == expected

    def test_the_recorded_workspace_survives_a_scratch_root_change(
        self, daemon: Daemon
    ) -> None:
        """The failure this issue is named for, from the scheduler side.

        Every later step -- polling, log reads, fetch, cancellation, remote
        cleanup -- goes through the handle, so pinning the handle pins all of
        them.
        """
        dispatcher = _RecordingDispatcher("/scratch/original")
        daemon._scheduler_dispatchers["host_f"] = dispatcher  # type: ignore[assignment]
        spec = self._scheduler_spec(daemon, "schedjob0002")
        assert daemon._start_scheduler_job(spec) is True
        on_disk = JobSpec.read(daemon._spec_path("schedjob0002"))

        moved = _RecordingDispatcher("/scratch/moved-by-the-operator")
        handle = scheduler_handle_for_spec(
            moved,  # type: ignore[arg-type]
            on_disk,
            job_id=on_disk.scheduler_job_id or on_disk.id,
        )

        assert handle.remote_workspace == (
            "/scratch/original/.vibeqc-cluster/jobs/schedjob0002"
        )
        from vq.scheduler_dispatch import bound_remote_workspace

        assert (
            bound_remote_workspace(moved, on_disk)  # type: ignore[arg-type]
            == handle.remote_workspace
        )

    def test_remote_cleanup_accepts_a_workspace_from_a_former_scratch_root(
        self,
    ) -> None:
        """It used to refuse anything whose parent was not the *current* root.

        So changing ``scratch_root`` did not merely strand a workspace: it
        made the reaper refuse it forever, which is a disk leak on the
        cluster with no verb to fix it.
        """
        runner = _RecordingRunner()
        dispatcher = SchedulerDispatcher(
            dialect_for("torque"),
            runner,  # type: ignore[arg-type]
            scratch_root="/scratch/moved-by-the-operator",
        )
        stale = SchedulerHandle(
            job_id="1.cluster",
            remote_workspace="/scratch/original/.vibeqc-cluster/jobs/schedjob0003",
        )

        dispatcher.cleanup_remote_workspace(stale, vq_job_id="schedjob0003")

        assert runner.runs and runner.runs[0][:2] == ["rm", "-rf"]
        assert stale.remote_workspace in runner.runs[0]

    @pytest.mark.parametrize(
        "workspace",
        [
            "/etc",
            "/scratch/original/.vibeqc-cluster/jobs",
            "/scratch/original/.vibeqc-cluster/jobs/../../../etc",
            "/not-a-vq-root/schedjob0004",
            ".vibeqc-cluster/jobs/schedjob0004",
            "/scratch/original/.vibeqc-cluster/jobs/someone-elses-job",
        ],
    )
    def test_remote_cleanup_still_refuses_anything_that_is_not_a_job_dir(
        self, workspace: str
    ) -> None:
        dispatcher = SchedulerDispatcher(
            dialect_for("torque"),
            _RefusingRunner(),  # type: ignore[arg-type]
            scratch_root="/scratch/original",
        )
        with pytest.raises(SchedulerError, match="outside vq job root"):
            dispatcher.cleanup_remote_workspace(
                SchedulerHandle(job_id="1.cluster", remote_workspace=workspace),
                vq_job_id="schedjob0004",
            )

    def test_an_unbound_record_binds_on_the_next_observation(
        self, daemon: Daemon
    ) -> None:
        """The stated transition for records that predate the binding."""
        dispatcher = _RecordingDispatcher("/scratch/original")
        spec = self._scheduler_spec(daemon, "schedjob0005")
        spec.state = JobState.RUNNING
        spec.scheduler_job_id = "5.cluster"
        spec.write(daemon._spec_path("schedjob0005"))
        assert spec.scheduler_remote_workspace is None

        bound = daemon._bind_scheduler_workspace(
            spec,
            dispatcher,  # type: ignore[arg-type]
            spec_path=daemon._spec_path("schedjob0005"),
        )

        expected = "/scratch/original/.vibeqc-cluster/jobs/schedjob0005"
        assert bound == expected
        assert (
            JobSpec.read(
                daemon._spec_path("schedjob0005")
            ).scheduler_remote_workspace
            == expected
        )

    def test_a_scratch_root_change_is_refused_while_a_job_is_unbound(
        self, daemon: Daemon, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """And the consequence is explicit: the old root keeps serving.

        A current-config fallback alone cannot make the change safe, which is
        why the refusal exists rather than a best-effort guess.
        """
        from vq import config as config_mod

        cfg_dir = tmp_path / "sched-cfg"
        cfg_dir.mkdir()

        def _write_host(scratch_root: str) -> None:
            (cfg_dir / "config.toml").write_text(
                "[hosts.host_f]\n"
                'ssh = "host-f.invalid"\n'
                'scheduler = "pbs"\n'
                'scheduler_dialect = "torque"\n'
                'scheduler_driver = "localhost"\n'
                f'scratch_root = "{scratch_root}"\n'
                "\n[hosts.localhost]\n"
                'ssh = "localhost"\n',
                encoding="utf-8",
            )

        monkeypatch.setenv(config_mod.ENV_CONFIG_DIR, str(cfg_dir))
        _write_host("/scratch/original")
        first = daemon._scheduler_dispatcher_for("host_f")
        assert first.scratch_root == "/scratch/original"

        unbound = self._scheduler_spec(daemon, "schedjob0006")
        unbound.state = JobState.RUNNING
        unbound.scheduler_job_id = "6.cluster"
        unbound.write(daemon._spec_path("schedjob0006"))

        _write_host("/scratch/moved-by-the-operator")
        held = daemon._scheduler_dispatcher_for("host_f")
        assert held.scratch_root == "/scratch/original", (
            "#63: adopting the new root would strand the unbound job's "
            "staged workspace"
        )

        # Bind it -- the next observation does this -- and the change lands.
        daemon._bind_scheduler_workspace(
            unbound,
            first,
            spec_path=daemon._spec_path("schedjob0006"),
        )
        adopted = daemon._scheduler_dispatcher_for("host_f")
        assert adopted.scratch_root == "/scratch/moved-by-the-operator"

    def test_the_unbound_probe_ignores_terminal_and_other_hosts(self) -> None:
        from vq.scheduler_dispatch import unbound_staged_job_ids

        def _spec(jobid: str, **kw: object) -> JobSpec:
            return JobSpec(
                id=jobid,
                command=["true"],
                cwd="/tmp",
                cpus=1,
                **kw,  # type: ignore[arg-type]
            )

        specs = [
            _spec("a", scheduler_target="host_f", state=JobState.RUNNING),
            _spec(
                "b",
                scheduler_target="host_f",
                state=JobState.RUNNING,
                scheduler_remote_workspace="/scratch/original/x/b",
            ),
            _spec("c", scheduler_target="host_f", state=JobState.COMPLETED),
            _spec("d", scheduler_target="other", state=JobState.RUNNING),
            _spec("e", state=JobState.RUNNING),
        ]

        assert unbound_staged_job_ids(specs, "host_f") == ["a"]
