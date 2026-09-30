"""#63 follow-up: what may hold a host's ``scratch_root``, and what may not.

Three things were wrong with the guard that refuses to adopt a changed
``scratch_root`` while a host still has an unbound job.

1. It counted **any** non-terminal job with no recorded workspace, including
   one that had never been dispatched anywhere. An ordinary PENDING job
   therefore pinned a host for as long as a queue kept feeding it, the
   operator's change never landed, and the only trace was one log line per
   daemon lifetime.
2. Its warning said it was protecting "their staged workspaces", which for a
   never-dispatched job do not exist.
3. The stranding it exists to prevent still happened on the first start after
   an upgrade: the memory it compares against is written by the very method
   that added it, so a vq from before it leaves none, and the runbook's own
   procedure -- set the key, restart -- puts that upgrade and that change
   inside one maintenance window.

Everything here runs against temporary directories and dispatcher doubles. No
SSH, no scheduler, no host. Site identifiers are the placeholders the sibling
modules use (``host_f``).
"""
from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest

from vq import paths
from vq.daemon import Daemon
from vq.spec import JobSpec, JobState

# The names #63's follow-up adds are reached through their modules rather than
# imported at the top, so this module still collects against a build that does
# not have them and each test then fails on its own claim instead of on
# collection -- the arrangement the sibling #51 and #63 modules use.


@pytest.fixture
def daemon(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Daemon:
    monkeypatch.setenv("VQ_STATE_DIR", str(tmp_path / "state"))
    monkeypatch.setenv("VQ_SCRATCH_DIR", str(tmp_path / "scratch"))
    d = Daemon(
        max_cpus=4,
        poll_interval=0.05,
        queue_dir=paths.queue_dir(),
        jobs_dir=paths.jobs_dir(),
    )
    d.queue_dir.mkdir(parents=True, exist_ok=True)
    d.jobs_dir.mkdir(parents=True, exist_ok=True)
    return d


@pytest.fixture
def host_config(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):  # noqa: ANN201
    """A writer for one scheduler host's ``scratch_root``."""
    from vq import config as config_mod

    cfg_dir = tmp_path / "sched-cfg"
    cfg_dir.mkdir()
    monkeypatch.setenv(config_mod.ENV_CONFIG_DIR, str(cfg_dir))

    def _write(scratch_root: str) -> None:
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

    return _write


def _spec(
    daemon: Daemon,
    jobid: str,
    *,
    state: JobState = JobState.PENDING,
    retry_count: int = 0,
    workspace: str | None = None,
) -> JobSpec:
    cwd = daemon.jobs_dir / jobid
    cwd.mkdir(parents=True, exist_ok=True)
    spec = JobSpec(
        id=jobid,
        command=["true"],
        cwd=str(cwd),
        cpus=1,
        scheduler_target="host_f",
        state=state,
        retry_count=retry_count,
        retry_max=3 if retry_count else 0,
        scheduler_remote_workspace=workspace,
    )
    spec.write(daemon._spec_path(jobid))
    return spec


def _remembered(daemon: Daemon) -> dict[str, object]:
    path = paths.scheduler_scratch_roots_path(multi_user=False)
    return json.loads(path.read_text(encoding="utf-8"))["roots"]


# --------------------------------------------------------------------------
# 1. Which jobs may hold a host back
# --------------------------------------------------------------------------


class TestWhatCanActuallyBeStranded:
    def test_a_pending_job_that_was_never_dispatched_holds_nothing(self) -> None:
        """The defect: an ordinary PENDING job pinned a host's root forever.

        Nothing stages a workspace before dispatch, and the PENDING ->
        SUBMITTING write that precedes the submit records it in the same
        durable transaction. A first attempt still PENDING has no directory on
        the host, so there is nothing for the old derivation to be the only
        way back to.
        """
        from vq.scheduler_dispatch import unbound_staged_job_ids

        def _row(jobid: str, **kw: object) -> JobSpec:
            return JobSpec(
                id=jobid, command=["true"], cwd="/tmp", cpus=1,
                **kw,  # type: ignore[arg-type]
            )

        specs = [
            _row("pending", scheduler_target="host_f", state=JobState.PENDING),
            _row("running", scheduler_target="host_f", state=JobState.RUNNING),
        ]

        assert unbound_staged_job_ids(specs, "host_f") == ["running"]

    @pytest.mark.parametrize(
        ("state", "retry_count", "held"),
        [
            (JobState.PENDING, 0, False),
            (JobState.PENDING, 1, True),
            (JobState.SUBMITTING, 0, True),
            (JobState.SUBMIT_OUTCOME_UNKNOWN, 0, True),
            (JobState.RUNNING, 0, True),
            (JobState.SUSPENDED, 0, True),
            (JobState.COMPLETED, 0, False),
            (JobState.FAILED, 0, False),
        ],
    )
    def test_the_line_is_has_this_id_ever_been_dispatched(
        self, state: JobState, retry_count: int, held: bool
    ) -> None:
        """A retry is a second attempt at the SAME id, so it may already have
        staged. ``retry_count`` is the durable record of that and is never
        reset, which is what keeps this from being "PENDING means safe"."""
        from vq.scheduler_dispatch import unbound_staged_job_ids

        spec = JobSpec(
            id="j",
            command=["true"],
            cwd="/tmp",
            cpus=1,
            scheduler_target="host_f",
            state=state,
            retry_count=retry_count,
            retry_max=3 if retry_count else 0,
        )

        assert unbound_staged_job_ids([spec], "host_f") == (["j"] if held else [])


# --------------------------------------------------------------------------
# 2. The guard, end to end through the daemon
# --------------------------------------------------------------------------


class TestTheGuardOnARunningDaemon:
    def test_a_pending_queue_no_longer_pins_a_host(
        self,
        daemon: Daemon,
        host_config,  # noqa: ANN001
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """#63's own scenario, with a continuously-fed queue: the operator's
        change has to land."""
        host_config("/scratch/original")
        assert daemon._scheduler_dispatcher_for("host_f").scratch_root == (
            "/scratch/original"
        )
        for index in range(3):
            _spec(daemon, f"pend{index}", state=JobState.PENDING)

        host_config("/scratch/moved-by-the-operator")
        with caplog.at_level(logging.WARNING, logger="vq.daemon"):
            adopted = daemon._scheduler_dispatcher_for("host_f")

        assert adopted.scratch_root == "/scratch/moved-by-the-operator"
        assert daemon._scheduler_pinned_roots == {}
        assert not [
            record.getMessage()
            for record in caplog.records
            if "keeping the previous root" in record.getMessage()
        ]

    def test_a_job_that_may_be_staged_still_pins_the_host(
        self,
        daemon: Daemon,
        host_config,  # noqa: ANN001
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        host_config("/scratch/original")
        first = daemon._scheduler_dispatcher_for("host_f")
        _spec(daemon, "live0", state=JobState.RUNNING)
        _spec(daemon, "pend0", state=JobState.PENDING)

        host_config("/scratch/moved-by-the-operator")
        with caplog.at_level(logging.WARNING, logger="vq.daemon"):
            held = daemon._scheduler_dispatcher_for("host_f")

        assert held.scratch_root == "/scratch/original"
        assert daemon._scheduler_pinned_roots == {"host_f": "/scratch/original"}
        (warning,) = [
            record.getMessage()
            for record in caplog.records
            if "keeping the previous root" in record.getMessage()
        ]
        # The warning names the job that can actually be stranded, and only it.
        assert "live0" in warning
        assert "pend0" not in warning
        assert "1 job(s)" in warning
        # And it no longer claims a never-dispatched job has a staged
        # workspace to protect.
        assert "their staged workspaces" not in warning
        assert "vq doctor host_f" in warning

        # Binding the one job that could be stranded releases the host.
        daemon._bind_scheduler_workspace(
            JobSpec.read(daemon._spec_path("live0")),
            first,
            spec_path=daemon._spec_path("live0"),
        )
        assert daemon._scheduler_dispatcher_for("host_f").scratch_root == (
            "/scratch/moved-by-the-operator"
        )


# --------------------------------------------------------------------------
# 3. The first start after an upgrade
# --------------------------------------------------------------------------


class TestTheFirstStartAfterAnUpgrade:
    """The case the guard was built for and could not see.

    The remembered-root file is written by ``_scheduler_dispatcher_for``, so a
    vq from before it leaves none. Upgrade and set the new root in one
    maintenance window and the first start had nothing to compare against: it
    built a dispatcher on the new root and bound a pre-upgrade job to it
    **durably**, which no later configuration change can undo.
    """

    def test_a_pre_upgrade_job_is_not_bound_to_an_unconfirmed_root(
        self,
        daemon: Daemon,
        host_config,  # noqa: ANN001
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        # The pre-upgrade job: live, and carrying no recorded workspace
        # because the vq that dispatched it had no such field.
        pre_upgrade = _spec(daemon, "legacy0", state=JobState.RUNNING)
        assert pre_upgrade.scheduler_remote_workspace is None
        # The operator sets the new root in the same window as the upgrade.
        host_config("/scratch/moved-by-the-operator")
        assert not paths.scheduler_scratch_roots_path(multi_user=False).exists()

        with caplog.at_level(logging.WARNING, logger="vq.daemon"):
            dispatcher = daemon._scheduler_dispatcher_for("host_f")
            derived = daemon._bind_scheduler_workspace(
                pre_upgrade,
                dispatcher,
                spec_path=daemon._spec_path("legacy0"),
            )

        # It serves the configured root, because no other root is known...
        assert dispatcher.scratch_root == "/scratch/moved-by-the-operator"
        assert derived.startswith("/scratch/moved-by-the-operator/")
        # ...but it does NOT write that guess onto the pre-upgrade spec.
        assert (
            JobSpec.read(
                daemon._spec_path("legacy0")
            ).scheduler_remote_workspace
            is None
        ), "a durable binding to an unconfirmed root cannot be undone"
        # The memory records the doubt, so a restart inherits it.
        assert _remembered(daemon) == {
            "host_f": {
                "root": "/scratch/moved-by-the-operator",
                "confirmed": False,
            }
        }
        messages = [record.getMessage() for record in caplog.records]
        assert any(
            "with no memory of a previous one" in message for message in messages
        ), messages
        assert any("legacy0" in message for message in messages), messages

    def test_restoring_the_previous_root_repairs_the_job(
        self,
        daemon: Daemon,
        host_config,  # noqa: ANN001
    ) -> None:
        """Which is the whole point of not writing the guess down.

        On the parent commit the spec now names a directory under the root the
        operator moved to, nothing ever staged anything there, and putting the
        key back changes nothing because the record is absolute.
        """
        pre_upgrade = _spec(daemon, "legacy1", state=JobState.RUNNING)
        host_config("/scratch/moved-by-the-operator")
        daemon._bind_scheduler_workspace(
            pre_upgrade,
            daemon._scheduler_dispatcher_for("host_f"),
            spec_path=daemon._spec_path("legacy1"),
        )

        # The operator reads the warning, puts the previous value back and
        # restarts the driver: fresh caches, so the next lookup re-derives.
        # (A reload alone would not re-derive a reattached job's handle.)
        host_config("/scratch/original")
        daemon._scheduler_dispatchers.clear()
        daemon._scheduler_dispatcher_fingerprints.clear()
        repaired = daemon._scheduler_dispatcher_for("host_f")

        from vq.scheduler_dispatch import bound_remote_workspace

        assert bound_remote_workspace(
            repaired,  # type: ignore[arg-type]
            JobSpec.read(daemon._spec_path("legacy1")),
        ) == repaired.remote_workspace("legacy1")
        assert repaired.remote_workspace("legacy1").startswith(
            "/scratch/original/"
        )

    def test_an_unconfirmed_root_is_followed_rather_than_defended(
        self,
        daemon: Daemon,
        host_config,  # noqa: ANN001
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """The refusal defends a root vq KNOWS was in force. An unconfirmed
        one is a guess, so there is nothing there to defend -- and refusing to
        move off it would block the repair its own warning asks for, which vq
        cannot tell from an ordinary migration anyway. Nothing is lost either
        way, because the doubt is what keeps the binding from being written."""
        legacy = _spec(daemon, "legacy2", state=JobState.RUNNING)
        host_config("/scratch/first-start")
        assert daemon._scheduler_dispatcher_for("host_f").scratch_root == (
            "/scratch/first-start"
        )

        host_config("/scratch/changed-again")
        with caplog.at_level(logging.WARNING, logger="vq.daemon"):
            served = daemon._scheduler_dispatcher_for("host_f")

        assert served.scratch_root == "/scratch/changed-again"
        assert daemon._scheduler_pinned_roots == {}
        assert daemon._scheduler_root_is_confirmed("host_f") is False
        assert any(
            "while the root in force was still unconfirmed" in message
            for message in (
                record.getMessage() for record in caplog.records
            )
        )
        # Still nothing durable on the pre-upgrade spec, through both moves.
        daemon._bind_scheduler_workspace(
            legacy, served, spec_path=daemon._spec_path("legacy2")
        )
        assert (
            JobSpec.read(
                daemon._spec_path("legacy2")
            ).scheduler_remote_workspace
            is None
        )

    def test_the_doubt_clears_when_the_jobs_drain(
        self,
        daemon: Daemon,
        host_config,  # noqa: ANN001
    ) -> None:
        """Without a restart: the pinned-root re-check owns this cadence."""
        legacy = _spec(daemon, "legacy3", state=JobState.RUNNING)
        host_config("/scratch/first-start")
        daemon._scheduler_dispatcher_for("host_f")
        assert daemon._scheduler_root_is_confirmed("host_f") is False

        legacy.state = JobState.COMPLETED
        legacy.write(daemon._spec_path("legacy3"))
        # Make the periodic re-check due rather than waiting out its interval.
        daemon._scheduler_pin_checked_at.pop("host_f", None)
        daemon._scheduler_dispatcher_for("host_f")

        assert daemon._scheduler_root_is_confirmed("host_f") is True
        assert _remembered(daemon) == {"host_f": "/scratch/first-start"}
        # And binding resumes for anything still unbound.
        later = _spec(daemon, "legacy4", state=JobState.RUNNING)
        dispatcher = daemon._scheduler_dispatcher_for("host_f")
        daemon._bind_scheduler_workspace(
            later,
            dispatcher,
            spec_path=daemon._spec_path("legacy4"),
        )
        assert JobSpec.read(
            daemon._spec_path("legacy4")
        ).scheduler_remote_workspace == dispatcher.remote_workspace("legacy4")

    def test_a_restart_inherits_the_doubt(
        self,
        daemon: Daemon,
        host_config,  # noqa: ANN001
    ) -> None:
        """A restart must not read an unconfirmed entry as adopted. One that
        did would defend the guessed root against the operator's repair and
        bind the pre-upgrade job to it durably.

        What this does *not* pin is that the daemon reads the flag at all: a
        restart with no entry for the host re-derives the same doubt from "no
        memory, and jobs that may be staged", and behaves identically but for
        which warning it logs. The flag's consumer that differs is ``vq
        doctor``, pinned in section 5 (fifth review of 8de79e1)."""
        legacy = _spec(daemon, "legacy5", state=JobState.RUNNING)
        host_config("/scratch/moved")
        daemon._scheduler_dispatcher_for("host_f")
        assert _remembered(daemon)["host_f"] == {
            "root": "/scratch/moved", "confirmed": False,
        }

        host_config("/scratch/original")
        daemon._queue_lock_fd.close()  # the first daemon stops
        restarted = Daemon(
            max_cpus=4,
            poll_interval=0.05,
            queue_dir=daemon.queue_dir,
            jobs_dir=daemon.jobs_dir,
        )
        served = restarted._scheduler_dispatcher_for("host_f")
        restarted._bind_scheduler_workspace(
            legacy, served, spec_path=restarted._spec_path("legacy5"),
        )

        assert served.scratch_root == "/scratch/original"
        assert restarted._scheduler_pinned_roots == {}
        assert JobSpec.read(
            restarted._spec_path("legacy5")
        ).scheduler_remote_workspace is None

    @pytest.mark.parametrize("confirmed", [False, True], ids=["unconfirmed", "held"])
    def test_an_edit_to_the_host_still_lands_while_its_root_is_in_question(
        self,
        daemon: Daemon,
        host_config,  # noqa: ANN001
        tmp_path: Path,
        confirmed: bool,
    ) -> None:
        """Both branches reuse the cached dispatcher, which is right on the
        periodic re-check and wrong on a config edit: the host kept its old
        ``ssh`` alias for as long as it stayed held or unconfirmed, while the
        log said the dispatcher was being rebuilt."""
        if confirmed:
            host_config("/scratch/original")
            daemon._scheduler_dispatcher_for("host_f")
        _spec(daemon, "legacy6", state=JobState.RUNNING)
        host_config("/scratch/moved")
        before = daemon._scheduler_dispatcher_for("host_f")
        assert daemon._scheduler_root_is_confirmed("host_f") is confirmed
        root = before.scratch_root

        cfg = tmp_path / "sched-cfg" / "config.toml"
        cfg.write_text(
            cfg.read_text(encoding="utf-8").replace(
                'ssh = "host-f.invalid"', 'ssh = "host-f-new.invalid"'
            ),
            encoding="utf-8",
        )
        after = daemon._scheduler_dispatcher_for("host_f")

        assert after is not before
        assert after.runner._host_cfg.ssh == "host-f-new.invalid"
        assert after.scratch_root == root, "the root question is unchanged"
        assert daemon._scheduler_root_is_confirmed("host_f") is confirmed

    @pytest.mark.parametrize("confirmed", [False, True], ids=["unconfirmed", "held"])
    def test_the_periodic_recheck_keeps_the_instance(
        self,
        daemon: Daemon,
        host_config,  # noqa: ANN001
        confirmed: bool,
    ) -> None:
        """The other half of the edit test above. With the config unchanged,
        the re-check that a held or unconfirmed host runs every
        ``_PINNED_SCRATCH_ROOT_RECHECK_SECONDS`` must hand back the same
        dispatcher: a rebuild would throw away whether the remote job root
        exists and the incremental readers' tail cursors, on every re-check."""
        from vq import daemon as daemon_mod

        now = [1000.0]
        daemon._scheduler_poll_clock = lambda: now[0]
        if confirmed:
            host_config("/scratch/original")
            daemon._scheduler_dispatcher_for("host_f")
        _spec(daemon, "legacy7", state=JobState.RUNNING)
        host_config("/scratch/moved")
        before = daemon._scheduler_dispatcher_for("host_f")
        assert daemon._scheduler_root_is_confirmed("host_f") is confirmed
        assert (daemon._scheduler_pinned_roots != {}) is confirmed

        # The re-check really ran -- it re-reads the queue -- rather than an
        # early return handing back the cache before it: returning the same
        # instance proves nothing on its own, and neither does the gate's
        # timestamp, which the gate writes before any return (sixth review
        # of fda39c4).
        real_read = daemon._strandable_scheduler_workspaces
        reads: list[str] = []

        def counted(target: str) -> list[str]:
            reads.append(target)
            return real_read(target)

        daemon._strandable_scheduler_workspaces = counted  # type: ignore[method-assign]
        now[0] += daemon_mod._PINNED_SCRATCH_ROOT_RECHECK_SECONDS + 1.0
        assert daemon._scheduler_dispatcher_for("host_f") is before
        assert reads == ["host_f"]

    def test_a_failed_write_of_the_record_is_retried(
        self,
        daemon: Daemon,
        host_config,  # noqa: ANN001
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The in-memory copy moved before the write, and an unchanged entry
        returned early -- so after one failed write every later lookup found
        its entry "recorded" and never wrote again. The file, which ``vq
        doctor`` reads and a restart inherits, then named a root the driver
        had stopped serving, for the rest of the daemon's life."""
        from vq import config as config_mod
        from vq import daemon as daemon_mod
        from vq import doctor

        now = [1000.0]
        daemon._scheduler_poll_clock = lambda: now[0]
        _spec(daemon, "legacy8", state=JobState.RUNNING)
        host_config("/scratch/moved")
        daemon._scheduler_dispatcher_for("host_f")
        assert _remembered(daemon)["host_f"] == {
            "root": "/scratch/moved", "confirmed": False,
        }

        host_config("/scratch/original")
        real_write = paths.atomic_write_text
        failures = [1]

        def flaky(path, text):  # noqa: ANN001, ANN202
            if failures[0]:
                failures[0] -= 1
                raise OSError("no space left on device")
            return real_write(path, text)

        monkeypatch.setattr(paths, "atomic_write_text", flaky)
        served = daemon._scheduler_dispatcher_for("host_f")
        assert served.scratch_root == "/scratch/original"
        assert _remembered(daemon)["host_f"]["root"] == "/scratch/moved"

        now[0] += daemon_mod._PINNED_SCRATCH_ROOT_RECHECK_SECONDS + 1.0
        daemon._scheduler_dispatcher_for("host_f")

        assert _remembered(daemon)["host_f"] == {
            "root": "/scratch/original", "confirmed": False,
        }
        result = doctor.scheduler_scratch_root_check(
            "host_f", config_mod.load_config().host("host_f")
        )
        assert result["scratch_root_in_force"] == "/scratch/original"

    def test_a_failed_write_on_a_confirmed_adoption_is_retried(
        self,
        daemon: Daemon,
        host_config,  # noqa: ANN001
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """The test above passes only because its host is unconfirmed, which
        keeps it on the re-check that reaches the record. A confirmed,
        unpinned host returns its cached dispatcher before any write, so a
        retry reached only from there never ran for it: the file named the
        root it had adopted away from until the daemon restarted (seventh
        review of 279e726). Also: the warning is said once, not per retry,
        and a successful retry stops the writes."""
        from vq import daemon as daemon_mod

        now = [1000.0]
        daemon._scheduler_poll_clock = lambda: now[0]
        host_config("/scratch/a")
        daemon._scheduler_dispatcher_for("host_f")
        assert _remembered(daemon) == {"host_f": "/scratch/a"}

        real_write = paths.atomic_write_text
        attempts: list[str] = []
        failing = [True]

        def flaky(path, text):  # noqa: ANN001, ANN202
            attempts.append(text)
            if failing[0]:
                raise OSError("no space left on device")
            return real_write(path, text)

        monkeypatch.setattr(paths, "atomic_write_text", flaky)
        host_config("/scratch/b")
        with caplog.at_level(logging.WARNING, logger="vq.daemon"):
            assert daemon._scheduler_dispatcher_for("host_f").scratch_root == (
                "/scratch/b"
            )
            # Still failing: retried once per interval, not per lookup, and
            # warned about once.
            for _ in range(3):
                daemon._scheduler_dispatcher_for("host_f")
            now[0] += daemon_mod._PINNED_SCRATCH_ROOT_RECHECK_SECONDS + 1.0
            daemon._scheduler_dispatcher_for("host_f")
        assert len(attempts) == 2
        assert _remembered(daemon) == {"host_f": "/scratch/a"}
        warnings = [
            r.getMessage() for r in caplog.records
            if "could not record scheduler scratch roots" in r.getMessage()
        ]
        assert len(warnings) == 1, warnings

        failing[0] = False
        now[0] += daemon_mod._PINNED_SCRATCH_ROOT_RECHECK_SECONDS + 1.0
        with caplog.at_level(logging.INFO, logger="vq.daemon"):
            daemon._scheduler_dispatcher_for("host_f")
        assert _remembered(daemon) == {"host_f": "/scratch/b"}
        assert any(
            "after a failed write" in r.getMessage() for r in caplog.records
        ), "the recovery is said, not only the failure"
        # Recovered: nothing further to write, however many lookups follow.
        now[0] += daemon_mod._PINNED_SCRATCH_ROOT_RECHECK_SECONDS + 1.0
        for _ in range(3):
            daemon._scheduler_dispatcher_for("host_f")
        assert len(attempts) == 3

    def test_a_daemon_that_only_polls_still_retries_the_write(
        self,
        daemon: Daemon,
        host_config,  # noqa: ANN001
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The adoption that failed to record is often a host's last lookup:
        from then on the daemon only polls, and the poll path never calls
        ``_scheduler_dispatcher_for``. A retry reached only from there left
        the file stale until the next dispatch or restart (eighth review of
        39946e3)."""
        from vq import daemon as daemon_mod

        now = [1000.0]
        daemon._scheduler_poll_clock = lambda: now[0]
        host_config("/scratch/a")
        daemon._scheduler_dispatcher_for("host_f")
        real_write = paths.atomic_write_text
        failing = [True]

        def flaky(path, text):  # noqa: ANN001, ANN202
            if failing[0]:
                raise OSError("no space left on device")
            return real_write(path, text)

        monkeypatch.setattr(paths, "atomic_write_text", flaky)
        host_config("/scratch/b")
        daemon._scheduler_dispatcher_for("host_f")
        assert _remembered(daemon) == {"host_f": "/scratch/a"}

        failing[0] = False
        lookups: list[str] = []
        real_lookup = daemon._scheduler_dispatcher_for
        daemon._scheduler_dispatcher_for = (  # type: ignore[method-assign]
            lambda target: lookups.append(target) or real_lookup(target)
        )
        now[0] += daemon_mod._PINNED_SCRATCH_ROOT_RECHECK_SECONDS + 1.0
        daemon._reconcile_scheduler()

        assert lookups == [], "the pass looked a host up; this test needs it not to"
        assert _remembered(daemon) == {"host_f": "/scratch/b"}

    def test_a_first_start_with_nothing_to_strand_confirms_immediately(
        self,
        daemon: Daemon,
        host_config,  # noqa: ANN001
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """The ordinary upgrade, which must stay silent and bind normally."""
        host_config("/scratch/original")
        _spec(daemon, "pend0", state=JobState.PENDING)

        with caplog.at_level(logging.WARNING, logger="vq.daemon"):
            daemon._scheduler_dispatcher_for("host_f")

        assert _remembered(daemon) == {"host_f": "/scratch/original"}
        assert daemon._scheduler_root_is_confirmed("host_f") is True
        assert not [
            record.getMessage()
            for record in caplog.records
            if "no memory of a previous one" in record.getMessage()
        ]


# --------------------------------------------------------------------------
# 4. The memory file, and who may read it
# --------------------------------------------------------------------------


class TestTheRememberedRootFile:
    def test_a_version_1_entry_reads_as_confirmed(self, tmp_path: Path) -> None:
        """A bare string is a root a daemon ADOPTED, which it only did with
        nothing on the host left to strand."""
        from vq.scheduler_dispatch import read_scheduler_scratch_roots

        path = tmp_path / "roots.json"
        path.write_text(
            json.dumps(
                {
                    "schema": "vq.scheduler-scratch-roots/1",
                    "roots": {"host_f": "/scratch/original/"},
                }
            ),
            encoding="utf-8",
        )

        memory = read_scheduler_scratch_roots(path)

        assert memory["host_f"].root == "/scratch/original"
        assert memory["host_f"].confirmed is True

    def test_a_confirmed_root_is_still_written_as_a_bare_string(
        self, daemon: Daemon, host_config  # noqa: ANN001
    ) -> None:
        """So a rollback to a vq that only knows version 1 keeps every entry
        that vq could itself have written, and loses only the unconfirmed ones
        -- of which it would have had no memory at all."""
        host_config("/scratch/original")
        daemon._scheduler_dispatcher_for("host_f")

        assert _remembered(daemon) == {"host_f": "/scratch/original"}

    @pytest.mark.parametrize(
        "body",
        [
            "not json at all",
            json.dumps({"roots": "not a mapping"}),
            json.dumps({"roots": {"host_f": {"confirmed": False}}}),
            json.dumps({"roots": {"host_f": ""}}),
        ],
    )
    def test_an_unreadable_note_is_no_memory_rather_than_a_wrong_one(
        self, tmp_path: Path, body: str
    ) -> None:
        from vq.scheduler_dispatch import read_scheduler_scratch_roots

        path = tmp_path / "roots.json"
        path.write_text(body, encoding="utf-8")

        assert read_scheduler_scratch_roots(path) == {}

    def test_a_missing_note_is_no_memory(self, tmp_path: Path) -> None:
        from vq.scheduler_dispatch import read_scheduler_scratch_roots

        assert read_scheduler_scratch_roots(tmp_path / "absent.json") == {}


# --------------------------------------------------------------------------
# 5. The operator's view
# --------------------------------------------------------------------------


class TestVqDoctorReportsAHeldHost:
    """The pinned state appeared in no command's output. It does now.

    ``vq doctor`` and not ``vq admin status``: the question is whether the
    ``scratch_root`` in this machine's ``config.toml`` is the one this
    machine's driver is running on, which is answered from local config and
    the driver's own record with no SSH and no daemon RPC -- and so needs no
    new daemon surface, which would need a SECURITY.md entry of its own.
    """

    def _host_cfg(self, scratch_root: str):  # noqa: ANN202
        from vq.config import HostConfig

        return HostConfig(
            ssh="host-f.invalid",
            scheduler="pbs",
            scheduler_dialect="torque",
            scheduler_driver="localhost",
            scratch_root=scratch_root,
        )

    def _write_memory(self, entry: object) -> None:
        path = paths.scheduler_scratch_roots_path(multi_user=False)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps({"schema": "x", "roots": {"host_f": entry}}),
            encoding="utf-8",
        )

    def test_a_pinned_host_fails_the_check_and_says_why(
        self, daemon: Daemon
    ) -> None:
        from vq import doctor

        self._write_memory("/scratch/original")
        _spec(daemon, "legacy0000001", state=JobState.RUNNING)

        result = doctor.scheduler_scratch_root_check(
            "host_f", self._host_cfg("/scratch/moved-by-the-operator")
        )

        assert result["ok"] is False
        assert result["scratch_root_in_force"] == "/scratch/original"
        assert result["scratch_root_configured"] == (
            "/scratch/moved-by-the-operator"
        )
        assert "held at" in str(result["message"])

    def test_an_unconfirmed_host_fails_the_check_and_says_what_to_do(
        self, daemon: Daemon
    ) -> None:
        from vq import doctor

        self._write_memory({"root": "/scratch/original", "confirmed": False})
        _spec(daemon, "legacy0000001", state=JobState.RUNNING)

        result = doctor.scheduler_scratch_root_check(
            "host_f", self._host_cfg("/scratch/original")
        )

        assert result["ok"] is False
        assert result["scratch_root_confirmed"] is False
        assert "restore the previous value and restart" in str(result["message"])
        assert "legacy0000001" in str(result["message"])

    # The record is written only when the driver looks a host up, never by a
    # poll, so on its own it lags config. These three are the verdicts a
    # record-only check got wrong (fourth review of 78711ba); each is what
    # the driver itself decides from the same queue.

    def test_a_moved_key_that_nothing_holds_is_not_a_held_host(
        self, daemon: Daemon
    ) -> None:
        """The runbook itself: set the key, restart. On a host with nothing
        that may be staged the driver adopts the new root at its first
        lookup, and a doctor failure here blocks a roll for nothing."""
        from vq import doctor

        self._write_memory("/scratch/original")
        _spec(daemon, "done000000001", state=JobState.COMPLETED)
        _spec(daemon, "fresh00000001", state=JobState.PENDING)

        result = doctor.scheduler_scratch_root_check(
            "host_f", self._host_cfg("/scratch/moved-by-the-operator")
        )

        assert result["ok"] is True, result["message"]
        assert "held" not in str(result["message"])
        assert "nothing holds it" in str(result["message"])

    def test_restoring_the_key_on_an_unconfirmed_host_is_not_reported_held(
        self, daemon: Daemon
    ) -> None:
        """An unconfirmed root is followed, not held. Restoring the previous
        value is the repair the warning asks for; calling the result "held
        at the root it last adopted" contradicts both the driver and the
        record, which says the root was never adopted."""
        from vq import doctor

        self._write_memory({"root": "/scratch/moved", "confirmed": False})
        _spec(daemon, "legacy0000001", state=JobState.RUNNING)

        result = doctor.scheduler_scratch_root_check(
            "host_f", self._host_cfg("/scratch/original")
        )

        assert result["ok"] is False
        assert result["scratch_root_confirmed"] is False
        assert "held at" not in str(result["message"])
        # The record proves the driver has not looked since the edit, so the
        # root in force is still the recorded one; the configured value is
        # what it will follow, and the message says which is which.
        assert result["scratch_root_in_force"] == "/scratch/moved"
        assert "'/scratch/moved' is in force unconfirmed" in str(result["message"])
        assert "config now says '/scratch/original'" in str(result["message"])

    def test_an_unconfirmed_host_whose_jobs_drained_passes(
        self, daemon: Daemon
    ) -> None:
        """The doubt is settled by the driver at its next lookup of the host,
        which may be a long way off on an idle host. Nothing is left that it
        could be wrong about, so it is not a failure in the meantime."""
        from vq import doctor

        self._write_memory({"root": "/scratch/original", "confirmed": False})
        _spec(daemon, "legacy0000001", state=JobState.COMPLETED)

        result = doctor.scheduler_scratch_root_check(
            "host_f", self._host_cfg("/scratch/original")
        )

        assert result["ok"] is True, result["message"]

    def test_an_adopted_root_passes(self, daemon: Daemon) -> None:
        from vq import doctor

        self._write_memory("/scratch/original")

        result = doctor.scheduler_scratch_root_check(
            "host_f", self._host_cfg("/scratch/original/")
        )

        assert result["ok"] is True
        assert result["scratch_root_in_force"] == "/scratch/original"

    def test_no_record_is_not_a_finding(self, daemon: Daemon) -> None:
        """Run from a machine that drives nothing, the check must not invent a
        verdict about a host it has no record of."""
        from vq import doctor

        result = doctor.scheduler_scratch_root_check(
            "host_f", self._host_cfg("/scratch/original")
        )

        assert result["ok"] is True
        assert "no record" in str(result["message"])
