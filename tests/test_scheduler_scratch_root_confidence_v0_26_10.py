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

        # The operator reads the warning and puts the previous value back.
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

        result = doctor.scheduler_scratch_root_check(
            "host_f", self._host_cfg("/scratch/original")
        )

        assert result["ok"] is False
        assert result["scratch_root_confirmed"] is False
        assert "restore the previous value" in str(result["message"])

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
