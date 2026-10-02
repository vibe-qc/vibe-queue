"""#82: the repeating pause-intent sweep skips the rows the daemon proved.

``Daemon.iterate`` walks its queue directory three times per tick. After #22
two of those walks -- dispatch and the deferred scheduler-reattach retry --
share one scan that costs a ``stat`` per settled terminal row. The third,
``pause_resume.reconcile_pause_intents(..., omit_rows_without_intent=True)``,
globs the directory itself and so got nothing from that work: it still opened,
JSON-parsed and model-validated every retained row on every tick just to learn
that a finished job carries no pause intent. At 100 live + 16,000 retained rows
that made it the largest single term in a tick by a factor of five.

The daemon now hands the sweep the verdict its own passes already recorded: a
row whose *exact bytes* an earlier full read found settled
(``_spec_is_off_admission_path``) and free of any durable pause intent. What
these tests pin:

* a settled row costs a ``stat``, not a read, from the second sweep on -- and a
  whole second tick reads no retained row at all;
* the verdict is about those bytes only. A row that gains an intent, a row
  rewritten for any other reason, a row the daemon has never read, a row that
  cannot be stat'ed and a row that cannot be parsed are all swept exactly as
  before, and an intent armed on a settled row is finished on the next sweep;
* being settled is not on its own a verdict: a terminal row that *already*
  carries an intent -- what a ``vq pause`` killed before SIGSTOP leaves behind
  when its job exits underneath it -- is never skipped;
* **the exact proofs are untouched.** ``pause_token_scope_with_proof`` and
  ``_prove_pause_token_absent`` still lock and read every row, including
  terminal ones, with the skip warm in the same process; the default
  ``omit_rows_without_intent=False`` sweep still locks and authorizes every
  row; and a skip predicate handed to a scan that has not asked for the
  unlocked hint is refused outright rather than quietly honoured.

No test here delivers a signal. The sweep's purpose is SIGSTOP/SIGCONT to a
process group, and a stale pgid in a fixture is not a bounded mistake -- it
could stop or kill the pytest process group. ``no_real_signals`` replaces the
process-group entry point for the whole module, records what would have been
sent, and restores the process-global handlers it guards.
"""
from __future__ import annotations

import contextlib
import os
import signal
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest

from vq import cgroup, config, ownership, paths, pause_resume, spec_access
from vq import daemon as daemon_module
from vq.daemon import _STAT_IDENTITY_ABSENT, Daemon, _stat_identity
from vq.spec import JobSpec, JobState, utcnow_iso

# A pgid no test may signal for real. Recorded, never delivered.
_FAKE_PGID = 424242

# Handlers a stray `signal.signal` in this module would leave behind for the
# rest of the suite. SIGSTOP is deliberately absent: it cannot be caught, so
# it has no handler to restore.
_GUARDED_SIGNALS = (signal.SIGCONT, signal.SIGTERM, signal.SIGINT)


def _group_is_gone(_pgid: int, _sig: int) -> None:
    raise ProcessLookupError(f"process group {_pgid} is gone")


class _SignalRecorder:
    """Records what the sweep would have signalled, and delivers nothing.

    ``outcome`` stands in for the kernel's answer: the default is a process
    group that has already gone, which is what a retained terminal row's
    recorded pgid almost always is by the time a sweep reaches it.
    """

    def __init__(self) -> None:
        self.calls: list[tuple[int, int]] = []
        self.outcome: Callable[[int, int], None] = _group_is_gone

    def __call__(self, pgid: int, sig: int, **_kwargs: object) -> None:
        assert pgid != os.getpgrp(), (
            "a test tried to signal the pytest process group; no test in this "
            "module may deliver a signal at all"
        )
        self.calls.append((pgid, int(sig)))
        self.outcome(pgid, sig)


@pytest.fixture(autouse=True)
def no_real_signals(monkeypatch: pytest.MonkeyPatch) -> Iterator[_SignalRecorder]:
    recorder = _SignalRecorder()
    monkeypatch.setattr(
        pause_resume.process_group, "signal_process_group", recorder,
    )
    installed = {number: signal.getsignal(number) for number in _GUARDED_SIGNALS}
    yield recorder
    leaked = [
        number
        for number, handler in installed.items()
        if signal.getsignal(number) is not handler
    ]
    for number, handler in installed.items():
        with contextlib.suppress(OSError, ValueError, TypeError):
            signal.signal(number, handler)
    assert not leaked, (
        f"process-global handlers left installed for {leaked}; restored, but "
        "a test in this module must not touch them"
    )


@pytest.fixture
def state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    paths.jobs_dir().mkdir(parents=True, exist_ok=True)
    return tmp_path


@pytest.fixture
def daemon(state: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Daemon]:
    monkeypatch.setattr(cgroup, "systemd_run_on_path", lambda: False)
    d = Daemon(
        max_cpus=8,
        max_jobs=8,
        poll_interval=0.05,
        queue_dir=paths.queue_dir(),
        jobs_dir=paths.jobs_dir(),
    )
    yield d
    d._queue_lock_fd.close()


class _OpenCounter:
    """Counts full reads of queue spec files, by whichever reader does them.

    ``JobSpec.read`` is ``path.read_text()``; the bulk pause helpers use
    ``spec_access.read_bounded_regular_spec``; the multi-user daemon scan uses
    ``daemon._read_untrusted_multi_user_spec``. All three are counted, so a
    "no read" assertion is about the file open and not about one code path.
    """

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.paths: list[Path] = []
        original_read_text = Path.read_text
        original_bounded = spec_access.read_bounded_regular_spec
        original_untrusted = daemon_module._read_untrusted_multi_user_spec

        def counting_read_text(
            inner_self: Path, *args: object, **kwargs: object,
        ) -> str:
            if inner_self.suffix == ".json" and inner_self.parent.name == "queue":
                self.paths.append(inner_self)
            return original_read_text(inner_self, *args, **kwargs)  # type: ignore[arg-type]

        def counting_bounded(path: Path) -> JobSpec:
            self.paths.append(path)
            return original_bounded(path)

        def counting_untrusted(path: Path) -> JobSpec:
            self.paths.append(path)
            return original_untrusted(path)

        monkeypatch.setattr(Path, "read_text", counting_read_text)
        monkeypatch.setattr(
            pause_resume.spec_access, "read_bounded_regular_spec", counting_bounded,
        )
        monkeypatch.setattr(
            daemon_module, "_read_untrusted_multi_user_spec", counting_untrusted,
        )

    def reset(self) -> None:
        self.paths.clear()

    @property
    def count(self) -> int:
        return len(self.paths)

    @property
    def stems(self) -> set[str]:
        return {p.stem for p in self.paths}

    def reads_of(self, jobid: str) -> int:
        return sum(1 for p in self.paths if p.stem == jobid)


class _LockCounter:
    """Counts the per-row control locks and ownership checks a scan takes."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.locked: list[Path] = []
        self.authorized: list[str] = []
        real_lock = paths.spec_lock
        real_check = ownership.check_owner

        @contextlib.contextmanager
        def counting_lock(spec_path: Path, **kwargs: object) -> Iterator[None]:
            self.locked.append(spec_path)
            with real_lock(spec_path, **kwargs):  # type: ignore[arg-type]
                yield

        def counting_check(spec: JobSpec, **kwargs: object) -> None:
            self.authorized.append(spec.id)
            real_check(spec, **kwargs)  # type: ignore[arg-type]

        monkeypatch.setattr(pause_resume.paths, "spec_lock", counting_lock)
        monkeypatch.setattr(pause_resume.ownership, "check_owner", counting_check)

    def locks_of(self, jobid: str) -> int:
        return sum(1 for p in self.locked if p.stem == jobid)


def _write_spec(
    jobid: str,
    *,
    state: JobState = JobState.COMPLETED,
    pause_intent: bool = False,
    queue_dir: Path | None = None,
    jobs_dir: Path | None = None,
) -> JobSpec:
    ws = (jobs_dir or paths.jobs_dir()) / jobid
    ws.mkdir(parents=True, exist_ok=True)
    spec = JobSpec(
        id=jobid,
        command=["true"],
        cwd=str(ws),
        cpus=1,
        state=state,
        submitter=str(os.getuid()),
    )
    if state in {JobState.COMPLETED, JobState.FAILED, JobState.KILLED}:
        spec.finished_at = utcnow_iso()
        spec.exit_code = 0 if state is JobState.COMPLETED else 1
    if state in {JobState.RUNNING, JobState.SUSPENDED}:
        spec.pgid = _FAKE_PGID
        spec.pid = _FAKE_PGID
    if pause_intent:
        spec.pgid = _FAKE_PGID
        spec.pause_intent_at = utcnow_iso()
        spec.pause_intent_pgid = _FAKE_PGID
        spec.pause_intent_by = "admin-issue82"
    spec.write((queue_dir or paths.queue_dir()) / f"{jobid}.json")
    return spec


def _arm_intent(path: Path) -> JobSpec:
    """Arm a durable pause intent on an existing row, as a killed pauser does."""
    spec = JobSpec.read(path)
    spec.pgid = _FAKE_PGID
    spec.pause_intent_at = utcnow_iso()
    spec.pause_intent_monotonic_at = 1.0
    spec.pause_intent_pgid = _FAKE_PGID
    spec.pause_intent_by = "admin-issue82"
    spec.write(path)
    return spec


def _sweep(daemon: Daemon) -> pause_resume.PauseIntentReconcileResult:
    """The sweep exactly as ``Daemon.iterate`` runs it."""
    return pause_resume.reconcile_pause_intents(
        "localhost",
        queue_dir=daemon.queue_dir,
        multi_user=daemon._multi_user,
        omit_rows_without_intent=True,
        rows_proven_without_intent=daemon._pause_intent_sweep_may_skip,
    )


class TestTheSweepCostFollowsTheLiveQueue:
    def test_a_settled_row_costs_a_stat_not_a_read_on_the_second_sweep(
        self, daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The measurement the issue asks for, as an open count.

        This is the load-bearing case: on the parent the sweep has no way to
        take a verdict at all, and reads all 43 rows on every tick.
        """
        live = [f"live-{i:03d}" for i in range(3)]
        settled = [f"kept-{i:03d}" for i in range(40)]
        for jobid in live:
            _write_spec(jobid, state=JobState.PENDING)
        for index, jobid in enumerate(settled):
            _write_spec(
                jobid,
                state=[JobState.COMPLETED, JobState.FAILED, JobState.KILLED][
                    index % 3
                ],
            )
        # The daemon's own pass records the verdict, exactly as the startup
        # walk and the previous tick's dispatch scan do.
        list(daemon._iter_specs())

        stated: list[Path] = []
        real_stat_identity = _stat_identity

        def counting_stat_identity(path: Path | None):
            if path is not None and path.suffix == ".json":
                stated.append(path)
            return real_stat_identity(path)

        monkeypatch.setattr(
            daemon_module, "_stat_identity", counting_stat_identity,
        )
        counter = _OpenCounter(monkeypatch)
        result = _sweep(daemon)

        assert result.success
        assert result.completed == ()
        assert result.cleared_gone == ()
        # Asserted first: this is the number the issue is about.
        assert counter.stems == set(live)
        assert counter.count == len(live)
        # ... and the settled rows were visited and stat'ed, not dropped from
        # the sweep's scope. A path the glob stopped returning would be a
        # different bug with the same open count.
        assert {p.stem for p in stated} == set(settled)

    def test_a_second_whole_tick_reads_no_retained_row(
        self, daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """All three per-tick walks together, through ``Daemon.iterate``."""
        _write_spec("live-001", state=JobState.PENDING)
        settled = {f"kept-{i:03d}" for i in range(12)}
        for jobid in sorted(settled):
            _write_spec(jobid, state=JobState.COMPLETED)
        monkeypatch.setattr(Daemon, "_start_job", lambda self, spec: True)

        daemon.iterate()
        counter = _OpenCounter(monkeypatch)
        daemon.iterate()

        assert counter.stems & settled == set()
        assert "live-001" in counter.stems

    def test_a_deleted_row_retires_its_verdict(self, daemon: Daemon) -> None:
        # The verdict must track the queue, not grow with everything the host
        # has ever run.
        for i in range(4):
            _write_spec(f"kept-{i:03d}")
        list(daemon._iter_specs())
        assert len(daemon._settled_spec_without_pause_intent) == 4
        (daemon.queue_dir / "kept-000.json").unlink()
        list(daemon._iter_specs())
        assert len(daemon._settled_spec_without_pause_intent) == 3


class TestTheVerdictIsAboutTheseExactBytes:
    def _settle(
        self, daemon: Daemon, jobid: str, monkeypatch: pytest.MonkeyPatch
    ) -> tuple[Path, _OpenCounter]:
        _write_spec(jobid)
        path = daemon.queue_dir / f"{jobid}.json"
        list(daemon._iter_specs())
        counter = _OpenCounter(monkeypatch)
        assert _sweep(daemon).success
        assert counter.count == 0, "the row was not proven settled and intent-free"
        counter.reset()
        return path, counter

    def test_an_intent_armed_on_a_settled_row_is_finished_by_the_next_sweep(
        self,
        daemon: Daemon,
        monkeypatch: pytest.MonkeyPatch,
        no_real_signals: _SignalRecorder,
    ) -> None:
        """The contract the whole saving rests on.

        Arming an intent rewrites the row, so its identity stops matching the
        one the verdict was recorded at -- with no intervening daemon scan.
        """
        path, counter = self._settle(daemon, "kept-1", monkeypatch)
        _arm_intent(path)

        result = _sweep(daemon)

        assert result.success
        assert result.cleared_gone == ("kept-1",)
        assert counter.reads_of("kept-1") >= 1
        # Terminal row: SIGCONT first, so a possibly-stopped process can
        # consume its pending signals, and only then is the intent cleared.
        assert no_real_signals.calls == [(_FAKE_PGID, int(signal.SIGCONT))]
        assert JobSpec.read(path).pause_intent_at is None

    def test_a_settled_row_that_already_carries_an_intent_is_never_skipped(
        self,
        daemon: Daemon,
        monkeypatch: pytest.MonkeyPatch,
        no_real_signals: _SignalRecorder,
    ) -> None:
        """Settled is not on its own a verdict about pause intents.

        A ``vq pause`` killed before SIGSTOP, whose job then exits underneath
        it, leaves a terminal row with a live intent. It is off the admission
        path -- there is nothing to dispatch -- and finishing it is exactly the
        sweep's job, so the admission scan's verdict must not cover it.
        """
        _write_spec("kept-intent", pause_intent=True)
        path = daemon.queue_dir / "kept-intent.json"
        # Two full scans: the row is settled, so from the second one the
        # admission path skips it. The sweep must not.
        list(daemon._iter_specs())
        list(daemon._iter_specs())
        assert path in daemon._settled_spec_identity
        assert path not in daemon._settled_spec_without_pause_intent

        counter = _OpenCounter(monkeypatch)
        result = _sweep(daemon)

        assert result.cleared_gone == ("kept-intent",)
        assert counter.reads_of("kept-intent") >= 1
        assert no_real_signals.calls == [(_FAKE_PGID, int(signal.SIGCONT))]
        assert JobSpec.read(path).pause_intent_at is None

    def test_an_ordinary_rewrite_is_read_again(
        self, daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # A `vq fetch` read stamp, a `--restore`, a hand edit: the sweep cannot
        # tell from a stat which field a write touched, so any write is read.
        path, counter = self._settle(daemon, "kept-1", monkeypatch)
        fresh = JobSpec.read(path)
        fresh.last_fetched_at = utcnow_iso()
        fresh.write(path)
        counter.reset()

        assert _sweep(daemon).success
        assert counter.reads_of("kept-1") == 1
        # ... and one daemon scan later it is proven again and costs a stat.
        list(daemon._iter_specs())
        counter.reset()
        assert _sweep(daemon).success
        assert counter.count == 0

    def test_a_row_the_daemon_has_never_read_is_swept(
        self, daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _path, counter = self._settle(daemon, "kept-1", monkeypatch)
        _write_spec("kept-2")

        assert _sweep(daemon).success
        assert counter.stems == {"kept-2"}

    def test_a_row_that_cannot_be_stated_is_swept(
        self, daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A stat that fails is not a proof, exactly as a read that fails is not.

        Simulated at the stat boundary, which is the only place the sweep looks
        before deciding: a row whose identity cannot be taken must fall back to
        the read.
        """
        path, counter = self._settle(daemon, "kept-1", monkeypatch)
        real_stat_identity = _stat_identity

        def unstatable(inner_path: Path | None):
            if inner_path == path:
                return _STAT_IDENTITY_ABSENT
            return real_stat_identity(inner_path)

        monkeypatch.setattr(daemon_module, "_stat_identity", unstatable)

        assert _sweep(daemon).success
        assert counter.reads_of("kept-1") == 1

    def test_a_proven_row_that_becomes_unparseable_is_swept_and_reported(
        self, daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        path, counter = self._settle(daemon, "kept-1", monkeypatch)
        lock_counter = _LockCounter(monkeypatch)
        path.write_text("{ this is not a job spec")

        result = _sweep(daemon)

        assert not result.success
        assert [jobid for jobid, _detail in result.errors] == ["kept-1"]
        assert counter.reads_of("kept-1") >= 1
        # Unreadable rows keep going through the locked path, as before.
        assert lock_counter.locked == [path]


class TestTheExactProofsStillScanEveryRow:
    """The contract `docs/operations.md` states, with the skip warm.

    ``vq admin update`` rests on these: an exact admission or
    token-clearance proof scans every row under its lock, including terminal
    rows, and a bulk summary is never proof of recovery.
    """

    def _warm(
        self, daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> tuple[list[str], _OpenCounter]:
        jobids = [f"kept-{i:03d}" for i in range(8)]
        for jobid in jobids:
            _write_spec(jobid)
        _write_spec("live-001", state=JobState.PENDING)
        list(daemon._iter_specs())
        counter = _OpenCounter(monkeypatch)
        assert _sweep(daemon).success
        assert counter.count == 1, "the retained rows were not being skipped"
        counter.reset()
        return [*jobids, "live-001"], counter

    def test_pause_admission_proof_locks_and_reads_every_row(
        self, daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        jobids, counter = self._warm(daemon, monkeypatch)
        lock_counter = _LockCounter(monkeypatch)

        proof = pause_resume.pause_token_scope_with_proof(
            "localhost", "admin-issue82", queue_dir=daemon.queue_dir,
        )

        assert proof.proven_quiescent
        for jobid in jobids:
            # The initial capture scan and the final proof scan, each under the
            # row's own lock, plus the two default reconciliation sweeps.
            assert lock_counter.locks_of(jobid) >= 2, jobid
            assert counter.reads_of(jobid) >= 2, jobid

    def test_the_token_absence_proof_locks_and_reads_every_row(
        self, daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        jobids, counter = self._warm(daemon, monkeypatch)
        lock_counter = _LockCounter(monkeypatch)

        proof = pause_resume.prove_pause_token_absent(
            "localhost", "admin-issue82", queue_dir=daemon.queue_dir,
        )

        assert proof.proven_clear
        for jobid in jobids:
            assert lock_counter.locks_of(jobid) >= 1, jobid
            assert counter.reads_of(jobid) >= 1, jobid

    def test_the_default_sweep_still_locks_and_authorizes_every_row(
        self, daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``omit_rows_without_intent=False`` is unchanged, verdict or no verdict."""
        jobids, counter = self._warm(daemon, monkeypatch)
        lock_counter = _LockCounter(monkeypatch)

        result = pause_resume.reconcile_pause_intents(
            "localhost", queue_dir=daemon.queue_dir,
        )

        assert result.success
        assert sorted(p.stem for p in lock_counter.locked) == sorted(jobids)
        assert len(lock_counter.authorized) == len(jobids)
        assert counter.stems == set(jobids)

    def test_a_verdict_without_the_unlocked_hint_is_refused(
        self, daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The guard that keeps an exact scan from inheriting a skip.

        A proof takes the default, so the only way one could ever be handed a
        predicate is a keyword threaded through by mistake. That has to fail
        closed, before a single row is read.
        """
        jobids, counter = self._warm(daemon, monkeypatch)
        lock_counter = _LockCounter(monkeypatch)

        with pytest.raises(ValueError, match="cannot be used to skip rows"):
            pause_resume.reconcile_pause_intents(
                "localhost",
                queue_dir=daemon.queue_dir,
                rows_proven_without_intent=daemon._pause_intent_sweep_may_skip,
            )

        assert counter.count == 0
        assert lock_counter.locked == []
        assert jobids  # the refusal is not an empty-queue artefact


class TestMultiUserQueues:
    def _make_daemon(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> Daemon:
        monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(tmp_path / "mu"))
        cfgdir = tmp_path / "cfg-mu"
        cfgdir.mkdir()
        (cfgdir / "config.toml").write_text("[multi_user]\nenabled = true\n")
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(cfgdir))
        monkeypatch.setattr(cgroup, "systemd_run_on_path", lambda: True)
        return Daemon(
            max_cpus=8,
            max_jobs=8,
            poll_interval=0.05,
            multi_user=True,
            queue_dir=tmp_path / "q",
            jobs_dir=tmp_path / "j",
        )

    def test_settled_rows_leave_the_per_user_sweep(
        self, state: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        uid = os.getuid()
        d = self._make_daemon(tmp_path, monkeypatch)
        try:
            # mkdir rather than `paths.provision_user_state`, which chowns.
            paths.user_queue_dir(uid).mkdir(parents=True, exist_ok=True)
            settled = {f"kept-{i:03d}" for i in range(6)}
            for jobid in sorted(settled):
                _write_spec(
                    jobid,
                    queue_dir=paths.user_queue_dir(uid),
                    jobs_dir=paths.user_jobs_dir(uid),
                )
            _write_spec(
                "live-001",
                state=JobState.PENDING,
                queue_dir=paths.user_queue_dir(uid),
                jobs_dir=paths.user_jobs_dir(uid),
            )
            assert [s.id for s in d._iter_specs()] == ["live-001"]

            counter = _OpenCounter(monkeypatch)
            result = _sweep(d)

            assert result.success
            assert counter.stems == {"live-001"}
        finally:
            d._queue_lock_fd.close()


class TestAVerdictNeverOutlivesTheReadThatDisprovesIt:
    """A full read showing an intent retires the verdict outright.

    The skip is keyed on `(st_mtime_ns, st_size, st_ino, st_dev)`, and arming
    an intent rewrites the spec, so a stale entry stops matching. True, but not
    a guarantee: `os.replace` frees the previous inode and a filesystem may
    hand the number back, after which only size and a nanosecond mtime separate
    the two writes. Recording only the intent-free rows would leave a
    contradicted verdict in place, inert for that reason alone. So a read that
    disproves a verdict removes it, and these pin that the set never holds an
    entry a read has disproved -- whatever the filesystem does with inodes.
    """

    def test_a_settled_row_that_gains_an_intent_leaves_the_verdict_set(
        self, daemon: Daemon
    ) -> None:
        _write_spec("kept-gains")
        path = daemon.queue_dir / "kept-gains.json"
        list(daemon._iter_specs())
        assert path in daemon._settled_spec_without_pause_intent
        assert daemon._pause_intent_sweep_may_skip(path) is True

        _arm_intent(path)
        list(daemon._iter_specs())

        assert path not in daemon._settled_spec_without_pause_intent
        assert daemon._pause_intent_sweep_may_skip(path) is False

    def test_the_verdict_returns_only_after_the_intent_is_gone(
        self, daemon: Daemon, no_real_signals: _SignalRecorder
    ) -> None:
        _write_spec("kept-cycle", pause_intent=True)
        path = daemon.queue_dir / "kept-cycle.json"
        list(daemon._iter_specs())
        assert path not in daemon._settled_spec_without_pause_intent

        # The sweep finishes the intent, which rewrites the row.
        _sweep(daemon)
        assert JobSpec.read(path).pause_intent_at is None
        list(daemon._iter_specs())

        assert path in daemon._settled_spec_without_pause_intent
        assert daemon._pause_intent_sweep_may_skip(path) is True
