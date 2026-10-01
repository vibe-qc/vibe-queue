"""#22: every settled terminal row leaves the daemon's admission scan, not
only the archived ones.

``0d30c9e`` took the read off the per-tick scan for a row that ``vq cleanup
--archive`` had stamped. Archiving is opt-in (``vq cleanup --auto-enable``), so
a driver that keeps its job history without ever archiving still paid an open, a
JSON parse and a model validation for every job it had ever run, on every pass
-- which is the cost this issue is about. The archived condition was
conservatism; the safety argument never rested on it.

A row is now left off the per-tick scan when it is **terminal**, carries **no
scheduler transaction that could still be open**, and its **on-disk identity is
unchanged** since the pass that last read it in full. What these tests pin:

* the steady-state open count follows the live queue, not the retained total;
* the identity is strong: a rewrite changes it through any component, including
  a same-size same-mtime replacement, which only ``st_ino`` catches;
* every exclusion ``0d30c9e`` listed still holds at any age -- the startup walk
  reads every row, an open (or unreadable) scheduler transaction is never
  skipped, a settled predecessor is still read back for a dependency gate, and a
  duplicate bare id is still quarantined in full.

The load-bearing tests fail on the parent commit, where a terminal row that was
never archived is reopened on every pass.
"""
from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path

import pytest

from vq import cgroup, config, listing, paths
from vq import daemon as daemon_module
from vq.daemon import (
    Daemon,
    _SchedulerSubmitBinding,
    _write_scheduler_submit_binding,
)
from vq.spec import JobSpec, JobState, utcnow_iso


@pytest.fixture
def daemon(tmp_path: Path) -> Iterator[Daemon]:
    d = Daemon(
        max_cpus=8,
        max_jobs=8,
        poll_interval=0.05,
        queue_dir=tmp_path / "queue",
        jobs_dir=tmp_path / "jobs",
    )
    d.queue_dir.mkdir(parents=True, exist_ok=True)
    d.jobs_dir.mkdir(parents=True, exist_ok=True)
    yield d
    d._queue_lock_fd.close()


class _OpenCounter:
    """Counts full reads of queue-directory spec files.

    Single-user ``JobSpec.read`` is ``path.read_text()``; the multi-user reader
    is ``daemon._read_untrusted_multi_user_spec``. Both are patched, so this
    counts the file open itself rather than a proxy for it.
    """

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.paths: list[Path] = []
        original_read_text = Path.read_text
        original_untrusted = daemon_module._read_untrusted_multi_user_spec

        def counting_read_text(inner_self: Path, *args: object, **kwargs: object):
            if inner_self.suffix == ".json" and inner_self.parent.name == "queue":
                self.paths.append(inner_self)
            return original_read_text(inner_self, *args, **kwargs)  # type: ignore[arg-type]

        def counting_untrusted(path: Path) -> JobSpec:
            self.paths.append(path)
            return original_untrusted(path)

        monkeypatch.setattr(Path, "read_text", counting_read_text)
        monkeypatch.setattr(
            daemon_module, "_read_untrusted_multi_user_spec", counting_untrusted
        )

    def reset(self) -> None:
        self.paths.clear()

    @property
    def count(self) -> int:
        return len(self.paths)


def _write_spec(
    daemon: Daemon,
    jobid: str,
    *,
    state: JobState = JobState.PENDING,
    archived: bool = False,
    scheduler_state: str | None = None,
    scheduler_target: str | None = None,
    depends_on: list[str] | None = None,
) -> JobSpec:
    ws = daemon.jobs_dir / jobid
    ws.mkdir(parents=True, exist_ok=True)
    spec = JobSpec(
        id=jobid,
        command=["true"],
        cwd=str(ws),
        cpus=1,
        state=state,
        submitter=str(os.getuid()),
        depends_on=depends_on or [],
    )
    if state in {
        JobState.COMPLETED,
        JobState.FAILED,
        JobState.KILLED,
        JobState.TIME_EXCEEDED,
        JobState.INTERRUPTED,
    }:
        spec.finished_at = utcnow_iso()
        spec.exit_code = 0 if state is JobState.COMPLETED else 1
    if archived:
        spec.archived_at = utcnow_iso()
        spec.archive_path = str(daemon.jobs_dir / f"{jobid}.tar.bz2")
    if scheduler_state is not None:
        spec.scheduler_state = scheduler_state
    if scheduler_target is not None:
        spec.scheduler_target = scheduler_target
    spec.write(daemon.queue_dir / f"{jobid}.json")
    return spec


class TestScanCostFollowsTheLiveQueue:
    def test_an_unarchived_terminal_row_costs_a_stat_on_the_second_pass(
        self, daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The measurement this issue asks for, with nothing archived at all."""
        live = [f"live-{i:03d}" for i in range(3)]
        settled = [f"kept-{i:03d}" for i in range(40)]
        for jobid in live:
            _write_spec(daemon, jobid, state=JobState.PENDING)
        for index, jobid in enumerate(settled):
            _write_spec(
                daemon,
                jobid,
                state=[JobState.COMPLETED, JobState.FAILED, JobState.KILLED][
                    index % 3
                ],
            )

        counter = _OpenCounter(monkeypatch)

        # Pass one still reads everything: a row has to be read once before the
        # daemon can know it has settled.
        first = [s.id for s in daemon._iter_specs()]
        first_opens = counter.count
        # Passes two and three are the steady state a tick pays.
        counter.reset()
        second = [s.id for s in daemon._iter_specs()]
        second_opens = counter.count
        counter.reset()
        list(daemon._iter_specs())
        third_opens = counter.count

        # Asserted first, so this is what fails on the parent commit: there,
        # nothing is archived, so every pass reopens all 43 rows.
        assert first_opens == len(live) + len(settled)
        assert second_opens == len(live)
        assert third_opens == len(live)
        assert sorted(first) == sorted(live)
        assert sorted(second) == sorted(live)

    def test_both_per_tick_passes_through_iter_specs_get_the_saving(
        self, daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``_dispatch_pending`` and ``_retry_deferred_scheduler_reattach``.

        They are the two per-tick passes that go through ``_iter_specs``. The
        third pass the issue measured, ``pause_resume.reconcile_pause_intents``,
        does its own glob and read and is unaffected by this change -- see the
        benchmark's ``pause_intent_sweep_seconds``.
        """
        _write_spec(daemon, "live-1", state=JobState.PENDING)
        for i in range(20):
            _write_spec(daemon, f"kept-{i:03d}", state=JobState.COMPLETED)
        monkeypatch.setattr(Daemon, "_start_job", lambda self, spec: True)
        daemon._dispatch_pending()

        counter = _OpenCounter(monkeypatch)
        daemon._dispatch_pending()
        assert {p.stem for p in counter.paths} == {"live-1"}
        counter.reset()
        daemon._retry_deferred_scheduler_reattach()
        assert {p.stem for p in counter.paths} <= {"live-1"}

    def test_deleting_a_spec_retires_its_proof(self, daemon: Daemon) -> None:
        # The proof set must track the queue, not grow with it forever.
        for i in range(4):
            _write_spec(daemon, f"kept-{i:03d}", state=JobState.COMPLETED)
        list(daemon._iter_specs())
        assert len(daemon._settled_spec_identity) == 4
        (daemon.queue_dir / "kept-000.json").unlink()
        list(daemon._iter_specs())
        assert len(daemon._settled_spec_identity) == 3


class TestTheIdentityIsStrong:
    def _settle(
        self, daemon: Daemon, jobid: str, monkeypatch: pytest.MonkeyPatch
    ) -> tuple[Path, _OpenCounter]:
        _write_spec(daemon, jobid, state=JobState.COMPLETED)
        path = daemon.queue_dir / f"{jobid}.json"
        list(daemon._iter_specs())
        counter = _OpenCounter(monkeypatch)
        list(daemon._iter_specs())
        assert counter.count == 0, "the row was not proven settled"
        return path, counter

    def test_an_ordinary_rewrite_is_re_read(
        self, daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        path, counter = self._settle(daemon, "kept-1", monkeypatch)
        fresh = JobSpec.read(path)
        fresh.state = JobState.PENDING
        fresh.finished_at = None
        fresh.exit_code = None
        fresh.write(path)
        counter.reset()
        # Re-read, and the fresh content puts it back on the scan outright.
        assert [s.id for s in daemon._iter_specs()] == ["kept-1"]
        assert counter.paths == [path]

    def test_a_fetch_read_stamp_is_re_read(
        self, daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # `vq fetch`'s acknowledgement mark-back sets `last_fetched_at` on the
        # terminal row and rewrites it (`fetch.py`, the mark-back under the spec
        # lock). It leaves the row terminal, so it must not be *yielded* again
        # -- but it must be re-read, because the daemon cannot know from a stat
        # alone which field an operator's write touched.
        path, counter = self._settle(daemon, "kept-1", monkeypatch)
        fresh = JobSpec.read(path)
        fresh.last_fetched_at = utcnow_iso()
        fresh.write(path)
        counter.reset()
        assert [s.id for s in daemon._iter_specs()] == []
        assert counter.paths == [path]
        # ... and one pass later it is proven settled again.
        counter.reset()
        assert [s.id for s in daemon._iter_specs()] == []
        assert counter.count == 0

    def test_a_same_size_same_mtime_replacement_is_re_read(
        self, daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The inode is what makes the proof robust rather than probable.

        ``(mtime_ns, size)`` alone would serve this row from the stale proof.
        Every spec write is an ``atomic_write_text`` rename onto a fresh inode,
        so adding ``st_ino`` -- out of the same ``stat`` -- closes it for free.
        This test fails if ``st_ino`` is dropped from ``_stat_identity``.
        """
        path, counter = self._settle(daemon, "kept-1", monkeypatch)
        before = path.stat()

        payload = path.read_bytes()
        replacement = path.parent / "replacement.tmpfile"
        replacement.write_bytes(payload)
        os.replace(replacement, path)
        os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))

        after = path.stat()
        assert after.st_mtime_ns == before.st_mtime_ns
        assert after.st_size == before.st_size
        assert after.st_ino != before.st_ino

        counter.reset()
        list(daemon._iter_specs())
        assert counter.paths == [path]

    def test_an_unreadable_row_records_no_proof(
        self, daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Corrupt content is not a settled row: it must keep costing a read,
        # so a repair puts it back on the scan on the very next pass.
        path = daemon.queue_dir / "broken-1.json"
        path.write_text("{ not json")
        counter = _OpenCounter(monkeypatch)
        assert [s.id for s in daemon._iter_specs()] == []
        assert [s.id for s in daemon._iter_specs()] == []
        assert counter.count == 2
        assert daemon._settled_spec_identity == {}


class TestExclusionsStillHold:
    def test_the_startup_walk_reads_every_row(
        self, daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # A terminal job's process group can outlive the daemon, and the
        # startup walk is what reaps it. Being terminal says nothing about
        # whether the children are gone.
        _write_spec(daemon, "kept-1", state=JobState.KILLED)
        _write_spec(daemon, "kept-2", state=JobState.COMPLETED)
        walked = [s.id for s in daemon._iter_specs(include_settled=True)]
        assert walked == ["kept-1", "kept-2"]
        # Repeatable: the walk never consults its own proofs.
        assert [s.id for s in daemon._iter_specs(include_settled=True)] == [
            "kept-1",
            "kept-2",
        ]

        counter = _OpenCounter(monkeypatch)
        daemon._reattach_or_interrupt_at_startup()
        assert {p.stem for p in counter.paths} == {"kept-1", "kept-2"}
        # ... and it warmed the skip set for the per-tick passes behind it.
        counter.reset()
        assert [s.id for s in daemon._iter_specs()] == []
        assert counter.count == 0

    @pytest.mark.parametrize(
        "scheduler_state",
        [
            "submitting",
            "submit_outcome_unknown",
            "submit_outcome_unknown_after_terminal",
            "submit_evidence_conflict_after_terminal",
            "submit_reconciliation_quarantined_after_terminal",
            "submit_cancel_pending_after_terminal",
            "scheduler_reconciliation_quarantined",
            "reattach_failed",
        ],
    )
    def test_an_open_scheduler_transaction_is_never_skipped(
        self, daemon: Daemon, scheduler_state: str
    ) -> None:
        # Age is not evidence. A terminal row whose submit is still ambiguous
        # keeps reserving its slot until remote evidence or an operator
        # resolves it, so it stays on the scan however long it sits there.
        _write_spec(
            daemon,
            "fenced-1",
            state=JobState.FAILED,
            scheduler_state=scheduler_state,
            scheduler_target="cluster",
        )
        assert [s.id for s in daemon._iter_specs()] == ["fenced-1"]
        assert [s.id for s in daemon._iter_specs()] == ["fenced-1"]
        assert daemon._settled_spec_identity == {}

    def test_a_non_terminal_row_is_never_skipped(self, daemon: Daemon) -> None:
        for state in (
            JobState.PENDING,
            JobState.RUNNING,
            JobState.SUSPENDED,
        ):
            _write_spec(daemon, f"live-{state.value}", state=state)
        expected = sorted(f"live-{s.value}" for s in (
            JobState.PENDING, JobState.RUNNING, JobState.SUSPENDED
        ))
        assert sorted(s.id for s in daemon._iter_specs()) == expected
        assert sorted(s.id for s in daemon._iter_specs()) == expected

    def test_dispatch_still_resolves_a_settled_predecessor(
        self, daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # A missing predecessor is deliberately read as "not finished yet", so
        # an unarchived settled predecessor left off the scan would park its
        # dependent forever -- a correctness regression, not a slow one.
        _write_spec(daemon, "pred-1", state=JobState.COMPLETED)
        _write_spec(daemon, "dep-1", state=JobState.PENDING, depends_on=["pred-1"])
        started: list[str] = []
        monkeypatch.setattr(
            Daemon, "_start_job", lambda self, spec: bool(started.append(spec.id))
        )
        daemon._dispatch_pending()
        daemon._dispatch_pending()
        daemon._dispatch_pending()
        assert started == ["dep-1", "dep-1", "dep-1"]

    def test_a_settled_failed_predecessor_still_cascades(
        self, daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_spec(daemon, "pred-1", state=JobState.FAILED)
        _write_spec(daemon, "dep-1", state=JobState.PENDING, depends_on=["pred-1"])
        monkeypatch.setattr(Daemon, "_start_job", lambda self, spec: True)
        # Two ticks: the second runs with the predecessor already skipped.
        daemon._dispatch_pending()
        assert (
            JobSpec.read(daemon.queue_dir / "dep-1.json").state is JobState.FAILED
        )

        _write_spec(daemon, "dep-2", state=JobState.PENDING, depends_on=["pred-1"])
        daemon._dispatch_pending()
        assert (
            JobSpec.read(daemon.queue_dir / "dep-2.json").state is JobState.FAILED
        )

    def test_a_settled_row_stays_exactly_as_retrievable(
        self, daemon: Daemon
    ) -> None:
        _write_spec(daemon, "live-1", state=JobState.PENDING)
        _write_spec(daemon, "kept-1", state=JobState.COMPLETED)
        list(daemon._iter_specs())
        list(daemon._iter_specs())
        listed = {s.id for s in listing.list_jobs("localhost", daemon.queue_dir)}
        assert listed == {"live-1", "kept-1"}
        direct = JobSpec.read(daemon.queue_dir / "kept-1.json")
        assert direct.state is JobState.COMPLETED
        assert daemon._spec_path("kept-1") == daemon.queue_dir / "kept-1.json"


class TestMultiUserQueues:
    def _make_daemon(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Daemon:
        monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(tmp_path / "mu"))
        cfgdir = tmp_path / "cfg"
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

    def _write_user_spec(
        self,
        uid: int,
        jobid: str,
        *,
        state: JobState,
        scheduler_target: str | None = None,
    ) -> JobSpec:
        # mkdir rather than `paths.provision_user_state`, which chowns and so
        # needs root for any uid but this process's own.
        ws = paths.user_workspace_dir(uid, jobid)
        ws.mkdir(parents=True, exist_ok=True)
        paths.user_queue_dir(uid).mkdir(parents=True, exist_ok=True)
        spec = JobSpec(
            id=jobid,
            command=["true"],
            cwd=str(ws),
            cpus=1,
            state=state,
            submitter=str(uid),
        )
        if state is JobState.COMPLETED:
            spec.finished_at = utcnow_iso()
            spec.exit_code = 0
        if scheduler_target is not None:
            spec.scheduler_target = scheduler_target
        spec.write(paths.user_spec_path(uid, jobid))
        return spec

    def test_settled_rows_leave_the_per_user_scan(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        uid = os.getuid()
        d = self._make_daemon(tmp_path, monkeypatch)
        try:
            self._write_user_spec(uid, "live-1", state=JobState.PENDING)
            for i in range(6):
                self._write_user_spec(
                    uid, f"kept-{i:03d}", state=JobState.COMPLETED
                )
            counter = _OpenCounter(monkeypatch)
            assert [s.id for s in d._iter_specs()] == ["live-1"]
            assert counter.count == 7
            counter.reset()
            assert [s.id for s in d._iter_specs()] == ["live-1"]
            assert counter.count == 1
            # A skipped row is not an admitted row: it must not claim an owner
            # mapping the resolver would then trust.
            assert set(d._job_uid) == {"live-1"}
            assert sorted(d._settled_spec_paths) == [
                f"kept-{i:03d}" for i in range(6)
            ]
            # ... and the startup walk still sees all seven.
            assert len(list(d._iter_specs(include_settled=True))) == 7
        finally:
            d._queue_lock_fd.close()

    def test_a_binding_change_puts_a_settled_row_back_on_the_read(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        uid = os.getuid()
        d = self._make_daemon(tmp_path, monkeypatch)
        try:
            spec = self._write_user_spec(
                uid, "kept-1", state=JobState.COMPLETED, scheduler_target="cluster"
            )
            assert [s.id for s in d._iter_specs()] == []
            counter = _OpenCounter(monkeypatch)
            assert [s.id for s in d._iter_specs()] == []
            assert counter.count == 0

            # The private binding is part of the row's identity. Writing one
            # where there was none changes it, so the row is read again.
            _write_scheduler_submit_binding(
                _SchedulerSubmitBinding(
                    owner_uid=str(uid),
                    job_id="kept-1",
                    scheduler_target="cluster",
                    cwd=spec.cwd,
                    cpus=1,
                    admitted_spec=spec,
                    transaction_state="closed",
                )
            )
            counter.reset()
            assert [s.id for s in d._iter_specs()] == []
            assert [p.name for p in counter.paths] == ["kept-1.json"]
        finally:
            d._queue_lock_fd.close()

    def test_an_open_binding_keeps_a_settled_row_on_the_scan(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The mutable row says the job is done; the daemon-owned binding says a
        # submit may still be open. The binding wins, at any age.
        uid = os.getuid()
        d = self._make_daemon(tmp_path, monkeypatch)
        try:
            spec = self._write_user_spec(
                uid, "kept-1", state=JobState.COMPLETED, scheduler_target="cluster"
            )
            _write_scheduler_submit_binding(
                _SchedulerSubmitBinding(
                    owner_uid=str(uid),
                    job_id="kept-1",
                    scheduler_target="cluster",
                    cwd=spec.cwd,
                    cpus=1,
                    admitted_spec=spec,
                    transaction_state="open",
                )
            )
            assert [s.id for s in d._iter_specs()] == ["kept-1"]
            assert [s.id for s in d._iter_specs()] == ["kept-1"]
            assert d._settled_spec_identity == {}
        finally:
            d._queue_lock_fd.close()

    def test_an_unreadable_binding_keeps_a_settled_row_on_the_scan(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # `_scheduler_binding_requires_fence` treats a binding it cannot read as
        # open, so the skip decision has to as well: absent and unreadable are
        # not the same answer.
        uid = os.getuid()
        d = self._make_daemon(tmp_path, monkeypatch)
        try:
            spec = self._write_user_spec(
                uid, "kept-1", state=JobState.COMPLETED, scheduler_target="cluster"
            )
            _write_scheduler_submit_binding(
                _SchedulerSubmitBinding(
                    owner_uid=str(uid),
                    job_id="kept-1",
                    scheduler_target="cluster",
                    cwd=spec.cwd,
                    cpus=1,
                    admitted_spec=spec,
                    transaction_state="closed",
                )
            )
            original = daemon_module._read_scheduler_submit_binding

            def refusing(owner_uid: str, job_id: str):
                if job_id == "kept-1":
                    raise ValueError("unsafe scheduler submit binding file")
                return original(owner_uid, job_id)

            monkeypatch.setattr(
                daemon_module, "_read_scheduler_submit_binding", refusing
            )
            assert [s.id for s in d._iter_specs()] == ["kept-1"]
            assert [s.id for s in d._iter_specs()] == ["kept-1"]
            assert d._settled_spec_identity == {}
        finally:
            d._queue_lock_fd.close()

    def test_a_duplicate_id_still_quarantines_a_skipped_path(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # A bare id is a compatibility identifier, not a storage identity. A
        # skipped row still registers its filename, so a collision that appears
        # only among skipped rows is still quarantined in full -- an ambiguous
        # record must never become the predecessor a dependency gate accepts.
        uid = os.getuid()
        d = self._make_daemon(tmp_path, monkeypatch)
        try:
            self._write_user_spec(uid, "kept-1", state=JobState.COMPLETED)
            assert [s.id for s in d._iter_specs()] == []
            assert d._settled_spec_paths == {
                "kept-1": paths.user_spec_path(uid, "kept-1")
            }

            other = uid + 1
            self._write_user_spec(other, "kept-1", state=JobState.COMPLETED)
            assert [s.id for s in d._iter_specs()] == []
            assert d._settled_spec_paths == {}
            assert "kept-1" not in d._job_uid

            # ... and it stays quarantined once both sides are skipped rather
            # than read.
            assert [s.id for s in d._iter_specs()] == []
            assert d._settled_spec_paths == {}
            assert "kept-1" not in d._job_uid
        finally:
            d._queue_lock_fd.close()
