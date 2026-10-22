"""#22: `vq cleanup --archive` bounds the daemon's admission scan.

Archiving tars a terminal job's workspace away, removes it, and stamps the spec
with ``archived_at``. Before this change the spec file stayed in the queue
directory and the daemon's own scan kept opening, parsing and model-validating
it on every pass, so archiving every retained job changed admission cost by
nothing: the only way to bound the scan was ``--delete``, i.e. discarding job
history outright.

What these tests pin:

* the per-tick scan's file opens are proportional to the **non-archived** row
  count, not the retained total;
* nothing about an archived row's retrievability changes -- the operator
  listing, ``vq cleanup``, and a direct read by id all still see it;
* the rows that must stay on the scan do: the startup walk still sees every
  row, an archived row holding an open scheduler transaction is never skipped,
  and a ``--restore`` puts the row straight back.

The first test fails on the parent commit, where the second pass opens every
retained spec.

Later in v0.26.10 the same skip was widened from archived rows to every settled
terminal row (``tests/test_admission_scan_settled_v0_26_10.py``). Archived rows
are now a subset of that, so every claim here still holds; the open counts below
are lower than they were, because the unarchived terminal rows left the scan
too.
"""
from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path

import pytest

from vq import cgroup, cleanup, config, listing, paths
from vq.admission import iter_unmet_dependencies
from vq.daemon import Daemon
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
    """Counts reads of queue-directory spec files.

    ``JobSpec.read`` is ``path.read_text()``, so patching ``Path.read_text`` is
    the file open itself rather than a proxy for it.
    """

    def __init__(self, queue_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        self.queue_dir = queue_dir
        self.paths: list[Path] = []
        original = Path.read_text

        def counting_read_text(inner_self: Path, *args: object, **kwargs: object):
            if (
                inner_self.suffix == ".json"
                and inner_self.parent.name == queue_dir.name
            ):
                self.paths.append(inner_self)
            return original(inner_self, *args, **kwargs)  # type: ignore[arg-type]

        monkeypatch.setattr(Path, "read_text", counting_read_text)

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


class TestScanCostFollowsLiveRows:
    def test_steady_state_opens_only_unsettled_specs(
        self, daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        live = [f"live-{i:03d}" for i in range(3)]
        retained = [f"kept-{i:03d}" for i in range(5)]
        archived = [f"arch-{i:03d}" for i in range(40)]
        for jobid in live:
            _write_spec(daemon, jobid, state=JobState.PENDING)
        for jobid in retained:
            _write_spec(daemon, jobid, state=JobState.COMPLETED)
        for jobid in archived:
            _write_spec(daemon, jobid, state=JobState.COMPLETED, archived=True)

        counter = _OpenCounter(daemon.queue_dir, monkeypatch)

        # Pass one still reads everything: an archived row has to be read once
        # before the daemon can know it is archived.
        first = [s.id for s in daemon._iter_specs()]
        first_opens = counter.count
        # Pass two and three are the steady state this issue is about.
        counter.reset()
        second = [s.id for s in daemon._iter_specs()]
        second_opens = counter.count
        counter.reset()
        list(daemon._iter_specs())
        third_opens = counter.count

        # The measurement, asserted first so it is what fails on the parent
        # commit (there, every pass reopens all 48 retained rows). ``retained``
        # is terminal but unarchived, so the widened rule takes it too.
        assert first_opens == len(live) + len(retained) + len(archived)
        assert second_opens == len(live)
        assert third_opens == len(live)
        # The scan's contents: a settled row is off it from the first pass.
        assert sorted(first) == sorted(live)
        assert sorted(second) == sorted(live)

    def test_dispatch_tick_does_not_reopen_archived_rows(
        self, daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_spec(daemon, "live-1", state=JobState.PENDING)
        for i in range(20):
            _write_spec(
                daemon, f"arch-{i:03d}", state=JobState.COMPLETED, archived=True
            )
        started: list[str] = []
        monkeypatch.setattr(
            Daemon, "_start_job", lambda self, spec: bool(started.append(spec.id))
        )
        daemon._dispatch_pending()
        counter = _OpenCounter(daemon.queue_dir, monkeypatch)
        daemon._dispatch_pending()
        # The tick re-reads the row it is about to start under its lock, so the
        # count is per-live-row rather than exactly one; what matters is that no
        # archived row is among them.
        assert {p.stem for p in counter.paths} == {"live-1"}
        assert started == ["live-1", "live-1"]

    def test_deleting_a_spec_retires_its_proof(
        self, daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The skip set must track the queue, not grow with it forever.
        for i in range(4):
            _write_spec(
                daemon, f"arch-{i:03d}", state=JobState.COMPLETED, archived=True
            )
        list(daemon._iter_specs())
        assert len(daemon._settled_spec_identity) == 4
        (daemon.queue_dir / "arch-000.json").unlink()
        list(daemon._iter_specs())
        assert len(daemon._settled_spec_identity) == 3


class TestArchivedRowsStayRetrievable:
    def test_listing_cleanup_and_direct_read_are_unchanged(
        self, daemon: Daemon
    ) -> None:
        _write_spec(daemon, "live-1", state=JobState.PENDING)
        _write_spec(daemon, "arch-1", state=JobState.COMPLETED, archived=True)
        # Two scans, so the archived row is skipped from the daemon's cache
        # rather than merely filtered.
        list(daemon._iter_specs())
        list(daemon._iter_specs())

        listed = {s.id for s in listing.list_jobs("localhost", daemon.queue_dir)}
        assert listed == {"live-1", "arch-1"}

        candidates = cleanup.find_candidates(queue_dir=daemon.queue_dir)
        assert {c.spec.id for c in candidates} == {"arch-1"}
        restorable = cleanup.find_candidates(
            queue_dir=daemon.queue_dir, require_archived=True
        )
        assert {c.spec.id for c in restorable} == {"arch-1"}

        direct = JobSpec.read(daemon.queue_dir / "arch-1.json")
        assert direct.id == "arch-1"
        assert direct.is_archived
        assert direct.state is JobState.COMPLETED

    def test_a_dependent_on_an_archived_predecessor_still_dispatches(
        self, daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # A missing predecessor is deliberately read as "not finished yet", so
        # an archived predecessor left off the scan would park its dependent
        # forever. The dispatch tick reads back exactly the rows it names.
        _write_spec(daemon, "pred-1", state=JobState.COMPLETED, archived=True)
        _write_spec(daemon, "dep-1", state=JobState.PENDING, depends_on=["pred-1"])
        started: list[str] = []
        monkeypatch.setattr(
            Daemon, "_start_job", lambda self, spec: bool(started.append(spec.id))
        )
        daemon._dispatch_pending()
        daemon._dispatch_pending()
        assert started == ["dep-1", "dep-1"]

        # And the resolved index is the real predecessor row, not a stand-in.
        specs_by_id: dict[str, JobSpec] = {}
        dependent = JobSpec.read(daemon.queue_dir / "dep-1.json")
        daemon._resolve_settled_dependencies([dependent], specs_by_id)
        assert specs_by_id["pred-1"].state is JobState.COMPLETED
        assert list(iter_unmet_dependencies(dependent, specs_by_id)) == []

    def test_restoring_a_row_puts_it_back_on_the_read(
        self, daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_spec(daemon, "arch-1", state=JobState.COMPLETED, archived=True)
        assert [s.id for s in daemon._iter_specs()] == []
        assert [s.id for s in daemon._iter_specs()] == []
        spec_path = daemon.queue_dir / "arch-1.json"
        counter = _OpenCounter(daemon.queue_dir, monkeypatch)
        list(daemon._iter_specs())
        assert counter.count == 0

        fresh = JobSpec.read(spec_path)
        fresh.archived_at = None
        fresh.archive_path = None
        fresh.write(spec_path)

        # `--restore` rewrites the row, so its on-disk identity changes, the
        # proof is retired and the next pass reads it in full. The fresh read
        # is what then decides: this row is still terminal, so it stays off the
        # scan -- but on the strength of what is on disk now, not of a stamp
        # recorded before the operator touched it.
        counter.reset()
        assert [s.id for s in daemon._iter_specs()] == []
        assert counter.paths == [spec_path]


class TestRowsThatMustStayOnTheScan:
    def test_the_startup_walk_still_sees_every_row(
        self, daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # `vq cleanup --archive` gates on terminal state alone, so an archived
        # row can still own a leaked process group; the startup walk reaps it.
        _write_spec(daemon, "arch-1", state=JobState.KILLED, archived=True)
        assert [s.id for s in daemon._iter_specs(include_settled=True)] == ["arch-1"]
        assert [s.id for s in daemon._iter_specs(include_settled=True)] == ["arch-1"]
        counter = _OpenCounter(daemon.queue_dir, monkeypatch)
        daemon._reattach_or_interrupt_at_startup()
        assert counter.count == 1
        # The walk warmed the skip set for the per-tick passes behind it.
        counter.reset()
        assert [s.id for s in daemon._iter_specs()] == []
        assert counter.count == 0

    @pytest.mark.parametrize(
        "scheduler_state",
        [
            "submit_outcome_unknown_after_terminal",
            "submit_evidence_conflict_after_terminal",
            "submit_cancel_pending_after_terminal",
            "reattach_failed",
        ],
    )
    def test_an_open_scheduler_transaction_is_never_skipped(
        self, daemon: Daemon, scheduler_state: str
    ) -> None:
        # Archiving is an age decision an operator can take over an ambiguous
        # submit. Its slot must keep being reserved until remote evidence or an
        # operator resolves it, so the row stays on the scan however old it is.
        _write_spec(
            daemon,
            "fenced-1",
            state=JobState.FAILED,
            archived=True,
            scheduler_state=scheduler_state,
            scheduler_target="cluster",
        )
        assert [s.id for s in daemon._iter_specs()] == ["fenced-1"]
        assert [s.id for s in daemon._iter_specs()] == ["fenced-1"]
        assert daemon._settled_spec_identity == {}

    def test_a_non_terminal_row_carrying_archived_at_is_never_skipped(
        self, daemon: Daemon
    ) -> None:
        # `cleanup.archive_workspace` refuses a non-terminal job, so this can
        # only come from a hand-edited or damaged row. Re-check the precondition
        # against what is on disk rather than trusting the stamp.
        _write_spec(daemon, "odd-1", state=JobState.RUNNING, archived=True)
        assert [s.id for s in daemon._iter_specs()] == ["odd-1"]
        assert [s.id for s in daemon._iter_specs()] == ["odd-1"]


class TestMultiUserQueues:
    def _make_daemon(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> Daemon:
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
        self, uid: int, jobid: str, *, state: JobState, archived: bool
    ) -> None:
        paths.provision_user_state(uid, os.getgid())
        ws = paths.user_workspace_dir(uid, jobid)
        ws.mkdir(parents=True, exist_ok=True)
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
        if archived:
            spec.archived_at = utcnow_iso()
            spec.archive_path = str(ws) + ".tar.bz2"
        spec.write(paths.user_spec_path(uid, jobid))

    def test_archived_rows_leave_the_per_user_scan_too(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        uid = os.getuid()
        d = self._make_daemon(tmp_path, monkeypatch)
        try:
            self._write_user_spec(
                uid, "live-1", state=JobState.PENDING, archived=False
            )
            for i in range(6):
                self._write_user_spec(
                    uid, f"arch-{i:03d}", state=JobState.COMPLETED, archived=True
                )
            assert [s.id for s in d._iter_specs()] == ["live-1"]
            assert [s.id for s in d._iter_specs()] == ["live-1"]
            # A skipped row is not an admitted row: it must not claim an owner
            # mapping that the resolver would then trust.
            assert set(d._job_uid) == {"live-1"}
            assert sorted(d._settled_spec_paths) == [
                f"arch-{i:03d}" for i in range(6)
            ]
            # ... and the startup walk still sees all seven.
            assert len(list(d._iter_specs(include_settled=True))) == 7
        finally:
            d._queue_lock_fd.close()
