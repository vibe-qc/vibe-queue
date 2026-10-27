"""Broker placement for one held job, and restart recovery (#64, B4a).

B2 (:func:`vq.overview.place_broker_job`) ranks hosts for one job from
snapshots; B3 (:mod:`vq.broker_store`) keeps the durable records. This module
joins them into the per-job transaction ``docs/design_broker.md`` describes:

1. Debit every outstanding broker charge from the snapshots, so capacity
   already promised to another held job is not promised twice.
2. Ask B2 for a host. No host means the job stays ``held``.
3. Resolve the chosen target's state root, ``reserve``, then durably enter
   ``transferring`` **before** any remote call.
4. Submit the keyed request (idempotency key = broker job id) and record the
   acceptance; any failure leaves the attempt ``transferring`` with a
   diagnostic, pinned to that target for replay or operator reconciliation.

:meth:`BrokerPlacer.recover` finishes what a crash interrupted: an unstarted
reservation is released and returned to holding, a return is completed, and a
``transferring`` record is replayed with the same key to the same target and
state root only, unless it was cancelled: a cancelled uncertain transfer is
never replayed and reports ``cancel-pending`` for an operator.
:meth:`BrokerPlacer.run_once` is one strict-FIFO pass over the held jobs
(B4b). :mod:`vq.broker_service` wires it into the broker role (B6).
"""

from __future__ import annotations

import math
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Any

from vq import overview
from vq.broker_store import (
    STATE_CANCELLED,
    STATE_HELD,
    STATE_PLACED,
    STATE_RESERVED,
    STATE_RETURNED,
    STATE_TRANSFERRING,
    BrokerRecord,
    BrokerRecordConflict,
    BrokerStore,
    BrokerStoreError,
    BrokerTransferRequest,
)
from vq.spec import validate_job_id

if TYPE_CHECKING:
    from vq.broker_completion import Reranker

Submitter = Callable[[BrokerTransferRequest], str]
"""Deliver one keyed request to its fixed target; return the target job id."""

StateRootOf = Callable[[str], str | None]
"""The state root a host's configured ``remote_vq`` resolves now, or None."""

TargetRefusal = Callable[[str], str | None]
"""Why a host must not be a broker target (checked before any attempt)."""

OUTCOME_HELD = "held"
OUTCOME_PLACED = "placed"
OUTCOME_UNCERTAIN = "uncertain"
OUTCOME_RELEASED = "released"
OUTCOME_CANCEL_PENDING = "cancel-pending"
"""A cancelled job whose transfer may have landed: never replayed; an operator
must reconcile the target (maintainer decision, 2026-09-27)."""


@dataclass(frozen=True)
class PlacementOutcome:
    broker_job_id: str
    outcome: str
    host: str | None = None
    target_job_id: str | None = None
    detail: str | None = None
    blockers: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True)
class PassReport:
    """What one strict-FIFO placement pass did, in order."""

    recovered: tuple[PlacementOutcome, ...] = ()
    settled: tuple[str, ...] = ()
    replayed: tuple[PlacementOutcome, ...] = ()
    placements: tuple[PlacementOutcome, ...] = ()
    head: str | None = None
    """The held job at the front of the queue that could not be placed."""
    waiting: tuple[str, ...] = ()
    """Held jobs behind ``head``, not evaluated: strict FIFO has no backfill."""
    stopped: str | None = None
    """Why the pass stopped before placing anything, or at a job it cannot place."""
    refused: dict[str, str] = field(default_factory=dict)
    """Non-held records a step could not handle; they do not block the queue."""
    skipped: dict[str, str] = field(default_factory=dict)
    """Queued jobs that left the queue legitimately during the pass (cancelled)."""


@dataclass(frozen=True)
class RecoveryReport:
    outcomes: tuple[PlacementOutcome, ...] = ()
    refused: dict[str, str] = field(default_factory=dict)
    """Records that could not be loaded; recovery did not touch them."""


def debit_snapshots(
    snapshots: Sequence[overview.BrokerCapacitySnapshot],
    charges: dict[str, Any],
) -> list[overview.BrokerCapacitySnapshot]:
    """Subtract outstanding broker charges from each host's headroom.

    Unknown headroom stays unknown. A charge can drive headroom negative,
    which B2 reads as insufficient: an over-promised host is never eligible.
    """
    debited = []
    for snapshot in snapshots:
        charge = charges.get(snapshot.host)
        if snapshot.is_scheduler_host:
            # Supported scheduler targets share this broker machine's driver.
            # Debit each unsettled job once across all lanes, including an old
            # target removed from current config. The per-lane cap is already
            # bounded by the driver's global cap. Charging other unresolved
            # direct transfers too is conservative until their next sample.
            slots = sum(item.job_slots for item in charges.values())
            debited.append(replace(
                snapshot, free_jobs=(None if snapshot.free_jobs is None
                                     else snapshot.free_jobs - slots),
            ))
            continue
        if charge is None:
            debited.append(snapshot)
            continue
        debited.append(
            replace(
                snapshot,
                free_cpus=(
                    None if snapshot.free_cpus is None
                    else snapshot.free_cpus - charge.cpus
                ),
                free_mem_mb=(
                    None if snapshot.free_mem_mb is None
                    else snapshot.free_mem_mb - charge.mem_mb
                ),
                free_jobs=(
                    None if snapshot.free_jobs is None
                    else snapshot.free_jobs - charge.job_slots
                ),
            )
        )
    return debited


def _diagnostic(exc: BaseException) -> str:
    text = " ".join(str(exc).split()) or type(exc).__name__
    return f"{type(exc).__name__}: {text}"


class BrokerPlacer:
    """Per-job placement and recovery over one open :class:`BrokerStore`."""

    def __init__(
        self,
        store: BrokerStore,
        *,
        submit: Submitter,
        state_root_of: StateRootOf,
        refuse_target: TargetRefusal = lambda host: None,
        job_refusal: Callable[[BrokerRecord, str], str | None] | None = None,
        clock: Callable[[], float] = time.time,
        rerank: Reranker | None = None,
    ) -> None:
        self.store = store
        # #160: an opt-in policy that orders (or refuses) the hosts B2 admits.
        self._rerank = rerank
        self._submit = submit
        self._state_root_of = state_root_of
        self._refuse_target = refuse_target
        self._job_refusal = job_refusal
        self._clock = clock

    # -- placement ------------------------------------------------------

    def place(
        self,
        broker_job_id: str,
        snapshots: Sequence[overview.BrokerCapacitySnapshot],
        *,
        max_age_seconds: float,
    ) -> PlacementOutcome:
        """Place one ``held`` job now, or leave it held with the reasons.

        Takes the pass lock, so it never runs beside a pass or another
        ``place()``: the charges it debits must not change before it reserves.
        """
        with self.store.pass_lock():
            return self._place_locked(
                broker_job_id, snapshots, max_age_seconds=max_age_seconds
            )

    def _place_locked(
        self,
        broker_job_id: str,
        snapshots: Sequence[overview.BrokerCapacitySnapshot],
        *,
        max_age_seconds: float,
    ) -> PlacementOutcome:
        record = self.store.get(broker_job_id)
        if record.state != STATE_HELD:
            raise BrokerRecordConflict(
                f"broker job {broker_job_id} is {record.state}, not held"
            )
        now = self._clock()
        # Refuses while any record is unusable: an unreadable charge is not
        # free capacity.
        charges = self.store.outstanding_charges()
        debited = debit_snapshots(snapshots, charges)
        decision = overview.place_broker_job(
            debited,
            record.intent.placement_request(),
            now=now,
            max_age_seconds=max_age_seconds,
        )
        if decision.host is not None and self._rerank is not None:
            decision = self._rerank(record, decision, debited, now, max_age_seconds)
        if decision.host is None:
            self.store.record_held_blockers(
                broker_job_id, record.revision, blockers=decision.blockers, now=now,
            )
            return PlacementOutcome(
                broker_job_id, OUTCOME_HELD, blockers=decision.blockers,
                detail="no eligible host can run it now",
            )
        # Walk B2's ranking in order and take the first eligible host that is
        # an allowed target with a known state root (maintainer decision,
        # 2026-09-27): one refused or unreachable host must not hold the
        # queue while a lower-ranked host could run the job.
        skipped: list[tuple[str, str]] = []
        chosen: str | None = None
        target_root: str | None = None
        for rank in decision.ranking:
            host = rank[-1]
            refusal = self._refuse_target(host)
            if refusal is not None:
                skipped.append((host, f"target-refused: {refusal}"))
                continue
            if self._job_refusal is not None:
                refusal = self._job_refusal(record, host)
                if refusal is not None:
                    skipped.append((host, f"job-refused: {refusal}"))
                    continue
            try:
                root = self._state_root_of(host)
            except Exception as exc:  # noqa: BLE001 - an unreachable probe proves nothing
                skipped.append((host, f"state-root-unknown: {_diagnostic(exc)}"))
                continue
            if not isinstance(root, str) or not root.startswith("/"):
                # Without the target's exact state authority there is
                # nothing to pin an idempotent replay to.
                skipped.append((host, "state-root-unknown"))
                continue
            chosen, target_root = host, root
            break
        blockers = tuple(sorted((*decision.blockers, *skipped)))
        if chosen is None or target_root is None:
            self.store.record_held_blockers(
                broker_job_id, record.revision, blockers=blockers, now=now,
            )
            return PlacementOutcome(
                broker_job_id, OUTCOME_HELD, blockers=blockers,
                detail="no eligible host is an allowed target with a known state root: "
                + "; ".join(f"{host}: {reason}" for host, reason in skipped),
            )
        decision = replace(decision, host=chosen, blockers=blockers)
        # Prove the payload before the irreversible step: once `transferring`
        # is written nothing may release the attempt, so a missing or changed
        # payload found afterwards would pin a job that was never sent.
        self.store.verify_payload(record)
        # Record what the decision saw: headroom after outstanding charges.
        snapshot = next(s for s in debited if s.host == decision.host)
        record = self.store.reserve(
            broker_job_id, record.revision, decision=decision, snapshot=snapshot,
            target_state_root=target_root, now=now,
        )
        record = self.store.begin_transfer(
            broker_job_id, record.revision, now=self._clock()
        )
        return self._transfer(record, observed_root=target_root)

    # -- transfer -------------------------------------------------------

    def _transfer(
        self, record: BrokerRecord, *, observed_root: str | None = None
    ) -> PlacementOutcome:
        """Send (or replay) the recorded keyed request to its fixed target.

        ``observed_root`` is the root a fresh placement probed just before
        reserving; a replay probes again.
        """
        # Checked before anything that could fail on the payload, so the
        # operator always sees the kill instruction for a cancelled job.
        pending = self._cancel_pending(record)
        if pending is not None:
            return pending
        request = self.store.transfer_request(record)
        host = request.target_host
        if observed_root is None:
            try:
                observed_root = self._state_root_of(host)
            except Exception as exc:  # noqa: BLE001 - unreachable is not terminal
                message = (
                    f"target {host} state root unavailable ({_diagnostic(exc)}); "
                    "replay held"
                )
                self._diagnose(record, message)
                return PlacementOutcome(
                    record.broker_job_id, OUTCOME_UNCERTAIN, host=host, detail=message
                )
        if observed_root is None:
            message = (
                f"target {host} gave no capacity evidence this pass; replay held "
                "until it answers"
            )
            self._diagnose(record, message)
            return PlacementOutcome(
                record.broker_job_id, OUTCOME_UNCERTAIN, host=host, detail=message
            )
        if observed_root != request.target_state_root:
            # A renamed alias or moved state root must not redirect the
            # replay: idempotency holds only within one state authority.
            message = (
                f"target {host} now resolves state root {observed_root!r}, "
                f"not the recorded {request.target_state_root!r}; replay held "
                "for operator reconciliation"
            )
            self._diagnose(record, message)
            return PlacementOutcome(
                record.broker_job_id, OUTCOME_UNCERTAIN, host=host, detail=message
            )
        # Again at the last moment before sending: the state-root probe can
        # take seconds, and a cancellation committed meanwhile must win.
        pending = self._cancel_pending(self.store.get(record.broker_job_id))
        if pending is not None:
            return pending
        try:
            target_job_id = validate_job_id(self._submit(request))
        except Exception as exc:  # noqa: BLE001 - any failure may have landed
            # An SSH timeout, a lost receipt or even an error exit is not proof
            # the target did not accept the request. Keep the attempt pinned.
            message = f"transfer outcome unknown: {_diagnostic(exc)}"
            self._diagnose(record, message)
            return PlacementOutcome(
                record.broker_job_id, OUTCOME_UNCERTAIN, host=host, detail=message
            )
        placed = self._retry_on_conflict(
            record,
            lambda current: self.store.record_placed(
                current.broker_job_id, current.revision,
                request_fingerprint=request.request_fingerprint,
                target_job_id=target_job_id, now=self._clock(),
            ),
        )
        return PlacementOutcome(
            placed.broker_job_id, OUTCOME_PLACED, host=host, target_job_id=target_job_id
        )

    def _retry_on_conflict(
        self, record: BrokerRecord, apply: Callable[[BrokerRecord], BrokerRecord]
    ) -> BrokerRecord:
        """Apply a transition that must not be lost to a concurrent writer.

        Another process may commit a revision (a cancellation, most often)
        between this pass reading a ``transferring`` record and recording
        what the target said. The evidence is still true, so it is applied to
        the current revision, as long as the record is still that same
        transfer; ``record_placed`` itself re-checks the fingerprint.
        """
        try:
            return apply(record)
        except BrokerRecordConflict:
            current = self.store.get(record.broker_job_id)
            if (
                current.state != STATE_TRANSFERRING
                or current.current_attempt is None
                or record.current_attempt is None
                or current.current_attempt.request_fingerprint
                != record.current_attempt.request_fingerprint
            ):
                raise
            return apply(current)

    def _diagnose(self, record: BrokerRecord, message: str) -> None:
        self._retry_on_conflict(
            record,
            lambda current: self.store.record_transfer_diagnostic(
                current.broker_job_id, current.revision, message=message,
                now=self._clock(),
            ),
        )

    def _cancel_pending(self, record: BrokerRecord) -> PlacementOutcome | None:
        """Cancellation wins over replay (maintainer decision, 2026-09-27).

        A replay could create the job the user cancelled, so nothing is sent.
        The attempt stays pinned, with its charge, until an operator checks
        the target and kills the job there if the first send landed.
        """
        attempt = record.current_attempt
        if record.cancellation is None or attempt is None:
            return None
        return PlacementOutcome(
            record.broker_job_id, OUTCOME_CANCEL_PENDING, host=attempt.target_host,
            detail=(
                "cancelled while its transfer was uncertain; not replayed. "
                f"Check {attempt.target_host} for job key "
                f"{attempt.idempotency_key} and kill it there if it exists"
            ),
        )

    # -- recovery -------------------------------------------------------

    def recover(self) -> RecoveryReport:
        """Finish every transition a crash interrupted, before new placement.

        Unusable records are reported and left alone; recovery never infers
        from missing evidence that a request was not delivered.
        """
        with self.store.pass_lock():
            return self._recover_locked()

    def _recover_locked(self) -> RecoveryReport:
        records, problems = self.store.scan()
        outcomes: list[PlacementOutcome] = []
        for record in records:
            try:
                outcome = self._recover_retrying(record)
            except BrokerStoreError as exc:
                problems[record.broker_job_id] = str(exc)
                continue
            if outcome is not None:
                outcomes.append(outcome)
        return RecoveryReport(outcomes=tuple(outcomes), refused=problems)

    def _recover_retrying(self, record: BrokerRecord) -> PlacementOutcome | None:
        """Recover one record; a concurrent writer's revision is re-read once."""
        try:
            return self._recover_one(record)
        except BrokerRecordConflict:
            # Another process (a cancellation, usually) moved it on between
            # the scan and this transition: act on what it is now.
            return self._recover_one(self.store.get(record.broker_job_id))

    # -- one strict-FIFO pass (B4b) ---------------------------------------

    def run_once(
        self,
        snapshots: Sequence[overview.BrokerCapacitySnapshot],
        *,
        max_age_seconds: float,
        target_jobs: dict[str, frozenset[str]] | None = None,
        retry_interval_seconds: float = 300.0,
    ) -> PassReport:
        """One placement pass: settle, replay, then place in strict FIFO.

        1. An interrupted reservation or return is finished, as in recovery.
        2. A placed job's charge is settled once ``target_jobs[host]`` lists
           its target job id and that host's snapshot was observed after the
           acceptance: from then on the target's own headroom counts it.
        3. An uncertain transfer is replayed at most once per
           ``retry_interval_seconds``, to its fixed target only; a cancelled
           one is never replayed and reports ``cancel-pending``.
        4. Held jobs are tried oldest first (acceptance time, then id). The
           first one that cannot be placed stays at the head and every later
           job waits behind it: no backfill (maintainer decision, 2026-09-27).

        Any unusable record stops the pass before any placement: its charge
        cannot be counted, and its place in the queue is unknown.
        """
        self._check_pass_inputs(snapshots, max_age_seconds, retry_interval_seconds)
        with self.store.pass_lock():
            return self.run_once_locked(
                snapshots, max_age_seconds=max_age_seconds, target_jobs=target_jobs,
                retry_interval_seconds=retry_interval_seconds,
            )

    def _check_pass_inputs(
        self,
        snapshots: Sequence[overview.BrokerCapacitySnapshot],
        max_age_seconds: float,
        retry_interval_seconds: float,
    ) -> None:
        if not math.isfinite(retry_interval_seconds) or retry_interval_seconds < 0:
            raise ValueError("retry interval must be finite and not negative")
        # Refuse bad inputs before any step writes, not first inside place().
        overview._broker_observation_policy(self._clock(), max_age_seconds)
        hosts = [snapshot.host for snapshot in snapshots]
        if len(set(hosts)) != len(hosts):
            raise ValueError("duplicate broker host authority")

    def run_once_locked(
        self,
        snapshots: Sequence[overview.BrokerCapacitySnapshot],
        *,
        max_age_seconds: float,
        target_jobs: dict[str, frozenset[str]] | None = None,
        retry_interval_seconds: float = 300.0,
    ) -> PassReport:
        """:meth:`run_once` for a caller that already holds the pass lock.

        The broker role takes the lock before collecting evidence, so a
        second pass stops before its SSH round.
        """
        self._check_pass_inputs(snapshots, max_age_seconds, retry_interval_seconds)
        records, problems = self.store.scan()
        if problems:
            return PassReport(
                stopped="unusable broker records: " + ", ".join(sorted(problems))
            )
        recovered = []
        refused: dict[str, str] = {}
        skipped: dict[str, str] = {}
        for record in records:
            if record.state in (STATE_RESERVED, STATE_RETURNED):
                try:
                    outcome = self._recover_retrying(record)
                except BrokerStoreError as exc:
                    refused[record.broker_job_id] = str(exc)
                    continue
                if outcome is not None:
                    recovered.append(outcome)
        records = [self.store.get(record.broker_job_id) for record in records]

        now = self._clock()
        # A listing counts only from a snapshot the placement itself would
        # accept: fresh and not future-dated. A target clock that runs ahead
        # must not release a charge its headroom does not yet include.
        observed = {
            snapshot.host: snapshot.observed_at
            for snapshot in snapshots
            if math.isfinite(snapshot.observed_at)
            and 0 <= now - snapshot.observed_at <= max_age_seconds
        }
        settled = []
        for record in records:
            attempt = record.current_attempt
            if (
                record.state != STATE_PLACED
                or attempt is None
                or attempt.charge_released_at is not None
                or target_jobs is None
                or attempt.target_job_id not in target_jobs.get(attempt.target_host, ())
                or attempt.accepted_at is None
                or observed.get(attempt.target_host, float("-inf")) <= attempt.accepted_at
            ):
                continue
            try:
                self.store.settle_charge(
                    record.broker_job_id, record.revision, now=self._clock()
                )
            except BrokerRecordConflict:
                # A concurrent writer (a cancellation, usually) moved it on.
                # Settle the same accepted attempt on its current revision.
                current = self.store.get(record.broker_job_id)
                latest = current.current_attempt
                if (
                    current.state != STATE_PLACED
                    or latest is None
                    or latest.charge_released_at is not None
                    or latest.target_job_id != attempt.target_job_id
                ):
                    continue
                self.store.settle_charge(
                    current.broker_job_id, current.revision, now=self._clock()
                )
            settled.append(record.broker_job_id)

        replayed = []
        now = self._clock()
        for record in records:
            if (
                record.state == STATE_TRANSFERRING
                and now - record.updated_at >= retry_interval_seconds
            ):
                try:
                    replayed.append(
                        self._transfer(self.store.get(record.broker_job_id))
                    )
                except BrokerStoreError as exc:
                    # Pinned and not held, so it must not stall the queue;
                    # recover() reports the same record the same way.
                    refused[record.broker_job_id] = str(exc)

        queue = sorted(
            (record for record in records if record.state == STATE_HELD),
            key=lambda record: (record.created_at, record.broker_job_id),
        )
        placements: list[PlacementOutcome] = []
        for index, record in enumerate(queue):
            waiting = tuple(later.broker_job_id for later in queue[index + 1:])
            try:
                outcome = self._place_locked(
                    record.broker_job_id, snapshots, max_age_seconds=max_age_seconds
                )
            except BrokerStoreError as exc:
                try:
                    state = self.store.get(record.broker_job_id).state
                except BrokerStoreError:
                    state = None
                if state == STATE_CANCELLED:
                    # Cancelled by another process during the pass: not an
                    # error, and not the head.
                    skipped[record.broker_job_id] = "cancelled during the pass"
                    continue
                if state != STATE_HELD:
                    # It left the queue (its transfer began, or the record is
                    # gone) before failing; it is not the head, and later
                    # jobs move up.
                    refused[record.broker_job_id] = str(exc)
                    continue
                # The head cannot be placed at all; under strict FIFO nothing
                # behind it may go first. An operator must resolve it.
                return PassReport(
                    tuple(recovered), tuple(settled), tuple(replayed),
                    tuple(placements), head=record.broker_job_id, waiting=waiting,
                    stopped=f"{record.broker_job_id}: {exc}", refused=refused,
                    skipped=skipped,
                )
            placements.append(outcome)
            if outcome.outcome == OUTCOME_HELD:
                return PassReport(
                    tuple(recovered), tuple(settled), tuple(replayed),
                    tuple(placements), head=record.broker_job_id, waiting=waiting,
                    refused=refused, skipped=skipped,
                )
        return PassReport(
            tuple(recovered), tuple(settled), tuple(replayed), tuple(placements),
            refused=refused, skipped=skipped,
        )

    def _recover_one(self, record: BrokerRecord) -> PlacementOutcome | None:
        if record.state == STATE_RESERVED:
            # The transfer never began: re-placing from fresh snapshots is
            # safer than trusting a reservation made before the crash.
            record = self.store.release_reservation(
                record.broker_job_id, record.revision,
                reason="unstarted reservation released during recovery",
                now=self._clock(),
            )
        if record.state == STATE_RETURNED:
            record = self.store.resume_holding(
                record.broker_job_id, record.revision, now=self._clock()
            )
            return PlacementOutcome(
                record.broker_job_id, OUTCOME_RELEASED, detail=record.state
            )
        if record.state == STATE_TRANSFERRING:
            return self._transfer(record)
        return None


def remote_target_refusal(cfg: Any) -> TargetRefusal:
    """Refuse an unconfigured host or a scheduler lane before any attempt."""

    def refuse(host: str) -> str | None:
        host_cfg = cfg.hosts.get(host)
        if host_cfg is None:
            return f"target {host} is not configured"
        if host_cfg.scheduler not in {"local", "slurm", "pbs"}:
            return f"target {host} scheduler lane is unsupported by broker placement"
        if host_cfg.scheduler in {"slurm", "pbs"}:
            driver = cfg.hosts.get(getattr(host_cfg, "scheduler_driver", None))
            if driver is None or driver.scheduler != "local":
                return f"target {host} scheduler lane requires a daemon driver"
        return None

    return refuse


def remote_submitter(cfg: Any) -> Submitter:
    """Deliver a broker transfer through the existing keyed remote submit.

    Direct hosts use keyed remote submit. Scheduler targets use the recorded local
    driver and its ordinary authenticated named-target submission pipeline.
    Every target must return one job id for the one logical job the key names.
    """
    from vq import submit

    refuse = remote_target_refusal(cfg)

    def deliver(request: BrokerTransferRequest) -> str:
        # Backstop for a config changed after the attempt began; the placer
        # refuses these targets before writing ``transferring``.
        refusal = refuse(request.target_host)
        if refusal is not None:
            raise BrokerStoreError(refusal)
        host_cfg = cfg.hosts[request.target_host]
        if host_cfg.scheduler != "local":
            from vq import broker_target
            return broker_target.submit_scheduler(cfg, request)
        ids = submit.submit_remote(
            host=request.target_host, host_cfg=host_cfg, **request.submit_kwargs
        )
        if len(ids) != 1:
            raise BrokerStoreError(
                f"target returned {len(ids)} job ids for one keyed broker job"
            )
        return ids[0]

    return deliver
