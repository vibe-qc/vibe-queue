"""Run a benchmark plan through the queue and collect its results (#162).

``vq benchmark run PLAN BINDING RUN_DIR`` submits every run of a
``vq.benchmark-plan/1`` in the plan's seeded order and writes what came
back as ``vq.benchmark-results/1``, ready for ``vq benchmark analyze``.

The **binding** (``vq.benchmark-binding/1``) is the operator's half: which
configured host runs each node class, which command runs each variant, and
which directory holds each workload's input. It is kept out of the plan, so
a plan and its results can be shared without a host name in them.

How a run is carried out:

* its payload is a copy of the workload's directory, whose content must match
  the plan's ``input_digest``;
* it is submitted with ``vq submit HOST --idempotency-key`` (the campaign
  controller's queue, so a restart resends under the same key instead of
  submitting twice), with the plan layout's cores and the variant's command
  followed by the workload's arguments;
* runs on one node class never overlap: the next is submitted only after
  the previous one finished, so no run competes with another for its node;
* a finished job's ``vq-benchmark-run.json`` report gives the timings, check
  values, the runtime it ran and the CPU model, and ``vq status`` gives its
  measured peak RSS and CPU time.

Nothing is invented. A completed job without a valid report is left
missing, which the analysis lists as missing. A job that failed, ran out of
memory or time, or was killed is recorded with that status and the planned
runtime digest, which the analysis never compares for a run that did not
complete. Nothing runs unless an operator starts it.
"""

from __future__ import annotations

import json
import os
import secrets
import shutil
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Annotated, Any, Literal, cast

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from vq import benchmark as bm
from vq.campaign import (
    CampaignError,
    JobRef,
    Node,
    _canonical_digest,
    _real_directory,
    safe_relative_path,
    source_digest,
)
from vq.campaign_controller import (
    CliQueue,
    FetchMissing,
    Queue,
    Transient,
    _copy,
    _fsync_dir,
    _remove,
    _write_atomic,
    locked,
)
from vq.host import is_local_host

BINDING_SCHEMA = "vq.benchmark-binding/1"
JOURNAL_SCHEMA = "vq.benchmark-journal/1"
REPORT_SCHEMA = "vq.benchmark-run-report/1"
REPORT_NAME = "vq-benchmark-run.json"
MAX_REPORT_BYTES = 256 * 1024

HostName = Annotated[str, Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")]
Arg = Annotated[str, Field(max_length=4096)]


class RunnerError(ValueError):
    """The plan, binding or run directory cannot be used as they stand."""


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, allow_inf_nan=False)


class NodeClassBinding(_Model):
    #: A configured host or scheduler lane; unset runs on this machine.
    host: HostName | None = None


class VariantBinding(_Model):
    command: list[Arg] = Field(min_length=1, max_length=64)
    program: Annotated[str, Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")] | None = None


class WorkloadBinding(_Model):
    #: A directory relative to the binding file, pinned by the plan's input_digest.
    sources: str
    args: list[Arg] = Field(default_factory=list, max_length=128)


class Binding(_Model):
    schema_name: Literal["vq.benchmark-binding/1"]
    plan_sha256: bm.Digest
    node_classes: dict[bm.Name, NodeClassBinding]
    variants: dict[bm.Name, VariantBinding]
    workloads: dict[bm.Name, WorkloadBinding]
    mem_mb: Annotated[int, Field(ge=1)] | None = None
    wall_time_seconds: Annotated[int, Field(ge=1)] | None = None


class RunReport(_Model):
    """What a benchmark job writes as ``vq-benchmark-run.json`` in its workspace."""

    schema_name: Literal["vq.benchmark-run-report/1"]
    runtime_digest: bm.Digest
    converged: bool = True
    steady_seconds: Annotated[float, Field(gt=0)]
    startup_seconds: bm.NonNegative | None = None
    scf_iterations: Annotated[int, Field(ge=0)] | None = None
    values: dict[bm.Name, float] = Field(default_factory=dict, max_length=32)
    cpu_model: bm.Text | None = None
    mpi_ranks: Annotated[int, Field(ge=1)] | None = None
    threads_per_rank: Annotated[int, Field(ge=1)] | None = None


RunState = Literal["submitting", "accepted", "recorded", "missing"]


class JournalEntry(_Model):
    sequence: Annotated[int, Field(ge=0)]
    state: RunState
    job: JobRef | None = None
    detail: str | None = None


class Journal(_Model):
    schema_name: Literal["vq.benchmark-journal/1"]
    plan_sha256: bm.Digest
    #: One campaign instance; absent in legacy journals whose keys use only the plan.
    run_id: bm.Digest | None = None
    runs: list[JournalEntry] = Field(default_factory=list)


def load_binding(path: Path) -> Binding:
    try:
        return Binding.model_validate_json(path.read_bytes())
    except (OSError, ValidationError) as exc:
        raise RunnerError(f"{path}: {exc}") from None


def check_binding(plan: bm.Plan, binding: Binding, base: Path,
                  hosts: frozenset[str] = frozenset()) -> list[str]:
    """Why the binding cannot run this plan; empty when it can."""
    problems = []
    if binding.plan_sha256 != bm.plan_digest(plan):
        problems.append(f"the binding is for plan {binding.plan_sha256[:12]}, not this one "
                        f"({bm.plan_digest(plan)[:12]})")
    for kind, planned, bound in (
        ("node class", plan.node_classes, binding.node_classes),
        ("variant", [v.name for v in plan.variants], binding.variants),
        ("workload", [w.name for w in plan.workloads], binding.workloads),
    ):
        problems += [f"the binding has no {kind} {name}" for name in planned if name not in bound]
        problems += [f"the binding names {kind} {name}, which the plan does not"
                     for name in bound if name not in planned]
    for name, node_class in binding.node_classes.items():
        if (node_class.host is not None and not is_local_host(node_class.host)
                and node_class.host not in hosts):
            problems.append(f"node class {name}: host {node_class.host} is neither this "
                            "machine nor a configured host")
    for workload in plan.workloads:
        inputs = binding.workloads.get(workload.name)
        if inputs is None:
            continue
        try:
            actual = source_digest(_sources(base, inputs.sources))
        except (CampaignError, RunnerError, OSError) as exc:
            problems.append(f"workload {workload.name}: {exc}")
            continue
        if actual != workload.input_digest:
            problems.append(f"workload {workload.name}: {inputs.sources} has sha256 {actual}, "
                            f"the plan pins {workload.input_digest}")
    return problems


def _sources(base: Path, relative: str) -> Path:
    """A workload's input directory, refusing a symbolic link on the way."""
    try:
        return _real_directory(base, safe_relative_path(relative))
    except FileNotFoundError:
        raise RunnerError(f"{base / relative} does not exist") from None


#: How a finished job's queue state reads as a benchmark run status.
_RUN_STATUS = {"oom_killed": "oom_killed", "time_exceeded": "time_exceeded",
               "killed": "killed"}


@dataclass
class Runner:
    """One plan's runner over one run directory. Hold :func:`locked` around it."""

    plan: bm.Plan
    binding: Binding
    base: Path
    run_dir: Path
    queue: Queue
    log: Callable[[str], None] = lambda _message: None
    problems: dict[int, str] = field(default_factory=dict)
    transient: dict[int, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.digest = bm.plan_digest(self.plan)
        self.order = bm.run_order(self.plan)
        path = self.run_dir / "journal.json"
        if path.exists():
            self.journal = Journal.model_validate_json(path.read_bytes())
            if self.journal.plan_sha256 != self.digest:
                raise RunnerError("the run directory belongs to another plan; a changed plan "
                                  "starts a new run directory")
        else:
            self.journal = Journal.model_validate(
                {"schema_name": JOURNAL_SCHEMA, "plan_sha256": self.digest,
                 "run_id": secrets.token_hex(32)})
        results = self.run_dir / "results.json"
        self.results = (bm.load_results(results) if results.exists()
                        else bm.Results.model_validate(
                            {"schema_name": bm.RESULTS_SCHEMA, "plan_sha256": self.digest,
                             "runs": []}))
        if self.results.plan_sha256 != self.digest:
            raise RunnerError(
                f"results.json belongs to plan {self.results.plan_sha256[:12]}, not this one "
                f"({self.digest[:12]}); restore this campaign's matching results before resuming"
            )
        # Results may precede their journal checkpoint after an interrupted
        # save, but a recorded run must never lose its measurement silently.
        recorded = {entry.sequence for entry in self.journal.runs if entry.state == "recorded"}
        result_keys = {run.key for run in self.results.runs}
        missing = [
            str(planned["sequence"]) for planned in self.order
            if planned["sequence"] in recorded
            and (planned["workload"], planned["variant"], planned["node_class"], planned["block"])
            not in result_keys
        ]
        if missing:
            raise RunnerError(
                f"results.json is missing recorded run(s) {', '.join(missing)}; "
                "restore this campaign's matching results before resuming"
            )

    # --- records -------------------------------------------------------------

    def _entry(self, sequence: int) -> JournalEntry | None:
        return next((e for e in self.journal.runs if e.sequence == sequence), None)

    def _save(self) -> None:
        self.run_dir.mkdir(parents=True, exist_ok=True)
        # Persist samples before marking them recorded. If the journal write
        # is interrupted, recovery recollects the accepted job and replaces
        # its existing row by run key instead of losing or duplicating it.
        _write_atomic(self.run_dir / "results.json",
                      json.dumps(self.results.model_dump(mode="json"), indent=2,
                                 sort_keys=True) + "\n")
        _write_atomic(self.run_dir / "journal.json",
                      json.dumps(self.journal.model_dump(mode="json", exclude_none=True), indent=2,
                                 sort_keys=True) + "\n")

    def _set(self, sequence: int, **fields: Any) -> JournalEntry:
        current = self._entry(sequence)
        data = {**(current.model_dump(mode="json") if current else {"sequence": sequence}),
                **fields}
        entry = JournalEntry.model_validate(data)
        self.journal.runs = [e for e in self.journal.runs if e.sequence != sequence] + [entry]
        self.journal.runs.sort(key=lambda e: e.sequence)
        self._save()
        return entry

    def key(self, sequence: int) -> str:
        """One run's key in this campaign; stable across restarts and directory moves."""
        identity: dict[str, object] = {
            "schema": JOURNAL_SCHEMA, "plan_sha256": self.digest, "sequence": sequence,
        }
        # Preserve keys for an already-started legacy campaign, including a
        # submission whose acknowledgement was lost before its job was recorded.
        if self.journal.run_id is not None:
            identity["run_id"] = self.journal.run_id
        return _canonical_digest(identity)

    # --- one run ---------------------------------------------------------------

    def _node(self, planned: dict[str, Any]) -> Node:
        variant = self.binding.variants[str(planned["variant"])]
        workload = self.binding.workloads[str(planned["workload"])]
        host = self.binding.node_classes[str(planned["node_class"])].host
        return Node.model_validate({
            "id": f"run{planned['sequence']}", "kind": "other", "host": host,
            "program": variant.program, "cpus": self.plan.layout.cores,
            "mem_mb": self.binding.mem_mb, "wall_time_seconds": self.binding.wall_time_seconds,
            "command": [*variant.command, *workload.args],
        })

    def _payload(self, planned: dict[str, Any]) -> Path:
        """The workload's input, copied once and checked against its pin."""
        payload = self.run_dir / "payloads" / str(planned["sequence"])
        workload = next(w for w in self.plan.workloads if w.name == planned["workload"])
        if not payload.is_dir():
            staging = payload.with_name(f".{payload.name}.staging")
            _remove(staging)
            staging.mkdir(parents=True)
            source = _sources(self.base, self.binding.workloads[workload.name].sources)
            for child in sorted(source.iterdir()):
                _copy(child, staging / child.name)
            os.replace(staging, payload)
            _fsync_dir(payload.parent)
        if source_digest(payload) != workload.input_digest:
            raise CampaignError(f"the staged input of run {planned['sequence']} does not match "
                                f"the plan's input_digest {workload.input_digest[:12]}")
        return payload

    def _submit(self, planned: dict[str, Any]) -> None:
        sequence = cast(int, planned["sequence"])
        payload = self._payload(planned)
        if self._entry(sequence) is None:
            self._set(sequence, state="submitting")
        name = f"{self.plan.plan_id}.{sequence}"[:50]
        job = self.queue.submit(self._node(planned), payload, self.key(sequence), name)
        self._set(sequence, state="accepted", job=job.model_dump(mode="json"))
        self.log(f"run {sequence} ({planned['variant']} on {planned['workload']}, "
                 f"{planned['node_class']} block {planned['block']}): job {job.job_id}")

    def _report(self, job: JobRef, sequence: int) -> RunReport | None:
        incoming = self.run_dir / "incoming" / str(sequence)
        _remove(incoming)
        incoming.mkdir(parents=True)
        try:
            path = self.queue.fetch(job, REPORT_NAME, incoming)
            data = path.read_bytes()[:MAX_REPORT_BYTES + 1]
            if len(data) > MAX_REPORT_BYTES:
                raise RunnerError(f"{REPORT_NAME} is larger than {MAX_REPORT_BYTES} bytes")
            return RunReport.model_validate_json(data)
        except FetchMissing:
            return None
        finally:
            shutil.rmtree(incoming, ignore_errors=True)

    def _finish(self, planned: dict[str, Any], entry: JournalEntry) -> bool:
        assert entry.job is not None
        seen = self.queue.observe(entry.job)
        if seen.state == "unknown":
            raise CampaignError(f"job {entry.job.job_id}: {seen.detail or 'state unknown'}")
        if seen.state in {"accepted", "running"}:
            return False
        sequence = cast(int, planned["sequence"])
        variant = self.plan.variant(str(planned["variant"]))
        layout = self.plan.layout
        run: dict[str, Any] = {
            "workload": planned["workload"], "variant": variant.name,
            "node_class": planned["node_class"], "block": planned["block"],
            "runtime_digest": variant.runtime_digest,
            "hardware": {"mpi_ranks": layout.mpi_ranks,
                         "threads_per_rank": layout.threads_per_rank},
        }
        if seen.state == "succeeded":
            try:
                report = self._report(entry.job, sequence)
            except (RunnerError, ValidationError, OSError) as exc:
                report, detail = None, f"its {REPORT_NAME} is not usable: {exc}"
            else:
                detail = f"the job wrote no {REPORT_NAME}"
            if report is None:
                self._set(sequence, state="missing", detail=detail)
                self.log(f"run {sequence}: left missing, {detail}")
                return True
            run.update(
                status="completed" if report.converged else "unconverged",
                runtime_digest=report.runtime_digest,
                steady_seconds=report.steady_seconds,
                startup_seconds=report.startup_seconds,
                scf_iterations=report.scf_iterations,
                values=report.values,
                hardware={"cpu_model": report.cpu_model,
                          "mpi_ranks": report.mpi_ranks or layout.mpi_ranks,
                          "threads_per_rank": report.threads_per_rank or layout.threads_per_rank},
            )
        else:
            run["status"] = _RUN_STATUS.get(seen.job_state or "", "failed")
        usage = seen.usage or {}
        if usage.get("status") == "ok":
            run["peak_rss_mb"] = usage.get("peak_rss_mb")
            run["cpu_seconds"] = usage.get("active_cpu_seconds")
        record = bm.Run.model_validate({k: v for k, v in run.items() if v is not None})
        self.results.runs = [r for r in self.results.runs if r.key != record.key] + [record]
        self._set(sequence, state="recorded", detail=None)
        self.log(f"run {sequence}: {record.status}"
                 + (f", {record.steady_seconds:.3f} s steady" if record.steady_seconds else ""))
        return True

    # --- passes ----------------------------------------------------------------

    def step(self) -> bool:
        """One pass: finish what ended, then start each node class's next run."""
        self.transient.clear()
        progressed = False
        busy: set[str] = set()
        for planned in self.order:
            sequence, node_class = cast(int, planned["sequence"]), str(planned["node_class"])
            entry = self._entry(sequence)
            if sequence in self.problems:
                # An error does not establish that this run stopped. Keep
                # its class isolated on later passes while other classes move.
                busy.add(node_class)
                continue
            if entry and entry.state in {"recorded", "missing"}:
                continue
            if node_class in busy:
                continue
            busy.add(node_class)  # this class's earliest unfinished run, and only it
            try:
                if entry is None or entry.state == "submitting":
                    self._submit(planned)
                    progressed = True
                elif self._finish(planned, entry):
                    progressed = True
                    busy.discard(node_class)
            except Transient as exc:
                self.transient[sequence] = str(exc)
                self.log(f"run {sequence}: {exc}; asking again next pass")
            except (CampaignError, RunnerError, OSError, ValueError) as exc:
                self.problems[sequence] = str(exc)
                self.log(f"run {sequence}: {exc}")
        return progressed

    def pending(self) -> int:
        done = {e.sequence for e in self.journal.runs if e.state in {"recorded", "missing"}}
        return sum(1 for planned in self.order
                   if planned["sequence"] not in done
                   and planned["sequence"] not in self.problems)

    def run(self, *, once: bool = False, interval: float = 30.0,
            sleep: Callable[[float], None] = time.sleep) -> None:
        """Pass until every run is recorded or nothing more can move."""
        while True:
            progressed = self.step()
            if once or not self.pending() or (not progressed and not self._in_flight()
                                               and not self.transient):
                return
            if not progressed:
                sleep(interval)

    def _in_flight(self) -> bool:
        return any(e.state == "accepted" and e.sequence not in self.problems
                   for e in self.journal.runs)


def open_runner(plan_file: Path, binding_file: Path, run_dir: Path, *,
                hosts: frozenset[str], queue: Queue | None = None,
                log: Callable[[str], None] = lambda _m: None) -> Runner:
    plan = bm.load_plan(plan_file)
    binding = load_binding(binding_file)
    base = binding_file.resolve().parent
    problems = check_binding(plan, binding, base, hosts)
    if problems:
        raise RunnerError("this binding cannot run the plan:\n  " + "\n  ".join(problems))
    return Runner(plan, binding, base, run_dir, queue or CliQueue(), log=log)


__all__ = ["Binding", "RunReport", "Runner", "RunnerError", "check_binding", "load_binding",
           "locked", "open_runner"]
