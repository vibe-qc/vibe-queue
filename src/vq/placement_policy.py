"""Explainable completion-time placement across daemon hosts and scheduler lanes (#160).

A read-only planner. Given normalized target capability records and one
job's requirements it first filters hard requirements, then ranks the
targets that remain by predicted completion time::

    staging + queue wait + execution + result transfer

Every component is an interval with its source, or explicitly unknown.
A rejected target lists every reason it was rejected; a target whose
essential capability is stale or unknown is rejected, never chosen as
the least bad of the ineligible. With no eligible target the job is held
with those reasons.

Nothing here reads the network or the clock, submits, or reserves. The
broker's FIFO pass and ``vq submit auto`` are unchanged; this policy is
exposed only through ``vq placement explain``.
"""

from __future__ import annotations

import json
import math
from datetime import datetime
from pathlib import Path
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

FLEET_SCHEMA = "vq.placement-fleet/1"
REQUEST_SCHEMA = "vq.placement-request/1"
EXPLANATION_SCHEMA = "vq.placement-explanation/1"
POLICY = "vq.completion-time/1"

MAX_FILE_BYTES = 16 * 1024 * 1024
#: A capability observed further in the future than this is clock skew,
#: not data, and is refused like stale data.
FUTURE_SKEW_SECONDS = 60.0

Name = Annotated[str, Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._+/-]{0,127}$")]
Label = Annotated[str, Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._+-]{0,63}$")]
Digest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
Positive = Annotated[float, Field(gt=0)]
NonNegative = Annotated[float, Field(ge=0)]
Count = Annotated[int, Field(ge=1)]

#: Components of the predicted completion time, in the order they happen.
COMPONENTS = ("staging", "queue_wait", "execution", "transfer")


class PlacementError(ValueError):
    """The fleet or request cannot be planned as they stand."""


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, allow_inf_nan=False)


class Interval(_Model):
    low: NonNegative
    high: NonNegative

    @model_validator(mode="after")
    def _ordered(self) -> Interval:
        if self.high < self.low:
            raise ValueError("an interval's high must not be below its low")
        return self


class WaitModel(Interval):
    """A lane's expected queue wait and where that expectation came from.

    Bounded history or an administrator's figure. A scheduler's own start
    estimate is advisory and is labelled as such; nothing submits a probe
    job to find out.
    """

    source: Literal["history", "declared", "scheduler-estimate"]


class NodeShape(_Model):
    """One compute node of the target, not the cluster or its login node."""

    cpus: Count | None = None
    mem_mb: Count | None = None
    #: Scratch available to one job per node, including any applicable quota.
    scratch_mb: Annotated[int, Field(ge=0)] | None = None


class FreeCapacity(_Model):
    """What a daemon host could start right now."""

    cpus: Annotated[int, Field(ge=0)] | None = None
    mem_mb: Annotated[int, Field(ge=0)] | None = None
    scratch_mb: Annotated[int, Field(ge=0)] | None = None


class Target(_Model):
    name: Name
    kind: Literal["daemon", "scheduler"]
    #: When this record was observed (ISO 8601 with a zone offset).
    observed_at: Annotated[str, Field(min_length=10, max_length=64)]
    reachable: bool
    maintenance: bool = False
    #: The compute node's operating system; absent is unknown.
    os: Label | None = None
    arch: Label | None = None
    isa_levels: list[Label] = Field(default_factory=list, max_length=32)
    #: Runtime digests per program; the first listed is the lane's current one.
    programs: dict[Label, list[Digest]] = Field(default_factory=dict, max_length=256)
    #: Accounts allowed to submit; absent is unknown, an empty list permits none.
    accounts: list[Label] | None = None
    #: Data-access labels available to a job; absent is unknown, empty means none.
    data_access: list[Label] | None = Field(default=None, max_length=256)
    #: License eligibility for the execution identity; absent is unknown.
    licenses: list[Label] | None = Field(default=None, max_length=256)
    node: NodeShape
    max_nodes: Count = 1
    max_walltime_seconds: Count | None = None
    #: Positive evidence of no walltime cap; an absent limit alone is unknown.
    walltime_unlimited: bool = False
    free: FreeCapacity | None = None
    queue_wait_seconds: WaitModel | None = None
    #: Execution speed relative to the request's reference estimate.
    speed_factor: Positive = 1.0
    speed_source: Literal["benchmark", "declared", "assumed"] = "assumed"
    staging_mb_per_second: Positive | None = None
    transfer_mb_per_second: Positive | None = None

    @model_validator(mode="after")
    def _consistent(self) -> Target:
        if self.walltime_unlimited and self.max_walltime_seconds is not None:
            raise ValueError(f"target {self.name}: walltime cannot be both limited and unlimited")
        if self.kind == "daemon" and self.max_nodes != 1:
            raise ValueError(f"target {self.name}: a daemon host is one node")
        if self.kind == "scheduler" and self.free is not None:
            raise ValueError(
                f"target {self.name}: a scheduler lane has no free-now capacity; "
                "model its queue wait instead"
            )
        _parse_time(self.observed_at, f"target {self.name} observed_at")
        return self


class Fleet(_Model):
    schema_name: Literal["vq.placement-fleet/1"]
    targets: list[Target] = Field(min_length=1, max_length=1024)

    @model_validator(mode="after")
    def _unique(self) -> Fleet:
        names = [t.name for t in self.targets]
        if len(set(names)) != len(names):
            raise ValueError("each target name may appear once")
        return self


class Request(_Model):
    """One job's hard requirements and the estimates its ranking uses."""

    schema_name: Literal["vq.placement-request/1"]
    program: Label
    #: An explicitly selected runtime is never substituted.
    runtime_digest: Digest | None = None
    os: Label | None = None
    arch: Label | None = None
    isa_level: Label | None = None
    account: Label | None = None
    required_data_access: list[Label] = Field(default_factory=list, max_length=256)
    required_licenses: list[Label] = Field(default_factory=list, max_length=256)
    cpus: Count
    nodes: Count = 1
    #: Optional explicit topology: each whole rank uses this many CPUs on one node.
    mpi_ranks: Count | None = None
    threads_per_rank: Count | None = None
    mem_mb_per_node: Count | None = None
    scratch_mb_per_node: Count | None = None
    #: The submitted walltime limit, independent of the execution estimate.
    wall_time_seconds: Count | None = None
    #: Execution time on a speed-1.0 target, e.g. from a calibrated estimate.
    runtime_seconds: Interval | None = None
    input_mb: NonNegative = 0.0
    output_mb: NonNegative = 0.0

    @model_validator(mode="after")
    def _shape(self) -> Request:
        if self.cpus < self.nodes:
            raise ValueError("cpus must cover at least one per node")
        if (self.mpi_ranks is None) != (self.threads_per_rank is None):
            raise ValueError("mpi_ranks and threads_per_rank must be provided together")
        if self.mpi_ranks is not None and self.threads_per_rank is not None:
            if self.cpus != self.mpi_ranks * self.threads_per_rank:
                raise ValueError("cpus must equal mpi_ranks * threads_per_rank")
            if self.mpi_ranks < self.nodes:
                raise ValueError("mpi_ranks must cover at least one rank per node")
        return self

    @property
    def cpus_per_node(self) -> int:
        if self.mpi_ranks is not None and self.threads_per_rank is not None:
            return ((self.mpi_ranks + self.nodes - 1) // self.nodes) * self.threads_per_rank
        return (self.cpus + self.nodes - 1) // self.nodes


def _parse_time(value: str, what: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        raise ValueError(f"{what} is not an ISO 8601 time: {value!r}") from None
    if parsed.tzinfo is None:
        raise ValueError(f"{what} needs a zone offset: {value!r}")
    return parsed


def _read_json(path: Path) -> object:
    try:
        with path.open("rb") as handle:
            data = handle.read(MAX_FILE_BYTES + 1)
    except OSError as exc:
        raise PlacementError(f"{path}: {exc.strerror or exc}") from None
    if len(data) > MAX_FILE_BYTES:
        raise PlacementError(f"{path}: larger than {MAX_FILE_BYTES} bytes")
    try:
        return json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PlacementError(f"{path}: not UTF-8 JSON: {exc}") from None


def load_fleet(path: Path) -> Fleet:
    return Fleet.model_validate(_read_json(path))


def load_request(path: Path) -> Request:
    return Request.model_validate(_read_json(path))


def _short(digest: str) -> str:
    return digest[:12]


def _rejections(
    target: Target, request: Request, *, age: float, max_age_seconds: float,
) -> list[str]:
    """Every hard requirement the target fails, in a fixed order."""
    reasons: list[str] = []
    if not target.reachable:
        reasons.append("unreachable when observed")
    if target.maintenance:
        reasons.append("in maintenance")
    if age > max_age_seconds:
        reasons.append(f"observation is {age:.0f} s old (limit {max_age_seconds:.0f} s)")
    elif age < -FUTURE_SKEW_SECONDS:
        reasons.append(f"observation is {-age:.0f} s in the future: clock skew")
    runtimes = target.programs.get(request.program)
    if not runtimes:
        reasons.append(f"program {request.program} is not available")
    elif request.runtime_digest is not None and request.runtime_digest not in runtimes:
        reasons.append(
            f"runtime {_short(request.runtime_digest)} of {request.program} is not available "
            f"(has {', '.join(_short(d) for d in runtimes)})"
        )
    if request.os is not None and target.os != request.os:
        reasons.append(f"operating system {target.os or 'unknown'}, not {request.os}")
    if request.arch is not None and target.arch != request.arch:
        reasons.append(f"architecture {target.arch or 'unknown'}, not {request.arch}")
    if request.isa_level is not None and request.isa_level not in target.isa_levels:
        reasons.append(f"ISA level {request.isa_level} is not supported")
    if request.account is not None:
        if target.accounts is None:
            reasons.append(f"permitted accounts unknown for requested account {request.account}")
        elif request.account not in target.accounts:
            reasons.append(f"account {request.account} is not permitted")
    if request.required_data_access:
        required = set(request.required_data_access)
        if target.data_access is None:
            reasons.append(
                f"data access unknown for required labels: {', '.join(sorted(required))}"
            )
        else:
            reasons.extend(
                f"data access {label} is not available"
                for label in sorted(required - set(target.data_access))
            )
    if request.required_licenses:
        required = set(request.required_licenses)
        if target.licenses is None:
            reasons.append(
                f"license eligibility unknown for required labels: {', '.join(sorted(required))}"
            )
        else:
            reasons.extend(
                f"license {label} is not permitted"
                for label in sorted(required - set(target.licenses))
            )
    if request.nodes > target.max_nodes:
        reasons.append(f"needs {request.nodes} nodes; at most {target.max_nodes} per job")
    if target.node.cpus is None:
        reasons.append("CPUs per node unknown")
    elif request.cpus_per_node > target.node.cpus:
        reasons.append(
            f"needs {request.cpus_per_node} CPUs per node; a node has {target.node.cpus}"
        )
    if request.mem_mb_per_node is not None:
        if target.node.mem_mb is None:
            reasons.append("memory per node unknown")
        elif request.mem_mb_per_node > target.node.mem_mb:
            reasons.append(
                f"needs {request.mem_mb_per_node} MB per node; a node has {target.node.mem_mb}"
            )
    if request.scratch_mb_per_node is not None:
        if target.node.scratch_mb is None:
            reasons.append("scratch per node unknown")
        elif request.scratch_mb_per_node > target.node.scratch_mb:
            reasons.append(
                f"needs {request.scratch_mb_per_node} MB scratch per node; "
                f"a node has {target.node.scratch_mb}"
            )
    if request.wall_time_seconds is not None:
        if target.max_walltime_seconds is None:
            if not target.walltime_unlimited:
                reasons.append("walltime limit unknown for an explicit walltime request")
        elif request.wall_time_seconds > target.max_walltime_seconds:
            reasons.append(
                f"requested walltime {request.wall_time_seconds} s exceeds the "
                f"{target.max_walltime_seconds} s walltime limit"
            )
    if request.runtime_seconds is not None:
        execution_high = request.runtime_seconds.high / target.speed_factor
        if request.wall_time_seconds is not None and execution_high > request.wall_time_seconds:
            reasons.append(
                f"predicted execution up to {execution_high:.0f} s exceeds the requested "
                f"{request.wall_time_seconds} s walltime"
            )
        if target.max_walltime_seconds is not None and execution_high > target.max_walltime_seconds:
            reasons.append(
                f"predicted execution up to {execution_high:.0f} s "
                f"exceeds the {target.max_walltime_seconds} s walltime limit"
            )
    return reasons


#: One completion component: its report entry, and its interval when known.
_Part = tuple[dict[str, object], tuple[float, float] | None]


def _component(low: float, high: float, source: str) -> _Part:
    return {"low": round(low, 3), "high": round(high, 3), "source": source}, (low, high)


def _unknown(reason: str) -> _Part:
    return {"unknown": True, "reason": reason}, None


def _moving(mb: float, rate: float | None, what: str) -> _Part:
    if mb == 0:
        return _component(0.0, 0.0, "nothing to move")
    if rate is None:
        return _unknown(f"no {what} rate recorded for this target")
    seconds = mb / rate
    return _component(seconds, seconds, f"{mb:g} MB at {rate:g} MB/s")


def _queue_wait(target: Target, request: Request) -> _Part:
    if target.kind == "daemon" and target.free is not None:
        free = target.free
        fits_cpus = free.cpus is not None and request.cpus <= free.cpus
        fits_mem = request.mem_mb_per_node is None or (
            free.mem_mb is not None and request.mem_mb_per_node <= free.mem_mb
        )
        fits_scratch = request.scratch_mb_per_node is None or (
            free.scratch_mb is not None and request.scratch_mb_per_node <= free.scratch_mb
        )
        if fits_cpus and fits_mem and fits_scratch:
            return _component(0.0, 0.0, "fits the free capacity now")
    if target.queue_wait_seconds is not None:
        wait = target.queue_wait_seconds
        return _component(wait.low, wait.high, wait.source)
    if target.kind == "daemon":
        return _unknown("does not fit the free capacity now and no wait model is recorded")
    return _unknown("no queue-wait model for this lane")


def _execution(target: Target, request: Request) -> _Part:
    if request.runtime_seconds is None:
        return _unknown("the request carries no runtime estimate")
    estimate = request.runtime_seconds
    return _component(
        estimate.low / target.speed_factor, estimate.high / target.speed_factor,
        f"reference estimate / speed {target.speed_factor:g} ({target.speed_source})",
    )


def explain(
    fleet: Fleet, request: Request, *, now: datetime, max_age_seconds: float,
) -> dict[str, object]:
    """The placement decision for ``request`` over ``fleet``, fully explained.

    Ranking: targets whose every component is known come first; then the
    midpoint of the predicted completion, its upper bound, the faster
    target, and the name, so equal inputs always give the same order.
    """
    if now.tzinfo is None:
        raise PlacementError("now needs a zone offset")
    if not math.isfinite(max_age_seconds) or max_age_seconds <= 0:
        raise PlacementError("max_age_seconds must be finite and positive")
    rejected: list[dict[str, object]] = []
    eligible: list[tuple[tuple[object, ...], dict[str, object]]] = []
    for target in sorted(fleet.targets, key=lambda t: t.name):
        age = (now - _parse_time(target.observed_at, "observed_at")).total_seconds()
        reasons = _rejections(target, request, age=age, max_age_seconds=max_age_seconds)
        if reasons:
            rejected.append({"target": target.name, "kind": target.kind,
                             "data_age_seconds": round(age, 3), "reasons": reasons})
            continue
        parts = {
            "staging": _moving(request.input_mb, target.staging_mb_per_second, "staging"),
            "queue_wait": _queue_wait(target, request),
            "execution": _execution(target, request),
            "transfer": _moving(request.output_mb, target.transfer_mb_per_second,
                                "result transfer"),
        }
        components = {name: payload for name, (payload, _) in parts.items()}
        known = [span for _, span in parts.values() if span is not None]
        low = sum(span[0] for span in known)
        high = sum(span[1] for span in known)
        unknown = [name for name in COMPONENTS if parts[name][1] is None]
        runtime = request.runtime_digest or target.programs[request.program][0]
        candidate: dict[str, object] = {
            "target": target.name,
            "kind": target.kind,
            "runtime_digest": runtime,
            "data_age_seconds": round(age, 3),
            "components": components,
            # Over the known components only; ``complete`` says whether
            # that is all of them.
            "completion": {"low": round(low, 3), "high": round(high, 3),
                           "complete": not unknown},
            "unknown_components": unknown,
        }
        key = (len(unknown), (low + high) / 2.0, high, -target.speed_factor, target.name)
        eligible.append((key, candidate))
    eligible.sort(key=lambda item: item[0])
    candidates = []
    for rank, (_, candidate) in enumerate(eligible, start=1):
        candidates.append({"rank": rank, **candidate})
    selected = candidates[0] if candidates else None
    return {
        "schema_name": EXPLANATION_SCHEMA,
        "policy": POLICY,
        "now": now.isoformat(),
        "max_age_seconds": max_age_seconds,
        "request": request.model_dump(mode="json"),
        "selected": None if selected is None else {
            "target": selected["target"],
            "runtime_digest": selected["runtime_digest"],
            "completion": selected["completion"],
        },
        "held": None if selected is not None else "no eligible target; see rejected",
        "candidates": candidates,
        "rejected": rejected,
    }


Explanation = dict[str, Any]


def explanation_json(explanation: Explanation) -> str:
    return json.dumps(explanation, indent=2, sort_keys=True, allow_nan=False) + "\n"


def _duration(seconds: float) -> str:
    total = int(round(seconds))
    hours, rem = divmod(total, 3600)
    minutes, secs = divmod(rem, 60)
    return f"{hours}:{minutes:02d}:{secs:02d}"


def _span(component: dict[str, Any]) -> str:
    if component.get("unknown"):
        return "unknown"
    low, high = component["low"], component["high"]
    return _duration(low) if low == high else f"{_duration(low)}-{_duration(high)}"


def explanation_text(explanation: Explanation) -> str:
    request = explanation["request"]
    lines = [
        f"placement for {request['program']}: {request['cpus']} CPUs on {request['nodes']} "
        f"node(s), {request['mem_mb_per_node'] or 'unstated'} MB per node",
    ]
    if request.get("os") is not None:
        lines.append(f"requested operating system: {request['os']}")
    if request.get("account") is not None:
        lines.append(f"requested account: {request['account']}")
    if request.get("required_data_access"):
        lines.append(f"required data access: {', '.join(request['required_data_access'])}")
    if request.get("required_licenses"):
        lines.append(f"required licenses: {', '.join(request['required_licenses'])}")
    if request.get("mpi_ranks") is not None:
        lines.append(f"requested layout: {request['mpi_ranks']} MPI rank(s), "
                     f"{request['threads_per_rank']} thread(s) per rank")
    if request.get("scratch_mb_per_node") is not None:
        lines.append(f"requested scratch: {request['scratch_mb_per_node']} MB per node")
    if request.get("wall_time_seconds") is not None:
        lines.append(f"requested walltime: {_duration(request['wall_time_seconds'])}")
    selected = explanation["selected"]
    if selected is None:
        lines.append(f"HELD: {explanation['held']}")
    else:
        lines.append(
            f"selected {selected['target']} (runtime {_short(selected['runtime_digest'])}); "
            f"predicted completion {_span(selected['completion'])}"
            + ("" if selected["completion"]["complete"] else " plus unknown components")
        )
    lines.append("")
    header = ("RANK", "TARGET", "COMPLETION", "STAGING", "QUEUE WAIT", "EXECUTION",
              "TRANSFER", "DATA AGE")
    rows = [
        (str(c["rank"]), c["target"],
         _span(c["completion"]) + ("" if c["completion"]["complete"] else " + unknown"),
         *(_span(c["components"][name]) for name in COMPONENTS),
         f"{c['data_age_seconds']:.0f} s")
        for c in explanation["candidates"]
    ]
    if rows:
        widths = [max(len(r[i]) for r in [header, *rows]) for i in range(len(header))]
        lines.extend("  ".join(r[i].ljust(widths[i]) for i in range(len(header))).rstrip()
                     for r in [header, *rows])
        unknown = [
            f"  {c['target']} {name}: {c['components'][name]['reason']}"
            for c in explanation["candidates"] for name in c["unknown_components"]
        ]
        if unknown:
            lines += ["", "Unknown components (ranked after complete predictions):", *unknown]
    if explanation["rejected"]:
        lines += ["", "Rejected:"]
        for item in explanation["rejected"]:
            lines.append(f"  {item['target']}: {'; '.join(item['reasons'])}")
    return "\n".join(lines) + "\n"
