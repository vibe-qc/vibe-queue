"""Scheduler dialect detection -- the ``vq scheduler-probe <host>`` diagnostic.

First contact with a cluster should turn its *unknown* batch scheduler into a
*detected* one, so the ``scheduler`` / ``scheduler_dialect`` config (see
``docs/pbs_dispatcher_backend_design.md`` §8) is set from a probe rather than a
build-time guess. This module SSHes into a host, asks the scheduler for its
version and which client binaries it ships, and classifies the dialect
(Torque / PBS Pro / SGE / SLURM).

It is pure logic over a :class:`~vq.scheduler_dispatch.RemoteRunner` (the SSH
boundary), so the classification is unit-tested against canned output with no
cluster, and the same code runs against a real login node through
:class:`~vq.scheduler_dispatch.SshRemoteRunner`. The detection facts were
grounded against the *pbs-cluster* cluster (TORQUE 2.5.12): ``qstat --version`` there
prints a bare lowercase ``version: 2.5.12``, which is Torque's signature (PBS
Pro prints ``pbs_version = ...`` instead).
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field

from vq.scheduler_dispatch import RemoteRunner

# Torque's `qstat --version` prints a bare lowercase "version: X.Y.Z". PBS Pro
# and OpenPBS print "pbs_version = ..." instead, which is the distinguishing
# tell (both ship pbsnodes/qmgr, so binary presence alone cannot separate them).
_TORQUE_VERSION_RE = re.compile(r"(?im)^\s*version:\s*(\d+\.\d+(?:\.\d+)?)\b")
_PBS_VERSION_RE = re.compile(r"(?i)pbs_version\s*=?\s*(\d+\.\d+(?:\.\d+)?)")
_ANY_VERSION_RE = re.compile(r"(\d+\.\d+(?:\.\d+)?)")

# Client binaries we check for. qsub/qstat/qdel are the PBS dispatch surface;
# qhold/qrls are the PBS hold/release surface behind `vq pause`/`resume`;
# pbsnodes/qmgr are PBS-family tells; qhost/qconf are SGE/Grid-Engine tells;
# sbatch/squeue/scancel/sacct/scontrol are the SLURM surface.
_CLIENT_BINS = (
    "qsub",
    "qstat",
    "qdel",
    "qhold",
    "qrls",
    "pbsnodes",
    "qmgr",
    "qhost",
    "qconf",
    "sbatch",
    "squeue",
    "scancel",
    "sacct",
    "scontrol",
    "sinfo",
)

_SERVER_STATE_RE = re.compile(r"(?im)^\s*server_state\s*=\s*(\S+)\s*$")
_QUEUE_RE = re.compile(r"(?im)^\s*Queue:\s*(\S+)\s*$")
_QUEUE_ATTR_RE = re.compile(r"(?im)^\s*(enabled|started)\s*=\s*(\S+)\s*$")


@dataclass(frozen=True)
class QueueLiveness:
    """Minimal read-only queue state from ``qstat -Qf``."""

    name: str
    enabled: bool | None = None
    started: bool | None = None


@dataclass(frozen=True)
class NodeCapacity:
    """One compute node's width, and how much of it is free right now."""

    name: str
    total_cpus: int | None = None
    """Cores the node has in total (PBS ``np``, SLURM ``%c``)."""

    free_cpus: int | None = None
    """Cores a job could take on this node right now. ``0`` for a node that
    is fully allocated *and* for one the scheduler cannot use at all."""

    state: str | None = None
    """The scheduler's own node state, verbatim."""

    usable: bool = True
    """False when the scheduler will not place work here at all (PBS
    ``down``/``offline``, SLURM ``down``/``drain``/``fail``/``maint``). A
    node that is merely busy stays usable: it frees up on its own."""

    group: str | None = None
    """The lane this node belongs to -- a SLURM partition, or a PBS node
    property. ``None`` when the scheduler did not report one."""


@dataclass(frozen=True)
class GroupCapacity:
    """The same two numbers for one partition or node property."""

    name: str
    max_cpus_now: int | None = None
    max_cpus_when_free: int | None = None
    usable_nodes: int = 0
    unusable_nodes: int = 0


@dataclass(frozen=True)
class SchedulableCapacity:
    """The largest request this host could start, now and at all.

    Two numbers, because they answer different questions and #148 turned on
    the difference. ``max_cpus_now`` answers "will this start": a request
    wider than it waits. ``max_cpus_when_free`` answers "can this *ever*
    start": a request wider than it waits forever, because no node the
    scheduler can use is that wide even when idle.
    """

    max_cpus_now: int | None = None
    max_cpus_when_free: int | None = None
    usable_nodes: int = 0
    unusable_nodes: int = 0
    nodes: tuple[NodeCapacity, ...] = ()
    groups: tuple[GroupCapacity, ...] = ()
    error: str | None = None
    """Why the census is unavailable. When set, every figure above is
    ``None``/empty and NO capacity claim is made -- an unreadable census must
    never read as "nothing is free"."""


@dataclass(frozen=True)
class ProbeResult:
    """What a scheduler probe found on one host."""

    dialect: str | None
    """``"torque"`` / ``"pbspro"`` / ``"sge"`` / ``"slurm"``, or ``None`` when
    undetermined. Maps directly onto the ``scheduler_dialect`` config value."""

    scheduler: str | None
    """The ``scheduler`` config value implied by the dialect: ``"pbs"`` for
    torque / pbspro, ``"sge"`` for sge, ``"slurm"`` for slurm, else ``None``."""

    version: str | None
    """The scheduler version string, if one could be parsed."""

    confidence: str
    """``"confirmed"`` (a signature matched), ``"likely"`` (weaker tells), or
    ``"unknown"`` (report raw output, ask the operator to set config by hand)."""

    binaries: dict[str, bool]
    """Which client binaries are on PATH (a subset of :data:`_CLIENT_BINS`)."""

    raw_version: str
    """Raw ``qstat --version`` output, surfaced in the report for the operator
    when detection is not confident."""

    server_state: str | None = None
    """PBS server state from ``qstat -Bf`` when available."""

    pbs_sched_running: bool | None = None
    """Whether the ``pbs_sched`` process is alive on the scheduler host.

    ``None`` means the read-only process probe was unavailable or inconclusive.
    """

    scheduler_daemons: tuple[str, ...] | None = None
    """Scheduler daemon processes found alive on the host.

    A subset of :data:`_SCHEDULER_DAEMONS`. Torque sites frequently run Maui
    (or Moab) instead of ``pbs_sched``; such a host is live even though
    ``pbs_sched`` is absent and ``qstat -Bf`` reports ``server_state = Idle``
    (the external scheduler drives dispatch, not pbs_server's internal cycle).
    ``None`` means the process probe was unavailable or inconclusive.
    """

    queues: tuple[QueueLiveness, ...] = field(default_factory=tuple)
    """Per-queue dispatch switches from ``qstat -Qf`` when available."""

    slurm_squeue_ok: bool | None = None
    """Whether a read-only ``squeue`` liveness probe succeeded.

    ``None`` means the host is not SLURM or the probe could not be attempted
    because the client was absent.
    """

    slurm_squeue_error: str | None = None
    """Diagnostic from a failed ``squeue`` liveness probe, if any."""

    slurm_partitions_not_up: tuple[str, ...] | None = None
    """Partitions whose ``sinfo`` availability is not ``up`` — the SLURM
    analog of a PBS queue with ``started = False``: an administrative
    scheduling hold. ``None`` means the state could not be read (``sinfo``
    absent or failing), in which case NO attribution is claimed; an empty
    tuple means every partition reported ``up``."""

    slurm_partition_probe_error: str | None = None
    """Diagnostic from a failed ``sinfo`` availability probe, if any."""

    capacity: SchedulableCapacity | None = None
    """What the scheduler could actually start right now, per host and lane.

    ``None`` when no census was attempted (the client binary is absent, or the
    host is not a scheduler host). A census that was attempted and failed is a
    :class:`SchedulableCapacity` carrying ``error``, so "unreadable" is never
    confused with "nothing free"."""

    notes: tuple[str, ...] = field(default_factory=tuple)


def _detect_binaries(runner: RemoteRunner) -> dict[str, bool]:
    """Return ``{binary: present}`` for the scheduler client binaries."""
    bins = " ".join(_CLIENT_BINS)
    script = f"for b in {bins}; do command -v \"$b\" >/dev/null 2>&1 && echo \"$b\"; done"
    result = runner.run(["sh", "-c", script], check=False)
    found = set(result.stdout.split())
    return {b: b in found for b in _CLIENT_BINS}


def _parse_server_state(output: str) -> str | None:
    match = _SERVER_STATE_RE.search(output)
    return match.group(1) if match else None


def _parse_bool(value: str) -> bool | None:
    lower = value.strip().lower()
    if lower in {"true", "yes", "1"}:
        return True
    if lower in {"false", "no", "0"}:
        return False
    return None


def _parse_queue_liveness(output: str) -> tuple[QueueLiveness, ...]:
    queues: list[QueueLiveness] = []
    current: dict[str, object] | None = None
    for line in output.splitlines():
        queue_match = _QUEUE_RE.match(line)
        if queue_match:
            if current is not None:
                queues.append(QueueLiveness(**current))
            current = {
                "name": queue_match.group(1),
                "enabled": None,
                "started": None,
            }
            continue
        if current is None:
            continue
        attr_match = _QUEUE_ATTR_RE.match(line)
        if attr_match:
            current[attr_match.group(1)] = _parse_bool(attr_match.group(2))
    if current is not None:
        queues.append(QueueLiveness(**current))
    return tuple(queues)


# Known PBS-family scheduler daemons. pbs_sched is Torque/PBS Pro's own
# scheduler; Maui and Moab are the common external replacements that leave
# pbs_sched stopped and pbs_server reporting ``server_state = Idle``.
_SCHEDULER_DAEMONS = ("pbs_sched", "maui", "moab")


def _probe_pbs_liveness(
    runner: RemoteRunner,
) -> tuple[
    str | None,
    bool | None,
    tuple[str, ...] | None,
    tuple[QueueLiveness, ...],
    tuple[str, ...],
]:
    notes: list[str] = []

    server = runner.run(["qstat", "-Bf"], check=False)
    server_state = None
    if server.returncode == 0:
        server_state = _parse_server_state(server.stdout + server.stderr)
        if server_state is None:
            notes.append("qstat -Bf did not report server_state")
    else:
        detail = (server.stderr or server.stdout).strip() or f"exit {server.returncode}"
        notes.append(f"qstat -Bf failed: {detail}")

    queues_result = runner.run(["qstat", "-Qf"], check=False)
    queues: tuple[QueueLiveness, ...] = ()
    if queues_result.returncode == 0:
        queues = _parse_queue_liveness(queues_result.stdout + queues_result.stderr)
        if not queues:
            notes.append("qstat -Qf did not report any queues")
    else:
        detail = (queues_result.stderr or queues_result.stdout).strip() or (
            f"exit {queues_result.returncode}"
        )
        notes.append(f"qstat -Qf failed: {detail}")

    daemon_sweep = "; ".join(
        f'pgrep -x "{daemon}" >/dev/null 2>&1 && echo "{daemon}"'
        for daemon in _SCHEDULER_DAEMONS
    )
    sched = runner.run(["sh", "-c", f"{daemon_sweep}; exit 0"], check=False)
    if sched.returncode == 0:
        found = tuple(d for d in _SCHEDULER_DAEMONS if d in sched.stdout.split())
        scheduler_daemons: tuple[str, ...] | None = found
        pbs_sched_running: bool | None = "pbs_sched" in found
    else:
        scheduler_daemons = None
        pbs_sched_running = None
        notes.append(f"scheduler daemon process probe failed: exit {sched.returncode}")

    return server_state, pbs_sched_running, scheduler_daemons, queues, tuple(notes)


def _probe_slurm_liveness(runner: RemoteRunner) -> tuple[bool, str | None]:
    """Read-only SLURM controller liveness check via ``squeue``."""
    result = runner.run(
        ["squeue", "--noheader", "--me", "--format=%i"],
        check=False,
    )
    if result.returncode == 0:
        return True, None
    detail = (result.stderr or result.stdout).strip() or f"exit {result.returncode}"
    return False, detail


def _probe_slurm_partition_availability(
    runner: RemoteRunner,
) -> tuple[tuple[str, ...] | None, str | None]:
    """Partitions whose availability is not ``up``, or ``None`` if unreadable.

    Read-only ``sinfo``; never mutates scheduler state. A failure returns
    ``(None, diagnostic)`` so the caller reports "state unavailable" instead
    of guessing an attribution.
    """
    result = runner.run(
        ["sinfo", "--noheader", "--format=%P %a"],
        check=False,
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()
        return None, detail or f"exit {result.returncode}"
    held: list[str] = []
    for line in result.stdout.splitlines():
        parts = line.split()
        if len(parts) >= 2 and parts[1].strip().lower() != "up":
            held.append(parts[0].rstrip("*"))
    return tuple(held), None


# A node the scheduler will not place work on at all, as opposed to one that
# is merely busy. PBS spells this in `state` (down / offline / unknown);
# SLURM in the %t abbreviation. Only these mean "waiting will not help".
_PBS_UNUSABLE_STATES = ("down", "offline", "unknown")
_SLURM_UNUSABLE_STATES = (
    "down", "drain", "drng", "draining", "drained", "fail", "failing",
    "maint", "unk", "unknown", "boot", "powering", "power", "resv", "inval",
)

# `jobs = 0/12345.host_f, 1/12345.host_f` or, on some Torque builds, the range
# form `jobs = 0-127/12345.host_f`. Both name the cores, not the jobs.
_PBS_JOB_SLOT_RE = re.compile(r"^\s*(\d+)(?:-(\d+))?/")


def _pbs_allocated_cpus(jobs_value: str) -> int:
    """Cores already taken on one PBS node, from its ``jobs`` attribute."""
    taken = 0
    for entry in jobs_value.split(","):
        match = _PBS_JOB_SLOT_RE.match(entry)
        if match is None:
            continue
        low = int(match.group(1))
        high = int(match.group(2)) if match.group(2) else low
        taken += max(0, high - low + 1)
    return taken


def _parse_pbsnodes(output: str) -> tuple[NodeCapacity, ...]:
    """Parse ``pbsnodes -a`` into a per-node width/free-core census.

    A node record is an unindented name followed by indented ``key = value``
    lines. Only ``np``, ``state``, ``properties`` and ``jobs`` are read; every
    other attribute is ignored, and a record missing ``np`` still counts as a
    node with an unknown width rather than disappearing.
    """
    nodes: list[NodeCapacity] = []
    name: str | None = None
    attrs: dict[str, str] = {}

    def flush() -> None:
        if name is None:
            return
        state = attrs.get("state")
        usable = not any(
            bad in (state or "").lower() for bad in _PBS_UNUSABLE_STATES
        )
        total: int | None = None
        if attrs.get("np", "").strip().isdigit():
            total = int(attrs["np"].strip())
        if not usable:
            free: int | None = 0
        elif total is None:
            free = None
        else:
            free = max(0, total - _pbs_allocated_cpus(attrs.get("jobs", "")))
        properties = attrs.get("properties", "").strip()
        nodes.append(
            NodeCapacity(
                name=name,
                total_cpus=total,
                free_cpus=free,
                state=state,
                usable=usable,
                group=properties.split(",")[0] if properties else None,
            )
        )

    for line in output.splitlines():
        if not line.strip():
            continue
        if not line[0].isspace():
            flush()
            name, attrs = line.strip(), {}
            continue
        key, sep, value = line.strip().partition("=")
        if sep:
            attrs[key.strip()] = value.strip()
    flush()
    return tuple(nodes)


def _parse_sinfo_nodes(output: str) -> tuple[NodeCapacity, ...]:
    """Parse ``sinfo -N`` rows (``%N|%P|%t|%c|%C``) into the same census.

    ``%C`` is SLURM's ``allocated/idle/other/total`` breakdown, so the idle
    field is the free-core count directly -- no arithmetic against running
    jobs, and no double counting on a mixed node.
    """
    nodes: list[NodeCapacity] = []
    for line in output.splitlines():
        if not line.strip():
            continue
        fields = line.split("|")
        if len(fields) < 5 or not fields[0].strip():
            continue
        name, partition, state, cpus, breakdown = (f.strip() for f in fields[:5])
        base = state.rstrip("*~#$@+").lower()
        usable = base not in _SLURM_UNUSABLE_STATES
        total = int(cpus) if cpus.isdigit() else None
        free: int | None = None
        parts = breakdown.split("/")
        if len(parts) == 4 and parts[1].isdigit():
            free = int(parts[1])
        if not usable:
            free = 0
        nodes.append(
            NodeCapacity(
                name=name,
                total_cpus=total,
                free_cpus=free,
                state=state or None,
                usable=usable,
                group=partition.rstrip("*") or None,
            )
        )
    return tuple(nodes)


def _summarize_capacity(
    nodes: tuple[NodeCapacity, ...],
    *,
    error: str | None = None,
) -> SchedulableCapacity:
    """Fold a node census into the per-host and per-group headline figures."""
    if error is not None:
        return SchedulableCapacity(error=error)

    def fold(subset: tuple[NodeCapacity, ...]) -> tuple[int | None, int | None, int, int]:
        usable = tuple(n for n in subset if n.usable)
        now = [n.free_cpus for n in usable if n.free_cpus is not None]
        ever = [n.total_cpus for n in usable if n.total_cpus is not None]
        return (
            max(now) if now else None,
            max(ever) if ever else None,
            len(usable),
            len(subset) - len(usable),
        )

    max_now, max_ever, usable_count, unusable_count = fold(nodes)
    groups: list[GroupCapacity] = []
    for name in sorted({n.group for n in nodes if n.group}):
        subset = tuple(n for n in nodes if n.group == name)
        g_now, g_ever, g_usable, g_unusable = fold(subset)
        groups.append(
            GroupCapacity(
                name=str(name),
                max_cpus_now=g_now,
                max_cpus_when_free=g_ever,
                usable_nodes=g_usable,
                unusable_nodes=g_unusable,
            )
        )
    return SchedulableCapacity(
        max_cpus_now=max_now,
        max_cpus_when_free=max_ever,
        usable_nodes=usable_count,
        unusable_nodes=unusable_count,
        nodes=nodes,
        groups=tuple(groups),
    )


def _probe_pbs_capacity(runner: RemoteRunner) -> SchedulableCapacity:
    """Read-only ``pbsnodes -a`` census; never mutates scheduler state."""
    result = runner.run(["pbsnodes", "-a"], check=False)
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()
        return _summarize_capacity((), error=detail or f"exit {result.returncode}")
    nodes = _parse_pbsnodes(result.stdout)
    if not nodes:
        return _summarize_capacity((), error="pbsnodes -a reported no nodes")
    return _summarize_capacity(nodes)


def _probe_slurm_capacity(runner: RemoteRunner) -> SchedulableCapacity:
    """Read-only ``sinfo -N`` census; never mutates scheduler state."""
    result = runner.run(
        ["sinfo", "--noheader", "--Node", "--format=%N|%P|%t|%c|%C"],
        check=False,
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()
        return _summarize_capacity((), error=detail or f"exit {result.returncode}")
    nodes = _parse_sinfo_nodes(result.stdout)
    if not nodes:
        return _summarize_capacity((), error="sinfo reported no nodes")
    return _summarize_capacity(nodes)


def probe(runner: RemoteRunner) -> ProbeResult:
    """Detect the scheduler dialect on the host behind ``runner``.

    Cheap and read-only: ``qstat --version`` plus a ``command -v`` sweep. Many
    schedulers print their version to stderr, so both streams are considered.
    """
    binaries = _detect_binaries(runner)
    ver = runner.run(["qstat", "--version"], check=False)
    raw = (ver.stdout + ver.stderr).strip()
    slurm_ver = runner.run(["sbatch", "--version"], check=False)
    raw_slurm = (slurm_ver.stdout + slurm_ver.stderr).strip()

    notes: list[str] = []
    if not (
        (binaries.get("qstat") and binaries.get("qsub"))
        or (binaries.get("sbatch") and binaries.get("squeue"))
    ):
        notes.append("no qsub/qstat or sbatch/squeue on PATH -- is this a scheduler host?")

    server_state: str | None = None
    pbs_sched_running: bool | None = None
    scheduler_daemons: tuple[str, ...] | None = None
    queues: tuple[QueueLiveness, ...] = ()
    slurm_squeue_ok: bool | None = None
    slurm_squeue_error: str | None = None
    if binaries.get("qstat") and (binaries.get("pbsnodes") or binaries.get("qmgr")):
        (
            server_state,
            pbs_sched_running,
            scheduler_daemons,
            queues,
            liveness_notes,
        ) = _probe_pbs_liveness(runner)
        notes.extend(liveness_notes)
    capacity: SchedulableCapacity | None = None
    if binaries.get("pbsnodes"):
        capacity = _probe_pbs_capacity(runner)
    slurm_partitions_not_up: tuple[str, ...] | None = None
    slurm_partition_probe_error: str | None = None
    if binaries.get("squeue"):
        slurm_squeue_ok, slurm_squeue_error = _probe_slurm_liveness(runner)
        (
            slurm_partitions_not_up,
            slurm_partition_probe_error,
        ) = _probe_slurm_partition_availability(runner)
        if binaries.get("sinfo"):
            capacity = _probe_slurm_capacity(runner)

    if binaries.get("sbatch") and binaries.get("squeue"):
        m = _ANY_VERSION_RE.search(raw_slurm)
        missing = [
            name
            for name in ("sbatch", "squeue", "scancel", "sacct", "scontrol")
            if not binaries.get(name)
        ]
        if missing:
            notes.append("missing SLURM client(s): " + ", ".join(missing))
        return ProbeResult(
            dialect="slurm",
            scheduler="slurm",
            version=m.group(1) if m else None,
            confidence="confirmed" if raw_slurm else "likely",
            binaries=binaries,
            raw_version=raw_slurm or raw,
            slurm_squeue_ok=slurm_squeue_ok,
            slurm_squeue_error=slurm_squeue_error,
            slurm_partitions_not_up=slurm_partitions_not_up,
            slurm_partition_probe_error=slurm_partition_probe_error,
            capacity=capacity,
            notes=tuple(notes),
        )

    # Torque: bare "version: X.Y.Z" from qstat --version, plus pbsnodes present.
    m = _TORQUE_VERSION_RE.search(raw)
    if m and binaries.get("pbsnodes"):
        return ProbeResult(
            dialect="torque",
            scheduler="pbs",
            version=m.group(1),
            confidence="confirmed",
            binaries=binaries,
            raw_version=raw,
            server_state=server_state,
            pbs_sched_running=pbs_sched_running,
            scheduler_daemons=scheduler_daemons,
            queues=queues,
            capacity=capacity,
            notes=tuple(notes),
        )

    # PBS Pro / OpenPBS: "pbs_version = X" tell.
    m = _PBS_VERSION_RE.search(raw)
    if m:
        notes.append("matched pbs_version; verify torque vs pbspro before relying on it")
        return ProbeResult(
            dialect="pbspro",
            scheduler="pbs",
            version=m.group(1),
            confidence="likely",
            binaries=binaries,
            raw_version=raw,
            server_state=server_state,
            pbs_sched_running=pbs_sched_running,
            scheduler_daemons=scheduler_daemons,
            queues=queues,
            capacity=capacity,
            notes=tuple(notes),
        )

    # SGE / Grid Engine: qhost/qconf present and no PBS pbsnodes.
    if (binaries.get("qhost") or binaries.get("qconf")) and not binaries.get("pbsnodes"):
        m = _ANY_VERSION_RE.search(raw)
        notes.append("SGE-family tells (qhost/qconf); the SGE dialect is not yet implemented")
        return ProbeResult(
            dialect="sge",
            scheduler="sge",
            version=m.group(1) if m else None,
            confidence="likely",
            binaries=binaries,
            raw_version=raw,
            notes=tuple(notes),
        )

    notes.append("could not classify the scheduler; set scheduler_dialect by hand")
    return ProbeResult(
        dialect=None,
        scheduler=None,
        version=None,
        confidence="unknown",
        binaries=binaries,
        raw_version=raw,
        server_state=server_state,
        pbs_sched_running=pbs_sched_running,
        scheduler_daemons=scheduler_daemons,
        queues=queues,
        capacity=capacity,
        notes=tuple(notes),
    )


def format_report(result: ProbeResult, host: str) -> str:
    """Render a probe result as the human-facing ``vq scheduler-probe`` output."""
    lines = [f"scheduler-probe {host}:"]
    if result.dialect is not None:
        lines.append(
            f"  detected: {result.dialect} "
            f"(version {result.version or '?'}, {result.confidence})"
        )
        lines.append("  suggested config:")
        lines.append(f'    scheduler         = "{result.scheduler}"')
        lines.append(f'    scheduler_dialect = "{result.dialect}"')
    else:
        lines.append(f"  detected: UNKNOWN ({result.confidence})")
    present = sorted(b for b, ok in result.binaries.items() if ok)
    lines.append(f"  client binaries: {', '.join(present) or '(none found)'}")
    if result.raw_version:
        first = result.raw_version.splitlines()[0] if result.raw_version else ""
        lines.append(f"  qstat --version: {first}")
    if result.server_state is not None:
        lines.append(f"  PBS server_state: {result.server_state}")
    if result.pbs_sched_running is not None:
        status = "running" if result.pbs_sched_running else "not running"
        lines.append(f"  pbs_sched: {status}")
    if result.scheduler_daemons is not None:
        found = ", ".join(result.scheduler_daemons) or "(none found)"
        lines.append(f"  scheduler daemons: {found}")
    stopped = [
        q.name
        for q in result.queues
        if q.enabled is True and q.started is False
    ]
    if result.queues:
        queue_bits = []
        for queue in result.queues:
            enabled = "enabled=?" if queue.enabled is None else f"enabled={queue.enabled}"
            started = "started=?" if queue.started is None else f"started={queue.started}"
            queue_bits.append(f"{queue.name}({enabled},{started})")
        lines.append(f"  queues: {', '.join(queue_bits)}")
    if stopped:
        lines.append(f"  warning: started=False queue(s): {', '.join(stopped)}")
    if result.slurm_squeue_ok is not None:
        status = "ok" if result.slurm_squeue_ok else "failed"
        lines.append(f"  SLURM squeue: {status}")
        if result.slurm_squeue_error:
            lines.append(f"  warning: squeue failed: {result.slurm_squeue_error}")
    lines.extend(_format_capacity(result.capacity))
    for note in result.notes:
        lines.append(f"  note: {note}")
    return "\n".join(lines)


def _format_capacity(capacity: SchedulableCapacity | None) -> list[str]:
    """Render the schedulable-capacity block of the probe report."""
    if capacity is None:
        return []
    if capacity.error is not None:
        return [f"  capacity: unavailable ({capacity.error})"]
    now = "?" if capacity.max_cpus_now is None else str(capacity.max_cpus_now)
    ever = (
        "?" if capacity.max_cpus_when_free is None
        else str(capacity.max_cpus_when_free)
    )
    lines = [
        f"  capacity: largest job that can start now: {now} cpu(s); "
        f"largest the usable nodes could ever run: {ever}",
        f"  nodes: {capacity.usable_nodes} usable, "
        f"{capacity.unusable_nodes} unusable",
    ]
    for group in capacity.groups:
        g_now = "?" if group.max_cpus_now is None else group.max_cpus_now
        g_ever = (
            "?" if group.max_cpus_when_free is None else group.max_cpus_when_free
        )
        lines.append(
            f"    {group.name}: now={g_now} when-free={g_ever} "
            f"({group.usable_nodes} usable, {group.unusable_nodes} unusable)"
        )
    unusable = [n.name for n in capacity.nodes if not n.usable]
    if unusable:
        shown = ", ".join(unusable[:8])
        if len(unusable) > 8:
            shown += f", +{len(unusable) - 8} more"
        lines.append(f"  warning: unusable node(s): {shown}")
    # The case this whole census exists for: a request can be admitted but
    # never started, which otherwise only shows up as days of silent queueing.
    if (
        capacity.max_cpus_when_free is not None
        and capacity.unusable_nodes > 0
    ):
        widest_down = max(
            (n.total_cpus or 0 for n in capacity.nodes if not n.usable),
            default=0,
        )
        if widest_down > capacity.max_cpus_when_free:
            lines.append(
                f"  warning: an unusable node is wider ({widest_down} cpu) than "
                f"anything the scheduler can still run ({capacity.max_cpus_when_free} "
                "cpu); requests sized for it will queue forever"
            )
    return lines


def to_json_dict(result: ProbeResult) -> dict[str, object]:
    """Stable machine-readable shape for ``vq scheduler-probe --json``."""
    payload = asdict(result)
    payload["notes"] = list(result.notes)
    payload["queues"] = [asdict(queue) for queue in result.queues]
    payload["scheduler_daemons"] = (
        list(result.scheduler_daemons) if result.scheduler_daemons is not None else None
    )
    if result.capacity is None:
        payload["capacity"] = None
    else:
        capacity = asdict(result.capacity)
        capacity["nodes"] = [asdict(node) for node in result.capacity.nodes]
        capacity["groups"] = [asdict(group) for group in result.capacity.groups]
        payload["capacity"] = capacity
    return payload


def probe_host(host_cfg: object) -> ProbeResult:
    """Probe the scheduler on ``host_cfg`` over a real SSH transport.

    Thin wrapper that builds an :class:`~vq.scheduler_dispatch.SshRemoteRunner`
    for the host and runs :func:`probe`. Kept out of :func:`probe` so the
    classification stays transport-free and unit-testable.
    """
    from vq.scheduler_dispatch import SshRemoteRunner  # noqa: PLC0415

    return probe(SshRemoteRunner(host_cfg))  # type: ignore[arg-type]
