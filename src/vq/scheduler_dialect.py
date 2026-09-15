"""Scheduler dialects — the batch-scheduler-specific string layer.

The live :class:`vq.scheduler_dispatch.SchedulerDispatcher` reaches external
batch schedulers over SSH. Its Torque path submits with ``qsub``, polls with
``qstat``, and cancels with ``qdel``; its Slurm path uses ``sbatch``, ``squeue``
and ``sacct``, and ``scancel``. Every scheduler speaks a different command
dialect, so the dispatcher codes against this :class:`SchedulerDialect`
protocol and stays dialect-agnostic. Torque and Slurm are implemented; PBS Pro
and SGE remain reserved config values without dialect implementations.

This module is the **pure-function** layer: it builds argv lists and job-script
text, and parses scheduler stdout. It performs **no I/O** — no SSH, no
``subprocess``, no filesystem — so it is fully unit-testable on any OS without a
cluster (design doc §12, "testing without a cluster"). The
``SchedulerDispatcher`` supplies the SSH transport; the daemon owns spec I/O
and durable lifecycle classification.

Dialect facts encoded here for Torque were confirmed against the *pbs-cluster* cluster
(TORQUE 2.5.12). Site-specific queue, account, and SSH values are supplied
through configuration rather than hard-coded in this module. Only the generic
dialect — flag spellings, state letters, and parse formats — lives here.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass
from enum import Enum, auto
from typing import Protocol


class DialectError(ValueError):
    """A scheduler value could not be built or parsed.

    Raised when a request is malformed (e.g. a job script that is not pure
    ASCII, which Torque ``qsub`` rejects) or scheduler output cannot be
    understood (e.g. ``qsub`` printed no recognizable job id). A
    :class:`ValueError` because it always denotes bad data, never an I/O
    failure — the latter belongs to the SSH transport in the dispatcher.
    """


def enforce_scheduler_wall_time_limit(
    requested_seconds: int | None,
    maximum_seconds: int | None,
    *,
    scheduler_host: str | None = None,
    partition: str | None = None,
) -> None:
    """Reject an ask above an operator-declared scheduler lane maximum.

    An unknown maximum remains backward compatible for generic vq clients;
    paper campaign tooling separately requires authoritative lane metadata.
    """
    if (
        requested_seconds is None
        or maximum_seconds is None
        or requested_seconds <= maximum_seconds
    ):
        return
    lane = (
        f"scheduler lane {scheduler_host!r}"
        if scheduler_host is not None
        else "scheduler lane"
    )
    if partition is not None:
        lane += f" (partition {partition!r})"
    raise DialectError(
        f"{lane} allows at most {maximum_seconds} s; "
        f"requested {requested_seconds} s"
    )


class SchedulerPhase(Enum):
    """Coarse, scheduler-agnostic phase derived from scheduler status output.

    The dialect classifies a job into exactly one of these from the
    scheduler's own state codes. This is distinct from the durable
    :class:`vq.spec.JobState`: after a successful submission the daemon keeps
    the vq lifecycle RUNNING while recording queued/running detail in
    ``scheduler_state``. Once the dispatcher classifies FINISHED, including
    when a job disappears from the live-status poll, the daemon owns terminal
    classification: a visible exit-marker return code is authoritative; without
    one, scheduler walltime evidence distinguishes TIME_EXCEEDED from
    ABORTED_BY_QUEUE.
    """

    PENDING = auto()
    RUNNING = auto()
    FINISHED = auto()


@dataclass(frozen=True)
class QstatDetail:
    """Per-job detail parsed from scheduler accounting output.

    Torque supplies a ``qstat -f`` block and Slurm supplies a ``sacct`` row.
    The coarse poll (``parse_poll``) gives only the phase; this adds the
    live-progress fields surfaced through ``vq status``: which node the job
    landed on and how much of its walltime budget it has used. The execution
    host and used walltime may be absent while queued, and every detail field is
    optional except ``raw_state``.
    """

    raw_state: str
    exec_host: str | None = None
    walltime_used: str | None = None
    walltime_limit: str | None = None
    exit_code: int | None = None


@dataclass(frozen=True)
class ResourceRequest:
    """The resource ask for one submission, dialect-independent.

    The ``SchedulerDispatcher`` builds this from a :class:`vq.spec.JobSpec`
    (``cpus`` / ``scheduler_tasks`` / ``mem_mb`` / ``wall_time_seconds``) plus
    the per-host site settings (``queue`` / ``account`` from ``submit_extra``)
    and the staged stdout/stderr paths. The dialect turns it into scheduler
    directives.
    """

    cpus: int
    scheduler_tasks: int | None = None
    mem_mb: int | None = None
    wall_time_seconds: int | None = None
    queue: str | None = None
    account: str | None = None
    job_name: str | None = None
    stdout_path: str | None = None
    stderr_path: str | None = None
    # Optional native scheduler-array width, rendered 0-indexed. The current
    # daemon does not set this for vq arrays, whose elements are independent
    # JobSpecs and independent scheduler submissions.
    array_size: int | None = None
    # Verbatim extra directives appended after the mapped ones (site escape
    # hatch, e.g. an extra ``-l`` feature request). Each element is a whole
    # directive body like ``"-l naccelerators=1"``.
    extra_directives: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.cpus < 1:
            raise ValueError(f"cpus must be >= 1, got {self.cpus}")
        if self.scheduler_tasks is not None and self.scheduler_tasks < 1:
            raise ValueError(
                f"scheduler_tasks must be >= 1 when set, got {self.scheduler_tasks}"
            )
        if self.mem_mb is not None and self.mem_mb < 1:
            raise ValueError(f"mem_mb must be >= 1 when set, got {self.mem_mb}")
        if self.wall_time_seconds is not None and self.wall_time_seconds < 1:
            raise ValueError(
                f"wall_time_seconds must be >= 1 when set, got {self.wall_time_seconds}"
            )
        if self.array_size is not None and self.array_size < 1:
            raise ValueError(f"array_size must be >= 1 when set, got {self.array_size}")


class SchedulerDialect(Protocol):
    """The scheduler command dialect (Torque, Slurm, or a future dialect).

    Pure string construction + parsing. The ``SchedulerDispatcher`` holds one
    of these and routes scheduler-specific commands through it. Adding a
    dialect requires implementing this protocol and registering its config
    value, but does not require changing the dispatcher's transport workflow.
    """

    name: str
    """Stable dialect identifier, e.g. ``"torque"`` (matches the
    ``scheduler_dialect`` config value)."""

    array_index_env: str
    """Scheduler environment variable holding the array element index."""

    job_id_env: str
    """Scheduler environment variable holding the running job's own id.

    Lets the job script record its scheduler id on the shared workspace, so the
    driver can recover a handle it never managed to persist locally (see
    ``SchedulerDispatcher.recorded_job_id``).
    """

    def resource_directives(self, req: ResourceRequest) -> list[str]:
        """Map a :class:`ResourceRequest` to scheduler directive bodies.

        Returns bodies such as ``["-N job", "-l nodes=1:ppn=4", ...]``.
        :meth:`render_job_script` prefixes them with the dialect's script
        marker (for example ``#PBS`` or ``#SBATCH``); the dispatcher does not
        also pass them as submit-command argv.
        """
        ...

    def render_job_script(
        self, directives: list[str], body: list[str], *, shell: str = "/bin/bash"
    ) -> str:
        """Assemble a submittable job script: shebang + directive lines + body."""
        ...

    def submit_command(self, script_path: str, *, extra_args: Sequence[str] = ()) -> list[str]:
        """argv that submits ``script_path`` (e.g. ``["qsub", …, script]``)."""
        ...

    def parse_submit_id(self, stdout: str) -> str:
        """Extract the scheduler job id from the submit command's stdout."""
        ...

    def poll_command(self, job_ids: Sequence[str]) -> list[str]:
        """argv for a single batched status poll of ``job_ids``."""
        ...

    def parse_poll(self, stdout: str) -> dict[str, SchedulerPhase]:
        """Parse a status poll into ``{job_id: phase}`` for every listed job.

        Jobs absent from the output have left the queue; the dispatcher (which
        knows which ids it asked about) treats an absent id as FINISHED.
        """
        ...

    def detail_command(self, job_id: str) -> list[str]:
        """argv for accounting/detail output for one requested job id."""
        ...

    def parse_exit_status(self, stdout: str) -> int | None:
        """Exit code when a detail record exposes one, otherwise ``None``."""
        ...

    def poll_detail_command(self, job_ids: Sequence[str]) -> list[str]:
        """argv for a single batched *detailed* poll of ``job_ids`` (§18)."""
        ...

    def parse_qstat_detail(self, stdout: str) -> dict[str, QstatDetail]:
        """Parse a detailed poll into ``{job_id: QstatDetail}`` per listed job."""
        ...

    def phase_for_state(self, state: str) -> SchedulerPhase:
        """Map one scheduler-native state code to a coarse phase."""
        ...

    def abnormal_termination(self, state: str) -> str | None:
        """Normalized state when the scheduler ended the job abnormally (#414).

        Returns the scheduler's own normalized terminal state (for example
        ``"OUT_OF_MEMORY"``, ``"CANCELLED"``, ``"TIMEOUT"``) when accounting
        reports that the *scheduler* terminated or invalidated the job, and
        ``None`` otherwise. A clean ``COMPLETED`` and an ordinary nonzero
        script exit (``FAILED``) are not abnormal: their truth flows through
        the exit-marker rc. The daemon uses this verdict to refuse a
        ``completed`` classification for a killed job whose exit-marker
        (mis)reads 0 — the exit marker stays the rc source of truth, but it
        cannot certify a clean completion the scheduler disowned.
        """
        ...

    def cancel_command(self, job_id: str) -> list[str]:
        """argv that cancels ``job_id`` (e.g. ``["qdel", job_id]``)."""
        ...

    def hold_command(self, job_id: str) -> list[str]:
        """argv that places a scheduler hold on ``job_id``."""
        ...

    def release_command(self, job_id: str) -> list[str]:
        """argv that releases a scheduler hold on ``job_id``."""
        ...


def format_walltime(seconds: int) -> str:
    """Render seconds as ``H:MM:SS`` for Torque and Slurm time directives.

    The formatter leaves hours unbounded. ``360000 -> "100:00:00"``, ``3661
    -> "01:01:01"``, and ``0 -> "00:00:00"``.
    """
    if seconds < 0:
        raise ValueError(f"walltime seconds must be >= 0, got {seconds}")
    hours, rem = divmod(seconds, 3600)
    minutes, secs = divmod(rem, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def parse_submit_extra(
    tokens: Sequence[str],
) -> tuple[str | None, str | None, tuple[str, ...]]:
    """Split site ``submit_extra`` tokens into queue, account, and directives.

    The per-host config carries the site's scheduler flags as a flat token list,
    e.g. ``["-q", "compute", "-A", "proj1", "-l", "naccelerators=1"]``. The
    ``SchedulerDispatcher`` feeds the result of this split into a
    :class:`ResourceRequest` so placement and extra directives render **once**
    through the dialect's ``#PBS`` or ``#SBATCH`` job-script path, never also
    as ``qsub`` or ``sbatch`` argv.

    Pairing rule: scheduler placement flags consume their value and are rendered
    once through the dialect's normal directive path. PBS/Torque uses ``-q`` and
    ``-A``; SLURM uses ``--partition`` / ``-p`` and ``--account`` / ``-A``.
    Unknown flags remain usable as extra directives where safe: a flag consumes
    one following non-flag value, while ``--flag=value`` passes through whole.
    Dangling known placement flags are malformed site config and raise
    :class:`DialectError`.
    """
    queue: str | None = None
    account: str | None = None
    extra: list[str] = []
    i = 0
    n = len(tokens)
    while i < n:
        tok = tokens[i]
        if tok in ("-q", "-p", "-A", "--partition", "--account"):
            if i + 1 >= n:
                raise DialectError(f"submit_extra: {tok} is missing its value")
            value = tokens[i + 1]
            if tok in ("-q", "-p", "--partition"):
                queue = value
            else:
                account = value
            i += 2
            continue
        if tok.startswith("--partition="):
            queue = tok.split("=", 1)[1]
            if not queue:
                raise DialectError("submit_extra: --partition is missing its value")
            i += 1
            continue
        if tok.startswith("--account="):
            account = tok.split("=", 1)[1]
            if not account:
                raise DialectError("submit_extra: --account is missing its value")
            i += 1
            continue
        if tok.startswith("--") and "=" in tok:
            extra.append(tok)
            i += 1
            continue
        if tok.startswith("-") and i + 1 < n and not tokens[i + 1].startswith("-"):
            extra.append(f"{tok} {tokens[i + 1]}")
            i += 2
            continue
        # A bare flag (e.g. "-X") or a stray non-flag token: pass through as its
        # own directive body so the site escape hatch stays fully expressive.
        extra.append(tok)
        i += 1
    return queue, account, tuple(extra)


# Torque 2.5.12 limits job names to 15 chars and wants the first char
# alphabetic; non-conforming names are silently truncated/rejected, so we
# normalize before submission rather than discover it at the scheduler.
_TORQUE_JOB_NAME_MAXLEN = 15
_TORQUE_JOB_NAME_BAD = re.compile(r"[^A-Za-z0-9_]")
_SLURM_JOB_NAME_MAXLEN = 50
_SLURM_JOB_NAME_BAD = re.compile(r"[^A-Za-z0-9._-]")


def sanitize_job_name(name: str) -> str:
    """Coerce ``name`` into a Torque-safe job name.

    Keeps ``[A-Za-z0-9_]`` (others → ``_``), prefixes ``j`` if the result is
    empty or does not start with a letter (vq ids can start with a digit), and
    truncates to 15 chars.
    """
    cleaned = _TORQUE_JOB_NAME_BAD.sub("_", name)
    if not cleaned or not cleaned[0].isalpha():
        cleaned = "j" + cleaned
    return cleaned[:_TORQUE_JOB_NAME_MAXLEN]


def sanitize_slurm_job_name(name: str) -> str:
    """Coerce ``name`` into vq's filesystem-safe 50-char scheduler label.

    SLURM does not need Torque's first-character rule or 15-character limit.
    Keep the same conservative character surface as ``JobSpec.job_name`` so a
    descriptive vq label remains recognizable in ``squeue`` / ``sacct``.
    """
    cleaned = _SLURM_JOB_NAME_BAD.sub("_", name)
    if not cleaned:
        cleaned = "j"
    return cleaned[:_SLURM_JOB_NAME_MAXLEN]


class TorqueDialect:
    """TORQUE 2.5.12 dialect (the *pbs-cluster* cluster scheduler).

    Confirmed mappings (design doc §6/§7/§16): ``cpus -> -l nodes=1:ppn=N``,
    ``mem_mb -> -l mem=Nmb``, ``wall -> -l walltime=HH:MM:SS``, ``queue -> -q``,
    ``name -> -N``, array ``-> -t 0-(N-1)``; ``qstat`` states ``Q/W/H/T``
    (pending), ``R/E/S`` (running), ``C`` (complete); rc via ``qstat -f``
    ``exit_status`` with the exit-marker file as the primary source of truth.
    """

    name = "torque"
    array_index_env = "PBS_ARRAYID"
    job_id_env = "PBS_JOBID"

    # Torque job-state letter -> coarse phase. The full 2.5.12 state set is
    # fixed (C,E,H,Q,R,S,T,W); an unseen letter means a non-Torque scheduler
    # or a parse bug and is surfaced rather than silently mis-stated.
    _STATE_MAP: dict[str, SchedulerPhase] = {
        "Q": SchedulerPhase.PENDING,  # queued, eligible to run
        "W": SchedulerPhase.PENDING,  # waiting on its execution-after time
        # Held remains a coarse pending phase; daemon status separately exposes
        # scheduler_state=held and vq pause records JobState.SUSPENDED.
        "H": SchedulerPhase.PENDING,
        "T": SchedulerPhase.PENDING,  # transiting / being moved between queues
        "R": SchedulerPhase.RUNNING,  # running
        "E": SchedulerPhase.RUNNING,  # exiting (still holds the node)
        "S": SchedulerPhase.RUNNING,  # suspended by the scheduler (still allocated)
        "C": SchedulerPhase.FINISHED,  # complete (lingers briefly, then drops out)
    }

    # A Torque job id: a sequence number, an optional array suffix (``[]`` or
    # ``[<n>]``), and an optional ``.server`` host part — e.g. ``12345.pbs-cluster``,
    # ``12345[].pbs-cluster``, ``12345[7].pbs-cluster``, or bare ``12345``.
    _JOB_ID = r"\d+(?:\[\d*\])?(?:\.\S+)?"
    _SUBMIT_ID_RE = re.compile(rf"^{_JOB_ID}$")
    _POLL_ROW_RE = re.compile(rf"^({_JOB_ID})\s")
    _EXIT_STATUS_RE = re.compile(r"(?im)^\s*exit_status\s*=\s*(-?\d+)\s*$")

    def resource_directives(self, req: ResourceRequest) -> list[str]:
        directives: list[str] = []
        if req.job_name is not None:
            directives.append(f"-N {sanitize_job_name(req.job_name)}")
        # Single node, N processors per node — the Torque idiom for an
        # N-core job (design doc §7).
        directives.append(f"-l nodes=1:ppn={req.cpus}")
        if req.mem_mb is not None:
            directives.append(f"-l mem={req.mem_mb}mb")
        if req.wall_time_seconds is not None:
            directives.append(f"-l walltime={format_walltime(req.wall_time_seconds)}")
        if req.queue is not None:
            directives.append(f"-q {req.queue}")
        if req.account is not None:
            directives.append(f"-A {req.account}")
        if req.stdout_path is not None:
            directives.append(f"-o {req.stdout_path}")
        if req.stderr_path is not None:
            directives.append(f"-e {req.stderr_path}")
        if req.array_size is not None:
            directives.append(f"-t 0-{req.array_size - 1}")
        directives.extend(req.extra_directives)
        return directives

    def render_job_script(
        self, directives: list[str], body: list[str], *, shell: str = "/bin/bash"
    ) -> str:
        lines = [f"#!{shell}"]
        lines.extend(f"#PBS {d}" for d in directives)
        lines.extend(body)
        script = "\n".join(lines) + "\n"
        # Torque qsub rejects non-ASCII scripts with a cryptic "file must be
        # an ascii script"; fail early and actionably instead.
        try:
            script.encode("ascii")
        except UnicodeEncodeError as exc:
            raise DialectError(
                "Torque qsub requires a pure-ASCII job script; "
                f"non-ASCII byte in rendered script: {exc}"
            ) from exc
        return script

    def submit_command(self, script_path: str, *, extra_args: Sequence[str] = ()) -> list[str]:
        return ["qsub", *extra_args, script_path]

    def parse_submit_id(self, stdout: str) -> str:
        lines = [ln.strip() for ln in stdout.splitlines() if ln.strip()]
        if not lines:
            raise DialectError("qsub produced no job id on stdout")
        job_id = lines[-1]  # any banner/warning precedes the id on its own line
        if not self._SUBMIT_ID_RE.match(job_id):
            raise DialectError(f"unrecognized qsub job id: {job_id!r}")
        return job_id

    def poll_command(self, job_ids: Sequence[str]) -> list[str]:
        # One batched poll for all tracked ids (design doc §4: not per-job).
        # The dispatcher tolerates a non-zero rc — qstat errors on ids that
        # already left the queue while still listing the live ones on stdout.
        return ["qstat", *job_ids]

    def parse_poll(self, stdout: str) -> dict[str, SchedulerPhase]:
        # Default qstat table columns: "Job id  Name  User  Time Use  S  Queue".
        # Header and the dashed separator do not start with a job-id token, so
        # the row regex skips them; the state ``S`` is the second-to-last
        # field, anchored off the trailing Queue column.
        result: dict[str, SchedulerPhase] = {}
        for line in stdout.splitlines():
            if not self._POLL_ROW_RE.match(line):
                continue
            fields = line.split()
            if len(fields) < 6:
                continue
            result[fields[0]] = self.phase_for_state(fields[-2])
        return result

    def detail_command(self, job_id: str) -> list[str]:
        return ["qstat", "-f", job_id]

    def parse_exit_status(self, stdout: str) -> int | None:
        match = self._EXIT_STATUS_RE.search(stdout)
        return int(match.group(1)) if match else None

    def poll_detail_command(self, job_ids: Sequence[str]) -> list[str]:
        # Batched `qstat -f` for all live ids in one call (§18 item 2). qstat
        # errors (nonzero rc) on ids that already left the queue while still
        # printing the live ones, exactly like the coarse poll; the dispatcher
        # tolerates the rc and parses stdout.
        return ["qstat", "-f", *job_ids]

    # A `qstat -f` record opens with "Job Id: <id>"; the body is `key = value`
    # lines (some wrapped onto indented continuation lines, which we ignore --
    # the fields we read fit one line on a single-node job).
    _DETAIL_FIELD = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_.]*)\s*=\s*(.*?)\s*$")

    def parse_qstat_detail(self, stdout: str) -> dict[str, QstatDetail]:
        result: dict[str, QstatDetail] = {}
        # Split into per-job blocks at the "Job Id:" headers; blocks[0] is the
        # preamble before the first header.
        blocks = re.split(r"(?im)^Job Id:\s*", stdout)
        for block in blocks[1:]:
            lines = block.splitlines()
            job_id = lines[0].strip()
            if not job_id:
                continue
            fields: dict[str, str] = {}
            for line in lines[1:]:
                m = self._DETAIL_FIELD.match(line)
                if m:
                    fields[m.group(1)] = m.group(2)
            result[job_id] = QstatDetail(
                raw_state=fields.get("job_state", ""),
                exec_host=fields.get("exec_host"),
                walltime_used=fields.get("resources_used.walltime"),
                walltime_limit=fields.get("Resource_List.walltime"),
            )
        return result

    def cancel_command(self, job_id: str) -> list[str]:
        # SchedulerDispatcher.cancel routes here. Torque owns the termination
        # policy behind qdel; vq assumes no separate graceful/force flag.
        return ["qdel", job_id]

    def hold_command(self, job_id: str) -> list[str]:
        # qhold is a scheduler hold: it prevents queued jobs from starting. It
        # is not a live SIGSTOP equivalent for jobs already executing on nodes.
        return ["qhold", job_id]

    def release_command(self, job_id: str) -> list[str]:
        return ["qrls", job_id]

    def phase_for_state(self, state: str) -> SchedulerPhase:
        try:
            return self._STATE_MAP[state]
        except KeyError:
            raise DialectError(f"unknown Torque job state {state!r}") from None

    def abnormal_termination(self, state: str) -> str | None:
        # Torque's qstat state letters carry no abnormality signal: "C" is the
        # only terminal letter and covers clean and killed jobs alike. PBS
        # truthfulness flows through qstat -f exit_status (>= 256 for signal
        # kills), the exit-marker, and walltime evidence — there is no
        # accounting state to reconcile against here.
        del state
        return None


class SlurmDialect:
    """SLURM dialect for daemonless scheduler hosts such as slurm-cluster.

    The exit marker remains vq's rc source of truth; ``sacct`` detail supplies
    telemetry, the rc fallback when no marker survives, and (since #414) the
    :meth:`abnormal_termination` verdict that keeps a scheduler-killed job
    (OUT_OF_MEMORY / CANCELLED / TIMEOUT / ...) from being classified
    ``completed`` off a marker that (mis)reads 0.
    """

    name = "slurm"
    array_index_env = "SLURM_ARRAY_TASK_ID"
    job_id_env = "SLURM_JOB_ID"

    _SUBMIT_RE = re.compile(r"(?im)^\s*Submitted batch job\s+(\d+)\s*$")
    _EXIT_CODE_RE = re.compile(r"^(-?\d+):(\d+)$")

    _PENDING_STATES = {
        "PENDING",
        "CONFIGURING",
        "REQUEUED",
        "RESV_DEL_HOLD",
        "REQUEUE_FED",
        "REQUEUE_HOLD",
    }
    _RUNNING_STATES = {
        "RUNNING",
        "COMPLETING",
        "SUSPENDED",
        "STOPPED",
        "RESIZING",
        "SIGNALING",
        "STAGE_OUT",
    }
    _FINISHED_STATES = {
        "BOOT_FAIL",
        "CANCELLED",
        "COMPLETED",
        "DEADLINE",
        "FAILED",
        "NODE_FAIL",
        "OUT_OF_MEMORY",
        "PREEMPTED",
        "REVOKED",
        "SPECIAL_EXIT",
        "TIMEOUT",
    }
    _STATE_ALIASES = {
        "PD": "PENDING",
        "CF": "CONFIGURING",
        "RQ": "REQUEUED",
        "R": "RUNNING",
        "CG": "COMPLETING",
        "S": "SUSPENDED",
        "ST": "STOPPED",
        "BF": "BOOT_FAIL",
        "CA": "CANCELLED",
        "CD": "COMPLETED",
        "DL": "DEADLINE",
        "F": "FAILED",
        "NF": "NODE_FAIL",
        "OOM": "OUT_OF_MEMORY",
        "PR": "PREEMPTED",
        "TO": "TIMEOUT",
    }
    _ARRAY_ELEMENT_RE = re.compile(
        r"^(\d+)_(?:\d+|\[[0-9,:%-]+\])$"
    )

    def _normalize_state(self, state: str) -> str:
        # sacct can annotate states, e.g. "CANCELLED by 1234", while squeue and
        # compact outputs may use two-letter aliases. The first token is the
        # scheduler state; any suffix is explanatory text.
        token = state.strip().split(maxsplit=1)[0].rstrip("+").upper()
        return self._STATE_ALIASES.get(token, token)

    def resource_directives(self, req: ResourceRequest) -> list[str]:
        directives: list[str] = []
        if req.job_name is not None:
            directives.append(f"--job-name={sanitize_slurm_job_name(req.job_name)}")
        if req.scheduler_tasks is not None:
            directives.append(f"--ntasks={req.scheduler_tasks}")
        directives.append(f"--cpus-per-task={req.cpus}")
        if req.mem_mb is not None:
            directives.append(f"--mem={req.mem_mb}M")
        if req.wall_time_seconds is not None:
            directives.append(f"--time={format_walltime(req.wall_time_seconds)}")
        if req.queue is not None:
            directives.append(f"--partition={req.queue}")
        if req.account is not None:
            directives.append(f"--account={req.account}")
        if req.stdout_path is not None:
            directives.append(f"--output={req.stdout_path}")
        if req.stderr_path is not None:
            directives.append(f"--error={req.stderr_path}")
        if req.array_size is not None:
            directives.append(f"--array=0-{req.array_size - 1}")
        directives.extend(req.extra_directives)
        return directives

    def render_job_script(
        self, directives: list[str], body: list[str], *, shell: str = "/bin/bash"
    ) -> str:
        lines = [f"#!{shell}"]
        lines.extend(f"#SBATCH {d}" for d in directives)
        lines.extend(body)
        return "\n".join(lines) + "\n"

    def submit_command(self, script_path: str, *, extra_args: Sequence[str] = ()) -> list[str]:
        return ["sbatch", *extra_args, script_path]

    def parse_submit_id(self, stdout: str) -> str:
        match = self._SUBMIT_RE.search(stdout)
        if not match:
            raise DialectError(f"unrecognized sbatch submit output: {stdout.strip()!r}")
        return match.group(1)

    def poll_command(self, job_ids: Sequence[str]) -> list[str]:
        return [
            "squeue",
            "--noheader",
            "--format=%i|%T|%M|%l|%N",
            "--jobs",
            ",".join(job_ids),
        ]

    def parse_poll(self, stdout: str) -> dict[str, SchedulerPhase]:
        result: dict[str, SchedulerPhase] = {}
        for line in stdout.splitlines():
            if not line.strip():
                continue
            fields = line.split("|")
            if len(fields) != 5 or not fields[0].strip() or not fields[1].strip():
                raise DialectError("malformed squeue row in scheduler poll output")
            job_id = fields[0].strip()
            if job_id in result:
                raise DialectError(
                    f"duplicate squeue row for scheduler job {job_id!r}"
                )
            result[job_id] = self.phase_for_state(fields[1].strip())
        return result

    def detail_command(self, job_id: str) -> list[str]:
        return [
            "sacct",
            "-X",
            "--array",
            "-j",
            job_id,
            "--parsable2",
            "--noheader",
            "--format=JobID,State,ExitCode,Elapsed,Timelimit,NodeList",
        ]

    def parse_exit_status(self, stdout: str) -> int | None:
        exit_codes: list[int] = []
        unknown_terminal = False
        seen_job_ids: set[str] = set()
        for line in stdout.splitlines():
            if not line.strip():
                continue
            fields = line.split("|")
            if len(fields) != 6 or not fields[0].strip() or not fields[1].strip():
                raise DialectError("malformed sacct row in scheduler detail output")
            job_id = fields[0].strip()
            if job_id in seen_job_ids:
                raise DialectError(
                    f"duplicate sacct row for scheduler job {job_id!r}"
                )
            seen_job_ids.add(job_id)
            if "." in job_id:
                continue
            raw_state = fields[1].strip()
            if self.phase_for_state(raw_state) is not SchedulerPhase.FINISHED:
                continue
            exit_code = self._terminal_exit_code(raw_state, fields[2].strip())
            if exit_code is None:
                unknown_terminal = True
            else:
                exit_codes.append(exit_code)
        if unknown_terminal or not exit_codes:
            return None
        for rc in exit_codes:
            if rc != 0:
                return rc
        return 0

    def poll_detail_command(self, job_ids: Sequence[str]) -> list[str]:
        return self.detail_command(",".join(job_ids))

    def parse_qstat_detail(self, stdout: str) -> dict[str, QstatDetail]:
        native: dict[str, QstatDetail] = {}
        aggregate_rows: dict[str, list[tuple[str, QstatDetail]]] = {}
        seen_job_ids: set[str] = set()
        for line in stdout.splitlines():
            if not line.strip():
                continue
            fields = line.split("|")
            if len(fields) != 6 or not fields[0].strip() or not fields[1].strip():
                raise DialectError("malformed sacct row in scheduler detail output")
            job_id, state, exit_code, elapsed, timelimit, nodelist = fields[:6]
            job_id = job_id.strip()
            if job_id in seen_job_ids:
                raise DialectError(
                    f"duplicate sacct row for scheduler job {job_id!r}"
                )
            seen_job_ids.add(job_id)
            if "." in job_id:
                continue
            raw_state = state.strip()
            phase = self.phase_for_state(raw_state)
            parsed_exit_code: int | None = None
            if phase is SchedulerPhase.FINISHED:
                parsed_exit_code = self._terminal_exit_code(
                    raw_state, exit_code.strip()
                )
            detail = QstatDetail(
                raw_state=raw_state,
                exec_host=nodelist.strip() or None,
                walltime_used=elapsed.strip() or None,
                walltime_limit=timelimit.strip() or None,
                exit_code=parsed_exit_code,
            )
            native[job_id] = detail
            array_match = self._ARRAY_ELEMENT_RE.match(job_id)
            master_id = array_match.group(1) if array_match else job_id
            aggregate_rows.setdefault(master_id, []).append((job_id, detail))

        result = dict(native)
        for master_id, rows in aggregate_rows.items():
            result[master_id] = self._aggregate_details(rows)
        return result

    def _terminal_exit_code(self, raw_state: str, value: str) -> int | None:
        """Return a fail-closed process-style rc from one terminal sacct row."""
        match = self._EXIT_CODE_RE.fullmatch(value)
        if match is None:
            return None
        status = int(match.group(1))
        signal_number = int(match.group(2))
        if status != 0:
            return status
        if signal_number != 0:
            return 128 + signal_number
        return 0 if self._normalize_state(raw_state) == "COMPLETED" else 1

    def _aggregate_details(
        self,
        rows: list[tuple[str, QstatDetail]],
    ) -> QstatDetail:
        """Collapse one Slurm allocation/array without row-order dependence."""
        by_phase: dict[
            SchedulerPhase, list[tuple[str, QstatDetail]]
        ] = {
            SchedulerPhase.RUNNING: [],
            SchedulerPhase.PENDING: [],
            SchedulerPhase.FINISHED: [],
        }
        for row in rows:
            by_phase[self.phase_for_state(row[1].raw_state)].append(row)

        for phase in (SchedulerPhase.RUNNING, SchedulerPhase.PENDING):
            if by_phase[phase]:
                return min(by_phase[phase], key=self._detail_sort_key)[1]

        terminal = by_phase[SchedulerPhase.FINISHED]
        failed = [
            row
            for row in terminal
            if self._normalize_state(row[1].raw_state) != "COMPLETED"
        ]
        selected = min(failed or terminal, key=self._detail_sort_key)[1]
        # Every expanded array row contributes terminal evidence. If any one
        # lacks a parseable exit status, the aggregate exit remains unknown;
        # a completed master or another known failure must not erase that
        # ambiguity merely because sacct emitted it earlier or later.
        if any(detail.exit_code is None for _job_id, detail in terminal):
            aggregate_exit_code = None
        else:
            nonzero = sorted(
                detail.exit_code
                for _job_id, detail in terminal
                if detail.exit_code not in (None, 0)
            )
            aggregate_exit_code = nonzero[0] if nonzero else 0
        return QstatDetail(
            raw_state=selected.raw_state,
            exec_host=selected.exec_host,
            walltime_used=selected.walltime_used,
            walltime_limit=selected.walltime_limit,
            exit_code=aggregate_exit_code,
        )

    def _detail_sort_key(
        self,
        row: tuple[str, QstatDetail],
    ) -> tuple[str, str, str, str, str]:
        job_id, detail = row
        return (
            self._normalize_state(detail.raw_state),
            job_id,
            detail.exec_host or "",
            detail.walltime_used or "",
            detail.walltime_limit or "",
        )

    def cancel_command(self, job_id: str) -> list[str]:
        return ["scancel", job_id]

    def hold_command(self, job_id: str) -> list[str]:
        return ["scontrol", "hold", job_id]

    def release_command(self, job_id: str) -> list[str]:
        return ["scontrol", "release", job_id]

    def phase_for_state(self, state: str) -> SchedulerPhase:
        normalized = self._normalize_state(state)
        if normalized in self._PENDING_STATES:
            return SchedulerPhase.PENDING
        if normalized in self._RUNNING_STATES:
            return SchedulerPhase.RUNNING
        if normalized in self._FINISHED_STATES:
            return SchedulerPhase.FINISHED
        raise DialectError(f"unknown SLURM job state {state!r}")

    # Terminal states in which SLURM itself ended or invalidated the job.
    # COMPLETED is the one clean terminal; FAILED is an ordinary nonzero
    # script exit whose rc flows through the exit-marker (and stays eligible
    # for vq's retry-on-failure), so neither classifies as abnormal.
    _ABNORMAL_FINISHED_STATES = _FINISHED_STATES - {"COMPLETED", "FAILED"}

    def abnormal_termination(self, state: str) -> str | None:
        # #414: sacct reported OUT_OF_MEMORY (ExitCode 0:125) while the
        # workspace exit-marker read 0, and vq recorded completed/exit-0. Any
        # of these accounting states is scheduler-attributed termination
        # evidence that must survive into vq's terminal classification, even
        # when the marker claims a clean exit. Annotations ("CANCELLED by
        # 1234") and compact aliases ("OOM") normalize like every other state.
        normalized = self._normalize_state(state)
        if normalized in self._ABNORMAL_FINISHED_STATES:
            return normalized
        return None


# Registry mapping the ``scheduler_dialect`` config value to its implementation.
# PBS Pro / SGE are reserved dialects whose resource/state matrices are
# sketched in the design doc but not yet coded.
_DIALECTS: dict[str, type[SchedulerDialect]] = {
    "torque": TorqueDialect,
    "slurm": SlurmDialect,
}


def dialect_for(name: str) -> SchedulerDialect:
    """Build the :class:`SchedulerDialect` for a ``scheduler_dialect`` value.

    Raises :class:`DialectError` for an unimplemented dialect (e.g. ``"sge"``),
    so a misconfigured scheduler host fails with a clear message rather than a
    silent wrong-flag mapping.
    """
    try:
        return _DIALECTS[name]()
    except KeyError:
        raise DialectError(
            f"unsupported scheduler_dialect {name!r}; implemented: {sorted(_DIALECTS)}"
        ) from None
