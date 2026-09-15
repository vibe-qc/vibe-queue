"""Resubmit a terminal-state job (v0.6.8).

`vq resubmit <jobid>` is the operator-driven counterpart to the
daemon's v0.5.30 auto-resume sibling logic. Both shapes write a new
JobSpec with ``parent_jobid`` pointing at the source; the differences:

  Auto-resume sibling (`daemon._auto_resume`)
    * fires unattended at daemon startup
    * only fires on ABORTED_BY_QUEUE-just-after-reboot + spec.recover_on_reboot
    * SAME workspace (cwd = dead.cwd) — for solver restart-from-disk
    * carries `recover_on_reboot=True` so it keeps resuming
    * carries retry_count forward

  `vq resubmit <jobid>` (this module)
    * operator-invoked, any terminal state
    * refuses non-terminal source states (RUNNING/PENDING/SUSPENDED)
    * FRESH workspace — deep-copies the source workspace (or extracts
      the archive tarball) into the new jobid's own jobs_dir entry.
      Matches the documented manual recovery recipe (fetch the
      workspace, vq submit -d that dir).
    * retry_count resets to 0 (fresh budget)
    * `recover_on_reboot` carries from source
    * optional per-flag overrides: cpus, mem_mb, wall_time_seconds,
      priority, retry_max, tags, job_name

Both ship the SUBMITTED event into the workspace's events.jsonl. The
new spec's workspace is independent of the source's, so the event log
on the new workspace is fresh (one continuous history per workspace,
not per lineage chain).

Why fresh-workspace as the default (not same-workspace):
  * matches handover.md / operations.md documented manual recipe
  * idempotent — rerunning doesn't inherit half-written outputs
  * concurrency-safe — no race against any process still holding cwd
  * solvers that need restart-from-disk already have
    `vq submit --auto-resume` for that purpose (daemon side)
"""
from __future__ import annotations

import getpass
import logging
import shutil
import socket
import tarfile
from dataclasses import dataclass
from pathlib import Path

from vq import events, paths, transport
from vq.config import HostConfig
from vq.ownership import check_owner
from vq.spec import TERMINAL_STATES, JobSpec, JobState
from vq.submit import new_jobid

log = logging.getLogger(__name__)


# v0.6.9: daemon-managed artifacts cleaned from the new workspace
# after the deep-copy / archive-extract. Anything not in this list
# (the user's inputs + any sidecar dirs the job's own command
# creates) survives untouched.
#
# Why each one:
# * stdout.log / stderr.log — appended by the dispatched command;
#   inheriting source content would interleave two jobs' output.
# * _vq/events.jsonl — append-only state-transition log; pre-v0.6.9
#   the source's history mixed with the new SUBMITTED event line.
# * _vq/exit-code — written by the command wrapper on exit. Stale source
#   value is overwritten by the new run, but cleaner to start fresh.
# * _vq/samples.jsonl — per-sample RSS/CPU stats; appended by the
#   watchdog. Mixing source + new run's samples confuses the
#   forensic trace.
# * _vq/resource-usage.json — terminal process accounting written by direct
#   and scheduler job wrappers. It describes the source run, never the retry.
# * _vq/workspace.tar.gz — pre-pack tarball some tooling stages.
#
# This is NOT a comprehensive scrub — anything else under _vq/
# (per-job markers, cached helper output, …) survives. The contract
# is "clean the slate for the daemon, not for the operator's
# bookkeeping."
_RESUBMIT_CLEAN_RELPATHS: tuple[str, ...] = (
    "stdout.log",
    "stderr.log",
    "_vq/events.jsonl",
    "_vq/exit-code",
    "_vq/samples.jsonl",
    "_vq/resource-usage.json",
)


def _clean_for_resubmit(workspace: Path) -> None:
    """Remove daemon-managed artifacts inherited from the source's
    workspace deep-copy so the new run starts with a clean slate.
    Best-effort — missing files are not an error."""
    for relpath in _RESUBMIT_CLEAN_RELPATHS:
        target = workspace / relpath
        try:
            target.unlink(missing_ok=True)
        except OSError as e:
            log.warning(
                "resubmit: could not unlink inherited artifact %s: %s "
                "(new run will inherit source content in this file)",
                target, e,
            )


@dataclass(frozen=True)
class ResubmitOverrides:
    """Per-flag overrides for `vq resubmit`. None means "inherit from
    the source spec."

    `tags` overrides instead of adding to the source's tags — symmetric
    with how `vq submit` treats tags (the operator declares the full
    set at submit time, not a delta). Pass None to inherit; pass an
    empty list to clear.
    """

    cpus: int | None = None
    scheduler_tasks: int | None = None
    mem_mb: int | None = None
    wall_time_seconds: int | None = None
    priority: int | None = None
    retry_max: int | None = None
    tags: list[str] | None = None
    job_name: str | None = None


def resubmit_local(
    jobid: str,
    *,
    overrides: ResubmitOverrides | None = None,
    queue_dir: Path | None = None,
    jobs_dir: Path | None = None,
    multi_user: bool = False,
) -> str:
    """Resubmit a local job by `jobid`. Returns the new jobid.

    Reads `<queue_dir>/<jobid>.json`, copies the workspace (or extracts
    the archive tarball), constructs a fresh JobSpec with
    `parent_jobid=source.id`, writes it, appends a SUBMITTED event,
    and returns the new jobid.

    ``multi_user`` (v0.6.40): the source spec is resolved from the
    per-user state dirs under ``/var/lib/vq/users/<uid>/``. The new
    job — its spec, workspace, and numeric-uid ``submitter`` — lands
    in that same user's tree, so the v0.6.35 multi-user dispatch
    gate accepts it.

    Raises:
      FileNotFoundError — source spec or workspace doesn't exist.
      ValueError — source state is non-terminal (RUNNING/PENDING/
        SUSPENDED); can't resubmit something still going.
    """
    overrides = overrides or ResubmitOverrides()
    if multi_user:
        # Resolve the source under /var/lib/vq/users/<uid>/queue/ and
        # derive the owning uid from the path (the trusted signal —
        # `users/` is root-owned). The new job goes in the same tree.
        source_path = paths.resolve_spec_path(jobid, multi_user=True)
        owner_uid: str | None = source_path.parent.parent.name
        queue_dir = paths.user_queue_dir(owner_uid)
        jobs_dir = paths.user_jobs_dir(owner_uid)
    else:
        owner_uid = None
        queue_dir = queue_dir or paths.queue_dir()
        jobs_dir = jobs_dir or paths.jobs_dir()
        queue_dir.mkdir(parents=True, exist_ok=True)
        jobs_dir.mkdir(parents=True, exist_ok=True)
        source_path = queue_dir / f"{jobid}.json"
        if not source_path.exists():
            raise FileNotFoundError(f"no such job: {jobid}")
    source = JobSpec.read(source_path)
    # ISO-2 (v0.8.25): ownership gate. Pre-fix, `vq resubmit` had no owner
    # check, so on a multi-user host user A could re-run user B's job — and
    # the resubmit re-stamps the source's submitter, so it would run AS B.
    # check_owner is a no-op in single-user mode and admin-aware in
    # multi-user (mirrors fetch.py / the kill + status gates).
    check_owner(source, multi_user=multi_user)

    if source.state not in TERMINAL_STATES:
        raise ValueError(
            f"cannot resubmit job {jobid}: state is {source.state.value} "
            f"(not terminal). `vq kill {jobid}` first if you want to "
            f"abort and then resubmit, or wait for the job to finish."
        )

    new_id = new_jobid()
    new_workspace = jobs_dir / new_id
    if new_workspace.exists():
        # uuid4 collision is statistically negligible (~2^-48 in a
        # 12-hex namespace) but defensive: the workspace MUST be fresh
        # so we don't accidentally write into a colliding job's dir.
        raise FileExistsError(
            f"new workspace already exists: {new_workspace} "
            "(uuid collision? rerun)"
        )

    if source.is_archived and source.archive_path:
        # Archived: extract the tarball into a fresh dir under jobs_dir.
        # The tarball's top-level is `<dest_dirname>/` (= the source's
        # `<job_name>-<jobid>` or just `<jobid>`); extract first and
        # then rename onto the new jobid's path.
        archive = Path(source.archive_path)
        if not archive.is_file():
            raise FileNotFoundError(
                f"archive for source job {jobid} not found at {archive} "
                "(spec.archive_path is stale)"
            )
        # Extract under jobs_dir, then rename the top-level dir to new_id.
        with tarfile.open(archive, mode="r:bz2") as tf:
            tf.extractall(jobs_dir, filter="data")
        extracted = jobs_dir / source.dest_dirname
        if not extracted.is_dir():
            raise FileNotFoundError(
                f"tarball {archive} did not contain expected dir "
                f"{source.dest_dirname}/; archive layout corrupt?"
            )
        extracted.rename(new_workspace)
        # Same cleanup as the deep-copy path: the archive tarball
        # holds the source run's logs/events/samples, which are
        # forensic noise in the new workspace.
        _clean_for_resubmit(new_workspace)
    else:
        # Not archived: deep-copy the live workspace.
        src = Path(source.cwd)
        if not src.is_dir():
            raise FileNotFoundError(
                f"workspace for source job {jobid} not found at {src} "
                "(was it cleaned up? Source spec exists but its files "
                "are gone — re-run from your original inputs instead)"
            )
        shutil.copytree(src, new_workspace)

    # v0.6.9: clean inherited daemon-managed artifacts so the new
    # run starts with a clean slate. Without this, the new
    # workspace's stdout.log / stderr.log / _vq/events.jsonl /
    # _vq/exit-code / _vq/samples.jsonl carry the SOURCE run's
    # forensics — the new daemon appends into the same files, so
    # the operator sees both runs' outputs interleaved + the
    # watchdog's samples.jsonl mixes two jobs' resource curves.
    # Inputs are NEVER cleaned — anything outside this set
    # survives the deep-copy untouched.
    _clean_for_resubmit(new_workspace)

    new_spec = JobSpec(
        id=new_id,
        command=list(source.command),
        cwd=str(new_workspace.resolve()),
        cpus=overrides.cpus if overrides.cpus is not None else source.cpus,
        scheduler_tasks=(
            overrides.scheduler_tasks
            if overrides.scheduler_tasks is not None
            else source.scheduler_tasks
        ),
        mem_mb=(
            overrides.mem_mb if overrides.mem_mb is not None else source.mem_mb
        ),
        wall_time_seconds=(
            overrides.wall_time_seconds
            if overrides.wall_time_seconds is not None
            else source.wall_time_seconds
        ),
        priority=(
            overrides.priority if overrides.priority is not None else source.priority
        ),
        recover_on_reboot=source.recover_on_reboot,
        retry_max=(
            overrides.retry_max
            if overrides.retry_max is not None
            else source.retry_max
        ),
        retry_count=0,  # fresh retry budget per the operator-rerun model
        parent_jobid=source.id,
        # Tags: explicit None means inherit; explicit list (even []) overrides.
        tags=(overrides.tags if overrides.tags is not None else list(source.tags)),
        job_name=(
            overrides.job_name
            if overrides.job_name is not None
            else source.job_name
        ),
        branch=source.branch,
        program=source.program,
        program_runtime_pin=(
            source.program_runtime_pin.model_copy(
                update={"resolved_git_sha": None},
                deep=True,
            )
            if source.program_runtime_pin is not None
            else None
        ),
        # Scheduler jobs should rerun on the same scheduler host. Runtime
        # scheduler fields (job id, qstat phase/detail) intentionally reset to
        # their defaults; the daemon will stamp fresh values after qsub.
        scheduler_target=source.scheduler_target,
        # v0.6.40: in multi-user mode the submitter must be the
        # numeric owning uid — the v0.6.35 dispatch gate rejects a
        # user@host submitter in a per-user queue dir.
        submitter=(
            owner_uid
            if multi_user
            else f"{getpass.getuser()}@{socket.gethostname()}"
        ),
        workspace_source=f"resubmit of {source.id}",
    )
    new_spec.write(queue_dir / f"{new_id}.json")
    events.append_event(
        new_workspace,
        events.EventKind.SUBMITTED,
        new_id,
        command=new_spec.command,
        cpus=new_spec.cpus,
        scheduler_tasks=new_spec.scheduler_tasks,
        mem_mb=new_spec.mem_mb,
        wall_time_seconds=new_spec.wall_time_seconds,
        priority=new_spec.priority,
        recover_on_reboot=new_spec.recover_on_reboot,
        retry_max=new_spec.retry_max,
        parent_jobid=source.id,
        program=new_spec.program,
        submitter=new_spec.submitter,
        workspace_source=new_spec.workspace_source,
        reason="vq resubmit",
    )
    log.info(
        "resubmit: %s -> %s (state=%s, fresh workspace at %s)",
        source.id, new_id, source.state.value, new_workspace,
    )
    return new_id


@dataclass(frozen=True)
class BulkResubmitResult:
    """Outcome of a `resubmit_state(...)` call.

    `pairs` is one entry per successfully-resubmitted source: the
    source jobid and the new jobid emitted for it. `errors` is one
    entry per source that matched the filter but couldn't be
    resubmitted (e.g. workspace gone, spec corrupt).
    """

    pairs: list[tuple[str, str]]  # (source_id, new_id) for each success
    errors: list[tuple[str, str]]  # (source_id, error_message)


def resubmit_state(
    states: list[JobState],
    *,
    overrides: ResubmitOverrides | None = None,
    queue_dir: Path | None = None,
    jobs_dir: Path | None = None,
    multi_user: bool = False,
    scheduler_target: str | None = None,
) -> BulkResubmitResult:
    """v0.6.10: bulk-resubmit every job whose state is in `states`.

    Scans the queue for matching specs, calls `resubmit_local` on
    each in sorted (source jobid) order so the output is
    deterministic, and aggregates results. A failure on one source
    does not abort the rest — failures land in `result.errors`.

    ``scheduler_target`` narrows the sweep to jobs previously submitted to that
    daemonless scheduler host, so ``vq resubmit pbs-cluster --state failed`` does not
    also resubmit local jobs that happen to live on the same driver daemon.

    Typical use: `resubmit_state([JobState.ABORTED_BY_QUEUE])` for
    "rerun everything the queue protectively aborted at the last
    daemon restart / host reboot."

    Empty `states` is a no-op returning an empty result rather than
    matching everything — matching every TERMINAL_STATES at once
    is rarely what an operator wants and would mass-resubmit jobs
    they explicitly killed. Operator must opt in to each state.
    """
    if not states:
        return BulkResubmitResult(pairs=[], errors=[])

    state_set = set(states)
    pairs: list[tuple[str, str]] = []
    errors: list[tuple[str, str]] = []

    # v0.6.40: in multi-user mode sweep every per-user queue dir;
    # resubmit_local resolves each job into its own per-user tree.
    if multi_user:
        spec_paths: list[Path] = []
        for user_dir in paths._all_user_dirs():
            qd = paths.user_queue_dir(user_dir.name)
            if qd.is_dir():
                spec_paths.extend(sorted(qd.glob("*.json")))
    else:
        queue_dir = queue_dir or paths.queue_dir()
        jobs_dir = jobs_dir or paths.jobs_dir()
        if not queue_dir.exists():
            return BulkResubmitResult(pairs=pairs, errors=errors)
        spec_paths = sorted(queue_dir.glob("*.json"))

    for spec_path in spec_paths:
        try:
            spec = JobSpec.read(spec_path)
        except Exception as e:
            errors.append((spec_path.stem, f"corrupt spec: {e}"))
            continue
        if spec.state not in state_set:
            continue
        if scheduler_target is not None and spec.scheduler_target != scheduler_target:
            continue
        try:
            new_id = resubmit_local(
                spec.id,
                overrides=overrides,
                queue_dir=queue_dir,
                jobs_dir=jobs_dir,
                multi_user=multi_user,
            )
        except (FileNotFoundError, FileExistsError, ValueError, OSError) as e:
            errors.append((spec.id, str(e)))
            continue
        pairs.append((spec.id, new_id))

    return BulkResubmitResult(pairs=pairs, errors=errors)


def _build_override_argv(overrides: ResubmitOverrides) -> list[str]:
    """Translate ResubmitOverrides into the CLI flags the remote
    `vq resubmit` accepts. Shared by `resubmit_remote` (single) +
    `resubmit_state_remote` (bulk)."""
    argv: list[str] = []
    if overrides.cpus is not None:
        argv.extend(["--cpus", str(overrides.cpus)])
    if overrides.scheduler_tasks is not None:
        argv.extend(["--scheduler-tasks", str(overrides.scheduler_tasks)])
    if overrides.mem_mb is not None:
        argv.extend(["--mem-mb", str(overrides.mem_mb)])
    if overrides.wall_time_seconds is not None:
        argv.extend(["--wall-time-seconds", str(overrides.wall_time_seconds)])
    if overrides.priority is not None:
        argv.extend(["--priority", str(overrides.priority)])
    if overrides.retry_max is not None:
        argv.extend(["--retry", str(overrides.retry_max)])
    if overrides.job_name is not None:
        argv.extend(["--job-name", overrides.job_name])
    if overrides.tags is not None:
        # Explicit empty list = clear: send a sentinel flag so the
        # remote can distinguish "inherit" from "clear". The CLI
        # treats `--clear-tags` as override-with-empty.
        if overrides.tags:
            for tag in overrides.tags:
                argv.extend(["--tag", tag])
        else:
            argv.append("--clear-tags")
    return argv


def resubmit_remote(
    host_cfg: HostConfig,
    jobid: str,
    *,
    overrides: ResubmitOverrides | None = None,
    target_host: str = "localhost",
) -> str:
    """Resubmit a job on a remote host. Returns the new jobid.

    Forwards as ``<remote_vq> resubmit <target_host> <jobid> [overrides]``
    over ssh. The remote vq does the actual work (read spec, copy
    workspace, write new spec); we just parse the printed jobid.
    """
    overrides = overrides or ResubmitOverrides()
    argv: list[str] = ["resubmit", target_host, jobid, *_build_override_argv(overrides)]
    proc = transport.run_remote_vq(host_cfg, *argv)
    new_id = proc.stdout.strip()
    if len(new_id) != 12 or not all(c in "0123456789abcdef" for c in new_id):
        raise transport.RemoteError(
            f"unexpected output from remote vq resubmit (expected 12-hex "
            f"jobid, got {new_id!r}); stderr: "
            f"{proc.stderr.strip() or '(empty)'}"
        )
    return new_id


def resubmit_state_remote(
    host_cfg: HostConfig,
    states: list[JobState],
    *,
    overrides: ResubmitOverrides | None = None,
    target_host: str = "localhost",
) -> tuple[list[str], str]:
    """v0.6.10: bulk-resubmit on a remote host.

    Forwards as ``<remote_vq> resubmit <target_host> --state STATE ...
    [overrides]``. Returns (new_ids, summary_text). The remote prints
    one new jobid per line on stdout and a summary line on stderr;
    we capture both and hand them back so the calling CLI can route
    them to its own stdout / stderr.
    """
    overrides = overrides or ResubmitOverrides()
    argv: list[str] = ["resubmit", target_host]
    for state in states:
        argv.extend(["--state", state.value])
    argv.extend(_build_override_argv(overrides))
    proc = transport.run_remote_vq(host_cfg, *argv)
    new_ids: list[str] = []
    for line in proc.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        if len(line) == 12 and all(c in "0123456789abcdef" for c in line):
            new_ids.append(line)
        else:
            raise transport.RemoteError(
                f"unexpected line in remote bulk-resubmit stdout "
                f"(expected 12-hex jobid per line, got {line!r}); "
                f"stderr: {proc.stderr.strip() or '(empty)'}"
            )
    return new_ids, proc.stderr
