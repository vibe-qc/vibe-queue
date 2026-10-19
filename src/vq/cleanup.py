"""Workspace archival + deletion (the ``vq cleanup`` verb).

Three operations on terminal-state jobs:

* **archive** — tar.bz2 the workspace to ``<archive_dir>/<jobid>.tar.bz2``,
  remove the workspace, set ``archived_at`` + ``archive_path`` on the
  spec. The spec stays in the queue so ``vq queue`` / ``vq status``
  still see the job (annotated "(archived)").
* **delete** — remove the spec, the workspace (if present), and the
  archive (if present). The job vanishes from ``vq queue``.
* **restore** — un-tar the archive back into the workspace, clear
  ``archived_at`` + ``archive_path``, remove the archive file.

Eligibility is gated by terminal-state (so a running / pending /
suspended job can never be archived or deleted out from under the
daemon) and by an ``--older-than`` age filter against
``finished_at``. The CLI defaults to dry-run; the caller passes
``execute=True`` to actually mutate the filesystem.
"""
from __future__ import annotations

import contextlib
import fcntl
import json
import logging
import os
import re
import shutil
import tarfile
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field

from vq import paths, spec_access, submit
from vq.spec import JobSpec, JobState, utcnow_iso, validate_job_id

log = logging.getLogger(__name__)

SchedulerWorkspaceReaper = Callable[[JobSpec], None]

_AGE_PATTERN = re.compile(r"^\s*(\d+)\s*([smhdw])\s*$")
_AGE_UNITS: dict[str, int] = {
    "s": 1,
    "m": 60,
    "h": 3600,
    "d": 86400,
    "w": 7 * 86400,
}


def parse_age(s: str) -> timedelta:
    """Parse ``"30d"`` / ``"4h"`` / ``"1w"`` / ``"600s"`` into a timedelta.

    Recognises a single integer + a unit suffix (``s`` seconds, ``m``
    minutes, ``h`` hours, ``d`` days, ``w`` weeks). No mixed forms; no
    fractional values; no implicit unit. Raises :class:`ValueError` on
    anything else, with a hint that ``--older-than`` expects e.g.
    ``30d``.
    """
    m = _AGE_PATTERN.match(s)
    if not m:
        raise ValueError(
            f"cannot parse age {s!r}; expected forms like '30d', '4h', "
            "'1w', '600s' (single integer + unit s/m/h/d/w)"
        )
    value, unit = int(m.group(1)), m.group(2)
    if value <= 0:
        raise ValueError(
            f"age must be positive (got {value!r}); "
            "use a positive integer like '30d'"
        )
    return timedelta(seconds=value * _AGE_UNITS[unit])


# v0.5.23: states that ``--archive-after-state`` / ``--delete-after-state``
# accept. Subset of TERMINAL_STATES — listed as plain strings to avoid
# the JobState import dependency in callers that just want to validate
# a CLI input.
VALID_STATE_NAMES: frozenset[str] = frozenset(
    {
        "completed",
        "failed",
        "killed",
        "interrupted",
        "oom_killed",
        "starved",
        "time_exceeded",
        "aborted_by_queue",
    }
)


def parse_state_age(s: str) -> tuple[str, timedelta]:
    """v0.5.23: parse ``"STATE:DUR"`` into (state_name, timedelta).

    Used by ``vq cleanup --archive-after-state failed:7d``. Raises
    ``ValueError`` for missing ``:`` separator, unknown state, or
    unparseable duration."""
    if ":" not in s:
        raise ValueError(
            f"--archive-after-state expects STATE:DUR (e.g. failed:7d); "
            f"got {s!r} (no ':' separator)"
        )
    state, age_str = s.split(":", 1)
    state = state.strip()
    if state not in VALID_STATE_NAMES:
        raise ValueError(
            f"unknown state {state!r}; valid states are "
            f"{sorted(VALID_STATE_NAMES)}"
        )
    return state, parse_age(age_str)


def _parse_iso(ts: str) -> datetime:
    """Tolerant ISO-8601 parser for spec timestamps (handles 'Z' suffix)."""
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    return datetime.fromisoformat(ts)


def _iter_specs(queue_dir: Path) -> Iterable[JobSpec]:
    if not queue_dir.exists():
        return
    for path in queue_dir.glob("*.json"):
        try:
            spec = JobSpec.read(path)
            if spec.id != path.stem:
                raise ValueError("spec filename/id mismatch")
            yield spec
        except Exception:
            # Skip corrupt specs; same convention as listing.list_jobs.
            continue


def _dir_size_bytes(path: Path) -> int:
    """Sum of ``stat().st_size`` over every regular file under ``path``.

    Symlinks aren't followed (matches ``du -sb`` semantics on a
    well-behaved tree). Missing files / permission errors are silently
    skipped — we're computing a friendly "freed disk" estimate, not an
    audit.
    """
    if not path.exists():
        return 0
    total = 0
    for child in path.rglob("*"):
        try:
            if child.is_file() and not child.is_symlink():
                total += child.stat().st_size
        except OSError:
            continue
    return total


def _workdir_size_bytes(spec: JobSpec, uid: str | None) -> int:
    """Bytes under a job's recorded workdir, if vq manages that path.

    Sized only when the record names a per-job directory under a root vq may
    remove it from, so accounting reports exactly the bytes ``--delete``
    would remove -- never bytes under an operator's own directory that vq
    happens to have been pointed at. Single-user mode includes the root the
    record itself names (:func:`vq.paths.workdir_roots_for_job`), so a
    workdir under a root the host has since been configured away from is
    reported rather than silently counted as zero.
    """
    if not spec.workdir:
        return 0
    roots = (
        paths.managed_user_workdir_roots(uid)
        if uid is not None
        else paths.workdir_roots_for_job(spec.workdir, spec.id)
    )
    recorded = Path(spec.workdir)
    for root in roots:
        candidates = {root / spec.id}
        with contextlib.suppress(OSError, RuntimeError, ValueError):
            candidates.add(root.resolve(strict=False) / spec.id)
        if recorded in candidates:
            return _dir_size_bytes(recorded)
    return 0


def _file_size_bytes(path: Path) -> int:
    try:
        return path.stat().st_size
    except OSError:
        return 0


def _human_bytes(n: int) -> str:
    """Render byte count as ``"1.2 GB"`` / ``"45 KB"`` / ``"512 B"``."""
    if n < 1024:
        return f"{n} B"
    units = ["KB", "MB", "GB", "TB"]
    f = float(n)
    for unit in units:
        f /= 1024.0
        if f < 1024 or unit == units[-1]:
            return f"{f:.1f} {unit}"
    return f"{n} B"  # unreachable


@dataclass(frozen=True)
class Candidate:
    """A spec that's eligible for an action, with size info pre-computed."""
    spec: JobSpec
    workspace_size: int
    archive_size: int
    # v0.6.43: the owning uid (a numeric-uid string) when the spec was
    # discovered under a per-user multi-user state dir; None in
    # single-user mode. Lets the caller archive/delete with that
    # user's own queue + archive dirs.
    uid: str | None = None
    # #63: bytes under the job's recorded workdir ($VQ_WORKDIR). Measured
    # from the *recorded* path, so it covers both the scratch root and the
    # pre-#63 root under the state root. It was missing entirely until #63,
    # which is why `vq cleanup` reported about 1 GB on a host whose workdirs
    # held 81 GB.
    workdir_size: int = 0

    @property
    def reclaimable(self) -> int:
        """Bytes that would be freed by deleting this job entirely."""
        return self.workspace_size + self.archive_size + self.workdir_size


def find_candidates_by_jobid(
    jobids: list[str],
    *,
    queue_dir: Path | None = None,
    require_archived: bool | None = None,
    multi_user: bool = False,
) -> tuple[list[Candidate], list[tuple[str, str]]]:
    """v0.6.23: like :func:`find_candidates` but selects specific
    jobids instead of filtering by age. Returns (candidates,
    errors_by_jobid). Each error is a (jobid, reason) tuple for
    jobids that don't exist, aren't terminal, or don't match the
    ``require_archived`` filter — operator sees per-jobid feedback
    rather than a silent skip.

    Used by `vq cleanup --archive --jobid X` / `--delete --jobid X`
    to act on specific jobs without an --older-than filter.

    ``multi_user`` (v0.6.43): resolve each jobid across the per-user
    state dirs and stamp the ``Candidate`` with its owning uid.
    """
    candidates: list[Candidate] = []
    errors: list[tuple[str, str]] = []
    for jobid in jobids:
        # v0.6.43: multi-user resolves the spec across the per-user
        # dirs and derives the owning uid from the resolved path.
        if multi_user:
            try:
                spec_path = paths.resolve_spec_path(jobid, multi_user=True)
            except FileNotFoundError:
                errors.append((jobid, "no such job"))
                continue
            owner_uid: str | None = spec_path.parent.parent.name
        else:
            qd = queue_dir or paths.queue_dir()
            spec_path = qd / f"{jobid}.json"
            owner_uid = None
            if not spec_path.exists():
                errors.append((jobid, "no such job"))
                continue
        try:
            spec = JobSpec.read(spec_path)
        except Exception as e:
            errors.append((jobid, f"corrupt spec: {e}"))
            continue
        if spec.id != jobid:
            errors.append((jobid, "corrupt spec: filename/id mismatch"))
            continue
        if not spec.is_terminal:
            errors.append(
                (jobid, f"state {spec.state.value} is not terminal")
            )
            continue
        if require_archived is True and not spec.is_archived:
            errors.append((jobid, "not archived (--restore needs an archive)"))
            continue
        if require_archived is False and spec.is_archived:
            errors.append((jobid, "already archived"))
            continue
        # Build the Candidate with the same shape find_candidates uses.
        ws = Path(spec.cwd)
        archive = Path(spec.archive_path) if spec.archive_path else None
        candidates.append(
            Candidate(
                spec=spec,
                workspace_size=_dir_size_bytes(ws),
                archive_size=_file_size_bytes(archive) if archive else 0,
                uid=owner_uid,
                workdir_size=_workdir_size_bytes(spec, owner_uid),
            )
        )
    return candidates, errors


def find_candidates(
    *,
    queue_dir: Path | None = None,
    older_than: timedelta | None = None,
    require_archived: bool | None = None,
    multi_user: bool = False,
) -> list[Candidate]:
    """Return terminal specs matching the filters, with size info.

    ``older_than``: only include specs whose ``finished_at`` is older
    than ``now - older_than``. ``None`` means no age filter.

    ``require_archived``: ``True`` to keep only archived specs (used by
    ``--restore``); ``False`` to keep only non-archived (used by
    ``--archive``); ``None`` for both (used by ``--delete`` and the
    no-action listing).

    ``multi_user`` (v0.6.43): sweep every per-user queue dir under
    ``/var/lib/vq/users/<uid>/`` and stamp each ``Candidate`` with
    its owning ``uid`` — otherwise ``vq cleanup`` finds nothing on a
    multi-user host.
    """
    now = datetime.now(UTC)
    cutoff = now - older_than if older_than is not None else None
    # v0.6.43: gather (spec, owning-uid) pairs. Multi-user sweeps
    # every per-user queue dir; single-user reads the one queue dir.
    spec_uid_pairs: list[tuple[JobSpec, str | None]] = []
    if multi_user:
        for user_dir in paths._all_user_dirs():
            uid = user_dir.name
            for spec in _iter_specs(paths.user_queue_dir(uid)):
                spec_uid_pairs.append((spec, uid))
    else:
        qd = queue_dir or paths.queue_dir()
        for spec in _iter_specs(qd):
            spec_uid_pairs.append((spec, None))
    out: list[Candidate] = []
    for spec, owner_uid in spec_uid_pairs:
        if not spec.is_terminal:
            continue
        if require_archived is True and not spec.is_archived:
            continue
        if require_archived is False and spec.is_archived:
            continue
        if cutoff is not None:
            if not spec.finished_at:
                # Terminal but no finished_at: shouldn't happen for v0.3+
                # specs, but if it does, treat as "infinitely old" so the
                # user can still clean it up. Conservative alternative
                # would be to skip; the verb is dry-run by default so
                # surfacing it is safer than hiding it.
                pass
            else:
                try:
                    finished = _parse_iso(spec.finished_at)
                except ValueError:
                    continue
                if finished > cutoff:
                    continue
        ws = Path(spec.cwd)
        archive = Path(spec.archive_path) if spec.archive_path else None
        out.append(
            Candidate(
                spec=spec,
                workspace_size=_dir_size_bytes(ws),
                archive_size=_file_size_bytes(archive) if archive else 0,
                uid=owner_uid,
                workdir_size=_workdir_size_bytes(spec, owner_uid),
            )
        )
    out.sort(key=lambda c: c.spec.finished_at or "")
    return out


def _spec_older_than(
    spec: JobSpec, cutoff: timedelta, now: datetime,
) -> bool:
    """v0.5.23 helper: True iff ``spec.finished_at`` is older than
    ``now - cutoff``. Used by ``run_auto_cleanup_pass`` for per-state
    filtering (where one call to ``find_candidates`` returns the
    superset and each candidate is age-checked against the relevant
    per-state threshold).

    Specs without ``finished_at`` (pre-v0.3 or corrupt) are treated as
    "infinitely old" — same conservative choice as ``find_candidates``;
    the auto-pass will at worst archive an already-stale spec, which
    is the desired outcome."""
    if not spec.finished_at:
        return True
    try:
        finished = _parse_iso(spec.finished_at)
    except ValueError:
        return False
    return finished <= now - cutoff


def archive_workspace(
    spec: JobSpec,
    *,
    archive_dir: Path | None = None,
    queue_dir: Path | None = None,
) -> Path:
    """Tar.bz2 the workspace, remove it, stamp the spec.

    Returns the archive path. Raises ``ValueError`` if the spec is
    non-terminal (refusing to archive an active job's workspace) or
    already archived. Raises ``FileNotFoundError`` if the workspace
    dir is missing — that's a sign the workspace was hand-removed,
    in which case the caller should ``--delete`` instead of ``--archive``.

    The tar ordering — write the archive first, write the updated spec
    second, remove the workspace last — keeps the failure modes safe:
    a crash mid-tar leaves the spec unchanged (no orphaned
    ``archived_at``); a crash between spec-write and workspace-remove
    leaves a redundant workspace (re-running ``--archive`` is a no-op
    that finalises by removing it).
    """
    if not spec.is_terminal:
        raise ValueError(
            f"cannot archive job {spec.id}: state is {spec.state.value}; "
            "only terminal-state jobs are eligible"
        )
    if spec.is_archived:
        raise ValueError(
            f"job {spec.id} is already archived at {spec.archive_path}"
        )
    qd = queue_dir or paths.queue_dir()
    ad = archive_dir or paths.archive_dir()
    ad.mkdir(parents=True, exist_ok=True)
    workspace = Path(spec.cwd)
    # v0.5.34: outer archive filename AND inner tarball top-level dir
    # both use ``dest_dirname`` (= ``<name>-<jobid>`` if job_name set,
    # else just ``<jobid>``). The outer filename makes archives
    # human-grep-able in the archive dir; the inner arcname feeds
    # straight into fetch_local / fetch_remote so the un-tarred dest
    # dir comes out human-readable too. Pre-v0.5.34 archives (no name)
    # naturally fall through to the same ``<jobid>.tar.bz2`` /
    # ``<jobid>/`` shape they always had.
    dest_name = spec.dest_dirname
    archive = ad / f"{dest_name}.tar.bz2"
    if not workspace.is_dir():
        raise FileNotFoundError(
            f"workspace for job {spec.id} not found at {workspace}; "
            "use `vq cleanup --delete` to remove the spec instead"
        )
    # Write to a tempfile-then-rename so a crash mid-tar can't leave a
    # half-written .tar.bz2 sitting where a future restore would try to
    # un-tar it.
    tmp = archive.with_suffix(archive.suffix + ".tmp")
    if tmp.exists():
        tmp.unlink()
    try:
        with tarfile.open(tmp, mode="w:bz2") as tf:
            tf.add(workspace, arcname=dest_name)
        tmp.replace(archive)
    except BaseException:
        # Clean the tmp file on any failure (including KeyboardInterrupt).
        with contextlib.suppress(OSError):
            tmp.unlink(missing_ok=True)
        raise
    # v0.8.13 *Gray's Transaction* (CLEAN-2): apply the archive stamps under
    # the per-spec lock with a fresh re-read, so a concurrent `vq status` /
    # `vq fetch` last_*_at stamp isn't clobbered. The (slow) tar above ran
    # OUTSIDE the lock — we hold it only across this spec read -> write.
    spec_file = qd / f"{spec.id}.json"
    with paths.spec_lock(spec_file):
        try:
            fresh = JobSpec.read(spec_file)
        except (OSError, ValueError):
            fresh = spec
        fresh.archived_at = utcnow_iso()
        fresh.archive_path = str(archive)
        fresh.write(spec_file)
    shutil.rmtree(workspace)
    return archive


def restore_workspace(
    spec: JobSpec,
    *,
    queue_dir: Path | None = None,
) -> Path:
    """Un-tar the archive back into the workspace, clear archive fields.

    Returns the workspace path. Raises ``ValueError`` if the spec
    isn't archived. Raises ``FileNotFoundError`` if the archive file
    is missing. Raises ``FileExistsError`` if the workspace dir
    already exists (that means a prior restore left it there or a
    re-dispatch re-created it; resolve manually rather than
    overwriting silently).
    """
    if not spec.is_archived or not spec.archive_path:
        raise ValueError(
            f"job {spec.id} is not archived; nothing to restore"
        )
    archive = Path(spec.archive_path)
    if not archive.is_file():
        raise FileNotFoundError(
            f"archive file for job {spec.id} not found at {archive}"
        )
    workspace = Path(spec.cwd)
    if workspace.exists():
        raise FileExistsError(
            f"workspace for job {spec.id} already exists at {workspace}; "
            "remove it manually before restoring"
        )
    qd = queue_dir or paths.queue_dir()
    workspace.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(archive, mode="r:bz2") as tf:
        # Tarball top-level is ``<dest_dirname>/`` (= ``<name>-<jobid>``
        # if job_name set when archived, else ``<jobid>``). For
        # pre-v0.5.34 archives — and v0.5.34+ archives of nameless jobs
        # — dest_dirname == jobid and the extracted path is already the
        # workspace dir, so the rename below is a no-op. For
        # v0.5.34+ named archives, extract puts content at
        # ``workspace.parent/<name>-<jobid>/`` and we rename it back
        # to ``workspace`` (= ``<jobs_dir>/<jobid>/``). The on-disk
        # workspace dir is always jobid-only — the name is purely a
        # human-readable artifact for archive filenames and fetch
        # destinations.
        tf.extractall(workspace.parent, filter="data")
    extracted = workspace.parent / spec.dest_dirname
    if extracted != workspace and extracted.is_dir():
        extracted.rename(workspace)
    # v0.8.13 *Gray's Transaction* (CLEAN-2): clear the archive fields under
    # the lock with a fresh re-read, after the (slow) extract above.
    spec_file = qd / f"{spec.id}.json"
    with paths.spec_lock(spec_file):
        try:
            fresh = JobSpec.read(spec_file)
        except (OSError, ValueError):
            fresh = spec
        fresh.archived_at = None
        fresh.archive_path = None
        fresh.write(spec_file)
    archive.unlink()
    return workspace


def delete_job(
    spec: JobSpec,
    *,
    queue_dir: Path | None = None,
    owner_uid: str | None = None,
) -> None:
    """Remove spec + workspace + workdir + archive (whichever exist).

    Refuses to act on non-terminal jobs (the daemon could still be
    writing to the workspace). Idempotent: missing workspace /
    archive / spec file is silently OK — the goal is "after this call,
    the job is gone."

    #63: the job's ``$VQ_WORKDIR`` goes too, which it did not before. On the
    two hosts that prompted the issue that is where over 99 % of the bytes
    were, so a delete that left it behind reclaimed about 1 % of what it
    reported. The removal is guarded by
    :func:`vq.paths.remove_managed_scratch_dir`: the recorded path must name
    this job (``<root>/<job id>``), the root is opened ``O_NOFOLLOW``, the
    entry must be a real directory rather than a symlink, and ownership must
    match. Anything else is left in place rather than deleted -- a workdir vq
    did not create is not vq's to remove.

    Which roots count is the one asymmetry worth naming. The workspace is
    removed at the path the spec records, unconditionally, because ``cwd``
    is only ever written by vq's own submit. ``workdir`` is only ever
    written by vq's own dispatch, so single-user mode now holds it to the
    same standard via :func:`vq.paths.workdir_roots_for_job` -- otherwise a
    host configured onto a third scratch root deleted the small workspace
    and left the large workdir, while reporting both as reclaimed. In
    multi-user mode the queued spec is user-writable, so the record stays
    evidence and the managed per-uid roots stay the authority.

    ``owner_uid`` names the multi-user owner whose trees are eligible; None
    means single-user.
    """
    if not spec.is_terminal:
        raise ValueError(
            f"cannot delete job {spec.id}: state is {spec.state.value}; "
            "only terminal-state jobs are eligible"
        )
    qd = queue_dir or paths.queue_dir()
    # v0.8.13 *Gray's Transaction* (CLEAN-2): remove the spec under the lock
    # so a concurrent status/fetch stamp (which re-reads under the same lock)
    # can't resurrect a half-deleted spec. Drop the lock sidecar too rather
    # than leaking one empty <spec>.lock per deleted job.
    spec_file = qd / f"{spec.id}.json"
    with paths.spec_lock(spec_file):
        spec_was_present = True
        try:
            fresh = spec_access.read_bounded_regular_spec(spec_file)
        except FileNotFoundError:
            spec_was_present = False
            fresh = spec
        if fresh.id != spec.id:
            raise ValueError(
                f"cannot delete job {spec.id}: spec filename/id mismatch"
            )
        if spec_was_present:
            verified = spec_access.read_bounded_regular_spec(spec_file)
            if verified.id != spec.id or verified.to_json() != fresh.to_json():
                raise ValueError(
                    f"cannot delete job {spec.id}: spec changed during cleanup"
                )
            fresh = verified
        if not fresh.is_terminal:
            raise ValueError(
                f"cannot delete job {fresh.id}: state is "
                f"{fresh.state.value}; only terminal-state jobs are eligible"
            )
        # A keyed submit publishes its spec before its durable tombstone.
        # Repair that crash gap before destroying the last evidence; if the
        # repair cannot be made durable, leave every job artifact intact.
        submit.ensure_idempotency_claim_for_spec(qd, fresh)
        workspace = Path(fresh.cwd)
        if workspace.is_dir():
            shutil.rmtree(workspace, ignore_errors=True)
        if fresh.workdir:
            refusal = paths.remove_managed_scratch_dir(
                fresh.workdir,
                fresh.id,
                roots=(
                    paths.managed_user_workdir_roots(owner_uid)
                    if owner_uid is not None
                    else paths.workdir_roots_for_job(fresh.workdir, fresh.id)
                ),
                expected_uid=(
                    int(owner_uid) if owner_uid is not None else os.geteuid()
                ),
            )
            if refusal is not None:
                log.info(
                    "cleanup: left job %s workdir %s in place: %s",
                    fresh.id,
                    fresh.workdir,
                    refusal,
                )
        if fresh.archive_path:
            archive = Path(fresh.archive_path)
            with contextlib.suppress(FileNotFoundError):
                archive.unlink()
        with contextlib.suppress(FileNotFoundError):
            spec_file.unlink()
    with contextlib.suppress(FileNotFoundError):
        paths.spec_lock_path(spec_file).unlink()


def _scheduler_remote_workspace_pending(spec: JobSpec) -> bool:
    """Is this spec's remote workspace ours to delete yet?

    Terminality alone is not enough. A spec can be terminal while its batch job
    is still running on the cluster: ``vq kill`` stamps the spec, and until
    v0.12.1 nothing cancelled a job the daemon was not tracking. Sweeping on
    terminality alone therefore ``rm -rf``-ed the working directory out from
    under a live calculation once the retention cutoff passed — losing the
    outputs *and* whatever the job wrote next.

    The narrow backstop: skip a spec that was **killed** without any evidence
    the cluster side ever ended — no exit code (the daemon never read an exit
    marker, so it never observed the job finish) and no fetch. Since v0.12.1
    ``vq kill`` issues its own ``qdel``, so this should be rare; it is the
    safety net for when that ``qdel`` could not be delivered. Every
    daemon-reconciled job is unaffected and still swept on schedule — the cost
    of being wrong here is some remote disk, which is far cheaper than deleting
    a live calculation's output.
    """
    if (
        not spec.is_terminal
        or spec.scheduler_target is None
        or spec.scheduler_remote_workspace_cleaned_at is not None
    ):
        return False
    killed_without_confirmed_end = (
        spec.state == JobState.KILLED
        and spec.exit_code is None
        and not spec.last_fetched_at
    )
    return not killed_without_confirmed_end


def cleanup_scheduler_remote_workspace(
    spec: JobSpec,
    reaper: SchedulerWorkspaceReaper,
    *,
    queue_dir: Path | None = None,
) -> bool:
    """Best-effort remote scheduler workspace cleanup with a spec breadcrumb.

    Returns True when a remote cleanup was attempted and stamped. Returns False
    for local jobs, live jobs, or scheduler specs already marked clean. Reaper
    exceptions propagate so callers can decide whether to retry later or block a
    destructive delete.
    """
    if not _scheduler_remote_workspace_pending(spec):
        return False
    reaper(spec)
    qd = queue_dir or paths.queue_dir()
    spec_file = qd / f"{spec.id}.json"
    with paths.spec_lock(spec_file):
        try:
            fresh = JobSpec.read(spec_file)
        except (OSError, ValueError):
            fresh = spec
        if not _scheduler_remote_workspace_pending(fresh):
            return False
        fresh.scheduler_remote_workspace_cleaned_at = utcnow_iso()
        fresh.write(spec_file)
    return True


def format_table(
    candidates: list[Candidate],
    *,
    action: str,
    dry_run: bool,
) -> str:
    """Render the eligibility / action plan as a fixed-width text table.

    ``action`` is the verb noun ("archive" / "delete" / "restore" /
    "list"). ``dry_run`` toggles the "would" prefix and the trailing
    summary line.
    """
    if not candidates:
        return f"({action}: no eligible jobs)"
    # #63: WORKDIR is its own column. Folding it into WORKSPACE would hide
    # the one number this table was missing -- the scratch tree is routinely
    # two orders of magnitude larger than the workspace beside it.
    header = ("ID", "STATE", "FINISHED (UTC)", "WORKSPACE", "WORKDIR", "ARCHIVE")
    rows: list[tuple[str, str, str, str, str, str]] = [header]
    total = 0
    for c in candidates:
        finished = (c.spec.finished_at or "")[:19].replace("T", " ")
        ws_label = (
            "(archived)" if c.workspace_size == 0 and c.spec.is_archived
            else (_human_bytes(c.workspace_size) if c.workspace_size else "-")
        )
        wd_label = _human_bytes(c.workdir_size) if c.workdir_size else "-"
        ar_label = _human_bytes(c.archive_size) if c.archive_size else "-"
        rows.append(
            (
                c.spec.id,
                c.spec.state.value,
                finished,
                ws_label,
                wd_label,
                ar_label,
            )
        )
        total += c.reclaimable
    widths = [max(len(r[i]) for r in rows) for i in range(len(rows[0]))]
    lines = [
        "  ".join(col.ljust(w) for col, w in zip(row, widths, strict=True))
        for row in rows
    ]
    if action in ("archive", "delete", "restore"):
        verb = "would " + action if dry_run else action + "d"
        workdir_total = sum(c.workdir_size for c in candidates)
        lines.append("")
        lines.append(
            f"{verb}: {len(candidates)} job(s); "
            f"reclaimable: {_human_bytes(total)} "
            f"(workdirs: {_human_bytes(workdir_total)})"
        )
        if action == "archive" and workdir_total:
            # --archive only tars the workspace; the workdir stays until the
            # job is deleted or the age sweep takes it. Saying so keeps the
            # "reclaimable" figure from reading as a promise this verb makes.
            lines.append(
                "note: --archive does not remove workdirs; "
                "--delete does, and `--auto-enable --workdir-max-age` "
                "sweeps them by age."
            )
    return "\n".join(lines)


# ======================================================================
# v0.5.17: scheduled / auto-cleanup
#
# Manual `vq cleanup --archive ... -x` works fine for ad-hoc cleanup, but
# unattended hosts grow workspaces without bound between manual sweeps.
# Auto-cleanup is a daemon-side opt-in: write a policy JSON to
# `<state_root>/auto-cleanup.json` and the daemon's main loop runs the
# sweep periodically using the same primitives the CLI verb uses.
#
# State file pattern mirrors drain.json / throttle.json: persisted to
# disk, mutated via a CLI verb, daemon reads it at every iteration
# (cheap stat). Cleanup runs aren't iteration-frequent (default interval
# 24h), so the daemon tracks ``last_run_at`` in the policy itself and
# only sweeps when the interval has elapsed.
# ======================================================================

AUTO_CLEANUP_FILENAME = "auto-cleanup.json"

_PositiveSeconds = Annotated[int, Field(strict=True, gt=0)]


class AutoCleanupPolicy(BaseModel):
    """Persisted auto-cleanup policy. Daemon reads this every iteration
    of its main loop; if ``enabled`` and the interval has elapsed since
    ``last_run_at``, runs an archive + delete sweep using the
    ``find_candidates`` / ``archive_workspace`` / ``delete_job``
    primitives the CLI verb already uses.

    Time fields are in seconds (not timedeltas) so JSON serialisation
    stays trivial; the CLI accepts user-friendly forms like "30d" /
    "24h" and converts via ``cleanup.parse_age``.
    """

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    enabled: bool = True
    """Master flag. ``vq cleanup --auto-disable`` clears the file
    rather than flipping this to False (saves one decision-point at
    the read side), but keeping the field allows future "paused
    auto-cleanup with state preserved" semantics."""

    archive_after_seconds: _PositiveSeconds | None = None
    """Archive terminal jobs whose ``finished_at`` is older than this.
    ``None`` disables the archive pass."""

    delete_after_seconds: _PositiveSeconds | None = None
    """Delete (spec + workspace + archive) jobs whose ``finished_at``
    is older than this. ``None`` disables the delete pass.
    Typically larger than ``archive_after_seconds`` so jobs get
    archived first and only deleted after the archive ages out."""

    interval_seconds: _PositiveSeconds = 24 * 60 * 60
    """Minimum gap between auto-cleanup sweeps. Default 24h: hourly
    sweeps would just churn the daemon for no benefit."""

    last_run_at: str | None = None
    """ISO timestamp of the last completed sweep. None until first run.
    Updated by ``run_auto_cleanup_pass`` after each successful pass."""

    set_at: str = Field(default_factory=utcnow_iso)
    """When the policy was set (vs last_run_at which moves with each
    sweep)."""

    reason: str | None = None
    """Optional free-text label, surfaced via ``--auto-status``."""

    archive_dir: str | None = None
    """v0.5.22: per-policy archive-dir override (absolute path).
    ``None`` defers to ``paths.archive_dir()`` (which itself respects
    ``$VQ_ARCHIVE_DIR``). Useful when a particular policy targets a
    different volume than the daemon's default — e.g. archives to a
    big secondary disk while the daemon state stays in ``~``."""

    archive_after_by_state: dict[str, _PositiveSeconds] = Field(
        default_factory=dict
    )
    """v0.5.23: per-terminal-state archive thresholds (seconds). Keys
    are JobState values: ``"completed"`` / ``"failed"`` / ``"killed"`` /
    ``"oom_killed"`` / ``"time_exceeded"`` / ``"starved"`` /
    ``"interrupted"`` / ``"aborted_by_queue"``. When a state is present
    here, its threshold wins over the global ``archive_after_seconds``;
    states not present fall back to the global. Use case: keep failed
    jobs around longer than completed jobs because forensics often
    matter for failures."""

    workdir_max_age_seconds: _PositiveSeconds | None = None
    """v0.6.54: sweep per-job workdirs (created at dispatch under
    ``<state>/workdirs/<jobid>/`` or ``users/<uid>/workdirs/<jobid>/``)
    whose owning job's ``finished_at`` is older than this. ``None``
    disables the workdir sweep (the v0.6.54 default — operator
    opts in like the archive/delete passes).

    Recommended starting value: ``1209600`` (14 days). Workdirs are
    operator-readable scratch (basis-opt convergence runs, vqfetch
    downloads, etc.); two weeks is enough for a chat to come back
    and inspect, short enough that a forgotten 30-element array
    sweep doesn't fill the disk. Set higher (or to ``None``) when
    workdirs hold paper-relevant runs that need long retention.

    Independent of the archive / delete passes — a job whose spec
    is archived (workspace tarred) can still have a workdir on
    disk; the workdir sweep cleans that too if it's old enough.
    ``--clean-tmp`` opt-in immediate cleanup (set at submit time
    via ``vq submit --clean-tmp``) bypasses this — those workdirs
    are gone the moment the job hits a terminal state."""

    delete_after_by_state: dict[str, _PositiveSeconds] = Field(
        default_factory=dict
    )
    """v0.5.23: per-state delete thresholds (seconds). Symmetric to
    ``archive_after_by_state``: per-state value wins over global
    ``delete_after_seconds``; states not present fall back."""


def auto_cleanup_policy_path() -> Path:
    # v0.6.39: in multi-user mode the auto-cleanup policy is a
    # daemon-wide setting shared by the root daemon and the (root)
    # CLI; it lives at the system root, alongside daemon.pid /
    # throttle.json — not under a single user's ~/.local/share/vq.
    if paths.is_multi_user():
        return paths.multi_user_root() / AUTO_CLEANUP_FILENAME
    return paths.state_root() / AUTO_CLEANUP_FILENAME


def read_auto_cleanup_policy() -> AutoCleanupPolicy | None:
    """Return the current policy or None. Corrupt files treated as
    no-policy (same conservative read-side stance as drain / throttle)."""
    path = auto_cleanup_policy_path()
    if not path.exists():
        return None
    try:
        with path.open() as f:
            data = json.load(f)
        return AutoCleanupPolicy.model_validate(data)
    except (OSError, ValueError):
        return None


def write_auto_cleanup_policy(policy: AutoCleanupPolicy) -> None:
    """Atomic tmpfile-then-rename write. Mirrors drain.write_drain_state."""
    path = auto_cleanup_policy_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(policy.model_dump_json(indent=2))
    tmp.replace(path)


def clear_auto_cleanup_policy() -> bool:
    """Remove the policy file. Returns True if removed, False if
    nothing to remove (idempotent)."""
    path = auto_cleanup_policy_path()
    if not path.exists():
        return False
    with contextlib.suppress(FileNotFoundError):
        path.unlink()
    return True


def should_run_auto_cleanup(policy: AutoCleanupPolicy, *, now: datetime | None = None) -> bool:
    """True iff ``policy.enabled`` and the interval has elapsed since
    ``last_run_at`` (or never run yet)."""
    if not policy.enabled:
        return False
    if policy.last_run_at is None:
        return True
    try:
        last = _parse_iso(policy.last_run_at)
    except ValueError:
        return True  # corrupt last_run_at: run anyway
    now = now or datetime.now(UTC)
    return (now - last).total_seconds() >= policy.interval_seconds


def _auto_cleanup_next_run_status(
    policy: AutoCleanupPolicy,
    *,
    now: datetime | None = None,
) -> str:
    """Human-readable next-run state for ``vq cleanup --auto-status``."""
    if not policy.enabled:
        return "next_run=paused"
    if policy.last_run_at is None:
        return "next_run=due"
    try:
        last = _parse_iso(policy.last_run_at)
    except ValueError:
        return "next_run=due (last_run_at invalid)"
    now = now or datetime.now(UTC)
    elapsed = (now - last).total_seconds()
    if elapsed >= policy.interval_seconds:
        return "next_run=due"
    next_run_at = last + timedelta(seconds=policy.interval_seconds)
    return f"next_run_at={next_run_at.isoformat()}"


def _cutoff_for(
    policy: AutoCleanupPolicy, kind: str, state: str,
) -> timedelta | None:
    """v0.5.23: resolve the effective archive/delete cutoff for a
    specific terminal state. Per-state override wins; otherwise the
    global ``<kind>_after_seconds`` applies; if both are unset, returns
    None (meaning "don't touch jobs in this state").

    ``kind`` must be "archive" or "delete"."""
    if kind == "archive":
        by_state = policy.archive_after_by_state
        global_seconds = policy.archive_after_seconds
    elif kind == "delete":
        by_state = policy.delete_after_by_state
        global_seconds = policy.delete_after_seconds
    else:
        raise ValueError(f"unknown cutoff kind: {kind!r}")
    if state in by_state:
        return timedelta(seconds=by_state[state])
    if global_seconds is not None:
        return timedelta(seconds=global_seconds)
    return None


def _has_any_cutoff(policy: AutoCleanupPolicy, kind: str) -> bool:
    """True if any state has a cutoff configured (either via the global
    knob or a per-state override). Used to short-circuit a pass that
    has nothing to do."""
    if kind == "archive":
        return (
            policy.archive_after_seconds is not None
            or bool(policy.archive_after_by_state)
        )
    if kind == "delete":
        return (
            policy.delete_after_seconds is not None
            or bool(policy.delete_after_by_state)
        )
    raise ValueError(f"unknown cutoff kind: {kind!r}")


def _scheduler_remote_workspace_cutoff(
    policy: AutoCleanupPolicy, state: str
) -> timedelta | None:
    """Retention threshold for duplicate scheduler-side workspaces.

    Prefer the archive cutoff because remote scheduler storage is duplicate once
    the driver has fetched the terminal workspace locally. If an operator sets
    only a delete cutoff, use that as the final safety net before the spec
    disappears.
    """
    archive_cutoff = _cutoff_for(policy, "archive", state)
    if archive_cutoff is not None:
        return archive_cutoff
    return _cutoff_for(policy, "delete", state)


def _scheduler_remote_workspace_pass(
    policy: AutoCleanupPolicy,
    qd: Path,
    now: datetime,
    counts: dict[str, int],
    scheduler_workspace_reaper: SchedulerWorkspaceReaper | None,
) -> None:
    if scheduler_workspace_reaper is None:
        return
    for c in find_candidates(queue_dir=qd, require_archived=None):
        if not _scheduler_remote_workspace_pending(c.spec):
            continue
        cutoff = _scheduler_remote_workspace_cutoff(policy, c.spec.state.value)
        if cutoff is None or not _spec_older_than(c.spec, cutoff, now):
            continue
        try:
            if cleanup_scheduler_remote_workspace(
                c.spec,
                scheduler_workspace_reaper,
                queue_dir=qd,
            ):
                counts["scheduler_workspaces_swept"] += 1
        except Exception as e:
            log.warning(
                "auto-cleanup scheduler workspace sweep failed for %s: %s",
                c.spec.id,
                e,
            )
            counts["scheduler_workspace_errors"] += 1


def _archive_and_delete_pass(
    policy: AutoCleanupPolicy,
    qd: Path,
    ad: Path | None,
    now: datetime,
    counts: dict[str, int],
    scheduler_workspace_reaper: SchedulerWorkspaceReaper | None = None,
    owner_uid: str | None = None,
) -> None:
    """v0.6.39: one archive+delete sweep over a single (queue dir,
    archive dir) pair. Factored out of :func:`run_auto_cleanup_pass`
    so that pass can run it once (single-user) or once per per-user
    state tree (multi-user). Mutates ``counts`` in place."""
    _scheduler_remote_workspace_pass(
        policy,
        qd,
        now,
        counts,
        scheduler_workspace_reaper,
    )
    if _has_any_cutoff(policy, "archive"):
        # require_archived=False so we don't re-archive already-archived
        # jobs (no time filter here; we filter per-state below).
        for c in find_candidates(queue_dir=qd, require_archived=False):
            cutoff = _cutoff_for(policy, "archive", c.spec.state.value)
            if cutoff is None or not _spec_older_than(c.spec, cutoff, now):
                continue
            try:
                archive_workspace(c.spec, queue_dir=qd, archive_dir=ad)
                counts["archived"] += 1
            except Exception as e:
                log.warning(
                    "auto-cleanup archive failed for %s: %s", c.spec.id, e
                )
                counts["archive_errors"] += 1
    if _has_any_cutoff(policy, "delete"):
        for c in find_candidates(queue_dir=qd, require_archived=None):
            cutoff = _cutoff_for(policy, "delete", c.spec.state.value)
            if cutoff is None or not _spec_older_than(c.spec, cutoff, now):
                continue
            try:
                if scheduler_workspace_reaper is not None and (
                    cleanup_scheduler_remote_workspace(
                        c.spec,
                        scheduler_workspace_reaper,
                        queue_dir=qd,
                    )
                ):
                    counts["scheduler_workspaces_swept"] += 1
                delete_job(c.spec, queue_dir=qd, owner_uid=owner_uid)
                counts["deleted"] += 1
            except Exception as e:
                log.warning(
                    "auto-cleanup delete failed for %s: %s", c.spec.id, e
                )
                counts["delete_errors"] += 1


def auto_cleanup_lock_path() -> Path:
    """Exclusive sweep lock beside the policy file."""
    return auto_cleanup_policy_path().with_name(AUTO_CLEANUP_FILENAME + ".lock")


@contextlib.contextmanager
def _auto_cleanup_sweep_lock():
    """Hold the sweep lock, or yield False when another sweep holds it.

    The daemon sweeps in process when idle and through a separate
    ``vq cleanup --auto-run-due`` process when jobs are active (#90); this
    keeps the two from ever sweeping at once.
    """
    path = auto_cleanup_lock_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_CLOEXEC, 0o600)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield False
            return
        try:
            yield True
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)


def run_auto_cleanup_pass(
    policy: AutoCleanupPolicy,
    *,
    queue_dir: Path | None = None,
    multi_user: bool = False,
    scheduler_workspace_reaper: SchedulerWorkspaceReaper | None = None,
) -> dict[str, int]:
    """Execute one auto-cleanup sweep, unless another sweep holds the lock.

    A sweep skipped for the lock returns ``{"locked": 1}`` and leaves
    ``last_run_at`` alone, so the policy stays due. See
    :func:`_run_auto_cleanup_pass_locked` for what a sweep does.
    """
    with _auto_cleanup_sweep_lock() as held:
        if not held:
            log.info("auto-cleanup pass skipped: another sweep holds the lock")
            return {"locked": 1}
        return _run_auto_cleanup_pass_locked(
            policy, queue_dir=queue_dir, multi_user=multi_user,
            scheduler_workspace_reaper=scheduler_workspace_reaper,
        )


def run_due_auto_cleanup(
    *,
    multi_user: bool = False,
    scheduler_workspace_reaper: SchedulerWorkspaceReaper | None = None,
) -> dict[str, int] | None:
    """Run the persisted policy's sweep if it is due; None when it is not."""
    policy = read_auto_cleanup_policy()
    if policy is None or not should_run_auto_cleanup(policy):
        return None
    return run_auto_cleanup_pass(
        policy, multi_user=multi_user,
        scheduler_workspace_reaper=scheduler_workspace_reaper,
    )


def _run_auto_cleanup_pass_locked(
    policy: AutoCleanupPolicy,
    *,
    queue_dir: Path | None = None,
    multi_user: bool = False,
    scheduler_workspace_reaper: SchedulerWorkspaceReaper | None = None,
) -> dict[str, int]:
    """Execute one auto-cleanup sweep.

    Two passes per call:
    1. Archive terminal jobs older than ``archive_after_seconds`` (or
       the per-state override from ``archive_after_by_state``). Skip
       if neither is set for any state.
    2. Delete terminal jobs older than ``delete_after_seconds`` (or
       the per-state override). Skip if neither is set for any state.

    Per-state cutoffs (v0.5.23) let a single policy archive completed
    jobs after 90d but failed jobs after 7d (or any combination): the
    per-state value wins for that state; other states fall back to the
    global ``<kind>_after_seconds``.

    Returns a dict with counts: ``archived``, ``deleted``,
    ``archive_errors``, ``delete_errors``, ``workdirs_swept``,
    ``workdir_errors``, ``scheduler_workspaces_swept``, and
    ``scheduler_workspace_errors``. The policy's
    ``last_run_at`` is stamped + persisted regardless of outcome
    (we don't want a chronically-failing sweep to retry every loop
    iteration).

    ``multi_user`` (v0.6.39): sweep every per-user state tree under
    ``/var/lib/vq/users/<uid>/`` instead of the single queue dir.
    Each user's workspaces archive into their own
    ``user_archive_dir``. Without this the daemon's auto-cleanup
    silently never touches multi-user job state — terminal jobs
    accumulate on disk forever.
    """
    counts = {
        "archived": 0, "deleted": 0,
        "archive_errors": 0, "delete_errors": 0,
        # v0.6.54: workdir sweep counters.
        "workdirs_swept": 0, "workdir_errors": 0,
        # Scheduler remote workspaces are duplicate once the driver has fetched
        # terminal results locally. Sweep them at archive age, before final
        # spec deletion would lose the deterministic remote path.
        "scheduler_workspaces_swept": 0,
        "scheduler_workspace_errors": 0,
    }
    now = datetime.now(UTC)

    # Archive pass first (so jobs about to be deleted have been
    # archived). v0.5.23: filter per-state inside the helper.
    if multi_user:
        # v0.6.39: each per-user state tree is its own (queue,
        # archive) pair. policy.archive_dir — a single-user override
        # — does not apply across users; each user archives into
        # their own user_archive_dir.
        for user_dir in paths._all_user_dirs():
            uid = user_dir.name
            _archive_and_delete_pass(
                policy,
                paths.user_queue_dir(uid),
                paths.user_archive_dir(uid),
                now,
                counts,
                scheduler_workspace_reaper,
                owner_uid=uid,
            )
    else:
        qd = queue_dir or paths.queue_dir()
        # v0.5.22: policy.archive_dir overrides the default (which
        # itself respects $VQ_ARCHIVE_DIR). None = use the default.
        ad = Path(policy.archive_dir).expanduser() if policy.archive_dir else None
        _archive_and_delete_pass(
            policy,
            qd,
            ad,
            now,
            counts,
            scheduler_workspace_reaper,
        )

    # v0.6.54: stale-workdir sweep. Independent of archive/delete —
    # a workdir can outlive a deleted spec (rare but possible if
    # the spec was deleted by hand while the workdir lingered) and
    # vice versa. Disabled by default (workdir_max_age_seconds=None).
    if policy.workdir_max_age_seconds is not None:
        _sweep_stale_workdirs(
            policy.workdir_max_age_seconds,
            now,
            counts,
            multi_user=multi_user,
        )

    # #74: opt-in runtime-generation retention rides the same sweep. It is
    # scheduled per program with `vq admin runtime-retention --schedule` and
    # skips any program whose update ownership is held.
    try:
        from vq import config as config_module
        from vq import runtime_retention

        if runtime_retention.schedule_path().exists():
            retention = runtime_retention.run_scheduled(config_module.load_config())
            counts["runtime_generations_removed"] = sum(
                len(result.get("removed", [])) for result in retention.values()
                if isinstance(result, dict)
            )
            counts["runtime_retention_skipped"] = sum(
                1 for result in retention.values()
                if isinstance(result, dict) and "skipped" in result
            ) + (1 if "error" in retention else 0)
    except Exception as e:  # noqa: BLE001 - retention must not break the sweep
        log.warning("auto-cleanup runtime retention failed: %s", e)
        counts["runtime_retention_skipped"] = counts.get("runtime_retention_skipped", 0) + 1

    # Leftovers of interrupted transactions and transfers, at least a week
    # old; see vq.leftovers for what is and is never considered.
    try:
        from vq import config as config_module
        from vq import leftovers

        swept = leftovers.sweep(config_module.load_config(), multi_user=multi_user)
        counts.update({k: v for k, v in swept.items() if v})
    except Exception as e:  # noqa: BLE001 - must not break the sweep
        log.warning("auto-cleanup leftover sweep failed: %s", e)

    # Stamp last_run_at and persist.
    policy.last_run_at = utcnow_iso()
    write_auto_cleanup_policy(policy)
    log.info(
        "auto-cleanup pass: archived=%d deleted=%d archive_errors=%d "
        "delete_errors=%d workdirs_swept=%d workdir_errors=%d "
        "scheduler_workspaces_swept=%d scheduler_workspace_errors=%d",
        counts["archived"], counts["deleted"],
        counts["archive_errors"], counts["delete_errors"],
        counts["workdirs_swept"], counts["workdir_errors"],
        counts["scheduler_workspaces_swept"], counts["scheduler_workspace_errors"],
    )
    return counts


def _stamp_workdir_swept(spec_path: Path) -> None:
    """CLEAN-3: record the age-sweep on the owning spec (``workdir_swept_at``)
    under the per-spec lock so a concurrent ``vq status`` / ``vq fetch`` stamp
    isn't clobbered. Best-effort — a failure to stamp must not break the sweep
    (the workdir is already gone)."""
    with contextlib.suppress(OSError, ValueError), paths.spec_lock(spec_path):
        fresh = JobSpec.read(spec_path)
        fresh.workdir_swept_at = utcnow_iso()
        fresh.write(spec_path)


def _single_user_sweep_roots(
    queue_dir: Path | None = None,
) -> tuple[tuple[Path, ...], tuple[Path, ...]]:
    """The workdir roots the age sweep walks: ``(configured, derived)``.

    The configured roots (:func:`vq.paths.managed_workdir_roots`) come first.
    Then every *other* root this host's own job records still name:
    ``workdir_root`` and ``scratch_root`` are operator configuration, so point
    a host at a third root and the second is in neither the current nor the
    pre-#63 pair, and a root-driven sweep stopped seeing the very directories
    it exists to remove while ``workdirs_swept`` reported 0.

    **The two are not swept with the same authority, and that is the point.**
    A configured root is one the operator is pointing this host at now, so
    enumerating it and removing what looks like a spent workdir is what they
    asked for -- including an *orphan*, a directory whose owning spec is gone,
    which is aged by its own mtime because there is no job left to endanger. A
    derived root is a directory this host merely *used to* write into. It may
    since have been repurposed, and an operator's own directory under it is
    indistinguishable from an orphan: ``validate_job_id`` accepts ``notes``,
    ``README`` and ``my-data``, and the mtime of a directory nobody has added
    to in a month is old. So under a derived root the sweep removes only what
    a spec still records, and never an orphan. The residue -- an orphan under
    a root nothing configures any more -- is an operator's to remove, and
    ``docs/operations.md`` says so rather than implying a sweep that cannot
    safely happen.

    Single-user only: a multi-user spec is user-writable, so a root named by
    one is not authority for anything.
    """
    configured = list(paths.managed_workdir_roots())
    known = set(configured)
    derived: list[Path] = []
    for spec in _iter_specs(queue_dir or paths.queue_dir()):
        if not spec.workdir:
            continue
        for root in paths.workdir_roots_for_job(spec.workdir, spec.id):
            if root not in known:
                known.add(root)
                derived.append(root)
    return tuple(configured), tuple(derived)


def _sweep_stale_workdirs(
    max_age_seconds: int,
    now: datetime,
    counts: dict[str, int],
    *,
    multi_user: bool,
) -> None:
    """v0.6.54: rmtree per-job workdirs older than ``max_age_seconds``.

    Iterates every workdir root the daemon manages (#63: the current
    scratch one *and* the pre-#63 one under the state root; single-user also
    every root this host's own job records still name, see
    :func:`_single_user_sweep_roots`; multi-user: the managed pair per uid),
    checks each per-job workdir's mtime against ``now - max_age_seconds``,
    and removes the stale ones.

    More than the current root, because nothing moves when the root does: an
    operator who splits scratch out of the state root -- or who moves it
    again afterwards -- still has every existing job's workdir under the
    previous one, and a sweep that only looked at the new root would leave
    exactly the bytes #63 was opened about.

    A directory whose name is not a job id is not ours: it is skipped, not
    swept and not an error. That guard is weak on its own -- ``validate_job_id``
    accepts ``notes`` and ``README`` -- so it is not what keeps an operator's
    directory safe. What does is where the sweep is willing to remove an
    orphan at all: under a root the operator configures now, and never under
    one this host merely used to write into. See
    :func:`_single_user_sweep_roots`.

    Removal goes through :func:`vq.paths.remove_managed_scratch_dir`, so a
    symlinked root, a symlinked or non-directory entry, or an entry owned by
    another uid is refused rather than followed.

    Terminal-gated (v0.8.9, CLEAN-1): a workdir is swept only if its
    owning job's spec is in a TERMINAL state AND that spec's
    ``finished_at`` is older than the cutoff — the same gate the
    archive / delete passes use via ``_spec_older_than``. A workdir
    whose spec is still PENDING / RUNNING / SUSPENDED is NEVER removed.

    The previous mtime-only heuristic was unsafe: a directory's mtime
    changes only when an entry is added/removed *in that directory*, not
    when a file already inside it (or in a subdir) is appended to. A
    long-running job that writes into a fixed set of files therefore
    leaves its workdir mtime frozen at dispatch time — so a multi-week
    SCF / basis-opt loop would cross the cutoff and have its *live*
    scratch rmtree'd out from under it. Directory mtime is kept only as
    a fallback for *orphan* workdirs that have no spec (the owning job
    was already cleaned up), where there is no live job to endanger.

    Errors are logged but never propagate — a single bad workdir
    must not break the rest of the sweep.
    """
    cutoff = timedelta(seconds=max_age_seconds)
    cutoff_ts = (now - cutoff).timestamp()

    def _read_spec_or_none(spec_path: Path) -> JobSpec | None:
        try:
            return JobSpec.read(spec_path)
        except (OSError, ValueError):
            return None

    def _sweep_one_root(
        root: Path,
        spec_path_for: Callable[[str], Path],
        *,
        expected_uid: int,
        allow_orphans: bool = True,
    ) -> None:
        try:
            entries = sorted(root.iterdir())
        except FileNotFoundError:
            return
        except OSError as e:
            log.warning(
                "auto-cleanup workdir-sweep: %s could not be listed: %s",
                root, e,
            )
            counts["workdir_errors"] += 1
            return
        for entry in entries:
            # follow_symlinks=False: a symlinked entry is not a managed
            # workdir, and resolving it here would let a planted link decide
            # which job's age gate applies to a directory elsewhere.
            try:
                if not entry.is_dir() or entry.is_symlink():
                    continue
            except OSError:
                continue
            jobid = entry.name
            try:
                validate_job_id(jobid)
            except ValueError:
                # Not a job id, so not a workdir of ours. #63 made this root
                # operator-configurable, which means it may be a shared
                # scratch volume that holds other people's directories. This
                # used to raise out of the whole pass -- and the daemon still
                # stamped ``last_run_at``, so archive, delete and the sweep
                # all stopped until a human removed the directory.
                log.debug(
                    "auto-cleanup workdir-sweep: %s is not a job directory; "
                    "leaving it alone",
                    entry,
                )
                continue
            spec = _read_spec_or_none(spec_path_for(jobid))
            if (
                spec is not None
                and not allow_orphans
                and (not spec.workdir or Path(spec.workdir) != entry)
            ):
                # A derived root, and a spec that shares this directory's
                # name but records its workdir somewhere else. The name is
                # not the record: legacy ids such as `data` are valid, and a
                # retried job's spec names only its latest attempt's
                # directory. Nothing says vq created *this* one.
                log.debug(
                    "auto-cleanup workdir-sweep: %s under the former root %s "
                    "is not the workdir job %s records; leaving it to the "
                    "operator",
                    entry, root, jobid,
                )
                continue
            if spec is not None:
                # Owned by a known job: never touch a non-terminal job's
                # live scratch, and age terminal ones by finished_at.
                if not spec.is_terminal:
                    continue
                if not _spec_older_than(spec, cutoff, now):
                    continue
            elif not allow_orphans:
                # A derived root: somewhere this host used to write, which
                # the operator may since have repurposed. Without a spec
                # there is nothing saying vq created this directory, and an
                # operator's own is indistinguishable from an orphan --
                # `validate_job_id` accepts `notes` and `README`, and an
                # untouched directory's mtime is old. Authority here comes
                # from the record, not from the listing.
                log.debug(
                    "auto-cleanup workdir-sweep: %s under the former root %s "
                    "has no spec; leaving it to the operator",
                    entry, root,
                )
                continue
            else:
                # Orphan workdir (no spec — owning job already cleaned
                # up): no live job to endanger; fall back to dir mtime.
                try:
                    mtime = entry.stat().st_mtime
                except OSError as e:
                    log.warning(
                        "auto-cleanup workdir-sweep: stat failed for %s: %s",
                        entry, e,
                    )
                    counts["workdir_errors"] += 1
                    continue
                if mtime > cutoff_ts:
                    continue  # too young
            refusal = paths.remove_managed_scratch_dir(
                entry,
                jobid,
                roots=(root,),
                expected_uid=expected_uid,
            )
            if refusal is not None:
                log.warning(
                    "auto-cleanup workdir-sweep: refused %s: %s",
                    entry, refusal,
                )
                counts["workdir_errors"] += 1
                continue
            counts["workdirs_swept"] += 1
            log.info(
                "auto-cleanup workdir-sweep: removed %s (older than %ds)",
                entry, max_age_seconds,
            )
            # CLEAN-3: leave a breadcrumb on the owning spec (if any —
            # orphan workdirs have none) so a later `vq fetch --workdir`
            # explains the workdir was age-swept, not removed by
            # --clean-tmp. Best-effort + lock-safe.
            if spec is not None:
                _stamp_workdir_swept(spec_path_for(jobid))

    if multi_user:
        for user_dir in paths._all_user_dirs():
            uid = user_dir.name
            try:
                expected_uid = int(uid)
            except ValueError:
                # _all_user_dirs only yields canonical numeric names, so this
                # is unreachable; refusing rather than guessing an owner keeps
                # it that way if that ever changes.
                continue
            for root in paths.managed_user_workdir_roots(uid):
                _sweep_one_root(
                    root,
                    lambda jid, uid=uid: paths.user_spec_path(uid, jid),
                    expected_uid=expected_uid,
                )
    else:
        configured, derived = _single_user_sweep_roots()
        for root in configured:
            _sweep_one_root(
                root, paths.spec_path, expected_uid=os.geteuid()
            )
        for root in derived:
            _sweep_one_root(
                root, paths.spec_path, expected_uid=os.geteuid(),
                allow_orphans=False,
            )


def format_auto_cleanup_status() -> str:
    """Human-readable summary for ``vq cleanup --auto-status``."""
    policy = read_auto_cleanup_policy()
    if policy is None:
        return "auto-cleanup: disabled (no policy set; manual `vq cleanup` only)"
    parts = ["auto-cleanup: ENABLED" if policy.enabled else "auto-cleanup: PAUSED"]
    if policy.archive_after_seconds is not None:
        parts.append(f"archive_after={policy.archive_after_seconds}s")
    if policy.delete_after_seconds is not None:
        parts.append(f"delete_after={policy.delete_after_seconds}s")
    if policy.workdir_max_age_seconds is not None:
        parts.append(f"workdir_max_age={policy.workdir_max_age_seconds}s")
    parts.append(f"interval={policy.interval_seconds}s")
    if policy.last_run_at is not None:
        parts.append(f"last_run_at={policy.last_run_at}")
    else:
        parts.append("last_run=never")
    parts.append(_auto_cleanup_next_run_status(policy))
    if policy.archive_dir:
        # v0.5.22: report per-policy archive_dir override if set
        parts.append(f"archive_dir={policy.archive_dir}")
    # v0.5.23: per-state overrides
    for st, secs in sorted(policy.archive_after_by_state.items()):
        parts.append(f"archive_after[{st}]={secs}s")
    for st, secs in sorted(policy.delete_after_by_state.items()):
        parts.append(f"delete_after[{st}]={secs}s")
    if policy.reason:
        parts.append(f"reason: {policy.reason}")
    return " | ".join(parts)


# ---------------------------------------------------------------------------
# Orphaned remote scheduler workspaces (#91)
# ---------------------------------------------------------------------------

DEFAULT_ORPHAN_MIN_AGE_SECONDS = 14 * 24 * 60 * 60


@dataclass
class OrphanWorkspace:
    """A remote job workspace no local spec records, and whether it may go."""

    job_id: str
    path: str
    age_seconds: float
    owner: str
    """``"this driver"``, ``"unknown"`` (unstamped) or ``"another driver"``."""
    scheduler_job_ids: tuple[str, ...]
    removable: bool
    reason: str


def local_job_ids(*, multi_user: bool = False) -> set[str]:
    """Every job id this driver still has a spec for, terminal or not."""
    queue_dirs = (
        [paths.user_queue_dir(user.name) for user in paths._all_user_dirs()]
        if multi_user else [paths.queue_dir()]
    )
    return {
        spec_file.stem
        for queue in queue_dirs if queue.is_dir()
        for spec_file in queue.glob("*.json")
    }


def find_orphan_workspaces(
    dispatcher: Any,
    *,
    local_ids: set[str],
    driver_id: str,
    older_than_seconds: float = DEFAULT_ORPHAN_MIN_AGE_SECONDS,
    include_unowned: bool = False,
    now: float | None = None,
) -> list[OrphanWorkspace]:
    """Workspaces under the host's jobs root that no local spec records.

    Removable only when stamped by this driver (or unstamped with
    ``include_unowned``), older than ``older_than_seconds``, and not queued or
    running according to the scheduler. A workspace stamped by another driver
    is never removable: that driver may not have fetched its results yet.
    Raises when the scheduler cannot be asked about a candidate's job.
    """
    from vq.scheduler_dialect import SchedulerPhase
    from vq.scheduler_dispatch import SchedulerHandle

    now = time.time() if now is None else now
    orphans: list[OrphanWorkspace] = []
    for entry in dispatcher.list_job_workspaces():
        if entry.job_id in local_ids:
            continue
        age = max(0.0, now - entry.modified)
        if entry.driver_id == driver_id:
            owner, removable, reason = "this driver", True, "no local spec"
        elif entry.driver_id is None:
            owner = "unknown"
            removable = include_unowned
            reason = ("no local spec; unstamped" if include_unowned
                      else "unstamped; needs --include-unowned")
        else:
            owner, removable, reason = (
                "another driver", False, "stamped by another driver",
            )
        if removable and age < older_than_seconds:
            removable, reason = False, "younger than --older-than"
        orphans.append(OrphanWorkspace(
            job_id=entry.job_id, path=entry.path, age_seconds=age, owner=owner,
            scheduler_job_ids=entry.scheduler_job_ids, removable=removable,
            reason=reason,
        ))
    asked = sorted({
        job for orphan in orphans if orphan.removable
        for job in orphan.scheduler_job_ids
    })
    if asked:
        evidence = dispatcher.poll_with_evidence(
            [SchedulerHandle(job_id=job, remote_workspace="") for job in asked]
        )
        live = {
            job for job, phase in evidence.phases.items()
            if phase in (SchedulerPhase.PENDING, SchedulerPhase.RUNNING)
        }
        for orphan in orphans:
            if orphan.removable and live.intersection(orphan.scheduler_job_ids):
                orphan.removable = False
                orphan.reason = "the scheduler still has this job queued or running"
    return orphans


def remove_orphan_workspace(dispatcher: Any, orphan: OrphanWorkspace) -> None:
    """Remove one orphan through the dispatcher's guarded, Lustre-aware path."""
    from vq.scheduler_dispatch import SchedulerHandle

    dispatcher.cleanup_remote_workspace(
        SchedulerHandle(job_id=orphan.job_id, remote_workspace=orphan.path),
        vq_job_id=orphan.job_id,
    )


@dataclass
class StrayArchive:
    """An upload or fetch tar left in a scheduler host's jobs root."""

    job_id: str
    path: str
    age_seconds: float
    removable: bool
    reason: str


def local_live_job_ids(*, all_users: bool = False) -> set[str]:
    """Ids of local specs that are not terminal (unreadable ones count as live)."""
    queue_dirs = (
        [paths.user_queue_dir(user.name) for user in paths._all_user_dirs()]
        if all_users else [paths.queue_dir()]
    )
    live: set[str] = set()
    for queue in queue_dirs:
        if not queue.is_dir():
            continue
        for spec_file in queue.glob("*.json"):
            try:
                if not JobSpec.read(spec_file).is_terminal:
                    live.add(spec_file.stem)
            except Exception:  # noqa: BLE001 - unknown state is kept as live
                live.add(spec_file.stem)
    return live


def find_stray_archives(
    dispatcher: Any,
    *,
    live_ids: set[str],
    older_than_seconds: float = DEFAULT_ORPHAN_MIN_AGE_SECONDS,
    now: float | None = None,
) -> list[StrayArchive]:
    """Transfer tars whose operation is over: old, and no live local job owns them.

    Each is normally removed by the submit or fetch that wrote it; a leftover
    means that operation died. A job that is still live may yet be staging or
    fetching, so its archives are kept whatever their age.
    """
    now = time.time() if now is None else now
    found: list[StrayArchive] = []
    for job_id, path, modified in dispatcher.list_transfer_archives():
        age = max(0.0, now - modified)
        if job_id in live_ids:
            removable, reason = False, "its job is still live"
        elif age < older_than_seconds:
            removable, reason = False, "younger than --older-than"
        else:
            removable, reason = True, "abandoned transfer archive"
        found.append(StrayArchive(job_id, path, age, removable, reason))
    return found
