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
import json
import logging
import re
import shutil
import tarfile
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field

from vq import paths, spec_access, submit
from vq.spec import JobSpec, JobState, utcnow_iso

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

    @property
    def reclaimable(self) -> int:
        """Bytes that would be freed by deleting this job entirely."""
        return self.workspace_size + self.archive_size


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
) -> None:
    """Remove spec + workspace + archive (whichever exist).

    Refuses to act on non-terminal jobs (the daemon could still be
    writing to the workspace). Idempotent: missing workspace /
    archive / spec file is silently OK — the goal is "after this call,
    the job is gone."
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
    header = ("ID", "STATE", "FINISHED (UTC)", "WORKSPACE", "ARCHIVE")
    rows: list[tuple[str, str, str, str, str]] = [header]
    total = 0
    for c in candidates:
        finished = (c.spec.finished_at or "")[:19].replace("T", " ")
        ws_label = (
            "(archived)" if c.workspace_size == 0 and c.spec.is_archived
            else (_human_bytes(c.workspace_size) if c.workspace_size else "-")
        )
        ar_label = _human_bytes(c.archive_size) if c.archive_size else "-"
        rows.append((c.spec.id, c.spec.state.value, finished, ws_label, ar_label))
        total += c.reclaimable
    widths = [max(len(r[i]) for r in rows) for i in range(len(rows[0]))]
    lines = [
        "  ".join(col.ljust(w) for col, w in zip(row, widths, strict=True))
        for row in rows
    ]
    if action in ("archive", "delete", "restore"):
        verb = "would " + action if dry_run else action + "d"
        lines.append("")
        lines.append(
            f"{verb}: {len(candidates)} job(s); "
            f"reclaimable: {_human_bytes(total)}"
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
                delete_job(c.spec, queue_dir=qd)
                counts["deleted"] += 1
            except Exception as e:
                log.warning(
                    "auto-cleanup delete failed for %s: %s", c.spec.id, e
                )
                counts["delete_errors"] += 1


def run_auto_cleanup_pass(
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


def _sweep_stale_workdirs(
    max_age_seconds: int,
    now: datetime,
    counts: dict[str, int],
    *,
    multi_user: bool,
) -> None:
    """v0.6.54: rmtree per-job workdirs older than ``max_age_seconds``.

    Iterates every workdir root the daemon manages (single-user:
    ``paths.workdir_root()``; multi-user: every
    ``users/<uid>/workdirs/``), checks each per-job workdir's mtime
    against ``now - max_age_seconds``, and rmtrees the stale ones.

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

    def _sweep_one_root(root: Path, spec_path_for: Callable[[str], Path]) -> None:
        if not root.exists():
            return
        for entry in root.iterdir():
            if not entry.is_dir():
                continue
            jobid = entry.name
            spec = _read_spec_or_none(spec_path_for(jobid))
            if spec is not None:
                # Owned by a known job: never touch a non-terminal job's
                # live scratch, and age terminal ones by finished_at.
                if not spec.is_terminal:
                    continue
                if not _spec_older_than(spec, cutoff, now):
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
            try:
                shutil.rmtree(entry)
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
            except OSError as e:
                log.warning(
                    "auto-cleanup workdir-sweep: rmtree failed for %s: %s",
                    entry, e,
                )
                counts["workdir_errors"] += 1

    if multi_user:
        for user_dir in paths._all_user_dirs():
            uid = user_dir.name
            _sweep_one_root(
                paths.user_workdir_root(uid),
                lambda jid, uid=uid: paths.user_spec_path(uid, jid),
            )
    else:
        _sweep_one_root(paths.workdir_root(), paths.spec_path)


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
