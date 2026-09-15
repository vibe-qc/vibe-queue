"""Fetch job outputs back from where the job ran.

Two pairs of functions, one per "kind" of payload to retrieve:

* :func:`fetch_local` / :func:`fetch_remote` — copy ``spec.cwd`` (the
  job's *workspace*: the submitted-source root the job ran out of).
* :func:`fetch_workdir_local` / :func:`fetch_workdir_remote` —
  v0.7.7: copy ``spec.workdir`` (the per-job scratch directory the
  daemon materializes at dispatch, exposed to the job as
  ``$VQ_WORKDIR``). Operators dump intermediate / large artefacts
  there per the v0.6.54 agent-protocol convention.

Both pairs place their content under ``output_dir/<dest>/`` so
multiple fetches don't collide; the workdir variants append
``-workdir`` to the dest name so a workspace fetch and a workdir
fetch of the same job can sit side-by-side without a name conflict.

Two internal verbs feed the remote variants:

* ``vq tar-workspace JOBID`` (hidden from --help) feeds
  :func:`fetch_remote`. Knows the remote's state-dir layout via the
  remote vq's own :mod:`paths` module.
* ``vq tar-workdir JOBID`` (hidden from --help) feeds
  :func:`fetch_workdir_remote`. Same delegation pattern.
"""

from __future__ import annotations

import contextlib
import hashlib
import io
import json
import logging
import os
import shutil
import stat
import sys
import tarfile
import tempfile
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TypeGuard

from vq import config, paths, transport
from vq.config import HostConfig
from vq.ownership import OwnershipError, check_owner
from vq.scheduler_dispatch import (
    SchedulerError,
    scheduler_dispatcher_for,
    scheduler_handle_for_spec,
)
from vq.spec import (
    TERMINAL_STATES,
    JobSpec,
    JobState,
    describe_exit_code,
    utcnow_iso,
)
from vq.spec_access import reread_authorized_spec, resolve_authorized_spec
from vq.status import terminal_diagnosis_for_spec

log = logging.getLogger(__name__)

TERMINAL_DIAGNOSIS_SIDECAR = "_vq/terminal-diagnosis.json"

# FETCH-FRESHNESS (issues #111 / #114).
#
# Every fetched tree carries this manifest so a consumer can age-check the
# payload WITHOUT trusting the CLI's exit code or its "fetched -> ..." line.
# The loop's mandated terminal-detection method is results-presence
# (LEARNINGS L37) precisely because scheduler state is unreliable; a fetched
# tree with no recorded fetch time makes results-presence unreliable in the
# same silent direction. `fetched_at` answers "how old is what I am reading",
# and `stale` / `refresh_error` answer "did the last fetch actually land".
FETCH_MANIFEST_SIDECAR = "_vq/fetch-manifest.json"
FETCH_MANIFEST_SCHEMA = "vq.fetch-manifest.v1"

# A mark-back is a tiny, idempotent remote mutation, not a payload transfer.
# Bound both its wall time and protocol output independently of the 600-second
# generic remote-vq default.  The owned process group makes the 30-second cap
# cover ProxyCommand descendants as well as the direct ssh child.
REMOTE_FETCH_ACK_TIMEOUT_SECONDS = 30.0
REMOTE_FETCH_ACK_STDOUT_MAX_BYTES = 4 * 1024
REMOTE_FETCH_ACK_STDERR_MAX_BYTES = 16 * 1024

_TERMINAL_STATE_VALUES = frozenset(state.value for state in TERMINAL_STATES)
_ALL_STATE_VALUES = frozenset(state.value for state in JobState)


class _InvalidFetchReceipt(ValueError):
    """A workspace tree cannot prove which queue snapshot it contains."""


@dataclass(frozen=True)
class _FetchAcknowledgementIdentity:
    """Terminal identity proved by one completely staged workspace fetch."""

    submitted_at: str
    state: str


@dataclass(frozen=True)
class _RemoteFetchResult:
    """A promoted remote tree plus evidence captured while it was staged."""

    destination: Path
    acknowledgement: _FetchAcknowledgementIdentity | None
    protocol_error: str | None


def _is_aware_iso_timestamp(value: object) -> TypeGuard[str]:
    if not isinstance(value, str) or not value or len(value) > 256:
        return False
    try:
        return datetime.fromisoformat(value).tzinfo is not None
    except ValueError:
        return False


def _validate_artifact_name(name: str) -> str:
    """Return a safe basename for artifact-only retrieval."""
    candidate = Path(name)
    if (
        not name
        or candidate.is_absolute()
        or candidate.name != name
        or name in {".", ".."}
        or "\0" in name
    ):
        raise ValueError(
            f"artifact name must be one directory-relative basename, got {name!r}"
        )
    return name


def _sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _publish_artifact(temp_path: Path, dst: Path, *, idempotent: bool) -> Path:
    """Publish one completely staged artifact, replacing a stale prior copy.

    (#114): an existing plain-file destination is OVERWRITTEN with the
    freshly fetched bytes rather than kept. `vq fetch --name` on a live job's
    growing artifact used to fail (different content) or no-op (identical
    content); either way the operator could not simply re-read the file. Only
    a same-content copy short-circuits, and only to skip the rename.
    Directories replace the whole prior tree using the workspace-fetch
    rollback path; they are never merged with stale files. Type changes and
    destination symlinks are refused. ``idempotent=False`` still refuses an existing
    destination so the sweep can report an explicit skip.
    """
    if dst.exists() or dst.is_symlink():
        if not idempotent:
            raise FileExistsError(f"artifact destination already exists: {dst}")
        if dst.is_symlink() or dst.is_dir() != temp_path.is_dir():
            raise FileExistsError(
                f"artifact destination has an incompatible type: {dst}"
            )
        if temp_path.is_dir():
            _replace_directory(temp_path, dst)
            return dst
        if not dst.is_file():
            raise FileExistsError(f"artifact destination is not a regular file: {dst}")
        if _sha256_path(dst) == _sha256_path(temp_path):
            return dst
    os.replace(temp_path, dst)
    return dst


def _read_diagnosis_sidecar(dst: Path) -> dict[str, object] | None:
    """Read one regular fetch diagnosis sidecar without trusting its fields."""
    if dst.is_symlink() or not dst.is_dir():
        return None
    try:
        payload = json.loads((dst / TERMINAL_DIAGNOSIS_SIDECAR).read_text())
    except (OSError, json.JSONDecodeError, TypeError):
        return None
    return payload if isinstance(payload, dict) else None


def _is_fetched_destination_for_job(dst: Path, jobid: str) -> bool:
    """Return whether ``dst`` is a provenance-marked fetch of ``jobid``.

    A bare existing directory is not enough: it may be a partial legacy fetch
    or unrelated user data.  The diagnosis sidecar is written into every
    current local and remote workspace/workdir fetch before publication, so it
    is the cross-host idempotency key.  Symlinks remain collisions rather than
    being followed as trusted destinations.
    """
    payload = _read_diagnosis_sidecar(dst)
    if payload is None:
        return False
    return bool(
        payload.get("schema") == "vq.terminal-diagnosis.v1"
        and payload.get("jobid") == jobid
    )


def _diagnosis_sidecar_payload(spec: JobSpec) -> dict[str, object]:
    """Machine-readable queue outcome copied with fetched artifacts."""
    return {
        "schema": "vq.terminal-diagnosis.v1",
        "jobid": spec.id,
        "job_name": spec.job_name,
        "state": spec.state.value,
        "exit_code": spec.exit_code,
        "exit_code_description": (
            describe_exit_code(spec.exit_code)
            if spec.exit_code is not None
            else None
        ),
        "failure_reason": spec.failure_reason,
        "failure_tail": spec.failure_tail,
        "submitted_at": spec.submitted_at,
        "started_at": spec.started_at,
        "finished_at": spec.finished_at,
        "scheduler_target": spec.scheduler_target,
        "separate_workdir": bool(spec.workdir and spec.workdir != spec.cwd),
        "scheduler_job_id": spec.scheduler_job_id,
        "scheduler_state": spec.scheduler_state,
        "scheduler_walltime_used": spec.scheduler_walltime_used,
        "scheduler_walltime_limit": spec.scheduler_walltime_limit,
        "terminal_diagnosis": terminal_diagnosis_for_spec(spec),
        "generated_at": utcnow_iso(),
    }


def _diagnosis_sidecar_bytes(spec: JobSpec) -> bytes:
    return (
        json.dumps(
            _diagnosis_sidecar_payload(spec),
            indent=2,
            sort_keys=True,
            default=str,
        )
        + "\n"
    ).encode("utf-8")


def _write_diagnosis_sidecar(dst: Path, spec: JobSpec) -> None:
    path = dst / TERMINAL_DIAGNOSIS_SIDECAR
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(_diagnosis_sidecar_bytes(spec))


# ----------------------------------------------------------------------
# fetch freshness — manifest + real refresh (issues #111 / #114)
# ----------------------------------------------------------------------


def _fetch_manifest_payload(
    *,
    jobid: str,
    job_name: str | None,
    source_host: str,
    source_kind: str,
    source_path: str | None,
    transport_kind: str,
    stale: bool = False,
    refresh_error: str | None = None,
) -> dict[str, object]:
    """Machine-readable record of WHEN this tree was fetched and FROM WHAT.

    ``stale`` is the load-bearing field: it is False only when the bytes in
    this directory were written by the fetch that wrote this manifest. A
    consumer age-checks ``fetched_at`` and refuses to reason about a tree
    whose ``stale`` is true or whose ``fetched_at`` is older than its poll
    interval.
    """
    now = utcnow_iso()
    return {
        "schema": FETCH_MANIFEST_SCHEMA,
        "jobid": jobid,
        "job_name": job_name,
        "fetched_at": now,
        "refresh_attempted_at": now,
        "source_host": source_host,
        "source_kind": source_kind,
        "source_path": source_path,
        "transport": transport_kind,
        "stale": stale,
        "refresh_error": refresh_error,
    }


def _fetch_manifest_bytes(payload: dict[str, object]) -> bytes:
    return (
        json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n"
    ).encode("utf-8")


def _write_fetch_manifest(dst: Path, payload: dict[str, object]) -> None:
    path = dst / FETCH_MANIFEST_SIDECAR
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(_fetch_manifest_bytes(payload))


def read_fetch_manifest(dst: Path) -> dict[str, object] | None:
    """Return the fetch manifest of a fetched tree, or None when absent."""
    try:
        payload = json.loads(
            (dst / FETCH_MANIFEST_SIDECAR).read_text(encoding="utf-8")
        )
    except (OSError, UnicodeError, json.JSONDecodeError, TypeError):
        return None
    if not isinstance(payload, dict):
        return None
    if payload.get("schema") != FETCH_MANIFEST_SCHEMA:
        return None
    return payload


def require_fresh_fetch_manifest(
    dst: Path,
    *,
    jobid: str,
    source_kind: str,
) -> dict[str, object]:
    """Return the manifest proving ``dst`` is this job's fresh fetched tree.

    Reading a schema tag alone is not freshness evidence. The identity, tree
    kind, timestamps, transport provenance, and explicit non-stale verdict
    must all agree before a caller reports a successful whole-tree fetch.
    """
    if source_kind not in {"workspace", "workdir"}:
        raise ValueError(f"unsupported fetched tree kind: {source_kind!r}")
    manifest = read_fetch_manifest(dst)
    if manifest is None:
        raise ValueError("fetch-manifest metadata is missing or unreadable")
    source_host = manifest.get("source_host")
    transport_kind = manifest.get("transport")
    if (
        manifest.get("jobid") != jobid
        or manifest.get("source_kind") != source_kind
        or not _is_aware_iso_timestamp(manifest.get("fetched_at"))
        or not _is_aware_iso_timestamp(manifest.get("refresh_attempted_at"))
        or not isinstance(source_host, str)
        or not source_host
        or not isinstance(transport_kind, str)
        or not transport_kind
        or manifest.get("stale") is not False
        or manifest.get("refresh_error") is not None
    ):
        raise ValueError(
            "fetch-manifest metadata does not identify a fresh "
            f"{source_kind} snapshot for job {jobid}"
        )
    return manifest


def require_workspace_artifact_selection(dst: Path) -> None:
    """Refuse implicit workspace-only success when local outputs may be elsewhere.

    Old diagnosis sidecars lack the workdir field. For terminal local jobs,
    require an explicit selection in that case instead of guessing that the
    submitted workspace contains the completed calculation's artifacts.
    """
    try:
        payload = json.loads((dst / TERMINAL_DIAGNOSIS_SIDECAR).read_text())
    except (OSError, UnicodeError, json.JSONDecodeError):
        return
    if not isinstance(payload, dict) or payload.get("schema") != "vq.terminal-diagnosis.v1":
        return
    if payload.get("scheduler_target"):
        return
    separate = payload.get("separate_workdir")
    if separate is True or (
        separate is not False and payload.get("state") in _TERMINAL_STATE_VALUES
    ):
        raise ValueError(
            "this local-daemon workspace does not establish retrieval of the "
            "job's separate output artifacts; use --workdir to fetch the "
            "managed output directory, or --workspace to explicitly fetch "
            "only the submitted workspace"
        )


def _terminal_fetch_acknowledgement_identity(
    fetched_tree: Path,
    *,
    jobid: str,
    source_host: str,
) -> _FetchAcknowledgementIdentity | None:
    """Return exact terminal evidence carried by a fresh workspace tree.

    The diagnosis sidecar binds the queue identity and state.  The receiving
    side's manifest independently proves that a complete, non-stale workspace
    from this host was published.  A live snapshot, workdir, stale refresh, or
    malformed/mismatched sidecar is not mark-back authority.
    """
    diagnosis = _read_diagnosis_sidecar(fetched_tree)
    manifest = read_fetch_manifest(fetched_tree)
    if diagnosis is None:
        raise _InvalidFetchReceipt(
            "terminal-diagnosis metadata is missing or unreadable"
        )
    if manifest is None:
        raise _InvalidFetchReceipt("fetch-manifest metadata is missing or unreadable")
    submitted_at = diagnosis.get("submitted_at")
    state = diagnosis.get("state")
    fetched_at = manifest.get("fetched_at")
    if (
        diagnosis.get("schema") != "vq.terminal-diagnosis.v1"
        or diagnosis.get("jobid") != jobid
        or not _is_aware_iso_timestamp(submitted_at)
        or not isinstance(state, str)
        or state not in _ALL_STATE_VALUES
        or manifest.get("jobid") != jobid
        or manifest.get("source_host") != source_host
        or manifest.get("source_kind") != "workspace"
        or manifest.get("transport") != "ssh-stream"
        or manifest.get("stale") is not False
        or manifest.get("refresh_error") is not None
        or not _is_aware_iso_timestamp(fetched_at)
    ):
        raise _InvalidFetchReceipt(
            "diagnosis and fetch-manifest metadata do not identify one "
            "complete workspace snapshot"
        )
    if state not in _TERMINAL_STATE_VALUES:
        return None
    return _FetchAcknowledgementIdentity(
        submitted_at=submitted_at,
        state=state,
    )


def mark_terminal_fetch(
    jobid: str,
    *,
    submitted_at: str,
    state: str,
    multi_user: bool = False,
    expected_path: Path | None = None,
) -> str:
    """Stamp a terminal fetch only for the exact authorized spec identity.

    ``submitted_at`` and ``state`` come from the fetched diagnosis sidecar.
    Re-resolving and authorizing under the per-spec lock prevents a same-ID
    replacement or a live-to-terminal race from inheriting another snapshot's
    acknowledgement.  Replaying the exact identity is safe and preserves every
    field from the fresh locked read.
    """
    if (
        not _is_aware_iso_timestamp(submitted_at)
        or state not in _TERMINAL_STATE_VALUES
    ):
        raise ValueError("fetch acknowledgement identity is not terminal and complete")
    if expected_path is None:
        expected_path, _ = resolve_authorized_spec(
            jobid,
            multi_user=multi_user,
        )
    with paths.spec_lock(expected_path):
        fresh = reread_authorized_spec(
            jobid,
            expected_path=expected_path,
            multi_user=multi_user,
        )
        if (
            "submitted_at" not in fresh.model_fields_set
            or fresh.submitted_at != submitted_at
            or fresh.state.value != state
            or not fresh.is_terminal
        ):
            raise ValueError(
                "fetch acknowledgement identity no longer matches the terminal job"
            )
        marked_at = utcnow_iso()
        fresh.last_fetched_at = marked_at
        fresh.write(expected_path)
    return marked_at


def _send_remote_fetch_acknowledgement(
    host_cfg: HostConfig,
    jobid: str,
    identity: _FetchAcknowledgementIdentity,
    *,
    destination: Path,
) -> None:
    """Commit one bounded, replay-safe last_fetched_at mark-back."""
    try:
        proc = transport.run_remote_vq(
            host_cfg,
            "mark-fetched",
            jobid,
            "--submitted-at",
            identity.submitted_at,
            "--state",
            identity.state,
            timeout=REMOTE_FETCH_ACK_TIMEOUT_SECONDS,
            retry_transient=0,
            owned_process_group=True,
            max_stdout_bytes=REMOTE_FETCH_ACK_STDOUT_MAX_BYTES,
            max_stderr_bytes=REMOTE_FETCH_ACK_STDERR_MAX_BYTES,
        )
    except transport.RemoteOutcomeUnknown as exc:
        raise transport.RemoteOutcomeUnknown(
            f"workspace landed at {destination}, but the remote "
            "last_fetched_at mark-back outcome is unknown; the fetched "
            f"tree remains fresh and rerunning the fetch is safe: {exc}"
        ) from exc
    except transport.RemoteError as exc:
        # #644: old vq can transfer a complete workspace but has no mark-back
        # verb. Only Click's exact command-unavailable result is compatible;
        # an identity rejection, unsupported flag or unknown SSH outcome is not.
        if (
            isinstance(exc, transport.RemoteCommandError)
            and exc.returncode == 2
            and exc.stderr.strip().splitlines()[-1:]
            == ["Error: No such command 'mark-fetched'."]
        ):
            log.warning(
                "workspace landed at %s; remote vq lacks mark-fetched, so "
                "last_fetched_at was not recorded; update the remote vq",
                destination,
            )
            return
        raise transport.RemoteError(
            f"workspace landed at {destination}, but remote last_fetched_at "
            f"mark-back failed: {exc}; rerun the fetch safely"
        ) from exc
    receipt = (proc.stdout or "").strip()
    if not _is_aware_iso_timestamp(receipt):
        raise transport.RemoteError(
            f"workspace landed at {destination}, but remote last_fetched_at "
            "mark-back returned an invalid receipt; rerun the fetch safely"
        ) from None


def prepare_remote_bulk_fetch(
    host_cfg: HostConfig,
    spec: JobSpec,
    output_dir: Path,
) -> bool:
    """Reconcile a fetch-all destination before opening a new tar stream.

    Returns whether an existing same-job tree must be refreshed.  An exact
    terminal receipt is skipped; when its remote row lacks ``last_fetched_at``,
    the idempotent mark-back is retried first.  A prior live/stale/mismatched
    snapshot is refreshed instead of being silently blessed or skipped.  Bare
    and foreign destinations remain ordinary collisions in the fetch primitive.
    """
    destination = output_dir / spec.dest_dirname
    if not _is_fetched_destination_for_job(destination, spec.id):
        return False
    try:
        identity = _terminal_fetch_acknowledgement_identity(
            destination,
            jobid=spec.id,
            source_host=host_cfg.ssh,
        )
    except _InvalidFetchReceipt:
        return True
    if (
        identity is None
        or not spec.is_terminal
        or identity.submitted_at != spec.submitted_at
        or identity.state != spec.state.value
    ):
        return True
    if spec.last_fetched_at is None:
        _send_remote_fetch_acknowledgement(
            host_cfg,
            spec.id,
            identity,
            destination=destination,
        )
    raise FileExistsError(f"destination already fetched: {destination}")


def stamp_stale_fetch_manifest(dst: Path, *, jobid: str, error: str) -> bool:
    """Mark an ALREADY-fetched tree as stale after a failed refresh.

    ``vq fetch`` exits non-zero when it cannot refresh (that is the primary
    contract), but the operator's previous snapshot is still sitting on disk
    and some other process may read it minutes later without ever having seen
    that exit code. Stamping the tree closes the gap: the bytes themselves now
    say they are stale and why. Best effort — a failure to stamp never
    replaces the transport error the caller is about to raise.
    """
    if dst.is_symlink() or not dst.is_dir():
        return False
    existing = read_fetch_manifest(dst) or {}
    payload = dict(existing)
    payload.update(
        {
            "schema": FETCH_MANIFEST_SCHEMA,
            "jobid": existing.get("jobid", jobid),
            "refresh_attempted_at": utcnow_iso(),
            "stale": True,
            "refresh_error": error,
        }
    )
    payload.setdefault("fetched_at", None)
    payload.setdefault("job_name", None)
    payload.setdefault("source_host", None)
    payload.setdefault("source_kind", None)
    payload.setdefault("source_path", None)
    payload.setdefault("transport", None)
    try:
        _write_fetch_manifest(dst, payload)
    except OSError:
        return False
    return True


def _stamp_stale_after_failed_refresh(
    output_dir: Path,
    jobid: str,
    dst: Path | None,
    error: str,
) -> None:
    """Mark this job's existing snapshot(s) under ``output_dir`` as stale.

    ``dst`` is known only once the remote tar's first member named it; a
    transport failure before that still needs the previous snapshot marked, so
    fall back to scanning ``output_dir`` for a tree whose provenance sidecar
    names this job.
    """
    candidates: list[Path] = []
    if dst is not None and _is_fetched_destination_for_job(dst, jobid):
        candidates.append(dst)
    else:
        with contextlib.suppress(OSError):
            candidates = [
                child
                for child in output_dir.iterdir()
                if _is_fetched_destination_for_job(child, jobid)
            ]
    for candidate in candidates:
        stamp_stale_fetch_manifest(candidate, jobid=jobid, error=error)


def _replace_directory(new_dir: Path, dst: Path) -> None:
    """Publish ``new_dir`` at ``dst``, replacing any existing directory.

    ``os.replace`` refuses a non-empty destination directory, which is exactly
    how a re-fetch used to end up silently returning the OLD snapshot. The
    previous tree is moved aside first, the fresh one is put in its place, and
    only then is the old one deleted — so a failure mid-way leaves either the
    old tree or the new one at ``dst``, never a half-written mixture.
    """
    if not dst.exists() and not dst.is_symlink():
        try:
            os.replace(new_dir, dst)
            return
        except OSError:
            # A concurrent fetch of the same job published at `dst` between
            # our pre-check and this promote (ENOTEMPTY on macOS). Fall
            # through to the replace-existing path below so the freshest
            # snapshot wins; any other promote error still fails closed.
            if not dst.exists() or dst.is_symlink():
                raise
    retired = Path(
        tempfile.mkdtemp(dir=dst.parent, prefix=f".vq-stale-{dst.name}-")
    )
    retired_dst = retired / dst.name
    os.replace(dst, retired_dst)
    try:
        os.replace(new_dir, dst)
    except BaseException:
        # Put the operator's previous snapshot back rather than leaving the
        # destination missing entirely.
        with contextlib.suppress(OSError):
            os.replace(retired_dst, dst)
        raise
    finally:
        shutil.rmtree(retired, ignore_errors=True)


def _add_diagnosis_sidecar_to_tar(
    tf: tarfile.TarFile,
    *,
    arcname_root: str,
    spec: JobSpec,
) -> None:
    data = _diagnosis_sidecar_bytes(spec)
    info = tarfile.TarInfo(f"{arcname_root}/{TERMINAL_DIAGNOSIS_SIDECAR}")
    info.size = len(data)
    info.mode = 0o644
    tf.addfile(info, io.BytesIO(data))


def _stream_archive_with_diagnosis(archive: Path, spec: JobSpec) -> None:
    """Stream an archived workspace tar while appending fresh vq metadata."""
    arcname_root = spec.dest_dirname
    with (
        tarfile.open(archive, mode="r:*") as src_tf,
        tarfile.open(fileobj=sys.stdout.buffer, mode="w|") as out_tf,
    ):
        for member in src_tf:
            if member.name:
                arcname_root = member.name.split("/", 1)[0]
            fileobj = src_tf.extractfile(member) if member.isfile() else None
            try:
                out_tf.addfile(member, fileobj)
            finally:
                if fileobj is not None:
                    fileobj.close()
        _add_diagnosis_sidecar_to_tar(
            out_tf,
            arcname_root=arcname_root,
            spec=spec,
        )


def _refresh_live_scheduler_workspace(spec: JobSpec) -> None:
    """Stage a live scheduler job's current remote workspace back to ``spec.cwd``.

    Terminal scheduler jobs are already staged back by the daemon's reap path, and
    ordinary local jobs have no scheduler metadata. For live scheduler jobs, this
    gives ``vq fetch`` an explicit mid-run snapshot without changing the normal
    local/remote tar extraction contract.
    """
    if (
        spec.is_archived
        or spec.is_terminal
        or spec.scheduler_target is None
        or spec.scheduler_job_id is None
    ):
        return
    try:
        host_cfg = config.load_config().host(spec.scheduler_target)
        dispatcher = scheduler_dispatcher_for(host_cfg)
        handle = scheduler_handle_for_spec(
            dispatcher,
            spec,
            job_id=spec.scheduler_job_id,
        )
        dispatcher.fetch_results(handle, Path(spec.cwd))
    except config.ConfigError as exc:
        raise transport.RemoteError(
            f"cannot refresh live scheduler workspace for {spec.id}: {exc}"
        ) from exc
    except SchedulerError as exc:
        raise transport.RemoteError(
            f"failed to refresh live scheduler workspace for {spec.id}: {exc}"
        ) from exc


def fetch_local(
    jobid: str,
    output_dir: Path,
    *,
    multi_user: bool = False,
    idempotent: bool = True,
) -> Path:
    """Copy the local workspace for ``jobid`` into ``output_dir/<dest>/``.

    v0.5.34: ``<dest>`` is ``<job_name>-<jobid>`` when the spec has
    ``job_name`` set, else ``<jobid>`` (pre-v0.5.34 behaviour). The
    name-prefixed form makes a directory of fetched workspaces self-
    documenting — ``ls ./results`` shows what each job was.

    Returns the path of the copied workspace. If the job has been
    archived by ``vq cleanup --archive``, un-tars from the archive
    instead of copying the (gone) workspace dir. Raises
    :class:`FileNotFoundError` if the job isn't in the local queue, or
    if the workspace and archive are both missing. A destination whose
    diagnosis sidecar identifies the same job is returned unchanged when
    ``idempotent`` is true; callers such as ``fetch-all`` can set it false to
    receive :class:`FileExistsError` and report an explicit skip.
    """
    if multi_user:
        spec_path = paths.resolve_spec_path(jobid, multi_user=True)
    else:
        spec_path = paths.spec_path(jobid)
        if not spec_path.exists():
            raise FileNotFoundError(f"no such job: {jobid}")
    spec = JobSpec.read(spec_path)
    # v0.6.x: multi-user ownership check.
    check_owner(spec, multi_user=multi_user)
    output_dir.mkdir(parents=True, exist_ok=True)
    dst = output_dir / spec.dest_dirname
    refreshing = False
    if dst.exists() or dst.is_symlink():
        if _is_fetched_destination_for_job(dst, jobid):
            if not idempotent:
                raise FileExistsError(f"destination already fetched: {dst}")
            # (#114): a populated same-job destination is REFRESHED,
            # never returned unchanged. Returning it made `vq fetch` print
            # "fetched" over a frozen snapshot, which is how a converged run
            # was nearly reported as hung.
            refreshing = True
        else:
            raise FileExistsError(
                f"destination already exists: {dst} (remove it or pick another -o)"
            )
    try:
        _refresh_live_scheduler_workspace(spec)
    except transport.RemoteError as exc:
        # #111: the previous snapshot is still on disk and some other process
        # will read it. Exit loudly (we re-raise) AND mark the bytes stale.
        if refreshing:
            stamp_stale_fetch_manifest(dst, jobid=jobid, error=str(exc))
        raise
    staging = Path(tempfile.mkdtemp(dir=output_dir, prefix=".vq-fetch-"))
    staged = staging / dst.name
    try:
        if spec.is_archived and spec.archive_path:
            archive = Path(spec.archive_path)
            if not archive.is_file():
                raise FileNotFoundError(
                    f"archive for job {jobid} not found at {archive} "
                    "(record exists in spec but file is gone)"
                )
            # Tarball top-level is ``<dest_dirname>/`` (= ``<name>-<jobid>``
            # for v0.5.34+ named jobs, or just ``<jobid>`` for pre-v0.5.34
            # archives and unnamed jobs), so extracting under the staging dir
            # yields ``staging/<dest_dirname>``.
            with tarfile.open(archive, mode="r:bz2") as tf:
                tf.extractall(staging, filter="data")
            if not staged.is_dir():
                raise FileNotFoundError(
                    f"archive for job {jobid} at {archive} did not contain a "
                    f"{spec.dest_dirname}/ directory"
                )
            transport_kind = "local-archive"
            source_path: str | None = str(archive)
        else:
            src = Path(spec.cwd)
            if not src.is_dir():
                raise FileNotFoundError(
                    f"workspace for job {jobid} not found at {src} "
                    "(was it cleaned up or never materialized?)"
                )
            shutil.copytree(src, staged)
            transport_kind = "local-copy"
            source_path = str(src)
        _write_diagnosis_sidecar(staged, spec)
        _write_fetch_manifest(
            staged,
            _fetch_manifest_payload(
                jobid=jobid,
                job_name=spec.job_name,
                source_host="local",
                source_kind="workspace",
                source_path=source_path,
                transport_kind=transport_kind,
            ),
        )
        _replace_directory(staged, dst)
    finally:
        shutil.rmtree(staging, ignore_errors=True)
    # Stamp only the exact terminal snapshot that authorized this copy.  The
    # spec read at the top is stale after the filesystem work, so the helper
    # securely re-reads under lock and rejects a same-ID replacement or state
    # transition.  Local mark-back remains best-effort: the copied bytes are
    # the primary result and retain the historical success contract.
    if spec.is_terminal:
        with contextlib.suppress(OSError, ValueError, config.ConfigError):
            mark_terminal_fetch(
                jobid,
                submitted_at=spec.submitted_at,
                state=spec.state.value,
                multi_user=multi_user,
                expected_path=spec_path,
            )
    return dst


def _stream_extract_remote_tar(
    host_cfg: HostConfig,
    verb: str,
    jobid: str,
    output_dir: Path,
    *,
    idempotent: bool,
) -> _RemoteFetchResult:
    """Stream ``<remote_vq> <verb> <jobid>`` (a tarball on stdout) and extract
    it under ``output_dir/<dest>/``, where ``<dest>`` is the tar's top-level
    directory. Shared by :func:`fetch_remote` (``verb="tar-workspace"``) and
    :func:`fetch_workdir_remote` (``verb="tar-workdir"``).  Only the
    workspace caller requests acknowledgement evidence; workdir fetches do
    not change ``last_fetched_at``.

    ``<dest>`` (v0.5.34) is whatever the streaming tar's top-level dir says —
    the remote emitter sets ``arcname=<name>-<jobid>`` when ``job_name`` is
    set, else ``arcname=<jobid>``. Streaming tar (``mode="r|"``) can't seek
    back, so we learn the dest name from the FIRST member, pre-check
    existence, then extract.

    REMOTE-2/3/4/5/6 hardening (v0.8.17):

    * Transport via :func:`transport.stream_remote_vq` — ``_ssh_base`` +
      ``shlex.join`` (ConnectTimeout / BatchMode / ServerAlive, and no
      remote-shell word-split of the argv), a concurrently-drained stderr
      (no pipe-buffer deadlock when the remote is chatty on stderr), and
      ssh-exit-255-vs-real-remote-rc discipline.
    * **Temp-then-rename**: members extract into a hidden staging dir on the
      SAME filesystem as ``output_dir``; only on full success is the
      top-level dir ``os.replace``-d into place. A mid-stream failure (the
      remote dies, the tar truncates, the disk fills) therefore leaves NO
      half-written ``<dest>/`` for the next fetch / ``vq cleanup --restore``
      to trip over — just a staging dir we ``rmtree`` in ``finally``.
    """
    capture_workspace_ack = verb == "tar-workspace"
    what = verb.removeprefix("tar-")  # "workspace" / "workdir", for messages
    output_dir.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(dir=output_dir, prefix=".vq-fetch-"))
    dst: Path | None = None
    stream: transport.RemoteStream | None = None
    try:
        with (
            transport.stream_remote_vq(host_cfg, verb, jobid) as stream,
            tarfile.open(fileobj=stream.stdout, mode="r|") as tf,
        ):
            for member in tf:
                if dst is None:
                    # Top-level is the part before the first '/'. For the
                    # typical ``tf.add(dir, arcname=NAME)`` shape the first
                    # member is just NAME (a dir entry).
                    top_level = member.name.split("/", 1)[0]
                    dst = output_dir / top_level
                    if dst.exists() or dst.is_symlink():
                        if _is_fetched_destination_for_job(dst, jobid):
                            # (#114): keep streaming and REPLACE the
                            # old snapshot. Aborting here is what made a
                            # re-fetch print "fetched" over frozen bytes.
                            if not idempotent:
                                raise FileExistsError(
                                    f"destination already fetched: {dst}"
                                )
                        else:
                            raise FileExistsError(
                                f"destination already exists: {dst} "
                                "(remove it or pick another -o)"
                            )
                tf.extract(member, staging, filter="data")
        # stream_remote_vq.__exit__ has run: a non-zero remote / ssh exit
        # already raised RemoteError, and stream.stderr_text is fully drained.
        if dst is None:
            raise transport.RemoteError(
                f"remote {verb} produced an empty tarball (job {jobid}); "
                f"remote stderr: {stream.stderr_text or '(empty)'}"
            )
        staged = staging / dst.name
        _write_fetch_manifest(
            staged,
            _fetch_manifest_payload(
                jobid=jobid,
                job_name=None,
                source_host=host_cfg.ssh,
                source_kind=what,
                source_path=None,
                transport_kind="ssh-stream",
            ),
        )
        # Capture acknowledgement authority from the private staging tree.
        # The promoted destination is operator-writable and may be changed as
        # soon as os.replace returns, so it must never be the source of truth
        # for this fetch's remote mutation.
        acknowledgement = None
        protocol_error = None
        if capture_workspace_ack:
            try:
                acknowledgement = _terminal_fetch_acknowledgement_identity(
                    staged,
                    jobid=jobid,
                    source_host=host_cfg.ssh,
                )
            except _InvalidFetchReceipt as exc:
                # Preserve the successfully received bytes, but do not report
                # a current workspace fetch as successful when its queue
                # identity cannot participate in the mark-back protocol.
                protocol_error = str(exc)
        # Promote: staging/<dest> -> output_dir/<dest>, replacing any previous
        # same-job snapshot. Same filesystem (staging is under output_dir).
        _replace_directory(staged, dst)
        return _RemoteFetchResult(dst, acknowledgement, protocol_error)
    except tarfile.TarError as e:
        # A truncated / malformed stream — usually the remote vq errored and
        # wrote a partial (or no) tarball. stream_remote_vq drained the
        # remote stderr into stream.stderr_text as the exception propagated
        # out of its ``with`` block.
        remote_stderr = stream.stderr_text if stream is not None else ""
        failure = transport.RemoteError(
            f"failed to extract remote {what} tarball for {jobid}: {e}; "
            f"remote stderr: {remote_stderr or '(empty)'}"
        )
        _stamp_stale_after_failed_refresh(output_dir, jobid, dst, str(failure))
        raise failure from e
    except transport.RemoteError as e:
        # #111: the transport failed, so whatever snapshot of this job is
        # already sitting in output_dir was NOT refreshed. We exit non-zero
        # (this propagates), and we also mark those bytes stale so a consumer
        # that reads them later without seeing our exit code can tell.
        _stamp_stale_after_failed_refresh(output_dir, jobid, dst, str(e))
        raise
    finally:
        shutil.rmtree(staging, ignore_errors=True)


def fetch_remote(
    host_cfg: HostConfig,
    jobid: str,
    output_dir: Path,
    *,
    idempotent: bool = True,
) -> Path:
    """Stream the remote *workspace* (``spec.cwd``) for ``jobid`` into
    ``output_dir/<dest>/`` via the ``vq tar-workspace`` verb. See
    :func:`_stream_extract_remote_tar` for the transport + extraction
    contract. ``idempotent`` has the same-job destination semantics of
    :func:`fetch_local`."""
    result = _stream_extract_remote_tar(
        host_cfg,
        "tar-workspace",
        jobid,
        output_dir,
        idempotent=idempotent,
    )
    if result.protocol_error is not None:
        raise transport.RemoteError(
            f"workspace landed at {result.destination}, but its remote "
            f"diagnosis protocol is invalid: {result.protocol_error}; "
            "update the remote vq and rerun the fetch safely"
        )
    if result.acknowledgement is not None:
        _send_remote_fetch_acknowledgement(
            host_cfg,
            jobid,
            result.acknowledgement,
            destination=result.destination,
        )
    return result.destination


def _artifact_member_parts(member: tarfile.TarInfo) -> tuple[str, ...]:
    """Accept only canonical relative names and ordinary directory/file nodes."""
    name = member.name.rstrip("/") if member.isdir() else member.name
    parts = tuple(name.split("/"))
    if (
        any(part in {"", ".", ".."} or "\0" in part for part in parts)
        or not (member.isfile() or member.isdir())
    ):
        raise ValueError(f"unsafe artifact archive member: {member.name!r}")
    return parts


def _copy_artifact_members(
    tf: tarfile.TarFile, members: Iterator[tarfile.TarInfo],
    root: tuple[str, ...], destination: Path,
) -> None:
    """Copy a single rooted tree into private staging, never following links."""
    seen: set[tuple[str, ...]] = set()
    root_is_dir = False
    for member in members:
        parts = _artifact_member_parts(member)
        if parts[:len(root)] != root:
            raise ValueError(f"unexpected artifact archive member: {member.name!r}")
        relative = parts[len(root):]
        if parts in seen:
            raise ValueError(f"duplicate artifact archive member: {member.name!r}")
        if not seen:
            if relative:
                raise ValueError("artifact archive must start with the requested root")
            root_is_dir = member.isdir()
        elif not root_is_dir or not relative:
            raise ValueError("artifact archive contains more than the requested file")
        seen.add(parts)
        dst = destination.joinpath(*relative)
        if member.isdir():
            dst.mkdir(mode=0o700, parents=True, exist_ok=True)
        else:
            dst.parent.mkdir(parents=True, exist_ok=True)
            source = tf.extractfile(member)
            if source is None:
                raise ValueError(f"unreadable artifact archive member: {member.name!r}")
            with source, dst.open("xb") as out:
                shutil.copyfileobj(source, out)
            dst.chmod(0o600)
    if not seen:
        raise FileNotFoundError("artifact archive is empty")


def _copy_named_artifact_from_archive(
    archive: Path, name: str, destination: Path
) -> None:
    """Copy one unique named file or directory, rejecting links in its tree."""
    with tarfile.open(archive, mode="r:*") as tf:
        members = tf.getmembers()
        matches = [
            member for member in members
            if member.name.rstrip("/").split("/")[-1] == name
        ]
        if len(matches) != 1:
            raise FileNotFoundError(
                f"artifact {name!r} was not found uniquely in archive {archive}"
            )
        root = _artifact_member_parts(matches[0])
        # A link or special node in the selected path is not a directory.
        for member in members:
            parts = tuple(member.name.rstrip("/").split("/"))
            if len(parts) < len(root) and root[:len(parts)] == parts and not member.isdir():
                raise ValueError("artifact archive has a non-directory ancestor")
        selected = [matches[0]]
        selected.extend(
            member for member in members if member is not matches[0]
            and member.name.startswith("/".join(root) + "/")
        )
        _copy_artifact_members(tf, iter(selected), root, destination)


@contextlib.contextmanager
def _open_artifact(
    spec: JobSpec, name: str, *, workdir: bool, subdir: str | None = None,
) -> Iterator[int]:
    """Anchor every path component with a descriptor and refuse symlinks."""
    if workdir and not spec.workdir:
        raise FileNotFoundError(_no_workdir_message(spec, spec.id))
    root = spec.workdir if workdir else spec.cwd
    assert root is not None
    try:
        directory_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    except FileNotFoundError as exc:
        hint = _workdir_missing_hint(spec) if workdir else ""
        raise FileNotFoundError(
            f"artifact source for job {spec.id} not found at {root}{hint}"
        ) from exc
    try:
        for part in _validate_artifact_subdir(subdir, workdir=workdir):
            child_fd = os.open(
                part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=directory_fd,
            )
            os.close(directory_fd)
            directory_fd = child_fd
        fd = os.open(
            name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory_fd,
        )
        try:
            yield fd
        finally:
            os.close(fd)
    finally:
        os.close(directory_fd)


def _copy_artifact_fd(fd: int, destination: Path) -> None:
    """Snapshot only regular files and directories through anchored handles."""
    mode = os.fstat(fd).st_mode
    if stat.S_ISREG(mode):
        with os.fdopen(os.dup(fd), "rb") as source, destination.open("xb") as out:
            shutil.copyfileobj(source, out)
        destination.chmod(0o600)
    elif stat.S_ISDIR(mode):
        destination.mkdir(mode=0o700)
        for name in sorted(os.listdir(fd)):
            child = os.open(
                name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd,
            )
            try:
                _copy_artifact_fd(child, destination / name)
            finally:
                os.close(child)
    else:
        raise ValueError("artifact must contain only regular files and directories")


def _validate_artifact_subdir(subdir: str | None, *, workdir: bool) -> tuple[str, ...]:
    """Validate explicit relative directory components before any I/O."""
    if subdir is None:
        return ()
    if not workdir:
        raise ValueError("artifact --subdir requires --workdir")
    parts = tuple(subdir.split("/"))
    if any(part in {"", ".", ".."} or "\0" in part for part in parts):
        raise ValueError("artifact subdir must be a relative directory without . or ..")
    return parts


def fetch_artifact_local(
    jobid: str,
    name: str,
    output_dir: Path,
    *,
    multi_user: bool = False,
    idempotent: bool = True,
    workdir: bool = False,
    subdir: str | None = None,
) -> Path:
    """Fetch one artifact from the explicit source directory, without sidecars."""
    name = _validate_artifact_name(name)
    _validate_artifact_subdir(subdir, workdir=workdir)
    spec_path = _tar_spec_path(jobid, multi_user=multi_user)
    spec = JobSpec.read(spec_path)
    check_owner(spec, multi_user=multi_user)
    if not workdir:
        _refresh_live_scheduler_workspace(spec)
    output_dir.mkdir(parents=True, exist_ok=True)
    dst = output_dir / name
    staging = Path(tempfile.mkdtemp(dir=output_dir, prefix=f".{name}.vq-fetch-"))
    temp_path = staging / name
    try:
        _stage_named_artifact(spec, name, temp_path, workdir=workdir, subdir=subdir)
        published = _publish_artifact(temp_path, dst, idempotent=idempotent)
        if spec.is_terminal and not workdir:
            with contextlib.suppress(OSError, ValueError, config.ConfigError):
                mark_terminal_fetch(
                    jobid,
                    submitted_at=spec.submitted_at,
                    state=spec.state.value,
                    multi_user=multi_user,
                    expected_path=spec_path,
                )
        return published
    finally:
        shutil.rmtree(staging, ignore_errors=True)


def _stage_named_artifact(
    spec: JobSpec, name: str, destination: Path, *, workdir: bool,
    subdir: str | None,
) -> None:
    if not workdir and spec.is_archived and spec.archive_path:
        _copy_named_artifact_from_archive(Path(spec.archive_path), name, destination)
    else:
        with _open_artifact(spec, name, workdir=workdir, subdir=subdir) as fd:
            _copy_artifact_fd(fd, destination)


def emit_artifact_tar(
    jobid: str, name: str, *, multi_user: bool = False, workdir: bool = False,
    subdir: str | None = None,
) -> None:
    """Stream a validated snapshot of one named file or directory artifact."""
    name = _validate_artifact_name(name)
    _validate_artifact_subdir(subdir, workdir=workdir)
    _, spec = resolve_authorized_spec(jobid, multi_user=multi_user)
    if not workdir:
        _refresh_live_scheduler_workspace(spec)
    # Finish validation before emitting any bytes, including for a directory
    # with a forbidden child. Only this private snapshot is passed to tar.
    with tempfile.TemporaryDirectory(prefix="vq-artifact-") as temporary:
        staged = Path(temporary) / name
        _stage_named_artifact(spec, name, staged, workdir=workdir, subdir=subdir)
        with tarfile.open(fileobj=sys.stdout.buffer, mode="w|") as out_tf:
            out_tf.add(staged, arcname=name)


def fetch_artifact_remote(
    host_cfg: HostConfig,
    jobid: str,
    name: str,
    output_dir: Path,
    *,
    idempotent: bool = True,
    workdir: bool = False,
    subdir: str | None = None,
) -> Path:
    """Stream exactly one named artifact from a remote queue host."""
    name = _validate_artifact_name(name)
    _validate_artifact_subdir(subdir, workdir=workdir)
    output_dir.mkdir(parents=True, exist_ok=True)
    dst = output_dir / name
    staging = Path(tempfile.mkdtemp(dir=output_dir, prefix=f".{name}.vq-fetch-"))
    temp_path = staging / name
    stream: transport.RemoteStream | None = None
    try:
        with (
            transport.stream_remote_vq(
                host_cfg, "tar-artifact", jobid, name,
                *(("--workdir",) if workdir else ()),
                *(("--subdir", subdir) if subdir is not None else ()),
            ) as stream,
            tarfile.open(fileobj=stream.stdout, mode="r|") as tf,
        ):
            try:
                _copy_artifact_members(tf, iter(tf), (name,), temp_path)
            except (ValueError, FileNotFoundError) as exc:
                raise transport.RemoteError(
                    f"remote tar-artifact did not contain exactly the requested "
                    f"regular file or directory {name!r}: {exc}"
                ) from exc
        return _publish_artifact(temp_path, dst, idempotent=idempotent)
    except tarfile.TarError as exc:
        remote_stderr = stream.stderr_text if stream is not None else ""
        raise transport.RemoteError(
            f"failed to fetch remote artifact {name!r} for {jobid}: {exc}; "
            f"remote stderr: {remote_stderr or '(empty)'}"
        ) from exc
    finally:
        shutil.rmtree(staging, ignore_errors=True)


def _tar_spec_path(jobid: str, *, multi_user: bool) -> Path:
    if multi_user:
        return paths.resolve_spec_path(jobid, multi_user=True)
    spec_path = paths.spec_path(jobid)
    if not spec_path.exists():
        raise FileNotFoundError(f"no such job: {jobid}")
    return spec_path


def emit_workspace_tar(jobid: str, *, multi_user: bool = False) -> None:
    """Write the local workspace tarball for ``jobid`` to ``sys.stdout.buffer``.

    Used by the internal ``vq tar-workspace`` verb. Single-file functions
    like this stay separate from :func:`fetch_local` because the latter
    writes to a directory and this one writes a stream.

    Archive-aware (v0.5.10.1+): when the spec has ``archived_at`` set, the
    workspace dir is gone but the tarball lives at ``spec.archive_path``.
    We stream those bytes directly to stdout. The receiving end
    (``fetch_remote``) opens with ``tarfile.open(mode="r|")`` which
    autodetects compression, so the bz2-compressed archive bytes flow
    through unchanged.
    """
    _, spec = resolve_authorized_spec(jobid, multi_user=multi_user)
    if spec.is_archived and spec.archive_path:
        archive = Path(spec.archive_path)
        if not archive.is_file():
            raise FileNotFoundError(
                f"archive for job {jobid} not found at {archive} "
                "(record exists in spec but file is gone)"
            )
        _stream_archive_with_diagnosis(archive, spec)
        return
    _refresh_live_scheduler_workspace(spec)
    src = Path(spec.cwd)
    if not src.is_dir():
        raise FileNotFoundError(f"workspace for job {jobid} not found at {src}")
    # v0.5.34: arcname is the spec's ``dest_dirname`` (= ``<name>-<jobid>``
    # when job_name is set, else just ``<jobid>``). The streaming tar's
    # top-level directory becomes the receiver's destination directory
    # name — see ``fetch_remote``'s peek-first-member logic.
    with tarfile.open(fileobj=sys.stdout.buffer, mode="w|") as tf:
        tf.add(src, arcname=spec.dest_dirname)
        _add_diagnosis_sidecar_to_tar(
            tf,
            arcname_root=spec.dest_dirname,
            spec=spec,
        )


# ----------------------------------------------------------------------
# v0.7.7 *Cerf's Datagram* — workdir variants
# ----------------------------------------------------------------------
#
# `spec.workdir` is the per-job scratch dir the daemon materializes at
# dispatch (v0.6.54). The job sees it as ``$VQ_WORKDIR`` and is
# expected to dump intermediate / large artefacts there. Operators
# need a way to pull that content back to the laptop without ssh'ing
# in and reading the workdir path out of `vq status`.
#
# The implementation mirrors workspace fetch closely — same streaming-
# tar pipeline, same per-host helper split, same destination naming
# rule with ``-workdir`` appended. The few differences:
#
# * No archive path. Workdirs are not archived by `vq cleanup
#   --archive` (which only touches the workspace + spec). If a workdir
#   has been swept by `clean_workdir_on_terminal=True` or by the
#   daemon's auto-cleanup pass, the fetch surfaces a precise "workdir
#   for job X was cleaned up at <time>" error rather than letting
#   `FileNotFoundError` bubble up cryptically.
# * `spec.workdir` is `Optional[str]` — pre-v0.6.54 specs and jobs
#   submitted with ``--no-workdir`` (a future opt-out) won't have one
#   to fetch. The error message names the gap explicitly.
# * Destination dir is ``<dest_dirname>-workdir`` so a workspace fetch
#   and a workdir fetch of the same job can coexist under one
#   ``-o DIR``.


def _workdir_dest_name(spec: JobSpec) -> str:
    """v0.7.7: pick the destination directory name for a workdir fetch.

    Same shape as :py:attr:`JobSpec.dest_dirname` but with
    ``-workdir`` appended so the two payloads (workspace + workdir) of
    the same job land in distinct sibling directories under the
    operator's ``-o DIR``.
    """
    return f"{spec.dest_dirname}-workdir"


def _workdir_missing_hint(spec: JobSpec) -> str:
    """Explain WHY a job's workdir directory is gone, for the 'not found'
    errors below.

    CLEAN-3: the daemon's auto-cleanup *age-sweep* and the opt-in
    ``--clean-tmp`` immediate cleanup are different things, and the previous
    hint blamed ``--clean-tmp`` for both — misdirecting an operator whose
    workdir was simply swept for age (they never passed ``--clean-tmp``). The
    sweep now stamps ``workdir_swept_at``, so we can name the real cause.
    """
    if spec.workdir_swept_at:
        return (
            " (the daemon's auto-cleanup age-sweep removed this workdir at "
            f"{spec.workdir_swept_at} — workdirs are swept once their job has "
            "been terminal longer than the configured workdir_max_age; fetch "
            "sooner, or raise / disable that retention)"
        )
    if spec.is_terminal and spec.clean_workdir_on_terminal:
        return (
            " (workdir was swept on terminal — the job was submitted with "
            "--clean-tmp; re-submit without it to keep the workdir fetchable)"
        )
    return ""


def _no_workdir_message(spec: JobSpec, jobid: str) -> str:
    """Explain the workspace-only contract for jobs without a workdir field."""
    if spec.scheduler_target:
        return (
            f"job {jobid} is scheduler-backed for {spec.scheduler_target}; "
            "scheduler jobs are workspace-only in vq. Use "
            f"`vq fetch {spec.scheduler_target} {jobid} -o DIR` without "
            "--workdir to copy the preserved scheduler workspace, including "
            "stdout, stderr, _vq markers, and generated output files."
        )
    return (
        f"job {jobid} has no workdir (pre-v0.6.54 spec or --no-workdir "
        "submit); only the workspace is fetchable with "
        f"`vq fetch {jobid} -o DIR`"
    )


def fetch_workdir_local(
    jobid: str,
    output_dir: Path,
    *,
    multi_user: bool = False,
    idempotent: bool = True,
) -> Path:
    """v0.7.7: copy the local workdir for ``jobid`` into
    ``output_dir/<dest_dirname>-workdir/``.

    Returns the path of the copied workdir. Raises
    :class:`FileNotFoundError` if the job isn't in the local queue,
    if the spec has no workdir field, or if the workdir directory is
    gone (cleaned up by ``clean_workdir_on_terminal`` or the daemon's
    auto-cleanup sweep). Same-job destinations follow the ``idempotent``
    contract of :func:`fetch_local`; other existing destinations raise
    :class:`FileExistsError`.
    """
    if multi_user:
        spec_path = paths.resolve_spec_path(jobid, multi_user=True)
    else:
        spec_path = paths.spec_path(jobid)
        if not spec_path.exists():
            raise FileNotFoundError(f"no such job: {jobid}")
    spec = JobSpec.read(spec_path)
    # v0.6.x: multi-user ownership check — same as workspace fetch.
    check_owner(spec, multi_user=multi_user)
    if not spec.workdir:
        raise FileNotFoundError(_no_workdir_message(spec, jobid))
    src = Path(spec.workdir)
    if not src.is_dir():
        # Be explicit about WHY the directory is gone (CLEAN-3): the age-sweep
        # and --clean-tmp are different causes; the hint helper names the real
        # one from workdir_swept_at / clean_workdir_on_terminal.
        raise FileNotFoundError(
            f"workdir for job {jobid} not found at {src}"
            f"{_workdir_missing_hint(spec)}"
        )
    output_dir.mkdir(parents=True, exist_ok=True)
    dst = output_dir / _workdir_dest_name(spec)
    if dst.exists() or dst.is_symlink():
        if _is_fetched_destination_for_job(dst, jobid):
            if not idempotent:
                raise FileExistsError(f"destination already fetched: {dst}")
            # (#114): refresh, never return the old snapshot.
        else:
            raise FileExistsError(
                f"destination already exists: {dst} (remove it or pick another -o)"
            )
    staging = Path(tempfile.mkdtemp(dir=output_dir, prefix=".vq-fetch-"))
    staged = staging / dst.name
    try:
        shutil.copytree(src, staged)
        _write_diagnosis_sidecar(staged, spec)
        _write_fetch_manifest(
            staged,
            _fetch_manifest_payload(
                jobid=jobid,
                job_name=spec.job_name,
                source_host="local",
                source_kind="workdir",
                source_path=str(src),
                transport_kind="local-copy",
            ),
        )
        _replace_directory(staged, dst)
    finally:
        shutil.rmtree(staging, ignore_errors=True)
    return dst


def fetch_workdir_remote(
    host_cfg: HostConfig,
    jobid: str,
    output_dir: Path,
    *,
    idempotent: bool = True,
) -> Path:
    """v0.7.7: stream the remote *workdir* (``spec.workdir``, the per-job
    scratch dir) for ``jobid`` into ``output_dir/<dest>/`` via the
    ``vq tar-workdir`` verb. See :func:`_stream_extract_remote_tar` for the
    transport + extraction contract. ``idempotent`` has the same same-job
    destination semantics as :func:`fetch_workdir_local`."""
    return _stream_extract_remote_tar(
        host_cfg,
        "tar-workdir",
        jobid,
        output_dir,
        idempotent=idempotent,
    ).destination


def emit_workdir_tar(jobid: str, *, multi_user: bool = False) -> None:
    """v0.7.7: write the local workdir tarball for ``jobid`` to
    ``sys.stdout.buffer``.

    Backs the internal ``vq tar-workdir`` verb. Unlike
    :func:`emit_workspace_tar`, there is no archive-aware path —
    workdirs aren't archived by ``vq cleanup --archive`` (which only
    touches the workspace + spec). If the workdir has been swept
    (via ``clean_workdir_on_terminal`` or the daemon's auto-cleanup
    pass), we surface a precise error so the operator knows why.
    """
    _, spec = resolve_authorized_spec(jobid, multi_user=multi_user)
    if not spec.workdir:
        raise FileNotFoundError(_no_workdir_message(spec, jobid))
    src = Path(spec.workdir)
    if not src.is_dir():
        raise FileNotFoundError(
            f"workdir for job {jobid} not found at {src}"
            f"{_workdir_missing_hint(spec)}"  # CLEAN-3: name the real cause
        )
    # arcname mirrors `_workdir_dest_name(spec)` so the receiver's
    # peek-first-member logic lands on a name with the ``-workdir``
    # suffix.
    with tarfile.open(fileobj=sys.stdout.buffer, mode="w|") as tf:
        tf.add(src, arcname=_workdir_dest_name(spec))
        _add_diagnosis_sidecar_to_tar(
            tf,
            arcname_root=_workdir_dest_name(spec),
            spec=spec,
        )


# ----------------------------------------------------------------------
# v0.12.0 *Hollerith's Return* — bulk fetch-back (`vq fetch-all`)
# ----------------------------------------------------------------------
#
# `vq fetch-all [HOST]` pulls EVERY terminal job's workspace back in one
# shot, so an operator who fired a fleet of jobs (an `--array` sweep, a
# batch of AICCM runs on compute-a / build-host) gets all the outputs back with a
# single command from the submitting folder, instead of one
# `vq fetch HOST JOBID` per job. In a PBS-style "everything runs in a
# copied scratch dir" world this is how the generated files (vibe-qc
# `.out` / `.system`, CRYSTAL, ORCA) find their way back to the user's
# directory.
#
# Idempotence is grounded in the destination's diagnosis + fetch manifest,
# not only the queue timestamp.  A remote terminal workspace acknowledges its
# exact spec after promotion, while an older exact receipt can safely retry a
# missing acknowledgement.  Live, stale, mismatched, bare, and foreign trees
# are never silently treated as completed fetches.


@dataclass(frozen=True)
class BulkFetchResult:
    """One job's outcome in a `vq fetch-all` run.

    ``outcome`` is one of ``"fetched"`` (``detail`` is the destination
    path), ``"skipped"`` (``detail`` is why, e.g. already present), or
    ``"error"`` (``detail`` is the message). Non-terminal and
    state-filtered-out jobs are dropped before a result is recorded, so
    they never show up as a row.

    ``state`` is the job's terminal state and ``failure_hint`` is the
    first line of a failed job's stderr tail (None for a COMPLETED job),
    so a bulk-sweep summary can flag inline WHICH fetched jobs died and
    why, closing the loop with the crash-feedback field.
    """

    jobid: str
    job_name: str | None
    outcome: str
    detail: str
    state: str | None = None
    failure_hint: str | None = None


def bulk_fetch(
    specs: list[JobSpec],
    *,
    states: set[str] | None,
    fetch_one: Callable[[str], Path],
) -> list[BulkFetchResult]:
    """Fetch every terminal spec via ``fetch_one(jobid) -> Path``.

    ``fetch_one`` is bound to the local or remote primitive by the caller
    (``fetch_local`` / ``fetch_remote`` with ``output_dir`` already closed
    over), which keeps this loop transport-agnostic and unit-testable with
    a fake.

    Filtering: a non-terminal spec is dropped silently (an active job has
    nothing to fetch yet, which is not an error). When ``states`` is given,
    only those terminal states pass, and ``None`` means every terminal
    state.

    Each job's fetch is isolated so one bad job never aborts the sweep: a
    FileExistsError becomes a ``"skipped"`` (already present, the
    idempotence path), and a missing workspace or a remote-transport
    failure or ownership denial becomes an ``"error"`` row the caller can
    report and move past.
    """
    results: list[BulkFetchResult] = []
    for spec in specs:
        if not spec.is_terminal:
            continue
        if states is not None and spec.state.value not in states:
            continue
        st = spec.state.value
        # First line of the crash tail for a non-COMPLETED terminal, capped
        # so the summary stays one line per job. COMPLETED jobs and pre-
        # v0.12.0 specs (no failure_tail) carry no hint.
        hint = None
        if st != "completed" and spec.failure_tail:
            hint = spec.failure_tail.splitlines()[0][:120]
        elif st != "completed" and spec.exit_code is not None:
            # No stderr to tail (a hard SIGKILL/OOM or a segfault often leaves
            # none): fall back to decoding the signal from the exit code, so
            # the sweep still names the crash instead of a bare [FAILED].
            decoded = describe_exit_code(spec.exit_code)
            if decoded != str(spec.exit_code):
                hint = decoded
        try:
            dst = fetch_one(spec.id)
            results.append(
                BulkFetchResult(spec.id, spec.job_name, "fetched", str(dst), st, hint)
            )
        except FileExistsError:
            results.append(
                BulkFetchResult(
                    spec.id, spec.job_name, "skipped", "already present", st, hint
                )
            )
        except (
            FileNotFoundError,
            OwnershipError,
            ValueError,
            transport.RemoteError,
        ) as e:
            results.append(
                BulkFetchResult(spec.id, spec.job_name, "error", str(e), st, hint)
            )
    return results
