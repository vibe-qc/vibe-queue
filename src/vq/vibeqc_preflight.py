"""vibe-qc submit pre-flight — harvest the OutputPlan before queueing.

When the submitted job is a Python script that uses a vibe-qc driver with
dry-run support (for example ``vibeqc.run_job`` or ``vibeqc.run_neb``), the
script can be executed *once* with ``VIBEQC_DRY_RUN=1`` set to make the driver
short-circuit after building its ``OutputPlan``: it writes ``{stem}.system``
with the ``[plan]`` section populated and ``[outputs].status = "dry_run"``,
prints the declared-artefacts summary, and returns ``None`` without running
the SCF / NEB.

This module runs that pre-flight inside the freshly-materialised
workspace, reads the resulting manifest, and returns the declared
artefact list to :func:`vq.submit.submit_local` so it can populate
:attr:`JobSpec.expected_outputs` / :attr:`JobSpec.output_stem` /
:attr:`JobSpec.last_output_status`.

Opt-in by design — the pre-flight executes the user's script (even
in dry-run mode the import side-effects run), so submitting an
arbitrary Python file should not silently run it. The CLI exposes
the toggle as ``vq submit --vibeqc-preflight``. The submit-side
plumbing is unconditional; the *running* of the pre-flight is gated
on the caller's opt-in flag.

Failure semantics: any error (timeout, non-zero exit, missing
manifest, malformed TOML, no ``[plan]`` section) is *non-fatal* —
the function logs a warning and returns ``None``. The submit
proceeds normally; only the optional fields stay empty. The dry-run
pre-flight is a best-effort observability win, not a correctness
prerequisite.
"""

from __future__ import annotations

import logging
import os
import subprocess
import time
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

__all__ = ["PreflightResult", "vibeqc_dry_run_preflight"]

log = logging.getLogger(__name__)


# Default timeout for the pre-flight run. The dry-run path through
# vibe-qc drivers is meant to be very fast (sub-second for typical
# scripts) — anything beyond 10s is almost certainly a script doing
# work outside run_job (network fetches, heavy imports, …). The
# caller can override via the ``timeout`` kwarg.
_DEFAULT_TIMEOUT_S = 10.0

# Recursive discovery is deliberately bounded separately from the submitted
# command. A copied source tree can be arbitrarily large, and optional
# preflight must never turn submission into an unbounded filesystem walk.
_MANIFEST_SCAN_TIMEOUT_S = 1.0
_MANIFEST_SCAN_MAX_ENTRIES = 20_000
_MANIFEST_SCAN_MAX_CANDIDATES = 256

_ManifestFingerprint = tuple[int, int, int, int, int]


@dataclass(frozen=True)
class _ManifestCandidate:
    path: Path
    relative: Path
    fingerprint: _ManifestFingerprint


@dataclass(frozen=True)
class PreflightResult:
    """The successful outcome of a dry-run pre-flight.

    ``expected_outputs`` are workspace-relative paths. When the
    user's script writes ``output-h2o.{out,system,molden,...}`` next
    to their input file, every path in this list is relative to the
    workspace root.
    """

    expected_outputs: list[str] = field(default_factory=list)
    output_stem: str | None = None
    # Method / basis / functional from the [plan] section. Kept for
    # the daemon's event log / ``vq status`` UI so the operator
    # knows what kind of run is queued without running the SCF.
    method: str | None = None
    basis: str | None = None
    functional: str | None = None
    # v0.11.0: peak-memory estimate in bytes from the manifest's
    # ``[memory].estimate_bytes`` (vibe-qc's estimate_memory, already
    # carrying its 1.2× headroom). Present only when the pre-flight was
    # run with ``with_estimate=True`` (sets ``VIBEQC_DRY_RUN_ESTIMATE=1``)
    # and the job is an estimable method; ``None`` otherwise. Feeds
    # ``vq submit auto`` RAM-fit placement.
    estimate_bytes: int | None = None


def vibeqc_dry_run_preflight(
    workspace: Path,
    command: list[str],
    *,
    timeout: float = _DEFAULT_TIMEOUT_S,
    with_estimate: bool = False,
) -> PreflightResult | None:
    """Run the submitted command once with ``VIBEQC_DRY_RUN=1``, read
    the resulting ``*.system`` manifest, return the declared
    artefacts.

    Parameters
    ----------
    workspace
        The materialised workspace directory (``<jobs_dir>/<jobid>``).
        The command is run with this as cwd.
    command
        The same command list that will be passed to the daemon —
        typically ``[python_interpreter, input_script_basename]``.
        Reused verbatim so the interpreter resolution matches the
        live job's, and the script can ``import`` workspace-local
        modules cleanly.
    timeout
        Seconds before the pre-flight is killed. Default 10s. A
        timeout returns ``None`` and logs a warning; the submit
        proceeds normally.

    Returns
    -------
    PreflightResult | None
        ``None`` if the pre-flight failed for any reason (no
        manifest produced, command returned non-zero, malformed
        TOML, manifest has no ``[plan]`` section, timed out, …).
        On success, the declared artefact set.
    """
    env = dict(os.environ)
    env["VIBEQC_DRY_RUN"] = "1"
    # v0.11.0: opt into the heavier estimate pass (build basis + call
    # estimate_memory, recorded in the manifest's [memory] section). Off by
    # default so the output-discovery pre-flight stays cheap; `vq submit auto`
    # sets it to learn the job's peak-memory estimate for RAM-fit placement.
    if with_estimate:
        env["VIBEQC_DRY_RUN_ESTIMATE"] = "1"
    # Suppress live-progress output and the auto-on banner so the
    # pre-flight is quiet — its job is to write the manifest, not
    # to mirror the SCF banner to vq's submit stdout.
    env.setdefault("VIBEQC_LIVE_LOGGING", "0")
    # The dry-run path in vibe-qc drivers already returns before any
    # crash-dump scope opens, but disabling them here costs nothing
    # and removes a class of side-effect.
    env.setdefault("VIBEQC_NO_CRASH_DUMP", "1")

    before = _scan_manifest_candidates(workspace)
    if before is None:
        log.info(
            "vibe-qc dry-run pre-flight manifest discovery exceeded its "
            "bounded scan; submit proceeds without expected_outputs"
        )
        return None
    before_fingerprints = {
        candidate.relative.as_posix(): candidate.fingerprint
        for candidate in before
    }

    try:
        proc = subprocess.run(
            command,
            cwd=str(workspace),
            env=env,
            capture_output=True,
            timeout=timeout,
            check=False,
            stdin=subprocess.DEVNULL,
        )
    except subprocess.TimeoutExpired:
        log.warning(
            "vibe-qc dry-run pre-flight timed out after %.1fs; "
            "submit proceeds without expected_outputs",
            timeout,
        )
        return None
    except OSError as exc:
        log.warning(
            "vibe-qc dry-run pre-flight could not start (%s: %s); "
            "submit proceeds without expected_outputs",
            type(exc).__name__, exc,
        )
        return None

    if proc.returncode != 0:
        # Non-zero exit could be: script doesn't use vibeqc at all,
        # script crashes during import, --vibeqc-dry-run not honoured
        # (older vibe-qc). Not a submit-blocker — the user's intent is
        # still clear ("queue this script"); we just can't harvest the
        # plan.
        stderr_tail = (proc.stderr or b"").decode(
            "utf-8", errors="replace",
        ).strip().splitlines()[-3:]
        log.info(
            "vibe-qc dry-run pre-flight exited %d; submit proceeds "
            "without expected_outputs (stderr tail: %r)",
            proc.returncode, stderr_tail,
        )
        return None

    after = _scan_manifest_candidates(workspace)
    if after is None:
        log.info(
            "vibe-qc dry-run pre-flight manifest discovery exceeded its "
            "bounded scan; submit proceeds without expected_outputs"
        )
        return None
    manifest = _find_dry_run_manifest(after, before_fingerprints)
    if manifest is None:
        log.info(
            "vibe-qc dry-run pre-flight produced no *.system manifest "
            "in %s; submit proceeds without expected_outputs",
            workspace,
        )
        return None

    return _parse_manifest_for_outputs(workspace, manifest)


# ---------------------------------------------------------------------- #
# Helpers                                                                #
# ---------------------------------------------------------------------- #

def _scan_manifest_candidates(
    workspace: Path,
) -> list[_ManifestCandidate] | None:
    """Return contained manifest candidates, or ``None`` if scan bounds hit."""
    try:
        workspace_root = workspace.resolve()
    except (OSError, RuntimeError):
        return None

    deadline = time.monotonic() + _MANIFEST_SCAN_TIMEOUT_S
    candidates: list[_ManifestCandidate] = []
    pending = [workspace_root]
    entries_seen = 0
    while pending:
        if time.monotonic() > deadline:
            return None
        directory = pending.pop()
        children: list[Path] = []
        try:
            entries = os.scandir(directory)
        except OSError:
            return None
        with entries:
            for entry in entries:
                entries_seen += 1
                if (
                    entries_seen > _MANIFEST_SCAN_MAX_ENTRIES
                    or time.monotonic() > deadline
                ):
                    return None
                try:
                    if entry.is_dir(follow_symlinks=False):
                        children.append(Path(entry.path))
                        continue
                    if not entry.name.endswith(".system"):
                        continue
                    if not entry.is_file(follow_symlinks=True):
                        continue
                    resolved = Path(entry.path).resolve()
                    relative = resolved.relative_to(workspace_root)
                    stat_result = resolved.stat()
                except (OSError, RuntimeError, ValueError):
                    continue
                candidates.append(
                    _ManifestCandidate(
                        path=resolved,
                        relative=relative,
                        fingerprint=(
                            stat_result.st_dev,
                            stat_result.st_ino,
                            stat_result.st_size,
                            stat_result.st_mtime_ns,
                            stat_result.st_ctime_ns,
                        ),
                    )
                )
                if len(candidates) > _MANIFEST_SCAN_MAX_CANDIDATES:
                    return None
        pending.extend(
            sorted(children, key=lambda path: path.as_posix(), reverse=True)
        )

    return sorted(candidates, key=lambda item: item.relative.as_posix())


def _find_dry_run_manifest(
    candidates: list[_ManifestCandidate],
    before: dict[str, _ManifestFingerprint],
) -> Path | None:
    """Return the unique new or changed dry-run manifest.

    Source payloads may already contain valid manifests. Accepting one merely
    because it sorts first can attach stale output metadata to a new job, so a
    candidate must differ from the pre-execution snapshot. Multiple changed
    dry-run manifests are ambiguous and make optional preflight fail closed.
    """
    matches: list[Path] = []
    for candidate in candidates:
        if before.get(candidate.relative.as_posix()) == candidate.fingerprint:
            continue
        try:
            with candidate.path.open("rb") as fh:
                body = tomllib.load(fh)
        except (OSError, tomllib.TOMLDecodeError):
            continue
        if body.get("outputs", {}).get("status") == "dry_run":
            matches.append(candidate.path)

    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        log.info(
            "vibe-qc dry-run pre-flight produced multiple changed "
            "*.system manifests (%s); submit proceeds without "
            "expected_outputs",
            [path.name for path in matches],
        )
    return None


def _parse_manifest_for_outputs(
    workspace: Path,
    manifest_path: Path,
) -> PreflightResult | None:
    """Read ``manifest_path``, pull the ``[plan]`` section, return a
    :class:`PreflightResult` with workspace-relative paths."""
    try:
        with manifest_path.open("rb") as fh:
            body = tomllib.load(fh)
    except (OSError, tomllib.TOMLDecodeError) as exc:
        log.warning(
            "vibe-qc dry-run pre-flight: %s parsed unsuccessfully "
            "(%s: %s); submit proceeds without expected_outputs",
            manifest_path, type(exc).__name__, exc,
        )
        return None

    plan = body.get("plan")
    if not isinstance(plan, dict):
        log.info(
            "vibe-qc dry-run pre-flight: %s has no [plan] section "
            "(pre-Phase-O1 manifest?); submit proceeds without "
            "expected_outputs",
            manifest_path,
        )
        return None

    declared_files = plan.get("files") or []
    expected: list[str] = []
    for row in declared_files:
        if not isinstance(row, dict):
            continue
        raw = row.get("path")
        if not raw:
            continue
        # Normalize to a contained workspace-relative POSIX path so the JSON
        # field round-trips identically across hosts. Absolute paths outside
        # the workspace, lexical parent traversal, NULs, and existing symlink
        # escapes are dropped because they cannot be fetched safely.
        try:
            raw_text = str(raw)
            if "\x00" in raw_text:
                continue
            p = Path(raw_text)
            if p.is_absolute():
                p = p.resolve().relative_to(workspace.resolve())
            else:
                if p == Path(".") or ".." in p.parts:
                    continue
                p = (workspace / p).resolve().relative_to(workspace.resolve())
            if p != Path("."):
                expected.append(p.as_posix())
        except (OSError, RuntimeError, ValueError):
            continue

    stem_raw = plan.get("stem")
    stem_str: str | None = None
    if isinstance(stem_raw, str) and stem_raw.strip():
        try:
            if "\x00" in stem_raw:
                raise ValueError("NUL in output stem")
            sp = Path(stem_raw)
            if sp.is_absolute():
                sp = sp.resolve().relative_to(workspace.resolve())
            else:
                if sp == Path(".") or ".." in sp.parts:
                    raise ValueError("unsafe relative output stem")
                sp = (workspace / sp).resolve().relative_to(
                    workspace.resolve()
                )
            if sp != Path(".") and len(Path(sp.name).parts) == 1:
                stem_str = sp.name
        except (OSError, RuntimeError, ValueError):
            stem_str = None

    def _opt_str(key: str) -> str | None:
        val = plan.get(key)
        if isinstance(val, str) and val.strip():
            return val
        return None

    # v0.11.0: peak-memory estimate from the manifest's [memory] section
    # (written only when VIBEQC_DRY_RUN_ESTIMATE=1). bool is an int subclass
    # in Python, so guard it out explicitly. Absent / non-positive → None.
    estimate_bytes: int | None = None
    mem_section = body.get("memory")
    if isinstance(mem_section, dict):
        raw = mem_section.get("estimate_bytes")
        if isinstance(raw, int) and not isinstance(raw, bool) and raw > 0:
            estimate_bytes = raw

    return PreflightResult(
        expected_outputs=expected,
        output_stem=stem_str,
        method=_opt_str("method"),
        basis=_opt_str("basis"),
        functional=_opt_str("functional"),
        estimate_bytes=estimate_bytes,
    )
