"""vq admin auto-update — latest-tag drift detection + apply (v0.6.11).

Operator-driven helper for the common pattern:

   "is there a vibe-qc release tag newer than what my vibeqc-release
    env has checked out? If yes, refresh the env to it."

Policy is explicit per managed environment:

* ``auto_update_policy="tag"`` tracks the newest SemVer tag and applies it
  inline through :func:`admin.update_env`.
* ``auto_update_policy="branch"`` tracks ``origin/<branch>`` and routes drift
  through a capped, deduplicated ``build-env`` job. A branch-mode target that
  is the running vq daemon's own environment is rejected before submission;
  use the exact-SHA ``vq self-update`` lifecycle for that case.
* ``--all`` and ``--all-hosts`` compose those per-environment policies while
  isolating failures. Standalone scheduler-runtime auto-update is retired;
  immutable scheduler deployments are reconciled from accepted release
  reports through ``admin rollout-latest``.
* Implement the timer itself. ``contrib/`` has the daemon's
  systemd-user unit; operators can mirror that shape for an auto-
  update timer trivially once the CLI works.

Decision logic:

1. ``git ls-remote --tags origin`` on the env's clone → set of remote tags.
2. Strictly parse direct and peeled tag refs, rejecting malformed,
   contradictory, or incomplete inventories rather than choosing from partial
   evidence.
3. Pick the newest strict SemVer tag by precedence, including prerelease
   identifiers while ignoring build metadata.
4. Enumerate all strict SemVer tags pointing at ``HEAD`` and select the
   highest unambiguous local precedence for the no-downgrade comparison.
5. If different → :class:`AutoUpdateDecision` ``action="update"`` with the
   exact named tag and peeled 40-hex commit. Else → ``action="skip"``.

Apply is a thin delegate to :func:`admin.update_env` with
``expected_tag=target_tag`` and ``expected_sha=target_sha`` so both named-tag
and immutable-commit verification fire.
"""
from __future__ import annotations

import logging
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from vq import admin, build_job, config
from vq.host import is_local_host

log = logging.getLogger(__name__)


SCHEDULER_RUNTIME_AUTO_UPDATE_DISABLED_REASON = (
    "standalone scheduler-runtime auto-update is disabled; use "
    "`vq admin rollout-latest`, which reconciles scheduler runtimes from "
    "an accepted release report"
)

# SemVer tag pattern with the repository's required leading ``v``.  Capture
# prerelease separately because it participates in precedence; build metadata
# deliberately does not.  This is strict enough to reject empty/dangling
# identifiers while retaining the tag spellings historically supported by vq.
_SEMVER_NUMBER = r"(?:0|[1-9][0-9]*)"
_SEMVER_IDENTIFIER = r"[0-9A-Za-z-]+"
_SEMVER_TAG_RE = re.compile(
    rf"^v({_SEMVER_NUMBER})\.({_SEMVER_NUMBER})\.({_SEMVER_NUMBER})"
    rf"(?:-({_SEMVER_IDENTIFIER}(?:\.{_SEMVER_IDENTIFIER})*))?"
    rf"(?:\+({_SEMVER_IDENTIFIER}(?:\.{_SEMVER_IDENTIFIER})*))?$"
)
_FULL_SHA_RE = re.compile(r"[0-9a-fA-F]{40}")

# Timeout for git ls-remote. Network operation; allow more time than
# the local git probes but still bounded so a hung remote doesn't
# wedge an unattended timer.
_LS_REMOTE_TIMEOUT_SECONDS = 30


@dataclass(frozen=True)
class AutoUpdateDecision:
    """The outcome of :func:`check_env_drift` (and the dry-run output
    of :func:`auto_update_env`)."""

    env_name: str
    action: Literal["update", "skip", "error"]
    reason: str
    current_tag: str | None
    """The highest unambiguous strict SemVer tag pointing at ``HEAD``.

    ``None`` if HEAD has no strict SemVer tag (for example, a dev branch
    checkout) or when ``policy="branch"`` (which does not query tags)."""
    target_tag: str | None
    """Tag to update to. None when ``action != "update"`` OR when
    ``policy="branch"`` (which doesn't target tags)."""
    policy: Literal["tag", "branch"] = "tag"
    """v0.7.4 *Ritchie's Pipe*: which drift signal was used to make
    this decision. Mirrors the source ``VenvProgram.auto_update_policy``
    field — kept on the decision so the formatter can render
    ``"already at origin/main (a1b2c3d)"`` vs ``"already at latest
    tag v0.9.1"`` without re-consulting config."""
    current_sha: str | None = None
    """The env's exact HEAD SHA at decision time.

    In tag mode it is bound to ``current_tag``; in branch mode it is the
    no-downgrade baseline. Scheduler-runtime decisions use it for the last
    verified deployment SHA."""
    target_sha: str | None = None
    """The exact target commit for an update.

    In tag mode this is the peeled commit of ``target_tag``; in branch mode it
    is the authenticated ``origin/<branch>`` SHA. ``None`` when no update
    target can be resolved."""


@dataclass(frozen=True)
class AutoUpdateOutcome:
    """The outcome of :func:`auto_update_env` (decision + optional
    apply result). When ``decision.action == "update"`` and the verb
    wasn't run in dry-run mode, ``update_result`` carries the
    underlying :class:`admin.UpdateResult`; otherwise it's None."""

    decision: AutoUpdateDecision
    update_result: admin.UpdateResult | None
    build_submit: build_job.BuildSubmitOutcome | None = None
    """v0.12.x fix 2: set (instead of ``update_result``) when branch-mode
    drift was routed through a capped, deduped build-env JOB on the local
    daemon rather than an inline rebuild. Carries the submit verdict
    (submitted / deduped / backed_off / error); the build itself runs
    async and its success is reported later by the job, not here."""
    scheduler_runtime_result: admin.SchedulerRuntimeUpdateResult | None = None
    """Retained for response compatibility with the retired scheduler-runtime
    auto-update surface. Public entry points now fail closed, so this remains
    ``None``."""


def _parse_semver_tag(tag: str) -> tuple[int, int, int] | None:
    """Parse a ``vMAJOR.MINOR.PATCH`` tag into a sortable tuple. Returns
    None for non-semver tags (which are excluded from the drift
    comparison — operator can still use ``vq admin update --tag``
    directly for unusual tag names)."""
    m = _SEMVER_TAG_RE.match(tag)
    if m is None or not _valid_prerelease(m.group(4)):
        return None
    return (int(m.group(1)), int(m.group(2)), int(m.group(3)))


def _valid_prerelease(prerelease: str | None) -> bool:
    """Whether numeric prerelease identifiers obey SemVer's zero rule."""
    if prerelease is None:
        return True
    return all(
        not (
            identifier.isdigit()
            and len(identifier) > 1
            and identifier.startswith("0")
        )
        for identifier in prerelease.split(".")
    )


def _semver_precedence(
    tag: str,
) -> tuple[int, int, int, int, tuple[tuple[int, int | str], ...]] | None:
    """Return a comparison key implementing SemVer 2 precedence.

    Stable releases sort after prereleases with the same core version;
    numeric prerelease identifiers compare numerically and sort before text
    identifiers; build metadata is ignored.  The helper intentionally stays
    private because accepted tag syntax remains the public policy boundary.
    """
    match = _SEMVER_TAG_RE.match(tag)
    if match is None or not _valid_prerelease(match.group(4)):
        return None
    core = (int(match.group(1)), int(match.group(2)), int(match.group(3)))
    prerelease = match.group(4)
    if prerelease is None:
        return (*core, 1, ())
    identifiers: list[tuple[int, int | str]] = []
    for identifier in prerelease.split("."):
        if identifier.isdigit():
            identifiers.append((0, int(identifier)))
        else:
            identifiers.append((1, identifier))
    return (*core, 0, tuple(identifiers))


def _newest_semver_tag(tags: list[str]) -> str | None:
    """Return the newest tag in ``tags`` by SemVer 2 precedence.

    Build metadata has no precedence; the full tag is only a deterministic
    final tiebreak for two otherwise-equal spellings.  Returns ``None`` when
    no input tag matches the accepted SemVer shape.
    """
    parsed = [(tag, _semver_precedence(tag)) for tag in tags]
    versioned = [(t, v) for t, v in parsed if v is not None]
    if not versioned:
        return None
    # The full tag only resolves equal-precedence spellings (usually build
    # metadata); it must never override prerelease precedence.
    versioned.sort(key=lambda x: (x[1], x[0]))
    return versioned[-1][0]


def _equal_precedence_newest_tags(tags: list[str]) -> tuple[str, ...]:
    """Return every spelling tied at the highest SemVer precedence."""
    versioned = [
        (tag, precedence)
        for tag in tags
        if (precedence := _semver_precedence(tag)) is not None
    ]
    if not versioned:
        return ()
    highest = max(precedence for _tag, precedence in versioned)
    return tuple(sorted(tag for tag, precedence in versioned if precedence == highest))


def _list_remote_tag_refs(git_dir: Path) -> dict[str, str]:
    """Return remote tag names mapped to peeled commit identities.

    Lightweight tags use the direct ref SHA. Annotated tags appear twice in
    ``ls-remote`` output; their ``^{}`` row is the commit and wins over the
    tag-object SHA. Once a row claims the tag namespace, malformed or
    contradictory identity fails the whole inventory rather than silently
    hiding a possibly-newest release.
    """
    proc = subprocess.run(
        ["git", "-C", str(git_dir), "ls-remote", "--tags", "origin"],
        capture_output=True,
        text=True,
        timeout=_LS_REMOTE_TIMEOUT_SECONDS,
        check=True,
        stdin=subprocess.DEVNULL,
    )
    direct: dict[str, str] = {}
    peeled: dict[str, str] = {}
    for line in proc.stdout.splitlines():
        # Each line: "<sha>\trefs/tags/<tag>" or "...^{}"
        parts = line.split("\t", 1)
        if len(parts) != 2:
            if "refs/tags/" in line:
                raise ValueError(f"malformed remote tag row: {line!r}")
            continue
        ref = parts[1].strip()
        if not ref.startswith("refs/tags/"):
            continue
        sha = parts[0].strip().lower()
        if _FULL_SHA_RE.fullmatch(sha) is None:
            raise ValueError(f"remote tag row has invalid object ID: {line!r}")
        tag = ref[len("refs/tags/"):]
        is_peeled = tag.endswith("^{}")
        if tag.endswith("^{}"):
            tag = tag[:-3]
        if not tag:
            raise ValueError("remote tag row has an empty tag name")
        if is_peeled:
            if tag in peeled and peeled[tag] != sha:
                raise ValueError(
                    f"remote tag {tag!r} has conflicting peeled identities"
                )
            peeled[tag] = sha
        else:
            if tag in direct and direct[tag] != sha:
                raise ValueError(
                    f"remote tag {tag!r} has conflicting direct identities"
                )
            direct[tag] = sha
    # A peeled row is meaningful only alongside its direct tag-object row.
    # Ignore orphan ``^{}`` records rather than manufacturing a tag identity
    # from malformed remote output.
    orphaned = sorted(set(peeled) - set(direct))
    if orphaned:
        raise ValueError(
            "remote tag inventory has orphan peeled rows: "
            + ", ".join(orphaned)
        )
    return {tag: peeled.get(tag, sha) for tag, sha in direct.items()}


def _local_semver_tags_at_head(
    git_dir: Path,
) -> tuple[list[str] | None, str | None]:
    """Enumerate every strict SemVer tag pointing at current HEAD."""
    try:
        proc = subprocess.run(
            ["git", "-C", str(git_dir), "tag", "--points-at", "HEAD"],
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
            stdin=subprocess.DEVNULL,
        )
    except (subprocess.TimeoutExpired, OSError) as exc:
        return None, str(exc)
    if proc.returncode != 0:
        return None, (proc.stderr or "git tag --points-at failed").strip()
    tags = [line.strip() for line in proc.stdout.splitlines() if line.strip()]
    return [tag for tag in tags if _semver_precedence(tag) is not None], None


def _list_remote_tags(git_dir: Path) -> list[str]:
    """Compatibility wrapper returning the validated remote tag names."""
    return list(_list_remote_tag_refs(git_dir))


def _fetch_origin(git_dir: Path) -> tuple[int, str]:
    """v0.7.4: ``git fetch origin`` so the local origin-tracking refs
    reflect upstream HEAD. Returns ``(rc, stderr)``; rc != 0 means
    network / auth / config problem. Bounded by the same network
    timeout as ``_list_remote_tags`` since the operation type is
    identical."""
    try:
        proc = admin._mutating_git_run(
            ["git", "-C", str(git_dir), "fetch", "origin"],
            capture_output=True,
            text=True,
            timeout=_LS_REMOTE_TIMEOUT_SECONDS,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return 1, f"git fetch timed out after {_LS_REMOTE_TIMEOUT_SECONDS}s"
    except OSError as e:
        return 1, f"git fetch failed to start: {e}"
    return proc.returncode, (proc.stderr or "").strip()


def _rev_parse(git_dir: Path, ref: str) -> str | None:
    """v0.7.4: ``git -C <dir> rev-parse <ref>`` returning the full
    SHA, or ``None`` on any failure (no such ref, no .git, network
    error). Used by branch-mode drift to compare HEAD vs
    origin/<branch>."""
    try:
        proc = subprocess.run(
            ["git", "-C", str(git_dir), "rev-parse", ref],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
            stdin=subprocess.DEVNULL,
        )
    except (subprocess.TimeoutExpired, OSError):
        return None
    if proc.returncode != 0:
        return None
    sha = proc.stdout.strip()
    return sha or None


def _is_ancestor(git_dir: Path, older: str, newer: str) -> bool | None:
    """Return whether ``older`` is an ancestor of ``newer``.

    ``None`` means Git could not make an authoritative determination.  An
    unattended updater must treat that as an error, never as permission to
    replace a checkout in an unknown direction.
    """
    try:
        proc = subprocess.run(
            [
                "git", "-C", str(git_dir), "merge-base", "--is-ancestor",
                older, newer,
            ],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
            stdin=subprocess.DEVNULL,
        )
    except (subprocess.TimeoutExpired, OSError):
        return None
    if proc.returncode == 0:
        return True
    if proc.returncode == 1:
        return False
    return None


def _check_branch_drift(
    env: str, prog: config.VenvProgram, git_dir: Path,
) -> AutoUpdateDecision:
    """v0.7.4 *Ritchie's Pipe* branch-mode drift check.

    Compares local ``HEAD`` SHA to ``origin/<branch>`` SHA after a
    fresh ``git fetch``. Returns ``action="update"`` only when the current
    checkout is an ancestor of the remote target. A current-ahead checkout is
    retained and divergence is an error; unattended branch policy never
    authorizes a downgrade or sideways rewrite.

    Pre-conditions checked + reported as ``action="error"``:
      * ``prog.branch`` must be set (else there's no branch to
        track and the policy is mis-configured)
      * ``git fetch`` must succeed (network / auth probe)
      * Both rev-parse calls (local HEAD + origin/<branch>) must
        succeed (else config / clone state is broken)
    """
    if not prog.branch:
        return AutoUpdateDecision(
            env_name=env,
            action="error",
            reason=(
                f"auto_update_policy='branch' requires `branch = \"...\"` "
                f"to be set in [programs.{env}] — branch-mode tracks "
                f"origin/<branch> and there's nothing to track when "
                f"the field is empty."
            ),
            current_tag=None, target_tag=None,
            policy="branch",
        )

    rc, fetch_err = _fetch_origin(git_dir)
    if rc != 0:
        return AutoUpdateDecision(
            env_name=env,
            action="error",
            reason=f"git fetch origin failed (rc={rc}): {fetch_err or '(no stderr)'}",
            current_tag=None, target_tag=None,
            policy="branch",
        )

    current_sha = _rev_parse(git_dir, "HEAD")
    target_ref = f"origin/{prog.branch}"
    target_sha = _rev_parse(git_dir, target_ref)
    if current_sha is None:
        return AutoUpdateDecision(
            env_name=env,
            action="error",
            reason="git rev-parse HEAD failed — broken .git directory?",
            current_tag=None, target_tag=None,
            policy="branch",
        )
    if target_sha is None:
        return AutoUpdateDecision(
            env_name=env,
            action="error",
            reason=(
                f"git rev-parse {target_ref} failed — branch may not "
                f"exist on origin, or fetch silently dropped the ref. "
                f"Check `git remote -v` + `git branch -a` in {git_dir}."
            ),
            current_tag=None, target_tag=None,
            policy="branch",
        )

    if current_sha == target_sha:
        return AutoUpdateDecision(
            env_name=env,
            action="skip",
            reason=(
                f"already at origin/{prog.branch} "
                f"(SHA {current_sha[:12]})"
            ),
            current_tag=None, target_tag=None,
            policy="branch",
            current_sha=current_sha,
            target_sha=target_sha,
        )

    current_before_target = _is_ancestor(git_dir, current_sha, target_sha)
    if current_before_target is None:
        return AutoUpdateDecision(
            env_name=env,
            action="error",
            reason=(
                f"could not prove ancestry from local HEAD {current_sha[:12]} "
                f"to origin/{prog.branch} {target_sha[:12]}; refusing an "
                "unattended move"
            ),
            current_tag=None, target_tag=None,
            policy="branch",
            current_sha=current_sha,
            target_sha=target_sha,
        )
    if not current_before_target:
        target_before_current = _is_ancestor(git_dir, target_sha, current_sha)
        if target_before_current is True:
            return AutoUpdateDecision(
                env_name=env,
                action="skip",
                reason=(
                    f"local HEAD {current_sha[:12]} is ahead of "
                    f"origin/{prog.branch} {target_sha[:12]}; refusing "
                    "unattended downgrade"
                ),
                current_tag=None, target_tag=None,
                policy="branch",
                current_sha=current_sha,
                target_sha=target_sha,
            )
        return AutoUpdateDecision(
            env_name=env,
            action="error",
            reason=(
                f"local HEAD {current_sha[:12]} and origin/{prog.branch} "
                f"{target_sha[:12]} have diverged; refusing an unattended "
                "sideways move"
            ),
            current_tag=None, target_tag=None,
            policy="branch",
            current_sha=current_sha,
            target_sha=target_sha,
        )

    return AutoUpdateDecision(
        env_name=env,
        action="update",
        reason=(
            f"branch drift: local HEAD={current_sha[:12]}, "
            f"origin/{prog.branch}={target_sha[:12]}"
        ),
        current_tag=None, target_tag=None,
        policy="branch",
        current_sha=current_sha,
        target_sha=target_sha,
    )


def check_env_drift(env: str, cfg: config.Config) -> AutoUpdateDecision:
    """Decide whether ``env`` is behind its tracked source on origin.
    Pure function — no side effects, no spec writes, no workspace
    touches. Returns an :class:`AutoUpdateDecision` the caller can
    inspect or apply.

    Behavior depends on ``VenvProgram.auto_update_policy``:

    * ``"tag"`` (default, v0.6.11): drift = HEAD doesn't point at
      the newest semver-shaped tag on origin. Update target is the
      new tag (``decision.target_tag``).
    * ``"branch"`` (v0.7.4 *Ritchie's Pipe*): drift = local HEAD
      SHA differs from ``origin/<branch>`` SHA. Update target is
      the branch tip (``decision.target_sha``); no tag is involved.
      Requires ``prog.branch`` to be set — otherwise the decision
      is ``action="error"`` with a config-pointing reason.

    Refuses non-``kind=venv`` programs (the verb only makes sense
    for git-backed envs) via :class:`admin.AdminError`.
    """
    prog = admin._resolve_venv_program(env, cfg)
    git_dir = Path(prog.git_dir)

    if prog.auto_update_policy == "branch":
        return _check_branch_drift(env, prog, git_dir)
    # Fall through to tag-mode (the v0.6.11 default).

    # Probe remote for tag list.
    try:
        remote_tag_refs = _list_remote_tag_refs(git_dir)
    except subprocess.CalledProcessError as e:
        return AutoUpdateDecision(
            env_name=env,
            action="error",
            reason=(
                f"git ls-remote --tags origin failed (rc={e.returncode}): "
                f"{(e.stderr or '').strip() or '(no stderr)'}"
            ),
            current_tag=None,
            target_tag=None,
        )
    except ValueError as e:
        return AutoUpdateDecision(
            env_name=env,
            action="error",
            reason=f"invalid remote tag inventory: {e}",
            current_tag=None,
            target_tag=None,
        )
    except subprocess.TimeoutExpired:
        return AutoUpdateDecision(
            env_name=env,
            action="error",
            reason=f"git ls-remote --tags origin timed out after "
                   f"{_LS_REMOTE_TIMEOUT_SECONDS}s",
            current_tag=None,
            target_tag=None,
        )
    except OSError as e:
        return AutoUpdateDecision(
            env_name=env,
            action="error",
            reason=f"git ls-remote --tags origin failed to start: {e}",
            current_tag=None,
            target_tag=None,
        )

    newest = _newest_semver_tag(list(remote_tag_refs))
    if newest is None:
        return AutoUpdateDecision(
            env_name=env,
            action="error",
            reason=(
                f"no semver-shaped tags found on origin "
                f"({len(remote_tag_refs)} non-semver tags ignored). "
                f"Use `vq admin update {env} --tag X` manually for "
                f"non-semver tag names."
            ),
            current_tag=None,
            target_tag=None,
        )
    newest_peers = _equal_precedence_newest_tags(list(remote_tag_refs))
    if len(newest_peers) > 1:
        return AutoUpdateDecision(
            env_name=env,
            action="error",
            reason=(
                "origin exposes multiple newest tags with equal SemVer "
                f"precedence ({', '.join(newest_peers)}); refusing an "
                "ambiguous unattended target"
            ),
            current_tag=None,
            target_tag=None,
        )

    local_tags, local_error = _local_semver_tags_at_head(git_dir)
    if local_tags is None:
        return AutoUpdateDecision(
            env_name=env,
            action="error",
            reason=f"could not enumerate exact local tags: {local_error}",
            current_tag=None,
            target_tag=newest,
        )
    local_peers = _equal_precedence_newest_tags(local_tags)
    if len(local_peers) > 1:
        return AutoUpdateDecision(
            env_name=env,
            action="error",
            reason=(
                "current HEAD has multiple exact tags with equal highest "
                f"SemVer precedence ({', '.join(local_peers)}); refusing "
                "ambiguous unattended movement"
            ),
            current_tag=None,
            target_tag=newest,
        )
    current_tag = _newest_semver_tag(local_tags)

    if current_tag is None:
        return AutoUpdateDecision(
            env_name=env,
            action="error",
            reason=(
                "current checkout has no exact SemVer tag; refusing an "
                "unattended move because downgrade direction is unknown. "
                "Use `vq admin update` with an exact selector after review."
            ),
            current_tag=None,
            target_tag=newest,
        )
    current_sha = _rev_parse(git_dir, "HEAD")
    if current_sha is None or _FULL_SHA_RE.fullmatch(current_sha) is None:
        return AutoUpdateDecision(
            env_name=env,
            action="error",
            reason=(
                "could not resolve the current checkout's full HEAD SHA; "
                "refusing unattended tag movement"
            ),
            current_tag=current_tag,
            target_tag=newest,
        )
    current_sha = current_sha.lower()
    target_sha = remote_tag_refs[newest]
    remote_current_sha = remote_tag_refs.get(current_tag)
    if remote_current_sha is not None and remote_current_sha != current_sha:
        return AutoUpdateDecision(
            env_name=env,
            action="error",
            reason=(
                f"remote tag {current_tag!r} resolves to "
                f"{remote_current_sha[:12]}, but current HEAD is "
                f"{current_sha[:12]}; refusing rewritten-tag movement"
            ),
            current_tag=current_tag,
            target_tag=newest,
            current_sha=current_sha,
            target_sha=target_sha,
        )
    if current_tag == newest:
        return AutoUpdateDecision(
            env_name=env,
            action="skip",
            reason=(
                f"already at latest semver tag {newest!r} and SHA "
                f"{target_sha[:12]}"
            ),
            current_tag=current_tag,
            target_tag=newest,
            current_sha=current_sha,
            target_sha=target_sha,
        )
    current_precedence = _semver_precedence(current_tag)
    target_precedence = _semver_precedence(newest)
    if current_precedence is None:
        return AutoUpdateDecision(
            env_name=env,
            action="error",
            reason=(
                f"current exact tag {current_tag!r} is not valid SemVer; "
                "refusing an unattended move because downgrade direction "
                "is unknown"
            ),
            current_tag=current_tag,
            target_tag=newest,
            current_sha=current_sha,
            target_sha=target_sha,
        )
    if (
        current_precedence is not None
        and target_precedence is not None
        and current_precedence > target_precedence
    ):
        return AutoUpdateDecision(
            env_name=env,
            action="skip",
            reason=(
                f"current tag {current_tag!r} is newer than newest remote "
                f"tag {newest!r}; refusing unattended downgrade"
            ),
            current_tag=current_tag,
            target_tag=newest,
            current_sha=current_sha,
            target_sha=target_sha,
        )
    if current_precedence == target_precedence:
        return AutoUpdateDecision(
            env_name=env,
            action="error",
            reason=(
                f"current tag {current_tag!r} and remote target {newest!r} "
                "have equal SemVer precedence but different identities; "
                "refusing an unattended sideways move"
            ),
            current_tag=current_tag,
            target_tag=newest,
            current_sha=current_sha,
            target_sha=target_sha,
        )

    return AutoUpdateDecision(
        env_name=env,
        action="update",
        reason=(
            f"drift detected: current={current_tag!r}, "
            f"newest_remote={newest!r}"
        ),
        current_tag=current_tag,
        target_tag=newest,
        current_sha=current_sha,
        target_sha=target_sha,
    )


def auto_update_all(
    cfg: config.Config,
    *,
    host: str,
    dry_run: bool = False,
    admin_token: str | None = None,
) -> list[AutoUpdateOutcome]:
    """v0.6.49: drift-check + (when not dry-run) apply across EVERY
    ``kind="venv"`` program in the registry, in sorted-by-name order.

    The fleet-scale companion to :func:`auto_update_env`. v0.6.47
    shipped per-env systemd-timer template units; an operator with N
    envs × M hosts needs N×M timer instances and N×M cron entries (one
    per env per host) to fully drive the existing single-env CLI. This
    verb collapses the env dimension: one cron entry on each host
    refreshes every venv env on that host.

    Per-env failure isolation: if env A's ``git ls-remote`` errors or
    its apply fails, env B / C / ... are still attempted. The caller
    decides the batch verdict (``all(o.decision.action != "error" and
    (o.update_result is None or o.update_result.success) for o in
    outcomes)``).

    Returns a list of :class:`AutoUpdateOutcome` in the same sorted
    order. Raises :class:`admin.AdminError` only if the registry has
    zero venv programs (the operator should hear about that loudly
    rather than silently get an empty list).

    Pause/resume: deliberately NOT bracketed for the whole batch.
    Each env's apply reuses :func:`auto_update_env`, which in turn
    calls :func:`admin.update_env` — and `update_env` already
    pauses, pulls, runs the update script, then resumes per-env.
    Doing one big batch-wide pause around `auto_update_all` would
    keep the queue paused across N envs' ls-remote probes (network-
    bounded, slow); per-env bracketing is the right granularity here.

    --dry-run is honored per-env; no apply runs for any env when set.
    """
    venv_envs = sorted(
        name for name, prog in cfg.programs.items()
        if isinstance(prog, config.VenvProgram)
    )
    if not venv_envs:
        raise admin.AdminError(
            "no kind=\"venv\" programs registered; nothing for "
            "`vq admin auto-update --all` to do. Run `vq programs` "
            "to inspect the registry."
        )

    outcomes: list[AutoUpdateOutcome] = []
    for name in venv_envs:
        try:
            outcomes.append(
                auto_update_env(
                    name,
                    cfg,
                    host=host,
                    dry_run=dry_run,
                    admin_token=admin_token,
                )
            )
        except admin.AdminError as e:
            # Per-env isolation: AdminError from one env (e.g.
            # `_resolve_venv_program` rejecting a malformed config
            # mid-batch) must not abort the sweep. Wrap it as a
            # decision-phase error outcome so the formatter has
            # something to render and the batch verdict catches it.
            outcomes.append(
                AutoUpdateOutcome(
                    decision=AutoUpdateDecision(
                        env_name=name,
                        action="error",
                        reason=f"admin error: {e}",
                        current_tag=None,
                        target_tag=None,
                    ),
                    update_result=None,
                )
            )
    return outcomes


def auto_update_env(
    env: str,
    cfg: config.Config,
    *,
    host: str,
    dry_run: bool = False,
    admin_token: str | None = None,
) -> AutoUpdateOutcome:
    """Run drift detection and (when ``dry_run=False`` and drift is
    detected) apply the update via :func:`admin.update_env`.

    Returns :class:`AutoUpdateOutcome` carrying both the decision and
    (when applied) the :class:`admin.UpdateResult`. Errors during the
    apply phase don't raise — the underlying ``update_env`` returns a
    failed UpdateResult, which is wrapped into the outcome verbatim.
    Errors during the *decision* phase land as
    ``decision.action="error"`` and skip apply (you can't update what
    you couldn't probe).
    """
    if not is_local_host(host):
        return AutoUpdateOutcome(
            decision=AutoUpdateDecision(
                env_name=env,
                action="error",
                reason=(
                    "direct auto-update helpers may only apply on the local "
                    "host; the CLI owns SSH delegation"
                ),
                current_tag=None,
                target_tag=None,
            ),
            update_result=None,
        )
    prog = admin._resolve_venv_program(env, cfg)
    try:
        with (
            admin.admin_update_ownership(),
            admin.toolset_lifecycle_lock(
                [prog], action="vq-admin-auto-update",
            ),
        ):
            return _auto_update_env_owned(
                env,
                cfg,
                host=host,
                dry_run=dry_run,
                admin_token=admin_token,
                prog=prog,
            )
    except (admin.AdminUpdateInProgress, admin.AdminError) as exc:
        return AutoUpdateOutcome(
            decision=AutoUpdateDecision(
                env_name=env,
                action="error",
                reason=str(exc),
                current_tag=None,
                target_tag=None,
                policy=prog.auto_update_policy,
            ),
            update_result=None,
        )


def _auto_update_env_owned(
    env: str,
    cfg: config.Config,
    *,
    host: str,
    dry_run: bool,
    admin_token: str | None,
    prog: config.VenvProgram,
) -> AutoUpdateOutcome:
    """Decision and apply while one checkout/venv ownership fence is held."""
    # Reject a local branch-policy self-target before its drift check performs
    # `git fetch origin`.  Fetch mutates remote-tracking refs even in dry-run
    # mode, so the lifecycle boundary must precede decision probing, not merely
    # precede build-job submission.
    if is_local_host(host) and prog.auto_update_policy == "branch":
        probe = admin._detect_vq_self_update(prog)
        if probe.is_self_update or not probe.manager_available:
            return AutoUpdateOutcome(
                decision=AutoUpdateDecision(
                    env_name=env,
                    action="error",
                    reason=(
                        "branch-mode auto-update requires authoritative "
                        "proof that the target is not the serving vq "
                        "daemon before fetch or mutation; use `vq "
                        "self-update --expected-sha FULL_SHA` for the "
                        "serving environment"
                    ),
                    current_tag=None,
                    target_tag=None,
                    policy="branch",
                ),
                update_result=None,
            )

    decision = check_env_drift(env, cfg)
    if dry_run or decision.action != "update":
        return AutoUpdateOutcome(decision=decision, update_result=None)

    # Apply. Two paths depending on the policy:
    #   "tag" (v0.6.11): pass expected_tag so v0.5.24's post-pull
    #     tag verification fires — if the pull lands HEAD anywhere
    #     other than target_tag, update_env marks the result failed.
    #   "branch" (v0.7.4): no expected_tag — branch-mode trusts the
    #     v0.7.1 post-pull BRANCH validation instead (if origin/<branch>
    #     pointed at the wrong commit at decision time, the pull will
    #     follow it and update_env's branch check still pins the env
    #     to prog.branch).
    if decision.policy == "branch":
        # v0.12.x fix 2: route dev-HEAD branch drift through a deduped,
        # backed-off, wall-time-capped ``vq build-env`` JOB on the local
        # daemon instead of an uncapped inline rebuild in the timer process.
        # The inline path (pre-v0.12.x) is what wedged the 2026-06-26 fleet:
        # a stuck rebuild ran for hours with no cgroup cap, no watchdog, no
        # `vq status` visibility. Only when the target host is local — a
        # remote ``--all-hosts`` sweep ssh-delegates ``vq admin auto-update
        # <env> localhost``, which re-enters here locally and submits there.
        if is_local_host(host):
            log.info(
                "auto-update: env=%s branch drift %s -> %s; submitting "
                "build-env job", env, (decision.current_sha or "?")[:12],
                (decision.target_sha or "?")[:12],
            )
            if decision.current_sha is None or decision.target_sha is None:
                return AutoUpdateOutcome(
                    decision=AutoUpdateDecision(
                        env_name=env,
                        action="error",
                        reason=(
                            "branch update decision lacks an exact baseline "
                            "and target SHA"
                        ),
                        current_tag=None,
                        target_tag=None,
                        policy="branch",
                        current_sha=decision.current_sha,
                        target_sha=decision.target_sha,
                    ),
                    update_result=None,
                )
            submit = build_job.submit_build_env_job(
                env,
                cfg,
                host=host,
                baseline_sha=decision.current_sha,
                target_sha=decision.target_sha,
            )
            log.info(
                "auto-update: env=%s build submit -> %s (%s)",
                env, submit.action, submit.reason,
            )
            return AutoUpdateOutcome(
                decision=decision, update_result=None, build_submit=submit,
            )
        raise AssertionError("non-local auto-update escaped the entry guard")
    else:
        log.info(
            "auto-update: env=%s applying tag-mode drift fix %s -> %s",
            env, decision.current_tag, decision.target_tag,
        )
        update_result = admin.update_env(
            env,
            cfg,
            host=host,
            admin_token=admin_token,
            expected_tag=decision.target_tag,
            expected_sha=decision.target_sha,
        )
    return AutoUpdateOutcome(decision=decision, update_result=update_result)


# ---------------------------------------------------------------------------
# Scheduler-runtime auto-update (v0.16.x)
# ---------------------------------------------------------------------------


def _resolve_tag_sha(
    repo_path: str | None, tag: str,
) -> str | None:
    """Return the full 40-hex SHA ``tag`` resolves to in the driver-local
    clone at ``repo_path``, or ``None`` if resolution fails."""
    if not repo_path:
        return None
    try:
        proc = subprocess.run(
            [
                "git", "-C", repo_path, "rev-parse", "--verify",
                f"refs/tags/{tag}^{{commit}}",
            ],
            capture_output=True, text=True, timeout=15, check=False,
            stdin=subprocess.DEVNULL,
        )
    except (subprocess.TimeoutExpired, OSError):
        return None
    if proc.returncode != 0:
        return None
    sha = proc.stdout.strip().lower()
    return sha if _FULL_SHA_RE.fullmatch(sha) else None


def _driver_repo_tags(
    repo_path: str,
) -> tuple[list[str] | None, str | None]:
    """Probe tags in the driver's local clone without network access.

    ``([], None)`` is a successful probe of a repository with no tags.
    ``(None, detail)`` is a probe failure. Keeping those states distinct is
    essential: an unattended auto-update must not report a broken checkout
    as an already-converged empty-tag repository.
    """
    try:
        proc = subprocess.run(
            ["git", "-C", repo_path, "tag"],
            capture_output=True, text=True, timeout=15, check=False,
            stdin=subprocess.DEVNULL,
        )
    except subprocess.TimeoutExpired:
        return None, "git tag timed out after 15s"
    except OSError as e:
        return None, f"git tag failed to start: {e}"
    if proc.returncode != 0:
        detail = (proc.stderr or "").strip() or "(no stderr)"
        return None, f"git tag failed (rc={proc.returncode}): {detail}"
    return [t.strip() for t in proc.stdout.splitlines() if t.strip()], None


def check_scheduler_runtime_drift(
    host: str,
    program: str,
    cfg: config.Config,
) -> AutoUpdateDecision:
    """Decide whether a scheduler host's managed runtime is behind the
    newest semver tag available from the driver's source repo.

    Resolves the newest semver tag from the driver-local clone at
    the driver's vibe-qc checkout, compares against the
    scheduler runtime status record for ``host:program``, and returns
    an :class:`AutoUpdateDecision`.  No side effects.

    Returns ``action="skip"`` when:
    * the runtime's coherent last-good tag AND SHA match the newest tag and
      its locally resolved SHA, and the status record confirms a successful
      deployment; or
    * the driver's source repo has no semver tags.

    Returns ``action="error"`` when:
    * ``scheduler_runtime_source_repo`` is not configured;
    * the repo path is not a git checkout;
    * ``git tag`` fails;
    * the tag cannot be resolved to a SHA.
    """
    repo = cfg.vibeqc_source_repo
    if not repo:
        return AutoUpdateDecision(
            env_name=f"{host}:{program}",
            action="error",
            reason=(
                "no vibe-qc checkout is configured on the driver; cannot "
                'resolve latest release tag. Set [pin_source_repos] '
                f'"{config.VIBEQC_REPO_SLUG}" = "/path/to/vibe-qc", or '
                "update manually with `vq admin update <program> "
                f"{host} --expected-sha <SHA>`."
            ),
            current_tag=None, target_tag=None,
        )
    repo_path = Path(repo)
    if not (repo_path / ".git").exists():
        return AutoUpdateDecision(
            env_name=f"{host}:{program}",
            action="error",
            reason=(
                f"scheduler_runtime_source_repo {repo!r} is not a "
                f"git checkout; cannot resolve tags"
            ),
            current_tag=None, target_tag=None,
        )

    tags, tag_probe_error = _driver_repo_tags(repo)
    if tags is None:
        return AutoUpdateDecision(
            env_name=f"{host}:{program}",
            action="error",
            reason=(
                f"cannot inspect tags in scheduler_runtime_source_repo "
                f"{repo!r}: {tag_probe_error or 'unknown git tag failure'}"
            ),
            current_tag=None, target_tag=None,
        )
    newest = _newest_semver_tag(tags)
    if newest is None:
        return AutoUpdateDecision(
            env_name=f"{host}:{program}",
            action="skip",
            reason=(
                f"no semver tags in driver repo {repo!r} "
                f"({len(tags)} non-semver tags); nothing to update to"
            ),
            current_tag=None, target_tag=None,
        )
    newest_peers = _equal_precedence_newest_tags(tags)
    if len(newest_peers) > 1:
        return AutoUpdateDecision(
            env_name=f"{host}:{program}",
            action="error",
            reason=(
                "driver repo exposes multiple newest tags with equal SemVer "
                f"precedence ({', '.join(newest_peers)}); refusing an "
                "ambiguous unattended target"
            ),
            current_tag=None,
            target_tag=None,
        )

    # Read the deployed status.  This file is operator-owned JSON rather than
    # a typed database: dataclass construction does not validate runtime field
    # types or bind the value back to the mapping key.  Treat the complete
    # lane identity as one fail-closed proof before comparing versions.
    records = admin.load_scheduler_runtime_status()
    record = records.get(f"{host}:{program}")
    deployed_tag: str | None = None
    deployed_sha: str | None = None
    identity_error: str | None = None
    if record is not None:
        if record.host != host or record.program != program:
            identity_error = (
                "scheduler runtime status key does not match its embedded "
                f"lane identity ({record.host!r}:{record.program!r})"
            )
        elif type(record.last_success) is not bool:
            identity_error = (
                "scheduler runtime status last_success is not a boolean"
            )
        else:
            # LAST OK is one identity pair. Never fill a missing member from
            # the most recent attempt: that could combine a prior tag with a
            # failed candidate's SHA and manufacture a false match. Records
            # written before last_ok_* existed fall back only to an exactly
            # successful actual pair.
            if record.last_ok_tag is not None or record.last_ok_sha is not None:
                candidate_tag = record.last_ok_tag
                candidate_sha = record.last_ok_sha
            elif record.last_success is True:
                candidate_tag = record.actual_tag
                candidate_sha = record.actual_sha
            else:
                candidate_tag = None
                candidate_sha = None
            if candidate_tag is not None or candidate_sha is not None:
                if not isinstance(candidate_tag, str):
                    identity_error = "LAST OK tag is not a string"
                elif _semver_precedence(candidate_tag) is None:
                    identity_error = (
                        f"LAST OK tag {candidate_tag!r} is not valid SemVer"
                    )
                elif not isinstance(candidate_sha, str):
                    identity_error = "LAST OK SHA is not a string"
                elif _FULL_SHA_RE.fullmatch(candidate_sha) is None:
                    identity_error = (
                        f"LAST OK SHA {candidate_sha!r} is not a full 40-hex SHA"
                    )
                else:
                    deployed_tag = candidate_tag
                    deployed_sha = candidate_sha.lower()

    # Resolve before deciding to skip. A matching tag string alone does not
    # prove convergence when the recorded deployment SHA differs or the tag
    # can no longer be resolved.
    target_sha = _resolve_tag_sha(repo, newest)
    if target_sha is None:
        return AutoUpdateDecision(
            env_name=f"{host}:{program}",
            action="error",
            reason=(
                f"could not resolve tag {newest} to a SHA in "
                f"{repo!r}; check the clone state"
            ),
            current_tag=deployed_tag, target_tag=newest,
            current_sha=deployed_sha,
        )

    if identity_error is not None:
        return AutoUpdateDecision(
            env_name=f"{host}:{program}",
            action="error",
            reason=(
                f"invalid scheduler runtime deployment record: "
                f"{identity_error}; refusing unattended update"
            ),
            current_tag=deployed_tag,
            target_tag=newest,
            current_sha=deployed_sha,
            target_sha=target_sha,
        )

    if deployed_tag is None or deployed_sha is None:
        return AutoUpdateDecision(
            env_name=f"{host}:{program}",
            action="error",
            reason=(
                "scheduler runtime has no complete LAST OK SemVer tag/SHA "
                "identity; refusing an unattended move because downgrade "
                "direction is unknown. Use `vq admin update` with the exact "
                "SHA after review."
            ),
            current_tag=deployed_tag,
            target_tag=newest,
            current_sha=deployed_sha,
            target_sha=target_sha,
        )
    deployed_precedence = _semver_precedence(deployed_tag)
    target_precedence = _semver_precedence(newest)
    if deployed_precedence is None:
        return AutoUpdateDecision(
            env_name=f"{host}:{program}",
            action="error",
            reason=(
                f"deployed LAST OK tag {deployed_tag!r} is not valid SemVer; "
                "refusing an unattended move because downgrade direction "
                "is unknown"
            ),
            current_tag=deployed_tag,
            target_tag=newest,
            current_sha=deployed_sha,
            target_sha=target_sha,
        )
    if (
        deployed_precedence is not None
        and target_precedence is not None
        and deployed_precedence > target_precedence
    ):
        return AutoUpdateDecision(
            env_name=f"{host}:{program}",
            action="skip",
            reason=(
                f"deployed tag {deployed_tag!r} is newer than the newest "
                f"driver-local tag {newest!r}; refusing unattended downgrade"
            ),
            current_tag=deployed_tag,
            target_tag=newest,
            current_sha=deployed_sha,
            target_sha=target_sha,
        )
    if deployed_precedence == target_precedence and (
        deployed_tag != newest or deployed_sha != target_sha
    ):
        return AutoUpdateDecision(
            env_name=f"{host}:{program}",
            action="error",
            reason=(
                f"deployed identity {deployed_tag!r}@{deployed_sha[:12]} "
                f"and target {newest!r}@{target_sha[:12]} have equal SemVer "
                "precedence but differ; refusing an unattended sideways move"
            ),
            current_tag=deployed_tag,
            target_tag=newest,
            current_sha=deployed_sha,
            target_sha=target_sha,
        )

    if (
        record is not None
        and record.last_success is True
        and deployed_tag == newest
        and deployed_sha == target_sha
    ):
        return AutoUpdateDecision(
            env_name=f"{host}:{program}",
            action="skip",
            reason=(
                f"already at latest semver tag {newest!r} and SHA "
                f"{target_sha[:12]} "
                f"(last OK {record.last_ok_at or record.last_updated_at})"
            ),
            current_tag=deployed_tag, target_tag=newest,
            current_sha=deployed_sha, target_sha=target_sha,
        )

    return AutoUpdateDecision(
        env_name=f"{host}:{program}",
        action="update",
        reason=(
            f"drift: deployed={deployed_tag or 'none'} "
            f"(SHA {(deployed_sha or 'none')[:12]}), "
            f"latest={newest} (SHA {target_sha[:12]})"
        ),
        current_tag=deployed_tag, target_tag=newest,
        current_sha=deployed_sha,
        target_sha=target_sha,
    )


def auto_update_scheduler_runtime(
    host: str,
    program: str,
    cfg: config.Config,
    *,
    dry_run: bool = False,
) -> AutoUpdateOutcome:
    """Reject the retired standalone scheduler-runtime updater.

    Accepted release reports are the sole source of scheduler-runtime pins.
    Keep this Python entry point fail-closed as defense in depth for callers
    that bypass the CLI.
    """
    del cfg, dry_run
    return AutoUpdateOutcome(
        decision=AutoUpdateDecision(
            env_name=f"{host}:{program}",
            action="error",
            reason=SCHEDULER_RUNTIME_AUTO_UPDATE_DISABLED_REASON,
            current_tag=None,
            target_tag=None,
        ),
        update_result=None,
    )


def scheduler_runtime_deployment_hosts(
    cfg: config.Config,
) -> dict[str, config.HostConfig]:
    """Return scheduler deployment hosts only when every target is managed.

    Retained for configuration diagnostics and compatibility callers after the
    aggregate updater was retired. An alias or unresolved fleet role must not
    become a mutable target merely because it carries a deployment table.
    """
    deployment_hosts = {
        name: host_cfg
        for name, host_cfg in cfg.hosts.items()
        if host_cfg.scheduler_runtime_deployments
    }
    unmanaged_hosts = sorted(
        name
        for name, host_cfg in deployment_hosts.items()
        if host_cfg.fleet_role != "managed"
    )
    if unmanaged_hosts:
        raise admin.AdminError(
            "scheduler runtime deployments are mutable only on hosts with "
            "fleet_role='managed'; non-managed target(s): "
            + ", ".join(unmanaged_hosts)
        )
    return deployment_hosts


def auto_update_scheduler_runtimes(
    cfg: config.Config,
    *,
    host: str | None = None,
    dry_run: bool = False,
) -> list[AutoUpdateOutcome]:
    """Reject the retired aggregate scheduler-runtime auto-updater."""
    del cfg, dry_run
    return [
        AutoUpdateOutcome(
            decision=AutoUpdateDecision(
                env_name=host or "scheduler-runtimes",
                action="error",
                reason=SCHEDULER_RUNTIME_AUTO_UPDATE_DISABLED_REASON,
                current_tag=None,
                target_tag=None,
            ),
            update_result=None,
        )
    ]
