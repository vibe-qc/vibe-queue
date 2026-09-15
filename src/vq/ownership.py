"""v0.6.x: ownership checks for multi-user mode.

In multi-user mode, kill / fetch / resubmit operations must verify that the
caller owns the job (spec.submitter matches the caller's UID) or is a member
of the admin group configured in ``[multi_user] admin_group``.

In single-user mode (default), these checks are no-ops — all callers share
the same queue directory and there's no ownership to enforce.
"""

from __future__ import annotations

import grp
import os
from pathlib import Path

from vq import config as config_module
from vq.spec import JobSpec


class OwnershipError(PermissionError):
    """Raised when a caller attempts an operation on a job they don't own."""


def _caller_uid() -> int:
    """Return the effective UID of the calling process."""
    return os.geteuid()


def _caller_is_admin(cfg: config_module.Config) -> bool:
    """True when the caller's UID is a member of the configured admin group.

    Membership is satisfied by either a supplementary membership (the
    caller's username appears in the group's member list) or the admin
    group being the caller's *primary* group. Root is always admin.
    """
    if not cfg.multi_user.enabled:
        return True  # single-user mode: everyone is admin
    uid = _caller_uid()
    if uid == 0:
        return True  # root is always admin
    group_name = cfg.multi_user.admin_group
    try:
        g = grp.getgrnam(group_name)
    except KeyError:
        return False  # group doesn't exist → no one is admin
    # grp.gr_mem is a list of usernames, not uids — resolve the
    # caller's uid to a name before checking. (The pre-fix code
    # compared the int uid against gr_mem and so never matched: a
    # non-root admin-group member was wrongly denied.)
    try:
        import pwd

        pw = pwd.getpwuid(uid)
    except KeyError:
        return False  # uid has no passwd entry
    return pw.pw_name in g.gr_mem or pw.pw_gid == g.gr_gid


def admin_group_uids(cfg: config_module.Config) -> list[int]:
    """v0.6.x: return the uids of every member of the configured
    admin group — supplementary members (listed in ``grp.gr_mem``)
    plus users whose *primary* gid is the group.

    Used by the multi-user daemon at startup to pre-provision a
    per-user state dir for each admin, so an admin's first
    ``vq submit`` doesn't fail on the root-owned ``users/`` dir.

    Returns an empty list in single-user mode, or when the admin
    group does not exist. Sorted + de-duplicated."""
    if not cfg.multi_user.enabled:
        return []
    import pwd

    try:
        g = grp.getgrnam(cfg.multi_user.admin_group)
    except KeyError:
        return []
    uids: set[int] = set()
    for name in g.gr_mem:
        try:
            uids.add(pwd.getpwnam(name).pw_uid)
        except KeyError:
            continue  # stale member name with no passwd entry
    # Users whose primary gid is the admin group won't appear in
    # gr_mem — walk the passwd db to catch them too.
    try:
        for pw in pwd.getpwall():
            if pw.pw_gid == g.gr_gid:
                uids.add(pw.pw_uid)
    except OSError:  # pragma: no cover - defensive
        pass
    return sorted(uids)


def _uid_from_spec(spec: JobSpec) -> int | None:
    """Extract the submitter UID from a spec. Returns None if no submitter set."""
    if spec.submitter is None:
        return None
    # submitter stores "$USER" at submit time in single-user mode;
    # in multi-user mode it stores the UID as a string.
    try:
        return int(spec.submitter)
    except ValueError:
        # Not a numeric UID — try to resolve by name (backward compat).
        try:
            import pwd

            return pwd.getpwnam(spec.submitter).pw_uid
        except KeyError:
            return None


def _authorization_config(
    cfg: config_module.Config | None,
    *,
    multi_user: bool,
) -> config_module.Config:
    """Return the authoritative config for an ownership decision.

    An enabled system config wins over personal policy, including its
    ``admin_group``.  A valid disabled system config preserves the historical
    personal-config opt-in.  Explicit multi-user path routing without either
    enabled policy is a configuration error rather than a fail-open no-op.
    """
    system_hint = config_module.system_multi_user_enabled()
    system: config_module.Config | None = None
    system_loaded = False
    if multi_user or system_hint:
        system = config_module.load_system_config()
        system_loaded = True
        if system is not None and system.multi_user.enabled:
            return system

    personal = cfg if cfg is not None else config_module.load_config()
    if not (multi_user or personal.multi_user.enabled or system_hint):
        return personal
    if not system_loaded:
        system = config_module.load_system_config()
        if system is not None and system.multi_user.enabled:
            return system
    if personal.multi_user.enabled:
        return personal
    if multi_user or system_hint:
        raise config_module.ConfigError(
            "multi-user state was selected, but neither the system nor "
            "personal config enables [multi_user]; refusing an owner check"
        )
    return personal


def _check_owner_with_config(spec: JobSpec, cfg: config_module.Config) -> None:
    if not cfg.multi_user.enabled:
        return

    if _caller_is_admin(cfg):
        return

    submitter_uid = _uid_from_spec(spec)
    if submitter_uid is None:
        return

    caller = _caller_uid()
    if caller != submitter_uid:
        raise OwnershipError(
            f"job {spec.id} belongs to uid {submitter_uid}; "
            f"caller is uid {caller}. "
            f"Join the '{cfg.multi_user.admin_group}' group for admin access, "
            f"or run as root."
        )


def check_owner(
    spec: JobSpec,
    *,
    cfg: config_module.Config | None = None,
    multi_user: bool = False,
) -> None:
    """Raise :class:`OwnershipError` if the caller doesn't own this job.

    In single-user mode, this is a no-op.
    In multi-user mode, the check passes when:
      * The caller's UID matches the submitter, OR
      * The caller is a member of the admin group, OR
      * The job has no submitter (pre-multi-user spec)

    Args:
        spec: The job spec to check ownership of.
        cfg: Optional pre-loaded personal config. If None, loaded from disk.
        multi_user: Whether the caller explicitly selected multi-user paths.

    Raises:
        OwnershipError: when the caller doesn't own the job and isn't admin.
    """
    effective = _authorization_config(cfg, multi_user=multi_user)
    _check_owner_with_config(spec, effective)


def check_spec_path_owner(
    spec_path: Path,
    *,
    cfg: config_module.Config | None = None,
    multi_user: bool = False,
) -> None:
    """Convenience: read spec from ``spec_path`` and check ownership.

    Raises :class:`OwnershipError` or :class:`FileNotFoundError`.
    In single-user mode, this is a no-op.
    """
    effective = _authorization_config(cfg, multi_user=multi_user)
    if not effective.multi_user.enabled:
        return
    if not spec_path.exists():
        raise FileNotFoundError(f"no such job: {spec_path.stem}")
    spec = JobSpec.read(spec_path)
    _check_owner_with_config(spec, effective)
