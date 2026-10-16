"""Leftovers of interrupted transactions and transfers, and their removal.

Every temporary vq creates is normally removed by the operation that made it.
A crash, a kill or a failed final cleanup leaves it behind, and nothing looked
for it again. This module finds the ones vq can attribute with certainty:

* **admin transaction leftovers** beside each registered venv:
  ``.<venv>.vq-admin-backup-<txn>`` (the pre-update venv kept for rollback) and
  ``.<venv>.vq-admin-failed-<txn>`` (a failed target moved aside). While an
  admin update or recovery marker exists, ``vq admin recover-update`` owns
  them and they are never removable here. Removal holds the admin-update
  ownership and the program's lifecycle lock.
* **fetch staging** in vq's own job trees: ``.vq-fetch-*`` directories and
  archives, and ``.vq-stale-*`` retired snapshots, at the top of each jobs
  root and of each job workspace.

Everything must be older than a minimum age, owned by this user, and not a
symlink. Deliberately **not** touched: migration and serving-daemon rollback
stages (their receipts keep them as evidence), remote uploads kept after an
unproven submit (duplicate-job protection), and temporaries in directories vq
does not own, such as a user's ``vq fetch -o`` destination.
"""
from __future__ import annotations

import os
import re
import shutil
import time
from dataclasses import dataclass
from pathlib import Path

from vq import admin, config, paths

DEFAULT_MIN_AGE_SECONDS = 24 * 60 * 60
AUTO_MIN_AGE_SECONDS = 7 * 24 * 60 * 60

_ADMIN = re.compile(r"\.(?P<venv>.+)\.vq-admin-(?P<kind>backup|failed)-[0-9a-f]{32}")
_FETCH = re.compile(r"\.vq-(?:fetch-[A-Za-z0-9_]+(?:\.tar)?|stale-.+)")


@dataclass
class Leftover:
    path: Path
    kind: str
    age_seconds: float
    removable: bool
    reason: str
    program: str | None = None


def _age(path: Path, now: float) -> float | None:
    try:
        info = path.lstat()
    except OSError:
        return None
    if path.is_symlink() or info.st_uid != os.geteuid():
        return None
    return max(0.0, now - info.st_mtime)


def _admin_parents(prog: config.VenvProgram) -> set[Path]:
    """Where a transaction puts ``prog``'s backup and failed-target copies."""
    venv = Path(os.path.abspath(prog.python)).parent.parent
    parents = {venv.parent}
    checkout = Path(os.path.abspath(prog.git_dir))
    if venv.is_relative_to(checkout):
        parents.add(checkout.parent)
    return parents


def _jobs_roots(multi_user: bool) -> list[Path]:
    if not multi_user:
        return [paths.jobs_dir()]
    return [paths.user_jobs_dir(user.name) for user in paths._all_user_dirs()]


def find(
    cfg: config.Config,
    *,
    older_than_seconds: float = DEFAULT_MIN_AGE_SECONDS,
    multi_user: bool = False,
    now: float | None = None,
) -> list[Leftover]:
    now = time.time() if now is None else now
    found: list[Leftover] = []
    marker = admin.admin_update_marker_exists()
    seen: set[Path] = set()
    for name, prog in sorted(cfg.programs.items()):
        if not isinstance(prog, config.VenvProgram):
            continue
        venv_name = Path(os.path.abspath(prog.python)).parent.parent.name
        for parent in _admin_parents(prog):
            try:
                entries = list(parent.iterdir())
            except OSError:
                continue
            for entry in entries:
                match = _ADMIN.fullmatch(entry.name)
                if match is None or match["venv"] != venv_name or entry in seen:
                    continue
                age = _age(entry, now)
                if age is None:
                    continue
                seen.add(entry)
                if marker:
                    removable, reason = False, (
                        "an admin marker exists; `vq admin recover-update` owns it")
                elif age < older_than_seconds:
                    removable, reason = False, "younger than --older-than"
                else:
                    removable, reason = True, "no admin transaction references it"
                found.append(Leftover(entry, f"admin-{match['kind']}", age,
                                      removable, reason, program=name))
    for root in _jobs_roots(multi_user):
        try:
            workspaces = [root, *(p for p in root.iterdir() if p.is_dir()
                                  and not p.is_symlink())]
        except OSError:
            continue
        for directory in workspaces:
            try:
                entries = list(directory.iterdir())
            except OSError:
                continue
            for entry in entries:
                if _FETCH.fullmatch(entry.name) is None:
                    continue
                age = _age(entry, now)
                if age is None:
                    continue
                removable = age >= older_than_seconds
                found.append(Leftover(
                    entry, "fetch-staging", age, removable,
                    "abandoned fetch staging" if removable else "younger than --older-than",
                ))
    return found


def remove(leftover: Leftover, cfg: config.Config) -> None:
    """Remove one leftover after rechecking what made it removable."""
    if not leftover.removable:
        raise ValueError(f"{leftover.path} is not removable: {leftover.reason}")
    if leftover.program is None:
        _remove_path(leftover.path)
        return
    prog = cfg.programs[leftover.program]
    assert isinstance(prog, config.VenvProgram)
    with admin.admin_update_ownership(), admin.toolset_lifecycle_lock(
        [prog], action="vq-cleanup-leftovers",
    ):
        if admin.admin_update_marker_exists():
            raise ValueError("an admin marker appeared; recover-update owns it")
        _remove_path(leftover.path)


def _remove_path(path: Path) -> None:
    if path.is_symlink():
        raise ValueError(f"{path} became a symlink")
    if path.is_dir():
        shutil.rmtree(path)
    elif path.exists():
        path.unlink()


def sweep(cfg: config.Config, *, multi_user: bool = False) -> dict[str, int]:
    """Remove removable leftovers older than the auto-cleanup minimum age."""
    counts = {"leftovers_removed": 0, "leftover_errors": 0}
    for leftover in find(cfg, older_than_seconds=AUTO_MIN_AGE_SECONDS,
                         multi_user=multi_user):
        if not leftover.removable:
            continue
        try:
            remove(leftover, cfg)
            counts["leftovers_removed"] += 1
        except Exception:  # noqa: BLE001 - one entry must not stop the sweep
            counts["leftover_errors"] += 1
    return counts
