"""Reviewed retirement of a registration's legacy build checkout (#72).

Once a registration builds from its active generation
(``runtime_slot_build_input = "current"``), its registered ``git_dir`` is only
a compatibility path. This module plans and applies deleting it, as an
explicit operator transaction:

* :func:`plan` is read-only. It reports the checkout, its HEAD and upstream,
  its size, and every blocker. Deletion is blocked by: a registration sharing
  the checkout or its interpreter that still builds from it; a service
  definition or the running daemon naming it; a live job whose command or
  runtime selection names it, or a started job of the program without a
  recorded selection; an unresolved admin marker; uncommitted changes;
  stashes; or commits that no remote-tracking ref contains. Two more follow
  the 2026-09-24 decision on #72 (#123): an unfinished or unreadable
  ``vq admin migrate-runtime`` transaction of the registration family, which
  may still need the checkout to roll back; and a launcher or registry file
  that names the checkout, whether a migration bound it or the operator
  declares it with ``launchers``. The loaded registry is always scanned too,
  so a host whose migration receipts are not indexed plans like one whose
  are (#143). In every registry, exactly the retiring family's own
  ``git_dir``/``python`` are exempt while the family builds from its current
  generation and executes a verified slot outside the checkout; the plan
  lists them as ``compatibility_paths``.
* :func:`apply` takes the reviewed digest, re-plans under the admin and
  lifecycle ownership, refuses on any change or blocker, removes the tree
  (with ``munlink`` on Lustre), and writes a receipt with the exact HEAD and
  upstream it was at.

Apply does not edit any registry. The ``compatibility_paths`` it records stay
as they are, deliberately: ``git_dir``/``python`` are required fields from
which the updater entry points and virtualenv name are still derived by path
arithmetic, a rewrite would drop the operator's TOML comments, and every
``migrate-runtime`` receipt binds the registry's exact bytes and file
identity for its recovery. A path that no longer exists there is the
documented state of a retired checkout, on every host.

Migrations are found through the receipt index that ``migrate-runtime``
writes (:func:`vq.runtime_migration.indexed_transactions`); a transaction
applied by a vq that predates the index is not seen, so its launchers must be
declared.
"""
from __future__ import annotations

import copy
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import vq
from vq import admin, bulk_delete, config, paths, runtime_migration, runtime_retention
from vq.spec import JobState

SCHEMA = "vq.checkout-retirement/1"


class RetirementError(ValueError):
    """The checkout cannot be retired safely."""


def _digest(value: object) -> str:
    return hashlib.sha256(
        (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()
    ).hexdigest()


def _git(checkout: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", "-C", str(checkout), *args], capture_output=True, text=True,
        timeout=120, check=False, stdin=subprocess.DEVNULL,
        env={**os.environ, "GIT_OPTIONAL_LOCKS": "0"},
    )


def _inside(path: str | None, checkout: Path) -> bool:
    if not path:
        return False
    try:
        return Path(os.path.abspath(path)).is_relative_to(checkout)
    except (OSError, ValueError):
        return False


def _names(text: str, checkout: Path) -> bool:
    return re.search(re.escape(str(checkout)) + r"(?:/|\b|$)", text) is not None


def _git_blockers(checkout: Path) -> tuple[list[str], dict[str, Any]]:
    blockers: list[str] = []
    head = _git(checkout, "rev-parse", "HEAD")
    if head.returncode != 0:
        return [f"not a readable git checkout: {head.stderr.strip()}"], {}
    status = _git(checkout, "status", "--porcelain")
    if status.returncode != 0:
        blockers.append("git status failed; cannot prove there is no local work")
    elif status.stdout.strip():
        blockers.append("uncommitted changes or untracked files that are not ignored")
    stashes = _git(checkout, "stash", "list")
    if stashes.returncode != 0 or stashes.stdout.strip():
        blockers.append("stashes exist (or the stash list is unreadable)")
    local_only = _git(checkout, "rev-list", "--count", "--branches", "HEAD",
                      "--not", "--remotes")
    if local_only.returncode != 0:
        blockers.append("cannot prove every commit is on a remote")
    elif local_only.stdout.strip() != "0":
        blockers.append(
            f"{local_only.stdout.strip()} commit(s) exist only in this checkout"
        )
    origin = _git(checkout, "remote", "get-url", "origin")
    return blockers, {
        "head": head.stdout.strip(),
        "upstream": origin.stdout.strip() if origin.returncode == 0 else None,
    }


def _names_checkout(text: str, checkout: Path) -> bool:
    real = Path(os.path.realpath(checkout))
    return _names(text, checkout) or (real != checkout and _names(text, real))


def _path_uses_checkout(path: str, checkout: Path) -> bool:
    """Keep intermediate symlink targets: a venv Python often ends outside it."""
    current = Path(os.path.abspath(path))
    for _ in range(40):
        if _inside(str(current), checkout):
            return True
        current = current.parent.resolve() / current.name
        if _inside(str(current), Path(os.path.realpath(checkout))):
            return True
        if not current.is_symlink():
            return False
        current = Path(os.path.abspath(current.parent / os.readlink(current)))
    raise OSError("too many symlinks while resolving a checkout consumer")


def _binding_blockers(
    paths_to_read: list[str], checkout: Path, *, label: str,
) -> list[str]:
    blockers = []
    for path in paths_to_read:
        try:
            if _path_uses_checkout(path, checkout):
                blockers.append(f"{label} {path} resolves inside this checkout")
                continue
            text = Path(path).read_bytes().decode("utf-8", "replace")
        except FileNotFoundError:
            continue
        except OSError as exc:
            blockers.append(f"cannot read {label} {path}: {exc}")
            continue
        if _names_checkout(text, checkout):
            blockers.append(f"{label} {path} names this checkout")
    return blockers


def _compatibility_fields(
    cfg: config.Config, family: set[str], checkout: Path,
) -> list[dict[str, str]]:
    """The retiring family's own compatibility ``git_dir``/``python`` (#143).

    With ``runtime_slot_build_input = "current"`` these two registered fields
    are path templates that may name a retired checkout (see
    :attr:`vq.config.VenvProgram.runtime_slot_build_input`). Only a member of
    ``family`` qualifies, and only while its verified execution binding
    resolves outside the checkout; a binding that cannot be resolved raises
    :class:`vq.config.ConfigError`.
    """
    fields = []
    for name in sorted(family):
        prog = cfg.programs.get(name)
        if not isinstance(prog, config.VenvProgram) or prog.runtime_slot_build_input != "current":
            continue
        inside = [key for key in ("git_dir", "python") if _inside(getattr(prog, key), checkout)]
        if not inside:
            continue
        selected = prog.execution_program()
        if _inside(selected.git_dir, checkout) or _inside(selected.python, checkout):
            continue
        fields += [{"program": name, "field": key, "path": getattr(prog, key)}
                   for key in inside]
    return fields


def _registry_names_checkout(
    raw: dict[str, Any], cfg: config.Config, family: set[str], checkout: Path,
    *, reported: frozenset[str] = frozenset(),
) -> bool:
    """Whether a registry names the checkout outside the exempt fields.

    The exemption is structural: exactly the family's compatibility fields are
    dropped from the parsed registry. Commands, hooks, ``remote_vq``, other
    programs, other fields of the family and every other table stay in the scan.
    ``reported`` names registrations whose ``git_dir``/``python`` already
    produced their own blocker, so the scan does not repeat it.
    """
    scanned = copy.deepcopy(raw)
    dropped = [(item["program"], item["field"])
               for item in _compatibility_fields(cfg, family, checkout)]
    dropped += [(name, key) for name in reported for key in ("git_dir", "python")]
    for name, key in dropped:
        scanned["programs"][name].pop(key, None)
    return _names_checkout(json.dumps(scanned, default=str, ensure_ascii=False), checkout)


def _registry_file_names_checkout(
    data: bytes, path: str, family: set[str], checkout: Path,
) -> bool:
    raw, cfg = runtime_migration._model(data, path)
    return _registry_names_checkout(raw, cfg, family, checkout)


def _registry_blockers(registries: list[str], family: set[str], checkout: Path) -> list[str]:
    blockers = []
    for path in registries:
        try:
            if _registry_file_names_checkout(Path(path).read_bytes(), path, family, checkout):
                blockers.append(f"migrated registry {path} names this checkout")
        except (OSError, ValueError, config.ConfigError) as exc:
            blockers.append(f"cannot verify migrated registry {path}: {exc}")
    return blockers


def _migration_blockers(family: set[str], checkout: Path) -> list[str]:
    blockers = []
    for item in runtime_migration.indexed_transactions():
        where = item["receipt_dir"]
        if "error" in item:
            blockers.append(f"migrate-runtime receipt {where}: {item['error']}")
            continue
        rollback_uses_checkout = False
        for binding in item.get("rollback_bindings", []):
            try:
                rollback_uses_checkout |= (
                    _registry_file_names_checkout(
                        binding["text"].encode(), binding["path"], family, checkout,
                    )
                    if binding["kind"] == "registries"
                    else _names_checkout(binding["text"], checkout)
                )
            except (OSError, ValueError, config.ConfigError):
                rollback_uses_checkout = True
        if not item["finished"] and (item["program"] in family or rollback_uses_checkout):
            blockers.append(
                f"migrate-runtime transaction {where} is {item['state']}; finish or "
                "roll it back first, since a rollback may restore this checkout"
            )
        registries = item.get("registries", [])
        blockers.extend(_registry_blockers(registries, family, checkout))
        blockers.extend(_binding_blockers(
            [path for path in item["bindings"] if path not in registries],
            checkout, label="migrated binding",
        ))
    return blockers


def plan(
    env: str, cfg: config.Config, *, launchers: tuple[str, ...] = (),
) -> dict[str, Any]:
    """Read-only retirement plan for ``env``'s registered checkout.

    ``launchers`` are operator-declared launcher files; any that names the
    checkout blocks, and the list is part of the plan identity.
    """
    prog = cfg.programs.get(env)
    if not isinstance(prog, config.VenvProgram):
        raise RetirementError(f"{env!r} is not a registered venv program")
    if prog.runtime_slot_build_input != "current":
        raise RetirementError(
            f"{env!r} still builds from its checkout; set "
            'runtime_slot_build_input = "current" first'
        )
    # Proves an active verified generation exists to build and run from.
    try:
        admin._runtime_build_source(prog)
    except admin.AdminError as exc:
        raise RetirementError(str(exc)) from exc
    checkout = Path(os.path.abspath(prog.git_dir))
    if not checkout.is_dir():
        raise RetirementError(f"{checkout} does not exist; nothing to retire")
    blockers: list[str] = []
    for label, path in (("interpreter", sys.executable), ("package", vq.__file__)):
        if path and _path_uses_checkout(path, checkout):
            blockers.append(f"the running retirement command's {label} uses this checkout")
    automatic = {str(Path.home() / ".local/bin/vq")}
    on_path = shutil.which("vq")
    if on_path:
        automatic.add(os.path.abspath(on_path))
    blockers.extend(_binding_blockers(sorted(automatic), checkout, label="automatic launcher"))
    aliases: list[str] = []
    building: set[str] = set()
    for name, other in sorted(cfg.programs.items()):
        if not isinstance(other, config.VenvProgram):
            continue
        if not (_inside(other.git_dir, checkout) or _inside(other.python, checkout)):
            continue
        aliases.append(name)
        if other.runtime_slot_build_input != "current":
            building.add(name)
            blockers.append(f"registration {name!r} still builds from this checkout")
    files, service_blockers = runtime_retention._service_files()
    blockers.extend(service_blockers)
    files = sorted(set(files))
    service_hashes: dict[str, str | None] = {str(path): None for path in files}
    for path in files:
        try:
            if _path_uses_checkout(str(path), checkout):
                blockers.append(f"service definition {path} resolves inside this checkout")
                continue
            data = path.read_bytes()
            service_hashes[str(path)] = hashlib.sha256(data).hexdigest()
            text = data.decode("utf-8", "replace")
        except OSError as exc:
            blockers.append(f"cannot read service definition {path}: {exc}")
            continue
        if _names(text, checkout):
            blockers.append(f"service definition {path} names this checkout")
    try:
        from vq import rpc

        identity = rpc.call("get_process_identity", timeout=5.0)
    except Exception:  # noqa: BLE001 - no reachable daemon uses nothing
        identity = None
    if isinstance(identity, dict) and _inside(str(identity.get("python_executable")), checkout):
        blockers.append("the running vq daemon executes from this checkout")
    try:
        specs = admin._runtime_slot_spec_snapshot()
    except Exception as exc:  # noqa: BLE001 - an unreadable census blocks
        blockers.append(f"cannot read the job census: {exc}")
        specs = []
    for spec in specs:
        if getattr(spec, "is_terminal", False) or getattr(spec, "scheduler_target", None):
            continue
        texts = [a for a in (getattr(spec, "command", None) or ()) if isinstance(a, str)]
        pin = getattr(spec, "program_runtime_pin", None)
        if pin is not None:
            texts += [pin.resolved_runtime_source or "", pin.resolved_runtime_python or ""]
        if any(_names(text, checkout) for text in texts):
            blockers.append(f"live job {spec.id} names this checkout")
        elif (
            spec.program in aliases
            and spec.state != JobState.PENDING
            and (pin is None or not pin.resolved_runtime_root)
        ):
            blockers.append(f"started job {spec.id} has no recorded runtime selection")
    if admin.admin_update_marker_exists():
        blockers.append("an admin update or recovery marker must be resolved first")
    family = {env, *aliases}
    # The loaded registry is scanned on every host, so a host whose migration
    # receipts are not indexed plans exactly like one whose are (#143).
    try:
        compatibility = _compatibility_fields(cfg, family, checkout)
        if _registry_names_checkout(cfg.model_dump(), cfg, family, checkout,
                                    reported=frozenset(building)):
            blockers.append(
                "the loaded registry names this checkout outside the "
                "compatibility git_dir/python of the registrations retiring it"
            )
    except config.ConfigError as exc:
        compatibility = []
        blockers.append(f"cannot verify the loaded registry: {exc}")
    try:
        blockers.extend(_migration_blockers(family, checkout))
    except OSError as exc:
        blockers.append(f"cannot read the migrate-runtime receipt index: {exc}")
    declared = sorted({os.path.abspath(path) for path in launchers})
    for path in declared:
        if not Path(path).exists():
            blockers.append(f"declared launcher {path} does not exist")
    blockers.extend(_binding_blockers(declared, checkout, label="declared launcher"))
    git_blockers, identity_fields = _git_blockers(checkout)
    blockers.extend(git_blockers)
    body = {"schema": SCHEMA, "env": env, "checkout": str(checkout),
            "aliases": aliases, "launchers": declared,
            "automatic_launchers": sorted(automatic), **identity_fields,
            "service_files": [str(path) for path in files],
            "service_file_sha256": service_hashes,
            "compatibility_paths": compatibility, "blockers": sorted(blockers)}
    return {**body, "plan_id": _digest(body),
            "bytes": runtime_retention._tree_bytes(checkout)}


def apply(
    env: str, cfg: config.Config, expected_plan_id: str, *,
    launchers: tuple[str, ...] = (),
) -> dict[str, Any]:
    """Delete exactly the reviewed checkout, or refuse.

    Pass the same ``launchers`` as the plan: they are part of its identity.
    """
    prog = cfg.programs.get(env)
    if not isinstance(prog, config.VenvProgram):
        raise RetirementError(f"{env!r} is not a registered venv program")
    with admin.admin_update_ownership(), admin.toolset_lifecycle_lock(
        [prog], action="vq-retire-checkout",
    ):
        fresh = plan(env, cfg, launchers=launchers)
        if fresh["plan_id"] != expected_plan_id:
            raise RetirementError("the checkout or its references changed; plan again")
        if fresh["blockers"]:
            raise RetirementError("plan has blockers: " + "; ".join(fresh["blockers"]))
        removed = subprocess.run(
            bulk_delete.rmtree_argv(fresh["checkout"]), capture_output=True, text=True,
            timeout=3600, check=False, stdin=subprocess.DEVNULL,
        )
        receipt = {
            "schema": SCHEMA, "env": env, "checkout": fresh["checkout"],
            "head": fresh.get("head"), "upstream": fresh.get("upstream"),
            "plan_id": expected_plan_id, "bytes": fresh["bytes"],
            "service_files": fresh["service_files"],
            "service_file_sha256": fresh["service_file_sha256"],
            "launchers": fresh["launchers"],
            "automatic_launchers": fresh["automatic_launchers"],
            # Left in the registry on purpose (#143): see the module docstring.
            "compatibility_paths": fresh["compatibility_paths"],
            "applied_at": datetime.now(UTC).isoformat(),
            "removed": removed.returncode == 0 and not Path(fresh["checkout"]).exists(),
            "error": removed.stderr.strip() or None,
        }
        receipt_dir = paths.state_root() / "checkout-retirement"
        receipt_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
        path = receipt_dir / f"{stamp}-{re.sub(r'[^A-Za-z0-9_.-]', '_', env)}.json"
        path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
        return {**receipt, "receipt": str(path)}
