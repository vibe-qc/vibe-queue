"""Participant coordination for ``vq admin migrate-runtime`` (#72, piece 2).

A scheduler host whose runtimes are packed releases published by a reviewed
site adapter can take part in a runtime migration as a **participant**. The
adapter is the host's configured ``runtime_participant_command``, run over the
host's ssh path. vq sends it one JSON request on stdin and reads one JSON
response from stdout. Nothing here names a site.

Protocol ``vq.runtime-participant/1``:

* canonical form and digest are the migration's own ``_json``/``_digest``;
  duplicate keys, NaN/Infinity and documents over 16 MiB refuse on parse;
* ``op_id`` is the idempotency key, and a response must echo the request's
  ``schema``, ``op`` and ``op_id``;
* ``outcome`` is ``ok``, ``refused``, ``pending`` or ``unknown``, and only
  ``ok`` carries a receipt that vq accepts as evidence;
* ``participant.adapter_sha256`` is bound into the plan; a later response from
  a different adapter refuses.

This module implements the read-only half: the protocol client and the
participant-only plan, whose only remote call is one passive ``inventory`` per
participant. Preparation, apply, verification, rollback and recovery build on
it.
"""
from __future__ import annotations

import dataclasses
import json
import logging
import shlex
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from vq import config, transport
from vq.host import is_local_host

_log = logging.getLogger(__name__)

SCHEMA = "vq.runtime-participant/1"
MAX_DOCUMENT_BYTES = 16 * 1024 * 1024
OUTCOMES = frozenset({"ok", "refused", "pending", "unknown"})
DEFAULT_TIMEOUT_SECONDS = 600.0


class ParticipantError(ValueError):
    """A participant response cannot be trusted as evidence."""


def _json(value: object) -> bytes:
    from vq import runtime_migration

    return runtime_migration._json(value)


def _digest(data: bytes) -> str:
    from vq import runtime_migration

    return runtime_migration._digest(data)


def _no_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ParticipantError(f"duplicate key {key!r} in participant document")
        result[key] = value
    return result


def _no_constants(name: str) -> Any:
    raise ParticipantError(f"non-finite number {name} in participant document")


def parse_document(data: str | bytes) -> dict[str, Any]:
    """Strictly parse one participant JSON document."""
    raw = data.encode() if isinstance(data, str) else data
    if len(raw) > MAX_DOCUMENT_BYTES:
        raise ParticipantError("participant document exceeds 16 MiB")
    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=_no_duplicates,
                           parse_constant=_no_constants)
    except UnicodeDecodeError as exc:
        raise ParticipantError(f"participant document is not UTF-8: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise ParticipantError(f"participant document is not JSON: {exc}") from exc
    if not isinstance(value, dict):
        raise ParticipantError("participant document is not an object")
    return value


Runner = Callable[[config.HostConfig, list[str], str, float], tuple[int, str, str]]


def _ssh_runner(
    host_cfg: config.HostConfig, argv: list[str], stdin: str, timeout: float,
) -> tuple[int, str, str]:
    # The request travels on stdin, which the transport's bounded capture
    # cannot carry; the owned group still bounds the call in time, and
    # parse_document refuses any answer over MAX_DOCUMENT_BYTES.
    proc = transport.run_remote_shell(
        host_cfg, *argv, check=False, timeout=timeout, stdin_data=stdin,
        owned_process_group=True,
    )
    return proc.returncode, proc.stdout or "", proc.stderr or ""


RUNNER: Runner = _ssh_runner
"""How requests reach a participant; tests substitute a local runner."""


@dataclass(frozen=True)
class Response:
    op: str
    op_id: str
    outcome: str
    receipt: Mapping[str, Any] | None
    receipt_sha256: str | None
    errors: tuple[str, ...]
    adapter_sha256: str
    document: Mapping[str, Any]


def command_for(host_cfg: config.HostConfig) -> list[str]:
    """The reviewed participant argv for a host, or refuse."""
    command = getattr(host_cfg, "runtime_participant_command", None)
    if not command:
        raise ParticipantError("host has no reviewed participant command")
    return shlex.split(command)


def request(
    host_cfg: config.HostConfig,
    op: str,
    *,
    fields: Mapping[str, Any],
    op_id: str | None = None,
    expected_adapter: str | None = None,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
) -> Response:
    """Send one request and validate its response strictly."""
    document = {"schema": SCHEMA, "op": op, "op_id": op_id or str(uuid.uuid4()), **fields}
    body = _json(document).decode()
    rc, stdout, stderr = RUNNER(host_cfg, command_for(host_cfg), body, timeout)
    if rc != 0:
        raise ParticipantError(
            f"participant {op} exited {rc}: {stderr.strip()[:500] or '(no stderr)'}"
        )
    answer = parse_document(stdout)
    if (
        answer.get("schema") != SCHEMA
        or answer.get("op") != op
        or answer.get("op_id") != document["op_id"]
    ):
        raise ParticipantError("participant response does not answer this request")
    outcome = answer.get("outcome")
    if outcome not in OUTCOMES:
        raise ParticipantError(f"participant response has unknown outcome {outcome!r}")
    participant = answer.get("participant")
    adapter = participant.get("adapter_sha256") if isinstance(participant, dict) else None
    if not isinstance(adapter, str) or len(adapter) != 64:
        raise ParticipantError("participant response lacks adapter_sha256")
    if expected_adapter is not None and adapter != expected_adapter:
        raise ParticipantError("participant adapter changed since the plan was reviewed")
    receipt = answer.get("receipt")
    receipt_sha = answer.get("receipt_sha256")
    if outcome == "ok" and (
        not isinstance(receipt, dict) or receipt_sha != _digest(_json(receipt))
    ):
        raise ParticipantError("participant receipt digest does not match its receipt")
    errors = answer.get("errors") or []
    if not isinstance(errors, list) or not all(isinstance(e, str) for e in errors):
        raise ParticipantError("participant errors are malformed")
    return Response(op, document["op_id"], outcome,
                    receipt if isinstance(receipt, dict) else None,
                    receipt_sha if isinstance(receipt_sha, str) else None,
                    tuple(errors), adapter, answer)


BINDINGS_SCHEMA = "vq.runtime-bindings/1"
RECORD_SCHEMA = "vq.runtime-migration-record/1"
INVENTORY_SCHEMA = "vq.runtime-participant.inventory/1"
INVENTORY_RECORD_SCHEMA = "vq.runtime-inventory-record/1"
_INVENTORY_PROGRAM_KEYS = ("store", "current", "previous", "launcher_schema", "cache",
                           "inventory_sha256")
_AGREEMENT_KEYS = ("remote_vq", "remote_python", "branches", "branch_aliases",
                   "command_wrappers", "scheduler_runtime_deployments")
"""Binding keys every authority must agree on; fleet_role and
scheduler_driver legitimately differ between drivers."""


def runtime_bindings(cfg: config.Config, host: str) -> dict[str, Any]:
    """This driver's binding-relevant config for ``host`` and its status rows.

    Only paths and commands, never secrets: any key not listed here is left
    out, not redacted. The digest covers the bindings, not the status rows,
    which change with every update.
    """
    from vq import admin

    host_cfg = cfg.host(host)
    bindings = {
        "remote_vq": host_cfg.remote_vq,
        "remote_python": host_cfg.remote_python,
        "branches": dict(host_cfg.branches),
        "branch_aliases": dict(host_cfg.branch_aliases),
        "command_wrappers": {
            program: list(hooks.command_wrapper)
            for program, hooks in sorted(host_cfg.scheduler_program_hooks.items())
        },
        "scheduler_runtime_deployments": {
            program: deployment.model_dump(mode="json")
            for program, deployment in sorted(host_cfg.scheduler_runtime_deployments.items())
        },
        "fleet_role": host_cfg.fleet_role,
        "scheduler_driver": host_cfg.scheduler_driver,
    }
    rows = {
        key: dataclasses.asdict(record)
        for key, record in sorted(admin.load_scheduler_runtime_status().items())
        if record.host == host
    }
    return {"schema": BINDINGS_SCHEMA, "host": host, "bindings": bindings,
            "bindings_sha256": _digest(_json(bindings)), "status": rows}


def record_migration(host: str, record: Mapping[str, Any]) -> list[str]:
    """Attach a migration record to ``host``'s status rows; returns their keys.

    ``record`` is ``vq.runtime-migration-record/1``: ``host``, ``plan_id``,
    ``ready_id`` and per-program ``{receipt_sha256, store, launcher_schema}``.
    Every named lane must already have a status row; nothing else changes.
    """
    from vq import admin

    if record.get("schema") != RECORD_SCHEMA or record.get("host") != host:
        raise ParticipantError("migration record is not for this host")
    programs = record.get("programs")
    if not isinstance(programs, dict) or not programs:
        raise ParticipantError("migration record names no programs")
    records = admin.load_scheduler_runtime_status()
    missing = [p for p in programs if f"{host}:{p}" not in records]
    if missing:
        raise ParticipantError(
            "no status row to record on for " + ", ".join(f"{host}:{p}" for p in missing)
        )
    stamp = admin.utcnow_iso()
    for program, entry in programs.items():
        if not isinstance(entry, dict):
            raise ParticipantError(f"migration record for {program} is malformed")
        records[f"{host}:{program}"].migration = {
            "plan_id": record.get("plan_id"),
            "ready_id": record.get("ready_id"),
            "receipt_sha256": entry.get("receipt_sha256"),
            "store": entry.get("store"),
            "launcher_schema": entry.get("launcher_schema"),
            "recorded_at": stamp,
        }
        if isinstance(entry.get("inventory"), dict):
            records[f"{host}:{program}"].inventory = {**entry["inventory"],
                                                      "recorded_at": stamp}
    admin._write_scheduler_runtime_status(records)
    return [f"{host}:{p}" for p in programs]


def inventory_identity(receipt: Mapping[str, Any] | None, receipt_sha256: str | None) -> str | None:
    """What binds an inventory into a plan and gates apply: its stable state.

    A ``vq.runtime-participant.inventory/1`` receipt carries volatile keys
    (``observed_at``, ``slurm_jobs``) beside the adapter's ``state_sha256``
    over the rest, so binding the whole receipt would make every re-plan
    stale. Any other receipt is bound by its full digest.
    """
    if (
        isinstance(receipt, Mapping) and receipt.get("schema") == INVENTORY_SCHEMA
        and isinstance(receipt.get("state_sha256"), str) and receipt["state_sha256"]
    ):
        return f"state:{receipt['state_sha256']}"
    return receipt_sha256


def summarize_inventory(receipt: Mapping[str, Any] | None) -> dict[str, dict[str, Any]]:
    """Per-program status summary of a ``vq.runtime-participant.inventory/1`` receipt.

    Keeps what ``vq admin status`` shows (store, current, previous, launcher
    schema, cache, the workspace lease expiry) and when it was observed. Any
    other receipt yields ``{}``: a summary is status, never evidence of health.
    """
    if not isinstance(receipt, Mapping) or receipt.get("schema") != INVENTORY_SCHEMA:
        return {}
    programs = receipt.get("programs")
    if not isinstance(programs, Mapping):
        return {}
    workspace = receipt.get("workspace")
    lease = workspace.get("lease_expires_at") if isinstance(workspace, Mapping) else None
    summary: dict[str, dict[str, Any]] = {}
    for program, entry in programs.items():
        if not isinstance(entry, Mapping):
            continue
        summary[str(program)] = {
            **{key: entry.get(key) for key in _INVENTORY_PROGRAM_KEYS},
            "lease_expires_at": lease,
            "observed_at": receipt.get("observed_at"),
        }
    return summary


def record_inventory(host: str, record: Mapping[str, Any]) -> list[str]:
    """Store a participant inventory summary on ``host``'s status rows.

    ``record`` is ``vq.runtime-inventory-record/1``: ``host`` and per-program
    summaries from :func:`summarize_inventory`. Programs without a status row
    are skipped (there is nothing to show them on); returns the keys written.
    Nothing but the ``inventory`` field changes, so LAST OK is untouched.
    """
    from vq import admin

    if record.get("schema") != INVENTORY_RECORD_SCHEMA or record.get("host") != host:
        raise ParticipantError("inventory record is not for this host")
    programs = record.get("programs")
    if not isinstance(programs, dict):
        raise ParticipantError("inventory record names no programs")
    records = admin.load_scheduler_runtime_status()
    stamp = admin.utcnow_iso()
    written: list[str] = []
    for program, entry in sorted(programs.items()):
        key = f"{host}:{program}"
        if key not in records or not isinstance(entry, dict):
            continue
        records[key].inventory = {**entry, "recorded_at": stamp}
        written.append(key)
    if written:
        admin._write_scheduler_runtime_status(records)
    return written


def _local_authority_bindings(cfg: config.Config, host: str) -> dict[str, Any]:
    return runtime_bindings(cfg, host)


def _remote_authority_bindings(cfg: config.Config, authority: str, host: str) -> dict[str, Any]:
    proc = transport.run_remote_vq(
        cfg.host(authority), "admin", "runtime-bindings", host, "--json",
    )
    return parse_document(proc.stdout)


AUTHORITY_BINDINGS: Callable[[config.Config, str, str], dict[str, Any]] = (
    _remote_authority_bindings
)
"""How another authority's bindings are read; tests substitute a fake."""


def authority_bindings(
    cfg: config.Config, entry: Any,
) -> tuple[dict[str, str], list[str]]:
    """Bindings digest per authority, and blockers for disagreement."""
    digests: dict[str, str] = {}
    documents: dict[str, dict[str, Any]] = {}
    blockers: list[str] = []
    for authority in entry.authorities:
        try:
            if authority == "localhost" or (authority in cfg.hosts and is_local_host(authority)):
                document = _local_authority_bindings(cfg, entry.host)
            else:
                document = AUTHORITY_BINDINGS(cfg, authority, entry.host)
        except Exception as exc:  # noqa: BLE001 - any failure is a blocker, never a guess
            blockers.append(f"authority {authority} could not report bindings for "
                            f"{entry.host}: {exc}")
            continue
        bindings = document.get("bindings")
        if (
            document.get("schema") != BINDINGS_SCHEMA
            or not isinstance(bindings, dict)
            or document.get("bindings_sha256") != _digest(_json(bindings))
        ):
            blockers.append(f"authority {authority} returned malformed bindings")
            continue
        digests[authority] = str(document["bindings_sha256"])
        documents[authority] = bindings
    if len(documents) > 1:
        reference = next(iter(documents.values()))
        differing = sorted({
            key for bindings in documents.values() for key in _AGREEMENT_KEYS
            if bindings.get(key) != reference.get(key)
        })
        if differing:
            blockers.append(f"authorities disagree on {entry.host} bindings: "
                            + ", ".join(differing))
    return digests, blockers


def participant_plan(manifest: Any, cfg: config.Config) -> dict[str, Any]:
    """Read-only plan for a participant-only manifest.

    One ``inventory`` per participant is the only remote call. Its receipt
    digest and the adapter identity are part of the plan, so any change to the
    participant's inventory or adapter produces a different ``plan_id``.
    """
    blockers: list[str] = []
    consumers = set(manifest.external_consumers)
    participants: list[dict[str, Any]] = []
    for entry in manifest.participants:
        record: dict[str, Any] = entry.model_dump(mode="json")
        if entry.host in consumers:
            blockers.append(f"{entry.host} is both a participant and an external consumer")
        try:
            host_cfg = cfg.host(entry.host)
        except (KeyError, config.ConfigError, ValueError):
            blockers.append(f"{entry.host} is not a configured host")
            participants.append(record)
            continue
        local = [a for a in entry.authorities if a in cfg.hosts and is_local_host(a)]
        if not local and "localhost" not in entry.authorities:
            blockers.append(f"this driver is not an authority listed for {entry.host}")
        for authority in entry.authorities:
            if authority == "localhost" or is_local_host(authority):
                continue
            if authority not in cfg.hosts:
                blockers.append(f"authority {authority} is not a configured host")
        try:
            record["command"] = command_for(host_cfg)
        except ParticipantError as exc:
            blockers.append(f"{entry.host}: {exc}")
            participants.append(record)
            continue
        try:
            response = request(host_cfg, "inventory", fields={
                "programs": list(entry.programs), "kind": entry.kind,
            })
        except (ParticipantError, transport.RemoteError, OSError) as exc:
            blockers.append(f"{entry.host} inventory failed: {exc}")
            participants.append(record)
            continue
        if response.outcome != "ok" or response.receipt is None:
            blockers.append(
                f"{entry.host} inventory answered {response.outcome}: "
                + ("; ".join(response.errors) or "no receipt")
            )
        digests, authority_blockers = authority_bindings(cfg, entry)
        blockers.extend(authority_blockers)
        record["authority_bindings_sha256"] = digests
        record.update({
            "adapter_sha256": response.adapter_sha256,
            "inventory": response.receipt,
            "inventory_sha256": response.receipt_sha256,
            "inventory_identity": inventory_identity(response.receipt,
                                                     response.receipt_sha256),
        })
        participants.append(record)
    if manifest.registries:
        blockers.append(
            "migrate local and participant components in separate transactions"
        )
    body = {
        "schema": "vq.runtime-migration/1",
        "mode": "participants",
        "manifest": manifest.model_dump(mode="json"),
        "participants": participants,
        "blockers": sorted(blockers),
        "status": "preparation_required",
    }
    # The plan binds each inventory by its stable identity, not the whole
    # receipt, whose volatile keys change on every call.
    bound = {**body, "participants": [
        {k: v for k, v in p.items() if k not in {"inventory", "inventory_sha256"}}
        for p in participants
    ]}
    return {**body, "plan_id": _digest(_json(bound))}


# ---------------------------------------------------------------------------
# Preparation (P3): prepare without activation, and the ready plan
# ---------------------------------------------------------------------------

POLL_SECONDS = 30.0
DEFAULT_PREPARE_TIMEOUT_SECONDS = 14400.0
SLEEP: Callable[[float], None] = __import__("time").sleep
CLOCK: Callable[[], float] = __import__("time").monotonic


def _migration_error(message: str) -> Exception:
    from vq import runtime_migration

    return runtime_migration.MigrationError(message)


def _ready_dir():
    from vq import paths

    return paths.state_root() / "runtime-participants"


def _store_ready(ready: dict[str, Any]) -> None:
    from vq import paths

    directory = _ready_dir()
    directory.mkdir(parents=True, exist_ok=True)
    text = _json(ready).decode()
    for key in (ready["plan_id"], ready["ready_id"]):
        paths.atomic_write_text(directory / f"{key}.json", text)


def load_ready(key: str) -> dict[str, Any] | None:
    """A stored ready plan by its ``plan_id`` or ``ready_id``, digest-checked."""
    path = _ready_dir() / f"{key}.json"
    if len(key) != 64 or not all(c in "0123456789abcdef" for c in key) or not path.exists():
        return None
    ready = parse_document(path.read_bytes())
    body = {k: v for k, v in ready.items() if k != "ready_id"}
    if ready.get("ready_id") != _digest(_json(body)):
        raise _migration_error(f"stored ready plan {key} does not match its digest")
    return ready


def _prepare_timeout(cfg: config.Config, entry: Mapping[str, Any]) -> float:
    host_cfg = cfg.host(entry["host"])
    timeouts = [
        deployment.timeout_seconds
        for program, deployment in host_cfg.scheduler_runtime_deployments.items()
        if program in entry["programs"] and deployment.timeout_seconds
    ]
    return max(timeouts, default=DEFAULT_PREPARE_TIMEOUT_SECONDS)


def _until_done(
    host_cfg: config.HostConfig, op: str, fields: Mapping[str, Any], *, op_id: str,
    adapter: str, deadline_seconds: float,
) -> Response:
    """Send ``op``; poll ``inspect`` while it is pending; return an ``ok`` answer."""
    response = request(host_cfg, op, fields=fields, op_id=op_id, expected_adapter=adapter)
    deadline = CLOCK() + deadline_seconds
    while response.outcome == "pending":
        job = (response.document.get("job_id")
               or (response.receipt or {}).get("job_id") or "unknown")
        if CLOCK() >= deadline:
            raise _migration_error(
                f"{op} on {host_cfg.ssh} is still running (job {job}); resume later")
        SLEEP(POLL_SECONDS)
        inspected = request(host_cfg, "inspect", fields={"target_op_id": op_id},
                            expected_adapter=adapter)
        if inspected.outcome != "ok" or inspected.receipt is None:
            raise _migration_error(f"inspect of {op} answered {inspected.outcome}")
        status = inspected.receipt.get("status")
        if status == "in_progress":
            continue
        if status != "applied" or not isinstance(inspected.receipt.get("receipt"), dict):
            raise _migration_error(f"{op} on {host_cfg.ssh} ended {status!r}")
        inner = inspected.receipt["receipt"]
        response = Response(op, op_id, "ok", inner, _digest(_json(inner)), (),
                            adapter, inspected.document)
    if response.outcome != "ok" or response.receipt is None:
        raise _migration_error(
            f"{op} on {host_cfg.ssh} answered {response.outcome}: "
            + ("; ".join(response.errors) or "no receipt"))
    return response


def _build_inputs(
    cfg: config.Config, host: str, program: str, source_sha: str,
) -> dict[str, Any]:
    """``source_archive`` and ``allocation`` for a participant's build.

    A host whose deployment block sets ``stage_source`` has no repository
    credentials: the driver archives ``source_sha`` and stages it there
    exactly as ``vq admin update`` does, and the adapter builds from that
    absolute path. ``update_allocation`` contributes its ``sbatch_args``.
    """
    from vq import admin

    host_cfg = cfg.host(host)
    deployment = host_cfg.scheduler_runtime_deployments.get(program)
    if deployment is None:
        return {}
    extra: dict[str, Any] = {}
    if deployment.stage_source:
        build_host_cfg = (
            host_cfg.model_copy(update={"ssh": deployment.update_host})
            if deployment.update_host is not None else host_cfg
        )
        scratch = admin.SchedulerRuntimeUpdateResult(
            host=host, program=program, mode="update", command="", command_ssh=build_host_cfg.ssh,
            verify_command="", verify_ssh=host_cfg.ssh, expected_sha=source_sha,
        )
        try:
            extra["source_archive"] = STAGE_SOURCE(
                host, build_host_cfg, program, source_sha, cfg, scratch)
        except admin.AdminError as exc:
            raise _migration_error(f"source staging for {host}:{program} failed: {exc}") from None
    if deployment.update_allocation is not None:
        extra["allocation"] = {"sbatch_args": list(deployment.update_allocation.sbatch_args)}
    return extra


def _stage_source(*args: Any) -> str:
    from vq import admin

    return admin._stage_scheduler_runtime_source(*args)


STAGE_SOURCE: Callable[..., str] = _stage_source
"""How the driver stages a participant's source archive; tests substitute a fake."""


def participant_prepare(
    manifest: Any, cfg: config.Config, expected_plan_id: str,
) -> dict[str, Any]:
    """Prepare every participant without activation; return the ready plan.

    A repeat with the same ``plan_id`` replays the recorded ``op_id``s, which a
    conforming adapter answers idempotently, and must reproduce the same
    receipts, so it yields the same ``ready_id``. The ready plan embeds each
    participant's inventory after preparation: apply refuses any drift from it.
    """
    from vq import admin

    stored = load_ready(expected_plan_id)
    with admin.admin_update_ownership():
        if stored is not None:
            for row in stored["preparation"]:
                again = _until_done(
                    cfg.host(row["host"]), "prepare", row["fields"], op_id=row["op_id"],
                    adapter=row["adapter_sha256"],
                    deadline_seconds=_prepare_timeout(cfg, row),
                )
                if again.receipt_sha256 != row["receipt_sha256"]:
                    raise _migration_error(
                        f"{row['host']} returned a different preparation receipt on replay")
            return stored
        value = participant_plan(manifest, cfg)
        if value["plan_id"] != expected_plan_id:
            raise _migration_error("plan is stale or its reviewed digest disagrees")
        if value["blockers"]:
            raise _migration_error("plan has unresolved blockers")
        preparation: list[dict[str, Any]] = []
        post_inventory: dict[str, dict[str, Any]] = {}
        for participant in value["participants"]:
            host_cfg = cfg.host(participant["host"])
            adapter = participant["adapter_sha256"]
            for program in participant["programs"]:
                fields = {
                    "plan_id": expected_plan_id,
                    "program": program,
                    "identity": {"source_sha": manifest.source_sha},
                    **_build_inputs(cfg, participant["host"], program, manifest.source_sha),
                }
                row = {"host": participant["host"], "programs": [program],
                       "op_id": str(uuid.uuid4()), "fields": fields,
                       "adapter_sha256": adapter}
                answer = _until_done(host_cfg, "prepare", fields, op_id=row["op_id"],
                                     adapter=adapter,
                                     deadline_seconds=_prepare_timeout(cfg, row))
                row.update(receipt=answer.receipt, receipt_sha256=answer.receipt_sha256)
                preparation.append(row)
            after = request(host_cfg, "inventory", fields={
                "programs": list(participant["programs"]), "kind": participant["kind"],
            }, expected_adapter=adapter)
            if after.outcome != "ok" or after.receipt is None:
                raise _migration_error(
                    f"{participant['host']} inventory after preparation answered {after.outcome}")
            post_inventory[participant["host"]] = {
                "inventory": after.receipt, "inventory_sha256": after.receipt_sha256,
                "inventory_identity": inventory_identity(after.receipt, after.receipt_sha256),
            }
        ready = {**value, "status": "ready", "preparation": preparation,
                 "post_inventory": post_inventory}
        ready["ready_id"] = _digest(_json(ready))
        _store_ready(ready)
        for participant in value["participants"]:
            results = _record_inventory_everywhere(
                cfg, participant, post_inventory[participant["host"]]["inventory"])
            for key, outcome in results.items():
                if outcome.startswith("failed"):
                    _log.warning("inventory status for %s not recorded: %s", key, outcome)
        return ready


# ---------------------------------------------------------------------------
# Apply, verify, rollback, recovery, recording, acknowledge (P4)
# ---------------------------------------------------------------------------

TRANSACTION_SCHEMA = "vq.runtime-participant-transaction/1"


def _control(argv: list[str]) -> tuple[int, str]:
    import subprocess
    import sys

    proc = subprocess.run([sys.executable, "-m", "vq.cli", *argv], stdin=subprocess.DEVNULL,
                          capture_output=True, text=True, timeout=300, check=False)
    return proc.returncode, (proc.stderr or proc.stdout or "").strip()


LEASE: Callable[[list[str]], tuple[int, str]] = _control
"""Runs a local ``vq drain`` for the migration lease; tests substitute a fake."""


def _remote_record(
    cfg: config.Config, authority: str, host: str, record: Mapping[str, Any],
) -> None:
    transport.run_remote_vq(
        cfg.host(authority), "admin", "runtime-bindings", host,
        "--record-migration", "-", stdin_data=_json(record).decode(),
    )


RECORDER: Callable[[config.Config, str, str, Mapping[str, Any]], None] = _remote_record
"""How another authority records the migration; tests substitute a fake."""


def _remote_record_inventory(
    cfg: config.Config, authority: str, host: str, record: Mapping[str, Any],
) -> None:
    transport.run_remote_vq(
        cfg.host(authority), "admin", "runtime-bindings", host,
        "--record-inventory", "-", stdin_data=_json(record).decode(),
    )


INVENTORY_RECORDER: Callable[[config.Config, str, str, Mapping[str, Any]], None] = (
    _remote_record_inventory
)
"""How another authority records an inventory summary; tests substitute a fake."""


def _record_inventory_everywhere(
    cfg: config.Config, participant: Mapping[str, Any], receipt: Mapping[str, Any] | None,
) -> dict[str, str]:
    """Best effort: an inventory summary is status, so a failure is reported, not fatal."""
    summary = summarize_inventory(receipt)
    host = participant["host"]
    if not summary:
        return {f"{a}:{host}": "skipped: no inventory/1 receipt"
                for a in participant["authorities"]}
    record = {"schema": INVENTORY_RECORD_SCHEMA, "host": host, "programs": summary}
    results: dict[str, str] = {}
    for authority in participant["authorities"]:
        key = f"{authority}:{host}"
        try:
            if authority == "localhost" or (authority in cfg.hosts and is_local_host(authority)):
                record_inventory(host, record)
            else:
                INVENTORY_RECORDER(cfg, authority, host, record)
            results[key] = "recorded"
        except Exception as exc:  # noqa: BLE001 - reported per authority
            results[key] = f"failed: {exc}"
    return results


def _owner(plan_id: str) -> str:
    return f"runtime-migration:{plan_id}"


def _lease(cfg: config.Config, state: dict[str, Any], *, acquire: bool) -> None:
    plan_id = state["ready"]["plan_id"]
    for participant in state["ready"]["participants"]:
        host = participant["host"]
        control = cfg.host(host).scheduler_driver or "localhost"
        argv = (["drain", "--scheduler-host", host, "--reason",
                 f"runtime migration {plan_id[:12]}", "--lease-owner", _owner(plan_id), control]
                if acquire else
                ["drain", "--release", "--scheduler-host", host,
                 "--lease-owner", _owner(plan_id), control])
        rc, detail = LEASE(argv)
        if rc != 0:
            verb = "acquire" if acquire else "release"
            raise _migration_error(f"could not {verb} the migration lease on {host}: {detail}")
    state["lease"] = {"owner": _owner(plan_id), "held": acquire}


def _save(directory: Any, state: dict[str, Any]) -> None:
    from vq import runtime_migration

    runtime_migration._journal(directory, state)


def _row(state: dict[str, Any], step: str, host: str, program: str) -> dict[str, Any] | None:
    for row in state["rows"]:
        if (row["step"], row["host"], row["program"]) == (step, host, program):
            return row
    return None


def _call_timeout(cfg: config.Config, host: str, program: str, op: str) -> float:
    """Per-call bound: an apply, verify or rollback may queue an allocation step.

    ``verify`` uses the deployment's ``verify_timeout_seconds``; ``apply`` and
    ``rollback`` its ``timeout_seconds``; anything else, or a program without
    a deployment block, the protocol default.
    """
    deployment = cfg.host(host).scheduler_runtime_deployments.get(program)
    if deployment is not None:
        if op == "verify" and deployment.verify_timeout_seconds:
            return float(deployment.verify_timeout_seconds)
        if op in {"apply", "rollback"} and deployment.timeout_seconds:
            return float(deployment.timeout_seconds)
    return DEFAULT_TIMEOUT_SECONDS


def _rpc(
    cfg: config.Config, directory: Any, state: dict[str, Any], *, step: str, op: str,
    host: str, program: str, fields: Mapping[str, Any], adapter: str,
) -> dict[str, Any]:
    """One journaled participant call: request row durable before, response after.

    A recorded response is returned without contacting the participant. A row
    whose outcome is unknown is resolved through ``inspect`` first; a refused
    row is re-sent with the same ``op_id`` and identical request bytes.
    """
    row = _row(state, step, host, program)
    host_cfg = cfg.host(host)
    if row is not None and row.get("response") is not None:
        return row["response"]
    if row is not None and row.get("needs_inspect"):
        inspected = request(host_cfg, "inspect", fields={"target_op_id": row["op_id"]},
                            expected_adapter=adapter)
        status = (inspected.receipt or {}).get("status") if inspected.outcome == "ok" else None
        if status == "applied" and isinstance((inspected.receipt or {}).get("receipt"), dict):
            inner = inspected.receipt["receipt"]
            row["response"] = {"outcome": "ok", "receipt": inner,
                               "receipt_sha256": _digest(_json(inner)), "via": "inspect"}
            row["needs_inspect"] = False
            _save(directory, state)
            return row["response"]
        if status == "in_progress":
            job = (inspected.receipt or {}).get("job_id") or "unknown"
            raise _migration_error(
                f"{op} on {host} is still in progress (job {job}); not replaying")
        if status != "not_applied":
            raise _migration_error(f"inspect of {op} on {host} answered {status!r}")
        row["needs_inspect"] = False
    if row is None:
        row = {"step": step, "host": host, "program": program, "op": op,
               "op_id": str(uuid.uuid4()), "fields": dict(fields),
               "request_sha256": _digest(_json(dict(fields))), "response": None,
               "needs_inspect": False}
        state["rows"].append(row)
    row["needs_inspect"] = True  # durable before the call: a lost answer is unknown
    _save(directory, state)
    try:
        response = request(host_cfg, op, fields=row["fields"], op_id=row["op_id"],
                           expected_adapter=adapter,
                           timeout=_call_timeout(cfg, host, program, op))
    except (ParticipantError, transport.RemoteError, OSError) as exc:
        raise _migration_error(
            f"{op} on {host} has an unknown outcome ({exc}); run --resume") from exc
    if response.outcome == "unknown":
        raise _migration_error(f"{op} on {host} reported an unknown outcome; run --resume")
    row["needs_inspect"] = False
    if response.outcome == "refused":
        row["last_refusal"] = list(response.errors)
        _save(directory, state)
        raise _RefusedError(f"{op} on {host} refused: " + ("; ".join(response.errors) or "-"))
    if response.outcome != "ok":
        _save(directory, state)
        raise _migration_error(f"{op} on {host} answered {response.outcome}")
    row["response"] = {"outcome": "ok", "receipt": response.receipt,
                       "receipt_sha256": response.receipt_sha256}
    _save(directory, state)
    return row["response"]


class _RefusedError(Exception):
    """A participant refused: nothing changed on its side."""


def _prep_rows(state: dict[str, Any]) -> list[dict[str, Any]]:
    return list(state["ready"]["preparation"])


def _healthy(receipt: Mapping[str, Any] | None) -> bool:
    return bool(
        receipt and receipt.get("healthy") is True
        and receipt.get("activation") == "atomic" and receipt.get("quiescent") is True
    )


def _migration_record(state: dict[str, Any], host: str, *, step: str) -> dict[str, Any]:
    ready = state["ready"]
    programs = {}
    for prep in _prep_rows(state):
        if prep["host"] != host:
            continue
        program = prep["programs"][0]
        row = _row(state, step, host, program)
        receipt = (row or {}).get("response", {}).get("receipt") or {}
        programs[program] = {
            "receipt_sha256": (row or {}).get("response", {}).get("receipt_sha256"),
            "store": receipt.get("store"),
            "launcher_schema": receipt.get("launcher_schema"),
        }
        observed = summarize_inventory(state.get("observed_inventory", {}).get(host))
        if program in observed:
            programs[program]["inventory"] = observed[program]
    return {"schema": RECORD_SCHEMA, "host": host, "plan_id": ready["plan_id"],
            "ready_id": ready["ready_id"], "programs": programs}


def _observe_inventory(cfg: config.Config, state: dict[str, Any]) -> None:
    """One passive inventory per participant, for the status the record carries.

    It never decides anything: a failed or refused inventory only leaves the
    record without a summary.
    """
    observed: dict[str, Any] = {}
    for participant in state["ready"]["participants"]:
        try:
            answer = request(cfg.host(participant["host"]), "inventory", fields={
                "programs": list(participant["programs"]), "kind": participant["kind"],
            }, expected_adapter=participant["adapter_sha256"])
        except Exception as exc:  # noqa: BLE001 - status only
            _log.warning("inventory of %s for status failed: %s", participant["host"], exc)
            continue
        if answer.outcome == "ok" and answer.receipt is not None:
            observed[participant["host"]] = answer.receipt
    state["observed_inventory"] = observed


def _record_everywhere(cfg: config.Config, state: dict[str, Any], *, step: str) -> None:
    _observe_inventory(cfg, state)
    results: dict[str, str] = {}
    for participant in state["ready"]["participants"]:
        record = _migration_record(state, participant["host"], step=step)
        for authority in participant["authorities"]:
            key = f"{authority}:{participant['host']}"
            try:
                if authority == "localhost" or (
                    authority in cfg.hosts and is_local_host(authority)
                ):
                    record_migration(participant["host"], record)
                else:
                    RECORDER(cfg, authority, participant["host"], record)
                results[key] = "recorded"
            except Exception as exc:  # noqa: BLE001 - reported per authority
                results[key] = f"failed: {exc}"
    state["records"] = results


def _drive(
    cfg: config.Config, directory: Any, state: dict[str, Any], *, rollback: bool,
) -> dict[str, Any]:
    ready = state["ready"]
    base = {"plan_id": ready["plan_id"], "ready_id": ready["ready_id"]}
    if state["state"] == "rollback_unverified":
        # Re-verify with a fresh op_id; the unhealthy answer stays on record.
        state.setdefault("superseded_rows", []).extend(
            r for r in state["rows"] if r["step"] == "verify-rollback")
        state["rows"] = [r for r in state["rows"] if r["step"] != "verify-rollback"]
        state["state"] = "rolling_back"
    if rollback or state["state"] == "rolling_back":
        state["state"] = "rolling_back"
        _save(directory, state)
        for prep in _prep_rows(state):
            program = prep["programs"][0]
            applied = _row(state, "apply", prep["host"], program)
            if applied is None or applied.get("response") is None:
                continue  # never applied there: nothing to roll back
            _rpc(cfg, directory, state, step="rollback", op="rollback", host=prep["host"],
                 program=program, adapter=prep["adapter_sha256"],
                 fields={**base, "program": program,
                         "expected_after": applied["response"]["receipt"]})
            before = applied["response"]["receipt"].get("before")
            restored = before.get("current") if isinstance(before, dict) else None
            checked = _rpc(cfg, directory, state, step="verify-rollback", op="verify",
                           host=prep["host"], program=program,
                           adapter=prep["adapter_sha256"],
                           fields={**base, "program": program, "mode": "active",
                                   **({"identity": {"source_sha": restored}}
                                      if restored else {})})
            if not _healthy(checked["receipt"]):
                # The lease stays: the runtime in service is neither the new
                # one nor proven to be the old one.
                state["state"] = "rollback_unverified"
                state["failure"] = (f"{prep['host']}:{program} did not verify healthy "
                                    "after rollback")
                _save(directory, state)
                raise _migration_error(state["failure"] + "; the lease is kept, --resume "
                                       "verifies again")
        if state.get("records"):
            # A recorded migration was rolled back: re-record the authorities.
            # A rollback after failed verification recorded nothing to undo.
            _record_everywhere(cfg, state, step="rollback")
        state["state"] = "rolled_back"
        _save(directory, state)
        return state
    state["state"] = "applying"
    _save(directory, state)
    for prep in _prep_rows(state):
        program = prep["programs"][0]
        _rpc(cfg, directory, state, step="apply", op="apply", host=prep["host"],
             program=program, adapter=prep["adapter_sha256"],
             fields={**base, "program": program, "identity": prep["fields"]["identity"],
                     "expected_before": ready["post_inventory"][prep["host"]]["inventory"]})
    state["state"] = "verifying"
    _save(directory, state)
    for prep in _prep_rows(state):
        program = prep["programs"][0]
        verified = _rpc(cfg, directory, state, step="verify", op="verify", host=prep["host"],
                        program=program, adapter=prep["adapter_sha256"],
                        fields={**base, "program": program, "mode": "active",
                                "identity": prep["fields"]["identity"]})
        if not _healthy(verified["receipt"]):
            state["failure"] = f"{prep['host']}:{program} did not verify healthy after apply"
            return _drive(cfg, directory, state, rollback=True)
    state["state"] = "recording"
    _save(directory, state)
    _record_everywhere(cfg, state, step="apply")
    state["state"] = "applied"
    _save(directory, state)
    return state


def _run(
    cfg: config.Config, directory: Any, state: dict[str, Any], *, rollback: bool,
) -> dict[str, Any]:
    from vq import admin

    with admin.admin_update_ownership():
        if not state.get("lease", {}).get("held"):
            _lease(cfg, state, acquire=True)
            _save(directory, state)
        try:
            result = _drive(cfg, directory, state, rollback=rollback)
        except _RefusedError as exc:
            if any(r["step"] == "apply" and r.get("response") for r in state["rows"]):
                # An applied runtime is active but unverified: keep the lease.
                _save(directory, state)
                raise _migration_error(
                    f"{exc}; an applied runtime is not verified, so the lease is kept; "
                    "--resume retries, --rollback rolls back") from None
            _lease(cfg, state, acquire=False)
            _save(directory, state)
            raise _migration_error(f"{exc}; nothing changed there, --resume retries") from None
        _lease(cfg, state, acquire=False)
        _save(directory, state)
        return result


def participant_apply(
    manifest: Any, cfg: config.Config, ready_id: str, receipt_dir: Any,
) -> dict[str, Any]:
    """Apply a reviewed ready plan to every participant, as a journaled transaction."""
    import os

    from vq import runtime_migration

    ready = load_ready(ready_id)
    if ready is None or ready.get("ready_id") != ready_id:
        raise _migration_error("apply needs the ready_id of a prepared, reviewed ready plan")
    if ready["manifest"] != manifest.model_dump(mode="json"):
        raise _migration_error("manifest differs from the one the ready plan was prepared for")
    for participant in ready["participants"]:
        host_cfg = cfg.host(participant["host"])
        now = request(host_cfg, "inventory", fields={
            "programs": list(participant["programs"]), "kind": participant["kind"],
        }, expected_adapter=participant["adapter_sha256"])
        prepared = ready["post_inventory"][participant["host"]]
        if inventory_identity(now.receipt, now.receipt_sha256) != prepared.get(
            "inventory_identity", prepared["inventory_sha256"]
        ):
            raise _migration_error(
                f"{participant['host']} inventory changed since preparation; prepare again")
        entry = runtime_migration.Participant.model_validate(
            {k: participant[k] for k in ("host", "kind", "programs", "authorities")})
        digests, blockers = authority_bindings(cfg, entry)
        if blockers or digests != participant.get("authority_bindings_sha256"):
            raise _migration_error(
                f"{participant['host']} authority bindings changed since the plan was reviewed")
    directory = runtime_migration._path(receipt_dir)
    with runtime_migration._directory(directory.parent) as fd:
        os.mkdir(directory.name, mode=0o700, dir_fd=fd)
        os.fsync(fd)
    state = {"schema": TRANSACTION_SCHEMA, "ready": ready, "state": "initializing",
             "rows": [], "lease": {"owner": _owner(ready["plan_id"]), "held": False},
             "records": {}}
    _save(directory, state)
    return _run(cfg, directory, state, rollback=False)


def participant_recover(
    cfg: config.Config, directory: Any, state: dict[str, Any], *, rollback: bool,
) -> dict[str, Any]:
    """``--resume`` / ``--rollback`` for a participant transaction."""
    if state["state"] in {"rolled_back", "acknowledged"}:
        if rollback and state["state"] == "rolled_back":
            return state
        raise _migration_error(f"transaction is {state['state']}; make a new plan")
    if state["state"] == "applied" and not rollback:
        failed = {k: v for k, v in state.get("records", {}).items() if v != "recorded"}
        if failed:
            _record_everywhere(cfg, state, step="apply")
            _save(directory, state)
        return state
    return _run(cfg, directory, state, rollback=rollback)


def participant_acknowledge(
    cfg: config.Config, directory: Any, state: dict[str, Any],
) -> dict[str, Any]:
    """Terminal acknowledgement on every participant; grants no cleanup."""
    if state["state"] != "applied":
        raise _migration_error(f"only an applied transaction can be acknowledged "
                               f"(this one is {state['state']})")
    failed = sorted(k for k, v in state.get("records", {}).items() if v != "recorded")
    if failed:
        raise _migration_error("not recorded on " + ", ".join(failed) + "; run --resume")
    base = {"plan_id": state["ready"]["plan_id"], "ready_id": state["ready"]["ready_id"]}
    try:
        for prep in _prep_rows(state):
            program = prep["programs"][0]
            _rpc(cfg, directory, state, step="acknowledge", op="acknowledge",
                 host=prep["host"], program=program, adapter=prep["adapter_sha256"],
                 fields={**base, "program": program})
    except _RefusedError as exc:
        # Nothing changed there and the transaction stays applied; the
        # journal keeps the refusal, and the next --acknowledge re-sends.
        raise _migration_error(
            f"{exc}; the transaction stays applied, --acknowledge retries") from None
    state["state"] = "acknowledged"
    _save(directory, state)
    return state
