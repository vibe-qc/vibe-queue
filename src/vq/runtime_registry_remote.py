"""Bounded SSH registry operations for a journaled runtime migration.

The coordinator owns the durable transaction. An authority only snapshots,
stages or compare-and-swaps one explicitly reviewed regular registry file.
There is no retry of an unknown write; recovery reads the actual identity.
"""

from __future__ import annotations

import os
from typing import Any

from vq import config, transport

SCHEMA = "vq.runtime-registry/1"
LIMIT = 16 * 1024 * 1024


def authority(host: str) -> dict[str, str]:
    from vq.host import is_local_host
    from vq.runtime_migration import MigrationError

    cfg = config.load_config().host(host)
    if is_local_host(host) or is_local_host(cfg.ssh.rsplit("@", 1)[-1]):
        raise MigrationError("local registry authority must use a local registry entry")
    return {"host": host, "ssh": cfg.ssh, "remote_vq": cfg.remote_vq}


def request(peer: dict[str, str], op: str, **fields: Any) -> dict[str, Any]:
    from vq import runtime_migration as migration
    from vq.runtime_participant import parse_document

    if authority(peer["host"]) != peer:
        raise migration.MigrationError("registry authority transport changed")
    body = {"schema": SCHEMA, "op": op, **fields}
    encoded = migration._json(body)
    if len(encoded) > LIMIT:
        raise migration.MigrationError("registry request exceeds its byte limit")
    try:
        proc = transport.run_remote_vq(
            config.load_config().host(peer["host"]),
            "admin",
            "runtime-registry",
            stdin_data=encoded.decode(),
            timeout=60,
            retry_transient=0,
            owned_process_group=True,
            max_stdout_bytes=LIMIT,
            max_stderr_bytes=64 * 1024,
        )
    except transport.RemoteError as exc:
        raise migration.MigrationError(
            f"registry authority operation did not return a receipt: {exc}"
        ) from exc
    answer = parse_document(proc.stdout)
    result = answer.get("result")
    if (
        answer.get("schema") != SCHEMA
        or answer.get("request_sha256") != migration._digest(encoded)
        or not isinstance(result, dict)
    ):
        raise migration.MigrationError("registry authority response does not match the request")
    return result


def snapshot(peer: dict[str, str], path: str) -> dict[str, Any]:
    from vq import runtime_migration as migration

    result = request(peer, "snapshot", path=path)
    if result.get("path") != path or result.get("entry", {}).get("kind") != "file":
        raise migration.MigrationError(
            "remote registry must be a regular file at the reviewed path"
        )
    migration._bytes(result["entry"])
    return {**result, "authority": peer}


def _stage_name(name: str) -> str:
    from vq import runtime_migration as migration

    if (
        not isinstance(name, str)
        or not name.startswith(".vq-migrate-")
        or len(name) != len(".vq-migrate-") + 32
        or any(c not in "0123456789abcdef" for c in name[len(".vq-migrate-") :])
    ):
        raise migration.MigrationError("invalid registry staging name")
    return name


def handle(document: dict[str, Any]) -> dict[str, Any]:
    """Authority-side operation; the CLI parses and bounds the input first."""
    from vq import runtime_migration as migration

    if document.get("schema") != SCHEMA:
        raise migration.MigrationError("unsupported registry operation schema")
    op = document.get("op")
    path = migration._path(document["path"])
    if op == "snapshot":
        result = migration._snapshot(path)
        migration._bytes(result["entry"])
    elif op == "stage":
        before = document["before"]
        if before.get("path") != str(path) or before.get("entry", {}).get("kind") != "file":
            raise migration.MigrationError("remote staging requires a reviewed regular registry")
        after = document["after"]
        if after.get("kind") != "file":
            raise migration.MigrationError("remote registry replacement must remain a regular file")
        migration._bytes(after)
        if migration._snapshot(path) != before:
            raise migration.MigrationError("remote registry changed before staging")
        with migration._directory(path.parent) as fd:
            parent = os.fstat(fd)
            if [parent.st_dev, parent.st_ino] != before["parent"] or migration._entry(
                fd, path.name
            ) != before["entry"]:
                raise migration.MigrationError("remote registry changed at staging boundary")
            result = {
                "prepared_after": migration._write_new(
                    fd, _stage_name(document["forward_stage"]), after
                ),
                "prepared_before": migration._write_new(
                    fd, _stage_name(document["rollback_stage"]), before["entry"]
                ),
            }
    elif op == "replace":
        before, desired, expected = document["before"], document["desired"], document["expected"]
        if (
            before.get("path") != str(path)
            or desired.get("kind") != "file"
            or expected.get("kind") != "file"
        ):
            raise migration.MigrationError(
                "remote replacement requires regular registry identities"
            )
        migration._bytes(desired)
        stage = _stage_name(document["stage"])
        with migration._directory(path.parent) as fd:
            parent = os.fstat(fd)
            if [parent.st_dev, parent.st_ino] != before["parent"]:
                raise migration.MigrationError("remote registry parent changed")
            actual = migration._entry(fd, path.name)
            if actual != desired:
                if actual != expected:
                    raise migration.MigrationError("remote registry has a foreign edit")
                if migration._entry(fd, stage) != desired:
                    raise migration.MigrationError("remote staged registry changed")
                os.replace(stage, path.name, src_dir_fd=fd, dst_dir_fd=fd)
                os.fsync(fd)
            result = {"entry": migration._entry(fd, path.name)}
            if result["entry"] != desired:
                raise migration.MigrationError("remote registry readback differs")
    else:
        raise migration.MigrationError("unknown registry operation")
    return {
        "schema": SCHEMA,
        "request_sha256": migration._digest(migration._json(document)),
        "result": result,
    }


def local_snapshot(snapshot_value: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in snapshot_value.items() if k != "authority"}


def stage(action: dict[str, Any]) -> None:
    before = action["before"]
    response = request(
        before["authority"],
        "stage",
        path=before["path"],
        before=local_snapshot(before),
        after=action["after"],
        forward_stage=action["forward_stage"],
        rollback_stage=action["rollback_stage"],
    )
    action.update(response)


def replace(action: dict[str, Any], *, rollback: bool) -> None:
    before = action["before"]
    request(
        before["authority"],
        "replace",
        path=before["path"],
        before=local_snapshot(before),
        desired=action["prepared_before"] if rollback else action["prepared_after"],
        expected=action["prepared_after"] if rollback else before["entry"],
        stage=action["rollback_stage"] if rollback else action["forward_stage"],
    )
