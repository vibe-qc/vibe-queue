"""Durable managed-generation reservations for cooperating runtime providers.

A provider must hold :meth:`ReservationStore.transaction` around both generation
verification/reservation and every removal, replacement or relocation of a
published generation. Merely using this store does not authenticate a provider:
its configured transport, immutable root and normal health verifier do that.
Reservations never expire. A lost answer or driver crash is not terminal proof.
"""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import os
import re
import stat
from collections.abc import Callable, Iterator, Mapping
from pathlib import Path
from typing import Any

SCHEMA = "vq.runtime-admission/1"
LIMIT = 1024 * 1024
HEX40 = re.compile(r"[0-9a-f]{40}")
HEX64 = re.compile(r"[0-9a-f]{64}")


class AdmissionError(ValueError):
    """Admission or cleanup cannot establish its required authority."""


class AdmissionBusy(AdmissionError):
    """The authenticated provider authority is held; retry without dispatch."""


_AUTHORITY_BUSY = "provider cleanup/admission authority is busy"


def canonical(value: object) -> bytes:
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n"
    ).encode()


def digest(value: object) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()


def _hex(value: object, pattern: re.Pattern[str], name: str) -> str:
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        raise AdmissionError(f"invalid {name}")
    return value


def _real_directory(path: Path) -> None:
    if not path.is_absolute() or path != path.resolve(strict=True):
        raise AdmissionError(f"directory is not a canonical immutable authority: {path}")
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid():
        raise AdmissionError("directory is not owned by the provider")
    if info.st_mode & 0o022:
        raise AdmissionError("directory permits writes by another principal")


def _regular(fd: int) -> os.stat_result:
    info = os.fstat(fd)
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_uid != os.geteuid()
        or info.st_nlink != 1
        or info.st_mode & 0o022
    ):
        raise AdmissionError("reservation authority is not a private regular file")
    return info


def validate_identity(identity: Mapping[str, Any], program: str, source: str) -> dict[str, Any]:
    """Validate a health verifier's complete identity, never a caller's path."""
    result = dict(identity)
    expected = {
        "program",
        "source_sha",
        "generation",
        "source_root",
        "python",
        "native_sha256",
        "content_sha256",
        "healthy",
    }
    packed = result.get("layout") == "packed-v1"
    if packed:
        expected |= {"layout", "archive_sha256", "launcher_sha256"}
    if set(result) != expected or result["program"] != program or result["source_sha"] != source:
        raise AdmissionError("provider identity does not match the exact request")
    if result["healthy"] is not True:
        raise AdmissionError("generation did not pass normal provider health")
    _hex(source, HEX40, "source SHA")
    for key in ("native_sha256", "content_sha256"):
        _hex(result[key], HEX64, key)
    for key in ("generation", "source_root", "python"):
        value = result[key]
        if (
            not isinstance(value, str)
            or not value.startswith("/")
            or any(ord(c) < 32 for c in value)
            or ".." in Path(value).parts
            or str(Path(value)) != value
        ):
            raise AdmissionError(f"invalid immutable {key}")
    generation = Path(result["generation"])
    if generation.name != source or generation.parent.name != "releases":
        raise AdmissionError("generation does not name its exact source")
    if packed:
        # Provider-authenticated packed publications have no unpacked source tree.
        # The archive and exact immutable launcher are part of the durable binding.
        for key in ("archive_sha256", "launcher_sha256"):
            _hex(result[key], HEX64, key)
        if Path(result["source_root"]) != generation:
            raise AdmissionError("packed source root must name its publication")
        launcher = Path(result["python"])
        if not (
            launcher.name.startswith(program + "-")
            and re.search(
                r"-" + re.escape(source) + r"(?:-sha256-[0-9a-f]{64})?(?:-store)?-python$",
                launcher.name,
            )
        ):
            raise AdmissionError("packed interpreter does not name its exact source")
        return result
    if Path(result["source_root"]) != generation / "source":
        raise AdmissionError("source root escapes the generation")
    if Path(result["python"]) != generation / "source/.venv/bin/python":
        raise AdmissionError("interpreter does not name the immutable generation")
    return result


class ReservationStore:
    """A provider-owned durable state machine serialized with its cleanup.

    ``directory`` is a fixed configured provider path outside generation trees;
    it is never selected by a request. ``verify`` must run the provider's normal
    isolated dependency/native checks and return :func:`validate_identity`'s
    shape. All operations below must be called inside ``transaction``. A cleanup
    owner checks ``protects`` while holding the *same* transaction through its
    final removal/rename. Unsupported cleanup owners cannot advertise admission.
    """

    def __init__(self, directory: Path, verify: Callable[[str, str], Mapping[str, Any]]):
        self.directory = Path(directory)
        self.verify = verify
        self._directory_fd: int | None = None
        self._lock_fd: int | None = None

    @contextlib.contextmanager
    def transaction(self) -> Iterator[ReservationStore]:
        if self._directory_fd is not None:
            raise AdmissionError("nested admission transaction")
        _real_directory(self.directory)
        directory_fd = os.open(self.directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        lock_fd = None
        try:
            lock_fd = os.open(
                "authority.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600, dir_fd=directory_fd
            )
            before = _regular(lock_fd)
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise AdmissionBusy(_AUTHORITY_BUSY) from exc
            after = os.stat("authority.lock", dir_fd=directory_fd, follow_symlinks=False)
            if (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino):
                raise AdmissionError("admission lock authority changed")
            self._directory_fd, self._lock_fd = directory_fd, lock_fd
            self._authority()
            yield self
        finally:
            self._directory_fd = self._lock_fd = None
            if lock_fd is not None:
                os.close(lock_fd)
            os.close(directory_fd)

    def _authority(self) -> int:
        if self._directory_fd is None or self._lock_fd is None:
            raise AdmissionError("operation requires the provider cleanup transaction")
        _real_directory(self.directory)
        before, after = os.fstat(self._directory_fd), self.directory.stat()
        if (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino):
            raise AdmissionError("reservation directory authority changed")
        current = os.stat("authority.lock", dir_fd=self._directory_fd, follow_symlinks=False)
        locked = _regular(self._lock_fd)
        if (current.st_dev, current.st_ino) != (locked.st_dev, locked.st_ino):
            raise AdmissionError("admission lock authority changed")
        return self._directory_fd

    def _read(self, key: str) -> dict[str, Any] | None:
        _hex(key, HEX64, "reservation key")
        directory = self._authority()
        try:
            fd = os.open(key + ".json", os.O_RDONLY | os.O_NOFOLLOW, dir_fd=directory)
        except FileNotFoundError:
            return None
        try:
            info = _regular(fd)
            if info.st_size > LIMIT:
                raise AdmissionError("reservation exceeds the document bound")
            raw = os.read(fd, LIMIT + 1)
            if len(raw) > LIMIT:
                raise AdmissionError("reservation exceeds the document bound")

            def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
                result = {}
                for k, v in pairs:
                    if k in result:
                        raise AdmissionError("duplicate reservation field")
                    result[k] = v
                return result

            value = json.loads(
                raw,
                object_pairs_hook=unique,
                parse_constant=lambda _: (_ for _ in ()).throw(
                    AdmissionError("non-finite reservation")
                ),
            )
            if (
                not isinstance(value, dict)
                or value.get("schema") != SCHEMA
                or value.get("key") != key
                or value.get("state") not in {"reserved", "committed", "released"}
            ):
                raise AdmissionError("invalid durable reservation")
            return value
        finally:
            os.close(fd)

    def _write(self, row: dict[str, Any]) -> None:
        directory = self._authority()
        key = _hex(row["key"], HEX64, "reservation key")
        # A private random temporary name cannot replace a foreign writer's file.
        name = ".write-" + os.urandom(16).hex()
        fd = os.open(
            name, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600, dir_fd=directory
        )
        try:
            raw = canonical(row)
            if len(raw) > LIMIT:
                raise AdmissionError("reservation exceeds the document bound")
            with os.fdopen(fd, "wb", closefd=False) as stream:
                stream.write(raw)
                stream.flush()
                os.fsync(fd)
            self._authority()
            os.replace(name, key + ".json", src_dir_fd=directory, dst_dir_fd=directory)
            os.fsync(directory)
            self._authority()
        finally:
            os.close(fd)
            with contextlib.suppress(FileNotFoundError):
                os.unlink(name, dir_fd=directory)

    def reserve(
        self,
        *,
        key: str,
        owner: str,
        intent: str,
        program: str,
        source: str,
        identity: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Verify and durably protect before returning; exact replay is harmless."""
        _hex(key, HEX64, "reservation key")
        _hex(owner, HEX64, "driver owner")
        _hex(intent, HEX64, "submission intent")
        _hex(source, HEX40, "source SHA")
        if (
            not isinstance(program, str)
            or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", program) is None
        ):
            raise AdmissionError("invalid program")
        desired = validate_identity(identity, program, source)
        binding = dict(owner=owner, intent=intent, program=program, source=source, identity=desired)
        old = self._read(key)
        if old is not None:
            if old.get("binding") != binding:
                raise AdmissionError("reservation key belongs to a different immutable intent")
            if old["state"] == "released":
                raise AdmissionError("released reservation cannot be reused")
        # Recheck even a replay; neither old receipts nor old successful imports
        # are evidence that today's files match the protected identity.
        actual = validate_identity(self.verify(program, source), program, source)
        if actual != desired:
            raise AdmissionError("generation changed since its requested identity")
        row = old or dict(schema=SCHEMA, key=key, binding=binding, state="reserved", job=None)
        if old is None:
            self._write(row)
        return row

    def commit(self, key: str, *, owner: str, intent: str, job: str) -> dict[str, Any]:
        """Bind protection to one durably published JobSpec, without a gap."""
        row = self._bound(key, owner, intent)
        if not isinstance(job, str) or re.fullmatch(r"[0-9a-f]{12}", job) is None:
            raise AdmissionError("invalid durable job identity")
        if row["state"] == "released" or row["job"] not in (None, job):
            raise AdmissionError("reservation is released or belongs to another job")
        row.update(state="committed", job=job)
        self._write(row)
        return row

    def _bound(self, key: str, owner: str, intent: str) -> dict[str, Any]:
        row = self._read(key)
        if row is None or row["binding"]["owner"] != owner or row["binding"]["intent"] != intent:
            raise AdmissionError("reservation does not belong to this driver intent")
        return row

    def check(self, key: str, *, owner: str, intent: str, job: str) -> dict[str, Any]:
        """Reverify the exact committed identity immediately before dispatch."""
        row = self._bound(key, owner, intent)
        if row["state"] != "committed" or row["job"] != job:
            raise AdmissionError("dispatch lacks its committed generation reference")
        binding = row["binding"]
        actual = validate_identity(
            self.verify(binding["program"], binding["source"]),
            binding["program"],
            binding["source"],
        )
        if actual != binding["identity"]:
            raise AdmissionError("protected generation content or native identity changed")
        return row

    def release(
        self,
        key: str,
        *,
        owner: str,
        intent: str,
        job: str,
        terminal_proof: Mapping[str, Any],
        verify_terminal: Callable[[str, Mapping[str, Any]], bool],
    ) -> dict[str, Any]:
        """Release only after the authoritative provider confirms termination.

        The callback must independently confirm the driver publication and
        scheduler completion. A caller's boolean or expired timeout is no proof.
        An uncommitted/unknown reservation deliberately has no automatic release.
        """
        row = self._bound(key, owner, intent)
        if row["job"] != job or row["state"] == "reserved":
            raise AdmissionError("unknown admission outcome remains protected")
        if row["state"] == "released":
            if row.get("terminal_proof") != dict(terminal_proof):
                raise AdmissionError("release replay has a different terminal proof")
            return row
        if verify_terminal(job, terminal_proof) is not True:
            raise AdmissionError("provider could not establish actual job termination")
        row.update(state="released", terminal_proof=dict(terminal_proof))
        self._write(row)
        return row

    def protects(self, generation: str) -> list[str]:
        """Fail closed on unreadable state; caller holds the lock through cleanup."""
        directory = self._authority()
        result = []
        for name in os.listdir(directory):
            if name == "authority.lock" or name.startswith(".write-"):
                continue
            if not re.fullmatch(r"[0-9a-f]{64}\.json", name):
                raise AdmissionError("unrecognized reservation authority entry")
            row = self._read(name[:-5])
            if row is None:
                raise AdmissionError("reservation disappeared during cleanup census")
            if (
                row["state"] != "released"
                and row["binding"]["identity"]["generation"] == generation
            ):
                result.append(row["key"])
        return sorted(result)


def _provider_call(
    cfg, authority: str, op: str, fields: Mapping[str, Any], *, adapter: str | None = None
):
    """The reviewed host command authenticates a provider; paths never do."""
    from vq import runtime_participant

    response = runtime_participant.request(
        cfg.host(authority),
        "generation-" + op,
        fields=dict(fields),
        expected_adapter=adapter,
        timeout=600,
    )
    # Only the authenticated provider's exact lock-contention error is transient.
    # Accept the old exception spelling during a containing helper rollout.
    busy_errors = {"AdmissionBusy: " + _AUTHORITY_BUSY, "AdmissionError: " + _AUTHORITY_BUSY}
    if response.outcome == "refused" and tuple(response.errors) in {
        (error,) for error in busy_errors
    }:
        raise AdmissionBusy(_AUTHORITY_BUSY)
    if response.outcome != "ok" or response.receipt is None:
        raise AdmissionError(
            f"provider does not support safe managed-generation {op}: " + "; ".join(response.errors)
        )
    receipt = dict(response.receipt)
    if receipt.get("schema") != SCHEMA or receipt.get("request_sha256") != digest(dict(fields)):
        raise AdmissionError("provider admission receipt does not bind this request")
    return receipt, response.adapter_sha256


def describe(
    cfg,
    scheduler_host: str,
    program: str,
    source: str,
    *,
    native_sha256: str | None = None,
    content_sha256: str | None = None,
):
    """Resolve an explicitly requested retained generation, without reserving."""
    from vq.spec import ProgramRuntimePin
    from vq.submit import _scheduler_deployment_authority

    _hex(source, HEX40, "full managed generation SHA")
    authority = _scheduler_deployment_authority(cfg, scheduler_host, program)
    if authority is None:
        raise AdmissionError("managed generation requires a canonical deployment provider")
    fields = dict(program=program, source_sha=source)
    receipt, adapter = _provider_call(cfg, authority, "describe", fields)
    if receipt.get("cleanup_contract") != SCHEMA:
        raise AdmissionError("provider has no matching cleanup/reservation authority")
    identity = validate_identity(receipt.get("identity", {}), program, source)
    for expected, field in ((native_sha256, "native_sha256"), (content_sha256, "content_sha256")):
        if expected is not None:
            _hex(expected, HEX64, field)
            if expected != identity[field]:
                raise AdmissionError("provider generation does not match requested " + field)
    return ProgramRuntimePin(
        expected_git_sha=source,
        scheduler_host=scheduler_host,
        resolved_executable=identity["python"],
        program_kind="scheduler-runtime",
        artifact_identity=identity["python"],
        managed_generation=dict(
            schema=SCHEMA, authority=authority, adapter_sha256=adapter, identity=identity
        ),
    )


def _descriptor(spec) -> dict[str, Any] | None:
    pin = spec.program_runtime_pin
    value = getattr(pin, "managed_generation", None) if pin else None
    if value is None:
        return None
    if not isinstance(value, dict) or value.get("schema") != SCHEMA:
        raise AdmissionError("invalid managed generation binding")
    identity = validate_identity(value.get("identity", {}), spec.program, pin.expected_git_sha)
    if pin.resolved_executable != identity["python"] or pin.scheduler_host != spec.scheduler_target:
        raise AdmissionError("managed generation binding does not match the job launcher/target")
    _hex(value.get("adapter_sha256"), HEX64, "provider adapter")
    return value


def _call_bound(spec, op: str, extra: Mapping[str, Any] | None = None):
    from vq.config import load_config
    from vq.submit import _scheduler_deployment_authority

    descriptor = _descriptor(spec)
    if descriptor is None:
        return None
    cfg = load_config()
    authority = _scheduler_deployment_authority(cfg, spec.scheduler_target, spec.program)
    if authority != descriptor["authority"]:
        raise AdmissionError("canonical provider authority changed")
    reservation = descriptor.get("reservation")
    if not isinstance(reservation, dict):
        raise AdmissionError("managed generation has no durable reservation")
    fields = dict(
        program=spec.program,
        source_sha=spec.program_runtime_pin.expected_git_sha,
        key=reservation["key"],
        owner=reservation["owner"],
        intent=spec.submission_intent_digest,
        **dict(extra or {}),
    )
    original_adapter = descriptor["adapter_sha256"]
    if op == "release":
        # Only terminal reconciliation may use the authenticated current provider.
        # Admission/dispatch still require the exact original adapter. Preserve
        # the immutable JobSpec pin and bind successor evidence to that digest.
        fields["release_predecessor_adapter_sha256"] = original_adapter
    receipt, current_adapter = _provider_call(
        cfg, authority, op, fields, adapter=None if op == "release" else original_adapter
    )
    row = receipt.get("reservation")
    if not isinstance(row, dict) or row.get("key") != reservation["key"]:
        raise AdmissionError("provider returned another reservation")
    expected = dict(
        owner=reservation["owner"],
        intent=spec.submission_intent_digest,
        program=spec.program,
        source=spec.program_runtime_pin.expected_git_sha,
        identity=descriptor["identity"],
    )
    if row.get("binding") != expected:
        raise AdmissionError("provider reservation changed its immutable binding")
    if op == "release" and current_adapter != original_adapter:
        reconciliation = receipt.get("release_reconciliation")
        terminal = row.get("terminal_proof")
        if (
            row.get("state") != "released"
            or row.get("job") != spec.id
            or not isinstance(terminal, dict)
            or terminal.get("driver") != fields.get("terminal_proof")
            or terminal.get("original_adapter_sha256") != original_adapter
            or not isinstance(terminal.get("scheduler"), dict)
            or not terminal["scheduler"]
            or reconciliation
            != dict(
                schema="vq.generation-release-successor/1",
                original_adapter_sha256=original_adapter,
                current_adapter_sha256=current_adapter,
                binding_sha256=digest(expected),
                job=spec.id,
                terminal_proof_sha256=digest(terminal),
            )
        ):
            raise AdmissionError("current provider lacks exact terminal successor reconciliation")
    return row


def reserve_spec(spec, queue_dir: Path) -> None:
    """Called under the existing idempotency claim lock, before publication."""
    import socket

    descriptor = _descriptor(spec)
    if descriptor is None:
        return
    if not spec.idempotency_key_hash or not spec.submission_intent_digest:
        raise AdmissionError("managed-generation admission requires an idempotency key")
    if spec.retry_max or spec.rerun_until_file_exists or spec.refresh_before:
        raise AdmissionError("managed-generation admission requires retry 0 and no rerun/refresh")
    owner = digest(dict(host=socket.getfqdn(), queue=str(queue_dir.resolve()), uid=os.geteuid()))
    key = digest(dict(owner=owner, key=spec.idempotency_key_hash))
    descriptor["reservation"] = dict(key=key, owner=owner)
    row = _call_bound(spec, "reserve", dict(identity=descriptor["identity"]))
    if row["state"] != "reserved" or row.get("job") is not None:
        # A durable-spec replay must have been recovered by the existing Queue
        # claim scan before reaching here. Never create a second job when that
        # authority is missing or uncertain.
        raise AdmissionError(
            "reservation already belongs to a job; reconcile its original durable spec"
        )


def commit_spec(spec) -> None:
    if _descriptor(spec) is None:
        return
    row = _call_bound(spec, "commit", dict(job=spec.id))
    if row["state"] != "committed" or row.get("job") != spec.id:
        raise AdmissionError("provider did not commit this durable job reference")


def check_spec(spec) -> None:
    """Reconcile a lost commit answer, then reverify before scheduler dispatch."""
    if _descriptor(spec) is None:
        return
    commit_spec(spec)
    row = _call_bound(spec, "check", dict(job=spec.id))
    if row["state"] != "committed" or row.get("job") != spec.id:
        raise AdmissionError("provider has no exact dispatch reference")


def release_spec(spec) -> None:
    """Archive/delete callers hold a fresh durable spec lock and retain failures."""
    if _descriptor(spec) is None:
        return
    if not spec.is_terminal:
        raise AdmissionError("unknown or never-observed scheduler outcome remains protected")
    # Provider independently checks scheduler termination. A terminal Queue
    # label, unavailable scheduler, killed transport or old timeout is not proof.
    proof = dict(
        job=spec.id,
        scheduler_job_id=spec.scheduler_job_id,
        queue_state=spec.state.value,
        scheduler_state=spec.scheduler_state,
        exit_code=spec.exit_code,
        never_dispatched=(
            spec.scheduler_job_id is None
            and spec.scheduler_remote_workspace is None
            and spec.started_at is None
            and spec.scheduler_state is None
        ),
        remote_workspace=spec.scheduler_remote_workspace,
        started_at=spec.started_at,
    )
    row = _call_bound(spec, "release", dict(job=spec.id, terminal_proof=proof))
    if row["state"] != "released" or row.get("job") != spec.id:
        raise AdmissionError("provider did not release the terminal job reference")
