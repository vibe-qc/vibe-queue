"""Client relay to a fleet broker, and the broker's proxy verbs (#64, B5).

Clients reach only the broker. A client ``[hosts.<name>]`` entry with
``broker = true`` makes ``vq submit <name> ...`` relay the payload over the
existing SSH transport into the broker's held queue (:func:`submit_via_broker`
runs ``vq broker accept`` there). Everything else about a broker job is asked
of the broker by its ``b-...`` id through the ordinary remote routing:
``vq status|logs|kill|fetch <name> b-...`` run the same verb on the broker,
whose local verb recognises the id and answers from the broker record, or,
once placed, proxies to the chosen target with the broker's own credentials
(:func:`resolve_local`, :func:`target_of`).
"""

from __future__ import annotations

import contextlib
import shutil
import tarfile
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from vq import broker_store, broker_target, transport

ACCEPT_TIMEOUT_SECONDS = 300.0


class BrokerRelayError(ValueError):
    """The relay refused before sending anything."""


class BrokerRelayUncertain(RuntimeError):
    """The broker may have accepted the job; do not resubmit blindly."""


def is_broker_job_id(value: str) -> bool:
    return broker_store.BROKER_JOB_ID_PATTERN.fullmatch(value) is not None


# --------------------------------------------------------------------------
# Client side


@dataclass(frozen=True)
class RelayRequest:
    """The subset of ``vq submit`` a broker job carries (see BrokerIntent)."""

    command: tuple[str, ...]
    cpus: int
    mem_mb: int | None
    wall_time_seconds: int | None = None
    priority: int = 0
    auto_resume: bool = False
    retry: int = 0
    job_name: str | None = None
    program: str | None = None
    expected_sha: str | None = None
    tags: tuple[str, ...] = ()
    clean_workdir_on_terminal: bool = False
    rerun_until_file_exists: str | None = None
    rerun_max: int = 10
    pool: str | None = None
    idempotency_key: str | None = None


def _payload_and_command(
    *,
    input_file: str | None,
    directory: str | None,
    archive: str | None,
    command: Sequence[str] | None,
    python: str | None,
) -> tuple[Path, bool, tuple[str, ...]]:
    """(archive to upload, whether it is a temporary, command to run)."""
    try:
        payload, temporary, full = _pack(
            input_file=input_file, directory=directory, archive=archive,
            command=command, python=python,
        )
    except (OSError, tarfile.TarError) as exc:
        raise BrokerRelayError(f"could not pack the payload: {exc}") from exc
    try:
        from vq import submit

        # The same staged-entrypoint check a direct remote submit runs before
        # uploading, so an obviously broken job is refused here, not later.
        submit.validate_staged_python_entrypoint(
            command=list(full), source_archive=payload,
            interpreter_explicit=python is not None,
        )
    except (OSError, ValueError, tarfile.TarError) as exc:
        if temporary:
            with contextlib.suppress(OSError):
                payload.unlink()
        raise BrokerRelayError(str(exc)) from exc
    return payload, temporary, full


def _pack(
    *,
    input_file: str | None,
    directory: str | None,
    archive: str | None,
    command: Sequence[str] | None,
    python: str | None,
) -> tuple[Path, bool, tuple[str, ...]]:
    if input_file is not None:
        if python is None:
            # The target is chosen later; its interpreter is unknown here.
            raise BrokerRelayError(
                "a single-file broker job needs --python PATH (valid on every "
                "target) or an explicit command with --dir/-c: the broker picks "
                "the target, so the client cannot know its interpreter"
            )
        source = Path(input_file)
        if not source.is_file():
            raise BrokerRelayError(f"input file not found: {source}")
        tar = _temp_tar()
        try:
            with tarfile.open(tar, "w") as out:
                out.add(source, arcname=source.name)
        except BaseException:
            tar.unlink(missing_ok=True)
            raise
        return tar, True, (python, source.name)
    if not command:
        raise BrokerRelayError("--dir/-c broker jobs need an explicit command after --")
    full = ((python,) if python else ()) + tuple(command)
    if directory is not None:
        source = Path(directory)
        if not source.is_dir():
            raise BrokerRelayError(f"directory not found: {source}")
        tar = _temp_tar()
        try:
            with tarfile.open(tar, "w") as out:
                for child in sorted(source.iterdir()):
                    out.add(child, arcname=child.name)
        except BaseException:
            tar.unlink(missing_ok=True)  # never leave a partial temp behind
            raise
        return tar, True, full
    assert archive is not None
    path = Path(archive)
    if not path.is_file():
        raise BrokerRelayError(f"archive not found: {path}")
    return path, False, full


def _temp_tar() -> Path:
    import os
    import tempfile

    fd, name = tempfile.mkstemp(prefix="vq-broker-", suffix=".tar")
    os.close(fd)
    return Path(name)


def accept_argv(remote_payload: str, request: RelayRequest) -> list[str]:
    argv = ["broker", "accept", remote_payload, "--cpus", str(request.cpus)]
    if request.mem_mb is not None:
        argv += ["--mem-mb", str(request.mem_mb)]
    if request.wall_time_seconds is not None:
        argv += ["--wall-time-seconds", str(request.wall_time_seconds)]
    if request.priority:
        argv += ["--priority", str(request.priority)]
    if request.retry:
        argv += ["--retry", str(request.retry)]
    if request.auto_resume:
        argv.append("--auto-resume")
    for flag, value in (
        ("--job-name", request.job_name), ("--program", request.program),
        ("--expected-sha", request.expected_sha), ("--pool", request.pool),
        ("--rerun-until", request.rerun_until_file_exists),
    ):
        if value is not None:
            argv += [flag, value]
    if request.rerun_until_file_exists is not None:
        argv += ["--rerun-max", str(request.rerun_max)]
    for tag in request.tags:
        argv += ["--tag", tag]
    if request.clean_workdir_on_terminal:
        argv.append("--clean-tmp")
    if request.idempotency_key is not None:
        argv += ["--idempotency-key", request.idempotency_key]
    return [*argv, "--", *request.command]


def submit_via_broker(
    host_cfg: Any,
    request: RelayRequest,
    *,
    input_file: str | None = None,
    directory: str | None = None,
    archive: str | None = None,
    python: str | None = None,
) -> str:
    """Relay one job into the broker's held queue; return its broker job id.

    A refused accept (the broker answered with an error) leaves nothing
    accepted, so the upload is removed. A lost answer (timeout, SSH loss) may
    have been accepted: the upload is kept and :class:`BrokerRelayUncertain`
    says to check ``vq broker list`` there before resubmitting. With an
    idempotency key the job id is known in advance, and resubmitting with the
    same key is safe: it returns that job rather than a second one.
    """
    expected_id = None
    if request.idempotency_key is not None:
        try:
            expected_id = broker_store.broker_job_id_for_key(request.idempotency_key)
        except broker_store.BrokerStoreError as exc:
            raise BrokerRelayError(str(exc)) from exc
    if request.mem_mb is None:
        raise BrokerRelayError(
            "a broker job needs --mem-mb: the broker never places a job whose "
            "memory is unknown"
        )
    payload, temporary, command = _payload_and_command(
        input_file=input_file, directory=directory, archive=archive,
        command=request.command, python=python,
    )
    request = RelayRequest(**{**request.__dict__, "command": command})
    remote_payload = transport.remote_temp_tar_path()
    try:
        transport.upload_file(host_cfg, payload, remote_payload)
        try:
            proc = transport.run_remote_vq(
                host_cfg, *accept_argv(remote_payload, request),
                timeout=ACCEPT_TIMEOUT_SECONDS, owned_process_group=True,
                max_stdout_bytes=4096, max_stderr_bytes=64 * 1024,
            )
        except transport.RemoteOutcomeUnknown as exc:
            if expected_id is not None:
                raise BrokerRelayUncertain(
                    f"the broker may have accepted the job as {expected_id} "
                    f"({exc}); submitting again with the same --idempotency-key "
                    f"is safe and returns that job. The upload stays at "
                    f"{remote_payload}"
                ) from exc
            raise BrokerRelayUncertain(
                f"the broker may have accepted the job ({exc}); check "
                f"`vq broker list` on {host_cfg.ssh} before submitting again. "
                f"The upload stays at {remote_payload}"
            ) from exc
        except transport.RemoteError:
            _remove_remote(host_cfg, remote_payload)
            raise
        job_id = (proc.stdout or "").strip()
        if expected_id is not None and job_id != expected_id:
            raise BrokerRelayUncertain(
                f"the broker answered {job_id!r} for a key that names "
                f"{expected_id}; an older broker may have ignored the key. "
                f"Check `vq broker list` on {host_cfg.ssh}"
            )
        if not is_broker_job_id(job_id):
            raise BrokerRelayUncertain(
                f"the broker answered {job_id!r}, not a broker job id; check "
                f"`vq broker list` on {host_cfg.ssh} before submitting again"
            )
        _remove_remote(host_cfg, remote_payload)
        return job_id
    finally:
        if temporary:
            with contextlib.suppress(OSError):
                payload.unlink()


def _remove_remote(host_cfg: Any, remote_payload: str) -> None:
    with contextlib.suppress(transport.RemoteError, OSError):
        transport.run_remote_shell(host_cfg, "rm", "-f", remote_payload, timeout=60)


def remote_target_job_id(host_cfg: Any, broker_job_id: str) -> str:
    """Ask the broker which target job a placed broker job became."""
    import json

    try:
        proc = transport.run_remote_vq(
            host_cfg, "status", "localhost", broker_job_id, "--json",
            timeout=120, owned_process_group=True,
            max_stdout_bytes=16 * 1024 * 1024, max_stderr_bytes=64 * 1024,
        )
        view = json.loads(proc.stdout)
    except (transport.RemoteError, ValueError) as exc:
        raise BrokerRelayError(f"could not ask the broker about {broker_job_id}: {exc}") from exc
    target = view.get("target_job_id") if isinstance(view, dict) else None
    if not isinstance(target, str) or view.get("state") != broker_store.STATE_PLACED:
        raise BrokerRelayError(
            f"{broker_job_id} is {view.get('state') if isinstance(view, dict) else 'unknown'}; "
            "it has no target job to fetch yet"
        )
    from vq.spec import validate_job_id

    try:
        return validate_job_id(target)
    except ValueError as exc:
        raise BrokerRelayError(f"the broker named an invalid target job: {exc}") from exc


# --------------------------------------------------------------------------
# Broker side


def resolve_local(broker_job_id: str) -> tuple[Any, broker_store.BrokerRecord]:
    """This broker's config and record for a ``b-...`` id, or an error."""
    from vq import broker_service, config

    cfg = config.load_config()
    broker_service.require_enabled(cfg)
    with broker_store.BrokerStore.open() as store:
        return cfg, store.get(broker_job_id)


def target_of(cfg: Any, record: broker_store.BrokerRecord) -> tuple[Any, str] | None:
    """The target host config and target job id of a placed job."""
    attempt = record.current_attempt
    if record.state != broker_store.STATE_PLACED or attempt is None:
        return None
    from vq import broker_target
    host_cfg = broker_target.bound_route(cfg, attempt)
    if attempt.target_job_id is None:
        raise broker_store.BrokerStoreError(
            f"placed on {attempt.target_host}, which is no longer configured here"
        )
    return host_cfg, attempt.target_job_id


def status_payload(cfg: Any, record: broker_store.BrokerRecord) -> dict[str, Any]:
    """The broker's view, plus the target's own status once placed."""
    import json

    from vq import broker_service

    view = broker_service.record_view(record)
    target = target_of(cfg, record)
    if target is not None:
        host_cfg, target_job_id = target
        try:
            proc = broker_target.run(
                host_cfg, "status", "localhost", target_job_id, "--json",
                timeout=60, owned_process_group=True,
                max_stdout_bytes=16 * 1024 * 1024, max_stderr_bytes=64 * 1024,
            )
            view["target_status"] = json.loads(proc.stdout)
        except (transport.RemoteError, ValueError) as exc:
            view["target_status_error"] = " ".join(str(exc).split())
    return view


def proxy_text(cfg: Any, record: broker_store.BrokerRecord, *argv: str) -> str:
    """Run a read-only verb for the placed job on its target; return stdout."""
    target = target_of(cfg, record)
    if target is None:
        raise broker_store.BrokerStoreError(
            f"broker job {record.broker_job_id} is {record.state}; it has no "
            "target job yet"
        )
    host_cfg, target_job_id = target
    proc = broker_target.run(
        host_cfg, *[target_job_id if arg == "{target}" else arg for arg in argv],
        timeout=600, owned_process_group=True,
        max_stdout_bytes=64 * 1024 * 1024, max_stderr_bytes=1024 * 1024,
    )
    return proc.stdout or ""


def kill(cfg: Any, broker_job_id: str, *, reason: str | None = None,
         root: Path | None = None) -> str:
    """Cancel on the broker; once placed, kill the target job too.

    Repeatable: a target job that already ended is reported, not an error.
    ``root`` names a broker store other than the default (the gateway, #163).
    """
    return _kill(cfg, broker_job_id, reason=reason, root=root)[0]


def _kill(
    cfg: Any, broker_job_id: str, *, reason: str | None, root: Path | None = None,
) -> tuple[str, broker_store.BrokerRecord, bool]:
    """(message, record after the cancel, whether the job provably stopped).

    Stopped means it can never run, or its target job was killed or had
    already ended. A transfer in doubt is not stopped: its first send may
    have landed.
    """
    from vq import broker_service

    reason = reason or "vq kill through the broker"
    record = broker_service.cancel(cfg, broker_job_id, reason=reason, root=root)
    if record.state == broker_store.STATE_CANCELLED:
        return f"{broker_job_id}: cancelled before placement", record, True
    view = broker_service.record_view(record)
    if "operator_action" in view:
        return (
            f"{broker_job_id}: not killed. OPERATOR ACTION: {view['operator_action']}",
            record,
            False,
        )
    target = target_of(cfg, record)
    if target is None:
        # Reserved or returned: a cancelled job never begins a transfer.
        return (
            f"{broker_job_id}: cancellation recorded ({record.state}); it takes "
            "effect before any placement",
            record,
            record.state != broker_store.STATE_TRANSFERRING,
        )
    host_cfg, target_job_id = target
    proc = broker_target.run(
        host_cfg, "kill", "localhost", target_job_id, "--reason", reason, check=False,
        timeout=120, owned_process_group=True,
        max_stdout_bytes=1024 * 1024, max_stderr_bytes=1024 * 1024,
    )
    output = ((proc.stdout or "") + (proc.stderr or "")).strip()
    prefix = f"{broker_job_id} -> {record.current_attempt.target_host} {target_job_id}"
    if proc.returncode != 0 and "already in terminal state" in output:
        return f"{prefix}: {output}", record, True
    if proc.returncode != 0:
        raise broker_store.BrokerStoreError(
            f"{broker_job_id}: the kill of {target_job_id} on "
            f"{record.current_attempt.target_host} failed ({proc.returncode}): {output}"
        )
    return f"{prefix}: {output}", record, True


def kill_and_resubmit(cfg: Any, broker_job_id: str, *, reason: str | None = None) -> str:
    """``vq kill --resubmit`` for a broker job: kill it, then hold a fresh copy.

    The copy is a new broker job with the same retained payload and intent,
    placed afresh by the pass. It is made only when the old job provably
    stopped; a transfer in doubt is left for its operator action instead,
    because resubmitting it could run the work twice.
    """
    from vq import broker_service

    message, record, stopped = _kill(cfg, broker_job_id, reason=reason)
    if not stopped:
        raise broker_store.BrokerStoreError(
            f"{message}\nnot resubmitted: {broker_job_id} may still run on its "
            "target; resolve the operator action, then submit again"
        )
    with broker_store.BrokerStore.open() as store:
        payload = store.verify_payload(record)
    fresh = broker_service.accept(cfg, payload, record.intent)
    return f"{message}\nresubmitted {broker_job_id} -> {fresh.broker_job_id}"


FOLLOW_PLACEMENT_POLL_SECONDS = 2.0
"""How often ``vq logs -f`` re-reads a broker job that is not placed yet."""


def follow_logs(
    cfg: Any,
    record: broker_store.BrokerRecord,
    *argv: str,
    notify: Callable[[str], None] = lambda message: None,
    poll_seconds: float | None = None,
    sleep: Callable[[float], None] | None = None,
) -> None:
    """Follow a broker job's output: wait for placement, then stream the target.

    A job the broker still holds, or whose transfer is in doubt, has no
    output yet, so this waits (saying so once) and re-reads the record until
    it is placed. A job cancelled before placement never ran and is an
    error. Once placed, the target's own ``vq logs -f`` is streamed through
    chunk by chunk, so lines arrive as the target writes them; it ends when
    the target job does, and Ctrl-C stops the remote follow too.
    """
    import sys

    poll_seconds = FOLLOW_PLACEMENT_POLL_SECONDS if poll_seconds is None else poll_seconds
    sleep = time.sleep if sleep is None else sleep
    job_id = record.broker_job_id
    announced = False
    while record.state != broker_store.STATE_PLACED:
        if record.state == broker_store.STATE_CANCELLED:
            raise broker_store.BrokerStoreError(
                f"broker job {job_id} was cancelled before placement; it never ran"
            )
        if not announced:
            notify(f"{job_id} is {record.state}; waiting for placement")
            announced = True
        sleep(poll_seconds)
        try:
            with broker_store.BrokerStore.open() as store:
                record = store.get(job_id)
        except broker_store.BrokerStoreBusy:
            continue
    target = target_of(cfg, record)
    assert target is not None
    host_cfg, target_job_id = target
    if announced:
        notify(f"{job_id} placed on {record.current_attempt.target_host} as {target_job_id}")
    remote_argv = [target_job_id if arg == "{target}" else arg for arg in argv]
    out = sys.stdout.buffer
    with broker_target.stream(host_cfg, *remote_argv) as stream:
        while chunk := stream.stdout.read1(65536):
            out.write(chunk)
            out.flush()


def stream_workspace(cfg: Any, record: broker_store.BrokerRecord, *argv: str) -> None:
    """Pipe the target's ``tar-workspace`` stream through to our stdout."""
    import sys

    target = target_of(cfg, record)
    if target is None:
        raise FileNotFoundError(
            f"broker job {record.broker_job_id} is {record.state}; it has no "
            "results yet"
        )
    host_cfg, target_job_id = target
    with broker_target.stream(host_cfg, argv[0], target_job_id, *argv[1:]) as stream:
        shutil.copyfileobj(stream.stdout, sys.stdout.buffer, 1024 * 1024)
    sys.stdout.buffer.flush()


def forward_mark_fetched(
    cfg: Any, record: broker_store.BrokerRecord, submitted_at: str, state: str
) -> str:
    target = target_of(cfg, record)
    if target is None:
        raise FileNotFoundError(f"broker job {record.broker_job_id} has no target job")
    host_cfg, target_job_id = target
    try:
        proc = broker_target.run(
            host_cfg, "mark-fetched", target_job_id, "--submitted-at", submitted_at,
            "--state", state, timeout=60, owned_process_group=True,
            max_stdout_bytes=4096, max_stderr_bytes=64 * 1024,
        )
    except transport.RemoteCommandError as exc:
        if exc.returncode == 2 and exc.stderr.strip().splitlines()[-1:] == [
            OLD_MARK_FETCHED_ERROR
        ]:
            raise TargetLacksMarkFetched() from exc
        raise
    return (proc.stdout or "").strip()


OLD_MARK_FETCHED_ERROR = "Error: No such command 'mark-fetched'."


class TargetLacksMarkFetched(Exception):
    """The target's vq predates mark-fetched; the client tolerates that."""

