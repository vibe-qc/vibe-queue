"""Broker target authority routing, including a driver on the broker machine."""
from __future__ import annotations

import contextlib
import hashlib
import json
import subprocess
import tempfile
from collections.abc import Iterator
from typing import Any

from vq import broker_store, transport


def route(cfg: Any, target: str) -> tuple[Any, dict[str, str]]:
    host = cfg.hosts.get(target)
    if host is None:
        raise broker_store.BrokerStoreError(f"target {target} is no longer configured")
    driver = host.scheduler_driver if host.scheduler != "local" else target
    authority = cfg.hosts.get(driver)
    if authority is None or authority.scheduler != "local":
        raise broker_store.BrokerStoreError(f"target {target} has no daemon driver authority")
    binding = {"target": target, "driver": driver, "ssh": authority.ssh,
               "remote_vq": authority.remote_vq, "scheduler": host.scheduler}
    if host.scheduler != "local":
        # Pin all routing/runtime configuration, including canonical aliases.
        binding["target_config_sha256"] = hashlib.sha256(
            json.dumps(host.model_dump(mode="json"), sort_keys=True).encode()
        ).hexdigest()
    return authority, binding


def bound_route(cfg: Any, attempt: Any) -> Any:
    authority, current = route(cfg, attempt.target_host)
    recorded = attempt.snapshot.get("authority")
    if recorded is None:
        if current["scheduler"] != "local":
            raise broker_store.BrokerStoreError(
                "scheduler attempt has no recorded driver authority"
            )
    elif recorded != current:
        raise broker_store.BrokerStoreError(
            "broker target/driver authority changed; restore it before recovery"
        )
    return authority


def _local_argv(host: Any, args: tuple[str, ...]) -> list[str]:
    # Broker isolation is not the driver's state authority. Run the installed
    # helper against its operator configuration, exactly as an SSH login does.
    return ["/usr/bin/env", "-u", "VQ_CONFIG_DIR", "-u", "VQ_STATE_DIR",
            host.remote_vq, *args]


def run(host: Any, *args: str, **kwargs: Any) -> subprocess.CompletedProcess[str]:
    if host.ssh != "localhost":
        return transport.run_remote_vq(host, *args, **kwargs)
    check = kwargs.pop("check", True)
    kwargs.pop("owned_process_group", None)
    kwargs.pop("retry_transient", None)
    try:
        result = transport.run_owned_subprocess(_local_argv(host, args), **kwargs)
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise transport.RemoteOutcomeUnknown(str(exc)) from exc
    if check and result.returncode:
        raise transport.RemoteCommandError(
            f"local driver command failed: {result.stderr}", returncode=result.returncode,
            stdout=result.stdout or "", stderr=result.stderr or "",
        )
    return result


@contextlib.contextmanager
def stream(host: Any, *args: str) -> Iterator[transport.RemoteStream]:
    if host.ssh != "localhost":
        with transport.stream_remote_vq(host, *args) as remote:
            yield remote
        return
    # A file drains stderr without a second pipe deadlock. The caller enforces
    # its artifact byte budget while consuming stdout.
    with tempfile.TemporaryFile() as errors:
        process = subprocess.Popen(_local_argv(host, args), stdin=subprocess.DEVNULL,
                                   stdout=subprocess.PIPE, stderr=errors,
                                   start_new_session=True)
        assert process.stdout is not None
        result = transport.RemoteStream(process.stdout)
        try:
            yield result
            code = process.wait(timeout=60)
            errors.seek(0)
            result.stderr_text = errors.read(65536).decode(errors="replace")
            if code:
                raise transport.RemoteError(
                    f"local driver stream failed ({code}): {result.stderr_text}"
                )
        finally:
            if process.poll() is None:
                import os
                import signal
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=10)
            process.stdout.close()


def submit_scheduler(cfg: Any, request: broker_store.BrokerTransferRequest) -> str:
    """Run the normal named-target submit on its bound local driver.

    The ordinary submit pipeline authenticates the scheduler runtime and
    normalizes bare Python before committing the idempotency transaction.
    """
    from vq import submit
    host, binding = route(cfg, request.target_host)
    if request.authority != binding:
        raise broker_store.BrokerStoreError("scheduler transfer driver authority changed")
    if host.ssh != "localhost":
        raise broker_store.BrokerStoreError("scheduler broker requires a local driver")
    values = {
        "scheduler_tasks": None, "branch": None, "not_before": None,
        "depends_on": None, "depends_on_any": None, "array": 1, "chain": 1,
        "refresh_before": None, "scheduler_target": None, "qvf_force": False,
        **request.submit_kwargs,
    }
    archive = values.pop("archive")
    command = values.pop("command")
    argv = submit._build_remote_submit_argv(
        remote_tar=archive, remote_input_file=None, remote_command=command, **values,
    )
    argv[1] = request.target_host
    result = run(host, *argv, timeout=120, owned_process_group=True,
                 max_stdout_bytes=65536, max_stderr_bytes=65536)
    ids = result.stdout.strip().splitlines()
    import re
    if len(ids) != 1 or re.fullmatch(r"[0-9a-f]{12}", ids[0]) is None:
        raise transport.RemoteOutcomeUnknown("scheduler driver returned no single job identity")
    return ids[0]
