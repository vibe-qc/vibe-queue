"""Daemon-owned local Docker execution; a client exit is never worker exit.

The opt-in payload is ``vq-docker IMAGE_DIGEST -- COMMAND [ARGS...]``. Only
single-user Linux, a local Unix engine endpoint and pre-existing immutable
images are supported. Binding files are outside the mounted workspace. Engine
errors retain the reservation; they never authorize replacement or cleanup.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import time
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path

from vq import cgroup, paths

_EXECUTOR = ThreadPoolExecutor(max_workers=4, thread_name_prefix="vq-container")

COMMAND = "vq-docker"
SCHEMA = "vq.managed-container.v1"
LABEL = "org.vibe-queue.execution"
ID = re.compile(r"[0-9a-f]{64}\Z")
IMAGE = re.compile(r"(?:sha256:|[a-zA-Z0-9][^\s]*@sha256:)[0-9a-f]{64}\Z")


class ContainerError(RuntimeError):
    """Identity or transport is unknown; preserve capacity and refuse control."""


def requested(command: list[str]) -> bool:
    return bool(command) and command[0] == COMMAND


def payload(command: list[str]) -> tuple[str, list[str]]:
    if (
        len(command) < 4
        or command[0] != COMMAND
        or command[2] != "--"
        or not IMAGE.fullmatch(command[1])
    ):
        raise ContainerError(
            "use vq-docker IMAGE@sha256:DIGEST -- COMMAND; mutable image tags are refused"
        )
    if not command[3] or any("\x00" in part for part in command):
        raise ContainerError("container payload requires valid nonempty executable argv")
    return command[1], command[3:]


def binding_path(spec_path: Path) -> Path:
    return spec_path.parent / ".containers" / spec_path.name


def present(spec_path: Path) -> bool:
    p = binding_path(spec_path)
    return p.exists() or p.is_symlink()


def fingerprint(spec) -> dict:
    return {
        "job_id": spec.id,
        "command": list(spec.command),
        "workspace": str(Path(spec.cwd).resolve()),
        "cpus": spec.cpus,
        "mem_mb": spec.mem_mb,
        "stdout_path": spec.stdout_path,
        "stderr_path": spec.stderr_path,
    }


def _read(path: Path) -> dict:
    if path.is_symlink() or path.parent.is_symlink():
        raise ContainerError("symlink container authority refused")
    try:
        b = json.loads(path.read_text())
    except (OSError, ValueError) as exc:
        raise ContainerError("container authority unavailable") from exc
    if b.get("schema") != SCHEMA or not re.fullmatch(r"[0-9a-f]{32}", b.get("token", "")):
        raise ContainerError("invalid container authority")
    if b.get("id") is not None and not ID.fullmatch(b["id"]):
        raise ContainerError("invalid immutable container identity")
    return b


def _write(path: Path, b: dict) -> None:
    if path.is_symlink() or path.parent.is_symlink():
        raise ContainerError("symlink container authority refused")
    paths.atomic_write_text(path, json.dumps(b, sort_keys=True) + "\n")
    path.chmod(0o600)


@dataclass
class Engine:
    executable: str
    endpoint: str

    @classmethod
    def local(cls) -> Engine:
        exe = shutil.which("docker")
        endpoint = os.environ.get("DOCKER_HOST", "unix:///var/run/docker.sock")
        if sys.platform != "linux" or exe is None or not endpoint.startswith("unix:///"):
            raise ContainerError(
                "managed Docker requires Linux and an accessible local Unix Docker endpoint"
            )
        return cls(str(Path(exe).resolve()), endpoint)

    def call(self, *args: str) -> str:
        # Fixed endpoint ignores mutable docker contexts; no sudo or shell.
        env = dict(os.environ)
        env.pop("DOCKER_CONTEXT", None)
        try:
            p = subprocess.run(
                [self.executable, "--host", self.endpoint, *args],
                stdin=subprocess.DEVNULL,
                capture_output=True,
                text=True,
                timeout=5,
                env=env,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise ContainerError("container operation outcome unknown") from exc
        if p.returncode != 0:
            raise ContainerError("container operation failed; identity/reservation retained")
        if len(p.stdout) > 2 * 1024 * 1024:
            raise ContainerError("container response exceeds bound")
        return p.stdout

    def logs(self, target: str) -> tuple[str, str]:
        env = dict(os.environ)
        env.pop("DOCKER_CONTEXT", None)
        try:
            p = subprocess.run(
                [
                    self.executable,
                    "--host",
                    self.endpoint,
                    "logs",
                    "--timestamps",
                    "--tail",
                    "1000",
                    target,
                ],
                stdin=subprocess.DEVNULL,
                capture_output=True,
                text=True,
                timeout=5,
                env=env,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise ContainerError("container log collection unavailable") from exc
        if p.returncode or len(p.stdout) + len(p.stderr) > 2 * 1024 * 1024:
            raise ContainerError("container log collection failed or exceeded bound")
        return p.stdout, p.stderr

    def inspect(self, target: str) -> dict:
        try:
            rows = json.loads(self.call("inspect", "--type", "container", target))
            if len(rows) != 1 or not isinstance(rows[0], dict):
                raise ValueError("one container required")
            return rows[0]
        except (ValueError, TypeError) as exc:
            raise ContainerError("invalid container observation") from exc

    def preflight(self, image: str) -> str:
        # Never pull an image or let an engine context select a remote worker.
        rows = json.loads(self.call("image", "inspect", image))
        if len(rows) != 1 or not ID.fullmatch(rows[0].get("Id", "").removeprefix("sha256:")):
            raise ContainerError("cached immutable image unavailable")
        return rows[0]["Id"]

    def create(self, b: dict) -> str:
        f = b["fingerprint"]
        args = [
            "create",
            "--pull=never",
            "--name",
            b["name"],
            "--label",
            LABEL + "=" + b["token"],
            "--network=none",
            "--restart=no",
            "--cpus",
            str(f["cpus"]),
            "--user",
            f"{os.getuid()}:{os.getgid()}",
            "--env",
            "VQ_CPUS=" + str(f["cpus"]),
            "--env",
            "VQ_JOB_ID=" + f["job_id"],
            "--workdir",
            "/workspace",
            "--mount",
            "type=bind,src=" + f["workspace"] + ",dst=/workspace",
        ]
        if f["mem_mb"] is not None:
            args += ["--memory", str(f["mem_mb"]) + "m"]
        args += [b["image"], *b["argv"]]
        return self.call(*args).strip()


def authenticate(b: dict, observed: dict) -> dict:
    cid = observed.get("Id", "")
    labels = observed.get("Config", {}).get("Labels") or {}
    if (
        not isinstance(cid, str)
        or not ID.fullmatch(cid)
        or (b["id"] is not None and cid != b["id"])
        or observed.get("Image") != b["image_id"]
        or observed.get("HostConfig", {}).get("RestartPolicy", {}).get("Name") != "no"
        or observed.get("HostConfig", {}).get("AutoRemove") is not False
        or labels.get(LABEL) != b["token"]
        or observed.get("Name", "").removeprefix("/") != b["name"]
    ):
        raise ContainerError("container identity mismatch; no control authorized")
    state = observed.get("State")
    if not isinstance(state, dict) or state.get("Status") not in {
        "created",
        "running",
        "paused",
        "restarting",
        "exited",
        "dead",
        "removing",
    }:
        raise ContainerError("container state unknown")
    return state


def prepare(spec, spec_path: Path, *, engine: Engine | None = None) -> ContainerHandle:
    image, argv = payload(spec.command)
    if spec.scheduler_target is not None or spec.program is not None or spec.gpus or spec.retry_max:
        raise ContainerError(
            "managed Docker is a local CPU payload; scheduler/program/GPU bindings "
            "and automatic retries are unsupported"
        )
    # Bind mount CSV escaping is deliberately not guessed.
    if "," in str(Path(spec.cwd).resolve()):
        raise ContainerError("managed container workspace cannot contain a comma")
    engine = engine or Engine.local()
    image_id = engine.preflight(image)
    p = binding_path(spec_path)
    if p.parent.is_symlink():
        raise ContainerError("symlink container authority directory refused")
    p.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    with paths.spec_lock(p):
        if p.exists() or p.is_symlink():
            raise ContainerError("existing execution authority must be reconciled, never replaced")
        token = uuid.uuid4().hex
        b = dict(
            schema=SCHEMA,
            token=token,
            name="vq-" + token,
            id=None,
            image=image,
            image_id=image_id,
            argv=argv,
            fingerprint=fingerprint(spec),
            executable=engine.executable,
            endpoint=engine.endpoint,
            phase="intent",
            requested_terminal=None,
            requested_reason=None,
            stop_requested_at=None,
            last_error=None,
            exit_code=None,
            created_at=time.time(),
        )
        _write(p, b)
    return ContainerHandle(p, engine=engine)


def restore(spec, spec_path: Path, *, engine: Engine | None = None) -> ContainerHandle:
    p = binding_path(spec_path)
    b = _read(p)
    if b["fingerprint"] != fingerprint(spec):
        raise ContainerError("container job binding changed; reservation must remain fenced")
    return ContainerHandle(p, engine=engine)


def pending(spec_path: Path) -> bool:
    """Conservative durable reservation/cleanup guard, including corrupt state."""
    if not present(spec_path):
        return False
    try:
        return _read(binding_path(spec_path))["phase"] != "settled"
    except (ContainerError, KeyError):
        return True


def _stop_path(path: Path) -> Path:
    return path.with_suffix(".stop")


def _apply_stop(path: Path, b: dict) -> None:
    intent = _stop_path(path)
    if not intent.exists():
        return
    if intent.is_symlink():
        raise ContainerError("symlink stop intent refused")
    try:
        stop = json.loads(intent.read_text())
    except (OSError, ValueError) as exc:
        raise ContainerError("stop intent unreadable; reservation retained") from exc
    if not isinstance(stop, dict) or stop.get("token") != b["token"]:
        raise ContainerError("stop intent identity mismatch")
    if b["requested_terminal"] is None:
        b.update(
            requested_terminal=stop["terminal"],
            requested_reason=stop["reason"],
            stop_requested_at=stop["requested_at"],
        )


def request_stop(spec_path: Path, terminal: str, reason: str) -> None:
    if terminal not in {"killed", "starved", "time_exceeded", "oom_killed"}:
        raise ContainerError("unsupported container stop disposition")
    p = binding_path(spec_path)
    # Separate durable intent: cancellation never waits on an engine RPC lock.
    intent = _stop_path(p)
    with paths.spec_lock(intent):
        b = _read(p)
        if b["phase"] == "settled" or intent.exists():
            return
        if intent.is_symlink():
            raise ContainerError("symlink stop intent refused")
        paths.atomic_write_text(
            intent,
            json.dumps(
                dict(token=b["token"], terminal=terminal, reason=reason, requested_at=time.time())
            )
            + "\n",
        )
        intent.chmod(0o600)


class ContainerHandle:
    """Local dispatcher handle whose poll returns only authenticated worker exit."""

    def __init__(self, path: Path, *, engine: Engine | None = None):
        self.path = path
        b = _read(path)
        self.engine = engine or Engine(b["executable"], b["endpoint"])
        self.pid = 0
        self.observation: dict | None = None
        self.last_error: str | None = None
        self._flight: Future | None = None

    def verify(self, spec) -> None:
        if _read(self.path)["fingerprint"] != fingerprint(spec):
            raise ContainerError("container job binding changed; reservation retained")

    def _observe(self, b: dict) -> tuple[dict, dict]:
        observed = self.engine.inspect(b["id"] or b["name"])
        state = authenticate(b, observed)
        if b["id"] is None:
            b["id"] = observed["Id"]
            b["phase"] = "bound"
            _write(self.path, b)  # full identity persisted BEFORE start
        self.observation = observed
        self.pid = int(state.get("Pid") or 0)
        b["worker_state"] = state["Status"]
        b["worker_pid"] = self.pid
        return observed, state

    def poll(self) -> int | None:
        if self._flight is None:
            self._flight = _EXECUTOR.submit(self._advance)
        if not self._flight.done():
            return None
        flight, self._flight = self._flight, None
        return flight.result()

    def _advance(self) -> int | None:
        with paths.spec_lock(self.path):
            b = _read(self.path)
            if b["phase"] == "settled":
                return b["exit_code"]
            try:
                _apply_stop(self.path, b)
                if b["phase"] == "intent" and b["requested_terminal"] is not None:
                    b.update(phase="settled", exit_code=125, never_started=True, never_created=True)
                    _write(self.path, b)
                    return 125
                if b["phase"] == "intent":
                    # Durable pre-call phase means ambiguous create NEVER repeats.
                    b["phase"] = "creating"
                    _write(self.path, b)
                    cid = self.engine.create(b)
                    if not ID.fullmatch(cid):
                        raise ContainerError("create did not return a full immutable container ID")
                    b["id"] = cid
                    b["phase"] = "bound"
                    _write(self.path, b)
                observed, state = self._observe(b)
                _apply_stop(self.path, b)  # A stop may arrive while create/inspect runs.
                if state["Status"] in {"exited", "dead"}:
                    rc = state.get("ExitCode")
                    if type(rc) is not int:
                        raise ContainerError("missing worker exit code")
                    # Retain the authenticated terminal receipt and bounded log tails
                    # before removing only this stopped container. Unknown removal
                    # outcome retains the reservation for explicit reconciliation.
                    b.update(exit_code=rc, terminal_observation=observed)
                    stdout, stderr = self.engine.logs(b["id"])
                    workspace = Path(b["fingerprint"]["workspace"])
                    (workspace / "_vq").mkdir(exist_ok=True)
                    paths.atomic_write_text(workspace / b["fingerprint"]["stdout_path"], stdout)
                    paths.atomic_write_text(workspace / b["fingerprint"]["stderr_path"], stderr)
                    paths.atomic_write_text(
                        workspace / "_vq" / "container-receipt.json",
                        json.dumps(
                            {
                                "container_id": b["id"],
                                "image": b["image"],
                                "exit_code": rc,
                                "log_tail_lines": 1000,
                                "terminal": observed,
                            }
                        )
                        + "\n",
                    )
                    # Worker termination is already proven. Removal is best effort;
                    # failure leaves a stopped, named container and an explicit receipt.
                    try:
                        self.engine.call("rm", b["id"])
                        b["removed"] = True
                    except ContainerError as exc:
                        b["cleanup_error"] = str(exc)
                        b["removed"] = False
                    b.update(phase="settled", last_error=None)
                    _write(self.path, b)
                    return rc
                if b["requested_terminal"] is not None:
                    if state["Status"] == "created":
                        # No payload ever started; authenticated removal is safe.
                        self.engine.call("rm", b["id"])
                        b.update(
                            phase="settled",
                            exit_code=125,
                            terminal_observation=observed,
                            never_started=True,
                            last_error=None,
                        )
                        _write(self.path, b)
                        return 125
                    signal = "KILL" if time.time() - b["stop_requested_at"] >= 10 else "TERM"
                    if state["Status"] == "paused":
                        self.engine.call("unpause", b["id"])
                    if b.get("last_signal") != signal:
                        self.engine.call("kill", "--signal", signal, b["id"])
                        b["last_signal"] = signal
                    # Sending a signal is not confirmation of worker termination.
                elif state["Status"] == "created":
                    self.engine.call("start", b["id"])
                b["last_error"] = None
                _write(self.path, b)
                self.last_error = None
            except ContainerError as exc:
                self.observation = None
                self.pid = 0
                self.last_error = str(exc)
                b["last_error"] = str(exc)
                _write(self.path, b)
            return None

    def usage(self) -> tuple[float | None, int | None, str]:
        b = _read(self.path)
        source = "container:" + str(b["id"])
        if self.observation is None or self.pid <= 0:
            return None, None, source
        path = cgroup.cgroup_path_for_pid(self.pid)
        # A PID may have been recycled. Never fall back to that unrelated PID.
        if (
            not path
            or not b["id"]
            or b["id"] not in Path(path).parts
            and ("docker-" + b["id"] + ".scope") not in Path(path).parts
        ):
            return None, None, source
        return cgroup.read_cpu_usage_seconds(path), cgroup.read_memory_current_mb(path), source

    def terminate(self) -> None:
        self._stop("killed", "managed container cancellation")

    def kill(self) -> None:
        self._stop("killed", "managed container cancellation escalation")

    def _stop(self, terminal: str, reason: str) -> None:
        # Same directory identity as binding_path, without consulting mutable cwd.
        request_stop(self.path.parent.parent / self.path.name, terminal, reason)

    def result(self) -> dict:
        b = _read(self.path)
        _apply_stop(self.path, b)
        return b


class FencedHandle(ContainerHandle):
    """Unreadable/conflicting authority still reserves resources after restart."""

    def __init__(self, spec_path: Path, reason: str):
        self.path = binding_path(spec_path)
        self.pid = 0
        self.observation = None
        self.last_error = reason

    def poll(self) -> None:
        return None

    def usage(self) -> tuple[None, None, str]:
        return None, None, "container:unknown"

    def terminate(self) -> None:
        return None  # Unknown identity must not authorize a control operation.

    def kill(self) -> None:
        return None


def status(spec_path: Path) -> dict | None:
    if not present(spec_path):
        return None
    try:
        path = binding_path(spec_path)
        b = _read(path)
        _apply_stop(path, b)
        return {
            k: b.get(k)
            for k in (
                "phase",
                "id",
                "image",
                "requested_terminal",
                "requested_reason",
                "last_error",
                "exit_code",
                "removed",
                "cleanup_error",
                "worker_state",
                "worker_pid",
            )
        }
    except ContainerError as exc:
        return {"phase": "outcome_unknown", "last_error": str(exc)}
