"""Per-attempt start fence for scheduler submissions (#179).

vq cannot prove that a lost qsub/sbatch was never admitted: the schedulers
expose no complete admission history and no request barrier. It can make an
attempt provably unable to start its command. Every new attempt carries a
random token, issued and kept by the daemon before the scheduler is called,
and the job script's first action is an exclusive hard-link claim on
``_vq/attempt-<token>.claim`` in the shared workspace:

* ``started <token> <scheduler-id>``, written by the job when it starts;
* ``fenced <token> <fence-id>``, written by an operator-requested fence.

Whichever link lands first decides. A job that finds a fence (or any claim
that is not its own start) exits without running and without touching the
shared markers. The operator outcome is therefore "fenced before start",
neither a scheduler rejection nor proven non-admission. Attempts submitted
before this protocol have no token and cannot be fenced.
"""
from __future__ import annotations

import contextlib
import json
import os
import re
import secrets
import shlex
import stat
import tempfile
from pathlib import Path
from typing import Any

from vq import paths
from vq.spec import utcnow_iso, validate_job_id

TOKEN_RE = re.compile(r"[0-9a-f]{32}")
TOKEN_SCHEMA = "vq.scheduler-start-token/1"
FENCE_SCHEMA = "vq.scheduler-start-fence/1"
RESULT_SCHEMA = "vq.scheduler-start-fence-result/1"
CLAIM_PREFIX = "attempt-"
CLAIM_SUFFIX = ".claim"
FENCED_STATE = "submit_fenced_unstarted"
FENCED_AFTER_TERMINAL_STATE = "submit_fenced_after_terminal"
FENCED_REASON = (
    "scheduler attempt fenced before start by operator: the command never ran "
    "under this attempt (not a scheduler rejection, not proven non-admission)"
)
_MAX_RECORD_BYTES = 256 * 1024
_MAX_REPLY_BYTES = 4096


def new_token() -> str:
    return secrets.token_hex(16)


def claim_name(token: str) -> str:
    if not TOKEN_RE.fullmatch(token):
        raise ValueError("invalid scheduler start token")
    return f"{CLAIM_PREFIX}{token}{CLAIM_SUFFIX}"


def _private_path(job_id: str, kind: str, *, owner_uid: str | None,
                  multi_user: bool, retry_count: int) -> Path:
    uid = owner_uid if owner_uid is not None else str(os.geteuid())
    if not uid.isascii() or not uid.isdecimal() or str(int(uid)) != uid:
        raise ValueError("canonical owner uid required")
    if isinstance(retry_count, bool) or not isinstance(retry_count, int) or retry_count < 0:
        raise ValueError("invalid original retry count")
    root = paths.multi_user_root() if multi_user else paths.state_root()
    return root / "scheduler-start-fence" / (
        f"{uid}.{validate_job_id(job_id)}.{retry_count}.{kind}.json"
    )


def _read(path: Path) -> dict | None:
    try:
        directory = path.parent.lstat()
    except FileNotFoundError:
        return None
    if (not stat.S_ISDIR(directory.st_mode) or directory.st_uid != os.geteuid()
            or directory.st_mode & 0o077):
        raise ValueError("unsafe daemon-owned start-fence directory")
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    except FileNotFoundError:
        return None
    with os.fdopen(fd, "rb") as stream:
        metadata = os.fstat(stream.fileno())
        if (not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.geteuid()
                or metadata.st_mode & 0o077 or metadata.st_size > _MAX_RECORD_BYTES):
            raise ValueError("unsafe daemon-owned start-fence record")
        value = json.loads(stream.read(_MAX_RECORD_BYTES + 1))
    if not isinstance(value, dict):
        raise ValueError("invalid start-fence record")
    return value


def _create(path: Path, value: dict) -> dict:
    """Publish ``value`` once; an existing record wins and is returned."""
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    existing = _read(path)
    if existing is not None:
        return existing
    serialized = json.dumps(value, sort_keys=True) + "\n"
    if len(serialized.encode()) > _MAX_RECORD_BYTES:
        raise ValueError("start-fence record exceeds its size limit")
    with tempfile.NamedTemporaryFile(mode="w", dir=path.parent, delete=False) as stream:
        temporary = Path(stream.name)
        try:
            stream.write(serialized)
            stream.flush()
            os.fsync(stream.fileno())
            # link() is the exclusive publish: a concurrent writer's record is
            # never replaced.
            with contextlib.suppress(FileExistsError):
                os.link(temporary, path)
            directory_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        finally:
            temporary.unlink(missing_ok=True)
    published = _read(path)
    if published is None:  # pragma: no cover - removed between link and read
        raise ValueError("start-fence record disappeared after publication")
    return published


def issue_token(job_id: str, *, owner_uid: str | None, multi_user: bool,
                retry_count: int, remote_workspace: str) -> str:
    """The daemon-owned token for this attempt, created before any submit.

    A repeated dispatch of the same attempt (retry count) reuses the recorded
    token, so one fence stops every script that attempt may have produced.
    """
    path = _private_path(job_id, "token", owner_uid=owner_uid,
                         multi_user=multi_user, retry_count=retry_count)
    record = _create(path, {
        "schema": TOKEN_SCHEMA, "job_id": job_id, "retry_count": retry_count,
        "remote_workspace": remote_workspace, "token": new_token(),
        "issued_at": utcnow_iso(),
    })
    token = record.get("token")
    if (record.get("schema") != TOKEN_SCHEMA or record.get("job_id") != job_id
            or record.get("retry_count") != retry_count
            or record.get("remote_workspace") != remote_workspace
            or not isinstance(token, str) or not TOKEN_RE.fullmatch(token)):
        raise ValueError("recorded start token conflicts with this attempt")
    return token


def read_token(job_id: str, *, owner_uid: str | None, multi_user: bool,
               retry_count: int) -> dict | None:
    return _read(_private_path(job_id, "token", owner_uid=owner_uid,
                               multi_user=multi_user, retry_count=retry_count))


def plan(original: dict[str, Any], token_record: dict | None) -> dict[str, Any]:
    """The exact fence an operator previews and later applies."""
    from vq import scheduler_recovery

    intent = original["intent"]
    if token_record is None:
        raise ValueError(
            "this attempt predates the start-fence protocol (no daemon-issued "
            "start token); only exact positive scheduler evidence can resolve it"
        )
    token = token_record.get("token")
    if (token_record.get("schema") != TOKEN_SCHEMA
            or token_record.get("job_id") != intent["id"]
            or token_record.get("retry_count") != intent["retry_count"]
            or token_record.get("remote_workspace") != intent["scheduler_remote_workspace"]
            or not isinstance(token, str) or not TOKEN_RE.fullmatch(token)):
        raise ValueError("recorded start token does not match the original attempt")
    fence_id = scheduler_recovery.digest({"binding": original, "token": token})[:32]
    result = {
        "schema": FENCE_SCHEMA, "decision": "fence-unstarted", "binding": original,
        "token": token, "claim": claim_name(token),
        "claim_line": f"fenced {token} {fence_id}",
    }
    return {**result, "plan_digest": scheduler_recovery.digest(result)}


def read_intent(job_id: str, *, owner_uid: str | None, multi_user: bool,
                retry_count: int) -> dict | None:
    return _read(_private_path(job_id, "fence", owner_uid=owner_uid,
                               multi_user=multi_user, retry_count=retry_count))


def persist_intent(plan_data: dict, *, owner_uid: str | None, multi_user: bool,
                   operator_uid: int | None) -> dict:
    """Caller holds the original spec lock; an identical repeat is a read."""
    intent = plan_data["binding"]["intent"]
    path = _private_path(intent["id"], "fence", owner_uid=owner_uid,
                         multi_user=multi_user, retry_count=intent["retry_count"])
    record = _create(path, {
        **plan_data, "requested_at": utcnow_iso(), "operator_uid": operator_uid,
        "operator_auth": "admin-token" if multi_user else "owner-socket",
    })
    if record.get("plan_digest") != plan_data["plan_digest"]:
        raise ValueError("a different start fence is already recorded for this attempt")
    return record


def record_result(intent_record: dict, reply: dict, *, owner_uid: str | None,
                  multi_user: bool) -> dict:
    """Retain the first observed remote outcome beside the intent."""
    intent = intent_record["binding"]["intent"]
    path = _private_path(intent["id"], "result", owner_uid=owner_uid,
                         multi_user=multi_user, retry_count=intent["retry_count"])
    return _create(path, {
        "schema": RESULT_SCHEMA, "plan_digest": intent_record["plan_digest"],
        "observed_at": utcnow_iso(), **reply,
    })


# The operator fence, run on the scheduler host in the original workspace.
# Prints exactly one of: "created", "exists:<claim bytes>", "unsafe:<why>".
FENCE_SCRIPT = r'''
workspace=$1; marker_dir=$2; claim=$3; line=$4
if [ -L "$workspace" ] || { [ -e "$workspace" ] && [ ! -d "$workspace" ]; }; then
    echo 'unsafe:workspace'; exit 0
fi
mkdir -p -- "$workspace" 2>/dev/null || { echo 'unsafe:workspace'; exit 0; }
if [ -L "$marker_dir" ] || { [ -e "$marker_dir" ] && [ ! -d "$marker_dir" ]; }; then
    echo 'unsafe:marker-dir'; exit 0
fi
[ -d "$marker_dir" ] || mkdir -- "$marker_dir" 2>/dev/null || [ -d "$marker_dir" ] || {
    echo 'unsafe:marker-dir'; exit 0; }
tmp=$marker_dir/.fence.$$
( set -C; printf '%s\n' "$line" > "$tmp" ) 2>/dev/null || { echo 'unsafe:temporary'; exit 0; }
if ln -- "$tmp" "$claim" 2>/dev/null; then
    rm -f -- "$tmp"
    sync 2>/dev/null || :
    echo created; exit 0
fi
rm -f -- "$tmp"
if [ -L "$claim" ] || [ ! -f "$claim" ]; then echo 'unsafe:claim'; exit 0; fi
printf 'exists:'; head -c 256 -- "$claim"
'''


def fence_argv(intent_record: dict) -> list[str]:
    workspace = intent_record["binding"]["intent"]["scheduler_remote_workspace"]
    marker_dir = f"{workspace}/_vq"
    return [
        "sh", "-c", FENCE_SCRIPT, "vq-scheduler-start-fence", workspace, marker_dir,
        f"{marker_dir}/{claim_name(intent_record['token'])}", intent_record["claim_line"],
    ]


def interpret(intent_record: dict, stdout: str) -> dict:
    """Classify the remote reply: fenced, started, conflict or unsafe."""
    text = stdout[:_MAX_REPLY_BYTES]
    token = intent_record["token"]
    if text.strip() == "created":
        return {"outcome": "fenced", "claim": intent_record["claim_line"], "created": True}
    if text.startswith("exists:"):
        claim = text.removeprefix("exists:").rstrip("\n")
        if claim == intent_record["claim_line"]:
            return {"outcome": "fenced", "claim": claim, "created": False}
        parts = claim.split(" ")
        if len(parts) == 3 and parts[0] == "started" and parts[1] == token:
            return {"outcome": "started", "claim": claim, "scheduler_job_id": parts[2]}
        return {"outcome": "conflict", "claim": claim}
    if text.startswith("unsafe:"):
        return {"outcome": "unsafe", "detail": text.removeprefix("unsafe:").strip()}
    return {"outcome": "unsafe", "detail": "unrecognized reply"}


def job_script_lines(*, token: str, workspace: str, marker_dir: str,
                     script_path: str, job_id_env: str) -> list[str]:
    """The job's first action: claim its own start, or exit without running.

    A Torque rerun of the same admitted job finds its own ``started`` claim
    and continues, as before this protocol. A newer attempt rewrites the
    staged script, so a late older job finds a foreign token and stops.
    """
    name = claim_name(token)
    claim = shlex.quote(f"{marker_dir}/{name}")
    tmp = shlex.quote(f"{marker_dir}/.{name}.") + "$$"
    refused = shlex.quote(f"{marker_dir}/{CLAIM_PREFIX}{token}.refused-start.") + "$$"
    started = f"started {token} ${{{job_id_env}:-unknown}}"
    return [
        f"__vq_start_token={token}",
        "__vq_refuse_start() {",
        (
            f'  ( set -C; printf "%s %s\\n" "$1" "${{{job_id_env}:-unknown}}" '
            f"> {refused} ) 2>/dev/null || :"
        ),
        '  printf "vq: start refused (%s); command not run\\n" "$1" >&2',
        "  exit 0",
        "}",
        (
            f"grep -qx {shlex.quote('__vq_start_token=' + token)} "
            f"{shlex.quote(script_path)} 2>/dev/null || "
            "__vq_refuse_start superseded-attempt"
        ),
        f'( set -C; printf "%s\\n" "{started}" > {tmp} ) 2>/dev/null || '
        "__vq_refuse_start claim-unwritable",
        f"if ln {tmp} {claim} 2>/dev/null; then rm -f {tmp}; else",
        f"  rm -f {tmp}",
        f'  [ ! -L {claim} ] && [ "$(head -c 256 {claim} 2>/dev/null)" = "{started}" ] || '
        "__vq_refuse_start fenced-or-claimed",
        "fi",
    ]
