#!/usr/bin/env bash
# Provision or refresh the supported root-owned multi-user vq runtime.
#
# Run as the ordinary checkout owner. The script seals the explicit accepted
# commit as that user, installs only the small root-owned bootstrap surface,
# then delegates all /opt/vq/venv mutation to vq-multi-user-refresh. Never run
# this whole script under sudo.

set -euo pipefail

PATH=/usr/sbin:/usr/bin:/sbin:/bin
export PATH
IFS=$' \t\n'
umask 077
unset PYTHONHOME PYTHONPATH PYTHONSTARTUP VIRTUAL_ENV
unset VQ_CONFIG_DIR VQ_STATE_DIR XDG_CONFIG_HOME XDG_DATA_HOME
PYTHONNOUSERSITE=1
export PYTHONNOUSERSITE

DEPLOY_USER="$(id -un)"
DEPLOY_HOME="$(getent passwd "$DEPLOY_USER" | cut -d: -f6)"
[ -n "$DEPLOY_HOME" ] || {
    echo "ERROR: cannot resolve the current user's home" >&2
    exit 1
}
SRC="$DEPLOY_HOME/gitlab/vibeqc-queue/vibe-queue"
REPO="$(dirname "$SRC")"
REPO="$(cd "$REPO" && pwd -P)"
SRC="$REPO/vibe-queue"
VQ_USER_VENV="$SRC/.venv"
VQ_USER_BIN="$SRC/.venv/bin/vq"
VQ_USER_PY="$SRC/.venv/bin/python"
OPT_VENV="/opt/vq/venv"
ETC_DIR="/etc/vq"
STATE_DIR="/var/lib/vq"
ADMIN_GROUP="vq-admins"
UNIT="vq-daemon-multi-user.service"
HELPER_BIN="/opt/vq/bin/vq-multi-user-refresh"
LOCK_HELPER_BIN="/opt/vq/libexec/vibe-toolset-lifecycle-lock.sh"
SUDOERS_FILE="/etc/sudoers.d/vq-multi-user-refresh"
MIGRATION_RECEIPT=""
MIGRATION_ARMING=""
MIGRATION_OPERATION_LOCK=""

say() { printf '\n==> %s\n' "$*"; }
die() { printf '\nERROR: %s\n' "$*" >&2; exit 1; }

expected_sha=""
prepare_only=0
while [ "$#" -gt 0 ]; do
    case "$1" in
        --expected-sha)
            [ "$#" -ge 2 ] && [ -n "${2-}" ] \
                || die "--expected-sha requires a non-empty argument"
            case "$2" in -*) die "--expected-sha requires a value" ;; esac
            expected_sha="$2"
            shift 2
            ;;
        --prepare-only)
            prepare_only=1
            shift
            ;;
        -h|--help)
            cat <<'EOF'
Usage: deploy-multi-user.sh --expected-sha FULL_SHA [--prepare-only]

Bootstrap or refresh the multi-user /opt/vq runtime from one accepted full
40-hex commit. Passwordless source-build grants are not supported.

--prepare-only installs the sealed root-owned helper, lifecycle helper, exact
unit, and retired timer tombstones without touching config, state, daemons, or
/opt/vq/venv. Use it to upgrade the helper before the required dry-run on an
existing host.
EOF
            exit 0
            ;;
        *) die "unknown argument: $1" ;;
    esac
done

[ "$(id -u)" != "0" ] || die "run as the checkout owner, not as root"
[[ "$expected_sha" =~ ^[0-9A-Fa-f]{40}$ ]] \
    || die "--expected-sha must be a full 40-hex accepted commit"
expected_sha="${expected_sha,,}"
[ "$(uname -s)" = "Linux" ] || die "multi-user mode is Linux-only"
[ -d "$REPO/.git" ] || die "vibe-qc checkout not found at $REPO"
[ -d "$SRC/src/vq" ] || die "vibe-queue package not found at $SRC"
command -v python3 >/dev/null || die "python3 not found"
command -v systemd-run >/dev/null \
    || die "systemd-run is required to drop job privileges"
command -v git >/dev/null || die "git not found"

[ -f "$REPO/scripts/_lifecycle_lock.sh" ] \
    && [ ! -L "$REPO/scripts/_lifecycle_lock.sh" ] \
    || die "shared lifecycle-lock helper is missing or symlinked"
# shellcheck source=../../scripts/_lifecycle_lock.sh
. "$REPO/scripts/_lifecycle_lock.sh"
vibe_toolset_multi_user_migration_receipt_path \
    MIGRATION_RECEIPT "$REPO" "$VQ_USER_VENV" \
    || die "cannot derive the canonical user-runtime migration receipt"
MIGRATION_ARMING="$MIGRATION_RECEIPT.arming"
MIGRATION_OPERATION_LOCK="$REPO/.git/vq-multi-user-bootstrap.operation.lock"

git_observe() {
    /usr/bin/env -i HOME="$DEPLOY_HOME" USER="$DEPLOY_USER" LOGNAME="$DEPLOY_USER" \
        PATH=/usr/bin:/bin GIT_CONFIG_NOSYSTEM=1 GIT_CONFIG_GLOBAL=/dev/null \
        GIT_OPTIONAL_LOCKS=0 GIT_TERMINAL_PROMPT=0 GIT_NO_REPLACE_OBJECTS=1 \
        /usr/bin/git -c core.fsmonitor= -c core.hooksPath=/dev/null \
        -c core.attributesFile=/dev/null -c credential.helper= \
        -c core.pager=cat "$@"
}

[ ! -e "$REPO/.git/info/attributes" ] \
    || die "untracked .git/info/attributes could rewrite the accepted archive"
[ ! -e "$REPO/.git/info/grafts" ] \
    || die "untracked .git/info/grafts could rewrite ancestry"

head_sha="$(git_observe -C "$REPO" rev-parse --verify 'HEAD^{commit}')" \
    || die "cannot resolve checkout HEAD"
[ "$head_sha" = "$expected_sha" ] \
    || die "checkout HEAD is $head_sha, not accepted pin $expected_sha"
dirty="$(git_observe -C "$REPO" status --porcelain=v1 --untracked-files=all -- \
    vibe-queue scripts/_lifecycle_lock.sh)"
[ -z "$dirty" ] \
    || die "bootstrap sources are dirty; accepted SHA would not describe them"

single_user_runtime=0
if [ -x "$VQ_USER_BIN" ] && [ -x "$VQ_USER_PY" ]; then
    single_user_runtime=1
elif [ -e "$VQ_USER_BIN" ] || [ -L "$VQ_USER_BIN" ] \
    || [ -e "$VQ_USER_PY" ] || [ -L "$VQ_USER_PY" ]; then
    die "partial or non-executable single-user runtime at $SRC/.venv"
fi
if [ "$prepare_only" = "0" ] && [ "$single_user_runtime" != "1" ]; then
    die "bootstrap requires the checkout's existing single-user runtime at $SRC/.venv"
fi

echo "  host:       $(hostname)"
echo "  accepted:   $expected_sha"
echo "  source:     $REPO"
echo "  install to: $OPT_VENV"
printf '\nThis installs accepted root-owned multi-user deployment files'
if [ "$prepare_only" = "0" ]; then
    printf ' and activates the root daemon'
fi
printf '. Continue? [y/N] '
read -r reply
[ "$reply" = "y" ] || [ "$reply" = "Y" ] || die "aborted by operator"

# Seal the exact bootstrap files before any sudo operation. Root never reads
# Git metadata or executes the moving checkout as part of deployment.
sealed_dir="$(mktemp -d)"
admission_owned=0
admission_reason=""
admission_set_at=""
user_was_active=0
user_stopped=0
queue_fenced=0
queue_path=""
queue_mode=""
queue_fenced_mode=""
queue_dev=""
queue_ino=""
queue_fenced_dev=""
queue_fenced_ino=""
queue_stage_path=""
queue_stage_dev=""
queue_stage_ino=""
queue_created=0
migration_transaction=""
migration_phase=""
queue_existed=0
root_activated=0
config_created=0
config_created_sha=""
root_tree_sha=""
user_tree_sha=""
user_package_root=""
user_daemon_identity_json=""
observed_user_identity_json=""
migration_receipt_sha=""
user_lifecycle_locked=0
migration_operation_locked=0
migration_recovery_adopted=0
migration_preexisting=0
bootstrap_lock_archive=""
cleanup() {
    local status="$?"
    trap - EXIT
    if [ "$status" != "0" ] && [ "$prepare_only" = "0" ] \
        && [ "$migration_operation_locked" = "1" ] \
        && [ "$migration_recovery_adopted" = "1" ] \
        && { [ -e "$MIGRATION_RECEIPT" ] || [ -L "$MIGRATION_RECEIPT" ]; }; then
        if ! recover_pending_migration; then
            echo "ERROR: durable migration recovery failed; receipt retained at $MIGRATION_RECEIPT" >&2
            status=1
        fi
    fi
    if [ "$user_lifecycle_locked" = "1" ]; then
        vibe_toolset_release_lifecycle_lock || status=1
        user_lifecycle_locked=0
    fi
    if [ "$migration_operation_locked" = "1" ]; then
        exec 196>&- || status=1
        migration_operation_locked=0
    fi
    case "$bootstrap_lock_archive" in
        /var/tmp/vq-bootstrap-lock.*.tar)
            sudo rm -f -- "$bootstrap_lock_archive" 2>/dev/null || status=1
            ;;
        "") ;;
        *) status=1 ;;
    esac
    case "$sealed_dir" in /tmp/*|/var/folders/*) rm -rf -- "$sealed_dir" ;; esac
    exit "$status"
}
trap cleanup EXIT

user_python() {
    /usr/bin/env -i HOME="$DEPLOY_HOME" USER="$DEPLOY_USER" \
        LOGNAME="$DEPLOY_USER" PATH=/usr/bin:/bin PYTHONNOUSERSITE=1 \
        "$VQ_USER_PY" -I "$@"
}

prove_user_runtime_matches_accepted_checkout() {
    local output fields=()
    output="$(/usr/bin/python3 -I -S - "$REPO/vibe-queue/src/vq" \
        "$VQ_USER_VENV" "$expected_sha" "$(id -u)" \
        <<'PY_USER_RUNTIME_SOURCE'
import hashlib
import os
import pathlib
import stat
import sys

source = pathlib.Path(sys.argv[1])
venv = pathlib.Path(sys.argv[2])
expected_sha = sys.argv[3]
expected_uid = int(sys.argv[4])


def digest_tree(root, *, installed):
    root_stat = root.lstat()
    if not stat.S_ISDIR(root_stat.st_mode) or root_stat.st_uid != expected_uid:
        raise SystemExit("vq package root has unsafe ownership or type")
    files = []
    for path in sorted(root.rglob("*")):
        metadata = path.lstat()
        if metadata.st_uid != expected_uid or stat.S_IMODE(metadata.st_mode) & 0o022:
            raise SystemExit("vq package has unsafe ownership or mode")
        if stat.S_ISLNK(metadata.st_mode) or (
            not stat.S_ISREG(metadata.st_mode) and not stat.S_ISDIR(metadata.st_mode)
        ):
            raise SystemExit("vq package contains a symlink or special file")
        if (
            stat.S_ISREG(metadata.st_mode)
            and "__pycache__" not in path.parts
            and path.suffix not in {".pyc", ".pyo"}
            and path.name not in {"SOURCE-SHA", "SOURCE-TREE-SHA256"}
        ):
            files.append(path)
    if not files:
        raise SystemExit("vq package tree is empty")
    digest = hashlib.sha256()
    for path in files:
        digest.update(path.relative_to(root).as_posix().encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


expected_tree = digest_tree(source, installed=False)
candidates = []
for lib in (venv / "lib", venv / "lib64"):
    if not lib.exists():
        continue
    candidates.extend(lib.glob("python*/site-packages/vq"))
candidates = [path for path in candidates if path.is_dir() and not path.is_symlink()]
if len(candidates) != 1:
    raise SystemExit("serving venv does not contain exactly one installed vq package")
installed = candidates[0]
installed_tree = digest_tree(installed, installed=True)
if installed_tree != expected_tree:
    raise SystemExit("serving venv package tree differs from the accepted checkout")
sha_marker = (installed / "SOURCE-SHA").read_text(encoding="ascii")
tree_marker = (installed / "SOURCE-TREE-SHA256").read_text(encoding="ascii")
if sha_marker != expected_sha + "\n" or tree_marker != expected_tree + "\n":
    raise SystemExit("serving venv provenance markers do not bind accepted bytes")
print(expected_tree)
print(installed)
PY_USER_RUNTIME_SOURCE
    )" || return 1
    mapfile -t fields <<<"$output"
    [ "${#fields[@]}" = "2" ] || return 1
    [[ "${fields[0]}" =~ ^[0-9a-f]{64}$ ]] || return 1
    user_tree_sha="${fields[0]}"
    user_package_root="${fields[1]}"
}

user_unit_snapshot() {
    systemctl --user show vq-daemon.service --no-pager \
        --property=Id --property=LoadState --property=ActiveState \
        --property=SubState --property=MainPID --property=FragmentPath \
        --property=ExecStart --property=DropInPaths --property=Environment \
        --property=ExecCondition --property=ExecStartPre \
        --property=ExecStartPost --property=ExecReload --property=ExecStop \
        --property=ExecStopPost --property=EnvironmentFiles \
        --property=PassEnvironment --property=RootDirectory
}

user_manager_environment_snapshot() {
    systemctl --user show-environment
}

observe_user_daemon_identity() {
    local expect_active="$1"
    local before after environment_before environment_after ping_json=""
    prove_user_runtime_matches_accepted_checkout || return 1
    before="$(user_unit_snapshot)" || return 1
    environment_before="$(user_manager_environment_snapshot)" || return 1
    if [ "$expect_active" = "1" ]; then
        ping_json="$(user_python - <<'PY_USER_DAEMON_PING'
import json

from vq.daemon_control import local_daemon_ping

code, envelope = local_daemon_ping(2.0, multi_user=False, verbose=True)
if code != 0:
    raise SystemExit("single-user daemon did not answer its exact user socket")
print(json.dumps(envelope, sort_keys=True, separators=(",", ":")))
PY_USER_DAEMON_PING
        )" || return 1
    elif [ "$expect_active" != "0" ]; then
        return 1
    fi
    after="$(user_unit_snapshot)" || return 1
    environment_after="$(user_manager_environment_snapshot)" || return 1
    observed_user_identity_json="$(/usr/bin/python3 -I -S - \
        "$before" "$after" "$environment_before" "$environment_after" \
        "$ping_json" "$expect_active" "$(id -u)" \
        "$VQ_USER_BIN" "$VQ_USER_PY" "$expected_sha" "$user_tree_sha" \
        "$DEPLOY_HOME/.local/share/vq/daemon.sock" \
        "$DEPLOY_HOME/.config/systemd/user/vq-daemon.service" \
        "$DEPLOY_HOME" \
        <<'PY_USER_DAEMON_IDENTITY'
import json
import hashlib
import os
import pathlib
import re
import shlex
import stat
import sys

(
    before_raw,
    after_raw,
    environment_before_raw,
    environment_after_raw,
    ping_raw,
    expect_active_text,
    uid_text,
    want_vq,
    want_python,
    want_sha,
    want_tree,
    want_socket,
    want_fragment,
    want_home,
) = sys.argv[1:]
want_uid = int(uid_text)
expect_active = expect_active_text == "1"


def parse_snapshot(raw):
    result = {}
    for line in raw.splitlines():
        key, separator, value = line.partition("=")
        if not separator or key in result:
            raise SystemExit("malformed or duplicate user systemd property")
        result[key] = value
    required = {
        "Id", "LoadState", "ActiveState", "SubState", "MainPID",
        "FragmentPath", "ExecStart", "DropInPaths", "Environment",
        "ExecCondition", "ExecStartPre", "ExecStartPost", "ExecReload",
        "ExecStop", "ExecStopPost", "EnvironmentFiles", "PassEnvironment",
        "RootDirectory",
    }
    if set(result) != required:
        raise SystemExit("user systemd snapshot does not have the exact fields")
    return result


before = parse_snapshot(before_raw)
after = parse_snapshot(after_raw)
if before != after:
    raise SystemExit("single-user unit changed across identity proof")


def parse_manager_environment(raw):
    result = {}
    for line in raw.splitlines():
        words = shlex.split(line)
        if len(words) != 1:
            raise SystemExit("malformed user-manager environment")
        key, separator, value = words[0].partition("=")
        if not separator or not key or key in result:
            raise SystemExit("malformed or duplicate user-manager environment")
        result[key] = value
    return result


environment_before = parse_manager_environment(environment_before_raw)
environment_after = parse_manager_environment(environment_after_raw)
if environment_before != environment_after:
    raise SystemExit("user-manager environment changed across identity proof")
if environment_before.get("HOME") != want_home:
    raise SystemExit("user-manager HOME does not select the serving user's state")
for variable in ("VQ_CONFIG_DIR", "VQ_STATE_DIR", "XDG_CONFIG_HOME", "XDG_DATA_HOME"):
    if variable in environment_before:
        raise SystemExit(f"user-manager environment has unsupported {variable}")

properties = before
if (
    properties["Id"] != "vq-daemon.service"
    or properties["LoadState"] != "loaded"
    or properties["FragmentPath"] != want_fragment
    or properties["DropInPaths"]
):
    raise SystemExit("single-user unit is not the exact supported unit")
unsupported_properties = {
    "Environment", "ExecCondition", "ExecStartPre", "ExecStartPost",
    "ExecReload", "ExecStop", "ExecStopPost", "EnvironmentFiles",
    "PassEnvironment", "RootDirectory",
}
for property_name in unsupported_properties:
    if properties[property_name]:
        raise SystemExit(
            f"single-user unit has unsupported {property_name}="
            f"{properties[property_name]}"
        )
fragment = pathlib.Path(want_fragment)
fragment_stat = fragment.lstat()
if (
    not stat.S_ISREG(fragment_stat.st_mode)
    or fragment_stat.st_uid != want_uid
    or stat.S_IMODE(fragment_stat.st_mode) & 0o022
):
    raise SystemExit("single-user unit file has unsafe ownership or mode")
path_matches = re.findall(r"(?:^|[ {;])path=([^\s;]+)", properties["ExecStart"])
argv_matches = re.findall(r"(?:^|[ {;])argv\[\]=([^;]+)", properties["ExecStart"])
if len(path_matches) != 1 or len(argv_matches) != 1:
    raise SystemExit("single-user unit ExecStart is ambiguous")
argv = shlex.split(argv_matches[0].strip())
if (
    path_matches[0] != want_vq
    or argv != [want_vq, "daemon", "run"]
):
    raise SystemExit("single-user unit does not execute the serving venv")
try:
    main_pid = int(properties["MainPID"])
except ValueError as error:
    raise SystemExit("single-user unit MainPID is malformed") from error
if expect_active:
    if (
        properties["ActiveState"] != "active"
        or properties["SubState"] != "running"
        or main_pid <= 0
    ):
        raise SystemExit("single-user unit is not exactly active/running")
    data = json.loads(ping_raw)
    identity = data.get("process_identity")
    if not isinstance(identity, dict):
        raise SystemExit("single-user RPC omitted process identity")
    if not (
        data.get("ok") is True
        and data.get("source_sha") == want_sha
        and data.get("source_tree_sha256") == want_tree
        and data.get("multi_user") is False
        and data.get("socket_path") == want_socket
        and identity.get("status") == "ok"
        and identity.get("pid") == main_pid
        and identity.get("euid") == want_uid
        and identity.get("python_executable") == want_python
        and identity.get("argv") == argv
        and identity.get("source_sha") == want_sha
        and identity.get("source_tree_sha256") == want_tree
        and identity.get("multi_user") is False
        and identity.get("socket_path") == want_socket
        and identity.get("version") == data.get("version")
    ):
        raise SystemExit("single-user RPC is not the exact systemd MainPID")
    pid = main_pid
else:
    if (
        properties["ActiveState"] != "inactive"
        or properties["SubState"] != "dead"
        or main_pid != 0
        or ping_raw
    ):
        raise SystemExit("single-user unit is not exactly inactive/dead")
    pid = None
result = {
    "active": expect_active,
    "exec_start_argv": argv,
    "exec_start_path": path_matches[0],
    "fragment_path": want_fragment,
    "initial_main_pid": pid,
    "multi_user": False,
    "python_executable": want_python,
    "socket_path": want_socket,
    "source_sha": want_sha,
    "source_tree_sha256": want_tree,
    "user_uid": want_uid,
}
print(json.dumps(result, sort_keys=True, separators=(",", ":")))
PY_USER_DAEMON_IDENTITY
    )" || return 1
}

capture_user_daemon_identity() {
    observe_user_daemon_identity "$user_was_active" || return 1
    user_daemon_identity_json="$observed_user_identity_json"
}

prove_user_daemon_identity() {
    local current_active="$1"
    local allow_new_pid="$2"
    observe_user_daemon_identity "$current_active" || return 1
    /usr/bin/python3 -I -S - "$user_daemon_identity_json" \
        "$observed_user_identity_json" "$current_active" "$allow_new_pid" \
        <<'PY_COMPARE_USER_DAEMON'
import json
import sys

expected = json.loads(sys.argv[1])
observed = json.loads(sys.argv[2])
current_active = sys.argv[3] == "1"
allow_new_pid = sys.argv[4] == "1"
immutable = set(expected) - {"active", "initial_main_pid"}
if type(expected) is not dict or type(observed) is not dict or set(observed) != set(expected):
    raise SystemExit("single-user daemon identity schema changed")
if any(expected[key] != observed[key] for key in immutable):
    raise SystemExit("single-user daemon identity changed")
if observed["active"] is not current_active:
    raise SystemExit("single-user daemon state changed")
if current_active:
    pid = observed["initial_main_pid"]
    if type(pid) is not int or pid <= 0:
        raise SystemExit("single-user daemon PID is invalid")
    if not allow_new_pid and pid != expected["initial_main_pid"]:
        raise SystemExit("single-user daemon MainPID changed")
elif observed["initial_main_pid"] is not None:
    raise SystemExit("inactive single-user daemon unexpectedly has a PID")
PY_COMPARE_USER_DAEMON
}

acquire_migration_operation_lock() {
    local status=""
    /usr/bin/python3 -I -S - "$MIGRATION_OPERATION_LOCK" "$REPO" \
        "$(id -u)" <<'PY_PREPARE_MIGRATION_OPERATION_LOCK' || return 1
import os
import pathlib
import stat
import sys

path = pathlib.Path(sys.argv[1])
repo = pathlib.Path(sys.argv[2])
expected_uid = int(sys.argv[3])
if path != repo / ".git" / "vq-multi-user-bootstrap.operation.lock":
    raise SystemExit("wrong migration-operation lock path")
parent = path.parent
parent_stat = parent.lstat()
if (
    not stat.S_ISDIR(parent_stat.st_mode)
    or parent_stat.st_uid != expected_uid
    or stat.S_IMODE(parent_stat.st_mode) & 0o022
):
    raise SystemExit("unsafe migration-operation lock parent")
flags = os.O_RDWR | getattr(os, "O_NOFOLLOW", 0)
try:
    fd = os.open(path, flags | os.O_CREAT | os.O_EXCL, 0o600)
except FileExistsError:
    fd = os.open(path, flags)
try:
    metadata = os.fstat(fd)
    named = os.stat(path, follow_symlinks=False)
    if (
        (metadata.st_dev, metadata.st_ino) != (named.st_dev, named.st_ino)
        or not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != expected_uid
        or metadata.st_nlink != 1
        or stat.S_IMODE(metadata.st_mode) != 0o600
    ):
        raise SystemExit("unsafe migration-operation lock inode")
finally:
    os.close(fd)
PY_PREPARE_MIGRATION_OPERATION_LOCK
    if ! exec 196<>"$MIGRATION_OPERATION_LOCK"; then
        return 1
    fi
    if status="$(/usr/bin/python3 -I -S - "$MIGRATION_OPERATION_LOCK" \
        "$REPO" "$(id -u)" 196>&196 <<'PY_LOCK_MIGRATION_OPERATION'
import fcntl
import os
import pathlib
import stat
import sys

path = pathlib.Path(sys.argv[1])
repo = pathlib.Path(sys.argv[2])
expected_uid = int(sys.argv[3])
if path != repo / ".git" / "vq-multi-user-bootstrap.operation.lock":
    raise SystemExit("error:path")
opened = os.fstat(196)
named = os.stat(path, follow_symlinks=False)
parent = path.parent.lstat()
if (
    not stat.S_ISDIR(parent.st_mode)
    or parent.st_uid != expected_uid
    or stat.S_IMODE(parent.st_mode) & 0o022
    or (opened.st_dev, opened.st_ino) != (named.st_dev, named.st_ino)
    or not stat.S_ISREG(opened.st_mode)
    or opened.st_uid != expected_uid
    or opened.st_nlink != 1
    or stat.S_IMODE(opened.st_mode) != 0o600
):
    raise SystemExit("error:identity")
try:
    fcntl.flock(196, fcntl.LOCK_EX | fcntl.LOCK_NB)
except BlockingIOError:
    print("busy")
    raise SystemExit(2)
print("acquired")
PY_LOCK_MIGRATION_OPERATION
    )"; then
        :
    else
        exec 196>&-
        [ "$status" = "busy" ] \
            && echo "ERROR: another multi-user deploy/recovery transaction is active" >&2
        return 1
    fi
    if [ "$status" != "acquired" ]; then
        exec 196>&-
        return 1
    fi
    migration_operation_locked=1
}

acquire_user_lifecycle_admission() {
    local lock_python=""
    vibe_toolset_find_external_python lock_python "$VQ_USER_VENV" 3 8 \
        || return 1
    vibe_toolset_acquire_lifecycle_lock \
        "$lock_python" "$REPO" "$VQ_USER_VENV" \
        "vq-multi-user-deploy" || return 1
    user_lifecycle_locked=1
}

release_user_lifecycle_admission() {
    [ "$user_lifecycle_locked" = "1" ] || return 1
    vibe_toolset_release_lifecycle_lock || return 1
    user_lifecycle_locked=0
}

reconcile_migration_arming() {
    /usr/bin/python3 -I -S - "$MIGRATION_RECEIPT" "$MIGRATION_ARMING" \
        "$expected_sha" "$REPO" "$VQ_USER_VENV" "$(id -u)" \
        <<'PY_MIGRATION_ARMING'
import json
import os
import pathlib
import re
import stat
import sys

receipt = pathlib.Path(sys.argv[1])
arming = pathlib.Path(sys.argv[2])
expected_sha, expected_repo, expected_venv = sys.argv[3:6]
expected_uid = int(sys.argv[6])
flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)


def unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate migration arming key")
        result[key] = value
    return result


def inspect(path):
    fd = os.open(path, flags)
    try:
        metadata = os.fstat(fd)
        raw = os.read(fd, 16385)
    finally:
        os.close(fd)
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != expected_uid
        or stat.S_IMODE(metadata.st_mode) != 0o600
        or not raw
        or len(raw) > 16384
    ):
        raise SystemExit("unsafe migration arming receipt")
    data = json.loads(raw.decode("utf-8"), object_pairs_hook=unique)
    required = {
        "schema", "transaction", "phase", "expected_sha", "repo",
        "user_venv", "user_uid", "drain_path", "drain_reason",
        "drain_set_at", "user_was_active", "queue_path", "queue_existed",
        "queue_dev", "queue_ino", "queue_mode", "queue_fenced_mode",
        "queue_fenced_dev", "queue_fenced_ino",
        "queue_stage_path", "queue_stage_dev", "queue_stage_ino",
        "config_created", "config_sha256", "root_tree_sha256",
        "user_daemon_identity",
    }
    if type(data) is not dict or set(data) != required:
        raise SystemExit("migration arming receipt has the wrong schema")
    if (
        data.get("schema") != 1
        or data.get("phase") != "intent"
        or type(data.get("transaction")) is not str
        or re.fullmatch(r"[0-9a-f]{32}", data["transaction"]) is None
        or data.get("expected_sha") != expected_sha
        or data.get("repo") != expected_repo
        or data.get("user_venv") != expected_venv
        or data.get("user_uid") != expected_uid
        or receipt != pathlib.Path(expected_repo) / ".git" /
            "vq-multi-user-bootstrap.json"
    ):
        raise SystemExit("migration arming receipt is bound to another operation")
    return metadata


if not os.path.lexists(arming):
    raise SystemExit(0)
arming_stat = inspect(arming)
if os.path.lexists(receipt):
    receipt_stat = inspect(receipt)
    if (receipt_stat.st_dev, receipt_stat.st_ino) != (
        arming_stat.st_dev,
        arming_stat.st_ino,
    ):
        raise SystemExit("migration arming inode differs from canonical receipt")
else:
    if arming_stat.st_nlink != 1:
        raise SystemExit("unpublished migration arming receipt has extra links")
    os.link(arming, receipt, follow_symlinks=False)
    parent = os.open(receipt.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(parent)
    finally:
        os.close(parent)
os.unlink(arming)
parent = os.open(receipt.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
try:
    os.fsync(parent)
finally:
    os.close(parent)
PY_MIGRATION_ARMING
}

write_migration_intent() {
    local output fields=()
    output="$(user_python - "$MIGRATION_RECEIPT" "$expected_sha" "$REPO" \
        "$VQ_USER_VENV" "$user_was_active" "$user_daemon_identity_json" \
        <<'PY_MIGRATION_INTENT'
import json
import os
import pathlib
import secrets
import stat
import sys

from vq import drain, paths

receipt = pathlib.Path(sys.argv[1])
expected_sha, repo, user_venv_text, was_active_text, identity_raw = sys.argv[2:]
user_daemon_identity = json.loads(identity_raw)
if os.path.lexists(receipt):
    raise SystemExit(f"migration receipt already exists: {receipt}")
user_venv = pathlib.Path(user_venv_text)
expected_receipt = pathlib.Path(repo) / ".git" / "vq-multi-user-bootstrap.json"
if receipt != expected_receipt:
    raise SystemExit("migration receipt is not in the canonical checkout metadata path")
receipt.parent.mkdir(parents=True, exist_ok=True)
root_stat = receipt.parent.lstat()
if (
    not stat.S_ISDIR(root_stat.st_mode)
    or root_stat.st_uid != os.geteuid()
    or stat.S_IMODE(root_stat.st_mode) & 0o022
):
    raise SystemExit("unsafe single-user state root")
queue = paths.queue_dir()
try:
    queue_stat = queue.lstat()
except FileNotFoundError:
    queue_existed = False
    queue_dev = queue_ino = None
    queue_mode = 0o700
else:
    if (
        not stat.S_ISDIR(queue_stat.st_mode)
        or queue_stat.st_uid != os.geteuid()
        or stat.S_IMODE(queue_stat.st_mode) & 0o022
    ):
        raise SystemExit("unsafe single-user queue directory")
    queue_existed = True
    queue_dev = queue_stat.st_dev
    queue_ino = queue_stat.st_ino
    queue_mode = stat.S_IMODE(queue_stat.st_mode)
transaction = secrets.token_hex(16)
queue_stage = (
    None
    if queue_existed
    else queue.parent / f".{queue.name}.vq-multi-user-bootstrap-{transaction}.blocker"
)
drain_state = drain.DrainState(
    enabled=True,
    reason=f"vq-multi-user-bootstrap:{expected_sha}:{transaction}",
    reject_submits=True,
    update_mode="deny",
    full_dispatch=True,
)
payload = {
    "schema": 1,
    "transaction": transaction,
    "phase": "intent",
    "expected_sha": expected_sha,
    "repo": repo,
    "user_venv": str(user_venv),
    "user_uid": os.geteuid(),
    "drain_path": str(drain.drain_state_path(multi_user=False)),
    "drain_reason": drain_state.reason,
    "drain_set_at": drain_state.set_at,
    "user_was_active": was_active_text == "1",
    "user_daemon_identity": user_daemon_identity,
    "queue_path": str(queue),
    "queue_existed": queue_existed,
    "queue_dev": queue_dev,
    "queue_ino": queue_ino,
    "queue_mode": queue_mode,
    "queue_fenced_mode": queue_mode & ~0o222,
    "queue_fenced_dev": None,
    "queue_fenced_ino": None,
    "queue_stage_path": str(queue_stage) if queue_stage is not None else None,
    "queue_stage_dev": None,
    "queue_stage_ino": None,
    "config_created": False,
    "config_sha256": None,
    "root_tree_sha256": None,
}
data = (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode()
flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
arming = receipt.with_name(receipt.name + ".arming")
temporary = arming.with_name(
    f".{arming.name}.{os.getpid()}.{transaction}.tmp"
)
fd = os.open(temporary, flags, 0o600)
try:
    remaining = memoryview(data)
    while remaining:
        written = os.write(fd, remaining)
        if written <= 0:
            raise OSError("short migration receipt write")
        remaining = remaining[written:]
    os.fsync(fd)
finally:
    os.close(fd)
if os.path.lexists(arming):
    raise SystemExit(f"migration arming receipt already exists: {arming}")
os.rename(temporary, arming)
parent_fd = os.open(
    receipt.parent,
    os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
)
try:
    os.fsync(parent_fd)
finally:
    os.close(parent_fd)
os.link(arming, receipt, follow_symlinks=False)
parent_fd = os.open(
    receipt.parent,
    os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
)
try:
    os.fsync(parent_fd)
finally:
    os.close(parent_fd)
os.unlink(arming)
parent_fd = os.open(
    receipt.parent,
    os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
)
try:
    os.fsync(parent_fd)
finally:
    os.close(parent_fd)
print(transaction)
print(drain_state.reason)
print(drain_state.set_at)
print(queue)
print("1" if queue_existed else "0")
print(queue_dev or "")
print(queue_ino or "")
print(f"{queue_mode:o}")
print(f"{queue_mode & ~0o222:o}")
print(queue_stage or "")
PY_MIGRATION_INTENT
    )" || return 1
    mapfile -t fields <<<"$output"
    [ "${#fields[@]}" = "10" ] || return 1
    migration_transaction="${fields[0]}"
    migration_phase="intent"
    admission_reason="${fields[1]}"
    admission_set_at="${fields[2]}"
    queue_path="${fields[3]}"
    queue_existed="${fields[4]}"
    queue_dev="${fields[5]}"
    queue_ino="${fields[6]}"
    queue_mode="${fields[7]}"
    queue_fenced_mode="${fields[8]}"
    queue_stage_path="${fields[9]}"
    queue_created="$((1 - queue_existed))"
}

load_migration_receipt() {
    local output fields=()
    output="$(user_python - "$MIGRATION_RECEIPT" "$expected_sha" "$REPO" \
        "$VQ_USER_VENV" "$user_tree_sha" \
        <<'PY_MIGRATION_LOAD'
import hashlib
import json
import os
import pathlib
import re
import stat
import sys

from vq import drain, paths

receipt = pathlib.Path(sys.argv[1])
expected_sha, expected_repo, user_venv_text, expected_user_tree = sys.argv[2:]
flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
fd = os.open(receipt, flags)
try:
    metadata = os.fstat(fd)
    raw = os.read(fd, 16385)
finally:
    os.close(fd)
if (
    not stat.S_ISREG(metadata.st_mode)
    or metadata.st_uid != os.geteuid()
    or metadata.st_nlink != 1
    or stat.S_IMODE(metadata.st_mode) != 0o600
    or not raw
    or len(raw) > 16384
):
    raise SystemExit("unsafe multi-user migration receipt")

def unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate migration receipt key")
        result[key] = value
    return result

data = json.loads(raw.decode(), object_pairs_hook=unique)
keys = {
    "schema", "transaction", "phase", "expected_sha", "repo", "user_uid",
    "drain_path", "drain_reason", "drain_set_at", "user_was_active",
    "queue_path", "queue_existed", "queue_dev", "queue_ino", "queue_mode",
    "queue_fenced_mode", "queue_fenced_dev", "queue_fenced_ino",
    "queue_stage_path", "queue_stage_dev", "queue_stage_ino",
    "config_created", "config_sha256",
    "root_tree_sha256", "user_venv", "user_daemon_identity",
}
phases = {
    "intent", "drained", "queue-intent", "queue-staged", "queue-fenced",
    "daemon-stop-intent", "daemon-stopped", "config-intent", "config-ready",
    "root-intent", "root-active", "queue-restored",
}
transitions = {
    "intent": "drained",
    "drained": "queue-intent",
    "queue-intent": "queue-staged",
    "queue-staged": "queue-fenced",
    "queue-fenced": "daemon-stop-intent",
    "daemon-stop-intent": "daemon-stopped",
    "daemon-stopped": "config-intent",
    "config-intent": "config-ready",
    "config-ready": "root-intent",
    "root-intent": "root-active",
    "root-active": "queue-restored",
}
if type(data) is not dict or set(data) != keys:
    raise SystemExit("migration receipt does not have the exact schema")
if type(data["schema"]) is not int or data["schema"] != 1:
    raise SystemExit("unsupported migration receipt schema")
if type(data["transaction"]) is not str or re.fullmatch(
    r"[0-9a-f]{32}", data["transaction"]
) is None:
    raise SystemExit("invalid migration transaction")
if type(data["phase"]) is not str or data["phase"] not in phases:
    raise SystemExit("invalid migration phase")
scratch = receipt.with_name(f".{receipt.name}.{data['transaction']}.tmp")
if os.path.lexists(scratch):
    scratch_fd = os.open(scratch, flags)
    try:
        scratch_metadata = os.fstat(scratch_fd)
        scratch_raw = os.read(scratch_fd, 16385)
    finally:
        os.close(scratch_fd)
    if (
        not stat.S_ISREG(scratch_metadata.st_mode)
        or scratch_metadata.st_uid != os.geteuid()
        or scratch_metadata.st_nlink != 1
        or stat.S_IMODE(scratch_metadata.st_mode) != 0o600
        or not scratch_raw
        or len(scratch_raw) > 16384
    ):
        raise SystemExit("unsafe migration receipt scratch")
    pending = json.loads(scratch_raw.decode(), object_pairs_hook=unique)
    if type(pending) is not dict or set(pending) != keys:
        raise SystemExit("migration receipt scratch changed schema")
    immutable = keys - {
        "phase", "config_created", "config_sha256", "root_tree_sha256",
        "queue_fenced_dev", "queue_fenced_ino",
        "queue_stage_dev", "queue_stage_ino",
    }
    if (
        pending.get("schema") != 1
        or pending.get("transaction") != data["transaction"]
        or pending.get("phase") != transitions.get(data["phase"])
        or any(pending[key] != data[key] for key in immutable)
    ):
        raise SystemExit("migration receipt scratch is not the exact pending transition")
    if data["phase"] != "daemon-stopped" and (
        pending["config_created"] != data["config_created"]
        or pending["config_sha256"] != data["config_sha256"]
    ):
        raise SystemExit("migration receipt scratch changed config outside its transition")
    if data["phase"] != "root-intent" and (
        pending["root_tree_sha256"] != data["root_tree_sha256"]
    ):
        raise SystemExit("migration receipt scratch changed root tree outside its transition")
    if data["phase"] == "queue-intent":
        if data["queue_existed"]:
            if pending["queue_stage_dev"] is not None or pending["queue_stage_ino"] is not None:
                raise SystemExit("existing queue scratch unexpectedly gained a blocker stage")
        elif (
            type(pending["queue_stage_dev"]) is not int
            or pending["queue_stage_dev"] < 0
            or type(pending["queue_stage_ino"]) is not int
            or pending["queue_stage_ino"] <= 0
        ):
            raise SystemExit("migration receipt scratch has an invalid blocker stage")
    elif (
        pending["queue_stage_dev"] != data["queue_stage_dev"]
        or pending["queue_stage_ino"] != data["queue_stage_ino"]
    ):
        raise SystemExit("migration receipt scratch changed blocker stage outside its transition")
    if data["phase"] == "queue-staged":
        if (
            type(pending["queue_fenced_dev"]) is not int
            or pending["queue_fenced_dev"] < 0
            or type(pending["queue_fenced_ino"]) is not int
            or pending["queue_fenced_ino"] <= 0
            or data["queue_fenced_dev"] is not None
            or data["queue_fenced_ino"] is not None
            or (
                data["queue_existed"]
                and (
                    pending["queue_fenced_dev"], pending["queue_fenced_ino"]
                ) != (data["queue_dev"], data["queue_ino"])
            )
        ):
            raise SystemExit("migration receipt scratch has an invalid queue fence")
    elif (
        pending["queue_fenced_dev"] != data["queue_fenced_dev"]
        or pending["queue_fenced_ino"] != data["queue_fenced_ino"]
    ):
        raise SystemExit("migration receipt scratch changed queue identity outside its transition")
    os.replace(scratch, receipt)
    parent = os.open(receipt.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(parent)
    finally:
        os.close(parent)
    data = pending
    raw = scratch_raw
if (
    data["expected_sha"] != expected_sha
    or data["repo"] != expected_repo
    or data["user_uid"] != os.geteuid()
    or receipt != pathlib.Path(expected_repo) / ".git" / "vq-multi-user-bootstrap.json"
    or data["user_venv"] != user_venv_text
    or data["drain_path"] != str(drain.drain_state_path(multi_user=False))
    or data["queue_path"] != str(paths.queue_dir())
):
    raise SystemExit("migration receipt is bound to another operation")
for key in ("expected_sha", "repo", "user_venv", "drain_path", "drain_reason", "drain_set_at", "queue_path"):
    if type(data[key]) is not str or not data[key] or any(
        character in data[key] for character in "\x00\r\n"
    ):
        raise SystemExit(f"invalid migration field {key}")
if re.fullmatch(r"[0-9a-f]{40}", data["expected_sha"]) is None:
    raise SystemExit("invalid migration SHA")
for key in ("user_was_active", "queue_existed", "config_created"):
    if type(data[key]) is not bool:
        raise SystemExit(f"migration field {key} is not boolean")
identity = data["user_daemon_identity"]
identity_keys = {
    "active", "exec_start_argv", "exec_start_path", "fragment_path",
    "initial_main_pid", "multi_user", "python_executable", "socket_path",
    "source_sha", "source_tree_sha256", "user_uid",
}
if type(identity) is not dict or set(identity) != identity_keys:
    raise SystemExit("migration user-daemon identity has the wrong schema")
if (
    identity["active"] is not data["user_was_active"]
    or identity["multi_user"] is not False
    or identity["user_uid"] != data["user_uid"]
    or identity["python_executable"] != str(pathlib.Path(user_venv_text) / "bin" / "python")
    or identity["exec_start_path"] != str(pathlib.Path(user_venv_text) / "bin" / "vq")
    or type(identity["exec_start_argv"]) is not list
    or identity["exec_start_argv"] != [identity["exec_start_path"], "daemon", "run"]
    or identity["source_sha"] != expected_sha
    or identity["source_tree_sha256"] != expected_user_tree
    or identity["fragment_path"] != str(
        pathlib.Path.home() / ".config" / "systemd" / "user" / "vq-daemon.service"
    )
    or identity["socket_path"] != str(
        pathlib.Path.home() / ".local" / "share" / "vq" / "daemon.sock"
    )
):
    raise SystemExit("migration user-daemon identity is not bound to the serving venv")
if data["user_was_active"]:
    if type(identity["initial_main_pid"]) is not int or identity["initial_main_pid"] <= 0:
        raise SystemExit("active migration identity is missing MainPID")
elif identity["initial_main_pid"] is not None:
    raise SystemExit("inactive migration identity unexpectedly has MainPID")
if data["queue_existed"]:
    if type(data["queue_dev"]) is not int or data["queue_dev"] < 0:
        raise SystemExit("invalid queue device")
    if type(data["queue_ino"]) is not int or data["queue_ino"] <= 0:
        raise SystemExit("invalid queue inode")
elif data["queue_dev"] is not None or data["queue_ino"] is not None:
    raise SystemExit("absent queue unexpectedly has an inode")
stage_bound_phases = {
    "queue-staged", "queue-fenced", "daemon-stop-intent", "daemon-stopped",
    "config-intent", "config-ready", "root-intent", "root-active",
    "queue-restored",
}
if data["queue_existed"]:
    if any(
        data[key] is not None
        for key in ("queue_stage_path", "queue_stage_dev", "queue_stage_ino")
    ):
        raise SystemExit("existing queue unexpectedly has a blocker stage")
else:
    queue = pathlib.Path(data["queue_path"])
    expected_stage = queue.parent / (
        f".{queue.name}.vq-multi-user-bootstrap-{data['transaction']}.blocker"
    )
    if data["queue_stage_path"] != str(expected_stage):
        raise SystemExit("absent queue blocker stage path changed")
    if data["phase"] in stage_bound_phases:
        if (
            type(data["queue_stage_dev"]) is not int
            or data["queue_stage_dev"] < 0
            or type(data["queue_stage_ino"]) is not int
            or data["queue_stage_ino"] <= 0
        ):
            raise SystemExit("absent queue blocker stage is missing its inode")
    elif data["queue_stage_dev"] is not None or data["queue_stage_ino"] is not None:
        raise SystemExit("absent queue blocker was bound before its stage phase")
fenced_phases = {
    "queue-fenced", "daemon-stop-intent", "daemon-stopped", "config-intent",
    "config-ready", "root-intent", "root-active", "queue-restored",
}
if data["phase"] in fenced_phases:
    if (
        type(data["queue_fenced_dev"]) is not int
        or data["queue_fenced_dev"] < 0
        or type(data["queue_fenced_ino"]) is not int
        or data["queue_fenced_ino"] <= 0
    ):
        raise SystemExit("fenced queue is missing its exact inode")
    if data["queue_existed"] and (
        data["queue_fenced_dev"], data["queue_fenced_ino"]
    ) != (data["queue_dev"], data["queue_ino"]):
        raise SystemExit("existing queue changed inode while fenced")
elif data["queue_fenced_dev"] is not None or data["queue_fenced_ino"] is not None:
    raise SystemExit("pre-fence phase unexpectedly binds a fenced queue inode")
if (
    type(data["queue_mode"]) is not int
    or data["queue_mode"] < 0
    or data["queue_mode"] > 0o777
    or data["queue_mode"] & 0o022
    or type(data["queue_fenced_mode"]) is not int
    or data["queue_fenced_mode"] != data["queue_mode"] & ~0o222
):
    raise SystemExit("invalid queue mode binding")
if data["config_created"]:
    if type(data["config_sha256"]) is not str or re.fullmatch(
        r"[0-9a-f]{64}", data["config_sha256"]
    ) is None:
        raise SystemExit("created config is missing its digest")
elif data["config_sha256"] is not None:
    raise SystemExit("unchanged config unexpectedly has a digest")
if data["phase"] in {"root-active", "queue-restored"}:
    if type(data["root_tree_sha256"]) is not str or re.fullmatch(
        r"[0-9a-f]{64}", data["root_tree_sha256"]
    ) is None:
        raise SystemExit("active root phase is missing its accepted tree")
elif data["root_tree_sha256"] is not None:
    raise SystemExit("pre-activation phase unexpectedly names a root tree")
print(data["transaction"])
print(data["phase"])
print(data["drain_reason"])
print(data["drain_set_at"])
print("1" if data["user_was_active"] else "0")
print(data["queue_path"])
print("1" if data["queue_existed"] else "0")
print(data["queue_dev"] if data["queue_dev"] is not None else "")
print(data["queue_ino"] if data["queue_ino"] is not None else "")
print(data["queue_fenced_dev"] if data["queue_fenced_dev"] is not None else "")
print(data["queue_fenced_ino"] if data["queue_fenced_ino"] is not None else "")
print(data["queue_stage_path"] or "")
print(data["queue_stage_dev"] if data["queue_stage_dev"] is not None else "")
print(data["queue_stage_ino"] if data["queue_stage_ino"] is not None else "")
print(f"{data['queue_mode']:o}")
print(f"{data['queue_fenced_mode']:o}")
print("1" if data["config_created"] else "0")
print(data["config_sha256"] or "")
print(data["root_tree_sha256"] or "")
print(json.dumps(identity, sort_keys=True, separators=(",", ":")))
print(hashlib.sha256(raw).hexdigest())
PY_MIGRATION_LOAD
    )" || return 1
    mapfile -t fields <<<"$output"
    [ "${#fields[@]}" = "21" ] || return 1
    migration_transaction="${fields[0]}"
    migration_phase="${fields[1]}"
    admission_reason="${fields[2]}"
    admission_set_at="${fields[3]}"
    user_was_active="${fields[4]}"
    queue_path="${fields[5]}"
    queue_existed="${fields[6]}"
    queue_dev="${fields[7]}"
    queue_ino="${fields[8]}"
    queue_fenced_dev="${fields[9]}"
    queue_fenced_ino="${fields[10]}"
    queue_stage_path="${fields[11]}"
    queue_stage_dev="${fields[12]}"
    queue_stage_ino="${fields[13]}"
    queue_mode="${fields[14]}"
    queue_fenced_mode="${fields[15]}"
    queue_created="$((1 - queue_existed))"
    config_created="${fields[16]}"
    config_created_sha="${fields[17]}"
    root_tree_sha="${fields[18]}"
    user_daemon_identity_json="${fields[19]}"
    migration_receipt_sha="${fields[20]}"
}

update_migration_receipt() {
    local expected_phase="$1"
    local next_phase="$2"
    local config_created_arg="${3:-$config_created}"
    local config_sha_arg="${4:-$config_created_sha}"
    local root_tree_arg="${5:-$root_tree_sha}"
    local queue_fenced_dev_arg="${6:-$queue_fenced_dev}"
    local queue_fenced_ino_arg="${7:-$queue_fenced_ino}"
    local queue_stage_dev_arg="${8:-$queue_stage_dev}"
    local queue_stage_ino_arg="${9:-$queue_stage_ino}"
    user_python - "$MIGRATION_RECEIPT" "$migration_transaction" \
        "$expected_phase" "$next_phase" "$config_created_arg" \
        "$config_sha_arg" "$root_tree_arg" "$queue_fenced_dev_arg" \
        "$queue_fenced_ino_arg" "$queue_stage_dev_arg" \
        "$queue_stage_ino_arg" <<'PY_MIGRATION_UPDATE'
import json
import os
import pathlib
import re
import stat
import sys

path = pathlib.Path(sys.argv[1])
(
    transaction,
    expected_phase,
    next_phase,
    created_text,
    config_sha,
    root_tree,
    queue_fenced_dev_text,
    queue_fenced_ino_text,
    queue_stage_dev_text,
    queue_stage_ino_text,
) = sys.argv[2:]
transitions = {
    "intent": "drained",
    "drained": "queue-intent",
    "queue-intent": "queue-staged",
    "queue-staged": "queue-fenced",
    "queue-fenced": "daemon-stop-intent",
    "daemon-stop-intent": "daemon-stopped",
    "daemon-stopped": "config-intent",
    "config-intent": "config-ready",
    "config-ready": "root-intent",
    "root-intent": "root-active",
    "root-active": "queue-restored",
}
def unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate migration receipt key")
        result[key] = value
    return result

keys = {
    "schema", "transaction", "phase", "expected_sha", "repo", "user_uid",
    "drain_path", "drain_reason", "drain_set_at", "user_was_active",
    "queue_path", "queue_existed", "queue_dev", "queue_ino", "queue_mode",
    "queue_fenced_mode", "queue_fenced_dev", "queue_fenced_ino",
    "queue_stage_path", "queue_stage_dev", "queue_stage_ino",
    "config_created", "config_sha256",
    "root_tree_sha256", "user_venv", "user_daemon_identity",
}
phases = set(transitions) | set(transitions.values())

def read_exact(candidate):
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(candidate, flags)
    try:
        metadata = os.fstat(fd)
        raw = os.read(fd, 16385)
    finally:
        os.close(fd)
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or metadata.st_nlink != 1
        or stat.S_IMODE(metadata.st_mode) != 0o600
        or not raw
        or len(raw) > 16384
    ):
        raise SystemExit("unsafe migration receipt")
    data = json.loads(raw.decode(), object_pairs_hook=unique)
    if type(data) is not dict or set(data) != keys:
        raise SystemExit("migration receipt changed schema before update")
    if (
        type(data["schema"]) is not int
        or data["schema"] != 1
        or type(data["transaction"]) is not str
        or re.fullmatch(r"[0-9a-f]{32}", data["transaction"]) is None
        or data["transaction"] != transaction
        or type(data["phase"]) is not str
        or data["phase"] not in phases
        or type(data["user_uid"]) is not int
        or data["user_uid"] != os.geteuid()
    ):
        raise SystemExit("migration receipt changed before update")
    for key in ("user_was_active", "queue_existed", "config_created"):
        if type(data[key]) is not bool:
            raise SystemExit("migration receipt boolean changed before update")
    if type(data["user_daemon_identity"]) is not dict:
        raise SystemExit("migration receipt user identity changed before update")
    if (
        type(data["queue_mode"]) is not int
        or data["queue_mode"] < 0
        or data["queue_mode"] > 0o777
        or data["queue_mode"] & 0o022
        or type(data["queue_fenced_mode"]) is not int
        or data["queue_fenced_mode"] != data["queue_mode"] & ~0o222
    ):
        raise SystemExit("migration receipt queue binding changed before update")
    if data["queue_existed"]:
        if (
            type(data["queue_dev"]) is not int
            or data["queue_dev"] < 0
            or type(data["queue_ino"]) is not int
            or data["queue_ino"] <= 0
        ):
            raise SystemExit("migration receipt inode binding changed before update")
    elif data["queue_dev"] is not None or data["queue_ino"] is not None:
        raise SystemExit("migration receipt absent queue gained an inode")
    stage_bound_phases = {
        "queue-staged", "queue-fenced", "daemon-stop-intent", "daemon-stopped",
        "config-intent", "config-ready", "root-intent", "root-active",
        "queue-restored",
    }
    if data["queue_existed"]:
        if any(
            data[key] is not None
            for key in ("queue_stage_path", "queue_stage_dev", "queue_stage_ino")
        ):
            raise SystemExit("existing queue unexpectedly has a blocker stage")
    else:
        queue = pathlib.Path(data["queue_path"])
        expected_stage = queue.parent / (
            f".{queue.name}.vq-multi-user-bootstrap-{transaction}.blocker"
        )
        if data["queue_stage_path"] != str(expected_stage):
            raise SystemExit("absent queue blocker stage path changed")
        if data["phase"] in stage_bound_phases:
            if (
                type(data["queue_stage_dev"]) is not int
                or data["queue_stage_dev"] < 0
                or type(data["queue_stage_ino"]) is not int
                or data["queue_stage_ino"] <= 0
            ):
                raise SystemExit("absent queue blocker stage inode changed")
        elif data["queue_stage_dev"] is not None or data["queue_stage_ino"] is not None:
            raise SystemExit("absent queue blocker was bound too early")
    fenced_phases = {
        "queue-fenced", "daemon-stop-intent", "daemon-stopped", "config-intent",
        "config-ready", "root-intent", "root-active", "queue-restored",
    }
    if data["phase"] in fenced_phases:
        if (
            type(data["queue_fenced_dev"]) is not int
            or data["queue_fenced_dev"] < 0
            or type(data["queue_fenced_ino"]) is not int
            or data["queue_fenced_ino"] <= 0
        ):
            raise SystemExit("migration receipt fenced inode changed before update")
        if data["queue_existed"] and (
            data["queue_fenced_dev"], data["queue_fenced_ino"]
        ) != (data["queue_dev"], data["queue_ino"]):
            raise SystemExit("migration receipt existing queue changed inode")
    elif data["queue_fenced_dev"] is not None or data["queue_fenced_ino"] is not None:
        raise SystemExit("migration receipt gained a fenced inode too early")
    if data["config_created"]:
        if type(data["config_sha256"]) is not str or re.fullmatch(
            r"[0-9a-f]{64}", data["config_sha256"]
        ) is None:
            raise SystemExit("migration receipt config binding changed before update")
    elif data["config_sha256"] is not None:
        raise SystemExit("migration receipt config binding changed before update")
    if data["phase"] in {"root-active", "queue-restored"}:
        if type(data["root_tree_sha256"]) is not str or re.fullmatch(
            r"[0-9a-f]{64}", data["root_tree_sha256"]
        ) is None:
            raise SystemExit("migration receipt root tree changed before update")
    elif data["root_tree_sha256"] is not None:
        raise SystemExit("migration receipt root tree changed before update")
    return data

if transitions.get(expected_phase) != next_phase:
    raise SystemExit("invalid migration receipt transition")
data = read_exact(path)
if data["phase"] != expected_phase:
    raise SystemExit("migration receipt changed before update")
desired = dict(data)
desired["phase"] = next_phase
if expected_phase == "queue-intent":
    if data["queue_existed"]:
        if queue_stage_dev_text or queue_stage_ino_text:
            raise SystemExit("existing queue unexpectedly supplied a blocker stage")
    else:
        try:
            queue_stage_dev = int(queue_stage_dev_text)
            queue_stage_ino = int(queue_stage_ino_text)
        except ValueError as error:
            raise SystemExit("queue blocker stage is missing its exact inode") from error
        if queue_stage_dev < 0 or queue_stage_ino <= 0:
            raise SystemExit("queue blocker stage has an invalid inode")
        desired["queue_stage_dev"] = queue_stage_dev
        desired["queue_stage_ino"] = queue_stage_ino
if expected_phase == "queue-staged":
    try:
        queue_fenced_dev = int(queue_fenced_dev_text)
        queue_fenced_ino = int(queue_fenced_ino_text)
    except ValueError as error:
        raise SystemExit("queue fence is missing its exact inode") from error
    if queue_fenced_dev < 0 or queue_fenced_ino <= 0:
        raise SystemExit("queue fence has an invalid inode")
    if data["queue_existed"] and (
        queue_fenced_dev, queue_fenced_ino
    ) != (data["queue_dev"], data["queue_ino"]):
        raise SystemExit("existing queue changed inode while fenced")
    if not data["queue_existed"] and (
        queue_fenced_dev, queue_fenced_ino
    ) != (data["queue_stage_dev"], data["queue_stage_ino"]):
        raise SystemExit("published queue blocker changed its staged inode")
    desired["queue_fenced_dev"] = queue_fenced_dev
    desired["queue_fenced_ino"] = queue_fenced_ino
if expected_phase == "daemon-stopped":
    if created_text not in {"0", "1"}:
        raise SystemExit("invalid config-created flag")
    if created_text == "1" and re.fullmatch(r"[0-9a-f]{64}", config_sha) is None:
        raise SystemExit("invalid config digest")
    if created_text == "0" and config_sha:
        raise SystemExit("unchanged config unexpectedly supplied a digest")
    desired["config_created"] = created_text == "1"
    desired["config_sha256"] = config_sha or None
if expected_phase == "root-intent":
    if re.fullmatch(r"[0-9a-f]{64}", root_tree) is None:
        raise SystemExit("root activation is missing its accepted tree")
    desired["root_tree_sha256"] = root_tree
encoded = (json.dumps(desired, sort_keys=True, separators=(",", ":")) + "\n").encode()
tmp = path.with_name(f".{path.name}.{transaction}.tmp")
if os.path.lexists(tmp):
    pending = read_exact(tmp)
    if pending != desired:
        raise SystemExit("migration receipt scratch does not match the exact transition")
    os.replace(tmp, path)
    parent = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(parent)
    finally:
        os.close(parent)
    raise SystemExit(0)
out = os.open(
    tmp,
    os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
    0o600,
)
try:
    remaining = memoryview(encoded)
    while remaining:
        written = os.write(out, remaining)
        if written <= 0:
            raise OSError("short migration receipt update")
        remaining = remaining[written:]
    os.fsync(out)
finally:
    os.close(out)
parent = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
try:
    os.fsync(parent)
finally:
    os.close(parent)
os.replace(tmp, path)
parent = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
try:
    os.fsync(parent)
finally:
    os.close(parent)
PY_MIGRATION_UPDATE
    migration_phase="$next_phase"
}

clear_migration_receipt() {
    load_migration_receipt || return 1
    user_python - "$MIGRATION_RECEIPT" "$migration_transaction" \
        "$migration_phase" "$migration_receipt_sha" "$expected_sha" \
        "$REPO" "$VQ_USER_VENV" <<'PY_MIGRATION_CLEAR'
import hashlib
import json
import os
import pathlib
import stat
import sys

path = pathlib.Path(sys.argv[1])
transaction, expected_phase, expected_digest, expected_sha, repo, user_venv = (
    sys.argv[2:]
)
fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
try:
    metadata = os.fstat(fd)
    raw = os.read(fd, 16385)
finally:
    os.close(fd)

def unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate migration receipt key")
        result[key] = value
    return result

data = json.loads(raw.decode(), object_pairs_hook=unique)
keys = {
    "schema", "transaction", "phase", "expected_sha", "repo", "user_uid",
    "user_venv", "drain_path", "drain_reason", "drain_set_at",
    "user_was_active", "user_daemon_identity", "queue_path",
    "queue_existed", "queue_dev", "queue_ino", "queue_mode",
    "queue_fenced_mode", "queue_fenced_dev", "queue_fenced_ino",
    "queue_stage_path", "queue_stage_dev", "queue_stage_ino",
    "config_created", "config_sha256",
    "root_tree_sha256",
}
if (
    not stat.S_ISREG(metadata.st_mode)
    or metadata.st_uid != os.geteuid()
    or metadata.st_nlink != 1
    or stat.S_IMODE(metadata.st_mode) != 0o600
    or not raw
    or len(raw) > 16384
    or type(data) is not dict
    or set(data) != keys
    or data.get("schema") != 1
    or data.get("transaction") != transaction
    or data.get("phase") != expected_phase
    or data.get("expected_sha") != expected_sha
    or data.get("repo") != repo
    or data.get("user_venv") != user_venv
    or data.get("user_uid") != os.geteuid()
    or hashlib.sha256(raw).hexdigest() != expected_digest
):
    raise SystemExit("migration receipt changed before clear")
scratch = path.with_name(f".{path.name}.{transaction}.tmp")
if os.path.lexists(scratch):
    raise SystemExit("migration receipt scratch remains before clear")
path.unlink()
parent = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
try:
    os.fsync(parent)
finally:
    os.close(parent)
PY_MIGRATION_CLEAR
}

acquire_user_admission_fence() {
    local output fields=()
    output="$(user_python - "$admission_reason" "$admission_set_at" \
        <<'PY_ADMISSION_ACQUIRE'
import os
import stat
import sys

from vq import drain, paths

reason, set_at = sys.argv[1:]
path = drain.drain_state_path(multi_user=False)
with drain._legacy_drain_state_lock(path):
    if os.path.lexists(path):
        raise SystemExit(
            f"single-user drain already exists at {path}; refusing to replace it"
        )
    state = drain.DrainState(
        enabled=True,
        reason=reason,
        set_at=set_at,
        reject_submits=True,
        update_mode="deny",
        full_dispatch=True,
    )
    paths.atomic_write_text(path, state.model_dump_json(indent=2))
    persisted = drain.read_drain_state(via_rpc=False, multi_user=False)
    if (
        persisted is None
        or persisted.reason != reason
        or persisted.set_at != state.set_at
        or persisted.reject_submits is not True
        or persisted.update_mode != "deny"
        or not persisted.is_full_drain
    ):
        raise SystemExit("single-user admission drain readback failed")
    metadata = path.lstat()
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or metadata.st_nlink != 1
        or stat.S_IMODE(metadata.st_mode) & 0o022
    ):
        raise SystemExit("single-user admission drain has unsafe metadata")
print(reason)
print(set_at)
PY_ADMISSION_ACQUIRE
    )" || return 1
    mapfile -t fields <<<"$output"
    [ "${#fields[@]}" = "2" ] || return 1
    admission_reason="${fields[0]}"
    admission_set_at="${fields[1]}"
    [ -n "$admission_reason" ] && [ -n "$admission_set_at" ] || return 1
    admission_owned=1
}

prove_user_admission_fence() {
    user_python - "$admission_reason" "$admission_set_at" \
        "$user_was_active" <<'PY_ADMISSION_PROOF'
import sys

from vq import drain, rpc

reason, set_at, daemon_active = sys.argv[1:]

def exact(state):
    return (
        state is not None
        and state.reason == reason
        and state.set_at == set_at
        and state.reject_submits is True
        and state.update_mode == "deny"
        and state.is_full_drain
    )

if not exact(drain.read_drain_state(via_rpc=False, multi_user=False)):
    raise SystemExit("single-user deny drain changed before proof")
if daemon_active == "1":
    payload = rpc.call(
        "get_drain_state",
        multi_user=False,
        socket_override=rpc.user_socket_path(),
    )
    state = drain.DrainState.model_validate(payload) if payload is not None else None
    if not exact(state):
        raise SystemExit("single-user daemon did not observe the exact deny drain")
PY_ADMISSION_PROOF
}

release_user_admission_fence() {
    local allow_missing="${1:-0}"
    user_python - "$admission_reason" "$admission_set_at" "$allow_missing" \
        <<'PY_ADMISSION_RELEASE'
import sys

from vq import drain

existing = drain.read_drain_state(via_rpc=False, multi_user=False)
if existing is None and sys.argv[3] in {"1", "2"}:
    raise SystemExit(0)
if (
    existing is not None
    and (existing.reason != sys.argv[1] or existing.set_at != sys.argv[2])
    and sys.argv[3] == "2"
):
    # The durable phase is still intent: our drain was never checkpointed.
    # Preserve a pre-existing or concurrently installed operator hold.
    raise SystemExit(0)
changed = drain.release_owned_full_drain(
    expected_reason=sys.argv[1],
    expected_set_at=sys.argv[2],
    via_rpc=False,
    multi_user=False,
)
if changed is not True:
    raise SystemExit("owned single-user admission drain is missing")
PY_ADMISSION_RELEASE
}

user_nonterminal_count() {
    user_python - "$queue_created" "$queue_fenced_dev" "$queue_fenced_ino" \
        "$MIGRATION_RECEIPT" "$migration_transaction" <<'PY_QUEUE_COUNT'
import json
import os
import pathlib
import stat
import sys

from vq import paths
from vq.spec import JobSpec

created = sys.argv[1] == "1"
fenced_dev = int(sys.argv[2]) if sys.argv[2] else None
fenced_ino = int(sys.argv[3]) if sys.argv[3] else None
receipt = sys.argv[4]
transaction = sys.argv[5]
queue = paths.queue_dir()
if not os.path.lexists(queue):
    print(0)
    raise SystemExit
metadata = queue.lstat()
if created and stat.S_ISREG(metadata.st_mode):
    if (
        fenced_dev is None
        or fenced_ino is None
        or (metadata.st_dev, metadata.st_ino) != (fenced_dev, fenced_ino)
        or metadata.st_uid != os.geteuid()
        or metadata.st_nlink != 1
        or stat.S_IMODE(metadata.st_mode) != 0o600
    ):
        raise SystemExit("single-user absent-queue blocker changed")
    blocker = json.loads(queue.read_text(encoding="utf-8"))
    if (
        type(blocker) is not dict
        or set(blocker) != {"schema", "transaction", "repo", "receipt", "queue_path"}
        or blocker.get("schema") != 1
        or blocker.get("transaction") != transaction
        or blocker.get("queue_path") != str(queue)
        or type(blocker.get("repo")) is not str
        or receipt != str(pathlib.Path(blocker["repo"]) / ".git" / "vq-multi-user-bootstrap.json")
        or blocker.get("receipt") != receipt
    ):
        raise SystemExit("single-user absent-queue blocker payload changed")
    print(0)
    raise SystemExit
if not stat.S_ISDIR(metadata.st_mode) or metadata.st_uid != os.geteuid():
    raise SystemExit(f"unsafe single-user queue directory: {queue}")
count = 0
for path in sorted(queue.glob("*.json")):
    entry = path.lstat()
    if not stat.S_ISREG(entry.st_mode) or entry.st_uid != os.geteuid():
        raise SystemExit(f"unsafe single-user queue entry: {path}")
    try:
        spec = JobSpec.read(path)
    except Exception as exc:
        raise SystemExit(f"unreadable single-user queue entry {path}: {exc}") from exc
    if not spec.is_terminal:
        count += 1
print(count)
PY_QUEUE_COUNT
}

wait_for_user_queue_empty() {
    local count
    for _ in $(seq 1 60); do
        count="$(user_nonterminal_count)" || return 1
        case "$count" in
            0) return 0 ;;
            *[!0-9]*|'') return 1 ;;
            *) echo "  waiting for $count single-user nonterminal job(s)" ;;
        esac
        sleep 2
    done
    return 1
}

prepare_user_queue_blocker_stage() {
    local output fields=()
    if [ "$queue_existed" = "1" ]; then
        queue_stage_path=""
        queue_stage_dev=""
        queue_stage_ino=""
        return 0
    fi
    output="$(user_python - "$queue_path" "$queue_stage_path" \
        "$MIGRATION_RECEIPT" "$migration_transaction" "$REPO" \
        <<'PY_QUEUE_BLOCKER_STAGE'
import json
import os
import pathlib
import stat
import sys

queue = pathlib.Path(sys.argv[1])
stage = pathlib.Path(sys.argv[2])
receipt = pathlib.Path(sys.argv[3])
transaction = sys.argv[4]
repo = pathlib.Path(sys.argv[5])
expected_stage = queue.parent / (
    f".{queue.name}.vq-multi-user-bootstrap-{transaction}.blocker"
)
if stage != expected_stage or receipt != repo / ".git" / "vq-multi-user-bootstrap.json":
    raise SystemExit("queue blocker stage is bound to another transaction")
if os.path.lexists(queue):
    raise SystemExit("absent queue appeared before blocker staging")
parent = queue.parent
parent_stat = parent.lstat()
if (
    not stat.S_ISDIR(parent_stat.st_mode)
    or parent_stat.st_uid != os.geteuid()
    or stat.S_IMODE(parent_stat.st_mode) & 0o022
):
    raise SystemExit("unsafe queue blocker parent")
payload = {
    "queue_path": str(queue),
    "receipt": str(receipt),
    "repo": str(repo),
    "schema": 1,
    "transaction": transaction,
}
encoded = (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode()
flags = os.O_RDWR | getattr(os, "O_NOFOLLOW", 0)
try:
    fd = os.open(stage, flags | os.O_CREAT | os.O_EXCL, 0o600)
except FileExistsError:
    fd = os.open(stage, flags)
    created = False
else:
    created = True
try:
    metadata = os.fstat(fd)
    named = os.stat(stage, follow_symlinks=False)
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or metadata.st_nlink != 1
        or stat.S_IMODE(metadata.st_mode) != 0o600
        or (metadata.st_dev, metadata.st_ino) != (named.st_dev, named.st_ino)
    ):
        raise SystemExit("unsafe queue blocker stage inode")
    if created:
        remaining = memoryview(encoded)
        while remaining:
            written = os.write(fd, remaining)
            if written <= 0:
                raise OSError("short queue blocker stage write")
            remaining = remaining[written:]
        os.fsync(fd)
    else:
        raw = os.read(fd, len(encoded) + 1)
        if raw != encoded:
            raise SystemExit("existing queue blocker stage payload changed")
finally:
    os.close(fd)
parent_fd = os.open(parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
try:
    os.fsync(parent_fd)
finally:
    os.close(parent_fd)
print(metadata.st_dev)
print(metadata.st_ino)
PY_QUEUE_BLOCKER_STAGE
    )" || return 1
    mapfile -t fields <<<"$output"
    [ "${#fields[@]}" = "2" ] || return 1
    queue_stage_dev="${fields[0]}"
    queue_stage_ino="${fields[1]}"
    [[ "$queue_stage_dev" =~ ^[0-9]+$ ]] \
        && [[ "$queue_stage_ino" =~ ^[1-9][0-9]*$ ]]
}

fence_user_queue_directory() {
    local output fields=()
    output="$(user_python - "$queue_path" "$queue_existed" "$queue_dev" \
        "$queue_ino" "$queue_mode" "$queue_fenced_mode" \
        "$queue_stage_path" "$queue_stage_dev" "$queue_stage_ino" \
        "$MIGRATION_RECEIPT" "$migration_transaction" "$REPO" \
        <<'PY_QUEUE_FENCE'
import json
import os
import pathlib
import stat
import sys

path = pathlib.Path(sys.argv[1])
existed = sys.argv[2] == "1"
want_dev = int(sys.argv[3]) if sys.argv[3] else None
want_ino = int(sys.argv[4]) if sys.argv[4] else None
original_mode = int(sys.argv[5], 8)
fenced_mode = int(sys.argv[6], 8)
stage = pathlib.Path(sys.argv[7]) if sys.argv[7] else None
stage_dev = int(sys.argv[8]) if sys.argv[8] else None
stage_ino = int(sys.argv[9]) if sys.argv[9] else None
receipt = pathlib.Path(sys.argv[10])
transaction = sys.argv[11]
repo = pathlib.Path(sys.argv[12])
created = not existed
if created:
    if os.path.lexists(path):
        raise SystemExit("absent queue appeared before its owned fence")
    expected_stage = path.parent / (
        f".{path.name}.vq-multi-user-bootstrap-{transaction}.blocker"
    )
    if stage != expected_stage or receipt != repo / ".git" / "vq-multi-user-bootstrap.json":
        raise SystemExit("queue blocker stage changed before publication")
    metadata = stage.lstat()
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or metadata.st_nlink != 1
        or stat.S_IMODE(metadata.st_mode) != 0o600
        or (metadata.st_dev, metadata.st_ino) != (stage_dev, stage_ino)
    ):
        raise SystemExit("queue blocker stage inode changed before publication")
    payload = json.loads(stage.read_text(encoding="utf-8"))
    if (
        type(payload) is not dict
        or set(payload) != {"schema", "transaction", "repo", "receipt", "queue_path"}
        or payload.get("schema") != 1
        or payload.get("transaction") != transaction
        or payload.get("queue_path") != str(path)
        or payload.get("receipt") != str(receipt)
        or payload.get("repo") != str(repo)
    ):
        raise SystemExit("queue blocker stage payload changed before publication")
    os.replace(stage, path)
    parent_fd = os.open(
        path.parent,
        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
    )
    try:
        os.fsync(parent_fd)
    finally:
        os.close(parent_fd)
    metadata = path.lstat()
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or metadata.st_nlink != 1
        or stat.S_IMODE(metadata.st_mode) != 0o600
        or (metadata.st_dev, metadata.st_ino) != (stage_dev, stage_ino)
    ):
        raise SystemExit("published absent-queue blocker changed inode")
    print(path)
    print(f"{original_mode:o}")
    print(f"{fenced_mode:o}")
    print(metadata.st_dev)
    print(metadata.st_ino)
    print("1")
    raise SystemExit
metadata = path.lstat()
if not stat.S_ISDIR(metadata.st_mode) or metadata.st_uid != os.geteuid():
    raise SystemExit(f"unsafe single-user queue directory: {path}")
if existed and (
    (metadata.st_dev, metadata.st_ino) != (want_dev, want_ino)
    or stat.S_IMODE(metadata.st_mode) != original_mode
):
    raise SystemExit("single-user queue changed after migration intent")
if stat.S_IMODE(metadata.st_mode) != original_mode:
    raise SystemExit("single-user queue mode changed before fence")
if fenced_mode == original_mode:
    raise SystemExit("single-user queue directory is already write-fenced")
flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
fd = os.open(path, flags)
try:
    opened = os.fstat(fd)
    if (opened.st_dev, opened.st_ino) != (metadata.st_dev, metadata.st_ino):
        raise SystemExit("single-user queue directory changed during fence")
    os.fchmod(fd, fenced_mode)
    os.fsync(fd)
finally:
    os.close(fd)
print(path)
print(f"{original_mode:o}")
print(f"{fenced_mode:o}")
print(metadata.st_dev)
print(metadata.st_ino)
print("1" if created else "0")
PY_QUEUE_FENCE
    )" || return 1
    mapfile -t fields <<<"$output"
    [ "${#fields[@]}" = "6" ] || return 1
    queue_path="${fields[0]}"
    queue_mode="${fields[1]}"
    queue_fenced_mode="${fields[2]}"
    queue_fenced_dev="${fields[3]}"
    queue_fenced_ino="${fields[4]}"
    queue_created="${fields[5]}"
    [ "$queue_created" = "0" ] || [ "$queue_created" = "1" ] || return 1
    queue_fenced=1
}

restore_user_queue_directory() {
    user_python - "$queue_path" "$queue_mode" "$queue_fenced_mode" \
        "$queue_fenced_dev" "$queue_fenced_ino" "$queue_created" \
        "$queue_dev" "$queue_ino" "$MIGRATION_RECEIPT" \
        "$migration_transaction" "$queue_stage_path" "$queue_stage_dev" \
        "$queue_stage_ino" "$REPO" <<'PY_QUEUE_RESTORE'
import json
import os
import pathlib
import stat
import sys

path = pathlib.Path(sys.argv[1])
original_mode = int(sys.argv[2], 8)
fenced_mode = int(sys.argv[3], 8)
fenced_dev = int(sys.argv[4]) if sys.argv[4] else None
fenced_ino = int(sys.argv[5]) if sys.argv[5] else None
created = sys.argv[6] == "1"
original_dev = int(sys.argv[7]) if sys.argv[7] else None
original_ino = int(sys.argv[8]) if sys.argv[8] else None
receipt = pathlib.Path(sys.argv[9])
transaction = sys.argv[10]
stage = pathlib.Path(sys.argv[11]) if sys.argv[11] else None
stage_dev = int(sys.argv[12]) if sys.argv[12] else None
stage_ino = int(sys.argv[13]) if sys.argv[13] else None
repo = pathlib.Path(sys.argv[14])


def validate_blocker(candidate, expected_dev, expected_ino):
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(candidate, flags)
    try:
        metadata = os.fstat(fd)
        raw = os.read(fd, 4097)
    finally:
        os.close(fd)
    named = os.stat(candidate, follow_symlinks=False)
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or metadata.st_nlink != 1
        or stat.S_IMODE(metadata.st_mode) != 0o600
        or (metadata.st_dev, metadata.st_ino) != (named.st_dev, named.st_ino)
        or (
            expected_dev is not None
            and (metadata.st_dev, metadata.st_ino) != (expected_dev, expected_ino)
        )
        or not raw
        or len(raw) > 4096
    ):
        raise SystemExit("queue blocker inode changed during recovery")
    blocker = json.loads(raw.decode("utf-8"))
    if (
        type(blocker) is not dict
        or set(blocker) != {"schema", "transaction", "repo", "receipt", "queue_path"}
        or blocker.get("schema") != 1
        or blocker.get("transaction") != transaction
        or blocker.get("repo") != str(repo)
        or blocker.get("receipt") != str(receipt)
        or blocker.get("queue_path") != str(path)
    ):
        raise SystemExit("queue blocker payload changed during recovery")


if created:
    expected_stage = path.parent / (
        f".{path.name}.vq-multi-user-bootstrap-{transaction}.blocker"
    )
    if stage != expected_stage or receipt != repo / ".git" / "vq-multi-user-bootstrap.json":
        raise SystemExit("queue blocker recovery scope changed")
    stage_exists = os.path.lexists(stage)
    final_exists = os.path.lexists(path)
    if stage_exists and final_exists:
        raise SystemExit("queue blocker exists at both staged and live paths")
    if stage_exists:
        validate_blocker(stage, stage_dev, stage_ino)
        stage.unlink()
    elif final_exists:
        if stage_dev is None or stage_ino is None:
            raise SystemExit("published queue blocker lacks a durable staged inode")
        expected_dev = fenced_dev if fenced_dev is not None else stage_dev
        expected_ino = fenced_ino if fenced_ino is not None else stage_ino
        validate_blocker(path, expected_dev, expected_ino)
        path.unlink()
    else:
        raise SystemExit(0)
    parent_fd = os.open(
        path.parent,
        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
    )
    try:
        os.fsync(parent_fd)
    finally:
        os.close(parent_fd)
    raise SystemExit(0)

try:
    metadata = path.lstat()
except FileNotFoundError:
    raise
if (
    not stat.S_ISDIR(metadata.st_mode)
    or metadata.st_uid != os.geteuid()
):
    raise SystemExit("single-user queue directory no longer matches owned fence")
if fenced_dev is None or fenced_ino is None:
    want_dev, want_ino = original_dev, original_ino
else:
    want_dev, want_ino = fenced_dev, fenced_ino
if (metadata.st_dev, metadata.st_ino) != (want_dev, want_ino):
    raise SystemExit("single-user queue inode changed during migration")
current_mode = stat.S_IMODE(metadata.st_mode)
if current_mode not in {original_mode, fenced_mode}:
    raise SystemExit("single-user queue mode changed during migration")
flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
fd = os.open(path, flags)
try:
    opened = os.fstat(fd)
    if (opened.st_dev, opened.st_ino) != (metadata.st_dev, metadata.st_ino):
        raise SystemExit("single-user queue directory changed during restore")
    if current_mode == fenced_mode:
        os.fchmod(fd, original_mode)
        os.fsync(fd)
finally:
    os.close(fd)
PY_QUEUE_RESTORE
}

stop_and_prove_user_daemon() {
    local active_state sub_state main_pid
    active_state="$(systemctl --user show vq-daemon \
        --property=ActiveState --value)" || return 1
    sub_state="$(systemctl --user show vq-daemon \
        --property=SubState --value)" || return 1
    main_pid="$(systemctl --user show vq-daemon \
        --property=MainPID --value)" || return 1
    if [ "$active_state" = "active" ] && [ "$sub_state" = "running" ] \
        && [[ "$main_pid" =~ ^[1-9][0-9]*$ ]]; then
        prove_user_daemon_identity 1 0 || return 1
        systemctl --user stop vq-daemon.service || return 1
    elif [ "$active_state" = "inactive" ] && [ "$sub_state" = "dead" ] \
        && [ "$main_pid" = "0" ]; then
        prove_user_daemon_identity 0 0 || return 1
    else
        return 1
    fi
    active_state="$(systemctl --user show vq-daemon.service \
        --property=ActiveState --value)" || return 1
    sub_state="$(systemctl --user show vq-daemon.service \
        --property=SubState --value)" || return 1
    main_pid="$(systemctl --user show vq-daemon.service \
        --property=MainPID --value)" || return 1
    [ "$active_state" = "inactive" ] && [ "$sub_state" = "dead" ] \
        && [ "$main_pid" = "0" ] || return 1
    prove_user_daemon_identity 0 0 || return 1
    user_stopped=1
}

phase_at_least() {
    local want="$1"
    local current_rank want_rank
    case "$migration_phase" in
        intent) current_rank=0 ;;
        drained) current_rank=1 ;;
        queue-intent) current_rank=2 ;;
        queue-staged) current_rank=3 ;;
        queue-fenced) current_rank=4 ;;
        daemon-stop-intent) current_rank=5 ;;
        daemon-stopped) current_rank=6 ;;
        config-intent) current_rank=7 ;;
        config-ready) current_rank=8 ;;
        root-intent) current_rank=9 ;;
        root-active) current_rank=10 ;;
        queue-restored) current_rank=11 ;;
        *) return 1 ;;
    esac
    case "$want" in
        queue-intent) want_rank=2 ;;
        daemon-stop-intent) want_rank=4 ;;
        config-intent) want_rank=7 ;;
        root-intent) want_rank=8 ;;
        root-active) want_rank=9 ;;
        *) return 1 ;;
    esac
    [ "$current_rank" -ge "$want_rank" ]
}

build_prospective_config() {
    : >"$new_config"
    if [ -f "$DEPLOY_HOME/.config/vq/config.toml" ]; then
        cp -- "$DEPLOY_HOME/.config/vq/config.toml" "$new_config"
    else
        cp -- "$SEALED/vibe-queue/docs/config.toml.example" "$new_config"
    fi
    chmod 0600 "$new_config"
    tee -a "$new_config" >/dev/null <<'TOML'

# --- multi-user mode (appended by deploy-multi-user.sh) ---
[multi_user]
enabled = true
admin_group = "vq-admins"

[quotas]
default_max_pending_jobs = 20
default_max_concurrent_cpus = 12
TOML
}

provision_system_state_from_receipt() {
    local prospective_sha current_sha
    sudo test ! -L "$ETC_DIR/config.toml" || return 1
    sudo groupadd -f "$ADMIN_GROUP" || return 1
    sudo usermod -aG "$ADMIN_GROUP" "$DEPLOY_USER" || return 1
    sudo install -d -m 0755 -o root -g root "$ETC_DIR" || return 1
    sudo install -d -m 2775 -o root -g "$ADMIN_GROUP" "$STATE_DIR" || return 1
    sudo chgrp "$ADMIN_GROUP" "$STATE_DIR" || return 1
    sudo chmod 2775 "$STATE_DIR" || return 1
    if [ "$config_created" = "1" ]; then
        build_prospective_config || return 1
        prospective_sha="$(sha256sum "$new_config" | cut -d' ' -f1)"
        [ "$prospective_sha" = "$config_created_sha" ] || {
            echo "ERROR: prospective system config changed after durable intent" >&2
            return 1
        }
        if sudo test -f "$ETC_DIR/config.toml"; then
            current_sha="$(sudo sha256sum "$ETC_DIR/config.toml" | cut -d' ' -f1)"
            [ "$current_sha" = "$config_created_sha" ] || return 1
        else
            sudo install -m 0644 -o root -g root \
                "$new_config" "$ETC_DIR/config.toml" || return 1
            current_sha="$(sudo sha256sum "$ETC_DIR/config.toml" | cut -d' ' -f1)"
            [ "$current_sha" = "$config_created_sha" ] || return 1
        fi
    else
        sudo test -f "$ETC_DIR/config.toml" || return 1
    fi
    sudo /usr/bin/python3 -I -S - "$ETC_DIR/config.toml" "$ETC_DIR" \
        <<'PY_FSYNC_SYSTEM_CONFIG'
import os
import stat
import sys

config, parent = sys.argv[1:]
fd = os.open(config, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
try:
    metadata = os.fstat(fd)
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != 0
        or stat.S_IMODE(metadata.st_mode) & 0o022
    ):
        raise SystemExit("system config has unsafe ownership or mode")
    os.fsync(fd)
finally:
    os.close(fd)
parent_fd = os.open(parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
try:
    os.fsync(parent_fd)
finally:
    os.close(parent_fd)
PY_FSYNC_SYSTEM_CONFIG
    if [ "$config_created" = "1" ]; then
        current_sha="$(sudo sha256sum "$ETC_DIR/config.toml" | cut -d' ' -f1)"
        [ "$current_sha" = "$config_created_sha" ] || return 1
    fi
}

prove_root_runtime() {
    local ping_json proven_pid current_main_pid running_sha running_tree
    [[ "$root_tree_sha" =~ ^[0-9a-f]{64}$ ]] || return 1
    sudo systemctl is-active --quiet "$UNIT" || return 1
    ping_json="$(sudo /usr/bin/env -i HOME=/root PATH=/usr/bin:/bin \
        PYTHONNOUSERSITE=1 VQ_CONFIG_DIR="$ETC_DIR" VQ_STATE_DIR="$STATE_DIR" \
        "$OPT_VENV/bin/vq" daemon ping --json --verbose localhost)" || return 1
    proven_pid="$(/usr/bin/python3 -I -S - \
        "$expected_sha" "$root_tree_sha" "$OPT_VENV/bin/python" \
        "$OPT_VENV/bin/vq" "$STATE_DIR/daemon.sock" "$ping_json" \
        <<'PY_ROOT_RUNTIME_PROOF'
import json
import sys

want_sha, want_tree, want_python, want_vq, want_socket, raw = sys.argv[1:]
data = json.loads(raw)
identity = data.get("process_identity")
service = data.get("system_service")
if data.get("ok") is not True or not isinstance(identity, dict):
    raise SystemExit("missing root process identity")
if not isinstance(service, dict) or service.get("status") != "ok":
    raise SystemExit("missing root systemd identity")
pid = identity.get("pid")
if not (
    identity.get("status") == "ok"
    and type(pid) is int
    and pid > 0
    and identity.get("euid") == 0
    and identity.get("python_executable") == want_python
    and identity.get("argv") == [want_vq, "daemon", "run"]
    and identity.get("multi_user") is True
    and identity.get("socket_path") == want_socket
    and identity.get("source_sha") == want_sha
    and identity.get("source_tree_sha256") == want_tree
    and data.get("source_sha") == want_sha
    and data.get("source_tree_sha256") == want_tree
    and service.get("main_pid") == pid
    and service.get("active_state") == "active"
    and service.get("sub_state") == "running"
    and service.get("user") == "root"
    and service.get("executable") == want_vq
    and service.get("argv") == [want_vq, "daemon", "run"]
):
    raise SystemExit("root RPC responder is not the accepted systemd MainPID")
print(pid)
PY_ROOT_RUNTIME_PROOF
    )" || return 1
    [[ "$proven_pid" =~ ^[1-9][0-9]*$ ]] || return 1
    current_main_pid="$(sudo systemctl show "$UNIT" \
        --property=MainPID --value)" || return 1
    [ "$current_main_pid" = "$proven_pid" ] || return 1
    running_sha="$(sudo /usr/bin/env -i HOME=/root PATH=/usr/bin:/bin \
        PYTHONNOUSERSITE=1 "$OPT_VENV/bin/vq" source-sha)" || return 1
    running_tree="$(sudo /usr/bin/env -i HOME=/root PATH=/usr/bin:/bin \
        PYTHONNOUSERSITE=1 "$OPT_VENV/bin/vq" source-tree-sha256)" || return 1
    [ "$running_sha" = "$expected_sha" ] \
        && [ "$running_tree" = "$root_tree_sha" ]
}

admin_token_is_durable() {
    sudo /usr/bin/env -i HOME=/root PATH=/usr/bin:/bin PYTHONNOUSERSITE=1 \
        "$OPT_VENV/bin/python" -I -S - "$ETC_DIR/web-token" \
        <<'PY_ADMIN_TOKEN_PROOF'
import os
import stat
import sys

path = sys.argv[1]
fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
try:
    info = os.fstat(fd)
    raw = os.read(fd, 4097)
finally:
    os.close(fd)
if (
    not stat.S_ISREG(info.st_mode)
    or info.st_uid != 0
    or info.st_nlink != 1
    or stat.S_IMODE(info.st_mode) != 0o600
    or not raw
    or len(raw) > 4096
):
    raise SystemExit("unsafe or empty root admin token")
try:
    token = raw.decode("ascii")
except UnicodeDecodeError as exc:
    raise SystemExit("root admin token is not ASCII") from exc
if token.count("\n") != 1 or not token.endswith("\n") or len(token.strip()) < 32:
    raise SystemExit("root admin token is incomplete")
PY_ADMIN_TOKEN_PROOF
}

ensure_admin_token() {
    if ! admin_token_is_durable >/dev/null 2>&1; then
        sudo /usr/bin/env -i HOME=/root PATH=/usr/bin:/bin PYTHONNOUSERSITE=1 \
            VQ_CONFIG_DIR="$ETC_DIR" "$OPT_VENV/bin/vq" web init-token \
            --quiet --force || return 1
    fi
    admin_token_is_durable
}

rollback_pending_migration() {
    local active sub_state pid proven=0
    if phase_at_least queue-intent; then
        restore_user_queue_directory || return 1
    fi
    if phase_at_least daemon-stop-intent; then
        active="$(systemctl --user show vq-daemon --property=ActiveState --value)" \
            || return 1
        pid="$(systemctl --user show vq-daemon --property=MainPID --value)" \
            || return 1
        sub_state="$(systemctl --user show vq-daemon --property=SubState --value)" \
            || return 1
        if [ "$user_was_active" = "1" ]; then
            if [ "$active" = "inactive" ] && [ "$sub_state" = "dead" ] \
                && [ "$pid" = "0" ]; then
                prove_user_daemon_identity 0 0 || return 1
                systemctl --user start vq-daemon.service || return 1
                for _ in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15; do
                    if prove_user_daemon_identity 1 1; then
                        proven=1
                        break
                    fi
                    sleep 2
                done
                [ "$proven" = "1" ] || return 1
            elif [ "$active" = "active" ] && [ "$sub_state" = "running" ] \
                && [[ "$pid" =~ ^[1-9][0-9]*$ ]]; then
                prove_user_daemon_identity 1 0 || return 1
            else
                return 1
            fi
        else
            [ "$active" = "inactive" ] && [ "$sub_state" = "dead" ] \
                && [ "$pid" = "0" ] || return 1
            prove_user_daemon_identity 0 0 || return 1
        fi
    fi
    if [ "$migration_phase" = "intent" ]; then
        release_user_admission_fence 2 || return 1
    else
        release_user_admission_fence 1 || return 1
    fi
    clear_migration_receipt || return 1
    admission_owned=0
    queue_fenced=0
}

ensure_root_activation_from_receipt() {
    case "$migration_phase" in
        config-intent|config-ready|root-intent)
            provision_system_state_from_receipt || return 1
            ;;
    esac
    if [ "$migration_phase" = "config-intent" ]; then
        update_migration_receipt config-intent config-ready || return 1
    fi
    if [ "$migration_phase" = "config-ready" ]; then
        update_migration_receipt config-ready root-intent || return 1
    fi
    if [ "$migration_phase" = "root-intent" ]; then
        if ! sudo "$HELPER_BIN" --checkout "$REPO" \
            --expected-sha "$expected_sha"; then
            if sudo test ! -e "$OPT_VENV" && sudo test ! -L "$OPT_VENV"; then
                sudo "$HELPER_BIN" --checkout "$REPO" \
                    --expected-sha "$expected_sha" --bootstrap
            else
                return 1
            fi
        fi
        root_tree_sha="$(sudo /usr/bin/env -i HOME=/root PATH=/usr/bin:/bin \
            PYTHONNOUSERSITE=1 "$OPT_VENV/bin/vq" source-tree-sha256)" \
            || return 1
        [[ "$root_tree_sha" =~ ^[0-9a-f]{64}$ ]] || return 1
        update_migration_receipt root-intent root-active \
            "$config_created" "$config_created_sha" "$root_tree_sha" || return 1
    fi
    root_activated=1
    sudo systemctl start "$UNIT" || return 1
    prove_root_runtime || return 1
}

finish_pending_migration() {
    ensure_root_activation_from_receipt || return 1
    ensure_admin_token || return 1
    if [ "$single_user_runtime" = "1" ]; then
        systemctl --user disable vq-daemon || return 1
        stop_and_prove_user_daemon || return 1
    fi
    if [ "$migration_phase" = "root-active" ]; then
        restore_user_queue_directory || return 1
        update_migration_receipt root-active queue-restored || return 1
    else
        restore_user_queue_directory || return 1
    fi
    release_user_admission_fence 1 || return 1
    sudo systemctl enable "$UNIT" || return 1
    prove_root_runtime || return 1
    systemctl --user is-active --quiet vq-daemon && return 1
    clear_migration_receipt || return 1
    admission_owned=0
    queue_fenced=0
}

recover_pending_migration() {
    load_migration_receipt || return 1
    echo "  recovering durable single-user migration phase: $migration_phase"
    admission_owned=1
    if phase_at_least config-intent; then
        finish_pending_migration
    else
        rollback_pending_migration
    fi
}

acquire_migration_operation_lock \
    || die "another multi-user deploy/recovery operation is active or its lock is unsafe"

# Retire the old delayed root-code surface only after this deploy owns the
# durable migration-operation admission. If a later step fails, leaving this
# unsafe timer disabled is intentional.
sudo systemctl disable --now 'vq-admin-auto-update@*.timer' 2>/dev/null || true
sudo systemctl stop 'vq-admin-auto-update@*.service' 2>/dev/null || true

acquire_user_lifecycle_admission \
    || die "another checkout or serving-runtime lifecycle operation is active"
reconcile_migration_arming \
    || die "durable migration arming state is malformed or ambiguous"
if [ "$prepare_only" = "0" ]; then
    prove_user_runtime_matches_accepted_checkout \
        || die "single-user serving venv does not exactly match the accepted checkout; run the supported user update first"
    if [ -e "$MIGRATION_RECEIPT" ] || [ -L "$MIGRATION_RECEIPT" ]; then
        load_migration_receipt \
            || die "durable migration receipt is malformed; no recovery mutation was attempted"
        migration_preexisting=1
        migration_recovery_adopted=1
    else
        user_active_state="$(systemctl --user show vq-daemon \
            --property=ActiveState --value)" \
            || die "cannot inspect the single-user vq-daemon unit"
        user_main_pid="$(systemctl --user show vq-daemon \
            --property=MainPID --value)" \
            || die "cannot inspect the single-user vq-daemon MainPID"
        if [ "$user_active_state" = "active" ] \
            && [[ "$user_main_pid" =~ ^[1-9][0-9]*$ ]]; then
            user_was_active=1
        elif [ "$user_active_state" = "inactive" ] \
            && [ "$user_main_pid" = "0" ]; then
            user_was_active=0
        else
            die "single-user vq-daemon has unsupported state $user_active_state/MainPID=$user_main_pid"
        fi
        capture_user_daemon_identity \
            || die "single-user daemon is not the accepted serving-venv systemd identity"
        write_migration_intent \
            || die "could not durably arm the user-runtime migration admission"
        migration_recovery_adopted=1
    fi
fi
git_observe -C "$REPO" archive --format=tar "$expected_sha" \
    vibe-queue/contrib vibe-queue/docs/config.toml.example \
    scripts/_lifecycle_lock.sh >"$sealed_dir/bootstrap.tar"
mkdir "$sealed_dir/snapshot"
tar --extract --file "$sealed_dir/bootstrap.tar" --directory "$sealed_dir/snapshot" \
    --no-same-owner --no-same-permissions
recheck_sha="$(git_observe -C "$REPO" rev-parse --verify 'HEAD^{commit}')"
recheck_dirty="$(git_observe -C "$REPO" status --porcelain=v1 --untracked-files=all -- \
    vibe-queue scripts/_lifecycle_lock.sh)"
[ "$recheck_sha" = "$expected_sha" ] && [ -z "$recheck_dirty" ] \
    || die "checkout changed while bootstrap files were sealed"
# Feed the accepted lock helper directly from sanitized Git into a unique
# root-owned archive. Root never opens the caller-owned snapshot as code, and a
# same-UID process cannot rewrite this inode after sudo closes it.
bootstrap_lock_archive="$(sudo mktemp /var/tmp/vq-bootstrap-lock.XXXXXX.tar)" \
    || die "could not allocate a root-owned bootstrap archive"
case "$bootstrap_lock_archive" in
    /var/tmp/vq-bootstrap-lock.*.tar) ;;
    *) die "sudo mktemp returned an unexpected bootstrap path" ;;
esac
git_observe -C "$REPO" archive --format=tar "$expected_sha" \
    scripts/_lifecycle_lock.sh \
    | sudo tee "$bootstrap_lock_archive" >/dev/null \
    || die "could not seal the accepted lifecycle helper into root custody"
sudo /usr/bin/python3 -I -S - "$bootstrap_lock_archive" /var/tmp \
    <<'PY_FSYNC_BOOTSTRAP_ARCHIVE'
import os
import stat
import sys

archive, parent = sys.argv[1:]
fd = os.open(archive, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
try:
    metadata = os.fstat(fd)
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != 0
        or metadata.st_nlink != 1
        or metadata.st_size <= 0
        or metadata.st_size > 1024 * 1024
    ):
        raise SystemExit("unsafe root bootstrap archive")
    os.fsync(fd)
finally:
    os.close(fd)
parent_fd = os.open(parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
try:
    os.fsync(parent_fd)
finally:
    os.close(parent_fd)
PY_FSYNC_BOOTSTRAP_ARCHIVE
release_user_lifecycle_admission \
    || die "could not release the armed user-runtime lifecycle admission lock"
SEALED="$sealed_dir/snapshot"
new_config="$sealed_dir/new-config.toml"

say "1/7  Install the sealed privileged bootstrap surface"
# One root shell first takes a root-owned bootstrap admission lock, then sources
# only an already-installed trusted helper or the accepted helper extracted
# from the immutable root-owned archive. It subsequently owns the exact
# checkout+/opt target locks while publishing and fsyncing every live file.
sudo /usr/bin/env -i HOME=/root PATH=/usr/sbin:/usr/bin:/sbin:/bin \
    PYTHONNOUSERSITE=1 SUDO_USER="$DEPLOY_USER" \
    /bin/bash -p -s -- "$REPO" "$expected_sha" "$DEPLOY_USER" \
    "$OPT_VENV" "$HELPER_BIN" "$LOCK_HELPER_BIN" "$bootstrap_lock_archive" \
    "$UNIT" "$MIGRATION_RECEIPT" "$migration_transaction" "$prepare_only" \
    <<'ROOT_BOOTSTRAP_SURFACE'
set -euo pipefail
IFS=$' \t\n'
umask 077

repo="$1"
expected_sha="$2"
source_user="$3"
opt_venv="$4"
helper_bin="$5"
lock_helper_bin="$6"
bootstrap_lock_archive="$7"
unit="$8"
migration_receipt="$9"
migration_transaction="${10}"
prepare_only="${11}"
opt_root="$(dirname "$opt_venv")"

case "$bootstrap_lock_archive" in
    /var/tmp/vq-bootstrap-lock.*.tar) ;;
    *) exit 1 ;;
esac
[ ! -L "$opt_root" ] || exit 1
install -d -m 0755 -o root -g root "$opt_root"
[ "$(stat -c '%u:%a' "$opt_root")" = "0:755" ] || exit 1
exec 198>>"$opt_root/.bootstrap-surface.lock"
/usr/bin/flock -x 198
[ ! -L "$opt_root/.bootstrap-surface.lock" ] || exit 1
chmod 0600 "$opt_root/.bootstrap-surface.lock"
[ "$(stat -c '%u:%a' "$opt_root/.bootstrap-surface.lock")" = "0:600" ] || exit 1
[ ! -L "$opt_root/bin" ] && [ ! -L "$opt_root/libexec" ] || exit 1
install -d -m 0755 -o root -g root "$opt_root/bin" "$opt_root/libexec"

root_work="$(mktemp -d "$opt_root/.bootstrap-work.XXXXXX")"
lifecycle_acquired=0
cleanup_root() {
    status="$?"
    trap - EXIT
    if [ "$lifecycle_acquired" = "1" ]; then
        vibe_toolset_release_lifecycle_lock || status=1
    fi
    case "$root_work" in /opt/vq/.bootstrap-work.*) rm -rf -- "$root_work" ;; esac
    rm -f -- "$bootstrap_lock_archive"
    exit "$status"
}
trap cleanup_root EXIT

bootstrap_helper="$root_work/vibe-toolset-lifecycle-lock.sh"
/usr/bin/python3 -I -S - "$bootstrap_lock_archive" "$bootstrap_helper" \
    <<'PY_EXTRACT_BOOTSTRAP_LOCK'
import os
import stat
import sys
import tarfile

archive_path, output_path = sys.argv[1:]
fd = os.open(archive_path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
try:
    metadata = os.fstat(fd)
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != 0
        or metadata.st_nlink != 1
        or metadata.st_size <= 0
        or metadata.st_size > 1024 * 1024
    ):
        raise SystemExit("unsafe root-owned bootstrap archive")
    with os.fdopen(os.dup(fd), "rb") as source, tarfile.open(fileobj=source) as archive:
        members = archive.getmembers()
        files = [member for member in members if member.isfile()]
        if (
            len(files) != 1
            or files[0].name != "scripts/_lifecycle_lock.sh"
            or files[0].size <= 0
            or files[0].size > 256 * 1024
            or any(
                member.name not in {"scripts", "scripts/", "scripts/_lifecycle_lock.sh"}
                or member.issym()
                or member.islnk()
                or not (member.isdir() or member.isfile())
                for member in members
            )
        ):
            raise SystemExit("bootstrap archive does not contain exactly the accepted helper")
        extracted = archive.extractfile(files[0])
        if extracted is None:
            raise SystemExit("accepted lifecycle helper could not be extracted")
        payload = extracted.read(256 * 1024 + 1)
        if len(payload) != files[0].size:
            raise SystemExit("accepted lifecycle helper size changed")
finally:
    os.close(fd)
out = os.open(
    output_path,
    os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
    0o600,
)
try:
    remaining = memoryview(payload)
    while remaining:
        written = os.write(out, remaining)
        if written <= 0:
            raise OSError("short bootstrap-helper write")
        remaining = remaining[written:]
    os.fsync(out)
finally:
    os.close(out)
parent = os.open(os.path.dirname(output_path), os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
try:
    os.fsync(parent)
finally:
    os.close(parent)
PY_EXTRACT_BOOTSTRAP_LOCK

lock_source="$bootstrap_helper"
# shellcheck source=/opt/vq/libexec/vibe-toolset-lifecycle-lock.sh
. "$lock_source"
source_home="$(getent passwd "$source_user" | cut -d: -f6)"
source_uid="$(id -u "$source_user")"
[ -n "$source_home" ] && [ "$source_uid" != "0" ]
lock_python=""
vibe_toolset_find_external_python lock_python "$opt_venv" 3 12
lifecycle_action="vq-multi-user-bootstrap-surface"
if [ "$prepare_only" = "0" ]; then
    lifecycle_action="vq-multi-user-deploy"
fi
vibe_toolset_acquire_lifecycle_lock \
    "$lock_python" "$repo" "$opt_venv" "$lifecycle_action"
lifecycle_acquired=1

if [ "$prepare_only" = "0" ]; then
    PYTHONNOUSERSITE=1 "$lock_python" -I -S - \
        "$migration_receipt" "$migration_transaction" "$repo" \
        "$expected_sha" "$source_uid" <<'PY_ROOT_BOOTSTRAP_RECEIPT'
import json
import os
import pathlib
import re
import stat
import sys

path = pathlib.Path(sys.argv[1])
transaction, repo_text, expected_sha, source_uid_text = sys.argv[2:]
repo = pathlib.Path(repo_text)
source_uid = int(source_uid_text)
if path != repo / ".git" / "vq-multi-user-bootstrap.json":
    raise SystemExit("wrong root bootstrap receipt path")
fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
try:
    metadata = os.fstat(fd)
    raw = os.read(fd, 16385)
finally:
    os.close(fd)
if (
    not stat.S_ISREG(metadata.st_mode)
    or metadata.st_uid != source_uid
    or metadata.st_nlink != 1
    or stat.S_IMODE(metadata.st_mode) != 0o600
    or not raw
    or len(raw) > 16384
):
    raise SystemExit("unsafe root bootstrap receipt")


def unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate root bootstrap receipt key")
        result[key] = value
    return result


data = json.loads(raw.decode("utf-8"), object_pairs_hook=unique)
keys = {
    "schema", "transaction", "phase", "expected_sha", "repo", "user_uid",
    "user_venv", "drain_path", "drain_reason", "drain_set_at",
    "user_was_active", "user_daemon_identity", "queue_path",
    "queue_existed", "queue_dev", "queue_ino", "queue_mode",
    "queue_fenced_mode", "queue_fenced_dev", "queue_fenced_ino",
    "queue_stage_path", "queue_stage_dev", "queue_stage_ino",
    "config_created", "config_sha256", "root_tree_sha256",
}
phases = {
    "intent", "drained", "queue-intent", "queue-staged", "queue-fenced",
    "daemon-stop-intent", "daemon-stopped", "config-intent", "config-ready",
    "root-intent", "root-active", "queue-restored",
}
if (
    type(data) is not dict
    or set(data) != keys
    or data.get("schema") != 1
    or data.get("transaction") != transaction
    or re.fullmatch(r"[0-9a-f]{32}", transaction) is None
    or data.get("phase") not in phases
    or data.get("expected_sha") != expected_sha
    or data.get("repo") != str(repo)
    or data.get("user_uid") != source_uid
    or data.get("user_venv") != str(repo / "vibe-queue" / ".venv")
):
    raise SystemExit("root bootstrap receipt does not authorize this exact deploy")
PY_ROOT_BOOTSTRAP_RECEIPT
fi

archive="$root_work/bootstrap.tar"
snapshot="$root_work/snapshot"
mkdir -m 0700 "$snapshot"
/usr/sbin/runuser -u "$source_user" -- /usr/bin/env -i \
    HOME="$source_home" USER="$source_user" LOGNAME="$source_user" \
    PATH=/usr/bin:/bin GIT_CONFIG_NOSYSTEM=1 GIT_CONFIG_GLOBAL=/dev/null \
    GIT_OPTIONAL_LOCKS=0 GIT_TERMINAL_PROMPT=0 GIT_NO_REPLACE_OBJECTS=1 \
    /usr/bin/git -c core.fsmonitor= -c core.hooksPath=/dev/null \
    -c core.attributesFile=/dev/null -c credential.helper= -c core.pager=cat \
    -C "$repo" archive --format=tar "$expected_sha" \
    vibe-queue/contrib/vq-multi-user-refresh \
    vibe-queue/contrib/vq-daemon-multi-user.service \
    vibe-queue/contrib/vq-admin-auto-update@.service \
    vibe-queue/contrib/vq-admin-auto-update@.timer \
    scripts/_lifecycle_lock.sh >"$archive"
tar --extract --file "$archive" --directory "$snapshot" \
    --no-same-owner --no-same-permissions
[ -z "$(find "$snapshot" ! -type f ! -type d -print -quit)" ] \
    || { echo "unsafe special or symlinked bootstrap archive entry" >&2; exit 1; }
for accepted_file in \
    "$snapshot/vibe-queue/contrib/vq-multi-user-refresh" \
    "$snapshot/vibe-queue/contrib/vq-daemon-multi-user.service" \
    "$snapshot/vibe-queue/contrib/vq-admin-auto-update@.service" \
    "$snapshot/vibe-queue/contrib/vq-admin-auto-update@.timer" \
    "$snapshot/scripts/_lifecycle_lock.sh"; do
    [ -f "$accepted_file" ] && [ ! -L "$accepted_file" ] || exit 1
done
chown -R root:root "$snapshot"
chmod -R go-w "$snapshot"

fsync_file() {
    PYTHONNOUSERSITE=1 "$lock_python" -I -S -c '
import os, sys
fd = os.open(sys.argv[1], os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
try:
    os.fsync(fd)
finally:
    os.close(fd)
' "$1"
}
fsync_dir() {
    PYTHONNOUSERSITE=1 "$lock_python" -I -S -c '
import os, sys
fd = os.open(sys.argv[1], os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
try:
    os.fsync(fd)
finally:
    os.close(fd)
' "$1"
}
atomic_install() {
    source="$1"
    target="$2"
    mode="$3"
    stage="$(mktemp "$(dirname "$target")/.$(basename "$target").XXXXXX.tmp")"
    [ -f "$stage" ] && [ ! -L "$stage" ]
    install -m "$mode" -o root -g root "$source" "$stage"
    fsync_file "$stage"
    mv -f -- "$stage" "$target"
    fsync_dir "$(dirname "$target")"
    [ "$(stat -c '%u:%a' "$target")" = "0:$mode" ]
    [ "$(sha256sum "$source" | cut -d' ' -f1)" \
        = "$(sha256sum "$target" | cut -d' ' -f1)" ]
}

atomic_install "$snapshot/vibe-queue/contrib/vq-multi-user-refresh" \
    "$helper_bin" 755
atomic_install "$snapshot/scripts/_lifecycle_lock.sh" "$lock_helper_bin" 644
atomic_install "$snapshot/vibe-queue/contrib/$unit" \
    "/etc/systemd/system/$unit" 644
atomic_install "$snapshot/vibe-queue/contrib/vq-admin-auto-update@.service" \
    /etc/systemd/system/vq-admin-auto-update@.service 644
atomic_install "$snapshot/vibe-queue/contrib/vq-admin-auto-update@.timer" \
    /etc/systemd/system/vq-admin-auto-update@.timer 644
systemctl daemon-reload
ROOT_BOOTSTRAP_SURFACE

# The retired sudoers fragment allowed a group member to choose source code
# that root would build. Authenticated sudo is now mandatory.
if sudo test -e "$SUDOERS_FILE" || sudo test -L "$SUDOERS_FILE"; then
    sudo rm -f -- "$SUDOERS_FILE"
    echo "  removed retired passwordless source-build grant $SUDOERS_FILE"
fi

say "2/7  Exact unit installed; root auto-update timer retired"

if [ "$prepare_only" = "1" ]; then
    cat <<EOF

Prepared the accepted root-owned refresh surface without changing config,
state, daemons, or $OPT_VENV. Continue with the required read-only check:

  sudo $HELPER_BIN --checkout $REPO --expected-sha $expected_sha --dry-run

Then run that same helper without --dry-run to activate the accepted release.
EOF
    exit 0
fi

if [ "$migration_preexisting" = "1" ]; then
    recover_pending_migration \
        || die "durable single-user migration recovery failed closed"
    echo "OK: completed the interrupted exact-SHA multi-user transaction."
    echo "Re-run this exact accepted-SHA deploy explicitly to begin a new migration."
    exit 0
fi
if [ -e "$OPT_VENV" ] || [ -L "$OPT_VENV" ]; then
    die "existing $OPT_VENV must be refreshed explicitly: run --prepare-only, then the documented helper --dry-run and activation"
fi

say "3/7  Fence, drain, and quiesce the single-user runtime"
if [ "$single_user_runtime" = "1" ]; then
    acquire_user_admission_fence \
        || die "could not acquire an owned deny/full-dispatch single-user drain"
    admission_owned=1
    update_migration_receipt intent drained \
        || die "could not durably record the exact admission drain"
    prove_user_admission_fence \
        || die "the single-user daemon did not prove the exact admission drain"
    wait_for_user_queue_empty \
        || die "single-user jobs did not drain to an exact readable zero"
    update_migration_receipt drained queue-intent \
        || die "could not durably record the queue-fence intent"
    prepare_user_queue_blocker_stage \
        || die "could not durably stage the single-user queue admission blocker"
    update_migration_receipt queue-intent queue-staged \
        || die "could not durably bind the queue blocker stage"
    fence_user_queue_directory \
        || die "could not close the single-user queue directory admission race"
    queue_fenced=1
    update_migration_receipt queue-staged queue-fenced \
        || die "could not durably record the queue write fence"
    [ "$(user_nonterminal_count)" = "0" ] \
        || die "a submission crossed the drain before the queue write fence"
    update_migration_receipt queue-fenced daemon-stop-intent \
        || die "could not durably record the single-user stop intent"
    stop_and_prove_user_daemon \
        || die "single-user vq-daemon did not become inactive with MainPID=0"
    update_migration_receipt daemon-stop-intent daemon-stopped \
        || die "could not durably record single-user daemon quiescence"
    [ "$(user_nonterminal_count)" = "0" ] \
        || die "single-user queue became non-empty while its daemon stopped"
    sleep 2
    [ "$(user_nonterminal_count)" = "0" ] \
        || die "single-user queue did not remain empty behind its admission fence"
else
    [ ! -e "$DEPLOY_HOME/.local/share/vq/queue" ] \
        && [ ! -L "$DEPLOY_HOME/.local/share/vq/queue" ] \
        || die "single-user queue state exists but its runtime is unavailable"
    fresh_active="$(systemctl --user show vq-daemon \
        --property=ActiveState --value 2>/dev/null || echo inactive)"
    fresh_pid="$(systemctl --user show vq-daemon \
        --property=MainPID --value 2>/dev/null || echo 0)"
    [ "$fresh_active" = "inactive" ] && [ "$fresh_pid" = "0" ] \
        || die "a single-user daemon is active without the expected runtime"
    echo "  no single-user runtime or queue state: clean bootstrap"
fi

sudo test ! -L "$ETC_DIR/config.toml" \
    || die "refusing symlinked $ETC_DIR/config.toml"
config_created=0
config_created_sha=""
if ! sudo test -f "$ETC_DIR/config.toml"; then
    config_created=1
    build_prospective_config
    config_created_sha="$(sha256sum "$new_config" | cut -d' ' -f1)"
    [[ "$config_created_sha" =~ ^[0-9a-f]{64}$ ]] \
        || die "could not bind the prospective system config"
fi
update_migration_receipt daemon-stopped config-intent \
    "$config_created" "$config_created_sha" \
    || die "could not durably record privileged config intent"
say "4/7  Multi-user group, config, and state"
provision_system_state_from_receipt \
    || die "privileged config/state provisioning diverged from its durable intent"
update_migration_receipt config-intent config-ready \
    "$config_created" "$config_created_sha" \
    || die "could not durably bind the privileged config result"

say "5/7  Transactional exact-SHA runtime activation"
update_migration_receipt config-ready root-intent \
    || die "could not durably record root activation intent"
sudo "$HELPER_BIN" --checkout "$REPO" --expected-sha "$expected_sha" --bootstrap
root_tree_sha="$(sudo /usr/bin/env -i HOME=/root PATH=/usr/bin:/bin \
    PYTHONNOUSERSITE=1 "$OPT_VENV/bin/vq" source-tree-sha256)" \
    || die "could not read the activated root package tree"
[[ "$root_tree_sha" =~ ^[0-9a-f]{64}$ ]] \
    || die "activated root package tree digest is malformed"
update_migration_receipt root-intent root-active \
    "$config_created" "$config_created_sha" "$root_tree_sha" \
    || die "could not durably record exact root activation"
root_activated=1

say "6/7  Admin bearer token"
ensure_admin_token || die "could not durably create or verify $ETC_DIR/web-token"
echo "  durable root-owned token verified at $ETC_DIR/web-token"

say "7/7  Retire the old unit, release its fence, and verify root"
if [ "$single_user_runtime" = "1" ]; then
    systemctl --user disable vq-daemon
    stop_and_prove_user_daemon \
        || die "single-user vq-daemon changed during root activation"
fi
sudo systemctl enable "$UNIT"
sudo systemctl is-active --quiet "$UNIT" \
    || die "$UNIT is not active after the helper's exact RPC proof"
if [ "$single_user_runtime" = "1" ]; then
    restore_user_queue_directory \
        || die "single-user queue directory changed during admission-fence release"
    queue_fenced=0
    update_migration_receipt root-active queue-restored \
        || die "could not durably record the restored queue directory"
    release_user_admission_fence \
        || die "single-user admission drain changed; refusing a broad release"
    admission_owned=0
fi
systemctl --user is-active --quiet vq-daemon \
    && die "single-user vq-daemon restarted during migration"
sudo systemctl is-active --quiet "$UNIT" \
    || die "$UNIT stopped before final migration proof"
prove_root_runtime \
    || die "$UNIT identity changed before terminal migration receipt clear"
clear_migration_receipt \
    || die "migration completed but its exact durable receipt could not be cleared"

cat <<EOF

Done: $UNIT runs accepted source $expected_sha from $OPT_VENV.

Future refreshes use the same authenticated transaction:
  sudo $HELPER_BIN --checkout $REPO --expected-sha <accepted-40-hex-sha>

The legacy vq-admin-auto-update@ timer and passwordless refresh grant are
retired. Privileged activation follows an accepted fleet release report.

Rollback to single-user mode:
  sudo systemctl disable --now $UNIT
  systemctl --user enable --now vq-daemon

Full runbook: $SRC/docs/multi_user_deployment.md
EOF
