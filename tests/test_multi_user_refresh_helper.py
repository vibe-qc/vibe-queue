"""Release contract for the privileged ``/opt/vq`` transaction.

The live path needs Linux, root, systemd, and a real /opt/vq install. These
tests therefore pin the shipped shell's security and ordering invariants while
the release runbook supplies the real-host acceptance proof.
"""

from __future__ import annotations

import copy
import hashlib
import json
import os
import stat
import subprocess
import sys
import time
from pathlib import Path

import pytest

CONTRIB = Path(__file__).parents[1] / "contrib"
HELPER = CONTRIB / "vq-multi-user-refresh"
DEPLOY = CONTRIB / "deploy-multi-user.sh"
SUDOERS = CONTRIB / "vq-multi-user-refresh.sudoers"
RUNTIME_LOCK = CONTRIB / "vq-multi-user-runtime-requirements.txt"
LIFECYCLE_HELPER = Path(__file__).parents[1] / "scripts" / "_lifecycle_lock.sh"


def _text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _helper() -> str:
    return _text(HELPER)


def _deploy() -> str:
    return _text(DEPLOY)


def _run(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", str(HELPER), *args],
        capture_output=True,
        text=True,
        check=False,
    )


def _heredoc(script: str, tag: str) -> str:
    opener = f"<<'{tag}'"
    start = script.index(opener)
    start = script.index("\n", start) + 1
    end = script.index(f"\n{tag}\n", start)
    return script[start:end]


def _run_heredoc(
    script: str,
    tag: str,
    *args: str,
    home: Path | None = None,
) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    for name in (
        "PYTEST_ADDOPTS",
        "PYTEST_CURRENT_TEST",
        "PYTEST_VERSION",
        "VQ_CONFIG_DIR",
        "VQ_STATE_DIR",
        "XDG_CONFIG_HOME",
        "XDG_DATA_HOME",
    ):
        env.pop(name, None)
    if home is not None:
        env["HOME"] = str(home)
    env["PYTHONNOUSERSITE"] = "1"
    return subprocess.run(
        [sys.executable, "-I", "-", *args],
        input=_heredoc(script, tag),
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )


def test_exact_accepted_full_sha_is_mandatory() -> None:
    script = _helper()

    assert '[ -n "$expected_sha" ] || die "--expected-sha is mandatory"' in script
    assert "^[0-9A-Fa-f]{40}$" in script
    assert '[ "$derived_sha" = "$expected_sha" ]' in script
    assert "--allow-dirty" not in script
    assert "allow-downgrade" not in script


def test_existing_install_must_be_an_ancestor_of_the_pin() -> None:
    script = _helper()

    assert "merge-base --is-ancestor" in script
    assert '"$previous_sha" "$expected_sha"' in script
    assert "downgrade/divergence refused" in script
    assert script.index("merge-base --is-ancestor") < script.index(
        'write_initial_receipt "$transaction"'
    )


def test_git_observation_is_unprivileged_sanitized_and_lock_free() -> None:
    script = _helper()

    assert 'source_user="${SUDO_USER:-}"' in script
    assert '"$RUNUSER" -u "$source_user"' in script
    assert "GIT_CONFIG_NOSYSTEM=1" in script
    assert "GIT_CONFIG_GLOBAL=/dev/null" in script
    assert "GIT_OPTIONAL_LOCKS=0" in script
    assert "core.fsmonitor=" in script
    assert "core.hooksPath=/dev/null" in script
    assert "direct root use is refused" in script


def test_root_builds_only_an_explicit_sealed_archive() -> None:
    script = _helper()

    archive = 'git_observe -C "$checkout" archive --format=tar "$expected_sha"'
    assert archive in script
    assert '"$snapshot/vibe-queue"' in script
    assert 'pip" install --quiet --upgrade "$checkout' not in script
    assert script.index(archive) < script.index("PY_BUILD_VQ_WHEEL")
    assert script.index('recheck_sha="$(git_observe') < script.index(
        "PY_BUILD_VQ_WHEEL"
    )
    assert "GIT_NO_REPLACE_OBJECTS=1" in script
    assert "core.attributesFile=/dev/null" in script
    assert '.git/info/attributes could rewrite the accepted archive' in script


def test_shared_lifecycle_helper_is_root_owned_and_locks_exact_resources() -> None:
    script = _helper()

    assert 'LOCK_HELPER="$OPT_ROOT/libexec/vibe-toolset-lifecycle-lock.sh"' in script
    assert '[ "$lock_owner" = "0" ]' in script
    assert '. "$LOCK_HELPER"' in script
    assert "vibe_toolset_acquire_lifecycle_lock" in script
    assert 'lifecycle_action="vq-multi-user-refresh"' in script
    assert 'lifecycle_action="vq-multi-user-deploy"' in script
    assert '"$LOCK_PYTHON" "$checkout" "$OPT_VENV" "$lifecycle_action"' in script
    assert script.count("validate_deploy_handoff_receipt") >= 3
    assert "vibe_toolset_release_lifecycle_lock" in script
    assert script.startswith("#!/bin/bash -p\n")


def test_wheel_is_prebuilt_but_new_venv_is_created_only_after_quiescence() -> None:
    script = _helper()
    wheel = script.index("PY_BUILD_VQ_WHEEL")
    stop = script.index('systemctl stop "$UNIT"', wheel)
    create = script.index('"$LOCK_PYTHON" -I -m venv "$OPT_VENV"', stop)

    assert wheel < stop < create
    assert 'mv -- "$OPT_VENV" "$backup"' in script
    assert 'mv -- "$builder" "$OPT_VENV"' not in script
    assert 'mv -- "$work_dir' not in script
    assert "Create the\n# new venv directly at its final path" in script


def test_durable_receipt_precedes_every_destructive_live_step() -> None:
    script = _helper()
    receipt = script.index('write_initial_receipt "$transaction"')
    stop = script.index('systemctl stop "$UNIT"', receipt)
    backup = script.index('mv -- "$OPT_VENV" "$backup"', stop)
    create = script.index('"$LOCK_PYTHON" -I -m venv "$OPT_VENV"', backup)

    assert receipt < stop < backup < create
    assert 'os.fsync(fd)' in script
    assert 'os.fsync(dirfd)' in script
    assert 'install -d -m 0700 -o root -g root "$RECEIPT_DIR"' in script
    assert 'stat.S_IMODE(st.st_mode) != 0o600' in script
    assert script.count("while remaining:") >= 2
    assert "short write while creating refresh receipt" in script
    assert "short write while updating refresh receipt" in script


def test_backup_is_same_filesystem_and_recovery_restores_exact_old_identity() -> None:
    script = _helper()

    assert 'backup="$OPT_ROOT/.venv-backup-$transaction"' in script
    assert 'mv -- "$backup" "$OPT_VENV"' in script
    assert '[ "${identity[0]}" = "$previous_sha" ]' in script
    assert '[ "${identity[1]}" = "$previous_tree" ]' in script
    assert 'rpc_proof "$previous_sha" "$previous_tree"' in script
    assert "automatic rollback failed; receipt retained" in script


def _valid_receipt(root: Path) -> tuple[dict[str, object], Path, str, Path]:
    transaction = "ab" * 16
    target = root / "opt" / "venv"
    backup = target.parent / f".venv-backup-{transaction}"
    stage = target.parent / f".venv-new-{transaction}"
    checkout = str(root / "checkout")
    payload: dict[str, object] = {
        "schema": 1,
        "transaction": transaction,
        "phase": "prepared",
        "checkout": checkout,
        "target": str(target),
        "backup": str(backup),
        "stage": str(stage),
        "unit": "vq-daemon-multi-user.service",
        "expected_sha": "1" * 40,
        "previous_sha": "2" * 40,
        "previous_tree": "3" * 64,
        "new_tree": None,
        "original_dev": 10,
        "original_ino": 20,
        "new_dev": None,
        "new_ino": None,
        "start_after": True,
        "archive_sha256": "4" * 64,
        "wheelhouse_sha256": "5" * 64,
    }
    return payload, target, checkout, backup


def _validate_receipt(
    receipt: Path,
    *,
    target: Path,
    checkout: str,
) -> subprocess.CompletedProcess[str]:
    return _run_heredoc(
        _helper(),
        "PY_RECEIPT",
        str(receipt),
        checkout,
        str(target),
        "vq-daemon-multi-user.service",
        str(os.getuid()),
    )


def test_receipt_validator_accepts_only_exact_transaction_bound_schema(
    tmp_path: Path,
) -> None:
    payload, target, checkout, _backup = _valid_receipt(tmp_path)
    receipt = tmp_path / "receipt.json"
    receipt.write_text(json.dumps(payload) + "\n", encoding="utf-8")
    receipt.chmod(0o600)

    result = _validate_receipt(receipt, target=target, checkout=checkout)

    assert result.returncode == 0, result.stderr
    fields = result.stdout.splitlines()
    assert len(fields) == 20
    assert fields[1] == payload["transaction"]
    assert fields[5] == payload["backup"]
    assert fields[15] == "1"
    assert fields[18] == payload["stage"]
    assert fields[19] == hashlib.sha256(receipt.read_bytes()).hexdigest()


def test_refresh_recovers_exact_empty_stage_before_inode_checkpoint(
    tmp_path: Path,
) -> None:
    parent = tmp_path / "opt"
    parent.mkdir(mode=0o700)
    target = parent / "venv"
    stage = parent / f".venv-new-{'ab' * 16}"
    stage.mkdir(mode=0o755)
    inspected = _run_heredoc(
        _helper(),
        "PY_INSPECT_EMPTY_REFRESH_STAGE",
        str(stage),
        str(target),
        str(parent),
        str(os.getuid()),
        "inspect",
        "",
    )
    assert inspected.returncode == 0, inspected.stderr
    identity = inspected.stdout.strip()
    assert identity == f"{stage.stat().st_dev}:{stage.stat().st_ino}"

    removed = _run_heredoc(
        _helper(),
        "PY_REMOVE_EMPTY_REFRESH_STAGE",
        str(stage),
        str(target),
        str(parent),
        str(os.getuid()),
        "remove",
        identity,
    )
    assert removed.returncode == 0, removed.stderr
    assert not stage.exists()

    stage.mkdir(mode=0o755)
    (stage / "unexpected").write_text("package bytes\n", encoding="utf-8")
    nonempty = _run_heredoc(
        _helper(),
        "PY_INSPECT_EMPTY_REFRESH_STAGE",
        str(stage),
        str(target),
        str(parent),
        str(os.getuid()),
        "inspect",
        "",
    )
    assert nonempty.returncode != 0
    assert "uncheckpointed refresh stage" in nonempty.stderr

    recovery = _helper()[
        _helper().index("recover_receipt() {") : _helper().index("cleanup() {")
    ]
    classify = recovery.index('elif [ "$phase" = "stage-pending" ]')
    stop = recovery.index('systemctl stop "$UNIT"')
    remove = recovery.index("remove_uncheckpointed_stage", stop)
    clear = recovery.index('clear_receipt "$receipt_transaction"', remove)
    assert classify < stop < remove < clear


def test_refresh_reconciles_exact_staged_inode_scratch_before_recovery(
    tmp_path: Path,
) -> None:
    payload, target, checkout, _backup = _valid_receipt(tmp_path)
    transaction = str(payload["transaction"])
    stage = Path(str(payload["stage"]))
    stage.mkdir(parents=True)
    metadata = stage.stat()
    payload["phase"] = "stage-pending"
    receipt = tmp_path / "receipt.json"
    receipt.write_text(json.dumps(payload) + "\n", encoding="utf-8")
    receipt.chmod(0o600)
    pending = copy.deepcopy(payload)
    pending["phase"] = "staged"
    pending["new_dev"] = metadata.st_dev
    pending["new_ino"] = metadata.st_ino
    scratch = receipt.with_name(f".{receipt.name}.{transaction}.tmp")
    scratch.write_text(json.dumps(pending) + "\n", encoding="utf-8")
    scratch.chmod(0o600)

    reconciled = _run_heredoc(
        _helper(),
        "PY_RECONCILE_REFRESH_SCRATCH",
        str(receipt),
        str(os.getuid()),
    )

    assert reconciled.returncode == 0, reconciled.stderr
    assert not scratch.exists()
    adopted = json.loads(receipt.read_text(encoding="utf-8"))
    assert adopted["phase"] == "staged"
    assert adopted["new_dev"] == metadata.st_dev
    assert adopted["new_ino"] == metadata.st_ino
    validated = _validate_receipt(receipt, target=target, checkout=checkout)
    assert validated.returncode == 0, validated.stderr

    update = _helper()[
        _helper().index("update_receipt() {") : _helper().index(
            "reconcile_receipt_scratch() {"
        )
    ]
    scratch_file_fsync = update.index("os.fsync(fd)")
    scratch_parent_fsync = update.index("os.fsync(dirfd)", scratch_file_fsync)
    publish = update.index("os.replace(tmp, path)", scratch_parent_fsync)
    published_parent_fsync = update.index("os.fsync(dirfd)", publish)
    assert scratch_file_fsync < scratch_parent_fsync < publish
    assert publish < published_parent_fsync


def test_malformed_receipt_matrix_fails_before_recovery_mutation(
    tmp_path: Path,
) -> None:
    base, target, checkout, _backup = _valid_receipt(tmp_path)
    malformed: list[tuple[str, dict[str, object]]] = []

    def changed(name: str, **updates: object) -> None:
        payload = copy.deepcopy(base)
        payload.update(updates)
        malformed.append((name, payload))

    changed("schema-bool", schema=True)
    changed("transaction-short", transaction="a" * 31)
    changed(
        "backup-other-transaction",
        backup=str(tmp_path / "opt" / f".venv-backup-{'c' * 32}"),
    )
    changed("phase-unknown", phase="mystery")
    changed("start-after-int", start_after=1)
    changed("previous-pair-incoherent", previous_tree=None)
    changed("original-inode-missing", original_ino=None)
    changed("new-inode-too-early", new_dev=10, new_ino=30)
    changed("stage-other-transaction", stage=str(tmp_path / "opt" / ".other"))
    changed("preinstall-new-tree", new_tree="6" * 64)
    changed("installed-tree-missing", phase="installed", new_tree=None)
    changed("expected-uppercase", expected_sha="A" * 40)
    changed("archive-malformed", archive_sha256="4" * 63)
    extra = copy.deepcopy(base)
    extra["unexpected"] = "value"
    malformed.append(("extra-key", extra))
    missing = copy.deepcopy(base)
    del missing["wheelhouse_sha256"]
    malformed.append(("missing-key", missing))

    for index, (name, payload) in enumerate(malformed):
        receipt = tmp_path / f"receipt-{index}.json"
        receipt.write_text(json.dumps(payload) + "\n", encoding="utf-8")
        receipt.chmod(0o600)
        result = _validate_receipt(receipt, target=target, checkout=checkout)
        assert result.returncode != 0, name

    duplicate = tmp_path / "receipt-duplicate.json"
    raw = json.dumps(base, separators=(",", ":"))
    duplicate.write_text(raw[:-1] + ',"schema":1}\n', encoding="utf-8")
    duplicate.chmod(0o600)
    assert _validate_receipt(
        duplicate, target=target, checkout=checkout
    ).returncode != 0

    oversized = tmp_path / "receipt-oversized.json"
    oversized.write_text(json.dumps(base) + " " * 17000, encoding="utf-8")
    oversized.chmod(0o600)
    assert _validate_receipt(
        oversized, target=target, checkout=checkout
    ).returncode != 0

    script = _helper()
    recovery = script[
        script.index("recover_receipt() {") : script.index("cleanup() {")
    ]
    validation_end = recovery.index("verify_unit_contract")
    before_validation = recovery[:validation_end]
    assert before_validation.index("load_receipt") < before_validation.index(
        '[ "$backup" = "$OPT_ROOT/.venv-backup-$receipt_transaction" ]'
    )
    assert "systemctl " not in before_validation
    assert "mv --" not in before_validation
    assert "rm -" not in before_validation
    assert "clear_receipt" not in before_validation


def test_dangling_symlink_backup_is_rejected_before_recovery(tmp_path: Path) -> None:
    payload, target, checkout, backup = _valid_receipt(tmp_path)
    target.parent.mkdir(parents=True)
    backup.symlink_to(tmp_path / "missing-backup")
    receipt = tmp_path / "receipt.json"
    receipt.write_text(json.dumps(payload) + "\n", encoding="utf-8")
    receipt.chmod(0o600)

    result = _validate_receipt(receipt, target=target, checkout=checkout)

    assert result.returncode != 0
    assert backup.is_symlink()
    assert not target.exists()


def test_committed_phase_is_durable_before_backup_cleanup() -> None:
    script = _helper()

    barrier = script.index('fsync_tree_no_follow "$OPT_VENV"')
    commit = script.index('update_receipt "$transaction" "committed"')
    cleanup = script.index('rm -rf -- "$backup"', commit)
    clear = script.index("clear_receipt", cleanup)

    assert barrier < commit < cleanup < clear
    assert "os.fwalk(" in script
    assert "follow_symlinks=False" in script
    assert "installed venv contains a non-root-owned entry" in script
    assert "installed venv contains an unsafe directory" in script
    assert 'flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)' in script
    assert "os.fsync(fd)" in script
    assert "os.fsync(dirfd)" in script
    assert 'fsync_dir "$OPT_ROOT"' in script
    assert 'if [ "$phase" = "committed" ]; then' in script
    assert 'rpc_proof "$receipt_expected" "$new_tree"' in script


def test_runtime_fsync_rejects_a_world_writable_payload(tmp_path: Path) -> None:
    runtime = tmp_path / "venv"
    runtime.mkdir(mode=0o755)
    payload = runtime / "daemon.py"
    payload.write_text("root daemon code\n", encoding="utf-8")
    payload.chmod(0o666)

    rejected = _run_heredoc(
        _helper(),
        "PY_FSYNC_TREE",
        str(runtime),
        str(Path(sys.executable).resolve()),
        str(os.geteuid()),
        "0",
    )
    assert rejected.returncode != 0
    assert "writable regular file" in rejected.stderr

    payload.chmod(0o644)
    accepted = _run_heredoc(
        _helper(),
        "PY_FSYNC_TREE",
        str(runtime),
        str(Path(sys.executable).resolve()),
        str(os.geteuid()),
        "0",
    )
    assert accepted.returncode == 0, accepted.stderr


def test_dry_run_never_recovers_an_interrupted_receipt() -> None:
    script = _helper()
    dry_branch = script.index('if [ "$dry_run" = "1" ]; then')
    observational_else = script.index("else", dry_branch)
    recovery = script.index("recover_receipt \\", observational_else)
    source_observation = script.index("command -v runuser", recovery)

    assert "recover_receipt" not in script[dry_branch:observational_else]
    assert "dry-run is read-only" in script[dry_branch:observational_else]
    assert dry_branch < observational_else < recovery < source_observation


def test_privileged_runtime_uses_only_commit_locked_dependency_wheels() -> None:
    script = _helper()
    lock = _text(RUNTIME_LOCK)

    assert 'RUNTIME_LOCK_REL="vibe-queue/contrib/' in script
    assert '"$builder/bin/pip" wheel' not in script
    assert "PY_BUILD_VQ_WHEEL" in script
    assert '"$RUNUSER" -u "$source_user"' in script
    assert "--only-binary=:all: --require-hashes --no-deps" in script
    assert "PY_SEAL_RUNTIME_WHEELS" in script
    assert "dependency wheel contains an unsafe archive member" in script
    assert "dependency wheel does not match the accepted version/hash" in script
    assert '"$OPT_VENV/bin/pip" install' in script
    assert '--no-index --no-deps "$wheelhouse"/*.whl' in script
    assert lock.count("==") == 6
    assert lock.count("--hash=sha256:") == 8
    for token in lock.split():
        if token.startswith("--hash=sha256:"):
            assert len(token.removeprefix("--hash=sha256:")) == 64


def test_exact_unit_quiescence_and_strict_post_rpc_proof_are_required() -> None:
    script = _helper()

    assert '[ "$fragment" = "$UNIT_FILE" ]' in script
    assert 'stat -c \'%u:%a\' "$UNIT_FILE"' in script
    assert 'path=$OPT_VQ ; argv[]=$OPT_VQ daemon run ;' in script
    assert '[ "$environment" = "VQ_CONFIG_DIR=/etc/vq" ]' in script
    assert "DropInPaths" in script
    assert "ExecStartPre ExecStartPost ExecReload" in script
    assert "EnvironmentFiles PassEnvironment RootDirectory" in script
    assert 'systemctl stop "$UNIT"' in script
    assert 'MainPID --value' in script
    assert 'ActiveState --value' in script
    assert 'SubState --value' in script
    assert '[ "$active_state" = "inactive" ]' in script
    assert '[ "$sub_state" = "dead" ]' in script
    assert 'systemctl is-active --quiet "$UNIT"' in script
    assert 'identity.get("source_sha") == want_sha' in script
    assert 'identity.get("source_tree_sha256") == want_tree' in script
    assert 'identity.get("euid") == 0' in script
    assert 'identity.get("python_executable") == want_python' in script
    assert 'identity.get("argv") == [want_vq, "daemon", "run"]' in script
    assert 'service.get("main_pid") == pid' in script
    assert 'service.get("executable") == want_vq' in script
    assert '[ "$current_main_pid" = "$proven_pid" ]' in script


def _rpc_validator() -> str:
    function = _helper()
    function = function[
        function.index("rpc_proof() {") : function.index("recover_receipt() {")
    ]
    marker = "-I -S -c '\n"
    start = function.index(marker) + len(marker)
    end = function.index("\n' \"$want_sha\"", start)
    return function[start:end]


def _rpc_envelope(*, process_pid: int, service_pid: int) -> dict[str, object]:
    vq = "/opt/vq/venv/bin/vq"
    return {
        "ok": True,
        "process_identity": {
            "status": "ok",
            "pid": process_pid,
            "euid": 0,
            "python_executable": "/opt/vq/venv/bin/python",
            "argv": [vq, "daemon", "run"],
            "multi_user": True,
            "socket_path": "/var/lib/vq/daemon.sock",
            "source_sha": "1" * 40,
            "source_tree_sha256": "2" * 64,
        },
        "system_service": {
            "status": "ok",
            "main_pid": service_pid,
            "active_state": "active",
            "sub_state": "running",
            "user": "root",
            "executable": vq,
            "argv": [vq, "daemon", "run"],
        },
    }


def _run_rpc_validator(payload: dict[str, object]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            "-I",
            "-S",
            "-c",
            _rpc_validator(),
            "1" * 40,
            "2" * 64,
            "/opt/vq/venv/bin/python",
            "/opt/vq/venv/bin/vq",
            "/var/lib/vq/daemon.sock",
        ],
        input=json.dumps(payload),
        capture_output=True,
        text=True,
        check=False,
    )


def test_rpc_same_provenance_from_the_wrong_process_is_rejected() -> None:
    exact = _run_rpc_validator(_rpc_envelope(process_pid=4242, service_pid=4242))
    wrong_process = _run_rpc_validator(
        _rpc_envelope(process_pid=4343, service_pid=4242)
    )

    assert exact.returncode == 0, exact.stderr
    assert exact.stdout.strip() == "4242"
    assert wrong_process.returncode != 0
    assert "exact root systemd MainPID" in wrong_process.stderr


def test_marker_and_tree_are_read_back_before_unit_start() -> None:
    script = _helper()

    marker = script.index('source-sha --write-marker "$expected_sha"')
    readback = script.index('installed_identity "$OPT_VQ"', marker)
    start = script.index('systemctl start "$UNIT"', readback)
    assert marker < readback < start


def test_installed_tree_must_equal_the_explicit_commit_tree() -> None:
    script = _helper()

    expected = script.index('expected_tree="$(PYTHONNOUSERSITE=1')
    installed = script.index('installed_tree="$("$OPT_VQ" source-tree-sha256)"')
    comparison = script.index('[ "$installed_tree" = "$expected_tree" ]')
    marker = script.index('source-sha --write-marker "$expected_sha"')
    assert expected < installed < comparison < marker


def test_expected_tree_sealer_executes_and_rejects_symlinks(tmp_path: Path) -> None:
    package = tmp_path / "vq"
    package.mkdir()
    first = package / "alpha.py"
    second = package / "nested" / "beta.txt"
    second.parent.mkdir()
    first.write_bytes(b"alpha\n")
    second.write_bytes(b"beta\n")

    sealed = _run_heredoc(
        _helper(),
        "PY_EXPECTED_TREE",
        str(package),
        str(os.geteuid()),
    )
    assert sealed.returncode == 0, sealed.stderr
    digest = hashlib.sha256()
    for path in (first, second):
        digest.update(path.relative_to(package).as_posix().encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    assert sealed.stdout.strip() == digest.hexdigest()

    (package / "link.py").symlink_to(first)
    rejected = _run_heredoc(
        _helper(),
        "PY_EXPECTED_TREE",
        str(package),
        str(os.geteuid()),
    )
    assert rejected.returncode != 0
    assert "symlink or special file" in rejected.stderr


def test_bootstrap_uses_the_same_transaction_and_requires_absent_target() -> None:
    script = _helper()

    assert "--bootstrap" in script
    assert '"--bootstrap requires an absent $OPT_VENV"' in script
    assert 'write_initial_receipt "$transaction"' in script
    assert 'systemctl start "$UNIT"' in script
    assert 'rpc_proof "$expected_sha" "$installed_tree"' in script


def test_passwordless_source_build_rule_is_retired() -> None:
    active = [
        line
        for line in _text(SUDOERS).splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]
    assert active == []
    assert "DO NOT INSTALL" in _text(SUDOERS)
    assert 'sudo rm -f -- "$SUDOERS_FILE"' in _deploy()
    assert "--grant-nopasswd" not in _deploy()


def test_deploy_seals_exact_bootstrap_files_and_installs_root_owned_copies() -> None:
    script = _deploy()

    assert "--expected-sha must be a full 40-hex accepted commit" in script
    assert "GIT_OPTIONAL_LOCKS=0" in script
    assert 'archive --format=tar "$expected_sha"' in script
    assert '| sudo tee "$bootstrap_lock_archive" >/dev/null' in script
    assert 'bootstrap_helper="$root_work/vibe-toolset-lifecycle-lock.sh"' in script
    assert 'lock_source="$bootstrap_helper"' in script
    assert 'lock_source="$lock_helper_bin"' not in script
    assert 'lifecycle_action="vq-multi-user-deploy"' in script
    assert 'atomic_install "$snapshot/vibe-queue/contrib/vq-multi-user-refresh"' in script
    assert 'atomic_install "$snapshot/scripts/_lifecycle_lock.sh"' in script
    assert 'exec 198>>"$opt_root/.bootstrap-surface.lock"' in script
    assert "/usr/bin/flock -x 198" in script
    assert 'LOCK_HELPER_BIN="/opt/vq/libexec/vibe-toolset-lifecycle-lock.sh"' in script
    assert "GIT_NO_REPLACE_OBJECTS=1" in script
    assert "core.attributesFile=/dev/null" in script


def test_deploy_bootstraps_and_refreshes_only_through_the_helper() -> None:
    script = _deploy()

    assert 'sudo "$HELPER_BIN" --checkout "$REPO" --expected-sha "$expected_sha"' in script
    assert "--bootstrap" in script
    assert "existing $OPT_VENV must be refreshed explicitly" in script
    assert '"$OPT_VENV/bin/pip" install' not in script
    assert 'python3 -m venv "$OPT_VENV"' not in script
    quiesce = script.index("Fence, drain, and quiesce the single-user runtime")
    activation = script.index("Transactional exact-SHA runtime activation")
    helper = script.index(
        'sudo "$HELPER_BIN" --checkout "$REPO" --expected-sha "$expected_sha"',
        activation,
    )
    assert quiesce < activation < helper


def test_deploy_admission_fence_and_quiescence_precede_root_activation() -> None:
    script = _deploy()
    calls = script.index('say "3/7  Fence, drain, and quiesce')
    acquire = script.index("acquire_user_admission_fence \\", calls)
    prove = script.index("prove_user_admission_fence \\", acquire)
    first_zero = script.index("wait_for_user_queue_empty \\", prove)
    directory_fence = script.index("fence_user_queue_directory \\", first_zero)
    stop = script.index("stop_and_prove_user_daemon \\", directory_fence)
    second_zero = script.index('user_nonterminal_count)" = "0"', stop)
    activation = script.index('say "5/7  Transactional exact-SHA', second_zero)
    helper = script.index('sudo "$HELPER_BIN"', activation)

    assert acquire < prove < first_zero < directory_fence
    assert directory_fence < stop < second_zero < helper
    assert 'active_state" = "inactive"' in script
    assert '[ "$main_pid" = "0" ]' in script
    assert "reject_submits=True" in script
    assert 'update_mode="deny"' in script
    assert "full_dispatch=True" in script
    assert "release_owned_full_drain(" in script
    assert 'expected_reason=sys.argv[1]' in script
    assert 'expected_set_at=sys.argv[2]' in script
    assert "summary localhost" not in script


def test_deploy_checkpoints_every_process_death_boundary() -> None:
    script = _deploy()
    intent = script.index("write_migration_intent \\")
    main = script.index('say "3/7  Fence, drain, and quiesce', intent)
    drain = script.index("acquire_user_admission_fence \\", intent)
    drain_done = script.index(
        "update_migration_receipt intent drained", drain
    )
    queue_intent = script.index(
        "update_migration_receipt drained queue-intent", drain_done
    )
    queue_stage = script.index("prepare_user_queue_blocker_stage \\", queue_intent)
    stage_bound = script.index(
        "update_migration_receipt queue-intent queue-staged", queue_stage
    )
    queue_fence = script.index("fence_user_queue_directory \\", stage_bound)
    queue_bound = script.index(
        "update_migration_receipt queue-staged queue-fenced", queue_fence
    )
    stop_intent = script.index(
        "update_migration_receipt queue-fenced daemon-stop-intent", queue_bound
    )
    stop = script.index("stop_and_prove_user_daemon \\", stop_intent)
    config_intent = script.index(
        "update_migration_receipt daemon-stopped config-intent", stop
    )
    root_intent = script.index(
        "update_migration_receipt config-ready root-intent", config_intent
    )
    helper = script.index('sudo "$HELPER_BIN"', root_intent)
    root_done = script.index(
        "update_migration_receipt root-intent root-active", helper
    )
    restore = script.index("restore_user_queue_directory \\", root_done)
    restore_done = script.index(
        "update_migration_receipt root-active queue-restored", restore
    )
    release = script.index("release_user_admission_fence \\", restore_done)
    clear = script.index("clear_migration_receipt \\", release)

    assert intent < main < drain < drain_done < queue_intent < queue_stage
    assert queue_stage < stage_bound < queue_fence < queue_bound < stop_intent
    assert stop_intent < stop < config_intent < root_intent
    assert root_intent < helper < root_done < restore < restore_done < release < clear
    assert "recover_pending_migration" in script
    assert "rollback_pending_migration" in script
    assert "finish_pending_migration" in script
    assert "migration receipt is bound to another operation" in script
    assert "migration receipt does not have the exact schema" in script


def test_daemon_stopped_receipt_is_not_misclassified_as_config_intent() -> None:
    script = _deploy()
    phase_function = script[
        script.index("phase_at_least() {") : script.index(
            "build_prospective_config() {"
        )
    ]
    probe = subprocess.run(
        [
            "bash",
            "-c",
            phase_function
            + "\nmigration_phase=daemon-stopped\n"
            + "if phase_at_least config-intent; then exit 9; fi\n"
            + "migration_phase=config-intent\nphase_at_least config-intent\n",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert probe.returncode == 0, probe.stderr


def test_successful_preexisting_rollback_exits_before_a_fresh_drain(
    tmp_path: Path,
) -> None:
    script = _deploy()
    start = script.index('if [ "$migration_preexisting" = "1" ]; then')
    end = script.index('if [ -e "$OPT_VENV" ]', start)
    recovery_block = script[start:end]
    recovered = tmp_path / "recovered"
    fresh = tmp_path / "fresh"
    probe = subprocess.run(
        [
            "bash",
            "-c",
            "migration_preexisting=1\n"
            + f"recovered={recovered!s}\nfresh={fresh!s}\n"
            + "recover_pending_migration() { : >\"$recovered\"; }\n"
            + "die() { exit 99; }\n"
            + recovery_block
            + '\n: >"$fresh"\n',
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert probe.returncode == 0, probe.stderr
    assert recovered.exists()
    assert not fresh.exists()


def test_migration_operation_lock_excludes_a_second_deploy(
    tmp_path: Path,
) -> None:
    script = _deploy()
    repo = tmp_path / "repo"
    (repo / ".git").mkdir(parents=True)
    lock = repo / ".git" / "vq-multi-user-bootstrap.operation.lock"
    prepared = _run_heredoc(
        script,
        "PY_PREPARE_MIGRATION_OPERATION_LOCK",
        str(lock),
        str(repo),
        str(os.getuid()),
    )
    assert prepared.returncode == 0, prepared.stderr
    code = tmp_path / "lock-operation.py"
    code.write_text(
        _heredoc(script, "PY_LOCK_MIGRATION_OPERATION"), encoding="utf-8"
    )
    ready = tmp_path / "ready"
    holder = subprocess.Popen(
        [
            "bash",
            "-c",
            'set -e; exec 196<>"$1"; "$2" -I -S "$3" "$1" "$4" "$5" '
            '196>&196; : >"$6"; sleep 30',
            "bash",
            str(lock),
            sys.executable,
            str(code),
            str(repo),
            str(os.getuid()),
            str(ready),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        for _ in range(100):
            if ready.exists():
                break
            if holder.poll() is not None:
                break
            time.sleep(0.01)
        assert ready.exists(), holder.stderr.read() if holder.stderr else ""
        contender = subprocess.run(
            [
                "bash",
                "-c",
                'exec 196<>"$1"; "$2" -I -S "$3" "$1" "$4" "$5" 196>&196',
                "bash",
                str(lock),
                sys.executable,
                str(code),
                str(repo),
                str(os.getuid()),
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        assert contender.returncode != 0
        assert contender.stdout.strip() == "busy"
    finally:
        holder.terminate()
        holder.communicate(timeout=10)

    cleanup = script[script.index("cleanup() {") : script.index("user_python() {")]
    assert '[ "$migration_operation_locked" = "1" ]' in cleanup
    assert '[ "$migration_recovery_adopted" = "1" ]' in cleanup
    assert script.index("acquire_migration_operation_lock \\") < script.index(
        "systemctl disable --now 'vq-admin-auto-update@*.timer'"
    )


def test_active_deploy_receipt_allows_only_strict_trusted_handoffs(
    tmp_path: Path,
) -> None:
    deploy = _deploy()
    refresh = _helper()
    expected_sha = "1" * 40
    transaction = "ab" * 16
    repo = tmp_path / "repo"
    (repo / ".git").mkdir(parents=True)
    user_venv = repo / "vibe-queue" / ".venv"
    user_venv.mkdir(parents=True)
    queue = tmp_path / ".local" / "share" / "vq" / "queue"
    queue.mkdir(parents=True)
    queue_stat = queue.stat()
    user_vq = user_venv / "bin" / "vq"
    payload: dict[str, object] = {
        "schema": 1,
        "transaction": transaction,
        "phase": "root-intent",
        "expected_sha": expected_sha,
        "repo": str(repo),
        "user_venv": str(user_venv),
        "user_uid": os.getuid(),
        "drain_path": str(tmp_path / ".local" / "share" / "vq" / "drain.json"),
        "drain_reason": f"vq-multi-user-bootstrap:{expected_sha}:{transaction}",
        "drain_set_at": "2026-08-13T20:00:00+00:00",
        "user_was_active": True,
        "user_daemon_identity": {
            "active": True,
            "exec_start_argv": [str(user_vq), "daemon", "run"],
            "exec_start_path": str(user_vq),
            "fragment_path": str(
                tmp_path / ".config" / "systemd" / "user" / "vq-daemon.service"
            ),
            "initial_main_pid": 4242,
            "multi_user": False,
            "python_executable": str(user_venv / "bin" / "python"),
            "socket_path": str(
                tmp_path / ".local" / "share" / "vq" / "daemon.sock"
            ),
            "source_sha": expected_sha,
            "source_tree_sha256": "2" * 64,
            "user_uid": os.getuid(),
        },
        "queue_path": str(queue),
        "queue_existed": True,
        "queue_dev": queue_stat.st_dev,
        "queue_ino": queue_stat.st_ino,
        "queue_mode": 0o700,
        "queue_fenced_mode": 0o500,
        "queue_fenced_dev": queue_stat.st_dev,
        "queue_fenced_ino": queue_stat.st_ino,
        "queue_stage_path": None,
        "queue_stage_dev": None,
        "queue_stage_ino": None,
        "config_created": False,
        "config_sha256": None,
        "root_tree_sha256": None,
    }
    receipt = repo / ".git" / "vq-multi-user-bootstrap.json"
    receipt.write_text(json.dumps(payload) + "\n", encoding="utf-8")
    receipt.chmod(0o600)

    refresh_handoff = _run_heredoc(
        refresh,
        "PY_DEPLOY_HANDOFF_RECEIPT",
        str(receipt),
        str(repo),
        expected_sha,
        str(os.getuid()),
        str(tmp_path),
    )
    root_handoff = _run_heredoc(
        deploy,
        "PY_ROOT_BOOTSTRAP_RECEIPT",
        str(receipt),
        transaction,
        str(repo),
        expected_sha,
        str(os.getuid()),
    )
    assert refresh_handoff.returncode == 0, refresh_handoff.stderr
    assert root_handoff.returncode == 0, root_handoff.stderr

    command = (
        '. "$1"; vibe_toolset_acquire_lifecycle_lock '
        '"$2" "$3" "$4" "$5"'
    )
    ordinary = subprocess.run(
        [
            "bash",
            "-c",
            command,
            "bash",
            str(LIFECYCLE_HELPER),
            sys.executable,
            str(repo),
            str(user_venv),
            "vq-multi-user-refresh",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    deploy_handoff = subprocess.run(
        [
            "bash",
            "-c",
            command,
            "bash",
            str(LIFECYCLE_HELPER),
            sys.executable,
            str(repo),
            str(user_venv),
            "vq-multi-user-deploy",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert ordinary.returncode != 0
    assert "durable multi-user migration admission is active" in ordinary.stderr
    assert deploy_handoff.returncode == 0, deploy_handoff.stderr

    payload["phase"] = "config-ready"
    receipt.write_text(json.dumps(payload) + "\n", encoding="utf-8")
    wrong_phase = _run_heredoc(
        refresh,
        "PY_DEPLOY_HANDOFF_RECEIPT",
        str(receipt),
        str(repo),
        expected_sha,
        str(os.getuid()),
        str(tmp_path),
    )
    assert wrong_phase.returncode != 0
    acquire = refresh.index("vibe_toolset_acquire_lifecycle_lock \\")
    assert refresh.index("validate_deploy_handoff_receipt \\", acquire) > acquire


def test_user_daemon_identity_binds_exact_unit_rpc_and_main_pid(
    tmp_path: Path,
) -> None:
    script = _deploy()
    expected_sha = "1" * 40
    expected_tree = "2" * 64
    venv = tmp_path / "repo" / "vibe-queue" / ".venv"
    vq = venv / "bin" / "vq"
    python = venv / "bin" / "python"
    fragment = tmp_path / ".config" / "systemd" / "user" / "vq-daemon.service"
    fragment.parent.mkdir(parents=True)
    fragment.write_text("[Service]\n", encoding="utf-8")
    fragment.chmod(0o644)
    socket = tmp_path / ".local" / "share" / "vq" / "daemon.sock"
    snapshot = "\n".join(
        (
            "Id=vq-daemon.service",
            "LoadState=loaded",
            "ActiveState=active",
            "SubState=running",
            "MainPID=4242",
            f"FragmentPath={fragment}",
            f"ExecStart={{ path={vq} ; argv[]={vq} daemon run ; ignore_errors=no ; }}",
            "DropInPaths=",
            "Environment=",
            "ExecCondition=",
            "ExecStartPre=",
            "ExecStartPost=",
            "ExecReload=",
            "ExecStop=",
            "ExecStopPost=",
            "EnvironmentFiles=",
            "PassEnvironment=",
            "RootDirectory=",
        )
    )
    manager_environment = f"HOME={tmp_path}\nPATH=/usr/bin:/bin"
    ping = {
        "ok": True,
        "source_sha": expected_sha,
        "source_tree_sha256": expected_tree,
        "multi_user": False,
        "socket_path": str(socket),
        "version": "0.1",
        "process_identity": {
            "status": "ok",
            "pid": 4242,
            "euid": os.getuid(),
            "python_executable": str(python),
            "argv": [str(vq), "daemon", "run"],
            "source_sha": expected_sha,
            "source_tree_sha256": expected_tree,
            "multi_user": False,
            "socket_path": str(socket),
            "version": "0.1",
        },
    }

    def prove(payload: dict[str, object]) -> subprocess.CompletedProcess[str]:
        return _run_heredoc(
            script,
            "PY_USER_DAEMON_IDENTITY",
            snapshot,
            snapshot,
            manager_environment,
            manager_environment,
            json.dumps(payload, separators=(",", ":")),
            "1",
            str(os.getuid()),
            str(vq),
            str(python),
            expected_sha,
            expected_tree,
            str(socket),
            str(fragment),
            str(tmp_path),
        )

    exact = prove(ping)
    assert exact.returncode == 0, exact.stderr
    identity = json.loads(exact.stdout)
    assert identity["initial_main_pid"] == 4242
    wrong = copy.deepcopy(ping)
    wrong["process_identity"]["pid"] = 4343  # type: ignore[index]
    rejected = prove(wrong)
    assert rejected.returncode != 0
    assert "exact systemd MainPID" in rejected.stderr

    inactive = snapshot.replace("ActiveState=active", "ActiveState=inactive")
    inactive = inactive.replace("SubState=running", "SubState=dead")
    inactive = inactive.replace("MainPID=4242", "MainPID=0")
    alternate_state = inactive.replace(
        "\nEnvironment=\n", "\nEnvironment=VQ_STATE_DIR=/alternate/vq\n"
    )
    rejected_state = _run_heredoc(
        script,
        "PY_USER_DAEMON_IDENTITY",
        alternate_state,
        alternate_state,
        manager_environment,
        manager_environment,
        "",
        "0",
        str(os.getuid()),
        str(vq),
        str(python),
        expected_sha,
        expected_tree,
        str(socket),
        str(fragment),
        str(tmp_path),
    )
    assert rejected_state.returncode != 0
    assert "unsupported Environment" in rejected_state.stderr

    alternate_manager = manager_environment + "\nVQ_STATE_DIR=/alternate/vq"
    rejected_manager = _run_heredoc(
        script,
        "PY_USER_DAEMON_IDENTITY",
        inactive,
        inactive,
        alternate_manager,
        alternate_manager,
        "",
        "0",
        str(os.getuid()),
        str(vq),
        str(python),
        expected_sha,
        expected_tree,
        str(socket),
        str(fragment),
        str(tmp_path),
    )
    assert rejected_manager.returncode != 0
    assert "unsupported VQ_STATE_DIR" in rejected_manager.stderr

    suffix = inactive.replace(
        f"argv[]={vq} daemon run ;",
        f"argv[]={vq} daemon run --web ;",
    )
    rejected_suffix = _run_heredoc(
        script,
        "PY_USER_DAEMON_IDENTITY",
        suffix,
        suffix,
        manager_environment,
        manager_environment,
        "",
        "0",
        str(os.getuid()),
        str(vq),
        str(python),
        expected_sha,
        expected_tree,
        str(socket),
        str(fragment),
        str(tmp_path),
    )
    assert rejected_suffix.returncode != 0
    assert "does not execute the serving venv" in rejected_suffix.stderr


def test_terminal_recovery_proves_exact_root_runtime_before_receipt_clear() -> None:
    script = _deploy()
    sha = "1" * 40
    tree = "2" * 64
    vq = "/opt/vq/venv/bin/vq"
    python = "/opt/vq/venv/bin/python"
    socket = "/var/lib/vq/daemon.sock"
    envelope = {
        "ok": True,
        "source_sha": sha,
        "source_tree_sha256": tree,
        "process_identity": {
            "status": "ok",
            "pid": 4242,
            "euid": 0,
            "python_executable": python,
            "argv": [vq, "daemon", "run"],
            "multi_user": True,
            "socket_path": socket,
            "source_sha": sha,
            "source_tree_sha256": tree,
        },
        "system_service": {
            "status": "ok",
            "main_pid": 4242,
            "active_state": "active",
            "sub_state": "running",
            "user": "root",
            "executable": vq,
            "argv": [vq, "daemon", "run"],
        },
    }
    exact = _run_heredoc(
        script,
        "PY_ROOT_RUNTIME_PROOF",
        sha,
        tree,
        python,
        vq,
        socket,
        json.dumps(envelope, separators=(",", ":")),
    )
    assert exact.returncode == 0, exact.stderr
    wrong = copy.deepcopy(envelope)
    wrong["process_identity"]["source_tree_sha256"] = "3" * 64  # type: ignore[index]
    rejected = _run_heredoc(
        script,
        "PY_ROOT_RUNTIME_PROOF",
        sha,
        tree,
        python,
        vq,
        socket,
        json.dumps(wrong, separators=(",", ":")),
    )
    assert rejected.returncode != 0

    finish = script[
        script.index("finish_pending_migration() {") : script.index(
            "recover_pending_migration() {"
        )
    ]
    enable = finish.index('sudo systemctl enable "$UNIT"')
    prove = finish.index("prove_root_runtime", enable)
    clear = finish.index("clear_migration_receipt", prove)
    assert enable < prove < clear


def test_owned_single_user_drain_refuses_replacement_and_releases_exactly(
    tmp_path: Path,
) -> None:
    script = _deploy()
    accepted_sha = "1" * 40
    reason = f"vq-multi-user-bootstrap:{accepted_sha}:{'a' * 32}"
    set_at = "2026-08-13T20:00:00+00:00"

    acquired = _run_heredoc(
        script,
        "PY_ADMISSION_ACQUIRE",
        reason,
        set_at,
        home=tmp_path,
    )
    assert acquired.returncode == 0, acquired.stderr
    reason, set_at = acquired.stdout.splitlines()
    drain_path = tmp_path / ".local" / "share" / "vq" / "drain.json"
    original = drain_path.read_text(encoding="utf-8")
    persisted = json.loads(original)
    assert persisted["reason"] == reason
    assert persisted["set_at"] == set_at
    assert persisted["reject_submits"] is True
    assert persisted["update_mode"] == "deny"
    assert persisted["full_dispatch"] is True

    collision = _run_heredoc(
        script,
        "PY_ADMISSION_ACQUIRE",
        reason,
        set_at,
        home=tmp_path,
    )
    assert collision.returncode != 0
    assert drain_path.read_text(encoding="utf-8") == original

    wrong_release = _run_heredoc(
        script,
        "PY_ADMISSION_RELEASE",
        reason,
        set_at + "-replacement",
        "0",
        home=tmp_path,
    )
    assert wrong_release.returncode != 0
    assert drain_path.read_text(encoding="utf-8") == original

    released = _run_heredoc(
        script,
        "PY_ADMISSION_RELEASE",
        reason,
        set_at,
        "0",
        home=tmp_path,
    )
    assert released.returncode == 0, released.stderr
    assert not drain_path.exists()


def test_durable_migration_receipt_recovers_a_queue_fence_kill_window(
    tmp_path: Path,
) -> None:
    script = _deploy()
    expected_sha = "1" * 40
    user_tree = "2" * 64
    repo = tmp_path / "accepted-checkout"
    (repo / ".git").mkdir(parents=True)
    user_venv = repo / "vibe-queue" / ".venv"
    receipt = repo / ".git" / "vq-multi-user-bootstrap.json"
    queue = tmp_path / ".local" / "share" / "vq" / "queue"
    user_vq = user_venv / "bin" / "vq"
    identity = {
        "active": True,
        "exec_start_argv": [str(user_vq), "daemon", "run"],
        "exec_start_path": str(user_vq),
        "fragment_path": str(
            tmp_path / ".config" / "systemd" / "user" / "vq-daemon.service"
        ),
        "initial_main_pid": 4242,
        "multi_user": False,
        "python_executable": str(user_venv / "bin" / "python"),
        "socket_path": str(tmp_path / ".local" / "share" / "vq" / "daemon.sock"),
        "source_sha": expected_sha,
        "source_tree_sha256": user_tree,
        "user_uid": os.getuid(),
    }

    intent = _run_heredoc(
        script,
        "PY_MIGRATION_INTENT",
        str(receipt),
        expected_sha,
        str(repo),
        str(user_venv),
        "1",
        json.dumps(identity, separators=(",", ":")),
        home=tmp_path,
    )
    assert intent.returncode == 0, intent.stderr
    fields = intent.stdout.splitlines()
    transaction, reason, set_at = fields[:3]
    assert len(fields) == 10

    acquired = _run_heredoc(
        script,
        "PY_ADMISSION_ACQUIRE",
        reason,
        set_at,
        home=tmp_path,
    )
    assert acquired.returncode == 0, acquired.stderr

    def update(
        current: str,
        following: str,
        *,
        fenced_dev: str = "",
        fenced_ino: str = "",
        stage_dev: str = "",
        stage_ino: str = "",
    ) -> subprocess.CompletedProcess[str]:
        return _run_heredoc(
            script,
            "PY_MIGRATION_UPDATE",
            str(receipt),
            transaction,
            current,
            following,
            "0",
            "",
            "",
            fenced_dev,
            fenced_ino,
            stage_dev,
            stage_ino,
            home=tmp_path,
        )

    for current, following in (
        ("intent", "drained"),
        ("drained", "queue-intent"),
    ):
        updated = update(current, following)
        assert updated.returncode == 0, updated.stderr

    staged = _run_heredoc(
        script,
        "PY_QUEUE_BLOCKER_STAGE",
        fields[3],
        fields[9],
        str(receipt),
        transaction,
        str(repo),
        home=tmp_path,
    )
    assert staged.returncode == 0, staged.stderr
    stage_dev, stage_ino = staged.stdout.splitlines()
    stage_bound = update(
        "queue-intent",
        "queue-staged",
        stage_dev=stage_dev,
        stage_ino=stage_ino,
    )
    assert stage_bound.returncode == 0, stage_bound.stderr

    fenced = _run_heredoc(
        script,
        "PY_QUEUE_FENCE",
        fields[3],
        fields[4],
        fields[5],
        fields[6],
        fields[7],
        fields[8],
        fields[9],
        stage_dev,
        stage_ino,
        str(receipt),
        transaction,
        str(repo),
        home=tmp_path,
    )
    assert fenced.returncode == 0, fenced.stderr
    assert queue.is_file()
    assert stat.S_IMODE(queue.stat().st_mode) == 0o600
    fence_fields = fenced.stdout.splitlines()
    checkpointed = update(
        "queue-staged",
        "queue-fenced",
        fenced_dev=fence_fields[3],
        fenced_ino=fence_fields[4],
    )
    assert checkpointed.returncode == 0, checkpointed.stderr

    # Simulate SIGKILL here: only durable files remain. A new process validates
    # and adopts the exact receipt, restores the bound inode/mode, releases only
    # its reason+set_at drain, then durably clears the receipt.
    loaded = _run_heredoc(
        script,
        "PY_MIGRATION_LOAD",
        str(receipt),
        expected_sha,
        str(repo),
        str(user_venv),
        user_tree,
        home=tmp_path,
    )
    assert loaded.returncode == 0, loaded.stderr
    adopted = loaded.stdout.splitlines()
    assert adopted[0] == transaction
    assert adopted[1] == "queue-fenced"

    restored = _run_heredoc(
        script,
        "PY_QUEUE_RESTORE",
        adopted[5],
        adopted[14],
        adopted[15],
        adopted[9],
        adopted[10],
        str(1 - int(adopted[6])),
        adopted[7],
        adopted[8],
        str(receipt),
        transaction,
        adopted[11],
        adopted[12],
        adopted[13],
        str(repo),
        home=tmp_path,
    )
    assert restored.returncode == 0, restored.stderr
    assert not queue.exists()

    released = _run_heredoc(
        script,
        "PY_ADMISSION_RELEASE",
        adopted[2],
        adopted[3],
        "0",
        home=tmp_path,
    )
    assert released.returncode == 0, released.stderr
    cleared = _run_heredoc(
        script,
        "PY_MIGRATION_CLEAR",
        str(receipt),
        transaction,
        adopted[1],
        adopted[20],
        expected_sha,
        str(repo),
        str(user_venv),
        home=tmp_path,
    )
    assert cleared.returncode == 0, cleared.stderr
    assert not receipt.exists()
    assert not (tmp_path / ".local" / "share" / "vq" / "drain.json").exists()


@pytest.mark.skipif(os.geteuid() == 0, reason="root bypasses directory mode bits")
def test_queue_write_fence_closes_a_deterministic_submission_race(
    tmp_path: Path,
) -> None:
    script = _deploy()
    queue = tmp_path / ".local" / "share" / "vq" / "queue"
    queue.mkdir(parents=True, mode=0o700)
    metadata = queue.stat()
    gate = tmp_path / "release-racing-submit"
    racer = subprocess.Popen(
        [
            sys.executable,
            "-I",
            "-c",
            (
                "import os,pathlib,sys,time; "
                "queue=pathlib.Path(sys.argv[1]); gate=pathlib.Path(sys.argv[2]); "
                "\nwhile not gate.exists(): time.sleep(0.01)\n"
                "try:\n"
                " fd=os.open(queue/'raced.json', os.O_WRONLY|os.O_CREAT|os.O_EXCL, 0o600)\n"
                "except PermissionError: raise SystemExit(0)\n"
                "else: os.close(fd); raise SystemExit(9)\n"
            ),
            str(queue),
            str(gate),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    fenced = _run_heredoc(
        script,
        "PY_QUEUE_FENCE",
        str(queue),
        "1",
        str(metadata.st_dev),
        str(metadata.st_ino),
        "700",
        "500",
        "",
        "",
        "",
        str(tmp_path / "repo" / ".git" / "vq-multi-user-bootstrap.json"),
        "a" * 32,
        str(tmp_path / "repo"),
        home=tmp_path,
    )
    assert fenced.returncode == 0, fenced.stderr
    path, original_mode, fenced_mode, device, inode, created = (
        fenced.stdout.splitlines()
    )
    assert Path(path) == queue
    assert stat.S_IMODE(queue.stat().st_mode) == int(fenced_mode, 8)

    gate.touch()
    _stdout, stderr = racer.communicate(timeout=10)
    assert racer.returncode == 0, stderr
    assert not (queue / "raced.json").exists()

    restored = _run_heredoc(
        script,
        "PY_QUEUE_RESTORE",
        path,
        original_mode,
        fenced_mode,
        device,
        inode,
        created,
        str(metadata.st_dev),
        str(metadata.st_ino),
        str(tmp_path / "repo" / ".git" / "vq-multi-user-bootstrap.json"),
        "a" * 32,
        "",
        "",
        "",
        str(tmp_path / "repo"),
        home=tmp_path,
    )
    assert restored.returncode == 0, restored.stderr
    assert stat.S_IMODE(queue.stat().st_mode) == int(original_mode, 8)


def test_deploy_can_prepare_the_new_helper_before_required_dry_run() -> None:
    script = _deploy()

    assert "--prepare-only" in script
    prepare_exit = script.index('if [ "$prepare_only" = "1" ]; then')
    activation = script.index("Transactional exact-SHA runtime activation")
    assert prepare_exit < activation
    assert "without changing config" in script
    assert '--expected-sha $expected_sha --dry-run' in script


def test_deploy_disables_legacy_root_timer() -> None:
    script = _deploy()

    assert "systemctl disable --now 'vq-admin-auto-update@*.timer'" in script
    assert "systemctl stop 'vq-admin-auto-update@*.service'" in script
    assert "vq-admin-auto-update@.service" in script
    assert "vq-admin-auto-update@.timer" in script
    assert script.index("systemctl disable --now") < script.index(
        "git_observe -C \"$REPO\" archive"
    )


@pytest.mark.parametrize("path", [HELPER, DEPLOY])
def test_scripts_are_executable_and_bash_syntax_is_valid(path: Path) -> None:
    assert path.stat().st_mode & 0o111
    assert subprocess.run(
        ["bash", "-n", str(path)], capture_output=True, check=False
    ).returncode == 0


def test_help_exits_zero_and_names_the_transaction_contract() -> None:
    proc = _run("--help")

    assert proc.returncode == 0
    assert "full 40-hex" in proc.stdout
    assert "recovered" in proc.stdout


@pytest.mark.parametrize(
    ("args", "message"),
    [
        (("--bogus",), "unknown argument: --bogus"),
        (("--checkout",), "--checkout requires a non-empty argument"),
        (("--checkout", ""), "--checkout requires a non-empty argument"),
        (("--checkout", "--dry-run"), "--checkout requires a value, not option-like"),
        (("--expected-sha",), "--expected-sha requires a non-empty argument"),
        (("--expected-sha", ""), "--expected-sha requires a non-empty argument"),
        (
            ("--expected-sha", "--dry-run"),
            "--expected-sha requires a value, not option-like",
        ),
    ],
)
def test_bad_invocations_fail_closed(args: tuple[str, ...], message: str) -> None:
    proc = _run(*args)

    assert proc.returncode != 0
    assert message in proc.stderr
