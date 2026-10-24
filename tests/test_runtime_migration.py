"""Migration edits are recoverable bindings, never moves of a live environment."""
from __future__ import annotations

import json
import os
import shlex
import subprocess
import sys
from pathlib import Path

import pytest
from click.testing import CliRunner

from tests.test_runtime_execution_binding import runtimes as runtimes
from vq import admin, runtime_slots
from vq import runtime_migration as migration
from vq.cli import main


@pytest.fixture
def request_manifest(runtimes, tmp_path: Path):
    root, shas, program, _cfg = runtimes
    registries = []
    for index in range(2):
        directory = tmp_path / f"authority-{index}"
        directory.mkdir()
        path, candidate = directory / "config.toml", directory / "candidate.toml"
        before = "# Operator comment must survive rollback byte for byte.\n"
        after = before
        for name in ("demo", "demo-dev"):
            section = (f'[programs.{name}]\nkind = "venv"\n'
                       f'python = {json.dumps(program.python)}\n'
                       f'git_dir = {json.dumps(program.git_dir)}\n')
            before += section
            after += (section + f'runtime_slot_root = {json.dumps(str(root))}\n'
                      'runtime_slot_execution = true\n')
        path.write_text(before)
        candidate.write_text(after)
        registries.append(migration.Registry(path=str(path), candidate=str(candidate)))
    bootstrap = tmp_path / "retained-vq"
    bootstrap.write_text('#!/bin/sh\nexec ' + shlex.quote(sys.executable) + ' -m vq.cli "$@"\n')
    bootstrap.chmod(0o700)
    manifest = migration.Manifest(
        component="demo", program="demo", source_sha=shas[1], runtime_root=str(root),
        registries=registries,
        launchers=[migration.Launcher(path=str(tmp_path / "stable-python"),
                                     bootstrap=str(bootstrap))],
        external_consumers=[],
    )
    return manifest


def test_plan_is_read_only_and_inventory_groups_aliases(request_manifest, tmp_path: Path):
    before = sorted(str(p) for p in tmp_path.rglob("*"))
    value = migration.plan(request_manifest)
    assert value["status"] == "ready"
    assert value["blockers"] == []
    assert [r["aliases"] for r in value["registries"]] == [["demo", "demo-dev"]] * 2
    assert value["generation"]["sha"] == request_manifest.source_sha
    assert sorted(str(p) for p in tmp_path.rglob("*")) == before
    assert migration.plan(request_manifest)["plan_id"] == value["plan_id"]


def test_apply_launches_exact_generation_and_rollback_preserves_legacy(
    request_manifest, runtimes, tmp_path: Path,
):
    root, shas, program, _cfg = runtimes
    value = migration.plan(request_manifest)
    originals = [Path(r.path).read_bytes() for r in request_manifest.registries]
    receipt = tmp_path / "receipt"
    result = migration.apply(request_manifest, value["plan_id"], receipt)
    assert result["state"] == "applied"
    assert runtime_slots.resolve_current(root) == shas[1]
    assert runtime_slots._previous_sha(root) == shas[0]
    observed = subprocess.check_output(
        [request_manifest.launchers[0].path, "-c", "import sys; print(sys.executable)"],
        text=True, timeout=30,
    ).strip()
    assert observed == str(runtime_slots.slot_python(root, shas[1]))
    assert migration.recover(receipt)["state"] == "applied"
    assert migration.recover(receipt, rollback=True)["state"] == "rolled_back"
    assert migration.recover(receipt, rollback=True)["state"] == "rolled_back"
    assert runtime_slots.resolve_current(root) == shas[0]
    assert runtime_slots._previous_sha(root) is None
    assert [Path(r.path).read_bytes() for r in request_manifest.registries] == originals
    assert Path(program.python).exists()
    assert runtime_slots.slot_python(root, shas[1]).exists()
    assert not Path(request_manifest.launchers[0].path).exists()


@pytest.mark.parametrize("after_rename", [False, True])
@pytest.mark.parametrize("boundary", ["previous", "current", "stable-python", "config.toml"])
def test_interrupted_apply_resumes_or_rolls_back_without_guessing(
    request_manifest, runtimes, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    boundary: str, after_rename: bool,
):
    root, shas, _prog, _cfg = runtimes
    value = migration.plan(request_manifest)
    receipt = tmp_path / "receipt"
    replace = os.replace
    fired = False

    def interrupt(source, destination, **kwargs):
        nonlocal fired
        should_fire = str(destination) == boundary and not fired
        if should_fire and not after_rename:
            fired = True
            raise OSError("injected power loss before rename")
        result = replace(source, destination, **kwargs)
        if should_fire:
            fired = True
            raise OSError("injected power loss after rename")
        return result

    monkeypatch.setattr(os, "replace", interrupt)
    with pytest.raises(OSError, match="injected"):
        migration.apply(request_manifest, value["plan_id"], receipt)
    monkeypatch.setattr(os, "replace", replace)
    assert fired
    assert migration.recover(receipt)["state"] == "applied"
    assert runtime_slots.resolve_current(root) == shas[1]
    assert migration.recover(receipt, rollback=True)["state"] == "rolled_back"
    assert runtime_slots.resolve_current(root) == shas[0]


def test_rollback_foreign_edit_refuses_before_restoring_other_bindings(
    request_manifest, runtimes, tmp_path: Path,
):
    root, shas, _prog, _cfg = runtimes
    receipt = tmp_path / "receipt"
    migration.apply(request_manifest, migration.plan(request_manifest)["plan_id"], receipt)
    first = Path(request_manifest.registries[0].path)
    first.write_text(first.read_text() + "\n# Later operator edit\n")
    with pytest.raises(migration.MigrationError, match="foreign edit"):
        migration.recover(receipt, rollback=True)
    assert runtime_slots.resolve_current(root) == shas[1]
    assert first.read_text().endswith("# Later operator edit\n")
    assert "runtime_slot_execution" in Path(request_manifest.registries[1].path).read_text()


def test_stale_plan_and_replaced_identical_bytes_refuse(request_manifest, tmp_path: Path):
    value = migration.plan(request_manifest)
    path = Path(request_manifest.registries[0].path)
    replacement = path.with_name("replace")
    replacement.write_bytes(path.read_bytes())
    replacement.replace(path)
    with pytest.raises(migration.MigrationError, match="stale"):
        migration.apply(request_manifest, value["plan_id"], tmp_path / "receipt")
    assert not (tmp_path / "receipt").exists()


@pytest.mark.parametrize("change", ["alias", "unrelated", "profile"])
def test_conflicting_candidates_refuse(request_manifest, change: str):
    registry = request_manifest.registries[0]
    path = Path(registry.candidate)
    if change == "alias":
        path.write_text(path.read_text().replace("runtime_slot_execution = true", "", 1))
    elif change == "unrelated":
        path.write_text('max_cpus = 3\n' + path.read_text())
    else:
        old = Path(registry.path)
        old.write_text(old.read_text() + 'branch = "other"\n')
    with pytest.raises(migration.MigrationError, match="candidate|conflicting profiles"):
        migration.plan(request_manifest)


@pytest.mark.parametrize(("pin", "blocked"), [
    ("lower", False), ("upper", False), ("other", True),
])
def test_a_configured_fixed_sha_is_compared_case_insensitively(
    request_manifest, pin: str, blocked: bool,
) -> None:
    """#175: an upper-case configured pin is the same SHA, not a disagreement."""
    prefix = request_manifest.source_sha[:12]
    value = {"lower": prefix, "upper": prefix.upper(),
             "other": ("0" if prefix[0] != "0" else "1") * 12}[pin]
    for registry in request_manifest.registries:
        for path in (Path(registry.path), Path(registry.candidate)):
            path.write_text(path.read_text().replace(
                'kind = "venv"\n', f'kind = "venv"\nexpected_git_sha = "{value}"\n'))
    blockers = migration.plan(request_manifest)["blockers"]
    assert ("target generation disagrees with the configured fixed SHA" in blockers) is blocked


def test_external_consumer_blocks_all_mutation(request_manifest, tmp_path: Path):
    manifest = request_manifest.model_copy(update={"external_consumers": ["other driver"]})
    value = migration.plan(manifest)
    assert value["blockers"]
    with pytest.raises(migration.MigrationError, match="blockers"):
        migration.apply(manifest, value["plan_id"], tmp_path / "receipt")
    assert not (tmp_path / "receipt").exists()


def test_serving_or_unknown_daemon_refuses(request_manifest, monkeypatch: pytest.MonkeyPatch):
    original = admin._detect_vq_self_update
    probe = original(migration._programs(request_manifest)[1])
    monkeypatch.setattr(admin, "_detect_vq_self_update", lambda _p: type(probe)(
        is_self_update=True, daemon_running=True, service_manager="systemd",
        manager_available=True, diagnostic="serving fixture",
    ))
    assert "#73" in migration.plan(request_manifest)["blockers"][0]


def test_ownership_contention_does_not_create_receipt(request_manifest, tmp_path: Path,
                                                    monkeypatch: pytest.MonkeyPatch):
    import contextlib

    @contextlib.contextmanager
    def busy():
        raise admin.AdminError("owned by another updater")
        yield

    monkeypatch.setattr(admin, "admin_update_ownership", busy)
    with pytest.raises(admin.AdminError, match="another updater"):
        migration.apply(request_manifest, migration.plan(request_manifest)["plan_id"],
                        tmp_path / "receipt")
    assert not (tmp_path / "receipt").exists()


def test_symlinked_registry_refuses(request_manifest, tmp_path: Path):
    registry = Path(request_manifest.registries[0].path)
    saved = tmp_path / "saved"
    registry.rename(saved)
    registry.symlink_to(saved)
    with pytest.raises(migration.MigrationError, match="regular file"):
        migration.plan(request_manifest)


def test_cli_default_only_plans(request_manifest, tmp_path: Path):
    manifest = tmp_path / "manifest.json"
    manifest.write_text(request_manifest.model_dump_json())
    result = CliRunner().invoke(main, ["admin", "migrate-runtime", "--manifest", str(manifest)])
    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["status"] == "ready"
    assert not Path(request_manifest.launchers[0].path).exists()


@pytest.mark.no_autopatch_lifecycle_lock
@pytest.mark.no_autopatch_branch_check
@pytest.mark.parametrize("empty_store", [False, True])
def test_prepare_builds_at_final_path_without_switching_bindings(
    request_manifest, runtimes, tmp_path: Path, empty_store: bool,
):
    root, shas, program, _cfg = runtimes
    repo = Path(program.git_dir)
    (repo / ".gitignore").write_text(".venv/\n")
    (repo / "build.sh").write_text(
        "#!/bin/sh\nset -eu\n" + shlex.quote(sys.executable) + " -m venv --without-pip .venv\n"
    )
    for argv in (["add", "build.sh", ".gitignore"],
                 ["-c", "user.name=Migration Test", "-c", "user.email=test@example.invalid",
                  "commit", "-qm", "generation builder"]):
        subprocess.run(["git", "-C", str(repo), *argv], check=True, capture_output=True)
    sha = subprocess.check_output(["git", "-C", str(repo), "rev-parse", "HEAD"], text=True).strip()
    selected_root = tmp_path / "empty-store" if empty_store else root
    for registry in request_manifest.registries:
        for path in (Path(registry.path), Path(registry.candidate)):
            text = path.read_text().replace('kind = "venv"',
                                             'kind = "venv"\nupdate_script = "build.sh"\n'
                                             'install_script = "build.sh"')
            path.write_text(text.replace(str(root), str(selected_root)))
    manifest = request_manifest.model_copy(update={
        "source_sha": sha, "runtime_root": str(selected_root),
    })
    before = migration.plan(manifest)
    assert before["status"] == "preparation_required"
    old = [Path(r.path).read_bytes() for r in manifest.registries]
    after = migration.prepare(manifest, before["plan_id"])
    assert after["status"] == "ready"
    assert runtime_slots.resolve_current(selected_root) == (None if empty_store else shas[0])
    assert [Path(r.path).read_bytes() for r in manifest.registries] == old
    python = runtime_slots.slot_python(selected_root, sha)
    observed = subprocess.check_output([str(python), "-c", "import sys; print(sys.executable)"],
                                       text=True, timeout=30).strip()
    assert observed == str(python)
    assert migration.prepare(manifest, after["plan_id"])["plan_id"] == after["plan_id"]
    result = migration.apply(manifest, after["plan_id"], tmp_path / "receipt")
    assert result["state"] == "applied"
    assert runtime_slots.resolve_current(selected_root) == sha
    assert migration.recover(tmp_path / "receipt", rollback=True)["state"] == "rolled_back"
    assert runtime_slots.resolve_current(selected_root) == (None if empty_store else shas[0])


def test_config_permissions_do_not_widen_to_candidate_mode(request_manifest, tmp_path: Path):
    path = Path(request_manifest.registries[0].path)
    path.chmod(0o600)
    migration.apply(request_manifest, migration.plan(request_manifest)["plan_id"],
                    tmp_path / "receipt")
    assert path.stat().st_mode & 0o777 == 0o600
    migration.recover(tmp_path / "receipt", rollback=True)
    assert path.stat().st_mode & 0o777 == 0o600


def test_interrupted_rollback_resumes(request_manifest, runtimes, tmp_path: Path,
                                     monkeypatch: pytest.MonkeyPatch):
    root, shas, _program, _cfg = runtimes
    receipt = tmp_path / "receipt"
    migration.apply(request_manifest, migration.plan(request_manifest)["plan_id"], receipt)
    replace = os.replace
    fired = False

    def interrupt(source, destination, **kwargs):
        nonlocal fired
        result = replace(source, destination, **kwargs)
        if destination == "config.toml" and not fired:
            fired = True
            raise OSError("interrupted rollback")
        return result

    monkeypatch.setattr(os, "replace", interrupt)
    with pytest.raises(OSError, match="interrupted rollback"):
        migration.recover(receipt, rollback=True)
    monkeypatch.setattr(os, "replace", replace)
    with pytest.raises(migration.MigrationError, match="rollback has started"):
        migration.recover(receipt)
    assert migration.recover(receipt, rollback=True)["state"] == "rolled_back"
    assert runtime_slots.resolve_current(root) == shas[0]


def test_admin_marker_blocks_plan(request_manifest, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(admin, "admin_update_marker_exists", lambda: True)
    assert "marker" in migration.plan(request_manifest)["blockers"][0]


def test_generation_profile_mismatch_refuses_before_binding(request_manifest):
    for registry in request_manifest.registries:
        for path in (Path(registry.path), Path(registry.candidate)):
            path.write_text(path.read_text().replace("/.venv/bin/python", "/.other/bin/python"))
    with pytest.raises(runtime_slots.RuntimeSlotError, match="virtualenv binding"):
        migration.plan(request_manifest)


def test_unhealthy_generation_refuses_before_receipt(request_manifest, tmp_path: Path):
    for registry in request_manifest.registries:
        for path in (Path(registry.path), Path(registry.candidate)):
            path.write_text(path.read_text().replace(
                'kind = "venv"', 'kind = "venv"\nimport_check = "absent_module"',
            ))
    value = migration.plan(request_manifest)
    with pytest.raises(migration.MigrationError, match="configured probes"):
        migration.apply(request_manifest, value["plan_id"], tmp_path / "receipt")
    assert not (tmp_path / "receipt").exists()


def test_changed_queue_authority_refuses_recovery(request_manifest, tmp_path: Path,
                                                 monkeypatch: pytest.MonkeyPatch):
    receipt = tmp_path / "receipt"
    migration.apply(request_manifest, migration.plan(request_manifest)["plan_id"], receipt)
    monkeypatch.setenv("VQ_STATE_DIR", str(tmp_path / "other-state"))
    with pytest.raises(migration.MigrationError, match="queue state authority"):
        migration.recover(receipt, rollback=True)


def test_preparation_failure_preserves_all_bindings(request_manifest, runtimes, tmp_path: Path,
                                                   monkeypatch: pytest.MonkeyPatch):
    root, shas, _program, _cfg = runtimes
    before = migration.plan(request_manifest)
    originals = [Path(r.path).read_bytes() for r in request_manifest.registries]

    def fail(*_args, **_kwargs):
        result = admin.UpdateResult(env="demo", git_dir="/example", branch=None, update_script=None)
        result.work_errors.append("injected build failure")
        return result

    monkeypatch.setattr(admin, "_do_slot_update_work", fail)
    with pytest.raises(migration.MigrationError, match="injected build failure"):
        migration.prepare(request_manifest, before["plan_id"])
    assert runtime_slots.resolve_current(root) == shas[0]
    assert [Path(r.path).read_bytes() for r in request_manifest.registries] == originals


@pytest.mark.parametrize("rollback", [False, True])
@pytest.mark.parametrize("service", ["legacy", "slot", "unknown"])
def test_recovery_refreshes_service_hold_before_any_binding_changes(
    request_manifest, runtimes, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    rollback: bool, service: str,
):
    root, shas, program, _cfg = runtimes
    receipt = tmp_path / "receipt"
    replace = os.replace

    def interrupt(source, destination, **kwargs):
        if destination == "current":
            raise OSError("injected interruption before pointer change")
        return replace(source, destination, **kwargs)

    monkeypatch.setattr(os, "replace", interrupt)
    with pytest.raises(OSError, match="injected"):
        migration.apply(request_manifest, migration.plan(request_manifest)["plan_id"], receipt)
    monkeypatch.setattr(os, "replace", replace)
    originals = [Path(r.path).read_bytes() for r in request_manifest.registries]
    current_before = os.readlink(root / "current")
    previous_before = os.readlink(root / "previous")

    def probe(candidate):
        serving = (candidate.python == program.python if service == "legacy" else
                   candidate.python == str(runtime_slots.slot_python(root, shas[1])))
        return admin._SelfUpdateProbe(
            is_self_update=serving if service != "unknown" else False,
            daemon_running=True, service_manager="systemd",
            manager_available=service != "unknown", diagnostic="new service hold",
        )

    monkeypatch.setattr(admin, "_detect_vq_self_update", probe)
    with pytest.raises(migration.MigrationError, match="service identity|serving-daemon"):
        migration.recover(receipt, rollback=rollback)
    assert os.readlink(root / "current") == current_before
    assert os.readlink(root / "previous") == previous_before
    assert [Path(r.path).read_bytes() for r in request_manifest.registries] == originals


def _set_build_input(path: Path, *, aliases: int) -> None:
    text = path.read_text()
    marker = "runtime_slot_execution = true\n"
    parts = text.split(marker)
    rebuilt = parts[0]
    for index, part in enumerate(parts[1:]):
        extra = 'runtime_slot_build_input = "current"\n' if index < aliases else ""
        rebuilt += marker + extra + part
    path.write_text(rebuilt)


def test_candidate_may_move_every_alias_to_the_current_build_input(
    request_manifest, tmp_path: Path,
):
    for registry in request_manifest.registries:
        _set_build_input(Path(registry.candidate), aliases=2)
    value = migration.plan(request_manifest)
    assert value["status"] == "ready"
    assert value["build_input"] == "current"
    originals = [Path(r.path).read_bytes() for r in request_manifest.registries]

    applied = migration.apply(request_manifest, value["plan_id"], tmp_path / "receipt")

    assert applied["state"] == "applied"
    for registry in request_manifest.registries:
        text = Path(registry.path).read_text()
        assert text.count('runtime_slot_build_input = "current"') == 2
    assert migration.recover(tmp_path / "receipt", rollback=True)["state"] == "rolled_back"
    assert [Path(r.path).read_bytes() for r in request_manifest.registries] == originals


def test_build_input_must_move_every_alias_together(request_manifest):
    _set_build_input(Path(request_manifest.registries[0].candidate), aliases=1)
    with pytest.raises(migration.MigrationError, match="candidate"):
        migration.plan(request_manifest)


def test_build_input_must_agree_across_authorities(request_manifest):
    _set_build_input(Path(request_manifest.registries[0].candidate), aliases=2)
    with pytest.raises(migration.MigrationError, match="build input"):
        migration.plan(request_manifest)


def test_default_candidate_keeps_the_legacy_build_input(request_manifest):
    assert migration.plan(request_manifest)["build_input"] == "legacy"


@pytest.mark.parametrize("prepared", [False, True])
def test_blockers_take_precedence_over_generation_status(request_manifest, monkeypatch, prepared):
    manifest = request_manifest.model_copy(update={"external_consumers": ["other driver"]})
    if not prepared:
        monkeypatch.setattr(migration, "_verified", lambda _manifest: None)

    value = migration.plan(manifest)

    assert value["blockers"]
    assert value["status"] == "blocked"


def test_apply_registers_its_receipt_for_checkout_retirement(
    request_manifest, runtimes, tmp_path: Path,
):
    """#123: retire-checkout finds unfinished transactions through this index."""
    value = migration.plan(request_manifest)
    receipt = tmp_path / "receipt"
    migration.apply(request_manifest, value["plan_id"], receipt)
    (item,) = migration.indexed_transactions()
    assert (item["receipt_dir"], item["program"]) == (str(receipt), request_manifest.program)
    assert (item["state"], item["finished"]) == ("applied", True)
    assert set(item["bindings"]) == {
        *(launcher.path for launcher in request_manifest.launchers),
        *(registry.path for registry in request_manifest.registries),
    }
    migration.register_receipt(receipt, request_manifest.program)  # idempotent
    assert len(migration.indexed_transactions()) == 1
    migration.recover(receipt, rollback=True)
    assert migration.indexed_transactions()[0]["state"] == "rolled_back"
