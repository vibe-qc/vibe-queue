"""Direct-host migration reconciles real registry files at two authorities."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest
from click.testing import CliRunner

from tests.test_runtime_execution_binding import runtimes as runtimes
from tests.test_runtime_migration import request_manifest as request_manifest
from vq import config, runtime_slots, transport
from vq import runtime_migration as migration
from vq.cli import main


@pytest.fixture
def direct(request_manifest, runtimes, monkeypatch):
    _root, _shas, program, cfg = runtimes
    local, external = request_manifest.registries
    launcher = request_manifest.launchers[0].path
    for candidate, path in ((False, local.path), (True, local.candidate)):
        python = launcher if candidate else program.python
        text = Path(path).read_text()
        Path(path).write_text(
            text + '\n[hosts.localhost]\nssh = "localhost"\n'
            f"remote_python = {json.dumps(python)}\n"
            f"branches = {{release = {json.dumps(python)}}}\n"
        )
    for candidate, path in ((False, external.path), (True, external.candidate)):
        python = launcher if candidate else program.python
        Path(path).write_text(
            "# Driver comment and unrelated settings are retained.\n"
            'max_cpus = 3\n[hosts.compute]\nssh = "compute"\n'
            f"remote_python = {json.dumps(python)}\n"
            f'branches = {{release = {json.dumps(python)}, other = "/site/other/python"}}\n'
        )
    request_manifest = request_manifest.model_copy(
        update={
            "registries": [
                local.model_copy(update={"binding_hosts": ["localhost"]}),
                external.model_copy(update={"authority": "driver", "binding_hosts": ["compute"]}),
            ],
            "external_consumers": ["driver"],
        }
    )
    cfg.hosts["driver"] = config.HostConfig(ssh="driver-fixture", remote_vq="/site/vq")
    requests = []

    def endpoint(host, *args, stdin_data=None, **kwargs):
        assert host.ssh == "driver-fixture" and args == ("admin", "runtime-registry")
        assert kwargs["retry_transient"] == 0 and kwargs["owned_process_group"]
        request = json.loads(stdin_data)
        requests.append(request)
        result = CliRunner().invoke(main, list(args), input=stdin_data)
        if result.exit_code:
            raise migration.MigrationError(result.output)
        return subprocess.CompletedProcess(args, 0, result.output, "")

    monkeypatch.setattr(transport, "run_remote_vq", endpoint)
    return request_manifest, requests, endpoint


def test_two_authorities_activate_and_rollback_without_changing_legacy(
    direct,
    runtimes,
    tmp_path: Path,
):
    manifest, requests, _endpoint = direct
    root, shas, program, _cfg = runtimes
    before = [Path(r.path).read_bytes() for r in manifest.registries]
    value = migration.plan(manifest)
    assert value["status"] == "ready" and not value["blockers"]
    assert {r["op"] for r in requests} == {"snapshot"}
    receipt = tmp_path / "receipt"
    # Start on the old immutable generation, then import another module only
    # after the pointer changes. Its interpreter and prefix remain pinned.
    trigger = tmp_path / "continue"
    code = (
        'import sys,time; print("ready",flush=True); '
        f'\nwhile not __import__("os").path.exists({str(trigger)!r}): time.sleep(.01)\n'
        "import pathlib; print(sys.executable); print(sys.prefix)"
    )
    process = subprocess.Popen(
        [str(runtime_slots.slot_python(root, shas[0])), "-c", code],
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert process.stdout.readline().strip() == "ready"
        assert migration.apply(manifest, value["plan_id"], receipt)["state"] == "applied"
        assert runtime_slots.resolve_current(root) == shas[1]
        for registry in manifest.registries:
            assert manifest.launchers[0].path in Path(registry.path).read_text()
        trigger.touch()
        late = process.communicate(timeout=30)[0].splitlines()
        assert process.returncode == 0 and late == [
            str(runtime_slots.slot_python(root, shas[0])),
            str(runtime_slots.slot_python(root, shas[0]).parent.parent),
        ]
        selected = subprocess.check_output(
            [manifest.launchers[0].path, "-c", "import sys; print(sys.executable)"],
            text=True,
            timeout=30,
        )
        assert selected.strip() == str(runtime_slots.slot_python(root, shas[1]))
        assert migration.recover(receipt, rollback=True)["state"] == "rolled_back"
        assert [Path(r.path).read_bytes() for r in manifest.registries] == before
        assert runtime_slots.resolve_current(root) == shas[0]
        assert Path(program.python).is_file()
        assert all(runtime_slots.slot_python(root, sha).exists() for sha in shas)
    finally:
        if process.poll() is None:
            process.kill()
        process.wait(timeout=30)


@pytest.mark.parametrize("changed", ["config", "candidate", "transport"])
def test_stale_remote_inventory_refuses_before_any_activation(
    direct,
    runtimes,
    tmp_path: Path,
    changed,
):
    manifest, _requests, _endpoint = direct
    root, shas, _program, cfg = runtimes
    plan = migration.plan(manifest)
    registry = manifest.registries[1]
    if changed == "transport":
        cfg.hosts["driver"].remote_vq = "/site/different-vq"
    else:
        path = Path(registry.path if changed == "config" else registry.candidate)
        replacement = path.with_name("replacement")
        replacement.write_bytes(path.read_bytes())
        replacement.replace(path)
    with pytest.raises(migration.MigrationError, match="stale"):
        migration.apply(manifest, plan["plan_id"], tmp_path / "receipt")
    assert runtime_slots.resolve_current(root) == shas[0]
    assert not (tmp_path / "receipt").exists()


@pytest.mark.parametrize("rollback", [False, True])
def test_unknown_remote_write_is_recovered_by_identity_not_replayed(
    direct,
    runtimes,
    tmp_path: Path,
    monkeypatch,
    rollback,
):
    manifest, requests, endpoint = direct
    root, shas, _program, _cfg = runtimes
    plan = migration.plan(manifest)
    receipt = tmp_path / "receipt"
    fired = False

    def lose_reply(*args, **kwargs):
        nonlocal fired
        result = endpoint(*args, **kwargs)
        if json.loads(kwargs["stdin_data"])["op"] == "replace" and not fired:
            fired = True
            raise OSError("remote write completed but reply was lost")
        return result

    monkeypatch.setattr(transport, "run_remote_vq", lose_reply)
    with pytest.raises(OSError, match="reply was lost"):
        migration.apply(manifest, plan["plan_id"], receipt)
    assert fired
    assert json.loads((receipt / "transaction.json").read_text())["state"] == "applying"
    calls = sum(r["op"] == "replace" for r in requests)
    monkeypatch.setattr(transport, "run_remote_vq", endpoint)
    result = migration.recover(receipt, rollback=rollback)
    assert result["state"] == ("rolled_back" if rollback else "applied")
    assert runtime_slots.resolve_current(root) == shas[0 if rollback else 1]
    assert sum(r["op"] == "replace" for r in requests) == calls + int(rollback)


def test_unknown_external_consumer_and_unrelated_candidate_edits_still_refuse(direct):
    manifest, _requests, _endpoint = direct
    extra = manifest.model_copy(update={"external_consumers": ["driver", "unreviewed-driver"]})
    assert migration.plan(extra)["blockers"] == [
        "external consumer requires a reviewed participant: unreviewed-driver"
    ]
    candidate = Path(manifest.registries[1].candidate)
    candidate.write_text(candidate.read_text().replace("max_cpus = 3", "max_cpus = 4"))
    with pytest.raises(migration.MigrationError, match="exactly"):
        migration.plan(manifest)


def test_remote_foreign_edit_blocks_rollback_before_other_authorities_change(
    direct,
    runtimes,
    tmp_path: Path,
):
    manifest, _requests, _endpoint = direct
    root, shas, _program, _cfg = runtimes
    receipt = tmp_path / "receipt"
    migration.apply(manifest, migration.plan(manifest)["plan_id"], receipt)
    external = Path(manifest.registries[1].path)
    external.write_text(external.read_text() + "# another writer\n")
    with pytest.raises(migration.MigrationError, match="foreign edit"):
        migration.recover(receipt, rollback=True)
    assert runtime_slots.resolve_current(root) == shas[1]
    assert manifest.launchers[0].path in Path(manifest.registries[0].path).read_text()


def test_remote_aliases_cannot_stage_the_same_registry_twice(direct, runtimes):
    manifest, _requests, _endpoint = direct
    _root, _shas, _program, cfg = runtimes
    cfg.hosts["driver-alias"] = cfg.hosts["driver"].model_copy()
    alias = manifest.registries[1].model_copy(update={"authority": "driver-alias"})
    duplicate = manifest.model_copy(update={
        "registries": [*manifest.registries, alias],
        "external_consumers": ["driver", "driver-alias"],
    })
    with pytest.raises(migration.MigrationError, match="duplicate registry authority"):
        migration.plan(duplicate)


def test_remote_registry_cannot_follow_a_symlink(direct):
    manifest, _requests, _endpoint = direct
    registry = Path(manifest.registries[1].path)
    saved = registry.with_name("saved-registry")
    registry.rename(saved)
    registry.symlink_to(saved)
    with pytest.raises(migration.MigrationError, match="regular file"):
        migration.plan(manifest)


def test_remote_authority_still_requires_the_discovery_declaration(direct):
    manifest, _requests, _endpoint = direct
    missing = manifest.model_copy(update={"external_consumers": []})
    assert migration.plan(missing)["blockers"] == [
        "registry authority must be declared in external_consumers: driver"
    ]
