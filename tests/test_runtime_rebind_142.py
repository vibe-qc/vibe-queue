"""Existing runtime launchers rebind through reviewed, reversible bytes."""
from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from tests.test_runtime_execution_binding import runtimes as runtimes
from tests.test_runtime_migration import request_manifest as request_manifest
from vq import admin, runtime_slots
from vq import runtime_migration as migration


@pytest.fixture
def rebind(request_manifest, runtimes, tmp_path):
    initial = request_manifest
    legacy = Path(runtimes[2].git_dir)
    old_bootstrap = legacy / "bootstrap-vq"
    Path(initial.launchers[0].bootstrap).rename(old_bootstrap)
    initial.launchers[0].bootstrap = str(old_bootstrap)
    migration.apply(initial, migration.plan(initial)["plan_id"], tmp_path / "initial-receipt")
    registries = []
    for r in initial.registries:
        candidate = Path(r.candidate).with_name("rebind.toml")
        candidate.write_text(Path(r.path).read_text().replace(
            "runtime_slot_execution = true", 'runtime_slot_execution = true\n'
            'runtime_slot_build_input = "current"',
        ))
        registries.append(migration.Registry(path=r.path, candidate=str(candidate)))
    bootstrap = tmp_path / "independent-vq"
    bootstrap.write_text(old_bootstrap.read_text() + "# retained independent bootstrap\n")
    bootstrap.chmod(0o700)
    return migration.Manifest(
        schema_version=migration.REBIND_SCHEMA, component=initial.component,
        program=initial.program, source_sha=initial.source_sha, runtime_root=initial.runtime_root,
        registries=registries, launchers=[migration.Launcher(
            path=initial.launchers[0].path, bootstrap=str(bootstrap),
        )], external_consumers=[],
    )


def test_rebind_preserves_pointers_and_restores_exact_files(rebind, runtimes, tmp_path):
    root = runtimes[0]
    pointers = {name: migration._pointer(root, name) for name in ("current", "previous")}
    files = [Path(r.path) for r in rebind.registries] + [Path(rebind.launchers[0].path)]
    original = [p.read_bytes() for p in files]
    planned = migration.plan(rebind)
    assert planned["status"] == "ready"
    assert {a["before"]["path"] for a in migration._actions(planned)} == {str(p) for p in files}
    receipt = tmp_path / "rebind-receipt"

    assert migration.apply(rebind, planned["plan_id"], receipt)["state"] == "applied"
    assert rebind.launchers[0].bootstrap.encode() in files[-1].read_bytes()
    assert {n: migration._pointer(root, n) for n in pointers} == pointers
    assert migration.recover(receipt)["state"] == "applied"
    assert migration.recover(receipt, rollback=True)["state"] == "rolled_back"
    assert [p.read_bytes() for p in files] == original
    assert {n: migration._pointer(root, n) for n in pointers} == pointers


@pytest.mark.parametrize("after", [False, True])
@pytest.mark.parametrize("boundary", ["stable-python", "config.toml"])
def test_rebind_recovers_interruption_at_each_binding(rebind, runtimes, tmp_path, monkeypatch,
                                                     after, boundary):
    receipt = tmp_path / "rebind-receipt"
    planned = migration.plan(rebind)
    original = [Path(r.path).read_bytes() for r in rebind.registries]
    old_launcher = Path(rebind.launchers[0].path).read_bytes()
    replace = os.replace
    fired = False

    def interrupt(source, target, **kwargs):
        nonlocal fired
        selected = str(target) == boundary and not fired
        if selected:
            fired = True
            if not after:
                raise OSError("injected interruption")
        result = replace(source, target, **kwargs)
        if selected:
            raise OSError("injected interruption")
        return result

    monkeypatch.setattr(os, "replace", interrupt)
    with pytest.raises(OSError, match="injected"):
        migration.apply(rebind, planned["plan_id"], receipt)
    monkeypatch.setattr(os, "replace", replace)
    assert fired
    assert migration.recover(receipt)["state"] == "applied"
    assert migration.recover(receipt, rollback=True)["state"] == "rolled_back"
    assert [Path(r.path).read_bytes() for r in rebind.registries] == original
    assert Path(rebind.launchers[0].path).read_bytes() == old_launcher
    assert runtime_slots.resolve_current(runtimes[0]) == rebind.source_sha


@pytest.mark.parametrize("change", ["arguments", "noncanonical", "unbound-alias", "profile",
                                    "other-generation", "new-path"])
def test_rebind_refuses_scope_expansion(rebind, runtimes, tmp_path, change):
    if change == "arguments":
        rebind.launchers[0].arguments = ["-m", "other"]
    elif change == "noncanonical":
        Path(rebind.launchers[0].path).write_text("#!/bin/sh\necho unrelated\n")
    elif change == "unbound-alias":
        r = Path(rebind.registries[0].path)
        r.write_text(r.read_text().replace("runtime_slot_execution = true", "", 1))
    elif change == "profile":
        r = Path(rebind.registries[0].candidate)
        r.write_text(r.read_text() + '\nbranch = "unreviewed"\n')
    elif change == "other-generation":
        rebind.source_sha = runtimes[1][0]
    else:
        rebind.launchers[0].path = str(tmp_path / "new-launcher")
    with pytest.raises(migration.MigrationError):
        migration.apply(rebind, migration.plan(rebind)["plan_id"], tmp_path / "refused-receipt")
    assert not (tmp_path / "refused-receipt").exists()


def test_rebind_serving_registration_never_stops_service(rebind, monkeypatch, tmp_path):
    monkeypatch.setattr(admin, "_detect_vq_self_update", lambda _: pytest.fail("probed service"))
    monkeypatch.setattr(admin, "_run_daemon_service_command",
                        lambda *a, **k: pytest.fail("service"))
    monkeypatch.setattr(admin, "_do_slot_update_work",
                        lambda *a, **k: pytest.fail("rebind must not build"))
    value = migration.plan(rebind)
    assert migration.prepare(rebind, value["plan_id"]) == value
    result = migration.apply(rebind, value["plan_id"], tmp_path / "rebind-receipt")
    assert result["state"] == "applied"


@pytest.mark.parametrize("damage", ["new-bootstrap", "old-bootstrap", "launcher", "pointer"])
def test_rebind_recovery_refuses_foreign_changes(rebind, runtimes, tmp_path, damage):
    planned = migration.plan(rebind)
    receipt = tmp_path / "rebind-receipt"
    migration.apply(rebind, planned["plan_id"], receipt)
    rollback = damage != "new-bootstrap"
    if damage == "pointer":
        runtime_slots.activate(runtimes[0], runtimes[1][0])
    else:
        path = (rebind.launchers[0].bootstrap if damage == "new-bootstrap" else
                planned["launchers"][0]["rollback_bootstrap"]["path"] if damage == "old-bootstrap"
                else rebind.launchers[0].path)
        with Path(path).open("a") as stream:
            stream.write("# foreign edit\n")
    before = [Path(r.path).read_bytes() for r in rebind.registries]
    with pytest.raises(migration.MigrationError):
        migration.recover(receipt, rollback=rollback)
    assert [Path(r.path).read_bytes() for r in rebind.registries] == before


def test_rebind_manifest_has_an_explicit_reader_gate(rebind):
    raw = json.loads(rebind.model_dump_json())
    assert raw["schema_version"] == "vq.runtime-rebind/1"
    raw["participants"] = [{"host": "cluster", "kind": "scheduler-packed",
                            "programs": ["demo"], "authorities": ["driver"]}]
    with pytest.raises(ValueError, match="local registries only"):
        migration.Manifest.model_validate(raw)


def test_real_slot_queue_bootstrap_allows_retirement_plan(rebind, runtimes, tmp_path, monkeypatch):
    """The new bootstrap runs actual vq code from a separate sealed queue slot."""
    import shutil
    import subprocess
    import sys
    import venv

    import click

    from tests.test_retire_checkout_72 import _git
    from vq import checkout_retirement, config, runtime_daemon, runtime_retention

    upstream = tmp_path / "queue-upstream"
    source = upstream / "src" / "vq"
    source.parent.mkdir(parents=True)
    shutil.copytree(Path(runtime_slots.__file__).parent, source,
                    ignore=shutil.ignore_patterns("__pycache__"))
    _git(upstream, "init", "-q", "-b", "main")
    _git(upstream, "add", "src")
    _git(upstream, "commit", "-qm", "queue source fixture")
    sha = _git(upstream, "rev-parse", "HEAD")
    queue_root = tmp_path / "queue-runtime"
    runtime_slots.begin_slot_build(queue_root, sha, transaction_id="a" * 32,
                                   in_use=lambda: set())
    runtime_slots.materialize_source(upstream, queue_root, sha)
    python = runtime_slots.slot_python(queue_root, sha)
    venv.EnvBuilder(with_pip=False, symlinks=True).create(python.parent.parent)
    site = next((python.parent.parent / "lib").glob("python*/site-packages"))
    shutil.copytree(runtime_slots.slot_source(queue_root, sha) / "src/vq", site / "vq")
    # Reuse only the test runner's installed dependencies. The actual vq
    # package is copied into this slot; isolated Python ignores PYTHONPATH.
    (site / "dependencies.pth").write_text(str(Path(click.__file__).parents[1]) + "\n")
    runtime_slots.seal_slot_build(queue_root, sha, transaction_id="a" * 32)
    runtime_slots.activate(queue_root, sha)
    queue_program = config.VenvProgram(
        kind="venv", git_dir=str(upstream), python=str(upstream / ".venv/bin/python"),
        update_script="build.sh", runtime_slot_root=str(queue_root),
        runtime_slot_execution=True, runtime_slot_build_input="current",
    )
    # The queue is already migrated. Another program still bootstraps through
    # its old checkout, exactly the dependency that queue retirement must see.
    old_bootstrap = upstream / "bootstrap-vq"
    shutil.copy2(Path(runtimes[2].git_dir) / "bootstrap-vq", old_bootstrap)
    wrapper = Path(rebind.launchers[0].path)
    wrapper.write_text(wrapper.read_text().replace(
        str(Path(runtimes[2].git_dir) / "bootstrap-vq"), str(old_bootstrap),
    ))
    for registry in rebind.registries:
        for location in (registry.path, registry.candidate):
            raw, _ = migration._model(Path(location).read_bytes(), location)
            raw["programs"]["queue"] = queue_program.model_dump(exclude_none=True)
            Path(location).write_bytes(runtime_daemon._toml_bytes(raw))
    bootstrap = Path(rebind.launchers[0].bootstrap)
    bootstrap.write_text(runtime_slots.render_launcher(queue_root, Path(sys.executable),
                                                       ["-I", "-m", "vq"]))
    value = migration.plan(rebind)
    migration.apply(rebind, value["plan_id"], tmp_path / "rebind-receipt")
    _, cfg = migration._model(Path(rebind.registries[0].path).read_bytes(),
                              rebind.registries[0].path)
    legacy = Path(runtimes[2].git_dir)
    (legacy / ".gitignore").write_text(".venv/\nbootstrap-vq\n")
    _git(legacy, "add", ".gitignore")
    _git(legacy, "commit", "-qm", "ignore generated fixture paths")
    published = tmp_path / "published.git"
    subprocess.run(["git", "clone", "--bare", "-q", str(legacy), str(published)], check=True)
    _git(legacy, "remote", "add", "origin", str(published))
    _git(legacy, "fetch", "-q", "origin")
    monkeypatch.setattr(runtime_retention, "_service_files", lambda: ([], []))
    monkeypatch.setattr(admin, "_runtime_slot_spec_snapshot", lambda: [])
    assert checkout_retirement.plan("demo", cfg)["blockers"] == []

    (upstream / ".gitignore").write_text("bootstrap-vq\n")
    _git(upstream, "add", ".gitignore")
    _git(upstream, "commit", "-qm", "ignore fixture bootstrap")
    queue_published = tmp_path / "queue-published.git"
    subprocess.run(["git", "clone", "--bare", "-q", str(upstream), str(queue_published)],
                   check=True)
    _git(upstream, "remote", "add", "origin", str(queue_published))
    _git(upstream, "fetch", "-q", "origin")
    assert checkout_retirement.plan("queue", cfg)["blockers"] == []

    # Rename only this fixture checkout; do not exercise any production deletion.
    legacy.rename(tmp_path / "retired-legacy")
    upstream.rename(tmp_path / "retired-queue-legacy")
    env = {k: v for k, v in os.environ.items() if k != "PYTHONPATH"}
    invoked = subprocess.run(
        [rebind.launchers[0].path, "-c", "import sys; print(sys.executable)"],
        env=env, capture_output=True, text=True, timeout=30,
    )
    assert invoked.returncode == 0, invoked.stderr
    assert invoked.stdout.strip() == str(runtime_slots.slot_python(runtimes[0], rebind.source_sha))
    direct = subprocess.run([str(bootstrap), "--version"], env=env, capture_output=True,
                            text=True, timeout=30)
    assert direct.returncode == 0, direct.stderr
    assert direct.stdout.startswith("vq, version ")
