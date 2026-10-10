"""#143: retirement after migrate-runtime, whether or not receipts are indexed.

The retiring family's own ``git_dir``/``python`` are compatibility templates
once it builds from its active generation. Exactly those fields are exempt, in
the loaded registry and in every registry a migration bound; any other
reference to the checkout still blocks. Apply leaves the registry untouched.
"""
from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from tests.test_retire_checkout_72 import migrated as migrated
from tests.test_runtime_execution_binding import runtimes as runtimes
from tests.test_runtime_migration import _set_build_input
from tests.test_runtime_migration import request_manifest as request_manifest
from vq import (
    admin,
    checkout_retirement,
    config,
    runtime_daemon,
    runtime_migration,
    runtime_retention,
)

pytestmark = [
    pytest.mark.no_autopatch_lifecycle_lock,
    pytest.mark.no_autopatch_branch_check,
]

COMPAT = [
    {"program": "demo", "field": "git_dir"},
    {"program": "demo", "field": "python"},
]


def _fields(value: dict) -> list[dict]:
    return [{k: v for k, v in item.items() if k != "path"}
            for item in value["compatibility_paths"]]


def _record(tmp_path: Path, program: str, path: Path) -> None:
    receipt = tmp_path / f"receipt-{program}-{path.parent.name}-{path.name}"
    receipt.mkdir()
    before, _ = runtime_migration._file(str(path))
    (receipt / "transaction.json").write_text(json.dumps({
        "schema": runtime_migration.SCHEMA, "state": "applied",
        "plan": {"launchers": [], "registries": [{"before": before}]},
    }))
    runtime_migration.register_receipt(receipt, program)


def _registry(tmp_path: Path, cfg: config.Config, name: str = "registry.toml") -> Path:
    path = tmp_path / name
    path.write_bytes(runtime_daemon._toml_bytes(cfg.model_dump(exclude_none=True)))
    return path


@pytest.fixture
def laptop(request_manifest, runtimes, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """The indexed-receipt host shape (host-a): two aliases, current build input, a rolled-back
    first apply and an applied second one, both indexed and binding the same
    registries, which keep the legacy git_dir/python and an operator comment."""
    _root, _shas, program, _cfg = runtimes
    repo = Path(program.git_dir)
    upstream = tmp_path / "upstream.git"
    subprocess.run(["git", "clone", "-q", "--bare", str(repo), str(upstream)], check=True)
    subprocess.run(["git", "-C", str(repo), "remote", "add", "origin", str(upstream)],
                   check=True)
    subprocess.run(["git", "-C", str(repo), "fetch", "-q", "origin"], check=True)
    (repo / ".gitignore").write_text(".venv/\n.gitignore\n")
    for registry in request_manifest.registries:
        _set_build_input(Path(registry.candidate), aliases=2)
    first = tmp_path / "apply-01"
    value = runtime_migration.plan(request_manifest)
    runtime_migration.apply(request_manifest, value["plan_id"], first)
    assert runtime_migration.recover(first, rollback=True)["state"] == "rolled_back"
    second = tmp_path / "apply-02"
    value = runtime_migration.plan(request_manifest)
    assert runtime_migration.apply(request_manifest, value["plan_id"], second)["state"] \
        == "applied"
    assert sorted(item["state"] for item in runtime_migration.indexed_transactions()) \
        == ["applied", "rolled_back"]
    path = Path(request_manifest.registries[0].path)
    cfg = config._validate_config_data(config._read_config_data(path), path)
    assert cfg.programs["demo"].git_dir == str(repo)  # compatibility path kept
    monkeypatch.setattr(admin, "_runtime_slot_spec_snapshot", lambda: [])
    monkeypatch.setattr(runtime_retention, "_service_files", lambda: ([], []))
    return repo, cfg, second


def test_laptop_registry_naming_only_compatibility_paths_plans_clean(laptop):
    repo, cfg, _receipt = laptop

    value = checkout_retirement.plan("demo", cfg)

    assert value["blockers"] == []
    assert value["aliases"] == ["demo", "demo-dev"]
    assert _fields(value) == [
        {"program": "demo", "field": "git_dir"},
        {"program": "demo", "field": "python"},
        {"program": "demo-dev", "field": "git_dir"},
        {"program": "demo-dev", "field": "python"},
    ]
    assert {item["path"] for item in value["compatibility_paths"]} \
        == {str(repo), str(repo / ".venv/bin/python")}


def test_apply_leaves_the_migrated_registry_untouched(laptop, request_manifest):
    """The kept fields remain valid path templates. The registry keeps its exact
    bytes and file identity, which the migration receipt's recovery binds."""
    repo, cfg, receipt = laptop
    registries = [Path(r.path) for r in request_manifest.registries]
    before = [runtime_migration._snapshot(path) for path in registries]
    value = checkout_retirement.plan("demo", cfg)

    result = checkout_retirement.apply("demo", cfg, value["plan_id"])

    assert result["removed"] is True and not repo.exists()
    assert [runtime_migration._snapshot(path) for path in registries] == before
    assert b"# Operator comment must survive" in runtime_migration._bytes(before[0]["entry"])
    assert json.loads(Path(result["receipt"]).read_text())["compatibility_paths"] \
        == value["compatibility_paths"]
    journal = json.loads((receipt / "transaction.json").read_text())
    assert journal["state"] == "applied"
    selected = cfg.programs["demo"].execution_program()
    assert Path(selected.python).exists()
    assert not Path(selected.python).is_relative_to(repo)


def test_unindexed_host_reports_the_same_plan(migrated, tmp_path):
    """Hosts without indexed receipts (host-b..d): same blockers and inventory."""
    _repo, _fields_, cfg, _specs = migrated
    unindexed = checkout_retirement.plan("demo", cfg)
    _record(tmp_path, "demo", _registry(tmp_path, cfg))

    indexed = checkout_retirement.plan("demo", cfg)

    assert unindexed["blockers"] == indexed["blockers"] == []
    assert _fields(unindexed) == _fields(indexed) == COMPAT


def _remote_vq(cfg, repo):
    cfg.hosts["worker"] = config.HostConfig(
        ssh="worker.example.invalid", remote_vq=str(repo / ".venv/bin/vq"))


def _healthcheck(cfg, repo):
    cfg.programs["demo"].healthcheck_command = f"{repo}/.venv/bin/python -c pass"


def _wrapper(cfg, repo):
    cfg.programs["tool"] = config.BinaryProgram(kind="binary", binary=str(repo / "bin/tool"))


@pytest.mark.parametrize("reference", [_remote_vq, _healthcheck, _wrapper])
def test_loaded_registry_reference_blocks_without_an_indexed_receipt(
    migrated, reference,
):
    """A host whose migration was not indexed still scans its loaded registry."""
    repo, _fields_, cfg, _specs = migrated
    cfg = cfg.model_copy(deep=True)
    reference(cfg, repo)

    blockers = checkout_retirement.plan("demo", cfg)["blockers"]

    assert any("loaded registry" in b and "names this checkout" in b for b in blockers)


def test_another_programs_compatibility_paths_in_a_bound_registry_block(
    migrated, tmp_path,
):
    """Only the retiring family's own fields are exempt, not any program's."""
    _repo, _fields_, cfg, _specs = migrated
    other = cfg.model_copy(deep=True)
    other.programs["stranger"] = other.programs.pop("demo")
    _record(tmp_path, "stranger", _registry(tmp_path, other))

    blockers = checkout_retirement.plan("demo", cfg)["blockers"]

    assert any("migrated registry" in b and "names this checkout" in b for b in blockers)


def test_family_legacy_build_input_in_a_bound_registry_blocks(migrated, tmp_path):
    _repo, fields, cfg, _specs = migrated
    other = cfg.model_copy(deep=True)
    other.programs["demo"] = config.VenvProgram(**fields)  # build_input "legacy"
    _record(tmp_path, "demo", _registry(tmp_path, other))

    blockers = checkout_retirement.plan("demo", cfg)["blockers"]

    assert any("migrated registry" in b and "names this checkout" in b for b in blockers)
