"""Retirement distinguishes compatibility fields from executable consumers."""
from __future__ import annotations

import json

import pytest

from tests.test_retire_checkout_72 import migrated as migrated
from vq import checkout_retirement, config, runtime_daemon, runtime_migration


def _record(tmp_path, program, kind, path, *, state="applied", before=None):
    receipt = tmp_path / "receipt"
    receipt.mkdir()
    if before is None:
        before, _ = runtime_migration._file(str(path))
    (receipt / "transaction.json").write_text(json.dumps({
        "schema": runtime_migration.SCHEMA, "state": state,
        "plan": {"launchers": [], "registries": [], kind: [{"before": before}]},
    }))
    runtime_migration.register_receipt(receipt, program)


def _registry(tmp_path, cfg):
    path = tmp_path / "registry.toml"
    path.write_bytes(runtime_daemon._toml_bytes(cfg.model_dump(exclude_none=True)))
    return path


def test_migrated_registry_compatibility_paths_do_not_block(migrated, tmp_path):
    repo, _, cfg, _ = migrated
    path = _registry(tmp_path, cfg)
    _record(tmp_path, "demo", "registries", path)

    assert checkout_retirement.plan("demo", cfg)["blockers"] == []
    assert repo.exists()


@pytest.mark.parametrize("consumer", ["legacy-alias", "remote-vq", "script", "invalid"])
def test_registry_real_dependencies_still_block(migrated, tmp_path, consumer):
    repo, _, cfg, _ = migrated
    other = cfg.model_copy(deep=True)
    if consumer == "legacy-alias":
        other.programs["old"] = other.programs["demo"].model_copy(
            update={"runtime_slot_build_input": "legacy"},
        )
    elif consumer == "remote-vq":
        other.hosts["worker"] = config.HostConfig(
            ssh="worker.example.invalid", remote_vq=str(repo / ".venv/bin/vq"),
        )
    elif consumer == "script":
        other.programs["demo"].update_script = str(repo / "build.sh")
    path = _registry(tmp_path, other)
    if consumer == "invalid":
        path.write_text("invalid TOML [")
    _record(tmp_path, "demo", "registries", path)

    assert checkout_retirement.plan("demo", cfg)["blockers"]


def test_other_components_bootstrap_is_a_checkout_dependency(migrated, tmp_path):
    repo, _, cfg, _ = migrated
    launcher = tmp_path / "other-python"
    launcher.write_text(f'#!/bin/sh\nexec "{repo}/.venv/bin/vq" runtime-python "$@"\n')
    _record(tmp_path, "other", "launchers", launcher)

    assert any("migrated binding" in b for b in checkout_retirement.plan("demo", cfg)["blockers"])


def test_unfinished_other_component_can_restore_its_old_bootstrap(migrated, tmp_path):
    repo, _, cfg, _ = migrated
    launcher = tmp_path / "other-python"
    launcher.write_text(f'#!/bin/sh\nexec "{repo}/.venv/bin/vq" runtime-python "$@"\n')
    before, _ = runtime_migration._file(str(launcher))
    launcher.write_text('#!/bin/sh\nexec /opt/example/retained-vq "$@"\n')
    _record(tmp_path, "other", "launchers", launcher, state="applying", before=before)

    assert any("migrate-runtime transaction" in b
               for b in checkout_retirement.plan("demo", cfg)["blockers"])


def test_declared_symlink_is_a_checkout_dependency(migrated, tmp_path):
    repo, _, cfg, _ = migrated
    launcher = tmp_path / "shell-command"
    launcher.symlink_to(repo / "build.sh")

    assert any("declared launcher" in b for b in checkout_retirement.plan(
        "demo", cfg, launchers=(str(launcher),),
    )["blockers"])


def test_external_registry_invalid_active_slot_blocks(migrated, tmp_path):
    _, _, cfg, _ = migrated
    other = cfg.model_copy(deep=True)
    other.programs["demo"].runtime_slot_root = str(tmp_path / "missing-store")
    path = _registry(tmp_path, other)
    _record(tmp_path, "other", "registries", path)

    assert any("cannot verify migrated registry" in b
               for b in checkout_retirement.plan("demo", cfg)["blockers"])
