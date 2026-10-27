"""Leftovers of interrupted admin updates and fetches: found, gated, removed."""
from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import admin, cleanup, config, leftovers, paths

pytestmark = [pytest.mark.no_autopatch_lifecycle_lock]

TXN = "0123456789abcdef0123456789abcdef"


def _old(path: Path, days: float) -> Path:
    stamp = time.time() - days * 86400
    os.utime(path, (stamp, stamp), follow_symlinks=False)
    return path


@pytest.fixture
def host(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    (tmp_path / "cfg" / "config.toml").write_text(
        'default_host = "localhost"\n[hosts.localhost]\nssh = "localhost"\n'
    )
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    checkout = tmp_path / "apps" / "demo"
    checkout.mkdir(parents=True)
    subprocess.run(["git", "-C", str(checkout), "init", "-q"], check=True)
    python = checkout / ".venv" / "bin" / "python"
    python.parent.mkdir(parents=True)
    python.symlink_to(sys.executable)
    cfg = config.Config(programs={"demo": config.VenvProgram(
        kind="venv", python=str(python), git_dir=str(checkout))})
    monkeypatch.setattr(config, "load_config", lambda: cfg)
    return tmp_path, checkout, cfg


def _admin_leftovers(checkout: Path, days: float) -> tuple[Path, Path]:
    backup = checkout.parent / f"..venv.vq-admin-backup-{TXN}"
    failed = checkout.parent / f"..venv.vq-admin-failed-{TXN}"
    for path in (backup, failed):
        (path / "lib").mkdir(parents=True)
        (path / "lib" / "x.py").write_text("x")
        _old(path, days)
    return backup, failed


def _fetch_leftovers(days: float) -> list[Path]:
    workspace = paths.jobs_dir() / "job000000001"
    workspace.mkdir(parents=True)
    staging = workspace / ".vq-fetch-abc123"
    staging.mkdir()
    tar = workspace / ".vq-fetch-0f0f0f.tar"
    tar.write_text("t")
    stale = paths.jobs_dir() / ".vq-stale-job000000002-x1y2"
    stale.mkdir()
    keep = workspace / "result.out"
    keep.write_text("keep")
    return [_old(p, days) for p in (staging, tar, stale)]


def test_old_admin_and_fetch_leftovers_are_found_and_removed(host) -> None:
    _tmp, checkout, cfg = host
    backup, failed = _admin_leftovers(checkout, days=3)
    fetches = _fetch_leftovers(days=3)

    found = {item.path: item for item in leftovers.find(cfg)}

    assert set(found) == {backup, failed, *fetches}
    assert all(item.removable for item in found.values())
    for item in found.values():
        leftovers.remove(item, cfg)
    assert not any(path.exists() for path in found)
    assert (paths.jobs_dir() / "job000000001" / "result.out").exists()
    assert (checkout / ".venv").is_dir()


def test_young_leftovers_are_kept(host) -> None:
    _tmp, checkout, cfg = host
    _admin_leftovers(checkout, days=0.1)
    _fetch_leftovers(days=0.1)

    assert not any(item.removable for item in leftovers.find(cfg))


def test_an_admin_marker_protects_transaction_leftovers(host, monkeypatch) -> None:
    _tmp, checkout, cfg = host
    backup, _failed = _admin_leftovers(checkout, days=30)
    monkeypatch.setattr(admin, "admin_update_marker_exists", lambda: True)

    found = {item.path: item for item in leftovers.find(cfg)}

    assert not found[backup].removable
    assert "recover-update" in found[backup].reason
    with pytest.raises(ValueError, match="not removable"):
        leftovers.remove(found[backup], cfg)
    assert backup.is_dir()


def test_foreign_names_and_symlinks_are_ignored(host, tmp_path) -> None:
    _tmp, checkout, cfg = host
    other = checkout.parent / f".other-venv.vq-admin-backup-{TXN}"
    other.mkdir()
    _old(other, 30)
    target = tmp_path / "precious"
    target.mkdir()
    link = checkout.parent / f"..venv.vq-admin-backup-{'f' * 32}"
    link.symlink_to(target)
    _old(link, 30)

    assert leftovers.find(cfg) == []
    assert target.is_dir()


def test_auto_cleanup_sweeps_week_old_leftovers_only(host) -> None:
    _tmp, checkout, _cfg = host
    week_old = _admin_leftovers(checkout, days=10)
    fresh = _fetch_leftovers(days=2)

    counts = cleanup.run_auto_cleanup_pass(
        cleanup.AutoCleanupPolicy(delete_after_seconds=86400 * 30)
    )

    assert counts["leftovers_removed"] == 2
    assert not any(path.exists() for path in week_old)
    assert all(path.exists() for path in fresh)


def test_cli_dry_run_then_execute(host) -> None:
    from vq.cli import main

    _tmp, checkout, _cfg = host
    backup, failed = _admin_leftovers(checkout, days=3)

    listed = CliRunner().invoke(main, ["cleanup", "localhost", "--leftovers"])
    assert listed.exit_code == 0, listed.output
    assert "dry run: 2 removable" in listed.output
    assert backup.is_dir()
    removed = CliRunner().invoke(main, ["cleanup", "localhost", "--leftovers", "-x"])
    assert removed.exit_code == 0, removed.output
    assert not backup.exists() and not failed.exists()
