"""Deployment cache copying and identity fences; no chemistry fixture (#127)."""
from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "agentic-loop/fleet/seed-gfn2-cache.py"

# seed-gfn2-cache.py lives in the agentic-loop tree, which is internal and
# was not carried into the split repositories. The test is kept so it can
# be revived if that tooling is ever re-homed here.
pytestmark = pytest.mark.skipif(
    not SCRIPT.is_file(),
    reason="agentic-loop/fleet/seed-gfn2-cache.py is not part of this repository",
)
SOURCE_SHA = "a" * 64
RAW = f'# source_sha256 = "{SOURCE_SHA}"\nprojection = "per-l"\n'.encode()
CACHE_SHA = hashlib.sha256(RAW).hexdigest()


@pytest.fixture
def seeder():
    spec = importlib.util.spec_from_file_location("gfn2_cache_seed", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_plan_is_read_only_and_apply_is_idempotent(seeder, tmp_path):
    root = tmp_path / "cache"
    plan = seeder.seed(RAW, root, CACHE_SHA, SOURCE_SHA)
    assert plan["phase"] == "planned" and not root.exists()
    staged = seeder.seed(RAW, root, CACHE_SHA, SOURCE_SHA, apply=True)
    target = Path(staged["cache_file"])
    assert target.read_bytes() == RAW
    assert target.stat().st_mode & 0o222 == 0
    before = target.stat()
    again = seeder.seed(RAW, root, CACHE_SHA, SOURCE_SHA, apply=True)
    assert again["phase"] == "already_present"
    assert target.stat().st_ino == before.st_ino
    assert target.stat().st_mtime_ns == before.st_mtime_ns


@pytest.mark.parametrize("failure", ["cache-hash", "source-hash", "missing-source",
                                    "duplicate-source", "old-projection", "invalid-hash"])
def test_invalid_identity_never_creates_destination(seeder, tmp_path, failure):
    raw, cache_sha, source_sha = RAW, CACHE_SHA, SOURCE_SHA
    if failure == "cache-hash":
        raw += b"changed\n"
    elif failure == "source-hash":
        source_sha = "b" * 64
    elif failure == "invalid-hash":
        cache_sha = "abc"
    else:
        if failure == "missing-source":
            raw = b'projection = "per-l"\n'
        elif failure == "duplicate-source":
            raw = RAW + RAW.splitlines(keepends=True)[0]
        else:
            raw = RAW.replace(b"per-l", b"per-shell")
        cache_sha = hashlib.sha256(raw).hexdigest()
    with pytest.raises(ValueError):
        seeder.seed(raw, tmp_path / "cache", cache_sha, source_sha, apply=True)
    assert not (tmp_path / "cache").exists()


@pytest.mark.parametrize("redirect", ["root", "target", "corrupt"])
def test_existing_foreign_or_corrupt_destination_is_preserved(seeder, tmp_path, redirect):
    root = tmp_path / "cache"
    foreign = tmp_path / "foreign"
    foreign.mkdir()
    target = root / CACHE_SHA / seeder.CACHE_NAME
    if redirect == "root":
        root.symlink_to(foreign, target_is_directory=True)
    else:
        target.parent.mkdir(parents=True)
        if redirect == "target":
            (foreign / "data").write_bytes(b"old")
            target.symlink_to(foreign / "data")
        else:
            target.write_bytes(b"old")
    with pytest.raises((ValueError, OSError)):
        seeder.seed(RAW, root, CACHE_SHA, SOURCE_SHA, apply=True)
    if redirect == "root":
        assert not list(foreign.iterdir())
    else:
        assert target.read_bytes() == b"old"


def test_nonregular_source_is_rejected_without_blocking(seeder, tmp_path):
    fifo = tmp_path / "fifo"
    os.mkfifo(fifo)
    with pytest.raises(ValueError, match="regular file"):
        seeder.read_regular(fifo)


def test_cli_accepts_bounded_stdin_and_records_job_environment(tmp_path):
    proc = subprocess.run(
        [sys.executable, str(SCRIPT), "--source", "-", "--cache-root", str(tmp_path),
         "--expected-cache-sha256", CACHE_SHA, "--expected-source-sha256", SOURCE_SHA,
         "--apply"], input=RAW, capture_output=True, timeout=10,
    )
    assert proc.returncode == 0, proc.stderr
    receipt = json.loads(proc.stdout)
    cache_dir = Path(receipt["environment"]["VIBEQC_GFN2_CACHE_DIR"])
    assert (cache_dir / "gfn2_xtb_params.toml").read_bytes() == RAW


def test_interruption_before_publication_can_be_retried(seeder, tmp_path, monkeypatch):
    def fail_link(*args):
        raise OSError("injected publication failure")

    with monkeypatch.context() as patch:
        patch.setattr(seeder.os, "link", fail_link)
        with pytest.raises(OSError, match="publication failure"):
            seeder.seed(RAW, tmp_path, CACHE_SHA, SOURCE_SHA, apply=True)
    assert not (tmp_path / CACHE_SHA / seeder.CACHE_NAME).exists()
    receipt = seeder.seed(RAW, tmp_path, CACHE_SHA, SOURCE_SHA, apply=True)
    assert Path(receipt["cache_file"]).read_bytes() == RAW


def test_concurrent_seeders_publish_one_complete_file(seeder, tmp_path):
    import concurrent.futures

    def apply(_):
        return seeder.seed(RAW, tmp_path, CACHE_SHA, SOURCE_SHA, apply=True)

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        receipts = list(pool.map(apply, range(8)))
    assert all(r["phase"] in {"staged", "already_present"} for r in receipts)
    assert (tmp_path / CACHE_SHA / seeder.CACHE_NAME).read_bytes() == RAW
    assert len(list((tmp_path / CACHE_SHA).iterdir())) == 1
