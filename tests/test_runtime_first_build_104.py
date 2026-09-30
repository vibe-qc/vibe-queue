"""A runtime slot's first build creates its environment through the installer (#104).

The fixture's updater is strict in the way vibe-qc's and vibe-view's are: it
refuses in a tree without a virtualenv. A fresh generation never has one, so a
slot built through ``update_script`` alone could not be prepared, while the
migration plan said nothing was in the way.
"""
from __future__ import annotations

import shlex
import subprocess
import sys
from pathlib import Path

import pytest

from tests.test_runtime_execution_binding import runtimes as runtimes
from tests.test_runtime_migration import request_manifest as request_manifest
from vq import admin, paths, runtime_slots
from vq import runtime_migration as migration

UPDATER = """#!/bin/sh
set -eu
if [ ! -x .venv/bin/python ]; then
  echo "Error: no usable virtualenv found at .venv/; create one with install.sh" >&2
  exit 1
fi
echo "refreshed $*" > .venv/built-by
"""

INSTALLER = """#!/bin/sh
set -eu
if [ -n "${FIRST_BUILD_PROBE_FAIL:-}" ]; then
  echo "installer refused: probe flag set" >&2
  exit 3
fi
@PYTHON@ -m venv --without-pip .venv
echo "installed $*" > .venv/built-by
echo "installer ran"
""".replace("@PYTHON@", shlex.quote(sys.executable))


def _commit_builders(repo: Path) -> str:
    (repo / ".gitignore").write_text(".venv/\n")
    (repo / "update.sh").write_text(UPDATER)
    (repo / "install.sh").write_text(INSTALLER)
    for argv in (["add", "update.sh", "install.sh", ".gitignore"],
                 ["-c", "user.name=First Build", "-c", "user.email=test@example.invalid",
                  "commit", "-qm", "strict updater and installer"]):
        subprocess.run(["git", "-C", str(repo), *argv], check=True, capture_output=True)
    return subprocess.check_output(["git", "-C", str(repo), "rev-parse", "HEAD"],
                                   text=True).strip()


def _manifest(request_manifest, runtimes, *, install: str | None):
    root, _shas, program, _cfg = runtimes
    sha = _commit_builders(Path(program.git_dir))
    lines = 'kind = "venv"\nupdate_script = "update.sh --dev"'
    if install is not None:
        lines += f"\ninstall_script = {install!r}".replace("'", '"')
    for registry in request_manifest.registries:
        for path in (Path(registry.path), Path(registry.candidate)):
            path.write_text(path.read_text().replace('kind = "venv"', lines))
    return request_manifest.model_copy(update={"source_sha": sha}), root, sha


def _built_by(root: Path, sha: str) -> str:
    return (runtime_slots.slot_source(root, sha) / ".venv" / "built-by").read_text().strip()


def test_plan_blocks_a_first_generation_without_an_installer(request_manifest, runtimes):
    manifest, _root, _sha = _manifest(request_manifest, runtimes, install=None)
    value = migration.plan(manifest)
    assert value["status"] == "blocked"
    assert any("install_script" in blocker for blocker in value["blockers"])
    with pytest.raises(migration.MigrationError, match="unresolved blockers"):
        migration.prepare(manifest, value["plan_id"])


def test_a_verified_generation_needs_no_installer(request_manifest):
    # The fixture's generation is sealed already: nothing is built, nothing blocks.
    value = migration.plan(request_manifest)
    assert value["status"] == "ready"
    assert value["blockers"] == []


@pytest.mark.no_autopatch_lifecycle_lock
@pytest.mark.no_autopatch_branch_check
@pytest.mark.no_autopatch_build_runner
def test_first_generation_builds_through_the_installer(request_manifest, runtimes):
    manifest, root, sha = _manifest(request_manifest, runtimes, install="install.sh --current")
    old = [Path(r.path).read_bytes() for r in manifest.registries]
    current = runtime_slots.resolve_current(root)
    before = migration.plan(manifest)
    assert before["blockers"] == []
    after = migration.prepare(manifest, before["plan_id"])
    assert after["status"] == "ready"
    # The configured flags reach the installer unchanged and no ref selector is
    # appended: the slot source is already pinned. The updater never ran.
    assert _built_by(root, sha) == "installed --current"
    assert runtime_slots.resolve_current(root) == current
    assert [Path(r.path).read_bytes() for r in manifest.registries] == old
    transcripts = sorted(paths.admin_update_log_target_dir("demo").glob("*.log"))
    assert transcripts and "installer ran" in transcripts[-1].read_text()


@pytest.mark.no_autopatch_lifecycle_lock
@pytest.mark.no_autopatch_branch_check
@pytest.mark.no_autopatch_build_runner
def test_failed_first_build_names_its_output_and_retries_cleanly(
    request_manifest, runtimes, monkeypatch: pytest.MonkeyPatch,
):
    manifest, root, sha = _manifest(request_manifest, runtimes, install="install.sh --current")
    old = [Path(r.path).read_bytes() for r in manifest.registries]
    monkeypatch.setenv("FIRST_BUILD_PROBE_FAIL", "1")
    first = migration.plan(manifest)
    with pytest.raises(migration.MigrationError) as failure:
        migration.prepare(manifest, first["plan_id"])
    message = str(failure.value)
    assert "install.sh --current exited 3" in message
    assert "installer refused: probe flag set" in message
    transcript = Path(message.split("transcript: ", 1)[1].split(";", 1)[0])
    assert "installer refused: probe flag set" in transcript.read_text()
    assert [Path(r.path).read_bytes() for r in manifest.registries] == old
    # The failed generation is retained for the slot primitive's own recovery.
    assert runtime_slots.slot_path(root, sha).is_dir()
    monkeypatch.delenv("FIRST_BUILD_PROBE_FAIL")
    retry = migration.plan(manifest)
    assert retry["plan_id"] != first["plan_id"]  # the retained receipts are inventory
    assert retry["blockers"] == []
    assert migration.prepare(manifest, retry["plan_id"])["status"] == "ready"
    assert _built_by(root, sha) == "installed --current"


@pytest.mark.no_autopatch_lifecycle_lock
@pytest.mark.no_autopatch_branch_check
def test_external_installer_refuses_before_reserving_a_slot(request_manifest, runtimes,
                                                           tmp_path: Path):
    outside = tmp_path / "elsewhere" / "install.sh"
    outside.parent.mkdir()
    outside.write_text(INSTALLER)
    manifest, root, sha = _manifest(request_manifest, runtimes, install=str(outside))
    value = migration.plan(manifest)
    # The plan says so first (#104 verification), so prepare refuses on it.
    assert any("inside the configured source tree" in b for b in value["blockers"])
    with pytest.raises(migration.MigrationError, match="unresolved blockers"):
        migration.prepare(manifest, value["plan_id"])
    assert not runtime_slots.slot_path(root, sha).exists()
    # The slot builder refuses the same installer on its own, too.
    prog = migration.config.VenvProgram.model_validate(value["program"])
    result = admin._do_slot_update_work("demo", prog.model_copy(
        update={"runtime_slot_root": str(root)}), expected_sha=sha)
    assert any("inside the configured source tree" in e for e in result.work_errors)
    assert not runtime_slots.slot_path(root, sha).exists()


@pytest.mark.no_autopatch_lifecycle_lock
@pytest.mark.no_autopatch_branch_check
def test_ordinary_slot_update_builds_a_new_generation_through_the_installer(runtimes):
    root, _shas, program, _cfg = runtimes
    sha = _commit_builders(Path(program.git_dir))
    prog = program.model_copy(update={"update_script": "update.sh --dev",
                                      "install_script": "install.sh --current"})
    result = admin._do_slot_update_work("demo", prog, expected_sha=sha)
    assert result.success, result.work_errors
    assert runtime_slots.resolve_current(root) == sha
    assert _built_by(root, sha) == "installed --current"
