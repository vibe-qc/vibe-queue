"""Disjoint local immutable builds share the driver, not mutation ownership."""
from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from pathlib import Path

import pytest

from tests.test_admin_lifecycle_transaction import _git
from vq import admin, config, runtime_slots

pytestmark = [
    pytest.mark.no_autopatch_lifecycle_lock,
    pytest.mark.no_autopatch_branch_check,
]


def _program(root: Path) -> tuple[config.VenvProgram, str]:
    repo = root / "source"
    repo.mkdir(parents=True)
    _git(repo, "init", "--initial-branch", "main")
    (repo / ".gitignore").write_text(".venv/\n")
    (repo / "build.sh").write_text(
        "#!/bin/sh\nset -eu\nmkdir -p .venv/bin\n"
        "printf '#!/bin/sh\\nexit 0\\n' > .venv/bin/python\n"
        "chmod +x .venv/bin/python\n"
    )
    _git(repo, "add", ".gitignore", "build.sh")
    _git(repo, "commit", "-m", "synthetic builder")
    (repo / ".venv/bin").mkdir(parents=True)
    (repo / ".venv/bin/python").write_text("placeholder\n")
    return config.VenvProgram(
        kind="venv", python=str(repo / ".venv/bin/python"),
        git_dir=str(repo), branch="main", update_script="build.sh",
        runtime_slot_root=str(root / "runtime"), runtime_slot_execution=True,
    ), _git(repo, "rev-parse", "HEAD")


@pytest.mark.parametrize("first_rc", [0, 1])
@pytest.mark.parametrize("mode", ["disjoint", "unknown-driver", "nested", "unbound-runtime"])
def test_local_build_overlap_retains_real_resource_fences(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, first_rc: int, mode: str,
) -> None:
    first_prog, sha = _program(tmp_path / "first")
    if mode == "unbound-runtime":
        first_prog = first_prog.model_copy(update={"runtime_slot_execution": False})
    second_prog, second_sha = _program(tmp_path / "second")
    driver, _ = _program(tmp_path / "driver")
    cfg = config.Config(programs={
        "first": first_prog, "second": second_prog,
        "same-store": second_prog.model_copy(update={
            "runtime_slot_root": first_prog.runtime_slot_root,
        }),
    })
    monkeypatch.setattr(config, "load_config", lambda: cfg)
    driver_resources = admin.toolset_lifecycle_resources([driver])
    monkeypatch.setattr(admin, "_scheduler_runtime_driver_resources",
                        lambda: () if mode == "unknown-driver" else driver_resources)
    monkeypatch.setattr(admin, "_maybe_restart_daemon", lambda *a, **kw: None)
    monkeypatch.setattr(admin, "_maybe_refresh_console", lambda *a, **kw: None)
    entered, release = threading.Event(), threading.Event()
    original = admin._run_update_script

    def build(git_dir, *args, **kwargs):
        if Path(first_prog.runtime_slot_root) in git_dir.parents:
            entered.set()
            assert release.wait(10), "test did not release first build"
            if first_rc:
                return first_rc, "controlled build failure", 0.0
        return original(git_dir, *args, **kwargs)

    monkeypatch.setattr(admin, "_run_update_script", build)

    def update_first():
        with admin.admin_update_ownership() if mode == "nested" else nullcontext():
            return admin.update_env("first", cfg, host="localhost", expected_sha=sha)

    with ThreadPoolExecutor(max_workers=1) as executor:
        first = executor.submit(update_first)
        try:
            if not entered.wait(10):
                pytest.fail(f"first build never started: {first.result(timeout=1)!r}")
            marker_env = "first" if mode == "unbound-runtime" else "runtime-slot:first"
            marker = next(m for m in admin.read_admin_update_markers()
                          if m.envs == [marker_env])
            if mode != "unbound-runtime":
                assert not marker.owns_pause_scope
            if mode != "disjoint":
                with pytest.raises(admin.AdminUpdateInProgress, match="checkout.*mutated"):
                    admin.update_env("second", cfg, host="localhost", expected_sha=second_sha)
            else:
                second = admin.update_env("second", cfg, host="localhost", expected_sha=second_sha)
                assert second.success, second.work_errors
                assert runtime_slots.resolve_current(second_prog.runtime_slot_root) == second_sha
                assert not first.done()
                with pytest.raises(admin.AdminUpdateInProgress, match="lifecycle"):
                    admin.update_env("same-store", cfg, host="localhost", expected_sha=second_sha)
                alias = tmp_path / "source-alias"
                alias.symlink_to(first_prog.git_dir, target_is_directory=True)
                aliased = first_prog.model_copy(update={"git_dir": str(alias)})
                for protected in (aliased, driver):
                    with (
                        pytest.raises(admin.AdminUpdateInProgress, match="lifecycle"),
                        admin.toolset_lifecycle_lock([protected], action="mutation probe"),
                    ):
                        pytest.fail("protected mutation admitted")
                retained = next(m for m in admin.read_admin_update_markers()
                                if m.envs == ["runtime-slot:first"])
                assert retained.marker_id == marker.marker_id
                assert admin.read_admin_status()["second"].last_success
        finally:
            release.set()
        first_result = first.result(timeout=10)
    assert first_result.success is (first_rc == 0), first_result.work_errors
    assert admin.read_admin_status()["first"].last_success is (first_rc == 0)
    if mode == "disjoint":
        assert admin.read_admin_status()["second"].last_success
    remaining = admin.read_admin_update_markers()
    assert bool(remaining) is (first_rc != 0)
    if remaining:
        assert remaining[0].marker_id == marker.marker_id
        assert remaining[0].state == admin.ADMIN_UPDATE_STATE_FAILED
