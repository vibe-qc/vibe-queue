"""#104 independent verification: two defects in the first-build route.

* An installer outside the source tree refuses at prepare time, but the plan
  reported ``preparation_required`` with no blockers: the same "plan promises
  a preparation that cannot succeed" that #104 was filed for.
* An ordinary ``vq admin update`` of a slot program with an ``install_script``
  dropped the operator's ``--update-script-arg`` flags and reported success on
  a profile nobody asked for.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from tests.test_runtime_execution_binding import runtimes  # noqa: F401 -- fixture
from tests.test_runtime_first_build_104 import INSTALLER, _built_by, _commit_builders, _manifest
from tests.test_runtime_migration import request_manifest  # noqa: F401 -- fixture
from vq import admin
from vq import runtime_migration as migration


def test_the_plan_blocks_an_installer_outside_the_source_tree(
    request_manifest, runtimes, tmp_path: Path,  # noqa: F811
) -> None:
    outside = tmp_path / "elsewhere" / "install.sh"
    outside.parent.mkdir()
    outside.write_text(INSTALLER)
    manifest, _root, _sha = _manifest(request_manifest, runtimes, install=str(outside))

    value = migration.plan(manifest)

    assert value["status"] == "blocked"
    assert any("inside the configured source tree" in b for b in value["blockers"]), value
    with pytest.raises(migration.MigrationError, match="unresolved blockers"):
        migration.prepare(manifest, value["plan_id"])


def test_an_installer_inside_the_tree_still_plans_without_a_blocker(
    request_manifest, runtimes,  # noqa: F811
) -> None:
    manifest, _root, _sha = _manifest(request_manifest, runtimes, install="install.sh --current")
    assert migration.plan(manifest)["blockers"] == []


def _slot_prog(program, **changes):
    return program.model_copy(update={"update_script": "update.sh --dev",
                                      "install_script": "install.sh --current", **changes})


@pytest.mark.no_autopatch_lifecycle_lock
@pytest.mark.no_autopatch_branch_check
@pytest.mark.parametrize(("supplied", "installer_saw"), [
    # The managed profile-change spelling: --recreate-venv is implicit.
    (["--recreate-venv", "--extras", "web"], "installed --current --extras web"),
    # Source selectors are stripped; the slot is already pinned.
    (["--ref", "main", "--extras", "test"], "installed --current --extras test"),
    ([], "installed --current"),
])
def test_operator_flags_reach_the_installer(
    runtimes, supplied: list[str], installer_saw: str,  # noqa: F811
) -> None:
    root, _shas, program, _cfg = runtimes
    sha = _commit_builders(Path(program.git_dir))

    result = admin._do_slot_update_work("demo", _slot_prog(program), expected_sha=sha,
                                        update_script_args=supplied)

    assert result.success, result.work_errors
    assert _built_by(root, sha) == installer_saw


@pytest.mark.no_autopatch_lifecycle_lock
@pytest.mark.no_autopatch_branch_check
def test_a_request_below_the_declared_floor_is_refused(runtimes, monkeypatch) -> None:  # noqa: F811
    _root, _shas, program, _cfg = runtimes
    sha = _commit_builders(Path(program.git_dir))
    # The floor applies only through vq's own installer; prove that for the fixture.
    monkeypatch.setattr(admin, "_vq_own_script_proof", lambda prog, cmd, names: (cmd, True))

    result = admin._do_slot_update_work(
        "demo", _slot_prog(program, extras=["web"]), expected_sha=sha,
        update_script_args=["--extras", "test"],
    )

    assert not result.success
    assert any("requested profile 'test' does not install web" in e
               for e in result.work_errors), result.work_errors


@pytest.mark.no_autopatch_lifecycle_lock
@pytest.mark.no_autopatch_branch_check
def test_a_malformed_request_names_the_flag_the_operator_used(
    runtimes, monkeypatch,  # noqa: F811
) -> None:
    _root, _shas, program, _cfg = runtimes
    sha = _commit_builders(Path(program.git_dir))
    monkeypatch.setattr(admin, "_vq_own_script_proof", lambda prog, cmd, names: (cmd, True))

    result = admin._do_slot_update_work(
        "demo", _slot_prog(program, extras=["web"]), expected_sha=sha,
        update_script_args=["--extras", "everything"],
    )

    assert not result.success
    assert any("--update-script-arg names extras profile 'everything'" in e
               for e in result.work_errors), result.work_errors
