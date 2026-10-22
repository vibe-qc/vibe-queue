"""Independent admin updates share a driver without sharing a global mutex."""
from __future__ import annotations

import json
import multiprocessing
import os
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from vq import admin, config, paths
from vq.cli import main


def _concurrent_acquire(
    state_root: str,
    envs: list[str],
    host: str,
    start: Any,
    results: Any,
) -> None:
    os.environ[paths.ENV_STATE_DIR] = state_root
    start.wait(10)
    try:
        marker = admin.acquire_admin_update_marker(envs=envs, host=host)
    except admin.AdminError as exc:
        results.put(("error", str(exc)))
    else:
        results.put(("ok", marker.marker_id))


@pytest.fixture
def state_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    (tmp_path / "cfg" / "config.toml").write_text(
        'default_host = "localhost"\n'
    )
    admin._set_owned_admin_update_marker_path(None)
    return tmp_path


@pytest.mark.parametrize(
    ("left_envs", "left_host", "right_envs", "right_host", "conflicts"),
    [
        (["scheduler:host_f"], "host_f", ["scheduler:host_c"], "host_c", False),
        (
            ["scheduler:host_f"],
            "host_f",
            ["scheduler-runtime:host_f:vibeqc-release"],
            "host_f",
            True,
        ),
        (
            ["scheduler-runtime:host_f:vibeqc-release"],
            "host_f",
            ["scheduler-runtime:host_f:vibeqc-dev"],
            "host_f",
            False,
        ),
        (
            ["scheduler-runtime:host_f:vibeqc-release"],
            "host_f",
            ["scheduler-runtime:host_f:vibeqc-release"],
            "host_f",
            True,
        ),
        (["vibeqc-release"], "localhost", ["vibeqc-dev"], "localhost", False),
        (
            ["vibeqc-release"],
            "localhost",
            ["vibeqc-release"],
            "localhost",
            True,
        ),
        (["future:scope"], "host_f", ["vibeqc-dev"], "localhost", True),
    ],
)
def test_scope_conflict_matrix(
    left_envs: list[str],
    left_host: str,
    right_envs: list[str],
    right_host: str,
    conflicts: bool,
) -> None:
    assert admin.admin_update_scopes_conflict(
        left_envs, left_host, right_envs, right_host
    ) is conflicts


def test_disjoint_update_acquires_own_lease_and_clears_only_it(
    state_dir: Path,
) -> None:
    first = admin.write_admin_update_marker(
        envs=["scheduler:host_f"], host="host_f"
    )
    second = admin.acquire_admin_update_marker(
        envs=["scheduler-runtime:host_c:vibeqc-release"],
        host="host_c",
    )

    assert [marker.envs for marker in admin.read_admin_update_markers()] == [
        first.envs,
        second.envs,
    ]
    assert admin.clear_admin_update_marker() == second
    assert admin.read_admin_update_markers() == [first]


def test_disjoint_processes_acquire_concurrently(state_dir: Path) -> None:
    context = multiprocessing.get_context("spawn")
    start = context.Event()
    results = context.Queue()
    state_root = str(paths.state_root())
    processes = [
        context.Process(
            target=_concurrent_acquire,
            args=(
                state_root,
                ["scheduler:host_f"],
                "host_f",
                start,
                results,
            ),
        ),
        context.Process(
            target=_concurrent_acquire,
            args=(
                state_root,
                ["scheduler-runtime:host_c:vibeqc-release"],
                "host_c",
                start,
                results,
            ),
        ),
    ]
    for process in processes:
        process.start()
    start.set()
    outcomes = [results.get(timeout=15) for _ in processes]
    for process in processes:
        process.join(timeout=15)
        assert process.exitcode == 0

    assert sorted(outcome[0] for outcome in outcomes) == ["ok", "ok"]
    assert len(admin.read_admin_update_markers()) == 2


def test_same_component_and_helper_runtime_overlap_fail_closed(
    state_dir: Path,
) -> None:
    admin.write_admin_update_marker(envs=["scheduler:host_f"], host="host_f")

    with pytest.raises(admin.AdminUpdateInProgress):
        admin.acquire_admin_update_marker(
            envs=["scheduler-runtime:host_f:vibeqc-release"],
            host="host_f",
        )
    with pytest.raises(admin.AdminUpdateInProgress):
        admin.acquire_admin_update_marker(
            envs=["scheduler:host_f"],
            host="host_f",
        )
    assert len(admin.read_admin_update_markers()) == 1


def test_different_runtime_components_on_same_host_are_independent(
    state_dir: Path,
) -> None:
    admin.write_admin_update_marker(
        envs=["scheduler-runtime:host_f:vibeqc-dev"],
        host="host_f",
    )

    admin.acquire_admin_update_marker(
        envs=["scheduler-runtime:host_f:vibe-view"],
        host="host_f",
    )

    assert len(admin.read_admin_update_markers()) == 2


def test_stale_marker_blocks_its_scope_but_not_an_independent_host(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stale = admin.write_admin_update_marker(
        envs=["scheduler:host_f"], host="host_f"
    )
    monkeypatch.setattr(admin, "_pid_liveness", lambda pid: False)
    assert admin.admin_update_marker_stale_reason(stale) is not None

    with pytest.raises(admin.AdminUpdateInProgress):
        admin.acquire_admin_update_marker(
            envs=["scheduler-runtime:host_f:vibeqc-release"],
            host="host_f",
        )
    admin.acquire_admin_update_marker(
        envs=["scheduler-runtime:host_c:vibeqc-release"],
        host="host_c",
    )
    assert len(admin.read_admin_update_markers()) == 2


def test_unreadable_legacy_marker_blocks_every_scope(state_dir: Path) -> None:
    path = admin.admin_update_marker_path()
    path.parent.mkdir(parents=True)
    path.write_text("not-json")

    with pytest.raises(admin.AdminUpdateInProgress):
        admin.acquire_admin_update_marker(
            envs=["scheduler-runtime:host_c:vibeqc-release"],
            host="host_c",
        )


def test_status_preserves_single_marker_shape_and_lists_all_leases(
    state_dir: Path,
) -> None:
    first = admin.write_admin_update_marker(
        envs=["scheduler:host_f"], host="host_f"
    )
    second = admin.acquire_admin_update_marker(
        envs=["scheduler-runtime:host_c:vibeqc-release"],
        host="host_c",
    )

    payload = json.loads(admin.format_admin_status_json(config.load_config()))

    assert payload["marker"]["envs"] == first.envs
    assert [item["envs"] for item in payload["markers"]] == [
        first.envs,
        second.envs,
    ]


def test_targeted_clear_preserves_an_independent_scheduler_lease(
    state_dir: Path,
) -> None:
    (state_dir / "cfg" / "config.toml").write_text(
        "\n".join(
            [
                'default_host = "localhost"',
                "[hosts.localhost]",
                'ssh = "localhost"',
                "[hosts.host_f]",
                'ssh = "host_f"',
                'scheduler = "pbs"',
                'scheduler_dialect = "torque"',
                'scratch_root = "/scratch/user"',
                'scheduler_driver = "localhost"',
                "[hosts.host_c]",
                'ssh = "host_c"',
                'scheduler = "slurm"',
                'scheduler_dialect = "slurm"',
                'scratch_root = "/scratch/user"',
                'scheduler_driver = "localhost"',
            ]
        )
    )
    host_f = admin.write_admin_update_marker(
        envs=["scheduler:host_f"], host="host_f"
    )
    host_c = admin.acquire_admin_update_marker(
        envs=["scheduler:host_c"], host="host_c"
    )

    result = CliRunner().invoke(
        main,
        [
            "admin",
            "clear-update-marker",
            "host_f",
            "--yes",
            "--force-live",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "marker cleared" in result.output
    assert admin.read_admin_update_markers() == [host_c]
    assert host_f not in admin.read_admin_update_markers()


def test_clear_failed_runtime_slot_marker(state_dir: Path) -> None:
    marker = admin.acquire_admin_update_marker(
        envs=["runtime-slot:vibeqc-release"], host="localhost",
    )
    admin.transition_admin_update_state(
        admin.ADMIN_UPDATE_STATE_FAILED, failure_reason="build failed",
    )
    result = CliRunner().invoke(
        main, ["admin", "clear-update-marker", "localhost", "--json"],
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["cleared"] is True
    assert payload["marker"]["marker_id"] == marker.marker_id
    assert not admin.read_admin_update_markers()


def test_clear_multiple_local_markers_requires_exact_selection(
    state_dir: Path,
) -> None:
    first = admin.acquire_admin_update_marker(
        envs=["runtime-slot:vibeqc-release"], host="localhost",
    )
    second = admin.acquire_admin_update_marker(
        envs=["runtime-slot:vibeqc-dev"], host="localhost",
    )
    scheduler = admin.acquire_admin_update_marker(
        envs=["scheduler:host_f"], host="host_f",
    )
    args = ["admin", "clear-update-marker", "localhost", "--json", "--force-live"]
    ambiguous = CliRunner().invoke(main, args)
    assert ambiguous.exit_code != 0
    assert "multiple admin-update markers" in ambiguous.output
    assert len(admin.read_admin_update_markers()) == 3
    selected = CliRunner().invoke(main, [*args, "--marker-id", second.marker_id])
    assert selected.exit_code == 0, selected.output
    assert json.loads(selected.output)["marker"]["marker_id"] == second.marker_id
    assert admin.read_admin_update_markers() == [first, scheduler]
    wrong_scope = CliRunner().invoke(main, [*args, "--marker-id", scheduler.marker_id])
    assert wrong_scope.exit_code != 0
    assert "requested host scope" in wrong_scope.output
    assert admin.read_admin_update_markers() == [first, scheduler]


def test_exact_runtime_slot_selection_retains_live_marker(state_dir: Path) -> None:
    marker = admin.acquire_admin_update_marker(
        envs=["runtime-slot:vibeqc-release"], host="localhost",
    )
    result = CliRunner().invoke(
        main,
        ["admin", "clear-update-marker", "localhost", "--json",
         "--marker-id", marker.marker_id],
    )
    assert result.exit_code != 0
    assert "refusing to clear live" in json.loads(result.output)["error"]
    assert admin.read_admin_update_markers() == [marker]


@pytest.mark.parametrize("receipt", ["managed", "pause"])
def test_exact_runtime_slot_selection_retains_protected_receipt(
    state_dir: Path, receipt: str,
) -> None:
    marker = admin.acquire_admin_update_marker(
        envs=["runtime-slot:vibeqc-release"], host="localhost",
    )
    if receipt == "managed":
        marker.managed_transaction = {"schema": "test-managed-receipt"}
    else:
        # A legacy pause receipt lacks the exact namespace needed for recovery.
        marker.pause_token = "admin-update-0123456789ab"
    admin._write_admin_update_marker_atomic(marker)
    result = CliRunner().invoke(
        main,
        ["admin", "clear-update-marker", "localhost", "--yes", "--force-live",
         "--marker-id", marker.marker_id],
    )
    assert result.exit_code != 0
    assert ("recover-update" if receipt == "managed" else "queue namespace") in result.output
    assert admin.read_admin_update_markers() == [marker]


def test_exact_marker_selection_delegates_to_remote(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    (state_dir / "cfg" / "config.toml").write_text(
        'default_host = "localhost"\n[hosts.host_e]\nssh = "host_e"\n'
    )
    calls = []
    monkeypatch.setattr(
        "vq.cli._delegate_to_remote",
        lambda host, cfg, *args: calls.append((host, args)) or "cleared\n",
    )
    result = CliRunner().invoke(
        main,
        ["admin", "clear-update-marker", "host_e", "--json",
         "--marker-id", "exact-marker"],
    )
    assert result.exit_code == 0, result.output
    assert calls == [("host_e", (
        "admin", "clear-update-marker", "--marker-id", "exact-marker", "--json", "localhost",
    ))]
