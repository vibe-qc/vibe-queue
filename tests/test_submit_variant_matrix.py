"""CLI contract for local single, array, chain, and QVF submission routing."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import cli as cli_module
from vq import config, paths
from vq import submit as submit_module
from vq.cli import main
from vq.spec import ProgramRuntimePin

EXPECTED_SHA = "a" * 40


@pytest.fixture
def submit_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    config_dir = tmp_path / "cfg"
    config_dir.mkdir()
    (config_dir / "config.toml").write_text(
        '\n'.join(
            [
                'default_host = "localhost"',
                "",
                "[hosts.host_f]",
                'ssh = "host_f.invalid"',
                'scheduler = "pbs"',
                'scheduler_dialect = "torque"',
                'scratch_root = "/cluster"',
                'scheduler_driver = "localhost"',
                "",
            ]
        ),
        encoding="utf-8",
    )
    return tmp_path


@pytest.mark.parametrize("route", ["localhost", "host_f"])
@pytest.mark.parametrize(
    ("variant", "variant_args", "jobids"),
    [
        ("single", ["--refresh", "matrix-prog"], ["single000001"]),
        (
            "array",
            ["--array", "2"],
            ["array0000001", "array0000002"],
        ),
        (
            "chain",
            ["--chain", "2"],
            ["chain0000001", "chain0000002"],
        ),
        ("qvf", ["--qvf-force"], ["qvf000000001"]),
    ],
)
def test_local_submit_variant_matrix(
    submit_state: Path,
    monkeypatch: pytest.MonkeyPatch,
    route: str,
    variant: str,
    variant_args: list[str],
    jobids: list[str],
) -> None:
    """Both local routes preserve cardinality, provenance, and receipts."""
    local_pin = ProgramRuntimePin(
        expected_git_sha=EXPECTED_SHA,
        enforce_git_sha=True,
    )
    scheduler_pin = ProgramRuntimePin(
        expected_git_sha=EXPECTED_SHA,
        enforce_git_sha=True,
        scheduler_host="host_f",
        resolved_executable="/cluster/runtime/python",
        program_kind="scheduler-runtime",
        program_version="0.15.test",
        artifact_identity="/cluster/runtime/python",
    )
    calls: list[tuple[str, dict[str, object]]] = []
    warning_sinks: list[object] = []

    def record_call(kind: str, kwargs: dict[str, object]) -> None:
        warning_sinks.append(kwargs.pop("warning_sink"))
        calls.append((kind, kwargs))

    def fake_single(**kwargs: object) -> str:
        record_call("single", kwargs)
        return jobids[0]

    def fake_array(**kwargs: object) -> list[str]:
        record_call("array", kwargs)
        return list(jobids)

    def fake_chain(**kwargs: object) -> list[str]:
        record_call("chain", kwargs)
        return list(jobids)

    def fail_remote(**_kwargs: object) -> list[str]:
        raise AssertionError("local submit variants must not use submit_remote")

    monkeypatch.setattr(submit_module, "submit_local", fake_single)
    monkeypatch.setattr(submit_module, "submit_local_array", fake_array)
    monkeypatch.setattr(submit_module, "submit_local_chain", fake_chain)
    monkeypatch.setattr(submit_module, "submit_remote", fail_remote)
    monkeypatch.setattr(
        cli_module,
        "_validate_program_for_submit",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        cli_module,
        "_validate_expected_sha_for_submit",
        lambda *_args, **_kwargs: EXPECTED_SHA,
    )
    monkeypatch.setattr(
        cli_module,
        "_program_runtime_pin_for_submit",
        lambda *_args, **_kwargs: local_pin,
    )
    monkeypatch.setattr(
        cli_module,
        "_validate_scheduler_target_expected_sha",
        lambda *_args, **_kwargs: scheduler_pin,
    )
    monkeypatch.setattr(
        submit_module,
        "_validate_program_for_submit",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        submit_module,
        "_validate_expected_sha_for_submit",
        lambda *_args, **_kwargs: EXPECTED_SHA,
    )
    monkeypatch.setattr(
        submit_module,
        "_program_runtime_pin_for_submit",
        lambda *_args, **_kwargs: local_pin,
    )
    monkeypatch.setattr(
        submit_module,
        "_validate_scheduler_target_expected_sha",
        lambda *_args, **_kwargs: scheduler_pin,
    )
    monkeypatch.setattr(cli_module, "_multi_user_active", lambda *_args: False)
    monkeypatch.setattr(cli_module.host_status, "is_down", lambda _host: None)
    monkeypatch.setattr(
        cli_module.drain_module,
        "read_drain_state",
        lambda: None,
    )
    monkeypatch.setattr(
        cli_module.admin_module,
        "admin_update_marker_exists",
        lambda: False,
    )

    suffix = ".qvf" if variant == "qvf" else ".py"
    source = submit_state / f"matrix{suffix}"
    source.write_text("matrix\n", encoding="utf-8")
    args = [
        "submit",
        "--host",
        route,
        "--json",
        "--program",
        "matrix-prog",
        "--expected-sha",
        EXPECTED_SHA,
        "--cpus",
        "3",
        "--ntasks",
        "4",
        "--mem-mb",
        "512",
        "--wall-time-seconds",
        "90",
        "--priority",
        "2",
        "--auto-resume",
        "--retry",
        "3",
        "--job-name",
        "matrix",
        "--branch-name",
        "release",
        "--tag",
        "beta",
        "--tag",
        "alpha",
        "--at",
        "2026-08-03T10:00:00Z",
        "--clean-tmp",
        "--depends-on",
        "dep-ok",
        "--depends-on-any",
        "dep-any",
        "--rerun-until",
        "$VQ_WORKDIR/DONE",
        "--rerun-max",
        "7",
        "--vibeqc-preflight",
    ]
    if variant != "qvf":
        args.extend(["--python", "/matrix/python"])
    args.extend(variant_args)
    args.append(str(source))

    result = CliRunner().invoke(main, args)

    assert result.exit_code == 0, result.output
    expected_builder = "single" if variant in {"single", "qvf"} else variant
    expected_kwargs: dict[str, object] = {
        "host": "localhost",
        "input_file": str(source),
        "directory": None,
        "archive": None,
        "command": None,
        "python": (
            None
            if variant == "qvf"
            else (
                "/cluster/runtime/python"
                if route == "host_f"
                else "/matrix/python"
            )
        ),
        "cpus": 3,
        "scheduler_tasks": 4,
        "mem_mb": 512,
        "wall_time_seconds": 90,
        "priority": 2,
        "auto_resume": True,
        "retry": 3,
        "job_name": "matrix",
        "branch": "release",
        "program": "matrix-prog",
        "program_runtime_pin": scheduler_pin if route == "host_f" else local_pin,
        "tags": ["beta", "alpha"],
        "not_before": "2026-08-03T10:00:00+00:00",
        "depends_on": ["dep-ok"],
        "depends_on_any": ["dep-any"],
        "rerun_until_file_exists": "$VQ_WORKDIR/DONE",
        "rerun_max": 7,
        "clean_workdir_on_terminal": True,
        "vibeqc_preflight": True,
        "multi_user": False,
        "scheduler_target": "host_f" if route == "host_f" else None,
    }
    if variant == "array":
        expected_kwargs["array"] = 2
    elif variant == "chain":
        expected_kwargs["chain"] = 2
    else:
        expected_kwargs["refresh_before"] = (
            "matrix-prog" if variant == "single" else None
        )
        expected_kwargs["qvf_force"] = variant == "qvf"
    assert calls == [(expected_builder, expected_kwargs)]
    assert len(warning_sinks) == 1
    assert callable(warning_sinks[0])

    receipt = {
        "acceptance_scope": "queue",
        "execution_status": "not_observed",
        "capacity_warnings": [],
        "dispatch_holds": [],
        "host": route,
        "jobids": jobids,
        "next": [
            f"vq status {route} {jobids[0]}",
            f"vq logs {route} {jobids[0]} -f",
        ],
    }
    if route == "host_f":
        receipt.update(
            {
                "program_runtime_pin": {
                    "artifact_identity": "/cluster/runtime/python",
                    "expected_git_sha": EXPECTED_SHA,
                    "program_kind": "scheduler-runtime",
                    "program_version": "0.15.test",
                    "resolved_executable": "/cluster/runtime/python",
                    "scheduler_host": "host_f",
                },
                "scheduler": "pbs",
                "scheduler_driver": "localhost",
            }
        )
    assert json.loads(result.output) == receipt
