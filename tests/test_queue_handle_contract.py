"""Compatibility characterization for queue handles and delegated hosts."""
from __future__ import annotations

import json
import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest

from vq import cli, logs, status, top, wait
from vq.config import HostConfig
from vq.spec import JobSpec, JobState
from vq.web import fleet as fleet_mod

_JOB_ID = "handle000001"
_SUBMITTED_AT = "2026-08-02T12:34:56+00:00"
_CONTEXT_HOST = "host_d"
_LOCAL_HANDLE_HOSTS = (None, "", "localhost", "127.0.0.1", "::1")


def _running_spec(
    tmp_path: Path,
    *,
    scheduler_target: str | None,
) -> JobSpec:
    return JobSpec(
        id=_JOB_ID,
        command=["true"],
        cwd=str(tmp_path),
        cpus=1,
        state=JobState.RUNNING,
        submitted_at=_SUBMITTED_AT,
        scheduler_target=scheduler_target,
    )


@pytest.mark.parametrize(
    ("scheduler_target", "context_host", "local_host"),
    [
        (None, _CONTEXT_HOST, "localhost"),
        ("", _CONTEXT_HOST, "localhost"),
        ("host_f", "host_f", "host_f"),
    ],
)
def test_spec_backed_queue_handle_projection_matrix(
    tmp_path: Path,
    scheduler_target: str | None,
    context_host: str,
    local_host: str,
) -> None:
    """Every real-spec producer emits the same ordered three-key record."""
    spec = _running_spec(tmp_path, scheduler_target=scheduler_target)
    expected_context = {
        "job_id": _JOB_ID,
        "host": context_host,
        "submitted_at": _SUBMITTED_AT,
    }
    direct_context_handles = {
        "status": status._queue_handle_payload(spec, _CONTEXT_HOST),
        "logs": logs._queue_handle_payload(spec, _CONTEXT_HOST),
        "queue": cli._queue_json_row(spec, _CONTEXT_HOST)["queue_handle"],
        "top": top.gather_top_rows([spec], host=_CONTEXT_HOST)[0].queue_handle,
        "web_fleet": fleet_mod._job_row(spec, _CONTEXT_HOST)["queue_handle"],
    }
    for producer, handle in direct_context_handles.items():
        assert handle == expected_context, producer
        assert list(handle) == ["job_id", "host", "submitted_at"], producer

    local_handle = wait._queue_handle_from_spec(spec)
    assert local_handle == {
        "job_id": _JOB_ID,
        "host": local_host,
        "submitted_at": _SUBMITTED_AT,
    }
    assert list(local_handle) == ["job_id", "host", "submitted_at"]

    tail_payload = json.loads(
        cli._tail_json_payload(
            jobid=_JOB_ID,
            requested_host=_CONTEXT_HOST,
            filename="calc.out",
            lines=25,
            text="SCF done",
            path=f"{tmp_path}/calc.out",
            spec=spec,
        )
    )
    assert tail_payload["queue_handle"] == expected_context
    assert list(tail_payload["queue_handle"]) == [
        "host",
        "job_id",
        "submitted_at",
    ]


@pytest.mark.parametrize("reported_host", _LOCAL_HANDLE_HOSTS)
def test_delegated_object_rewrites_only_legacy_local_handles(
    reported_host: str | None,
) -> None:
    source = json.dumps(
        {
            "id": _JOB_ID,
            "host": reported_host,
            "submitted_at": _SUBMITTED_AT,
            "queue_handle": {
                "job_id": _JOB_ID,
                "host": reported_host,
                "submitted_at": _SUBMITTED_AT,
            },
        }
    )

    rendered = cli._json_with_requested_queue_handle_host(source, _CONTEXT_HOST)
    payload = json.loads(rendered)

    assert rendered.endswith("\n")
    assert payload["host"] == _CONTEXT_HOST
    assert payload["queue_handle"] == {
        "job_id": _JOB_ID,
        "host": _CONTEXT_HOST,
        "submitted_at": _SUBMITTED_AT,
    }


@pytest.mark.parametrize("reported_host", _LOCAL_HANDLE_HOSTS)
def test_delegated_rows_and_fleet_rewrite_the_same_legacy_aliases(
    reported_host: str | None,
) -> None:
    row = {
        "id": _JOB_ID,
        "submitted_at": _SUBMITTED_AT,
        "queue_handle": {
            "job_id": _JOB_ID,
            "host": reported_host,
            "submitted_at": _SUBMITTED_AT,
        },
    }

    rendered = cli._json_rows_with_requested_queue_handle_host(
        json.dumps([row]), _CONTEXT_HOST
    )
    cli_row = json.loads(rendered)[0]
    fleet_rows = [json.loads(json.dumps(row))]
    normalized = fleet_mod._normalize_rows(fleet_rows, _CONTEXT_HOST)

    assert not rendered.endswith("\n")
    assert cli_row["queue_handle"]["host"] == _CONTEXT_HOST
    assert normalized is fleet_rows
    assert normalized[0]["queue_host"] == _CONTEXT_HOST
    assert normalized[0]["queue_handle"]["host"] == _CONTEXT_HOST


def test_delegated_normalizers_preserve_existing_nonlocal_handle() -> None:
    row = {
        "id": _JOB_ID,
        "command": ["true"],
        "cwd": "/tmp/queue-handle-contract",
        "cpus": 1,
        "state": "running",
        "host": "existing-top-level",
        "scheduler_target": "host_f",
        "submitted_at": _SUBMITTED_AT,
        "queue_handle": {
            "job_id": _JOB_ID,
            "host": "existing-handle",
            "submitted_at": _SUBMITTED_AT,
        },
    }

    obj = json.loads(
        cli._json_with_requested_queue_handle_host(json.dumps(row), _CONTEXT_HOST)
    )
    rows = json.loads(
        cli._json_rows_with_requested_queue_handle_host(
            json.dumps([row]), _CONTEXT_HOST
        )
    )
    fleet_rows = fleet_mod._normalize_rows(
        [json.loads(json.dumps(row))], _CONTEXT_HOST
    )

    assert obj["host"] == "existing-top-level"
    assert obj["queue_handle"]["host"] == "existing-handle"
    assert rows[0]["queue_handle"]["host"] == "existing-handle"
    assert fleet_rows[0]["queue_host"] == "host_f"
    assert fleet_rows[0]["queue_handle"]["host"] == "existing-handle"


def test_empty_scheduler_target_falls_back_to_requested_host() -> None:
    row = {
        "id": _JOB_ID,
        "host": "localhost",
        "scheduler_target": "",
        "submitted_at": _SUBMITTED_AT,
        "queue_handle": {
            "job_id": _JOB_ID,
            "host": "localhost",
            "submitted_at": _SUBMITTED_AT,
        },
    }

    obj = json.loads(
        cli._json_with_requested_queue_handle_host(json.dumps(row), _CONTEXT_HOST)
    )
    rows = json.loads(
        cli._json_rows_with_requested_queue_handle_host(
            json.dumps([row]), _CONTEXT_HOST
        )
    )
    fleet_rows = fleet_mod._normalize_rows(
        [json.loads(json.dumps(row))], _CONTEXT_HOST
    )

    assert obj["host"] == _CONTEXT_HOST
    assert obj["queue_handle"]["host"] == _CONTEXT_HOST
    assert rows[0]["queue_handle"]["host"] == _CONTEXT_HOST
    assert fleet_rows[0]["queue_host"] == _CONTEXT_HOST
    assert fleet_rows[0]["queue_handle"]["host"] == _CONTEXT_HOST


def test_missing_handle_synthesis_remains_shape_specific() -> None:
    object_payload = json.loads(
        cli._json_with_requested_queue_handle_host(
            json.dumps(
                {
                    "id": _JOB_ID,
                    "host": "localhost",
                    "submitted_at": 17,
                }
            ),
            _CONTEXT_HOST,
        )
    )
    legacy_logs_payload = json.loads(
        cli._json_with_requested_queue_handle_host(
            json.dumps(
                {
                    "jobid": _JOB_ID,
                    "host": "localhost",
                    "submitted_at": _SUBMITTED_AT,
                }
            ),
            _CONTEXT_HOST,
        )
    )
    row_payload = json.loads(
        cli._json_rows_with_requested_queue_handle_host(
            json.dumps(
                [
                    {
                        "id": 7,
                        "jobid": 42,
                        "submitted_at": {"legacy": True},
                    }
                ]
            ),
            _CONTEXT_HOST,
        )
    )[0]
    fleet_rows = fleet_mod._normalize_rows(
        [{"id": _JOB_ID, "submitted_at": _SUBMITTED_AT}], _CONTEXT_HOST
    )

    assert object_payload["host"] == _CONTEXT_HOST
    assert object_payload["queue_handle"] == {
        "job_id": _JOB_ID,
        "host": _CONTEXT_HOST,
        "submitted_at": None,
    }
    assert legacy_logs_payload["host"] == _CONTEXT_HOST
    assert "queue_handle" not in legacy_logs_payload
    assert row_payload["queue_handle"] == {
        "job_id": 42,
        "host": _CONTEXT_HOST,
        "submitted_at": {"legacy": True},
    }
    assert fleet_rows == [
        {
            "id": _JOB_ID,
            "submitted_at": _SUBMITTED_AT,
            "queue_handle": {
                "job_id": _JOB_ID,
                "host": _CONTEXT_HOST,
                "submitted_at": _SUBMITTED_AT,
            },
            "queue_host": _CONTEXT_HOST,
            "effective_state": "scheduler_unknown",
            "scheduler_running_confirmed": None,
        }
    ]


def test_invalid_monitor_rows_never_publish_an_unsafe_handle() -> None:
    row = {
        "id": "../../escape",
        "state": "running",
        "scheduler_target": "host_f",
        "scheduler_state": ["running"],
        "queue_handle": {
            "job_id": "different-job",
            "host": "untrusted-host",
        },
    }

    queue_row = json.loads(
        cli._json_queue_rows_with_requested_handle_host(
            json.dumps([row]),
            _CONTEXT_HOST,
        )
    )[0]
    status_row = json.loads(
        cli._json_status_with_requested_handle_host(
            json.dumps(row),
            _CONTEXT_HOST,
        )
    )
    fleet_row = fleet_mod._normalize_rows(
        [json.loads(json.dumps(row))],
        _CONTEXT_HOST,
    )[0]

    assert queue_row["queue_handle"] is None
    assert status_row["queue_handle"] is None
    assert fleet_row["queue_handle"] is None


@pytest.mark.parametrize(
    ("normalizer", "source"),
    [
        (cli._json_with_requested_queue_handle_host, "not-json\n"),
        (cli._json_with_requested_queue_handle_host, "[]"),
        (cli._json_rows_with_requested_queue_handle_host, "not-json\n"),
        (cli._json_rows_with_requested_queue_handle_host, "{}"),
    ],
)
def test_delegated_normalizers_leave_wrong_shape_bytes_unchanged(
    normalizer,
    source: str,
) -> None:
    assert normalizer(source, _CONTEXT_HOST) == source


def test_tail_without_spec_keeps_complete_null_fallback() -> None:
    payload = json.loads(
        cli._tail_json_payload(
            jobid=_JOB_ID,
            requested_host=_CONTEXT_HOST,
            filename="calc.out",
            lines=25,
            text="SCF done",
            path=f"/workspace/{_JOB_ID}/calc.out",
            spec=None,
        )
    )

    assert payload == {
        "filename": "calc.out",
        "host": _CONTEXT_HOST,
        "jobid": _JOB_ID,
        "path": f"/workspace/{_JOB_ID}/calc.out",
        "queue_handle": {
            "host": _CONTEXT_HOST,
            "job_id": _JOB_ID,
            "submitted_at": None,
        },
        "state": None,
        "tail": 25,
        "text": "SCF done",
    }


@pytest.mark.parametrize(
    ("submitted_at", "expected_submitted_at"),
    [
        (_SUBMITTED_AT, _SUBMITTED_AT),
        (17, None),
        (None, None),
    ],
)
def test_remote_wait_missing_handle_uses_ssh_endpoint_and_typed_timestamp(
    submitted_at: object,
    expected_submitted_at: str | None,
) -> None:
    host_cfg = HostConfig(ssh="ssh-endpoint.invalid", remote_vq="/remote/vq")
    proc = subprocess.CompletedProcess(
        args=[],
        returncode=0,
        stdout=json.dumps(
            {
                "state": "completed",
                "exit_code": 0,
                "submitted_at": submitted_at,
            }
        ),
        stderr="",
    )

    with patch("vq.wait.transport.run_remote_vq", return_value=proc):
        result = wait.wait_for_terminal_remote(
            host_cfg,
            _JOB_ID,
            poll_interval=0.1,
            _now=lambda: 0.0,
            _sleep=lambda _seconds: None,
        )

    assert result.queue_handle == {
        "job_id": _JOB_ID,
        "host": "ssh-endpoint.invalid",
        "submitted_at": expected_submitted_at,
    }


def test_remote_wait_preserves_any_returned_mapping_verbatim() -> None:
    host_cfg = HostConfig(ssh="ssh-endpoint.invalid", remote_vq="/remote/vq")
    proc = subprocess.CompletedProcess(
        args=[],
        returncode=0,
        stdout=json.dumps(
            {
                "state": "completed",
                "exit_code": 0,
                "queue_handle": {},
            }
        ),
        stderr="",
    )

    with patch("vq.wait.transport.run_remote_vq", return_value=proc):
        result = wait.wait_for_terminal_remote(
            host_cfg,
            _JOB_ID,
            poll_interval=0.1,
            _now=lambda: 0.0,
            _sleep=lambda _seconds: None,
        )

    assert result.queue_handle == {}
