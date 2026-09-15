"""capacity.py — daemon budget advertisement (v0.11.0, increment ① of the
load-aware `vq submit auto` work) + its threading through overview's local
gather and the remote --json round-trip."""
from __future__ import annotations

import contextlib
import json
import os
import threading

import pytest
from pydantic import ValidationError

from vq import capacity, config, overview, paths, submit
from vq.spec import JobSpec, JobState
from vq.status import show_status_json


@pytest.fixture
def state_dir(tmp_path, monkeypatch):
    """Isolate ``state_root()`` to a fresh temp dir per test so the
    capacity file doesn't leak across tests."""
    monkeypatch.setenv("VQ_STATE_DIR", str(tmp_path))
    return tmp_path


_CAPACITY_LIMIT_FIELDS = [
    "max_cpus",
    "max_jobs",
    "max_mem_mb",
    "max_scheduler_jobs",
    "default_job_mem_mb",
]


def _capacity_payload(**overrides: object) -> dict[str, object]:
    return {
        "max_cpus": 4,
        "written_at": "2026-08-26T00:00:00+00:00",
        **overrides,
    }


@pytest.mark.parametrize("field", _CAPACITY_LIMIT_FIELDS)
@pytest.mark.parametrize("value", [True, False, 0, -1, 2.0, "2"])
def test_capacity_limits_require_strict_positive_integers(
    field: str,
    value: object,
) -> None:
    with pytest.raises(ValidationError):
        capacity.DaemonCapacity.model_validate(
            _capacity_payload(**{field: value})
        )


def test_capacity_limits_accept_positive_integers_and_optional_none() -> None:
    snapshot = capacity.DaemonCapacity(
        max_cpus=1,
        max_jobs=1,
        max_mem_mb=1,
        max_scheduler_jobs=1,
        default_job_mem_mb=1,
        written_at="2026-08-26T00:00:00+00:00",
    )
    assert [getattr(snapshot, field) for field in _CAPACITY_LIMIT_FIELDS] == [
        1,
        1,
        1,
        1,
        1,
    ]

    optional = capacity.DaemonCapacity(
        max_cpus=1,
        max_jobs=None,
        max_mem_mb=None,
        max_scheduler_jobs=None,
        default_job_mem_mb=None,
        written_at="2026-08-26T00:00:00+00:00",
    )
    assert optional.max_jobs is None
    assert optional.max_mem_mb is None
    assert optional.max_scheduler_jobs is None
    assert optional.default_job_mem_mb is None
    with pytest.raises(ValidationError):
        capacity.DaemonCapacity.model_validate(
            _capacity_payload(max_cpus=None)
        )


@pytest.mark.parametrize("field", _CAPACITY_LIMIT_FIELDS)
def test_invalid_capacity_assignment_is_rejected_without_mutation(
    field: str,
) -> None:
    snapshot = capacity.DaemonCapacity(
        max_cpus=4,
        max_jobs=4,
        max_mem_mb=4,
        max_scheduler_jobs=4,
        default_job_mem_mb=4,
        written_at="2026-08-26T00:00:00+00:00",
    )
    before = snapshot.model_dump()

    with pytest.raises(ValidationError):
        setattr(snapshot, field, 0)

    assert snapshot.model_dump() == before


@pytest.mark.parametrize("field", _CAPACITY_LIMIT_FIELDS)
def test_write_rejects_invalid_limits_without_creating_snapshot(
    state_dir,
    field: str,
) -> None:
    limits: dict[str, object] = {
        "max_cpus": 4,
        "max_jobs": None,
        "max_mem_mb": None,
        "max_scheduler_jobs": None,
        "default_job_mem_mb": None,
    }
    limits[field] = 0

    with pytest.raises(ValidationError):
        capacity.write_daemon_capacity(**limits)  # type: ignore[arg-type]

    assert not capacity.capacity_path().exists()


@pytest.mark.parametrize("field", _CAPACITY_LIMIT_FIELDS)
def test_invalid_persisted_limit_returns_none(
    state_dir,
    field: str,
) -> None:
    capacity.capacity_path().write_text(
        json.dumps(_capacity_payload(**{field: -1}))
    )

    assert capacity.read_daemon_capacity(via_rpc=False) is None


def test_invalid_rpc_mapping_falls_back_to_same_mode_file(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "operator"))
    monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(tmp_path / "system"))
    capacity.write_daemon_capacity(
        max_cpus=3,
        max_jobs=None,
        max_mem_mb=None,
        multi_user=False,
    )
    capacity.write_daemon_capacity(
        max_cpus=12,
        max_jobs=None,
        max_mem_mb=None,
        multi_user=True,
    )
    monkeypatch.setattr(
        "vq.rpc.try_rpc_or_fallback",
        lambda *args, **kwargs: _capacity_payload(
            max_cpus=-1,
            future_capacity_gate=99,
        ),
    )

    snapshot = capacity.read_daemon_capacity(multi_user=True)

    assert snapshot is not None
    assert snapshot.max_cpus == 12


def test_invalid_rpc_mapping_without_file_returns_none(
    state_dir,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "vq.rpc.try_rpc_or_fallback",
        lambda *args, **kwargs: _capacity_payload(max_mem_mb=False),
    )

    assert capacity.read_daemon_capacity(multi_user=False) is None


def test_valid_mixed_version_rpc_mapping_filters_unknown_fields(
    state_dir,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "vq.rpc.try_rpc_or_fallback",
        lambda *args, **kwargs: _capacity_payload(
            max_cpus=8,
            max_mem_mb=32_000,
            future_capacity_gate=99,
        ),
    )

    snapshot = capacity.read_daemon_capacity(multi_user=False)

    assert snapshot is not None
    assert snapshot.max_cpus == 8
    assert snapshot.max_mem_mb == 32_000


def test_write_then_read_round_trips(state_dir):
    capacity.write_daemon_capacity(
        max_cpus=6, max_jobs=2, max_mem_mb=32000, max_scheduler_jobs=3
    )
    cap = capacity.read_daemon_capacity()
    assert cap is not None
    assert (cap.max_cpus, cap.max_jobs, cap.max_mem_mb, cap.max_scheduler_jobs) == (
        6,
        2,
        32000,
        3,
    )
    assert cap.written_at  # non-empty ISO timestamp


def test_none_gates_round_trip(state_dir):
    # max_jobs / max_mem_mb / max_scheduler_jobs None = "no gate".
    capacity.write_daemon_capacity(max_cpus=18, max_jobs=None, max_mem_mb=None)
    cap = capacity.read_daemon_capacity()
    assert cap is not None
    assert cap.max_cpus == 18
    assert cap.max_jobs is None
    assert cap.max_mem_mb is None
    assert cap.max_scheduler_jobs is None


def test_configured_overages_cover_cpu_and_explicit_memory() -> None:
    snapshot = capacity.DaemonCapacity(
        max_cpus=16,
        max_mem_mb=49_340,
        default_job_mem_mb=4_000,
        written_at="2026-08-17T12:00:00+00:00",
    )

    overages = capacity.configured_capacity_overages(
        cpus=32,
        mem_mb=50_000,
        snapshot=snapshot,
    )

    assert [overage.to_payload() for overage in overages] == [
        {
            "resource": "cpus",
            "requested": 32,
            "limit": 16,
            "uses_default": False,
        },
        {
            "resource": "memory",
            "requested": 50_000,
            "limit": 49_340,
            "uses_default": False,
        },
    ]


def test_configured_overages_charge_undeclared_memory_at_daemon_default() -> None:
    snapshot = capacity.DaemonCapacity(
        max_cpus=8,
        max_mem_mb=4_000,
        default_job_mem_mb=6_000,
        written_at="2026-08-17T12:00:00+00:00",
    )

    overages = capacity.configured_capacity_overages(
        cpus=4,
        mem_mb=None,
        snapshot=snapshot,
    )

    assert len(overages) == 1
    assert overages[0].resource == "memory"
    assert overages[0].requested == 6_000
    assert overages[0].uses_default is True


def test_multi_user_submit_warning_reads_multi_user_capacity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    modes: list[bool] = []

    def read_capacity(*, multi_user: bool, **_kwargs: object):
        modes.append(multi_user)
        return capacity.DaemonCapacity(
            max_cpus=4,
            max_mem_mb=None,
            written_at="2026-08-17T12:00:00+00:00",
        )

    monkeypatch.setattr(capacity, "read_daemon_capacity", read_capacity)

    warnings = submit._impossible_capacity_warnings(
        cpus=8,
        mem_mb=None,
        jobid="abc123def456",
        multi_user=True,
        scheduler_target=None,
    )

    assert modes == [True]
    assert warnings == (
        "requested 8 CPUs but this daemon caps at --max-cpus 4; job "
        "abc123def456 will park PENDING until the daemon is restarted with a "
        "higher cap",
    )


def test_read_absent_returns_none(state_dir):
    # No file (old daemon / never started) → None, never raises.
    assert capacity.read_daemon_capacity() is None


def test_read_corrupt_returns_none(state_dir):
    capacity.capacity_path().write_text("{not valid json")
    assert capacity.read_daemon_capacity() is None


def test_read_fifo_returns_none_without_blocking(state_dir):
    fifo = capacity.capacity_path(multi_user=False)
    os.mkfifo(fifo)
    results: list[capacity.DaemonCapacity | None] = []
    reader = threading.Thread(
        target=lambda: results.append(
            capacity.read_daemon_capacity(via_rpc=False, multi_user=False)
        ),
        daemon=True,
    )

    reader.start()
    # In-process, so interpreter start-up is not on the clock. A blocking
    # open of a writerless FIFO never returns, so this budget only has to
    # tell "returned" from "stuck"; it is not a latency bound.
    reader.join(timeout=5)
    try:
        assert not reader.is_alive(), "read_daemon_capacity blocked on a FIFO"
        assert results == [None]
    finally:
        if reader.is_alive():
            # Give the parked reader a writer so it returns instead of
            # leaking into the rest of the session.
            with contextlib.suppress(OSError):
                writer = os.open(fifo, os.O_WRONLY | os.O_NONBLOCK)
                reader.join(timeout=1)
                os.close(writer)


def test_oversize_capacity_is_rejected_before_json_parse(
    state_dir,
    monkeypatch: pytest.MonkeyPatch,
):
    capacity.capacity_path().write_bytes(b" " * 65_537)
    parsed_lengths: list[int] = []
    monkeypatch.setattr(
        capacity.DaemonCapacity,
        "model_validate_json",
        classmethod(
            lambda _cls, raw: parsed_lengths.append(len(raw))
        ),
    )

    assert capacity.read_daemon_capacity(via_rpc=False) is None
    assert parsed_lengths == []


def test_atomic_write_leaves_no_tmp(state_dir):
    capacity.write_daemon_capacity(max_cpus=4, max_jobs=None, max_mem_mb=None)
    p = capacity.capacity_path()
    assert not (p.parent / (p.name + ".tmp")).exists()


def test_multi_user_capacity_uses_system_root(
    tmp_path, monkeypatch: pytest.MonkeyPatch
):
    """The root daemon and an operator CLI must share one capacity file.

    The system service exports ``VQ_CONFIG_DIR=/etc/vq`` but deliberately does
    not export ``VQ_STATE_DIR``.  A multi-user daemon must therefore select the
    system root from its mode, not root's per-user XDG state directory.
    """
    operator_state = tmp_path / "operator-state"
    system_state = tmp_path / "system-state"
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(operator_state))
    monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(system_state))

    capacity.write_daemon_capacity(
        max_cpus=12,
        max_jobs=None,
        max_mem_mb=48_000,
        default_job_mem_mb=4_000,
        multi_user=True,
    )

    assert capacity.capacity_path(multi_user=True) == (
        system_state / capacity.CAPACITY_FILENAME
    )
    assert not (operator_state / capacity.CAPACITY_FILENAME).exists()
    advertised = capacity.read_daemon_capacity(
        via_rpc=False,
        multi_user=True,
    )
    assert advertised is not None
    assert advertised.max_cpus == 12
    assert advertised.max_mem_mb == 48_000


def test_multi_user_status_and_summary_expose_impossible_request(
    tmp_path, monkeypatch: pytest.MonkeyPatch
):
    """host_a incident: a 56 GiB request cannot look blocker-free at 48 GiB."""
    operator_state = tmp_path / "operator-state"
    system_state = tmp_path / "system-state"
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(operator_state))
    monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(system_state))
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "config.toml").write_text(
        "[multi_user]\nenabled = true\n"
    )
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(config_dir))
    monkeypatch.setattr(
        config, "SYSTEM_CONFIG_PATH", tmp_path / "absent-system.toml"
    )

    jobid = "resourceceil01"
    queue = paths.user_queue_dir("1000")
    workspace = paths.user_jobs_dir("1000") / jobid
    queue.mkdir(parents=True)
    workspace.mkdir(parents=True)
    (workspace / "stdout.log").write_text("")
    (workspace / "stderr.log").write_text("")
    JobSpec(
        id=jobid,
        command=["true"],
        cwd=str(workspace),
        cpus=12,
        mem_mb=56_000,
        state=JobState.PENDING,
    ).write(queue / f"{jobid}.json")
    capacity.write_daemon_capacity(
        max_cpus=12,
        max_jobs=None,
        max_mem_mb=48_000,
        default_job_mem_mb=4_000,
        multi_user=True,
    )

    status = json.loads(
        show_status_json("localhost", jobid, multi_user=True)
    )
    assert status["pending_over_capacity"] is True
    assert status["configured_capacity_overages"] == [
        {
            "resource": "memory",
            "requested": 56_000,
            "limit": 48_000,
            "uses_default": False,
        }
    ]
    assert status["pending_blockers"] == [
        "memory request exceeds configured cap: requested 56000 MB, cap is "
        "48000 MB; cannot dispatch until the daemon cap changes or the job "
        "is resubmitted"
    ]
    assert status["pending_admission_reason"] == status["pending_blockers"][0]

    summary = overview.gather_overview_local(
        "localhost",
        config.Config(hosts={}),
        multi_user=True,
    )
    assert summary.max_cpus == 12
    assert summary.max_mem_mb == 48_000
    assert summary.queue_counts["pending"] == 1
    assert summary.over_capacity_pending_jobs == 1
    assert "⚠ over-cap pending: 1 job(s) cannot dispatch" in (
        overview.format_overview_text(summary)
    )
    assert overview.format_overview_json(summary)[
        "over_capacity_pending_jobs"
    ] == 1


def test_gather_overview_local_reads_capacity(state_dir):
    capacity.write_daemon_capacity(
        max_cpus=12, max_jobs=4, max_mem_mb=64000, max_scheduler_jobs=7
    )
    ov = overview.gather_overview_local("localhost", config.Config(hosts={}))
    assert ov.max_cpus == 12
    assert ov.max_jobs == 4
    assert ov.max_mem_mb == 64000
    assert ov.max_scheduler_jobs == 7
    assert ov.over_capacity_pending_jobs == 0


def test_gather_overview_local_capacity_absent_is_none(state_dir):
    ov = overview.gather_overview_local("localhost", config.Config(hosts={}))
    assert ov.max_cpus is None
    assert ov.max_jobs is None
    assert ov.max_mem_mb is None
    assert ov.max_scheduler_jobs is None
    assert ov.over_capacity_pending_jobs is None
    assert overview.format_overview_json(ov)["over_capacity_pending_jobs"] is None


def test_gather_overview_mixed_version_default_memory_is_unknown(
    state_dir, monkeypatch: pytest.MonkeyPatch
) -> None:
    queue = paths.queue_dir()
    queue.mkdir(parents=True)
    JobSpec(
        id="legacy-cap",
        command=["true"],
        cwd=str(state_dir / "legacy-cap"),
        cpus=1,
        mem_mb=None,
        state=JobState.PENDING,
    ).write(queue / "legacy-cap.json")
    mixed_version = capacity.DaemonCapacity.model_validate(
        {
            "max_cpus": 8,
            "max_mem_mb": 4_000,
            "written_at": "2026-08-20T12:00:00+00:00",
        }
    )
    monkeypatch.setattr(
        overview.capacity,
        "read_daemon_capacity",
        lambda **_kwargs: mixed_version,
    )

    ov = overview.gather_overview_local("localhost", config.Config(hosts={}))

    assert ov.max_mem_mb == 4_000
    assert ov.over_capacity_pending_jobs is None


def test_overview_json_round_trips_capacity():
    ov = overview.HostOverview(host="host_a")
    ov.max_cpus, ov.max_jobs, ov.max_mem_mb = 8, 2, 16000
    ov.max_scheduler_jobs = 5
    ov.over_capacity_pending_jobs = 7
    payload = overview.format_overview_json(ov)
    assert payload["max_cpus"] == 8
    assert payload["max_jobs"] == 2
    assert payload["max_mem_mb"] == 16000
    assert payload["max_scheduler_jobs"] == 5
    assert payload["over_capacity_pending_jobs"] == 7
    back = overview._overview_from_json("host_a", payload)
    assert (
        back.max_cpus,
        back.max_jobs,
        back.max_mem_mb,
        back.max_scheduler_jobs,
    ) == (8, 2, 16000, 5)
    assert back.over_capacity_pending_jobs == 7


def test_overview_json_pre_v0_11_0_host_reads_none():
    # A pre-v0.11.0 remote omits the keys → parsed as None (unknown
    # capacity), while existing fields still parse.
    payload = {"host": "old", "running_cpus": 4, "pending_cpus": 0}
    back = overview._overview_from_json("old", payload)
    assert back.max_cpus is None
    assert back.max_jobs is None
    assert back.max_mem_mb is None
    assert back.max_scheduler_jobs is None
    assert back.over_capacity_pending_jobs is None
    assert back.running_cpus == 4


# --- capacity-aware recommend_host (step 2a) ------------------------------

def _ov(host, *, running=0, pending=0, max_cpus=None, idle=0,
        reachable=True, admin_down=False):
    o = overview.HostOverview(host=host)
    o.reachable = reachable
    o.admin_down = admin_down
    o.running_cpus = running
    o.pending_cpus = pending
    o.max_cpus = max_cpus
    o.idle_seconds = idle
    return o


def test_recommend_prefers_most_free_capacity():
    # Both idle; capacity-aware picks the host with more free headroom.
    hosts = [_ov("small", max_cpus=4), _ov("big", max_cpus=16)]
    assert overview.recommend_host(hosts, job_cpus=2) == "big"


def test_recommend_fits_first():
    # 'full' is bigger but saturated (free 0); 'roomy' has room (free 3).
    full = _ov("full", max_cpus=16, running=16)
    roomy = _ov("roomy", max_cpus=4, running=1)
    assert overview.recommend_host([full, roomy], job_cpus=2) == "roomy"


def test_recommend_skips_admin_down():
    down = _ov("down", max_cpus=99, admin_down=True)
    up = _ov("up", max_cpus=4)
    assert overview.recommend_host([down, up], job_cpus=1) == "up"


def test_recommend_unknown_capacity_falls_back_to_workload():
    # max_cpus None on both → least-committed wins (pre-v0.11.0 proxy).
    assert overview.recommend_host(
        [_ov("busy", running=4), _ov("quiet", running=1)]
    ) == "quiet"


def test_recommend_known_capacity_beats_unknown_busy():
    known = _ov("known", max_cpus=8, running=2)   # free 6
    unknown_idle = _ov("unknown", running=0)       # proxy free 0
    assert overview.recommend_host(
        [known, unknown_idle], job_cpus=4
    ) == "known"


def test_recommend_none_when_no_candidates():
    assert overview.recommend_host([_ov("x", reachable=False)]) is None


# --- vq submit auto picker (step 2b) --------------------------------------

def test_pick_auto_host_returns_best_by_capacity(monkeypatch):
    from vq import cli
    from vq import overview as ovmod
    cfg = config.Config(hosts={
        "host_a": config.HostConfig(ssh="host_a"),
        "host_e": config.HostConfig(ssh="host_e"),
    })
    fakes = {"host_a": _ov("host_a", max_cpus=16), "host_e": _ov("host_e", max_cpus=4)}
    monkeypatch.setattr(cli, "is_local_host", lambda h: False)
    monkeypatch.setattr(ovmod, "gather_overview_remote", lambda h, hc, **kw: fakes[h])
    monkeypatch.setattr("vq.host_status.load_down", lambda: {})
    assert cli._pick_auto_host(cfg, job_cpus=2) == "host_a"


def test_pick_auto_host_raises_when_none_qualify(monkeypatch):
    import click

    from vq import cli
    from vq import overview as ovmod
    cfg = config.Config(hosts={"host_a": config.HostConfig(ssh="host_a")})
    monkeypatch.setattr(cli, "is_local_host", lambda h: False)
    monkeypatch.setattr(
        ovmod, "gather_overview_remote", lambda h, hc, **kw: _ov(h, reachable=False)
    )
    monkeypatch.setattr("vq.host_status.load_down", lambda: {})
    with pytest.raises(click.ClickException):
        cli._pick_auto_host(cfg, job_cpus=1)
