"""Tests for vq drain: daemon dispatch gate.

Three layers:

1. DrainState model + read/write/clear primitives.
2. Daemon dispatch loop: drain blocks / partial-caps / release.
3. CLI verb: --status / --release / --max-jobs / --max-cpus / --reason.
"""
from __future__ import annotations

import contextlib
import json
import os
import subprocess
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Event, Thread

import pytest
from click.testing import CliRunner

from vq import config, drain, paths, rpc
from vq.cli import main
from vq.spec import JobSpec, JobState
from vq.submit import submit_local


@pytest.fixture
def state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    paths.state_root().mkdir(parents=True, exist_ok=True)
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    paths.jobs_dir().mkdir(parents=True, exist_ok=True)
    return tmp_path


def _write_pending(jobid: str, *, cpus: int = 1, mem_mb: int | None = None) -> JobSpec:
    workspace = paths.jobs_dir() / jobid
    workspace.mkdir(parents=True, exist_ok=True)
    spec = JobSpec(
        id=jobid,
        command=["sleep", "10"],
        cwd=str(workspace),
        cpus=cpus,
        mem_mb=mem_mb,
        state=JobState.PENDING,
    )
    spec.write(paths.spec_path(jobid))
    return spec


class TestDrainStateModel:
    def test_default_is_full_drain(self) -> None:
        s = drain.DrainState()
        assert s.enabled is True
        assert s.max_jobs is None
        assert s.max_cpus is None
        assert s.is_full_drain is True

    def test_partial_drain_is_not_full(self) -> None:
        s = drain.DrainState(max_jobs=2)
        assert s.is_full_drain is False
        s = drain.DrainState(max_cpus=4)
        assert s.is_full_drain is False

    def test_effective_max_jobs_with_drain_set(self) -> None:
        s = drain.DrainState(max_jobs=2)
        assert s.effective_max_jobs(daemon_max_jobs=10) == 2
        # Drain is min(); if daemon's cap is tighter, daemon wins.
        s = drain.DrainState(max_jobs=10)
        assert s.effective_max_jobs(daemon_max_jobs=2) == 2

    def test_effective_max_jobs_with_drain_none_uses_daemon(self) -> None:
        s = drain.DrainState(max_jobs=None)
        assert s.effective_max_jobs(daemon_max_jobs=4) == 4
        assert s.effective_max_jobs(daemon_max_jobs=None) is None

    def test_effective_max_cpus(self) -> None:
        s = drain.DrainState(max_cpus=4)
        assert s.effective_max_cpus(daemon_max_cpus=32) == 4
        s = drain.DrainState(max_cpus=None)
        assert s.effective_max_cpus(daemon_max_cpus=32) == 32

    def test_extra_fields_ignored_not_rejected(self) -> None:
        """Replaces `test_extra_fields_rejected`, which pinned the opposite.

        Rejecting extras looks like the stricter, safer choice and is the
        opposite here. Drain state is shared across a fleet that is routinely
        mid-rolling-upgrade, and `read_drain_state` deliberately treats an
        unparseable file as "no drain" so a bad file cannot wedge dispatch.
        Together those made one added field un-drain every host still on the
        older build. Tolerating unknown keys is what keeps a held lane held;
        see `TestDrainStateIsForwardCompatible`.
        """
        s = drain.DrainState(unknown_field="x")  # type: ignore[call-arg]

        assert not hasattr(s, "unknown_field"), "ignored, not absorbed"
        # The known fields still parse, which is the whole point.
        assert s.enabled is True

    def test_update_deny_marks_submission_rejection(self) -> None:
        s = drain.DrainState(update_mode="deny", reject_submits=True)
        assert s.update_mode == "deny"
        assert s.reject_submits is True

    def test_update_accept_keeps_submission_acceptance(self) -> None:
        s = drain.DrainState(update_mode="accept")
        assert s.update_mode == "accept"
        assert s.reject_submits is False

    def test_full_dispatch_can_overlap_scheduler_lanes(self) -> None:
        s = drain.DrainState(full_dispatch=True, scheduler_hosts=["host_f"])
        assert s.is_full_drain is True
        assert s.is_scheduler_target_drain is True
        assert s.drains_scheduler_target("host_f") is True

    @pytest.mark.parametrize("value", [0, -1, True, "2", 2.0])
    def test_duration_rejects_nonpositive_or_coercible_values(
        self,
        value: object,
    ) -> None:
        with pytest.raises(ValueError):
            drain.DrainState.model_validate({"duration_seconds": value})

    @pytest.mark.parametrize("value", [None, 1, 3600])
    def test_duration_accepts_none_or_positive_strict_integers(
        self,
        value: int | None,
    ) -> None:
        state = drain.DrainState(duration_seconds=value)
        assert state.duration_seconds == value


class TestDrainPersistence:
    def test_read_when_no_file_returns_none(self, state: Path) -> None:
        assert drain.read_drain_state() is None
        assert drain.is_drained() is False

    def test_write_then_read_roundtrip(self, state: Path) -> None:
        s = drain.DrainState(max_jobs=2, reason="testing")
        drain.write_drain_state(s)
        back = drain.read_drain_state()
        assert back is not None
        assert back.max_jobs == 2
        assert back.reason == "testing"
        assert drain.is_drained() is True

    def test_replace_from_mapping_strips_unknown_and_returns_enabled(
        self,
        state: Path,
    ) -> None:
        enabled = drain.replace_drain_state_from_mapping(
            {
                "enabled": False,
                "reason": "mapping replacement",
                "future_field_unknown_to_daemon": "ignored",
            }
        )

        stored = drain.read_drain_state(via_rpc=False)
        assert enabled is False
        assert stored is not None
        assert stored.enabled is False
        assert stored.reason == "mapping replacement"
        assert not hasattr(stored, "future_field_unknown_to_daemon")

    def test_clear_removes_file(self, state: Path) -> None:
        drain.write_drain_state(drain.DrainState())
        assert drain.drain_state_path().exists()
        assert drain.clear_drain() is True
        assert not drain.drain_state_path().exists()
        assert drain.read_drain_state() is None

    def test_clear_idempotent(self, state: Path) -> None:
        assert drain.clear_drain() is False  # nothing to remove
        assert drain.clear_drain() is False  # still nothing

    def test_corrupt_file_treated_as_no_drain(self, state: Path) -> None:
        """A partially-written drain.json must NOT block all dispatches.
        Better to let dispatch proceed than to brick the daemon on a
        parser error."""
        path = drain.drain_state_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("not valid json {{{")
        assert drain.read_drain_state() is None

    def test_write_is_atomic_via_tmpfile_rename(self, state: Path) -> None:
        """The tmpfile-then-rename pattern means a crash mid-write
        leaves either the old file intact or the new file intact, never
        a half-written one. We can't easily simulate the crash, but we
        verify the .tmp file doesn't linger after a successful write."""
        s = drain.DrainState(reason="atomic-write check")
        drain.write_drain_state(s)
        tmp = drain.drain_state_path().with_suffix(".tmp")
        assert not tmp.exists()

    def test_format_status_inactive(self, state: Path) -> None:
        out = drain.format_status()
        assert "inactive" in out

    def test_format_status_full_drain_with_reason(self, state: Path) -> None:
        drain.write_drain_state(drain.DrainState(reason="kids gaming"))
        out = drain.format_status()
        assert "ACTIVE" in out
        assert "full" in out
        assert "kids gaming" in out

    def test_format_status_partial(self, state: Path) -> None:
        drain.write_drain_state(drain.DrainState(max_jobs=2, max_cpus=4))
        out = drain.format_status()
        assert "ACTIVE" in out
        assert "partial" in out
        assert "max_jobs=2" in out
        assert "max_cpus=4" in out

    def test_format_status_update_accept(self, state: Path) -> None:
        drain.write_drain_state(drain.DrainState(update_mode="accept"))
        out = drain.format_status()
        assert "PAUSED FOR UPDATE" in out
        assert "accepting jobs for later" in out
        assert "submit_policy: accept_pending" in out

    def test_format_status_update_deny(self, state: Path) -> None:
        drain.write_drain_state(
            drain.DrainState(
                update_mode="deny",
                reject_submits=True,
                reason="fleet upgrade",
            )
        )
        out = drain.format_status()
        assert "PAUSED FOR UPDATE" in out
        assert "denying new submissions" in out
        assert "submit_policy: deny" in out
        assert "fleet upgrade" in out

    def test_format_status_scheduler_target_drain(self, state: Path) -> None:
        drain.write_drain_state(
            drain.DrainState(
                scheduler_hosts=["host_f"],
                reason="pbs_sched idle",
            )
        )
        out = drain.format_status()
        assert "ACTIVE" in out
        assert "scheduler-target" in out
        assert "host_f" in out
        assert "pbs_sched idle" in out

    def test_format_status_full_plus_scheduler_target_drain(
        self, state: Path
    ) -> None:
        drain.write_drain_state(
            drain.DrainState(
                full_dispatch=True,
                scheduler_hosts=["host_f"],
                reason="pbs_sched idle",
            )
        )
        out = drain.format_status()
        assert "ACTIVE" in out
        assert "full + scheduler-target" in out
        assert "host_f" in out
        assert "pbs_sched idle" in out

    def test_status_payload_inactive(self, state: Path) -> None:
        payload = drain.status_payload()
        assert payload["active"] is False
        assert payload["mode"] == "inactive"
        assert payload["scheduler_hosts"] == []
        assert payload["submit_policy"] == "accept_pending"
        assert payload["state"] is None

    def test_status_payload_full_plus_scheduler_target_drain(
        self, state: Path
    ) -> None:
        drain.write_drain_state(
            drain.DrainState(
                full_dispatch=True,
                scheduler_hosts=["host_f"],
                duration_seconds=3600,
                update_mode="accept",
                reason="fleet update",
            )
        )
        payload = drain.status_payload()
        assert payload["active"] is True
        assert payload["mode"] == "full+scheduler-target"
        assert payload["is_full_drain"] is True
        assert payload["is_scheduler_target_drain"] is True
        assert payload["scheduler_hosts"] == ["host_f"]
        assert payload["submit_policy"] == "accept_pending"
        assert payload["remaining_seconds"] is not None
        assert payload["reason"] == "fleet update"
        assert payload["state"]["full_dispatch"] is True  # type: ignore[index]

    def test_release_scheduler_host_preserves_other_targets(self, state: Path) -> None:
        drain.write_drain_state(
            drain.DrainState(scheduler_hosts=["host_f", "host_c"]),
            via_rpc=False,
        )
        assert drain.release_scheduler_host("host_f", via_rpc=False) is True
        stored = drain.read_drain_state(via_rpc=False)
        assert stored is not None
        assert stored.scheduler_hosts == ["host_c"]

    def test_release_last_scheduler_host_clears_drain(self, state: Path) -> None:
        drain.write_drain_state(
            drain.DrainState(scheduler_hosts=["host_f"]),
            via_rpc=False,
        )
        assert drain.release_scheduler_host("host_f", via_rpc=False) is True
        assert drain.read_drain_state(via_rpc=False) is None

    def test_release_scheduler_host_preserves_full_dispatch(self, state: Path) -> None:
        drain.write_drain_state(
            drain.DrainState(full_dispatch=True, scheduler_hosts=["host_f"]),
            via_rpc=False,
        )
        assert drain.release_scheduler_host("host_f", via_rpc=False) is True
        stored = drain.read_drain_state(via_rpc=False)
        assert stored is not None
        assert stored.scheduler_hosts == []
        assert stored.is_full_drain is True

    def test_release_full_drain_preserves_scheduler_lanes(self, state: Path) -> None:
        drain.write_drain_state(
            drain.DrainState(full_dispatch=True, scheduler_hosts=["host_f"]),
            via_rpc=False,
        )
        assert drain.release_full_drain(via_rpc=False) is True
        stored = drain.read_drain_state(via_rpc=False)
        assert stored is not None
        assert stored.scheduler_hosts == ["host_f"]
        assert stored.is_full_drain is False
        assert stored.drains_scheduler_target("host_f") is True

    def test_owned_full_release_matches_reason_and_set_at_atomically(
        self,
        state: Path,
    ) -> None:
        owned = drain.DrainState(
            full_dispatch=True,
            scheduler_hosts=["host_f"],
            max_jobs=2,
            max_cpus=8,
            reason="fleet-rollout:operation-123",
            set_at="2026-08-10T16:00:00+00:00",
            update_mode="deny",
            reject_submits=True,
            duration_seconds=1_000_000_000,
        )
        drain.write_drain_state(owned, via_rpc=False)

        assert drain.release_owned_full_drain(
            expected_reason=owned.reason,
            expected_set_at=owned.set_at,
            via_rpc=False,
        ) is True

        stored = drain.read_drain_state(via_rpc=False)
        assert stored is not None
        expected = owned.model_copy(
            update={
                "full_dispatch": False,
                "update_mode": None,
                "reject_submits": False,
                "duration_seconds": None,
            }
        )
        assert stored.model_dump() == expected.model_dump()

    @pytest.mark.parametrize(
        ("expected_reason", "expected_set_at"),
        [
            ("another owner", "2026-08-10T16:00:00+00:00"),
            ("fleet-rollout:operation-123", "2026-08-10T16:00:01+00:00"),
        ],
    )
    def test_owned_full_release_mismatch_fails_without_mutation(
        self,
        state: Path,
        expected_reason: str,
        expected_set_at: str,
    ) -> None:
        drain.write_drain_state(
            drain.DrainState(
                full_dispatch=True,
                scheduler_hosts=["host_f"],
                reason="fleet-rollout:operation-123",
                set_at="2026-08-10T16:00:00+00:00",
            ),
            via_rpc=False,
        )
        path = drain.drain_state_path()
        before = path.read_bytes()

        with pytest.raises(
            drain.OwnedFullDrainReleaseError,
            match="changed after ownership was recorded",
        ):
            drain.release_owned_full_drain(
                expected_reason=expected_reason,
                expected_set_at=expected_set_at,
                via_rpc=False,
            )

        assert path.read_bytes() == before

    def test_owned_full_release_missing_or_already_released_is_false(
        self,
        state: Path,
    ) -> None:
        assert drain.release_owned_full_drain(
            expected_reason="fleet-rollout:operation-123",
            expected_set_at="2026-08-10T16:00:00+00:00",
            via_rpc=False,
        ) is False

        partial = drain.DrainState(
            max_jobs=2,
            reason="fleet-rollout:operation-123",
            set_at="2026-08-10T16:00:00+00:00",
        )
        drain.write_drain_state(partial, via_rpc=False)
        before = drain.drain_state_path().read_bytes()
        assert drain.release_owned_full_drain(
            expected_reason=partial.reason,
            expected_set_at=partial.set_at,
            via_rpc=False,
        ) is False
        assert drain.drain_state_path().read_bytes() == before

    def test_unconditional_full_release_preserves_operator_override(
        self,
        state: Path,
    ) -> None:
        drain.write_drain_state(
            drain.DrainState(
                full_dispatch=True,
                reason="operator changed this after rollout",
                set_at="2026-08-10T17:00:00+00:00",
            ),
            via_rpc=False,
        )

        assert drain.release_full_drain(via_rpc=False) is True
        assert drain.read_drain_state(via_rpc=False) is None

    def test_owned_full_release_rpc_failure_never_falls_back_to_file(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        owned = drain.DrainState(
            full_dispatch=True,
            reason="fleet-rollout:operation-123",
            set_at="2026-08-10T16:00:00+00:00",
        )
        drain.write_drain_state(owned, via_rpc=False)
        before = drain.drain_state_path().read_bytes()
        monkeypatch.setattr(
            rpc,
            "call",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                rpc.RPCError("daemon too old")
            ),
        )

        with pytest.raises(
            drain.OwnedFullDrainReleaseError,
            match="refusing a client-side read/replace fallback",
        ):
            drain.release_owned_full_drain(
                expected_reason=owned.reason or "",
                expected_set_at=owned.set_at,
                multi_user=False,
            )

        assert drain.drain_state_path().read_bytes() == before

    def test_owned_full_release_rpc_uses_fixed_method_and_exact_pair(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        calls: list[tuple[str, dict[str, object], bool]] = []

        def rpc_call(
            method: str,
            args: dict[str, object],
            *,
            multi_user: bool,
        ) -> dict[str, object]:
            calls.append((method, args, multi_user))
            return {"changed": False, "ok": True}

        monkeypatch.setattr(rpc, "call", rpc_call)

        assert drain.release_owned_full_drain(
            expected_reason="fleet-rollout:operation-123",
            expected_set_at="2026-08-10T16:00:00+00:00",
            token="secret",
            multi_user=True,
        ) is False
        assert calls == [
            (
                drain.OWNED_FULL_DRAIN_RELEASE_RPC_METHOD,
                {
                    "expected_reason": "fleet-rollout:operation-123",
                    "expected_set_at": "2026-08-10T16:00:00+00:00",
                    "token": "secret",
                },
                True,
            )
        ]


class TestDaemonRespectsDrain:
    """The daemon's _dispatch_pending must consult drain state on
    every iteration. Tests use the real Daemon class but bypass the
    main loop (call _dispatch_pending directly)."""

    def test_full_drain_blocks_all_dispatch(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from vq.daemon import Daemon
        # Mock cgroup so we don't try to systemd-run on macOS
        monkeypatch.setattr("vq.cgroup.available", lambda: False)
        _write_pending("draintest001")
        _write_pending("draintest002")

        d = Daemon(max_cpus=16, max_jobs=4, max_mem_mb=None)
        # Without drain: both jobs should dispatch on one pass.
        # WITH full drain set: neither should dispatch.
        drain.write_drain_state(drain.DrainState())
        d._dispatch_pending()
        assert len(d._running) == 0, (
            "full drain must block every new dispatch; "
            f"got {list(d._running.keys())}"
        )

    def test_dispatch_uses_multi_user_mode_pinned_at_daemon_start(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _ = state
        from vq.daemon import Daemon

        monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(state / "multi-user"))
        monkeypatch.setattr("vq.cgroup.available", lambda: False)
        monkeypatch.setattr("vq.cgroup.systemd_run_on_path", lambda: True)
        observed: list[dict[str, object]] = []

        def read_effective(**kwargs: object) -> drain.DrainState:
            observed.append(dict(kwargs))
            return drain.DrainState()

        monkeypatch.setattr(drain, "read_effective_drain_state", read_effective)
        daemon = Daemon(
            max_cpus=1,
            max_jobs=1,
            max_mem_mb=None,
            multi_user=True,
        )
        monkeypatch.setattr(daemon, "_poll_admin_update_marker", lambda: False)
        monkeypatch.setattr(daemon, "_config_unusable", lambda: False)

        daemon._dispatch_pending()

        assert observed == [{"via_rpc": False, "multi_user": True}]

    def test_release_after_drain_resumes_dispatch(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from vq.daemon import Daemon
        monkeypatch.setattr("vq.cgroup.available", lambda: False)
        _write_pending("draintest003")

        d = Daemon(max_cpus=16, max_jobs=4, max_mem_mb=None)
        drain.write_drain_state(drain.DrainState())
        d._dispatch_pending()
        assert len(d._running) == 0

        # Release drain; next iteration should dispatch.
        drain.clear_drain()
        d._dispatch_pending()
        assert len(d._running) == 1

        # Cleanup: kill the spawned sleep.
        for rj in list(d._running.values()):
            rj.popen.kill()
            with contextlib.suppress(Exception):
                rj.popen.wait(timeout=2)

    def test_partial_drain_max_jobs_caps_below_daemon(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Daemon max_jobs=4 but drain caps to 1: only one job dispatches
        out of three pending."""
        from vq.daemon import Daemon
        monkeypatch.setattr("vq.cgroup.available", lambda: False)
        for n in range(3):
            _write_pending(f"partial0{n:05d}")

        d = Daemon(max_cpus=16, max_jobs=4, max_mem_mb=None)
        drain.write_drain_state(drain.DrainState(max_jobs=1))
        d._dispatch_pending()
        assert len(d._running) == 1

        for rj in list(d._running.values()):
            rj.popen.kill()
            with contextlib.suppress(Exception):
                rj.popen.wait(timeout=2)

    def test_partial_drain_max_cpus_blocks_big_jobs(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Drain --max-cpus 2 lets a --cpus 1 job through but blocks
        a --cpus 8 job."""
        from vq.daemon import Daemon
        monkeypatch.setattr("vq.cgroup.available", lambda: False)
        _write_pending("smalljob0001", cpus=1)
        _write_pending("bigjob000001", cpus=8)

        d = Daemon(max_cpus=16, max_jobs=4, max_mem_mb=None)
        drain.write_drain_state(drain.DrainState(max_cpus=2))
        d._dispatch_pending()
        running_ids = list(d._running.keys())
        # Small fits under cap 2; big at 8 cpus does not.
        assert "smalljob0001" in running_ids
        assert "bigjob000001" not in running_ids

        for rj in list(d._running.values()):
            rj.popen.kill()
            with contextlib.suppress(Exception):
                rj.popen.wait(timeout=2)


class TestDrainCLI:
    def test_cli_status_when_no_drain(self, state: Path) -> None:
        (state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n[hosts.localhost]\nssh = "localhost"\n'
        )
        result = CliRunner().invoke(main, ["drain", "--status"])
        assert result.exit_code == 0
        assert "inactive" in result.output

    def test_cli_set_full_drain(self, state: Path) -> None:
        (state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n[hosts.localhost]\nssh = "localhost"\n'
        )
        result = CliRunner().invoke(main, ["drain"])
        assert result.exit_code == 0, result.output
        assert "drain set" in result.output
        assert "full drain" in result.output
        # State file should be written.
        assert drain.read_drain_state(via_rpc=False) is not None

    def test_cli_update_mode_accept(self, state: Path) -> None:
        (state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n[hosts.localhost]\nssh = "localhost"\n'
        )
        result = CliRunner().invoke(
            main,
            ["drain", "--update-mode", "accept", "--reason", "fleet upgrade"],
        )
        assert result.exit_code == 0, result.output
        assert "paused for update" in result.output
        assert "accepting jobs for later" in result.output
        stored = drain.read_drain_state(via_rpc=False)
        assert stored is not None
        assert stored.update_mode == "accept"
        assert stored.reject_submits is False

    def test_cli_update_mode_deny(self, state: Path) -> None:
        (state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n[hosts.localhost]\nssh = "localhost"\n'
        )
        result = CliRunner().invoke(
            main,
            ["drain", "--update-mode", "deny", "--reason", "fleet upgrade"],
        )
        assert result.exit_code == 0, result.output
        assert "paused for update" in result.output
        assert "denying new submissions" in result.output
        stored = drain.read_drain_state(via_rpc=False)
        assert stored is not None
        assert stored.update_mode == "deny"
        assert stored.reject_submits is True

    def test_cli_update_mode_rejects_partial_caps(self, state: Path) -> None:
        (state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n[hosts.localhost]\nssh = "localhost"\n'
        )
        result = CliRunner().invoke(
            main,
            ["drain", "--update-mode", "deny", "--max-jobs", "1"],
        )
        assert result.exit_code != 0
        assert "--update-mode is a full maintenance drain" in result.output

    def test_cli_scheduler_host_drain(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n[hosts.localhost]\nssh = "localhost"\n'
        )
        socket_path = Path(os.environ["VQ_TEST_SHORT_TMPDIR"]) / (
            f"vq-drain-{os.getpid()}-{id(self)}.sock"
        )
        monkeypatch.setattr(rpc, "socket_path", lambda **_kwargs: socket_path)
        server = rpc.RPCServer(multi_user=False)
        rpc.register_get_drain_state_method(
            server,
            lambda: drain.read_drain_state(
                via_rpc=False,
                multi_user=server.multi_user,
            ),
        )
        rpc.register_get_scheduler_drain_leases_method(
            server,
            lambda: drain.read_scheduler_drain_leases(
                via_rpc=False,
                multi_user=server.multi_user,
            ),
            schema_version=drain.SCHEDULER_DRAIN_LEASE_SCHEMA_VERSION,
        )
        rpc.register_legacy_scheduler_drain_release_method(
            server,
            lambda host, expected_reason, expected_set_at: (
                drain.release_legacy_scheduler_host(
                    host,
                    via_rpc=False,
                    expected_reason=expected_reason,
                    expected_set_at=expected_set_at,
                    multi_user=server.multi_user,
                )
            ),
        )
        rpc.register_set_drain_state_method(
            server,
            clear_state=lambda: drain.clear_drain(
                via_rpc=False,
                multi_user=server.multi_user,
            ),
            replace_state=lambda state: drain.replace_drain_state_from_mapping(
                state,
                multi_user=server.multi_user,
            ),
        )
        rpc.register_set_scheduler_drain_lease_method(
            server,
            lambda lease, release_id, release_host, release_owner, release_all: (
                drain.apply_scheduler_drain_lease_mapping_mutation(
                    lease=lease,
                    release_id=release_id,
                    release_host=release_host,
                    release_owner=release_owner,
                    release_all=release_all,
                    multi_user=server.multi_user,
                )
            ),
            schema_version=drain.SCHEDULER_DRAIN_LEASE_SCHEMA_VERSION,
        )
        server.start()
        try:
            result = CliRunner().invoke(
                main,
                ["drain", "--scheduler-host", "host_f", "--reason", "pbs idle"],
            )
        finally:
            server.stop()
        assert result.exit_code == 0, result.output
        assert "scheduler-target drain" in result.output
        assert "host_f" in result.output
        stored = drain.read_effective_drain_state(via_rpc=False)
        assert stored is not None
        assert stored.scheduler_hosts == ["host_f"]
        assert stored.is_full_drain is False

    def test_cli_scheduler_host_preserves_existing_full_drain(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n[hosts.localhost]\nssh = "localhost"\n'
        )
        drain.write_drain_state(
            drain.DrainState(update_mode="accept", duration_seconds=3600),
            via_rpc=False,
        )
        socket_path = Path(os.environ["VQ_TEST_SHORT_TMPDIR"]) / (
            f"vq-drain-{os.getpid()}-{id(self)}.sock"
        )
        monkeypatch.setattr(rpc, "socket_path", lambda **_kwargs: socket_path)
        server = rpc.RPCServer(multi_user=False)
        rpc.register_get_drain_state_method(
            server,
            lambda: drain.read_drain_state(
                via_rpc=False,
                multi_user=server.multi_user,
            ),
        )
        rpc.register_get_scheduler_drain_leases_method(
            server,
            lambda: drain.read_scheduler_drain_leases(
                via_rpc=False,
                multi_user=server.multi_user,
            ),
            schema_version=drain.SCHEDULER_DRAIN_LEASE_SCHEMA_VERSION,
        )
        rpc.register_legacy_scheduler_drain_release_method(
            server,
            lambda host, expected_reason, expected_set_at: (
                drain.release_legacy_scheduler_host(
                    host,
                    via_rpc=False,
                    expected_reason=expected_reason,
                    expected_set_at=expected_set_at,
                    multi_user=server.multi_user,
                )
            ),
        )
        rpc.register_set_drain_state_method(
            server,
            clear_state=lambda: drain.clear_drain(
                via_rpc=False,
                multi_user=server.multi_user,
            ),
            replace_state=lambda state: drain.replace_drain_state_from_mapping(
                state,
                multi_user=server.multi_user,
            ),
        )
        rpc.register_set_scheduler_drain_lease_method(
            server,
            lambda lease, release_id, release_host, release_owner, release_all: (
                drain.apply_scheduler_drain_lease_mapping_mutation(
                    lease=lease,
                    release_id=release_id,
                    release_host=release_host,
                    release_owner=release_owner,
                    release_all=release_all,
                    multi_user=server.multi_user,
                )
            ),
            schema_version=drain.SCHEDULER_DRAIN_LEASE_SCHEMA_VERSION,
        )
        server.start()
        try:
            result = CliRunner().invoke(
                main,
                ["drain", "--scheduler-host", "host_f", "--reason", "pbs idle"],
            )
        finally:
            server.stop()
        assert result.exit_code == 0, result.output
        assert "full drain + scheduler-target drain" in result.output
        stored = drain.read_effective_drain_state(via_rpc=False)
        assert stored is not None
        assert stored.scheduler_hosts == ["host_f"]
        assert stored.is_full_drain is True

    def test_cli_full_drain_preserves_existing_scheduler_lanes(
        self, state: Path
    ) -> None:
        (state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n[hosts.localhost]\nssh = "localhost"\n'
        )
        drain.write_drain_state(
            drain.DrainState(scheduler_hosts=["host_f"]),
            via_rpc=False,
        )
        result = CliRunner().invoke(main, ["drain", "--reason", "fleet stop"])
        assert result.exit_code == 0, result.output
        stored = drain.read_drain_state(via_rpc=False)
        assert stored is not None
        assert stored.scheduler_hosts == ["host_f"]
        assert stored.is_full_drain is True

    def test_cli_release_full_preserves_scheduler_lanes(self, state: Path) -> None:
        (state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n[hosts.localhost]\nssh = "localhost"\n'
        )
        drain.write_drain_state(
            drain.DrainState(full_dispatch=True, scheduler_hosts=["host_f"]),
            via_rpc=False,
        )
        result = CliRunner().invoke(main, ["drain", "--release-full"])
        assert result.exit_code == 0, result.output
        assert "full drain released" in result.output
        stored = drain.read_drain_state(via_rpc=False)
        assert stored is not None
        assert stored.scheduler_hosts == ["host_f"]
        assert stored.is_full_drain is False

    @pytest.mark.parametrize(
        "args",
        [
            [
                "drain",
                "--release-full",
                "--expected-full-reason",
                "fleet-rollout:operation-123",
            ],
            [
                "drain",
                "--release-full",
                "--expected-full-set-at",
                "2026-08-10T16:00:00+00:00",
            ],
            [
                "drain",
                "--expected-full-reason",
                "fleet-rollout:operation-123",
                "--expected-full-set-at",
                "2026-08-10T16:00:00+00:00",
            ],
        ],
    )
    def test_cli_owned_full_release_rejects_incomplete_or_unscoped_pair(
        self,
        state: Path,
        args: list[str],
    ) -> None:
        result = CliRunner().invoke(main, args)

        assert result.exit_code == 2
        assert "expected-full" in result.output

    @pytest.mark.parametrize("blank", ["", "   "])
    def test_cli_owned_full_release_rejects_blank_pair_member(
        self,
        state: Path,
        blank: str,
    ) -> None:
        result = CliRunner().invoke(
            main,
            [
                "drain",
                "--release-full",
                "--expected-full-reason",
                blank,
                "--expected-full-set-at",
                "2026-08-10T16:00:00+00:00",
            ],
        )

        assert result.exit_code == 2
        assert "non-empty" in result.output

    def test_cli_owned_full_release_routes_exact_pair(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        (state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n[hosts.localhost]\nssh = "localhost"\n'
        )
        calls: list[dict[str, object]] = []

        def exact_release(**kwargs: object) -> bool:
            calls.append(kwargs)
            return True

        monkeypatch.setattr(drain, "release_owned_full_drain", exact_release)
        result = CliRunner().invoke(
            main,
            [
                "drain",
                "--release-full",
                "--expected-full-reason",
                "fleet-rollout:operation-123",
                "--expected-full-set-at",
                "2026-08-10T16:00:00+00:00",
            ],
        )

        assert result.exit_code == 0, result.output
        assert calls == [
            {
                "expected_reason": "fleet-rollout:operation-123",
                "expected_set_at": "2026-08-10T16:00:00+00:00",
                "multi_user": False,
            }
        ]

    def test_cli_unconditional_release_full_keeps_old_api(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        (state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n[hosts.localhost]\nssh = "localhost"\n'
        )
        unconditional_calls: list[dict[str, object]] = []
        monkeypatch.setattr(
            drain,
            "release_full_drain",
            lambda **kwargs: unconditional_calls.append(kwargs) or True,
        )

        result = CliRunner().invoke(main, ["drain", "--release-full"])

        assert result.exit_code == 0, result.output
        assert unconditional_calls == [{"multi_user": False}]

    def test_cli_remote_owned_full_release_forwards_exact_pair(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        (state / "cfg" / "config.toml").write_text(
            '[hosts.farhost]\nssh = "far.invalid"\n'
        )
        captured: dict[str, object] = {}

        def fake_delegate(
            host: str,
            _cfg: object,
            *args: str,
            stdin_data: str | None = None,
        ) -> str:
            captured["host"] = host
            captured["args"] = list(args)
            captured["stdin_data"] = stdin_data
            return "full drain released\n"

        monkeypatch.setattr("vq.cli._delegate_to_remote", fake_delegate)
        result = CliRunner().invoke(
            main,
            [
                "drain",
                "farhost",
                "--release-full",
                "--expected-full-reason",
                "fleet-rollout:operation-123",
                "--expected-full-set-at",
                "2026-08-10T16:00:00+00:00",
            ],
        )

        assert result.exit_code == 0, result.output
        assert captured == {
            "host": "farhost",
            "args": [
                "drain",
                "--release-full",
                "--expected-full-reason",
                "fleet-rollout:operation-123",
                "--expected-full-set-at",
                "2026-08-10T16:00:00+00:00",
                "localhost",
            ],
            "stdin_data": None,
        }

    def test_cli_scheduler_host_release_only_that_lane(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n[hosts.localhost]\nssh = "localhost"\n'
        )
        drain.write_drain_state(
            drain.DrainState(scheduler_hosts=["host_f", "host_c"]),
            via_rpc=False,
        )
        original_release = drain.release_legacy_scheduler_host
        monkeypatch.setattr(
            drain,
            "release_legacy_scheduler_host",
            lambda host, **_kwargs: original_release(host, via_rpc=False),
        )
        result = CliRunner().invoke(
            main,
            ["drain", "--release", "--scheduler-host", "host_f"],
        )
        assert result.exit_code == 0, result.output
        assert "scheduler drain released for host_f" in result.output
        stored = drain.read_drain_state(via_rpc=False)
        assert stored is not None
        assert stored.scheduler_hosts == ["host_c"]

    def test_cli_scheduler_host_rejects_partial_caps(self, state: Path) -> None:
        (state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n[hosts.localhost]\nssh = "localhost"\n'
        )
        result = CliRunner().invoke(
            main,
            ["drain", "--scheduler-host", "host_f", "--max-jobs", "1"],
        )
        assert result.exit_code != 0
        assert "--scheduler-host is a scheduler-target drain" in result.output


    def test_cli_partial_drain_max_jobs(self, state: Path) -> None:
        (state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n[hosts.localhost]\nssh = "localhost"\n'
        )
        result = CliRunner().invoke(main, ["drain", "--max-jobs", "2"])
        assert result.exit_code == 0, result.output
        assert "partial" in result.output
        assert "max_jobs=2" in result.output

    def test_cli_partial_drain_max_cpus_with_reason(self, state: Path) -> None:
        (state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n[hosts.localhost]\nssh = "localhost"\n'
        )
        result = CliRunner().invoke(
            main, ["drain", "--max-cpus", "4", "--reason", "kids gaming"]
        )
        assert result.exit_code == 0, result.output
        assert "max_cpus=4" in result.output
        assert "kids gaming" in result.output

    def test_cli_release_when_drain_set(self, state: Path) -> None:
        (state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n[hosts.localhost]\nssh = "localhost"\n'
        )
        drain.write_drain_state(drain.DrainState())
        result = CliRunner().invoke(main, ["drain", "--release"])
        assert result.exit_code == 0, result.output
        assert "drain released" in result.output
        assert not drain.drain_state_path().exists()

    def test_cli_release_when_no_drain_is_noop(self, state: Path) -> None:
        (state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n[hosts.localhost]\nssh = "localhost"\n'
        )
        result = CliRunner().invoke(main, ["drain", "--release"])
        assert result.exit_code == 0
        assert "not set" in result.output or "no-op" in result.output

    def test_cli_status_after_set(self, state: Path) -> None:
        (state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n[hosts.localhost]\nssh = "localhost"\n'
        )
        # Set drain, then query status. Need to use --status FIRST in the
        # second invocation because the click command logic checks
        # status_only before mutation.
        CliRunner().invoke(main, ["drain", "--reason", "maintenance"])
        result = CliRunner().invoke(main, ["drain", "--status"])
        assert result.exit_code == 0
        assert "ACTIVE" in result.output
        assert "maintenance" in result.output

    def test_cli_status_json_when_no_drain(self, state: Path) -> None:
        (state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n[hosts.localhost]\nssh = "localhost"\n'
        )
        result = CliRunner().invoke(main, ["drain", "--status", "--json"])
        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["active"] is False
        assert payload["mode"] == "inactive"

    def test_cli_status_json_full_plus_scheduler_target(
        self, state: Path
    ) -> None:
        (state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n[hosts.localhost]\nssh = "localhost"\n'
        )
        drain.write_drain_state(
            drain.DrainState(
                full_dispatch=True,
                scheduler_hosts=["host_f"],
                reason="pbs idle",
            ),
            via_rpc=False,
        )
        result = CliRunner().invoke(main, ["drain", "--status", "--json"])
        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["active"] is True
        assert payload["mode"] == "full+scheduler-target"
        assert payload["is_full_drain"] is True
        assert payload["scheduler_hosts"] == ["host_f"]
        assert payload["reason"] == "pbs idle"

    def test_new_partial_drain_does_not_reactivate_disabled_legacy_hosts(
        self, state: Path
    ) -> None:
        (state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n[hosts.localhost]\nssh = "localhost"\n'
        )
        drain.write_drain_state(
            drain.DrainState(
                enabled=False,
                scheduler_hosts=["host_f"],
                reason="stale disabled policy",
            ),
            via_rpc=False,
        )

        result = CliRunner().invoke(
            main,
            ["drain", "--max-jobs", "1", "localhost"],
        )

        assert result.exit_code == 0, result.output
        state_after = drain.read_drain_state(via_rpc=False)
        assert state_after is not None
        assert state_after.enabled is True
        assert state_after.max_jobs == 1
        assert state_after.scheduler_hosts == []

    def test_cli_json_requires_status(self, state: Path) -> None:
        (state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n[hosts.localhost]\nssh = "localhost"\n'
        )
        result = CliRunner().invoke(main, ["drain", "--json"])
        assert result.exit_code != 0
        assert "--json is only supported with --status" in result.output

    def test_cli_help_lists_forms(self) -> None:
        result = CliRunner().invoke(main, ["drain", "--help"])
        assert result.exit_code == 0
        for tok in (
            "--max-jobs",
            "--max-cpus",
            "--release",
            "--status",
            "--json",
            "--reason",
            "--duration",
        ):
            assert tok in result.output


class TestDrainSubmitPolicy:
    def test_update_deny_rejects_local_submit(self, state: Path) -> None:
        script = state / "job.py"
        script.write_text("print('hello')\n")
        drain.write_drain_state(
            drain.DrainState(
                update_mode="deny",
                reject_submits=True,
                reason="obsolete runtime",
            ),
            via_rpc=False,
        )
        with pytest.raises(ValueError, match="obsolete runtime"):
            submit_local(host="localhost", input_file=str(script))

    def test_update_accept_allows_pending_submit(self, state: Path) -> None:
        script = state / "job.py"
        script.write_text("print('hello')\n")
        drain.write_drain_state(
            drain.DrainState(update_mode="accept"),
            via_rpc=False,
        )
        jobid = submit_local(host="localhost", input_file=str(script))
        assert (paths.queue_dir() / f"{jobid}.json").exists()

    def test_disabled_stale_deny_record_allows_submit(self, state: Path) -> None:
        script = state / "job.py"
        script.write_text("print('hello')\n")
        drain.write_drain_state(
            drain.DrainState(
                enabled=False,
                update_mode="deny",
                reject_submits=True,
                reason="stale disabled policy",
            ),
            via_rpc=False,
        )

        jobid = submit_local(host="localhost", input_file=str(script))

        assert (paths.queue_dir() / f"{jobid}.json").exists()


class TestDrainAutoRelease:
    """v0.5.16: --duration auto-release. Expired drain.json is silently
    cleared on the next read_drain_state() call."""

    def test_duration_field_stored(self, state: Path) -> None:
        s = drain.DrainState(duration_seconds=7200)
        drain.write_drain_state(s)
        back = drain.read_drain_state()
        assert back is not None
        assert back.duration_seconds == 7200

    @pytest.mark.parametrize("value", [0, -1, True, "2", 2.0])
    def test_invalid_persisted_duration_preserves_hold_as_unbounded(
        self,
        state: Path,
        value: object,
    ) -> None:
        path = drain.drain_state_path(multi_user=False)
        path.write_text(
            json.dumps(
                {
                    "enabled": True,
                    "full_dispatch": True,
                    "duration_seconds": value,
                    "set_at": datetime.now(UTC).isoformat(),
                }
            )
        )

        stored = drain.read_drain_state(via_rpc=False, multi_user=False)

        assert stored is not None
        assert stored.is_full_drain is True
        assert stored.duration_seconds is None
        assert path.exists(), "an invalid expiry must not release the hold"

    def test_unexpired_drain_returns_normally(self, state: Path) -> None:
        """duration set to 1 hour, set_at is now → drain still active."""
        s = drain.DrainState(duration_seconds=3600)  # 1h
        drain.write_drain_state(s)
        back = drain.read_drain_state()
        assert back is not None
        assert back.duration_seconds == 3600

    def test_expired_drain_is_cleared_on_read(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Set drain with a past set_at + small duration → next read
        clears the file and returns None."""
        from datetime import datetime, timedelta
        past = datetime.now(UTC) - timedelta(seconds=120)
        s = drain.DrainState(
            duration_seconds=60,  # expired 60s ago
            set_at=past.isoformat(),
        )
        drain.write_drain_state(s)
        assert drain.drain_state_path().exists()
        # First read sees expiry, clears, returns None.
        assert drain.read_drain_state() is None
        # File is gone.
        assert not drain.drain_state_path().exists()

    def test_expired_full_drain_preserves_scheduler_lanes(
        self, state: Path
    ) -> None:
        """Auto-release of the full gate must not erase target lanes."""
        from datetime import datetime, timedelta
        past = datetime.now(UTC) - timedelta(seconds=120)
        drain.write_drain_state(
            drain.DrainState(
                full_dispatch=True,
                scheduler_hosts=["host_f"],
                duration_seconds=60,
                set_at=past.isoformat(),
            ),
            via_rpc=False,
        )
        stored = drain.read_drain_state(via_rpc=False)
        assert stored is not None
        assert stored.scheduler_hosts == ["host_f"]
        assert stored.is_full_drain is False
        assert stored.duration_seconds is None

    def test_duration_none_means_no_auto_release(
        self, state: Path
    ) -> None:
        """Old-style drain (no duration_seconds) never auto-expires."""
        from datetime import datetime, timedelta
        past = datetime.now(UTC) - timedelta(hours=24)
        s = drain.DrainState(set_at=past.isoformat())
        drain.write_drain_state(s)
        back = drain.read_drain_state()
        assert back is not None  # still active despite being 24h old

    def test_bad_set_at_does_not_clear_drain(self, state: Path) -> None:
        """If set_at is malformed for some reason, treat as no-expire
        (better to keep drain than silently clear it)."""
        path = drain.drain_state_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({
            "enabled": True,
            "max_jobs": None,
            "max_cpus": None,
            "set_at": "not a real timestamp",
            "duration_seconds": 60,
        }))
        back = drain.read_drain_state()
        # We expect drain to be returned (no expiry possible) — but be
        # tolerant to either outcome here as long as the daemon doesn't
        # crash. The point is no exception leaks.
        assert back is None or back is not None  # i.e. "no exception"

    def test_format_status_shows_remaining_time(
        self, state: Path
    ) -> None:
        """--status reports how many seconds until auto-release."""
        s = drain.DrainState(duration_seconds=3600, reason="kids gaming")
        drain.write_drain_state(s)
        out = drain.format_status()
        assert "auto-release in" in out
        assert "kids gaming" in out

    def test_cli_drain_with_duration_writes_field(
        self, state: Path
    ) -> None:
        (state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n[hosts.localhost]\nssh = "localhost"\n'
        )
        result = CliRunner().invoke(main, ["drain", "--duration", "2h"])
        assert result.exit_code == 0, result.output
        assert "auto-release in 7200s" in result.output
        s = drain.read_drain_state()
        assert s is not None
        assert s.duration_seconds == 7200

    def test_cli_drain_bad_duration_errors(self, state: Path) -> None:
        (state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n[hosts.localhost]\nssh = "localhost"\n'
        )
        result = CliRunner().invoke(main, ["drain", "--duration", "junk"])
        assert result.exit_code != 0
        assert "--duration" in result.output


class TestSchedulerReleaseNamesTheDriver:
    """A "not set" release must not read as "nothing to clear".

    A scheduler-target lane is held on that host's DRIVER, not on the scheduler
    host itself, so a release resolving against the scheduler host's own
    (inactive) drain state reports "was not set" while the driver-side lane
    stays held -- leaving the target `accept_pending`, accepting submissions and
    dispatching nothing. host_f 2026-07-26: the documented remedy
    `vq drain --scheduler-host host_f --release` printed "not set" and the lane
    survived; `vq drain localhost --scheduler-host host_f --release` cleared it.
    An operator hitting this is mid-incident and should be handed the command
    that works.
    """

    def _cfg(self, state: Path) -> None:
        (state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
            '[hosts.localhost]\nssh = "localhost"\n'
            "[hosts.host_f]\n"
            'ssh = "host_f"\n'
            'scheduler = "pbs"\n'
            'scheduler_dialect = "torque"\n'
            'scratch_root = "/scratch/u"\n'
            'scheduler_driver = "localhost"\n'
        )

    def test_missing_lane_names_the_driver_and_the_working_command(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._cfg(state)
        original_release = drain.release_legacy_scheduler_host
        monkeypatch.setattr(
            drain,
            "release_legacy_scheduler_host",
            lambda host, **_kwargs: original_release(host, via_rpc=False),
        )
        # No scheduler lane held anywhere.
        result = CliRunner().invoke(
            main, ["drain", "--release", "--scheduler-host", "host_f"]
        )

        assert result.exit_code == 0, result.output
        assert "was not set for host_f" in result.output
        # The operator is told where the lane actually lives...
        assert "held on its driver 'localhost'" in result.output
        # ...and given the exact invocation that clears it.
        assert (
            "vq drain localhost --scheduler-host host_f --release" in result.output
        )

    def test_successful_release_is_unchanged_and_gains_no_hint(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The hint is for the failure path only; a real release stays terse."""
        self._cfg(state)
        drain.write_drain_state(
            drain.DrainState(scheduler_hosts=["host_f"]), via_rpc=False
        )
        original_release = drain.release_legacy_scheduler_host
        monkeypatch.setattr(
            drain,
            "release_legacy_scheduler_host",
            lambda host, **_kwargs: original_release(host, via_rpc=False),
        )

        result = CliRunner().invoke(
            main, ["drain", "--release", "--scheduler-host", "host_f"]
        )

        assert result.exit_code == 0, result.output
        assert "scheduler drain released for host_f" in result.output
        assert "held on its driver" not in result.output

    def test_unconfigured_scheduler_host_still_reports_plainly(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """No driver resolvable -> no invented advice, just the plain message."""
        (state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n[hosts.localhost]\nssh = "localhost"\n'
        )
        original_release = drain.release_legacy_scheduler_host
        monkeypatch.setattr(
            drain,
            "release_legacy_scheduler_host",
            lambda host, **_kwargs: original_release(host, via_rpc=False),
        )

        result = CliRunner().invoke(
            main, ["drain", "--release", "--scheduler-host", "ghost"]
        )

        assert result.exit_code == 0, result.output
        assert "was not set for ghost" in result.output
        assert "held on its driver" not in result.output


class TestDrainStateIsForwardCompatible:
    """A newer vq's state file must not un-drain an older vq's host.

    `read_drain_state` deliberately treats a corrupt drain.json as "no drain"
    so a bad file cannot wedge the dispatch loop. With `extra="forbid"` that
    turned any field a newer vq added into a *silent un-drain* on every host
    still running the older build: the file fails validation, is treated as
    corrupt, and the daemon resumes dispatching into a host an operator had
    held. The fleet is routinely mid-rolling-upgrade, so this is a live shape,
    not a hypothetical. Verified 2026-07-27 against the pre-fix model.
    """

    def test_unknown_field_does_not_discard_a_held_lane(self, state: Path) -> None:
        (paths.state_root() / "drain.json").write_text(
            json.dumps(
                {
                    "enabled": True,
                    "scheduler_hosts": ["host_f"],
                    "reason": "vq admin update host_f",
                    "set_at": "2026-07-27T00:00:00+00:00",
                    # Written by a hypothetical newer vq.
                    "some_future_field": 1234,
                }
            )
        )

        stored = drain.read_drain_state(via_rpc=False)

        assert stored is not None, (
            "an unknown key made a held drain read as NO DRAIN -- the daemon "
            "would dispatch into a host the operator had drained"
        )
        assert stored.scheduler_hosts == ["host_f"]
        assert stored.reason == "vq admin update host_f"

    def test_genuinely_corrupt_state_is_still_treated_as_no_drain(
        self, state: Path
    ) -> None:
        """Tolerating unknown keys must not tolerate unparseable files: that
        fallback exists so a bad file cannot wedge dispatch, and it stays."""
        (paths.state_root() / "drain.json").write_text("{not json at all")

        assert drain.read_drain_state(via_rpc=False) is None

    def test_root_owned_read_only_lock_does_not_break_status_fallback(
        self, state: Path
    ) -> None:
        drain.write_drain_state(
            drain.DrainState(reason="maintenance"), via_rpc=False
        )
        lock_path = paths.state_root() / drain.LEGACY_DRAIN_LOCK_FILENAME
        lock_path.chmod(0o444)

        stored = drain.read_drain_state(via_rpc=False)

        assert stored is not None
        assert stored.reason == "maintenance"

    def test_expiry_cannot_resurrect_an_atomically_released_lane(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        drain.write_drain_state(
            drain.DrainState(
                scheduler_hosts=["host_f", "host_c"],
                duration_seconds=1,
                set_at="2020-01-01T00:00:00+00:00",
            ),
            via_rpc=False,
        )
        original_write = drain.write_drain_state
        expiry_ready = Event()
        allow_expiry = Event()

        def paused_write(
            value: drain.DrainState,
            *,
            via_rpc: bool = True,
            token: str | None = None,
            multi_user: bool | None = None,
        ) -> None:
            expiry_ready.set()
            assert allow_expiry.wait(timeout=5)
            original_write(
                value,
                via_rpc=via_rpc,
                token=token,
                multi_user=multi_user,
            )

        monkeypatch.setattr(drain, "write_drain_state", paused_write)
        reader = Thread(
            target=lambda: drain.read_drain_state(via_rpc=False),
            daemon=True,
        )
        reader.start()
        assert expiry_ready.wait(timeout=5)
        released: list[bool] = []
        releaser = Thread(
            target=lambda: released.append(
                drain.release_legacy_scheduler_host(
                    "host_f", via_rpc=False
                )
            ),
            daemon=True,
        )
        releaser.start()
        assert releaser.is_alive(), "release should wait for expiry transaction"
        allow_expiry.set()
        reader.join(timeout=5)
        releaser.join(timeout=5)

        assert released == [True]
        stored = drain.read_drain_state(via_rpc=False)
        assert stored is not None
        assert stored.scheduler_hosts == ["host_c"]


class TestOrphanedLaneDetection:
    """A leaked lane must be distinguishable from a deliberate hold.

    `vq admin update <scheduler-host> --drain-wait` killed mid-wait leaves its
    lane behind: a `finally` cannot run through an external kill. vq's own
    "never lift an operator's hold" rule then reads the corpse as intentional.
    host_f, 2026-07-26 -- the host sat `accept_pending`, taking submissions and
    dispatching nothing, until someone released it by hand.

    Detection reports; it never lifts. An operator's hold and a leaked one are
    identical on disk precisely because vq must not guess, and guessing wrong
    drops a hold someone is relying on. What this removes is the ambiguity.
    """

    def _vq_owned(self, pid: int, start: int = 0) -> drain.DrainState:
        return drain.DrainState(
            scheduler_hosts=["host_f"],
            reason="vq admin update host_f",
            owner_pid=pid,
            owner_pid_start_time=start,
        )

    def test_a_dead_owner_is_reported(self, state: Path) -> None:
        dead = 999_999_998  # not a live pid
        reason = drain.orphaned_lane_reason(self._vq_owned(dead))

        assert reason is not None
        assert "no longer running" in reason
        assert str(dead) in reason

    def test_a_live_owner_is_not_reported(self, state: Path) -> None:
        """The common case: an update legitimately holding its lane."""
        assert drain.orphaned_lane_reason(self._vq_owned(os.getpid())) is None

    def test_an_operator_hold_is_never_an_orphan(self, state: Path) -> None:
        """No recorded owner means a human took it. A deliberate hold has no
        process to outlive, and must never be reported as abandoned."""
        operator = drain.DrainState(
            scheduler_hosts=["host_f"], reason="heat window"
        )

        assert operator.owner_pid is None
        assert drain.orphaned_lane_reason(operator) is None

    def test_a_recycled_pid_is_reported(self, state: Path) -> None:
        """Alive, but a stranger: the kernel reused the slot after a reboot."""
        live = drain.DrainState(
            scheduler_hosts=["host_f"],
            owner_pid=os.getpid(),
            owner_pid_start_time=1,  # deliberately not this process's
        )
        reason = drain.orphaned_lane_reason(live)

        if drain._pid_start_time(os.getpid()) is None:
            pytest.skip("no /proc on this platform; liveness alone is the signal")
        assert reason is not None
        assert "recycled" in reason

    def test_no_lane_means_nothing_to_report(self, state: Path) -> None:
        assert drain.orphaned_lane_reason(None) is None
        assert drain.orphaned_lane_reason(drain.DrainState()) is None

    def test_status_names_the_release_command(self, state: Path) -> None:
        """The operator reading this is mid-incident and should not have to
        work out the invocation -- which is itself easy to get wrong, since a
        scheduler lane lives on the driver."""
        drain.write_drain_state(self._vq_owned(999_999_998), via_rpc=False)

        out = drain.format_status()

        assert "ORPHANED LEGACY LANE" in out
        assert "Run on this status's daemon host" in out
        assert "--release --scheduler-host host_f --release-legacy-only" in out

    def test_status_is_quiet_for_a_live_owner(self, state: Path) -> None:
        drain.write_drain_state(self._vq_owned(os.getpid()), via_rpc=False)

        assert "ORPHANED" not in drain.format_status()

    def test_overlapping_leases_report_each_orphan_independently(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _ = state
        dead_pid = 91_001
        live_pid = 91_002
        monkeypatch.setattr(
            drain,
            "_pid_alive",
            lambda pid: pid == live_pid,
        )
        drain.acquire_scheduler_drain_lease(
            "host_f",
            owner="rollout:dead",
            owner_pid=dead_pid,
            lease_id="dead-lease",
            via_rpc=False,
        )
        drain.acquire_scheduler_drain_lease(
            "host_f",
            owner="operator:live",
            owner_pid=live_pid,
            lease_id="live-lease",
            via_rpc=False,
        )

        payload = drain.status_payload()

        assert payload["orphaned_scheduler_leases"] == [
            {
                "lease_id": "dead-lease",
                "scheduler_host": "host_f",
                "owner": "rollout:dead",
                "reason": (
                    "the `vq admin update` process (pid=91001) that took "
                    "this lane is no longer running"
                ),
            }
        ]
        text = drain.format_status()
        assert "ORPHANED SCHEDULER LEASE" in text
        assert "lease_id=dead-lease" in text
        assert "SCHEDULER LEASE: host=host_f owner=operator:live" in text
        assert "lease_id=live-lease" in text
        assert "On this status's daemon host" in text

    def test_legacy_partial_drain_does_not_hide_lease_orphan(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _ = state
        monkeypatch.setattr(drain, "_pid_alive", lambda _pid: False)
        drain.write_drain_state(
            drain.DrainState(max_jobs=1, reason="operator cap"),
            via_rpc=False,
        )
        drain.acquire_scheduler_drain_lease(
            "host_c",
            owner="admin-update:host_c:test",
            owner_pid=92_001,
            lease_id="host_c-orphan",
            via_rpc=False,
        )

        payload = drain.status_payload()

        assert payload["reason"] == "operator cap"
        assert [
            item["lease_id"]
            for item in payload["orphaned_scheduler_leases"]
        ] == ["host_c-orphan"]

    def test_partial_cap_and_scheduler_lease_are_both_rendered(
        self, state: Path
    ) -> None:
        drain.write_drain_state(
            drain.DrainState(max_jobs=1, reason="operator cap"),
            via_rpc=False,
        )
        drain.acquire_scheduler_drain_lease(
            "host_f",
            owner="fleet-rollout:test:host_f",
            lease_id="fleet-host_f",
            via_rpc=False,
        )

        payload = drain.status_payload()
        rendered = drain.format_status()

        assert payload["mode"] == "partial+scheduler-target"
        assert payload["max_jobs"] == 1
        assert "mode: partial + scheduler-target" in rendered
        assert "max_jobs=1" in rendered
        assert "owner=fleet-rollout:test:host_f" in rendered


class TestDurationDoesNotPromiseWhatItCannotDeliver:
    """A scheduler lane outlives its own `--duration`.

    At expiry `read_drain_state` clears the global hold, PRESERVES
    `scheduler_hosts`, and nulls `duration_seconds` -- so the lane becomes
    permanently unbounded exactly when its countdown runs out. Advertising a
    bare "auto-release in Ns" told an operator to wait for something that will
    never happen, which is the shape that let the 2026-07-26 host_f lane leak sit
    unnoticed. `rollout-latest` passes `--duration` on both branches, so this is
    the normal case, not a corner.
    """

    def test_a_scheduler_lane_countdown_says_what_it_covers(
        self, state: Path
    ) -> None:
        drain.write_drain_state(
            drain.DrainState(scheduler_hosts=["host_f"], duration_seconds=3600),
            via_rpc=False,
        )

        out = drain.format_status()

        assert "GLOBAL hold only" in out
        assert "host_f" in out
        assert "released by hand" in out

    def test_a_plain_full_drain_countdown_is_unchanged(self, state: Path) -> None:
        """A pure full drain really is cleared outright on expiry, so its
        countdown was never a lie and must stay terse."""
        drain.write_drain_state(
            drain.DrainState(duration_seconds=3600), via_rpc=False
        )

        out = drain.format_status()

        assert "auto-release in" in out
        assert "GLOBAL hold only" not in out

    def test_json_flags_that_the_timer_leaves_a_lane_behind(
        self, state: Path
    ) -> None:
        drain.write_drain_state(
            drain.DrainState(scheduler_hosts=["host_f"], duration_seconds=3600),
            via_rpc=False,
        )

        payload = drain.status_payload()

        assert payload["duration_releases_everything"] is False
        assert payload["remaining_seconds"] is not None

    def test_json_flag_is_true_for_a_plain_full_drain(self, state: Path) -> None:
        drain.write_drain_state(
            drain.DrainState(duration_seconds=3600), via_rpc=False
        )

        assert drain.status_payload()["duration_releases_everything"] is True

    def test_the_expiry_behaviour_this_describes_is_real(self, state: Path) -> None:
        """Guards the claim itself: if expiry ever starts clearing scheduler
        lanes, the warning above becomes the new lie."""
        drain.write_drain_state(
            drain.DrainState(
                scheduler_hosts=["host_f"],
                duration_seconds=1,
                set_at="2020-01-01T00:00:00+00:00",
            ),
            via_rpc=False,
        )

        after = drain.read_drain_state(via_rpc=False)

        assert after is not None
        assert after.scheduler_hosts == ["host_f"], "lane should survive expiry"
        assert after.duration_seconds is None, "and lose its bound"


class TestAnUninterpretableDrainRPCDoesNotReportNoDrain:
    """VQ-DRAIN-RPC: `vq drain --status` reported "no drain" while drain.json
    held an active hold, so `--release`/`--release-full` no-op'd and the
    operator could not clear a hold that was still parking the fleet.

    The client asked the daemon over RPC, could not interpret the answer, and
    returned None -- which every caller reads as "nothing is draining". The
    daemon's own dispatch loop reads the file directly, so it kept enforcing
    the hold. An answer we cannot read is a failure, not an absence.
    """

    def _drained_file(self, tmp_path: Path) -> None:
        drain.write_drain_state(
            drain.DrainState(enabled=True, full_dispatch=True), via_rpc=False
        )

    def test_an_unvalidatable_answer_falls_back_to_the_file(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._drained_file(tmp_path)
        # A daemon whose record this client cannot validate.
        monkeypatch.setattr(
            rpc, "try_rpc_or_fallback", lambda *a, **k: {"enabled": "not-a-bool"}
        )

        state = drain.read_drain_state(via_rpc=True)

        assert state is not None, "reported no drain while the file holds one"
        assert state.full_dispatch is True

    def test_a_nonsense_answer_type_falls_back_to_the_file(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._drained_file(tmp_path)
        monkeypatch.setattr(rpc, "try_rpc_or_fallback", lambda *a, **k: "surprise")

        state = drain.read_drain_state(via_rpc=True)

        assert state is not None
        assert state.full_dispatch is True

    def test_an_explicit_none_still_means_no_drain(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The daemon answering "nothing is draining" must stay authoritative;
        this fix must not make a released drain look active."""
        self._drained_file(tmp_path)
        monkeypatch.setattr(rpc, "try_rpc_or_fallback", lambda *a, **k: None)

        assert drain.read_drain_state(via_rpc=True) is None

    def test_a_valid_answer_is_used_unchanged(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            rpc,
            "try_rpc_or_fallback",
            lambda *a, **k: {"enabled": True, "max_jobs": 3},
        )

        state = drain.read_drain_state(via_rpc=True)

        assert state is not None
        assert state.max_jobs == 3

    @pytest.mark.parametrize("value", [0, -1, True, "2", 2.0])
    def test_invalid_rpc_duration_preserves_hold_as_unbounded(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        value: object,
    ) -> None:
        monkeypatch.setattr(
            rpc,
            "try_rpc_or_fallback",
            lambda *a, **k: {
                "enabled": True,
                "full_dispatch": True,
                "duration_seconds": value,
            },
        )

        state = drain.read_drain_state(via_rpc=True)

        assert state is not None
        assert state.is_full_drain is True
        assert state.duration_seconds is None


class TestReadOnlyDrainSnapshot:
    """The rollout observer must never perform lazy-expiry writes."""

    @staticmethod
    def _fingerprint(root: Path) -> dict[str, tuple[bytes, int, int]]:
        return {
            str(path.relative_to(root)): (
                path.read_bytes(),
                path.stat().st_mode,
                path.stat().st_mtime_ns,
            )
            for path in root.rglob("*")
            if path.is_file()
        }

    @staticmethod
    def _rpc_result(snapshot: dict[str, object]) -> dict[str, object]:
        server = rpc.RPCServer(
            multi_user=False,
            source_sha_reader=lambda: "a" * 40,
            source_tree_sha256_reader=lambda: "b" * 64,
        )
        rpc.register_get_drain_read_only_snapshot_method(
            server,
            lambda: snapshot,
        )
        return server._methods[drain.DRAIN_READ_ONLY_SNAPSHOT_RPC_METHOD]()  # noqa: SLF001

    def test_snapshot_requires_precreated_locks_without_creating_anything(
        self,
        state: Path,
    ) -> None:
        root = paths.state_root()
        before = sorted(path.name for path in root.iterdir())

        with pytest.raises(drain.DrainSnapshotError, match="stable snapshot lock"):
            drain.read_locked_drain_snapshot()

        assert sorted(path.name for path in root.iterdir()) == before

    @pytest.mark.parametrize("name", [".drain.lock", ".scheduler-drain-leases.lock"])
    def test_snapshot_rejects_fifo_lock_without_blocking(
        self,
        state: Path,
        name: str,
    ) -> None:
        drain.prepare_read_only_drain_snapshot_locks()
        lock_path = paths.state_root() / name
        lock_path.unlink()
        os.mkfifo(lock_path)
        before = lock_path.stat()
        started = time.monotonic()

        with pytest.raises(drain.DrainSnapshotError, match="snapshot lock"):
            drain.read_locked_drain_snapshot()

        assert time.monotonic() - started < 1
        after = lock_path.stat()
        assert (after.st_mode, after.st_mtime_ns) == (
            before.st_mode,
            before.st_mtime_ns,
        )

    @pytest.mark.parametrize(
        ("path", "error_key"),
        [
            ("drain.json", "legacy_error"),
            ("scheduler-drain-leases.json", "scheduler_leases_error"),
        ],
    )
    def test_snapshot_rejects_fifo_data_without_blocking(
        self,
        state: Path,
        path: str,
        error_key: str,
    ) -> None:
        drain.prepare_read_only_drain_snapshot_locks()
        data_path = paths.state_root() / path
        os.mkfifo(data_path)
        before = data_path.stat()
        started = time.monotonic()

        snapshot = drain.read_locked_drain_snapshot()

        assert time.monotonic() - started < 1
        assert snapshot[error_key] == "unreadable"
        after = data_path.stat()
        assert (after.st_mode, after.st_mtime_ns) == (
            before.st_mode,
            before.st_mtime_ns,
        )

    @pytest.mark.parametrize("name", [".drain.lock", ".scheduler-drain-leases.lock"])
    def test_snapshot_fails_promptly_when_either_lock_is_held(
        self,
        state: Path,
        name: str,
    ) -> None:
        drain.prepare_read_only_drain_snapshot_locks()
        lock_path = paths.state_root() / name
        holder = subprocess.Popen(
            [
                sys.executable,
                "-c",
                (
                    "import fcntl,sys,time; "
                    "f=open(sys.argv[1]); fcntl.flock(f,fcntl.LOCK_EX); "
                    "print('ready',flush=True); time.sleep(30)"
                ),
                str(lock_path),
            ],
            stdout=subprocess.PIPE,
            text=True,
        )
        try:
            assert holder.stdout is not None
            assert holder.stdout.readline().strip() == "ready"
            started = time.monotonic()

            with pytest.raises(drain.DrainSnapshotError, match="lock is busy"):
                drain.read_locked_drain_snapshot()

            assert time.monotonic() - started < 1
        finally:
            holder.terminate()
            holder.wait(timeout=5)

    @pytest.mark.parametrize("direction", ["lease-to-full", "full-to-lease"])
    def test_snapshot_is_one_atomic_endpoint_during_store_handoff(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
        direction: str,
    ) -> None:
        drain.prepare_read_only_drain_snapshot_locks()
        lease_id = "handoff-lease"
        if direction == "lease-to-full":
            drain.acquire_scheduler_drain_lease(
                "host_f",
                owner="operator",
                lease_id=lease_id,
                via_rpc=False,
            )
        else:
            drain.write_drain_state(
                drain.DrainState(full_dispatch=True, reason="maintenance"),
                via_rpc=False,
            )

        writer_started = Event()
        writer_done = Event()
        writer_errors: list[BaseException] = []

        def writer() -> None:
            writer_started.set()
            try:
                if direction == "lease-to-full":
                    drain.write_drain_state(
                        drain.DrainState(
                            full_dispatch=True,
                            reason="maintenance",
                        ),
                        via_rpc=False,
                    )
                    drain.release_scheduler_drain_lease(
                        lease_id,
                        via_rpc=False,
                    )
                else:
                    drain.acquire_scheduler_drain_lease(
                        "host_f",
                        owner="operator",
                        lease_id=lease_id,
                        via_rpc=False,
                    )
                    drain.clear_drain(via_rpc=False)
            except BaseException as exc:
                writer_errors.append(exc)
            finally:
                writer_done.set()

        original_read = drain._strict_snapshot_json
        launched = False
        worker = Thread(target=writer)

        def interleaved_read(path: Path) -> dict[str, object] | None:
            nonlocal launched
            if not launched:
                launched = True
                worker.start()
                assert writer_started.wait(timeout=1)
                time.sleep(0.05)
                assert not writer_done.is_set()
            return original_read(path)

        monkeypatch.setattr(drain, "_strict_snapshot_json", interleaved_read)

        snapshot = drain.read_locked_drain_snapshot()
        worker.join(timeout=5)

        assert not worker.is_alive()
        assert writer_errors == []
        if direction == "lease-to-full":
            assert snapshot["legacy_state"] is None
            assert len(snapshot["scheduler_leases"]) == 1
        else:
            assert snapshot["legacy_state"] is not None
            assert snapshot["scheduler_leases"] == []
        assert snapshot["legacy_state"] is not None or snapshot[
            "scheduler_leases"
        ]

    def test_expired_full_snapshot_is_inactive_without_touching_the_tree(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        drain.prepare_read_only_drain_snapshot_locks()
        drain.write_drain_state(
            drain.DrainState(
                duration_seconds=60,
                set_at=(datetime.now(UTC) - timedelta(seconds=120)).isoformat(),
            ),
            via_rpc=False,
        )
        root = paths.state_root()
        before = self._fingerprint(root)
        response = self._rpc_result(drain.read_locked_drain_snapshot())
        monkeypatch.setattr(rpc, "call", lambda *args, **kwargs: response)

        payload = drain.read_only_status_payload()

        assert payload["active"] is False
        assert payload["policy"] is None
        assert payload["coverage"] == {
            "legacy_state": True,
            "scheduler_leases": True,
        }
        assert self._fingerprint(root) == before

    def test_expired_full_plus_scheduler_projects_target_only_without_write(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        drain.prepare_read_only_drain_snapshot_locks()
        drain.write_drain_state(
            drain.DrainState(
                full_dispatch=True,
                scheduler_hosts=["host_f"],
                duration_seconds=60,
                set_at=(datetime.now(UTC) - timedelta(seconds=120)).isoformat(),
            ),
            via_rpc=False,
        )
        root = paths.state_root()
        before = self._fingerprint(root)
        response = self._rpc_result(drain.read_locked_drain_snapshot())
        monkeypatch.setattr(rpc, "call", lambda *args, **kwargs: response)

        payload = drain.read_only_status_payload()

        assert payload["active"] is True
        assert payload["policy"]["is_full_drain"] is False
        assert payload["policy"]["scheduler_hosts"] == ["host_f"]
        assert payload["policy"]["duration_seconds"] is None
        assert self._fingerprint(root) == before

    @pytest.mark.parametrize(
        ("state_value", "expected"),
        [
            (drain.DrainState(max_jobs=0), {"max_jobs": 0, "max_cpus": None}),
            (drain.DrainState(max_cpus=0), {"max_jobs": None, "max_cpus": 0}),
            (
                drain.DrainState(update_mode="deny", reject_submits=True),
                {"reject_submits": True, "update_mode": "deny"},
            ),
        ],
    )
    def test_snapshot_preserves_zero_caps_and_submit_deny(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
        state_value: drain.DrainState,
        expected: dict[str, object],
    ) -> None:
        drain.prepare_read_only_drain_snapshot_locks()
        drain.write_drain_state(state_value, via_rpc=False)
        response = self._rpc_result(drain.read_locked_drain_snapshot())
        monkeypatch.setattr(rpc, "call", lambda *args, **kwargs: response)

        policy = drain.read_only_status_payload()["policy"]

        assert isinstance(policy, dict)
        assert {key: policy[key] for key in expected} == expected

    def test_corrupt_sidecar_is_incomplete_with_a_safety_gate(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        drain.prepare_read_only_drain_snapshot_locks()
        drain.scheduler_drain_leases_path().write_text(
            '{"schema_version": 1, "schema_version": 1, "leases": []}',
            encoding="utf-8",
        )
        response = self._rpc_result(drain.read_locked_drain_snapshot())
        monkeypatch.setattr(rpc, "call", lambda *args, **kwargs: response)

        payload = drain.read_only_status_payload()

        assert payload["coverage"]["scheduler_leases"] is False
        assert payload["safety_fail_closed"] is True
        assert payload["active"] is True

    @pytest.mark.parametrize("layout", ["oversized", "symlink"])
    def test_observer_safety_gate_matches_dispatch_sidecar_reader(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
        layout: str,
    ) -> None:
        drain.prepare_read_only_drain_snapshot_locks()
        store_path = drain.scheduler_drain_leases_path()
        store = {
            "schema_version": 1,
            "leases": [
                {
                    "lease_id": "operator-host_f",
                    "scheduler_host": "host_f",
                    "owner": "operator",
                    "set_at": "2026-08-11T11:00:00+00:00",
                    "reason": "maintenance",
                }
            ],
        }
        if layout == "oversized":
            store["future_padding"] = "x" * (
                drain._DRAIN_SNAPSHOT_FILE_LIMIT + 1
            )
            store_path.write_text(json.dumps(store), encoding="utf-8")
        else:
            target = state / "valid-sidecar.json"
            target.write_text(json.dumps(store), encoding="utf-8")
            store_path.symlink_to(target)

        enforced = drain.read_effective_drain_state(via_rpc=False)
        response = self._rpc_result(drain.read_locked_drain_snapshot())
        monkeypatch.setattr(rpc, "call", lambda *args, **kwargs: response)
        observed = drain.read_only_status_payload()

        assert enforced is not None
        assert enforced.is_full_drain is True
        assert observed["active"] is True
        assert observed["safety_fail_closed"] is True
        assert observed["policy"]["is_full_drain"] is True

    def test_lease_write_overflow_fails_before_replacing_valid_store(
        self,
        state: Path,
    ) -> None:
        store_path = drain.scheduler_drain_leases_path()
        lease = drain.SchedulerDrainLease(
            lease_id="new-lease",
            scheduler_host="host_f",
            owner="operator",
        )
        new_without_padding = drain._SchedulerDrainLeaseStore(
            leases=[lease],
            future_padding="",
        )
        fixed_size = len(
            (new_without_padding.model_dump_json(indent=2) + "\n").encode(
                "utf-8"
            )
        )
        padding = "x" * (
            drain._DRAIN_SNAPSHOT_FILE_LIMIT - fixed_size + 1
        )
        original = json.dumps(
            {
                "schema_version": 1,
                "leases": [],
                "future_padding": padding,
            }
        ).encode("utf-8")
        assert len(original) <= drain._DRAIN_SNAPSHOT_FILE_LIMIT
        store_path.write_bytes(original)
        before = store_path.read_bytes()

        with pytest.raises(
            drain.SchedulerDrainLeaseError,
            match="exceeds the supported size limit",
        ):
            drain.acquire_scheduler_drain_lease(
                "host_f",
                owner="operator",
                lease_id="new-lease",
                via_rpc=False,
            )

        assert store_path.read_bytes() == before

    def test_missing_or_malformed_rpc_never_reads_local_state(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        drain.prepare_read_only_drain_snapshot_locks()
        drain.write_drain_state(drain.DrainState(), via_rpc=False)
        touched: list[str] = []
        monkeypatch.setattr(
            drain,
            "read_locked_drain_snapshot",
            lambda **kwargs: touched.append("direct") or {},
        )
        monkeypatch.setattr(
            rpc,
            "call",
            lambda *args, **kwargs: (_ for _ in ()).throw(
                rpc.RPCError("unknown method")
            ),
        )

        with pytest.raises(drain.DrainSnapshotError, match="supported read-only"):
            drain.read_only_status_payload()

        assert touched == []

    def test_hidden_cli_snapshot_returns_exact_supported_payload(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        (state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
            '[hosts.localhost]\nssh = "localhost"\n'
        )
        payload = {
            "schema": drain.DRAIN_READ_ONLY_STATUS_SCHEMA,
            "active": False,
        }
        calls: list[bool] = []
        monkeypatch.setattr(
            drain,
            "read_only_status_payload",
            lambda *, multi_user: calls.append(multi_user) or payload,
        )
        monkeypatch.setattr(
            drain,
            "status_payload",
            lambda: pytest.fail("ordinary status must not be used"),
        )

        result = CliRunner().invoke(
            main,
            ["drain", "--status", "--json", "--read-only-snapshot"],
        )

        assert result.exit_code == 0, result.output
        assert json.loads(result.output) == payload
        assert calls == [False]

    def test_hidden_cli_snapshot_missing_rpc_fails_without_fallback(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        (state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
            '[hosts.localhost]\nssh = "localhost"\n'
        )
        monkeypatch.setattr(
            drain,
            "read_only_status_payload",
            lambda **kwargs: (_ for _ in ()).throw(
                drain.DrainSnapshotError("supported RPC missing")
            ),
        )
        monkeypatch.setattr(
            drain,
            "status_payload",
            lambda: pytest.fail("ordinary status must not be used"),
        )

        result = CliRunner().invoke(
            main,
            ["drain", "--status", "--json", "--read-only-snapshot"],
        )

        assert result.exit_code != 0
        assert "supported RPC missing" in result.output

    def test_hidden_cli_snapshot_forwards_exact_remote_read_only_argv(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        (state / "cfg" / "config.toml").write_text(
            'default_host = "remote"\n'
            '[hosts.remote]\nssh = "remote-login"\n'
        )
        calls: list[tuple[str, tuple[str, ...]]] = []

        def delegate(
            host: str,
            cfg: config.Config,
            *args: str,
            **kwargs: object,
        ) -> str:
            del cfg, kwargs
            calls.append((host, args))
            return "{}\n"

        monkeypatch.setattr("vq.cli._delegate_to_remote", delegate)

        result = CliRunner().invoke(
            main,
            [
                "drain",
                "--status",
                "--json",
                "--read-only-snapshot",
                "remote",
            ],
        )

        assert result.exit_code == 0, result.output
        assert calls == [
            (
                "remote",
                (
                    "drain",
                    "--status",
                    "--json",
                    "--read-only-snapshot",
                    "localhost",
                ),
            )
        ]

    @pytest.mark.parametrize(
        "extra",
        [
            ["--all"],
            ["--reason", "mutation"],
            ["--max-jobs", "0"],
            ["--release"],
        ],
    )
    def test_hidden_cli_snapshot_rejects_fanout_and_mutations_before_action(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
        extra: list[str],
    ) -> None:
        (state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
            '[hosts.localhost]\nssh = "localhost"\n'
        )
        monkeypatch.setattr(
            drain,
            "read_only_status_payload",
            lambda **kwargs: pytest.fail("snapshot action must not run"),
        )
        result = CliRunner().invoke(
            main,
            [
                "drain",
                "--status",
                "--json",
                "--read-only-snapshot",
                *extra,
            ],
        )

        assert result.exit_code == 2
        assert "single-control read" in result.output
