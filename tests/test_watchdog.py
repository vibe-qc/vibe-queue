"""Tests for the Watchdog: threshold logic, wall-time, /proc readers."""
from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

import pytest

from vq import cgroup, watchdog
from vq.spec import JobSpec, JobState
from vq.watchdog import Watchdog, WatchdogAction

LINUX = sys.platform.startswith("linux")


@pytest.fixture(autouse=True)
def _no_ambient_cgroup_lookup(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make generic sampler tests independent of the runner's own cgroup.

    Tests of cgroup sampling override these stubs explicitly. Without this
    baseline, asking the watchdog to inspect PID 1 inside a Linux CI
    container samples the container cgroup instead of the mocked pgid data.
    """
    monkeypatch.setattr(cgroup, "cgroup_path_for_pid", lambda pid: None)
    monkeypatch.setattr(
        cgroup,
        "cgroup_path_for_scope",
        lambda unit, **kwargs: None,
    )


def _spec_with(tmp_path: Path, **overrides: object) -> JobSpec:
    base: dict[str, object] = {
        "id": "wd-job-001",
        "command": ["true"],
        "cwd": str(tmp_path / "ws"),
        "cpus": 1,
    }
    base.update(overrides)
    workspace = tmp_path / "ws"
    workspace.mkdir(parents=True, exist_ok=True)
    return JobSpec(**base)


class TestWallTimeEnforcement:
    """wall_time_seconds fires regardless of sampling cadence -- the
    watchdog must catch it on the very next evaluate() call after the
    deadline, not wait for sample_interval."""

    def test_within_limit_returns_ok(self, tmp_path: Path) -> None:
        wd = Watchdog(interval_seconds=60)  # long sample interval
        spec = _spec_with(tmp_path, wall_time_seconds=10)
        wd.register("wd-job-001", started_monotonic=time.monotonic())
        v = wd.evaluate("wd-job-001", pid=1, pgid=1, spec=spec)
        assert v.action == WatchdogAction.OK

    def test_over_limit_returns_sigterm_with_time_exceeded(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        wd = Watchdog(interval_seconds=60)
        spec = _spec_with(tmp_path, wall_time_seconds=1)
        # Backdate started_monotonic by 5s so the deadline has passed
        wd.register("wd-job-001", started_monotonic=time.monotonic() - 5.0)
        v = wd.evaluate("wd-job-001", pid=1, pgid=1, spec=spec)
        assert v.action == WatchdogAction.SIGTERM
        assert v.terminal_state == JobState.TIME_EXCEEDED
        assert "wall_time_seconds exceeded" in v.reason

    def test_finite_pause_credit_does_not_disable_wall_limit(
        self,
        tmp_path: Path,
    ) -> None:
        wd = Watchdog(interval_seconds=60)
        spec = _spec_with(
            tmp_path,
            wall_time_seconds=3,
            paused_seconds_total=2.0,
        )
        wd.register("wd-job-001", started_monotonic=time.monotonic() - 6.0)

        verdict = wd.evaluate("wd-job-001", pid=1, pgid=1, spec=spec)

        assert verdict.action == WatchdogAction.SIGTERM
        assert verdict.terminal_state == JobState.TIME_EXCEEDED
        assert "paused total 2s" in verdict.reason

    def test_no_wall_time_set_never_fires(self, tmp_path: Path) -> None:
        wd = Watchdog(interval_seconds=60)
        spec = _spec_with(tmp_path)  # no wall_time_seconds
        wd.register("wd-job-001", started_monotonic=time.monotonic() - 1e9)
        # Skip /proc reads -- patch them to None
        v = wd.evaluate("wd-job-001", pid=1, pgid=1, spec=spec)
        # No wall-time, no rss, no cputime -> OK
        assert v.action == WatchdogAction.OK


class TestKillEscalation:
    """First trigger: SIGTERM. Subsequent evaluate() calls within the
    grace period: OK (don't double-signal). After the grace expires:
    SIGKILL."""

    def test_grace_then_sigkill(self, tmp_path: Path) -> None:
        wd = Watchdog(interval_seconds=60, grace_seconds=2.0)
        spec = _spec_with(tmp_path, wall_time_seconds=1)
        wd.register("wd-job-001", started_monotonic=time.monotonic() - 5.0)

        v1 = wd.evaluate("wd-job-001", pid=1, pgid=1, spec=spec)
        assert v1.action == WatchdogAction.SIGTERM

        # Within grace period: no second signal
        v2 = wd.evaluate("wd-job-001", pid=1, pgid=1, spec=spec)
        assert v2.action == WatchdogAction.OK

        # Backdate the SIGTERM stamp so the grace looks expired
        wd._states["wd-job-001"].sigterm_sent_at_monotonic = time.monotonic() - 10.0
        v3 = wd.evaluate("wd-job-001", pid=1, pgid=1, spec=spec)
        assert v3.action == WatchdogAction.SIGKILL
        assert v3.terminal_state == JobState.TIME_EXCEEDED  # carried through

    def test_sigkill_emitted_only_once(self, tmp_path: Path) -> None:
        """v0.6.17 audit fix: after SIGKILL has been emitted once,
        subsequent evaluate() calls return OK rather than emitting
        SIGKILL again. The daemon's _reap_finished_jobs loop owns
        the actual termination detection; the watchdog's job ends
        with the SIGKILL dispatch.

        Without this guard, a stuck D-state process (broken NFS,
        kernel hang) would generate "SIGKILL emitted" log lines
        every evaluate-tick forever."""
        wd = Watchdog(interval_seconds=60, grace_seconds=2.0)
        spec = _spec_with(tmp_path, wall_time_seconds=1)
        wd.register("wd-job-001", started_monotonic=time.monotonic() - 5.0)

        # Trigger SIGTERM → backdate → SIGKILL emitted (first time)
        wd.evaluate("wd-job-001", pid=1, pgid=1, spec=spec)
        wd._states["wd-job-001"].sigterm_sent_at_monotonic = (
            time.monotonic() - 10.0
        )
        v_first_sigkill = wd.evaluate("wd-job-001", pid=1, pgid=1, spec=spec)
        assert v_first_sigkill.action == WatchdogAction.SIGKILL

        # The state should now record we emitted SIGKILL
        assert wd._states["wd-job-001"].sigkill_emitted is True

        # Subsequent evaluate() calls return OK regardless of how long
        # grace has been elapsed for. Even if we let "grace" stretch
        # to 100s (representing a truly stuck process), no second
        # SIGKILL fires.
        wd._states["wd-job-001"].sigterm_sent_at_monotonic = (
            time.monotonic() - 100.0
        )
        for _ in range(5):
            v = wd.evaluate("wd-job-001", pid=1, pgid=1, spec=spec)
            assert v.action == WatchdogAction.OK, (
                "every evaluate after the first SIGKILL must be OK; "
                "the daemon's reaper handles termination from here"
            )

    def test_within_grace_then_sigkill_marks_emitted(
        self, tmp_path: Path
    ) -> None:
        """Sanity: the sigkill_emitted flag is False during the grace
        window (we're still in SIGTERM territory) and only flips True
        once we actually emit SIGKILL."""
        wd = Watchdog(interval_seconds=60, grace_seconds=2.0)
        spec = _spec_with(tmp_path, wall_time_seconds=1)
        wd.register("wd-job-001", started_monotonic=time.monotonic() - 5.0)

        wd.evaluate("wd-job-001", pid=1, pgid=1, spec=spec)
        assert wd._states["wd-job-001"].sigkill_emitted is False  # SIGTERM only

        # Within grace: still False
        wd.evaluate("wd-job-001", pid=1, pgid=1, spec=spec)
        assert wd._states["wd-job-001"].sigkill_emitted is False

        # Grace expired → SIGKILL → flag now True
        wd._states["wd-job-001"].sigterm_sent_at_monotonic = (
            time.monotonic() - 10.0
        )
        wd.evaluate("wd-job-001", pid=1, pgid=1, spec=spec)
        assert wd._states["wd-job-001"].sigkill_emitted is True


class TestRSSCheck:
    """RSS-based kills: per-job declared cap and host-percent ceiling.
    /proc readers are mocked since macOS dev boxes have no /proc."""

    def test_rss_over_declared_mem_mb_kills(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        wd = Watchdog(interval_seconds=0.0)  # always sample
        spec = _spec_with(tmp_path, mem_mb=100)
        # Force a previous-sample so cputime delta computes.
        wd.register("wd-job-001", started_monotonic=time.monotonic() - 1.0)
        wd._states["wd-job-001"].last_cputime_seconds = 0.0
        wd._states["wd-job-001"].last_wall_monotonic = time.monotonic() - 1.0

        monkeypatch.setattr(watchdog, "read_rss_mb_pgid", lambda pid: 999)
        monkeypatch.setattr(watchdog, "read_cputime_seconds_pgid", lambda pid: 0.5)

        v = wd.evaluate("wd-job-001", pid=1, pgid=1, spec=spec)
        assert v.action == WatchdogAction.SIGTERM
        assert v.terminal_state == JobState.OOM_KILLED
        assert "999 MB exceeded declared mem_mb=100" in v.reason

    def test_rss_under_declared_is_ok(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        wd = Watchdog(interval_seconds=0.0)
        spec = _spec_with(tmp_path, mem_mb=1000)
        wd.register("wd-job-001", started_monotonic=time.monotonic() - 1.0)
        wd._states["wd-job-001"].last_cputime_seconds = 0.0
        wd._states["wd-job-001"].last_wall_monotonic = time.monotonic() - 1.0

        monkeypatch.setattr(watchdog, "read_rss_mb_pgid", lambda pid: 500)
        monkeypatch.setattr(watchdog, "read_cputime_seconds_pgid", lambda pid: 0.5)

        v = wd.evaluate("wd-job-001", pid=1, pgid=1, spec=spec)
        assert v.action == WatchdogAction.OK

    def test_host_percent_ceiling_fires_without_per_job_cap(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # 90% of 1000 MB = 900 MB. RSS is 950.
        wd = Watchdog(
            interval_seconds=0.0, max_rss_percent=90.0, host_total_mem_mb=1000
        )
        spec = _spec_with(tmp_path)  # no mem_mb declared
        wd.register("wd-job-001", started_monotonic=time.monotonic() - 1.0)
        wd._states["wd-job-001"].last_cputime_seconds = 0.0
        wd._states["wd-job-001"].last_wall_monotonic = time.monotonic() - 1.0

        monkeypatch.setattr(watchdog, "read_rss_mb_pgid", lambda pid: 950)
        monkeypatch.setattr(watchdog, "read_cputime_seconds_pgid", lambda pid: 0.5)

        v = wd.evaluate("wd-job-001", pid=1, pgid=1, spec=spec)
        assert v.action == WatchdogAction.SIGTERM
        assert v.terminal_state == JobState.OOM_KILLED
        assert "host cap 90%" in v.reason

    def test_host_total_zero_does_not_trigger_oom(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """v0.6.17 audit fix: host_total_mem_mb=0 must be treated
        as 'unknown' (same as None), NOT as 'cap is 0 MB' which
        would OOM-kill every job whose rss > 0.

        Pre-fix, a misconfigured /proc/meminfo parser, a future
        psutil-free fallback returning 0 on parse failure, or a CLI
        passing --host-total-mem-mb 0 would set host_cap=0 and
        produce 'host cap 90% of 0 MB (0 MB)' confusion."""
        wd = Watchdog(
            interval_seconds=0.0,
            max_rss_percent=90.0,
            host_total_mem_mb=0,  # the bug-triggering value
        )
        spec = _spec_with(tmp_path)  # no per-job mem_mb declared
        wd.register("wd-job-001", started_monotonic=time.monotonic() - 1.0)
        wd._states["wd-job-001"].last_cputime_seconds = 0.0
        wd._states["wd-job-001"].last_wall_monotonic = time.monotonic() - 1.0

        # RSS is large; with the bug, host_cap=0 would trigger OOM.
        monkeypatch.setattr(watchdog, "read_rss_mb_pgid", lambda pid: 999)
        monkeypatch.setattr(watchdog, "read_cputime_seconds_pgid", lambda pid: 0.5)

        v = wd.evaluate("wd-job-001", pid=1, pgid=1, spec=spec)
        # Job survives: no host cap applied, no per-job cap.
        assert v.action == WatchdogAction.OK, (
            f"host_total_mem_mb=0 must not trigger OOM, got {v!r}"
        )

    def test_no_host_total_no_per_job_cap_means_no_oom_check(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        wd = Watchdog(interval_seconds=0.0, host_total_mem_mb=None)
        spec = _spec_with(tmp_path)  # no mem_mb
        wd.register("wd-job-001", started_monotonic=time.monotonic() - 1.0)
        wd._states["wd-job-001"].last_cputime_seconds = 0.0
        wd._states["wd-job-001"].last_wall_monotonic = time.monotonic() - 1.0

        monkeypatch.setattr(watchdog, "read_rss_mb_pgid", lambda pid: 999_999)
        monkeypatch.setattr(watchdog, "read_cputime_seconds_pgid", lambda pid: 0.5)

        v = wd.evaluate("wd-job-001", pid=1, pgid=1, spec=spec)
        assert v.action == WatchdogAction.OK

    def test_host_percent_ceiling_still_fires_with_enforce_memory_off(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Regression: enforce_memory=False (cgroup mode) must NOT disable
        the host-percent RSS ceiling. Without this, undeclared jobs on a
        cgroup-enabled daemon have no memory cap at all -- because cgroup
        only enforces declared per-job caps and the watchdog was the
        only thing enforcing the host-percent ceiling."""
        wd = Watchdog(
            interval_seconds=0.0,
            max_rss_percent=90.0,
            host_total_mem_mb=1000,
            enforce_memory=False,  # cgroup is enforcing per-job; this is the v0.4 mode
        )
        spec = _spec_with(tmp_path)  # no mem_mb declared
        wd.register("wd-job-001", started_monotonic=time.monotonic() - 1.0)
        wd._states["wd-job-001"].last_cputime_seconds = 0.0
        wd._states["wd-job-001"].last_wall_monotonic = time.monotonic() - 1.0

        monkeypatch.setattr(watchdog, "read_rss_mb_pgid", lambda pid: 950)
        monkeypatch.setattr(watchdog, "read_cputime_seconds_pgid", lambda pid: 0.5)

        v = wd.evaluate("wd-job-001", pid=1, pgid=1, spec=spec)
        assert v.action == WatchdogAction.SIGTERM
        assert v.terminal_state == JobState.OOM_KILLED
        assert "host cap 90%" in v.reason

    def test_per_job_check_skipped_when_enforce_memory_off(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Conversely: with enforce_memory=False, an over-spec.mem_mb but
        under-host-percent RSS does NOT trigger a watchdog kill (the
        kernel cgroup will handle that case). Avoids double-killing."""
        wd = Watchdog(
            interval_seconds=0.0,
            max_rss_percent=90.0,
            host_total_mem_mb=10_000,  # 9 GB cap
            enforce_memory=False,
        )
        spec = _spec_with(tmp_path, mem_mb=100)  # tiny declared cap
        wd.register("wd-job-001", started_monotonic=time.monotonic() - 1.0)
        wd._states["wd-job-001"].last_cputime_seconds = 0.0
        wd._states["wd-job-001"].last_wall_monotonic = time.monotonic() - 1.0

        # RSS 500 MB: over spec.mem_mb (100) but under host cap (9000).
        # cgroup mode -> watchdog stays out of the per-job check.
        monkeypatch.setattr(watchdog, "read_rss_mb_pgid", lambda pid: 500)
        monkeypatch.setattr(watchdog, "read_cputime_seconds_pgid", lambda pid: 0.5)

        v = wd.evaluate("wd-job-001", pid=1, pgid=1, spec=spec)
        assert v.action == WatchdogAction.OK


class TestStarvation:
    """CPU < threshold for >= window seconds -> STARVED."""

    def test_below_threshold_starts_counter(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        wd = Watchdog(
            interval_seconds=0.0,
            starve_threshold_percent=5.0,
            starve_window_seconds=60.0,
        )
        spec = _spec_with(tmp_path)
        wd.register("wd-job-001", started_monotonic=time.monotonic() - 1.0)
        wd._states["wd-job-001"].last_cputime_seconds = 0.0
        wd._states["wd-job-001"].last_wall_monotonic = time.monotonic() - 1.0

        # 0.5% CPU -- well below 5% threshold
        monkeypatch.setattr(watchdog, "read_rss_mb_pgid", lambda pid: 100)
        monkeypatch.setattr(watchdog, "read_cputime_seconds_pgid", lambda pid: 0.005)

        v = wd.evaluate("wd-job-001", pid=1, pgid=1, spec=spec)
        # Counter just started, window not yet expired -> OK
        assert v.action == WatchdogAction.OK
        assert wd._states["wd-job-001"].starve_since_monotonic is not None

    def test_below_threshold_for_window_kills(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from vq import cgroup as cgroup_mod

        wd = Watchdog(
            interval_seconds=0.0,
            starve_threshold_percent=5.0,
            starve_window_seconds=60.0,
        )
        spec = _spec_with(tmp_path)
        wd.register("wd-job-001", started_monotonic=time.monotonic() - 200.0)
        wd._states["wd-job-001"].last_cputime_seconds = 0.0
        wd._states["wd-job-001"].last_wall_monotonic = time.monotonic() - 1.0
        # Pretend we've been at-or-below threshold for 90 seconds
        wd._states["wd-job-001"].starve_since_monotonic = time.monotonic() - 90.0

        monkeypatch.setattr(cgroup_mod, "cgroup_path_for_pid", lambda pid: None)
        monkeypatch.setattr(watchdog, "read_rss_mb_pgid", lambda pid: 100)
        monkeypatch.setattr(watchdog, "read_cputime_seconds_pgid", lambda pid: 0.005)

        v = wd.evaluate("wd-job-001", pid=1, pgid=1, spec=spec)
        assert v.action == WatchdogAction.SIGTERM
        assert v.terminal_state == JobState.STARVED
        assert "source=pgid" in v.reason

    def test_recovery_resets_counter(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        wd = Watchdog(
            interval_seconds=0.0,
            starve_threshold_percent=5.0,
            starve_window_seconds=60.0,
        )
        spec = _spec_with(tmp_path)
        wd.register("wd-job-001", started_monotonic=time.monotonic() - 5.0)
        wd._states["wd-job-001"].last_cputime_seconds = 0.0
        wd._states["wd-job-001"].last_wall_monotonic = time.monotonic() - 1.0
        wd._states["wd-job-001"].starve_since_monotonic = time.monotonic() - 30.0

        # Now CPU is 50%
        monkeypatch.setattr(watchdog, "read_rss_mb_pgid", lambda pid: 100)
        monkeypatch.setattr(watchdog, "read_cputime_seconds_pgid", lambda pid: 0.5)

        v = wd.evaluate("wd-job-001", pid=1, pgid=1, spec=spec)
        assert v.action == WatchdogAction.OK
        assert wd._states["wd-job-001"].starve_since_monotonic is None


class TestSampleStorage:
    """Each successful sample should append a JSON line to
    <workspace>/_vq/samples.jsonl."""

    def test_sample_lands_in_samples_jsonl(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from vq import cgroup as cgroup_mod

        wd = Watchdog(interval_seconds=0.0, host_total_mem_mb=10_000)
        spec = _spec_with(tmp_path)
        wd.register("wd-job-001", started_monotonic=time.monotonic() - 1.0)
        wd._states["wd-job-001"].last_cputime_seconds = 0.0
        wd._states["wd-job-001"].last_wall_monotonic = time.monotonic() - 1.0

        monkeypatch.setattr(cgroup_mod, "cgroup_path_for_pid", lambda pid: None)
        monkeypatch.setattr(watchdog, "read_rss_mb_pgid", lambda pid: 200)
        monkeypatch.setattr(watchdog, "read_cputime_seconds_pgid", lambda pid: 0.4)

        wd.evaluate("wd-job-001", pid=1, pgid=1, spec=spec)
        path = Path(spec.cwd) / "_vq" / "samples.jsonl"
        assert path.exists()
        line = path.read_text().strip().splitlines()[-1]
        record = json.loads(line)
        assert record["rss_mb"] == 200
        assert record["cpu_percent"] is not None
        assert record["cpu_time_source"] == "pgid"
        assert record["cpu_time_seconds"] == 0.4
        assert record["cgroup_lookup"] is None
        assert record["cgroup_path"] is None
        assert record["sample_pid"] == 1
        assert record["sample_pgid"] == 1
        assert "elapsed_seconds" in record
        assert "ts" in record


class TestProcReaders:
    """Self-test the /proc readers against real PIDs. Linux only -- macOS
    skips because /proc/<pid>/* doesn't exist."""

    @pytest.mark.skipif(not LINUX, reason="/proc only on Linux")
    def test_rss_for_self_pid_is_positive(self) -> None:
        rss = watchdog.read_rss_mb(os.getpid())
        assert rss is not None
        assert rss > 0

    @pytest.mark.skipif(not LINUX, reason="/proc only on Linux")
    def test_cputime_for_self_pid_is_nonnegative(self) -> None:
        ct = watchdog.read_cputime_seconds(os.getpid())
        assert ct is not None
        assert ct >= 0.0

    def test_rss_for_dead_pid_is_none(self) -> None:
        # PID 1 is init; PID 0 is the kernel; pick something unlikely to exist
        assert watchdog.read_rss_mb(2_147_483_646) is None

    def test_cputime_for_dead_pid_is_none(self) -> None:
        assert watchdog.read_cputime_seconds(2_147_483_646) is None


class TestKillpgHelper:
    def test_killpg_returns_false_for_dead_group(self) -> None:
        assert watchdog.killpg(2_147_483_646, watchdog.SIGTERM) is False


class TestPgidReaders:
    """v0.5.12 pgid-aware /proc readers. The motivation: v0.5.9's bash-wrap
    around every dispatched command makes ``/proc/<popen.pid>/stat`` show
    bash (sleeping in wait() at ~0% CPU, ~5MB RSS) instead of the inner
    crystal/python/orca. Aggregating over the pgroup recovers the
    meaningful number AND, as a side benefit, captures MPI ranks / OMP
    forks the old per-pid sampler missed."""

    def test_pgid_pids_aggregates_across_proc(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Mock /proc to contain three procs in pgrp 100 + one in pgrp 99.
        ``_pgid_pids(100)`` must return exactly the three matching pids."""
        # Lay out fake /proc entries; we'll redirect open()/listdir().
        fake_proc = {
            "111": "1111 (bash) S 1 100 100 0 -1 4194304 100 0 0 0 5 2 0 0 20 0 1 0",
            "112": "1112 (crystal) R 111 100 100 0 -1 4194304 5000 0 0 0 5000 200 0 0 20 0 1 0",
            "113": "1113 (python) S 111 100 100 0 -1 4194304 800 0 0 0 100 30 0 0 20 0 1 0",
            "200": "1200 (other) S 1 99 99 0 -1 4194304 50 0 0 0 1 1 0 0 20 0 1 0",
            "1": "1 (systemd) S 0 1 1 0 -1 4194304 100 0 0 0 1 1 0 0 20 0 1 0",
        }
        real_listdir = os.listdir
        def fake_listdir(p: str) -> list[str]:
            if p == "/proc":
                return list(fake_proc.keys()) + ["self", "stat", "cpuinfo"]
            return real_listdir(p)
        real_open = open
        def fake_open(path, *a, **k):  # type: ignore[no-untyped-def]
            if isinstance(path, str) and path.startswith("/proc/"):
                pid = path.split("/")[2]
                if pid in fake_proc and path == f"/proc/{pid}/stat":
                    import io
                    return io.StringIO(fake_proc[pid])
                raise FileNotFoundError(path)
            return real_open(path, *a, **k)
        monkeypatch.setattr(watchdog.os, "listdir", fake_listdir)
        monkeypatch.setattr("builtins.open", fake_open)

        matched = watchdog._pgid_pids(100)
        assert sorted(matched) == [111, 112, 113]

    def test_pgid_pids_empty_when_proc_unavailable(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """macOS dev box has no /proc; the function returns []."""
        def fake_listdir(p: str) -> list[str]:
            raise OSError("ENOENT /proc")
        monkeypatch.setattr(watchdog.os, "listdir", fake_listdir)
        assert watchdog._pgid_pids(123) == []

    def test_read_rss_mb_pgid_sums_across_pgroup(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Three procs in the pgroup, RSS values [5, 800, 2000] MB → 2805 MB.
        Exactly the bash-wrap scenario: bash 5MB + python 800MB + child
        2000MB; per-pid sampler would have seen only bash's 5MB."""
        monkeypatch.setattr(watchdog, "_pgid_pids", lambda pgid: [111, 112, 113])
        rss_by_pid = {111: 5, 112: 800, 113: 2000}
        monkeypatch.setattr(watchdog, "read_rss_mb", lambda pid: rss_by_pid.get(pid))
        assert watchdog.read_rss_mb_pgid(100) == 2805

    def test_read_rss_mb_pgid_none_when_no_pids(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(watchdog, "_pgid_pids", lambda pgid: [])
        assert watchdog.read_rss_mb_pgid(100) is None

    def test_read_rss_mb_pgid_tolerates_individual_read_failures(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """One pid disappears between _pgid_pids() and read_rss_mb(): the
        aggregate is built from the surviving readings, not failed entirely."""
        monkeypatch.setattr(watchdog, "_pgid_pids", lambda pgid: [111, 112, 113])
        monkeypatch.setattr(
            watchdog, "read_rss_mb",
            lambda pid: {111: 5, 112: None, 113: 2000}.get(pid),
        )
        # 5 + 2000 = 2005 (112's None is silently dropped)
        assert watchdog.read_rss_mb_pgid(100) == 2005

    def test_read_cputime_seconds_pgid_sums_across_pgroup(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(watchdog, "_pgid_pids", lambda pgid: [111, 112])
        monkeypatch.setattr(
            watchdog, "read_cputime_seconds",
            lambda pid: {111: 0.05, 112: 12.3}.get(pid),
        )
        assert watchdog.read_cputime_seconds_pgid(100) == pytest.approx(12.35)

    @pytest.mark.skipif(not LINUX, reason="/proc only on Linux")
    def test_pgid_pids_finds_self_in_own_pgroup(self) -> None:
        """End-to-end: this test process's pgid should contain at least
        this process. Verifies the real /proc parsing works against
        actual kernel data, not just mocks."""
        own_pgid = os.getpgid(os.getpid())
        pids = watchdog._pgid_pids(own_pgid)
        assert os.getpid() in pids

    def test_evaluate_uses_pgid_when_pgid_provided(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The bug-fix surface: evaluate() routes through *_pgid* readers
        when pgid is not None. Mock per-pid readers to return ABSURD
        kill-trigger values; mock pgid readers to return safe values.
        If the fix is in place, the verdict is OK (pgid readers used);
        if regressed, it'd be SIGTERM (per-pid readers used)."""
        from vq.spec import JobSpec, JobState
        from vq.watchdog import Watchdog, WatchdogAction

        spec = JobSpec(
            id="pgidtest0001", command=["python", "x.py"],
            cwd=str(tmp_path / "ws"), cpus=1, mem_mb=1000,
            state=JobState.RUNNING,
        )
        (tmp_path / "ws").mkdir()

        wd = Watchdog(interval_seconds=0.0, host_total_mem_mb=10_000)
        wd.register("pgidtest0001", started_monotonic=time.monotonic() - 1.0)
        wd._states["pgidtest0001"].last_cputime_seconds = 0.0
        wd._states["pgidtest0001"].last_wall_monotonic = time.monotonic() - 1.0

        # Per-pid readers say RSS=99999 MB (would trigger OOM_KILLED if used).
        monkeypatch.setattr(watchdog, "read_rss_mb", lambda pid: 99_999)
        monkeypatch.setattr(watchdog, "read_cputime_seconds", lambda pid: 0.5)
        # Pgid readers say RSS=500 MB (safe).
        monkeypatch.setattr(watchdog, "read_rss_mb_pgid", lambda pgid: 500)
        monkeypatch.setattr(watchdog, "read_cputime_seconds_pgid", lambda pgid: 0.5)

        v = wd.evaluate("pgidtest0001", pid=1, pgid=100, spec=spec)
        assert v.action == WatchdogAction.OK, (
            f"evaluate() with pgid=100 should route to pgid readers (500 MB, safe), "
            f"not per-pid readers (99999 MB, OOM); got verdict {v}"
        )

    def test_evaluate_falls_back_to_per_pid_when_pgid_none(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """If pgid is None (pre-v0.3 specs that lacked it, or macOS test
        scaffolding), evaluate() falls back to per-pid sampling so legacy
        behaviour is preserved."""
        from vq.spec import JobSpec, JobState
        from vq.watchdog import Watchdog, WatchdogAction

        spec = JobSpec(
            id="pgidtest0002", command=["python", "x.py"],
            cwd=str(tmp_path / "ws"), cpus=1, mem_mb=1000,
            state=JobState.RUNNING,
        )
        (tmp_path / "ws").mkdir()

        wd = Watchdog(interval_seconds=0.0, host_total_mem_mb=10_000)
        wd.register("pgidtest0002", started_monotonic=time.monotonic() - 1.0)
        wd._states["pgidtest0002"].last_cputime_seconds = 0.0
        wd._states["pgidtest0002"].last_wall_monotonic = time.monotonic() - 1.0

        # Per-pid says safe; pgid says kill. With pgid=None, per-pid wins.
        monkeypatch.setattr(watchdog, "read_rss_mb", lambda pid: 200)
        monkeypatch.setattr(watchdog, "read_cputime_seconds", lambda pid: 0.5)
        monkeypatch.setattr(watchdog, "read_rss_mb_pgid", lambda pgid: 99_999)
        monkeypatch.setattr(watchdog, "read_cputime_seconds_pgid", lambda pgid: 0.5)

        v = wd.evaluate("pgidtest0002", pid=1, pgid=None, spec=spec)
        assert v.action == WatchdogAction.OK


# ----------------------------------------------------------------------
# v0.5.38: cgroup-v2 sampler preferred over pgid-walk when available.
# The setsid-escape regression (the operator's report 2026-05-16): a build like
# `pip install -e .` lets ninja spawn cc1plus in its own session via
# setsid/PR_SET_PGID, escaping the parent pgid. The pgid-walk sees ~0%
# CPU for the whole build → STARVED kill. cgroup readers aggregate
# across the cgroup membership regardless of pgid/session, so this can
# never happen when cgroup-v2 is in play.
# ----------------------------------------------------------------------


class TestCgroupPreferredSampling:
    def test_scope_cgroup_path_preferred_over_popen_pid_path(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Sample the named job scope before falling back to Popen's cgroup.

        ``systemd-run --scope`` can leave the local Popen handle attached to
        a quiet wrapper/waiter while the payload burns CPU in the named
        ``vq-job-<id>.scope``. The starvation heuristic must use the scope's
        ControlGroup, not the wrapper PID's cgroup.
        """
        from vq import cgroup as cgroup_mod
        from vq.spec import JobSpec, JobState
        from vq.watchdog import Watchdog, WatchdogAction

        scope_path = "/sys/fs/cgroup/user.slice/vq-job-scope_pref04.scope"
        wrapper_path = "/sys/fs/cgroup/user.slice/app-vq-daemon.scope"

        spec = JobSpec(
            id="scope_pref04",
            command=["bash", "run.sh"],
            cwd=str(tmp_path / "ws"),
            cpus=4,
            mem_mb=8000,
            state=JobState.RUNNING,
        )
        (tmp_path / "ws").mkdir()

        wd = Watchdog(
            interval_seconds=0.0,
            host_total_mem_mb=64_000,
            starve_threshold_percent=5.0,
            starve_window_seconds=60.0,
        )
        wd.register("scope_pref04", started_monotonic=time.monotonic() - 200.0)
        st = wd._states["scope_pref04"]
        st.last_cputime_seconds = 0.0
        st.last_wall_monotonic = time.monotonic() - 1.0
        st.starve_since_monotonic = time.monotonic() - 90.0

        monkeypatch.setattr(
            cgroup_mod,
            "cgroup_path_for_scope",
            lambda unit, *, multi_user=False: scope_path,
        )
        monkeypatch.setattr(
            cgroup_mod, "cgroup_path_for_pid", lambda pid: wrapper_path
        )
        monkeypatch.setattr(
            cgroup_mod,
            "read_cpu_usage_seconds",
            lambda path: 30.0 if path == scope_path else 0.0,
        )
        monkeypatch.setattr(
            cgroup_mod,
            "read_memory_current_mb",
            lambda path: 4000 if path == scope_path else 100,
        )
        monkeypatch.setattr(watchdog, "read_rss_mb_pgid", lambda pgid: 99_999)
        monkeypatch.setattr(watchdog, "read_cputime_seconds_pgid", lambda pgid: 0.0)

        v = wd.evaluate(
            "scope_pref04",
            pid=123,
            pgid=123,
            spec=spec,
            cgroup_unit_name="vq-job-scope_pref04",
        )

        assert v.action == WatchdogAction.OK
        assert st.starve_since_monotonic is None
        sample = _read_samples(tmp_path / "ws")[-1]
        assert sample["cgroup_lookup"] == "scope"
        assert sample["cgroup_path"] == scope_path
        assert sample["cpu_time_source"] == "cgroup"

    def test_cgroup_path_present_prefers_cgroup_readers(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """v0.5.38: when cgroup.cgroup_path_for_pid returns a path,
        watchdog reads usage_usec / memory.current — NOT the pgid walk.
        Test by making the pgid readers return absurd kill-trigger values
        and the cgroup readers return safe values; verdict must be OK."""
        from vq import cgroup as cgroup_mod
        from vq.spec import JobSpec, JobState
        from vq.watchdog import Watchdog, WatchdogAction

        spec = JobSpec(
            id="cgroup_pref01", command=["bash", "build.sh"],
            cwd=str(tmp_path / "ws"), cpus=8, mem_mb=8000,
            state=JobState.RUNNING,
        )
        (tmp_path / "ws").mkdir()

        wd = Watchdog(interval_seconds=0.0, host_total_mem_mb=64_000)
        wd.register("cgroup_pref01", started_monotonic=time.monotonic() - 1.0)
        wd._states["cgroup_pref01"].last_cputime_seconds = 0.0
        wd._states["cgroup_pref01"].last_wall_monotonic = time.monotonic() - 1.0

        # cgroup readers: this is the build phase; ninja's cc1plus's are
        # in their own sessions but still in the cgroup. cgroup sees
        # plenty of CPU + reasonable RSS.
        monkeypatch.setattr(
            cgroup_mod, "cgroup_path_for_pid",
            lambda pid: "/sys/fs/cgroup/.../vq-job-cgroup_pref01.scope",
        )
        monkeypatch.setattr(
            cgroup_mod, "read_cpu_usage_seconds",
            lambda p: 30.0,   # ~ 30 CPU-seconds in last sample
        )
        monkeypatch.setattr(
            cgroup_mod, "read_memory_current_mb",
            lambda p: 4000,   # 4 GB, well under the 8 GB cap
        )

        # pgid walk: setsid-escaped descendants are INVISIBLE.
        # If the watchdog ignored cgroup and used pgid walk, the 0 CPU
        # delta would push CPU% to 0 and the starve heuristic would
        # eventually fire. Per-pid kill-trigger values too — if it
        # somehow used those instead, the test would fail loudly.
        monkeypatch.setattr(watchdog, "read_rss_mb_pgid", lambda pgid: 99_999)
        monkeypatch.setattr(watchdog, "read_cputime_seconds_pgid", lambda pgid: 0.0)
        monkeypatch.setattr(watchdog, "read_rss_mb", lambda pid: 99_999)
        monkeypatch.setattr(watchdog, "read_cputime_seconds", lambda pid: 0.0)

        v = wd.evaluate("cgroup_pref01", pid=1, pgid=100, spec=spec)
        assert v.action == WatchdogAction.OK, (
            "cgroup-aware path should have used cgroup readers "
            "(4 GB RSS, 30 CPU-s) — instead got verdict "
            f"{v.action} which suggests it used the pgid walk "
            "(99999 MB / 0 CPU) and would mistakenly kill the job"
        )

    def test_cgroup_path_none_falls_back_to_pgid_walk(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """If cgroup_path_for_pid returns None (macOS dev box, Linux
        without cgroup-v2 delegation, or a transient /proc miss), the
        watchdog falls back to the v0.5.12 pgid walk so legacy
        behaviour is preserved."""
        from vq import cgroup as cgroup_mod
        from vq.spec import JobSpec, JobState
        from vq.watchdog import Watchdog, WatchdogAction

        spec = JobSpec(
            id="cgroup_fall02", command=["python", "x.py"],
            cwd=str(tmp_path / "ws"), cpus=1, mem_mb=1000,
            state=JobState.RUNNING,
        )
        (tmp_path / "ws").mkdir()

        wd = Watchdog(interval_seconds=0.0, host_total_mem_mb=10_000)
        wd.register("cgroup_fall02", started_monotonic=time.monotonic() - 1.0)
        wd._states["cgroup_fall02"].last_cputime_seconds = 0.0
        wd._states["cgroup_fall02"].last_wall_monotonic = time.monotonic() - 1.0

        # cgroup unavailable: return None → fallback engages.
        monkeypatch.setattr(cgroup_mod, "cgroup_path_for_pid", lambda pid: None)
        # pgid walk says safe. If fallback works, verdict is OK.
        monkeypatch.setattr(watchdog, "read_rss_mb_pgid", lambda pgid: 500)
        monkeypatch.setattr(watchdog, "read_cputime_seconds_pgid", lambda pgid: 0.5)
        # Per-pid says kill — must NOT be used since pgid is provided.
        monkeypatch.setattr(watchdog, "read_rss_mb", lambda pid: 99_999)
        monkeypatch.setattr(watchdog, "read_cputime_seconds", lambda pid: 0.0)

        v = wd.evaluate("cgroup_fall02", pid=1, pgid=100, spec=spec)
        assert v.action == WatchdogAction.OK

    def test_partial_cgroup_data_fills_remainder_from_pgid_walk(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Defensive: if cgroup_path_for_pid succeeds but one of the
        sub-files is missing (e.g. memory.current absent on a host with
        only cpu controller delegated), we want to use the cgroup's CPU
        value AND fall back to pgid walk for RSS — not abandon either."""
        from vq import cgroup as cgroup_mod
        from vq.spec import JobSpec, JobState
        from vq.watchdog import Watchdog, WatchdogAction

        spec = JobSpec(
            id="cgroup_part03", command=["bash", "build.sh"],
            cwd=str(tmp_path / "ws"), cpus=8, mem_mb=8000,
            state=JobState.RUNNING,
        )
        (tmp_path / "ws").mkdir()

        wd = Watchdog(interval_seconds=0.0, host_total_mem_mb=64_000)
        wd.register("cgroup_part03", started_monotonic=time.monotonic() - 1.0)
        wd._states["cgroup_part03"].last_cputime_seconds = 0.0
        wd._states["cgroup_part03"].last_wall_monotonic = time.monotonic() - 1.0

        monkeypatch.setattr(
            cgroup_mod, "cgroup_path_for_pid",
            lambda pid: "/sys/fs/cgroup/.../scope",
        )
        # cgroup has CPU but not memory.
        monkeypatch.setattr(cgroup_mod, "read_cpu_usage_seconds", lambda p: 30.0)
        monkeypatch.setattr(cgroup_mod, "read_memory_current_mb", lambda p: None)
        # pgid fills memory in.
        monkeypatch.setattr(watchdog, "read_rss_mb_pgid", lambda pgid: 2000)
        monkeypatch.setattr(watchdog, "read_cputime_seconds_pgid", lambda pgid: 0.0)
        monkeypatch.setattr(watchdog, "read_rss_mb", lambda pid: 99_999)
        monkeypatch.setattr(watchdog, "read_cputime_seconds", lambda pid: 0.0)

        v = wd.evaluate("cgroup_part03", pid=1, pgid=100, spec=spec)
        assert v.action == WatchdogAction.OK


# ----------------------------------------------------------------------
# v0.5.48 Bug B regression: SUSPENDED -> RUNNING transition resets
# sampling state so a paused-then-resumed job isn't STARVED-killed
# at the first post-resume sample.
# ----------------------------------------------------------------------


class TestSuspendedToRunningTransition:
    """v0.5.48 (Bug B): Watchdog.reset_sampling_for_resume was defined
    but never invoked from anywhere — pausing a CPU-bound job for
    longer than starve_window_seconds and then resuming would
    STARVED-kill it because (a) the first post-resume sample
    computes CPU% against pre-pause cputime over a wall-window that
    includes the entire pause, yielding ~0% CPU, and (b)
    starve_since_monotonic was set before the pause and was never
    cleared. The fix is in Watchdog.evaluate's entry: detect the
    SUSPENDED -> RUNNING transition via st.last_observed_spec_state
    and call reset_sampling_for_resume."""

    def test_transition_clears_sampling_state(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The bug-prevention property: a starve counter set
        pre-pause is cleared by the SUSPENDED -> RUNNING transition,
        so the first post-resume sample cannot fire STARVED on a
        stale counter."""
        # Mock the sample readers so the post-transition sample
        # takes the "both None" early-return path — keeps the test
        # focused on the transition-detection behavior rather than
        # sample-reader mechanics.
        monkeypatch.setattr(watchdog, "read_rss_mb_pgid", lambda pgid: None)
        monkeypatch.setattr(watchdog, "read_cputime_seconds_pgid", lambda pgid: None)
        monkeypatch.setattr(watchdog, "read_rss_mb", lambda pid: None)
        monkeypatch.setattr(watchdog, "read_cputime_seconds", lambda pid: None)

        wd = Watchdog(interval_seconds=5.0)
        spec = _spec_with(tmp_path, state=JobState.RUNNING)
        wd.register("wd-job-001", started_monotonic=time.monotonic() - 10)
        # Prime per-job sampling state to look "running, with starve
        # counter ticking" before the pause.
        st = wd._states["wd-job-001"]
        st.last_cputime_seconds = 42.0
        st.last_wall_monotonic = time.monotonic() - 1.0
        st.starve_since_monotonic = time.monotonic() - 100.0

        # Tick once with spec SUSPENDED — evaluate short-circuits to
        # OK and stamps last_observed_spec_state=SUSPENDED.
        spec.state = JobState.SUSPENDED
        v = wd.evaluate("wd-job-001", pid=1, pgid=1, spec=spec)
        assert v.action == WatchdogAction.OK
        assert st.last_observed_spec_state == JobState.SUSPENDED
        # Pre-pause counter values still present (reset only fires on
        # the RUNNING transition).
        assert st.starve_since_monotonic is not None

        # Tick again with spec RUNNING — transition detected; reset
        # fires; the stale starve counter is cleared.
        spec.state = JobState.RUNNING
        v = wd.evaluate("wd-job-001", pid=1, pgid=1, spec=spec)
        assert v.action == WatchdogAction.OK
        # The bug-prevention property: starve counter cleared, so no
        # STARVED kill can fire on the next ticks until a fresh
        # starve window accumulates.
        assert st.starve_since_monotonic is None
        # We've recorded the new spec state for the next tick.
        assert st.last_observed_spec_state == JobState.RUNNING

    def test_no_reset_when_state_unchanged(self, tmp_path: Path) -> None:
        """Two consecutive RUNNING-state evaluate() calls must NOT
        reset the sampling baselines — the reset is only for the
        transition. (Otherwise CPU% deltas would always be zero.)"""
        wd = Watchdog(interval_seconds=5.0)
        spec = _spec_with(tmp_path, state=JobState.RUNNING)
        wd.register("wd-job-001", started_monotonic=time.monotonic() - 10)
        st = wd._states["wd-job-001"]
        st.last_cputime_seconds = 42.0
        st.last_wall_monotonic = time.monotonic() - 1.0

        # First evaluate — records last_observed=RUNNING.
        wd.evaluate("wd-job-001", pid=1, pgid=1, spec=spec)
        # Sampling state should not have been reset (it may have been
        # mutated by a real sample firing, but specifically
        # last_cputime_seconds should not be None).
        # Second evaluate — same state, no transition, no reset.
        st.last_cputime_seconds = 99.0  # something a real sample would set
        wd.evaluate("wd-job-001", pid=1, pgid=1, spec=spec)
        assert st.last_cputime_seconds == 99.0  # untouched by transition logic

    def test_no_reset_on_first_sight_with_running_state(
        self, tmp_path: Path
    ) -> None:
        """The first evaluate() ever called for a job (no prior
        last_observed_spec_state) must NOT trigger a reset — there
        was no prior SUSPENDED to transition from."""
        wd = Watchdog(interval_seconds=5.0)
        spec = _spec_with(tmp_path, state=JobState.RUNNING)
        # First sight; evaluate auto-registers.
        wd.evaluate("wd-job-001", pid=1, pgid=1, spec=spec)
        st = wd._states["wd-job-001"]
        # No reset happened — reset_sampling_for_resume would set
        # last_sample_monotonic=0.0 explicitly, but the freshly-
        # registered state already has that as the default. So check
        # the more specific signal: last_observed was recorded.
        assert st.last_observed_spec_state == JobState.RUNNING


# ----------------------------------------------------------------------
# v0.5.48 Bug A regression: admin.update_env's marker-clear ordering.
# The actual ordering test lives in test_admin.py — this file owns
# watchdog-only regressions.
# ----------------------------------------------------------------------


@pytest.mark.no_autopatch_host_pressure  # #563: these exercise the real reader
class TestReadHostMemoryPressurePct:
    """v0.6.20: pure-function /proc/meminfo parser."""

    def test_returns_pct_when_meminfo_readable(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """MemTotal=131072 KB, MemAvailable=65536 KB → 50% pressure."""
        # We can't mock /proc/meminfo directly; instead patch builtins.open
        # to redirect that one path. Easier: write a fake meminfo file
        # and monkeypatch the open() call site.
        fake = tmp_path / "meminfo"
        fake.write_text(
            "MemTotal:       131072 kB\n"
            "MemFree:         10000 kB\n"
            "MemAvailable:    65536 kB\n"
            "Buffers:          1000 kB\n"
        )
        # Re-bind `open` inside the watchdog module to redirect
        # /proc/meminfo reads to our fake. Less invasive: override the
        # entire function with a wrapper that opens fake first.
        real_open = open

        def fake_open(path, *args, **kwargs):  # type: ignore[no-untyped-def]
            if path == "/proc/meminfo":
                return real_open(fake, *args, **kwargs)
            return real_open(path, *args, **kwargs)

        monkeypatch.setattr("builtins.open", fake_open)
        result = watchdog.read_host_memory_pressure_pct()
        assert result is not None
        assert 49.0 < result < 51.0  # ~50%

    def test_returns_none_when_meminfo_missing(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def fake_open(path, *args, **kwargs):  # type: ignore[no-untyped-def]
            raise FileNotFoundError(path)

        monkeypatch.setattr("builtins.open", fake_open)
        assert watchdog.read_host_memory_pressure_pct() is None

    def test_returns_none_when_meminfo_missing_field(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """MemTotal present but MemAvailable absent → None (we can't
        compute pressure without both)."""
        fake = tmp_path / "meminfo"
        fake.write_text("MemTotal: 131072 kB\n")
        real_open = open

        def fake_open(path, *args, **kwargs):  # type: ignore[no-untyped-def]
            if path == "/proc/meminfo":
                return real_open(fake, *args, **kwargs)
            return real_open(path, *args, **kwargs)

        monkeypatch.setattr("builtins.open", fake_open)
        assert watchdog.read_host_memory_pressure_pct() is None

    def test_returns_none_on_garbled_meminfo(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Best-effort: parse errors return None, not raise."""
        fake = tmp_path / "meminfo"
        fake.write_text("MemTotal: this is not a number\nMemAvailable: also no\n")
        real_open = open

        def fake_open(path, *args, **kwargs):  # type: ignore[no-untyped-def]
            if path == "/proc/meminfo":
                return real_open(fake, *args, **kwargs)
            return real_open(path, *args, **kwargs)

        monkeypatch.setattr("builtins.open", fake_open)
        assert watchdog.read_host_memory_pressure_pct() is None


class TestCheckHostPressure:
    """v0.6.20: hysteresis state machine for the host-pressure pause /
    resume decision. All pressure values injected via _pressure_reader
    so the tests are deterministic regardless of host.
    """

    def test_low_pressure_returns_noop(self) -> None:
        wd = watchdog.Watchdog(
            host_pressure_pause_pct=85.0, host_pressure_resume_pct=70.0,
        )
        verdict = wd.check_host_pressure(
            ["jobA", "jobB"], _pressure_reader=lambda: 30.0
        )
        assert verdict.action == watchdog.HostPressureAction.NO_OP
        assert verdict.pressure_pct == 30.0
        assert wd._host_pressure_active is False

    def test_high_pressure_with_running_jobs_pauses(self) -> None:
        wd = watchdog.Watchdog(
            host_pressure_pause_pct=85.0, host_pressure_resume_pct=70.0,
        )
        verdict = wd.check_host_pressure(
            ["jobA", "jobB"], _pressure_reader=lambda: 90.0
        )
        assert verdict.action == watchdog.HostPressureAction.PAUSE
        assert set(verdict.jobids) == {"jobA", "jobB"}
        assert wd._host_pressure_active is True
        assert wd._host_pressure_paused_jobids == {"jobA", "jobB"}

    def test_high_pressure_no_running_jobs_noop(self) -> None:
        """Pressure is high but nothing to pause — don't enter the
        active state (would never resume otherwise)."""
        wd = watchdog.Watchdog(
            host_pressure_pause_pct=85.0, host_pressure_resume_pct=70.0,
        )
        verdict = wd.check_host_pressure([], _pressure_reader=lambda: 95.0)
        assert verdict.action == watchdog.HostPressureAction.NO_OP
        assert wd._host_pressure_active is False

    def test_pause_already_active_stays_paused_under_pressure(self) -> None:
        wd = watchdog.Watchdog(
            host_pressure_pause_pct=85.0, host_pressure_resume_pct=70.0,
        )
        wd._host_pressure_active = True
        wd._host_pressure_paused_jobids = {"jobA"}
        # Pressure still high — stay paused, no second PAUSE verdict
        verdict = wd.check_host_pressure(
            ["jobA"], _pressure_reader=lambda: 88.0
        )
        assert verdict.action == watchdog.HostPressureAction.NO_OP
        assert wd._host_pressure_active is True

    def test_pressure_drops_below_resume_threshold_resumes(self) -> None:
        wd = watchdog.Watchdog(
            host_pressure_pause_pct=85.0, host_pressure_resume_pct=70.0,
        )
        wd._host_pressure_active = True
        wd._host_pressure_paused_jobids = {"jobA", "jobB"}
        verdict = wd.check_host_pressure(
            ["jobA", "jobB"], _pressure_reader=lambda: 60.0
        )
        assert verdict.action == watchdog.HostPressureAction.RESUME
        assert set(verdict.jobids) == {"jobA", "jobB"}
        assert wd._host_pressure_active is False
        # Set was cleared so a future PAUSE starts fresh
        assert wd._host_pressure_paused_jobids == set()

    def test_hysteresis_no_flap_between_thresholds(self) -> None:
        """Pressure between resume (70) and pause (85) does NOT
        trigger anything in either direction — hysteresis property.
        This is the test that proves we don't flap."""
        wd = watchdog.Watchdog(
            host_pressure_pause_pct=85.0, host_pressure_resume_pct=70.0,
        )
        # Currently inactive, mid-range pressure → NO_OP
        verdict = wd.check_host_pressure(
            ["jobA"], _pressure_reader=lambda: 78.0
        )
        assert verdict.action == watchdog.HostPressureAction.NO_OP
        # Cross pause threshold → PAUSE
        verdict = wd.check_host_pressure(
            ["jobA"], _pressure_reader=lambda: 86.0
        )
        assert verdict.action == watchdog.HostPressureAction.PAUSE
        # Drop back into mid-range → stay paused (would be a flap otherwise)
        verdict = wd.check_host_pressure(
            ["jobA"], _pressure_reader=lambda: 78.0
        )
        assert verdict.action == watchdog.HostPressureAction.NO_OP
        assert wd._host_pressure_active is True
        # Cross below resume threshold → RESUME
        verdict = wd.check_host_pressure(
            ["jobA"], _pressure_reader=lambda: 65.0
        )
        assert verdict.action == watchdog.HostPressureAction.RESUME

    def test_pressure_reader_none_is_noop(self) -> None:
        """macOS / no /proc / parse failure → NO_OP rather than
        raise or default to a wrong assumption."""
        wd = watchdog.Watchdog()
        verdict = wd.check_host_pressure(
            ["jobA"], _pressure_reader=lambda: None
        )
        assert verdict.action == watchdog.HostPressureAction.NO_OP
        assert verdict.pressure_pct is None

    def test_disabled_via_config(self) -> None:
        wd = watchdog.Watchdog(enforce_host_pressure_pause=False)
        verdict = wd.check_host_pressure(
            ["jobA"], _pressure_reader=lambda: 99.9
        )
        assert verdict.action == watchdog.HostPressureAction.NO_OP

    def test_resume_only_targets_jobs_we_paused(self) -> None:
        """If the operator added a NEW running job between the
        PAUSE and the RESUME, the RESUME's jobids list should NOT
        include it — we only resume what WE paused. (The new job
        was never paused by us; nothing to resume.)"""
        wd = watchdog.Watchdog(
            host_pressure_pause_pct=85.0, host_pressure_resume_pct=70.0,
        )
        # First call: pressure spikes, we pause [jobA]
        wd.check_host_pressure(["jobA"], _pressure_reader=lambda: 90.0)
        assert wd._host_pressure_paused_jobids == {"jobA"}
        # Pressure drops, we resume — running set now has jobB too
        # (e.g. dispatched while paused — shouldn't really happen
        # since pause-active gates dispatch, but defensively test
        # that we don't accidentally "resume" jobB).
        verdict = wd.check_host_pressure(
            ["jobA", "jobB"], _pressure_reader=lambda: 60.0
        )
        assert verdict.action == watchdog.HostPressureAction.RESUME
        assert set(verdict.jobids) == {"jobA"}
        assert "jobB" not in verdict.jobids


def _read_samples(workspace: Path) -> list[dict]:
    p = workspace / "_vq" / "samples.jsonl"
    return [json.loads(ln) for ln in p.read_text().splitlines() if ln.strip()]


class TestCpuPercentSourceGuard:
    """WD-1: a CPU% delta is only meaningful between two samples taken from
    the SAME cputime reader, with a non-negative delta. A negative delta (a
    child exited and left the pgid/cgroup aggregate) or a cgroup↔pgid↔pid
    source switch must be discarded + re-baselined, not logged as a bogus
    (often negative, sometimes huge) cpu_percent / tripped as STARVED."""

    def test_negative_delta_is_skipped_not_logged_negative(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        wd = Watchdog(interval_seconds=0.0)  # sample every evaluate
        spec = _spec_with(tmp_path)
        wd.register("wd-job-001", started_monotonic=time.monotonic())
        monkeypatch.setattr(watchdog, "read_rss_mb_pgid", lambda pid: 100)
        cputimes = iter([10.0, 20.0, 15.0])  # rises, then DROPS (child exit)
        monkeypatch.setattr(
            watchdog, "read_cputime_seconds_pgid", lambda pid: next(cputimes)
        )
        for _ in range(3):
            wd.evaluate("wd-job-001", pid=1, pgid=1, spec=spec)
        samples = _read_samples(tmp_path / "ws")
        assert samples[0]["cpu_percent"] is None  # first sample: no baseline
        assert samples[1]["cpu_percent"] is not None  # +10 -> a real reading
        assert samples[2]["cpu_percent"] is None  # -5 -> WD-1 discards it

    def test_source_switch_re_baselines(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from vq import cgroup

        wd = Watchdog(interval_seconds=0.0)
        spec = _spec_with(tmp_path)
        wd.register("wd-job-001", started_monotonic=time.monotonic())
        # sample 1 reads cputime from the cgroup (10s); sample 2 falls back to
        # the pgid walk (100s). Raw delta is +90, but the baselines differ —
        # WD-1 must discard it rather than log a huge cpu%.
        cg_paths = iter(["/sys/fs/cgroup/vq-job.scope", None])
        monkeypatch.setattr(cgroup, "cgroup_path_for_pid", lambda pid: next(cg_paths))
        monkeypatch.setattr(cgroup, "read_cpu_usage_seconds", lambda p: 10.0)
        monkeypatch.setattr(cgroup, "read_memory_current_mb", lambda p: 100)
        monkeypatch.setattr(watchdog, "read_rss_mb_pgid", lambda pid: 100)
        monkeypatch.setattr(watchdog, "read_cputime_seconds_pgid", lambda pid: 100.0)
        wd.evaluate("wd-job-001", pid=1, pgid=1, spec=spec)  # cgroup source
        wd.evaluate("wd-job-001", pid=1, pgid=1, spec=spec)  # pgid source
        samples = _read_samples(tmp_path / "ws")
        assert samples[1]["cpu_percent"] is None
