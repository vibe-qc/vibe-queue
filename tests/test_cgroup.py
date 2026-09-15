"""Tests for vq.cgroup: command-wrapping logic + availability detection.

Most tests force ``available()`` via monkeypatch so the unit tests don't
depend on the test machine actually having systemd delegation set up.
The "real" probe is exercised by an opt-in test that runs only on Linux
hosts where systemd-run is present.
"""
from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from vq import cgroup


@pytest.fixture(autouse=True)
def _reset_cache() -> None:
    """Each test starts with a fresh availability cache."""
    cgroup.reset_availability_cache()
    yield
    cgroup.reset_availability_cache()


class TestAvailable:
    def test_no_systemd_run_means_unavailable(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(cgroup, "_systemd_run_path", lambda: None)
        assert cgroup.available() is False

    def test_systemd_run_failing_probe_means_unavailable(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(cgroup, "_systemd_run_path", lambda: "/usr/bin/systemd-run")

        def fake_run(*a: Any, **kw: Any) -> subprocess.CompletedProcess[str]:
            return subprocess.CompletedProcess(
                args=a[0], returncode=1, stdout="",
                stderr="Failed to set unit properties: ...\n",
            )

        monkeypatch.setattr(cgroup.subprocess, "run", fake_run)
        assert cgroup.available() is False

    def test_systemd_run_passing_probe_means_available(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(cgroup, "_systemd_run_path", lambda: "/usr/bin/systemd-run")

        def fake_run(*a: Any, **kw: Any) -> subprocess.CompletedProcess[str]:
            return subprocess.CompletedProcess(
                args=a[0], returncode=0, stdout="", stderr=""
            )

        monkeypatch.setattr(cgroup.subprocess, "run", fake_run)
        assert cgroup.available() is True

    def test_oserror_during_probe_swallowed(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(cgroup, "_systemd_run_path", lambda: "/usr/bin/systemd-run")

        def boom(*a: Any, **kw: Any) -> Any:
            raise OSError("ENOENT")

        monkeypatch.setattr(cgroup.subprocess, "run", boom)
        assert cgroup.available() is False

    def test_timeout_during_probe_swallowed(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(cgroup, "_systemd_run_path", lambda: "/usr/bin/systemd-run")

        def stuck(*a: Any, **kw: Any) -> Any:
            raise subprocess.TimeoutExpired(cmd=a[0], timeout=5.0)

        monkeypatch.setattr(cgroup.subprocess, "run", stuck)
        assert cgroup.available() is False


class TestWrapCommand:
    @pytest.fixture(autouse=True)
    def _force_available(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Most wrap_command tests assume cgroup is available; we test the
        unavailable / no-cap fallback paths separately."""
        monkeypatch.setattr(cgroup, "_systemd_run_path", lambda: "/usr/bin/systemd-run")
        monkeypatch.setattr(
            cgroup.subprocess, "run",
            lambda *a, **kw: subprocess.CompletedProcess(
                args=a[0], returncode=0, stdout="", stderr=""
            ),
        )

    def test_no_caps_no_unit_returns_command_unchanged(self) -> None:
        """Only a capless *and* nameless command (not a job) runs raw.
        A capless job carries a ``unit_name`` and IS scoped (see
        ``test_unit_name_without_caps_still_scopes``)."""
        cmd = ["python", "run.py"]
        assert cgroup.wrap_command(cmd) is cmd
        assert cgroup.wrap_command(cmd, mem_mb=None, cpus=None) is cmd
        assert cgroup.wrap_command(cmd, unit_name=None) is cmd

    def test_mem_mb_only_emits_memorymax_and_memoryhigh(self) -> None:
        wrapped = cgroup.wrap_command(["true"], mem_mb=16000)
        assert wrapped[0] == "/usr/bin/systemd-run"
        # Find the property args we care about
        assert "--property=MemoryMax=16000M" in wrapped
        # MemoryHigh = 90% of MemoryMax
        assert "--property=MemoryHigh=14400M" in wrapped
        assert "--" in wrapped
        # The original command lives after the "--"
        sep = wrapped.index("--")
        assert wrapped[sep + 1 :] == ["true"]

    def test_cpus_only_emits_cpuquota(self) -> None:
        wrapped = cgroup.wrap_command(["true"], cpus=8)
        assert "--property=CPUQuota=800%" in wrapped

    def test_mem_and_cpus_combined(self) -> None:
        wrapped = cgroup.wrap_command(
            ["python", "x.py", "--flag"],
            mem_mb=8000, cpus=4,
        )
        assert "--property=MemoryMax=8000M" in wrapped
        assert "--property=MemoryHigh=7200M" in wrapped
        assert "--property=CPUQuota=400%" in wrapped
        # v0.5.8: RuntimeMaxSec is no longer emitted -- the Python
        # watchdog owns wall-time enforcement (pause-aware).
        assert not any(
            arg.startswith("--property=RuntimeMaxSec") for arg in wrapped
        )
        sep = wrapped.index("--")
        assert wrapped[sep + 1 :] == ["python", "x.py", "--flag"]

    def test_unit_name_passes_through(self) -> None:
        wrapped = cgroup.wrap_command(
            ["true"], mem_mb=1000, unit_name="vq-job-abc123"
        )
        assert "--unit" in wrapped
        unit_idx = wrapped.index("--unit")
        assert wrapped[unit_idx + 1] == "vq-job-abc123"

    def test_unit_name_without_caps_still_scopes(self) -> None:
        """A job (identified by ``unit_name``) is wrapped in an
        accounting-only scope even with no MemoryMax/CPUQuota, so the
        kernel cgroup captures setsid-escaped descendants the pgid walk
        misses and the daemon can reap the unit. Pre-fix a capless job
        ran unwrapped under pgid-only accounting."""
        wrapped = cgroup.wrap_command(["true"], unit_name="vq-job-xyz")
        assert wrapped[0] == "/usr/bin/systemd-run"  # scoped, not raw
        assert "--scope" in wrapped
        assert wrapped[wrapped.index("--unit") + 1] == "vq-job-xyz"
        # No caps -> no resource-limit properties on the scope.
        assert not any(a.startswith("--property=MemoryMax") for a in wrapped)
        assert not any(a.startswith("--property=CPUQuota") for a in wrapped)
        # Original command still lives after the "--".
        sep = wrapped.index("--")
        assert wrapped[sep + 1 :] == ["true"]

    def test_user_scope_collect_quiet_always_present(self) -> None:
        wrapped = cgroup.wrap_command(["true"], mem_mb=1000)
        assert "--user" in wrapped
        assert "--scope" in wrapped
        assert "--quiet" in wrapped
        assert "--collect" in wrapped


class TestWrapCommandWhenUnavailable:
    def test_unavailable_returns_command_unchanged_even_with_caps(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # No systemd-run on PATH -> wrap is a no-op even though caps are set
        monkeypatch.setattr(cgroup, "_systemd_run_path", lambda: None)
        cmd = ["python", "run.py"]
        assert cgroup.wrap_command(cmd, mem_mb=16000, cpus=4) is cmd


@pytest.mark.skipif(
    not sys.platform.startswith("linux") or shutil.which("systemd-run") is None,
    reason="real cgroup probe requires Linux + systemd-run on PATH",
)
class TestRealProbe:
    """Opt-in: actually exercise the systemd-run probe on a Linux host.
    The result is "True if delegation is configured, False if not" --
    we don't assert which because either is a valid host configuration.
    The point is that the probe doesn't blow up."""

    def test_real_probe_does_not_raise(self) -> None:
        result = cgroup.available()
        assert isinstance(result, bool)


class TestSetCpuWeight:
    """v0.5.13 set_cpu_weight helper (used by `vq throttle`). Most cases
    are unit-level mocks of subprocess; the no-cgroup-available branch
    short-circuits without calling systemctl at all."""

    def test_no_cgroup_returns_false_without_running_systemctl(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(cgroup, "_systemd_run_path", lambda: None)
        cgroup.reset_availability_cache()
        called: list[object] = []
        def fake_run(*a, **kw):  # type: ignore[no-untyped-def]
            called.append(a)
            return subprocess.CompletedProcess(
                args=a[0], returncode=0, stdout="", stderr=""
            )
        monkeypatch.setattr(cgroup.subprocess, "run", fake_run)
        assert cgroup.set_cpu_weight("vq-job-x.scope", 50) is False
        assert called == []

    def test_no_systemctl_on_path_returns_false(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Force available() to True so we get past the early-out.
        monkeypatch.setattr(cgroup, "available", lambda: True)
        monkeypatch.setattr(cgroup, "_systemctl_path", lambda: None)
        assert cgroup.set_cpu_weight("vq-job-x.scope", 50) is False

    def test_success_emits_expected_argv(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(cgroup, "available", lambda: True)
        monkeypatch.setattr(cgroup, "_systemctl_path", lambda: "/usr/bin/systemctl")
        captured: list[list[str]] = []
        def fake_run(argv, *a, **kw):  # type: ignore[no-untyped-def]
            captured.append(list(argv))
            return subprocess.CompletedProcess(
                args=argv, returncode=0, stdout="", stderr=""
            )
        monkeypatch.setattr(cgroup.subprocess, "run", fake_run)
        assert cgroup.set_cpu_weight("vq-job-abc.scope", 20) is True
        assert captured == [[
            "/usr/bin/systemctl", "--user", "set-property",
            "vq-job-abc.scope", "CPUWeight=20",
        ]]

    def test_nonzero_exit_returns_false(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """systemctl set-property can fail because the scope is gone or
        the weight is out of range; both surface as a non-zero exit
        rather than a Python exception."""
        monkeypatch.setattr(cgroup, "available", lambda: True)
        monkeypatch.setattr(cgroup, "_systemctl_path", lambda: "/usr/bin/systemctl")
        monkeypatch.setattr(
            cgroup.subprocess, "run",
            lambda *a, **kw: subprocess.CompletedProcess(
                args=a[0], returncode=1, stdout="",
                stderr="Failed to set unit property CPUWeight: Unit not found",
            ),
        )
        assert cgroup.set_cpu_weight("vq-job-gone.scope", 20) is False

    def test_subprocess_timeout_returns_false(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """systemctl hanging is escape-hatched at 5s timeout; we just
        return False rather than blocking the watchdog loop."""
        monkeypatch.setattr(cgroup, "available", lambda: True)
        monkeypatch.setattr(cgroup, "_systemctl_path", lambda: "/usr/bin/systemctl")
        def fake_run(*a, **kw):  # type: ignore[no-untyped-def]
            raise subprocess.TimeoutExpired(cmd=a[0], timeout=5.0)
        monkeypatch.setattr(cgroup.subprocess, "run", fake_run)
        assert cgroup.set_cpu_weight("vq-job-slow.scope", 20) is False

    def test_subprocess_oserror_returns_false(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(cgroup, "available", lambda: True)
        monkeypatch.setattr(cgroup, "_systemctl_path", lambda: "/usr/bin/systemctl")
        def fake_run(*a, **kw):  # type: ignore[no-untyped-def]
            raise OSError("ENOENT")
        monkeypatch.setattr(cgroup.subprocess, "run", fake_run)
        assert cgroup.set_cpu_weight("vq-job-x.scope", 20) is False


# ----------------------------------------------------------------------
# v0.5.38: cgroup-v2 cpu / memory readers for the watchdog
# ----------------------------------------------------------------------


def _patch_proc_open(
    monkeypatch: pytest.MonkeyPatch, pid: int, fake_path: Path
) -> None:
    """Redirect reads of /proc/<pid>/cgroup to a tmp file. Used because
    we can't write to /proc directly in tests. Cheap and surgical."""
    import builtins
    orig_open = builtins.open

    def fake_open(path, *a, **kw):  # type: ignore[no-untyped-def]
        if str(path) == f"/proc/{pid}/cgroup":
            return orig_open(fake_path, *a, **kw)
        return orig_open(path, *a, **kw)

    monkeypatch.setattr(builtins, "open", fake_open)


class TestCgroupPathForPid:
    """``/proc/<pid>/cgroup`` parser. cgroup-v2 emits one line of the
    shape ``0::<path>``; v1 emits multiple lines per controller. We
    support v2 only — the watchdog's fallback handles v1 hosts."""

    def test_returns_absolute_path_for_typical_v2_line(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        fake = tmp_path / "cgroup"
        fake.write_text(
            "0::/user.slice/user-1000.slice/user@1000.service/app.slice/"
            "vq-job-abc123def456.scope\n"
        )
        _patch_proc_open(monkeypatch, 12345, fake)
        assert cgroup.cgroup_path_for_pid(12345) == (
            "/sys/fs/cgroup/user.slice/user-1000.slice/"
            "user@1000.service/app.slice/vq-job-abc123def456.scope"
        )

    def test_missing_proc_entry_returns_none(self) -> None:
        # Guaranteed-absent pid (32-bit max would be 4_194_303 on Linux;
        # 2 billion is safely beyond any system's range).
        assert cgroup.cgroup_path_for_pid(2_000_000_000) is None

    def test_v1_only_format_returns_none(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """v1 hosts emit one line per controller (``1:cpu:/...``,
        ``2:memory:/...``); no ``0::`` prefix exists. Return None so
        the watchdog's pgid-walk fallback engages."""
        fake = tmp_path / "cgroup"
        fake.write_text(
            "12:freezer:/\n"
            "11:cpu,cpuacct:/user.slice\n"
            "10:memory:/user.slice\n"
        )
        _patch_proc_open(monkeypatch, 12345, fake)
        assert cgroup.cgroup_path_for_pid(12345) is None

    def test_root_cgroup_returns_fs_root(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """A process in the root cgroup reports ``0::/``; return
        /sys/fs/cgroup so callers reading sub-files still work."""
        fake = tmp_path / "cgroup"
        fake.write_text("0::/\n")
        _patch_proc_open(monkeypatch, 12345, fake)
        assert cgroup.cgroup_path_for_pid(12345) == cgroup.CGROUP_FS_ROOT


class TestCgroupPathForScope:
    """``systemctl show <scope> -p ControlGroup --value`` parser."""

    def test_returns_absolute_path_from_control_group(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        captured: list[list[str]] = []

        def fake_run(*a, **kw):  # type: ignore[no-untyped-def]
            captured.append(a[0])
            return subprocess.CompletedProcess(
                args=a[0],
                returncode=0,
                stdout=(
                    "/user.slice/user-1000.slice/user@1000.service/"
                    "app.slice/vq-job-abc.scope\n"
                ),
                stderr="",
            )

        monkeypatch.setattr(cgroup, "_systemctl_path", lambda: "/usr/bin/systemctl")
        monkeypatch.setattr(cgroup.subprocess, "run", fake_run)

        assert cgroup.cgroup_path_for_scope("vq-job-abc") == (
            "/sys/fs/cgroup/user.slice/user-1000.slice/"
            "user@1000.service/app.slice/vq-job-abc.scope"
        )
        assert "--user" in captured[0]
        assert "ControlGroup" in captured[0]
        assert "vq-job-abc.scope" in captured[0]

    def test_multi_user_scope_uses_system_manager(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        captured: list[list[str]] = []

        def fake_run(*a, **kw):  # type: ignore[no-untyped-def]
            captured.append(a[0])
            return subprocess.CompletedProcess(
                args=a[0],
                returncode=0,
                stdout="/user.slice/vq-job-mu.scope\n",
                stderr="",
            )

        monkeypatch.setattr(cgroup, "_systemctl_path", lambda: "/usr/bin/systemctl")
        monkeypatch.setattr(cgroup.subprocess, "run", fake_run)

        assert cgroup.cgroup_path_for_scope("vq-job-mu", multi_user=True) == (
            "/sys/fs/cgroup/user.slice/vq-job-mu.scope"
        )
        assert "--user" not in captured[0]

    def test_no_systemctl_returns_none(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(cgroup, "_systemctl_path", lambda: None)
        assert cgroup.cgroup_path_for_scope("vq-job-missing") is None

    def test_missing_control_group_returns_none(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(cgroup, "_systemctl_path", lambda: "/usr/bin/systemctl")
        monkeypatch.setattr(
            cgroup.subprocess,
            "run",
            lambda *a, **kw: subprocess.CompletedProcess(
                args=a[0], returncode=0, stdout="-\n", stderr=""
            ),
        )
        assert cgroup.cgroup_path_for_scope("vq-job-missing.scope") is None


class TestReadCpuUsageSeconds:
    """``cpu.stat`` parser. Pulls ``usage_usec`` and converts to seconds."""

    def test_parses_typical_cpu_stat(self, tmp_path: Path) -> None:
        cg = tmp_path / "scope"
        cg.mkdir()
        # Real-world cpu.stat shape from a 3-second CPU-busy workload.
        (cg / "cpu.stat").write_text(
            "usage_usec 3014872\n"
            "user_usec 2997145\n"
            "system_usec 17727\n"
            "core_sched.force_idle_usec 0\n"
            "nr_periods 0\n"
            "nr_throttled 0\n"
        )
        result = cgroup.read_cpu_usage_seconds(str(cg))
        assert result is not None
        assert abs(result - 3.014872) < 1e-6

    def test_missing_cpu_stat_returns_none(self, tmp_path: Path) -> None:
        cg = tmp_path / "empty_scope"
        cg.mkdir()
        assert cgroup.read_cpu_usage_seconds(str(cg)) is None

    def test_malformed_usage_usec_returns_none(self, tmp_path: Path) -> None:
        cg = tmp_path / "scope"
        cg.mkdir()
        (cg / "cpu.stat").write_text("usage_usec not_a_number\n")
        assert cgroup.read_cpu_usage_seconds(str(cg)) is None

    def test_aggregates_across_all_descendants_documented_intent(
        self, tmp_path: Path
    ) -> None:
        """v0.5.38 fix invariant: ``usage_usec`` is the cgroup-wide
        total regardless of pgid escapes. The kernel guarantees that
        aggregation; this test asserts the parser contract — 240s
        of cumulative cc1plus / ninja / make activity ends up here
        even if they all setsid'd out of the parent pgid."""
        cg = tmp_path / "scope"
        cg.mkdir()
        (cg / "cpu.stat").write_text("usage_usec 240000000\n")
        assert cgroup.read_cpu_usage_seconds(str(cg)) == 240.0


class TestReadMemoryCurrentMb:
    """``memory.current`` parser. Bytes → MB integer."""

    def test_parses_typical_value(self, tmp_path: Path) -> None:
        cg = tmp_path / "scope"
        cg.mkdir()
        # 512 MiB in bytes
        (cg / "memory.current").write_text(str(512 * 1024 * 1024))
        assert cgroup.read_memory_current_mb(str(cg)) == 512

    def test_missing_file_returns_none(self, tmp_path: Path) -> None:
        cg = tmp_path / "no_mem_file"
        cg.mkdir()
        assert cgroup.read_memory_current_mb(str(cg)) is None

    def test_malformed_value_returns_none(self, tmp_path: Path) -> None:
        cg = tmp_path / "scope"
        cg.mkdir()
        (cg / "memory.current").write_text("garbage_not_an_int\n")
        assert cgroup.read_memory_current_mb(str(cg)) is None

    def test_rounds_down_to_whole_mb(self, tmp_path: Path) -> None:
        """1.5 MiB → 1 MB (floor division), matching the watchdog's
        integer-MB comparison against ``spec.mem_mb`` caps."""
        cg = tmp_path / "scope"
        cg.mkdir()
        (cg / "memory.current").write_text(str(int(1.5 * 1024 * 1024)))
        assert cgroup.read_memory_current_mb(str(cg)) == 1


# v0.6.18: coverage fill for the v0.5.50/v0.5.51 scope-inspection
# helpers. These three functions were uncovered (cgroup.py at 67%
# coverage) because real systemctl invocations require a Linux box
# with systemd-user — the daemon-startup code paths exercise them
# integration-style but no unit tests existed.


class TestScopeMainPid:
    """v0.5.50: scope_main_pid asks systemctl `--user show <scope>
    -p MainPID --value` and parses the integer. Used by the
    cgroup-scope MainPID cross-check during daemon-startup recovery."""

    def test_returns_int_pid_on_success(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(cgroup, "_systemctl_path", lambda: "/usr/bin/systemctl")
        monkeypatch.setattr(
            cgroup.subprocess, "run",
            lambda *a, **kw: subprocess.CompletedProcess(
                args=a[0], returncode=0, stdout="12345\n", stderr=""
            ),
        )
        assert cgroup.scope_main_pid("vq-job-abc.scope") == 12345

    def test_appends_scope_suffix_if_missing(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        captured: list[list[str]] = []

        def fake_run(*a, **kw):  # type: ignore[no-untyped-def]
            captured.append(a[0])
            return subprocess.CompletedProcess(
                args=a[0], returncode=0, stdout="100\n", stderr=""
            )

        monkeypatch.setattr(cgroup, "_systemctl_path", lambda: "/usr/bin/systemctl")
        monkeypatch.setattr(cgroup.subprocess, "run", fake_run)
        cgroup.scope_main_pid("vq-job-abc")
        # The unit name passed to systemctl ends with .scope
        assert any("vq-job-abc.scope" in arg for arg in captured[0])

    def test_no_systemctl_returns_none(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(cgroup, "_systemctl_path", lambda: None)
        assert cgroup.scope_main_pid("vq-job-abc.scope") is None

    def test_systemctl_nonzero_returns_none(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(cgroup, "_systemctl_path", lambda: "/usr/bin/systemctl")
        monkeypatch.setattr(
            cgroup.subprocess, "run",
            lambda *a, **kw: subprocess.CompletedProcess(
                args=a[0], returncode=1, stdout="", stderr="unit not found"
            ),
        )
        assert cgroup.scope_main_pid("vq-job-abc.scope") is None

    def test_pid_zero_returns_none(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """systemd MainPID=0 means the scope is loaded but inactive
        — same outcome as 'no MainPID known'."""
        monkeypatch.setattr(cgroup, "_systemctl_path", lambda: "/usr/bin/systemctl")
        monkeypatch.setattr(
            cgroup.subprocess, "run",
            lambda *a, **kw: subprocess.CompletedProcess(
                args=a[0], returncode=0, stdout="0\n", stderr=""
            ),
        )
        assert cgroup.scope_main_pid("vq-job-abc.scope") is None

    def test_unparseable_pid_returns_none(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(cgroup, "_systemctl_path", lambda: "/usr/bin/systemctl")
        monkeypatch.setattr(
            cgroup.subprocess, "run",
            lambda *a, **kw: subprocess.CompletedProcess(
                args=a[0], returncode=0, stdout="garbage\n", stderr=""
            ),
        )
        assert cgroup.scope_main_pid("vq-job-abc.scope") is None

    def test_empty_stdout_returns_none(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(cgroup, "_systemctl_path", lambda: "/usr/bin/systemctl")
        monkeypatch.setattr(
            cgroup.subprocess, "run",
            lambda *a, **kw: subprocess.CompletedProcess(
                args=a[0], returncode=0, stdout="", stderr=""
            ),
        )
        assert cgroup.scope_main_pid("vq-job-abc.scope") is None

    def test_subprocess_error_returns_none(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(cgroup, "_systemctl_path", lambda: "/usr/bin/systemctl")

        def fake_run(*a, **kw):  # type: ignore[no-untyped-def]
            raise OSError("systemctl not executable")

        monkeypatch.setattr(cgroup.subprocess, "run", fake_run)
        assert cgroup.scope_main_pid("vq-job-abc.scope") is None


class TestScopeExists:
    """v0.5.51: scope_exists is the pre-flight against the
    'Unit already exists' failure mode (audit § 2e). Tri-state
    return: True / False / None."""

    def test_loaded_state_returns_true(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(cgroup, "_systemctl_path", lambda: "/usr/bin/systemctl")
        monkeypatch.setattr(
            cgroup.subprocess, "run",
            lambda *a, **kw: subprocess.CompletedProcess(
                args=a[0], returncode=0, stdout="loaded\n", stderr=""
            ),
        )
        assert cgroup.scope_exists("vq-job-abc.scope") is True

    def test_not_found_returns_false(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(cgroup, "_systemctl_path", lambda: "/usr/bin/systemctl")
        monkeypatch.setattr(
            cgroup.subprocess, "run",
            lambda *a, **kw: subprocess.CompletedProcess(
                args=a[0], returncode=0, stdout="not-found\n", stderr=""
            ),
        )
        assert cgroup.scope_exists("vq-job-abc.scope") is False

    def test_empty_state_returns_none(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(cgroup, "_systemctl_path", lambda: "/usr/bin/systemctl")
        monkeypatch.setattr(
            cgroup.subprocess, "run",
            lambda *a, **kw: subprocess.CompletedProcess(
                args=a[0], returncode=0, stdout="\n", stderr=""
            ),
        )
        assert cgroup.scope_exists("vq-job-abc.scope") is None

    def test_no_systemctl_returns_none(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(cgroup, "_systemctl_path", lambda: None)
        assert cgroup.scope_exists("vq-job-abc.scope") is None

    def test_systemctl_nonzero_returns_none(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """rc != 0 = uncertain ('can't tell'), not False."""
        monkeypatch.setattr(cgroup, "_systemctl_path", lambda: "/usr/bin/systemctl")
        monkeypatch.setattr(
            cgroup.subprocess, "run",
            lambda *a, **kw: subprocess.CompletedProcess(
                args=a[0], returncode=2, stdout="", stderr="bus error"
            ),
        )
        assert cgroup.scope_exists("vq-job-abc.scope") is None

    def test_subprocess_error_returns_none(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(cgroup, "_systemctl_path", lambda: "/usr/bin/systemctl")

        def fake_run(*a, **kw):  # type: ignore[no-untyped-def]
            raise subprocess.TimeoutExpired(a[0], 5)

        monkeypatch.setattr(cgroup.subprocess, "run", fake_run)
        assert cgroup.scope_exists("vq-job-abc.scope") is None


class TestStopScope:
    """v0.5.51: stop_scope is the recovery path for a leaked scope
    detected via scope_exists."""

    def test_success_returns_true(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(cgroup, "_systemctl_path", lambda: "/usr/bin/systemctl")
        monkeypatch.setattr(
            cgroup.subprocess, "run",
            lambda *a, **kw: subprocess.CompletedProcess(
                args=a[0], returncode=0, stdout="", stderr=""
            ),
        )
        assert cgroup.stop_scope("vq-job-abc.scope") is True

    def test_failure_returns_false(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(cgroup, "_systemctl_path", lambda: "/usr/bin/systemctl")
        monkeypatch.setattr(
            cgroup.subprocess, "run",
            lambda *a, **kw: subprocess.CompletedProcess(
                args=a[0], returncode=1, stdout="", stderr="job failed"
            ),
        )
        assert cgroup.stop_scope("vq-job-abc.scope") is False

    def test_no_systemctl_returns_false(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(cgroup, "_systemctl_path", lambda: None)
        assert cgroup.stop_scope("vq-job-abc.scope") is False

    def test_subprocess_error_returns_false(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(cgroup, "_systemctl_path", lambda: "/usr/bin/systemctl")

        def fake_run(*a, **kw):  # type: ignore[no-untyped-def]
            raise OSError("perm denied")

        monkeypatch.setattr(cgroup.subprocess, "run", fake_run)
        assert cgroup.stop_scope("vq-job-abc.scope") is False

    def test_appends_scope_suffix_if_missing(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        captured: list[list[str]] = []

        def fake_run(*a, **kw):  # type: ignore[no-untyped-def]
            captured.append(a[0])
            return subprocess.CompletedProcess(
                args=a[0], returncode=0, stdout="", stderr=""
            )

        monkeypatch.setattr(cgroup, "_systemctl_path", lambda: "/usr/bin/systemctl")
        monkeypatch.setattr(cgroup.subprocess, "run", fake_run)
        cgroup.stop_scope("vq-job-abc")  # no .scope suffix
        assert any("vq-job-abc.scope" in arg for arg in captured[0])
