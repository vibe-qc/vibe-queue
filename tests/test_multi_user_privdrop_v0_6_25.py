"""v0.6.25: multi-user privilege-drop — jobs run as their submitter.

The v0.6.x multi-user backbone shipped per-user state dirs + ownership
checks but never dropped privileges when spawning jobs: a root daemon
ran every job as root. These tests cover the fix:

  * cgroup.wrap_command(run_as_uid=...) builds a SYSTEM-mode
    `systemd-run --scope --uid/--gid` argv (no --user), and the wrap
    is mandatory (applied even with no resource caps).
  * a missing systemd-run raises rather than returning a root-running
    command.
  * the Daemon refuses to construct in multi-user mode without
    systemd-run on PATH.
  * ownership._caller_is_admin resolves group membership correctly
    (the pre-fix code compared an int uid against a list of name
    strings and never matched).
  * the _gid_for_uid / _chown_tree dispatch helpers.
"""
from __future__ import annotations

import os
from pathlib import Path

import pytest

from vq import cgroup, ownership
from vq import config as config_module
from vq.daemon import Daemon, _chown_tree, _gid_for_uid

# ----------------------------------------------------------------------
# cgroup.wrap_command — multi-user system-mode wrap
# ----------------------------------------------------------------------


class TestWrapCommandPrivDrop:
    def _systemd_run(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            cgroup, "_systemd_run_path", lambda: "/usr/bin/systemd-run"
        )

    def test_run_as_uid_builds_system_mode_scope(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._systemd_run(monkeypatch)
        argv = cgroup.wrap_command(
            ["echo", "hi"], run_as_uid=1001, run_as_gid=1001,
        )
        # System mode: --scope present, --user ABSENT (the whole point).
        assert "--scope" in argv
        assert "--user" not in argv
        assert "--uid=1001" in argv
        assert "--gid=1001" in argv
        # The inner command is still there after the -- separator.
        assert argv[-2:] == ["echo", "hi"]
        assert "--" in argv

    def test_wrap_is_mandatory_even_without_caps(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """In single-user mode wrap_command returns the cmd unchanged
        when no caps are set. In multi-user mode the wrap IS the
        privilege-drop, so it must always apply."""
        self._systemd_run(monkeypatch)
        cmd = ["echo", "hi"]
        argv = cgroup.wrap_command(cmd, run_as_uid=1001, run_as_gid=1001)
        assert argv is not cmd
        assert "--uid=1001" in argv

    def test_missing_systemd_run_raises_not_root_run(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The critical safety property: if privileges cannot be
        dropped, wrap_command must RAISE — never return a command that
        would run as root."""
        monkeypatch.setattr(cgroup, "_systemd_run_path", lambda: None)
        with pytest.raises(RuntimeError, match="systemd-run"):
            cgroup.wrap_command(
                ["echo", "hi"], run_as_uid=1001, run_as_gid=1001,
            )

    def test_resource_caps_still_applied_in_multi_user_wrap(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._systemd_run(monkeypatch)
        argv = cgroup.wrap_command(
            ["echo", "hi"], mem_mb=512, cpus=4,
            run_as_uid=1001, run_as_gid=1001, unit_name="vq-job-abc",
        )
        assert "--property=MemoryMax=512M" in argv
        assert "--property=CPUQuota=400%" in argv
        assert "--unit" in argv and "vq-job-abc" in argv

    def test_gid_optional(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self._systemd_run(monkeypatch)
        argv = cgroup.wrap_command(["echo", "hi"], run_as_uid=1001)
        assert "--uid=1001" in argv
        assert not any(a.startswith("--gid=") for a in argv)

    def test_single_user_wrap_unchanged(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Regression guard: with no run_as_uid, behaviour is the old
        single-user path — bare cmd when no caps, --user scope when
        caps are set."""
        self._systemd_run(monkeypatch)
        monkeypatch.setattr(cgroup, "available", lambda: True)
        cmd = ["echo", "hi"]
        # No caps → returned unchanged.
        assert cgroup.wrap_command(cmd) is cmd
        # Caps → --user scope (single-user mode).
        argv = cgroup.wrap_command(cmd, mem_mb=256)
        assert "--user" in argv
        assert not any(a.startswith("--uid=") for a in argv)


class TestSystemdRunOnPath:
    def test_true_when_binary_present(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            cgroup, "_systemd_run_path", lambda: "/usr/bin/systemd-run"
        )
        assert cgroup.systemd_run_on_path() is True

    def test_false_when_absent(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(cgroup, "_systemd_run_path", lambda: None)
        assert cgroup.systemd_run_on_path() is False


# ----------------------------------------------------------------------
# Daemon startup guard
# ----------------------------------------------------------------------


class TestDaemonMultiUserStartupGuard:
    def test_refuses_construction_without_systemd_run(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("VQ_MULTI_USER_ROOT", str(tmp_path / "mu"))
        monkeypatch.setattr(cgroup, "systemd_run_on_path", lambda: False)
        with pytest.raises(RuntimeError, match="systemd-run"):
            Daemon(
                max_cpus=4,
                poll_interval=0.05,
                multi_user=True,
                queue_dir=tmp_path / "q",
                jobs_dir=tmp_path / "j",
            )

    def test_constructs_with_systemd_run(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("VQ_MULTI_USER_ROOT", str(tmp_path / "mu"))
        monkeypatch.setattr(cgroup, "systemd_run_on_path", lambda: True)
        d = Daemon(
            max_cpus=4,
            poll_interval=0.05,
            multi_user=True,
            queue_dir=tmp_path / "q",
            jobs_dir=tmp_path / "j",
        )
        assert d._multi_user is True
        # Release the queue lock the constructor claimed.
        d._queue_lock_fd.close()

    def test_single_user_construction_needs_no_systemd_run(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The guard is multi-user-only — single-user daemons still
        construct fine on hosts without systemd-run (macOS dev box)."""
        monkeypatch.setattr(cgroup, "systemd_run_on_path", lambda: False)
        d = Daemon(
            max_cpus=4,
            poll_interval=0.05,
            queue_dir=tmp_path / "q",
            jobs_dir=tmp_path / "j",
        )
        assert d._multi_user is False
        d._queue_lock_fd.close()


# ----------------------------------------------------------------------
# Dispatch helpers
# ----------------------------------------------------------------------


class TestDispatchHelpers:
    def test_gid_for_uid_resolves_current_user(self) -> None:
        gid = _gid_for_uid(os.getuid())
        assert gid == os.getgid() or gid is not None

    def test_gid_for_uid_unknown_uid_returns_none(self) -> None:
        # A uid with no passwd entry → None (caller fails the job
        # rather than guessing a gid).
        assert _gid_for_uid(4_000_123) is None

    def test_chown_tree_to_self_succeeds(self, tmp_path: Path) -> None:
        """chowning to the caller's own uid/gid is always permitted —
        exercises the walk without needing root."""
        (tmp_path / "sub").mkdir()
        (tmp_path / "sub" / "f.txt").write_text("x")
        (tmp_path / "top.txt").write_text("y")
        # Should not raise.
        _chown_tree(tmp_path, os.getuid(), os.getgid())
        assert (tmp_path / "sub" / "f.txt").read_text() == "x"


# ----------------------------------------------------------------------
# ownership._caller_is_admin — group resolution fix
# ----------------------------------------------------------------------


class _FakeGroup:
    def __init__(self, gr_mem: list[str], gr_gid: int) -> None:
        self.gr_mem = gr_mem
        self.gr_gid = gr_gid


class _FakePw:
    def __init__(self, pw_name: str, pw_gid: int) -> None:
        self.pw_name = pw_name
        self.pw_gid = pw_gid


class TestCallerIsAdmin:
    def _cfg(self) -> config_module.Config:
        cfg = config_module.load_config()
        cfg.multi_user.enabled = True
        cfg.multi_user.admin_group = "vq-admins"
        return cfg

    def test_root_is_always_admin(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(ownership, "_caller_uid", lambda: 0)
        assert ownership._caller_is_admin(self._cfg()) is True

    def test_supplementary_member_is_admin(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(ownership, "_caller_uid", lambda: 1001)
        monkeypatch.setattr(
            ownership.grp, "getgrnam",
            lambda n: _FakeGroup(gr_mem=["alice"], gr_gid=5000),
        )
        import pwd as _pwd

        monkeypatch.setattr(
            _pwd, "getpwuid", lambda u: _FakePw("alice", pw_gid=1001)
        )
        # alice is in gr_mem → admin (the pre-fix int-vs-names bug
        # would have wrongly denied her).
        assert ownership._caller_is_admin(self._cfg()) is True

    def test_primary_group_member_is_admin(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(ownership, "_caller_uid", lambda: 1002)
        monkeypatch.setattr(
            ownership.grp, "getgrnam",
            lambda n: _FakeGroup(gr_mem=[], gr_gid=5000),
        )
        import pwd as _pwd

        # bob's PRIMARY gid is the admin group's gid.
        monkeypatch.setattr(
            _pwd, "getpwuid", lambda u: _FakePw("bob", pw_gid=5000)
        )
        assert ownership._caller_is_admin(self._cfg()) is True

    def test_non_member_is_not_admin(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(ownership, "_caller_uid", lambda: 1003)
        monkeypatch.setattr(
            ownership.grp, "getgrnam",
            lambda n: _FakeGroup(gr_mem=["alice"], gr_gid=5000),
        )
        import pwd as _pwd

        monkeypatch.setattr(
            _pwd, "getpwuid", lambda u: _FakePw("carol", pw_gid=1003)
        )
        assert ownership._caller_is_admin(self._cfg()) is False

    def test_missing_group_means_no_admin(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(ownership, "_caller_uid", lambda: 1004)

        def _no_group(n: str) -> _FakeGroup:
            raise KeyError(n)

        monkeypatch.setattr(ownership.grp, "getgrnam", _no_group)
        assert ownership._caller_is_admin(self._cfg()) is False
