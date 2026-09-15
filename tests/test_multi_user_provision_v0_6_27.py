"""v0.6.27: multi-user daemon auto-provisions admin-group state dirs.

The v0.6.x multi-user backbone left a submit-side bootstrap gap:
``/var/lib/vq/users/`` is root-owned, so an unprivileged user could
not create their own ``<uid>/`` subtree and their first ``vq submit``
failed with PermissionError. v0.6.27 has the multi-user daemon (which
runs as root) pre-create a per-user state dir for every admin-group
member at startup.

Tests cover:
  * paths.provision_user_state — creates the {queue,jobs,archive}
    tree, idempotent, keeps the structural parent daemon-owned, and
    chowns writable children to the target uid/gid.
  * ownership.admin_group_uids — resolves supplementary + primary-gid
    members; empty in single-user mode / when the group is missing.
  * Daemon._provision_admin_user_dirs — provisions dirs for the uids
    admin_group_uids reports.
"""
from __future__ import annotations

import os
import stat
from pathlib import Path

import pytest

from vq import cgroup, ownership, paths
from vq import config as config_module
from vq.daemon import Daemon

# ----------------------------------------------------------------------
# paths.provision_user_state
# ----------------------------------------------------------------------


class TestProvisionUserState:
    def test_creates_full_tree(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(tmp_path / "vq"))
        uid, gid = os.getuid(), os.getgid()
        paths.provision_user_state(uid, gid)
        assert paths.user_dir(uid).is_dir()
        assert paths.user_queue_dir(uid).is_dir()
        assert paths.user_jobs_dir(uid).is_dir()
        assert paths.user_archive_dir(uid).is_dir()

    def test_idempotent(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(tmp_path / "vq"))
        uid, gid = os.getuid(), os.getgid()
        paths.provision_user_state(uid, gid)
        # Second call must not raise on already-existing dirs.
        paths.provision_user_state(uid, gid)
        assert paths.user_queue_dir(uid).is_dir()

    def test_structural_parent_owned_by_provisioner_and_children_by_target(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(tmp_path / "vq"))
        uid, gid = os.getuid(), os.getgid()
        paths.provision_user_state(uid, gid)
        structural = paths.user_dir(uid).stat()
        assert structural.st_uid == os.geteuid()
        assert structural.st_gid == gid
        assert stat.S_IMODE(structural.st_mode) == 0o750
        for child in (
            paths.user_queue_dir(uid),
            paths.user_jobs_dir(uid),
            paths.user_archive_dir(uid),
            paths.user_workdir_root(uid),
        ):
            st = child.stat()
            assert st.st_uid == uid
            assert st.st_gid == gid

    def test_preserves_safe_modes_and_strips_group_world_write(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(tmp_path / "vq"))
        uid, gid = os.getuid(), os.getgid()
        user_root = paths.user_dir(uid)
        user_root.mkdir(parents=True)
        expected_modes = {
            "queue": 0o700,
            "jobs": 0o770,
            "archive": 0o777,
            "workdirs": 0o751,
        }
        hardened_modes = {
            "queue": 0o700,
            "jobs": 0o750,
            "archive": 0o755,
            "workdirs": 0o751,
        }
        for name, mode in expected_modes.items():
            child = user_root / name
            child.mkdir()
            child.chmod(mode)

        paths.provision_user_state(uid, gid)

        assert stat.S_IMODE(user_root.stat().st_mode) == 0o750
        for name, mode in hardened_modes.items():
            assert stat.S_IMODE((user_root / name).stat().st_mode) == mode

    def test_repairs_writable_structural_roots(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        state_root = tmp_path / "vq"
        users_root = state_root / "users"
        users_root.mkdir(parents=True)
        state_root.chmod(0o2777)
        users_root.chmod(0o777)
        monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(state_root))

        paths.ensure_users_root()

        # The state/control root intentionally stays group-writable for the
        # trusted admin group; only world write is removed. ``users/`` is the
        # narrower structural boundary and loses both group/world write.
        assert stat.S_IMODE(state_root.stat().st_mode) == 0o2775
        assert stat.S_IMODE(users_root.stat().st_mode) == 0o755

    @pytest.mark.parametrize("component", ["state", "users"])
    def test_refuses_symlinked_structural_root(
        self,
        component: str,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        state_root = tmp_path / "vq"
        outside = tmp_path / "outside"
        outside.mkdir()
        if component == "state":
            state_root.symlink_to(outside, target_is_directory=True)
        else:
            state_root.mkdir()
            (state_root / "users").symlink_to(outside, target_is_directory=True)
        monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(state_root))

        with pytest.raises(paths.UnsafeMultiUserStateError, match="real directory"):
            paths.ensure_users_root()

    def test_daemon_refuses_symlinked_state_root_before_opening_lock(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        state_root = tmp_path / "vq"
        outside = tmp_path / "outside"
        outside.mkdir()
        state_root.symlink_to(outside, target_is_directory=True)
        monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(state_root))
        monkeypatch.setattr(cgroup, "systemd_run_on_path", lambda: True)

        with pytest.raises(paths.UnsafeMultiUserStateError, match="real directory"):
            Daemon(
                max_cpus=4,
                poll_interval=0.05,
                multi_user=True,
                queue_dir=tmp_path / "q",
                jobs_dir=tmp_path / "j",
            )

        assert not (outside / ".vq-daemon.lock").exists()

    @pytest.mark.parametrize("name", ["queue", "jobs", "archive", "workdirs"])
    def test_refuses_symlinked_managed_child_without_following_target(
        self,
        name: str,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(tmp_path / "vq"))
        uid, gid = os.getuid(), os.getgid()
        user_root = paths.user_dir(uid)
        user_root.mkdir(parents=True)
        outside = tmp_path / f"outside-{name}"
        outside.mkdir()
        before = outside.stat()
        (user_root / name).symlink_to(outside, target_is_directory=True)

        with pytest.raises(paths.UnsafeMultiUserStateError, match=name):
            paths.provision_user_state(uid, gid)

        after = outside.stat()
        assert (user_root / name).is_symlink()
        assert (after.st_uid, after.st_gid) == (before.st_uid, before.st_gid)

    def test_refuses_non_directory_managed_child(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(tmp_path / "vq"))
        uid, gid = os.getuid(), os.getgid()
        user_root = paths.user_dir(uid)
        user_root.mkdir(parents=True)
        (user_root / "jobs").write_text("not a directory", encoding="utf-8")

        with pytest.raises(paths.UnsafeMultiUserStateError, match="jobs"):
            paths.provision_user_state(uid, gid)


# ----------------------------------------------------------------------
# ownership.admin_group_uids
# ----------------------------------------------------------------------


class _FakeGroup:
    def __init__(self, gr_mem: list[str], gr_gid: int) -> None:
        self.gr_mem = gr_mem
        self.gr_gid = gr_gid


class _FakePw:
    def __init__(self, pw_name: str, pw_uid: int, pw_gid: int) -> None:
        self.pw_name = pw_name
        self.pw_uid = pw_uid
        self.pw_gid = pw_gid


class TestAdminGroupUids:
    def _cfg(self, *, multi_user: bool) -> config_module.Config:
        cfg = config_module.load_config()
        cfg.multi_user.enabled = multi_user
        cfg.multi_user.admin_group = "vq-admins"
        return cfg

    def test_single_user_returns_empty(self) -> None:
        assert ownership.admin_group_uids(self._cfg(multi_user=False)) == []

    def test_missing_group_returns_empty(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def _no_group(n: str) -> _FakeGroup:
            raise KeyError(n)

        monkeypatch.setattr(ownership.grp, "getgrnam", _no_group)
        assert ownership.admin_group_uids(self._cfg(multi_user=True)) == []

    def test_resolves_supplementary_members(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            ownership.grp, "getgrnam",
            lambda n: _FakeGroup(gr_mem=["alice", "bob"], gr_gid=5000),
        )
        import pwd as _pwd

        names = {
            "alice": _FakePw("alice", 1001, 1001),
            "bob": _FakePw("bob", 1002, 1002),
        }
        monkeypatch.setattr(_pwd, "getpwnam", lambda n: names[n])
        monkeypatch.setattr(_pwd, "getpwall", list)  # no primary-gid members
        assert ownership.admin_group_uids(self._cfg(multi_user=True)) == [
            1001, 1002,
        ]

    def test_includes_primary_gid_members(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            ownership.grp, "getgrnam",
            lambda n: _FakeGroup(gr_mem=[], gr_gid=5000),
        )
        import pwd as _pwd

        # carol's PRIMARY gid is the admin group → must be included
        # even though she's not in gr_mem.
        monkeypatch.setattr(
            _pwd, "getpwall",
            lambda: [_FakePw("carol", 1003, 5000), _FakePw("dave", 1004, 1004)],
        )
        assert ownership.admin_group_uids(self._cfg(multi_user=True)) == [1003]

    def test_stale_member_name_skipped(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            ownership.grp, "getgrnam",
            lambda n: _FakeGroup(gr_mem=["ghost"], gr_gid=5000),
        )
        import pwd as _pwd

        def _no_user(n: str) -> _FakePw:
            raise KeyError(n)

        monkeypatch.setattr(_pwd, "getpwnam", _no_user)
        monkeypatch.setattr(_pwd, "getpwall", list)
        # 'ghost' has no passwd entry → silently dropped, not a crash.
        assert ownership.admin_group_uids(self._cfg(multi_user=True)) == []


# ----------------------------------------------------------------------
# Daemon._provision_admin_user_dirs
# ----------------------------------------------------------------------


class TestDaemonProvisionsAdminDirs:
    def test_provisions_reported_uids(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(tmp_path / "mu"))
        monkeypatch.setattr(cgroup, "systemd_run_on_path", lambda: True)
        # admin_group_uids → just the current uid (so the chown in
        # provision_user_state is chown-to-self and needs no root).
        monkeypatch.setattr(
            ownership, "admin_group_uids", lambda cfg: [os.getuid()]
        )
        d = Daemon(
            max_cpus=4,
            poll_interval=0.05,
            multi_user=True,
            queue_dir=tmp_path / "q",
            jobs_dir=tmp_path / "j",
        )
        try:
            d._provision_admin_user_dirs()
            assert paths.user_queue_dir(os.getuid()).is_dir()
            assert paths.user_jobs_dir(os.getuid()).is_dir()
        finally:
            d._queue_lock_fd.close()

    def test_no_admins_is_noop(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(tmp_path / "mu"))
        monkeypatch.setattr(cgroup, "systemd_run_on_path", lambda: True)
        monkeypatch.setattr(ownership, "admin_group_uids", lambda cfg: [])
        d = Daemon(
            max_cpus=4,
            poll_interval=0.05,
            multi_user=True,
            queue_dir=tmp_path / "q",
            jobs_dir=tmp_path / "j",
        )
        try:
            # Must not raise with an empty admin set.
            d._provision_admin_user_dirs()
        finally:
            d._queue_lock_fd.close()

    def test_existing_non_admin_tree_is_hardened_at_startup(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(tmp_path / "mu"))
        monkeypatch.setattr(cgroup, "systemd_run_on_path", lambda: True)
        existing_uid = os.getuid()
        paths.user_dir(existing_uid).mkdir(parents=True)
        monkeypatch.setattr(ownership, "admin_group_uids", lambda cfg: [])
        seen: list[tuple[int, int]] = []
        real_provision = paths.provision_user_state

        def record(uid: int, gid: int) -> None:
            seen.append((uid, gid))
            real_provision(uid, gid)

        monkeypatch.setattr(paths, "provision_user_state", record)
        d = Daemon(
            max_cpus=4,
            poll_interval=0.05,
            multi_user=True,
            queue_dir=tmp_path / "q",
            jobs_dir=tmp_path / "j",
        )
        try:
            d._provision_admin_user_dirs()
            assert seen == [(existing_uid, os.getgid())]
        finally:
            d._queue_lock_fd.close()

    def test_unsafe_existing_tree_fails_closed(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(tmp_path / "mu"))
        monkeypatch.setattr(cgroup, "systemd_run_on_path", lambda: True)
        existing_uid = os.getuid()
        paths.user_dir(existing_uid).mkdir(parents=True)
        monkeypatch.setattr(ownership, "admin_group_uids", lambda cfg: [])

        def reject(uid: int, gid: int) -> None:
            raise paths.UnsafeMultiUserStateError(f"unsafe uid {uid}:{gid}")

        monkeypatch.setattr(paths, "provision_user_state", reject)
        d = Daemon(
            max_cpus=4,
            poll_interval=0.05,
            multi_user=True,
            queue_dir=tmp_path / "q",
            jobs_dir=tmp_path / "j",
        )
        try:
            with pytest.raises(paths.UnsafeMultiUserStateError, match="unsafe uid"):
                d._provision_admin_user_dirs()
        finally:
            d._queue_lock_fd.close()

    def test_numeric_non_directory_user_entry_fails_closed(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(tmp_path / "mu"))
        monkeypatch.setattr(cgroup, "systemd_run_on_path", lambda: True)
        existing_uid = os.getuid()
        paths.users_root().mkdir(parents=True)
        paths.user_dir(existing_uid).write_text("unsafe", encoding="utf-8")
        monkeypatch.setattr(ownership, "admin_group_uids", lambda cfg: [])
        d = Daemon(
            max_cpus=4,
            poll_interval=0.05,
            multi_user=True,
            queue_dir=tmp_path / "q",
            jobs_dir=tmp_path / "j",
        )
        try:
            with pytest.raises(
                paths.UnsafeMultiUserStateError,
                match="expected a real directory",
            ):
                d._provision_admin_user_dirs()
        finally:
            d._queue_lock_fd.close()
