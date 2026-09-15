"""Tests for vq filesystem layout helpers."""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from vq import paths


def _resolve_default_in_subprocess(
    tmp_path: Path,
    expression: str,
    *,
    xdg_data: Path | None = None,
    xdg_config: Path | None = None,
) -> Path:
    """Resolve a production default outside pytest, under a disposable HOME."""
    env = dict(os.environ)
    for name in (
        paths.ENV_STATE_DIR,
        paths.ENV_CONFIG_DIR,
        "PYTEST_CURRENT_TEST",
        "PYTEST_VERSION",
        "PYTEST_ADDOPTS",
        "XDG_DATA_HOME",
        "XDG_CONFIG_HOME",
    ):
        env.pop(name, None)
    home = tmp_path / "subprocess-home"
    home.mkdir()
    env["HOME"] = str(home)
    if xdg_data is not None:
        env["XDG_DATA_HOME"] = str(xdg_data)
    if xdg_config is not None:
        env["XDG_CONFIG_HOME"] = str(xdg_config)
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1] / "src")
    result = subprocess.run(
        [sys.executable, "-c", f"from vq import paths; print({expression})"],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    return Path(result.stdout.strip())


@pytest.fixture(autouse=True)
def isolate_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Wipe path env vars so tests see a deterministic default."""
    for var in (
        paths.ENV_STATE_DIR,
        paths.ENV_CONFIG_DIR,
        paths.ENV_ARCHIVE_DIR,
        paths.ENV_MULTI_USER_ROOT,
        "XDG_DATA_HOME",
        "XDG_CONFIG_HOME",
    ):
        monkeypatch.delenv(var, raising=False)


class TestStateRoot:
    def test_default_is_xdg_data_home(self, tmp_path: Path) -> None:
        xdg_data = tmp_path / "xdg-data"
        assert _resolve_default_in_subprocess(
            tmp_path,
            "paths.state_root()",
            xdg_data=xdg_data,
        ) == xdg_data / "vq"

    def test_default_falls_back_to_dot_local_share(self, tmp_path: Path) -> None:
        assert _resolve_default_in_subprocess(
            tmp_path,
            "paths.state_root()",
        ) == tmp_path / "subprocess-home" / ".local" / "share" / "vq"

    def test_env_override_takes_precedence(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        monkeypatch.setenv("XDG_DATA_HOME", "/data")
        custom = tmp_path / "custom-queue"
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(custom))
        assert paths.state_root() == custom

    def test_env_override_expands_user(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(paths.ENV_STATE_DIR, "~/queue")
        assert paths.state_root() == Path("~/queue").expanduser()


class TestConfigDir:
    def test_default_falls_back_to_dot_config(self, tmp_path: Path) -> None:
        assert _resolve_default_in_subprocess(
            tmp_path,
            "paths.config_dir()",
        ) == tmp_path / "subprocess-home" / ".config" / "vq"

    def test_env_override(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        custom = tmp_path / "etc-vq"
        monkeypatch.setenv(paths.ENV_CONFIG_DIR, str(custom))
        assert paths.config_dir() == custom


class TestSubpaths:
    def test_layout(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        state = tmp_path / "state"
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(state))
        assert paths.queue_dir() == state / "queue"
        assert paths.jobs_dir() == state / "jobs"
        assert paths.spec_path("abc") == state / "queue" / "abc.json"
        assert paths.workspace_dir("abc") == state / "jobs" / "abc"
        assert paths.daemon_pidfile() == state / "daemon.pid"
        assert paths.daemon_logfile() == state / "daemon.log"


class TestAtomicWrite:
    def test_writes_content(self, tmp_path: Path) -> None:
        target = tmp_path / "file.json"
        paths.atomic_write_text(target, '{"hello": "world"}')
        assert target.read_text() == '{"hello": "world"}'

    def test_creates_parent_dirs(self, tmp_path: Path) -> None:
        target = tmp_path / "a" / "b" / "c" / "file.json"
        paths.atomic_write_text(target, "x")
        assert target.read_text() == "x"

    def test_overwrites_existing_atomically(self, tmp_path: Path) -> None:
        target = tmp_path / "file.json"
        target.write_text("old")
        paths.atomic_write_text(target, "new")
        assert target.read_text() == "new"

    def test_does_not_leave_tempfile_behind(self, tmp_path: Path) -> None:
        target = tmp_path / "file.json"
        paths.atomic_write_text(target, "x")
        siblings = list(tmp_path.iterdir())
        assert siblings == [target]

    def test_preserves_mode_on_overwrite(self, tmp_path: Path) -> None:
        # mkstemp forces 0600; the primitive must not silently tighten an
        # existing file's permissions when it overwrites it.
        target = tmp_path / "file.json"
        target.write_text("old")
        target.chmod(0o640)
        paths.atomic_write_text(target, "new")
        assert (target.stat().st_mode & 0o777) == 0o640

    def test_concurrent_writers_never_enoent_or_tear(self, tmp_path: Path) -> None:
        """Regression for CONC-1: pre-v0.8.9 the temp name was a fixed
        ``<name>.tmp`` shared by every writer, so concurrent writers raced on
        one temp file and the loser's ``replace`` hit ``FileNotFoundError``
        (or a reader saw a torn file). The unique-temp primitive must let many
        writers hammer one path with no exception and never expose a partial
        read.
        """
        import threading

        target = tmp_path / "spec.json"
        # Each writer writes a distinct, valid, equal-length JSON payload.
        payloads = [f'{{"writer": {i:04d}}}' for i in range(40)]
        valid = set(payloads)
        errors: list[BaseException] = []
        torn: list[str] = []
        stop = threading.Event()

        def writer(payload: str) -> None:
            try:
                for _ in range(25):
                    paths.atomic_write_text(target, payload)
            except BaseException as exc:  # noqa: BLE001 — capture for assert
                errors.append(exc)

        def reader() -> None:
            while not stop.is_set():
                try:
                    text = target.read_text()
                except FileNotFoundError:
                    continue
                if text and text not in valid:
                    torn.append(text)

        rthread = threading.Thread(target=reader)
        rthread.start()
        wthreads = [threading.Thread(target=writer, args=(p,)) for p in payloads]
        for t in wthreads:
            t.start()
        for t in wthreads:
            t.join()
        stop.set()
        rthread.join()

        assert not errors, f"concurrent writers raised: {errors[:3]}"
        assert not torn, f"reader observed a torn (partial) spec: {torn[:3]}"
        # Final content is exactly one writer's payload, and no temp leaked.
        assert target.read_text() in valid
        assert list(tmp_path.iterdir()) == [target]


class TestSpecLock:
    def test_uses_sidecar_lock_file(self, tmp_path: Path) -> None:
        p = tmp_path / "spec.json"
        p.write_text("{}")
        with paths.spec_lock(p):
            assert (tmp_path / "spec.json.lock").exists()

    def test_serializes_read_modify_write(self, tmp_path: Path) -> None:
        """The whole point: a read->mutate->write wrapped in spec_lock does
        NOT lose updates under concurrency. Without the lock, the sleep
        between read and write guarantees lost increments (final < N*M).
        """
        import threading
        import time as _time

        p = tmp_path / "counter"
        p.write_text("0")
        n_threads, bumps = 24, 4

        def bump() -> None:
            for _ in range(bumps):
                with paths.spec_lock(p):
                    v = int(p.read_text())
                    _time.sleep(0.0005)  # widen the read->write window
                    paths.atomic_write_text(p, str(v + 1))

        threads = [threading.Thread(target=bump) for _ in range(n_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert int(p.read_text()) == n_threads * bumps

    def test_falls_back_to_readonly_when_no_write_permission(
        self, tmp_path: Path
    ) -> None:
        # Simulate a lock file another principal created without giving us
        # write (e.g. root made it in our queue dir). chmod 0444 makes even
        # the owner unable to open O_RDWR, so the helper must fall back to an
        # O_RDONLY open — which still supports flock. (If the suite runs as
        # root, O_RDWR succeeds and the block simply runs; either way no raise.)
        p = tmp_path / "spec.json"
        p.write_text("{}")
        lock = tmp_path / "spec.json.lock"
        lock.write_text("")
        lock.chmod(0o444)
        try:
            with paths.spec_lock(p):
                pass
        finally:
            lock.chmod(0o644)
