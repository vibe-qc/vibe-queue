"""Tests for vq.auth: web-token file management + constant-time match."""
from __future__ import annotations

import os
import stat
from pathlib import Path

import pytest

from vq import auth, config


@pytest.fixture
def cfg_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Isolate config dir so tests don't read the user's real token."""
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    monkeypatch.delenv(auth.ENV_WEB_TOKEN_FILE, raising=False)
    return tmp_path / "cfg"


class TestGenerateToken:
    def test_token_is_urlsafe_base64_and_long_enough(self) -> None:
        t = auth.generate_token()
        # 256 bits in urlsafe-base64 = 43 chars (no padding for token_urlsafe)
        assert len(t) >= 40
        # urlsafe means [A-Za-z0-9_-]
        assert all(c.isalnum() or c in "-_" for c in t), t

    def test_two_tokens_differ(self) -> None:
        assert auth.generate_token() != auth.generate_token()


class TestWriteAndLoadToken:
    def test_write_creates_file_with_mode_0600(self, cfg_dir: Path) -> None:
        token = auth.generate_token()
        path = auth.write_token(token)
        assert path.exists()
        assert (path.stat().st_mode & 0o777) == 0o600
        assert path.read_text().strip() == token

    def test_write_creates_parent_dirs(self, cfg_dir: Path) -> None:
        # cfg_dir doesn't exist yet (fixture didn't mkdir)
        token = auth.generate_token()
        path = auth.write_token(token)
        assert path.parent.exists()

    def test_write_refuses_overwrite_without_force(self, cfg_dir: Path) -> None:
        auth.write_token("first")
        with pytest.raises(FileExistsError, match="refusing to overwrite"):
            auth.write_token("second")

    def test_write_overwrites_with_force(self, cfg_dir: Path) -> None:
        auth.write_token("first")
        auth.write_token("second", force=True)
        assert auth.load_token() == "second"

    def test_force_rotation_repairs_permissive_mode(self, cfg_dir: Path) -> None:
        path = auth.write_token("first")
        os.chmod(path, 0o644)

        rotated = auth.write_token("second", force=True)

        assert rotated == path
        assert (path.stat().st_mode & 0o777) == 0o600
        assert auth.load_token() == "second"

    def test_force_rotation_replaces_symlink_without_touching_target(
        self, cfg_dir: Path, tmp_path: Path
    ) -> None:
        outside = tmp_path / "outside-token"
        outside.write_text("do-not-change\n", encoding="utf-8")
        path = auth.web_token_path()
        path.parent.mkdir(parents=True)
        path.symlink_to(outside)

        with pytest.raises(FileExistsError, match="refusing to overwrite"):
            auth.write_token("new-token")
        rotated = auth.write_token("new-token", force=True)

        assert rotated == path
        assert not path.is_symlink()
        assert path.read_text(encoding="utf-8") == "new-token\n"
        assert outside.read_text(encoding="utf-8") == "do-not-change\n"

    @pytest.mark.parametrize(
        "token", ["", "two\nlines", "carriage\rreturn", "nul\0byte"]
    )
    def test_write_rejects_non_single_line_tokens(
        self, cfg_dir: Path, token: str
    ) -> None:
        with pytest.raises(ValueError, match="one non-empty line"):
            auth.write_token(token)
        assert not auth.web_token_path().exists()

    def test_write_retries_short_writes(
        self, cfg_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        real_write = os.write
        calls = 0

        def one_byte(fd: int, data: bytes | memoryview) -> int:
            nonlocal calls
            calls += 1
            return real_write(fd, data[:1])

        monkeypatch.setattr(os, "write", one_byte)
        path = auth.write_token("short-write-proof")

        assert calls == len("short-write-proof\n")
        assert path.read_text(encoding="utf-8") == "short-write-proof\n"

    def test_write_fsyncs_temporary_and_parent_around_publish(
        self, cfg_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        real_fsync = os.fsync
        real_link = os.link
        events: list[str] = []

        def recorded_fsync(fd: int) -> None:
            kind = "parent" if stat.S_ISDIR(os.fstat(fd).st_mode) else "file"
            events.append(f"fsync-{kind}")
            real_fsync(fd)

        def recorded_link(
            src: os.PathLike[str],
            dst: os.PathLike[str],
            *,
            follow_symlinks: bool = True,
        ) -> None:
            events.append("publish")
            real_link(src, dst, follow_symlinks=follow_symlinks)

        monkeypatch.setattr(os, "fsync", recorded_fsync)
        monkeypatch.setattr(os, "link", recorded_link)
        auth.write_token("durable-token")

        assert events == ["fsync-file", "fsync-parent", "publish", "fsync-parent"]

    def test_load_returns_none_when_no_file(self, cfg_dir: Path) -> None:
        assert auth.load_token() is None

    def test_load_returns_token_string(self, cfg_dir: Path) -> None:
        auth.write_token("rotation-test-token-1")
        assert auth.load_token() == "rotation-test-token-1"

    def test_load_strips_trailing_newline(self, cfg_dir: Path) -> None:
        path = auth.web_token_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        # Write with a trailing newline manually -- we want to ensure
        # load_token strips it (write_token already does, but a hand-
        # edited file might have whitespace).
        path.write_text("token-with-newline\n")
        os.chmod(path, 0o600)
        assert auth.load_token() == "token-with-newline"

    def test_load_returns_none_if_mode_too_permissive(
        self, cfg_dir: Path
    ) -> None:
        path = auth.write_token("secret")
        # Loosen permissions; load should refuse to read.
        os.chmod(path, 0o644)
        assert auth.load_token() is None

    def test_load_returns_none_if_empty_file(self, cfg_dir: Path) -> None:
        path = auth.web_token_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("")
        os.chmod(path, 0o600)
        assert auth.load_token() is None


class TestEnvOverride:
    def test_env_var_overrides_default_path(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        custom = tmp_path / "custom-token"
        monkeypatch.setenv(auth.ENV_WEB_TOKEN_FILE, str(custom))
        assert auth.web_token_path() == custom


class TestConstantTimeEq:
    def test_equal_strings_match(self) -> None:
        assert auth.constant_time_eq("abc", "abc")

    def test_different_strings_dont_match(self) -> None:
        assert not auth.constant_time_eq("abc", "abd")

    def test_different_lengths_dont_match(self) -> None:
        assert not auth.constant_time_eq("abc", "abcd")
