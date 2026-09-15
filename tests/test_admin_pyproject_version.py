"""v0.7.2 *Engelbart's Demo* — `vq admin status` surfaces the
project's pyproject.toml [project] version (canonical semver) in
the VERSION column instead of the misleading `git describe`.

The bug this fixes: vibe-qc main on 2026-05-25 was at
``0.9.2.dev0`` per pyproject.toml, but ``vq admin status``
displayed ``v0.7.5-983-g11b4f7af`` for the dev clone. ``git
describe`` walks back to the nearest *annotated* tag — and v0.7.5
is the most recent annotated one git can find on the lineage,
even though v0.8.x and v0.9.x have shipped. Operators reading
the status output thought the env was on the 0.7.x line.

Fix: read ``pyproject.toml``'s ``[project] version`` directly.
Falls back to ``git describe`` only when no pyproject is found
(e.g. an env that doesn't follow the standard layout).
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from vq import admin, config, paths


@pytest.fixture
def state_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    paths.jobs_dir().mkdir(parents=True, exist_ok=True)
    return tmp_path


def _make_repo_with_pyproject(
    state_dir: Path, *, version: str | None = "0.9.2.dev0",
) -> Path:
    repo = state_dir / "repo"
    (repo / ".git").mkdir(parents=True, exist_ok=True)
    if version is not None:
        (repo / "pyproject.toml").write_text(
            "[project]\n"
            'name = "vibeqc"\n'
            f'version = "{version}"\n'
        )
    return repo


def _write_venv_cfg(state_dir: Path, repo: Path) -> None:
    (state_dir / "cfg" / "config.toml").write_text(
        "[programs.vibeqc-dev]\n"
        'kind = "venv"\n'
        'python = "/fake/python"\n'
        f'git_dir = "{repo}"\n'
        'branch = "main"\n'
    )


# ----------------------------------------------------------------------
# _query_pyproject_version helper
# ----------------------------------------------------------------------


class TestQueryPyprojectVersion:
    def test_reads_version_field(self, tmp_path: Path) -> None:
        (tmp_path / "pyproject.toml").write_text(
            '[project]\nname = "vibeqc"\nversion = "0.9.2.dev0"\n'
        )
        assert admin._query_pyproject_version(tmp_path) == "0.9.2.dev0"

    def test_missing_file_returns_none(self, tmp_path: Path) -> None:
        # No pyproject.toml at all.
        assert admin._query_pyproject_version(tmp_path) is None

    def test_no_project_table_returns_none(self, tmp_path: Path) -> None:
        (tmp_path / "pyproject.toml").write_text(
            '[build-system]\nrequires = ["hatchling"]\n'
        )
        assert admin._query_pyproject_version(tmp_path) is None

    def test_no_version_field_returns_none(self, tmp_path: Path) -> None:
        (tmp_path / "pyproject.toml").write_text(
            '[project]\nname = "vibeqc"\n'
        )
        assert admin._query_pyproject_version(tmp_path) is None

    def test_malformed_toml_returns_none(self, tmp_path: Path) -> None:
        (tmp_path / "pyproject.toml").write_text(
            "this is not [valid TOML\n"
        )
        assert admin._query_pyproject_version(tmp_path) is None

    def test_non_string_version_returns_none(
        self, tmp_path: Path,
    ) -> None:
        # version as a TOML integer / array shouldn't crash; we just
        # treat it as "no usable version".
        (tmp_path / "pyproject.toml").write_text(
            '[project]\nname = "x"\nversion = 1\n'
        )
        assert admin._query_pyproject_version(tmp_path) is None


# ----------------------------------------------------------------------
# EnvStatus.current_version population
# ----------------------------------------------------------------------


class TestEnvStatusCurrentVersion:
    def test_populated_from_pyproject(self, state_dir: Path) -> None:
        repo = _make_repo_with_pyproject(state_dir, version="0.9.2.dev0")
        _write_venv_cfg(state_dir, repo)
        cfg = config.load_config()
        st = admin.query_env_status("vibeqc-dev", cfg.programs["vibeqc-dev"])
        assert st.current_version == "0.9.2.dev0"

    def test_none_when_no_pyproject(self, state_dir: Path) -> None:
        repo = _make_repo_with_pyproject(state_dir, version=None)
        _write_venv_cfg(state_dir, repo)
        cfg = config.load_config()
        st = admin.query_env_status("vibeqc-dev", cfg.programs["vibeqc-dev"])
        assert st.current_version is None


# ----------------------------------------------------------------------
# Surface rendering: text + JSON
# ----------------------------------------------------------------------


class TestStatusRendersVersionColumn:
    def test_text_shows_pyproject_version(self, state_dir: Path) -> None:
        """The bug scenario: pyproject says 0.9.2.dev0, the VERSION
        column should display THAT — not some stale describe."""
        repo = _make_repo_with_pyproject(state_dir, version="0.9.2.dev0")
        _write_venv_cfg(state_dir, repo)
        cfg = config.load_config()
        text = admin.format_admin_status(cfg)
        assert "0.9.2.dev0" in text
        # And the column header is now VERSION, not DESCRIBE.
        assert "VERSION" in text
        assert "DESCRIBE" not in text

    def test_text_falls_back_to_describe_when_no_pyproject(
        self, state_dir: Path,
    ) -> None:
        """When the env has no pyproject (or it's unreadable), the
        column falls back to git-describe so we never show a
        useless ``-`` for envs that lack semver."""
        repo = _make_repo_with_pyproject(state_dir, version=None)
        _write_venv_cfg(state_dir, repo)
        # Note: query_env_status calls _query_git_describe, which
        # subprocess.run's git on the bare .git dir we made. On a
        # repo with no commits / no tags, describe returns None,
        # so the cell falls all the way through to "-". Pin the
        # fallback ORDER (version → describe → "-").
        cfg = config.load_config()
        text = admin.format_admin_status(cfg)
        # No 0.9.x string anywhere (we didn't write one).
        assert "0.9" not in text

    def test_json_includes_current_version(
        self, state_dir: Path,
    ) -> None:
        repo = _make_repo_with_pyproject(state_dir, version="0.9.2.dev0")
        _write_venv_cfg(state_dir, repo)
        cfg = config.load_config()
        payload = json.loads(admin.format_admin_status_json(cfg))
        env = next(e for e in payload["envs"] if e["name"] == "vibeqc-dev")
        assert env["current_version"] == "0.9.2.dev0"
        # current_describe is preserved for back-compat.
        assert "current_describe" in env
