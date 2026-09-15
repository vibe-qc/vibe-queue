"""`vq admin install`: bring a host up through vq instead of by hand.

A host that does not yet have a program's `git_dir` could not be managed by
vq at all -- `_resolve_venv_program` refuses and nothing clones. So every host
in the 2026-09 migration needed a manual `git clone` plus `scripts/install.sh`
first: the one step that could not go through vibe-queue, and therefore the
step most likely to be done inconsistently. It was.

These run real git against real local upstreams. A provisioning verb that is
only tested against mocks is one whose first real use is on a host.
"""
from __future__ import annotations

import subprocess
from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import admin, config, paths
from vq.cli import main


def _git(cwd: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=cwd, check=True, capture_output=True, text=True,
    ).stdout.strip()


@pytest.fixture
def upstream(tmp_path: Path) -> Path:
    repo = tmp_path / "upstream"
    repo.mkdir()
    _git(repo, "init", "-q", "-b", "main")
    _git(repo, "config", "user.email", "test@example.invalid")
    _git(repo, "config", "user.name", "Test User")
    (repo / "scripts").mkdir()
    installer = repo / "scripts" / "install.sh"
    installer.write_text(
        "#!/usr/bin/env bash\n"
        'printf "INSTALLED %s in %s\\n" "$*" "$PWD"\n',
        encoding="utf-8",
    )
    installer.chmod(0o755)
    _git(repo, "add", ".")
    _git(repo, "commit", "-qm", "first")
    _git(repo, "tag", "v1.0.0")
    return repo


@pytest.fixture
def sha(upstream: Path) -> str:
    return _git(upstream, "rev-parse", "HEAD")


@pytest.fixture
def cfg(
    tmp_path: Path, upstream: Path, monkeypatch: pytest.MonkeyPatch,
) -> config.Config:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    (tmp_path / "cfg" / "config.toml").write_text(
        'default_host = "localhost"\n'
        "\n"
        "[hosts.localhost]\n"
        'ssh = "localhost"\n'
        "\n"
        "[programs.demo]\n"
        'kind = "venv"\n'
        f'python = "{tmp_path}/checkout/.venv/bin/python"\n'
        f'git_dir = "{tmp_path}/checkout"\n'
        f'upstream = "{upstream}"\n'
        'install_script = "scripts/install.sh"\n',
        encoding="utf-8",
    )
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    paths.jobs_dir().mkdir(parents=True, exist_ok=True)
    return config.load_config()


class TestProvisionEnv:
    def test_it_clones_at_the_pin_and_runs_the_installer(
        self, cfg: config.Config, tmp_path: Path, sha: str,
    ) -> None:
        result = admin.provision_env(
            "demo", cfg, host="localhost", expected_sha=sha,
        )

        assert result.success is True, result.work_errors
        assert result.operation == "install"
        checkout = tmp_path / "checkout"
        # Verified by what the checkout resolves to, not by the exit code.
        assert _git(checkout, "rev-parse", "HEAD") == sha
        assert result.actual_sha == sha
        assert result.update_script_rc == 0
        assert "INSTALLED" in result.update_script_output
        assert str(checkout) in result.update_script_output

    def test_it_forwards_installer_arguments(
        self, cfg: config.Config, sha: str,
    ) -> None:
        result = admin.provision_env(
            "demo", cfg, host="localhost", expected_sha=sha,
            install_script_args=["--editable", "--extras", "dev"],
        )

        assert result.success is True, result.work_errors
        assert "INSTALLED --editable --extras dev" in result.update_script_output

    def test_it_verifies_the_tag_it_was_given(
        self, cfg: config.Config, sha: str,
    ) -> None:
        result = admin.provision_env(
            "demo", cfg, host="localhost", expected_sha=sha,
            expected_tag="v1.0.0",
        )

        assert result.success is True, result.work_errors
        assert result.actual_tag == "v1.0.0"
        assert result.tag_matches is True

    def test_a_wrong_tag_fails_the_provision(
        self, cfg: config.Config, sha: str,
    ) -> None:
        result = admin.provision_env(
            "demo", cfg, host="localhost", expected_sha=sha,
            expected_tag="v9.9.9",
        )

        assert result.success is False
        assert result.tag_matches is False

    def test_an_unknown_commit_fails_before_the_installer(
        self, cfg: config.Config, tmp_path: Path,
    ) -> None:
        result = admin.provision_env(
            "demo", cfg, host="localhost", expected_sha="0" * 40,
        )

        assert result.success is False
        assert result.update_script_rc is None, "the installer must not run"
        assert any("checkout failed" in e for e in result.work_errors)
        admin.clear_admin_update_marker()

    def test_it_refuses_an_existing_checkout(
        self, cfg: config.Config, sha: str,
    ) -> None:
        admin.provision_env("demo", cfg, host="localhost", expected_sha=sha)

        with pytest.raises(admin.AdminError, match="already a git checkout"):
            admin.provision_env("demo", cfg, host="localhost", expected_sha=sha)

    def test_it_refuses_a_non_empty_directory(
        self, cfg: config.Config, tmp_path: Path, sha: str,
    ) -> None:
        """Somebody's work, whatever it is."""
        checkout = tmp_path / "checkout"
        checkout.mkdir()
        (checkout / "notes.txt").write_text("mine", encoding="utf-8")

        with pytest.raises(admin.AdminError, match="not empty"):
            admin.provision_env("demo", cfg, host="localhost", expected_sha=sha)
        assert (checkout / "notes.txt").read_text() == "mine"

    def test_an_empty_directory_is_fine(
        self, cfg: config.Config, tmp_path: Path, sha: str,
    ) -> None:
        """Pre-creating a mount point says nothing about its contents."""
        (tmp_path / "checkout").mkdir()

        result = admin.provision_env(
            "demo", cfg, host="localhost", expected_sha=sha,
        )

        assert result.success is True, result.work_errors

    def test_it_refuses_a_symlinked_git_dir(
        self, cfg: config.Config, tmp_path: Path, sha: str,
    ) -> None:
        elsewhere = tmp_path / "elsewhere"
        elsewhere.mkdir()
        (tmp_path / "checkout").symlink_to(elsewhere)

        with pytest.raises(admin.AdminError, match="symlink"):
            admin.provision_env("demo", cfg, host="localhost", expected_sha=sha)

    def test_a_program_with_no_upstream_cannot_be_provisioned(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, sha: str,
    ) -> None:
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state2"))
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg2"))
        (tmp_path / "cfg2").mkdir()
        (tmp_path / "cfg2" / "config.toml").write_text(
            "[programs.demo]\n"
            'kind = "venv"\n'
            f'python = "{tmp_path}/other/.venv/bin/python"\n'
            f'git_dir = "{tmp_path}/other"\n'
            'install_script = "scripts/install.sh"\n',
            encoding="utf-8",
        )
        bare = config.load_config()

        with pytest.raises(admin.AdminError, match="no upstream configured"):
            admin.provision_env(
                "demo", bare, host="localhost", expected_sha=sha,
            )

    def test_a_program_with_no_installer_is_not_guessed_at(
        self, tmp_path: Path, upstream: Path, monkeypatch: pytest.MonkeyPatch,
        sha: str,
    ) -> None:
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state3"))
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg3"))
        (tmp_path / "cfg3").mkdir()
        (tmp_path / "cfg3" / "config.toml").write_text(
            "[programs.demo]\n"
            'kind = "venv"\n'
            f'python = "{tmp_path}/other/.venv/bin/python"\n'
            f'git_dir = "{tmp_path}/other"\n'
            f'upstream = "{upstream}"\n',
            encoding="utf-8",
        )
        bare = config.load_config()

        with pytest.raises(admin.AdminError, match="no install_script"):
            admin.provision_env(
                "demo", bare, host="localhost", expected_sha=sha,
            )


class TestProvisionCommand:
    def test_it_requires_a_pinned_commit(
        self, cfg: config.Config,
    ) -> None:
        """A fleet host is provisioned at a pin, never at a branch tip."""
        result = CliRunner().invoke(main, ["admin", "install", "demo"])

        assert result.exit_code == 2
        assert "--expected-sha is required" in result.output

    def test_it_rejects_a_short_sha(self, cfg: config.Config, sha: str) -> None:
        result = CliRunner().invoke(
            main, ["admin", "install", "demo", "--expected-sha", sha[:12]],
        )

        assert result.exit_code == 2
        assert "full 40-hex SHA" in result.output

    def test_it_provisions_and_reports_as_an_install(
        self, cfg: config.Config, tmp_path: Path, sha: str,
    ) -> None:
        result = CliRunner().invoke(
            main, ["admin", "install", "demo", "--expected-sha", sha],
        )

        assert result.exit_code == 0, result.output
        assert "== admin install demo ==" in result.output
        assert "install_script: scripts/install.sh" in result.output
        assert "-- git clone (rc=0) --" in result.output
        assert "== OK ==" in result.output
        assert _git(tmp_path / "checkout", "rev-parse", "HEAD") == sha

    def test_json_carries_the_operation(
        self, cfg: config.Config, sha: str,
    ) -> None:
        import json

        result = CliRunner().invoke(
            main, ["admin", "install", "demo", "--expected-sha", sha, "--json"],
        )

        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["operation"] == "install"
        assert payload["actual_sha"] == sha
        assert payload["success"] is True


class TestRefusalLeavesNoMarker:
    """A refusal that mutated nothing must not leave state behind.

    The admin-update marker means "an update is in progress, or died
    mid-flight". A provision refused before it touched anything is neither,
    and a marker left for it is one the operator has to clear by hand just to
    find out nothing happened.
    """

    def test_a_refusal_before_the_marker_leaves_none(
        self, cfg: config.Config, tmp_path: Path, sha: str,
    ) -> None:
        checkout = tmp_path / "checkout"
        checkout.mkdir()
        (checkout / "notes.txt").write_text("mine", encoding="utf-8")

        with pytest.raises(admin.AdminError, match="not empty"):
            admin.provision_env("demo", cfg, host="localhost", expected_sha=sha)

        assert admin.read_admin_update_marker() is None

    def test_a_refusal_under_the_marker_clears_it_too(
        self, cfg: config.Config, tmp_path: Path, sha: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The re-check is the guarantee, and it runs *after* the marker is
        taken. It still mutates nothing, so it must still leave nothing --
        otherwise an operator clears a marker by hand only to discover that
        the operation it names never started.

        Observed on the marker itself: a second `provision_env` would hit the
        pre-marker check first and tell us nothing about what the first left.
        """
        calls: list[int] = []
        real = admin._refuse_occupied_git_dir

        def racing(env: str, git_dir: Path) -> None:
            calls.append(1)
            if len(calls) == 1:
                return  # the pre-marker check sees an empty world
            git_dir.mkdir(parents=True, exist_ok=True)
            (git_dir / "appeared.txt").write_text("race", encoding="utf-8")
            real(env, git_dir)

        monkeypatch.setattr(admin, "_refuse_occupied_git_dir", racing)

        with pytest.raises(admin.AdminError, match="not empty"):
            admin.provision_env("demo", cfg, host="localhost", expected_sha=sha)

        assert len(calls) == 2, "the re-check under the marker must run"
        assert admin.read_admin_update_marker() is None
