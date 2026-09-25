"""`vq admin install` applies a program's declared extras, or refuses (#47).

`VenvProgram.extras` says what a venv is *for*. Until v0.26.10 only the managed
daemon transaction applied it, because that is the one path that had proved the
updater is vq's own `scripts/update.sh` before naming an install target on it.
A fresh provision ran the program's `install_script` with its own arguments and
ignored the key: the venv converged on the next managed update, and until then
a host could run a venv without the runtime its program declared. That is the
motivating failure of the feature itself -- a console venv declared
`extras = ["web"]` and provisioned without uvicorn, so `vq web install` refused
it and the unit failed on every start -- reintroduced by the one verb whose job
is to create such a venv.

An accepted key that changes nothing is the failure mode, so install now
applies the floor on the same proof, and **refuses** when it cannot: a notice
would be read after the console was already down. A program that declares no
extras installs exactly as before, and the managed update path is untouched
(its host validation is #25).

These run real git against real local upstreams and a real bash installer, as
the rest of the provisioning lane does: an install path tested only against
mocks is one whose first real use is on a host.
"""
from __future__ import annotations

import os
import shutil
import socket
import subprocess
import sys
from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import admin, config, paths
from vq.cli import main

PROJECT = Path(__file__).resolve().parents[1]

REMEDY = "vq admin update demo"
"""The follow-up an install refusal has to name, spelled as the CLI takes it.

It carries no flags. It used to be spelled with ``--update-script-arg
--recreate-venv ...``, copied from the managed plan, and that command **exits
1** on any environment that is not the daemon-serving one: ``update.sh``
refuses a direct ``--recreate-venv`` unless ``--skip-git`` comes with it. Since
#85 the flags are not needed either, because an ordinary update applies the
declared floor by itself wherever its ``update_script`` is vq's own. A remedy
that cannot be pasted is not a remedy, and one that exits 1 is worse.
"""


def _git(cwd: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=cwd, check=True, capture_output=True, text=True,
    ).stdout.strip()


def _script(path: Path, label: str) -> None:
    """A stub lifecycle script that records the argv it was handed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "#!/usr/bin/env bash\n"
        'printf "%s" "$*" > "$PWD/installer-args"\n'
        f'printf "RAN {label} %s\\n" "$*"\n',
        encoding="utf-8",
    )
    path.chmod(0o755)


def _commit(repo: Path) -> str:
    _git(repo, "add", "-A")
    _git(repo, "commit", "-qm", "first")
    return _git(repo, "rev-parse", "HEAD")


def _init(repo: Path) -> None:
    repo.mkdir(parents=True, exist_ok=True)
    _git(repo, "init", "-q", "-b", "main")
    _git(repo, "config", "user.email", "test@example.invalid")
    _git(repo, "config", "user.name", "Test User")


def _vq_layout(repo: Path) -> None:
    """What :func:`vq.admin.vq_project_root` recognises as a vq checkout."""
    (repo / "pyproject.toml").write_text("", encoding="utf-8")
    (repo / "src" / "vq").mkdir(parents=True)
    (repo / "src" / "vq" / "__init__.py").write_text("", encoding="utf-8")


@pytest.fixture
def vq_upstream(tmp_path: Path) -> tuple[Path, str]:
    """An upstream shaped like vq's own checkout, with vq's own scripts."""
    repo = tmp_path / "upstream"
    _init(repo)
    _vq_layout(repo)
    _script(repo / "scripts" / "install.sh", "install.sh")
    _script(repo / "scripts" / "update.sh", "update.sh")
    return repo, _commit(repo)


@pytest.fixture
def foreign_upstream(tmp_path: Path) -> tuple[Path, str]:
    """A program with its own installer, which has never heard of --extras."""
    repo = tmp_path / "foreign"
    _init(repo)
    _script(repo / "bin" / "setup.sh", "setup.sh")
    return repo, _commit(repo)


@pytest.fixture
def decoy_upstream(tmp_path: Path) -> tuple[Path, str]:
    """A vq checkout whose configured installer only *looks* like vq's.

    ``contrib/scripts/install.sh`` passes any check made on the name alone.
    The proof reads the file, so it does not pass that.
    """
    repo = tmp_path / "decoy"
    _init(repo)
    _vq_layout(repo)
    _script(repo / "scripts" / "install.sh", "install.sh")
    _script(repo / "contrib" / "scripts" / "install.sh", "decoy")
    return repo, _commit(repo)


@pytest.fixture
def not_a_vq_checkout(tmp_path: Path) -> tuple[Path, str]:
    """``scripts/install.sh``, but no vq for it to be vq's own copy of."""
    repo = tmp_path / "lookalike"
    _init(repo)
    _script(repo / "scripts" / "install.sh", "install.sh")
    return repo, _commit(repo)


def _configure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    upstream: Path,
    install_script: str,
    extras: list[str] | None = None,
    update_script: str | None = "scripts/update.sh",
) -> config.Config:
    """Write a one-program config for the provision under test.

    ``update_script`` is a parameter rather than a constant because the two
    shapes behave differently everywhere a message names a follow-up command:
    a program configuring only ``install_script`` is built once and never
    rebuilt, so ``vq admin update`` pulls its checkout and does nothing. This
    helper wrote the key unconditionally, which is how three remedies that
    name that command landed green (#47).
    """
    state = tmp_path / "state"
    cfg_dir = tmp_path / "cfg"
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(state))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(cfg_dir))
    cfg_dir.mkdir(parents=True, exist_ok=True)
    declared = (
        "extras = [" + ", ".join(f'"{name}"' for name in extras) + "]\n"
        if extras
        else ""
    )
    (cfg_dir / "config.toml").write_text(
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
        f'install_script = "{install_script}"\n'
        + (f'update_script = "{update_script}"\n' if update_script else "")
        + declared,
        encoding="utf-8",
    )
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    paths.jobs_dir().mkdir(parents=True, exist_ok=True)
    return config.load_config()


def _installer_argv(tmp_path: Path) -> str:
    return (tmp_path / "checkout" / "installer-args").read_text(encoding="utf-8")


class TestTheDeclarationReachesTheInstaller:
    """The gap #47 names: a declared runtime the fresh venv did not get."""

    def test_a_declared_extra_becomes_the_installers_profile(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        vq_upstream: tuple[Path, str],
    ) -> None:
        upstream, sha = vq_upstream
        cfg = _configure(
            tmp_path, monkeypatch, upstream=upstream,
            install_script="scripts/install.sh", extras=["web"],
        )

        result = admin.provision_env(
            "demo", cfg, host="localhost", expected_sha=sha,
        )

        assert result.success is True, result.work_errors
        # Read back from the installer itself, not from what vq planned.
        assert _installer_argv(tmp_path) == "--extras web"

    def test_the_floor_widens_the_configured_profile_and_never_narrows_it(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        vq_upstream: tuple[Path, str],
    ) -> None:
        """``dev`` plus a declared ``web`` is ``all``, not ``web``.

        Narrowing here would provision a development host without its test
        tooling because somebody declared the console.
        """
        upstream, sha = vq_upstream
        cfg = _configure(
            tmp_path, monkeypatch, upstream=upstream,
            install_script="scripts/install.sh --editable --extras dev",
            extras=["web"],
        )

        result = admin.provision_env(
            "demo", cfg, host="localhost", expected_sha=sha,
        )

        assert result.success is True, result.work_errors
        # Last wins in install.sh's parser, so the floor has to come last.
        assert _installer_argv(tmp_path) == "--editable --extras dev --extras all"

    def test_vqs_own_update_script_may_also_build_the_venv(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        vq_upstream: tuple[Path, str],
    ) -> None:
        """``update.sh --skip-git --recreate-venv`` is a supported installer."""
        upstream, sha = vq_upstream
        cfg = _configure(
            tmp_path, monkeypatch, upstream=upstream,
            install_script="scripts/update.sh --skip-git", extras=["web"],
        )

        result = admin.provision_env(
            "demo", cfg, host="localhost", expected_sha=sha,
        )

        assert result.success is True, result.work_errors
        assert "RAN update.sh" in result.update_script_output
        assert _installer_argv(tmp_path) == "--skip-git --extras web"

    def test_an_operator_profile_request_decides_over_the_configured_one(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        vq_upstream: tuple[Path, str],
    ) -> None:
        """The same shape the managed update has: the request sets the base.

        It replaces the configured profile rather than widening it -- an
        operator asking for ``web`` gets ``web``, not ``all`` -- and the
        declaration is the floor under whatever it set.
        """
        upstream, sha = vq_upstream
        cfg = _configure(
            tmp_path, monkeypatch, upstream=upstream,
            install_script="scripts/install.sh --editable --extras dev",
            extras=["web"],
        )

        result = admin.provision_env(
            "demo", cfg, host="localhost", expected_sha=sha,
            install_script_args=["--extras", "web"],
        )

        assert result.success is True, result.work_errors
        # Nothing appended: the request already installs the declaration, and
        # install.sh keeps the last --extras it is given.
        assert _installer_argv(tmp_path) == "--editable --extras dev --extras web"

    def test_a_configured_profile_that_already_covers_it_is_left_alone(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        vq_upstream: tuple[Path, str],
    ) -> None:
        """A declaration the installer already satisfies changes nothing.

        The argv is the one an operator wrote, byte for byte: vq appends the
        floor when it raises the profile and stays quiet when it does not.
        """
        upstream, sha = vq_upstream
        cfg = _configure(
            tmp_path, monkeypatch, upstream=upstream,
            install_script="scripts/install.sh --copied --extras web",
            extras=["web"],
        )

        result = admin.provision_env(
            "demo", cfg, host="localhost", expected_sha=sha,
        )

        assert result.success is True, result.work_errors
        assert _installer_argv(tmp_path) == "--copied --extras web"

    def test_an_operator_request_that_drops_a_declared_extra_is_refused(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        vq_upstream: tuple[Path, str],
    ) -> None:
        """Two instructions contradicting each other; say which two.

        Silently widening ``core`` to ``web`` would install something nobody
        typed, which is what the managed update refuses for the same reason.
        """
        upstream, sha = vq_upstream
        cfg = _configure(
            tmp_path, monkeypatch, upstream=upstream,
            install_script="scripts/install.sh", extras=["web"],
        )

        with pytest.raises(admin.AdminError) as excinfo:
            admin.provision_env(
                "demo", cfg, host="localhost", expected_sha=sha,
                install_script_args=["--extras", "core"],
            )

        message = str(excinfo.value)
        assert "does not install web" in message
        assert "['web'] on this program declares" in message
        assert not (tmp_path / "checkout" / "installer-args").exists(), (
            "the installer must not run on a contradicted request"
        )

    def test_an_unknown_profile_name_is_refused_before_the_shell_sees_it(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        vq_upstream: tuple[Path, str],
    ) -> None:
        upstream, sha = vq_upstream
        cfg = _configure(
            tmp_path, monkeypatch, upstream=upstream,
            install_script="scripts/install.sh --extras wbe", extras=["web"],
        )

        with pytest.raises(admin.AdminError, match="extras profile 'wbe'"):
            admin.provision_env(
                "demo", cfg, host="localhost", expected_sha=sha,
            )

        assert not (tmp_path / "checkout" / "installer-args").exists()


class TestAnInstallThatCannotApplyTheDeclarationIsRefused:
    """Option 1's refusal, and the remedy it has to name.

    A notice would be the cheaper change and the worse one: an ignored notice
    produces exactly the venv this issue is about.
    """

    def test_a_foreign_installer_is_refused_before_anything_is_created(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        foreign_upstream: tuple[Path, str],
    ) -> None:
        upstream, sha = foreign_upstream
        cfg = _configure(
            tmp_path, monkeypatch, upstream=upstream,
            install_script="bin/setup.sh", extras=["web"],
        )

        with pytest.raises(admin.AdminError) as excinfo:
            admin.provision_env(
                "demo", cfg, host="localhost", expected_sha=sha,
            )

        assert "declares extras ['web']" in str(excinfo.value)
        assert not (tmp_path / "checkout").exists(), "nothing may be cloned"
        assert admin.read_admin_update_marker() is None, (
            "a refusal that mutated nothing must not leave a marker"
        )

    def test_the_refusal_names_both_ways_out(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        foreign_upstream: tuple[Path, str],
    ) -> None:
        upstream, sha = foreign_upstream
        cfg = _configure(
            tmp_path, monkeypatch, upstream=upstream,
            install_script="bin/setup.sh", extras=["web"],
        )

        with pytest.raises(admin.AdminError) as excinfo:
            admin.provision_env(
                "demo", cfg, host="localhost", expected_sha=sha,
            )

        message = str(excinfo.value)
        assert "scripts/install.sh" in message
        assert REMEDY in message

    def test_the_cli_refuses_and_prints_the_remedy(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        foreign_upstream: tuple[Path, str],
    ) -> None:
        """The operator-visible half: a usage error, not a traceback."""
        upstream, sha = foreign_upstream
        _configure(
            tmp_path, monkeypatch, upstream=upstream,
            install_script="bin/setup.sh", extras=["web"],
        )

        invoked = CliRunner().invoke(
            main, ["admin", "install", "demo", "--expected-sha", sha],
        )

        assert invoked.exit_code == 2, invoked.output
        assert REMEDY in invoked.output
        assert "--recreate-venv" not in invoked.output, (
            "the remedy must not carry a flag update.sh refuses outside a "
            "managed transaction"
        )
        assert not (tmp_path / "checkout").exists()

    def test_a_script_that_merely_has_the_name_is_not_vqs_own(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        decoy_upstream: tuple[Path, str],
    ) -> None:
        """The proof reads the file; the pre-clone check reads the name.

        ``contrib/scripts/install.sh`` gets past the name, and is refused
        after the clone -- before anything is built.
        """
        upstream, sha = decoy_upstream
        cfg = _configure(
            tmp_path, monkeypatch, upstream=upstream,
            install_script="contrib/scripts/install.sh", extras=["web"],
        )

        with pytest.raises(admin.AdminError) as excinfo:
            admin.provision_env(
                "demo", cfg, host="localhost", expected_sha=sha,
            )

        assert "contrib/scripts/install.sh" in str(excinfo.value)
        assert REMEDY in str(excinfo.value)
        checkout = tmp_path / "checkout"
        assert _git(checkout, "rev-parse", "HEAD") == sha, "the clone ran"
        assert not (checkout / "installer-args").exists(), (
            "the installer must not run on an unproved declaration"
        )

    def test_a_lookalike_checkout_is_refused_after_the_clone(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        not_a_vq_checkout: tuple[Path, str],
    ) -> None:
        """A tree with the right file name and no vq in it proves nothing."""
        upstream, sha = not_a_vq_checkout
        cfg = _configure(
            tmp_path, monkeypatch, upstream=upstream,
            install_script="scripts/install.sh", extras=["web"],
        )

        with pytest.raises(admin.AdminError) as excinfo:
            admin.provision_env(
                "demo", cfg, host="localhost", expected_sha=sha,
            )

        assert "is not a vq tree" in str(excinfo.value)
        assert REMEDY not in str(excinfo.value), (
            "no vq script is in this checkout, so no update can apply it"
        )
        assert "src/vq" in str(excinfo.value), "say what the checkout lacked"
        assert not (tmp_path / "checkout" / "installer-args").exists()


class TestAProgramWithoutADeclarationInstallsAsBefore:
    """The unaffected case, pinned: no declaration, no new behaviour."""

    def test_nothing_is_appended(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        vq_upstream: tuple[Path, str],
    ) -> None:
        upstream, sha = vq_upstream
        cfg = _configure(
            tmp_path, monkeypatch, upstream=upstream,
            install_script="scripts/install.sh --editable --extras dev",
        )

        result = admin.provision_env(
            "demo", cfg, host="localhost", expected_sha=sha,
        )

        assert result.success is True, result.work_errors
        assert _installer_argv(tmp_path) == "--editable --extras dev"

    def test_forwarded_operator_arguments_are_untouched(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        vq_upstream: tuple[Path, str],
    ) -> None:
        upstream, sha = vq_upstream
        cfg = _configure(
            tmp_path, monkeypatch, upstream=upstream,
            install_script="scripts/install.sh",
        )

        result = admin.provision_env(
            "demo", cfg, host="localhost", expected_sha=sha,
            install_script_args=["--editable", "--extras", "dev"],
        )

        assert result.success is True, result.work_errors
        assert _installer_argv(tmp_path) == "--editable --extras dev"

    def test_a_foreign_installer_is_still_perfectly_fine(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        foreign_upstream: tuple[Path, str],
    ) -> None:
        """Only a declaration vq cannot apply is refused, never an installer.

        vibe-qc and vibe-view programs are provisioned by their own scripts
        and declare nothing; this verb must keep working for them.
        """
        upstream, sha = foreign_upstream
        cfg = _configure(
            tmp_path, monkeypatch, upstream=upstream,
            install_script="bin/setup.sh",
        )

        result = admin.provision_env(
            "demo", cfg, host="localhost", expected_sha=sha,
        )

        assert result.success is True, result.work_errors
        assert "RAN setup.sh" in result.update_script_output
        assert _installer_argv(tmp_path) == ""


class TestTheProofIsTheOneTheUpdatePathUses:
    """Both paths ask :func:`vq.admin._vq_own_script_proof`, not two questions."""

    def test_the_nested_checkout_layout_is_recognised(
        self, tmp_path: Path,
    ) -> None:
        """``git_dir`` may be the parent of the vq tree, as on legacy hosts."""
        root = tmp_path / "repo"
        project = root / "vibe-queue"
        project.mkdir(parents=True)
        _vq_layout(project)
        _script(project / "scripts" / "install.sh", "install.sh")
        prog = config.VenvProgram(
            kind="venv",
            python=str(project / ".venv" / "bin" / "python"),
            git_dir=str(root),
            install_script="vibe-queue/scripts/install.sh",
            update_script="vibe-queue/scripts/update.sh",
            extras=["web"],
        )

        admin._refuse_install_that_cannot_apply_declared_extras("demo", prog)
        assert admin._declared_extras_install_args(
            "demo", prog, supplied=[],
        ) == ["--extras", "web"]

    def test_a_symlink_to_the_canonical_script_is_the_canonical_script(
        self, tmp_path: Path,
    ) -> None:
        """Identity is the resolved file, not the spelling that reached it."""
        project = tmp_path / "repo"
        project.mkdir()
        _vq_layout(project)
        _script(project / "scripts" / "install.sh", "install.sh")
        (project / "scripts" / "provision.sh").symlink_to(
            project / "scripts" / "install.sh"
        )
        prog = config.VenvProgram(
            kind="venv",
            python=str(project / ".venv" / "bin" / "python"),
            git_dir=str(project),
            install_script="scripts/provision.sh",
            update_script="scripts/update.sh",
            extras=["web"],
        )

        resolved, proven = admin._vq_own_script_proof(
            prog, "scripts/provision.sh", names=admin._VQ_OWN_VENV_SCRIPTS,
        )
        assert proven is True
        assert resolved.name == "install.sh"


@pytest.fixture
def serving_program(tmp_path: Path) -> tuple[config.VenvProgram, Path]:
    """A managed venv recorded as ``core``, shaped like the console host's.

    A real virtualenv, because the managed path proves the environment reports
    an *external* base interpreter before it will name it on the updater's
    command line. ``git_dir`` is this checkout, so the canonical
    ``scripts/update.sh`` the proof compares against is the real one.
    """
    venv = tmp_path / "vibe-queue" / ".venv"
    subprocess.run(
        [sys.executable, "-m", "venv", "--without-pip", str(venv)],
        check=True,
        capture_output=True,
    )
    (venv / ".vq-install-metadata").write_text(
        "version=1\nextras=core\neditable=1\n", encoding="utf-8",
    )
    prog = config.VenvProgram(
        kind="venv",
        python=str(venv / "bin" / "python"),
        git_dir=str(PROJECT),
        update_script="scripts/update.sh",
    )
    return prog, venv


class TestTheManagedUpdatePathIsUnchanged:
    """Pinned, because its host validation is #25 and must not be perturbed.

    The proof moved into a shared function; what the managed transaction
    accepts, what it plans and what it says when it refuses did not.
    """

    def test_the_planned_argv_of_a_declared_serving_env(
        self, serving_program: tuple[config.VenvProgram, Path],
    ) -> None:
        prog, venv = serving_program
        prog = prog.model_copy(update={"extras": ["web"]})

        args = admin._managed_update_script_args(prog, None)

        assert args[:2] == ["--venv", str(venv.resolve())]
        assert args[2] == "--python"
        base = Path(args[3])
        assert base.is_absolute() and base.is_file()
        assert venv.resolve() not in base.parents
        assert args[4:] == ["--extras", "web", "--editable", "--recreate-venv"]

    def test_an_undeclared_serving_env_still_gets_its_recorded_profile(
        self, serving_program: tuple[config.VenvProgram, Path],
    ) -> None:
        prog, _venv = serving_program

        args = admin._managed_update_script_args(prog, None)

        assert args[4:] == ["--extras", "core", "--editable", "--recreate-venv"]

    def test_an_operator_request_below_the_declaration_is_still_refused(
        self, serving_program: tuple[config.VenvProgram, Path],
    ) -> None:
        prog, _venv = serving_program
        prog = prog.model_copy(update={"extras": ["web"]})

        with pytest.raises(admin.AdminError, match="does not install web"):
            admin._managed_update_script_args(
                prog, ["--recreate-venv", "--extras", "core"],
            )

    def test_an_updater_that_is_not_vqs_own_is_refused_in_the_same_words(
        self, serving_program: tuple[config.VenvProgram, Path],
    ) -> None:
        prog, _venv = serving_program
        prog = prog.model_copy(
            update={"update_script": "scripts/_venv_helpers.sh"},
        )

        with pytest.raises(
            admin.AdminError,
            match="managed daemon update requires the checkout's canonical",
        ):
            admin._managed_update_script_args(prog, None)

    def test_an_ordinary_update_of_this_program_now_applies_the_floor(
        self, monkeypatch: pytest.MonkeyPatch,
        serving_program: tuple[config.VenvProgram, Path],
    ) -> None:
        """The same proof, on the lane that used to skip it (#85).

        This program's ``update_script`` is the checkout's own
        ``scripts/update.sh``, which is exactly what lets the managed
        transaction name an install target on it. An ordinary update of it
        now applies the declaration on that same proof, so the note this test
        once asserted -- "which this update does not apply" -- would be false
        for it. What it says instead is which profile it appended.

        The managed plan above is unchanged; this is the contrast, and the
        reason the two are asserted side by side.
        """
        prog, _venv = serving_program
        prog = prog.model_copy(update={"extras": ["web"]})
        said: list[str] = []
        monkeypatch.setattr(
            admin.output, "narrate", lambda text, *a, **k: said.append(text),
        )

        args = admin._declared_extras_on_ordinary_update("demo", prog, supplied=[])

        assert args == ["--extras", "web"]
        assert said and "applies it" in said[0]
        # A provision is still not offered anywhere near this: the checkout
        # exists -- that is what is being updated -- and
        # `_refuse_occupied_git_dir` refuses to provision over one.
        assert "vq admin install" not in said[0]
        # Nor is the per-update profile request, which is now the long way
        # round to what this update just did on its own.
        assert REMEDY not in said[0]


class TestAProgramVqBuildsOnceAndNeverRebuilds:
    """`install_script` and no `update_script`: one route, and it fires once.

    The model admits this shape because a provision really does apply the
    floor (#47). What it does not have is any way back: `vq admin update` runs
    the program's `update_script` and nothing else, so on this shape it pulls
    the checkout, reports success and rebuilds nothing. Three messages named
    `vq admin update` as the way out regardless, and this lane never noticed
    because its config helper wrote `update_script` unconditionally.
    """

    def test_the_provision_still_applies_the_floor(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        vq_upstream: tuple[Path, str],
    ) -> None:
        """The one route this shape has works, which is why it is admitted."""
        upstream, sha = vq_upstream
        cfg = _configure(
            tmp_path, monkeypatch, upstream=upstream,
            install_script="scripts/install.sh", extras=["web"],
            update_script=None,
        )
        assert cfg.programs["demo"].update_script is None

        result = admin.provision_env(
            "demo", cfg, host="localhost", expected_sha=sha,
        )

        assert result.success is True, result.work_errors
        assert _installer_argv(tmp_path) == "--extras web"

    def test_the_refusal_does_not_present_an_inert_command_as_the_way_out(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        foreign_upstream: tuple[Path, str],
    ) -> None:
        """Naming `vq admin update` unqualified here is a remedy that exits 0.

        The operator runs it, git pulls, vq reports success and the venv is
        exactly as wrong as before -- the two-day dead end the refusal's
        wording exists to prevent, one level further on.
        """
        upstream, sha = foreign_upstream
        cfg = _configure(
            tmp_path, monkeypatch, upstream=upstream,
            install_script="bin/setup.sh", extras=["web"],
            update_script=None,
        )

        with pytest.raises(admin.AdminError) as excinfo:
            admin.provision_env("demo", cfg, host="localhost", expected_sha=sha)

        message = str(excinfo.value)
        assert "configures no update_script" in message
        assert "applies no declaration" in message
        # The command is still named, because it is the right one *after* the
        # config edit the same sentence asks for -- and only after it.
        assert "scripts/update.sh makes that second route real" in message
        assert REMEDY in message
        assert message.index("makes that second route real") < message.rindex(REMEDY)

    def test_the_two_shapes_are_told_different_things(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        foreign_upstream: tuple[Path, str],
    ) -> None:
        """A program vq can rebuild keeps the plain two-ways-out wording."""
        upstream, sha = foreign_upstream
        cfg = _configure(
            tmp_path, monkeypatch, upstream=upstream,
            install_script="bin/setup.sh", extras=["web"],
        )

        with pytest.raises(admin.AdminError) as excinfo:
            admin.provision_env("demo", cfg, host="localhost", expected_sha=sha)

        message = str(excinfo.value)
        assert "drop the declaration, provision, then restore it" in message
        assert "configures no update_script" not in message
        assert REMEDY in message
        assert "--recreate-venv" not in message

    def test_the_cli_prints_the_shape_specific_refusal(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        foreign_upstream: tuple[Path, str],
    ) -> None:
        upstream, sha = foreign_upstream
        _configure(
            tmp_path, monkeypatch, upstream=upstream,
            install_script="bin/setup.sh", extras=["web"],
            update_script=None,
        )

        invoked = CliRunner().invoke(
            main, ["admin", "install", "demo", "localhost", "--expected-sha", sha],
        )

        assert invoked.exit_code != 0
        assert "configures no update_script" in invoked.output

    def test_the_ordinary_update_note_does_not_describe_a_rebuild(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
    ) -> None:
        """Nothing is rebuilt and no metadata is read, so the note says so.

        The note claimed "This update rebuilds the environment with the profile
        its .vq-install-metadata records", which on this shape is a sentence
        about an operation that does not happen -- the note doing the very
        thing its docstring says it exists to prevent.
        """
        prog = config.VenvProgram(
            kind="venv",
            python=str(tmp_path / ".venv" / "bin" / "python"),
            git_dir=str(tmp_path),
            install_script="scripts/install.sh",
            extras=["web"],
        )
        said: list[str] = []
        monkeypatch.setattr(
            admin.output, "narrate", lambda text, *a, **k: said.append(text),
        )

        admin._warn_if_declared_extras_are_inert("demo", prog)

        assert said, "a declaration this update cannot apply is never silent"
        assert ".vq-install-metadata" not in said[0]
        assert "rebuilds nothing" in said[0]
        assert "vq admin install" not in said[0]
        # No command is offered that would exit 0 and change nothing.
        assert "--recreate-venv" not in said[0]
        assert "update_script" in said[0]


class TestTheSecondRouteRestoresTheDeclaration:
    """"Drop the declaration, provision, and apply the profile with `vq admin
    update ENV`" exited 0 and installed nothing.

    The command lost its ``--extras PROFILE`` when ``--recreate-venv`` was
    taken out of it (#47), on the grounds that an ordinary update applies the
    declared floor by itself (#85). It does -- while the floor is declared.
    The same sentence had just told the operator to drop it, and an update of
    a program declaring nothing appends no ``--extras``. The route has four
    steps and the remedy named three.
    """

    def test_an_update_applies_the_floor_only_while_it_is_declared(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        vq_upstream: tuple[Path, str],
    ) -> None:
        """The consumer the remedy has to agree with."""
        upstream, sha = vq_upstream
        _configure(
            tmp_path, monkeypatch, upstream=upstream,
            install_script="scripts/install.sh",
        )
        _git(tmp_path, "clone", "-q", str(upstream), str(tmp_path / "checkout"))
        dropped = config.load_config().programs["demo"]
        assert isinstance(dropped, config.VenvProgram)
        assert not dropped.extras
        monkeypatch.setattr(admin.output, "narrate", lambda *a, **k: None)

        assert admin._declared_extras_on_ordinary_update(
            "demo", dropped, supplied=[],
        ) == [], "with the declaration dropped, nothing is applied"
        restored = dropped.model_copy(update={"extras": ["web"]})
        assert admin._declared_extras_on_ordinary_update(
            "demo", restored, supplied=[],
        ) == ["--extras", "web"]

    @pytest.mark.parametrize(
        "update_script", ["scripts/update.sh", None], ids=["vq", "absent"],
    )
    def test_the_refusal_before_the_clone_says_to_restore_it(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        foreign_upstream: tuple[Path, str], update_script: str | None,
    ) -> None:
        upstream, sha = foreign_upstream
        cfg = _configure(
            tmp_path, monkeypatch, upstream=upstream,
            install_script="bin/setup.sh", extras=["web"],
            update_script=update_script,
        )

        with pytest.raises(admin.AdminError) as excinfo:
            admin.provision_env("demo", cfg, host="localhost", expected_sha=sha)

        message = str(excinfo.value)
        drop = message.index("drop the declaration, provision, then restore it")
        assert drop < message.rindex(REMEDY), (
            "the update is the step after the restore, not after the drop"
        )
        assert "Before any of that" not in message, "nothing was cloned"

    def test_the_refusal_after_the_clone_route_works_when_followed(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        decoy_upstream: tuple[Path, str],
    ) -> None:
        """Followed literally, through the CLI it names, to a provision that
        succeeds.

        The refusal comes after the clone, which leaves the checkout, and
        inside the provision's marker, which it leaves FAILED. Each refuses
        the next provision on its own -- both halves are shown below -- so a
        message that names only one of them sends the operator into the other
        (fifth and sixth reviews of 8b2e0dc).
        """
        upstream, sha = decoy_upstream
        cfg = _configure(
            tmp_path, monkeypatch, upstream=upstream,
            install_script="contrib/scripts/install.sh", extras=["web"],
        )
        checkout = tmp_path / "checkout"

        def refused() -> str:
            with pytest.raises(admin.AdminError) as excinfo:
                admin.provision_env(
                    "demo", cfg, host="localhost", expected_sha=sha,
                )
            return str(excinfo.value)

        message = refused()
        assert f"had already cloned into {checkout} when it refused" in message
        assert (
            f"on the machine this provision ran on ({socket.gethostname()}), "
            f"empty {checkout} and run `vq admin clear-update-marker "
            "localhost --yes`"
        ) in message
        # Where it is run, over which routes, is pinned end to end in
        # tests/test_admin_install_remedy_routes_v0_26_14.py.

        # The checkout alone refuses a provision.
        assert admin.clear_admin_update_marker() is not None
        with pytest.raises(admin.AdminError, match="refusing to provision into"):
            admin.provision_env("demo", cfg, host="localhost", expected_sha=sha)
        # The marker alone refuses one too.
        shutil.rmtree(checkout)
        refused()
        shutil.rmtree(checkout)
        with pytest.raises(admin.AdminMarkerPresent):
            admin.provision_env("demo", cfg, host="localhost", expected_sha=sha)

        # The marker command exactly as printed.
        cleared = CliRunner().invoke(
            main, ["admin", "clear-update-marker", "localhost", "--yes"],
        )
        assert cleared.exit_code == 0, cleared.output
        assert admin.read_admin_update_marker() is None

        # And the first way out, as printed, now works.
        cfg = _configure(
            tmp_path, monkeypatch, upstream=upstream,
            install_script="scripts/install.sh", extras=["web"],
        )
        result = admin.provision_env(
            "demo", cfg, host="localhost", expected_sha=sha,
        )
        assert result.success is True, result.work_errors
        assert _installer_argv(tmp_path) == "--extras web"

    @pytest.mark.parametrize(
        ("supplied", "said"),
        [
            (["--extras", "core"], "does not install web"),
            (["--extras", "no-such-profile"], "no-such-profile"),
            (["--extras"], "--extras"),
        ],
        ids=["contradiction", "unknown-profile", "no-profile"],
    )
    def test_every_refusal_after_the_clone_says_what_it_left(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        vq_upstream: tuple[Path, str], supplied: list[str], said: str,
    ) -> None:
        """Not only the extras refusal: each refusal of the operator's own
        profile request is raised at the same point, after the clone, and
        leaves the same two leftovers behind."""
        upstream, sha = vq_upstream
        cfg = _configure(
            tmp_path, monkeypatch, upstream=upstream,
            install_script="scripts/install.sh", extras=["web"],
        )

        with pytest.raises(admin.AdminError) as excinfo:
            admin.provision_env(
                "demo", cfg, host="localhost", expected_sha=sha,
                install_script_args=supplied,
            )

        message = str(excinfo.value)
        assert said in message
        assert "`vq admin clear-update-marker localhost --yes`" in message
        assert (tmp_path / "checkout" / ".git").exists(), "the note is true"
        assert admin.read_admin_update_marker() is not None, "so is this half"

    def test_an_empty_mount_point_is_not_read_as_a_foreign_checkout(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        foreign_upstream: tuple[Path, str],
    ) -> None:
        """A provision accepts an empty git_dir an operator created. Before
        the clone there is still no checkout in it, so the second route is
        named conditionally -- not refused with "its update_script is not one
        of vq's own scripts", which the config it names says otherwise."""
        upstream, sha = foreign_upstream
        cfg = _configure(
            tmp_path, monkeypatch, upstream=upstream,
            install_script="bin/setup.sh", extras=["web"],
        )
        (tmp_path / "checkout").mkdir()

        with pytest.raises(admin.AdminError) as excinfo:
            admin.provision_env("demo", cfg, host="localhost", expected_sha=sha)

        message = str(excinfo.value)
        assert "If update_script is the checkout's own scripts/update.sh" in message
        assert "not one of vq's own scripts either" not in message

    def test_a_checkout_with_no_vq_in_it_is_not_offered_vqs_routes(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        not_a_vq_checkout: tuple[Path, str],
    ) -> None:
        """install_script already names scripts/install.sh; the checkout has
        none of vq's. Telling the operator to point it there, or at
        scripts/update.sh, names two routes that cannot exist."""
        upstream, sha = not_a_vq_checkout
        cfg = _configure(
            tmp_path, monkeypatch, upstream=upstream,
            install_script="scripts/install.sh", extras=["web"],
        )

        with pytest.raises(admin.AdminError) as excinfo:
            admin.provision_env("demo", cfg, host="localhost", expected_sha=sha)

        message = str(excinfo.value)
        assert "is not a vq tree" in message
        assert "`vq admin clear-update-marker localhost --yes`" in message
        assert "Point install_script" not in message
        assert "makes that second route real" not in message
        assert "drop the declaration" in message

    @pytest.mark.parametrize(
        ("fixture", "install_script", "update_script", "branch"),
        [
            ("foreign_upstream", "bin/setup.sh", "scripts/update.sh",
             "If update_script is the checkout's own"),
            # The decoy holds vq's scripts/install.sh and no update.sh, and
            # install.sh is one of vq's own venv scripts.
            ("decoy_upstream", "contrib/scripts/install.sh",
             "scripts/install.sh", "or drop the declaration, provision"),
            ("decoy_upstream", "contrib/scripts/install.sh",
             "contrib/scripts/install.sh", "not one of vq's own scripts either"),
            ("foreign_upstream", "bin/setup.sh", None,
             "configures no update_script"),
        ],
        ids=["unknown", "vq", "foreign", "absent"],
    )
    def test_the_second_route_says_which_installer_it_needs(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        request: pytest.FixtureRequest, fixture: str, install_script: str,
        update_script: str | None, branch: str,
    ) -> None:
        """``scripts/update.sh`` refuses a venv vq's scripts did not build, so
        the restore-then-update route is real only for an installer that
        builds through ``scripts/install.sh``. Every wording of the route
        carries that, not only the one reached before the clone."""
        upstream, sha = request.getfixturevalue(fixture)
        cfg = _configure(
            tmp_path, monkeypatch, upstream=upstream,
            install_script=install_script, extras=["web"],
            update_script=update_script,
        )

        with pytest.raises(admin.AdminError) as excinfo:
            admin.provision_env("demo", cfg, host="localhost", expected_sha=sha)

        message = str(excinfo.value)
        assert branch in message
        caveat = message.index("scripts/update.sh refuses a venv vq's own")
        assert message.index("then restore it") < caveat

    def test_update_sh_refuses_a_venv_vq_did_not_build(
        self, tmp_path: Path,
    ) -> None:
        """The consumer the caveat describes, run rather than grepped: if
        update.sh ever accepts an unmarked venv, the caveat is what should
        go. Dry run, on a copy of this checkout's scripts, no network."""
        checkout = tmp_path / "vq"
        _init(checkout)
        _vq_layout(checkout)
        shutil.copytree(PROJECT / "scripts", checkout / "scripts")
        _commit(checkout)
        subprocess.run(
            [sys.executable, "-m", "venv", "--without-pip",
             str(checkout / ".venv")],
            check=True,
        )

        ran = subprocess.run(
            ["bash", "scripts/update.sh", "--skip-git", "--dry-run",
             "--extras", "web"],
            cwd=checkout, capture_output=True, text=True, timeout=120,
            env={**os.environ, "VQ_VENV": str(checkout / ".venv")},
        )

        assert ran.returncode != 0, ran.stdout + ran.stderr
        assert "refusing unowned virtualenv" in ran.stderr, ran.stderr

    def test_a_foreign_update_script_is_told_what_it_lacks(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        decoy_upstream: tuple[Path, str],
    ) -> None:
        """The third shape, reachable only once the checkout can be read."""
        upstream, sha = decoy_upstream
        cfg = _configure(
            tmp_path, monkeypatch, upstream=upstream,
            install_script="contrib/scripts/install.sh", extras=["web"],
            update_script="contrib/scripts/install.sh",
        )

        with pytest.raises(admin.AdminError) as excinfo:
            admin.provision_env("demo", cfg, host="localhost", expected_sha=sha)

        message = str(excinfo.value)
        assert "its update_script is not one of vq's own scripts" in message
        assert "configures no update_script" not in message
        assert message.index("makes that second route real") < message.index(
            "then restore it"
        ) < message.rindex(REMEDY)


def test_the_documented_remedy_restores_the_declaration() -> None:
    for page in ("operations.md", "lifecycle.md"):
        text = _reflowed(page)
        assert "drop the declaration, provision, and apply the profile" not in text
        assert "drop the declaration, provision, then restore it" in text, page
    help_text = CliRunner().invoke(main, ["admin", "install", "--help"]).output
    flat = " ".join(help_text.split())
    assert "restore the declaration and apply it" in flat


def _reflowed(name: str) -> str:
    """A page's text with its wrapping removed.

    A remedy is documented whether or not it fell across a line break, and a
    single-line pattern that misses one is indistinguishable from a page that
    never mentioned it.
    """
    return " ".join((PROJECT / "docs" / name).read_text(encoding="utf-8").split())


@pytest.mark.parametrize("page", ["operations.md", "lifecycle.md"])
def test_the_install_refusal_is_documented_where_operators_look(
    page: str,
) -> None:
    """A refusal nobody can look up is a refusal that costs an afternoon."""
    text = _reflowed(page)
    assert "vq admin install" in text
    assert "vq admin update vibeqc-queue" in text, (
        "the page must name the remedy exactly as the CLI takes it"
    )


def test_the_command_help_states_the_refusal_and_the_remedy() -> None:
    """A behaviour change an operator meets at the CLI is stated at the CLI.

    The remedy is checked with the wrapping and the shell continuation taken
    out, so it is pinned as a command rather than as a paragraph.
    """
    help_text = CliRunner().invoke(main, ["admin", "install", "--help"]).output
    flat = " ".join(help_text.replace("\\", " ").split())

    assert "refused" in flat
    assert "vq admin update ENV" in flat
    assert "vq admin update ENV --update-script-arg --recreate-venv" not in flat, (
        "the help must not print a command update.sh refuses"
    )


def test_the_config_key_says_where_it_now_takes_effect() -> None:
    """The key is read in the example config; the change belongs beside it."""
    example = _reflowed("config.toml.example")
    assert 'extras = ["web"]' in example
    assert "a fresh `vq admin install`" in example
    assert "REFUSED" in example
