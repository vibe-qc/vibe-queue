"""A managed venv declares the extras it requires, and a console refuses to
install where it could not run.

host_0, 2026-09-09: one vq-managed virtualenv served both ``vq-daemon`` and
``vq-web``. It had been rebuilt with the ``core`` profile, so it had no
uvicorn; ``vq web install`` pointed the console unit at it anyway, and systemd
restarted that unit 5230 times over the next two days, each start exiting on
"vq web requires the 'web' extra". Every supported route out was closed:
``update_script`` may not carry ``--extras`` (only source selectors are
allowed into a serving venv's install line), ``vq web install`` had no notion
of extras at all, and running pip inside a vq-managed checkout is forbidden on
a fleet host.

Two halves close it, and this file covers the seam between them:

* :attr:`vq.config.VenvProgram.extras` declares what a venv is *for*, and
  :func:`vq.admin._resolve_extras_profile` folds that into what
  ``.vq-install-metadata`` says it *was*, widening only.
* :func:`vq.web.install.require_console_runtime` refuses an install whose unit
  could not start, and words the remedy for the host it is on.
"""
from __future__ import annotations

import subprocess
import sys
import tomllib
from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import admin, cli, config
from vq.web import install as web_install

PROJECT = Path(__file__).resolve().parents[1]
UPDATE_SH = PROJECT / "scripts" / "update.sh"
HELPER = PROJECT / "scripts" / "_venv_helpers.sh"


def _pyproject_extras() -> dict[str, list[str]]:
    with (PROJECT / "pyproject.toml").open("rb") as handle:
        data = tomllib.load(handle)
    return data["project"]["optional-dependencies"]


def _run_helper(body: str, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", "-c", f'set -euo pipefail; . "{HELPER}"; {body}', "_", *args],
        cwd=PROJECT,
        capture_output=True,
        text=True,
        check=False,
    )


@pytest.fixture
def serving_program(tmp_path: Path) -> config.VenvProgram:
    """A managed venv recorded as ``core``, shaped like host_0's.

    A real virtualenv, because ``_managed_update_script_args`` proves the
    environment reports an *external* base interpreter before it will name it
    on the updater's command line.
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
    (venv / ".vq-checkout-owner").write_text(
        f"version=1\nproject={PROJECT.resolve()}\n", encoding="utf-8",
    )
    return config.VenvProgram(
        kind="venv",
        python=str(venv / "bin" / "python"),
        git_dir=str(PROJECT),
        update_script="scripts/update.sh",
    )


class TestProfileTable:
    """``_EXTRAS_PROFILES`` is a copy of a shell case statement. Prove it."""

    def test_known_extras_match_pyproject(self) -> None:
        assert set(config.KNOWN_PROGRAM_EXTRAS) == set(_pyproject_extras())

    @pytest.mark.parametrize(("profile", "contents"), admin._EXTRAS_PROFILES)
    def test_each_profile_installs_what_the_updater_thinks_it_does(
        self, profile: str, contents: frozenset[str],
    ) -> None:
        result = _run_helper(
            'vq_extras_to_spec "$1" SPEC; printf "%s" "$SPEC"', profile,
        )
        assert result.returncode == 0, result.stderr
        spec = result.stdout
        installed = set(spec.strip("[]").split(",")) - {""}
        assert installed == set(contents), f"{profile} -> {spec!r}"

    def test_the_web_profile_is_what_carries_uvicorn(self) -> None:
        """The last link: ``--extras web`` really does mean uvicorn."""
        web_extra = _pyproject_extras()["web"]
        assert any(req.startswith("uvicorn") for req in web_extra), web_extra
        assert _run_helper('vq_profile_has_web "$1"', "web").returncode == 0


class TestResolveExtrasProfile:
    @pytest.mark.parametrize(
        ("recorded", "declared", "expected"),
        [
            # No declaration: exactly the pre-existing behaviour.
            ("core", [], "core"),
            ("dev", [], "dev"),
            # The host_0 case: a daemon venv that must also serve the
            # console.
            ("core", ["web"], "web"),
            ("web", ["web"], "web"),
            # Declaring less than the venv has never takes anything away:
            # the managed path rebuilds with --recreate-venv, so a dropped
            # extra is really gone.
            ("all", ["web"], "all"),
            # web + dev is not a published profile; the smallest one that
            # contains both is.
            ("dev", ["web"], "all"),
            ("test", ["web"], "all"),
        ],
    )
    def test_resolution_widens_and_never_narrows(
        self, recorded: str, declared: list[str], expected: str,
    ) -> None:
        assert admin._resolve_extras_profile(recorded, declared) == expected

    @pytest.mark.parametrize("recorded", [name for name, _ in admin._EXTRAS_PROFILES])
    @pytest.mark.parametrize("declared", [[], ["web"], ["test"], ["dev"]])
    def test_the_result_is_never_smaller_than_what_the_venv_had(
        self, recorded: str, declared: list[str],
    ) -> None:
        sets = dict(admin._EXTRAS_PROFILES)
        resolved = admin._resolve_extras_profile(recorded, declared)
        assert sets[recorded] <= sets[resolved]
        assert set(declared) <= sets[resolved]


class TestManagedUpdateHonorsDeclaredExtras:
    def test_undeclared_program_still_preserves_its_recorded_profile(
        self, serving_program: config.VenvProgram,
    ) -> None:
        args = admin._managed_update_script_args(serving_program, None)
        assert args[args.index("--extras") + 1] == "core"

    def test_daemon_serving_env_declaring_web_rebuilds_with_uvicorn(
        self, serving_program: config.VenvProgram,
    ) -> None:
        """The whole chain, from the config key to the pip spec.

        Config ``extras = ["web"]`` on a venv recorded as ``core`` becomes
        ``--extras web`` on the canonical updater's command line, and that
        updater -- the real one, previewing the real arguments -- resolves it
        to the ``[web]`` pip suffix, which
        :meth:`TestProfileTable.test_the_web_profile_is_what_carries_uvicorn`
        pins to uvicorn.
        """
        declared = serving_program.model_copy(update={"extras": ["web"]})
        args = admin._managed_update_script_args(declared, None)
        assert args[args.index("--extras") + 1] == "web"

        preview = subprocess.run(
            ["bash", str(UPDATE_SH), "--skip-git", "--dry-run", *args],
            cwd=PROJECT,
            capture_output=True,
            text=True,
            check=False,
        )
        assert preview.returncode == 0, preview.stderr
        assert "[pip] would install vq[web] from this checkout." in preview.stdout
        assert "capabilities: web" in preview.stdout

    def test_declaration_cannot_drop_the_environments_own_capabilities(
        self, serving_program: config.VenvProgram,
    ) -> None:
        venv = Path(serving_program.python).parent.parent
        (venv / ".vq-install-metadata").write_text(
            "version=1\nextras=dev\neditable=1\n", encoding="utf-8",
        )
        declared = serving_program.model_copy(update={"extras": ["web"]})
        args = admin._managed_update_script_args(declared, None)
        assert args[args.index("--extras") + 1] == "all"

    def test_legacy_adoption_also_honours_the_declaration(
        self, serving_program: config.VenvProgram, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(admin, "_prove_legacy_managed_install", lambda *a: True)
        declared = serving_program.model_copy(update={"extras": ["web"]})
        args = admin._managed_update_script_args(
            declared, ["--adopt-legacy", "--extras", "core", "--editable"],
        )
        assert args[args.index("--extras") + 1] == "web"

    def test_an_operator_profile_request_is_raised_to_the_declared_floor(
        self, serving_program: config.VenvProgram,
    ) -> None:
        """#11's per-update request sets the base; the declaration is the floor.

        ``--extras test`` asks for a profile that does not carry web, so a
        program declaring web resolves to the smallest profile covering both.
        """
        declared = serving_program.model_copy(update={"extras": ["web"]})
        args = admin._managed_update_script_args(
            declared, ["--recreate-venv", "--extras", "all"],
        )
        assert args[args.index("--extras") + 1] == "all"

    def test_an_operator_request_that_drops_a_declared_extra_is_refused(
        self, serving_program: config.VenvProgram,
    ) -> None:
        """Two instructions contradicting each other is not a floor to raise."""
        declared = serving_program.model_copy(update={"extras": ["web"]})
        with pytest.raises(admin.AdminError) as excinfo:
            admin._managed_update_script_args(
                declared, ["--recreate-venv", "--extras", "core"],
            )
        message = str(excinfo.value)
        assert "does not install web" in message
        assert "change the declaration" in message

    def test_an_operator_request_is_untouched_without_a_declaration(
        self, serving_program: config.VenvProgram,
    ) -> None:
        """#11 keeps working exactly as it does on its own."""
        args = admin._managed_update_script_args(
            serving_program, ["--recreate-venv", "--extras", "web"],
        )
        assert args[args.index("--extras") + 1] == "web"

    def test_an_update_that_cannot_apply_a_declaration_says_so(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Config that looks applied and is not is the whole failure mode.

        A vibe-view program, built here rather than borrowed from the
        ``serving_program`` fixture: that fixture points at vq's own checkout
        and vq's own ``scripts/update.sh``, so an ordinary update of it now
        applies the declaration (#85) and this note is not what it gets. The
        test was named for a program it never had.
        """
        checkout = tmp_path / "vibe-view"
        (checkout / "scripts").mkdir(parents=True)
        (checkout / "scripts" / "update.sh").write_text(
            "#!/usr/bin/env bash\n", encoding="utf-8",
        )
        prog = config.VenvProgram(
            kind="venv",
            python=str(checkout / ".venv" / "bin" / "python"),
            git_dir=str(checkout),
            update_script="scripts/update.sh",
            extras=["web"],
        )
        said: list[str] = []
        monkeypatch.setattr(admin.output, "narrate", lambda text, *a, **k: said.append(text))

        admin._warn_if_declared_extras_are_inert("vibeview-dev", prog)

        assert said and "which this update does not apply" in said[0]
        # The note names a route an operator can actually take from here. A
        # fresh `vq admin install` is not one: this environment's checkout
        # exists -- it is what this update just pulled -- and
        # `_refuse_occupied_git_dir` refuses to provision over one, so naming
        # it sends the reader to a command that refuses.
        assert "vq admin install" not in said[0]
        # Nor is the managed daemon transaction: it requires the same
        # canonical script this program has not got, and refuses without it.
        assert "A managed daemon update applies the declaration" not in said[0]
        # And it does not describe a rebuild in terms of the metadata sidecar,
        # which decides nothing when the configured update_script is not vq's.
        assert ".vq-install-metadata" not in said[0]
        # The route that does exist: make update_script vq's own, and every
        # update applies the declaration.
        assert (
            "Point update_script at the checkout's own scripts/update.sh"
        ) in said[0]

    def test_an_undeclared_program_says_nothing(
        self, serving_program: config.VenvProgram, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        said: list[str] = []
        monkeypatch.setattr(admin.output, "narrate", lambda text, *a, **k: said.append(text))
        admin._warn_if_declared_extras_are_inert("vibeqc-dev", serving_program)
        assert said == []

    def test_update_script_extras_flag_names_the_config_key_instead(
        self, serving_program: config.VenvProgram,
    ) -> None:
        """The old dead end, now signposted.

        ``--extras`` stays rejected in ``update_script`` -- a serving venv's
        install target may not come from a free-form command line -- but the
        rejection now says where the supported one is.
        """
        misconfigured = serving_program.model_copy(
            update={"update_script": "scripts/update.sh --extras web"},
        )
        with pytest.raises(admin.AdminError) as excinfo:
            admin._managed_update_script_args(misconfigured, None)
        message = str(excinfo.value)
        assert 'extras = ["web"]' in message
        assert "--update-script-arg --extras" in message


class TestDeclarationValidation:
    def test_an_unknown_extra_is_refused_at_config_load(self) -> None:
        with pytest.raises(ValueError, match="unknown extra 'wbe'"):
            config.VenvProgram(
                kind="venv",
                python="/srv/vq/.venv/bin/python",
                git_dir="/srv/vq",
                update_script="scripts/update.sh",
                extras=["wbe"],
            )

    def test_a_program_vq_never_builds_may_not_declare_extras(self) -> None:
        """An accepted key that changes nothing is how this failed the first time."""
        with pytest.raises(
            ValueError, match="extras requires install_script or update_script",
        ):
            config.VenvProgram(
                kind="venv",
                python="/srv/vq/.venv/bin/python",
                git_dir="/srv/vq",
                extras=["web"],
            )

    @pytest.mark.parametrize(
        "scripts",
        [
            {"install_script": "vibe-queue/scripts/install.sh"},
            {"update_script": "scripts/update.sh"},
            {
                "install_script": "vibe-queue/scripts/install.sh",
                "update_script": "scripts/update.sh",
            },
        ],
    )
    def test_either_route_into_the_venv_admits_a_declaration(
        self, scripts: dict[str, str],
    ) -> None:
        """A provision applies the floor too (#47), so install_script alone is a route.

        Before `vq admin install` honoured the declaration, `update_script`
        was the only way it could reach pip and the model required it. A
        program that vq provisions but never rebuilds now has a working route,
        and refusing its config would refuse a configuration that works.
        """
        prog = config.VenvProgram(
            kind="venv",
            python="/srv/vq/.venv/bin/python",
            git_dir="/srv/vq",
            extras=["web"],
            **scripts,
        )
        assert prog.extras == ["web"]

    def test_the_admitted_shape_is_told_what_it_does_and_does_not_get(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Loading is not the claim; what the shape *does* downstream is.

        Asserting only `prog.extras == ["web"]` holds with every downstream
        behaviour removed, which is how three messages that name `vq admin
        update` as this shape's way out landed green. A program vq provisions
        but never rebuilds gets the floor once, at provision, and nothing
        after: an ordinary update runs its `update_script`, and there is none.
        """
        prog = config.VenvProgram(
            kind="venv",
            python="/srv/vq/.venv/bin/python",
            git_dir="/srv/vq",
            install_script="scripts/install.sh",
            extras=["web"],
        )
        said: list[str] = []
        monkeypatch.setattr(
            admin.output, "narrate", lambda text, *a, **k: said.append(text),
        )

        admin._warn_if_declared_extras_are_inert("console", prog)

        assert said and "rebuilds nothing" in said[0]
        assert ".vq-install-metadata" not in said[0]
        # The managed daemon transaction is not a route for it either: that
        # path *is* the canonical update script, which this program has not
        # got, so it refuses rather than planning a rebuild.
        with pytest.raises(
            admin.AdminError, match="requires the canonical update script",
        ):
            admin._managed_update_script_args(prog, None)


class TestConsoleRuntimeGuard:
    @pytest.fixture
    def no_uvicorn(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # `sys.modules[name] = None` is how CPython spells "this import must
        # fail", so the probe under test runs for real rather than stubbed.
        monkeypatch.setitem(sys.modules, "uvicorn", None)

    def test_probe_reports_the_module_the_service_would_die_on(
        self, no_uvicorn: None,
    ) -> None:
        assert web_install.console_runtime_import_error() == "uvicorn"

    def test_the_precondition_refuses_before_anything_is_planned(
        self, no_uvicorn: None, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(config, "load_config", lambda: config.Config())
        with pytest.raises(web_install.InstallError) as excinfo:
            web_install.require_console_runtime()
        assert "cannot serve the console (uvicorn is missing)" in str(excinfo.value)

    def test_the_daemon_unit_is_not_held_to_the_consoles_requirement(
        self, no_uvicorn: None, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """``vq daemon install`` shares the planner and needs no ``[web]``."""
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
        plan = web_install.build_plan(
            manager="systemd-user", kind=web_install.DAEMON_SERVICE,
        )
        assert plan.unit_name == web_install.DAEMON_UNIT_NAME

    def test_web_install_refuses_and_names_pip_on_an_unmanaged_install(
        self, no_uvicorn: None, monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
    ) -> None:
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
        monkeypatch.setattr(config, "load_config", lambda: config.Config())
        result = CliRunner().invoke(
            cli.main, ["web", "install", "--manager", "systemd-user", "--dry-run"],
        )
        assert result.exit_code != 0
        assert "would fail on every start" in result.output
        assert "pip install -e '.[web]'" in result.output

    def test_web_install_names_the_managed_program_and_its_config_key(
        self, no_uvicorn: None, monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
    ) -> None:
        """On a fleet host, pip is exactly the wrong advice."""
        cfg = config.Config(
            programs={
                "vq-console": config.VenvProgram(
                    kind="venv",
                    python=str(Path(sys.executable).parent / "python"),
                    git_dir=str(PROJECT),
                ),
            },
        )
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
        monkeypatch.setattr(config, "load_config", lambda: cfg)
        result = CliRunner().invoke(
            cli.main, ["web", "install", "--manager", "systemd-user", "--dry-run"],
        )
        assert result.exit_code != 0
        assert "pip install" not in result.output
        assert "[programs.vq-console]" in result.output
        assert 'extras = ["web"]' in result.output
        assert "vq admin update vq-console" in result.output

    def test_a_declared_but_unbuilt_program_is_told_to_rebuild_not_to_edit(
        self, no_uvicorn: None, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Between the config edit and the update, the config is already right."""
        cfg = config.Config(
            programs={
                "vq-console": config.VenvProgram(
                    kind="venv",
                    python=str(Path(sys.executable).parent / "python"),
                    git_dir=str(PROJECT),
                    update_script="scripts/update.sh",
                    extras=["web"],
                ),
            },
        )
        monkeypatch.setattr(config, "load_config", lambda: cfg)
        with pytest.raises(web_install.InstallError) as excinfo:
            web_install.require_console_runtime()
        message = str(excinfo.value)
        assert "already declares the web extra" in message
        assert "vq admin update vq-console" in message
        assert "[programs." not in message

    def test_the_remedy_keeps_extras_the_program_already_declares(
        self, no_uvicorn: None, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        cfg = config.Config(
            programs={
                "vq-console": config.VenvProgram(
                    kind="venv",
                    python=str(Path(sys.executable).parent / "python"),
                    git_dir=str(PROJECT),
                    update_script="scripts/update.sh",
                    extras=["dev"],
                ),
            },
        )
        monkeypatch.setattr(config, "load_config", lambda: cfg)
        with pytest.raises(web_install.InstallError) as excinfo:
            web_install.require_console_runtime()
        assert 'extras = ["dev", "web"]' in str(excinfo.value)

    def test_a_program_with_no_rebuild_route_is_told_to_add_one(
        self, no_uvicorn: None, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """`vq admin update` on an install_script-only program is a no-op.

        This is the guard that exists so a console unit does not fail on every
        start. Sending the operator to a command that pulls the checkout,
        reports success and leaves uvicorn missing is that failure with an
        extra step -- the unit keeps failing while the remedy reports success.
        """
        cfg = config.Config(
            programs={
                "vq-console": config.VenvProgram(
                    kind="venv",
                    python=str(Path(sys.executable).parent / "python"),
                    git_dir=str(PROJECT),
                    install_script="scripts/install.sh",
                    extras=["web"],
                ),
            },
        )
        monkeypatch.setattr(config, "load_config", lambda: cfg)
        with pytest.raises(web_install.InstallError) as excinfo:
            web_install.require_console_runtime()
        message = str(excinfo.value)
        assert "already declares the web extra" in message
        assert "configures no update_script" in message
        assert 'update_script = "scripts/update.sh"' in message
        assert "vq admin update vq-console" in message

    def test_an_undeclared_program_with_no_rebuild_route_is_told_both_keys(
        self, no_uvicorn: None, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Declaring the extra alone would leave the console down.

        The declaration is still right -- it is what the next rebuild
        preserves -- but on this shape there is no next rebuild until the
        program has an `update_script`, so the remedy names both keys.
        """
        cfg = config.Config(
            programs={
                "vq-console": config.VenvProgram(
                    kind="venv",
                    python=str(Path(sys.executable).parent / "python"),
                    git_dir=str(PROJECT),
                    install_script="scripts/install.sh",
                ),
            },
        )
        monkeypatch.setattr(config, "load_config", lambda: cfg)
        with pytest.raises(web_install.InstallError) as excinfo:
            web_install.require_console_runtime()
        message = str(excinfo.value)
        assert "[programs.vq-console]" in message
        assert 'extras = ["web"]' in message
        assert 'update_script = "scripts/update.sh"' in message
        assert "configures no update_script" in message

    def test_a_broken_config_still_yields_a_remedy(
        self, no_uvicorn: None, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        def explode() -> config.Config:
            raise RuntimeError("unreadable config")

        monkeypatch.setattr(config, "load_config", explode)
        with pytest.raises(web_install.InstallError) as excinfo:
            web_install.require_console_runtime()
        assert "pip install -e '.[web]'" in str(excinfo.value)


def test_the_config_key_is_documented_where_operators_look() -> None:
    """A key nobody can find from the error they hit is not a supported route."""
    text = (PROJECT / "docs" / "config.toml.example").read_text(encoding="utf-8")
    assert 'extras = ["web"]' in text
