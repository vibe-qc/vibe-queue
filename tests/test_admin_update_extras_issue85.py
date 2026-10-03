"""An ordinary `vq admin update` applies a program's declared extras (#85).

`VenvProgram.extras` says what a venv is *for*, and it reached pip on exactly
two paths: a fresh `vq admin install` (#47) and the managed daemon transaction
(`vq admin update` on the env serving this host's daemon, `vq self-update`).
Every other update rebuilt with whatever profile `.vq-install-metadata`
recorded and never consulted the declaration, so a program that declared
`extras = ["web"]`, was not the daemon-serving env and configured vq's own
updater could still be rebuilt without uvicorn -- by the verb an operator
reaches for to fix exactly that. The asymmetry had no reason an operator could
predict from the outside: the only thing separating the two was which code
path applied the floor.

The rule is now uniform: vq applies the floor wherever vq itself builds the
environment, and it may name an install target only on its own
`scripts/install.sh` or `scripts/update.sh`, resolved inside the checkout the
program points at. A program whose `update_script` is its own script keeps
exactly the behaviour it had, and is still told so.

Two things these tests are deliberately built to catch, because both have bitten
this feature already:

* a config helper that writes `update_script` whether or not the shape under
  test has one, which let a whole lane pass without exercising the shape it
  named (#47). Every program here is built from the scripts the test names and
  no others;
* a note that is true only because its caller happened to ask the right
  question first. `_warn_if_declared_extras_are_inert` asks the proof itself,
  and there is a test here that calls it directly on a program whose updater
  *is* vq's own.

The managed lane is pinned rather than exercised: its host validation is #25,
and nothing here may move it.
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from vq import admin, config, paths
from vq.cli import main

PROJECT = Path(__file__).resolve().parents[1]


def _proc(rc: int = 0, stdout: str = "") -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(
        args=[], returncode=rc, stdout=stdout, stderr="",
    )


def _script(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("#!/usr/bin/env bash\necho stub\n", encoding="utf-8")
    path.chmod(0o755)


def _vq_checkout(root: Path, *, recorded: str | None = "core") -> Path:
    """A checkout shaped like vq's own, carrying vq's own lifecycle scripts.

    ``recorded`` writes the ``.vq-install-metadata`` note the venv would carry;
    ``None`` leaves it out, which is what a venv built by the plain
    ``pip install -e`` in CONTRIBUTING.md looks like.
    """
    (root / "src" / "vq").mkdir(parents=True)
    (root / "pyproject.toml").write_text("", encoding="utf-8")
    (root / ".git").mkdir(exist_ok=True)
    _script(root / "scripts" / "install.sh")
    _script(root / "scripts" / "update.sh")
    venv = root / ".venv"
    (venv / "bin").mkdir(parents=True)
    if recorded is not None:
        (venv / ".vq-install-metadata").write_text(
            f"version=1\nextras={recorded}\neditable=1\n", encoding="utf-8",
        )
    return root


def _foreign_checkout(root: Path) -> Path:
    """A vibe-qc / vibe-view shaped program: its own updater, its own tree.

    ``scripts/update.sh`` by name, so a check made on the spelling would pass
    it. It is not vq's, because the checkout is not a vq tree at all.
    """
    (root / ".git").mkdir(parents=True)
    _script(root / "scripts" / "update.sh")
    (root / ".venv" / "bin").mkdir(parents=True)
    return root


def _configure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    programs: str,
) -> config.Config:
    """Load a config holding exactly the ``[programs.X]`` blocks given.

    The blocks are written by the caller rather than assembled from keyword
    defaults on purpose: a helper that fills in `update_script` for a test
    about a program without one is how three remedies naming `vq admin update`
    landed green (#47).
    """
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir(parents=True, exist_ok=True)
    (tmp_path / "cfg" / "config.toml").write_text(
        'default_host = "localhost"\n\n[hosts.localhost]\nssh = "localhost"\n\n'
        + programs,
        encoding="utf-8",
    )
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    paths.jobs_dir().mkdir(parents=True, exist_ok=True)
    return config.load_config()


def _venv_program(
    checkout: Path,
    *,
    extras: list[str] | None = None,
    update_script: str | None = None,
    install_script: str | None = None,
    post_update_script: str | None = None,
    name: str = "demo",
) -> str:
    lines = [
        f"[programs.{name}]",
        'kind = "venv"',
        f'python = "{checkout / ".venv" / "bin" / "python"}"',
        f'git_dir = "{checkout}"',
        'branch = "main"',
    ]
    if update_script is not None:
        lines.append(f'update_script = "{update_script}"')
    if install_script is not None:
        lines.append(f'install_script = "{install_script}"')
    if post_update_script is not None:
        lines.append(f'post_update_script = "{post_update_script}"')
    if extras is not None:
        lines.append("extras = [" + ", ".join(f'"{e}"' for e in extras) + "]")
    return "\n".join(lines) + "\n\n"


@pytest.fixture
def said(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Everything the update narrated, in order."""
    lines: list[str] = []
    monkeypatch.setattr(
        admin.output, "narrate", lambda text, *a, **k: lines.append(text),
    )
    return lines


def _update(env: str, cfg: config.Config, **kwargs: object) -> tuple[
    admin.UpdateResult, list[list[str]],
]:
    """Run one update with every subprocess answering "fine", and record argv."""
    captured: list[list[str]] = []

    def fake_run(*args: object, **_kw: object) -> subprocess.CompletedProcess[str]:
        captured.append(list(args[0]))  # type: ignore[arg-type]
        return _proc(rc=0, stdout="")

    with patch("vq.admin.subprocess.run", side_effect=fake_run):
        result = admin.update_env(env, cfg, host="localhost", **kwargs)  # type: ignore[arg-type]
    return result, captured


def _script_argv(captured: list[list[str]]) -> list[str]:
    """The arguments the update script itself was handed, or [] if it never ran."""
    for argv in captured:
        if "bash" in argv:
            return argv[argv.index("bash") + 2:]
    return []


class TestTheFloorReachesTheUpdater:
    """The gap #85 names: a declared runtime an ordinary rebuild left out."""

    @pytest.mark.parametrize("as_json", [False, True])
    @pytest.mark.parametrize("already_current", [False, True])
    def test_inert_notice_reaches_cli_and_transcript(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        as_json: bool, already_current: bool,
    ) -> None:
        checkout = _foreign_checkout(tmp_path / "foreign")
        _configure(tmp_path, monkeypatch, programs=_venv_program(
            checkout, extras=["web"], update_script="scripts/update.sh",
        ))
        monkeypatch.setattr(admin, "already_current", lambda *a, **kw: (already_current, "pinned"))
        monkeypatch.setattr(admin, "_update_env_logged", lambda *a, **kw: admin.UpdateResult(
            env="demo", git_dir=str(checkout), branch="main", update_script="scripts/update.sh",
            git_pull_rc=0, update_script_rc=0,
        ))
        args = ["admin", "update", "demo", "localhost"]
        if already_current:
            args += ["--expected-sha", "a" * 40]
        if as_json:
            args += ["--json"]
        result = CliRunner().invoke(main, args)
        assert result.exit_code == 0, result.output
        expected = "note: demo declares extras ['web'], which this update does not apply"
        assert result.stderr.count(expected) == 1
        if as_json:
            assert json.loads(result.stdout)["env"] == "demo"
        logs = list(paths.state_root().glob("admin-updates/demo/*.log"))
        assert len(logs) == 1
        assert logs[0].read_text().count(expected) == 1

    def test_a_declared_extra_becomes_the_updaters_profile(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, said: list[str],
    ) -> None:
        checkout = _vq_checkout(tmp_path / "checkout")
        cfg = _configure(
            tmp_path,
            monkeypatch,
            programs=_venv_program(
                checkout, extras=["web"], update_script="scripts/update.sh",
            ),
        )

        result, captured = _update("demo", cfg)

        assert result.update_script_rc == 0, result.work_errors
        assert _script_argv(captured) == ["--extras", "web"]
        assert said and "applies it" in said[0]
        assert "--extras web" in said[0]

    def test_the_floor_is_resolved_over_the_profile_the_venv_records(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, said: list[str],
    ) -> None:
        """A floor may only add. ``test`` plus a declared ``web`` is ``all``.

        Narrowing here would strip the test tooling off a venv because
        somebody declared the console -- the same rule the managed rebuild
        follows, for the same reason.
        """
        checkout = _vq_checkout(tmp_path / "checkout", recorded="test")
        cfg = _configure(
            tmp_path,
            monkeypatch,
            programs=_venv_program(
                checkout, extras=["web"], update_script="scripts/update.sh",
            ),
        )

        _result, captured = _update("demo", cfg)

        assert _script_argv(captured) == ["--extras", "all"]

    def test_the_floor_is_resolved_over_the_configured_profile(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, said: list[str],
    ) -> None:
        """``update.sh`` keeps the last ``--extras``, so the floor comes last."""
        checkout = _vq_checkout(tmp_path / "checkout")
        cfg = _configure(
            tmp_path,
            monkeypatch,
            programs=_venv_program(
                checkout,
                extras=["web"],
                update_script="scripts/update.sh --extras dev",
            ),
        )

        _result, captured = _update("demo", cfg)

        assert _script_argv(captured) == ["--extras", "dev", "--extras", "all"]

    def test_a_venv_with_no_metadata_note_still_gets_the_declaration(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, said: list[str],
    ) -> None:
        """A venv vq did not build carries no note, and is not refused for it.

        ``update.sh`` falls back to ``core`` when it finds no note, so
        resolving the floor over ``core`` can only widen what that run would
        have installed anyway.
        """
        checkout = _vq_checkout(tmp_path / "checkout", recorded=None)
        cfg = _configure(
            tmp_path,
            monkeypatch,
            programs=_venv_program(
                checkout, extras=["web"], update_script="scripts/update.sh",
            ),
        )

        _result, captured = _update("demo", cfg)

        assert _script_argv(captured) == ["--extras", "web"]

    def test_a_profile_that_already_covers_the_declaration_is_left_alone(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, said: list[str],
    ) -> None:
        """The argv is the one the operator configured, byte for byte."""
        checkout = _vq_checkout(tmp_path / "checkout", recorded="all")
        cfg = _configure(
            tmp_path,
            monkeypatch,
            programs=_venv_program(
                checkout, extras=["web"], update_script="scripts/update.sh",
            ),
        )

        _result, captured = _update("demo", cfg)

        assert _script_argv(captured) == []
        assert said and "already installs covers" in said[0]

    def test_an_operator_request_that_covers_the_declaration_decides(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, said: list[str],
    ) -> None:
        """#11's per-update request sets the base; the floor sits under it."""
        checkout = _vq_checkout(tmp_path / "checkout")
        cfg = _configure(
            tmp_path,
            monkeypatch,
            programs=_venv_program(
                checkout, extras=["web"], update_script="scripts/update.sh",
            ),
        )

        _result, captured = _update(
            "demo", cfg, update_script_args=["--extras", "all"],
        )

        # Nothing appended: the request already installs the declaration.
        assert _script_argv(captured) == ["--extras", "all"]

    def test_an_operator_request_that_drops_a_declared_extra_is_refused(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, said: list[str],
    ) -> None:
        """Two instructions contradicting each other; say which two.

        Word for word the install path's refusal, because it is the same
        contradiction. Silently widening ``core`` to ``web`` would install
        something nobody typed.
        """
        checkout = _vq_checkout(tmp_path / "checkout")
        cfg = _configure(
            tmp_path,
            monkeypatch,
            programs=_venv_program(
                checkout, extras=["web"], update_script="scripts/update.sh",
            ),
        )

        with pytest.raises(admin.AdminError) as excinfo:
            _update("demo", cfg, update_script_args=["--extras", "core"])

        message = str(excinfo.value)
        assert "does not install web" in message
        assert "['web'] on this program declares" in message


class TestEveryOtherProgramIsUpdatedExactlyAsBefore:
    """The proof decides. Nothing else about a program does."""

    def test_a_program_with_its_own_updater_is_untouched_and_told_so(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, said: list[str],
    ) -> None:
        checkout = _foreign_checkout(tmp_path / "vibeview")
        cfg = _configure(
            tmp_path,
            monkeypatch,
            programs=_venv_program(
                checkout, extras=["web"], update_script="scripts/update.sh",
            ),
        )

        _result, captured = _update("demo", cfg)

        assert _script_argv(captured) == []
        assert said and "does not apply" in said[0]
        # It names both the command it was given and what the checkout said
        # about it, so an operator can see why without re-deriving it.
        assert "'scripts/update.sh'" in said[0]
        assert str(checkout) in said[0]
        # And it does not offer the managed transaction as a way round: that
        # path requires the same canonical script and refuses without it.
        assert "A managed daemon update applies the declaration" not in said[0]
        assert "Point update_script at the checkout's own" in said[0]

    def test_a_program_that_declares_nothing_says_nothing(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, said: list[str],
    ) -> None:
        checkout = _vq_checkout(tmp_path / "checkout")
        cfg = _configure(
            tmp_path,
            monkeypatch,
            programs=_venv_program(checkout, update_script="scripts/update.sh"),
        )

        _result, captured = _update("demo", cfg)

        assert _script_argv(captured) == []
        assert [line for line in said if "extras" in line] == []

    def test_a_program_vq_cannot_rebuild_keeps_its_own_note(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, said: list[str],
    ) -> None:
        """`install_script` and no `update_script`: one route, and it fired."""
        checkout = _vq_checkout(tmp_path / "checkout")
        cfg = _configure(
            tmp_path,
            monkeypatch,
            programs=_venv_program(
                checkout, extras=["web"], install_script="scripts/install.sh",
            ),
        )
        assert cfg.programs["demo"].update_script is None

        _result, captured = _update("demo", cfg)

        assert _script_argv(captured) == []
        assert said and "rebuilds nothing" in said[0]
        assert "--extras" not in said[0]

    def test_a_checkout_that_cannot_be_read_is_not_called_misconfigured(
        self, tmp_path: Path, said: list[str],
    ) -> None:
        """"I cannot tell" and "this is wrong" are different answers.

        Reported through the note rather than an update, because an update
        refuses a missing checkout before it gets this far. `vq doctor` is
        where an operator meets this state, and it must not go red on it.
        """
        prog = config.VenvProgram(
            kind="venv",
            python=str(tmp_path / "gone" / ".venv" / "bin" / "python"),
            git_dir=str(tmp_path / "gone"),
            update_script="scripts/update.sh",
            extras=["web"],
        )

        admin._warn_if_declared_extras_are_inert("demo", prog)

        assert said and "cannot tell" in said[0]
        assert "is not a readable directory" in said[0]
        assert "does not apply" not in said[0]

    def test_the_note_never_fires_for_a_program_whose_updater_is_vqs_own(
        self, tmp_path: Path, said: list[str],
    ) -> None:
        """Called directly, with no caller to have asked the question first.

        The note is the one part of this feature whose whole job is to be
        true. It asks the proof itself, so it cannot be made to say a
        declaration was skipped by an update that applied it.
        """
        checkout = _vq_checkout(tmp_path / "checkout")
        prog = config.VenvProgram(
            kind="venv",
            python=str(checkout / ".venv" / "bin" / "python"),
            git_dir=str(checkout),
            update_script="scripts/update.sh",
            extras=["web"],
        )

        admin._warn_if_declared_extras_are_inert("demo", prog)

        assert said == []


class TestTheIdempotenceGate:
    """A checkout at the target commit is not evidence about its venv."""

    def test_a_raised_floor_outranks_already_current(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, said: list[str],
    ) -> None:
        """The state the console host actually sat in: right commit, wrong venv.

        ``already_current`` is stubbed because a real answer needs a checkout
        pinned at a commit and a healthy interpreter; what is under test is the
        gate that consults it, which a newly raised floor has to pass.
        """
        checkout = _vq_checkout(tmp_path / "checkout")
        cfg = _configure(
            tmp_path,
            monkeypatch,
            programs=_venv_program(
                checkout, extras=["web"], update_script="scripts/update.sh",
            ),
        )
        monkeypatch.setattr(
            admin, "already_current", lambda *a, **k: (True, "already at HEAD"),
        )

        result, captured = _update("demo", cfg)

        assert result.already_current is False
        assert _script_argv(captured) == ["--extras", "web"]

    def test_a_floor_that_adds_nothing_still_short_circuits(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, said: list[str],
    ) -> None:
        """A declaration the venv already carries is not a reason to rebuild."""
        checkout = _vq_checkout(tmp_path / "checkout", recorded="all")
        cfg = _configure(
            tmp_path,
            monkeypatch,
            programs=_venv_program(
                checkout, extras=["web"], update_script="scripts/update.sh",
            ),
        )
        monkeypatch.setattr(
            admin, "already_current", lambda *a, **k: (True, "already at HEAD"),
        )

        result, captured = _update("demo", cfg)

        assert result.already_current is True
        assert _script_argv(captured) == []


class TestTheManagedLaneIsNotPerturbed:
    """Its host validation is #25. This change may not move it."""

    def test_the_ordinary_floor_is_never_consulted_on_a_managed_update(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, said: list[str],
    ) -> None:
        """The refusal that comes back proves how far the managed lane got.

        ``post_update_script`` is rejected by the managed branch before it
        plans anything, so the AdminError below can only be raised on that
        lane -- and the exploding stub can only fire if the ordinary floor
        were computed for it, which is what moving the call out of the
        ``not managed_daemon_restart`` branch would do.
        """
        checkout = _vq_checkout(tmp_path / "checkout")
        cfg = _configure(
            tmp_path,
            monkeypatch,
            programs=_venv_program(
                checkout,
                extras=["web"],
                update_script="scripts/update.sh",
                post_update_script="scripts/install.sh",
            ),
        )
        monkeypatch.setattr(
            admin,
            "_detect_vq_self_update",
            lambda _prog: admin._SelfUpdateProbe(
                is_self_update=True,
                daemon_running=True,
                service_manager="systemd",
                manager_available=True,
                diagnostic="test fixture: this venv serves the daemon",
            ),
        )

        def _explode(*_args: object, **_kwargs: object) -> list[str]:
            raise AssertionError(
                "the managed lane must not consult the ordinary floor"
            )

        monkeypatch.setattr(
            admin, "_declared_extras_on_ordinary_update", _explode,
        )
        # Not what is under test, and it cannot answer with every subprocess
        # stubbed: it resolves the checkout's own metadata directory through
        # the version control tool itself.
        monkeypatch.setattr(
            admin, "_guard_git_index_unlocked", lambda _git_dir: None,
        )

        with pytest.raises(admin.AdminError, match="rejects post_update_script"):
            _update("demo", cfg)

    def test_the_managed_plan_is_still_the_frozen_argv(
        self, tmp_path: Path,
    ) -> None:
        """One ``--extras``, decided by vq, with the venv and mode frozen."""
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
            extras=["web"],
        )

        args = admin._managed_update_script_args(prog, None)

        assert args.count("--extras") == 1
        assert args[args.index("--extras") + 1] == "web"
        assert args[-1] == "--recreate-venv"


class TestTheBatchUpdate:
    """``vq admin update --all`` applies each program's own declaration."""

    def test_each_env_gets_its_own_floor(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, said: list[str],
    ) -> None:
        declaring = _vq_checkout(tmp_path / "console")
        plain = _vq_checkout(tmp_path / "plain")
        cfg = _configure(
            tmp_path,
            monkeypatch,
            programs=(
                _venv_program(
                    declaring,
                    extras=["web"],
                    update_script="scripts/update.sh",
                    name="console",
                )
                + _venv_program(
                    plain, update_script="scripts/update.sh", name="plain",
                )
            ),
        )
        captured: list[list[str]] = []

        def fake_run(
            *args: object, **_kw: object,
        ) -> subprocess.CompletedProcess[str]:
            captured.append(list(args[0]))  # type: ignore[arg-type]
            return _proc(rc=0, stdout="")

        with patch("vq.admin.subprocess.run", side_effect=fake_run):
            results = admin.update_all(cfg, host="localhost")

        assert [r.env for r in results] == ["console", "plain"]
        per_env = [
            argv[argv.index("bash") + 2:] for argv in captured if "bash" in argv
        ]
        assert per_env == [["--extras", "web"], []]


def _reflowed(name: str) -> str:
    """A page's text with its wrapping removed.

    A rule is documented whether or not it fell across a line break, and a
    single-line pattern that misses one is indistinguishable from a page that
    never mentioned it.
    """
    return " ".join((PROJECT / name).read_text(encoding="utf-8").split())


@pytest.mark.parametrize(
    "page", ["docs/lifecycle.md", "docs/operations.md", "docs/config.toml.example"],
)
def test_the_pages_that_described_the_old_rule_describe_this_one(
    page: str,
) -> None:
    """All three said the floor applied at install and on a managed update.

    They were right about the behaviour they described, which is why each of
    them is wrong the moment it changes. A page that still says an ordinary
    update cannot apply a declaration sends an operator to a workaround for a
    problem that is no longer there.
    """
    text = _reflowed(page)
    # The route that was missing, named on every page that described the two
    # it had.
    assert "an ordinary `vq admin update`" in text
    # And the precedence, because "the floor is applied" without it does not
    # tell an operator what their venv ends up with.
    assert "`.vq-install-metadata` records, else `core`" in text


def test_the_command_help_states_where_the_floor_now_applies() -> None:
    """A behaviour change an operator meets at the CLI is stated at the CLI."""
    help_text = CliRunner().invoke(main, ["admin", "update", "--help"]).output
    flat = " ".join(help_text.replace("\\", " ").split())

    assert "declares ``extras``" in flat
    assert "scripts/update.sh" in flat
    assert "refused rather than silently widened" in flat
