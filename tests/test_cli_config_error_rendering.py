"""A broken config reaches the terminal as an error message, not a traceback.

``config.ConfigError`` is raised for problems in a file the user owns and can
fix -- a TOML syntax error, an undefined ``default_pool``, a ``min_vq_version``
floor this vq cannot meet. Around twenty call sites converted it to a click
error by hand, so roughly half the verbs rendered it cleanly and the other half
printed a Python traceback with a pydantic dump stapled to the end.

``cli._ConfigErrorGroup`` catches it once, at the root group, so the rendering
no longer depends on which verb the user happened to type. These tests pin both
halves of that: the verbs that used to crash now do not, and the verbs that
already had a guard still render exactly what they rendered before.
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import click
import pytest
from click.testing import CliRunner

from vq import config, paths
from vq.cli import _ConfigErrorGroup, main

# A config that fails pydantic validation. The failure is deliberately one
# whose message is long and multi-line, because that is the case where a
# traceback and the actual explanation are hardest to tell apart.
INVALID_CONFIG = 'default_pool = "nonexistent"\n'

# The other shape of ConfigError, and the one whose message is a single
# sentence: tomllib refused the file outright. Worth pinning separately,
# because a one-line message is where a stray traceback is most conspicuous.
UNPARSABLE_CONFIG = "default_host = \n"

# The motivating case for the group-level guard: the whole point of the
# version floor is a legible sentence, and it was legible only after fifteen
# lines of traceback.
FUTURE_CONFIG = 'min_vq_version = "9.9.9"\npin_source_repos_v9 = 1\n'


@pytest.fixture
def broken_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point vq at a config directory holding an invalid config.toml."""
    cfg_dir = tmp_path / "cfg"
    cfg_dir.mkdir()
    (cfg_dir / "config.toml").write_text(INVALID_CONFIG)
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(cfg_dir))
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    return cfg_dir


class TestTheTerminalNeverSeesATraceback:
    """The regression this exists to prevent, verb by verb."""

    # A spread across the shapes of verb that reach load_config without a
    # guard: a plain lookup, the busiest write verb, a nested admin group, a
    # fan-out verb, and a daemon-facing one.
    @pytest.mark.parametrize(
        "argv",
        [
            ["programs"],
            ["submit", "--", "echo", "hi"],
            ["admin", "update"],
            ["list"],
            ["queue"],
            ["overview"],
            ["summary"],
            ["daemon", "ping"],
        ],
        ids=lambda argv: " ".join(argv),
    )
    def test_an_invalid_config_is_an_error_message(
        self, broken_config: Path, argv: list[str],
    ) -> None:
        result = CliRunner().invoke(main, argv)
        # SystemExit means click rendered and exited. Any other exception
        # would have escaped to the interpreter and printed a traceback.
        assert isinstance(result.exception, SystemExit), result.exception
        assert result.exit_code != 0
        assert result.stderr.startswith("Error: invalid config in ")
        assert "Traceback" not in result.stderr

    def test_an_unparsable_config_is_exactly_one_line(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A tomllib refusal already reads as a sentence, so the whole of
        stderr should be that sentence and nothing else."""
        cfg_dir = tmp_path / "cfg"
        cfg_dir.mkdir()
        (cfg_dir / "config.toml").write_text(UNPARSABLE_CONFIG)
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(cfg_dir))
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))

        result = CliRunner().invoke(main, ["programs"])

        assert result.exit_code == 1
        assert result.stderr.splitlines() == [
            f"Error: failed to parse {cfg_dir / 'config.toml'}: "
            f"Invalid value (at line 1, column 16)"
        ]

    def test_a_version_floor_refusal_is_one_line(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """``min_vq_version`` exists to say one sentence. Say only that."""
        cfg_dir = tmp_path / "cfg"
        cfg_dir.mkdir()
        (cfg_dir / "config.toml").write_text(FUTURE_CONFIG)
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(cfg_dir))
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))

        result = CliRunner().invoke(main, ["programs"])

        assert result.exit_code == 1
        assert result.stderr.splitlines() == [
            f"Error: {cfg_dir / 'config.toml'} requires vq >= 9.9.9; this is "
            f"vq {config.vq.__version__}. Upgrade vq on this host, or -- if "
            f"this host does not need the newer keys -- lower min_vq_version "
            f"in the config."
        ]

    def test_the_real_process_exits_cleanly(self, tmp_path: Path) -> None:
        """CliRunner intercepts exceptions; a shell does not. Prove the shell
        case too, since the traceback is what the bug report was about."""
        cfg_dir = tmp_path / "cfg"
        cfg_dir.mkdir()
        (cfg_dir / "config.toml").write_text(INVALID_CONFIG)
        repo_src = str(Path(__file__).resolve().parent.parent / "src")

        completed = subprocess.run(
            [sys.executable, "-m", "vq", "programs"],
            capture_output=True,
            text=True,
            env={
                **os.environ,
                "PYTHONPATH": repo_src,
                config.ENV_CONFIG_DIR: str(cfg_dir),
                paths.ENV_STATE_DIR: str(tmp_path / "state"),
                "VQ_LOG_DISABLED": "1",
            },
        )

        assert completed.returncode == 1
        assert "Traceback" not in completed.stderr
        assert "ConfigError" not in completed.stderr
        assert completed.stderr.startswith("Error: invalid config in ")


class TestVerbsThatAlreadyGuardedAreUnchanged:
    """The group guard is a floor beneath the hand-written guards, not a
    replacement: a verb that converts ConfigError itself never reaches it, so
    its wording and its exit code must not move."""

    def test_doctor_still_renders_its_own_usage_error(
        self, broken_config: Path,
    ) -> None:
        # cli.py's `vq doctor` guard raises click.UsageError -- exit 2 and a
        # usage block, which ClickException would not produce.
        result = CliRunner().invoke(main, ["doctor"])

        assert result.exit_code == 2
        # A per-verb usage block, which a ClickException cannot produce.
        assert result.stderr.startswith("Usage: main doctor [OPTIONS] [HOST]\n")
        assert "Error: invalid config in " in result.stderr

    def test_resolve_host_still_renders_its_own_usage_error(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A valid config with no default_host: ConfigError comes from
        ``_resolve_host``, which has raised UsageError since long before the
        group guard existed."""
        cfg_dir = tmp_path / "cfg"
        cfg_dir.mkdir()
        (cfg_dir / "config.toml").write_text('[hosts.host_a]\nssh = "host_a"\n')
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(cfg_dir))
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))

        result = CliRunner().invoke(main, ["queue"])

        assert result.exit_code == 2
        assert "Usage:" in result.stderr
        assert "HOST is required" in result.stderr


class TestTheGuardIsNarrow:
    """What the root group converts, and -- more importantly -- what it does
    not. A blanket ``except Exception`` here would hide real bugs behind a
    one-line message, which is the opposite of the point."""

    @staticmethod
    def _group() -> click.Group:
        @click.group(cls=_ConfigErrorGroup)
        def root() -> None: ...

        @root.command("direct")
        def direct() -> None:
            raise config.ConfigError("from a verb")

        @root.command("crash")
        def crash() -> None:
            raise RuntimeError("a real bug")

        @root.command("usage")
        def usage() -> None:
            raise click.UsageError("bad arguments")

        @root.group("sub")
        def sub() -> None: ...

        @sub.group("deeper")
        def deeper() -> None: ...

        @deeper.command("verb")
        def deep_verb() -> None:
            raise config.ConfigError("from two groups down")

        def load(ctx: click.Context, param: click.Parameter, value: str) -> str:
            raise config.ConfigError("from a parameter callback")

        @root.command("param")
        @click.option("--host", callback=load, default="x")
        def param(host: str) -> None: ...  # pragma: no cover - callback raises

        return root

    def test_a_verb_raising_config_error_is_converted(self) -> None:
        result = CliRunner().invoke(self._group(), ["direct"])
        assert result.exit_code == 1
        assert result.stderr == "Error: from a verb\n"

    def test_a_nested_group_is_covered_too(self) -> None:
        """click builds and invokes the whole subcommand chain inside the root
        group's invoke(), so `vq admin update` needs no guard of its own."""
        result = CliRunner().invoke(self._group(), ["sub", "deeper", "verb"])
        assert result.exit_code == 1
        assert result.stderr == "Error: from two groups down\n"

    def test_a_parameter_callback_is_covered_too(self) -> None:
        """Parsing happens in make_context, which the root group calls from
        inside invoke() -- so a callback that loads config is in scope."""
        result = CliRunner().invoke(self._group(), ["param"])
        assert result.exit_code == 1
        assert result.stderr == "Error: from a parameter callback\n"

    def test_an_unrelated_exception_still_escapes(self) -> None:
        """ConfigError subclasses RuntimeError. Catching one must not catch
        the other, or a genuine crash would be reported as a config problem."""
        result = CliRunner().invoke(self._group(), ["crash"])
        # `is`, not isinstance: a ConfigError would satisfy isinstance here
        # and the test would pass while asserting nothing.
        assert type(result.exception) is RuntimeError
        assert str(result.exception) == "a real bug"

    def test_a_click_exception_keeps_its_own_exit_code(self) -> None:
        result = CliRunner().invoke(self._group(), ["usage"])
        assert result.exit_code == 2
        assert "Error: bad arguments" in result.stderr

    def test_the_cause_is_dropped_so_no_chain_is_printed(self) -> None:
        """``raise ... from None``: the underlying pydantic/tomllib error is
        already quoted in the message, and re-printing it as ``The above
        exception was the direct cause of...`` is the bug."""
        with pytest.raises(click.ClickException) as excinfo:
            self._group().main(["direct"], standalone_mode=False)
        assert excinfo.value.__cause__ is None
        assert excinfo.value.__suppress_context__ is True
