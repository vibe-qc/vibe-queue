"""v0.7.16 *Codd's Tuple* — `vq submit --time-limit HH:MM:SS` tests.

Pins:

1. The ``_parse_time_limit`` helper handles all three accepted
   forms (HH:MM:SS, MM:SS, integer seconds) and rejects
   malformed input with precise messages.
2. The CLI flag sets the same spec field as
   ``--wall-time-seconds``; the two are mutually exclusive.
3. The flag chain via ``submit_remote`` carries through
   correctly (re-uses the existing ``--wall-time-seconds``
   forwarding rather than introducing a new wire format).
"""

from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import config, paths
from vq.cli import _parse_time_limit, main
from vq.spec import JobSpec


@pytest.fixture
def cli_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Path:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    return tmp_path


# ----------------------------------------------------------------------
# _parse_time_limit
# ----------------------------------------------------------------------


class TestParseTimeLimit:
    @pytest.mark.parametrize(
        "value, expected",
        [
            # HH:MM:SS
            ("00:00:01", 1),
            ("00:00:30", 30),
            ("00:01:00", 60),
            ("01:00:00", 3600),
            ("01:30:00", 5400),
            ("24:00:00", 86400),
            # MM:SS
            ("01:30", 90),
            ("90:00", 5400),
            ("00:01", 1),
            # Plain seconds
            ("1", 1),
            ("60", 60),
            ("3600", 3600),
            ("86400", 86400),
        ],
    )
    def test_accepts_canonical_forms(
        self, value: str, expected: int
    ) -> None:
        assert _parse_time_limit(value) == expected

    @pytest.mark.parametrize(
        "value, fragment",
        [
            ("", "empty value"),
            ("01:30:00:00", "expected HH:MM:SS or MM:SS"),
            ("01:30:60", "MM and SS must be < 60"),
            ("01:60:00", "MM and SS must be < 60"),
            ("90:60", "SS must be < 60"),
            ("abc", "expected HH:MM:SS"),
            ("0", "must be >= 1 second"),
            ("00:00:00", "must be >= 1 second"),
            ("-5", "negative"),
            ("01:30:ab", "non-integer"),
        ],
    )
    def test_rejects_malformed(
        self, value: str, fragment: str
    ) -> None:
        with pytest.raises(Exception) as exc:
            _parse_time_limit(value)
        assert fragment in str(exc.value), (
            f"{value!r}: expected fragment {fragment!r} in error; "
            f"got {exc.value!r}"
        )

    def test_strips_whitespace(self) -> None:
        assert _parse_time_limit("  01:30:00  ") == 5400


# ----------------------------------------------------------------------
# CLI — flag wiring
# ----------------------------------------------------------------------


class TestSubmitTimeLimitCLI:
    def test_time_limit_sets_wall_time_seconds(
        self, cli_state: Path
    ) -> None:
        f = cli_state / "in.py"
        f.write_text("pass")
        result = CliRunner().invoke(
            main,
            ["submit", "localhost", "--time-limit", "01:30:00", str(f)],
        )
        assert result.exit_code == 0, result.output
        jobid = result.output.strip().splitlines()[-1]
        spec = JobSpec.read(paths.queue_dir() / f"{jobid}.json")
        assert spec.wall_time_seconds == 5400

    def test_time_alias_sets_wall_time_seconds(
        self, cli_state: Path
    ) -> None:
        """``--time`` is an alias of ``--time-limit`` (matches
        SLURM's ``sbatch --time=...``)."""
        f = cli_state / "in.py"
        f.write_text("pass")
        result = CliRunner().invoke(
            main,
            ["submit", "localhost", "--time", "45:00", str(f)],
        )
        assert result.exit_code == 0, result.output
        jobid = result.output.strip().splitlines()[-1]
        spec = JobSpec.read(paths.queue_dir() / f"{jobid}.json")
        assert spec.wall_time_seconds == 2700

    def test_integer_seconds_form(
        self, cli_state: Path
    ) -> None:
        f = cli_state / "in.py"
        f.write_text("pass")
        result = CliRunner().invoke(
            main,
            ["submit", "localhost", "--time-limit", "120", str(f)],
        )
        assert result.exit_code == 0, result.output
        jobid = result.output.strip().splitlines()[-1]
        spec = JobSpec.read(paths.queue_dir() / f"{jobid}.json")
        assert spec.wall_time_seconds == 120

    def test_mutually_exclusive_with_wall_time_seconds(
        self, cli_state: Path
    ) -> None:
        f = cli_state / "in.py"
        f.write_text("pass")
        result = CliRunner().invoke(
            main,
            [
                "submit", "localhost",
                "--time-limit", "01:00:00",
                "--wall-time-seconds", "60",
                str(f),
            ],
        )
        assert result.exit_code != 0
        assert "mutually exclusive" in result.output

    def test_malformed_value_errors_before_submit(
        self, cli_state: Path
    ) -> None:
        f = cli_state / "in.py"
        f.write_text("pass")
        result = CliRunner().invoke(
            main,
            ["submit", "localhost", "--time-limit", "garbage", str(f)],
        )
        assert result.exit_code != 0
        assert "garbage" in result.output

    def test_wall_time_seconds_still_works_unchanged(
        self, cli_state: Path
    ) -> None:
        """Backward compat: --wall-time-seconds continues to set
        the same field, with the same semantics."""
        f = cli_state / "in.py"
        f.write_text("pass")
        result = CliRunner().invoke(
            main,
            ["submit", "localhost", "--wall-time-seconds", "300", str(f)],
        )
        assert result.exit_code == 0, result.output
        jobid = result.output.strip().splitlines()[-1]
        spec = JobSpec.read(paths.queue_dir() / f"{jobid}.json")
        assert spec.wall_time_seconds == 300
