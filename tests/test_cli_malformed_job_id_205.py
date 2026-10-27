"""#205: a malformed job id is a usage error, not a traceback."""
from __future__ import annotations

import pytest
from click.testing import CliRunner

from vq.cli import main


@pytest.mark.parametrize("command", ["status", "logs", "output", "events"])
@pytest.mark.parametrize("args", [["localhost", "bad id"], ["a\nb"]])
def test_a_malformed_job_id_is_refused_before_any_lookup(
    command: str, args: list[str]
) -> None:
    result = CliRunner().invoke(main, [command, *args])
    assert result.exit_code == 2, result.output
    assert result.exception is None or isinstance(result.exception, SystemExit)
    assert "invalid job id" in result.output
