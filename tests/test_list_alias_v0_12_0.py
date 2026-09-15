"""Tests for v0.12.0 `vq list`, a name-level alias for `vq queue` (the generic
intuitive alias pattern, like `summary` for `overview`). `vq list` previously
errored with "No such command".
"""
from __future__ import annotations

from click.testing import CliRunner

from vq.cli import main


def test_list_is_registered_as_queue_alias() -> None:
    assert "list" in main.commands
    assert "queue" in main.commands
    # Same Click command object under both names (no drift between them).
    assert main.commands["list"] is main.commands["queue"]


def test_bare_list_no_longer_errors() -> None:
    # The papercut: `vq list` used to fail with "No such command 'list'".
    result = CliRunner().invoke(main, ["list", "--help"])
    assert result.exit_code == 0
    assert "No such command" not in result.output
