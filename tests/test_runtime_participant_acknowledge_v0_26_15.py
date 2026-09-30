"""#72 review: a refused ``acknowledge`` is a migration error, not a traceback.

``participant_acknowledge`` sends the terminal ``acknowledge`` through the
journaled ``_rpc``, which raises the module-private ``_RefusedError`` on a
``refused`` answer. Inside apply and rollback that exception is caught by the
transaction driver; the acknowledgement ran outside it, so a participant that
answered "not yet" escaped ``vq admin migrate-runtime --acknowledge`` as an
unhandled exception. The transaction itself was fine, and stays ``applied``.
"""
from __future__ import annotations

import pytest

from tests.test_runtime_participant_72 import (  # noqa: F401 -- fixtures
    HEALTHY,
    _apply,
    _journal,
    site,
    tx,
)
from vq import runtime_migration


def test_a_refused_acknowledge_is_a_migration_error_and_retries(tx) -> None:  # noqa: F811
    tx.script("verify", HEALTHY)
    assert _apply(tx)["state"] == "applied"
    tx.script("acknowledge", {"outcome": "refused", "errors": ["publication lock held"]})

    with pytest.raises(runtime_migration.MigrationError, match="refused") as info:
        runtime_migration.recover(tx.receipt, acknowledge=True)

    assert "publication lock held" in str(info.value)
    assert "stays applied" in str(info.value)
    journal = _journal(tx)
    assert journal["state"] == "applied"
    [row] = [r for r in journal["rows"] if r["step"] == "acknowledge"]
    assert row["response"] is None and row["last_refusal"] == ["publication lock held"]

    # The next --acknowledge re-sends the same op_id and completes.
    state = runtime_migration.recover(tx.receipt, acknowledge=True)
    assert state["state"] == "acknowledged"
    acks = [r for r in tx.requests() if r["op"] == "acknowledge"]
    assert len(acks) == 2 and acks[0]["op_id"] == acks[1]["op_id"]


def test_the_cli_reports_a_refused_acknowledge_cleanly(tx, monkeypatch) -> None:  # noqa: F811
    from click.testing import CliRunner

    from vq.cli import main

    tx.script("verify", HEALTHY)
    assert _apply(tx)["state"] == "applied"
    tx.script("acknowledge", {"outcome": "refused", "errors": ["not yet"]})

    result = CliRunner().invoke(main, ["admin", "migrate-runtime", "--receipt-dir",
                                       str(tx.receipt), "--acknowledge"])

    assert result.exit_code != 0
    assert result.exception is None or isinstance(result.exception, SystemExit)
    assert "refused" in result.output and "not yet" in result.output
    assert "Traceback" not in result.output
