"""Regression guards for the two real F821 (undefined-name) bugs the v0.12.0
ruff-drift audit surfaced. Both would AttributeError pre-fix.

The systemic guard for this whole class is a ruff F821 gate in CI (proposed
separately); these pin the two specific fixes.
"""
from __future__ import annotations

import logging


def test_admin_defines_a_logger() -> None:
    # admin.py called log.warning in write_admin_status's multi-user
    # RPC-fallback but never defined `log` (no import logging / getLogger), so
    # that untested path raised NameError, masking the original RPCError.
    from vq import admin

    assert isinstance(admin.log, logging.Logger)


def test_resubmit_imports_jobstate() -> None:
    # resubmit.py annotated `states: list[JobState]` but imported only
    # TERMINAL_STATES + JobSpec, leaving JobState undefined in the annotation.
    from vq import resubmit
    from vq.spec import JobState

    assert resubmit.JobState is JobState
