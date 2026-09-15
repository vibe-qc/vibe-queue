"""Tests for v0.12.0 cli._exit_suffix: the single-fetch result line names the
signal for a hard kill (137 -> SIGKILL), matching vq status and the fetch-all
summary. signal_name_for_exit (spec.py) does the decode, and this wraps it for
the ` (exit_code=N[, SIGNAME])` suffix.
"""
from __future__ import annotations

from vq.cli import _exit_suffix


def test_signal_exit_names_the_signal() -> None:
    assert _exit_suffix(137) == " (exit_code=137, SIGKILL)"
    assert _exit_suffix(139) == " (exit_code=139, SIGSEGV)"


def test_plain_exit_has_no_signal() -> None:
    assert _exit_suffix(0) == " (exit_code=0)"
    assert _exit_suffix(1) == " (exit_code=1)"


def test_none_is_empty() -> None:
    assert _exit_suffix(None) == ""
