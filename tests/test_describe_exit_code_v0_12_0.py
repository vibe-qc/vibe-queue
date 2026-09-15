"""Tests for v0.12.0 describe_exit_code: decode bash's 128+sig convention so a
signal kill (137 SIGKILL / 139 SIGSEGV) reads as a named crash, not an opaque
number. The portable signal map covers only signals whose numbers match across
Linux and macOS, since the CLI may render a Linux job's exit code on a Mac.
"""
from __future__ import annotations

from vq.spec import describe_exit_code, signal_name_for_exit


class TestDescribeExitCode:
    def test_normal_exit_codes_render_plain(self) -> None:
        assert describe_exit_code(0) == "0"
        assert describe_exit_code(1) == "1"
        assert describe_exit_code(127) == "127"

    def test_none_is_unknown(self) -> None:
        assert describe_exit_code(None) == "unknown"

    def test_sigkill_137_names_signal_and_hints_oom(self) -> None:
        out = describe_exit_code(137)
        assert out.startswith("137 (killed by SIGKILL")
        assert "OOM" in out

    def test_sigsegv_139(self) -> None:
        out = describe_exit_code(139)
        assert out.startswith("139 (killed by SIGSEGV")
        assert "segmentation fault" in out

    def test_sigterm_143(self) -> None:
        assert describe_exit_code(143).startswith("143 (killed by SIGTERM")

    def test_portable_signal_without_hint(self) -> None:
        # SIGHUP (1) -> 129, in the portable map but carries no hint.
        assert describe_exit_code(129) == "129 (killed by SIGHUP)"

    def test_nonportable_signal_falls_back_to_number(self) -> None:
        # SIGBUS differs across platforms (7 on Linux, 10 on macOS), so it is
        # left out of the portable map: render the raw signal number.
        assert describe_exit_code(128 + 7) == "135 (killed by signal 7)"

    def test_boundaries_are_plain(self) -> None:
        # 128 itself is not a signal (sig 0); above 128+64 is out of range.
        assert describe_exit_code(128) == "128"
        assert describe_exit_code(255) == "255"


class TestSignalNameForExit:
    """The short companion used by brief surfaces (notifications, fetch suffix)."""

    def test_portable_signal_names(self) -> None:
        assert signal_name_for_exit(137) == "SIGKILL"
        assert signal_name_for_exit(139) == "SIGSEGV"
        assert signal_name_for_exit(143) == "SIGTERM"

    def test_non_signal_codes_are_none(self) -> None:
        assert signal_name_for_exit(0) is None
        assert signal_name_for_exit(1) is None
        assert signal_name_for_exit(128) is None
        assert signal_name_for_exit(None) is None

    def test_nonportable_signal_is_none(self) -> None:
        # SIGBUS (7 on Linux, 10 on macOS) is not portably mapped.
        assert signal_name_for_exit(128 + 7) is None
