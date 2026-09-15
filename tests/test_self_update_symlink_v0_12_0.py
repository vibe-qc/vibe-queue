"""Tests for v0.12.0 daemon self-restart fix: ``_execstart_matches_venv``
recognizes a ``~/.local/bin/vq`` symlink ExecStart (the host_b/host_e shape),
not only a direct ``.venv/bin`` path, so ``vq admin update`` restarts the
daemon onto the new code instead of leaving it on the old.
"""
from __future__ import annotations

import os
from pathlib import Path

from vq.admin import _execstart_matches_venv


def _make_venv_with_symlink(root: Path) -> tuple[Path, Path, Path]:
    """Build a venv bin with a real vq script + a ~/.local/bin/vq symlink
    into it. Returns (venv_bin, direct_vq, symlink_vq)."""
    venv_bin = root / "venv" / "bin"
    venv_bin.mkdir(parents=True)
    direct = venv_bin / "vq"
    direct.write_text("#!/bin/sh\n")
    localbin = root / "local" / "bin"
    localbin.mkdir(parents=True)
    symlink = localbin / "vq"
    os.symlink(direct, symlink)
    return venv_bin.resolve(), direct, symlink


class TestExecstartMatchesVenv:
    def test_direct_venv_path_matches(self, tmp_path: Path) -> None:
        venv_bin, direct, _ = _make_venv_with_symlink(tmp_path)
        assert _execstart_matches_venv(str(direct), venv_bin)

    def test_local_bin_symlink_matches(self, tmp_path: Path) -> None:
        # The host_b/host_e shape: ExecStart ~/.local/bin/vq -> .venv/bin/vq.
        # This is the case the pre-fix .parent.resolve() missed.
        venv_bin, _, symlink = _make_venv_with_symlink(tmp_path)
        assert _execstart_matches_venv(str(symlink), venv_bin)

    def test_unrelated_path_does_not_match(self, tmp_path: Path) -> None:
        venv_bin, _, _ = _make_venv_with_symlink(tmp_path)
        assert not _execstart_matches_venv("/usr/bin/vq", venv_bin)

    def test_symlink_into_other_venv_does_not_match(self, tmp_path: Path) -> None:
        # A symlink that resolves into a DIFFERENT venv must not match.
        venv_bin, _, _ = _make_venv_with_symlink(tmp_path)
        other_bin = tmp_path / "other" / "bin"
        other_bin.mkdir(parents=True)
        (other_bin / "vq").write_text("#!/bin/sh\n")
        ln = tmp_path / "ln_vq"
        os.symlink(other_bin / "vq", ln)
        assert not _execstart_matches_venv(str(ln), venv_bin)
