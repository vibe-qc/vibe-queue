"""#159: `vq estimate --input DIR` fingerprints a directory's regular files.

A directory input stands for every regular file under it, in relative-path
order, opened without following a link. Links and special files are left out
with a warning, as a `vq submit -d` payload carries a link as a link. The
bounds of file inputs apply after expansion. The decisive case runs the CLI:
an estimate of a directory is accepted by `vq submit --estimate -d` of that
directory, and refused once a file in it changes.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import config, paths
from vq import resource_estimate as estimates
from vq.cli import main
from vq.spec import JobSpec


def _manifest(tmp_path: Path) -> Path:
    manifest = tmp_path / "calc.system"
    manifest.write_text('[vibeqc]\nversion = "example"\n[plan]\nmethod = "hf"\n'
                        '[outputs]\nstatus = "dry_run"\n[memory]\nestimate_bytes = 67108864\n')
    return manifest


def _tree(root: Path) -> Path:
    (root / "a").mkdir(parents=True)
    (root / "a" / "x.py").write_text("print('x')\n")
    (root / "b.xyz").write_text("1\n\nH 0 0 0\n")
    (root / "a" / "deeper").mkdir()
    (root / "a" / "deeper" / "basis.gbs").write_text("H 0\n")
    return root


def test_a_directory_stands_for_its_files_in_relative_path_order(tmp_path: Path) -> None:
    tree = _tree(tmp_path / "calc")
    manifest = _manifest(tmp_path)
    whole = estimates.estimate_vibeqc(manifest, inputs=(tree,))
    named = estimates.estimate_vibeqc(manifest, inputs=(
        tree / "a" / "deeper" / "basis.gbs", tree / "a" / "x.py", tree / "b.xyz"))
    assert whole.input_files == named.input_files and len(whole.input_files) == 3
    assert whole.input_sha256 == named.input_sha256
    assert whole.input_binding == "caller_supplied_inputs"
    assert not any("link" in warning for warning in whole.warnings)


def test_file_inputs_give_the_receipt_they_always_did(tmp_path: Path) -> None:
    a = tmp_path / "a.py"
    a.write_text("print('a')\n")
    b = tmp_path / "b.xyz"
    b.write_text("1\n\nH 0 0 0\n")
    receipt = estimates.estimate_vibeqc(_manifest(tmp_path), inputs=(a, b))
    # Computed with the previous code, which took regular files only.
    assert receipt.input_sha256 == (
        "da1f4abedbccec62add5d7c5085bcea1444f37338758cfe3f270110c41c374dc")


def test_links_and_special_files_are_left_out_and_counted(tmp_path: Path) -> None:
    tree = _tree(tmp_path / "calc")
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "secret.py").write_text("not part of the payload\n")
    (tree / "linked.py").symlink_to(outside / "secret.py")
    (tree / "linked-dir").symlink_to(outside, target_is_directory=True)
    os.mkfifo(tree / "pipe")
    receipt = estimates.estimate_vibeqc(_manifest(tmp_path), inputs=(tree,))
    plain = estimates.estimate_vibeqc(_manifest(tmp_path), inputs=(_tree(tmp_path / "copy"),))
    assert receipt.input_files == plain.input_files
    assert ("3 link(s) or special file(s) under an input directory were not fingerprinted; "
            "a submission carries a link as a link.") in receipt.warnings


def test_a_file_swapped_for_a_link_is_never_followed(tmp_path: Path) -> None:
    target = tmp_path / "target.py"
    target.write_text("x\n")
    (tmp_path / "swapped.py").symlink_to(target)
    with pytest.raises(estimates.EstimateError, match="cannot fingerprint .*swapped.py"):
        estimates._read_unlinked(tmp_path / "swapped.py")
    os.mkfifo(tmp_path / "became-a-pipe")
    with pytest.raises(estimates.EstimateError, match="changed while it was being fingerprinted"):
        estimates._read_unlinked(tmp_path / "became-a-pipe")


@pytest.mark.parametrize(("bound", "value", "message"), [
    ("MAX_INPUT_FILES", 2, "more than the 2 files one estimate fingerprints"),
    ("MAX_INPUT_BYTES", 8, "exceeds the 8-byte estimation limit"),
    ("MAX_INPUT_ENTRIES", 4, "more than 4 entries to search"),
])
def test_the_bounds_hold_after_expansion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, bound: str, value: int, message: str,
) -> None:
    tree = _tree(tmp_path / "calc")
    monkeypatch.setattr(estimates, bound, value)
    with pytest.raises(estimates.EstimateError, match=message):
        estimates.estimate_vibeqc(_manifest(tmp_path), inputs=(tree,))


def test_files_named_before_a_directory_count_towards_the_bound(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    tree = _tree(tmp_path / "calc")
    monkeypatch.setattr(estimates, "MAX_INPUT_FILES", 3)
    with pytest.raises(estimates.EstimateError, match="more than the 3 files"):
        estimates.estimate_vibeqc(_manifest(tmp_path), inputs=(tree / "b.xyz", tree))
    with pytest.raises(estimates.EstimateError, match="at most 3 input files"):
        estimates.estimate_vibeqc(_manifest(tmp_path), inputs=(tree, tree / "b.xyz"))


def test_a_directory_with_nothing_to_fingerprint_is_refused(tmp_path: Path) -> None:
    empty = tmp_path / "empty"
    (empty / "sub").mkdir(parents=True)
    with pytest.raises(estimates.EstimateError, match="holds no regular file to fingerprint"):
        estimates.estimate_vibeqc(_manifest(tmp_path), inputs=(empty,))


@pytest.mark.skipif(os.geteuid() == 0, reason="root reads any directory")
def test_an_unreadable_subdirectory_is_an_error_not_a_gap(tmp_path: Path) -> None:
    tree = _tree(tmp_path / "calc")
    (tree / "a").chmod(0)
    try:
        with pytest.raises(PermissionError):
            estimates.estimate_vibeqc(_manifest(tmp_path), inputs=(tree,))
    finally:
        (tree / "a").chmod(0o755)


def test_end_to_end_a_directory_estimate_binds_a_dir_submission(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    tree = _tree(tmp_path / "calc")
    (tree / "linked.py").symlink_to("b.xyz")  # carried as a link, so not fingerprinted
    runner = CliRunner()
    estimated = runner.invoke(main, ["estimate", str(_manifest(tmp_path)), "--input", str(tree),
                                     "--json"])
    assert estimated.exit_code == 0, estimated.output
    receipt = tmp_path / "receipt.json"
    receipt.write_text(estimated.output)
    assert len(json.loads(estimated.output)["input_files"]) == 3

    submitted = runner.invoke(main, ["submit", "localhost", "--estimate", str(receipt),
                                     "-d", str(tree), "--", "true"])
    assert submitted.exit_code == 0, submitted.output
    jobid = submitted.output.strip().splitlines()[-1]
    assert JobSpec.read(paths.spec_path(jobid)).resource_estimate == json.loads(
        estimated.output)

    (tree / "a" / "x.py").write_text("print('edited')\n")
    refused = runner.invoke(main, ["submit", "localhost", "--estimate", str(receipt),
                                   "-d", str(tree), "--", "true"])
    assert refused.exit_code != 0
    assert "the estimate fingerprinted 1 input file(s) that this submission does not carry" in (
        refused.output)

    help_text = runner.invoke(main, ["estimate", "--help"]).output
    assert "every regular file" in help_text and "under a directory" in help_text
