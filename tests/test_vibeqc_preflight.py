"""``vq.vibeqc_preflight.vibeqc_dry_run_preflight`` — dry-run pre-flight.

Pins the contract for the helper that ``vq submit --vibeqc-preflight``
calls before writing the JobSpec:

  1. A script that produces a valid Phase-O1 ``*.system`` manifest
     (status = "dry_run", with a ``[plan]`` section) yields a
     :class:`PreflightResult` carrying the declared artefact paths,
     the stem, and the method / basis / functional.
  2. Failure modes are *non-fatal* — they return ``None`` and log a
     warning. The submit proceeds normally.
     * No ``*.system`` produced
     * Manifest has no ``[plan]`` section (pre-Phase-O1 vibe-qc)
     * Script exits non-zero
     * Script times out
     * Manifest is malformed TOML
  3. The ``VIBEQC_DRY_RUN`` env var is propagated to the
     subprocess (the workhorse short-circuit lives in vibe-qc's
     run_job, not here).
"""

from __future__ import annotations

import sys
from pathlib import Path
from textwrap import dedent

import pytest

from vq import vibeqc_preflight as preflight_module
from vq.vibeqc_preflight import (
    PreflightResult,
    vibeqc_dry_run_preflight,
)

# ---------------------------------------------------------------------- #
# Test helpers
# ---------------------------------------------------------------------- #

def _write_script(workspace: Path, name: str, body: str) -> Path:
    p = workspace / name
    p.write_text(dedent(body).lstrip(), encoding="utf-8")
    return p


def _seed_manifest(workspace: Path, stem: str = "output-h2o",
                   status: str = "dry_run",
                   files: tuple[str, ...] = (
                       "output-h2o.out",
                       "output-h2o.system",
                       "output-h2o.molden",
                   ),
                   method: str = "RHF",
                   basis: str = "sto-3g",
                   functional: str = "",
                   job_kind: str = "molecular_scf",
                   estimate_bytes: int | None = None) -> Path:
    """Write a hand-crafted *.system manifest into the workspace,
    matching the Phase-O1 shape closely enough that the helper
    can parse it. Used by tests that exercise the manifest-reading
    path without needing a vibe-qc install."""
    rows = "\n\n".join(
        f"[[plan.files]]\n"
        f'role = "log"\n'
        f'path = "{p}"\n'
        f'format = "text"\n'
        f'always = true\n'
        f'description = "x"\n'
        for p in files
    )
    memory = (
        f"\n[memory]\nestimate_bytes = {int(estimate_bytes)}\n"
        if estimate_bytes is not None else ""
    )
    body = (
        '[vibeqc]\nversion = "0.8.0-test"\n'
        '\n[plan]\n'
        f'stem = "{stem}"\n'
        f'method = "{method}"\n'
        f'basis = "{basis}"\n'
        f'functional = "{functional}"\n'
        f'options_digest = "f00f00"\n'
        f'job_kind = "{job_kind}"\n'
        '\n' + rows + '\n'
        + memory +
        '\n[outputs]\n'
        f'status = "{status}"\n'
        'finished_at_iso = ""\n'
    )
    manifest = workspace / f"{stem}.system"
    manifest.parent.mkdir(parents=True, exist_ok=True)
    manifest.write_text(body, encoding="utf-8")
    return manifest


def _write_manifest_producer(
    workspace: Path,
    *manifests: Path,
    prelude: str = "",
) -> Path:
    """Write a script that observably rewrites the supplied manifests."""
    lines = [dedent(prelude).strip(), "from pathlib import Path"]
    for manifest in manifests:
        relative = manifest.relative_to(workspace).as_posix()
        lines.extend(
            (
                f"manifest = Path({relative!r})",
                "manifest.write_text(manifest.read_text() + '\\n')",
            )
        )
    return _write_script(
        workspace,
        "input.py",
        "\n".join(line for line in lines if line),
    )


# ---------------------------------------------------------------------- #
# Happy path
# ---------------------------------------------------------------------- #

def test_preflight_returns_result_when_manifest_present(
    tmp_path: Path,
) -> None:
    # A script that just writes the manifest and exits — simulating
    # vibe-qc's dry-run short-circuit.
    manifest = _seed_manifest(tmp_path)
    script = _write_manifest_producer(tmp_path, manifest)
    result = vibeqc_dry_run_preflight(
        tmp_path, [sys.executable, script.name],
    )
    assert isinstance(result, PreflightResult)
    assert "output-h2o.out" in result.expected_outputs
    assert "output-h2o.molden" in result.expected_outputs
    assert result.output_stem == "output-h2o"
    assert result.method == "RHF"
    assert result.basis == "sto-3g"


def test_preflight_runs_command_with_dry_run_env(
    tmp_path: Path,
) -> None:
    """The pre-flight sets VIBEQC_DRY_RUN=1 — verify the script can
    see it (the short-circuit gate lives in vibe-qc, but here we
    confirm the env var is plumbed through). The script asserts the
    env var is set and then writes a manifest."""
    manifest = _seed_manifest(
        tmp_path,
        stem="job",
        method="RKS",
        functional="PBE",
    )
    script = _write_manifest_producer(
        tmp_path,
        manifest,
        prelude="""
        import os
        assert os.environ.get("VIBEQC_DRY_RUN") == "1", (
            "preflight didn't set VIBEQC_DRY_RUN"
        )
        """,
    )
    result = vibeqc_dry_run_preflight(
        tmp_path, [sys.executable, script.name],
    )
    assert result is not None
    assert result.method == "RKS"
    assert result.functional == "PBE"


def test_preflight_accepts_neb_manifest_with_estimate_and_no_files(
    tmp_path: Path,
) -> None:
    manifest = _seed_manifest(
        tmp_path,
        stem="h3-neb",
        files=(),
        method="uhf",
        basis="sto-3g",
        functional="",
        job_kind="neb",
        estimate_bytes=123456789,
    )
    script = _write_manifest_producer(tmp_path, manifest)

    result = vibeqc_dry_run_preflight(
        tmp_path,
        [sys.executable, script.name],
        with_estimate=True,
    )

    assert result is not None
    assert result.expected_outputs == []
    assert result.output_stem == "h3-neb"
    assert result.method == "uhf"
    assert result.basis == "sto-3g"
    assert result.estimate_bytes == 123456789


def test_preflight_returns_workspace_relative_paths(
    tmp_path: Path,
) -> None:
    """If the manifest declares absolute paths, they're normalised
    to workspace-relative POSIX paths so the field round-trips
    consistently across hosts."""
    abs_path = (tmp_path / "deep" / "nested" / "out.molden").resolve()
    manifest = _seed_manifest(
        tmp_path,
        files=(str(abs_path), "rel.out"),
    )
    script = _write_manifest_producer(tmp_path, manifest)
    result = vibeqc_dry_run_preflight(
        tmp_path, [sys.executable, script.name],
    )
    assert result is not None
    # The absolute path is rewritten relative to the workspace.
    assert any(p.endswith("out.molden")
               for p in result.expected_outputs)
    # POSIX separators only.
    for p in result.expected_outputs:
        assert "\\" not in p


def test_preflight_discovers_nested_dry_run_manifest(
    tmp_path: Path,
) -> None:
    manifest = _seed_manifest(
        tmp_path,
        stem="results/chemistry",
        files=(
            "results/chemistry.out",
            "results/chemistry.system",
            "telemetry/live.jsonl",
        ),
        method="RKS",
        functional="PBE",
    )
    script = _write_manifest_producer(tmp_path, manifest)

    result = vibeqc_dry_run_preflight(
        tmp_path,
        [sys.executable, script.name],
    )

    assert result is not None
    assert result.expected_outputs == [
        "results/chemistry.out",
        "results/chemistry.system",
        "telemetry/live.jsonl",
    ]
    assert result.output_stem == "chemistry"
    assert result.method == "RKS"
    assert result.functional == "PBE"


def test_preflight_rejects_multiple_changed_dry_run_manifests(
    tmp_path: Path,
) -> None:
    zeta = _seed_manifest(tmp_path, stem="zeta", method="UHF")
    alpha = _seed_manifest(tmp_path, stem="alpha", method="RKS")
    script = _write_manifest_producer(tmp_path, zeta, alpha)

    result = vibeqc_dry_run_preflight(
        tmp_path,
        [sys.executable, script.name],
    )

    assert result is None


def test_preflight_new_nested_manifest_beats_stale_top_level_manifest(
    tmp_path: Path,
) -> None:
    current = _seed_manifest(tmp_path, stem="alpha/job", method="RKS")
    _seed_manifest(tmp_path, stem="zeta", method="UHF")
    script = _write_manifest_producer(tmp_path, current)

    result = vibeqc_dry_run_preflight(
        tmp_path,
        [sys.executable, script.name],
    )

    assert result is not None
    assert result.method == "RKS"


def test_preflight_ignores_manifest_symlink_outside_workspace(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    outside_manifest = _seed_manifest(outside, stem="escape", method="UHF")
    (workspace / "escape.system").symlink_to(outside_manifest)
    current = _seed_manifest(
        workspace,
        stem="results/chemistry",
        method="RKS",
    )
    script = _write_manifest_producer(workspace, current)

    result = vibeqc_dry_run_preflight(
        workspace,
        [sys.executable, script.name],
    )

    assert result is not None
    assert result.method == "RKS"


def test_preflight_drops_unsafe_declared_output_paths(tmp_path: Path) -> None:
    outside = tmp_path.parent / f"{tmp_path.name}-outside"
    outside.mkdir()
    (tmp_path / "link").symlink_to(outside, target_is_directory=True)
    manifest = _seed_manifest(
        tmp_path,
        files=(
            "safe.out",
            "../escape.out",
            str(outside / "absolute.out"),
            "link/symlink-escape.out",
        ),
    )
    script = _write_manifest_producer(tmp_path, manifest)

    result = vibeqc_dry_run_preflight(
        tmp_path,
        [sys.executable, script.name],
    )

    assert result is not None
    assert result.expected_outputs == ["safe.out"]


@pytest.mark.parametrize(
    "stem",
    [
        "../../outside/chemistry",
        "/outside/chemistry",
        "unsafe\\u0000stem",
    ],
)
def test_preflight_drops_unsafe_output_stem(
    tmp_path: Path,
    stem: str,
) -> None:
    manifest = _seed_manifest(tmp_path)
    body = manifest.read_text(encoding="utf-8").replace(
        'stem = "output-h2o"',
        f'stem = "{stem}"',
    )
    manifest.write_text(body, encoding="utf-8")
    script = _write_manifest_producer(tmp_path, manifest)

    result = vibeqc_dry_run_preflight(
        tmp_path,
        [sys.executable, script.name],
    )

    assert result is not None
    assert result.output_stem is None


def test_preflight_scan_bound_fails_before_running_command(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    script = _write_script(tmp_path, "input.py", "raise AssertionError")
    (tmp_path / "extra.txt").write_text("x", encoding="utf-8")
    monkeypatch.setattr(
        preflight_module,
        "_MANIFEST_SCAN_MAX_ENTRIES",
        1,
    )

    def fail_run(*_args, **_kwargs):
        raise AssertionError("bounded discovery must fail before subprocess")

    monkeypatch.setattr(preflight_module.subprocess, "run", fail_run)

    assert vibeqc_dry_run_preflight(
        tmp_path,
        [sys.executable, script.name],
    ) is None


# ---------------------------------------------------------------------- #
# Failure modes — all non-fatal
# ---------------------------------------------------------------------- #

def test_no_manifest_returns_none(tmp_path: Path) -> None:
    # Script exits cleanly but writes no manifest — typical of a
    # script that doesn't use vibe-qc at all.
    script = _write_script(tmp_path, "input.py", """
        import sys; sys.exit(0)
    """)
    assert vibeqc_dry_run_preflight(
        tmp_path, [sys.executable, script.name],
    ) is None


def test_script_nonzero_exit_returns_none(tmp_path: Path) -> None:
    # Script crashes — pre-flight should swallow the failure.
    script = _write_script(tmp_path, "input.py", """
        import sys; sys.exit(7)
    """)
    assert vibeqc_dry_run_preflight(
        tmp_path, [sys.executable, script.name],
    ) is None


def test_script_timeout_returns_none(tmp_path: Path) -> None:
    # Script hangs forever — pre-flight should kill it and return None.
    script = _write_script(tmp_path, "input.py", """
        import time
        time.sleep(60)
    """)
    result = vibeqc_dry_run_preflight(
        tmp_path, [sys.executable, script.name], timeout=0.3,
    )
    assert result is None


def test_manifest_without_plan_section_returns_none(
    tmp_path: Path,
) -> None:
    # Pre-Phase-O1 vibe-qc manifest: has [outputs].status="dry_run"
    # but no [plan] section. The helper should not match it.
    manifest = tmp_path / "legacy.system"
    manifest.write_text(
        '[outputs]\nstatus = "dry_run"\n',
        encoding="utf-8",
    )
    script = _write_manifest_producer(tmp_path, manifest)
    assert vibeqc_dry_run_preflight(
        tmp_path, [sys.executable, script.name],
    ) is None


def test_malformed_manifest_returns_none(tmp_path: Path) -> None:
    # A *.system file in the workspace that isn't valid TOML.
    manifest = tmp_path / "broken.system"
    manifest.write_text(
        "this is not toml\n[[invalid",
        encoding="utf-8",
    )
    script = _write_manifest_producer(tmp_path, manifest)
    assert vibeqc_dry_run_preflight(
        tmp_path, [sys.executable, script.name],
    ) is None


def test_running_status_manifest_does_not_match(
    tmp_path: Path,
) -> None:
    """A leftover manifest with status="running" from a previous
    real (non-dry-run) execution should be ignored — we look only
    for the dry-run sentinel."""
    manifest = _seed_manifest(tmp_path, status="running")
    script = _write_manifest_producer(tmp_path, manifest)
    assert vibeqc_dry_run_preflight(
        tmp_path, [sys.executable, script.name],
    ) is None


def test_command_executable_missing_returns_none(
    tmp_path: Path,
) -> None:
    """A subprocess that fails to start (interpreter missing,
    permission denied, etc.) is non-fatal — submit proceeds without
    the optional fields."""
    assert vibeqc_dry_run_preflight(
        tmp_path,
        ["/this/path/does/not/exist/python", "x.py"],
    ) is None
