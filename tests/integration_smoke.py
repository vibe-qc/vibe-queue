#!/usr/bin/env python3
"""Live integration smoke test for the vq queue against a configured host.

NOT a pytest test — runs against REAL host_d (or whatever
default_host is set in ~/.config/vq/config.toml). Each engine gets a
tiny job that should finish in well under a minute, then we fetch the
workspace and check the expected output files are there.

Engines exercised:
    * vibe-qc dev      (`--branch main`)
    * vibe-qc release  (`--branch release`)
    * ORCA + orca_2mkl (separate absolute binaries; nonempty Molden output)
    * CRYSTAL serial   (run-crystal.sh --serial, CRYSTAL_BIN= override)
    * CRYSTAL parallel (run-crystal.sh --np 4, PCRYSTAL_BIN= override)
    * PySCF            (via vibeqc-dev venv, plain script with `import pyscf`)
    * Psi4             (absolute binary from `vq programs --json`)

Usage:
    python tests/integration_smoke.py
    python tests/integration_smoke.py --only crystal,pcrystal
    python tests/integration_smoke.py --keep   # don't auto-cleanup workspaces

v0.5.19 design: the smoke test reads `vq programs --json` once and
hands absolute binary paths into each engine's submit() function.
That removes the entire PATH-fragility class — a daemon launched via
``nohup`` with a minimal PATH still runs every engine, because the
engines no longer depend on ``crystal`` / ``orca`` / ``psi4`` being
discoverable through PATH lookups.

Skip rules:
* If a program isn't in `vq programs`, the corresponding test is
  skipped (not failed). Useful when Psi4 isn't installed yet, or when
  ORCA was removed.
* If the engine probe fails availability check (binary missing, venv
  broken), test is skipped with a descriptive reason.
* If a binary engine has no `binary` field in the JSON record (e.g.
  somebody mis-registered orca as `kind="venv"`), test is skipped.
* If an engine's separately registered companion is unavailable or has
  the wrong kind, the engine is skipped before any job is submitted.

Exit code: 0 if every NON-skipped test passed; non-zero otherwise.
The intent is "run this whenever you suspect something changed on the
queue host" — quick (< 2 minutes total), comprehensive, machine-checkable.
"""
from __future__ import annotations

import argparse
import contextlib
import json
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
import textwrap
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

# Per-engine wall-time budgets (generous; real jobs are 1-30 s each).
DEFAULT_WALL_TIME = 600  # 10 minutes per job


# ----------------------------------------------------------------------
# Result types
# ----------------------------------------------------------------------


@dataclass
class EngineResult:
    name: str
    status: str  # "PASS" | "FAIL" | "SKIP"
    reason: str = ""
    jobid: str | None = None
    wall_seconds: float | None = None
    artifacts_found: list[str] = field(default_factory=list)
    artifacts_missing: list[str] = field(default_factory=list)


# ----------------------------------------------------------------------
# vq command-line interface helpers
# ----------------------------------------------------------------------


def run(cmd: list[str], *, timeout: float = 30.0, check: bool = True) -> str:
    """Run a vq CLI command, return stdout. Raises RuntimeError on
    non-zero exit. ``check=False`` returns stdout even on failure."""
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if check and proc.returncode != 0:
        raise RuntimeError(
            f"{' '.join(cmd)} -> exit {proc.returncode}: {proc.stderr.strip()}"
        )
    return proc.stdout


def vq(*args: str) -> str:
    """Convenience wrapper for `vq <args>`."""
    return run(["vq", *args])


def get_registered_programs() -> dict[str, dict[str, object]]:
    """Parse `vq programs --json` -> {name: record} dict.

    Each record carries the kind-specific schema documented in
    `vq programs --help`:
      * binary: name, kind="binary", status, reason, binary
      * venv:   name, kind="venv", status, reason, python, git_dir, branch, update_script
      * import: name, kind="import", status, reason, python, import_check

    Returns empty dict on parse or command failure.
    """
    try:
        out = vq("programs", "--json")
    except RuntimeError as e:
        print(f"[!] vq programs --json failed: {e}", file=sys.stderr)
        return {}
    try:
        records = json.loads(out)
    except json.JSONDecodeError as e:
        print(f"[!] couldn't parse `vq programs --json` output: {e}", file=sys.stderr)
        return {}
    return {rec["name"]: rec for rec in records}


def poll_until_terminal(jobid: str, *, max_wait_seconds: float) -> tuple[str, float]:
    """Poll `vq status <jobid>` until terminal state. Returns (state, wall_seconds)."""
    TERMINAL = {
        "completed", "failed", "killed", "aborted_by_queue",
        "time_exceeded", "starved", "oom_killed",
    }
    t0 = time.monotonic()
    while time.monotonic() - t0 < max_wait_seconds:
        out = vq("status", jobid)
        state = None
        for line in out.splitlines():
            if line.startswith("state:"):
                # Tolerate "(archived)" suffix
                state = line.split(":", 1)[1].strip().split()[0]
                break
        if state in TERMINAL:
            return state, time.monotonic() - t0
        time.sleep(2.0)
    return "TIMEOUT", time.monotonic() - t0


def fetch_workspace(jobid: str, dest: Path) -> Path:
    """`vq fetch <jobid> -o <dest>`. Returns the per-job dir under dest."""
    dest.mkdir(parents=True, exist_ok=True)
    vq("fetch", jobid, "-o", str(dest))
    return dest / jobid


# ----------------------------------------------------------------------
# Engine definitions
# ----------------------------------------------------------------------


# Each engine's submit callable takes the workdir, its primary program
# record, and any separately registered companion program records from
# `vq programs --json`. Venv/import engines don't need the binary field;
# binary engines read record["binary"] for the absolute path.
SubmitFn = Callable[
    [Path, dict[str, object], dict[str, dict[str, object]]],
    list[str],
]


@dataclass
class Engine:
    """One thing-to-test: program registry name + how to submit a tiny
    job that exercises it + which output files must come back."""

    name: str
    """Display name in the report."""

    program: str
    """Name in [programs.X] for the availability gate AND for the
    absolute-path lookup. The smoke test reads `vq programs --json`
    once, then hands the matching record into ``submit()``."""

    kind_required: str | None
    """Expected ``kind`` value in the registry record. ``"binary"``
    engines require an executable; ``"venv"``/``"import"`` engines
    use the venv's python via ``--branch``. ``None`` skips the kind
    check (current code always sets this)."""

    submit: SubmitFn
    """Function ``(workdir, program_record, companions) -> list[str]``
    returning the ``vq`` args (after `vq`) that submits the test job.
    workdir is a laptop-side tmpdir the function should populate with
    input files; program_record is the JSON record for ``self.program``
    and companions holds records named by ``companion_programs``."""

    must_contain: list[str]
    """Substrings that must be in stdout.log after a successful run."""

    must_have_files: list[str]
    """Filenames that must exist in the fetched workspace."""

    companion_programs: dict[str, str] = field(default_factory=dict)
    """Additional registry records required by the smoke job, mapping
    program name to expected kind."""

    must_have_nonempty_files: list[str] = field(default_factory=list)
    """Fetched artifacts that must be regular, nonempty files."""


def _engine_vibeqc_dev() -> Engine:
    """tiny H2 RHF using vibeqc-dev (--branch main)."""

    def submit(
        workdir: Path,
        _record: dict[str, object],
        _companions: dict[str, dict[str, object]],
    ) -> list[str]:
        script = workdir / "h2_rhf.py"
        script.write_text(textwrap.dedent("""
            # Tiny vibe-qc smoke test: H2 at the equilibrium geometry.
            # Imports the library and prints a marker line; no SCF needed
            # to prove the venv is sane. v0.5.18 smoke is "does --branch
            # main reach a working vibe-qc dev install at all."
            import vibeqc
            print("vibeqc-dev smoke OK; version:", vibeqc.__version__)
        """).strip())
        return [
            "submit", str(script),
            "--cpus", "1",
            "--wall-time-seconds", str(DEFAULT_WALL_TIME),
            "--branch", "main",
        ]

    return Engine(
        name="vibeqc-dev",
        program="vibeqc-dev",
        kind_required="venv",
        submit=submit,
        must_contain=["vibeqc-dev smoke OK"],
        must_have_files=["stdout.log", "h2_rhf.py"],
    )


def _engine_vibeqc_release() -> Engine:
    def submit(
        workdir: Path,
        _record: dict[str, object],
        _companions: dict[str, dict[str, object]],
    ) -> list[str]:
        script = workdir / "h2_rhf_release.py"
        script.write_text(textwrap.dedent("""
            import vibeqc
            print("vibeqc-release smoke OK; version:", vibeqc.__version__)
        """).strip())
        return [
            "submit", str(script),
            "--cpus", "1",
            "--wall-time-seconds", str(DEFAULT_WALL_TIME),
            "--branch", "release",
        ]

    return Engine(
        name="vibeqc-release",
        program="vibeqc-release",
        kind_required="venv",
        submit=submit,
        must_contain=["vibeqc-release smoke OK"],
        must_have_files=["stdout.log", "h2_rhf_release.py"],
    )


def _engine_orca() -> Engine:
    """ORCA H2 RHF plus Molden conversion using two registry records."""

    def submit(
        workdir: Path,
        record: dict[str, object],
        companions: dict[str, dict[str, object]],
    ) -> list[str]:
        inp = workdir / "h2.inp"
        inp.write_text(textwrap.dedent("""
            ! HF STO-3G NoSym

            * xyz 0 1
            H 0.0 0.0 0.0
            H 0.0 0.0 0.74
            *
        """).strip())
        orca_bin = str(record["binary"])
        orca_2mkl_bin = str(companions["orca_2mkl"]["binary"])
        runner = workdir / "run_orca_smoke.sh"
        runner.write_text(
            "\n".join(
                [
                    "#!/usr/bin/env bash",
                    "set -euo pipefail",
                    f"{shlex.quote(orca_bin)} h2.inp",
                    f"{shlex.quote(orca_2mkl_bin)} h2 -molden",
                    "test -s h2.molden.input",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        return [
            "submit", "-d", str(workdir),
            "--cpus", "1",
            "--wall-time-seconds", str(DEFAULT_WALL_TIME),
            "--",
            "bash", runner.name,
        ]

    return Engine(
        name="orca",
        program="orca",
        kind_required="binary",
        submit=submit,
        # ORCA writes its banner + final SCF energy to stdout
        must_contain=["FINAL SINGLE POINT ENERGY"],
        # Canonical ORCA artifacts: .out (verbose log via stdout),
        # .gbw (binary basis + orbitals for restart / visualisation),
        # and converter-produced Molden orbitals.
        must_have_files=["stdout.log", "h2.gbw", "h2.molden.input"],
        companion_programs={"orca_2mkl": "binary"},
        must_have_nonempty_files=["h2.molden.input"],
    )


def _crystal_d12() -> str:
    """MgO rocksalt PBE/pob-TZVP — shared between serial and parallel."""
    return textwrap.dedent("""
        MgO rocksalt smoke test
        CRYSTAL
        0 0 0
        225
        4.21
        2
        12 0.0 0.0 0.0
         8 0.5 0.5 0.5
        BASISSET
        POB-TZVP
        DFT
        PBE
        END
        SHRINK
        8 8
        TOLDEE
        8
        END
    """).strip()


def _wrapper_path() -> str:
    """Absolute path to run-crystal.sh on the queue host. The wrapper
    isn't itself in the programs registry; the placeholder default
    below matches the canonical daemon-host clone layout. Override
    with the ``VQ_RUN_CRYSTAL_WRAPPER`` env var when running this
    smoke test against a host with a different install path."""
    return os.environ.get(
        "VQ_RUN_CRYSTAL_WRAPPER",
        "/home/USER/gitlab/vibeqc-queue/vibe-queue/contrib/run-crystal.sh",
    )


def _engine_crystal_serial() -> Engine:
    """CRYSTAL14 serial RKS/PBE on MgO conventional cubic, pob-TZVP.

    Passes CRYSTAL_BIN= via env so run-crystal.sh uses the absolute
    binary path from the registry instead of PATH-lookup."""

    def submit(
        workdir: Path,
        record: dict[str, object],
        _companions: dict[str, dict[str, object]],
    ) -> list[str]:
        (workdir / "mgo.d12").write_text(_crystal_d12())
        crystal_bin = str(record["binary"])
        return [
            "submit", "-d", str(workdir),
            "--cpus", "1",
            "--wall-time-seconds", str(DEFAULT_WALL_TIME),
            "--",
            # env(1) inherits the rest of the environment; we only need
            # to add CRYSTAL_BIN, not rebuild the whole environment.
            "env", f"CRYSTAL_BIN={crystal_bin}",
            "bash", _wrapper_path(),
            "--serial", "mgo.d12",
        ]

    return Engine(
        name="crystal-serial",
        program="crystal",
        kind_required="binary",
        submit=submit,
        must_contain=["SCF ENDED - CONVERGENCE ON ENERGY"],
        # fort.9 = final wave function; mgo.out = log; fort.98 = checkpoint.
        # fort.34 (final geometry) is only written when OPTGEOM runs — a
        # plain SCF input like this one doesn't produce it. The "SCF
        # ENDED" text + fort.9 are the canonical "SCF completed" pair.
        must_have_files=["mgo.out", "fort.9"],
    )


def _engine_crystal_parallel() -> Engine:
    """CRYSTAL14 parallel Pcrystal — same input, --np 4.

    Passes PCRYSTAL_BIN= via env; mpirun is still PATH-resolved (system
    /usr/bin/mpirun on host_d, fine)."""

    def submit(
        workdir: Path,
        record: dict[str, object],
        _companions: dict[str, dict[str, object]],
    ) -> list[str]:
        # Reuse the same .d12 the serial test uses (we don't share state
        # between the two; each engine gets its own workdir tmp).
        (workdir / "mgo.d12").write_text(_crystal_d12())
        pcrystal_bin = str(record["binary"])
        return [
            "submit", "-d", str(workdir),
            "--cpus", "4",
            "--wall-time-seconds", str(DEFAULT_WALL_TIME),
            "--",
            "env", f"PCRYSTAL_BIN={pcrystal_bin}",
            "bash", _wrapper_path(),
            "--np", "4", "mgo.d12",
        ]

    return Engine(
        name="crystal-parallel",
        program="Pcrystal",
        kind_required="binary",
        submit=submit,
        must_contain=["SCF ENDED - CONVERGENCE ON ENERGY"],
        # Same artifact set as crystal-serial: fort.9 + mgo.out are the
        # canonical SCF-completion pair. fort.34 needs OPTGEOM.
        must_have_files=["mgo.out", "fort.9"],
    )


def _engine_pyscf() -> Engine:
    """PySCF via vibeqc-dev venv (PySCF 2.13 lives there)."""

    def submit(
        workdir: Path,
        _record: dict[str, object],
        _companions: dict[str, dict[str, object]],
    ) -> list[str]:
        script = workdir / "h2_pyscf.py"
        script.write_text(textwrap.dedent("""
            # PySCF smoke test: H2 RHF/STO-3G. Tiny, runs in ~1 s.
            from pyscf import gto, scf
            mol = gto.M(atom='H 0 0 0; H 0 0 0.74', basis='sto-3g')
            mf = scf.RHF(mol)
            energy = mf.kernel()
            print(f"PYSCF SMOKE OK: H2 RHF/STO-3G energy = {energy:.6f} Ha")
        """).strip())
        return [
            "submit", str(script),
            "--cpus", "1",
            "--wall-time-seconds", str(DEFAULT_WALL_TIME),
            # Use --branch main so we hit vibeqc-dev's venv (where PySCF lives)
            "--branch", "main",
        ]

    return Engine(
        name="pyscf",
        program="pyscf",
        kind_required="import",
        submit=submit,
        must_contain=["PYSCF SMOKE OK"],
        must_have_files=["stdout.log", "h2_pyscf.py"],
    )


def _engine_psi4() -> Engine:
    """Psi4 H2 single-point RHF. Uses absolute binary path from
    `vq programs --json` so the daemon's PATH doesn't matter."""

    def submit(
        workdir: Path,
        record: dict[str, object],
        _companions: dict[str, dict[str, object]],
    ) -> list[str]:
        inp = workdir / "h2.in"
        inp.write_text(textwrap.dedent("""
            molecule h2 {
                H 0.0 0.0 0.0
                H 0.0 0.0 0.74
            }
            set basis sto-3g
            energy('scf')
        """).strip())
        psi4_bin = str(record["binary"])
        return [
            "submit", "-d", str(workdir),
            "--cpus", "1",
            "--wall-time-seconds", str(DEFAULT_WALL_TIME),
            "--",
            psi4_bin, "h2.in",
        ]

    return Engine(
        name="psi4",
        program="psi4",
        kind_required="binary",
        submit=submit,
        # Psi4 writes its banner + energies to <basename>.out.
        # The canonical line for a converged SCF in modern Psi4 (1.5+):
        #   Total Energy =     -1.11678331788...
        # plus an "@DF-RHF Final Energy:" or "@RHF Final Energy:" marker
        # when DF-RHF / RHF respectively. We match "Total Energy" — it's
        # universal across Psi4 versions and present in every SCF .out.
        must_contain=["Total Energy"],
        must_have_files=["h2.out"],
    )


ALL_ENGINES = [
    _engine_vibeqc_dev,
    _engine_vibeqc_release,
    _engine_orca,
    _engine_crystal_serial,
    _engine_crystal_parallel,
    _engine_pyscf,
    _engine_psi4,
]


# ----------------------------------------------------------------------
# Driver
# ----------------------------------------------------------------------


def _program_skip_reason(
    program: str,
    kind_required: str | None,
    programs: dict[str, dict[str, object]],
) -> str | None:
    """Return why a registry record cannot be used, or None when healthy."""
    record = programs.get(program)
    if record is None:
        return f"program {program!r} not registered in `vq programs`"
    if record.get("status") != "OK":
        return (
            f"program {program!r} status={record.get('status')!r}: "
            f"{record.get('reason')!r}"
        )
    if kind_required and record.get("kind") != kind_required:
        return (
            f"program {program!r} kind={record.get('kind')!r}, "
            f"expected {kind_required!r}"
        )
    if kind_required == "binary" and not record.get("binary"):
        return (
            f"program {program!r} record has no `binary` field "
            f"(kind={record.get('kind')!r})"
        )
    return None


def run_engine(
    engine: Engine, *, fetch_dir: Path, programs: dict[str, dict[str, object]],
) -> EngineResult:
    """Submit + wait + fetch + check. Returns EngineResult."""
    # Availability gate: program must be registered AND reported OK.
    reason = _program_skip_reason(engine.program, engine.kind_required, programs)
    if reason is not None:
        return EngineResult(engine.name, status="SKIP", reason=reason)
    record = programs[engine.program]

    companion_records: dict[str, dict[str, object]] = {}
    for program, kind_required in engine.companion_programs.items():
        reason = _program_skip_reason(program, kind_required, programs)
        if reason is not None:
            return EngineResult(
                engine.name,
                status="SKIP",
                reason=f"required companion {reason}",
            )
        companion_records[program] = programs[program]

    # Submit.
    with tempfile.TemporaryDirectory(prefix=f"vq-smoke-{engine.name}-") as tmp:
        workdir = Path(tmp)
        try:
            args = engine.submit(workdir, record, companion_records)
            jobid = vq(*args).strip()
        except Exception as e:
            return EngineResult(engine.name, "FAIL", f"submit failed: {e}")

        # Wait.
        state, wall = poll_until_terminal(jobid, max_wait_seconds=DEFAULT_WALL_TIME + 60)
        if state != "completed":
            return EngineResult(
                engine.name, "FAIL",
                f"state={state} (expected completed) after {wall:.1f}s",
                jobid=jobid, wall_seconds=wall,
            )

        # Fetch.
        try:
            ws_dir = fetch_workspace(jobid, fetch_dir)
        except Exception as e:
            return EngineResult(
                engine.name, "FAIL", f"fetch failed: {e}",
                jobid=jobid, wall_seconds=wall,
            )

    # Check artifacts.
    found: list[str] = []
    missing: list[str] = []
    for fname in engine.must_have_files:
        if (ws_dir / fname).exists():
            found.append(fname)
        else:
            missing.append(fname)
    empty = [
        fname
        for fname in engine.must_have_nonempty_files
        if (
            (artifact := ws_dir / fname).exists()
            and (not artifact.is_file() or artifact.stat().st_size == 0)
        )
    ]

    # Check stdout content.
    stdout_log = ws_dir / "stdout.log"
    content_ok = True
    content_reason = ""
    if engine.must_contain:
        text_to_check = ""
        if stdout_log.exists():
            text_to_check += stdout_log.read_text(errors="replace")
        # Some engines write to engine-specific files (e.g. h2.out for psi4,
        # mgo.out for crystal). Check all the must_have_files for content too.
        for fname in engine.must_have_files:
            fp = ws_dir / fname
            if fp.exists() and fp.is_file():
                # errors="replace" should never raise UnicodeDecodeError,
                # but binary files (fort.9, .gbw) can still hit OSError
                # on weird file types — be defensive without blocking
                # the rest of the loop.
                with contextlib.suppress(UnicodeDecodeError):
                    text_to_check += fp.read_text(errors="replace")
        for needle in engine.must_contain:
            if needle not in text_to_check:
                content_ok = False
                content_reason = f"missing expected text: {needle!r}"
                break

    if missing:
        return EngineResult(
            engine.name, "FAIL",
            f"missing artifacts: {missing}",
            jobid=jobid, wall_seconds=wall,
            artifacts_found=found, artifacts_missing=missing,
        )
    if empty:
        return EngineResult(
            engine.name, "FAIL",
            f"empty artifacts: {empty}",
            jobid=jobid, wall_seconds=wall,
            artifacts_found=found,
        )
    if not content_ok:
        return EngineResult(
            engine.name, "FAIL", content_reason,
            jobid=jobid, wall_seconds=wall,
            artifacts_found=found,
        )
    return EngineResult(
        engine.name, "PASS",
        reason=f"all good ({wall:.1f}s)",
        jobid=jobid, wall_seconds=wall,
        artifacts_found=found,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--only", default=None,
        help="Comma-separated list of engine names to run (default: all).",
    )
    parser.add_argument(
        "--keep", action="store_true",
        help="Don't auto-cleanup fetched workspaces.",
    )
    parser.add_argument(
        "--fetch-dir", default=None,
        help="Local dir for fetched workspaces (default: /tmp/vq-smoke-fetched).",
    )
    args = parser.parse_args(argv)

    fetch_dir = Path(args.fetch_dir or "/tmp/vq-smoke-fetched")
    if fetch_dir.exists() and not args.keep:
        shutil.rmtree(fetch_dir)
    fetch_dir.mkdir(parents=True, exist_ok=True)

    engines = [factory() for factory in ALL_ENGINES]
    if args.only:
        wanted = {n.strip() for n in args.only.split(",")}
        engines = [e for e in engines if e.name in wanted]
        if not engines:
            print(f"--only matched no engines (asked for {wanted})", file=sys.stderr)
            return 2

    print(f"== vq integration smoke test ({len(engines)} engines) ==")
    print(f"   fetch_dir = {fetch_dir}")
    print()

    try:
        programs = get_registered_programs()
    except Exception as e:
        print(f"[!] couldn't load `vq programs --json`: {e}", file=sys.stderr)
        programs = {}
    if not programs:
        print("[!] no programs registered. Engines that require a program "
              "will SKIP; the test only verifies the queue path.\n")
    else:
        bin_engines = [e.name for e in engines if e.kind_required == "binary"]
        if bin_engines:
            print("   binary engines will receive absolute paths from "
                  "`vq programs --json`:")
            for e in engines:
                if e.kind_required == "binary":
                    rec = programs.get(e.program)
                    abs_bin = rec.get("binary") if rec else "(not registered)"
                    print(f"     {e.name:20} {e.program:12} -> {abs_bin}")
                    for companion in e.companion_programs:
                        rec = programs.get(companion)
                        abs_bin = rec.get("binary") if rec else "(not registered)"
                        print(f"     {'':20} {companion:12} -> {abs_bin}")
            print()

    results: list[EngineResult] = []
    for engine in engines:
        print(f"-- {engine.name} ...", flush=True)
        result = run_engine(engine, fetch_dir=fetch_dir, programs=programs)
        results.append(result)
        marker = {"PASS": "OK", "FAIL": "FAIL", "SKIP": "skip"}[result.status]
        print(f"   {marker:5} {result.reason}")
        if result.jobid:
            print(f"   jobid={result.jobid}")
        if result.artifacts_found:
            print(f"   artifacts: {result.artifacts_found}")

    print()
    print("== summary ==")
    passed = sum(1 for r in results if r.status == "PASS")
    failed = sum(1 for r in results if r.status == "FAIL")
    skipped = sum(1 for r in results if r.status == "SKIP")
    print(f"   pass: {passed}  fail: {failed}  skip: {skipped}")

    # Leave the fetched workspaces around if --keep was passed or
    # anything failed (forensics); otherwise clean up.
    if not args.keep and failed == 0:
        shutil.rmtree(fetch_dir, ignore_errors=True)

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
