#!/usr/bin/env bash
#
# vq helper: run an ORCA single-point calculation. Companion to
# run-crystal.sh — hides the ORCA invocation conventions that vq's
# `--`-form argv pass-through can't express directly:
#
#   1. ORCA writes its human-readable output to STDOUT, not to a file.
#      You must redirect (`orca input.inp > input.out`). vq's argv
#      pass-through has no shell, so `>` isn't interpreted — the
#      wrapper does the redirect.
#
#   2. ORCA must be invoked by ABSOLUTE PATH. ORCA locates its sibling
#      executables (orca_scf, orca_gtoint, orca_mp2, the MPI launchers,
#      ...) relative to the resolved path of argv[0]. Invoked as a bare
#      `orca` that happens to be on PATH it usually works, but parallel
#      runs and some module setups break. The wrapper resolves the
#      absolute path before exec.
#
#   3. Parallel ORCA is driven by a `%pal nprocs N end` block INSIDE
#      the input file — ORCA spawns the MPI ranks itself. The wrapper
#      does NOT run mpirun (unlike run-crystal.sh, where the wrapper
#      owns mpirun). So this wrapper is parallelism-agnostic: it just
#      runs `orca <input> > <output>`. Whatever `%pal` the input
#      carries must agree with the `--cpus` you claimed on `vq submit`.
#      `mpirun` from ORCA's bundled OpenMPI must be on PATH for N > 1.
#
# Usage:
#   bash run-orca.sh [options] <input.inp> [output.out]
#
#   --keep-scratch   do NOT delete ORCA scratch on successful exit.
#                    Default: on rc=0, delete the bulky regenerable
#                    scratch (*.tmp*, *_atom*.{inp,out,gbw}, the
#                    per-rank temp dirs) and KEEP the canonical
#                    artefacts: <base>.out, <base>.gbw (orbitals —
#                    SCF restart / property runs read this),
#                    <base>.property.txt, <base>.engrad, <base>.xyz,
#                    <base>.densities, <base>.hess. A failed run
#                    (rc != 0) keeps everything regardless.
#   --help           this message
#
# The optional output path must not be the input path. Extra positional
# arguments are rejected instead of being silently ignored.
#
# Env-var binary override (mirrors run-crystal.sh's CRYSTAL_BIN etc.):
#   ORCA_BIN   absolute path to the orca binary. Unset = resolve via
#              PATH (`command -v orca`), the daemon prepends the ORCA
#              install dir to PATH per the per-host vq config.
#
# Submit pattern (with vq) — serial, 1 core, the parity-cell default:
#   vq submit -d ./mycell --cpus 1 --wall-time-seconds 1800 -- \
#     bash /path/to/vibe-queue/contrib/run-orca.sh cell.inp
#
# Parallel (input carries `%pal nprocs 4 end`; claim the matching cpus):
#   vq submit -d ./mycell --cpus 4 --wall-time-seconds 1800 -- \
#     bash /path/to/vibe-queue/contrib/run-orca.sh cell.inp
#
# ALWAYS pass --wall-time-seconds on vq submit: the v0.5.9 watchdog
# regression mis-kills bash-wrapped jobs as STARVED without it.

set -euo pipefail

KEEP_SCRATCH=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --keep-scratch)
            KEEP_SCRATCH=1
            shift
            ;;
        --help|-h)
            sed -n '2,/^$/p' "$0"
            exit 0
            ;;
        --)
            shift
            break
            ;;
        -*)
            echo "$0: unknown option: $1" >&2
            exit 2
            ;;
        *)
            break
            ;;
    esac
done

INPUT="${1:-}"
if [[ -z "$INPUT" ]]; then
    echo "usage: $0 [--keep-scratch] <input.inp> [output.out]" >&2
    exit 2
fi
if [[ ! -f "$INPUT" ]]; then
    echo "$0: input file not found: $INPUT" >&2
    exit 2
fi
if [[ "$#" -gt 2 ]]; then
    echo "$0: too many positional arguments: ${*:3}" >&2
    echo "usage: $0 [--keep-scratch] <input.inp> [output.out]" >&2
    exit 2
fi
OUTPUT="${2:-${INPUT%.*}.out}"
BASE="$(basename "${INPUT%.*}")"

normalise_existing_parent() {
    local path="$1"
    local dir
    local base
    dir="$(dirname "$path")"
    base="$(basename "$path")"
    if [[ -d "$dir" ]]; then
        (cd "$dir" && printf "%s/%s\n" "$(pwd -P)" "$base")
    else
        printf "%s\n" "$path"
    fi
}

INPUT_ABS="$(normalise_existing_parent "$INPUT")"
OUTPUT_ABS="$(normalise_existing_parent "$OUTPUT")"
if [[ "$INPUT_ABS" == "$OUTPUT_ABS" ]]; then
    echo "$0: output would overwrite input: $OUTPUT" >&2
    exit 2
fi

# Resolve the orca binary to an ABSOLUTE path (see header note 2).
BIN="${ORCA_BIN:-$(command -v orca || true)}"
if [[ -z "$BIN" ]]; then
    echo "$0: orca binary not found (set \$ORCA_BIN or put it on PATH)" >&2
    echo "PATH=$PATH" >&2
    exit 127
fi
# `command -v` already yields an absolute path when the daemon's PATH
# holds the install dir; normalise anything relative just in case.
case "$BIN" in
    /*) : ;;
    *)  BIN="$(cd "$(dirname "$BIN")" && pwd)/$(basename "$BIN")" ;;
esac
if [[ ! -x "$BIN" ]]; then
    echo "$0: resolved orca binary is not executable: $BIN" >&2
    exit 127
fi

# Put ORCA's install dir on PATH. The vq daemon dispatches jobs with a
# bare PATH (/usr/local/sbin:/usr/local/bin:/usr/bin) — it does NOT
# inject registered-program dirs. ORCA finds its sibling executables
# (orca_scf, orca_gtoint, ...) relative to the resolved argv[0] so a
# serial run is fine without this, but a parallel run (`%pal`) needs
# ORCA's bundled `mpirun` — which lives in the same dir — to be
# reachable. Prepending here covers both and is harmless for serial.
export PATH="$(dirname "$BIN"):$PATH"

echo "ORCA:   $BIN" >&2
echo "input:  $INPUT" >&2
echo "output: $OUTPUT" >&2
echo "cwd:    $(pwd)" >&2
echo >&2

# ORCA writes its main output to stdout and its scratch / artefact
# files (<base>.gbw, <base>.densities, *.tmp, ...) into cwd. The daemon
# dispatches with cwd = per-job workspace, so everything lands in the
# workspace and `vq fetch` brings it back. We do NOT exec — bash stays
# alive to run the scratch-cleanup logic after ORCA exits. The daemon's
# killpg reaches the orca process group directly, so the wrapper-as-
# parent is a free wrapper, not a signal-forwarding bottleneck.
#
# set -e is on; turn it off briefly so a non-zero ORCA exit doesn't
# bypass the cleanup logic below.
set +e
"$BIN" "$INPUT" > "$OUTPUT" 2>&1
rc=$?
set -e

# A successful ORCA run leaves regenerable scratch behind: *.tmp* work
# files, the per-fragment *_atomNN.{inp,out,gbw} from the SAD/atomic
# guess, and (parallel runs) per-rank temp dirs. On rc=0 + default
# behaviour, delete those and keep the canonical artefacts. On rc!=0
# keep everything — the debugger may want the scratch.
if [[ "$rc" -eq 0 && "$KEEP_SCRATCH" -eq 0 ]]; then
    nuked=$(find . -maxdepth 1 \( \
        -name "${BASE}.tmp*" -o \
        -name "${BASE}_atom*.inp" -o \
        -name "${BASE}_atom*.out" -o \
        -name "${BASE}_atom*.gbw" -o \
        -name "${BASE}.*tmp" -o \
        -name "${BASE}.bas*" -o \
        -name "${BASE}.cpcm*" \
        \) -print -delete 2>/dev/null | wc -l)
    nuked_dirs=$(find . -maxdepth 1 -type d -name "${BASE}.*proc*" \
        -exec rm -rf {} + -print 2>/dev/null | wc -l)
    total=$((nuked + nuked_dirs))
    if [[ "$total" -gt 0 ]]; then
        echo "cleanup: removed $total ORCA scratch files/dirs" >&2
        echo "         pass --keep-scratch to preserve them on next run" >&2
    fi
elif [[ "$rc" -ne 0 ]]; then
    echo "note: ORCA exited rc=$rc — scratch preserved for debugging" >&2
fi

exit "$rc"
