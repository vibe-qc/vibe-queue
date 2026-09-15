#!/usr/bin/env bash
#
# vq helper: run a CRYSTAL14 SCF or PROPERTIES post-processing
# calculation. Hides two CRYSTAL conventions that vq's `--`-form argv
# pass-through can't express directly:
#
#   1. SERIAL crystal / properties read input from stdin and write to
#      stdout. (`crystal < input.d12 > output.out`.) vq's argv
#      pass-through has no shell, so `<` and `>` aren't interpreted.
#
#   2. PARALLEL Pcrystal / Pproperties do NOT reliably accept stdin
#      redirection through mpirun -- not all ranks see the same fd, and
#      behaviour is OpenMPI-version dependent. Instead, they read from
#      a file literally named `INPUT` in the current working directory.
#      The wrapper copies the user's input to `INPUT`, runs the binary,
#      and restores the workspace on exit.
#
# Six binaries:
#
#   serial CRYSTAL14:    crystal           (~/bin/crystal)
#   parallel CRYSTAL14:  Pcrystal          (~/bin/Pcrystal, OpenMPI build)
#   serial PROPERTIES14: properties        (~/bin/properties)
#   parallel PROPERTIES14: Pproperties     (~/bin/Pproperties)
#   serial CRYSTAL23 demo:    crystal23demo    (~/bin/crystal23demo, v0.6.3)
#   serial PROPERTIES23 demo: properties23demo (~/bin/properties23demo, v0.6.3)
#
# CRYSTAL23 demo is feature-complete but capped at 10 atoms per primitive
# cell. No parallel demo binary ships — `--demo` is serial-only; combining
# with --np is rejected. Use the full CRYSTAL14 path (no --demo) for
# anything past the atom-count limit or when you want MPI.
#
# Select --np N (any positive integer) for parallel execution, or --serial
# for the single-core binary with no MPI startup cost. There is no site allocation default.
# --demo implies --serial.
#
# Usage:
#   bash run-crystal.sh [options] <input.d12 | propinput.d3> [output.out]
#
#   --serial            use the single-core binary (no mpirun)
#   --np N              explicit MPI rank allocation (required in parallel)
#   --properties        use PROPERTIES post-processing (instead of SCF)
#   --demo              v0.6.3: use the CRYSTAL23 demo binary
#                       (crystal23demo / properties23demo). Serial-only
#                       — combining with --np is an error. 10-atom
#                       primitive-cell limit; otherwise full v23 feature
#                       set. Composes with --properties for the
#                       properties23demo path.
#   --keep-scratch      do NOT delete per-rank fort.*.peN scratch files
#                       on successful exit (default: delete; the canonical
#                       outputs fort.9 / fort.98 / fort.34 / fort.87 /
#                       dffit3.dat / <out>.out are always preserved).
#                       Useful for debugging parallel runs. Failed runs
#                       (non-zero exit) always keep scratch regardless.
#   --help              this message
#
# The optional output path must not be the input path. Extra positional
# arguments are rejected instead of being silently ignored.
#
# Env-var binary overrides (v0.5.19, v0.6.3; unset = use PATH as before):
#   CRYSTAL_BIN          absolute path to serial CRYSTAL14 (crystal)
#   PCRYSTAL_BIN         absolute path to parallel CRYSTAL14 (Pcrystal)
#   PROPERTIES_BIN       absolute path to serial PROPERTIES14 (properties)
#   PPROPERTIES_BIN      absolute path to parallel PROPERTIES14 (Pproperties)
#   CRYSTAL23DEMO_BIN    absolute path to serial CRYSTAL23 demo
#                        (crystal23demo)              v0.6.3
#   PROPERTIES23DEMO_BIN absolute path to serial PROPERTIES23 demo
#                        (properties23demo)           v0.6.3
# Lets callers point at a specific binary without touching the daemon's
# PATH; used by tests/integration_smoke.py which reads these out of
# `vq programs --json`. mpirun is always resolved via PATH.
#
# Choose ranks to match the allocation requested from vq or your scheduler.
# The wrapper cannot infer the machine's available CPU or memory budget.
#
# OpenMPI 5 specifics: the standard transport (vader/sm shared memory)
# is automatic on a single node; we don't pass --mca anything. Do NOT
# run as root (OpenMPI 5 refuses unless --allow-run-as-root is set;
# vq dispatches as the daemon's user, so this is a non-issue).
#
# Submit pattern (with vq):
#   vq submit -d ./mycalc --cpus 14 -- bash \
#     /path/to/vibe-queue/contrib/run-crystal.sh \
#     --np 14 myinput.d12

set -euo pipefail

MODE=parallel
NP=1
NP_EXPLICITLY_SET=0
USE_PROPERTIES=0
USE_DEMO=0
KEEP_SCRATCH=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --serial)
            MODE=serial
            shift
            ;;
        --np)
            NP="$2"
            NP_EXPLICITLY_SET=1
            shift 2
            ;;
        --properties)
            USE_PROPERTIES=1
            shift
            ;;
        --demo)
            USE_DEMO=1
            shift
            ;;
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

# v0.6.3: --demo implies serial (no parallel demo binary ships). Combining
# --demo with an explicit --np is a user-error; reject loudly rather than
# silently dropping the --np value.
if [[ "$USE_DEMO" -eq 1 ]]; then
    if [[ "$NP_EXPLICITLY_SET" -eq 1 ]]; then
        echo "$0: --demo is serial-only — combining with --np N is rejected." \
             "Drop one of the two flags. The CRYSTAL23 demo binary has no" \
             "Pcrystal23demo counterpart." >&2
        exit 2
    fi
    MODE=serial
fi

if [[ "$MODE" == "parallel" && "$NP_EXPLICITLY_SET" -eq 0 ]]; then
    echo "$0: parallel execution requires --np N; use --serial for one core." >&2
    exit 2
fi

INPUT="${1:-}"
if [[ -z "$INPUT" ]]; then
    echo "usage: $0 [--serial] [--np N] [--properties] <input> [output]" >&2
    exit 2
fi
if [[ ! -f "$INPUT" ]]; then
    echo "$0: input file not found: $INPUT" >&2
    exit 2
fi
if [[ "$#" -gt 2 ]]; then
    echo "$0: too many positional arguments: ${*:3}" >&2
    echo "usage: $0 [--serial] [--np N] [--properties] <input> [output]" >&2
    exit 2
fi
OUTPUT="${2:-${INPUT%.*}.out}"

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

if [[ "$NP" =~ ^[0-9]+$ ]] && [[ "$NP" -ge 1 ]]; then
    :
else
    echo "$0: --np must be a positive integer (got: $NP)" >&2
    exit 2
fi

# Pick binary + invocation.
#
# Env-var overrides (v0.5.19) let callers point at an absolute binary
# without relying on the daemon's PATH:
#
#   CRYSTAL_BIN     -> override crystal      (serial CRYSTAL14)
#   PCRYSTAL_BIN    -> override Pcrystal     (parallel CRYSTAL14)
#   PROPERTIES_BIN  -> override properties   (serial PROPERTIES14)
#   PPROPERTIES_BIN -> override Pproperties  (parallel PROPERTIES14)
#
# Empty / unset falls back to `command -v <name>` (PATH lookup), the
# pre-v0.5.19 behaviour. tests/integration_smoke.py sets these from
# `vq programs --json` so smoke tests are PATH-independent; daily use
# without env vars is unchanged.
if [[ "$USE_PROPERTIES" -eq 1 ]]; then
    if [[ "$USE_DEMO" -eq 1 ]]; then
        # v0.6.3: PROPERTIES23 demo (serial-only; MODE was forced to
        # serial above when --demo was set).
        BIN="${PROPERTIES23DEMO_BIN:-$(command -v properties23demo || true)}"
        LABEL="PROPERTIES23 demo (serial; 10-atom cell limit)"
        WRAP=()
    elif [[ "$MODE" == "serial" ]]; then
        BIN="${PROPERTIES_BIN:-$(command -v properties || true)}"
        LABEL="PROPERTIES (serial)"
        WRAP=()
    else
        BIN="${PPROPERTIES_BIN:-$(command -v Pproperties || true)}"
        LABEL="PROPERTIES (parallel, np=$NP)"
        WRAP=(mpirun -np "$NP")
    fi
else
    if [[ "$USE_DEMO" -eq 1 ]]; then
        # v0.6.3: CRYSTAL23 demo (serial-only; MODE was forced to
        # serial above when --demo was set). Full v23 feature set
        # capped at 10 atoms per primitive cell.
        BIN="${CRYSTAL23DEMO_BIN:-$(command -v crystal23demo || true)}"
        LABEL="CRYSTAL23 demo (serial; 10-atom cell limit)"
        WRAP=()
    elif [[ "$MODE" == "serial" ]]; then
        BIN="${CRYSTAL_BIN:-$(command -v crystal || true)}"
        LABEL="CRYSTAL (serial)"
        WRAP=()
    else
        BIN="${PCRYSTAL_BIN:-$(command -v Pcrystal || true)}"
        LABEL="CRYSTAL (parallel, np=$NP)"
        WRAP=(mpirun -np "$NP")
    fi
fi

if [[ -z "$BIN" ]]; then
    echo "$0: $LABEL binary not found on PATH" >&2
    echo "PATH=$PATH" >&2
    exit 127
fi
if [[ "$MODE" == "parallel" ]] && ! command -v mpirun >/dev/null; then
    echo "$0: --parallel requested but mpirun not on PATH" >&2
    exit 127
fi

# CRYSTAL writes scratch (fort.9, fort.25, fort.98, ...) to the current
# directory; the daemon dispatches with cwd = per-job workspace, so
# every artefact ends up alongside the user's input + output. `vq fetch`
# brings them all back. No CRY_SCRDIR setup needed for v14 single-node.

echo "$LABEL: $BIN" >&2
echo "input:  $INPUT" >&2
echo "output: $OUTPUT" >&2
echo "cwd:    $(pwd)" >&2
echo >&2

if [[ "$MODE" == "serial" ]]; then
    # Serial CRYSTAL14 / PROPERTIES14: stdin from file, stdout+stderr
    # to file. exec replaces the shell so signals from the daemon's
    # killpg reach the binary directly.
    exec "$BIN" < "$INPUT" > "$OUTPUT" 2>&1
fi

# --- Parallel path -----------------------------------------------------
#
# Pcrystal / Pproperties read from a file literally named "INPUT" in
# cwd. mpirun's stdin is NOT a reliable channel (rank-distribution
# semantics differ across OpenMPI versions). We:
#   1. Stage the user's input to ./INPUT (defensive backup of any
#      pre-existing INPUT file)
#   2. Run mpirun with stdout going to OUTPUT (mpirun's stderr falls
#      through to vq's stderr.log -- meaningful to keep separate from
#      the CRYSTAL .out so the .out parses cleanly)
#   3. On exit (success, error, or signal), restore the workspace:
#      remove the staged INPUT and put back any backup
#
# We do NOT exec mpirun here -- bash needs to stay alive to fire the
# EXIT trap. The daemon's killpg reaches mpirun directly anyway, so
# the wrapper-as-parent is a free wrapper, not a signal-forwarding
# bottleneck.

BACKUP=""
STAGED_INPUT_ABS="$(normalise_existing_parent "INPUT")"
if [[ "$INPUT_ABS" == "$STAGED_INPUT_ABS" ]]; then
    # The user's input is already CRYSTAL's required ./INPUT file. Do not move
    # or delete it in the staging cleanup trap.
    :
elif [[ -e INPUT ]]; then
    BACKUP="INPUT.run-crystal.bak.$$"
    mv INPUT "$BACKUP"
    cp "$INPUT" INPUT
    # shellcheck disable=SC2064
    trap "rm -f INPUT; [[ -n '$BACKUP' && -e '$BACKUP' ]] && mv '$BACKUP' INPUT; true" EXIT
else
    cp "$INPUT" INPUT
    trap "rm -f INPUT; true" EXIT
fi

# Capture rc instead of exec'ing -- we still need to clean up scratch
# files after the binary exits. set -e is already on; turn it off
# briefly so a non-zero CRYSTAL exit doesn't bypass our cleanup logic.
#
# Pcrystal writes its human-readable output to stderr (NOT stdout) under
# OpenMPI -- the parallel binary uses Fortran unit 0 which mpirun routes
# to stderr per-rank, while serial CRYSTAL writes to unit 6 = stdout.
# Empirically confirmed with OpenMPI 5: with `> $OUTPUT` only,
# mgo.out lands empty and the SCF log is in vq's stderr.log. So we
# merge stderr into the output file. mpirun's transport-level chatter
# (process N of M working) ends up in the .out too, but it's a few
# lines at the top — acceptable cost for capturing the actual SCF.
set +e
"${WRAP[@]}" "$BIN" > "$OUTPUT" 2>&1
rc=$?
set -e

# Successful parallel CRYSTAL / PROPERTIES leaves behind ~14*12 = ~170+
# per-rank scratch files (fort.<N>.pe<RANK>) that are working memory
# only -- useless after the binary exits. A real CRYSTAL job can leave
# multi-GB of these. Default behaviour: delete on rc=0; keep on
# rc!=0 (debugger may want them) or when the user passed
# --keep-scratch.
#
# Files we deliberately keep (non-.pe*, canonical outputs):
#   fort.9        -- final wave function (binary; SCF restart from here)
#   fort.98       -- properties checkpoint (similar role)
#   fort.34       -- final geometry (CRYSTAL fortran format)
#   fort.87       -- bands / DOS output
#   fort.20       -- input wave function (if the user provided one via GUESSP)
#   dffit3.dat    -- DF fit data (regenerable but useful to keep)
#   <name>.out    -- the human-readable output ($OUTPUT, never matches the glob)
if [[ "$rc" -eq 0 && "$KEEP_SCRATCH" -eq 0 && "$MODE" == "parallel" ]]; then
    nuked=$(find . -maxdepth 1 -name 'fort.*.pe[0-9]*' -print -delete 2>/dev/null | wc -l)
    if [[ "$nuked" -gt 0 ]]; then
        echo "cleanup: removed $nuked per-rank scratch files (fort.*.pe<N>)" >&2
        echo "         pass --keep-scratch to preserve them on next run" >&2
    fi
elif [[ "$rc" -ne 0 && "$MODE" == "parallel" ]]; then
    # Hint the user that the scratch is still there and what it costs.
    n=$(find . -maxdepth 1 -name 'fort.*.pe[0-9]*' 2>/dev/null | wc -l)
    if [[ "$n" -gt 0 ]]; then
        echo "note: $n per-rank scratch files preserved (job exited rc=$rc); " >&2
        echo "      delete with: find . -maxdepth 1 -name 'fort.*.pe[0-9]*' -delete" >&2
    fi
fi

exit "$rc"
