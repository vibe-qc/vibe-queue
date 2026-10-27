# Tests and validation

Use the smallest check that answers the question. Stop when it passes unless
new code, a failure, or a specific unresolved concern requires another run.

## Default workflow

1. For a behavior change, run the relevant existing tests and, when useful,
   one regression demonstrating the bug. Do not add a matrix for every patch.
2. Development builds, ordinary MRs and pushes neither launch nor wait for CI.
   Manual, scheduled and API requests on development refs create no pipeline.
   Dev updates use the source revision and builder without a CI or release-report gate.
3. Every `release-candidate/*` pipeline runs Ruff, the full suite with web
   dependencies and the docs build. CI passes `--scope full` explicitly, so
   `VQ_TEST_SCOPE=fast` cannot weaken candidate qualification. Four isolated jobs
   partition whole test modules by source size, executing each file once. This
   avoids repeating collection or sharing mutable scheduler state across workers.
   `release` runs the docs build and optional manual publication, without repeating the suite.
   Local checks remain focused; pass `--scope full` for deliberate broad validation.

```sh
python scripts/run_ci_tests.py                 # smoke
python scripts/run_ci_tests.py --list-targets  # no pytest collection
python scripts/run_ci_tests.py --scope full    # deliberate broad run
# Focused source checks; clear inherited options and use this checkout:
env -u PYTEST_ADDOPTS PYTHONPATH="$PWD/src" python -m pytest tests/test_<area>.py -q -p no:cacheprovider
```

Install `[test]` for smoke or `[test,web]` for full validation. Never run as
root. All modes retain the isolated state/config sandbox and sentinel checks.
Failures remain failures; smaller selection deliberately defers some regression
coverage. The full suite remains available for release risk and investigations.

There are only two runner modes: `fast` and `full`. The old `auto` and `changed`
modes, Git-history classifier and separate prose-test inventory are removed.
Use explicit pytest paths for affected behavior. For prose-only changes, use
the relevant documentation check and privacy check, not runtime suites.

## Source checks and live acceptance

A source-only defect can be complete after its focused check passes and
the fix lands. Do not require a host experiment for behavior demonstrated
locally. Do not rerun unchanged tests to produce another handover or verdict.

A defect involving a real scheduler, service or deployment may still need live
acceptance. Record the missing observation, exact build, and responsible
operator once. Keep that acceptance pending without repeating source work or
blocking unrelated changes. Local mock tests cannot supply a live receipt.
Host operations still require task-specific authorization.

## Simplification audit, 2026-10-08

A static inventory found 341 test files, 8,233 test definitions and 234,836
lines before this change. These are source counts, not collected cases; no
full collection was needed. The largest file, `test_fleet_rollout.py`, had
21,967 lines. Pruning that suite safely requires identifying redundant behaviors,
not simply deleting the largest files.

The immediately removable complexity was the selector: four modes, CI-trigger
inference, Git subprocesses, a documentation inventory and temporary Git
repositories in its own tests. It is replaced with a fixed smoke list and
explicit full opt-in. Routine selection drops from 232 cases to 34. The local
34-case run passed in 11.45 seconds; this is an observation, not a controlled
speed comparison with earlier runs.

Follow-up priorities are consolidating overlapping rollout/admin integration
fixtures and reviewing private-loop acceptance rules. Those are separate from
this repository's test selection; no private-loop change is claimed here.

## Second simplification pass

The first pass reduced selection, not the size of the behavioral suite. This
pass removes the 665-line site-prose parser, its maintenance instructions and
eight wording-only tests from scheduler/admin/config modules. These checked
phrases in documentation rather than executing the behavior described. CLI
help behavior, runtime tests, privacy and the lightweight relative-link check
remain. Documentation token drift can now escape automation; reviewing the
changed examples is the accepted replacement.

Automatic CI on every MR and main push is also removed. A changelog edit no
longer launches another docs build for an otherwise completed source change.
That earlier policy allowed candidate/manual/scheduled smoke pipelines. The
current matrix above supersedes it: only release refs create CI, and candidate
qualification always runs in full.

The wider audit found a mismatch between product CI and the operational goal of
bounded local checks. It also found that source verification, deployment health
and live acceptance have different completion criteria. Repeatedly collecting
or rerunning source tests does not repair a missing host receipt. Keep one
record of the missing evidence and its owner, and reuse valid accepted evidence.
No private operational configuration or acceptance implementation is changed by
this product patch.

## Candidate runtime reduction

The candidate suite runs in four separate job sandboxes. Module source size is
an inexpensive balancing estimate, not measured runtime; slow process tests can
still make one shard longer. Use `--scope full --shard-count 4 --shard-index N
--list-targets` to inspect a partition (N is 1 through 4). Omitting shard options
retains the ordinary full-suite invocation. JUnit reports remain per job.

Three duplicate cases were removed: two empty-list assertions already covered
in `test_listing.py`, and an identical offsite-login redirect assertion retained
in `test_web_fleet_auth.py`. Unique expiry and redirect-recovery behavior remains.


### Avoid repeated setup in behavioral checks

Lock-routing tests use one history row per terminal state instead of 128 copies
of `COMPLETED` for each operation. They still assert that only the live row takes
a control lock and every terminal row is unchanged; this is not a throughput
benchmark. Source-stage retention checks cross `keep + 1` once, and the retained
two-deployment test checks successful cleanup after each operation. A separate
eight-deployment repetition was removed. Runtime identity mismatch tests keep
all nine mismatches, using plain fixture directories because their Git and tree
identity queries are already injected. Actual Git/update tests retain real repos.
