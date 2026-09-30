# Tests, builds and fast feedback

**Audience: contributors.** Listed under Reference on the [front page](index.md).

Routine MRs run a fixed representative sample, including privacy. Full
validation is reserved for release candidates or explicit full runs. This is a
throughput policy: some regressions will be found later, and that is an accepted
tradeoff rather than a claim that the sample covers everything.

## Inventory

The pre-change snapshot at `1a94da37372d` (2026-09-26) contains **312 test files and
7,783 test definitions**. Local collection expanded those to **10,526 cases**.
Parametrized tests account for much of the difference. The subsequent CI-scope
changes add a few policy cases. Collection alone took about four minutes on the
loaded local machine, so collecting the entire suite is not a free preflight.

The fixed smoke sample now runs **232 cases across five files**, down from 427.
A local run passed in 15.69 seconds (previous local sample: 46.16 seconds). The
previous MR test job took 81.81 seconds including setup. These observations are
not a controlled equivalent-host speedup benchmark.

| Kind | What is actually exercised |
|---|---|
| Unit/contract | JobSpec validation/serialization, state transitions, resource limits, token checks, scheduler parsers, configuration and CLI contracts |
| Local process/filesystem integration | Real temporary files, processes, sockets, lock contention, git repositories, virtual environments, lifecycle scripts and simulated failures |
| Daemon/scheduler/web integration | Daemon ticks, mocked scheduler/SSH replies, local fake helpers, ASGI clients, fleet plans and recovery protocols |
| Documentation/privacy | Documented command/schema/outcome contracts, whole-tree privacy assertions and staged-hook negative controls |
| Builds | Editable Python package install and Sphinx docs; some tests create small fixture venvs/repos or simulate update builds |

The queue is pure Python. The suite normally does not compile vibe-qc, submit
production calculations, run full chemistry, or prove a real fleet rollout.
Mocks and fake helpers test scheduling/admin behavior without contacting the
production fleet. Operational acceptance still needs actual runtime receipts.
The long suite is mostly many integration contracts, subprocesses and fixtures,
not scientific calculations.

## Pipeline policy

| Trigger | Runtime tests |
|---|---|
| Routine MR | Fixed smoke sample |
| Main push | No duplicate pytest job; Ruff and docs remain |
| Candidate | Full normal suite |
| Manual web or scheduled pipeline | Full normal suite |

The smoke sample covers JobSpec, authentication, local dispatch, dependency
cascade and privacy. Scheduler-dialect and selector tests join the larger
admin/daemon/web/fleet inventory in full validation. No Git history or changed
test files automatically expand the routine sample. Use focused local tests or
`--scope changed` explicitly; in an MR environment the latter adds changed test
files and uses documentation contracts for prose-only diffs.

Routine MRs install `[test]` without the `[web]` extra. Docs builds run on MRs
only when docs, contribution/changelog, packaging or CI inputs change; main and
candidate docs publication builds remain. Direct code pushes to main still
have no pytest gate. Delayed failures in omitted areas are an accepted tradeoff.

All pytest modes keep the same non-root sandbox, isolated state/config roots and
sentinel checks. Red tests still fail; the fast policy changes selection, not
assertions or failure handling. Superseded validation jobs are interruptible;
candidate auto-cancellation is disabled.

## Commands and maintenance

```sh
python scripts/run_ci_tests.py --scope fast
python scripts/run_ci_tests.py --scope fast --list-targets
python scripts/run_ci_tests.py --scope changed  # opt-in MR diff expansion
python scripts/run_ci_tests.py --scope full
python -m pytest tests/test_<area>.py -q -p no:cacheprovider
python -m pytest tests --collect-only -q -p no:cacheprovider
```

Use `[test]` for smoke and `[test,web]` for full/changed validation. Never run pytest as root. The explicit inventories
are `FAST_TESTS` and `PROSE_TESTS` in `scripts/run_ci_tests.py`. Keep document
contract assertions in the prose inventory. Full tests remain in the repository;
they were not bulk-deleted. GitLab publishes JUnit results and the 30 slowest
phases. A schedule must be configured separately; this patch creates no timer.
