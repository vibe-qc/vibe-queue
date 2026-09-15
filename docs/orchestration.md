# Driving vq from a script

**Match on `outcome`. Never on message text.**

This page is the contract between vq and an unattended caller. It exists
because the 2026-09 fleet migration had no such contract, and every chain
written during it ended up with a line like:

```sh
grep -q "local checkout mutation lock" "$log" && { sleep 90; continue; }
```

That is load-bearing infrastructure spelled as a substring match on a
sentence.

That sentence was in fact pinned inside vq, by one assertion in
`tests/test_self_update.py`, so a reword would fail vq's own CI. The
protection ends at the repository boundary: an orchestration greping a log
gets no signal whatsoever and simply stops matching. The hazard is not that a
reword goes unnoticed — it is that vq is the only thing that notices.

That message was reworded in the same release that added this page,
deliberately, so the guarantee below is real rather than aspirational.

## What is stable

| | stable | may change without notice |
|---|---|---|
| classification | `outcome`, and the exit code it maps to | — |
| identity | the JSON field *names* below | field order, indentation |
| prose | — | every message, every `error` string, every summary line |

`vq admin update --json` emits `outcome` on success and on failure. On failure
the object is `{"outcome": ..., "error": ...}` and nothing else is promised.

## The outcomes

| outcome | exit | meaning | do |
|---|---|---|---|
| `ok` | 0 | did the work | continue |
| `already-current` | 0 | target already deployed, nothing ran | continue |
| `locked` | 75 | another operation holds a lock | retry later |
| `marker-present` | 76 | a previous operation needs acknowledging | acknowledge, then retry |
| `precondition-failed` | 77 | a safety gate refused | **stop.** Do not retry, do not force |
| `failed` | 1 | the operation itself failed | stop, read the log |

Two notes on the codes:

* **`ok` and `already-current` share 0.** Both mean continue, and every
  `set -e` wrapper treats non-zero as stop, so a benign no-op returning
  non-zero would break more callers than it informs. Read `outcome` when you
  need to tell them apart.
* **75 is `EX_TEMPFAIL`**, whose established meaning is exactly "temporary
  failure, retry later". 76 and 77 continue that block.
* **Exit 2 is not an outcome.** It is click's usage error — a wrong argv, an
  unknown env. The operation never started and no host state is implied.

`precondition-failed` is deliberately distinct from `failed`. "The build
failed" invites a retry after fixing the build. "This host is not converged,
so I will not supersede its hold" is a *correct* answer that a retry cannot
change and a `--force` would defeat. Both used to be exit 1 plus prose.

### A gate that could not gather its evidence reports `locked`, not `precondition-failed`

A refusal is only `precondition-failed` when the gate actually decided. When
its *probe* did not answer — a remote call that ran out of its budget — then
nothing is known, and that is a `locked` condition: retry.

This distinction is not decorative. On 2026-09-10 five of six supersede
attempts on pbs-cluster refused with "lacks strictly healthy exact-target evidence"
while the host was fine: the `scheduler_remote_vq` check makes three remote vq
calls inside one 10 s budget, pbs-cluster's login node needs 1.6–2.5 s each, and
`source-sha` intermittently ran out. The helper's live SHA went missing from
that sweep and the lane read as not converged. A caller obeying "stop, do not
retry" would have stopped on a flake; the operator's actual remedy was a
bounded retry, and it was right.

So: **a `precondition-failed` from a supersede gate is a verdict about the
host. A `locked` from one means the measurement is missing.** If it persists,
raise the budget rather than retrying forever:

```toml
[fleet]
check_timeout_seconds = 30
```

That is a budget, not a delay — a healthy host answers well under it, so the
sweep is no slower. Until v0.26.1 the rollout could not pass one at all, and
every fleet got the 10 s default whatever its login node was like.

## Every lane of `vq admin update` answers the same way

The verb has four forms, and all four classify identically — whether the
refusal comes from the durable update marker, from a safety gate, or from the
checkout-mutation lock that every admin operation on a driver holds:

| form | lane |
|---|---|
| `vq admin update ENV [HOST]` | one venv program |
| `vq admin update --all [HOST]` | every venv program on a host |
| `vq admin update HOST` | scheduler helper, run on the host's driver |
| `vq admin update PROGRAM HOST --expected-sha SHA` | scheduler runtime, run on the host's driver |

The two scheduler lanes did not always answer this way: through v0.26.1 they
refused a held lock with exit 1 and a sentence. On 2026-09-11 the pbs-cluster and
slurm-cluster helper lanes, launched beside a workstation `vibeqc-release` build, both
did exactly that, and a sweep that was being told to wait stopped instead.

**A delegated update relays the code.** `vq admin update pbs-cluster` run anywhere
but pbs-cluster's driver hands the work to the driver over SSH. The driver's
classified exit code — 75, 76 or 77 — and, under `--json`, its `error` come
back unchanged, so a sweep driven from a laptop branches exactly as one on
the driver. Every other remote failure still reads as `remote vq failed
(exit N)` with exit 1: a remote exit 2 is a usage error there, not an
outcome, and 1 is what every unclassified error exits with, so neither is
relayed as one.

`vq admin clear-update-marker` takes the same lock when it recovers a pause
scope, and reports `locked` the same way.

## Sequencing: is anything running?

`vq admin status --json` carries two top-level fields for callers that need
to order work:

* `in_flight` — true only while an operation's writer is demonstrably alive.
  A failed or stale marker is **not** in flight: it is something to
  acknowledge, not something to wait for.
* `last_outcome` / `last_outcome_at` — the classification of the most
  recently *recorded* operation, or null.

`last_outcome` is a record. It says how the last operation ended, not whether
the environment is healthy now — the same true-but-insufficient story
`LAST OK True` told beside a venv that could not import. If you need the
present tense, read `vq programs --json`, which probes.

Without these fields, sequencing drove one orchestration to:

```sh
while [ "$(ssh host 'ps -eo command | grep -c "[n]inja"' || echo 1)" != "0" ]
```

`grep -c` exits 1 when the count is zero, so the `|| echo 1` fallback fired on
*success* and produced `"0\n1"`, which never equals `"0"`. The build finished;
the loop ran for six hours.

## Idempotence

`vq admin update ENV --expected-sha SHA` on an already-deployed target exits 0
with `already-current`, having taken no marker, paused nothing and built
nothing. A sweep can therefore re-run safely, which is what removes the need
to parse `vq admin status`'s columns to decide whether to skip.

"Already deployed" is not a SHA comparison. The commit must match, the tree
must be clean, any requested tag must resolve to HEAD, **and** the program
must answer its own availability probe — because a host sitting at the right
commit with a venv that cannot import is not converged, and calling it so
would skip it forever.

The request must also pass the usual managed service, target and install-mode
checks. Explicit `--update-script-arg` requests always run the updater, including
`--recreate-venv` at the same SHA and profile; malformed or conflicting managed
arguments fail before a marker or pause. A serving queue environment whose
recorded profile does not cover its declared `extras` also needs an update at
the same SHA. A satisfied profile without an explicit request remains a no-op.
Recorded extras describe the installation contract; the program's availability
probe remains the health check for the actual runtime and its dependencies.

## Acknowledging a failed marker

`vq admin update ENV --acknowledge-failed-marker` acknowledges an in-scope
marker whose previous run **failed**, writes the same durable receipt
`vq admin clear-update-marker` writes, and proceeds.

It is not `--force`. It refuses a live or stale marker with
`precondition-failed`, and it never touches a marker outside this update's
scope.

## What a failed marker does and does not block

**Updates: scoped.** A failed marker for `vibeview-dev` blocks
`vq admin update vibeview-dev` and nothing else. `vibeqc-release`,
`vibeqc-dev` and `vibe-view` all proceed. A `--all` request is refused, and
correctly so — it includes the program that failed.

**Dispatch: host-wide.** The same marker holds *job dispatch* on that host
until it is acknowledged, and it does not narrow when the update stops. That
is deliberate where the marker was born — a venv being rebuilt must not be
dispatched into — but it means a failed marker left sitting keeps the host
from running work, which is what "still scoping compute-c's dispatch" meant hours
after that update died.

So if a host has gone quiet after a failed update, the marker is the thing to
look at, and `--acknowledge-failed-marker` (or `vq admin clear-update-marker`)
is what releases it. Narrowing the dispatch hold to the failed program is a
separate design question and has not been done.

## A worked example

`contrib/fleet-sweep.sh` walks a list of `(host, program, sha)` using only
exit codes and `--json`, with no `grep` of any message anywhere. Read it
before writing your own.
