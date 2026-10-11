# Changelog

All notable changes to vq are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

This file starts at the first public commit, `4899089` (2026-09-08).
Development before that point took place in a private monorepo whose history
was deliberately not transferred — see the [README](README.md#history). vq's
version number does not, therefore, restart: the split inherited `0.25.7`,
which is the version the fleet was already running.

> **Entries arrive as fragments.** A change describes itself in
> `changelog.d/<issues>-<topic>.<kind>.md` (see `CONTRIBUTING.md`, "Changelog
> fragments"), and the release cut folds those files into this one with
> `scripts/fold_changelog.py`. Do not add entries here by hand.

## [Unreleased]

## [0.26.32] — 2026-10-11 — Raymond's Bazaar

### Added

- Add `vq estimate` for read-only, versioned resource previews from vibe-qc
  dry-run manifests. Preserve program memory headroom, mark missing quantities
  and aggregation explicitly, fingerprint supplied evidence, and retain explicit
  job limits without executing the calculation or changing placement (#159).

- Add local `vq runtime-bundle pack/publish/deploy` for verified build-once
  artifacts, distinct source/build/content identities, operator-pinned trust,
  target compatibility plans and atomic generation selection. Same-source
  dependency profiles coexist and old payload paths survive rollback. Legacy
  runtime slots and fleet bindings are unchanged (#161).

### Fixed

- **Allow disjoint updates during scheduler runtime builds.** Release driver-wide admin ownership during the remote build while retaining source/store fences, the exact marker lease and shared protection of the running driver. Reacquire ownership and revalidate registry identity before persisting results; shared checkouts and driver mutations still refuse concurrent access. (#154)

- Immutable runtime replacement builds no longer hold eligible new calculations on the healthy current generation. Update ownership and recovery receipts remain intact, while in-place mutations, explicit drains, resource admission and required runtime identities keep their existing fences.

- **Recover mirror inspection from a stalled SSH multiplex connection.** Read-only source-mirror inspection retries once after SSH exit 255 or timeout using a fresh connection. Explicit remote-shell retries now bypass multiplex reuse, retain failure diagnostics and leave shared masters untouched; ambiguous mutations still receive no automatic retry by default. (#169)

## [0.26.31] — 2026-10-11 — Raymond's Bazaar

### Added

- **Runtime retention can keep only the current verified runtime and referenced generations.**
  Use `vq admin runtime-retention ENV --keep 0 --drop-previous` to review removal
  of an unused previous generation, then repeat those options with `--apply-plan`.
  Apply clears the rollback pointer before deleting its target and preserves
  live references, explicit holds and stale-plan protections. Scheduled retention
  continues to keep the previous generation. (#152)

### Changed

- Reserve CI for releases: development refs no longer admit manual or scheduled
  pipelines, and release candidates always run the full test suite, including web
  coverage. Optional documentation publishing moves from `main` to `release`.

### Fixed

- **Updates across hosts fail when a host returns invalid JSON.**
  `vq admin update --all-hosts --json` and `vq admin auto-update --all-hosts --json`
  now include JSON decoding errors in the failed-host summary and exit nonzero,
  while preserving the JSON error entry and completing the other hosts.
  This also applies to auto-update dry runs. (#136, #145)

- Report legacy detached-build timeout termination as unconfirmed, retaining the run for reconciliation before retry.

- Treat failed SSH polls and malformed legacy detached-build responses as unavailable observations instead of refreshing a false running heartbeat.

## [0.26.30] — 2026-10-10 — Raymond's Bazaar

### Changed

- Release project identities now come from external operator configuration. Source staging accepts configured repository namespaces without embedding site accounts in product code.

### Fixed

- **Bound deferred scheduler recovery batches so old jobs do not monopolize polling.**
  The daemon yields after a five-second recovery quantum plus the current attempt,
  then resumes polling and dependency dispatch. Recovery visits the least recently
  attempted jobs first and starts each retry cooldown after the attempt finishes,
  including failed attempts. Existing terminal-evidence requirements remain in
  force. A single slow remote attempt can still exceed a status-refresh deadline;
  this change bounds backlog-induced delay, not individual transport latency. (#149)

- **Managed queue updates refresh their installed user console automatically.**
  After a successful update and verified daemon restart, refresh an existing
  systemd-user or launchd-user console with the target interpreter, preserving its
  service name and supported environment settings. Check its service status and
  runtime provenance; report refresh failures instead of silently leaving stale
  pages. Hosts without a console and consoles in other environments are untouched.
  System consoles still require an explicit operator reinstall. (#150)

- Scoped rollouts collect discovery and final health only for selected hosts,
  their bound scheduler aliases and the driver. Unobserved hosts remain unknown;
  global operation reconciliation and retained ownership holds still apply.

- Current-runtime builds retain lifecycle fences when a retired checkout leaves a plain directory behind. Legacy builds and damaged Git checkouts still fail closed; active runtime provenance checks are unchanged.

## [0.26.29] — 2026-10-08 — Raymond's Bazaar

### Fixed

- **Escalate cgroup cleanup when a scope outlives the stop request.** After the
  existing ten-second graceful-stop wait, vq sends SIGKILL to the remaining
  processes in that job's scope and waits for stop completion. A longer systemd
  stop timeout no longer leaves detached descendants waiting after vq abandons
  its stop client. The daemon logs cleanup it cannot confirm. This preserves job
  outcomes and pause-aware wall-time accounting; it does not establish an absolute
  elapsed-time deadline. (#148)

## [0.26.28] — 2026-10-05 — Raymond's Bazaar

### Changed

- Record completion of declared-extras and web-install acceptance, including the
  v0.26.27 ordinary-updater notice checks in text and JSON modes (#25).

- **Quickstarts use plain `vq fetch` after deployment acceptance.** The examples
  no longer require `--workspace` for jobs whose separate scratch directory is
  provably empty. The guides retain explicit scratch and workspace selection
  instructions for jobs that need them. (#141)

### Fixed

- **Create runtime-slot directories with owner-only permissions.** New store
  roots, release directories and generations no longer inherit group-write
  access from the updater's umask and then fail runtime selection. Existing
  directories and verified generations retain their permissions; unsafe existing
  store roots still refuse instead of being silently repaired (#72).

## [0.26.27] — 2026-10-02 — Raymond's Bazaar

### Fixed

- **Ordinary updates explain when declared extras cannot be applied.** The extras decision is now retained until the update's output channel is ready, so its notice reaches stderr and the run log, including when the requested revision is already current. JSON stdout remains machine-readable. (#25)

## [0.26.26] — 2026-10-01 — Raymond's Bazaar

### Fixed

- **Held broker jobs retain their placement blockers.** A placement pass now saves why no target could accept a held job, so CLI status, waits and the fleet console can explain it after the pass exits or the broker restarts. Later decisions replace the reasons, and reserving a target clears them without changing FIFO order or creating a premature capacity charge. (#64)

- **Archive payload validation rejects parent components consistently across Python versions.** The check now uses the original archive member name even when the standard-library data filter normalizes it, preserving rejection before queue or workspace creation. (#147)

## [0.26.25] — 2026-10-01 — Raymond's Bazaar

### Fixed

- Self-update with a current-slot build input no longer fetches through the legacy
  checkout: ancestry is proved in a temporary source without modifying the serving
  generation. Checkout retirement discovers web units, service markers, retained
  rollback files, service-install backups and local/PATH vq launchers, and refuses
  to delete the interpreter or package running the command.

  Explicit daemon/web installation refuses a migrated registration's legacy
  executable, checks for changes since planning, backs up replaced files and reads
  back the replacement. Web installation preserves unit environment settings with
  an explicit override for unsupported settings; service status detects executable
  path drift even when versions match. Host repointing and retirement remain
  reviewed operator procedures.

- Checkout-retirement plans and receipts now record the discovered service-file
  paths and hashes of the bytes read. Changed service inventories or content
  invalidate a reviewed plan. Receipts also retain declared and automatic
  launcher paths, allowing the retirement scan to be audited afterward.

- `vq admin auto-update --all-hosts` now exits non-zero and names every failed
  host when a local or delegated run raises an unexpected exception or usage
  error. Text and JSON output still report the other hosts, and each failed
  host is counted once, including during dry runs.

- `vq admin rollout-latest` now runs on a driver whose vibeqc-queue registration is slot-executed. The controller check compares the running checkout and interpreter with the verified slot generation instead of the registered legacy `git_dir`/`python`, which may have been retired. A driver re-entry execs the newly selected slot. A controller running outside the selected generation still refuses (#146).

## [0.26.24] — 2026-10-01 — Raymond's Bazaar

### Changed

- Update runtime migration documentation with the completed scheduler layout
  apply, rollback and reapply acceptance. Distinguish that measured scope from
  remaining helper, retention and fleet acceptance work.

### Fixed

- `vq admin update --all-hosts` now exits nonzero and names the failed host when
  an update raises an unexpected exception. Other hosts still run, and text and
  JSON output retain their per-host error details in serial and parallel modes.

- Detached scheduler helpers now see receipt temporaries and unexpected run-directory
  entries created after opening the directory on btrfs. Both observation and the
  detached recorder reopen the held directory inode before listing it, preserving
  receipt publication checks and rejecting unsafe entries.

- Export a standalone runtime launcher backed by a retained base Python, and
  rebind existing migrated launchers through reviewed, recoverable transactions.
  Checkout retirement distinguishes verified slot compatibility paths from live
  dependencies and checks other components' launchers and unfinished rollback
  bindings. Real-host checkout retirement acceptance remains pending.

- **Serving queue generations can build from the active slot.** A serving
  registration with `runtime_slot_build_input = "current"` can prepare and
  self-update without its legacy checkout. Recovery receipts bind to retained
  generation paths, including after an interrupted update or failed restart.
  Existing launchers and other external consumers still need separate rebinding
  before the checkout can be retired. (#142)

- **`vq admin retire-checkout` plans the same way on every host after `migrate-runtime`.**
  The plan now scans the loaded registry itself, not only the registries an
  indexed migration receipt binds, so a host whose receipts are not indexed no
  longer passes a registry that still names the checkout in `[hosts.*]`,
  `pin_source_repos`, commands or another program. In every scanned registry,
  exactly the retiring registrations' own `git_dir`/`python` are exempt, and only
  while they build from their current generation with a verified slot outside
  the checkout; another program's fields block. The plan and the apply receipt
  list those `compatibility_paths`. Apply leaves them in the registry on purpose:
  they are required path templates for the updater entry points and virtualenv
  name, and the migration receipt's recovery binds the registry's exact bytes
  (#143).

- `vq admin rollout-latest --resume ROLLOUT --abandon-host HOST` now also abandons a host whose started actions all failed verifiably: every attempt harvested, and the operation executed and exited nonzero. Before, a rollout that stopped after an update failed on a scheduler host kept its plan-bound holds there, and once a newer report was accepted they blocked every later rollout. The next rollout that plans the host consumes the failure fence (#144).

## [0.26.23] — 2026-09-30 — Raymond's Bazaar

### Fixed

- A keyed directory submit (`vq submit -d DIR --idempotency-key K`) no longer always refuses with "submission payload changed while acquiring idempotency claim" when the jobs directory is on btrfs. vq opened the new workspace while it was empty, and on btrfs a directory descriptor lists only the entries that existed when it was opened. So vq digested the staged payload as an empty directory. Every listing through a held directory descriptor now uses a fresh descriptor of the same directory. This also affects the staged-tree fsync, the idempotency crash-repair scan, the broker record scan, operation-directory validation, fetch and state-path repair (#139).

## [0.26.22] — 2026-09-30 — Raymond's Bazaar

### Fixed

- `vq admin rollout-latest --supersede-plan-hold` now accepts a scheduler lane that is healthy at a commit descending from the accepted pin, such as a dev lane deployed from `main`. Before, such a lane could never be superseded. A dry-run refused on an owned hold now names the supersede command that covers it (#138).

## [0.26.21] — 2026-09-30 — Raymond's Bazaar

### Added

- **The fleet console lists the broker's jobs.** When the console runs on the
  fleet broker, `/fleet/jobs` gains a "Broker jobs" table, read-only and
  metadata only: the broker job id, state, target host and target job id,
  CPUs, memory, job name, created and updated times, and the per-host blockers
  or operator action. It never shows the command, program, payload or
  diagnostics, never creates broker state, and adds no API route. (#64)

- **`vq broker reconcile` settles a broker transfer in doubt.** A job whose
  target refused the keyed submit, or whose answer was lost, stayed
  `transferring` forever, because the broker never resends on doubt. On the
  broker, `vq broker reconcile b-…` now asks the recorded target over SSH to
  run the new `vq admin fence-idempotency-key KEY`, under the same per-key lock
  every keyed submit holds.

  - If the submission had landed (including a spec that a crash left without
    its claim), the job becomes `placed` on that target job.
  - Otherwise the key is durably refused on the target, so no late copy can be
    accepted. The job then holds again for a fresh placement, or is cancelled
    with `--cancel` or an earlier cancellation.

  The answer must come from the recorded state root. A keyed `vq submit` that
  meets a fence is refused. (#64)

- **Filter job timelines with `vq events --kind KIND --tail N`.** Repeat
  `--kind` to include several event kinds, then use `-n` or `--tail` to show
  the last N matches in timeline order. Text and JSON output use the same
  selection, including through remote hosts and scheduler drivers. The default
  still shows the entire timeline; a bounded tail retains only the selected
  records in memory while reading the log. (#131)

- **Use readable durations with `vq wait --timeout`.** The timeout accepts
  one lowercase `s`, `m`, `h`, or `d` suffix, including fractions such as `1.5h`,
  alongside numeric seconds. Values are converted on the client for local,
  remote and broker jobs. The existing minimum of 0.1 seconds, timeout exit
  code 124, and JSON output are preserved; timing out leaves the job running.
  (#132)

### Changed

- **`vq admin update` no longer sets `CMAKE_BUILD_PARALLEL_LEVEL` itself.** Its
  level was computed from MemTotal (at most 6) and overrode the product build
  helper's memory-aware sizing. On a host whose available memory is far below
  MemTotal, it OOM-killed a compiler. The product's helper now owns build
  parallelism. An operator-set value still passes through, and `vq admin
  update HOST` forwards the driver's `CMAKE_BUILD_PARALLEL_LEVEL` to the remote
  update, which ssh alone never did. A malformed value is refused rather than
  forwarded. (#133)

### Fixed

- Restore clickable links from the published operator and agent documentation to the roadmap, design records and maintenance runbook in the public source tree.

- **Damaged event logs no longer break `vq events` when a line contains a JSON
  value that is not an object.** The reader skips nulls, arrays and scalar values
  with a warning naming the source line, before applying kind and tail filters.
  Valid event objects remain in order, JSON output contains only event objects,
  and the original log is left untouched. (#134)

## [0.26.20] — 2026-09-30 — Raymond's Bazaar

### Added

- **`--idempotency-key` works for broker submissions.** `vq submit BROKER
  --idempotency-key KEY …` relays the key to the broker, where it names the
  held job's `b-…` ID. Submitting again with the same key and content returns
  that same job, so a submission whose answer was lost can be retried safely.
  The same key with a different command, resources or payload is refused. A
  payload that a crash left without its record is adopted only when its bytes
  are identical. Submissions without a key are unchanged. (#64)

- **`vq kill --resubmit` works for broker jobs.** It kills the job through the
  broker, then holds a fresh copy: a new `b-…` job with the same payload and
  intent, which the broker places afresh. The copy is made only once the old
  job has provably stopped: cancelled, never able to begin a transfer, or with
  its target job killed or already ended. A transfer in doubt is refused with
  its operator action, because resubmitting it could run the work twice. (#64)

- **An operator page for runtime slots: `docs/runtime_slots.md`.** It explains what a generation is and what the three keys `runtime_slot_root`, `runtime_slot_execution` and `runtime_slot_build_input` each change, including that a vq older than a key fails to load the config. It walks the local `vq admin migrate-runtime` transaction: what blocks a plan, the manifest, the plan, prepare and apply steps, and resume and rollback. It also describes `vq runtime-python`, the launcher primitive. The flow has been accepted on one direct host, and the page says so; the scheduler-host `participants` form is not covered. `operations.md` now describes the JSON that `vq source-identity` prints. (#122)

- **`vq submit` warns about a scheduler host whose remote vq helper is not the
  driver's source tree.** The driver polls a scheduler job through that helper,
  and a helper from another tree left jobs on a host `unpolled` with no
  scheduler job id, silently. The submit now asks the doctor's
  `scheduler_remote_vq` question first. A proven mismatch queues the job with a
  warning that names both identities and `vq admin update HOST`, so a
  driver-first rollout never refuses submits. Setting `[fleet]
  strict_helper_identity = true` on the driver refuses such a submit instead,
  and `--allow-helper-mismatch` then queues one anyway. The flag is forwarded
  to a remote driver only when set. A helper that did not answer or could not
  be reached is always a warning. (#127)

### Changed

- **`docs/user/vasp.md` now records the live VASP qualification and says exactly what the synthetic test proves.** An independent verifier accepted live qualification at two sites, one Torque and one Slurm, on 2026-09-29: one-rank and two-rank runs of a small Gamma-point deck agreed to the printed precision, hashes matched, and a running job cancelled through vq ended with exit code 143 and its partial output retained. The page says what is not established: that the MPI child processes were gone after the cancellation, and anything for another deck, build or site. It also corrects the section written on the day before the acceptance: the synthetic test shows that a program prologue can set the thread counts, not that `--cpus` does, and its cancellation is signalled by a simulated `qdel`. (#110)

- **A plain `vq fetch` of a local-daemon job succeeds when its scratch
  directory is empty.** It used to copy the workspace and then exit 1 unless
  given `--workspace` or `--workdir`, even for a job that never wrote to
  `$VQ_WORKDIR`. The serving host now records in the fetched tree's
  `_vq/terminal-diagnosis.json` whether that directory is provably empty
  (`workdir_empty`). A job whose directory is empty needs neither flag. One
  that wrote to it, or whose directory is missing, swept or unreadable, still
  exits 1 with the `--workdir` hint. A host running an older vq does not
  record the field, so fetching from it keeps the old rule. (#120)

- **`vq admin retire-checkout` enforces the 2026-09-24 decision on #72, and a
  blocked plan exits 1.** It now refuses in two more cases:

  - a `migrate-runtime` transaction of the registration family is unfinished,
    or its receipt cannot be read;
  - a launcher or registry file names the checkout. This covers files a
    migration bound and files declared with the new repeatable `--launcher
    PATH`.

  `migrate-runtime` now records each receipt directory in
  `<state root>/runtime-migration/receipts/`, which is how retirement finds
  them. A transaction applied by an older vq is not in that index, so declare
  its launchers. A plan with blockers exits 1, as `migrate-runtime` and
  `repair-state-paths` do. The help lists the exit codes. (#123)

### Removed

- **`docs/chat-onboarding.md` is retired.** It described vq as living in a monorepo, stamped the version `0.25.7`, and repeated pages that are kept current. The submit, watch and fetch basics are in the user guide and the machine-readable contract is in the agent interaction protocol; the agent index, `hosts.md` and the help text of `vq programs` now point there. The two facts that had no other home moved to the user guide: `--at` (a job held until a time, with an explicit timezone) and the SLURM-style names `vq sbatch`, `squeue`, `scancel` and `sacct`. (#118)

### Fixed

- **Corrections to the documentation entries of 0.26.18.** `vq host retirement-audit` exits 1 when no journal holds retained evidence for the host, not when none mentions it. The abort reason is `failure_reason` in `vq status --json`, with `reason` only inside its `terminal_diagnosis`. The fetch examples were fixed on the front page, the user index, the abort page and the two pages written for vibe-qc.com, but not everywhere: `chat-onboarding.md` still showed the bare form until it was retired (#118). `release_process.md`, `testing.md`, `codenames.md` and `vasp.md` gained the audience line that the same release said every live page had. The user index now says `--chain` and `--rerun-until` are refused for an ordinary remote host. On the vibe-qc.com pages, `--program` is no longer listed as a way to choose the interpreter and is described as checking that a name is registered; the note that the job uses vq's own Python is limited to local submits; and only `VQ_PROGRAM_BIN`, `VQ_PROGRAM_PYTHON` and `VQ_PROGRAM_GIT_DIR` are described as local-only. (#39, #116, #119, #120)

- **`vq daemon install` keeps the environment of the unit it replaces.** The
  daemon's environment is what every job inherits, and the rewrite used to keep
  only the vq roots it re-derives from the shell, dropping everything else
  without a word. On one host that was `PATH`, which names `%h/bin` and the ORCA
  directory. systemd `Environment=` lines are now copied verbatim, with their
  quoting, several assignments per line and specifiers such as `%h`, which
  systemd still resolves at load time. launchd `EnvironmentVariables` are kept
  by value. `EnvironmentFile=`, `PassEnvironment=`, `UnsetEnvironment=`, an
  unparseable line, or a vq root that the unit sets differently from the
  installing shell cannot be carried over faithfully, so the install refuses
  and names them, as it does for capacity caps. `--allow-dropping-unit-environment`
  proceeds without them, and then the install's own roots win. The
  provenance marker records what was kept under `preserved_environment`. The
  serving-slot self-update accepts the regenerated unit and leaves those lines
  unchanged. (#125)

- **A `vq kill` that reaches a scheduler job after it finished no longer turns
  a successful run into `killed`.** The kill is recorded from the driver's last
  poll, and the scheduler cancel of a finished job does nothing, so such a job
  used to read `killed`, exit 0, `manual_kill`. When Slurm accounting reports
  the job's own end (`COMPLETED` or `FAILED`), the daemon now keeps that
  outcome and exit code and records the ignored request as an event. A
  cancellation that took effect (`CANCELLED`), a kill by the watchdog or a
  dependency, and Torque hosts, whose accounting cannot prove this, are
  unchanged. (#126)

- **`vq admin migrate-runtime --rollback` no longer skips a participant apply
  whose answer was lost.** Such an apply row has an unknown outcome, but the
  rollback treated it as never applied. It then marked the transaction
  `rolled_back` and released the lease, even if the participant had applied
  the change. The rollback now settles each such row through the participant's
  `inspect` first, as a forward resume does:

  - `applied`: the change is rolled back;
  - `not_applied`: the row is skipped;
  - `in_progress` or any other answer: the rollback refuses and keeps the
    lease.

  (#128)

- **`vq daemon install --manager` offers `launchd-user`, the value that works on
  macOS.** It offered `launchd`, which the planner does not accept, so on macOS
  only the auto-detected manager worked. The choices now match the planner and
  `vq web install`, and a test dry-runs every offered value. (#129)

- **`vq admin rollout-latest --supersede-plan-hold` no longer deadlocks when
  the new report pins something only a rollout can install.** It required
  every scheduler lane on the host to be healthy at the new pins. But when the
  new report moves a pin, only a rollout can install it, and the obsolete
  hold blocks that rollout, so every fleet rollout stopped. A lane now also
  counts when it is healthy at the obsolete rollout's own pin. That evidence
  requires every operation of that rollout on the host to be settled, is
  marked `"basis": "historical"` in the receipt, and is re-checked against the
  rollout's own authenticated report. (#130)

## [0.26.19] — 2026-09-29 — Raymond's Bazaar

### Fixed

- **Help and error texts now match what the commands do.**

  - `vq submit --help` no longer calls `--python` and `--branch` single-file
    only, or says `--python` is rejected with `--dir`. Both put the interpreter
    in front of a `--dir`/`--compressed` command, and a test now covers
    `--branch` with `--dir`. (#121)
  - `vq admin bootstrap-self-update` refusals name that command rather than
    `--with-driver-runtime`, a flag it does not have. Its options all have
    help text now, and the help lists what HOST must satisfy. (#123)

- **A runtime migration can record and acknowledge a participant lane that
  has no deployment.** `vq admin migrate-runtime` recorded a participant
  migration only on an existing `HOST:PROGRAM` runtime status row, and only
  `vq admin update` of a configured scheduler runtime deployment creates one.
  A lane such as a site's store-layout lane therefore failed to record on every
  authority, and `--acknowledge` refused. When the participant's
  post-preparation inventory lists the lane, the coordinator now marks it as
  declared in the migration record, and each authority creates the lane's row
  while recording, with the plan's identity and no health claim (LAST OK still
  comes only from a verified update). A deployment program without a row, a
  lane the participant did not declare, a record without a receipt digest and
  an invalid program name still refuse, and nothing is created then. Both
  authorities need this version before such a migration can be recorded. (#124)

## [0.26.18] — 2026-09-29 — Raymond's Bazaar

### Added

- **`vq logs --follow` works for broker jobs.** `vq logs -f BROKER b-…` waits
  while the broker has not placed the job yet, and says so once. It then
  streams the target job's output through the broker until the job ends, so a
  client still needs SSH to the broker only. A job cancelled before placement
  is reported as an error, because it never ran. (#64)

- **`vq wait` works for broker jobs.** `vq wait [BROKER] b-…` polls the broker,
  so a client still needs SSH to the broker only. The job counts as pending
  while the broker holds it or its transfer is in doubt. Once placed, it ends
  with the target job's state and exit code. A job cancelled before placement
  ends as `killed`. On timeout, the message names the broker state, the
  per-host blockers and any operator action. `--json` keeps its shape; its
  `queue_handle` names the broker job on the broker. (#64)

- **`vq admin reconcile-operation` settles a rollout operation left
  outcome-unknown by a driver crash during a detached scheduler-runtime
  build.** Until now, such an operation blocked every later rollout, even when
  the build host's receipt proved that the build succeeded. Run on the driver
  with the operation ID from the refusal, the command:

  - reads the exact bound run, whose retained request must match the local
    binding;
  - requires its terminal receipt to report success;
  - re-runs the independent login-host verification.

  Only then does it record LAST OK, clear the dead update's marker, and write
  the operation's reconciliation evidence and result. The next `vq admin
  rollout-latest` harvests the result and releases the hold. The command never
  relaunches or replays the build, and `--dry-run` checks everything and writes
  nothing. (#115)

### Changed

- **`docs/user/vasp.md` now says what is qualified and what is not, and its commands work from any host.** The page opens with the state of the evidence: the workflow is exercised end to end by `test_fixed_filename_binary_workspace_lifecycle` against a synthetic binary with Torque's commands simulated locally, and a real VASP run has not been qualified through vq. Its example named the host on `submit` but not on `kill` and `fetch`, which fall back to `default_host`, and passed `-o ./results/JOBID` although `-o` is a parent directory, so the tree landed in `./results/JOBID/JOBID/`. The user index now lists the page in its body as well as its sidebar. (#110)

- **Three command descriptions in the documentation now match the code.** `vq host retirement-audit` prints one JSON object with a `retained_receipts` map and exits 1 for a host no journal mentions, which `hosts.md` had described as a list of rollout IDs. `vq fetch --name` retrieves a file or a complete directory, not "one regular file" as `SPEC.md` said. `vq daemon stop` and `vq host list`, which appeared nowhere in the documentation, are described in `lifecycle.md` and `operations.md`. Which of the other under-documented commands to document is asked in #122. (#119, #122)

- **Every live documentation page now says who it is for, and the frozen ones say they are history.** Twenty-seven pages gained an audience line that links the index that owns them. `v0_7_1_lamports_clock_design.md`, `pbs_dispatcher_backend_design.md` and `wall_time_design.md` open by saying they are history and where the live description is, and `state_file_audit.md` says which state it does not yet list. The operator and agent indexes route `orchestration.md`; the operator index links the state-file audit and the maintenance-check pages in the source tree, and the root index links the roadmap and the history and design records. (#119)

### Fixed

- **The fetch examples in the documentation now work on a host that runs its own daemon.** A plain `vq fetch` of a job that a local daemon ran copies the tree and then exits 1 with "refusing to report success", unless it is given `--workspace` or `--workdir`. The front page, the user index, the abort page and both pages written for vibe-qc.com showed the bare form. They now pass `--workspace` and say why. Whether the bare form should keep failing is a question for the maintainer, filed as #120. (#39, #120)

- **The two vq pages written for vibe-qc.com were re-checked against the code, and eleven claims were corrected.** vq does not read the manifest's `[outputs].status`, `vq fetch` is not selective, `--chain` is refused for an ordinary remote host, the dry run uses vq's own Python unless told otherwise, `vq wait` has an exit status, and `provides_branches`, the `VQ_PROGRAM_*` variables and cgroup enforcement are each narrower than the pages said. The README records what was found, that the guard passed before and after, and how the pages compare with vibe-qc `main`. A straight copy into vibe-qc loses nothing. (#39)

- **Remote `vq logs|output|progress --follow` prints output as it arrives.**
  Before, the client read the SSH pipe in 64 KiB blocks, and a block read on
  a pipe waits until the block is full. A quiet job's new lines could
  therefore sit unseen until 64 KiB piled up or the job ended. The client now
  passes on whatever bytes have arrived. (#64)

- **Four documentation links to files removed in #54 are repaired, and the page on jobs aborted by the queue names the field it documents.** `docs/chat-onboarding.md`, `docs/operations.md` and `docs/version_compatibility.md` linked to `handover.md` and `STATUS.md`, which left the repository in #54. Nothing replaces them, so the links are reworded rather than redirected. The `operations.md` section on `pid_recycled` and `cgroup_scope_mismatch` sent operators to a `terminal_reason:` field and a `pgid_missing` reason, and neither exists: the reason is the `failure:` line of `vq status` (`reason` in `--json`). (#116)

## [0.26.17] — 2026-09-29 — Raymond's Bazaar

### Added

- **Submit through the fleet broker from any client.** A client host entry with
  `broker = true` makes `vq submit` relay the job into that broker's held queue
  and print a `b-…` broker job ID. A client then needs SSH access to the broker
  only. `vq status`, `vq logs`, `vq kill` and `vq fetch` of a broker job work
  through the broker. Once the job is placed, the broker proxies them to the
  chosen target, with the fetch stream and its acknowledgement passed through
  unchanged. A broker job needs `--mem-mb`, and options it cannot carry are
  refused by name. (#64)

- **Broker placement falls back past a refused host, and cancellation wins
  over replay.** When the best-ranked host is not an allowed target or its
  state root is unknown, the broker places the job on the next-ranked host that
  can run it, so one such host no longer holds the queue. A job cancelled while
  its transfer was uncertain is never resent. It is reported as
  `cancel-pending`, naming the target and key, for an operator to check and
  kill it there. (#64)

- **Add the broker's strict-FIFO placement pass.** One pass finishes
  interrupted reservations. It settles a placed job's charge once a newer
  target listing includes the job, and replays uncertain transfers at a paced
  interval to their fixed target only. It then places held jobs oldest first.
  The first job that cannot run yet stays at the head, and every later job
  waits behind it, with no backfill. Nothing schedules the pass or enables
  broker submission yet. (#64)

- **Add the broker's durable held-job store.** Accepted broker submissions are
  owner-only records and payload snapshots, outside the broker's own run queue.
  They move through revision-checked `held`, `reserved`, `transferring`,
  `placed`, `returned` and `cancelled` states under a single-writer lock. A
  transfer is keyed by the broker job ID, and its target and fingerprint are
  frozen before any submit, so a retry can never become a second job or reach a
  different host. This internal B3 primitive does not enable broker submission
  or daemon wiring. (#64)

- **Add broker placement of one held job, with restart recovery.** The broker
  debits every outstanding charge from the capacity snapshots, and B2 ranks the
  hosts. The broker reserves the chosen daemon host only when it knows that
  host's exact state root. It writes `transferring` before the keyed submit and
  records the acceptance. A failed or ambiguous submit stays pinned to its
  target. Recovery releases unstarted reservations and replays a transfer with
  the same key only to the same target and state root. This internal B4a
  primitive does not choose between held jobs or enable broker submission.
  (#64)

- **Add pure broker host eligibility and ranking.** Explicit, fresh capacity
  snapshots must establish daemon admission, state-root agreement and enough CPU,
  memory and job-slot headroom. Scheduler lanes and uncertain capacity stay
  ineligible; eligible hosts reuse the existing recommendation order. This
  internal B2 primitive does not enable broker submission or daemon wiring. (#64)

- **Add the fleet broker role.** A host whose config sets `[broker] enabled =
  true` with a list of daemon-host `targets` holds submissions and places them.
  It places them strictly first-in-first-out, and only when a target can run
  the job now. Its daemon runs `vq broker pass` every `interval_seconds`,
  collecting each target's evidence through the new read-only
  `vq broker capacity --json`. That evidence comes from the target daemon's own
  admission snapshot (new read-only RPC `get_admission_snapshot`).
  `vq broker accept`, `list`, `show` and `cancel` manage the held jobs.
  Operator steps and recovery are in `docs/operations.md`, "The fleet broker",
  and the trust boundary is in `SECURITY.md`. (#64)

- **Add the public fleet snapshot operator runbook.** It covers a single writer
  timer, isolated static serving, publication permissions, freshness monitoring,
  and backup/rollback with a site acceptance recipe. The approved public schema
  and writer behavior are unchanged; deployment remains an operator action. (#66)

- **Runtime retention reads live processes on Linux.** `vq admin runtime-retention`
  keeps any generation a live process references through its executable,
  working directory, command line, `VQ_RUNTIME_SLOT_SHA`, memory maps or open
  files. It proves a started job with no recorded generation from its complete
  process group, so a busy host no longer has to keep every generation until
  such jobs end. A group that is gone or partly unreadable still keeps
  everything, and hosts without `/proc` behave as before. (#74)

- **Let dev lanes track a branch instead of the release pin.** A driver's
  `[fleet_tracking_lanes]` names programs (`vibeqc-dev`, `vibe-view`) whose lanes
  follow a branch head. `vq admin rollout-latest` fetches and resolves each head
  once per run, refuses one that does not descend from the report pin, and
  deploys that exact SHA with `--expected-sha`; a later rollout deploys a newer
  head. `--only-tracking` advances only those lanes, and `--verify-only` accepts
  a tracking lane anywhere past its pin while reporting its SHA, age and position
  relative to the head. Release lanes, vq itself and configs without the key are
  unchanged. (#109)

- Document VASP submission, allocation, cancellation, complete output retrieval
  and explicit restart handling through the existing generic program and
  scheduler interfaces. Synthetic workflow tests require no licensed software or
  potentials; actual site qualification remains an operator responsibility.

### Changed

- **Record the approved durable-broker design.** The design defines held records,
  reservation and transfer recovery, SSH trust, daemon-only placement and
  broker-owned provenance. It documents later implementation gates and does not
  enable a broker role or change JobSpec fields. (#64)

- The retained `update_allocation` deploy logs under `<scratch_root>/.vq-admin/runtime-deploy/<program>/` are now capped at 100 MiB per program. After each run the newest attempts that fit are kept, the current one always, and older attempts are removed; the pre-per-attempt shared `deploy.out`/`deploy.err` are removed as well. Previously every attempt was kept indefinitely (#105).

- The runtime migration docs and help now describe what #106 and #107 changed. `vq admin runtime-bindings --help` and the design doc say that a program whose hooks block has no command wrapper is left out of `command_wrappers`, and that a `migrate-runtime` plan reviewed before the coordinator was upgraded past #106, for a host where a driver reported an empty wrapper list, must be made again, because `--apply-plan` refuses its old bindings digest. `vq admin migrate-runtime --help` and the design doc name the three plan statuses, `preparation_required`, `ready` and `blocked`, with blockers taking precedence (#106, #107).

### Fixed

- Two follow-ups to the runtime slot first build (#104). `vq admin update` of a slot program with an `install_script` now forwards the operator's `--update-script-arg` flags to the installer, without source selectors and without `--recreate-venv`, and checks an `--extras` request against the declared floor; they used to be dropped while the update reported success on a profile nobody asked for. A `vq admin migrate-runtime` plan that would build a first generation through an installer outside the source tree now lists that as a blocker instead of promising a preparation that always refuses (#104).

- **Scheduler allocation updates retain and settle each attempt's transcript.**
  Unique log directories prevent later attempts from overwriting evidence. After
  scheduler completion, bounded stability checks wait for delayed output; empty
  or unsettled output is explicitly marked incomplete in the receipt with its
  retained log directory. Deployment exit codes remain intact. (#105)

- **Runtime migration authorities agree on empty command wrappers.** Empty wrapper
  lists and omitted entries now produce the same reviewed binding digest, including
  reports from older drivers. Nonempty wrapper differences still block migration.
  (#106)

- **Blocked runtime migration plans report `status: "blocked"`.** Local and
  participant plans give blockers precedence over readiness or preparation, so
  operators are not directed to a preparation step that must refuse. (#107)

- **Process-group tests support Python builds without `os.waitid`.** The
  real-kernel zombie fixture skips before creating children when that API is
  unavailable, while portable signal-settling tests continue to run. (#108)

- **Runtime slots work across filesystems.** Runtime preparation now copies local Git objects instead of requiring hardlinks, so a legacy checkout and runtime store can live on different filesystems. Offline builds, exact source verification and independence from borrowed object stores are preserved. (#111)

- **Serving daemons can build a fresh runtime slot.** Serving-daemon runtime preparation creates an authenticated new slot at its final path without invoking legacy environment replacement. Existing and sealed generations still refuse mutation; failed preparations remain owned by the slot lifecycle. (#112)

- **The allocation deploy-log prune stays inside its log directory.** The prune of retained `deploy.*` attempts read `ls` output line by line, so an entry whose name held a newline was split and a continuation line was removed as a path relative to the login shell's home once the 100 MiB cap was passed. Each line must now name a `deploy.*` entry of the program's log directory; anything else is skipped (#113).

- **Runtime retention keeps every generation while a process runs through `current` or `previous`.** A process launched as `<root>/current/...` rather than through the stable launcher names no generation, so its complete process group used to count as proof that its job held nothing. A group any member of which names `<root>/current` or `<root>/previous` in its command line, working directory or an open file is now unresolved, and `vq admin runtime-retention` keeps every generation while it lives (#114).

## [0.26.16] — 2026-09-26 — Raymond's Bazaar

### Added

- **`vq web public-snapshot --out DIR` writes a public, read-only fleet snapshot as static files.** It sweeps the fleet with the operator's credentials, projects the result through the new default-deny schema `vq.public-fleet/1`, and atomically replaces `DIR/fleet.json` and a script-free `DIR/index.html`, both carrying the observation time and a stale-after time. It starts no server and opens no listener; the directory is meant to be served by a separate unprivileged account, and the authenticated console is unchanged on loopback. A host appears only under a display name configured in the new `[public_snapshot.hosts]` section, never under its config key, and a down host keeps its card without diagnostics. A calculation appears only when it was submitted with a `public.<alias>` tag, and then as that alias, a coarse state and its host's display name. Job ids, argv, paths, logs, energies, submitters and host identity are never included. A failed sweep writes nothing. `SECURITY.md` records the boundary (#66).

### Fixed

- `vq web public-snapshot` now re-checks every value of the document against the `vq.public-fleet/1` schema before writing, not only the key sets: a host state or load outside the allowed values, an alias that is not a well-formed public alias, a display name or title outside its bound, or a malformed timestamp is refused and nothing is written. A projection failure is likewise a clean "nothing written" error rather than a traceback (#66).

- A participant that answers `refused` to the terminal `acknowledge` of `vq admin migrate-runtime --acknowledge` is now reported as a migration error that names the refusal and says the transaction stays applied, so a later `--acknowledge` re-sends it. It used to escape as an internal exception and a traceback (#72).

- An immutable serving-daemon update (`vq self-update`, single-environment `vq admin update` on a program with `runtime_slot_root`) now refuses a symlinked `config.toml` or user service file, a group-writable config directory or a symlinked path component as an admission error that names the file and the rule, before anything is prepared. These used to escape as tracebacks. The operations guide now also says that the first such update rewrites `config.toml` as a whole, keeping its values but not its comments or layout (#73).

- `vq overview --json` and the fleet console's `/api/v1/fleet` now carry an admin-update marker's immutable-slot receipt as its root, SHA and staged flag only. The registry and service file bytes the transaction retains for recovery stay in the owner-only marker file on the host (#73).

- **A runtime slot's first build now runs the program's `install_script`.** Every generation the slot builder starts is new and has no environment, but it only ran `update_script`, which refreshes an existing one; a strict updater such as vibe-qc's or vibe-view's refuses there, so `vq admin migrate-runtime --prepare-plan` failed within seconds for those components while its plan had reported nothing in the way. A registration with an `install_script` now builds each generation through it, with the declared extras floor and no ref selector (the slot source is already pinned); without one the updater builds, as before. The migration plan reports a blocker when a generation still has to be built and there is no `install_script`. Preparation now keeps the same transcript as a managed update, and a failed preparation names it and quotes the failing script's last lines, where it used to lose the script's output entirely (#104).

## [0.26.15] — 2026-09-25 — Raymond's Bazaar

### Added

- **`vq doctor` checks that `remote_vq` and the host's daemon use the same state root.** A host reached through a bare `vq` while its service runs a wrapper exporting `VQ_STATE_DIR` resolves the default root, so status, overview and the console read an empty second store: one entry where the live store had thousands. The verbose daemon ping (`vq daemon ping --verbose --json`) now reports `state_roots`: the root vq resolves there, and the root each of this user's running `vq daemon run` processes resolves from its environment. `vq doctor HOST` adds a `state_root_agreement` check that fails when a daemon runs and none uses that root. It is skipped on multi-user hosts, for older remote vq versions, and when the process scan is not possible. This is a prerequisite for broker placement (#64).

- **Calculation previews also read the output file of ORCA jobs run through `run-orca.sh`.**
  The wrapper redirects ORCA into one named output file (its output argument,
  or `<input>.out`), so such a job's stdout held almost nothing and it showed no
  preview. For those jobs the preview now reads that one file's bounded tail
  through `vq tail --name`, which rejects absolute and `..` names. It never walks
  the workspace. The response names the source it used. (#65)

- **A public fleet snapshot schema, `vq.public-fleet/1`, with operator-chosen host names and per-calculation opt-in.** The schema is the complete, default-deny list of what a public page may say about the fleet: a display name, coarse availability and coarse load per host, and, per calculation, an alias, a coarse lifecycle state and its host's display name. A host appears only under a name configured in the new `[public_snapshot.hosts]` section, never under its config key; a calculation appears only when it was submitted with a `public.<alias>` tag. Job ids, argv, paths, logs, energies, submitters and host identity are never included. `SECURITY.md` records the boundary (#66).

- **`vq admin migrate-runtime` can plan a migration that a scheduler host's site adapter carries out.**
  A manifest may list `participants` (a host, `kind = "scheduler-packed"`, its
  programs and the drivers that are authorities for it) instead of local
  registries. The host's new `runtime_participant_command` names the reviewed
  adapter. vq exchanges one strict `vq.runtime-participant/1` JSON document
  with it over ssh. The plan is read-only: one `inventory` per participant, its
  receipt and adapter identity bound into the `plan_id`. Unreviewed, ambiguous
  or mixed manifests are blockers. `vq admin runtime-bindings HOST --json`
  prints a driver's binding-relevant config for a scheduler host. The plan
  compares it across every listed authority and blocks on disagreement.
  `--record-migration RECEIPT` attaches a completed migration to the host's
  runtime status rows. `--prepare-plan` prepares every participant
  without activating anything and returns a ready plan with a `ready_id`.
  For a deployment with `stage_source`, the driver stages the source archive on
  the host first, as `vq admin update` does.
  Repeating it is idempotent. `--apply-plan READY_ID` takes a migration lease,
  applies and verifies each participant (rolling back if verification fails),
  records the migration on every authority and releases the lease. Every
  call is journaled: an unknown outcome is resolved with `inspect` on
  `--resume` and never replayed blindly. `--rollback` and a terminal
  `--acknowledge` complete the lifecycle. `vq admin status HOST` shows each
  lane's STORE, PREVIOUS and launcher SCHEMA from the participant's last
  inventory (the `inventory` field of `--json`), recorded after preparation and
  after a verified apply; LAST OK still comes only from the verifier. Older vq versions reject `runtime_participant_command`:
  upgrade every reader of the config first. (#72)

### Fixed

- **A `vq admin install` refused after the clone now clears its own admin-update marker.**
  It used to leave the marker marked failed and tell the operator to run
  `vq admin clear-update-marker localhost --yes` on the machine where the
  provision ran. That refused the next provision until someone did, and no one
  host argument reached the marker on every route. The refusal now ends by
  naming that machine and saying only to empty `git_dir` there. A provision that
  then meets that checkout no longer tells the operator to refresh it with
  `vq admin update`, which on a checkout with no venv builds one through
  `update_script` and, for a program whose declared extras cannot be applied,
  reports success without them. For a checkout with no venv it says to empty it
  if a refused install left it, and names `vq admin update` only for an
  operator's own clone (#47).

- **A job re-attached after a daemon restart keeps its wall-time budget.** The watchdog counted a re-attached job's elapsed time from the restart instead of from its recorded `started_at`, so every restart, including the one an admin update performs, granted the job a fresh `wall_time_seconds` budget. One job ran nearly five hours against a 9000 s limit. The budget now counts from `started_at`, less paused time as before, and a job already past its limit is stopped after the restart. A missing, unparseable or future `started_at` falls back to the restart, with a warning (#97).

- **`vq admin rollout-latest --supersede-plan-hold` can pass its health gate again.**
  The gate required each scheduler action's lane evidence to have exactly
  nine keys. Lane state gained a tenth, `probe_unavailable`, so every plan the
  CLI built was refused as lacking healthy evidence, whatever the host's real
  state. The gate now accepts the current evidence shape and the older
  nine-key one. It requires `probe_unavailable` to be false and keeps
  reporting an unanswered probe as retryable. (#98)

- **A finished rollout no longer leaves holds on hosts it deferred and never started.**
  Finalizing a rollout now releases each owned plan-bound hold whose host has
  only `not-run` actions with no operation attempt, provided a read-only drain
  snapshot shows no claim but the rollout's own lease. It uses the same proof,
  journal rows and exact-owner release as `--abandon-host`. A hold that fails
  the proof is kept and records why (`auto_release_refused`), and
  `--abandon-host` stays the manual recovery. (#99)

- **Four newer subprocess calls no longer inherit the caller's stdin.** The two
  `ps` listings behind `vq doctor`'s state-root check, the local `vq drain` that
  takes and releases a runtime migration's scheduler lease, and the local
  `vq tail --json` behind the console's ORCA calculation preview now start their
  child with stdin closed, as every other vq subprocess already does. None of
  them reads stdin, so no prompt or flag changes; a child can no longer consume
  input meant for the invoking terminal or SSH session (#101).

## [0.26.14] — 2026-09-25 — Raymond's Bazaar

### Added

- **The fleet console can preview a vibe-qc or ORCA calculation.**
  `GET /api/v1/fleet/jobs/<id>/preview?host=H` returns a `vq.calc-preview/1`
  summary parsed from the job's stdout tail: formula and atom count, method
  and basis, the latest SCF iterations, per-step energies of an optimisation,
  total energy, gap, the optimisation summary, and any fatal error. It reads
  only the tail the job drill-down already shows (bounded to 2000 lines and
  256 KiB) through `vq status --json`, and sits behind the console's login or
  bearer gate. ORCA jobs are parsed too (input keywords, basis, coordinates,
  DIIS/SOSCF iterations, final energies, optimisation cycles, error
  termination). Other programs return `preview: null`. The job
  drill-down page shows the same summary under "calculation", with an SVG plot
  of the SCF residual per iteration. (#65)

- **`vq admin retire-checkout` deletes a legacy build checkout through a reviewed plan.**
  It applies to a registration with `runtime_slot_build_input = "current"` and
  an active verified generation. The read-only plan reports the checkout's
  HEAD, upstream, size and every blocker: a registration still building from
  it, a service or the running daemon using it, a live job naming it, an admin
  marker, uncommitted changes, stashes, or commits that no remote contains.
  `--apply-plan PLAN_ID` re-plans under admin and lifecycle ownership, refuses
  on any change, deletes the tree (with `munlink` on Lustre) and writes a
  receipt with the exact HEAD and upstream. (#72)

- **`vq cleanup HOST --orphans` also removes stray transfer archives on the scheduler host.**
  A submit that dies after uploading leaves `<job>.upload.tar`, and a fetch
  that dies midway leaves `<job>.result-<id>.tar` in the cluster jobs root.
  Neither was ever removed. They are now listed with their age. `-x` removes
  the ones older than `--older-than` (default 14d) whose job has no live local
  spec, using an exact-name `rm -f`. (#91)

- **`vq cleanup --leftovers` removes leftovers of interrupted admin updates and fetches.**
  It lists admin update backups and failed-target quarantines beside registered
  venvs, and fetch staging in vq's job trees, older than `--older-than`
  (default 1 day). `-x` removes them. Admin leftovers stay protected while an
  admin update or recovery marker exists, and their removal holds the
  admin-update and lifecycle locks. Auto-cleanup sweeps now also remove
  leftovers at least a week old. Rollback stages kept by migration or daemon
  receipts, remote uploads kept after an unproven submit, and directories vq
  does not own are never touched. (#95)

- **A finished rollout can release the hold on a host it deferred and never started.**
  A deferred host kept the rollout's owned plan-bound hold, and every later
  `vq admin rollout-latest` refused while it existed. `--supersede-plan-hold`
  needs health evidence that only a rollout of that host can produce.
  `vq admin rollout-latest --resume ROLLOUT --abandon-host HOST` now releases
  the hold when every action for HOST is still `not-run` with no operation
  attempt, the hold carries its exact control binding, and a supported
  read-only drain snapshot shows no claim other than the rollout's own lease.
  It records the abandonment in the journal, releases the lease by exact owner,
  and executes nothing on the host. Supersession rules are unchanged. (#96)

### Fixed

- **A `vq admin install` refused after the clone now says everything that stands between the operator and the next provision.** Any refusal raised once the checkout exists leaves that checkout in place and the provision's admin-update marker marked failed, and a provision refuses either. The refusal now ends by naming the machine the provision ran on and saying to empty `git_dir` there and run `vq admin clear-update-marker localhost --yes` there. No host argument typed on a driver reaches that marker on every route: a forwarded install runs on the far side as `localhost`, and a scheduler cluster's install runs on the cluster while `clear-update-marker CLUSTER` is routed to its driver. It used to name only the checkout, and only for one of the refusals raised there. The restore-then-update route it offers now says it needs `scripts/update.sh` to accept the venv, which that script refuses when none of vq's own scripts built it. A `git_dir` the operator created empty as a mount point is no longer read as a foreign checkout before the clone, and `vq doctor`'s declared-extras check no longer fails such a program as having no route: it cannot tell yet, as for a `git_dir` that does not exist. A checkout that is not a vq tree is no longer offered routes that end in scripts it does not contain (#47).

- **The remedy `vq admin install` prints when it refuses a declaration it cannot apply now works when followed.** Its second way out said to drop the declaration, provision, and apply the profile with plain `vq admin update ENV` — but an ordinary update applies only a floor the program still declares, so on a program that had just dropped it the update reported success and installed nothing. The refusal now says to restore the declaration before that update. A refusal raised after the clone also says that it left the checkout in place and that it has to be removed first, because a provision refuses an occupied `git_dir` and both ways out start with a provision (#47).

- **Correction to the 0.26.10 note on scheduler polling: an edited `ssh` alias or `scheduler_dialect` reaches the poll at the driver's next dispatch to that host, not at its next poll.** The poll's observer is the dispatcher the driver last built for the host, and a dispatcher is built only on a dispatch, a reattachment or a cleanup. Until one of those, a host that receives no new work keeps polling as before. Cancellation and exit-marker reads were and are addressed through each job's own dispatcher (#51).

- **Under a workdir root this host used to write into, the age sweep now removes only the directory a job's record names.** It previously accepted any directory whose name matched a finished job's spec, wherever that spec recorded its workdir, so an operator's `data` under a former root could be removed because a legacy job called `data` had finished under the current one. A record that names a different directory is now left to the operator, as an orphan there already was (#63).

- **`vq doctor`'s `scheduler_scratch_root` check now reaches the driver's verdict instead of reading a lagging record, and a held host takes config edits.** The check compared the driver's remembered root with `config.toml` alone, but the driver writes that record only when it looks a host up (a dispatch, a reattachment, a cleanup), never on a poll. So the runbook's own procedure, setting the key and restarting on an idle host, failed the check with "held" when nothing held the host. That failure blocks a fleet roll. An unconfirmed root whose previous value had been restored was also reported as "held at the root it last adopted", and a drained unconfirmed host kept failing until its next dispatch. The check now reads the queue and fails only while a job that may already be staged carries no recorded workspace, which is the driver's own test. Separately, while a host was held or unconfirmed the driver reused its cached dispatcher on a config edit, so a changed `ssh` alias or dialect did not take effect until the host was released, even though the log said the dispatcher was being rebuilt. The docs and the driver's warning now also say that restoring a `scratch_root` repairs pre-upgrade jobs only once the driver restarts, because a reload does not re-derive a running job's workspace (#63).

- **`vq doctor` no longer says an unconfirmed scheduler host is already serving a newly configured `scratch_root`.** Until the driver next looks the host up, the root in force is the one it recorded. The message now names that root as in force, as the result's `scratch_root_in_force` field already did, and names the configured value as the one the driver will follow (#63).

- **A failed write of the driver's per-host `scratch_root` record is now retried.** The driver updated its in-memory copy before writing the file and skipped any lookup whose entry was already current. So after one failed write, for example on a full disk, it never wrote the file again for the rest of its life. `vq doctor`, which reads that file, then reported a root the driver had stopped serving, and a restart inherited the stale record. The driver now retries the write at most once a minute, from its scheduler poll as well as from any dispatch, so a driver that only polls from then on still catches up. While writes keep failing, the warning is logged when they start and an info line when they recover, not once per attempt (#63).

## [0.26.13] — 2026-09-24 — Raymond's Bazaar

### Added

- **Old runtime generations can be reclaimed on every auto-cleanup sweep.**
  `vq admin runtime-retention ENV --schedule --keep N` opts a program in, and
  `--unschedule` removes it. Each auto-cleanup sweep, whether in the daemon or
  the busy-host sweep process, then plans and applies retention with the same
  revalidation as a manual `--apply-plan`. A program with blockers, or one
  whose host an update currently owns, is skipped until the next sweep. The
  schedule is stored apart from the auto-cleanup policy file, so older vq
  versions keep reading that policy. (#74)

- **`vq cleanup HOST --orphans` finds and removes abandoned scheduler workspaces.**
  It lists the scheduler host's jobs root and classifies every workspace no
  local spec records. Newly staged workspaces carry an opaque driver identity in
  `_vq/driver-id`. Removal (`-x`) takes only workspaces that this driver
  stamped, that are older than `--older-than` (default 14d), and whose job the
  scheduler no longer reports as queued or running. Unstamped workspaces need
  `--include-unowned`, and another driver's workspaces are never removed.
  Removal uses the guarded, Lustre-aware path. (#91)

### Fixed

- **Auto-cleanup now runs on hosts that are never idle.**
  The daemon deferred a due auto-cleanup sweep whenever any job was active, so
  a busy host never archived or deleted old calculations, workdirs or scheduler
  workspaces. While jobs are active, a due sweep now runs as a separate
  `vq cleanup --auto-run-due` process that the daemon starts and polls without
  blocking dispatch or child reaping. It runs at most one at a time and
  restarts no sooner than five minutes after a failure. A sweep lock prevents
  overlap with an in-process sweep. Idle hosts behave as before. (#90)

- **A queue-only re-pin of the same release report can supersede an obsolete rollout hold.**
  Plan-bound hold supersession required the current report's file name to carry
  a strictly higher version. A vq- or viewer-only re-pin regenerates the same
  `vX.Y.Z.json` with a new digest, so an obsolete rollout's hold could never be
  superseded and blocked the next rollout. A same-name report with a different
  digest now counts as newer when it pins the same products and every pin is
  either identical or a strictly higher version, with at least one higher.
  Downgrades, unparseable versions and unchanged pins still refuse. Receipts
  from an earlier re-pin of the same file no longer make the inventory report
  an incoherent identity. The lane-health and drain-snapshot proofs are
  unchanged. (#93)

## [0.26.12] — 2026-09-24 — Raymond's Bazaar

### Added

- **`vq admin migrate-runtime` can move a component to building from its active generation.**
  A candidate registry may now also set `runtime_slot_build_input = "current"`,
  as long as every alias sharing the component's checkout or interpreter sets it
  and every authority agrees; otherwise the plan refuses. The plan reports the
  resulting `build_input`, and rollback restores the original bytes as before.
  Plans digested by an earlier vq still recover. (#72)

- **Runtime generations can be built from the active generation instead of the checkout.**
  `[programs.NAME] runtime_slot_build_input = "current"` (default `"legacy"`,
  requires `runtime_slot_execution`) clones each new generation from the active
  verified generation, whose `origin` is the configured upstream. The registered
  `git_dir`/`python` no longer need to exist for `vq admin update`: the update
  gate checks the active generation, declared-extras routes read its scripts,
  and lifecycle locks keep the registered checkout's keys only while it exists.
  `vq programs --json` reports `runtime_slot_build_input` and the effective
  `build_git_dir`/`build_python`. A registration serving the vq daemon refuses
  the option for now. Older vq versions reject the key; upgrade every reader
  first. Setting it deletes nothing. (#72)

- Support single-user serving-vq updates in immutable runtime generations through
  `vq self-update` and single-environment `vq admin update`. Prepare and verify the
  new runtime before stopping the old daemon, transactionally rebind the registry
  and user service, and restore the old bindings on restart or provenance failure.
  Durable recovery validates exact file and service identities while retaining both
  generations. Batch and multi-user serving-slot updates remain explicit refusals.
  Forward-only self-update checks use the serving generation rather than the legacy
  build checkout and are repeated after preparation, before stopping the service.

- **`vq admin runtime-retention` reclaims old runtime generations through a reviewed plan.**
  It is read-only by default and prints every generation under a program's
  `runtime_slot_root` with its size and why it is kept or proposed, plus the
  candidates, blockers and a `plan_id`. It always keeps `current`, `previous`,
  generations referenced by a started local job, a service definition or the
  running daemon, every `--retain SHA`, the `--keep` newest others (default 2)
  and incomplete generations. A started job with no provable generation keeps
  everything. `--apply-plan PLAN_ID` repeats the plan under admin and lifecycle
  ownership, refuses if it changed, removes exactly the candidates and writes a
  receipt. Updates and activations never delete generations. (#74)

### Changed

- **Lifecycle locks for a slot-rooted program also fence its runtime store.**
  Every checkout/virtualenv lifecycle lock taken for a program with
  `runtime_slot_root` now also holds its canonical runtime root. Two
  registrations sharing one store therefore contend even when their build
  checkouts differ: the second refuses as an operation already in progress. The
  key is carried in the existing `checkout` lock scope, so rollout controller
  handoffs from older vq versions still parse. This is the identity that remains
  once a build checkout is retired. (#72)

- **Remote tree removal uses `munlink` on Lustre.**
  Scheduler workspace cleanup, node-local scratch removal in generated job
  scripts, and runtime-source stage reclaim used `rm -rf`, which stats every
  file and loads a Lustre metadata server with one request per file. Each tree
  is now checked first. If `stat -f` reports `lustre` and `munlink` is
  installed, files and symlinks are found without following links and unlinked
  with `munlink`, the emptied directories are removed with `find -delete`, and
  `rm -rf` removes anything left. Other filesystems are unchanged. No
  configuration is needed. (#89)

### Fixed

- `contrib/fleet_layout_migrate.py` treated a retired update marker (vq's
  `admin-update-in-progress` renamed with a suffix by the maintainer) as a live
  update and refused every plan on that host. It now blocks only on the exact
  marker name and reports renamed leftovers as notes.

- **Unattended updates of a slot-rooted program track its active generation.**
  For programs with `runtime_slot_root`, `vq admin auto-update` measured drift
  against the build checkout's HEAD, which slot updates never move. The drift
  therefore never cleared, and the branch policy queued a new `build-env` job on
  every tick. Drift is now judged from the active generation's SHA and exact
  tags, with the remote read through `ls-remote`. The branch policy's
  `build-env` job checks its baseline against the active generation and proves
  that the target descends from it inside the new, unpublished generation.
  Nothing is fetched into a published generation or into the build checkout, so
  this also works once a `runtime_slot_build_input = "current"` registration has
  retired its checkout. (#72)

- **A runtime generation builds from a local commit without reaching its upstream.**
  Since fresh generations keep the configured upstream as `origin`, the build
  step's own `git fetch origin` failed the build whenever that upstream was
  unreachable, even though the exact commit was already present. That
  contradicted materialization's promise that an available commit needs no
  network. Unpublished generations now skip the fetch when the commit is
  present; an absent commit is still fetched exactly and fails closed. Ordinary
  in-place updates are unchanged. (#72)

- **Slot-rooted programs are judged by their active generation, not the build checkout.**
  A slot update never moves the registered checkout, but `vq admin update`
  idempotence still compared that checkout's HEAD with `--expected-sha`. A
  rollback to the commit the checkout happened to sit on reported "nothing to
  do" and left `current` on the newer generation. Idempotence now requires the
  store's active, verified generation to be the target and healthy. For
  `runtime_slot_execution` registrations, `vq admin status` (text, `--json`,
  overview and console) now reports the executing generation's source, SHA and
  version, as `vq programs` already did. A verified-generation reuse now records
  that generation's SHA as `last_sha`/`last_installed_sha`, where it recorded
  the legacy checkout's. Fleet rollout verification compares both fields.
  (#72)

## [0.26.11] — 2026-09-24 — Raymond's Bazaar

### Added

- **Runtime slots preserve source-local custom virtualenv names.** A registration
  using `SOURCE/.venv-viewer/bin/python` now builds, verifies and launches that
  environment without changing updater arguments or dependency profiles. Custom
  generations record their venv path in schema-2 runtime-slot markers; update all
  readers before preparing them. Default `.venv` generations keep their format.
  External/nested environments still require a reviewed adapter. This is a
  prerequisite for registration migration, not an automatic host migration. (#72)

- **Local runtime registrations can migrate through a reviewed transaction.**
  `vq admin migrate-runtime` inventories all local aliases and candidate registry
  bytes, prepares a verified generation without activation, and applies pointer,
  new launcher and registry bindings with durable rollback material. Stale plans,
  foreign edits, conflicting profiles, active admin markers, serving daemons and
  unhandled external consumers refuse. Legacy paths and generations remain intact.
  Remote/private scheduler participants and real-host acceptance remain separate gates
  for #72; the command does not deploy, prune or restart services automatically.

- **Local programs can explicitly execute verified runtime slots.** The optional
  `runtime_slot_execution` binding selects one generation at dispatch, records
  its exact paths and SHA, and supplies the same paths to managed-program
  environment variables and generated QVF launches. Retries select again while
  fixed-SHA requirements remain enforced; legacy build inputs and paths are
  preserved. Missing current generations fail closed. This is source support
  for #72; coordinated alias/driver migration and serving-daemon slot updates
  remain separate gates.

- **Scheduler drivers can register programs without a local installation.**
  `[programs.NAME] kind = "scheduler"` satisfies program-name validation while
  preserving the target's existing hooks and runtime identity checks. Program
  inventory reports `SCHEDULER_ONLY`, without probing or claiming target health;
  local submit and dispatch refuse the registration. The operator runbook keeps
  real per-program scheduler validation separate from configuration acceptance.
  (#87)

### Fixed

- Preserve the configured upstream in fresh runtime generations even when their
  commit is already available locally, and copy borrowed Git alternate objects
  into the generation. Future preparation can use the upstream after a legacy
  checkout becomes unavailable, without fetching into an existing generation.
  Existing generations and local-only inputs remain unchanged; this does not
  authorize or perform legacy runtime retirement.

- **Runtime-slot updates bind their updater scripts to the new generation.**
  Absolute update and post-update script paths inside the configured source
  tree are rebased to that tree's path in the slot, preserving arguments. An
  external or escaping script entry point refuses instead of running against
  the live checkout. Script bodies and site-specific target arguments remain
  trusted configuration requiring a slot-aware migration contract. (#88)

## [0.26.10] — 2026-09-22 — Raymond's Bazaar

### Added

- `contrib/fleet_layout_migrate.py` phase 2 recognises a **packed-release**
  host (a scheduler site that ships each runtime as one bundle behind a
  per-SHA wrapper) instead of exempting it. A release there holds the bundle, a
  `SHA256SUMS` and `python`, a link to its wrapper; the tool checks that store
  (bundles present, wrapper links and `current`/`previous` resolve) and changes
  nothing, because publishing bundles is the site deploy tooling's job. See
  `docs/fleet_layout.md`.
- `contrib/fleet_layout_migrate.py` moves a host onto one lifecycle-sorted
  home layout (`~/vq`, `~/vqstate`, `~/vqscratch`, `~/vqruns`, a thin `~/bin`)
  for the three phases that stop no daemon and delete nothing: create the
  roots, give the existing runtime store the name `~/vq/runtimes`, and move
  external-program payloads out of `~/bin` behind symlinks, with installer
  archives kept under `~/vq/installers/` beside a `SHA256SUMS`. Phase 4 gives the live
  queue state root the *name* `~/vqstate`, a symlink to it, and never moves
  it: `vq admin update` requires its state root to be a real directory. `plan` is
  read-only and prints a plan id bound to the host and its state; `apply` runs
  only that reviewed id; every apply leaves a receipt that `undo` reverses.
  Payloads move by atomic exchange or hard link, so an old path resolves at
  every instant, which matters on a scheduler host whose running jobs the
  login node cannot see. Sync folders and managed checkouts are refused however
  they are reached. [The layout page](docs/fleet_layout.md) records the design
  and which later steps need the maintainer first. **Shipping the tool migrates
  no host** (vibe-qc#1).

- `tests/test_vibeqc_site_claims.py` replaces
  `tests/test_vibeqc_site_job_states.py` and widens what it guards on the
  vibe-qc.com pages (vibe-queue#39). Alongside the job-state checks it now
  asserts, by introspecting the Click tree, that every `vq <verb>` and
  `<verb> <subverb>` the pages show exists; that an option on a `vq` command
  line is an option of *that* command rather than of any command; that
  options passed after `--` to `contrib/run-crystal.sh` or
  `contrib/run-orca.sh` are options those scripts parse; and that every key
  in a `toml` fence is a real `Config`, `HostConfig` or program-model field.
  Options belonging to a tool this repository does not ship are declared with
  their owner instead of being skipped.

- `vq admin repair-state-paths` plans terminal-record `cwd`, `workdir` and
  `archive_path` repairs after a state-root move. Its read-only inventory
  distinguishes candidate and blocked records, counts applicable changes,
  and reports old-root residue and artifact-link dependencies without rewriting
  them. Application requires explicit roots, the exact reviewed plan and a
  verified direct compatibility alias with matching current artifact identities
  and digests. It preserves all original raw records, unknown JSON fields and
  per-record failure receipts, refusing active jobs, ambiguous destinations,
  stale artifacts and unsafe paths. Missing old roots remain unproved and block
  apply. No artifact is moved or deleted, and record repair does not authorize
  alias retirement (#69).

- **`vq scheduler-probe --json` now carries a `schema` field**, `vq.scheduler-probe.v1`,
  as `vq.fetch-manifest.v1` and `vq.scheduler-resource-usage.v1` already did. The
  payload is published as an agent contract, and until now it had no version an
  agent could pin, so a shape change reached a consumer as a key that had stopped
  being there rather than as a number that had moved. That was not hypothetical:
  the lane rework replaced `capacity.nodes[].group` with `lanes[]`, and anything
  reading the old field simply got nothing.

  `v1` names the shape as it stands now, not the one before that rework. There is
  deliberately no `v0`, because the unversioned payload was never something a
  consumer could have pinned. The number rises when a field changes meaning or
  goes away; adding a field does not raise it, since a reader that ignores unknown
  keys is unaffected (#83).

- **`vq doctor` now reports whether a declared `extras` floor has a route into
  its venv.** The floor reaches pip only through the checkout's own
  `scripts/install.sh` or `scripts/update.sh`, and config load cannot check
  where a configured command points, because at load time there may be no
  checkout to read. So a program whose installer is a site command loaded clean,
  was never provisioned through vq, and produced the failure the key exists to
  prevent — a console venv built without uvicorn whose unit failed on every
  start — with `vq doctor` reporting nothing unusual. The new
  `declared_extras_routes` check resolves both configured scripts for every venv
  program that declares `extras` and says which of them is vq's own, with the
  per-program evidence under the check's `programs` key in `--json`. It fails
  only where both scripts were resolved and neither is vq's own, which is the
  state with no route at all; a program whose only vq script is its installer is
  reported as applied once by the provision and re-applied by nothing. Where the
  checkout is absent or unreadable it says it could not answer and stays OK, and
  where no program declares `extras` it says nothing at all. The question needs
  the checkout, so it is asked only on the host that has one: `vq doctor REMOTE`
  leaves the check out rather than answering about a remote host from the local
  `[programs.X]` registry (#84).

### Changed

- **`vq scheduler-probe` now says what makes a PBS lane a lane, instead of
  guessing one from the order a node lists its properties.** On Slurm a lane is
  a partition and always was; on PBS a lane is a node *property*, and the probe
  used to take each node's **first** property as its lane — an arbitrary choice,
  because a PBS node carries as many properties as the site gave it and their
  order means nothing. A node's other properties became lanes that did not
  exist, and a per-lane figure counted the wrong nodes.

  The authority for which property a queue routes to is now that queue's
  `resources_default.neednodes`, read from the same read-only `qstat -Qf` pass
  that already reads `enabled` / `started`, so the census costs no extra
  scheduler round trip and still mutates nothing. A lane some queue declares
  carries `authority: "queue_default"` and `queues[]` naming those queues —
  which is the queue to submit to for that row's capacity, and what "capacity
  per host and queue" was missing. A property no queue routes to is still
  reported, with `authority: "node_property"`: its figures are true of the nodes
  carrying it, but that any queue sends work there is vq's inference and is
  marked as one. Where no queue sets the attribute, or `qstat -Qf` cannot be
  read at all, every lane comes back inferred and no authority is claimed.
  Slurm's lanes say `authority: "partition"`; nothing about Slurm changed.

  Two `vq scheduler-probe --json` fields change shape rather than being added.
  `capacity.nodes[].group`, the single guessed lane, is replaced by
  `capacity.nodes[].lanes[]`, every lane the node is in — all of a PBS node's
  properties, or the row's partition on Slurm — so a node now appears in each of
  its lanes' `capacity.groups[]` entries. `queues[]` gains
  `default_neednodes`, the raw attribute, so an attribution can be audited
  against the scheduler's own words. The human report marks a declared lane
  `[queue: NAME]` and an inferred one `[inferred]`, with one line explaining the
  marker when any lane carries it. `docs/agent_interaction.md`,
  `docs/operations.md` and the command's `--help` describe all of it (#23).

- **A program may now declare `extras` with only an `install_script`.** The key
  was refused at config load unless `update_script` was also set, on the reasoning
  that a program vq cannot rebuild would accept the declaration and change
  nothing. Applying the floor at install makes that false: a program vq
  provisions but never rebuilds now gets exactly what it declared, once, when the
  venv is built. The model asks for `install_script` **or** `update_script`, and a
  program with neither is still refused, because neither route exists for it. The
  refusal's wording and the note an ordinary update prints changed with it -- the
  note named a managed daemon update as the only path that applies a declaration,
  which is no longer true (#47).

- **The `scratch_root` config key no longer applies in multi-user mode, and a
  multi-user config that sets it is refused at load.** A config key is read from
  the *calling* process's file: the root daemon reads `/etc/vq/config.toml`
  while a user's CLI reads `~/.config/vq/config.toml`, and `vq submit` resolves
  the submitter's jobs directory in the user's own process — so the two could
  resolve different scratch trees, the user staged into one while the daemon's
  cwd gate expected another, and the spec was refused as "outside the
  submitter's jobs trees". The multi-user scratch root now resolves exactly like
  the multi-user state root, which has never had a config key:
  `$VQ_MULTI_USER_SCRATCH_ROOT`, else the `-scratch` sibling of the multi-user
  root. An operator who had set the key on a multi-user host gets a load error
  naming the environment variable rather than a silently per-caller root, and
  `docs/multi_user_deployment.md` says so beside the variable (#63).

- **The first start after an upgrade no longer binds a pre-upgrade scheduler job
  to a `scratch_root` it cannot vouch for.** The refusal to adopt a changed root
  compares against the root the driver last adopted, and that record is written
  by the code that introduced the refusal — so a vq from before it leaves none.
  Upgrade and set the new root in one maintenance window, which is what the
  phase 5 runbook's own procedure produces, and the first start had nothing to
  compare to: it built a dispatcher on the new root and reattachment wrote that
  derivation onto a pre-upgrade spec **durably**, which is precisely the
  stranding the guard exists to prevent, and which no later configuration change
  can undo. Such a start is now careful rather than confident. It still serves
  the configured root, because no other root is known, but it does not bind that
  host's unbound jobs to it and records the root as unconfirmed rather than
  adopted. Those jobs keep resolving their workspace from the derivation exactly
  as the vq that dispatched them did, so restoring the previous value repairs
  them; the doubt clears by itself once they drain, and `vq doctor HOST` reports
  it meanwhile. An unconfirmed root is followed rather than defended — there is
  nothing proven to defend, and refusing to move off it would block the very
  repair the warning asks for. The driver's memory file gains a second entry
  shape for this (`{"root": ..., "confirmed": false}`); an adopted root is still
  written as a bare string, so a rollback to a vq that reads only the first
  shape keeps every entry it could itself have written (#63)

- **A default changed: disposable job scratch no longer lives under the state
  root.** `jobs/` (each job's working directory and results) and `workdirs/`
  (`$VQ_WORKDIR`) now hang off a separate scratch root, defaulting to
  `<state root>-scratch` — `~/.local/share/vq-scratch` on a stock install,
  `/var/lib/vq-scratch` in multi-user mode, and `$VQ_STATE_DIR`-relative when
  that is set. The state root keeps the queue, its locks, archives and the
  daemon's own files, and remains the thing that is never pruned. The split is
  not cosmetic: on two measured daemon hosts job workdirs were **over 99 % of
  the state root's footprint** (132 GB of 138, 81 GB of 84), so a root
  documented as durable was in fact almost entirely disposable bytes.

  **What an operator with an existing install sees.** Nothing moves, and vq will
  not move anything: a job keeps the `cwd`, `workdir` and `archive_path` it
  recorded, only *new* dispatches select the new root, and `vq fetch`,
  `vq cleanup` (listing, accounting and `--delete`) and the age sweep iterate
  **both** roots until the old one is empty. So after an upgrade the old
  `<state root>/jobs` and `<state root>/workdirs` are still there and still
  work, and a new directory appears beside the state root as jobs are
  dispatched. Relocating existing bytes is an operator runbook step
  (`docs/fleet_layout.md`, phase 5), never something an upgrade does.

  `scratch_root` is a new top-level config key — the same concept, for this
  host, that `hosts.<name>.scratch_root` already expressed for a scheduler host
  — overridden by `$VQ_SCRATCH_DIR` and falling back to the derived sibling.
  `jobs_root` and `workdir_root` move the two trees independently for a site
  that wants results on durable storage; unset, they default together, and they
  are single-user only. Set `min_vq_version` when you set any of them: a vq that
  predates the keys ignores unrecognized top-level keys by design, and two
  builds on one host resolving scratch differently is a split store.

  `vq cleanup` now reports and reclaims workdir bytes. The listing has a WORKDIR
  column, `reclaimable` includes them with the workdir total called out, and
  `--delete` removes the workdir as well as the workspace and the archive —
  which it did not before, on a host where `vq cleanup` reported about 1 GB
  across ~1,200 terminal jobs while `workdirs/` held 81 GB. `--archive` still
  tars the workspace only and says so.

  Making the roots configurable narrowed deletion authority rather than widening
  it. A workdir is removed only when the recorded path still names that job
  directly under a root that may hold it; the root is opened
  `O_RDONLY | O_DIRECTORY | O_NOFOLLOW`, so it may be on any volume and may not
  itself be a symlink; the entry is `lstat`-ed and removed relative to that
  descriptor; and an entry owned by another uid is refused. Single-user
  immediate cleanup (`--clean-tmp`) went through those guards for the first
  time — it was previously an unguarded `rmtree` of whatever the spec recorded —
  and a refusal is now recorded on the spec, so `vq status` explains it.

  **Scheduler hosts can change `scratch_root` under jobs in flight.** A job now
  records the workspace it was staged into (`scheduler_remote_workspace`) in the
  same durable write that precedes `qsub`/`sbatch`, and reattachment,
  uncertain-submit reconciliation, retry-evidence rotation, polling, log reads,
  fetch and remote cleanup all address that recorded path instead of recomputing
  it from the host's current value. Remote cleanup checks the recorded path's
  shape rather than equality with the current jobs root, so a workspace staged
  under a former `scratch_root` is still reapable instead of being stranded and
  permanently un-deletable. A job dispatched by an older vq carries no such
  record: the driver binds it on its next observation, and refuses to adopt a
  changed `scratch_root` for that host — keeping the previous root, and logging
  the job ids — until every non-terminal job on it is bound (#63).

- **The Python distribution is now named `vibe-queue`**, matching the repository
  and the project; through v0.26.9 it was `vq`. The console command, the import
  package (`import vq`) and every `vq ...` verb are unchanged, so nothing an
  operator types changes. What changes is the name a requirement or an index
  resolves: `pip install vibe-queue` once published, `pip show vibe-queue` on a
  host, and `vibe-queue>=...` in a dependent's requirements -- vibe-view's
  `[queue]` extra follows in the same release cycle. Neither name existed on
  PyPI, so this settles the public name before either is published rather than
  correcting a collision; a two-letter name on a public index would have carried
  squatting exposure for no gain.

  pip does not know the two names are one project, so an environment installed
  by an earlier release is moved over deliberately: `scripts/update.sh`,
  `scripts/reinstall.sh`, `vq admin update` and the scheduler helper installer
  remove the `vq` record before installing `vibe-queue`. Left in place, that
  record would have kept `pip show vq` reporting a version the host no longer
  runs, and its file list still owns the `vq` command, so a later
  `pip uninstall vq` would have deleted it. Until an environment is updated, the
  installers' mode detection and `vq admin update --adopt-legacy` read the old
  record when no current one exists. The Python floor stays at 3.12, which is
  what the fleet runs (#68).

### Fixed

- Exact admin pause/recovery proofs share one bounded lock-admission deadline
  across nested scans and rechecks, so interrupted cleanup cannot wait forever
  on its own retained spec lock. Unresolved rows keep their recovery marker
  and exact pause token; failed cleanup durably records the original exception
  and permits supported later recovery without resuming other operators' jobs
  (#70). The deadline bounds contention, not total scan time.

- **A busy driver could not be updated: its own health check timed out and
  rolled a valid install back.** The daemon published its RPC socket only
  after reconciling every surviving job at startup — a walk over every
  `queue/*.json` plus one SSH probe per live scheduler job, strictly one at a
  time. On a driver supervising 175 scheduler jobs over an 18,013-spec queue
  that took 9 min 51 s, while `vq admin update`'s readiness window is at most
  600 s; the install was sound, the daemon was healthy, and the update rolled
  back anyway. Because the 600 s ceiling is a hard cap, no allowance formula
  could ever cover it: the busier the host, the less updatable it was, unless
  the operator knew to set `VQ_DAEMON_HEALTH_TIMEOUT`. The daemon now opens
  its RPC socket **before** the reattachment pass, so readiness and
  provenance — which read the installed package, not the queue — are
  answerable within seconds of process start, and local `vq` calls are
  answered during the pass instead of failing for its whole duration.
  Dispatch is unchanged and still waits for the pass: nothing is scheduled
  until every surviving job has been reconciled. A daemon stopped inside the
  pass withdraws the socket before exiting and never enters its main loop, so
  a reader that reaches the socket is still talking to a daemon that came up
  and was not asked to leave. The readiness allowance is kept as headroom for
  the restarts that do restore an older daemon (#67).

- **A scheduler host's whole observation rode on one remote command whose
  size nothing bounded.** With a campaign-sized batch in flight, the daemon
  named every tracked handle in a single `qstat` / `squeue` / `sacct`
  invocation, so the remote command line grew with the queue and one command
  decided whether the host was observed at all. The live and detail polls are
  now split into as few invocations as an 8 KiB rendered-command cap allows,
  issued in order, with the first failure aborting the rest — an ordinary
  small batch is still exactly one command, and a partial result is still
  never reported as a complete one. Two further economies on the same path:
  Slurm's coarse `squeue` poll now carries the same policy as Torque's
  `qstat` (the 120 s login-node budget and two retries for a replay-safe
  read) instead of the generic 30 s housekeeping budget with no retry at all;
  and a pass whose live poll failed no longer also issues the detail poll,
  whose result that pass discards unread, so a host that has just refused one
  command is not immediately handed a second batch. This removes concrete
  risk factors, and **does not** establish the cause of the reported live
  incident, which still needs real-host evidence (#51).

- **A busy driver could not be updated: its readiness window ignored the work
  that made it busy.** `vq admin update` on the fleet driver rolled back a
  valid install. The driver was supervising 175 live scheduler jobs and held
  18,013 queue records, and its daemon needed 9 min 51 s to publish its RPC
  socket — about 5 min 20 s rebuilding one handle per live cluster job, each
  over its own connection to the scheduler host, and about 4 min 30 s walking
  `queue/*.json`, all of it before RPC answers. The window that was supposed
  to cover that counted queued specs and nothing else, so the more expensive
  of the two phases was invisible to it, and its 600 s ceiling was below the
  driver's real startup time, so no allowance formula could reach it and only
  `VQ_DAEMON_HEALTH_TIMEOUT` could — a driver that is busy was exactly the
  machine that could never be updated without knowing that knob. Cost: about
  20 minutes of driver daemon downtime, every local `vq` call failing, and no
  finished cluster job harvested. The allowance now adds 2.5 s per live
  scheduler job (measured ~1.8 s apiece) alongside the existing 25 ms per
  spec, counted by a scan that is bounded in wall-clock time and extrapolated
  rather than allowed to become the same multi-minute walk it is sizing; the
  ceiling rises to 1800 s, which no longer clips the driver's modelled cost.

  Expiry also stops being the whole verdict. When the window runs out and the
  daemon has never answered RPC at all, the update asks the service manager
  whether a daemon process exists: if one does, the wait continues to at most
  three times that host's own window and never past an absolute 3600 s,
  re-asking ten times along the way, because a startup that outran its
  allowance is not an unhealthy daemon; if none does, verification fails
  **immediately** with a message saying so instead of waiting out the rest of
  the window, which is faster than before for a start that really did fail.
  The extension is a multiple of the window rather than a flat grace on
  purpose: a host whose allowance is a fraction of a second has nothing to
  scan, so an expiry there is real and must not be stretched into a long one.
  A probe that itself breaks counts as "cannot tell", never as death.
  Once the daemon has answered even once the deadline is never extended — the
  socket is up, and a missing or mismatched source identity is an answer, not
  a slow start. Daemon startup ordering is unchanged: the RPC socket still
  comes up after the startup walk, so the #53 guarantee that a daemon stopped
  mid-walk never publishes a socket an updater could read as "came up" still
  holds. Because `vq admin update` runs in the interpreter loaded before the
  install, these allowances govern the *next* update on a host, not the one
  that ships them (#67).

- **A fetch lost the whole job over one file it could not carry.** A sweep of
  428 terminal jobs returned 423: three workspaces held a `.qvf` the fetching
  account could not read, and two were inventory runs whose `mirror/`
  deliberately symlinked into a release checkout. The emitter's
  `tf.add(src, arcname=...)` walks the tree and raises on the first unreadable
  entry, so those three produced no tarball at all; the receiver's `data`
  extraction filter refuses absolute symlinks — correctly, they are a
  path-traversal primitive — and one refused member failed the other two
  entirely. Both sides now skip the entry and name it. Every fetched workspace
  and workdir carries `_vq/fetch-exclusions.json` (`vq.fetch-exclusions.v1`)
  listing each missing path, the stage that lost it (`archive` on the host
  holding the workspace, `extract` on the receiver) and why; the emitter ships
  its list as a tar member and the receiver merges it, so one tree has one
  manifest. `_vq/fetch-manifest.json` gains `partial` and `excluded_count`
  (`partial` is orthogonal to `stale`: a partial tree is current but
  incomplete). An empty list is a positive statement that the tree is complete,
  which a missing sidecar is not.

  The extraction filter is **not** relaxed, and `filter="data"` is unchanged: an
  absolute symlink is still never extracted. What is new is that the emitter
  rewrites one into an equivalent *relative* link when — and only when — its
  target resolves inside the workspace being archived, which is exactly the
  case the filter would have accepted anyway; anything else is skipped and
  listed. Only `tarfile.FilterError` is survivable on extraction, so a
  truncated or malformed stream is still a failed fetch rather than a tidy
  partial one.

  A partial fetch is deliberately reported as **success**, because discarding a
  mostly-complete workspace is the loss this fixes — but it is never silent.
  `vq fetch` prints a `PARTIAL` line (and `partial` / `excluded_count` in
  `--json`), and `vq fetch-all` tags the job's row `[PARTIAL: N excluded]` and
  counts partials separately in its summary. (#62)

- **`vq fetch-all` reported a count where a manifest was needed.** A consumer
  of the fetched corpus saw 423 directories with no record of which five were
  missing or why, short of re-reading the log — so a job lost to a permissions
  error looked, from the data, like a gap in the science. Every run now writes
  `<output_dir>/_vq/fetch-all-report.json` (`vq.fetch-all-report.v1`) naming
  every selected job with its host, outcome, state, destination and, for a
  partial tree, its exclusions, plus per-outcome totals and any host that could
  not be swept. `--report PATH` moves it; `--report none` declines it. A
  single-host sweep that dies at the host level still leaves the report. (#62)

- **The web console's session store gave cross-worker SQLite contention a
  two-second budget.** `vq web` workers are separate processes, so the store's
  writers really do contend -- and every session operation opens a fresh
  connection, with several workers racing to create the same two tables on
  first use. When the budget was exceeded the caller got
  `sqlite3.OperationalError: database is locked`. It fails closed, so this is
  availability rather than disclosure, but it already blocked a healthy commit:
  pipeline 5703 job 12025 failed `test_concurrent_first_sessions_share_one_
  complete_secret` and a hand retry of the same job at the same SHA passed. The
  budget is now `SESSION_DB_BUSY_TIMEOUT_SECONDS`, 15 s, chosen from a
  measurement: on a 12-core machine the worst single call was 0.213 s with 12
  concurrent worker processes idle and 0.884 s with 32 under load average 64,
  so 2 s was only about twice the worst contention that machine could produce.
  `scripts/measure_session_lock_budget.py` is that measurement, so a future
  value can be chosen the same way. Two things the report suggested turned out
  not to be needed and are deliberately not done: `sqlite3.connect(timeout=...)`
  already *is* the connection's `busy_timeout` and so already covered every
  statement, and `CREATE TABLE IF NOT EXISTS` takes no write lock once the
  tables exist, so initializing once would not reduce contention. `journal_mode`
  is unchanged at `delete`: WAL would add `-wal` and `-shm` sidecars that
  `_auth_db`'s hardening does not yet cover, which makes it a design decision
  rather than part of this fix. (#60)

- **Retained job history no longer sits on the daemon's admission path, and
  bounding it needs no operator action at all.** The daemon's per-tick scan
  opened, JSON-parsed and model-validated every spec in the queue directory, so
  its cost grew with everything a host had ever run rather than with what was
  live, and the only way to bound it was `vq cleanup --delete`: a choice between
  keeping job history and dispatching promptly.

  Every **settled** row is now left off the scan: one that has reached a terminal
  state and carries no scheduler transaction that could still be open, archived or
  not. The pass that first sees a row still reads it in full — that is what proves
  it settled — and records its on-disk identity; later passes cost one `stat`.
  That identity is `st_mtime_ns`, `st_size`, `st_ino` and `st_dev`, all out of the
  one `stat`, plus the same four for the private scheduler binding in multi-user
  mode — so a multi-user host pays more than the one `stat` a single-user host
  does, and less than the read it replaces. Every spec write is an atomic rename, so it never edits a row in place and cannot leave the identity unchanged while the content moves; an
  archive, a `--restore`, a `vq fetch` read stamp, a hand edit or a new binding
  always changes the identity and puts the row back on the read. A synthetic
  16,000-row retained queue with nothing archived scans 3.4x faster per pass
  (`scripts/benchmark_admission_scan.py`).

  Nothing becomes less visible or less retrievable, and every exclusion is kept:
  `vq queue`, `vq status`, `vq show`, `vq fetch`, `vq cleanup` and `--restore`
  never went through that scan; a row whose scheduler submit is still unresolved —
  or whose private binding is open or unreadable — keeps reserving its slot at any
  age; a settled predecessor still satisfies a `depends_on` gate, because a
  dispatch tick reads back exactly the predecessors a live spec names; a duplicate
  bare id is still quarantined in full even when both sides are skipped; and the
  startup walk still reads every row, so a terminal job's leaked process group is
  still reaped. `vq cleanup --archive` remains how a host reclaims disk, and
  `--delete` how it discards history; neither bounds dispatch latency any more,
  because nothing needs to (#22).

- **Two ways a PBS queue's lane attribution could be lost or misread.** The
  lane a queue routes to is taken from its `resources_default.neednodes`, and
  both of these made vq mis-parse that value at sites whose spelling is
  perfectly ordinary.

  A long value is folded by `qstat -f` onto a tab-indented continuation line,
  with the break placed anywhere, including mid-word. Reading a line at a time
  truncated the value there, so the real lane lost its `queue_default`
  attribution and reported as inferred, a partial word became a lane of its
  own, and the raw attribute the probe publishes as auditable against the
  scheduler's own words was not the scheduler's words. The output is now
  unfolded before it is read, which is a byte-exact rejoin; output with no
  folds is unchanged.

  A digit-only term was treated as a node count wherever it appeared, so a
  property whose name is numeric — a GPU generation such as `2080` — was
  dropped and the queue routing to it went unattributed. A count can only stand
  as the first term of a spec, which is where it is now read: `2:2080` is two
  nodes carrying the `2080` property, `1:ppn=128` still contributes no lane,
  and a bare `2080` is still a count (#23).

- `vq queue` and `vq list` now report scheduler-provided queued reasons in
  grouped footnotes without changing the table columns. Human output is one
  printable line per reason, capped at 240 characters, eight job IDs and eight
  distinct reasons; terminal rows are excluded. The JSON reason remains whole
  and its existing shape is unchanged. The agent contract now documents
  `scheduler_queued_reason` and the scheduler-probe capacity fields, including
  null/error semantics and the single-node scope of the maxima (#23).

- The vibe-qc.com pages in `docs/vibe-qc-site/` are audited against the vq
  source again, the first re-check since the 2026-09-13 reconciliation
  (vibe-queue#39). Six claims were wrong. The parallel CRYSTAL examples
  omitted `--np N`, which `contrib/run-crystal.sh` now requires and no longer
  defaults, so both invocations would have exited. A named `vq fetch` was
  described as refusing to overwrite a different result; it replaces it.
  `vq pause` and `vq resume` were documented as the dispatch gate, which is
  `vq drain` and `vq drain --release`; `vq pause` suspends one running job and
  needs a job id. The `vq queue` sample output named columns the listing does
  not render. The preflight's cost was an unmeasured "second or two" rather
  than its ten-second bound. A See also link labelled GitLab pointed at the
  GitHub mirror.

- **`vq admin install` now applies a venv program's declared `extras`, or
  refuses the install.** `extras` says what a venv is *for*, but only the
  managed daemon transaction applied it: a fresh provision ran the program's
  `install_script` with its own arguments and ignored the key, so a newly
  provisioned venv could lack the runtime its own program declares until some
  later managed update happened to converge it. That is the failure the key
  exists to prevent — a console venv declared `extras = ["web"]` and built
  without uvicorn, so `vq web install` refuses it and the unit fails on every
  start — reintroduced by the one verb whose job is to create such a venv.
  vq now appends `--extras PROFILE` to the installer, resolved to the smallest
  published profile covering both the declaration and whatever the configured
  command already asked for (`--extras dev` plus `extras = ["web"]` installs
  `all`), and appended only when it raises that profile, so a command line that
  already covers the declaration is passed through unchanged. An operator's
  `--install-script-arg --extras PROFILE` sets the base as the per-update
  request does, and one that does not cover the declaration is refused rather
  than silently widened (#47).

- **The install refusal named a follow-up command that exits 1.** When
  `vq admin install` refuses to provision a program whose declared `extras` it
  cannot apply, it prints the ways out. One of them was
  `vq admin update ENV --update-script-arg --recreate-venv --update-script-arg
  --extras --update-script-arg PROFILE`, copied from the managed daemon plan,
  where those flags are right. Outside a managed transaction they are not:
  `scripts/update.sh` refuses a direct `--recreate-venv` unless `--skip-git`
  comes with it, so an operator who pasted the remedy got a usage error from the
  script instead of the venv they were promised — the same shape of dead end the
  refusal's wording exists to prevent, one step further on.

  The remedy is now plain `vq admin update ENV`, which needs no flags because an
  ordinary update applies the declared floor by itself. It is offered only where
  that is true, which is where the program's `update_script` is one of vq's own
  scripts; a program with no `update_script`, or a foreign one, is told that
  pointing `update_script` at the checkout's own `scripts/update.sh` is what
  makes the second route real. The same correction is applied to
  `vq admin install --help`, `docs/operations.md` and `docs/lifecycle.md`, which
  each carried a copy. The managed transaction's own use of those flags, for the
  environment serving this host's daemon, is unchanged and still correct (#47).

- **An install that cannot apply a declaration is refused, not annotated.** The
  floor reaches the installer on the same proof the managed update rests on —
  the script is vq's own `scripts/install.sh` or `scripts/update.sh`, resolved
  inside the checkout the program points at — and any other installer refuses
  the provision, naming both ways out: point `install_script` at the checkout's
  own installer, or drop the declaration, provision, and apply the profile with
  `vq admin update ENV --update-script-arg --recreate-venv --update-script-arg
  --extras --update-script-arg PROFILE`. A notice would have been the cheaper
  change and the worse one, because an ignored notice leaves exactly the broken
  venv above. When the configured `install_script` does not even name one of
  vq's scripts, the refusal comes before the clone, so nothing is created and
  the fix is a config edit. **Operators may notice:** an install that was
  accepted before can now be refused — only for a program that declares
  `extras`, which is a key no vibe-qc or vibe-view program carries. A program
  that declares no extras installs exactly as it always did, and the managed
  update path is unchanged (#47).

- **Every remedy that named `vq admin update` now checks that the program has
  something for it to run.** Admitting `extras` with `install_script` alone made a
  config shape loadable that three messages assumed away: with no `update_script`
  there is nothing for an ordinary update to run, so it pulls the checkout,
  reports success and rebuilds nothing. `vq web install` and `vq web status` told
  an operator whose console could not import uvicorn to run exactly that command,
  so the remedy reported success while the unit kept failing on every start --
  the incident the console's runtime guard exists to prevent, one level further
  on. The install refusal named the same command as its second way out. And the
  note an ordinary update prints ended by describing a rebuild that was not
  happening, offered a fresh `vq admin install` that `_refuse_occupied_git_dir`
  refuses for an environment that already exists, and carried a docstring saying
  `extras` reaches pip only through the managed daemon transaction, which stopped
  being true when install started applying the floor. Each message is now worded
  for the shape it is describing: a program vq can rebuild keeps what it had, and
  one it cannot is told that `update_script` is the missing piece, with
  `vq admin install --help`, `docs/operations.md`, `docs/lifecycle.md` and
  `docs/config.toml.example` saying the same. `vq programs --json` also reports
  `install_script` and `extras` for venv programs, so an operator debugging an
  unapplied declaration can see both fields (#47).

- A scheduler host that refuses the daemon's poll connection is now left alone
  for a while instead of being re-contacted as fast as the failures return. ssh
  reporting a connection that never completed — a banner exchange that timed
  out, a peer that closed, reset or did not route it — defers that host's next
  observation by 30 s, doubling per consecutive refusal to a ceiling of 5
  minutes, and the first poll that succeeds clears it. The deferral is per
  scheduler host and never per job, so a newly submitted job cannot reopen a
  host that is turning vq away, and it never applies to another host. A failure
  that is not a refused connection — a rejected key, a missing `qstat`, an
  unparseable table — keeps the ordinary cadence, because waiting cannot fix a
  standing fault. A deferred pass makes no observation and claims none: it
  writes nothing, never cancels or fetches, and a job keeps the `RUNNING` state
  and the `poll_failed` diagnostic its last real attempt left. Submission and
  result fetch are not deferred. Measured on the previous code, a host that
  refuses everything received about 300 connection attempts an hour
  indefinitely; it now receives about 39 (#51).

- **The scheduler poll cooldown is now genuinely per host, and a host that
  never answers keeps escalating.** It was keyed on the dispatcher *object*,
  so any edit to `config.toml` — which rebuilds a target's dispatcher while
  its in-flight jobs keep the generation they were submitted with — gave one
  scheduler host two live cooldowns and two polls per pass, and the deferral
  warning for the older generation named no host at all, in exactly the
  incident an operator is trying to diagnose. The cooldown, the single
  in-flight observation and the fetch turn are now keyed on the configured
  target name, so one host stays one host across a reload. A run of deferrals
  also ended on any outcome that was not a refused connection, so a loaded
  login node that alternated between refusing connections and letting vq's own
  120-second poll timeout expire stayed pinned at the first 30-second interval
  indefinitely, and the escalation was dropped along with the host's last
  running job. What ends a run is now evidence that the host *answered*: a
  rejected key, a missing `qstat` or an unparseable table still returns it to
  the ordinary cadence at once, an attempt whose outcome vq never learned
  escalates like a refusal, and the record outlives the host's jobs until a
  full ceiling has passed with no attempt. An explicit `vq show --refresh`
  against a waiting host is no longer left to time out or told the job was
  simply not observed: it is answered immediately with
  `scheduler_host_poll_deferred`, and still does not reopen the host (#51)

- **A scheduler job is now cancelled and read through the dispatcher generation
  it was submitted under, not through whichever one happened to carry its host's
  poll.** Batching a host's whole observation onto one connection is right and
  stays, but the helper that picked the batch's dispatcher returned the first
  job's — by dict order — and the daemon then used that same object for
  everything in the pass, including `cancel()` (which runs `qdel`),
  `exit_marker_code()` and the missing-marker diagnostics. Its docstring
  justified that with "every generation of a target addresses the same
  configured host", which is false: `config.toml` is re-read on any edit, so two
  live generations of one target can differ in `ssh`, `scheduler_dialect` and
  `submit_extra`, and an external `vq kill` could send a `qdel` over an alias or
  a dialect the allocation was never submitted through. The two roles are now
  separate. One *observer* per host issues the batched `qstat`/`squeue` and owns
  everything that reads the rows it came back with — the accounting-required
  rule, `phase_from_detail` and the abnormal-termination classification, for
  every job in the batch, because those are questions about that dialect's
  output. Everything addressed to one job goes through that job's own
  generation, as `fetch_results` already did. The observer is chosen rather than
  stumbled into: it is the generation the daemon serves for the target now, so
  an edited `ssh` alias or dialect takes effect on the next pass, falling back
  to the batch's own generations when a reload has emptied the cache; and it is
  recorded on the in-flight observation instead of re-derived when that
  observation is reaped, so a rebuild landing mid-flight cannot hand one
  generation's rows to another's dialect. Separately, a poll whose local `ssh`
  never started (a missing or unexecutable binary) was wrapped as a plain
  scheduler error, which reads as "the host answered" and cleared the poll
  cooldown, letting vq spin at full rate against a fault only pacing responds
  to; it is now classified undetermined, as `_scheduler_poll_answer`'s own
  contract always said. The submit path is deliberately unchanged there, since
  it turns an undetermined outcome into a quarantined transaction and a launch
  that provably never happened must not produce one (#51)

- The same measurement disproves #51's own hypothesis about the cause: no
  scheduler poll attempt bypasses the multiplexed ssh master. A poll retry
  reuses the first attempt's exact argv, so it cannot be producing the
  brand-new unauthenticated connections an sshd `MaxStartups` throttle counts.
  A regression test now pins that (#51).

- **A fetched tree could report a smaller loss than the host it came from.**
  The emitting side's exclusion list is capped, and the receiver rebuilt only
  the records it could see, so "1500 entries excluded, 1000 listed" was merged
  into an untruncated "1000 excluded" in `_vq/fetch-exclusions.json`,
  `_vq/fetch-manifest.json` and the `vq fetch-all` report. The receiver now
  carries the emitter's unlisted remainder forward, so `excluded_count` stays
  the true total and `truncated` stays true across the merge (#62).

- **A partial fetch of a workspace holding hard links lost the whole job, and
  through `vq fetch-all` every job after it.** A hard link member carries no
  data of its own: it names an earlier member, and the receiver resolves it
  there. Skipping the first of a hard-linked set — what an unreadable file
  under a multi-user root does — left the rest naming a member that was not in
  the tarball, which `tarfile` reports as a bare `KeyError`: not a
  `FilterError`, not a `TarError`, so it escaped the fetch's handlers and the
  sweep's per-job guard alike. The emitter now excludes and lists such a link
  in its own right, so a partial tarball is a structurally valid one, and the
  receiver treats a link member it cannot resolve as one more excluded entry
  (#62).

- **`vq daemon status` prints a space after every colon.** The `workdirs:` line
  ran its path straight into the colon while every neighbouring line did not, so
  the `awk '/scratch:/{print $2}'` idiom `docs/operations.md` teaches returned
  nothing for the one line that points at the disposable bytes. All six path
  lines are now one column, labelled and padded the same way (#63).

- **The workdir age sweep could delete a directory vq never created.** Deriving
  sweep roots from the queue's own records let the sweep reach workdirs under a
  root this host used to write into, which is what makes `vq cleanup` account
  for and reclaim them after the root moves. It also handed the sweep the run of
  that directory: it enumerated the former root and removed any entry that had no
  spec, was older than the cutoff and had a name `validate_job_id` accepts — and
  that accepts `notes`, `README` and `my-data`. A root an operator has since
  repurposed could therefore lose directories that were never vq's, aged only by
  the mtime of a directory nobody had added to in a while.

  The authority under a former root now comes from the record, never from the
  listing: only a workdir a spec still names is removed there, and an orphan is
  left for the operator. A root the operator configures *now* is unchanged, orphan
  sweeping included — that is behaviour from long before the scratch split, and it
  is a directory they are pointing the host at today. The residue this leaves, an
  orphan under a root nothing configures any more, is documented as theirs to
  remove rather than described as something a sweep will get to (#63).

- **One foreign directory under a workdir root no longer stops the whole
  auto-cleanup pass.** The sweep resolved a spec path from each directory's name
  outside any `try`, and that raises for a name that is not a job id, so a single
  `ValueError: invalid job id 'not a job id!'` aborted archive, delete *and* the
  workdir sweep — while the daemon still stamped `last_run_at`, so the next pass
  waited out the full interval before failing the same way. It mattered more
  after #63 made the workdir root operator-configurable, since the root may now
  be a shared scratch volume with other tenants' directories in it. A directory
  whose name is not a job id is now skipped: not swept, not an error, and not
  the rest of the pass's problem (#63).

- **A workdir under a scratch root this host has been configured away from is
  visible to cleanup again.** `scratch_root` promised that changing it strands
  nothing, but accounting, `--delete` and the age sweep all asked "is this under
  a root I manage *now*", where "now" meant the current root and the pre-#63 one
  under the state root. Configure a third and the second fell
  out of both: a workdir plainly on disk was accounted as **0 bytes**, `--delete`
  refused it, and `workdirs_swept` reported 0 — while the same `--delete`
  removed the much smaller workspace beside it, because `cwd` has always been
  removed at its recorded path. The reported "reclaimable" total therefore
  under-counted exactly the bytes #63 was opened about. A job's `workdir` is
  written by vq's own dispatch and by nothing else, so single-user mode now
  holds it to the same standard as `cwd`: the record's own root counts, and the
  structural guards are unchanged (the path must be `<root>/<job id>`, the root
  is opened `O_NOFOLLOW`, a symlinked or non-directory entry is refused, and
  ownership must match). The age sweep walks the configured roots plus every
  root the queue's own records still name. Two places deliberately keep the
  narrow authority: multi-user mode, where a queued spec is user-writable so the
  record is evidence and the managed per-uid roots stay the authority, and the
  daemon's immediate `--clean-tmp` removal, which is automatic and unattended
  and where a refusal only defers -- the record survives, so the sweep and
  `--delete` still reclaim those bytes. `--delete` is where the narrow rule had
  to give, because it removes the record too: a workdir it refuses can never be
  found again. What a changed root can
  still leave behind is an *orphan* directory whose job record is already gone,
  under a root no remaining record names; nothing enumerates it, and
  `docs/operations.md` now says to remove that by hand instead of implying a
  sweep that cannot reach it (#63).

- **A changed scheduler `scratch_root` is now held back across a daemon restart
  and a config reload, not only within one running daemon.** The refusal to
  adopt a new root while a host still carries a non-terminal job with no
  recorded workspace was compared against the *cached* dispatcher for that
  host — and that cache is empty at construction and cleared wholesale by a
  reload, so on both paths the new root was adopted and reattachment then wrote
  it onto a pre-#63 job's spec **durably**, which is the stranding the recorded
  workspace exists to prevent. Since the documented procedure is "set the key,
  restart the daemon" (`docs/fleet_layout.md` phase 5), the guard was off
  exactly when an operator used it. The daemon now remembers the root it last
  served for each scheduler host in `<state root>/scheduler-scratch-roots.json`
  (daemon-owned, daemon-read; a missing or unreadable file means "no memory",
  never a guess) and, while a host is held, serves a dispatcher pinned to that
  previous root. The first start after this upgrade has nothing remembered and
  adopts what is configured, as before. The documentation said such a refusal
  "normally clears within one poll cycle"; binding happens when the driver
  *reattaches* to a job, which is a driver start, so it clears on the restart
  after the change and the runbooks now say that (#63).

- **An ordinary queued job no longer pins a scheduler host's `scratch_root`,
  and a host that is pinned now says so.** The refusal to adopt a changed
  `scratch_root` counted every non-terminal job with no recorded workspace,
  including one that had never been dispatched anywhere. Nothing stages a
  workspace before dispatch — the record is written in the same durable
  PENDING → SUBMITTING transition that precedes `qsub`/`sbatch` — so a job still
  PENDING on its first attempt has nothing on the host to strand, and on a
  continuously fed queue the operator's change never landed at all. The guard
  now counts only a job that may actually have been staged: one past PENDING, or
  PENDING again after a retry of that same vq id. The warning was wrong in the
  same way — it said it was protecting "their staged workspaces" for jobs that
  had none — and now names what it is holding and what will release it. The
  pinned state was also invisible: the warning was logged once per daemon
  lifetime and appeared in no command's output, so an operator whose config
  change had not taken effect had nothing to read. `vq doctor HOST` now reports
  `scheduler_scratch_root`, failing while the configured value is not the one in
  force and saying whether the host is held at a previous root or serving an
  unconfirmed one. It is answered from the driver's own record and the local
  config, so it costs no SSH and no new daemon interface; run somewhere the
  driver does not keep its state it reports "no record" rather than a verdict
  (#63)

- **The phase 5 scratch runbook and four pages of pre-#63 paths were corrected.**
  `docs/fleet_layout.md` told an operator to move existing bytes with a command
  that exits 2 (`vq admin repair-state-paths` without its required
  `--state-root`) and flags that do not exist (`--apply`, `--approve`,
  `--backup-dir`, rather than `--apply-plan SHA256` and `--receipt-dir DIR`) —
  and even corrected it could not work, because that verb repairs a state-root
  *alias* (#69): it requires the old root to be the reviewed compatibility
  symlink naming the new one, which a `cp -a` never produces, and it rewrites
  `archive_path`, which must stay on the state root. That section now says what
  is actually supported — `vq fetch` what you want to keep, then reclaim with
  `vq cleanup --delete --older-than`, which frees the same bytes and leaves no
  record pointing at a directory that is gone — and why a symlinked scratch root
  is not a workaround. `docs/agent_interaction.md`, `docs/abort.md` and
  `docs/multi_user_deployment.md` published the pre-#63 layout, two of them as
  copy-pasteable commands that return "No such file or directory" on a fresh
  install (#63).

- **`vq status` reports the scheduler workspace a job is bound to, instead of
  recomputing one.** It derived `runtime_workdir` from the host's *current*
  `scratch_root` and ignored the job's recorded `scheduler_remote_workspace`, so
  after a root change it published a path nothing had staged. That field is part
  of the published agent contract (`docs/agent_interaction.md`) and
  `checkpoint_qvf_path` is derived from it, so a polling agent watched a
  checkpoint file that would never appear. A job dispatched before the binding
  existed still falls back to the derivation, which is where it was staged (#63).

- **A job that COMPLETED no longer reports a `failure` because its workdir was
  left in place.** A refused `--clean-tmp` cleanup was appended to
  `failure_reason`, which `vq status` prints as `failure:` and `--json` reports
  as the terminal `reason`, though that field is reserved for non-COMPLETED
  terminal transitions. Combined with a scratch root the host had moved away
  from, every successful `--clean-tmp` job completing afterwards was stamped as
  a failure. The diagnostic now lands in its own additive spec field,
  `workdir_cleanup_note`, printed by `vq status` as a `workdir:` line and
  carried in `--json` under that name; a FAILED job's own reason is left exactly
  as the terminal transition wrote it. Older specs read clean, and an older vq
  reading a newer spec simply does not show the note (#63).

- The post-stop quiescence test now provides an explicit unresolved result
  for its compensating old-daemon identity check. A fake service that remains
  running no longer sends this unit fixture into a real RPC health wait.
  The test still checks the stop, refusal, compensation arguments and unchanged
  files; production readiness budgets and identity checks are unchanged (#67).

- **Sizing the post-restart readiness window could take down the update lane
  it was sizing.** `vq admin update` bounds its own readiness allowance by
  reading queue specs and counting the live scheduler jobs among them. That
  scan skipped a file it could not parse, but only for a named set of
  exceptions: a spec whose JSON nests deeply enough makes `json.loads` raise
  `RecursionError`, which is none of them, so the exception escaped the scan,
  escaped the window calculation, and aborted whichever lane had asked —
  including the recovery path that runs after an update has already gone
  wrong. Both guards in the scan are now broad, which is what the code always
  claimed: the worst it can do is size the window as if the host were idle,
  and that is a window no shorter than the one before the allowance grew
  (#67).

- Correct the startup-order explanation for the readiness-window change: the
  allowance change itself does not alter daemon startup order. The companion
  early-RPC change publishes its socket before reattachment and withdraws it
  before returning when stopped during that pass. This replaces the earlier
  claim that the socket is never published during the startup walk (#53, #67).
  The allowance used by an update is still the one loaded before installation,
  so new allowances govern the next update on that host.

- **A failed reinstall after the legacy distribution is removed now says that the
  environment has no `vq` command.** `vq_remove_legacy_distribution` uninstalls
  the pre-rename `vq` distribution before the reinstall, and it has to: that
  distribution's `RECORD` owns `bin/vq`, so uninstalling afterwards would delete
  the file the install had just written, and deleting only the `.dist-info` would
  leave a legacy *editable* install's `.pth` and `__editable__` finder on
  `sys.path`. The window between the two is therefore real, and on a direct
  in-place `scripts/update.sh` -- which deliberately owns no venv backup -- a pip
  failure in it strands an environment with no `vq` at all and no sign of why.
  `install.sh`, `reinstall.sh` and the managed fleet transaction all build into a
  replacement venv and roll the whole directory back, so no fleet host is
  exposed. The window cannot be closed from inside the helper, so it is now
  reported: the failure names the removed command and hands over both ways back
  -- the pip install that restores it in place, with this environment's own
  profile and install mode, and the `--recreate-venv` rebuild that does roll back
  (#68).

- **A venv holding a PEP 610 record under both distribution names is now refused
  rather than answered.** The rename moved pip's `direct_url.json` from
  `vq-*.dist-info` to `vibe_queue-*.dist-info`, and all three readers -- vq's own
  and the two in `scripts/_venv_helpers.sh` -- tried the current name and fell
  back to the old one. That preference settled an ambiguity each reader already
  spells out a guard against, because the guard is scoped to one spelling at a
  time: one independent install recorded under each name was answered
  confidently, where two under one name refuse. Its stated justification did not
  hold either -- the current name won because it was the current name, not
  because pip installed it last, so a legacy record written an hour later still
  lost, and the operator saw "legacy adoption PEP 610 origin does not match the
  checkout" while the venv's live record matched, with nothing naming the stale
  `.dist-info`. Both names are now read and unioned, neither preferred, and the
  refusal names the directories it found. An unreadable `.dist-info` also fails
  closed instead of promoting a stale record: `glob` swallows the
  `PermissionError` and reports no match, which made `scripts/update.sh` preserve
  the install mode recorded by a checkout the venv no longer installs -- the
  silent editable-to-copied conversion that reader exists to prevent. **Operators
  may notice:** a hand-built venv carrying both records now refuses adoption and
  reports its install mode as unknown; delete the stale `.dist-info`. A venv vq
  installed never carries both, because the install removes the old record first
  (#68).

- Remote `vq logs`, `vq output` and `vq progress --follow` use stdout's
  binary buffer or an existing byte sink instead of the deprecated
  `click.get_binary_stream` API. The stream is resolved per call and bytes are
  preserved across arbitrary chunk boundaries, including split multibyte
  sequences. The Click dependency range is unchanged (#71).

- The `[test]` extra now bounds httpx at `<0.29`, mirroring the constraint
  starlette's own `TestClient` declares. Starlette couples to httpx's private
  modules and states that bound only under its `full` extra, which vq does not
  install, so nothing prevented a future httpx from breaking the eight web test
  files and CI's unpinned `pip install -e ".[test,web]"`. Product code imports no
  httpx and is unaffected. The click dependency keeps its floor without a cap:
  the code is Click-9-clean since the remote-follow fix (#71), and the privileged
  multi-user runtime hash-pins click separately (#76).

- Readiness sizing retains unread scheduler records as an explicit conservative
  allowance instead of extrapolating an ordered prefix (#77). Service-manager
  failures retain unknown liveness within the existing bounded extension (#79).
  Observation-only recovery rejects an answering identity mismatch promptly;
  actual restarts keep their identity settling window (#78). The existing
  1800 s allowance ceiling, 3600 s extension ceiling and strict identity checks
  are unchanged. Busy-driver deployment and rollback acceptance remain separate
  operational gates under #67.

- Fetch now reports raw payload collisions with its generated `_vq` sidecars as
  errors before overwriting any bytes. Existing destinations and ordinary `_vq`
  lifecycle files are preserved. Only a positively identified same-job fetch is
  reported as already present; ordinary filesystem and archive failures become
  per-job error rows so later jobs in a bulk sweep still run.
  Whole-tree remote fetching now requires a negotiated provenance response from
  the remote queue client; older remotes must be updated. Missing or replayed
  responses fail explicitly, preventing a raw archived sidecar from being accepted
  as generated metadata. Artifact-only fetching is unchanged.

- The scheduler-runtime source-mirror feed no longer force-pushes. It used to
  build `+SHA:refs/heads/release` (or `main`), the matching forced
  remote-tracking ref, and `+refs/tags/*:refs/tags/*`, which broke the standing
  never-force-push rule and made the deployments configured to use that path
  unrunnable; it also mirrored the whole tag namespace rather than the one
  release identity being deployed. The feed is now a single unforced, atomic
  push of the immutable per-pin ref `refs/vq/pins/<sha>`, the lane's branch
  pair, and only the tags naming that pin in the driver's clone. Refs and tags
  the feed does not name stay byte-identical. An update that would need a force
  -- a lane branch that has moved past the accepted pin, a diverged branch, a
  mirror commit the driver's clone does not contain, or a tag the mirror holds
  at another object -- is refused before the push with the mirror's own values
  in the message, leaving the mirror, the active runtime and the canonical
  LAST OK record unchanged; an older accepted pin therefore fails visibly
  instead of silently picking up a newer tree (#81). `mirror_feed_refspecs` and
  `mirror_pin_ref` report what was fed in `--json` and in the transcript.
  Operators note: the atomic push needs git >= 2.4 on the mirror's host, and
  site preparers still read the lane branch pair, so a mirror whose lane branch
  has drifted must be reconciled by its owner before that pin can deploy.

- **The last of the daemon's three per-tick queue walks stops being sized by
  retained job history.** Once per tick the daemon finishes any durable pause
  intent a killed `vq pause` left behind. Taking every spec's lock for that went
  away earlier in this version, and the two per-tick walks that share the
  admission scan now cost a `stat` per settled row — but this sweep globs the
  queue directory itself, so it got nothing from that work and still opened,
  JSON-parsed and model-validated every job the host had ever run, on every tick,
  to learn that a finished job carries no intent. That made it the largest single
  term in a tick by a factor of five, and on a driver with 18-21k retained rows a
  single sweep approached the default 1.0 s poll interval on its own.

  The daemon now hands the sweep the verdict its own scan already recorded: the
  settled rows — terminal, with no scheduler transaction that could still be open
  — whose full read also showed no durable pause intent, keyed on the spec's
  `st_mtime_ns`, `st_size`, `st_ino` and `st_dev`. A path whose bytes still match
  costs the sweep one `stat`. On a synthetic 100 live + 16,000 retained queue
  (`scripts/benchmark_pause_intent_sweep.py`) the sweep drops from 0.489 s to
  0.122 s per tick, 4.0x, and 37.6x against the pre-#22 locked scan; it is now
  cheaper than the admission scan beside it rather than four times its cost.

  Nothing is skipped that has work in it. Being settled is not on its own a
  verdict: a `vq pause` killed before SIGSTOP whose job then exits underneath it
  leaves a terminal row with a live intent, and that row is never skipped,
  because finishing it is the sweep's whole purpose. Arming an intent rewrites
  the row, and every spec write is an atomic rename rather than an edit in
  place, so it cannot leave the identity unchanged while the content moves. It
  is not guaranteed to land on an inode number never used before, which is why
  a read that contradicts a verdict retires it outright rather than relying on
  the identity having moved. The rename onto a new
  inode, so it always changes the identity and puts the row back on the very next
  sweep — no daemon scan in between. A row no pass has read in full, a row
  rewritten for any other reason, a row that cannot be parsed and a row that
  cannot even be stat'ed are all swept exactly as before, through the locked and
  authorized path.

  The sweep stays first in a tick, ahead of scheduler polling and the watchdog,
  so a crash-window intent is still finished before anything decides what to do
  with a possibly-stopped child; the verdict it uses is therefore the previous
  tick's, or the startup walk's on a daemon's first tick. The cost of that
  ordering is bounded at one extra full read per row, once in its life. The exact
  proofs behind `vq admin update` are untouched: `pause_token_scope_with_proof`
  and the token-clearance scan still read and lock every row, terminal rows
  included, and the skip is refused outright if it is ever handed to a scan that
  has not asked for the unlocked read it rests on (#82).

- **An ordinary `vq admin update` now applies a venv program's declared
  `extras` floor.** The declaration reached pip on a fresh `vq admin install`
  and in the managed daemon transaction; every other update rebuilt with
  whatever profile `.vq-install-metadata` recorded and never consulted the key.
  So a program declaring `extras = ["web"]` that was not the env serving this
  host's daemon could still be rebuilt without uvicorn — by the verb an operator
  reaches for to fix exactly that — and the asymmetry had no reason an operator
  could predict from the outside. The floor is now applied wherever vq itself
  builds the environment, on the one proof that already governed the other two:
  the configured `update_script` resolves to vq's own `scripts/install.sh` or
  `scripts/update.sh` inside the checkout the program points at. A program whose
  `update_script` is its own script is updated exactly as before and still says
  which declaration it did not apply. The profile is resolved over whatever that
  run was going to install anyway — an operator's `--update-script-arg --extras
  PROFILE`, else the recorded profile, else `core` — so it can only add
  capability, and a requested profile that does not cover the declaration is now
  refused rather than silently widened. A newly raised floor is not satisfied by
  `--expected-sha` idempotence, because a checkout at the target commit says
  nothing about what its venv carries. The managed daemon transaction is
  unchanged. The note an update prints when it cannot apply a declaration no
  longer offers a managed daemon update as the way round, which was never true
  for a program with no canonical script: that path requires the same script and
  refuses without it (#85).

## [0.26.9] - 2026-09-17 - "Raymond's Bazaar"

### Added

- `vq submit` now warns when a scheduler request is wider than its lane can
  ever run, against a new operator-declared `scheduler_max_cpus` per scheduler
  host. It **warns and submits** rather than refusing: a declared width is a
  capacity observation and goes stale as nodes return to service, so refusing
  on it would reject work the cluster can now run. Until now this path emitted
  nothing at all for a scheduler target, so an unsatisfiable request was
  accepted in silence. `vq scheduler-probe` reports the figure to declare
  (vibe-qc#148).

- `vq scheduler-probe HOST` now reports **schedulable capacity** for PBS and
  SLURM hosts, from `pbsnodes -a` / `sinfo -N`: the largest request that could
  start now, the largest the usable nodes could ever run, and the same pair per
  partition or node property, with a per-node width/free/state census. A node
  that is merely busy is distinguished from one the scheduler cannot use at
  all, and the report warns when an unusable node is wider than anything still
  runnable — the shape of a request that will queue forever. An unreadable
  census reports `error` and claims nothing (vibe-qc#148).

- `vq status` now reports **why** a scheduler job has not started, as
  `queued_why` in the text output and `scheduler_queued_reason` in `--json`.
  The string is the scheduler's own: Torque's `qstat -f` `comment`, Slurm's
  `squeue` `Reason`. It is shown only while the cluster job is queued and is
  cleared the moment it starts, so it never explains a running job.

### Fixed

- **Uploaded runtime-source stages were never removed, and the prune verb could
  not see them.** A scheduler build host that cannot reach the source
  repository has the exact commit uploaded to it, about 110 MB per deploy,
  under `<scratch_root>/.vq-admin/runtime-source/<program>/<sha>-<uuid>/`.
  Nothing removed those after the build had consumed them. On the SLURM host
  235 had accumulated since July, 26 GB, and together with job output they
  pushed a shared ~100 GB home **over quota**: every write failed, down to a
  bare `mkdir`. `vq source-stage-prune` was no help either -- it reads only
  `STAGE_ROOT/generations/<name>`, and these stages have no `generations` level,
  so it reported `removed=0` for them. A deploy that verifies now removes its
  own stage, and a deploy that fails keeps its own for forensics while older
  ones are trimmed to `RUNTIME_SOURCE_STAGES_TO_KEEP` (3), so failures cannot
  accumulate either. This is deliberately **not** the rule for helper staging
  generations, which are still retained and still pruned only by an operator:
  another deployment may be using an older one, whereas a source-upload stage
  belongs to exactly one deploy and nothing reads it once the build is done.
  Nothing in the deploy path calls the prune verb on the host. Failing to
  reclaim never fails a deploy that verified; it is recorded on the result and
  logged. `vq source-stage-prune --runtime-source` is the operator-facing form
  for what older vq left behind, and both forms still only remove a directory
  named exactly `<40 hex>-<32 hex>`. (#61)
- **A job state could be named on the vibe-qc.com pages with nothing watching
  it.** `tests/test_vibeqc_site_job_states.py` reads only sentences that
  mention a *state*, deliberately: a sentence is the one filter that does not
  reduce to "names in the enum are in the enum", which is the defect `b5953c2`
  fixed. Its other two checks read only the tutorial's lifecycle block and
  `queue.md`'s terminal list, so between them they cover the terminal states
  and nothing else. A non-terminal state -- `pending`, `submitting`,
  `submit_outcome_unknown`, `running`, `suspended` -- named in a sentence that
  never says "state" was therefore guarded by nothing, and a later rename in
  `vq.spec.JobState` would have left the page reading as current with every
  test passing. Found while independently verifying `b5953c2` for the
  validation #39 asks for. No page was actually unguarded: all 13 states they
  name are covered today, and the three mentions that already sit outside a
  readable sentence are of terminal states pinned elsewhere. A fourth check
  now asserts that property directly, so the hole cannot open quietly, and
  `docs/vibe-qc-site/README.md` states the constraint it puts on how the pages
  are written. Widening the existing check instead would need a list of every
  backticked non-state identifier in the prose, twenty today, including the
  `cancelled` that the README names precisely because vq does not produce it.
  (#39)

- **A daemon stopped during its startup walk died ungracefully** (#53). Before
  answering RPC, the daemon reconciles every spec on disk, which on a
  driver-sized queue runs for minutes. Its `SIGTERM`/`SIGINT` handlers were
  installed only *after* that pass, so for its entire duration a stop met the
  default disposition and killed the process outright -- `vq daemon stop`,
  `systemctl --user stop` and `launchctl bootout` all ended in an instant
  death with no shutdown and nothing in the daemon log. The window that most
  needed a handler was the one window that had none, and a self-update whose
  health check expired mid-walk rolled back by removing a daemon that could
  not stop gracefully. The handlers now go on before the pass, and the pass
  gives up at the next spec boundary once a stop is requested, because
  `vq daemon stop` waits 10 s and both service managers escalate to `SIGKILL`
  on their own timeouts. A spec the pass never reached keeps its entry state
  for the next start; one it did reach stays reconciled, including any
  `--auto-resume` sibling. A daemon stopped this way exits without publishing
  its RPC socket, so an updater polling for readiness cannot mistake it for a
  daemon that came up.

- **The daemon's pause-intent sweep was sized by every job the host had ever
  run.** Once per tick the daemon finishes any durable pause intent left by a
  killed `vq pause`, and it did so by taking each spec's lock and loading the
  authorization config before looking at the row -- for every spec in the
  queue. A queue retains its terminal jobs, so on a long-lived driver that is
  three file opens per finished job per tick to discover that a finished job
  carries no intent, and it ran even in single-user mode, where the ownership
  check is documented as a no-op. Because the config is parsed and validated
  per row, the cost also scaled with the number of hosts declared in it. The
  daemon's sweep now reads each row first and skips the ones with no intent to
  finish; a row that carries one, and a row that cannot be read at all, still
  go through the locked and authorized path unchanged. Specs are published by
  atomic replace, so the unlocked read always sees one whole record, and an
  intent armed concurrently is reconciled by the next tick exactly as one
  armed a moment later already was. **The saving is opt-in and off by
  default**: `pause_token_scope_with_proof` reconciles before it captures, and
  an exact admission proof still scans every row under its lock, including
  terminal rows. Over 20,000 retained terminal specs and a 40-host config
  (`scripts/benchmark_pause_intent_sweep.py`) the sweep drops from a median
  6.125 s to 0.666 s, a 9.20x speedup, and stops opening and flocking 20,000
  lock sidecars per tick; against an empty config it is 1.761 s to 0.655 s,
  2.69x. This is one of the three full queue scans in a tick; the other two,
  and retained history on the admission path itself, are still open. (#22)

- **A bulk pause or resume paid the whole authorization cost once per retained
  job.** An ownership decision has two halves: a uid comparison that depends on
  the job row, and the policy behind it, which does not. Resolving that policy
  is the entire expense -- it reads and validates the personal and system
  configs and, in multi-user mode, resolves the caller's group and passwd
  entries through NSS. `vq pause --all`, `vq resume --all`, `pause --provides`,
  the scheduler-wide variants and the pause-token proofs all resolved it again
  for every row they checked, and a queue retains its terminal jobs. On a
  driver that had run 16,000 jobs that is 16,000 config parses per verb, and on
  a multi-user host 32,000 NSS lookups, to reach the same verdict every time --
  paid in full by `vq admin update`, which pauses the queue before it starts.
  Each verb now resolves the policy **once for the operation** and checks every
  row against it. The rows checked, the order, the verdicts and the messages
  are unchanged, the final verdict is still taken under the row's own lock, and
  an unreadable or invalid policy still stops the verb -- now before it takes
  its first lock rather than on its first row. A caller that passes no policy,
  which is every single-job verb, resolves one per check exactly as before.
  Over 16,000 retained terminal specs and a 40-host config
  (`scripts/benchmark_bulk_control_authorization.py`) the filter drops from a
  median 4.020 s to 0.516 s, a 7.79x speedup; against a one-host config it is
  1.127 s to 0.462 s, 2.44x. What remains in both figures is reading the
  retained specs themselves, which is the part of #22 that is still open. (#22)

- **Ownership checks deep-copied the whole parsed policy on every call**,
  so any loop that authorizes row by row scaled that copy with the number
  of rows. The config reader now copies only TOML's mutable containers,
  dicts and lists, and shares its immutable scalars. Complete policy bytes
  are still read and still validated on every call, and callers and
  validators still receive independent containers, so revocation and
  read-error behaviour are unchanged. A synthetic 17,000-check, 40-host
  comparison (`scripts/benchmark_queue_policy.py`) measured a median
  5.479 s before and 3.651 s after — 0.322 s down to 0.215 s per 1,000
  checks, a 1.50x speedup. This measures the policy check only, not
  end-to-end admission: per-call validation, now the larger remaining
  term, and retained terminal history are still open. (#22)

- A queued scheduler job gave no reason for waiting, so a request that could
  never be satisfied was indistinguishable from one merely next in line. vq
  already fetched Torque's explanation with every detail poll and discarded
  it; on a cluster where full-node requests outlived the only nodes that wide,
  jobs sat queued for six days while the reason was on the wire the whole
  time. The Torque detail parser now also rejoins values that Torque wrapped
  across lines, which additionally repairs a multi-node `exec_host` that was
  previously truncated at the fold (vibe-qc#148).

- **A backgrounding command's dispatch helper no longer pins a coin flip**
  (#46). In `tests/test_terminal_reaping.py`, `_dispatch_started` asserted
  `RUNNING` for every caller once the job's lock appeared, but
  `_BACKGROUNDING_COMMAND` hands the lock to a background child and then exits
  by design. That whole chain can complete inside the one `iterate()` that
  runs while the helper is between two checks of `started`, and
  `_reconcile_running()` leads that pass, so the spec the helper then read was
  already `COMPLETED` and the setup failed before the test reached what it
  pins. A new `command_exits` flag accepts either state for such a caller; the
  default stays strict for commands that hold the lock themselves, and the
  test's own claim -- a normal wrapper exit leaves background members alone --
  is unchanged. Test-only.

- Escalate killed reattached local jobs to `SIGKILL` after their grace period,
  so a command that ignores `SIGTERM` cannot run indefinitely after a daemon
  restart. Keep its resource reservation until exit or escalation (#18).

- `vq admin observe-update RUN --host HOST` now observes the host it was
  given. It forwarded the read without naming a destination, so a target
  whose own `default_host` pointed at a third machine delegated the
  observation onward and answered `missing` for a run that had completed
  successfully where it was launched. The delegated read now names
  `localhost` explicitly, as the update launch and the driver's own poller
  already did, and keeps the caller's offset, chunk size and JSON shape (#57).

- Stabilize the background scheduler-refresh regression test under load by
  using a consistent liveness budget and waiting for published poll results.
  Check stale-poll rejection independently of waiter thread timing (#48).

- Reap the whole holder process group in the multi-user migration
  operation-lock test, instead of signalling the holder shell alone. On a
  bash that forks its last command — 3.2, as macOS ships — `SIGTERM` killed
  only the shell and left its `sleep` reparented to init, still holding the
  inherited stdout and stderr pipes, so the teardown's `communicate()`
  blocked on EOF until that sleep ended and the test failed deterministically
  on Darwin while staying green on CI's bash 5. The readiness wait is now a
  monotonic liveness deadline rather than a fixed 1 s spin that had to cover
  an interpreter start (#58).

## [0.26.8] - 2026-09-16 - "Raymond's Bazaar"

### Changed

- Read accepted fleet reports from explicit external `fleet_report_repo`
  configuration, independently of the controller source. Retained original
  history authenticates old receipt digests without relaxing current deployment
  rules. Report output stays outside product source. Configure the report store
  before updating a managed controller; ordinary jobs and exact-SHA updates
  remain available without report storage.

- Keep public clone instructions, generic configuration and contributor guidance
  in source. Preserve site-specific historical wording and operator handovers
  in private operations storage with source and license provenance.

- Document public HTTPS cloning, externally configured private access,
  standalone source-download layout,
  and component-specific update paths. Correct installation and configuration
  examples, and identify the privileged helpers' remaining legacy-layout limit.

- Add GitHub issue forms and a pull request template that guide redacted reports,
  reproducible validation and the existing contribution policy.

## [0.26.7] - 2026-09-15 - "Raymond's Bazaar"

- Require an explicit recovery account for host bootstrap and an explicit
  `--np N` allocation for parallel CRYSTAL runs. Update examples and synthetic
  submitter identities to keep site choices outside product source.

- Replace lab-specific test and documentation examples with synthetic site names;
  retain portable CI validation beside the source and private deployment settings
  with the operator tooling.

### Maintenance: separate product source and private operations

Site-specific provisioning scripts, deployment configuration and operator
records move to private operations storage. Portable product installers remain
in source; private helpers are installed independently. Contributor privacy
checks support an external private terms file without publishing its contents.

### Maintenance

- Improve the portability of configuration examples and public package metadata.
  Strengthen contributor privacy checks and keep their diagnostics redacted.

### Fixed

- **Detached build observers tolerate receipts still being published** (#36).
  A receipt with the publisher's matching temporary hard link is pending
  until publication finishes. Strict single-link reads still reject unrelated
  aliases, extra links, mismatched inodes and unsafe permissions.

- **Large queues get more startup headroom during self-update** (#53).
  The default daemon readiness allowance is 60 seconds plus 25 ms per job
  spec, still capped at 600 seconds. This avoids the previous 247-second
  cutoff for a 21,683-spec queue; exact provenance, bounded failure and
  managed rollback remain required. An explicit driver health timeout
  continues to take precedence.

- **Scheduler poll failures retain useful bounded diagnostics** (#51). Long
  qstat, squeue and accounting argument lists no longer crowd the return code,
  failure category or sanitized stderr out of the stored poll error. Timeouts
  remain unknown observations. Polling, retries and lifecycle behavior are
  unchanged; this improves diagnosis without claiming a transport repair.

### Known limitations

- The #53 startup allowance is source-validated; actual host timing and graceful
  service stop during rollback remain open. The #37 logout-survival and
  multiplexing-disabled checks still need separately admitted host validation.
- The #51 diagnostic improvement does not establish the scheduler transport
  failure's cause or prove runtime recovery. A release alone does not establish
  fleet convergence.

## [0.26.6] - 2026-09-13 - "Raymond's Bazaar"

### Fixed

- **Lifecycle refusal messages print their suggested commands safely** (#20).
  Refusing to replace code under a running daemon no longer executes
  `vq self-update` and `vq admin update` through shell substitution. The
  diagnostic retains the daemon description, action, service and venv path.

- **Release discovery cannot silently certify an older release** (#52).
  Discovery refreshes the runtime repository and every distinct configured
  pin checkout under lifecycle locks before validating pins. Fetch failures
  stop discovery. Dry runs name rejected newer candidates alongside any
  fallback plan; rollout, verification and `--from-report` refuse that
  fallback, including during final report rechecks. Report contents and
  digests retain their existing format.

### Added

- **Named directory artifacts** (#55). `vq fetch --name BASENAME` retrieves a
  complete directory, including opt-in TREXIO text output, from a workspace,
  workdir or archived job over local or SSH transport. Files retain their
  existing behavior; links and special files are refused. A complete staged
  directory replaces the previous snapshot without retaining stale children.
  Real-producer integration covers HDF5/text export, transfer, archive readback
  and queued READ with staged inputs; TREXIO remains an optional workload
  dependency and QVF remains the default calculation container.

## [0.26.5] - 2026-09-13 - "Raymond's Bazaar"

### Fixed

- **Completed scheduler jobs receive fair artifact-transfer turns** (#50).
  A dispatcher with continuous completed arrivals could repeatedly take both
  transfer workers while another ready dispatcher stayed `finishing`. Actual
  admission now moves a group behind existing waiters; new groups join at the
  back. Slots freed during a pass become available on the next pass. The
  two-worker limit and existing ownership, marker and terminal-state checks
  remain in force. This does not bound scheduler polling or retained-history
  scan time (#22).
- **Scoped fleet convergence includes bound aliases and retained owned holds**
  (#49). `--only host_f` now reports a degraded result when a captured host_f alias
  fails its final doctor check or an owned rollout hold has no confirmed release.
  Canonical `--skip` excludes the same bound group. These diagnostics preserve
  action selection, external holds and release controls; an unscoped unfinished
  cleanup still exits 1.
- **`vq admin auto-update` runs a delegated rebuild outside the ssh session**
  (#34). Its drift apply is the same real rebuild as `vq admin update`, reached
  through its own verb, so logind `KillUserProcesses=yes` could still end it with
  the session. `vq admin auto-update ENV HOST`, `--all` and each host under
  `--all-hosts` now use the same detached launcher (a transient user unit where
  a user manager answers), polling, launch adoption, old-remote fallback and
  `VQ_ADMIN_NO_DETACH` escape hatch. `--dry-run`, which mutates nothing, stays
  attached.
- **A host's update-script wall cap is a config key** (#32). A cold native
  rebuild of vibe-qc on host_e (6 cores) needs slightly more than the
  four-hour default. On 2026-09-12 the cap reaped a healthy build that finished
  in fifteen minutes once the cap was raised, and the only override was
  `VQ_UPDATE_SCRIPT_TIMEOUT` in the invoking environment, which a
  planner-driven roll never sets. `[hosts.X] update_script_timeout_seconds`
  now sets it: a delegated update forwards it as the wall cap (detached builds
  included, and the SSH observer's own cap grows with it), each host in
  `--all-hosts` gets its own, and a local update exports it for its updater.
  An explicitly set `VQ_UPDATE_SCRIPT_TIMEOUT` still wins. A reap on the
  wall-clock cap no longer calls the build "wedged": it was still producing
  output, and the message names the key to raise.
- **The terminal-reaping tests no longer carry fixed five-second budgets**
  (#41, first slice). In `tests/test_terminal_reaping.py`, the readiness wait
  in `_spawn_running_job` and four waits after an uncatchable `SIGKILL` only
  bound a hang, so they now use the module's 30-second liveness constant. The
  ordinary-kill boundary test does make a timing claim, that the slot is
  released without waiting out the `SIGKILL` grace, so its ceiling is now that
  grace less a named two-second margin (8 s at the default 10 s grace) rather
  than a fixed 5 s. The wider audit of short budgets in other modules is still
  open. Test-only.
- **`vq web config` and `vq web run` say when the config file could not be
  loaded** (#40). The console's settings resolver falls back to built-in
  defaults on a broken config, deliberately, so a console can still start on a
  host whose config is what is broken. It did so silently: `vq web config`
  exited 0 and presented the defaults as "the resolved configuration", and
  `vq web run` started a console that ignored `[web]`, `fleet = true` and
  `bind` included. Both now print a warning naming why the file was not read
  and that `[web]` was ignored, and `vq web config --json` carries it as
  `config_error` (null when the file loads). The fallback and the exit codes are
  unchanged.
- **`vq web status` and `vq doctor` notice a console that cannot start**
  (#28). On example_hub in 2026-09 the console unit's venv had been rebuilt
  without the `web` extra, and systemd restarted it 5230 times; the evidence
  was only in the journal. `vq web status` now runs the interpreter recorded
  in the console's install marker, out of process and with a 30-second timeout,
  and reports whether it can import `uvicorn` and `vq.web.create_app`: a
  `runtime:` line in the text output, a warning with the same host-aware remedy
  `vq web install` prints, and `runtime_ok`, `runtime_detail` and
  `runtime_remedy` in `--json` (null when nothing was probed or the probe did
  not answer). `vq doctor` adds a `console_runtime` check for the local host
  and, for a remote host whose vq answered, from that host's own verdict. The
  daemon's service status is not probed. Existing `--json` fields are
  unchanged.
- **Two fleet-operation thread tests no longer fail under CPU contention**
  (#26). `test_polling_waits_for_hardlink_publish_cleanup` and
  `test_executed_nonzero_is_terminal_and_never_retry_safe` gave the supervisor,
  the ready waiter and the paused publish three to five seconds each, and
  failed together in a full run at load 88–131. Those waits are now bounded
  by a 60-second liveness constant; a healthy run still finishes them in
  milliseconds. The check that the waiter has not returned while the publish
  is paused now runs only once the waiter is seen observing the operation, so
  it can no longer pass before that thread has been scheduled, and it still
  fails when an observation reports the receipt early. Test-only.
- **Test cleanups no longer fail on a process group that is exiting** (#29).
  The idiom that flaked `tests/test_terminal_reaping.py`, a cleanup `killpg`
  that tolerated only `ProcessLookupError`, remained in four more modules. On
  macOS a group mid-teardown answers `EPERM` (#27). `tests/test_pause_resume.py`
  now releases a job's group only while the test's own leader process has not
  exited, which also keeps the signal off a reused id, and tolerates `OSError`.
  The cleanups in `tests/test_daemon.py`, `tests/test_host_f_maintenance_scripts.py`
  and `tests/test_fleet_rollout.py`, whose leader may already be gone while a
  member keeps the group, tolerate `OSError`. `tests/test_scoped_admin_update_hold.py`
  is handled by merge request !5. Test-only.
- **A job that is exiting is no longer reported as another user's** (#27).
  On macOS a process group whose last member is exiting, or is a zombie
  waiting to be reaped, answers `killpg` with `EPERM` for a moment before
  `ESRCH`. `vq kill` printed `could not be signaled: permission denied` for
  such a job, `vq pause` and `vq resume` said it belonged to another user, and
  the daemon's watchdog logged a permission warning. Every signal those paths
  send now re-probes an `EPERM` for up to one second: a group that settles to
  `ESRCH` is reported gone, and one that keeps answering `EPERM` (another
  owner's live group) keeps its existing message. The admission proof's
  liveness probe still counts `EPERM` as alive. On 2026-09-13 at load 86 the
  window measured a median of 3 ms and a maximum of 77 ms.
- **A config that fails validation is reported by key and reason** (#38).
  `ConfigError` messages, and the lines the daemon logs from them, are built
  from pydantic's structured error list: one line per problem, as
  `location: message`, without pydantic's default rendering or its
  documentation link. The same applies to `vq web install`'s refusal to write
  an invalid `[web]` section and to the multi-user probe that provisioning
  runs on a target.
- **`vq programs` no longer calls a runtime that imports `MISSING` because
  its healthcheck failed** (#45). On 2026-09-12 the driver listed
  `vibeview-dev`, which imported 2.16.1 but whose Linux-only `xvfb-run`
  healthcheck could not start on macOS, with the same `MISSING` as a
  `vibeqc-dev` that could not import, and the loop's fleet monitor had to
  separate the two itself. A venv program whose runtime loads and whose
  healthcheck does not pass is now `UNHEALTHY`, in the table and in `--json`,
  and `MISSING` keeps meaning that work dispatched there will fail. A new
  `--json` field, `healthcheck_status`, says whether the healthcheck
  `could-not-start` (a configuration problem), `timed-out` or `failed` after
  running, or was `not-run` because an earlier check failed. Both statuses are
  still not `OK`, so `--require` and the rollout planner refuse them as before.
- **An atomic update's rollback restores the vendored native libraries with
  the compiled core** (#44). The snapshot held only the package's `*.so`, while
  vibe-qc's `update.sh` wipes every `third_party/*/install` before a native-deps
  rebuild. A build that failed or was reaped after that point was "rolled back"
  to a core whose `libint2` and `libecpint` were gone: host_c2, host_e and
  host_d each reported a restored artifact beside a lane that no longer
  imported. Each `third_party/*/install` tree is now copied into the snapshot,
  and a tree the transaction changed is swapped back by rename before the
  post-rollback import check. An untouched tree is left alone. The copy costs
  about 300 MB of temporary space for vibe-qc.
- **An atomic update refuses to start from a checkout vq never installed**
  (#44). When the record's `last_installed_sha` and the checkout disagree
  (`installed_sha_matches_checkout` false, the state an update killed after its
  checkout leaves), the pre-update snapshot would capture an inconsistent pair
  and a failed build would restore it. `vq admin update` (and `--all`) now
  refuses with `precondition-failed` before any marker, pause, checkout or
  build, and names the remedy: rebuild at the checkout's own commit with
  `--expected-sha`, which the gate lets through because its rollback restores
  exactly the state it started from.

- **The vq pages for vibe-qc.com now match what vibe-qc publishes.**
  `docs/vibe-qc-site/` and vibe-qc's copy had diverged in both directions
  (vibe-queue#39). vibe-qc's integration corrected errors that this copy still
  carried: a CsCl cell labelled rocksalt, `--vibeqc-preflight` shown on a
  remote submit, vibe-qc API calls that vibe-qc does not accept, and output
  and timings nobody had re-run. This copy, in turn, had the `starved` fix that
  vibe-qc lacked. The maintainer chose this directory as the canonical copy.
  Each page is now vibe-qc `880b1ff`'s page plus that fix (vibe-qc#242), and
  nothing else. The README's do-not-copy warning is replaced by a straight-copy
  sync on each vq release.

- **The guard on those pages could not catch a renamed state in prose.**
  `tests/test_vibeqc_site_job_states.py`'s existence check kept only names
  already in `vq.spec.JobState`, then asserted that they were in it, so it
  could never fail. Independent verification on vibe-queue#39 renamed a state
  in `queue.md`, and in the tutorial's prose, and both passed. The check now
  collects every backticked name in a prose sentence about states, whether or
  not the enum has it. A new check also holds `queue.md`'s "are all terminal"
  list to every terminal state except `completed`, which is the omission
  vibe-qc#242 describes.
- **A dropped SSH session no longer kills a delegated venv build (#37).**
  `vq admin update ENV HOST` delegates to `ssh HOST ... vq admin update ENV
  localhost`, and the remote updater ran inside that ssh session. On
  2026-09-11 the release lane (`--tag v0.17.1 --expected-sha b4f6035e...`)
  lost builds on host_b, host_e and host_d that way. Those hosts run
  systemd-logind with `KillUserProcesses=yes`, so when the last ssh session
  ended logind stopped the session's scope and killed everything in it — the
  mechanism `203184e` established, and the reason a `setsid nohup` build dies
  there too. The atomic rollback never ran, and each host was left at v0.17.1
  with a marker reading `building`, a dead pid, a checkout already moved, and
  a venv whose `import vibeqc` failed on a half-built `libint2.so`. The
  driver, meanwhile, correctly reported that the outcome was unknown, which
  was true and of no help.

  The remote updater now starts outside the session. Where a systemd user
  manager answers it runs as a transient user service unit — the remedy
  validated on host_e — given the state roots and forwarded watchdogs
  explicitly, with a bearer token handed over through an owner-only file that
  is removed once the updater activates. Elsewhere it runs as a session of its
  own. The launch warns when the chosen mechanism cannot be trusted: lingering
  disabled, so the user manager itself stops at logout, or a Linux host with no
  user manager at all. The SSH call launches the updater, waits for it to
  activate, and returns; the driver then follows the build with short
  read-only polls of the new `vq admin observe-update`, which are replay-safe
  precisely because observing mutates nothing. A dropped connection costs one
  poll, and the driver re-attaches for up to 30 minutes, saying on stderr that
  the build is not the thing that broke.

  **Not yet validated on a logind host.** The systemd path is tested for what
  vq asks of systemd; that systemd then keeps the build alive across a dropped
  session is the real-host check requested on #37.

  The run id is minted on the driver *before* the launch, so a launch whose
  response goes missing is adopted by observation instead of being reported as
  unknown — the ambiguity is resolved by looking, not by guessing. What stays
  unknown is only what genuinely is: a run that died without publishing its
  terminal receipt, a host unreachable past the re-attach window, or an
  adopted launch that left no run on the target. Those keep the existing
  "outcome unknown … do not retry it yet" advice verbatim.

  `vq admin status HOST --json` tells the truth about a detached build:
  `in_flight` is true, and the marker carries a `detached_run_id` pointing at
  the run whose transcript and outcome explain it. The full build output stays
  on the host and comes back through `vq admin logs`; only phase narration is
  echoed to the driver, which is what the attached command showed anyway.

  A target whose vq predates the flag falls back to the attached path with a
  warning that names the risk — the tool that performs updates cannot require
  the update first. `VQ_ADMIN_NO_DETACH=1` forces the old behaviour for
  bisecting a transport problem.

  One deliberate difference: `--show-output`'s extra stderr copy of a failure
  tail no longer streams back live from a delegated update, because the
  remote's stderr is no longer this session's to read. The tail itself is
  unaffected — it is in the result the driver prints, in `admin-status.json`,
  and in full in the host's transcript.

  Scope: this covers the managed-venv lanes of `vq admin update`, which is
  where the incident happened. `vq admin auto-update HOST` delegates its own
  remote build through a different verb and now detaches the same way (see
  the #34 entry); the scheduler lanes were
  already detached under their own protocol, and `--with-driver-runtime` is
  excluded because it carries its own integrity-checked driver copy.

- **`test_teardown_reaps_a_paused_command_the_wrapper_already_forked` no
  longer flakes under load.** It checked that the fixture teardown had freed
  its SIGSTOPped command with `killpg(pgid, 0)` and accepted only `ESRCH`, so
  the macOS `EPERM` of a group whose last member is still being reaped failed
  it: 7 runs in 24 at load ~135 on 2026-09-13. The rate was the same with and
  without a second pytest session running beside it, so a lone failure while
  two full-suite runs overlap is load, not a collision between the runs. The
  test now probes the command's own `flock`, as `tests/test_terminal_reaping.py`
  does, and uses that module's liveness bound for its waits; it still fails
  when the teardown kills only the wrapper. No production code changed.
  Closes #35 and the two sites in this file listed in #29.

## [0.26.4] - 2026-09-13 - "Raymond's Bazaar"

Inherits v0.26.0's codename. Console users must log in again after the update;
stop old workers before starting new ones, and rotate the signing secret on
rollback. host_c's immutable publisher takes effect after the helper update;
existing legacy runtimes then use the normal verified recovery operation.
Source qualification alone does not establish fleet convergence.

### Added

- Fleet-console sessions now require a private persistent session record.
  Logout revokes copied cookies across workers and restarts; account changes
  invalidate existing sessions. Existing stateless cookies require a new login.
- Login admission is bounded per account, client and console before password
  hashing, with persistent counters and HTTP 429 retry guidance. HTTPS sessions
  use Secure cookies based on the server's trusted request scheme.
- Single-host bearer-token writes record correlated start/outcome entries in
  the fleet audit log. An unavailable start log refuses the operation; a failed
  outcome append is logged without suggesting that a completed write can be
  retried. Credentials and request query values are excluded from these records.

### Fixed

- host_c runtime publication preserves existing archives and full-SHA launchers.
  New archives carry full source and content identities, and launchers verify
  an embedded digest. Completed legacy deployments can be authenticated and
  reused without rebuilding or changing their published files; recovery still
  requires terminal Slurm evidence and a fresh runtime verifier. Publication
  no longer prunes generations referenced by queued launchers. (#17)

- **The paused-group reaping tests no longer flake under CPU contention.**
  `tests/test_terminal_reaping.py` ends each job's process group itself in a
  `finally`, and the group has usually just been `SIGKILL`ed by the daemon
  under test. A group between that signal and its last member leaving answers
  neither way: `killpg` raises `ESRCH` once the group is empty, and `EPERM`
  while a member is still tearing down — one already invisible to `ps`,
  reparented when its wrapper died and not yet reaped. That window is
  scheduling work, so it widens on a saturated host, and the cleanup only
  tolerated `ESRCH`; the `EPERM` surfaced as a `PermissionError` that failed
  the test it was meant to tidy up after, on 2026-09-12 inside a large batch
  and reproducibly at about one run in eight under synthetic load. A shared
  best-effort helper now tolerates both, as the fixture teardown behind it
  already did, and the waits for a reaped command to release its `flock` use
  the module's liveness bound rather than a five-second budget. No production
  code changed, and the tests still fail as they should when the reap they
  pin is reverted. Open: the same cleanup idiom in five other test modules
  (#29), and vq kill, pause and resume reading that teardown EPERM as another
  user's group (#27).

## [0.26.3] - 2026-09-12 - "Raymond's Bazaar"

Inherits v0.26.0's codename. The update and root-daemon inventory fixes take
effect after the administering vq installs this release; source qualification
alone does not establish fleet convergence.

### Fixed

- Same-SHA managed updates validate service, target, install-mode and request
  policy before returning `already-current`. Explicit rebuild/profile requests
  and missing declared extras run through the existing managed transaction;
  invalid or conflicting arguments are refused before it. A healthy current
  profile with no requested work remains a no-op. Local, delegated and
  driver-runtime updates share this policy, and generic explicit updater
  arguments are no longer skipped at the same SHA. (#11)
- Root-daemon inventory recognizes a nonexistent systemd unit when
  `systemctl show` omits `ExecStart`. Only the canonical unit's complete
  not-found, inactive, dead, PID-zero identity establishes absence; loaded,
  active, malformed and contradictory evidence remains deferred. This avoids
  false unknown applicability on single-user fleet hosts without changing
  their system configuration or services. (#16)

## [0.26.2] - 2026-09-12 - "Raymond's Bazaar"

Inherits v0.26.0's codename. The fleet ancestry fix takes effect after the
driver installs this release; source qualification alone does not establish
fleet convergence.

### Fixed

- Fleet rollout compares each component's live and accepted SHAs in the
  repository named by its report pin, in both ancestry directions. Dry runs,
  verification, execution and recovery now share this resolver; QC and viewer
  lanes no longer become ancestry-unknown merely because their commits are
  absent from the split queue repository. Legacy `/2` reports retain the
  configured monorepo mapping and original runtime fallback. Unknown or
  divergent histories, queue release drift and downgrade guards remain
  blocking. (#15)

## [0.26.1] - 2026-09-12 - "Raymond's Bazaar"

Inherits v0.26.0's codename. Scheduler result-integrity and fleet update
improvements take effect after hosts install this release; they do not repair
results overwritten by earlier jobs.

### Added

- **`extras` on a venv program, and a `vq web install` that refuses a vq it
  knows cannot serve the console.** A host whose vq-managed venv serves *both*
  the daemon and the web console could not be kept correct by vq, and
  example_hub had been in that state since 2026-09-09: its `vq-web.service`
  pointed at a venv with no uvicorn, so systemd restarted it 5230 times over
  two days, each start exiting on "vq web requires the 'web' extra", with the
  evidence only in the journal.

  `scripts/update.sh --extras web` in `update_script` is rejected, correctly,
  because a serving venv's install target must not come from a free-form
  command line; `vq web install` had no notion of extras and simply pointed
  the unit at the vq it was run with; and running pip inside a vq-managed
  checkout is forbidden on a fleet host.

  This is the declarative half of the capability #11 made changeable. #11's
  `--update-script-arg --recreate-venv --update-script-arg --extras
  --update-script-arg PROFILE` changes one environment once, which is the
  right tool for "fix this host now". A declaration says what the venv is
  *for*: it is re-applied on every managed rebuild, and it survives a venv
  rebuilt outside that path — a fresh provision, `install.sh`, `reinstall.sh`
  — where `.vq-install-metadata` dies with the venv it described.

  ```toml
  [programs.vibeqc-queue]
  extras = ["web"]          # what this venv is FOR, not what it last was
  ```

  The declaration is validated config, not free text — unknown names are
  rejected at load, where `pip install 'vq[wbe]'` would merely warn and
  install nothing — and `vq admin update` folds it into the recorded profile
  on the way to `update.sh`. It is a floor, never a ceiling: the effective
  profile is the smallest one containing both sides, so `["web"]` on a `dev`
  environment resolves to `all` rather than quietly dropping its test tooling
  on the next `--recreate-venv`. It is a floor under #11's per-update request
  too — but an operator asking for a profile that does *not* cover the
  declaration is refused rather than silently widened, because two
  instructions contradicting each other should not resolve into a third thing
  nobody typed. `--extras` stays rejected in `update_script`, and now names
  both supported routes instead of just refusing.

  It applies in the managed daemon transaction, which is the one path that has
  proved the updater is vq's own `scripts/update.sh` before naming an install
  target on it — an ordinary update runs whatever `update_script` configures,
  which for a vibe-qc or vibe-view program is a script that has never heard of
  `--extras`. An update that cannot apply a declaration says so, because
  silence there would rebuild the exact belief this fixes: config that looks
  applied and is not.

  Independently, `vq web install` now makes the same import `vq web run` makes
  before it writes anything, `--dry-run` included, and refuses with the remedy
  for the host it is actually on: the config key plus `vq admin update NAME`
  when this interpreter is a managed program (naming it, and saying "rebuild"
  rather than "edit" when the key is already there), `pip install -e '.[web]'`
  when it is not. An install that points a unit at a vq which cannot run it
  has all the information needed to say so, at the one moment the operator is
  still at the keyboard.

### Changed

- **A running job's command no longer outlives a wrapper killed by a signal.**
  The `vq.resource_receipt` wrapper leads a local job's process group, forks
  the command into it, and waits for that command before it writes the exit
  marker and the resource receipt. When the wrapper alone was killed while the
  job was `RUNNING` (`kill -9` on the pid `vq status` shows, or an OOM kill),
  the daemon recorded `FAILED` with the negative return code, or re-enqueued
  the job, and on macOS and on Linux without cgroups the command kept running
  under init. Its cpu and memory went back to the dispatcher while it ran, its
  outcome could no longer be recorded by anyone, and a retry started beside it
  in the same workspace.

  The daemon now sends `SIGCONT` and then `SIGKILL` to what is left of the
  group before it records the exit or the retry, as it already did for a
  paused job. On a host without a cgroup scope that is a semantics change: a
  command that might still have finished is killed.

  The alternative was to keep supervising the survivor as a reattached orphan,
  its capacity still reserved. That was rejected because the wrapper that
  writes the exit marker is the process that died, so the job could only have
  ended `ABORTED_BY_QUEUE` even where the command succeeded; an orphan that
  ends without a marker is not retried, so `--retry` would have quietly
  stopped applying to this failure; and `vq status` would have gone on
  reporting `RUNNING` against a pid the operator had just killed. A cgroup
  host already killed these survivors, since the `FAILED` path stops the job's
  scope, so this brings the other hosts into line rather than inventing a
  policy. `vq kill` and the watchdog record their terminal state under the
  spec lock before their `SIGTERM` can reach the wrapper, so neither reaches
  this reap: a killed job's surviving group is owed the grace that `SIGTERM`
  opened, and is held and escalated by the STATE-3 fix under Fixed below. A
  group this reap already ended as paused is not also held for a grace that
  has just been spent.

  Unchanged, and now pinned by its own test: a wrapper that exits normally has
  waited for its command, so on a host without cgroups a process the command
  left in the background still survives the job.

  The retry path also stops the attempt's cgroup scope now, as the `FAILED`
  path does. It used to return before that, so on a cgroup host a survivor
  that had escaped the process group ran on through the retry backoff, counted
  against nothing, until the retry's own dispatch found the leftover scope and
  stopped it.

  `tests/test_terminal_reaping.py::TestRunningGroupReap` kills the wrapper of
  a real dispatch with `SIGKILL` and with `SIGTERM`, down the `FAILED` and the
  retry exit, replacing the boundary test the paused fix left behind. Verified
  to discriminate: with the reap restricted to paused specs, exactly those
  three fail; with it widened to any live group, exactly the normal-exit
  boundary test fails; with the retry's scope reap removed, exactly its test
  fails. `docs/SPEC.md` § 4.4 now states the whole rule.

### Fixed

- **Scheduler terminal observations remain visible during artifact waits (#14).**
  Jobs waiting behind the two fetch workers now show `finishing`, with their
  ownership and capacity reservation intact until marker and final-fetch
  checks complete. Successful scheduler polls preserve explicit marker/fetch
  failure diagnostics; status distinguishes pending artifacts from completion.

- **Explicit retirement of hosts with retained rollout receipts (#13).**
  `[fleet.retired_hosts.HOST]` records a dated authorization reference and exact
  per-rollout evidence digests, printed by `vq host retirement-audit HOST`.
  Authenticated historical fences remain inspectable after removing the active
  host entry. Unknown outcomes and journal bytes remain intact; retired hosts
  are excluded from control/retry lists. Receipt hashes, committed-report
  authentication, live-host identity checks and durable-operation guards remain
  enforced. Active/retired overlap and mixed-group rewrites are rejected.

- **Managed profile changes and launchd service replacement (#11).** Admin
  updates recognize both `vq daemon run` and `python -m vq daemon run` launchd
  agents, with secure plist and serving-environment identity checks. Marked
  environments accept explicit `--recreate-venv --extras PROFILE` changes
  through the existing stopped-daemon transaction, preserving editable/copied
  mode and rejecting target/interpreter overrides. Configured `--editable` or
  `--copied` must agree with the recorded mode. Service installation waits for
  launchd unload and retries transient bootstrap I/O errors within 15 seconds;
  failures still prevent publication of a successful install marker.
  Matching configured modes are forwarded only once to the strict shell
  updater. Recovery accepts receipts emitted for either supported launchd
  command form, retaining exact venv, secure plist and command identity checks.

- **Fleet aggregation and freshness (#12).** A sweep shares one queue listing
  and one driver overview across scheduler aliases, avoiding repeated history
  reads and model conversion. Physical host totals exclude scheduler-owned
  reservations; scheduler totals retain effective pending/running phases and
  unconfirmed capacity. Distinct queue handles remain distinct. Snapshot age
  includes collection time, active slow refreshes are shown separately from
  failures, and manual/automatic sweeps share one admission guard.

- **A killed job that ignores `SIGTERM` is SIGKILLed again.** The STATE-3
  escalation stopped reaching any real local job the day jobs started running
  under the `vq.resource_receipt` wrapper. `vq kill` sends `SIGCONT` +
  `SIGTERM` to the whole process group and writes `KILLED`; the wrapper leads
  that group, installs no handlers, and so dies to its own `SIGTERM` at once.
  The daemon reaped it on the next pass, `_record_finish` preserved the
  terminal label, and `_reconcile_running` deleted the job — but
  `_escalate_if_killed` reads its deadline off the `_RunningJob` that had just
  been deleted, so the escalation it exists to perform could never arm, let
  alone fire. A command that ignores `SIGTERM` was left running in neither
  `_running` nor `_orphans`, with its cpu and memory handed back to the
  dispatcher and no one left to kill it. `_reap_scope` covers this on a host
  with cgroups; on macOS, and on Linux without cgroup delegation, nothing did.
  Confirmed on macOS on 2026-09-12.

  The watchdog's own `SIGTERM` -> grace -> `SIGKILL` had the same hole from
  the other side: `_record_finish` calls `watchdog.unregister` as soon as the
  wrapper is reaped, dropping the state that tracks the grace, so
  `OOM_KILLED`, `STARVED` and `TIME_EXCEEDED` lost their escalation too.

  A terminal job whose process group still answers after its wrapper is reaped
  now stays tracked as a `_TerminalSurvivor` and keeps its slot, cpus and
  memory charged against the host. One reconcile pass ends that record: the
  group goes and the budget is released, or the grace expires and vq sends
  `SIGCONT` + `SIGKILL`. Both killers reach it, since both write the terminal
  label the registration is keyed on, and a deadline `_escalate_if_killed` had
  already armed is carried over rather than restarted.

  Not an immediate `SIGKILL` at the reap: the grace the killer's `SIGTERM`
  opened is still running, and a command shutting down cleanly is owed the
  rest of it. Nothing waits for the group to go empty either — unreaped
  zombies answer `killpg(pgid, 0)` and a container's PID 1 need not reap
  them — so the `SIGKILL` is the last thing vq does for the job and the
  capacity is released in the same pass that sends it.

- **`--reconcile-legacy --dry-run` now binds every row it lists.** v0.26.1
  closed these comparisons on the mutating paths. The preview never ran that
  check at all, and it is the operator's only read-only look at a durable,
  fleet-wide recovery before they authorise it. On the driver (vq
  0.26.0 at `ede54c0`) it said

  ```
  retained holds that would be re-observed: v0.15.118-b9ea64e76214 host_a
  ```

  and `--reconcile-legacy` then refused that very receipt. The preview was
  telling the operator the opposite of what would happen.

  The four reconcile paths and the preview now authenticate and bind through
  one `_authenticate_journal_report`, so the preview refuses the same
  journals, in the same order, in the same words — a property a test pins by
  asserting both raise the identical message. Sharing the function is what
  makes that true rather than aspirational: the previous arrangement had the
  check written out where the preview could not reach it.

  A third test drives the migration's retained host_a hold against a **real**
  git checkout holding the report under `releases/` only, with nothing
  stubbed. Every sibling test rewrites a fixture's `source_path` and stubs
  discovery, so none of them proved that the real lookup and the binding rule
  agree; this one does, and it is the condition the driver actually ran into.

- Correct the release guide's obsolete first-cut instructions: `release`
  already exists and is protected, and `v0.26.0` is already published.
  Record #10's exact v0.26.1 candidate gates separately from the pending
  release cut. The version-output example now keeps the machine-readable
  prefix, and the tag-command example correctly quotes apostrophes in
  release codenames.

- **The scheduler lanes of `vq admin update` classify a held lock as
  `locked`, and a delegated update relays the driver's code.** `vq admin
  update HOST` (scheduler helper) and `vq admin update PROGRAM HOST`
  (scheduler runtime) answered `AdminUpdateInProgress` with a plain click
  error — exit 1 and "this checkout is being mutated by another admin
  operation; wait for it to finish, then retry" — while the env lane had
  classified the same condition as `locked` / 75 since the outcome contract
  landed. On 2026-09-11 13:30 UTC the host_f and host_c helper lanes, launched
  alongside a host_c2 `vibeqc-release` build, both returned exactly that,
  and a `contrib/fleet-sweep.sh`-style chain stopped where the sentence told
  it to wait.

  Both lanes now route `AdminUpdateInProgress`, `AdminMarkerPresent` and
  `AdminPreconditionFailed` through the classification the env lane uses,
  their `--json` success payloads carry `outcome` too, and a lane that ran
  but did not complete exits as `failed` without appending a second JSON
  object to the payload already on stdout.

  Two more gaps the audit of the contract's verbs found, both closed. A
  delegated update — `vq admin update host_f` run anywhere but host_f's driver —
  folded the driver's exit 75 into "remote vq failed (exit 75)" and exit 1,
  so the classification did not survive the SSH hop; the driver's 75, 76 or
  77 and, under `--json`, its `error` now come back unchanged, and every
  other remote failure reads exactly as before (`RemoteCommandError` carries
  the remote stdout for this). And `vq admin clear-update-marker`, the
  recovery step the contract points at, takes the same lock when it recovers
  a pause scope and answered it with exit 1; it reports `locked` now.

  The audit covered every verb that takes `admin_update_ownership()`. The
  contract names three — `vq admin update`, `vq admin status` and
  `vq admin clear-update-marker` — and all three are now right: `status` is a
  read that never takes the lock, and the other two are the fix above. The
  rest are outside the contract and deliberately left alone, recorded here so
  the next audit starts from a list rather than a grep: `vq admin install` and
  `vq admin reset-branch` render a held lock as a usage error (exit 2), and
  `vq self-update`, `vq admin recover-update` and the `--all-hosts` fan-out
  render it as an unclassified error (exit 1).

  The new tests in `tests/test_admin_outcomes.py` hold the real ownership
  lock from another thread — it is reentrant for its owner, so holding it in
  the test thread would let the verb straight through — and assert exit 75
  for both scheduler verbs; then the three refusal classes, the text form,
  the usage-error boundary, the success and failure payloads, the relayed
  codes on all three lanes, and `clear-update-marker`.

### Fixed: admin outcome tests work without a checkout virtualenv (#9)

The healthy-runtime fixture now uses the test interpreter, matching CI's
image-installed environment. It no longer treats a missing `.venv/bin/python`
as a supposedly healthy runtime. The missing-interpreter negative control
remains unchanged; production availability checks are unchanged.

### Fixed: node-scratch copy-back preserves persistent results (#8)

Scheduler jobs with node-local scratch used to copy the entire scratch tree
over the persistent workspace on exit. An unchanged historical result staged
as input could therefore replace a freshly computed `$VQ_WORKDIR` result;
stale staged logs could also replace live logs while the job reported success.

Copy-back now compares against a private seed copy, publishes only changed or
new outputs, and excludes wrapper-owned logs and `_vq` records. Independent
changes to the same path fail visibly and retain scratch output in a fetched
recovery directory. Copy errors fail rather than reporting successful output
publication. Array copy-backs serialize publication, and resource telemetry
still reports the payload's own status. Persistent `$VQ_WORKDIR` semantics
are unchanged; node-local disk now holds both the seed and working copy.

### Added

- **A machine-readable outcome for `vq admin update`, and an idempotent
  update.** An unattended orchestration could not choose between wait, skip,
  acknowledge and stop without parsing English, so every chain written during
  the post-split migration grew a line like

  ```sh
  grep -q "local checkout mutation lock" "$log" && { sleep 90; continue; }
  ```

  — load-bearing infrastructure spelled as a substring match on a sentence.
  (That sentence *was* pinned, by one assertion in `tests/test_self_update.py`,
  so vq's own CI would catch a reword. The protection ends at the repository
  boundary: an orchestration greping a log gets no signal and simply stops
  matching.) `ADMIN_OUTCOMES` is now a closed set of six values pinned by a
  test, each with an exit code: `ok`/`already-current` 0, `locked` 75
  (`EX_TEMPFAIL`, whose meaning is exactly "retry later"), `marker-present`
  76, `precondition-failed` 77, `failed` 1. `--json` carries `outcome` on
  success and on failure.

  `precondition-failed` is deliberately distinct from `failed`: "the build
  failed" invites a retry, while "this host is not converged" is a correct
  answer a retry cannot change and a force would defeat. Both were exit 1 plus
  prose.

  `vq admin update ENV --expected-sha SHA` on an already-deployed target now
  exits 0 with `already-current`, having taken no marker, paused nothing and
  built nothing — so a sweep can re-run safely instead of `awk`-ing the status
  table to decide whether to skip. "Already deployed" is not a SHA comparison:
  the commit must match, the tree must be clean, any requested tag must
  resolve to HEAD, **and** the program must pass its own availability probe,
  because a host at the right commit with a venv that cannot import is not
  converged and calling it so would skip it forever.

- **`vq admin update --acknowledge-failed-marker`.** Recovery from a failed
  update was read the marker, clear it interactively, re-run — three steps and
  a TTY, and a sweep has none of them. host_c2's `vibeview-dev` marker sat
  about five hours behind exactly that. The flag acknowledges an in-scope
  marker whose previous run *failed*, writes the same durable receipt
  `vq admin clear-update-marker` writes, and proceeds. It is not `--force`: a
  live or stale marker raises `precondition-failed` and is left alone, and it
  never touches a marker outside the update's scope.

  A delegated `vq admin clear-update-marker HOST` also no longer dies with a
  bare `Aborted!` when the far side has no TTY; it names `--yes`. Confirmation
  by pipe still works.

- **`vq admin status --json` answers the sequencing questions directly.**
  `in_flight` is true only while an operation's writer is demonstrably alive —
  a failed marker is something to acknowledge, not something to wait for — and
  `last_outcome` / `last_outcome_at` classify the most recently recorded
  operation. Their absence is why an orchestration ended up running
  `ps -eo command | grep -c "[n]inja"` to detect a finished build, in a loop
  whose `grep -c` exits 1 on a zero count and therefore spun for six hours
  after the build was done.

- **`docs/orchestration.md` and `contrib/fleet-sweep.sh`.** The contract —
  match on `outcome`, never on message text, and here is what is stable — plus
  a worked sweep that retries `locked`, acknowledges `marker-present`, skips
  `already-current` and stops on `precondition-failed` using only exit codes
  and `--json`. A test asserts the script greps no vq output and consults no
  process table, and that the exit codes it hardcodes are the ones vq emits.

  To prove the guarantee is real rather than aspirational, one message is
  reworded in this same release: the checkout-mutation-lock error, which is
  precisely the string the migration grepped for. The test that pinned that
  prose now asserts the classification instead.

- **The `update_script` lane is measured.** `vq admin update` now records how
  long the build script ran and parses `VQ-DEPLOY-METRIC` lines out of its
  output, the way the scheduler-runtime lane has since it landed.

  The asymmetry mattered: `SchedulerRuntimeUpdateResult` has carried
  dependency-cache decisions, ccache hit rates and per-phase durations for
  host_f and host_c, while `UpdateResult` — the lane where most of the fleet
  actually builds — carried no metrics and no duration at all. "Did that
  update take three minutes or seventy" had no answer short of reading
  timestamps out of a transcript, which makes every question about making
  updates faster unanswerable on the hosts that need it most.

  Durations render as `took 70m23s` rather than a float, because nobody reads
  4223.7 as seventy minutes. `None` means the script did not run, which is
  distinct from running instantly. A lane whose scripts emit no metrics grows
  no empty section — the absence is visible in that the block is missing,
  which is itself the finding: those hosts report no cache decision at all.

- **A config from a newer vq no longer takes an older host out entirely.**
  Unknown *top-level* keys are ignored with one `vq: warning:` line per
  process instead of failing the whole file, and a new top-level
  `min_vq_version = "X.Y.Z"` refuses the config on anything older, naming the
  version it needs.

  Found by adding `pin_source_repos` during the post-split migration. vq
  0.25.7 answered with

  ```
  pin_source_repos
    Extra inputs are not permitted [type=extra_forbidden]
  ```

  and died — the maintainer's `~/.local/bin/vq` still pointed at the pre-split
  runtime. On a fleet that is the same failure with worse consequences: a
  driver config a host's older vq cannot parse costs the host completely,
  rather than costing it one key it could not have used anyway.

  The tolerance deliberately stops at the top level. Inside a section a stray
  key is a typo, not version skew — `[notifications] webhook_urls` has one
  plausible meaning and silently disabling notifications is worse than a load
  error — and constructing `Config` in vq's own code stays strict too. What
  relaxes is reading a file *written by another vq*.

  `min_vq_version` is the other half, and the half that keeps the first one
  honest: an older vq cannot tell an additive key from a load-bearing one, so
  the author of the change says which it is. It is read out of the raw mapping
  before validation, so the refusal still works when every other key in the
  file postdates the vq reading it. `docs/version_compatibility.md` records
  the rule that follows — a top-level addition is free, an addition that must
  not be ignored raises the floor in the same commit.

- **Brand assets, and the docs site finally has a logo.** `docs/_static/logo/`
  holds a favicon, light and dark wordmarks, and a social preview card, wired
  into `conf.py` (`html_favicon`, `light_logo`, `dark_logo`) and into
  `docs/index.md`'s front matter for `og:image`. The sidebar showed plain text
  before.

  Hand-written SVG, not generated raster: the wordmark's lettering is drawn as
  geometric primitives, so it needs no font at render time. The glyph is
  vq's own subject, a queue of three tokens advancing along a rail, inside the
  same rounded unit-cell frame and the same teal vibe-qc uses, so the two read
  as one family. Tokens are solid with graded opacity rather than outlined,
  because at 16 px a 2 px stroke on a 5 px box collides with its neighbour and
  the row blurs into a blob.

  `tests/test_logo_assets.py` guards what would otherwise rot silently: the
  glyph is duplicated into four files, and the test fails naming the others
  when one is edited. Confirmed to fire before it was relied on.

- **`build_path_dirs`: extra PATH directories for a build, per host.** A
  remote command runs under a non-login shell, so anything a login profile
  puts on `PATH` is missing — on Arch/Manjaro that is `/usr/bin/core_perl`,
  and libecpint's vendored libcerf dies generating man pages with `pod2man`.
  vq has prepended the Arch perl directories to every `update_script` build
  since v0.11.0; this makes the list extensible from the config, so the next
  gap does not need a vq release and a fleet rollout. Absolute paths only,
  ahead of the built-in list, and directories absent on a given host are
  skipped, so one shared config stays safe across a mixed fleet.

  `docs/fleet_update_runbook.md` now records the decision and its scope: vq
  puts the directory on `PATH` rather than sourcing a login profile, because
  `bash -lc` would change how every remote command runs to obtain one
  directory; locating the tool inside the build recipe would change that
  recipe's hash and force a fleet-wide native rebuild; and provisioning the
  path per host needs root everywhere. It also states what the policy does
  *not* cover — a scheduler host's deployment command, whose environment is
  the remote shell's — so the next person does not have to rediscover the
  boundary.

  The end-to-end guarantee now has a test that runs a real build script and
  reads the `PATH` the script itself saw, rather than asserting on the
  helper's dict.

- **A `healthcheck_command` binary that exists only inside the venv is now
  flagged while it still works.** vq prepends the venv's `bin/` to `PATH` so a
  healthcheck can use the environment's own entry points. The failure mode is
  a binary that lives there and is installed by nothing: on host_b and host_e,
  `vibeview-dev`'s `xvfb-run` was a hand-written shim inside the old venv's
  `bin/`, present on no system path and reproduced by no reinstall, so
  migrating the venv broke every vibe-view healthcheck until it was copied
  across by hand.

  A successful healthcheck whose `argv[0]` resolves in the venv and nowhere on
  `PATH` now reports that beside its result — while it is still cheap to fix,
  rather than after the next reprovision. An `argv[0]` naming a path is not
  this case. The start-error message also names where vq looked, since
  `No such file or directory: 'xvfb-run'` on its own does not say that the
  venv was searched first.

- **`scheduler_runtime_source_repo` is now a deprecated alias, and a migrated
  driver stops losing features.** The split added `pin_source_repos` beside
  it, and the two then described overlapping things. Only
  `program_source_repo()` consulted both: source staging, deployment-tag
  resolution and auto-update tag discovery each read the old key alone, so a
  driver that had migrated fully to `pin_source_repos` silently lost all
  three, and `stage_source = true` was rejected outright with a message naming
  a key the operator had deliberately removed.

  Both spellings now resolve through one `Config.vibeqc_source_repo`, with
  `pin_source_repos` winning. Setting the old key warns once per process.
  Setting both to *different* paths is an error rather than a precedence rule
  — there is no reading of that config that is obviously right.

- **`vq daemon install` writes and refreshes the daemon's unit, like
  `vq web install` does for the console.** Nothing owned the daemon's unit, so
  across the fleet they disagreed: one host referenced `%h/.local/bin/vq`, two
  hardcoded a pre-split venv path. Repointing `~/.local/bin/vq` moved the first
  and left the other two running 0.25.7 after a restart, with their config and
  their symlink both looking correct. Only `vq doctor`'s `daemon_rpc` version
  revealed it.

  The generated unit points at the vq that ran the command and records that in
  a provenance marker, so "which vq owns this daemon" has one answer and it is
  on disk. It carries no capacity caps: those belong in `[daemon]`, which is
  what makes them survive a unit rewrite — and unit rewrites are exactly what
  has dropped them before. So if the unit being replaced hardcodes caps that
  `[daemon]` does not have, the install **refuses** and prints the config
  section to add; `--allow-dropping-unit-flags` proceeds once that is a
  decision rather than an accident.

  `vq web install`'s machinery is now shared rather than copied: a
  `ServiceKind` names the verb, unit name, marker and unit header, so both
  services get one implementation of the account checks, the atomic writes,
  the idempotent manager commands and `--dry-run`. The console's behaviour is
  unchanged, including its unit text.

  `vq daemon status` now names which vq installed the unit, and flags it when
  that is not the vq you are running — the same question `vq web status`
  answers for the console, in the place somebody checking on the daemon
  already looks. Until now the only thing that revealed this drift was
  `vq doctor`'s `daemon_rpc` version.

- **`vq admin install ENV [HOST]` provisions a checkout vq can then manage.**
  A host that did not yet have a program's `git_dir` could not be brought up
  through vq at all: `vq admin update` refuses (`git_dir ... is not a
  directory`) and nothing clones. Every host in the post-split migration
  therefore needed a manual `git clone` plus `scripts/install.sh` first — the
  one step that could not go through vibe-queue, and so the step most likely
  to be done inconsistently. It was.

  vq now clones the program's configured `upstream`, detaches at the exact
  commit, verifies HEAD against it rather than trusting the clone, optionally
  asserts `--tag`, and runs the program's own `install_script` under the same
  monitored-build supervision as an update — wall-clock and stall caps, the
  parallelism cap that keeps a cold native build from OOM-killing the box, and
  the build PATH policy. `--from-report` supplies the pin; otherwise
  `--expected-sha` is required, because a fleet host is provisioned at a pin
  and never at a moving branch tip.

  It refuses to write into an existing non-empty `git_dir` whether or not it
  is a checkout: that is somebody's work, and refreshing one is what `vq admin
  update` is for. An empty directory is fine. It runs under the same exclusive
  ownership and admin-update marker as an update, so the two cannot race, and
  the checkout refusal is re-checked under the marker. It does not drain or
  pause the queue — a program whose checkout does not exist has no jobs bound
  to it and no interpreter for a running job to hold open.

  Two additive program keys carry it: `upstream` (used only to *create*
  `git_dir`; an existing checkout keeps its own `origin` and vq never
  repoints it) and `install_script` (never run by `vq admin update`, just as
  `update_script` is never run by this).

- **`vq admin update ENV HOST --from-report` takes the pinned argv from the
  accepted report.** The report records the exact tail per pin — `release
  --tag v0.17.0 --expected-sha 6421ed34...`, `dev --expected-sha 6421ed34...`
  — and `rollout-latest` already deploys from it, but a single-host update
  made the operator retype it. Omitting `--tag` for `vibeqc-release` does not
  fail at the CLI; it fails inside the login-host preparer, several minutes
  in, with `vibeqc-release requires --tag`.

  vq now reads the newest accepted report, prints which one it read, and
  supplies the flags. An explicit `--expected-sha` or `--tag` that disagrees
  with the report is refused rather than silently overridden in either
  direction — that disagreement is exactly the mistake the option exists to
  prevent. It resolves on the driver, so a delegated host receives ordinary
  resolved flags and needs no report of its own.

- **`--reconcile-legacy --dry-run` previews the recovery instead of refusing.**
  The combination used to be rejected outright:

  ```
  --reconcile-legacy is a mutating recovery option and cannot be combined
  with read-only --dry-run or --verify-only
  ```

  So a durable, fleet-wide recovery that marks historical action outcomes
  permanently unknown had to be authorised blind — the operator could not see
  what it would rewrite. It now lists exactly which journal actions, obsolete
  scheduler claims, retained holds and failure transactions would be
  reconciled, and writes nothing.

  The preview is a separate read-only function, `inventory_legacy_rollout_
  state`, not a flag threaded through the mutating path: that path's
  `inspect_only` branches exist to make a read-only command *refuse* legacy
  state, and loosening them would weaken what a plain `--dry-run` depends on.
  It reaches the same three inventory primitives directly, observes durable
  operations with `recover=False` because recovery is a write, and raises on
  an orphaned or mismatched durable reference exactly where the mutating run
  would — a preview that hid those would be worse than none. A test asserts
  the journals are byte-identical afterwards, and another asserts the preview
  agrees with what the mutating pass finds (which caught a real disagreement
  while it was being written).

  `--verify-only` stays refused, and now says why: it answers a different
  question.

- **`--supersede-plan-hold` repeats.** One obsolete rollout can retain a hold
  per scheduler lane — host_f retains six — and the flag handled one pair per
  invocation, so clearing them took six near-identical invocations of a
  recovery-only command, each writing durable state. Repetition invites
  copy-paste error in exactly the place you least want it.

  The current plan is now built once and every other precondition is proved
  per pair, unchanged: they are correct, and they caught a genuine "host_f is
  not converged yet". A failing pair stops the run rather than degrading into
  a partial success, having printed the durable receipts already written; they
  are idempotent, so re-running after the cause is fixed replays and
  continues. Naming the same pair twice is a usage error. `--json` wraps
  several results in a `vq.fleet.plan_hold_supersede_batch/1` envelope; a
  single pair emits the same bare object it always has.

### Changed

- **`docs-build` no longer waits for the test stage.** It carried no `needs:`,
  so GitLab ordered it behind the whole suite. A Sphinx build has no
  dependency on pytest, so that was a false coupling costing about ten minutes
  of docs feedback on every pipeline. Worse, any unrelated test failure
  skipped `docs-build` and took `docs-deploy` with it, which blocked
  publishing a docs-only change across six consecutive pipelines while the
  rollout suite was being repaired. `docs-deploy` still needs `docs-build` and
  is still manual, so the human decision is unchanged; what is gone is a
  dependency that was never real.

- **Four forward codenames were used twice**, and now a test says so.
  `4e020b5` added names for v0.27.0, v0.28.0, v0.29.0 and v0.33.0 that each
  reused one already held by a patch release: *Lamport's Clock* (v0.7.1),
  *Chandy's Snapshot* (v0.8.16), *Postel's Robustness* (v0.7.17) and
  *Dijkstra's Semaphore* (v0.7.3). They came from a list checked against the
  minors, and the v0.7.x/v0.8.x lines named nearly every *patch*, so each name
  was free among the minors and taken anyway.

  Renamed to *Fidge's Timestamp*, *Mattern's Cut*, *Braden's Requirements* and
  *Erlang's Blocking*. Each keeps its predecessor's theme and changes only the
  person, so all four rendered images stay accurate and were renamed rather
  than regenerated: vector clocks are still logical ordering without a shared
  clock, Mattern's 1989 paper is the canonical consistent cut, RFC 1122 is
  where "liberal in what you accept" became a host requirement, and Erlang's
  blocking formula is N servers with arrivals turned away.

  `tests/test_codename.py::test_no_codename_is_used_twice` is the guard.
  It was written when the collision was found and deliberately held back,
  because landing it first would only have painted `main` red on a naming
  decision nobody had made.

- **The v0.33.0 and v0.27.0 artwork now depicts the codename it carries.**
  The renames in `e1088f1` reused the existing renders on the reasoning that
  each kept its predecessor's theme. That held for two of the four and not for
  these: v0.33.0's render left one permit standing free, which is availability
  rather than *blocking*, and v0.27.0 gave each token a single brass ring,
  which is a scalar logical clock -- Lamport's, the name the rename had just
  moved away from -- where Fidge means one counter per participant.

  Regenerated: three service bays all occupied with the queue behind unlit,
  and two detent rings per token at differing steps. `alt`, `caption`,
  `status` and the generation record follow the images in `prompts.json`,
  `docs/codenames.md` and `docs/version_compatibility.md`. (#6)

### Fixed

- **A split checkout no longer disagrees with its own journals.** The fix
  below closed five comparisons, and each failing check had been hiding the
  next. Driving a retained receipt and a v2 failure transition from a split
  checkout found 21 more report-path comparisons in `fleet_rollout` and 3 in
  `legacy_failure_transition`, plus one leak of the discovered spelling into
  a persisted forward intent. Before, a driver there stopped with
  `retained legacy action receipt has incoherent current report identity`,
  `historical report identity conflicts with journal`, or
  `failure forward intent source membership disagrees`.

  These were live, not waiting on the fleet repointing:
  `discover_latest_report` from a split checkout returns
  `releases/v0.17.0.json`. `runtime_repo()` said discovery finds nothing
  there, which was wrong, and has been corrected.

  Two changes. Every comparison of a report-path field now goes through
  `vq.report_paths.same_report`, which treats the two layouts' paths for one
  tag as one report and accepts every pair plain equality accepted. It lives
  in a new module with no vq imports, so the deliberately dependency-free
  transition module can use it; `fleet_release` re-exports the existing names.
  An AST test over both modules fails on the next `==` of such a field. And
  `_authenticate_failure_report`, once digest and rollout id are proven,
  returns the report under its journal's spelling, so the intents, backlinks
  and refs built from it agree with their sources. That second change is the
  one a guard cannot give: a path packed into a tuple or a set is compared
  without any `==` to catch.

- **The split's path spelling no longer un-binds a rollout from its own
  accepted report.** `b2aef94` fixed this on the supersede path. The same
  comparison was written out five more times, so it stayed broken everywhere
  else, and the migration hit it the same day: host_a's retained v0.15.118 full
  hold authenticated perfectly and was then rejected as not binding its own
  rollout, stopping `--reconcile-legacy`, `--verify-only` and `--dry-run`
  alike with no operator workaround, because the comparison happens inside vq.

  A journal persisted before the 2026-09-08 split records
  `vibe-queue/releases/<tag>.json`; vibe-queue's own repository holds the
  byte-identical blob at `releases/<tag>.json`. Discovery already searched
  both and authenticated on the digest — and then four reconcile paths
  (running action, failed rollout, obsolete scheduler claim, retained hold)
  threw away what it returned for being spelled differently.

  All five comparisons are now one function, `_historical_report_binds_run`,
  binding on the digest and the deterministic rollout id exactly as before. An
  AST test asserts that every comparison of a discovered report's path lives
  in that one function, so a sixth copy cannot be written silently.

  Two *receipts* carried the same defect one layer down, and there it was
  worse: they recorded the **discovered** spelling and were then validated
  against the **journal's**, so a driver running from a split checkout would
  write a retained-action receipt that its own next run refused — `has
  incoherent retained legacy action receipt`, a hard block on a journal vq had
  just poisoned itself. Both now record the journal's report identity, as
  every sibling receipt already did.

- **A probe that ran out of time no longer reads as a host that failed.** On
  2026-09-10 five of six supersede attempts on host_f refused with "lacks
  strictly healthy exact-target evidence" while the host was fine. The
  `scheduler_remote_vq` check makes three remote vq calls inside one 10 s
  budget; host_f's login node needs 1.6–2.5 s each, so `source-sha`
  intermittently timed out, the helper's live SHA went missing from that
  sweep, and the lane read as not converged.

  The planner said so in its own words one line above the bug —
  `detail = "live helper provenance probe unavailable"` immediately followed
  by `last_ok = False` — and `last_ok` has two values, so "we did not find
  out" had nowhere to go but "no".

  Three changes, and the middle one is the fix. The doctor payload now marks
  a transport timeout structurally (`timed_out: true`) rather than only in
  prose. `LaneState` carries `probe_unavailable` beside the verdict, rather
  than widening `last_ok` to three states that every reader would have to
  learn. And the supersede gate, when its refusal is caused by absent
  evidence rather than negative evidence, raises `FleetProbeUnavailable`,
  which classifies as **`locked` (exit 75, retry)** instead of
  `precondition-failed` (exit 77, stop).

  That last point corrects something shipped earlier in this same release:
  `docs/orchestration.md` told a caller to stop on exit 77, and the operator's
  actual remedy for this refusal was a bounded retry. The contract was not
  wrong — the refusal was overloaded, conflating a verdict with a missing
  measurement, which is the same indistinguishability the outcome work exists
  to remove, one level down.

- **`[fleet] check_timeout_seconds`.** The rollout sweep could not pass
  `--check-timeout` at all — `fleet_rollout.py` contained zero references to
  it — so every fleet got the 10 s default whatever its login node was like.
  A budget, not a delay: a healthy host answers well under it. Unconfigured,
  the sweep's argv is byte-identical to what it always was.
- **The `timed_out` mark never fired in production, and the helper probe now
  costs one round trip.** Two follow-ups to the entries above.
  `transport.run_remote_vq` raises its timeout `from None`, so a token-bearing
  ssh argv never rides the cause chain — and the doctor recognized a timeout
  by walking `__cause__` only, so host_f's actual failure, *"SOURCE-SHA check
  failed: remote vq timed out after 2.37s"*, carried no `timed_out` at all.
  The walk now follows `__context__` too: an outer-limited call surfaces as
  the doctor's structured deadline verdict, a plain inner stall carries
  `timed_out: true`, and the helper lane's detail names the subprobe, the
  budget it ran out of, and the remedy.

  And the check paid a remote round trip per identity question — three
  python start-ups on the login node, each a multiplexed ssh session that
  host_f's sshd was refusing under the concurrent sweeps. A new
  `vq source-identity` answers version, package digest and `SOURCE-SHA` as
  one JSON object; a helper newer than 0.26.0 costs one round trip instead of
  three, a helper at or before the pin is asked the legacy pair as before,
  and one the version gate misjudges answers "No such command" and falls
  back to it. The gate decides round trips, never the verdict: marker
  problems are fields of the answer, not exit codes, and a wrong SHA refuses
  exactly as before. The tests reproduce host_f's shape (2 s and 2.5 s per
  remote call, 1.3 s per driver probe) against a
  `[fleet] check_timeout_seconds = 30` sweep for a pin-era helper and against
  the doctor default for a one-round-trip helper, and pin the same shape
  under the doctor default as the recorded refusal.

- **The vibe-qc.com handoff tutorial omitted `starved` from the job
  lifecycle.** `docs/vibe-qc-site/tutorial/vq_queue_remote_job.md` presents a
  block under "The states you will see" and listed seven of the eight members
  of `vq.spec.TERMINAL_STATES`. Every state it showed was real, which is why
  it read as correct: a reader who handled all of them still missed one vq
  produces. `queue.md`'s terminal list was a true partial list but omitted
  `starved` and `interrupted`, both of which a script has to handle.

  Both fixed, and `tests/test_vibeqc_site_job_states.py` now enforces it. These
  pages are excluded from the Sphinx build, so nothing else here would have
  noticed them drifting from the code they describe.

  The pattern is `tests/test_admin_outcomes.py`'s documented-table check, with
  its three properties kept: rows are filtered by membership in the enum rather
  than by position, so the pages can be reordered or rewritten freely; the
  lifecycle block is asserted equal to `TERMINAL_STATES` in both directions,
  since "every documented state exists" would not have caught this omission;
  and the test was confirmed to discriminate by removing `starved` again, by
  renaming `oom_killed` to a state that does not exist, and by moving a real
  but non-terminal state into the outcome column. Extraction differs because
  that page carries a Markdown table and this one a fenced block.

  The outcome set is read from the block's own structure, as the token after
  each line's final `->`, rather than by scanning the block and subtracting a
  hand-written `{pending, running}`. The first version did the latter, which
  is this same defect one level up: a partial enumeration of the enum written
  out by hand, in the test meant to catch exactly that, where it would have
  gone stale the first time the lifecycle gained another pre-running state.
  Reading the column is also sharper, and is what catches the third case
  above: a state demoted out of `TERMINAL_STATES` but left in the outcome
  column still exists in `JobState`, so an existence check passes it.

  The cross-reference check derives its strings for the same reason. A literal
  `"vq.spec.JobState"` in a test asserting that a *document* names the enum
  survives the enum being renamed or moved: the note goes wrong and the
  assertion stays green, because it only ever asked whether some text appears,
  never whether it still names anything. It now derives the qualified name from
  the class, the page name from the path it guards, and its own filename from
  `__file__`. The note names the tutorial file explicitly so that middle one
  has something to match. The dividing line is whether a test *is* the pin: the
  ones that are should write values out longhand, and the ones checking
  something else against a contract must derive.

- **A broken `config.toml` printed a Python traceback for about half the
  verbs.** `config.ConfigError` covers the problems in a file the user owns
  and can fix — a TOML syntax error, an undefined `default_pool`, a
  `min_vq_version` floor this vq cannot meet. Around twenty call sites
  converted it to a click error by hand, so `vq doctor` and `vq status`
  answered with one `Error:` line, while `vq programs`, `vq submit` and
  `vq admin update` answered with fifteen lines of traceback and a pydantic
  dump.

  Inconsistent is the worse failure here, not the milder one. The same broken
  file reads as the user's mistake under one verb and as a vq crash under the
  next, and in the crash the reader cannot tell which half of the output is
  the explanation. It surfaced while landing `min_vq_version` above — a
  refusal whose entire point is a legible sentence, and which was legible
  only after the traceback.

  The root `click.Group` now catches `ConfigError` in `invoke()` and re-raises
  it as a `click.ClickException`: one message, exit 1, no traceback. click
  builds and invokes the whole subcommand chain inside that call, so nested
  groups (`vq admin update`) and parameter callbacks are covered as well, and
  so is any verb added later — which is the part a twenty-first hand-written
  guard would not have fixed. `ClickException` rather than `UsageError`
  because the command line was well-formed; printing the verb's usage block
  would point at the wrong thing.

  The per-verb guards are untouched. They convert the exception before it
  reaches the root frame, so a verb that already rendered cleanly keeps its
  exact wording and its exit code: `vq doctor` still answers with a usage
  block and exit 2. Sweeping 44 verbs against an invalid `default_pool`, 22
  changed — every one of them from a traceback to a single line — and the
  other 22 were byte-identical.

- **`update.sh` no longer converts an editable install to a copied one while
  reporting that it preserved the mode.** It documents *"By default the
  installed mode is preserved"* and read that mode only from
  `.vq-install-metadata` — vq's own note, written by `install.sh` and absent
  from any venv built the way `CONTRIBUTING.md` documents
  (`python -m venv .venv && .venv/bin/pip install -e '.[test,web]'`). With no
  note, the default fell through to *copied*.

  That is not a cosmetic downgrade. `fleet_release.runtime_repo()` resolves
  the controller checkout from where vq is imported; a copied install lands in
  site-packages, and `vq admin rollout-latest` then refuses on that host:

  ```
  vq runtime source .../.venv/lib/python3.14 is not a git checkout
  ```

  So a routine `vq admin update vibeqc-queue HOST` could disable rollout
  there, with no warning at any point. The mitigation until now was
  `--editable` in each host's `update_script`, a convention nothing enforced.

  PEP 610 `direct_url.json` — what pip itself wrote — is now the authority,
  with the note as fallback and copied only as a last resort. When the two
  disagree, pip wins and says so. An explicit `--copied` over an editable
  install still proceeds, because that is the operator's call, but it now
  warns and names the rollout consequence.

  `runtime_repo()`'s refusal says which problem it is, too: a path inside
  `lib/python3.14` tells an operator nothing about the thing they need to
  change, so the error now names the install mode and the fix.

- **A push-fed source mirror is checked before it is fed.**
  `feed_source_mirror` names a bare repository on the scheduler host that vq
  pushes into, but vq never created it and never checked it, and the
  convention it has to satisfy was written down nowhere: the preparer detects
  a push-fed mirror by whether its `origin` points at *its own path*, and a
  mirror made with `git init --bare` has no `origin` at all.

  So the feed succeeded and the preparation died several minutes later with
  `fatal: 'origin' does not appear to be a git repository` (rc=128), which
  reads as a problem with the source rather than with the setup.

  The update now probes the mirror before the fetch and the push, and refuses
  with the exact `ssh HOST 'git init --bare ... && git -C ... remote add
  origin ...'` remedy when it is missing, is not bare, is a working checkout,
  has no `origin`, or has an `origin` pointing somewhere else. It deliberately
  does not repair: an `origin` elsewhere is a *fetch-fed* mirror, a different
  valid arrangement that silently repointing would break, and creating a
  missing mirror would turn a typo in `feed_source_mirror` into a second empty
  mirror that pushes cleanly while the preparer keeps reading the real one.
  `docs/scheduler_runtime_deployment.md` now documents the convention where
  the setting is documented.

- **A failed scheduler preparation now says why.** `vq admin update PROGRAM
  HOST` captured the login-host `prepare_command`'s output into the run log
  only, so a failure rendered as

  ```
  -- deploy command (rc=None) --
  (no output)

  -- verification command (rc=None) --
  (no output)

  -- work errors --
     prepare command rc=2
  ```

  and `--show-output` added nothing, because it echoes the deploy and verify
  captures and neither command had run. Preparation happens *before* both, so
  that is precisely the case where they are empty and the prepare output is
  the only evidence there is.

  The result now carries `prepare_output`, the summary renders it as its own
  section ahead of the deploy section, the work error appends the last line of
  it, and `--show-output` echoes it. host_f failed this way twice during the
  post-split migration, `rc=2` and `rc=128`; both causes were one line of
  stderr — `vibeqc-release requires --tag` and `fatal: 'origin' does not
  appear to be a git repository` — and recovering either meant shelling into
  the login host and re-running the preparer by hand.

- A docstring in `src/vq/config.py` illustrated `[pin_source_repos]` with an
  absolute macOS home path under a lowercase username, which
  `tests/test_no_maintainer_paths.py` correctly rejects: the convention is
  `~/`, `/home/USER/` or `<vibe-queue-checkout>`. It uses `/home/USER/` now,
  absolute rather than `~/` because `fleet_release` builds these with
  `Path(path)` and no `expanduser()`, so a leading `~` would be taken
  literally.

  Worth noting how it got in: the pre-commit hook would have caught it, but
  `core.hooksPath` is per-clone local config, so a chat working in a clone
  that never ran the one-time setup does not have the gate. The always-runs
  test is the half that caught it.
- **The codename contiguity test read the minor field without its major, so
  the first `1.0.0` would have failed it.**
  `test_the_series_is_contiguous_from_its_first_minor` collected
  `int(v.split(".")[1])` from every `X.Y.0` key and asserted the result was one
  contiguous run. That holds only while every version is `0.x`: a `1.0.0`
  contributes minor `0`, which drags the expected range down to `0..N` and
  reports `1` through `6` as unnamed minor lines on the `0.x` series. A 1.0
  would have been blocked by a test that had nothing to say about it, and the
  diagnosis would have pointed at six releases that were never missing.

  Contiguity is now a property of a major line, checked through a
  `_contiguity_gaps` helper the tests drive directly with synthetic catalogues.
  The first major starts where the convention began at v0.7.0; every later
  major must start at `.0`, because `1.3.0` cannot ship without `1.0.0`.

  Verified to still discriminate rather than merely pass: adding a `1.0.0` to
  today's catalogue fails the old shape with a spurious `[1, 2, 3, 4, 5, 6]`
  and passes the new one, while dropping `0.20.0` still fails with `{0: [20]}`.

- **`test_daemon_scheduler.py` conflated liveness guards with timing
  assertions, and flaked under CI load.**
  `test_blocked_terminal_fetch_does_not_block_new_dispatch` failed twice in a
  row on a loaded shared runner, at two different deadlines, while passing
  locally in milliseconds. The file had fourteen `deadline = time.monotonic()
  + 1.0` spin-loop bounds whose value carried no meaning -- they exist so a
  hang fails instead of blocking the suite -- sitting alongside two
  assertions that genuinely test that a slow operation does not serialize a
  fast one. Both kinds were 1.0s, so raising one meant weakening the other.

  They are now named and separated: `_LIVENESS_SECONDS` (30s, generous, no
  semantics) and `_BLOCKING_SECONDS` (the 2s a deliberately-blocked mock
  stays blocked), with the real assertions comparing against half the
  blocking window. One of them also timed the wrong interval: it measured
  from before a liveness spin loop rather than over the dispatch call whose
  non-blocking-ness is the claim.

  Verified to still discriminate rather than merely pass: injecting a stall
  the length of the blocking window fails the assertion. (#5)

- **The first `docs-deploy` could not create its own subtree.** rsync creates
  only the final component of a destination path, and `/web/vibe-queue/` did
  not exist because vq had never published to vibe-qc.com before, so the
  first real deploy failed with
  `mkdir "/web/vibe-queue/docs" failed: No such file or directory`. Everything
  before that line worked: the protected-variable key decoded, the pinned host
  key was accepted under `StrictHostKeyChecking yes`, and Sphinx built clean.
  `--mkpath` now creates the missing parents.

  That flag removes the accidental safety net a plain rsync provided, where a
  typo'd `DEPLOY_PATH` failed instead of silently populating the wrong tree, so
  the job now asserts the destination is exactly `/web/vibe-queue/docs/` before
  anything with `--delete-after` runs.

- **A paused job's command no longer stays stopped forever after its wrapper
  dies.** A local job's process group is led by the `vq.resource_receipt`
  wrapper, which forks the command into the same group, and both `vq pause`
  and the host-pressure pause SIGSTOP the whole group. If the wrapper alone was
  SIGKILLed while the group was stopped (`kill -9` on the pid `vq status`
  shows, or an OOM kill), the daemon recorded the job FAILED with rc -9, or
  re-enqueued it when retries were left, and the command stayed stopped under
  init. Nothing reached it afterwards. A later host-pressure resume refuses a
  spec that is not `SUSPENDED`. A retry clears the pgid that the restart-time
  sweep for terminal survivors keys on. And `_reap_scope` acts only on cgroup
  and multi-user hosts, and not at all on the retry path. Reproduced on macOS
  on 2026-09-11: the command kept its memory on a host that had been paused
  precisely because memory ran short.

  When the daemon reaps a wrapper while the spec is `SUSPENDED` or still
  carries a pause intent, it now sends `SIGCONT` and then `SIGKILL` to what is
  left of the group. It does so before the retry path can clear the pgid, and
  with the same pair of signals the STATE-2 and STATE-3 reapers already send.

  Deliberately unchanged: on a host without cgroups, a command that outlives
  its wrapper while the job is `RUNNING` keeps running. It can still make
  progress, and killing it would change what happens to every job's leftover
  processes there. That is a separate decision, and a test marks it as this
  fix's boundary.

  New tests in `tests/test_terminal_reaping.py` drive the reported sequence
  through a real dispatch, down the FAILED exit, the retry exit and an
  unreconciled pause intent. All three fail at the parent with "the SIGSTOPped
  command outlived its wrapper". They probe liveness through an flock the
  command holds rather than `killpg(pgid, 0)`, which still answers for a zombie
  that a container's PID 1 has not reaped yet.

  Verified to still discriminate rather than merely pass: widening the check
  to any live group fails only the `RUNNING` boundary test, and dropping the
  pause-intent clause fails only the intent test.

## [0.26.0] - 2026-09-09 - "Raymond's Bazaar"

The release where vq became publishable. Everything below had been true of the
code for a while; what was missing was the licence text the package had been
declaring, and everything a reader outside the project needs in order to use
it, report a bug in it, or cut the next one.

### Added

- `LICENSE` — the MPL 2.0 text, byte-identical to vibe-qc's and vibe-view's.
  `pyproject.toml` had declared `license = "MPL-2.0"` since the split while
  shipping no licence text, so every wheel built from this tree carried a
  licence field that granted nothing. `license-files = ["LICENSE"]` now puts
  the text into the wheel's `.dist-info/licenses/`. (#1)
- `CONTRIBUTING.md` and `SECURITY.md`, adapted from vibe-view's so the
  contact surface and the PGP fingerprint stay identical across the toolset.
  `SECURITY.md`'s threat model is vq-specific: the product *is* remote
  execution, so the scope section is about whose command runs and as whom,
  not about whether input can cause execution. (#1)
- This changelog. (#1)
- `.githooks/pre-commit`, activated with
  `git config --local core.hooksPath .githooks`. It refuses staged additions
  containing absolute home paths, the maintainer's employer name, or private
  IPv4 literals outside the documented example prefixes. vq is the fleet's
  control plane, so real hosts, addresses and account names are the natural
  vocabulary of its docs and fixtures; they arrive by default rather than by
  accident. (#1, #2)
- **`docs/release_process.md`.** Project 36 has only `main`; `release` does
  not exist and is created by the first proper cut, fast-forwarded to a tag
  and never written any other way. Adapted from vibe-qc's, dropping the
  tiered test gate it needs for an expensive native build that vq does not
  have.

  Two findings the procedure had to account for. `v0.25.7` **already exists
  as a tag** on project 36: annotated `first commit of the split-out
  repository`, pointing at `4899089`, carrying no codename and proved by no
  pipeline. It is a split marker, cannot be reused, and must not be moved, so
  the first codenamed release starts at the next version. And the ancestry
  reconcile has to run *before* tagging, not after: a `release` tip one
  commit ahead of its tag forced an entire fleet rollout onto
  `--expected-sha` fallbacks in the 2026-07-24 cycle.
- **A documentation site.** `docs/` held roughly 1.5 MB of operational
  material with no generator and no entry point. It is now Sphinx + furo,
  published under `https://vibe-qc.com/vibe-queue/docs/`, and split by the
  three audiences the corpus had been mixing: a **user** submitting jobs, an
  **operator** running a host, and an **agent** following a machine-readable
  contract. `docs/index.md` routes to one of three landing pages rather than
  dropping a reader into a flat file list.

  The pages stay where they are, so no cross-reference in 1.5 MB of prose
  breaks; the landing pages group them. Working memory is excluded from the
  published site rather than deleted: `STATUS.md`, `roadmap.md` (444 KB), the
  handovers, per-cycle audits, superseded design notes, and the fleet
  runbooks, which name real hosts and have no external audience.

  The user section is the thin one, because almost no user-facing
  documentation existed. `docs/user/index.md` is therefore a written
  getting-started guide rather than a list of links.
- **`docs-build` and `docs-deploy` CI.** `docs-build` runs on `main`, merge
  requests and release candidates, with `-W --keep-going`: the tree builds
  clean today, so a new warning is a real regression. `docs-deploy` rsyncs
  `--delete-after` into `/web/vibe-queue/docs/` and nothing above it, from a
  pinned `known_hosts` with `StrictHostKeyChecking yes`.

  `docs-deploy` is **manual and must not be run yet**: the vibe-qc marketing
  deploy has to carry `--exclude='/vibe-queue/'` first, or it will
  `--delete-after` this subtree out of existence. The job also fails with a
  real diagnosis when `DEPLOY_SSH_KEY_B64` is empty, which is what an
  unprotected ref produces; that presented as `error in libcrypto` and cost
  vibe-qc a debugging session.
- **The codename series has a catalog and a runtime surface.**
  `src/vq/codename.py` holds `RELEASE_CODENAMES` (64 entries, v0.7.0 to
  v0.25.0) plus `codename_for_version()` and `format_version()`, with
  vibe-qc's inheritance contract: an explicit `X.Y.Z` wins, an unlisted patch
  inherits its parent minor, and a PEP 440 pre-release resolves to the
  release it heads toward. `vq --version` now prints
  `vq 0.25.7 "Härder's Atomicity"`; it surfaced no codename before.
  `tests/test_codename.py` fails the suite if a version ships unnamed or the
  minor sequence develops a gap. Thirteen entries are the v0.13.0 to v0.25.0
  backfill, reconstructed from the monorepo's own version-bump commits;
  those and the v0.11.0 / v0.12.0 resolutions were approved on 2026-09-09.
- `.release-status/CODENAME-PROPOSAL.md` and
  `.release-status/IMAGE-BRIEF-codename-series.md`, plus the generation
  manifest at `docs/_static/images-codenames/prompts.json`: nineteen
  paste-ready artwork prompts, one per minor. vq's artwork departs from
  vibe-qc's animal-and-lattice house style because its codenames name
  concepts rather than animals; the concept is the subject, and a shared
  queue substrate makes it a series.
- `tests/test_no_maintainer_paths.py` — the always-runs half of the privacy
  guard. The hook is opt-in and git skips a missing hooks directory silently,
  so a hook alone can go inert without anyone noticing; this re-checks every
  tracked file in CI, and asserts that the hook's two allowlists and the
  test's copies have not drifted. It cannot check host names: `docs/hosts.md`
  explains why that part stays a review responsibility. (#2)
- Post-split release composition: a release is **anchored** on vibe-qc's tag,
  while the `vq` and `vibe_view` pins resolve to their own repositories'
  newest release tags and are evidenced by their own projects' pipelines.
  `sibling_pin()` reads each component's own `pyproject.toml` at its own tag;
  `--vq-repo` / `--vibe-view-repo` supply the checkouts and the script refuses
  a split checkout without them. Gate names follow the schema: `/2` reports
  keep validating against the monorepo job names, because the new ones would
  reject the fleet's entire history.
- Release report schema `/3`: `repo` and `project_id` move onto each pin, so
  a release can pin four components living in three GitLab projects.
  `PIN_SOURCES` is defined identically in the reader and the generator, with
  a test asserting the two never drift. `parse_report` still accepts `/2`,
  and `REPORT_DIRECTORIES` accepts both `vibe-queue/releases/` and
  `releases/` on read, so vq can still read its own pre-split history.
- A CI pipeline of its own (`ruff`, `test`), running on `main`, on merge
  requests and on `release-candidate/*` branches. The monorepo ran CI once
  per release because its native build is expensive; vq is pure Python and
  its whole suite is cheap.

### Fixed

- **`vq --version` stopped being machine-parseable when the codename was
  added.** `doctor._scheduler_remote_vq_check` identifies a scheduler host's
  helper by running `vq --version` on it and matching
  `\bversion\s+([^\s,;]+)`; dropping click's `, version ` infix made that
  return `None` on every scheduler host. Silent, because the check reports on
  the return code and the SOURCE-SHA, not on whether the version parsed, and
  8285 tests passed over it. It also breaks *older* drivers reading a *newer*
  helper, which is the normal state of a fleet mid-rollout. The line is
  `vq, version 0.25.7 "Härder's Atomicity"` now: click's default prefix
  byte-for-byte, codename appended. Two tests pin it. (#4)

- **`README.md` was still monorepo-shaped.** Every path in it pointed at
  `./vibe-queue/scripts/...` or `vibe-queue/.venv/...`, the documentation
  links resolved to `../docs/...` outside the tree, and the History section
  said "begins at the commit below" without naming one. With that fixed,
  `readme = "README.md"` is back in `[project]`, so the wheel carries a long
  description again; verified against a real build
  (`Description-Content-Type: text/markdown`).
- **`docs/hosts.md` published the fleet's attack surface.** In one table: both
  compute hosts' public host names and external SSH ports, their static LAN
  addresses, the WAN address of the router in front of them, the `fail2ban`
  `ignoreip` range (the netblock exempt from brute-force banning), login
  account names and SSH identity-file names. The filled-in inventory moved to
  the private `example-org/scripts` project; what remains here describes the *shape*
  of a host record, for the operator audience, and says where the real one
  lives. Swept in the same pass: `docs/remote-access.md`'s worked example is
  now `compute.example.com` / `myuser` throughout, the example SSH port in
  `docs/config.toml.example`, `contrib/sshd_config.d/vq-hardening.conf` and
  two test fixtures is no longer the real one, and a WireGuard overlay's host
  addresses, a retired host's public-hostname-and-port pair, and an
  institutional cluster's domain came out of `HANDOVER_FLEET.md` and
  `docs/fleet_update_runbook.md`. (#2)
- **`src/vq/scheduler_dispatch.py` and two test fixtures named a real
  third-party cluster.** The Torque truncated-handle examples carried an
  academic cluster's FQDN, which leaks somebody else's infrastructure rather
  than ours. They are `pbs.cluster.example` now; the 16-character truncation
  and the listed-id-is-a-prefix relation the test exercises are unchanged. (#2)
- **`fleet_release` report discovery** searched only the hardcoded
  monorepo-relative `vibe-queue/releases/`, so run from this repository it
  found nothing. It now enumerates both split-transition layouts, without
  deriving them from the running vq's own install layout, since the repository
  being searched need not be the one vq runs from. Rejection reporting is
  capped at the newest three plus a count; it previously concatenated 100+
  multi-line git errors into a ~40 000-character message.
- **vibe-queue is its own repository root.** `VQ_REPO_ROOT` was derived one
  level above the project directory, which in the monorepo was the shared
  checkout root. In this repository that path points outside the tree, so
  every lifecycle script failed at source time. `scripts/` now carries its
  own `_lifecycle_lock.sh`; the cross-component lock only ever mattered when
  several components shared one checkout and one venv target.
- **`runtime_repo()` detects its layout** instead of walking a fixed
  `parents[3]`. vq installed from this repository finds its root at
  `parents[2]`; vq installed from the monorepo, which is what the fleet still
  runs, finds it at `parents[3]`. A wrong guess raised "not a git checkout"
  on every host.
- **Both cluster stagers built the viewer out of a vibe-qc clone.** Correct
  while vibe-view was a monorepo directory, wrong now: they would have staged
  whatever viewer sat in the vibe-qc tree at the pinned SHA. host_f now chooses
  the mirror and clone URL per program; host_c detects the staged layout
  rather than asserting it, so hosts staged the old way keep working.
- **294 `.pyc` files were tracked**, and the repository had no `.gitignore`.
  Because they were tracked *and* regenerated by every test run, the working
  tree was permanently dirty and `git pull --rebase` refused outright, which
  blocks the mandatory rebase-then-push cycle for every chat working here.
- Tests that carried their own copy of the monorepo layout: off-by-one repo
  roots in the lifecycle-lock and multi-user lanes, and five suites reaching
  for artifacts that were not carried into the split, which now skip with a
  reason instead of erroring.
- `ruff` findings introduced by the split fixes (`I001` import ordering,
  `UP037` a quoted annotation under `from __future__ import annotations`).

### Documentation

- `HANDOVER_FLEET_SPLIT_MIGRATION.md`: the state of the migration off the
  frozen `example-org/vibeqc` monorepo, correcting four claims in the originating
  brief that did not survive contact with the fleet, and recording that the
  bugctl cutover *inverts* rather than removes the provenance failure.
- The host_b non-login PATH wall (`pod2man` lives in `/usr/bin/core_perl`,
  which only a login shell puts on `PATH`, so `vq admin update host_b` cannot
  rebuild native dependencies) and the mace-runtime Python 3.13 pin.

### Known issues

- Known-red lanes, pre-existing and unrelated to any change above:
  `test_scheduler_admin_update` (x2, CI); `test_admin_lifecycle_transaction`
  and `test_multi_user_refresh_helper` (x2, local only).

[Unreleased]: https://github.com/vibe-qc/vibe-queue/compare/v0.26.1...main
[0.26.1]: https://github.com/vibe-qc/vibe-queue/releases/tag/v0.26.1
[0.26.0]: https://github.com/vibe-qc/vibe-queue/releases/tag/v0.26.0
