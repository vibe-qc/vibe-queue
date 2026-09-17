# Changelog

All notable changes to vq are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

This file starts at the first public commit, `4899089` (2026-09-08).
Development before that point took place in a private monorepo whose history
was deliberately not transferred — see the [README](README.md#history). vq's
version number does not, therefore, restart: the split inherited `0.25.7`,
which is the version the fleet was already running.

> **This is a shared ledger.** Several chats append to it. When a rebase
> conflicts here, resolve to the **union** of both sides — never
> `--ours`/`--theirs`. See `AGENTS.md`, "Shared files and shared trees".

## [Unreleased]

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
  the private `mpei/scripts` project; what remains here describes the *shape*
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
  frozen `mpei/vibeqc` monorepo, correcting four claims in the originating
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
