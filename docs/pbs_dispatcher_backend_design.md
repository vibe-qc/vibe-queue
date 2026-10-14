# PBS / SGE dispatcher backend — design proposal

**History: a design proposal from June 2026, written at v0.9.2 and kept for its
rationale.** The backend it proposes has shipped, with Torque/PBS and Slurm
dialects, and this page was not brought along. Its status line below is a
snapshot, not the state of the product. Do not use it to configure a scheduler
host: the operator reference is [operations](operations.md) and
[scheduler runtime deployment](scheduler_runtime_deployment.md), and what vq
guarantees is [SPEC § 8.2](SPEC.md#82-scheduler-backends).

The design uses an off-cluster driver and SSH to submit, monitor and retrieve
scheduler jobs. Scheduler dialects and site policies are selected through
configuration and verified with `vq scheduler-probe`. Deployment inventories,
acceptance receipts and current rollout status belong in private operator records.

---

## 1. Why now (roadmap fit)

We are on **v0.9.2** (*Kleinrock's Queue*). [`SPEC.md`](SPEC.md) §8.2 frames the
external-scheduler backend as the **v1.0 decision point**:

> "Decision point: is the workload now multi-host enough to need SLURM under the
> hood? If yes, SLURM backend ships." (SPEC §785)
> "every architectural decision through v0.9 must preserve the option to swap the
> dispatcher … without rewriting the rest of vq." (SPEC §568)

pbs-cluster is that workload trigger. The only deviation from the letter of the SPEC is
**dialect**: pbs-cluster uses `qsub` (PBS/Torque or SGE), not SLURM `sbatch`. The
abstraction is identical; only the adapter strings differ. We design the seam to
be dialect-pluggable so SLURM remains a drop-in third dialect later.

## 2. Deployment constraints

| Constraint to check | Consequence for vq |
|---|---|
| SSH gateway requirements | Configure `ProxyJump` or `ProxyCommand` in the operator's SSH configuration. |
| Network access policy | Use SSH transport; qualify source and dependency access before provisioning. |
| Scheduler and login-node policy | Submit compute through the configured scheduler; do not run workloads on login nodes. |

## 3. The current dispatcher reality (the seam has eroded)

SPEC §8.1 claims *"The dispatcher is already isolated in `daemon.dispatch_one` …
none of it should leak `Popen` semantics outside that one function."* **That is no
longer true.** `dispatch_one` doesn't exist; the local-process model is woven
through, with `Popen`/`pgid`/`killpg`/`/proc` semantics in at least:

- [`daemon.py:2397` `_start_job`](../src/vq/daemon.py) — workspace mkdir, exit-marker
  bash shim, cgroup/systemd-run wrap, multi-user priv-drop + chown, workdir +
  `VQ_WORKDIR`, the PENDING→RUNNING claim under `spec_lock` with the *Peterson's
  Lock* kill-race handling, and finally `subprocess.Popen(start_new_session=True)`
  + pid/pgid/pid_start_time capture + the post-Popen kill-race window.
- [`daemon.py:1407` `_reconcile_running`](../src/vq/daemon.py) — `rj.popen.poll()`.
- [`daemon.py:1425` `_escalate_if_killed`](../src/vq/daemon.py) — `killpg(SIGKILL)`.
- [`daemon.py:1771` `_watchdog_pass`](../src/vq/daemon.py) — `/proc` sampling + `killpg`.
- [`daemon.py:1121` `_reconcile_orphans`](../src/vq/daemon.py) +
  [`:829` `_reattach_or_interrupt_at_startup`](../src/vq/daemon.py) — `killpg(pgid,0)`
  liveness + exit-marker recovery.

**Implication:** the backend's hardest, most safety-critical sub-task is
**Increment 1 — re-establish the seam** (extract a `Dispatcher` protocol, move the
existing behavior behind a `LocalDispatcher` with *zero behavior change*). All the
race-condition discipline (Dekker/Peterson/Thompson reaper, orphan reattach) must
be preserved exactly. This is dialect- and architecture-independent and is the
foundation for everything else.

## 4. Two candidate architectures (forks on one pbs-cluster fact)

The whole build forks on: **does pbs-cluster's login node permit a persistent user
daemon** (`loginctl enable-linger`, or a tolerated `nohup`/`systemd --user`
process that only polls `qstat` — no compute)?

### Arch 1 — daemon-on-cluster (SPEC-faithful, simpler)
A vq daemon runs on pbs-cluster's login node. Its dispatcher is the `SchedulerDispatcher`:
`_start_job` writes a job script + `qsub` (local); reconcile polls `qstat` (local);
kill is `qdel`. The workspace lives on pbs-cluster's shared FS (home/scratch), visible to
compute nodes automatically. This is exactly SPEC §8.2 ("the dispatcher calls
`sbatch`; a reconciler replaces `subprocess.poll()` with `squeue`/`sacct`; state
dir on shared FS"). Existing `vq submit pbs-cluster …` (scp + `vq submit localhost`) works
unchanged — only the *dispatcher* the pbs-cluster daemon uses changes.

- **Pros:** matches SPEC; minimal new transport; single source of truth (pbs-cluster's
  daemon owns specs next to the qsub jobs); compute-node FS visibility is free.
- **Cons:** needs a persistent login-node process. Needs vq installed on pbs-cluster.

### Arch 2 — off-cluster, SSH-driven, stateless-on-pbs-cluster (robust, novel)
The vq daemon stays on **compute-d**. For `host=pbs-cluster scheduler=pbs`, the dispatcher
does everything over SSH: stage workspace to pbs-cluster scratch (rsync/scp) → `ssh pbs-cluster
qsub` → reconcile by `ssh pbs-cluster qstat` → stage results back → `qdel` to kill. pbs-cluster
holds **no persistent process** (only the PBS job itself, which is what the cluster
wants).

- **Pros:** robust to any login-node policy; no vq install needed on pbs-cluster; keeps
  pbs-cluster a "guest" cluster we don't control. Best fit for the stated constraints.
- **Cons:** novel (SPEC doesn't cover it); split brain (spec/state on compute-d,
  workspace on pbs-cluster) needs care; compute-d now needs outbound SSH to pbs-cluster
  (compute-d → `<cluster-gateway>` → pbs-cluster ProxyJump + key); N concurrent SSH `qstat` pollers
  (mitigated by one batched `qstat` per poll, not per-job).

**Recommendation:** **Arch 1 if persistence is allowed** (it's the SPEC design and
far less code/risk), **else Arch 2.** Most group clusters tolerate a
linger'd user process that only polls `qstat`; we should confirm before committing.
The `Dispatcher` protocol (Increment 1) is identical either way — only the concrete
`SchedulerDispatcher` and staging differ — so Increment 1 proceeds regardless.

> **Decided 2026-06-05: Arch 2.** pbs-cluster's login node will not host a persistent
> vq daemon. The daemon stays on compute-d and drives `qsub`/`qstat`/`qdel` over
> SSH (ProxyJump through `<cluster-gateway>`); pbs-cluster keeps zero persistent
> footprint. Increment 2 onward targets this model.

## 5. The `Dispatcher` protocol (Increment 1)

```python
class Dispatcher(Protocol):
    def start(self, spec: JobSpec, ctx: DispatchContext) -> DispatchHandle: ...
    def poll(self, handle: DispatchHandle) -> DispatchStatus: ...   # RUNNING | exited(rc) | gone
    def signal(self, handle: DispatchHandle, sig: KillKind) -> None: ...  # TERM/KILL
    def reattach(self, spec: JobSpec) -> DispatchHandle | None: ...  # startup recovery
```

- `LocalDispatcher` wraps today's `Popen`/`pgid`/`killpg`/`/proc` exactly.
- `SchedulerDispatcher(dialect)` wraps `qsub`/`qstat`/`qdel`; the handle carries the
  scheduler job-id (the analogue of pid/pgid). The exit-marker file already written
  by the bash shim ([`daemon.py:2423`](../src/vq/daemon.py)) survives across
  schedulers — read it from the (staged-back) workspace as the rc source of truth,
  with `qstat -f exit_status` / `qacct` as the fallback.

## 6. qstat state → JobState mapping (dialect matrix)

| vq `JobState` | Torque/PBS `qstat` | PBS Pro | SGE `qstat` |
|---|---|---|---|
| PENDING (queued) | `Q`, `H`, `W` | `Q`, `H` | `qw`, `hqw` |
| RUNNING | `R`, `E` | `R`, `E` | `r`, `t`, `Rr` |
| (gone → read exit marker / `exit_status`) | `C`, absent | `F`, absent | absent, `Eqw` |
| TIME_EXCEEDED | walltime-exceeded substate | likewise | `Eqw`+limit |

Terminal classification reuses the existing `_record_finish` rc logic; the
scheduler only tells us *when* the job left the queue, the exit marker tells us the
*rc*. Watchdog-attributed states (OOM/STARVED/TIME_EXCEEDED) come from the
scheduler's own accounting when available, else stay COMPLETED/FAILED by rc.

## 7. Resource → qsub flag mapping (dialect matrix)

| JobSpec | Torque | PBS Pro | SGE |
|---|---|---|---|
| `cpus` | `-l nodes=1:ppn=N` | `-l select=1:ncpus=N` | `-pe smp N` |
| `mem_mb` | `-l mem=Nmb` (or `pmem`) | `-l select=…:mem=Nmb` | `-l h_vmem=NM` |
| `wall_time_seconds` | `-l walltime=HH:MM:SS` | `-l walltime=HH:MM:SS` | `-l h_rt=HH:MM:SS` |
| array | `-t 0-(N-1)` | `-J 0-(N-1)` | `-t 1-N` |
| name | `-N <job_name>` | `-N` | `-N` |

These differ enough that a wrong guess produces silently mis-resourced jobs, so the
dialect is **explicit config + runtime-probed** (next section), never assumed.

## 8. Dialect detection — `vq scheduler-probe <host>`

A new diagnostic that SSHes in and reports `qsub`/`qstat` flavor + version
(`qstat --version`, `pbsnodes`/`qhost` presence, `$PBS_VERSION`), so first-contact
turns the unknown scheduler into a *detected* one rather than a build-time guess.
Output feeds the `scheduler =` / `scheduler_dialect =` config below.

## 9. Config schema (additive to `HostConfig`)

```toml
[hosts.pbs-cluster]
ssh = "pbs-cluster"                      # ~/.ssh/config alias (ProxyJump handles the 2 hops)
scheduler = "pbs"                 # NEW: "local" (default) | "pbs" | "sge" | "slurm" (later)
scheduler_dialect = "torque"      # NEW: "torque" | "pbspro" | "sge" — set from the probe
remote_vq = "/path/to/.venv/bin/vq"   # Arch 1 only
submit_extra = ["-q", "<compute-queue>", "-A", "<account>"]  # NEW: site flags (queue, account)
scratch_root = "/scratch/USER"    # NEW: where staged workspaces + $VQ_WORKDIR live
scheduler_prologue = ["module purge", "source /home/USER/cluster-env.sh"]
scheduler_epilogue = ["rm -f scratch.tmp"]
scheduler_update_command = "/home/USER/vibeqc-dev/scripts/update_cluster.sh"
scheduler_update_host = "cluster-build"  # optional: SSH target that can compile
scheduler_install_command = "/home/USER/vibeqc-dev/scripts/install_cluster.sh"
# Arch 2 only:
remote_scheduler_host = "pbs-cluster"    # the host the compute-d daemon SSHes to for qsub/qstat
```

Default `scheduler = "local"` ⇒ every existing host is unchanged; the seam is
inert until a host opts in. This is the §14 gating mechanism.

## 10. Watchdog deferral

Under a scheduler dialect the daemon's `/proc`-sampling watchdog **degrades to
telemetry-only** (SPEC §579): the scheduler owns cgroup/walltime enforcement, so vq
records observed usage but does not `killpg`. `wall_time_seconds` is enforced by
passing `-l walltime=` to qsub, not by vq's timer.

## 11. Provisioning pbs-cluster

vibe-qc's native deps (libint, libxc, FFTW3, Eigen, spglib, libecpint) make a
from-source build fight the cluster module system. SPEC §639 already names the HPC
answer: **Apptainer** (pbs-cluster can pull base images). Recommended: build/pull an
Apptainer image and run vibe-qc inside it; fall back to `module load` +
source build only if Apptainer is unavailable. `$VQ_WORKDIR`-equivalent and staged
workspaces go on cluster **scratch**, never `$HOME`. (pbs-cluster is a separately-owned
checkout — root CLAUDE.md §15's "don't touch the git trees" rule is about
compute-d/compute-a's `vq admin update`-managed checkouts; on pbs-cluster we own provisioning,
but if pbs-cluster becomes a daemon-managed Arch-1 host the same discipline applies.)

## 12. Testing without a cluster

All logic is unit-tested against a **mock scheduler**: fake `qsub`/`qstat`/`qdel`
shims on `PATH` (mirroring how `tests/` already fakes `systemd-run`/subprocess) that
emit canned PBS/SGE output for each dialect, driving the full submit → poll →
terminal → fetch cycle. The maintainer validates on the real pbs-cluster via a documented
smoke recipe once a dialect is selected. **No silent cap** (§ workflow discipline):
the test matrix logs which dialects are covered.

## 13. Increment plan (each independently landable + green, §14)

1a. **Seam — process lifecycle (DONE 2026-06-05, pending commit).** New
   [`src/vq/dispatch.py`](../src/vq/dispatch.py): `Dispatcher` Protocol +
   `LocalDispatcher` (+ `DispatchError`). The daemon's `Popen` launch / `poll` /
   `terminate` / `kill` / `wait` now route through `self.dispatcher`
   (`_start_job`, `_reconcile_running`, `_watchdog_pass`, the post-launch kill-race
   window). Zero behavior change; `_RunningJob.popen`, `_read_pid_start_time`,
   `killpg`-by-pgid, and orphan/reattach all unchanged (test-coupled / Linux-only —
   see 1b). 8 new unit tests; full suite 2230 passed / 11 skipped; ruff +
   mypy-strict clean (0 new errors vs HEAD's 49). *(Restores the SPEC §8.1
   invariant for the lifecycle ops; valuable on its own even before pbs-cluster.)*
1b. **Seam — command building + signalling (SUPERSEDED).** The original plan
   was to move the local exit-marker shim + cgroup / `systemd-run` priv-drop
   wrap behind the dispatcher, which required Linux validation. The landed
   scheduler route avoided that risky refactor: scheduler-target specs now use a
   parallel `_start_scheduler_job` / `_reconcile_scheduler` path, while the local
   `Popen` / pgid / cgroup path stays unchanged. Keep this as design history,
   not as a next action.
   The Torque dialect itself (the pure string layer) **landed
   separately** at commit `ffa436d8` ([`src/vq/scheduler_dialect.py`](../src/vq/scheduler_dialect.py),
   60 tests): resource→`#PBS` mapping, job-script render with the ASCII guard,
   `qstat` state→`SchedulerPhase`, `qstat -f` exit_status parse, `qdel`.
2. **Scheduler dispatcher (DONE 2026-06-24).**
   [`src/vq/scheduler_dispatch.py`](../src/vq/scheduler_dispatch.py):
   `SchedulerDispatcher` drives the dialect over an injectable `RemoteRunner`
   (`SshRemoteRunner` wraps `vq.transport`) — stage-to-cluster, `qsub`, ONE
   batched `qstat` poll, `qdel`, exit-marker rc (array-aware, per-`$PBS_ARRAYID`),
   and the single `SchedulerPhase`→`JobState` bridge (§6). Config: additive
   `HostConfig` fields `scheduler` / `scheduler_dialect` / `remote_scheduler_host`
   / `submit_extra` / `scratch_root`, default `scheduler = "local"` ⇒ every host
   unchanged. 30 mock-scheduler tests + config-validation tests; ruff + mypy-strict
   clean. Originally inert until daemon wiring; now reached by
   `scheduler_target` specs through the driver-daemon submit path. Three dialect review
   notes resolved here: queue/account render once via `submit_extra`→
   `ResourceRequest` (never duplicated as `qsub` argv); the poll stays the plain
   `qstat` `parse_poll` expects; array rc comes from the per-index marker, never
   the first-match `qstat -f` aggregate.
3. **Staging-back + node-scratch + daemon wiring + reattach (DONE 2026-06-25;
   reachable via submit routing 2026-06-28).** Landed: `fetch_results` (whole-workspace stage-home on any terminal,
   `0f51f193`/earlier); node-local scratch (`b8952e47`, run in `/tmp1/$USER`,
   copy back to `/home`); the dispatcher factory `scheduler_dispatcher_for` +
   `dialect_for` registry (`0f51f193`); and the **daemon wiring keystone**
   (`611c9adf`) + **reattach** (`66f76108`). The daemon now dispatches
   (`_start_scheduler_job`), reconciles (`_reconcile_scheduler`, one batched
   `qstat`/host → `fetch_results` → the shared `_record_finish`), kills (`qdel`),
   and reattaches scheduler-target specs, all in a parallel `_scheduler_running`
   dict so the local `Popen` path is byte-for-byte unchanged (full daemon suite
   green). The dispatch gate skips a scheduler job's local cpu/mem and local
   `max_jobs` cap because it runs on the cluster; use `max_scheduler_jobs` for a
   separate driver-side scheduler submission cap. `JobSpec` gained
   `scheduler_target` + `scheduler_job_id` (default `None`). 14 mock-dispatcher
   daemon tests.
3b. **Submit-routing (DONE 2026-06-28), vq arrays/chains (DONE 2026-06-30),
    program identity (DONE 2026-07-01), per-program script hooks/wrapper argv
    support (DONE 2026-07-05), host-specific wrapper binaries (remaining).**
   `vq submit --host pbs-cluster` sets `scheduler_target` + forwards the spec to the
   fixed fleet **driver daemon (service-host)**, which dispatches via the keystone
   above. Explicit `status` / `logs` / `fetch` / `admin update` operations on a
   scheduler host also route through the driver. `--array N` and `--chain N`
   are vq-managed on scheduler hosts: the driver creates N ordinary specs tagged
   with the scheduler target rather than using PBS-native `qsub -t` arrays.
   Rerun metadata (`--rerun-until` / `--rerun-max`) is copied to every
   generated spec.
   `vq submit --program NAME` now stores a `[programs.NAME]` identity on the
   spec, forwards it through remote and scheduler-driver submits, exposes
   `VQ_PROGRAM=NAME` in local and qsub-backed jobs, and shows it in `vq status`.
   Scheduler jobs also receive the same vq array/chain/rerun environment
   metadata as local jobs, so future pbs-cluster templates have a stable key and
   complete job context to build from.
   Host-level scheduler script hooks (`scheduler_prologue` /
   `scheduler_epilogue`) now cover trusted site setup/cleanup lines inside the
   generated qsub script. Per-program hook tables
   `[hosts.HOST.scheduler_program_hooks.NAME]` now add matching trusted
   prologue/epilogue lines around jobs submitted with `--program NAME`, after
   the host prologue and before the host epilogue. The same tables can set a
   `command_wrapper` argv prefix so a trusted site wrapper receives the original
   submitted argv while vq keeps stdout/stderr capture, rc capture,
   node-scratch copyback, and the exit marker. Full site-specific `vibeqcsub` /
   `orcasub` / `crystalsub` binaries remain host-managed future work.
4. **`vq scheduler-probe`** (DONE `6422fb8e`, live-validated against pbs-cluster) + the
   user-facing docs (hosts.md, agent_interaction.md recipe row) after real-pbs-cluster
   validation.
5. **Validate on pbs-cluster** (operator-run smoke),
   then CHANGELOG/SPEC §8 update (mark the v1.0 backend "shipped, PBS/SGE
   dialects").

## 14. Site qualification

Before deployment, verify login-node process policy, scheduler dialect, queue
and account requirements, default resource limits, and the available build or
container environment. Record the answers in private provisioning configuration.
Use `submit_extra` for the site's queue and account directives and qualify the
chosen runtime installation method before accepting work.

## 15. Risks

- Refactoring the kill-race code (Increment 1) is the highest-risk step; mitigated
  by zero-behavior-change + the full existing race-condition test suite.
- Can't integration-test on pbs-cluster from the dev box; mitigated by the mock-scheduler
  suite + a maintainer smoke recipe + experimental gating until validated.
- Dialect guess wrong → mis-resourced jobs; mitigated by explicit config + probe.

## 16. Scheduler environment qualification

Before provisioning, record the scheduler dialect, software modules, filesystem
sharing, resource limits and compute-node network access in external operator
configuration. Probe the intended site and preserve the receipt privately.
Do not infer current capacity or installed software from this design document.

## 17. Daemon + submit integration (the wiring) — IMPLEMENTED, validation pending

The submit-side mechanism is complete in the code path (Increment 2/3 +
node-scratch + `scheduler-probe`, submit-routing on 2026-06-28, and vq-managed
scheduler arrays/chains on 2026-06-30). What remains is real-pbs-cluster validation and
the future per-program qsub templates. This section is kept as design context
because it explains the foundational assumption the implementation had to pierce.

**The foundational gap.** The daemon is *single-host, all-jobs-local*
(`daemon.py` header: "Tracks running children's Popen objects in memory"). A
`JobSpec` has **no host field**. Remote submit (`submit.py:submit_remote`) does
**not** make a daemon dispatch elsewhere — it scp's the workspace to the target
and runs `<remote_vq> submit localhost` *there*, so the target's own daemon runs
it as a local child. Arch 2 has **no daemon on pbs-cluster**, so that path cannot carry
a pbs-cluster job. A fleet daemon must instead dispatch a pbs-cluster-targeted spec over
SSH+qsub. That requires changes in **two** shared, queue-chat-owned areas:

1. **Submit routing.** A scheduler-host submit must land the spec in a *driver
   daemon's* queue (tagged with the scheduler target host), NOT scp to the
   target. `JobSpec` gains a `scheduler_target: str | None` (the host-config key,
   e.g. `"pbs-cluster"`); `None` keeps the current all-local meaning.
2. **Daemon dispatch loop.** Per-spec dispatcher selection (`scheduler_target is
   None` → `LocalDispatcher`, unchanged; else build a `SchedulerDispatcher` from
   that host's `HostConfig`). `_RunningJob` holds a `Popen` **xor** a
   `SchedulerHandle`; `_reconcile_running` polls by handle type (one batched
   `qstat` for all scheduler jobs of a host); `_escalate_if_killed` /
   `_watchdog_pass` route kill to `qdel` and the `/proc` watchdog **degrades to
   telemetry** under a scheduler (§10); startup reattach rebuilds the
   `SchedulerHandle` from the persisted scheduler job-id; terminal calls
   `fetch_results(handle, spec.cwd)` so `_record_finish` (shared) and `vq
   fetch-all` see a normal workspace (the queue chat verified this chain).

**Gating + risk control.** `scheduler_target is None` is the default, so the
LocalDispatcher path is **byte-for-byte unchanged** — no 1b "move the kill-race
code behind the seam" refactor, so zero risk to existing hosts and no Linux
re-validation of the local cgroup/multi-user paths. The scheduler path is purely
additive and experimental-gated. Crucially it **skips** cgroup / `systemd-run` /
priv-drop entirely (the scheduler runs the job as the user on the compute node),
so it is fully **Mac-mockable**: new daemon tests drive `_start_job` →
reconcile → terminal → fetch with a mock `SchedulerDispatcher` and a
`scheduler_target` spec, no SSH/Linux/cluster. Qualify the scheduler path with an operator-run smoke test on the intended site.

**The one decision (the driver-daemon model).** Where does a pbs-cluster-targeted spec
get dispatched?

* **Option A — local driver.** `vq submit --host pbs-cluster` writes the spec to the
  *local* daemon's queue tagged `scheduler_target="pbs-cluster"`; that machine's daemon
  drives qsub over SSH. Simplest (no new transport), but the driver is wherever
  you happen to submit from, and that machine must stay up to monitor + stage
  back.
* **Option B — fixed fleet driver.** A configured always-on host (service-host, the
  WireGuard hub) owns all scheduler-host jobs; `vq submit --host pbs-cluster` forwards
  the spec to service-host's daemon, which drives qsub and stages results back to
  the submitter. Matches the "service-host monitors all queues" model from the
  onboarding discussion; costs one forwarding hop + a config key naming the
  driver.

Recommendation: **B** as the target (centralized, survives the laptop sleeping),
with **A** as a trivial interim for first validation. Either way the daemon-side
changes above are identical; only *how the spec reaches the driver* differs.

**Coordination.** `submit.py` routing and the daemon dispatch loop are shared
with the queue chat. This section is the coordination artifact; the spec-schema
add (`scheduler_target`) and the submit-routing change was landed by the queue
chat on 2026-06-28, with the same driver-daemon decision.

## 18. Live status + incremental retrieval (roadmap)

The base backend reaps a job and stages its whole workspace home on terminal.
For long cluster runs (pbs-cluster jobs run hours to days), users want progress
visibility and partial output *before* the job ends. These are post-MVP items on
top of the now-reachable submit-routing (§17 item 3b), and several have a
daemon/dispatcher core (pbs-cluster-chat lane) plus a CLI/display edge (queue-chat lane).

1. **Cluster-side queued/running status — DONE (`14d96ed1`; display DONE
   2026-06-28).** Each reconcile
   poll stamps `spec.scheduler_state` ("queued" vs "running") + refreshes
   `last_heartbeat_at` on a transition. Distinguishes "submitted but waiting in
   the cluster queue" from "computing on a node", which the vq `state` (RUNNING
   from qsub) hides. The queue chat now surfaces it in `vq status` and the
   conditional `CLUSTER` column in `vq queue`.

2. **Richer qstat detail — DONE (`365dddcb`; status display DONE 2026-06-28).**
   The driver does a batched
   `qstat -f` per host alongside the coarse poll and stamps the live detail onto
   the spec: `scheduler_exec_host`, `scheduler_walltime_used`,
   `scheduler_walltime_limit` (dialect `QstatDetail` + `poll_detail_command` +
   `parse_qstat_detail`, kept as a sibling of the coarse `parse_poll` per review
   note 2; dispatcher `poll_detail`). Refreshed at most every 60s/job. `vq
   status` now displays exec host and walltime detail when present.

3. **Walltime-budget visibility / warning — DONE (display DONE 2026-06-28).**
   The data is now on the spec (item 2: `scheduler_walltime_used` vs
   `scheduler_walltime_limit`). `vq status` now computes and displays "N% used"
   when both values parse as `HH:MM:SS`, and emits a warning once the scheduler
   budget reaches 90% used so a near-converged job can be checkpointed before
   the `-l walltime` kill.

4. **Incremental / mid-run retrieval — DONE (CLI DONE 2026-06-28).**
   `fetch_results` is idempotent and tars the *current shared* remote workspace,
   so the dispatcher mechanism works mid-run for files already visible there.
   With `node_scratch_dir`, relative calculation artifacts remain on the
   compute node until successful normal copy-back; stdout and stderr are the
   exception because the wrapper redirects them directly to shared files.
   `vq fetch` now triggers that path for a non-terminal scheduler spec before
   the normal copy/tar stream runs: the driver stages the current cluster
   workspace into `spec.cwd`, then local fetch copies it or remote
   `tar-workspace` streams it to the laptop. Explicit scheduler-host
   `vq fetch HOST JOBID` routes through the configured `scheduler_driver`.
   Directory-style PBS jobs do not require a separate vq workdir: stdout,
   stderr, and final artifacts are staged through the workspace, so
   `vq fetch HOST JOBID -o DIR` is the supported retrieval path. A
   `--workdir` fetch may legitimately report that no separate workdir exists.

5. **Live log tail — DONE (`11409ca4`; CLI DONE 2026-06-28).** The job script
   now redirects the command's stdout/stderr straight to `stdout.log` /
   `stderr.log` on the NFS workspace (live-tailable), instead of `#PBS -o`/`-e`
   which Torque only writes at job end; `#PBS -o`/`-e` go to a throwaway
   in-workspace spool so the home dir stays clean. `SchedulerDispatcher.tail_log`
   tails them over SSH. `vq logs` now calls that path for non-terminal
   scheduler specs, including `--stdout`, `--stderr`, `--json`, and `--follow`;
   terminal scheduler specs read the staged-back local workspace. Explicit
   scheduler-host `vq logs HOST JOBID` routes through the configured
   `scheduler_driver`.

   `vq tail HOST JOBID --name FILE` now has the matching arbitrary-file path for
   live scheduler jobs: the CLI resolves the driver-owned spec, validates that it
   belongs to the requested scheduler target, and uses
   `SchedulerDispatcher.tail_file` against the remote cluster workspace. With
   `-f`, the CLI polls the same scheduler-dispatch path by byte offset and emits
   only appended text until the driver spec reaches a terminal state; terminal
   scheduler jobs continue to use the staged-back local workspace. `vq logs HOST
   JOBID -f` remains the preferred stdout/stderr monitor because it is
   stream-aware. An arbitrary relative artifact produced under configured node
   scratch is not visible to this path until normal copy-back.

6. **Provisioning auto-update — DONE (CLI DONE 2026-06-28; build-host override
   DONE 2026-06-30).**
   Scheduler hosts can declare `scheduler_update_command` and optional
   `scheduler_install_command`. `vq admin update pbs-cluster` routes through the
   configured `scheduler_driver` when invoked off-driver, claims the
   admin-update marker to stop new dispatch, refuses to rebuild while submitted
   scheduler jobs for that target are still active, then runs the remote cluster
   command on either the scheduler login host or the optional
   `scheduler_update_host` build target. `--cluster-install` selects the fresh
   provisioning command.

7. **Multi-cluster routing groundwork — DONE (CLI DONE 2026-06-28).**
   `vq overview` and `vq submit auto` now treat scheduler hosts as daemonless
   candidates owned by their `scheduler_driver`: they propagate the driver's
   daemon health / drain / admin-marker state, read the driver's queue JSON, and
   count only specs tagged for the target cluster. Scheduler overview load is
   scheduler-effective: only an exact last scheduler phase of `running` counts
   as confirmed execution. Queued, held, and unpolled work counts as pending;
   failure, fence, finishing, reattachment, and unknown phases also retain
   capacity in the pending totals while `unconfirmed_scheduler_jobs` and
   `unconfirmed_scheduler_cpus` identify that subset. The latter is not an
   additive placement bucket. `scheduler_queue_counts` keeps the raw scheduler
   phase counts visible for operators. A `[pools.clusters]` group can therefore choose among scheduler
   hosts by queue depth today. Richer future
   policy (software availability, site capacity, accounts) waits until a second
   real scheduler target exists.

8. **Scheduler-host preflight + fan-out hygiene — DONE (CLI DONE 2026-06-29).**
   `vq doctor pbs-cluster` validates the configured `scheduler_driver`, pings that
   driver's daemon, and now runs the read-only scheduler probe from the driver
   side so missing `qsub` / `qstat` / `qdel` / `qhold` / `qrls` is caught
   before dispatch or scheduler hold/release.
   The same probe checks PBS dispatch liveness via `qstat -Bf`, `qstat -Qf`,
   and a read-only `pbs_sched` process probe; doctor reports a separate
   `scheduler_liveness` failure when the PBS scheduler is down or enabled
   queues have `started=False`. `vq scheduler-probe HOST --json` exposes the
   same facts for monitors. The
   daemonless-host rendering also keeps `vq programs` / `vq admin status` from
   probing scheduler hosts such as `pbs-cluster` as if they ran a remote `vq` daemon.
   `vq daemon ping` / `vq daemon health` wrap scheduler hosts around their
   configured driver daemon for the same reason. Explicit scheduler-host
   job-control now routes wait/kill/resubmit/fetch-all through the driver and
   filters bulk operations to that scheduler target. `vq top` filters the
   driver specs to the target cluster, `vq drain` exposes the driver-level
   dispatch gate, and `vq throttle` is explicitly unsupported as a scheduler
   control because cgroup throttling does not apply to jobs already submitted to
   PBS/SGE. `vq pause` / `vq resume` now route queued scheduler jobs through
   qhold/qrls via the driver while refusing live compute-node jobs, since qhold
   is a scheduler queue hold rather than a SIGSTOP-equivalent suspension.
   `vq tail HOST JOBID --name FILE` also routes through the driver for
   scheduler jobs, using the live cluster workspace for non-terminal jobs and
   the staged-back workspace once the job is terminal.

Sequencing: 1, 2, 3, 4, 5, 6, and 8 are done; 7 has queue-depth groundwork,
with richer multi-site policy deferred.
