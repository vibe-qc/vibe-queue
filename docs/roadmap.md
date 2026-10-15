# vq roadmap

What vq is working towards next, and what has been released but is not yet
proven on hosts. Rewritten on 2026-09-13, and refreshed on 2026-09-29 against
release `v0.26.18` at `1e7fd3c` and the canonical tracker. (It was refreshed
against `v0.26.10` on 2026-09-22; edits after that were made piece by piece and
left the header behind.) The previous file had become a release log for v0.1 to v0.9 with no forward section; it is
preserved unchanged as [`roadmap_history.md`](roadmap_history.md).

## Where each kind of fact lives

| question | authority |
|---|---|
| What shipped, and in which release? | `CHANGELOG.md`, from v0.26.0. Through v0.9: [`roadmap_history.md`](roadmap_history.md) |
| What is open, and how urgent is it? | The canonical queue tracker, by `priority::P1` / `P2`. Its connection details come from external operator configuration |
| In what order, and what gates what? | **This file** |
| How agents work here | [`AGENTS.md`](../AGENTS.md). Decided in #24 |
| What is deployed on which host? | The fleet ledger and accepted release reports, both in private operations storage (`releases/README.md`) |
| Why the design is what it is | [`SPEC.md`](SPEC.md) |
| Web console milestones | § 3 "Web console" below. The old definition in `fleet_dashboard_design.md` left the repository in #54 |
| Names for future minor releases | [`codenames.md`](codenames.md). A name is a reservation, not a scope |

Release cutting, fleet release reports and fleet rolls belong to the release
coordinator (the agentic loop) under the maintainer's authority. Development
chats land code, record validation, and ask for deployment.

---

## 1. Where things stand

### Releases

| tag | date | commit | headline |
|---|---|---|---|
| v0.26.0 | 2026-09-09 | `b0f157a` | First public release: licence, changelog, documentation site, own CI |
| v0.26.1 | 2026-09-12 | `95fd0fe` | Machine-readable `vq admin update` outcomes, declared `extras`, `vq admin install`, `vq daemon install`, scheduler and rollout fixes (#5–#14) |
| v0.26.2 | 2026-09-12 | `c4622ad` | Rollout compares each component's SHAs in its own repository (#15) |
| v0.26.3 | 2026-09-12 | `cfddd51` | Same-SHA update validation; nonexistent root-daemon unit (#16) |
| v0.26.4 | 2026-09-13 | `d072f92` | Revocable console sessions, bounded login admission, write audit; slurm-cluster immutable runtime publication (#17) |
| v0.26.5 | 2026-09-13 | `da2dbca` | Detached delegated updates (#37, #34), rollback of vendored native libraries (#44), per-host update cap (#32), `UNHEALTHY` programs (#45), console runtime checks (#28), and reliability fixes (#26–#29, #35, #38, #40, #41, #49, #50) |
| v0.26.6 | 2026-09-13 | `917c521` | Release discovery cannot silently certify an older release (#52); safe lifecycle refusal messages (#20); named directory artifacts (#55) |
| v0.26.7 | 2026-09-15 | `6402992` | Large queues get startup headroom during self-update (#53); detached observers tolerate receipts still publishing (#36); bounded scheduler-poll diagnostics (#51); the first privacy pass for public mirroring (#54) |
| v0.26.8 | 2026-09-16 | `d96f32c` | Accepted fleet reports move to external private storage; public clone and contribution guidance; external report consumers qualified (#54, #56) |
| v0.26.9 | 2026-09-17 | `5ed843a` | Console session-store lock budget; fetch that loses an entry rather than a job; readiness sizing under uncertainty (#60, #62, #77, #79) |
| v0.26.10 | 2026-09-22 | `8e694af` | Scratch/state separation and recovery guards, paced scheduler observation, retained-history scan costs, program extras and distribution-name fixes |
| v0.26.11 | 2026-09-24 | `ac608f1` | Runtime slots: custom venv names, reviewed local registration migration (`vq admin migrate-runtime`) and explicit slot execution (#72); scheduler-only program registrations (#87); slot updaters bound to the new generation (#88) |
| v0.26.12 | 2026-09-24 | `bc725d7` | Serving-vq self-update in immutable runtime generations (#73); generations built from the active one, slot-aware status and auto-update (#72); reviewed `vq admin runtime-retention` (#74); `munlink` on Lustre (#89) |
| v0.26.13 | 2026-09-24 | `2c10bcf` | Opt-in scheduled runtime retention (#74); `vq cleanup --orphans` for abandoned scheduler workspaces (#91); auto-cleanup on always-busy daemons (#90); rollout hold supersession after a re-pin (#93) |
| v0.26.14 | 2026-09-25 | `6b48285` | Calculation preview in the fleet console (#65); `vq admin retire-checkout` (#72); cleanup of stray transfer archives and interrupted-update leftovers (#91, #95); `--abandon-host` (#96); install-refusal remedies, the `scratch_root` doctor verdict and record retry (#47, #63) |
| v0.26.15 | 2026-09-25 | `99cb806` | Doctor check that `remote_vq` and the daemon share a state root (#64); ORCA previews (#65); the public snapshot schema `vq.public-fleet/1` (#66); scheduler-host participant coordinator for `migrate-runtime` (#72); wall-time budget kept across a daemon restart (#97); rollout hold fixes (#98, #99); stdin isolation (#101) |
| v0.26.16 | 2026-09-26 | `107a6d0` | `vq web public-snapshot` writes the static public view (#66); a slot's first build runs `install_script` (#104); serving-slot refusals and receipt narrowing (#73); a refused `--acknowledge` reported cleanly (#72) |
| v0.26.17 | 2026-09-29 | `90cb028` | The fleet broker: held store, placement, FIFO pass, role and client relay (#64); dev lanes can track a branch (#109); runtime retention reads live processes (#74); slots across filesystems, a fresh serving-slot build and retention through `current` (#111, #112, #114); allocation transcripts and the deploy-log cap (#105, #113); migration plan fixes (#104, #106, #107) |
| v0.26.18 | 2026-09-29 | `1e7fd3c` | The broker's `vq wait` and `vq logs --follow` (#64); remote `--follow` that prints output as it arrives (#64); `vq admin reconcile-operation` for a rollout operation a driver crash left outcome-unknown (#115) |

### Deployment

**This file no longer records host state, and must not start again.** Since
v0.26.8 the accepted reports and the fleet ledger live in private operations
storage, outside this repository (`releases/README.md`). Read deployment
facts there, or from a host with `vq admin status --json` and `vq programs`.

What this repository can still say:

* **A tag is not a deployment.** Everything in § 2 rides to hosts only on an
  accepted report that pins it, rolled by the release coordinator. The pin an
  operator should target is whichever report is accepted now.

---

## 2. Released, not yet validated

**A tag proves a build, not a behaviour on a host.** Everything cut since
v0.26.0 rides to hosts only on an accepted report that pins it.

These stay here because their evidence can only come from a specific host.
Most were added on 2026-09-19 and 2026-09-20, when two rounds of independent
review of the merged tree found twenty defects in that day's own fixes; the
fixes landed, and what each still needs from a host is below.

As of 2026-09-29 the tracker records host evidence for two rows only, #63 and
the local migration flow of #72 and #104, and each only in part. Evidence that
lives in private operations storage is not visible from this repository.

| issue | pri | what | in | what is still needed |
|---|---|---|---|---|
| #17 | P1 | Scheduler-site runtime publication preserves published artifacts | v0.26.4 | The cluster lanes, after the release-paper campaign drains |
| #47 | P2 | `vq admin install` applies a program's declared extras floor, or refuses | v0.26.10; the refusal flow reworked in v0.26.14 and v0.26.15 | One provision of a venv declaring `extras` on a host, and one refusal: an install accepted before may now be refused. Then the refusal's printed route walked end to end (drop, provision, restore, `vq admin update ENV`), and a refusal after the clone: `vq admin status` shows no update marker, `git_dir` is empty on the named machine, and `vq admin install` succeeds once the install script is fixed |
| #63 | P1 | Scratch separated from the state root by default | v0.26.10 | **Partly evidenced.** The fleet roll of 2026-09-22 applied the new scratch paths on every host of the fleet; idle daemon restarts kept their state roots and existing job paths; seven canary jobs ran, including a fetch (note on the issue). Still needed: `vq cleanup` reporting real workdir bytes on a host with a large `workdirs/` (no age-based cleanup was enabled); a restart with jobs running under both roots; a scheduler host whose `scratch_root` changes with jobs in flight |
| #23 | P2 | A PBS queue's `resources_default.neednodes` names its lane | v0.26.10 | A real PBS site: that the attribute is populated and spelled that way, that inferred lanes are genuinely unrouted, and per-lane figures for a node carrying several properties. PBS Pro spells the default differently and yields all-inferred lanes. Also one real `qstat -Qf` capture, to settle whether a folded line is reachable at all |
| #51 | P1 | Scheduler poll paced per host, and escalating on an outcome vq never learned | v0.26.10 | **A behaviour change:** a host that only ever times out is now paced, where it was not. The effect on observation latency for a slow but healthy login node is unmeasured. Also that a real login node under connection pressure produces the alternating refuse/hang pattern the escalation is tuned for, which is still an inference from #51's logs |
| #51 | P1 | One observer per host, one dispatcher per job | v0.26.10 | That a real `qdel` crosses the alias the job was submitted under, and that one pass still costs one connection on a host spanning two dispatcher generations |
| #63 | P1 | The nine follow-up defects, including the guard a restart bypassed | v0.26.10 | The remembered-root file read back after a real service restart; the multi-user load error seen by a root daemon reading its own config; the rewritten phase 5 runbook walked end to end |
| #63 | P1 | The scratch-root guard scoped to strandable jobs, and careful on a first start | v0.26.10; "held" judged from the queue since v0.26.14 | **A behaviour change:** `vq doctor` now exits non-zero while a host's root is held, which `fleet_rollout.doctor_failures` reads as a blocked rollout. Intended, and unproven against a real roll. Since `8de79e1` (v0.26.14) "held" is judged from the queue as the driver judges it, so a key changed on an idle host with nothing staged no longer fails; that is the case a roll most often meets and it is equally unproven. Also a scheduler host held at its previous root taking an ssh alias edit without releasing the hold, and the first-start-after-upgrade path against genuinely pre-upgrade specs |
| #68 | P2 | The distribution renamed, and the PEP 610 readers reworked | v0.26.10 | One host update from a v0.26.9 environment, where `pip show vq` should come back empty and `pip show vibe-queue` report the version. A venv carrying records under **both** names now refuses adoption and reports its mode as unknown; no fleet host was inspected for one |
| #66 | - | The public fleet snapshot: schema and projection, the `vq web public-snapshot` writer, and the operator runbook | v0.26.15, v0.26.16, v0.26.17 | The runbook's acceptance recipe on a real deployment: the timer survives logout and reboot, the serving identity can read only the two files, the HTTPS endpoint returns the intended types and headers and refuses other paths, and a failure exercise keeps the old output, fires the freshness alert and recovers. Also load buckets on a scheduler host whose `max_cpus` is unknown, and `rename` on a network filesystem. Labelled `workflow::needs_verification` |
| #72, #104 | - | Local runtime-registration migration (`vq admin migrate-runtime`) and the first build of a slot | v0.26.11 to v0.26.17 | **Partly evidenced.** One direct host accepted the flow on 2026-09-28 at v0.26.16: first-generation prepare and apply, supported rollback and a fresh reapply, for a development, a release and a viewer registration (note on #104). Still needed: the v0.26.17 follow-ups (an installer's operator flags, and the plan blocker) on a direct host; and one scheduler-host participant transaction that really switches a binding, which the independent verifier of #72 failed it for on 2026-09-26 |
| #73 | - | Serving-vq self-update in immutable runtime slots | v0.26.12, hardened in v0.26.16 | Real systemd or launchd restart, rollback and recovery evidence on a host. A simulator stood in |
| #74 | - | Runtime retention: `vq admin runtime-retention`, the opt-in schedule and the Linux live-process scan | v0.26.12, v0.26.13, v0.26.17 | The host checklists on the issue |
| #105, #106, #109 | - | Allocation transcripts and the deploy-log cap; empty wrapper lists in the participant plan; dev lanes that track a branch | v0.26.17 | Each has a checklist on its issue; #105 and #106 carry `workflow::needs_verification`, and #109 was closed on an independent source-level pass. #105, on the Slurm host with `update_allocation`: two consecutive updates carry deploy-metric lines, and the retained deploy logs stay near 100 MiB. #106: the two-authority plan no longer blocks on empty wrapper lists, still blocks on real differences, and refuses an older plan at `--apply-plan` by design. #109: with `[fleet_tracking_lanes]` set, a rollout moves the development lanes to the head and `--only-tracking` advances only those |

Fourteen further rows stood here on 2026-09-16 and are gone, not because their
host evidence arrived but because the rule changed: on the maintainer's
instruction of 2026-09-18 a bug closes once its fix is merged, and the host
and CI checks each one still wanted moved to the release coordinator's
validation lanes. Each issue's closing comment carries the commit, the
evidence that does exist, and the list of what was never exercised; a
recurrence reopens it. Nothing in that change makes an unvalidated fix
validated.

The ordering that governed those rolls is unchanged. The driver must carry
the per-host update cap before a target's configured timeout is forwarded,
and a target must carry detached delegated updates before a detach check on
it means anything; the cluster lanes come after the campaign drains.

## 3. Open work, by theme

Every item is a tracker issue. Order within a theme follows the priority
label.

The earlier defect backlog was worked through between 2026-09-17 and
2026-09-20; merged fixes and their remaining validation are recorded on the
issues. New findings still require triage against current source.

### Scheduler driver registration

* **#87** — a scheduler driver's program hooks name programs that have no
  local installation. Scheduler-only registration (`kind = "scheduler"`) shipped
  in v0.26.11 (`6e87587`), and a 2026-09-24 audit of every registry consumer
  found no further source gap. The issue was closed administratively on
  2026-09-26; that is **not** a verified operational pass. Still to do, and no
  longer on an open issue: apply the prepared driver configuration, get a clean
  `vq doctor` on each scheduler profile, and run one bounded submit and fetch
  per program on each scheduler authority.

### Publication

* **#54** — publication readiness: historical privacy findings, and keeping
  private operational material out of the product tree. Closed by the
  maintainer on 2026-09-26: the sanitization shipped in v0.26.7 and v0.26.8
  and was accepted, and the development history is not published (public
  refs are fresh snapshots). Publication itself has not happened. It needs a
  fresh snapshot and full scan of current `main`, the GitHub setup, and the
  maintainer's explicit yes immediately before visibility; that is
  maintainer-owned work, not agent work.

### Fleet layout and the common store

A group filed together on 2026-09-18, each depending on the one before.

* **#72** — migrate runtime registrations and scheduler helpers into the
  common store. Released: the local transaction, execution binding and custom
  venvs (v0.26.11); building from the active generation, decided 2026-09-24 as
  an explicit `runtime_slot_build_input` opt-in with locks keyed on the runtime
  root plus the legacy checkout while it exists, with slot-aware status and
  auto-update (v0.26.12); `vq admin retire-checkout` (v0.26.14); and the
  multi-authority participant coordinator (plan, prepare, journaled
  apply/rollback/recovery, status inventory; v0.26.15). The issue is open
  because the independent verifier failed it on 2026-09-26, when no real
  binding had switched through `migrate-runtime` (this became #104), and it was
  reopened. A direct host has since accepted the local flow (§ 2). Still open: a
  scheduler-host participant transaction that really moves a binding, and
  `retire-checkout` and retention on a host.
* **#73** — support serving-vq self-updates in immutable runtime slots. Closed
  on 2026-09-24; released in v0.26.12 and hardened in v0.26.16. Real
  systemd/launchd restart and rollback evidence is a release-coordinator gate
  (§ 2).
* **#74** — reviewed common-store runtime retention with authoritative
  liveness. Released: `vq admin runtime-retention` (v0.26.12), the opt-in
  schedule on auto-cleanup sweeps (v0.26.13) and the Linux live-process scan
  (v0.26.17). Still open: scheduler-host packed publications, which the command
  does not cover, and the host checklists.

### Scheduling and the console

* **#64** — submit to one broker host and let it place the job by RAM, CPUs
  and queue availability (central, late-binding placement). Scope decided
  2026-09-24: one durable broker, payload relayed through it, late binding
  with durable reservation and transfer receipts, daemon hosts only
  (scheduler lanes refused), SSH-only as the operator account, with a
  `SECURITY.md` entry. The increment split (B0–B7) is on the issue. Released:
  B1, the `vq doctor` check that `remote_vq` and the daemon share a state root
  (v0.26.15); and in v0.26.17 the design record (B0, `docs/design_broker.md`),
  pure placement (B2), the durable held store (B3, `vq.broker_store`), per-job
  placement and restart recovery (B4a, `vq.broker_placement`), one strict-FIFO
  placement pass (B4b), fallback to the next-ranked host with cancellation
  winning over replay (both decided 2026-09-27), the client relay (B5), and
  the `[broker]` role with its daemon-run pass, target capacity evidence from
  the daemon's own admission snapshot and the final `SECURITY.md` entry (B6).
  `vq wait` and `vq logs --follow` for broker jobs followed in v0.26.18, and
  `--idempotency-key` and `vq kill --resubmit` for broker jobs are on `main`
  after that tag. The role is off until a host enables it; operator steps are
  in `operations.md`, "The fleet broker". `vq broker reconcile` settles a
  transfer a target refused from that target's own idempotency fence (decided
  2026-09-29). B7, the console's metadata-only view of broker jobs on
  `/fleet/jobs`, is on `main`. Bounded two-target acceptance passed on
  v0.26.26: held reasons, client relay, idempotent submissions and lost-receipt
  recovery, streaming logs, fetch, running cancellation and resubmission,
  and the read-only console. This used an isolated broker store and manual
  placement passes; production enablement remains an operator choice.
* **#66** — fleet console on the coordinator host with a public read-only
  endpoint under the project domain. Decided 2026-09-24: a static snapshot
  served by a separate unprivileged process, with a default-deny public field
  envelope. The schema, projection, writer and operator runbook are released
  (v0.26.15 to v0.26.17); what is left is deployment and acceptance (§ 2, and
  the web console section below).
* **#65** — calculation previews in the fleet console. Closed on 2026-09-25;
  the preview endpoint shipped in v0.26.14 and ORCA output in v0.26.15.
* **#109** — let dev lanes track a branch instead of the release pin
  (`[fleet_tracking_lanes]`). Released in v0.26.17 and closed on 2026-09-29
  after an independent source-level pass; its fleet-driver checklist is still
  to be run on a host (§ 2).
* **#110** — the VASP directory workflow. The documentation and synthetic
  tests passed independent verification on 2026-09-27, and an independent
  verifier accepted live qualification at two sites (one Torque, one Slurm) on
  2026-09-29. Not established: that MPI child processes are gone after a
  cancellation, which no site let the verifier scan for. The issue was still
  open when this was written.

### Historical plan triage (#75)

The inherited v0.4–v1.0 plan is reconciled below, rather than carried as a
second backlog. #75 closed on 2026-09-22, its handover incorporated and
independently accepted, so this is a record and not open work. Its version numbers and test counts are historical. The
[old proposal](SPEC.md#13-revised-roadmap) is explicitly retired; current
source and the classified specification sections define what shipped.

| inherited item | disposition on v0.26.10 |
|---|---|
| VQ4a: cgroups v2 | Implemented where systemd delegation is available, with watchdog fallback on supported single-user hosts. Wall time remains watchdog-owned. See [resource enforcement](SPEC.md#52-cgroups-v2-strategy). |
| VQ4b: read-only web UI | Implemented, with later control actions and a fleet console. The console pages are not an anonymous-safe publication surface; the public view is the static snapshot written by `vq web public-snapshot` (#66). |
| VQ4c: per-job event log | Implemented as a best-effort JSONL timeline, not a transactional journal. See [events](SPEC.md#61-per-job-event-timeline). |
| v0.5: HTTP API and idempotency | Read/control API and CLI single-job submission idempotency exist. HTTP submit/upload is not implemented; the historical plan does not authorize adding it. See [submission interfaces](SPEC.md#35-submission-interfaces) and [idempotency](SPEC.md#48-submission-idempotency). |
| v0.5: SQLite index and Prometheus | Retired proposals, not missing release requirements. Queue JSON remains authoritative; the session SQLite database is not a job index. See [storage](SPEC.md#23-storage-model-and-its-evolution) and [metrics](SPEC.md#63-metrics). |
| v0.6: multi-user, adapters, per-user tokens | OS-user execution and quotas exist. Generic adapter discovery and per-user bearer tokens are retired proposals; the program registry, scheduler hooks, shared administrative token and fleet sessions are the current contracts. See [program integration](SPEC.md#34-program-integration-boundaries) and [authentication](SPEC.md#72-authentication-evolution). |
| v0.7: priorities, quotas, retries, dependencies, arrays, notifications | Implemented within the documented per-host queue model. Notifications are best-effort terminal webhooks on explicit paths, not guaranteed delivery or an email service. See [operator guidance](operations.md) and [notifications](SPEC.md#62-terminal-webhook-notifications). |
| v1.0: Slurm evaluation | Superseded by the implemented Torque/PBS and Slurm dialects. The batch scheduler owns cluster placement; central broker placement remains the separate #64 request. See [scheduler backends](SPEC.md#82-scheduler-backends). |

Unimplemented historical ideas need a new, approved requirement before source
work. This triage neither promises their implementation nor closes the
current feature and host-validation issues listed elsewhere on this page.

### Web console

The milestones were defined in `fleet_dashboard_design.md`, which #54 cut down
to a short overview, so the open ones are restated here. Still open:

* **M2:** the OIDC provider (needs a maintainer OAuth registration), and a
  queue-wide clear-failed action.
* **M3:** the public endpoint. The public view is a static snapshot (#66):
  the schema and projection (v0.26.15), `vq web public-snapshot` (v0.26.16)
  and the [operator runbook](public_snapshot_deployment.md) (v0.26.17) are
  released. Still open: authorized deployment (isolated serving account,
  proxy, DNS, TLS and backup) and end-to-end acceptance of permissions and
  freshness.
* **M4–M6:** results and QVF browsing, live operations and analytics, and
  organization-facing hardening.

### Carried from the archived roadmap, not filed since the split

These were recorded as open before the public repository existed. None has a
tracker issue. **Confirm they are still wanted before filing them.** They were
not re-checked against source on 2026-09-29.

* **RECOV-4:** guard reattach against a recycled pgid. Deferred to a Linux
  host.
* **ISO-1:** cross-user read isolation. Needs a decision on the admin access
  model, and Linux validation.
* **The privileged `/opt/vq` root-daemon apply lane,** and a supported
  deployment model for compute-d's root unit. Both need an authorization
  decision.

---

## 4. Decisions waiting on the maintainer

* **#104:** whether `vq admin update` of a slot program should keep forwarding
  the operator's flags to its installer (the current behaviour) or refuse them.
* **#66:** a reachable host whose daemon is stopped reads "up", because daemon
  health is not consulted; and a calculation on a host with no display name is
  published with `host: null`, which tells a reader that an unnamed machine
  exists. Say whether either should change.
* **#64:** whether a host that lists pools is reserved for them, and whether an
  unknown wall-time limit should hold a job. The review question of
  2026-09-26 has no recorded answer.
* **The out-of-scope list in § 6.** Two of its entries predate the fleet and
  now contradict current direction:
  * "distributed queue across multiple hosts": vq now drives many hosts and
    two batch schedulers from one driver;
  * "multi-tenant queues": console milestone M6 plans per-project separation.

  Decide whether each is still out of scope, and how it is bounded.

Seven decisions that stood here are settled, all on 2026-09-19. #21 (triage
notes on #10, #11 and #12) and #30 (report convergence while both repositories
ran untagged `main`) are recorded on their issues; #43 has candidate branches
deleted once the tag exists, written into `release_process.md`, and the seven
that remained are gone; #68 names the distribution `vibe-queue`; #22 needed no
deletion decision in the end, because the settled-row skip took retained
history off the admission path without discarding anything; #38 closed on its
own validation evidence. #47 honours a program's declared `extras` at install
and refuses the install when it cannot, and #63 separates scratch from state
by default. Both have landed and wait only on the host evidence in their § 2
rows. #83 settled on 2026-09-20: the probe payload carries
`vq.scheduler-probe.v1`, and the wider question of which JSON outputs are
contracts is answered for that payload only, deliberately, because a rule
derived from three instances would mostly be a guess about the fourth.

The two extras gaps found in those reviews, **#84** (doctor diagnostics for
unappliable declarations) and **#85** (the extras floor on ordinary updates),
are merged, closed, and included in v0.26.10. The #25 extras and web-install
acceptance completed on 2026-10-05: Linux and macOS refusal checks, a live
web-enabled service, a new-generation core-to-web daemon update, a real
dev-to-all generation-preparation fixture, and conflicting-request refusal.
The final v0.26.27 ordinary-updater checks covered actual work and
already-current updates in text and JSON modes: one notice in stderr and the
transcript, no extras arguments passed to the updater, and unchanged profiles
and production service state. The broader-profile fixture proves generation
preparation, not daemon activation. Operational receipts remain on the tracker
and in private evidence storage.

## 5. Future minor releases

No issue is assigned to a version. The tracker defines no milestones, and
the version of the next cut is chosen by the release coordinator when a
candidate is prepared.

[`codenames.md`](codenames.md) reserves provisional names and one-line
concepts for v0.27.0 to v0.33.0. They are image themes, not commitments. Where
a concept happens to match open work, that is noted as a possibility only:

* v0.31.0 *Stonebraker's Vacuum*, "reclaim dead space through archiving and
  automatic cleanup", is the nearest match for the common-store retention
  work (#74).
* v0.33.0 *Erlang's Blocking*, "counted permits enforcing admission and
  concurrency limits", is the nearest match for broker placement (#64).

---

## 6. Out of scope (under review, see § 4)

* A distributed queue across multiple hosts. The original wording read:
  "SLURM exists. vq's niche is single-host informal queues."
* GPU scheduling: the runtime's job.
* Container orchestration.
* Multi-tenant or cross-organization queues.

---

## How to update this file

* **A change ships:** write a `changelog.d/` fragment, not an entry here.
  `roadmap_history.md` is frozen.
* **A fix lands that needs host or CI evidence:** say so on its issue, with
  the commit and how to check it. Add a row to § 2 only when the evidence can
  come from one identified host and nothing else can substitute.
* **A release is tagged, or the fleet ledger records a deployment:** update
  § 1 in the same change.
* **New work:** file the issue first, then list it under its theme in § 3.
  Items without an issue go under "Carried … not filed" only, and only until
  someone files or drops them.
* **A decision is made:** move it out of § 4 and apply it wherever it lands.
* **Keep this file short.** Rationale belongs in the issue, the design
  document or `SPEC.md`, not here.
