# vq roadmap

What vq is working towards next, and what has been released but is not yet
proven on hosts. Rewritten on 2026-09-13, and refreshed on 2026-09-22 against
release `v0.26.10` at `8e694af` and the canonical tracker. The previous
file had become a release log for v0.1 to v0.9 with no forward section; it is
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
| Web console milestones | [`fleet_dashboard_design.md` § 6](fleet_dashboard_design.md#6-roadmap) |
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

### Deployment

**This file no longer records host state, and must not start again.** Since
v0.26.8 the accepted reports and the fleet ledger live in private operations
storage, outside this repository (`releases/README.md`). Read deployment
facts there, or from a host with `vq admin status --json` and `vq programs`.

What this repository can still say:

* **A tag is not a deployment.** Everything in § 2 rides to hosts only on an
  accepted report that pins it, rolled by the release coordinator.
* **The v0.17.2 report** (vq v0.26.5, calculation engine v0.17.2, viewer
  v2.16.2) was accepted on 2026-09-13 and was not rolled beyond the driver
  that day; releases up to v0.26.8 have been cut since, so the pin an
  operator should target is whichever report is accepted now.
* **One host carries an exception:** compute-c's own vq was installed from
  `main` on 2026-09-13 as the prerequisite for validating #37 there.

---

## 2. Released, not yet validated

**A tag proves a build, not a behaviour on a host.** Everything cut since
v0.26.0 rides to hosts only on an accepted report that pins it.

These stay here because their evidence can only come from a specific host.
Most were added on 2026-09-19 and 2026-09-20, when two rounds of independent
review of the merged tree found twenty defects in that day's own fixes; the
fixes landed, and what each still needs from a host is below.

| issue | pri | what | in | what is still needed |
|---|---|---|---|---|
| #17 | P1 | Scheduler-site runtime publication preserves published artifacts | v0.26.4 | The cluster lanes, after the release-paper campaign drains |
| #25 | P2 | Declared extras floor; `vq web install` runtime refusal | v0.26.1 | A real host |
| #47 | P2 | `vq admin install` applies a program's declared extras floor, or refuses | v0.26.10 | One provision of a venv declaring `extras` on a host, and one refusal: an install accepted before may now be refused |
| #63 | P1 | Scratch separated from the state root by default | v0.26.10 | `vq cleanup` reporting real workdir bytes on a host with a large `workdirs/`; a daemon restart with jobs under both roots; a scheduler host whose `scratch_root` changes with jobs in flight |
| #23 | P2 | A PBS queue's `resources_default.neednodes` names its lane | v0.26.10 | A real PBS site: that the attribute is populated and spelled that way, that inferred lanes are genuinely unrouted, and per-lane figures for a node carrying several properties. PBS Pro spells the default differently and yields all-inferred lanes. Also one real `qstat -Qf` capture, to settle whether a folded line is reachable at all |
| #51 | P1 | Scheduler poll paced per host, and escalating on an outcome vq never learned | v0.26.10 | **A behaviour change:** a host that only ever times out is now paced, where it was not. The effect on observation latency for a slow but healthy login node is unmeasured. Also that a real login node under connection pressure produces the alternating refuse/hang pattern the escalation is tuned for, which is still an inference from #51's logs |
| #51 | P1 | One observer per host, one dispatcher per job | v0.26.10 | That a real `qdel` crosses the alias the job was submitted under, and that one pass still costs one connection on a host spanning two dispatcher generations |
| #63 | P1 | The nine follow-up defects, including the guard a restart bypassed | v0.26.10 | The remembered-root file read back after a real service restart; the multi-user load error seen by a root daemon reading its own config; the rewritten phase 5 runbook walked end to end |
| #63 | P1 | The scratch-root guard scoped to strandable jobs, and careful on a first start | v0.26.10 | **A behaviour change:** `vq doctor` now exits non-zero while a host's root is held, which `fleet_rollout.doctor_failures` reads as a blocked rollout. Intended, and unproven against a real roll. Also the first-start-after-upgrade path against genuinely pre-upgrade specs |
| #68 | P2 | The distribution renamed, and the PEP 610 readers reworked | v0.26.10 | One host update from a v0.26.9 environment, where `pip show vq` should come back empty and `pip show vibe-queue` report the version. A venv carrying records under **both** names now refuses adoption and reports its mode as unknown; no fleet host was inspected for one |

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
  local installation. Scheduler-only registration is supported in source.
  Adoption and per-program dispatch/fetch on the scheduler authorities
  remain operational acceptance, separate from a passing registration check.

### Publication

* **#54** — publication readiness: historical privacy findings, and keeping
  private operational material out of the product tree. Much of it shipped
  in v0.26.7 and v0.26.8; the issue tracks what remains before mirroring.
  **The maintainer owns this one; it is not agent work.**

### Fleet layout and the common store

A group filed together on 2026-09-18, each depending on the one before.

* **#72** — migrate runtime registrations and scheduler helpers into the
  common store.
* **#73** — support serving-vq self-updates in immutable runtime slots.
* **#74** — reviewed common-store runtime retention with authoritative
  liveness.

### Scheduling and the console

* **#64** — submit to one broker host and let it place the job by RAM, CPUs
  and queue availability (central, late-binding placement).
* **#65** — calculation previews in the fleet console (structure,
  convergence, key results).
* **#66** — fleet console on the coordinator host with a public read-only
  endpoint under the project domain.

### Historical plan triage (#75)

The inherited v0.4–v1.0 plan is reconciled below, rather than carried as a
second backlog. Its version numbers and test counts are historical. The
[old proposal](SPEC.md#13-revised-roadmap) is explicitly retired; current
source and the classified specification sections define what shipped.

| inherited item | disposition on v0.26.10 |
|---|---|
| VQ4a: cgroups v2 | Implemented where systemd delegation is available, with watchdog fallback on supported single-user hosts. Wall time remains watchdog-owned. See [resource enforcement](SPEC.md#52-cgroups-v2-strategy). |
| VQ4b: read-only web UI | Implemented, with later control actions and a fleet console. Public exposure is still #66; existing pages are not an anonymous-safe publication surface. |
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

Milestones live in [`fleet_dashboard_design.md` § 6](fleet_dashboard_design.md#6-roadmap).
Still open:

* **M2:** the OIDC provider (needs a maintainer OAuth registration), and a
  queue-wide clear-failed action.
* **M3:** the public endpoint (proxy, DNS, TLS, backup), and deployment
  validation of the public-exposure gate.
* **M4–M6:** results and QVF browsing, live operations and analytics, and
  organization-facing hardening.

### Carried from the archived roadmap, not filed since the split

These were recorded as open before the public repository existed. None has a
tracker issue. **Confirm they are still wanted before filing them.**

* **RECOV-4:** guard reattach against a recycled pgid. Deferred to a Linux
  host.
* **ISO-1:** cross-user read isolation. Needs a decision on the admin access
  model, and Linux validation.
* **The privileged `/opt/vq` root-daemon apply lane,** and a supported
  deployment model for compute-d's root unit. Both need an authorization
  decision.

---

## 4. Decisions waiting on the maintainer

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
are merged, closed, and included in v0.26.10. Their real-host qualification
remains part of #25; source completion is not that evidence.

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
