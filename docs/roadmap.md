# vq roadmap

What vq is working towards next, and what has been released but is not yet
proven on hosts. Rewritten on 2026-09-13 against `main` at `bdbeda9`. The
previous file had become a release log for v0.1 to v0.9 with no forward
section; it is preserved unchanged as
[`roadmap_history.md`](roadmap_history.md).

## Where each kind of fact lives

| question | authority |
|---|---|
| What shipped, and in which release? | `CHANGELOG.md`, from v0.26.0. Through v0.9: [`roadmap_history.md`](roadmap_history.md) |
| What is open, and how urgent is it? | The project's GitLab issues (project 36), by `priority::P1` / `P2` |
| In what order, and what gates what? | **This file** |
| How agents work here | [`AGENTS.md`](../AGENTS.md). Decided in #24 |
| What is deployed on which host? | `HANDOVER_FLEET.md` (the fleet ledger) and the accepted report in `releases/` |
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

### Deployment

* **Accepted fleet report:** `releases/v0.17.2.json` (`bdbeda9`). It pins
  vq v0.26.5, vibe-qc v0.17.2 and vibe-view v2.16.2, all accepted under
  rule A. **It has not been rolled.**
* **Last recorded deployment:** the ledger's newest entry records the five
  ordinary hosts on vq v0.26.3 (`cfddd51`) under report v0.17.1. Host changes
  after that are not yet in the ledger.
* **Exception:** compute-c's own vq is at `main` `01b9ca7`, installed on
  2026-09-13 as the prerequisite for validating #37 on that host.

Nothing from v0.26.5 is proven on a host until that report is rolled, apart
from the #37 checks on compute-c described in § 2.

---

## 2. Released, not yet validated

An issue here is closed only on the evidence its own validation checklist
asks for, never on the merge. Each has a comment with the landed commit and
that checklist.

| issue | pri | what | in | validation state |
|---|---|---|---|---|
| #37 | P1 | Delegated venv update runs as a transient systemd user unit | v0.26.5 | On compute-c (logind `KillUserProcesses=yes`) the updater runs as a user unit outside every login session, and outlived the session that launched it. The brief's test, every driver session ended for 15 minutes, **has not yet been run correctly**: the first attempt left the driver polling. Pending: a rerun, then a repeat with ssh multiplexing off. **Until it passes, native rebuilds on compute-b, compute-c and compute-d still need the manual transient-unit workaround.** |
| #44 | P1 | Rollback restores `third_party/*/install`; refuses an inconsistent baseline (exit 77) | v0.26.5 | Needs a host roll. compute-c's `vibeview-dev` will refuse by design until pinned to its own checkout |
| #17 | P1 | slurm-cluster runtime publication preserves published artifacts | v0.26.4 | Only after the release-paper campaign's slurm-cluster jobs drain |
| #32 | P2 | `[hosts.X] update_script_timeout_seconds`, forwarded to delegated updates | v0.26.5 | The **driver** must run v0.26.5 before the key has any effect |
| #34 | P2 | `vq admin auto-update` detaches like `vq admin update` | v0.26.5 | Same prerequisite as #37 on the target |
| #45 | P2 | `vq programs` reports `UNHEALTHY` apart from `MISSING` | v0.26.5 | Needs the driver on v0.26.5 |
| #28 | P2 | `vq web status` / `vq doctor` probe the console's recorded interpreter | v0.26.5 | Host operator |
| #27 | P2 | An exiting process group is not reported as another user's | v0.26.5 | CI on Linux; a multi-user host |
| #40 | — | `vq web config` / `vq web run` say when the config did not load | v0.26.5 | Host operator |
| #38 | — | Config validation errors carry key and reason only (confidential) | v0.26.5 | The maintainer decides disclosure and closure |
| #26, #29 | P2 | Test-only reliability fixes | v0.26.5 | No recurrence in CI |
| #19 | P2 | STATE-3 survivor escalation | v0.26.1 | Hosts without cgroups |
| #25 | P2 | Declared extras floor; `vq web install` runtime refusal | v0.26.1 | Real hosts |

### Delivery order: dependencies, not preference

1. **Fix, or work around, #52 first.** `vq admin rollout-latest` silently plans
   against an older report when a newer one fails pin validation, for
   example when a pin checkout has not fetched. A roll that silently skips
   `v0.17.2.json` reports every lane "at target" and deploys nothing.
2. **Roll the v0.17.2 report, driver first.** On 2026-09-13 the driver's own
   update to v0.26.5 rolled back on #53. It needs a health window that
   covers the startup walk (`VQ_DAEMON_HEALTH_TIMEOUT`), or #22's spec
   reduction, before any other host rolls. After that, the driver must carry
   #32 before a host's configured cap is forwarded, and each target must
   carry #37 and #34 before a detach check means anything.
3. **Release compute-c back into the roll** once #37's remaining checks are
   recorded.
4. **slurm-cluster's lanes and #17's validation** come after the campaign drains.

---

## 3. Open work, by theme

Every item is a tracker issue. Order within a theme follows the priority
label.

### Fleet update and rollout correctness

* **#52 (P1)** — rollout-latest silently falls back to an older report; see § 2.
* **#53 (P1)** — a driver self-update rolls back a good install. The daemon
  health window (30 s + 10 ms per spec, 247 s for about 21,700 specs) is
  shorter than the daemon's startup walk over its queue, which measured more
  than 242 s cold. The rollback restarts the daemon a second time, and the
  failed attempt fences the rollout as not retry-safe.
* **#36 (P2)** — detached-build receipts are rejected during atomic
  publication. The fix, `03e8312`, is stranded on an unmerged branch.
* **#20 (P2)** — lifecycle scripts blank their own guidance: an unquoted
  heredoc runs `vq self-update` and `vq admin update`.

### Scheduler throughput and observability

On 2026-09-13 the driver's dispatch rate fell from about 375 to about 16 jobs
an hour, while the release-paper campaign had several hundred jobs pending.
**The cause is not established.** Several proposed mechanisms were tested and
disproved; the measurements are recorded on #51 and #22. Do not fix to a
mechanism that has not been measured.

* **#51 (P1)** — scheduler polls fail continuously with ssh
  banner-exchange timeouts.
* **#22 (P1)** — admission cost grows with every job ever run: terminal specs
  are never reaped. `vq cleanup --archive` does not shrink the daemon's scan;
  only deletion does, which is an owner decision (§ 4).
* **#23 (P2)** — no way to see why a scheduler job is still queued, or whether
  a request can be scheduled at all.

### Kill and lifecycle correctness

* **#18 (P2)** — `vq kill` never escalates to SIGKILL for a reattached orphan
  that ignores SIGTERM. Reproduced (note 25890). The fix sketched there waits
  on a maintainer decision (§ 4); see
  `handovers/HANDOVER_vq_kill_escalation.md`.
* **#31 (P2)** — tests for killed-job survivors in quota, memory, cleanup and
  drain. Tests landed in `fba3274` (v0.26.5); the issue is still open.

### Test reliability

* **#41** — the audit of short timeouts in `tests/`. The reaping module is
  done; `tests/test_pause_resume.py` and `tests/test_daemon.py` are next.
* **#48 (P2)** — a 1-second status-refresh budget flakes under CI load.
* **#46 (P2)** — `_dispatch_started` can read `COMPLETED` for a backgrounding
  command under load.

### Documentation

* **#39 (P2)** — `docs/vibe-qc-site` against the published vibe-qc pages.
  Fixes shipped in v0.26.5; the issue is still open.

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
project-36 issue. **Confirm they are still wanted before filing them.**

* **RECOV-4:** guard reattach against a recycled pgid. Deferred to a Linux
  host.
* **ISO-1:** cross-user read isolation. Needs a decision on the admin access
  model, and Linux validation.
* **The privileged `/opt/vq` root-daemon apply lane,** and a supported
  deployment model for compute-d's root unit. Both need an authorization
  decision.

---

## 4. Decisions waiting on the maintainer

* **#21** — triage notes on #10, #11 and #12. Its rule, that `(#N)` in a
  subject means a project-36 issue, is now written in `AGENTS.md`.
* **#30** — a fleet report cannot converge while vibe-queue and vibe-view run
  untagged `main`.
* **#43** — when release-candidate branches are deleted.
* **#47** — should `vq admin install` honour a program's declared `extras`?
* **#18** — go ahead with the reproduced kill-escalation fix for reattached
  orphans, or not.
* **#38** — disclosure of the confidential fix.
* **#22** — whether to delete old terminal job specs on the driver. This is
  an owner decision about possibly uncollected results.
* **The out-of-scope list below.** Two of its entries predate the fleet and
  now contradict current direction:
  * "distributed queue across multiple hosts": vq now drives many hosts and
    two batch schedulers from one driver;
  * "multi-tenant queues": console milestone M6 plans per-project separation.

  Decide whether each is still out of scope, and how it is bounded.

---

## 5. Future minor releases

No issue is assigned to a version. There are no GitLab milestones, and the
version of the next cut is chosen by the release coordinator when a candidate
is prepared.

[`codenames.md`](codenames.md) reserves provisional names and one-line
concepts for v0.27.0 to v0.33.0. They are image themes, not commitments. Where
a concept happens to match open work, that is noted as a possibility only:

* v0.31.0 *Stonebraker's Vacuum*, "reclaim dead space through archiving and
  automatic cleanup", is the nearest match for #22.
* v0.33.0 *Erlang's Blocking*, "counted permits enforcing admission and
  concurrency limits", is the nearest match for #23.

---

## 6. Out of scope (under review, see § 4)

* A distributed queue across multiple hosts. The original wording read:
  "SLURM exists. vq's niche is single-host informal queues."
* GPU scheduling: the runtime's job.
* Container orchestration.
* Multi-tenant or cross-organization queues.

---

## How to update this file

* **A change ships:** record it in `CHANGELOG.md` under `[Unreleased]`, not
  here. `roadmap_history.md` is frozen.
* **A fix lands that needs host or CI evidence:** add a row to § 2 with the
  release it ships in once tagged. Remove the row only when its issue is
  closed on that evidence.
* **A release is tagged, or the fleet ledger records a deployment:** update
  § 1 in the same change.
* **New work:** file the issue first, then list it under its theme in § 3.
  Items without an issue go under "Carried … not filed" only, and only until
  someone files or drops them.
* **A decision is made:** move it out of § 4 and apply it wherever it lands.
* **Keep this file short.** Rationale belongs in the issue, the design
  document or `SPEC.md`, not here.
