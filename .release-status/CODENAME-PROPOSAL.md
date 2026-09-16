# vq codename backfill: v0.13.0 - v0.25.0

> **Approved by the maintainer on 2026-09-09, as proposed.** Every name below is
> settled, including the two resolutions at the bottom. `src/vq/codename.py`
> is the authoritative catalog; this file records *why* each name was chosen,
> which the catalog's inline comments summarise but do not argue.
>
> A tag is immutable. Amending a name is a one-line edit until the release
> carrying it is tagged, and impossible afterwards.


Convention (docs/version_compatibility.md, "v0.7.x onward"): CS pioneers,
`[surname]'s [object]`, object names the concept the person is known for AND
maps to the release's substance. Picks happen at ship time, not pre-reserved.

Substance reconstructed from the monorepo bump commits
(`git log -- vibe-queue/src/vq/__init__.py` in a clone of the frozen
`mpei/vibeqc` monorepo, project 19).

| Version | Date | What shipped | Proposed | Theme link |
|---|---|---|---|---|
| 0.13.0 | 07-24 | job lifecycle timeline (`vq events`), pending-reason diagnostics | *Gray's Log* | Jim Gray, write-ahead logging: the durable ordered record of every transition, which is exactly what `vq events` exposes. (Gray's Transaction, v0.8.13, is a different object.) |
| 0.14.0 | 07-24 | `vq doctor --as-driver`, post-restart provenance ping | *Needham's Principal* | Roger Needham, authentication: a principal is the identity on whose behalf an action is checked. `--as-driver` runs the check as a different principal. |
| 0.15.0 | 07-24 | doctor attributes scheduler dispatch stops to their authority; detached builds survive SSH drops | *Schroeder's Authority* | Saltzer & Schroeder 1975, protection domains: "authority" is their vocabulary, and attributing a stop to the authority that caused it is that idea applied to dispatch. |
| 0.16.0 | 07-25 | fleet console M0/M1: fleet mode, live cross-host drill-down | *Sutherland's Sketchpad* | Ivan Sutherland, Sketchpad (1963), the first interactive graphical view of a system's state. Sits beside Engelbart's Demo (v0.7.2). |
| 0.17.0 | 07-25 | console M1 complete + M2 (accounts, sessions, audited writes); QVF containers as jobs | *Corbató's Password* | Fernando Corbató, CTSS, credited with the first computer password. Accounts and sessions arrive on the console. (Corbató's Daemon, v0.8.15, different object.) |
| 0.18.0 | 07-26 | rollout orchestration (`rollout-latest`), per-host delegated-admin auth, capacity caps | *Abadi's Delegation* | Martin Abadi, co-author of "Authentication in Distributed Systems" (1992), the canonical delegation paper. Per-host delegated admin is literally delegation. |
| 0.19.0 | 07-26 | release-only CI, report-pinned rollouts, exact-SHA evidence, `--expected-sha` staging | *Merkle's Hash* | Ralph Merkle, hash trees: the SHA *is* the evidence. A rollout that will not stage anything but the pinned commit is content-addressed deployment. |
| 0.20.0 | 07-26 | scheduler runtime update stops draining/waiting for running work; admission fix | *Herlihy's Wait-Free* | Maurice Herlihy, wait-free synchronization: an operation completes without waiting on any other. The update path becomes wait-free with respect to running jobs. |
| 0.21.0 | 07-27 | per-SHA runtime slot layout for venv hosts | *Kilburn's Page* | Tom Kilburn, Atlas, virtual memory: one name maps to different physical frames. One program name, one on-disk slot per SHA. |
| 0.22.0 | 07-28 | `default_max_concurrent_cpus` in `vq overview` | *Little's Law* | J.D.C. Little, L = lambda W: the relation an operator needs to reason about a queue. The quota that gates dispatch stops being invisible. |
| 0.23.0 | 07-28 | `runtime_slot_root` config key, opt-in per program | *Saltzer's Binding* | Jerry Saltzer, "On the Naming and Binding of Network Destinations" (1982). The key is a declared binding from a program to its slot root, not an implicit one. |
| 0.24.0 | 07-29 | slot ladder: materialize at exact commit, build beside the live venv, flip on success, reclaim unused | *Cheney's Semispace* | C.J. Cheney's copying collector: build the new space beside the old, flip, reclaim what nothing references. The closest one-to-one fit in the set. |
| 0.25.0 | 08-13 | managed exact self-update, durable `admin recover-update`, failure-atomic lifecycle transactions | *Haerder's Atomicity* | Haerder & Reuter (1983) coined ACID and wrote the transaction-recovery paper. A lifecycle transition either completes or leaves a recoverable state. |

## Two existing lines need resolving, not backfilling

The catalog needs exactly one name per minor. Two lines carry several,
because names were attached to feature bullets rather than to the release.

* **v0.11.0** had four: *Baran's Detour*, *Birman's Sweep*, *Eager's Sharing*,
  *Hamilton's Restart*. Its bump commit is bookkeeping, so there is no
  flagship to read off it. **Resolved to *Eager's Sharing*** -- Eager, Lazowska
  & Zahorjan's "Adaptive Load Sharing in Homogeneous Distributed Systems"
  (1986) is literally the paper on pooling homogeneous hosts, and `[pools]`
  + `default_pool` is the line's most structural addition.

* **v0.12.0** had five: *Ritchie's Signal*, *Thompson's Symlink*,
  *Dijkstra's Deadline*, *Hollerith's Return*, *Hopper's Bug*. Its bump commit
  says `harden daemonless scheduler hosts`, which none of the five names.
  **Resolved to *Hopper's Bug*** -- the line's unifying achievement is that a
  host with no resident daemon became diagnosable when it fails. This was the
  one genuinely arguable pick in the set, and it was approved as proposed
  rather than settled by default; if anything in the series is ever revisited,
  start here.

The unpicked names stay in `docs/STATUS.md` as the per-feature labels they
already are; nothing is deleted.

## v0.10.0 is already settled

Its bump commit subject carries the name: `(v0.10.0 Lampson's Hint)`.

## Mechanism (landed in `2e01565`)

1. `RELEASE_CODENAMES: dict[str, str]` in `src/vq/__init__.py` (or a new
   `src/vq/codename.py`), shaped like vibe-qc's `banner.py`: keyed by X.Y.Z,
   minors always present, patches may override, otherwise inherit.
2. `codename_for_version()` with PEP-440 suffix stripping
   (`.devN` / `aN` / `bN` / `rcN`), same contract as vibe-qc's.
3. Surface in `vq --version`: `vq 0.25.7 "Haerder's Atomicity"`.
   Nothing surfaces one today.
4. CHANGELOG headers: `## [0.25.0], 2026-08-13, "Haerder's Atomicity"`.
5. Annotated tag titles carry the codename; tagger `mpei@vibe-qc.com`.
6. Fold the stale tracker table in `docs/version_compatibility.md` (it stops
   at v0.8.10 with a "v0.8.11+ TBD" row) into the generated catalog, or point
   it at the catalog so there is one source of truth.
