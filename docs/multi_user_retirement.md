# Retiring multi-user mode on compute-d and compute-a

**Status: ON HOLD (maintainer, 2026-08-05). Nothing here has run, and nothing
here should run for now.**

The maintainer's direction is to stand back from root operations entirely:
they have caused too much trouble, and may be revisited at a later release.
That applies to this plan as well as to the machinery it would retire —
note the awkward shape of it, because it is the reason this is *on hold*
rather than *cancelled*:

> Retiring multi-user is what *ends* recurring root operations, but the
> transition itself needs a handful of one-time ones (steps 2–5: `sudo tar`,
> `sudo sed`, `sudo systemctl`). Standing back from root work therefore also
> defers the change that would make root work unnecessary.

**What holding costs, stated plainly:** compute-d and compute-a keep a root-owned
`/opt/vq` daemon that owns the real queue state and that no sanctioned
non-root path can refresh. It will fall further behind the accepted report
with every release. That is a known, accepted, bounded cost — not a surprise
to rediscover later — and it is the argument for picking this back up at a
release where a short root window is acceptable.

**Do not execute any step below without fresh approval.** When it is picked
up again, re-verify every command against the tree as it stands then; parts
of this will have gone stale.

Decision (2026-08-05, maintainer): every job on compute-d and compute-a originates
from a single operator account, including dev-chat submissions. Multi-user
mode's per-user privilege drop and per-user quotas are therefore buying
nothing, while their operational cost is the largest recurring item in the
release process.

This plan retires the **deployment**. It deliberately does not delete the
**code** -- see § "What is explicitly not in scope".

---

## What this buys

Each of these is a documented failure or standing cost that disappears
entirely, not one that gets easier:

* **`docs/fleet_update_runbook.md` § 3b stops existing.** Today compute-d and
  compute-a fail their vq lane on *every* release by design, because
  `vq admin update` cannot reach the root-owned `/opt/vq` install. That is the
  single largest recurring cost in a rollout.
* **The `/opt/vq` privileged lane goes away**, and with it the
  `vq-multi-user-refresh` helper, the `/etc/sudoers.d` fragment, and the
  open question of whether granting `vq-admins` NOPASSWD is acceptable. That
  decision simply stops needing an answer.
* **The split-store class of bug becomes unreachable.** `remote_vq` must point
  at a wrapper exporting `VQ_CONFIG_DIR` / `VQ_STATE_DIR`, or the CLI reads the
  per-user store while canonical writes go to the system one -- updates report
  `success: True` with no work errors and `LAST OK` never advances. Cost:
  hours, 2026-07-26.
* **`admin_token_file` and the 0600 token copy stop being required**, along
  with `PermissionError: admin token required in multi-user mode`.
* **`/var/lib/vq` `root:vq-admins` 2775 stops mattering**, along with the
  marker-acquisition failure that shape produces.
* **The dual-daemon routing fault becomes unreachable.** `_verify_restarted_daemon`
  carries a branch specifically because a provenance ping could land on the
  root daemon instead of the user daemon it restarted (compute-a, then compute-d,
  2026-08-02).
* **Six of the eight `vq admin provision` checks become "not applicable"** --
  which is also the cheapest confirmation that the switch worked.

## What it costs

Read these before approving; two are irreversible.

1. **Job history under `/var/lib/vq/users/<uid>/` does not migrate.**
   Single-user state lives under `~/.local/share/vq`; the multi-user daemon
   uses a different root and **there is no migration tool** (verified: nothing
   in `src/vq/` or `scripts/` implements one). The runbook records compute-a holding
   roughly 1398 jobs / 5540 queue rows / 2332 workdirs. Switching daemons
   strands all of it. `deploy-multi-user.sh` refuses the *opposite* switch on a
   non-empty queue for exactly this reason.
   **Mitigation in step 2 below: drain to zero, fetch what you want, then cold-archive
   the tree. Do not skip this and expect to come back for it.**
2. **Per-job privilege drop ends.** Every job will run as the daemon's own
   user. This is acceptable *only* under the single-operator premise above. If
   that premise ever stops holding, this must be reversed before anyone else
   submits.
3. **Per-user quotas end.** `docs/multi_user_deployment.md` notes the quota is
   usually the *tightest* of the three dispatch gates on a multi-user host --
   compute-d has run 32 physical cores with `max_cpus = 32` and a lower quota.
   Removing the quota raises effective concurrency and edges back toward the
   2026-05-16 host-wedge / OOM class. Step 5 re-tunes `--max-cpus` deliberately
   rather than letting it drift upward by omission.

---

## Sequence

Per host, one host at a time, **compute-d first**. Do not start compute-a until
compute-d has run a full release cycle cleanly.

### 1. Confirm the premise on that host

```sh
ssh <host> 'sudo ls -1 /var/lib/vq/users/'
```

Every uid listed is an account that has submitted. If this shows more than
your own uid, **stop** -- the single-operator premise is false for this host
and the whole plan is void.

### 2. Drain, harvest, archive

```sh
vq drain --max-jobs 0 --reason "multi-user retirement" --duration 24h <host>
vq queue <host> --active                       # wait until empty
vq fetch-all <host> -o ~/vq-harvest/<host>/    # pull results you still want
```

Then cold-archive the state tree. This is the only copy that will exist:

```sh
ssh -t <host> 'sudo tar -czf /var/tmp/vq-multiuser-state-$(date +%Y%m%d).tar.gz -C /var/lib/vq users'
# copy it somewhere durable, off the host
```

**Do not proceed until the queue is empty and the archive is off the host.**

### 3. Flip the mode switch

One key decides multi-user for both the daemon and the CLI
(`config.system_multi_user_enabled()` reads `/etc/vq/config.toml` and ORs it
into the client's decision, so client and daemon cannot disagree):

```sh
ssh -t <host> 'sudo sed -i "s/^enabled *= *true/enabled = false/" /etc/vq/config.toml'
ssh <host> 'grep -A2 "\[multi_user\]" /etc/vq/config.toml'   # verify
```

Leave the rest of `/etc/vq/config.toml` in place. Flipping the flag is
reversible; deleting the file is not.

### 4. Swap the daemons

This is the rollback path from `deploy-multi-user.sh`, run forwards:

```sh
ssh -t <host> 'sudo systemctl disable --now vq-daemon-multi-user.service'
ssh <host> 'systemctl --user enable --now vq-daemon'
ssh <host> 'systemctl --user status vq-daemon --no-pager | head -5'
```

### 5. Re-tune the CPU budget

The per-user quota is gone; the daemon's own cap is now the only gate. Set it
deliberately -- do not leave it at whatever the multi-user config implied:

```sh
ssh <host> 'nproc'
# edit the user unit's --max-cpus to the value you actually want, then:
ssh <host> 'systemctl --user daemon-reload && systemctl --user restart vq-daemon'
```

### 6. Repoint the driver config

On the driver, for that host in `config.toml`:

* `remote_vq` -- back to plain `vq` (the wrapper existed only to force the
  multi-user store).
* `admin_token_file` -- unset.

### 7. Verify

```sh
vq doctor <host>
vq admin provision <host> --check          # multi-user checks -> "not applicable"
vq admin status <host> --json
vq drain --release <host>
vq submit <host> -c 1 --wall-time-seconds 60 -- bash -c 'id'   # runs as you, expected
vq admin rollout-latest --verify-only --only <host>
```

The `provision --check` line is the real confirmation: `remote_vq_wrapper`,
`admin_token_file`, `state_root_perms`, `root_owned_install`, `refresh_helper`
and `delegation` should all report *not applicable*, and
`multi_user_mode` should report the host as single-user **from the target's own
config**, not from a driver fallback.

### 8. Close the loop in the docs

Only after both hosts are switched and one release cycle has passed clean:

* `docs/fleet_update_runbook.md` § 3b -- mark historical; it no longer applies
  to any host.
* `docs/operations.md` -- the `/opt/vq` refresh section becomes historical.
* `HANDOVER_FLEET.md` -- record the switch and the archive location.

---

## Rollback

Reversible at every step up to the point the state archive is discarded:

```sh
ssh -t <host> 'sudo sed -i "s/^enabled *= *false/enabled = true/" /etc/vq/config.toml'
ssh <host> 'systemctl --user disable --now vq-daemon'
ssh -t <host> 'sudo systemctl enable --now vq-daemon-multi-user.service'
```

Then restore `remote_vq` / `admin_token_file` on the driver. Jobs submitted
while in single-user mode stay in `~/.local/share/vq` and will not be visible
to the restored multi-user daemon -- the same one-way state boundary as step 2,
in the other direction.

---

## What is explicitly not in scope

**Do not delete the multi-user code.** It is 1647 references across 74 source
files plus 15 dedicated test files, touching ownership, spec validation,
privilege drop, quotas, cleanup, pause/resume, throttle, fetch, kill, audit,
`paths` and `cgroup`. Removing it is a grand refactor in the sense of
`CLAUDE.md` § 9 and needs its own reviewed change -- and doing it before the
single-user deployment has proven out would delete the rollback while still
depending on it.

Specifically keep:

* `contrib/deploy-multi-user.sh` -- it *is* the rollback.
* `contrib/vq-multi-user-refresh` and its sudoers fragment -- harmless once
  unused, and needed again if the premise changes. The sudoers rule should
  simply never be installed (it is already opt-in).
* Everything under `src/vq/` that branches on `multi_user`.

Revisit deletion only after both hosts have run single-user through at least
one full release cycle, and treat it as a separate proposal.
