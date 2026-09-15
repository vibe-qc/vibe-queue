# Host recovery channels — the 3-tier contract

**Audience:** anyone setting up a new shared compute host (compute-a,
compute-d, or whatever comes next) that will join vq's fleet.
**Status:** v0.7.5 *Hopper's Compiler* contract — every host added
to `[hosts.X]` in `vq` config MUST satisfy all three tiers OR
explicitly document which it doesn't and why.

---

## Why this exists

The 2026-05-26 compute-d incident was an architectural failure, not
a bad-luck event. compute-d had **exactly one** path in from the
laptop: sshd on port 22 + the laptop's pubkey in `~queue_operator/.ssh/
authorized_keys`. When *something* (cron / unattended-upgrade /
config-mgmt — we never identified the culprit) wiped the
authorized_keys file, every administrative path closed
simultaneously:

* sshd from workstation → rejected (no key)
* sshd from compute-a → rejected (no key)
* sshd from voyager (jump-host) → rejected (no key)
* root ssh → hardened off
* Cockpit / web admin → not installed
* IPMI / BMC → not configured

Recovery cost: **one calendar month** (waiting for physical
access). The science envs kept running because the queue daemon
was already up, but every administrative action was gone.

The fix is defense-in-depth: **multiple independent channels** so
losing one (or even two) doesn't lock you out. The contract below
codifies what "independent" means for vq's fleet.

---

## The three tiers

### Tier 1 — Hardware management (mandatory on server hardware)

**Channel:** IPMI / iDRAC / BMC / equivalent. Whatever your
hardware exposes for out-of-band management — Dell iDRAC,
Supermicro IPMI, HPE iLO, ASUS ASMB, etc.

**Independent of:** Everything below the BMC firmware. Survives
OS reinstall, kernel panic, full disk corruption, sshd config
wipe, network namespace mishaps.

**Setup:**

1. Configure the BMC on its own IP address (separate from the
   host's primary IP). Most server-class machines have a dedicated
   management port — wire it.
2. Set a strong password (treat it like a root password — store
   in 1Password / bitwarden / your usual credential vault).
3. Enable HTTPS web UI; disable HTTP redirect. The web UI's
   "Virtual Console" / "iKVM" / "Remote Console" feature is your
   keyboard-+-monitor-equivalent.
4. **Test it from your laptop before declaring the host
   provisioned.** Open the web UI, log in, launch the virtual
   console, see a login prompt, log in to the OS, log out.
5. Record the BMC IP + credential-vault entry in your provisioning
   notes.

**When skipped:** workstation/desktop class hardware often lacks a
BMC entirely. Document this explicitly in the host's vq config
comment — `# Tier 1: N/A (workstation hardware, no BMC)` — so
future-you knows the gap exists.

### Tier 2 — Cockpit web admin (mandatory on every host)

**Channel:** Cockpit on `https://<host>:9090/`. Browser-based
remote admin; PAM auth (Linux account password, NOT SSH keys);
includes a "Terminal" pane that gives you a real shell.

**Independent of:** sshd entirely. Cockpit is a separate daemon
(`cockpit.socket` + `cockpit.service`) listening on its own port,
using PAM (not the SSH key system). When sshd is broken / locked
out, Cockpit usually still works.

**Setup:** see [`contrib/setup-recovery-channels.sh`](../contrib/setup-recovery-channels.sh).
Idempotent; safe to re-run. The short version:

```sh
# Debian / Ubuntu
sudo apt install cockpit
sudo systemctl enable --now cockpit.socket

# Fedora / RHEL / Arch
sudo dnf install cockpit  # or pacman -S cockpit
sudo systemctl enable --now cockpit.socket
```

Verify:
```sh
sudo ss -lntp | grep 9090   # cockpit should be listening
curl -ksS -I https://localhost:9090/ | head -1   # 200 OK
```

**Browser access from your laptop:** `https://<host>:9090/`. If
that lands on a firewall, either open the port at the router
(temporarily, for a recovery session) or SSH-tunnel:

```sh
ssh -L 9090:localhost:9090 -N <host>
# Then browser to https://localhost:9090/
```

The SSH-tunnel path means Cockpit is reachable as long as sshd is
up at all — even if your normal user account's key trust is
broken, a single working ssh path (recovery sshd, tier 3) is
enough to forward.

### Tier 3 — Recovery sshd (mandatory on every host)

**Channel:** sshd on an alternate port (default `22222`) with a
**separate `AuthorizedKeysFile` in `/etc/ssh/`** — root-owned,
mode 0600, contains only your *recovery* key (a separate keypair
you keep in cold storage, NOT your day-to-day laptop key).

**Independent of:** Your normal user account's `~/.ssh/`. No
user-level process can wipe `/etc/ssh/recovery_authorized_keys`
(it requires root). No `pip install` / `ansible-pull` /
`cloud-init` running as your user can affect it. Even if your
primary account's home dir gets nuked, this path stays open.

**Setup:** see [`contrib/setup-recovery-channels.sh`](../contrib/setup-recovery-channels.sh).
Set `RECOVERY_USER` to your chosen local recovery account explicitly. For example:

```sh
sudo env RECOVERY_USER=queue_operator bash contrib/setup-recovery-channels.sh \
  --recovery-key-path /path/to/recovery.pub
```

The script validates the account setting before provisioning. It writes a
`/etc/ssh/sshd_config.d/recovery.conf` like:

```
# v0.7.5 Hopper's Compiler — recovery sshd block.
# DO NOT REMOVE without first verifying tier 1 (BMC) and tier 2
# (Cockpit) both work.

Port 22                # primary sshd (unchanged)
Port 22222             # recovery sshd

Match LocalPort 22222
    AuthorizedKeysFile /etc/ssh/recovery_authorized_keys
    AllowUsers queue_operator
    PasswordAuthentication no
    KbdInteractiveAuthentication no
    PermitRootLogin no
    # Hardened: this port exists ONLY to let you in when the
    # primary path is broken. No root, no password, only the
    # specific recovery key.
```

And places your **recovery pubkey** at `/etc/ssh/recovery_authorized_keys`:

```sh
sudo cp ~/.ssh/id_ed25519_vibeqc-recovery.pub /etc/ssh/recovery_authorized_keys
sudo chmod 600 /etc/ssh/recovery_authorized_keys
sudo chown root:root /etc/ssh/recovery_authorized_keys
sudo systemctl reload ssh   # or sshd, depending on distro
```

**Generate the recovery key on your laptop** (one-time, before
provisioning the first host):

```sh
ssh-keygen -t ed25519 -f ~/.ssh/id_ed25519_vibeqc-recovery \
           -C "vibeqc-recovery-$(date -u +%Y%m%d)" \
           -N ""   # no passphrase OR a passphrase you record in your vault
```

Treat the recovery key like a glass-break — used only in recovery
scenarios. Don't add it to your default ssh-agent. Use it
explicitly:

```sh
ssh -i ~/.ssh/id_ed25519_vibeqc-recovery -p 22222 queue_operator@<host>
```

### Tier failure modes — when each saves you

| Failure mode | Tier 1 (BMC) | Tier 2 (Cockpit) | Tier 3 (Recovery sshd) |
|--------------|--------------|------------------|------------------------|
| Primary sshd wiped your `~/.ssh/authorized_keys` (the compute-d case) | ✓ | ✓ | ✓ |
| sshd_config corrupted, sshd won't start | ✓ | ✓ | ✗ (same daemon) |
| Network unreachable | ✓ (if BMC has separate net) | ✗ | ✗ |
| Kernel panic / OS crash | ✓ | ✗ | ✗ |
| Disk corruption | ✓ (boot from media) | ✗ | ✗ |
| Hardware failure | ✗ | ✗ | ✗ |

In the compute-d case, **all three tiers would have saved us** —
the actual fix is "ssh in via tier 3 with the recovery key and
restore the primary authorized_keys."

---

## Operational rules

1. **Test all configured tiers at provisioning time.** Don't
   trust them just because the install script ran. The whole
   point of having them is that they work *when you need them*;
   verify before they're stressed.
2. **Re-test annually.** Or at every fleet OS upgrade. Whichever
   is sooner. Recovery channels rot the same as anything else.
3. **Document the BMC IP + credential-vault entry** in your
   provisioning notes. Future-you needs to find them in a
   3am-stressed state, not under "memory."
4. **Run `vq admin audit-recovery HOST` before declaring the
   host healthy.** That verb (v0.7.5) probes each tier and
   reports green/yellow/red. Treat any non-green tier as a
   ship-blocker.
5. **Don't disable any tier "temporarily for debugging"** without
   leaving a `vq admin status --verbose` note explaining what's
   off and when it'll be restored. Half-disabled recovery is
   worse than missing recovery — gives a false sense of safety.

---

## What the new machine needs before joining the fleet

Concrete pre-flight checklist for any new shared compute host:

- [ ] **Hardware:** BMC configured + tested. Web UI reachable.
      Virtual Console launches. Credentials in vault.
- [ ] **OS install:** standard distro (Ubuntu / Debian / Arch),
      queue_operator user account created, locale/timezone set, sshd active
      and accepting the laptop's primary key.
- [ ] **Cockpit:** installed + enabled + listening on 9090 +
      reachable via SSH tunnel + verified with a browser-side
      login.
- [ ] **Recovery sshd:** `/etc/ssh/sshd_config.d/recovery.conf`
      in place, `/etc/ssh/recovery_authorized_keys` populated
      with the recovery pubkey, recovery key tested from laptop:
      `ssh -i ~/.ssh/id_ed25519_vibeqc-recovery -p 22222 queue_operator@<host>` works.
- [ ] **Firewall:** port 22, 22222, and (optionally on a trusted
      LAN) 9090 reachable from your network position. Public
      exposure of 22222 is fine (auth-restricted to a single
      recovery key); 9090 should NOT be public long-term (PAM is
      a password-attack surface).
- [ ] **vq admin audit-recovery HOST returns all-green.**
- [ ] **vq registry:** `[hosts.X]` block in your laptop's
      `~/.config/vq/config.toml` configured with primary ssh
      settings.
- [ ] **vq daemon:** deployed via the standard multi-user
      install path (see `docs/multi_user_deployment.md`).
- [ ] **Smoke test:** submit a tiny `vq submit` job, verify it
      runs, terminates cleanly, leaves the workdir.

Only after every box is ticked does the host go in your
muscle-memory inventory of "machines I can use."

---

## Pointers

- [`contrib/setup-recovery-channels.sh`](../contrib/setup-recovery-channels.sh) — the idempotent bootstrap script
- `vq admin audit-recovery HOST` — the audit verb (v0.7.5)
- `docs/multi_user_deployment.md` — the existing multi-user daemon install runbook (adjacent but separate concern)
- `HANDOVER_compute-d_RECOVERY.md` — the dropbox doc capturing the 2026-05-26 incident (delete after compute-d is fixed)
- CLAUDE.md § 15 — the agent-protocol rule that points dev chats at this contract for host operations
