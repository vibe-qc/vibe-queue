# Reaching the queue from the internet

How to submit jobs to a vq queue host that lives behind a home/lab
router, from anywhere — securely.

The short version: **SSH on a non-standard port, key-only auth, and vq
needs no code changes.** vq's entire transport is already SSH (`ssh
HOST vq <verb>` for everything; `scp` for `vq fetch`). Make SSH
reachable and hardened, and vq is reachable and hardened — for free.

This is not the web dashboard. The dashboard (`vq web`) is read-only
plus a few kill/pause/resume endpoints; it has no submit path and is
**not** meant to face the internet. See [Do not expose the
dashboard](#do-not-expose-the-dashboard) below.


## The model

```
                            router
   internet ───────────▶  ext port E  ──forward──▶  compute :22 (sshd)
                                                         │
   laptop ~/.ssh/config:                                 │
     Host compute                                        │
       HostName compute.example.com                      │
       Port E                                            │
       User myuser                                       │
       IdentityFile ~/.ssh/compute_ed25519               │
                                                         │
   vq ~/.config/vq/config.toml:                          │
     [hosts.compute]                                     │
       ssh = "compute"   ◀── alias, resolved by ─────────┘
                              ~/.ssh/config
```

Three layers, each doing one job:

1. **Router** forwards an external port `E` to the queue host's sshd.
   `E` is not 22 (and not 80/443 if those are taken by another box).
2. **sshd on the queue host** is hardened to key-only auth — that's
   the security boundary.
3. **`~/.ssh/config` on the laptop** ties the `compute` alias to the
   public domain + port `E` + the right key. vq's `config.toml` keeps
   `ssh = "compute"` unchanged — it never needs to know the port.

That last point is the crux: **vq stays transport-agnostic.** The port
lives in `~/.ssh/config`, not in vq's config. `vq.transport` runs
`ssh <alias>` and `scp`; both honour `~/.ssh/config`. Nothing in vq
changes when the queue host moves onto the internet.


## Choosing the external port

Use a **high port in the dynamic/private range 49152–65535.** Reasons:

- **Low ports get scanned.** Automated scanners sweep the whole low
  range — port 24, 28, 222, 2222 all get probed nearly as hard as 22.
  Picking a low or "obvious alt-SSH" port earns you none of the
  noise-reduction benefit.
- **The dynamic range is never assigned to registered services**, so
  there's no collision risk with anything else on the host or network.
- **The port is not a security control.** Key-only auth (below) is the
  security. A non-standard port is *purely* log-noise reduction — it
  keeps your auth log from filling with drive-by scan attempts. So the
  only thing that matters is "high and unassigned."

Pick any number in 49152–65535 you'll remember. This doc uses `E` as a
placeholder; substitute your real choice everywhere.


## Walkthrough

Do these **in order.** Step 3 verifies key auth works *before* step 4
disables passwords — skip the ordering and you can lock yourself out.

### 1. Generate a dedicated SSH key (on the laptop)

```sh
ssh-keygen -t ed25519 -f ~/.ssh/compute_ed25519 -C "vq-queue compute"
```

- **ed25519** — modern, fast, short keys.
- **Dedicated key** — not a key you reuse for GitHub/other hosts. If it
  leaks you revoke exactly one thing.
- **Use a passphrase.** The key is a code-execution credential (see
  [Security model](#security-model)); a passphrase + ssh-agent means
  you type it once per laptop boot, and a stolen key file alone is
  useless.

### 2. Install the public key on the queue host

While you still have working SSH access (password or an existing key),
on a normal port:

```sh
ssh-copy-id -i ~/.ssh/compute_ed25519.pub compute
# or, if ssh-copy-id isn't available:
#   cat ~/.ssh/compute_ed25519.pub | ssh compute 'mkdir -p ~/.ssh && cat >> ~/.ssh/authorized_keys && chmod 600 ~/.ssh/authorized_keys'
```

### 3. Verify key auth works — BEFORE hardening

```sh
ssh -i ~/.ssh/compute_ed25519 -o PasswordAuthentication=no compute hostname
```

This **must** print the hostname. The `-o PasswordAuthentication=no`
forces key-only for this one connection, proving the key works on its
own. If it fails, fix it now — do not proceed to step 4.

### 4. Harden sshd on the queue host

vq ships a ready drop-in at
[`contrib/sshd_config.d/vq-hardening.conf`](../contrib/sshd_config.d/vq-hardening.conf).
It disables password auth, scopes login to the queue user, and trims
unused SSH features. On the queue host:

```sh
sudo cp ~/path/to/vibe-queue/contrib/sshd_config.d/vq-hardening.conf \
        /etc/ssh/sshd_config.d/
sudo sshd -t                        # validate — must print nothing
sudo systemctl reload sshd
```

**Keep your current SSH session open** while you do this. Then, in a
*second* terminal, open a fresh connection to confirm it still works:

```sh
ssh -i ~/.ssh/compute_ed25519 compute hostname
```

If the fresh connection fails, your still-open first session can undo
it (`sudo rm /etc/ssh/sshd_config.d/vq-hardening.conf && sudo systemctl
reload sshd`).

The drop-in ships a single `AllowUsers` entry as a placeholder — set it
to your queue host's account before you reload sshd, and keep the first
session open while you check.

### 5. Router: forward the external port

In the router admin UI, add a port-forward rule:

```
external TCP  E   ─▶  <queue host LAN IP> : 22
```

(`22` because the drop-in leaves sshd on 22 internally; the
non-standard port lives only in the forward + `~/.ssh/config`. If you
prefer sshd itself on a non-22 port, set `Port E` in the drop-in and
forward `E -> E` instead — see the comment in that file.)

The DNS `A` record (`compute.example.com -> public IP`) you've
already set up handles name resolution; the router handles the port.

### 6. Point the laptop's `~/.ssh/config` at it

Add (or edit) the `compute` host block in `~/.ssh/config`:

```
Host compute
    HostName compute.example.com
    Port E
    User myuser
    IdentityFile ~/.ssh/compute_ed25519
    IdentitiesOnly yes
```

`IdentitiesOnly yes` makes ssh offer *only* this key, not every key in
your agent — cleaner, and avoids `MaxAuthTries` exhaustion if you have
many keys loaded.

### 7. Verify end-to-end

```sh
ssh compute hostname                 # plain SSH via the alias
vq programs                          # vq's SSH transport, through the alias
vq queue --active                    # the real thing
```

`vq` should work identically to how it did on the LAN — because to vq,
nothing changed. Same alias, same commands; `~/.ssh/config` quietly
swapped the LAN address for the public domain + port.


## Diagnosing a broken chain

Add a bastion and every failure starts to look the same. OpenSSH reports
a failure *anywhere* in a proxied chain against the **final target**:

```
ssh: connect to host pbs-cluster port 22: Connection closed by UNKNOWN port 65535
```

That message is compatible with the local link being down, the jump host
being down, the target being down, and the key being rejected. The line
that names the real cause is one row up in `ssh -v`, which is not where
anyone looks first.

`vq doctor HOST` starts with a **local leg** that answers this before it
tries to reach anything:

```bash
vq doctor pbs-cluster
```

```
== vq doctor: pbs-cluster ==
OK config: configured ssh='pbs-cluster' scheduler='pbs'
OK ssh_route: 'pbs-cluster' -> ProxyJump 'gateway' -> gw.example.org:22 -> user@pbs-cluster.example.org:22
FAIL ssh_first_hop: jump host 'gateway' gw.example.org:22: connection refused
  verdict: gw.example.org:22 refused the connection, so the jump host is up
    but its sshd is not accepting on that port
  next: nothing is wrong with vq or your key. Check sshd on gw.example.org
    (or the port in ~/.ssh/config); the target host was never contacted.
verdict: failed
```

Three checks make up the leg:

- **`ssh_route`** asks OpenSSH itself (`ssh -G`) what the alias resolves
  to, and whether a `ProxyJump` or `ProxyCommand` sits in front of it. A
  nested jump chain is walked to the endpoint your machine dials first.
  This one is worth reading even when everything works: it makes an
  otherwise invisible bastion visible.
- **`ssh_first_hop`** opens a bare TCP connection to that endpoint. It
  runs below the SSH layer, so it separates "no path to the host" from
  "the host rejected my key" without offering a credential to anything.
  When it fails, doctor stops there. The remote checks would only
  reproduce the same failure, slower.
- **`ssh_transport`** appears when the first hop answered but ssh still
  failed. Doctor spends one `ssh -v` and reports a named verdict plus
  the transcript lines that actually name a cause, including whatever a
  `ProxyCommand` wrote to its own stderr.

A `ProxyCommand` is an opaque local program, so vq cannot know its first
hop and says so rather than guessing. The route line still prints the
command, and the `ssh -v` transcript still classifies the failure.

One case where a dead first hop is *not* the end of the story: if your
config uses `ControlMaster` / `ControlPersist`, an established master
socket keeps working after the route underneath it dies. Doctor checks
for a live master (`ssh -O check`, a local question) before it stops, and
when one exists it still reports the dead hop but runs the remote checks
anyway, because those are the only things that can say what vq can reach
right now. Expect that host to fail once the master expires.

`vq doctor --all` probes a shared bastion once, not once per host, so a
fleet behind one gateway costs one probe.

None of this knows what a VPN is. "First hop unreachable" is the
actionable verdict either way: check the local link, then the gateway.


## Security model

**`vq submit` runs arbitrary code on the queue host** as the queue
user. `vq submit foo.py` runs `foo.py`; `vq submit -d dir -- bash x.sh`
runs `x.sh`. That's the nature of a job queue, not a flaw — but it
means:

> **The SSH private key is a code-execution credential for the host.**
> There is no "submit jobs but can't run arbitrary code" mode —
> submitting a job *is* running code.

So treat `~/.ssh/compute_ed25519` like a root password:

- Passphrase-protected, loaded into `ssh-agent` (type it once per boot).
- Dedicated to this host — never reused, never committed to a repo.
- If a laptop holding it is ever lost: remove the pubkey line from the
  host's `~/.ssh/authorized_keys` immediately, generate a fresh key.

What you get in exchange for that discipline: key-only auth on a
non-standard port is a *small, well-understood* attack surface. It's
the same sshd the whole internet relies on, with passwords off.

**Also recommended on the queue host:**

- **fail2ban** (or sshguard) — bans IPs after repeated auth failures.
  Cheap insurance against scan volume; likely already installed.
- Keep the OS + `openssh-server` patched.


## Do not expose the dashboard

The web dashboard (`vq web run`, the FastAPI app) is **not** part of
this. It's read-only HTML plus bearer-token-gated kill/pause/resume —
no submit endpoint — and exposing it publicly would mean a second
public port, its own TLS certificate, and the bearer token as a
(weaker-than-an-SSH-key) credential.

If you want the dashboard in a browser while away from the LAN,
**tunnel it over the SSH connection you already have** — zero extra
public exposure:

```sh
ssh -L 8765:localhost:8765 compute
# leave that running, then open http://localhost:8765 in your browser
```

(The `vq-hardening.conf` drop-in deliberately leaves
`AllowTcpForwarding` at its default so this works.)


## Multiple submitters (later)

Today the model is single-user: one account, one key, one `AllowUsers`
entry in the sshd drop-in. Adding a second authorized submitter is:

1. Append their public key to the queue host's
   `~/.ssh/authorized_keys` (or give them their own account).
2. Add their account to `AllowUsers` in the drop-in if it's a separate
   account.

vq's `JobSpec` already records a `submitter` field, so per-job
attribution works. Per-user *quotas* and ownership checks on
`kill`/`fetch` are a genuine feature — tracked on the roadmap under
v0.6 multi-user — but they aren't needed while it's just you.
