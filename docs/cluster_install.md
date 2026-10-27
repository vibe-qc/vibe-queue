# Installing vq on machines and clusters

**Audience: you are setting up vq for yourself or a group, on a workstation,
a server, or an HPC cluster.** No root access is needed for any of it.

vq runs the same code everywhere, but a machine takes one of three roles.
Decide the role of each machine first; everything else follows from it.

| Role | What runs there | Typical machine |
|---|---|---|
| **Client** | only the `vq` command | your laptop |
| **Daemon host** | `vq` plus the always-on vq daemon, which dispatches jobs itself | a workstation or server with no batch scheduler |
| **Scheduler host** | `vq` as a helper, **no daemon**; PBS/Torque or Slurm dispatches | a cluster login node |

A scheduler host needs one daemon host as its **driver**. The driver's daemon
writes the batch script, submits it with `qsub` or `sbatch` over SSH, polls
the scheduler and brings the results back. The driver is often the same
workstation that runs your direct jobs.

```text
laptop (client) --ssh--> workstation (daemon host) --ssh--> cluster login node (scheduler host)
                              runs direct jobs                 qsub / sbatch -> compute nodes
                              drives the cluster
```

Every arrow is plain SSH. vq never opens a port of its own, so a machine you
can `ssh` to without a password is a machine vq can use.

## 1. Prerequisites on every machine

* **Python 3.12 or newer.** On a cluster this is often a module
  (`module load python/3.12`) or a user-level install such as `uv` or conda.
  The interpreter only has to exist when the venv is created.
* **Git**, for the supported installer.
* **Key-based SSH** between the machines in the arrows above. Put host names,
  ports, users and keys in `~/.ssh/config`, never in vq's configuration:

  ```text
  # ~/.ssh/config on the laptop
  Host workstation
      HostName workstation.example.org
      User myuser
      IdentityFile ~/.ssh/id_ed25519

  # ~/.ssh/config on the workstation
  Host cluster-login
      HostName login.cluster.example.org
      User myuser
  ```

  Check each hop with `ssh workstation hostname` and, from the workstation,
  `ssh cluster-login hostname`. Both must succeed with no prompt.

## 2. Install vq (the same on every machine)

```sh
git clone https://github.com/vibe-qc/vibe-queue.git ~/vibe-queue
cd ~/vibe-queue
./scripts/install.sh
.venv/bin/vq --version
```

This creates `~/vibe-queue/.venv`. Its `vq` is the one you will name as
`remote_vq` below. On a daemon host that also serves the
[web dashboard](web.md), use `./scripts/install.sh --extras web`. See
[clone, download, and install](installation.md) for archives, the GitHub
mirror and where vq stores its state.

A non-interactive SSH shell usually does not read your login profile, so the
venv is rarely on `PATH` for vq's SSH calls. Always give the absolute path.

## 3. A daemon host (a machine with no queue)

Install the daemon as a supervised user service:

```sh
# Linux, once per host: let your user services run without a login session.
# Some sites reserve this for administrators; ask them if it is refused.
loginctl enable-linger "$USER"

~/vibe-queue/.venv/bin/vq daemon install      # systemd user unit, or launchd on macOS
~/vibe-queue/.venv/bin/vq daemon status
```

`vq daemon install --dry-run` prints the unit without writing it. The daemon
refuses to run as root by design; see [daemon lifecycle](lifecycle.md).

Tell the daemon how much of the machine it may use, in that host's own
`~/.config/vq/config.toml`:

```toml
# ~/.config/vq/config.toml on the workstation
[daemon]
max_cpus = 16            # CPU slots shared by all running jobs
max_jobs = 4             # concurrent jobs
max_mem_mb = 64000       # memory budget
default_job_mem_mb = 4000  # charged to a job that does not declare --mem-mb
```

Restart the daemon (`systemctl --user restart vq-daemon`) or run
`vq daemon reload` after editing. For real CPU and memory containment rather
than polling, see [cgroup setup](cgroup-setup.md).

Then, on the laptop, add the host:

```toml
# ~/.config/vq/config.toml on the laptop
default_host = "workstation"

[hosts.workstation]
ssh = "workstation"                                   # the ~/.ssh/config alias
remote_vq = "/home/USER/vibe-queue/.venv/bin/vq"     # absolute path on that host
```

and check it end to end:

```sh
vq doctor workstation
vq submit workstation -- hostname
vq wait <jobid>
vq logs <jobid>
```

## 4. A cluster with PBS/Torque or Slurm

The login node gets vq (step 2) but **no daemon**: on a shared cluster the
batch scheduler is the only thing allowed to place work on compute nodes.

### Detect the scheduler

From the driver (the workstation), with a minimal host block in place,
probe the login node. The probe is read-only and submits nothing:

```sh
vq scheduler-probe cluster
```

It prints the `scheduler` and `scheduler_dialect` values to use, and the
partitions or node properties with their capacity.

### Configure the scheduler host

The block goes in the **driver's** config, because the driver's daemon is
what submits and polls. Put the same block in each client's config as well,
so a laptop knows to route submissions through the driver.

PBS/Torque:

```toml
[hosts.cluster]
ssh = "cluster-login"
remote_vq = "/home/USER/vibe-queue/.venv/bin/vq"
scheduler = "pbs"
scheduler_dialect = "torque"           # from `vq scheduler-probe`
scheduler_driver = "workstation"       # the daemon host that drives it
scratch_root = "/home/USER/vq"        # absolute path on the cluster, shared with compute nodes
submit_extra = ["-q", "batch"]         # passed to qsub verbatim
scheduler_prologue = ["module purge"]  # shell lines before the payload
```

Slurm:

```toml
[hosts.slurm]
ssh = "slurm-login"
remote_vq = "/home/USER/vibe-queue/.venv/bin/vq"
scheduler = "slurm"
scheduler_dialect = "slurm"
scheduler_driver = "workstation"
scratch_root = "/work/USER/vq"        # a filesystem the compute nodes can see
node_scratch_dir = "/tmp/$USER"        # optional node-local scratch
submit_extra = ["--account", "my-group", "--partition", "standard"]
scheduler_max_wall_time_seconds = 172800   # the partition's MaxTime, if known
```

`scratch_root` must be visible from the compute nodes: vq stages each job's
workspace there before submission and reads the results back from it.

`scheduler_driver` names the driver as the config it sits in knows it. In a
client's config that is the driver's `[hosts]` key (`"workstation"` above),
so `vq submit cluster ...` goes over SSH to the driver, which submits. In the
driver's own config, `"localhost"` names the driver itself.

On the driver, check the host and run a canary:

```sh
vq doctor cluster
vq submit cluster --cpus 1 --mem-mb 1000 --time-limit 00:05:00 -- hostname
vq status cluster <jobid>
vq fetch cluster <jobid> -o results/
```

Every key, including the wall-time and CPU lane limits, the memory directive
for Torque and the GNU Time path, is annotated in
[`config.toml.example`](https://github.com/vibe-qc/vibe-queue/blob/main/docs/config.toml.example).

### Keeping the login-node helper current

`vq admin update cluster`, run on the driver, stages the driver's exact
source on the login node and reinstalls the helper there through
`scheduler_update_command` (the shipped `contrib/update-scheduler-vq.sh`).
See [scheduler-side vq refresh](operations.md#scheduler-side-vq-refresh-for-daemonless-clusters).
Calculation runtimes on the cluster are separate; see
[scheduler runtime deployment](scheduler_runtime_deployment.md).

## 5. Register the programs

A host only runs programs you have told it about. Register them in the
config of the machine that has them, then check with `vq programs HOST`.
[Adding programs](user/programs.md) walks through binaries, Python
environments such as vibe-qc, and cluster wrappers.

## Checklist

| Check | Command | Where |
|---|---|---|
| SSH hops work without prompts | `ssh HOST hostname` | each hop |
| vq answers on the remote | `vq doctor HOST` | client or driver |
| daemon is supervised and up | `vq daemon status` | daemon host |
| scheduler detected | `vq scheduler-probe HOST` | driver |
| programs available | `vq programs HOST` | client or driver |
| a canary runs and fetches | `vq submit HOST -- hostname` | client |

Next: [machines with and without a batch scheduler](backends.md) describes
what changes for a job on each kind of host.
