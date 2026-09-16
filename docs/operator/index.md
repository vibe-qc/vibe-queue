# Running a host

**Audience: the machine is yours.** You install the daemon, decide what a job
may consume, and are the one who finds out when a host stops answering.

Most of this corpus is written for you. It is the largest of the three
audiences and the least forgiving, because the failure modes are operational
rather than conceptual.

## Start here

The daemon is a supervised, long-running process that owns durable on-disk
state. Three consequences shape everything below:

**It is not stateless.** State outlives any single process, and a daemon
inherits the state the previous one left. [Version
compatibility](../version_compatibility.md) is the contract between a daemon
and state written by an older one; a rolling fleet update depends on it.

**It refuses to run as root.** By design, and the lifecycle lock binds a
checkout to its owning uid. Multi-user deployment is a separate, deliberate
mode, not what you get by using `sudo`.

**Update and reinstall are different operations.** `vq self-update` /
`vq admin update` own service restart together with git and venv rollback.
`scripts/reinstall.sh` owns neither and must not run while a daemon is up.

## Setting a host up

* [Clone, download, and install](../installation.md) — the standalone
  repository, GitHub mirror, and installation profiles.
* [Host records](../hosts.md) — what to know about a machine before you point
  a queue at it, and why the filled-in inventory for this project's own fleet
  is deliberately not in this repository.
* [Reaching the queue from the internet](../remote-access.md) — SSH
  hardening, key-only auth, router forwarding. vq needs no changes: make SSH
  reachable and hardened, and vq is too.
* [Daemon lifecycle](../lifecycle.md) — supervision, provenance, restart and
  recovery rules.
* [The web dashboard](../web.md) — read-only by default, and not meant to
  face the internet.

## Keeping it honest

* [cgroup setup](../cgroup-setup.md) — real containment for CPU and memory,
  rather than a heuristic.
* [Draining a host](../drain.md) — stop dispatch without killing running work,
  which is what you want before maintenance.
* [Throttling](../throttle.md) — soft CPU-priority adjustment for a job that
  is winning too hard.
* [Automatic cleanup](../auto-cleanup.md) — terminal jobs do not archive
  themselves unless you say so.

## More than one machine, more than one user

* [The fleet console](../fleet_console.md) — cross-host view, accounts and
  audited write actions.
* [Multi-user deployment](../multi_user_deployment.md) — privileged shared
  installation under `/opt/vq`, ownership checks, admin group.
* [Retiring a multi-user deployment](../multi_user_retirement.md).
* [Scheduler runtime deployment](../scheduler_runtime_deployment.md) — hosts
  where PBS or SLURM, not a vq daemon, owns dispatch.
* [Operations](../operations.md) — the long-form operational reference.

## When a host stops answering

[Host recovery channels](../host_recovery_channels.md) defines the tiered
contract every fleet host must satisfy *before* it joins, precisely because
the moment you need a second way in is the moment you cannot set one up.

```{toctree}
:hidden:
:caption: Setting up

../hosts
../remote-access
../lifecycle
../web

```

```{toctree}
:hidden:
:caption: Containment

../cgroup-setup
../drain
../auto-cleanup

```

```{toctree}
:hidden:
:caption: Fleets

../fleet_console
../multi_user_deployment
../multi_user_retirement
../scheduler_runtime_deployment
../operations
../host_recovery_channels
```
