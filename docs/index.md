---
myst:
  html_meta:
    "description": "vq, a cross-machine job queue. Submit a command, run it on your laptop, a server over SSH, or a cluster behind PBS or SLURM, and fetch the results back. Durable job state, composable resource caps, and a machine-readable contract for agents."
    "og:title": "vq, a cross-machine job queue"
    "og:description": "Submit here, run there, fetch the results back. Durable job state across daemon restarts, CPU/memory/concurrency caps that compose, and stable JSON for agents. Python 3.12+, SSH transport, nothing to install on the remote host but vq."
    "og:type": "website"
    "og:url": "https://vibe-qc.com/vibe-queue/docs/"
    "og:image": "https://vibe-qc.com/vibe-queue/docs/_static/logo/vq-social.png"
    "og:image:width": "1200"
    "og:image:height": "630"
    "twitter:card": "summary_large_image"
    "twitter:image": "https://vibe-qc.com/vibe-queue/docs/_static/logo/vq-social.png"
---

# vq {{ version_display }}

**vq is a cross-machine job queue.** It takes a command, runs it on the
machine you point it at — your laptop, a server over SSH, or a cluster behind
PBS or SLURM — and tracks it until you fetch the results back. It was built to
run [vibe-qc](https://github.com/vibe-qc/vibe-qc) calculations and it
does not care whether that is what you run.

```sh
vq submit compute optimise.py
vq queue
vq fetch <jobid> -o results/
```

vq needs Python 3.12 or newer, and installs nothing on the remote host but
itself. Direct remote submission uses SSH. The optional
[submission gateway](gateway.md) also supports HTTPS clients through
`vq remote`, with resumable uploads, logs, cancellation and verified result
downloads. Its outbound connector can serve a broker behind a firewall.

---

## Get vq

Start with [clone, download, and install](installation.md) for the split
repository layout, source archives, and the GitHub mirror. To set up a
workstation, a server or a PBS/Slurm cluster, follow
[installing vq on machines and clusters](cluster_install.md), then work
through the [tutorials](user/tutorials.md).

## Three ways to read this

This documentation serves three readers who want almost nothing in common.
Start in the right place.

::::{grid} 1 1 3 3
:gutter: 2

:::{grid-item-card} I want to run jobs
:link: user/index
:link-type: doc

Submit work, watch it, get the results back. Dependencies, arrays, retries,
wall-time limits, and what to do when a job will not start.
:::

:::{grid-item-card} I run the machines
:link: operator/index
:link-type: doc

Install and supervise the daemon, register engines, expose a host safely,
cap what a job may consume, deploy for several users, and recover a host
that has stopped answering.
:::

:::{grid-item-card} I am an agent
:link: agent/index
:link-type: doc

The machine-readable contract: stable JSON shapes, exit codes, the states a
job moves through, and the invariants you may rely on between versions.
:::

::::

---

This manual follows the current source with package version **{{release}}**;
source updates may precede deployment on a particular host.

## Available workflows

The current source includes direct and SSH submission, PBS/Slurm driver
queues, an opt-in durable fleet broker and HTTPS gateway, immutable runtime
slots and portable bundles, campaigns, resource-estimate receipts and
placement explanations. The broker holds work
until a configured daemon host has capacity; PBS/Slurm targets continue
through their scheduler drivers. See [broker operation](operations.md#the-fleet-broker),
[runtime slots](runtime_slots.md), [portable runtime bundles](portable_runtime_bundles.md),
[gateway and relay](gateway.md), [campaigns](campaigns.md),
[resource estimates](resource_estimation.md) and [placement](placement_policy.md).
Availability on a particular machine depends on its installed version and
operator configuration. A source feature does not enable itself on a host.

## What vq guarantees

The three that matter most, because they are what the rest is built on:

**A job's state is durable.** Every transition is written to disk before it is
reported. A daemon restart, a reboot, or an SSH drop does not lose a job or
silently change its outcome; an interrupted job says it was interrupted.

**Dispatch is capped, and the caps compose.** A job runs only when the CPU,
memory and concurrency budgets all allow it. A per-submitter quota can tighten
a host's global cap and can never loosen it.

**vq stays out of the transport.** Hostnames, ports and keys live in
`~/.ssh/config`, not in vq's configuration. Nothing in vq changes when a host
moves onto the internet.

## Where to find the rest

* **Changelog** — [`CHANGELOG.md`](https://github.com/vibe-qc/vibe-queue/blob/main/CHANGELOG.md)
* **Reporting a bug** — [`CONTRIBUTING.md`](https://github.com/vibe-qc/vibe-queue/blob/main/CONTRIBUTING.md);
  for anything security-relevant, [`SECURITY.md`](https://github.com/vibe-qc/vibe-queue/blob/main/SECURITY.md)
  rather than a public issue
* **Public source** — [GitHub mirror](https://github.com/vibe-qc/vibe-queue).
  Private GitLab access details come from the maintainers.
* **Cutting a release** — [the release process](release_process.md), and
  [version compatibility](version_compatibility.md) for what a vq version
  means to a fleet
* **What is next** — the
  [roadmap](https://github.com/vibe-qc/vibe-queue/blob/main/docs/roadmap.md):
  what is planned, and what is released but not yet proven on a host
* **History and design records** — kept in the
  [source tree](https://github.com/vibe-qc/vibe-queue/tree/main/docs), not
  published here. Each says in its first paragraph what it is. Four are
  history: `roadmap_history.md` (v0.1 to v0.9),
  `v0_7_1_lamports_clock_design.md`, `pbs_dispatcher_backend_design.md` and
  `wall_time_design.md`. Two record designs now implemented in increments, including remaining
  limits: `design_immutable_venv_runtimes.md` and `design_broker.md`.
  Their current user-facing instructions are in [runtime slots](runtime_slots.md)
  and [broker operation](operations.md#the-fleet-broker). Where any of them
  disagrees with `vq <command> --help`, the help is right.

The family workflow is sketch → submit → calculate → analyze:
[vibe-sketch](https://vibe-qc.com/vibe-sketch/docs/) prepares structures and
inputs, vq submits and tracks work, [vibe-qc](https://vibe-qc.com/docs/)
computes, and [vibe-view](https://vibe-qc.com/vibe-view/docs/) analyzes the
results. Each product has its own installation; local calculations can skip
queue submission. [Running vibe-qc calculations](user/vibe_qc.md) and
[vibe-sketch and vibe-view with the queue](user/vibe_family.md) show how the
pieces connect.

```{toctree}
:hidden:
:caption: By audience

user/index
operator/index
agent/index
```

```{toctree}
:hidden:
:caption: Reference

installation
orchestration
version_compatibility
codenames
release_process
distribution
testing
```
