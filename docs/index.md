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
vq submit compute -- python optimise.py
vq queue
vq fetch <jobid> -o results/
```

vq needs Python 3.12 or newer, and installs nothing on the remote host but
itself. Its entire transport is SSH: if you can `ssh` to a machine, vq can
queue work on it.

---

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
* **Source** — [source-host.invalid/mpei/vibe-queue](https://github.com/vibe-qc/vibe-queue)
* **Cutting a release** — [the release process](release_process.md), and
  [version compatibility](version_compatibility.md) for what a vq version
  means to a fleet

vq is part of a set: [vibe-qc](https://vibe-qc.com/docs/) computes,
[vibe-view](https://github.com/vibe-qc/vibe-view) renders the results,
and vq decides where the work runs. Each is usable without the others.

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

orchestration
version_compatibility
codenames
release_process
```
