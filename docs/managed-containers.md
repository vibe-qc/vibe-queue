# Managed local Docker jobs

Queue's ordinary process route accounts for the submitted process group and
its cgroup. A shell that calls `docker run` does not own the Docker daemon's
worker processes. Client exit or idle client CPU cannot prove that worker has
finished. Do not submit such a wrapper as an ordinary native job.

Use the opt-in managed route to run the shell **inside** a container:

```sh
vq submit localhost -d payload --cpus 2 --mem-mb 2048 --wall-time-seconds 600 --retry 0 -- \
  vq-docker example/image@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef -- sh build.sh
```

Replace the example digest with an immutable image already present in the local
engine. `vq-docker` is a Queue dispatch marker, not an executable to run directly.
The daemon creates and owns the container; it does not adopt existing containers
by name. Previously submitted ordinary wrappers and live containers are unchanged.

This initial route supports single-user Linux daemons, CPU workloads, an existing
Docker executable and a local Unix endpoint (`DOCKER_HOST`, default
`unix:///var/run/docker.sock`). It does not run sudo, change permissions, pull
images, or select a mutable Docker context. Remote engines, multi-user daemons,
scheduler targets, registered program/runtime bindings, GPU allocation, automatic
retries and pause/resume are refused. Existing foreground OCI/HPC runtimes that
keep their work inside the normal job scope can continue using the normal route.

The payload workspace is mounted at `/workspace` and is the working directory.
The container uses the daemon user's UID/GID, declared CPU/memory limits,
`VQ_CPUS`/`VQ_JOB_ID`, no network and no restart policy. No engine socket or Queue
state directory is mounted. Put inputs and required offline dependencies in the
workspace; outputs written there remain available through ordinary fetch.

## Ownership and completion

A durable creation intent binds the job's argv, workspace and resources to a
random ownership token and fixed engine endpoint. The full container ID is
persisted before start. Every observation/control checks that ID, token, image,
name and disabled restart/auto-removal policy. Lost create replies reconcile
the same intent; they never create a replacement worker. Unknown or conflicting
identity retains the reservation and authorizes no guessed kill.

Worker CPU/memory come from its authenticated Linux cgroup, not the Docker client.
Unknown accounting intervals are not treated as zero CPU. Container operations
run in a bounded background pool so a slow engine does not block native dispatch.

`vq kill` records a durable cancellation request. Queue keeps the job and its
capacity active until the engine confirms worker exit; it sends TERM, then KILL
after the ordinary ten-second grace if needed. A signal reply alone is not
completion. Restart restores the same binding and capacity reservation, including
unknown outcomes. Cleanup and resubmission refuse unresolved workers even if a
stale spec was marked terminal. Do not remove binding files to clear a reservation.

`vq status localhost JOB --json` includes `managed_container` with phase,
immutable ID, pending disposition and any engine/cleanup error; the ownership
token is omitted. A corrupt or mismatched binding remains fenced for operator
reconciliation. Restore engine access or the original unmodified authority;
never infer termination from an absent client PID.

After confirmed exit, Queue retains `_vq/container-receipt.json`, collects the
last 1,000 timestamped lines from each Docker log stream (at most 2 MiB combined),
and removes only the authenticated stopped container. Full scientific/build
outputs must be files in `/workspace`; log tails are explicitly bounded. A failed
removal is recorded as `cleanup_error` with `removed: false`; it does not turn a
stopped container into live work. Preserve that identity for operator cleanup.

## Validation

`tests/test_managed_container.py` uses a controlled engine to cover active and
idle accounting, ambiguous creation, identity mismatch, cancellation, restart,
capacity fences, resubmission and cleanup. A real Linux engine smoke must also
exercise a small active and idle container, cancellation and terminal collection
before a fleet installation is described as accepted. Source tests do not
constitute that deployment receipt.
