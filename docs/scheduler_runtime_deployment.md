# Scheduler runtime deployment

Daemonless scheduler hosts have two independent maintenance lanes:

1. `vq admin update HOST` updates the scheduler-side vq helper.
2. `vq admin update PROGRAM HOST --expected-sha FULL_SHA` updates one managed
   program runtime.

A helper repair must not rebuild scientific software. A runtime rollout must
not silently replace the queue helper used by the driver. Site commands are
installed from a private operations provider, independently of product source
archives; an operator selects them through external configuration.

## External configuration

The canonical host must be explicitly managed. Alias, excluded and helper-only
roles do not gain runtime maintenance authority through a command-line request.
A portable example is:

```toml
[hosts.cluster]
fleet_role = "managed"

[hosts.cluster.scheduler_runtime_deployments.vibeqc-release]
update_command = "/site/bin/deploy-vibeqc-release"
install_command = "/site/bin/install-vibeqc-release"
update_host = "cluster-build"
verify_command = "/site/bin/verify-vibeqc-release"
timeout_seconds = 14400
```

Actual hosts, scheduler accounts, queues, module choices and paths belong in
private operator configuration. `update_host` selects a fixed build host;
`update_allocation` selects a scheduler allocation. They are mutually exclusive.
Verification runs on the scheduler login host so it sees the runtime paths
used by submitted jobs. Consult the configuration reference for the complete
profile schema and the arguments passed to provider commands.

## Source identity and activation

Stage an exact source revision and verify its archive and source-tree identity
before installation. Preserve the full requested commit identity through
build, verification and the activation receipt. A package version alone does
not distinguish two builds from different commits.

Build and validate a separate immutable runtime before changing a stable link.
Keep existing job launchers, archives and environments available to jobs that
already reference them. An interrupted transport is an unknown outcome until
readback resolves it; do not infer failure or success solely from an SSH error.

Private providers must return the existing structured verification and
activation evidence expected by the queue. A helper activation receipt is
valid only after checking the live selected helper against the requested
source. Runtime deployment evidence is distinct from helper evidence.
A missing or mismatched receipt does not authorize a successful marker.

## Provider maintenance

Review and install private helpers separately, recording the old and new file
hashes and interpreter mapping. Do not extract them from a product archive or
let a contributed patch replace private operational scripts. A provider change
must preserve configured command arguments, source staging, verifier behavior
and in-flight runtime references. Test with disposable roots and scheduler
doubles before an operator changes installed helpers.
