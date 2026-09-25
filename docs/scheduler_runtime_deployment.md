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

## Feeding a push-fed source mirror

A build node that cannot reach the source repository may instead be served by
a bare mirror on the login host, fed by push. A profile that sets
`feed_source_mirror` has vq perform that feed before preparation.

**The feed does not force anything, and it is not a mirror-sync.** It is one
unforced, atomic push of the immutable per-pin ref `refs/vq/pins/<sha>`, this
lane's branch pair, and the tags that name this pin in the driver's checkout.
Refs and tags outside that set are never named and so stay byte-identical,
including tags the driver's checkout holds at another commit.

An update that would need a force is refused before the push, naming what the
mirror holds and what was requested. A mirror branch that has advanced past
an accepted pin therefore fails visibly: the operator either deploys the
commit the mirror carries, or the mirror's owner moves that branch back
deliberately. A feed must never resolve the disagreement by picking up a
newer tree than the one that was accepted, and it must never rewind the
mirror to make an older pin fit. The immutable per-pin ref is what keeps the
accepted pin addressable regardless of where the mutable branches sit.

The site preparers read the lane branch pair today; `refs/vq/pins/<sha>` is
fed for every deployment so the exact accepted tree is anchored in the mirror
under a name that cannot be forced. **The intended contract is for a preparer
to read that ref, not the branch pair.** The pin ref names exactly the
accepted commit and can never move, so a mirror whose lane branch has drifted
cannot make a preparer build the wrong tree and need not be reconciled before
that pin deploys; the branch pair stays fed for the preparers that still read
it. Which ref a preparer reads is a provider-side contract change, made
deliberately and recorded with the profile, and until a site's preparer has
moved, a drifted lane branch is refused as described above (#81).

See `docs/operations.md`, "Feeding the source mirror", for the operator view
and the exact refusal text.

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
