# Refreshing scheduler runtime admission

A scheduler target can activate a healthy immutable runtime while another driver's
last deployment receipt still names its predecessor. If the target registry uses
a mutable wrapper, submission correctly refuses to mistake that wrapper for an
immutable pin. Refresh the driver's **observation**, without rebuilding or changing
any queued job:

```sh
vq admin reconcile-runtime CLUSTER PROGRAM --current-without-marker \
  --expected-sha FULL_COMMIT_SHA --dry-run --json
vq admin reconcile-runtime CLUSTER PROGRAM --current-without-marker \
  --expected-sha FULL_COMMIT_SHA --json
```

Supply `--tag TAG` when the configured verifier requires a release tag. The command
uses that target's configured runtime verifier. It requires matching source/tag,
an immutable launcher, native/dependency health and a quiescent publisher; it does
not trust a caller-supplied path. It holds the normal ownership/lifecycle locks,
refuses any conflicting marker and checks for marker or admission-record changes
before recording. Dry run performs the same verification and writes nothing.

The old deployment record is archived with the new verification. The new record
is explicitly a current-state observation, not proof that a historical build
succeeded. Existing job pins, runtime generations, source, launchers and active
pointers are unchanged. The normal submit resolver then authenticates new jobs
against the observed immutable generation; wrong expected identities still fail.

If an actual abandoned update marker exists, use the existing scoped recovery
form `--marker-id EXACT_MARKER_ID` instead. The two modes are exclusive. Do not
fabricate or delete a marker to refresh a healthy runtime. A live writer, paused
jobs or managed transaction still requires its existing owner/recovery path.
