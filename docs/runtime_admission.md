# Retained managed generations

`vq submit` can explicitly select an already-built managed scheduler generation
when the configured runtime provider implements `vq.runtime-admission/1`:

```sh
vq submit cluster calculation.py --program simulation \
  --managed-generation FULL_SOURCE_SHA --idempotency-key experiment-1 --retry 0
```

The source identifier must contain all 40 hexadecimal digits. This path supports
one logical job, with an idempotency key and no automatic retries, reruns or
runtime refresh. It cannot be combined with a portable runtime bundle. Without
this option, current-serving selection and existing source pins retain their
behavior. `--runtime-root` and `--runtime-artifact` continue to identify portable
bundles through their separate verification contract.

Selection uses the scheduler target's reviewed provider command, including the
canonical provider of an alias. A path supplied in a command cannot authenticate
itself. Providers without the admission capability refuse before publication.
No build, serving-pointer change or implicit fallback to current occurs.
Optional `--managed-native-sha256` and `--managed-content-sha256` require specific
previously observed digests before publication. Otherwise the authenticated
provider identity is recorded and kept exact through dispatch. Managed jobs
disable bytecode writes and export both digests for payload provenance.

## Provider contract

The existing authenticated participant transport carries `generation-describe`,
`generation-reserve`, `generation-commit`, `generation-check` and
`generation-release`. Every receipt binds the exact request digest and adapter
identity. The identity includes the canonical program, full source SHA, immutable
generation/source/interpreter paths, native SHA256, execution-content SHA256 and
normal dependency-health result. A describe result alone grants no protection.

A provider can use `vq.runtime_admission.ReservationStore` to implement the
transaction. Its directory is fixed by trusted provider configuration, private
to that provider, outside generation trees, and shared by all of its retention
and mutation owners. Verification and durable reservation publication hold the
same lock used by deletion, replacement and relocation. A cleanup owner holds
that lock through its final mutation and refuses a protected generation. An
adapter must not advertise the capability until every such owner participates;
a newly written reservation file alongside an unmodified cleanup tool is unsafe.

The Queue driver reserves under its existing idempotency lock, before publishing
its workspace and JobSpec. The reservation binds the driver queue authority,
idempotency key, complete submission intent and authenticated generation. Once
the JobSpec and claim are durable, the provider commits the same reservation to
that job. Both reserved and committed records protect the generation, so the
handoff never opens a cleanup window.

If a reserve or commit answer is lost, its protection remains. No timeout expires
it. A retry uses the original idempotency key; an already published job is returned
rather than duplicated. Dispatch reconciles a lost commit answer, verifies the
same provider and generation content again, and uses the persisted immutable
interpreter. Changed or missing identity fails closed.

Archive/delete reconciles the exact durable terminal JobSpec before asking the
provider to release. The provider must confirm scheduler termination; uncertain
submission, inaccessible scheduler state and another job's receipt retain the
reference. Release leaves a tombstone, and cannot turn a consumed reservation
back into a new admission. An unknown admission with no provable terminal job
requires explicit owner reconciliation; deleting its journal or assuming it
expired is not recovery.

## Scope and acceptance

Providers implement their own normal isolated native/dependency verifier and
retention integration. Queue's generic store and transport do not turn an
unreviewed path or an existing migration-only adapter into a capable provider.
The source controls exercise actual CLI publication, alias authority, immutable
dispatch, archive handoff, response-loss recovery, cleanup exclusion and negative
identity checks using synthetic generations. Installing a capable provider is a
separate operational action; this feature does not install or activate runtimes.

### Terminal reconciliation after a provider update

A completed job may reconcile its reservation through the authenticated current
provider at the same canonical authority. This exception applies only to
terminal release: admission and dispatch continue to require the original
adapter digest. The JobSpec and execution pins remain unchanged.

A successor must retain the original adapter digest, prove the exact original
reservation binding and job, independently verify the scheduler's terminal
allocation, and return a durable proof whose digest matches the reconciliation
receipt. Unknown outcomes, changed authorities, mismatched reservations and
unsupported successors retain protection. Identical terminal replay is
idempotent. No generation pruning or runtime activation is authorized by release.
