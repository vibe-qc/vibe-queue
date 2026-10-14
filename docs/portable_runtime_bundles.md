# Build once: portable runtime bundles

`vq runtime-bundle` is the first local-file-transport increment of #161.
A product-owned builder compiles and qualifies a relocatable runtime once;
Queue packages, authenticates, publishes and deploys those files. Deployment
has no installer/compiler fallback. It requires an explicit target profile
qualified on the intended compute-node class.

This is a separate store format. It does not change scheduler configuration,
legacy source-SHA venv slots, automatic runtime selection or JobSpec pins.
Passing an existing legacy runtime root fails before changing it. Existing
site providers must adopt this identity contract before they can support
same-source rebuilds; this feature alone does not repair their artifacts.

## Identities and provenance

| Identity | Meaning |
|---|---|
| `source_revision` | Full actual source revision, attested by the trusted publisher |
| `build_id` | SHA256 of the complete canonical build description, including recipe, dependency profile and target requirements |
| `archive_sha256` | SHA256 of the bundle bytes |
| `artifact_id` | SHA256 of the canonical manifest, binding build provenance and the complete file inventory to the archive |

Changing dependencies or configuration at the same source revision produces a
different build ID and artifact ID. Both generations can coexist; no fake source
SHA or overwrite is needed. The queue does not infer provenance from a source
directory or independently authenticate Git history. Product qualification and
the operator's authenticated publisher attestation supply that evidence.

An example build description, stored outside the product checkout:

```json
{
  "schema_name": "vq.runtime-build/1",
  "program": "example-runtime",
  "source_revision": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "recipe_sha256": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
  "dependency_sha256": "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
  "target": {
    "os": "Linux",
    "arch": "x86_64",
    "isa": ["sse2"],
    "abi": {
      "libc": "glibc-2.31",
      "python": "none",
      "blas": "none",
      "mpi": "none",
      "launcher": "none"
    },
    "modules": []
  },
  "variant": "generic",
  "toolchain": "product-builder/toolchain-1",
  "compiler_flags": ["-O2"],
  "publisher": "qualified-builder",
  "build_receipt": "build-record-1",
  "qualification_receipt": "qualification-record-1",
  "smoke_argv": ["bin/runtime", "--self-test"]
}
```

Replace every example digest with the actual authenticated identity. The recipe
and dependency digests should cover the complete product-owned inputs, including
dependency revisions and configuration. Their associated receipts remain in
operator-controlled storage. ABI values use exact equality, conservatively;
`none` explicitly means the artifact has no such external requirement. ISA
and module requirements must be subsets of the qualified target's capabilities.
The target profile has the same shape as `target` above. The mandatory ABI
fields cannot be omitted. Generic descriptions reject `-march=native`/`-mcpu=native`.
Queue cannot infer minimum ISA or ABI safety by inspecting arbitrary binaries.

## Package, trust, publish and deploy

Use absolute output/store/target paths. The runtime directory must contain only
distributable files: exclude credentials, signing keys, build workspaces and
operator configuration. Packaging creates a new directory containing
`bundle.tar` and `manifest.json`. No compilation or smoke execution occurs yet.

```sh
vq runtime-bundle pack /opt/build-output/example-runtime \
  --description build.json --output /opt/artifact-staging/example-runtime
```

The printed artifact ID is a **canonical manifest digest**, not the hash of its
pretty-printed JSON file. It is not automatically trusted. After authenticating
that exact manifest with the publisher through an independent trusted channel,
the operator adds its digest to an external `trust.json`:

```json
{
  "schema_name": "vq.runtime-trust/1",
  "publishers": {
    "qualified-builder": ["dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"]
  }
}
```

Replace the example pin with the reviewed artifact ID. This first increment
uses out-of-band content pins; it does not implement signing or signature-key
rotation. A checksum downloaded alongside a bundle does not authenticate it.
Trust is rechecked during publication and each deployment, so removing a pin
prevents a new apply. Existing running programs are not revoked or stopped.

```sh
vq runtime-bundle publish /opt/artifact-staging/example-runtime \
  --store /opt/runtime-artifacts --trust trust.json
vq runtime-bundle deploy ARTIFACT_ID --store /opt/runtime-artifacts \
  --root /opt/portable-runtimes/example-runtime --trust trust.json \
  --target-profile compute-class.json --plan
vq runtime-bundle deploy ARTIFACT_ID --store /opt/runtime-artifacts \
  --root /opt/portable-runtimes/example-runtime --trust trust.json \
  --target-profile compute-class.json --apply
```

Plan is the default, reads and verifies evidence, and makes no target changes.
Its JSON lists compatibility blockers. Apply rechecks those gates under the
existing physical lifecycle locks for both the store and target. It extracts
into staging, checks every declared file, publishes a generation, runs the
trusted product's smoke executable at its final prefix, and checks that the
smoke did not alter the payload. Only then does an atomic selection receipt
change `current` and `previous` together.

The smoke command runs as the invoking operator; this is not a sandbox. Its
timeout defaults to 30 seconds (maximum 120); a timeout kills its process group.
Product probes should check native libraries, data lookup and relocation, avoid
background processes, and make no changes to the runtime. Standard Python and
dynamic-library path overrides are removed from the smoke environment.

```text
artifact-store/
  format.json
  artifacts/<artifact_id>/bundle.tar
  artifacts/<artifact_id>/manifest.json
target-root/
  format.json
  selection.json                  # current and previous artifact IDs
  generations/<artifact_id>/manifest.json
  generations/<artifact_id>/verification.json
  generations/<artifact_id>/payload/
```

The same artifact can be applied to multiple independent compatible prefixes.
To roll back, deploy a retained artifact ID with `--apply`. Consumers must pin
the returned absolute generation payload path; resolving the mutable selection
again during a running job would not preserve that pin. This increment retains
all generations and does not integrate with automatic queue pinning or cleanup.

Interrupted publication never installs partial artifact bytes. Repeating the
same operation verifies the existing identity; it never overwrites a damaged
artifact or payload. A lost acknowledgement or missing verification receipt is
reconciled by checking the retained contents and rerunning the smoke. A failed
smoke leaves the current selection intact and may leave an unselected generation
for diagnosis. A killed process can leave unpublished staging directories; no
automatic deletion of unknown directories is performed.

## Limits and remaining qualification

The format accepts regular files and optional directories, at most 10,000 files
and 64 GiB unpacked. Symlinks, hard links, sparse/special files, traversal,
duplicate members, privileged modes and missing/unlisted files are refused.
Builders must dereference relocatable links safely before packaging; arbitrary
venvs and vendor archives are not automatically portable. The local fixture
builds a small native executable plus shared library and data once, removes
the build prefix, and deploys the identical artifact to two new prefixes.
That verifies the mechanism, not vibe-qc, ORCA or CRYSTAL qualification.

Follow-ups include SSH/HTTP transports, build-host scheduling, signature-based
trust, product/vendor import adapters, scientific and MPI smoke receipts on
real compute-node classes, and generation-aware scheduler/daemon binding and
retention. No production host has been changed by this source implementation.
See [scheduler runtime deployment](scheduler_runtime_deployment.md) for the
existing site-provider workflow, and [resource estimation](resource_estimation.md)
for the companion preview foundation.
