# Build once: portable runtime bundles

`vq runtime-bundle` provides local-file, HTTPS and SSH artifact transport for #161.
A product-owned builder compiles and qualifies a relocatable runtime once;
Queue packages, authenticates, publishes and deploys those files. Deployment
has no installer/compiler fallback. It requires an explicit target profile
qualified on the intended compute-node class.

This is a separate store format. It does not change scheduler configuration,
legacy source-SHA venv slots or automatic runtime selection. A job binds to a
deployed generation only when it asks to; see
[Running jobs on a deployed generation](#running-jobs-on-a-deployed-generation).
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

## Fetch from an HTTPS store

An HTTPS server can expose the published artifact store layout above. Fetch an
independently authenticated artifact ID into a local store, then use the same
deploy plan/apply commands:

```sh
vq runtime-bundle fetch ARTIFACT_ID \
  --url https://artifacts.example.org/runtime-store \
  --store /opt/runtime-artifacts --trust trust.json
```

The server must serve `artifacts/ARTIFACT_ID/manifest.json` and
`artifacts/ARTIFACT_ID/bundle.tar` beneath `--url`. The requested ID must already
be pinned in the external trust policy. Fetch authenticates the manifest and
checks its exact identity before requesting the archive, then verifies the
archive digest and complete file inventory before normal atomic publication.
Downloaded checksums cannot add trust. Fetch performs no compilation, smoke
execution, deployment or runtime selection.

TLS certificate and hostname verification are mandatory. The default system
CA roots are used; `--ca-file /opt/operator-config/artifact-ca.pem` selects an
operator-provided CA PEM for a private store. Redirects and HTTP downgrades are
refused. URLs containing credentials, queries or fragments are refused, and
transfer errors do not echo the URL or server response.

For a store requiring mutual TLS, supply an operator-managed client certificate
chain and private key from external files:

```sh
vq runtime-bundle fetch ARTIFACT_ID \
  --url https://artifacts.example.org/runtime-store \
  --store /opt/runtime-artifacts --trust trust.json \
  --ca-file /opt/operator-config/artifact-ca.pem \
  --client-cert /opt/operator-config/artifact-client.pem \
  --client-key /opt/operator-config/artifact-client-key.pem
```

The certificate chain must be PEM. The private key must be unencrypted; keep
it in an operator-protected file. Omit `--client-key` when the certificate PEM
also contains the key. A key without `--client-cert` is refused. Encrypted or
invalid keys fail without prompting, and credential-loading errors do not echo
file paths or contents. Credentials are read only when a download is needed,
so verified offline reuse does not require the original client files. They
are never copied into the artifact store or deployment. Client authentication
does not replace the artifact pin, and server certificate and hostname
verification remain mandatory. These TLS options apply to `--url` only.

For a store using bearer authentication, supply `--bearer-token-file` with an
external, owner-only regular file (for example mode `0600`). The file contains
one ASCII token, at most 16 KiB, with an optional final LF or CRLF. Whitespace,
control characters, empty tokens and symlinks are refused. Tokens use letters,
digits, `-._~+/` and optional trailing `=` padding. Use the token issued by the
artifact service, with read access limited to the required store:

```sh
vq runtime-bundle fetch ARTIFACT_ID \
  --url https://artifacts.example.org/runtime-store \
  --store /opt/runtime-artifacts --trust trust.json \
  --bearer-token-file /opt/operator-config/artifact-store-token
```

The token is loaded only for an actual download and sent as an `Authorization:
Bearer` header on the manifest and archive requests. It can be combined with
the CA and mutual TLS options above. Redirects remain refused; authentication
failures do not retry anonymously or echo response bodies, token contents or
credential paths. Verified offline reuse needs no token file. The token is
never saved in the artifact store or deployment, and vq does not automatically
reuse its administrative token. This option applies to `--url` only.

Downloads stream into private temporary staging outside the store and its
lifecycle lock. The manifest limit is 2 MiB; the default archive download limit
is 4 GiB. Set `--max-download-mib` to change the archive cap, up to 65,568 MiB
(64 GiB plus 32 MiB of archive overhead). The cap applies even if the server
omits `Content-Length`. `--timeout` sets the connection/read socket timeout
(default 30 seconds, maximum 120), rather than a total transfer deadline.

Interrupted or refused downloads are discarded without publishing partial
bytes. Repeat the same command to retry the full transfer under the same
immutable identity. If the local artifact already exists, fetch rechecks its
current trust and complete contents and reuses it without contacting the server.
A damaged local artifact is refused rather than overwritten. Fetch rechecks
trust before publication, so revoking a pin during download prevents publication.
This command is a client; it does not start a server or change network exposure.

## Fetch from a store on another host over SSH

A store that `vq runtime-bundle publish` filled on a builder host can be read
as it is from a configured host, over the same SSH connection vq already uses
for that host. No server is needed:

```sh
vq runtime-bundle fetch ARTIFACT_ID \
  --from-host builder --remote-store /srv/runtime-store \
  --store /opt/runtime-artifacts --trust trust.json
```

`--from-host` names a `[hosts.*]` entry; its `ssh` destination and vq's
connection, keep-alive and batch-mode options are used. `--remote-store` is
the absolute store root on that host: plain ASCII, without control characters
or `.` and `..` components. `--url` and `--from-host` are mutually exclusive;
`--ca-file`, `--client-cert` and `--client-key` apply to HTTPS only.

The gates are the HTTPS fetch's, in the same order:

1. The requested ID must already be pinned in the local trust policy before
   any connection is made.
2. An artifact already published locally is rechecked and reused without
   connecting.
3. The manifest is read and authenticated before the archive is requested.
4. Both files go to private staging, and the normal publication checks the
   archive digest and the complete inventory.

SSH adds no trust: the bytes are exactly as untrusted as a download, and only
the pin and digests decide.

Each file is read with `head -c LIMIT+1` on the remote side, so an oversized
file is stopped at the source and the extra byte is caught locally. The
manifest limit is 2 MiB, and the archive limit is `--max-download-mib`. Over
SSH, `--timeout` is the longest a transfer may go without data before it is
stopped. A stalled or failed transfer kills the whole local ssh process group
and publishes nothing. The remote's own error output is not echoed, since it
can name hosts and paths. Rerun the same command to retry.

## Running jobs on a deployed generation

A daemon-host job can be bound to a generation of a deployment target:

```sh
vq submit HOST --runtime-root /abs/runtime -d inputs -- \
    sh -c '"$VQ_RUNTIME_PAYLOAD"/bin/program input.inp'
vq submit HOST --runtime-root /abs/runtime --runtime-artifact SHA256 -d inputs -- ...
```

At submit, the execution host resolves the deployment's current selection,
or the generation `--runtime-artifact` names. It accepts it only as
`deploy` left it: the generation's manifest has that identity, its
verification receipt says it verified, and its payload is an operator-owned
directory nobody else can write. The job records it as `runtime_bundle`
(`root` and `artifact_id`). A refused root submits nothing.

Before the job starts, the daemon checks that same generation again and runs
the job with `VQ_RUNTIME_PAYLOAD` (its payload directory) and
`VQ_RUNTIME_ARTIFACT` (its identity). It never substitutes another
generation. A later `deploy` that moves the selection, or a rollback, changes
only what later submissions bind. If the bound generation is gone or no
longer verifies, the job fails with "bound runtime generation unavailable
before dispatch" instead of running on something else. Generations are not
deleted today, so a bound one stays available.

`vq status` shows a `runtime:` line, the fleet console names the generation in
the software column, and `vq resubmit` keeps the same generation. The
binding is part of the submission's identity for `--idempotency-key`.
Without `--runtime-root` nothing changes.

Scheduler targets refuse `--runtime-root` for now, because the driver cannot
yet verify a deployment on the cluster. Rewriting `{runtime}` in the command
is left to wrappers such as `vq benchmark measure`; the queue only exports
the two variables.

## Limits and remaining qualification

The format accepts regular files and optional directories, at most 10,000 files
and 64 GiB unpacked. Symlinks, hard links, sparse/special files, traversal,
duplicate members, privileged modes and missing/unlisted files are refused.
Builders must dereference relocatable links safely before packaging; arbitrary
venvs and vendor archives are not automatically portable. The local fixture
builds a small native executable plus shared library and data once, removes
the build prefix, and deploys the identical artifact to two new prefixes.
That verifies the mechanism, not vibe-qc, ORCA or CRYSTAL qualification.

Follow-ups include other client credential adapters, build-host
scheduling, signature-based trust, product/vendor import adapters, scientific
and MPI smoke receipts on real compute-node classes, generation binding for
scheduler targets, and retention that follows bound jobs. No production host has been changed
by this source implementation.
See [scheduler runtime deployment](scheduler_runtime_deployment.md) for the
existing site-provider workflow, and [resource estimation](resource_estimation.md)
for the companion preview foundation.
