# Fleet console

The fleet console presents the queue's existing host, job and maintenance
state. The product implementation, API and user interface remain in this
repository. Real fleet topology, deployment configuration, accounts and
historical operator records are maintained separately in private storage.

See [Web interface](web.md) for supported behavior and the
[operator guide](operator/index.md) for configuration. Fleet mode is selected
when the application is constructed. Authentication and authorization remain
required; displaying an action does not bypass the underlying managed-host,
ownership or lifecycle checks.

A console must preserve the distinction between observed state, an intended
operation and verified completion. Unknown transport outcomes and held
operations remain visible. Runtime and helper identities must be shown from
recorded evidence rather than inferred from a requested version.

Feature planning remains in [the roadmap](roadmap.md). Product changes use the
normal contributor process; deployment changes use private operator review.
