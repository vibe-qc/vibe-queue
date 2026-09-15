# Portable maintenance checks

Real fleet inventory and site-specific update commands live in private
operator storage. This page states the product contracts that an operator's
runbook must preserve. See [scheduler runtime deployment](scheduler_runtime_deployment.md)
and the [operator guide](operator/index.md).

Before maintenance, identify the intended canonical host, its managed role,
program, full source revision and currently selected runtime. Preserve active
jobs, explicit holds, retired-host fences and the source identity recorded at
submission. A hostname alias alone is not maintenance authorization.

Use the configured update lane. Scheduler helper installation and scientific
runtime deployment are separate operations. Private helper scripts are
installed from their independent provider; product archives contain portable
queue code and do not refresh those scripts.

Remote commands may execute in a non-login shell. Select required executables
and environment settings explicitly in private configuration rather than
assuming an interactive shell's PATH or activation state. Provisioning for
multiple users likewise needs the intended service environment and ownership.

After an update, verify the actual selected executable, full source identity,
health check and activation receipt. A successful upload or process exit alone
is not proof that the intended runtime is active. Transport errors remain
unknown until evidence resolves them; keep failed and interrupted outcomes
visible to recovery tools. Do not retry by overwriting a running environment.

Site-specific commands and incident records are maintained privately. Changes
to the provider require reviewed source and installed-file hashes, with a
post-install readback before any product source dependency is removed.
