Scheduler runtime reconciliation accepts an explicitly authenticated immutable
artifact-publication identity from the configured verifier, alongside the
existing store layout. Incomplete or mismatched identities fail closed;
current-state observations retain failed-update history and existing job pins.
