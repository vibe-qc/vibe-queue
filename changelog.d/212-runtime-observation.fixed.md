**Add `vq admin reconcile-runtime` to verify an immutable scheduler runtime
after a standalone updater loses its recorder.** It preserves the old marker
and receipt, records a new current-state observation for future admission,
and never relaunches a build or changes existing job pins. (#212)
