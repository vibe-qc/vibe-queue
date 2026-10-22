# Limiting scheduler load

For compatible short tasks, submit one resource-bounded script that starts a
bounded number of ordinary child processes inside its allocation. Match the
total concurrent CPU and memory demand to the allocation; limit each child's
thread count and collect every exit status. Keep incompatible runtime, resource
and failure domains separate. The queue does not automatically pack arbitrary
commands or change existing job identities.

`vq submit --array N` creates **N independent queue jobs**; it is not a single
native Slurm array or a process pool inside one allocation. Native Slurm array
tasks also count against job-related site limits. A site's array-size limit is
not additional per-user capacity. See [Slurm job arrays](https://slurm.schedmd.com/job_array.html).

The driver can limit outstanding allocations and space out submissions for a
particular scheduler target:

```toml
[hosts.cluster]
ssh = "cluster.example.com"
scheduler = "slurm"
scheduler_dialect = "slurm"
scheduler_driver = "driver"
scratch_root = "/scratch/user"
scheduler_max_jobs = 8
scheduler_submit_interval_seconds = 10
```

Choose values below the site's limits, leaving headroom for other drivers,
manual jobs and deployment jobs. These controls count only this driver's
allocations, including queued and uncertain submissions. They do not query or
reserve the entire site's account quota. Slurm remains authoritative for those
limits. There is no extra scheduler query per admission.

Aliases declared with `fleet_canonical_host` share the strictest configured cap
and longest interval. Declare aliases for multiple queue names of one cluster;
independent targets and local work retain their own budgets. The daemon-wide
`max_scheduler_jobs` cap remains an additional limit across targets. Existing
running jobs are preserved, and finished allocations awaiting artifact transfer
no longer consume a slot. Uncertain submissions retain capacity until resolved.

The interval applies to submission attempts, including rejected or ambiguous
attempts, and survives config reloads. On daemon restart capacity is rebuilt
from retained jobs; pacing starts with the first new attempt. Omitted settings
retain the existing global-cap-only behavior and unpaced submission. Apply host
configuration changes with `vq daemon reload`; no job cancellation is needed.

Status polling already batches live and accounting requests by configured
target and backs off unavailable hosts. Repeated manual polling adds requests;
use bounded waits and avoid a separate query for every short task.
