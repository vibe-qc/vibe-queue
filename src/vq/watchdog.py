"""Resource and liveness watchdog for running jobs.

For each ``RUNNING`` job the daemon supervises, the watchdog:

1. **Samples** RSS and CPU-time at ``interval_seconds`` cadence by reading
   ``/proc/<pid>/status`` and ``/proc/<pid>/stat``. Linux only; on macOS
   the sampler returns None for everything and the watchdog effectively
   no-ops (matches "Linux first, macOS dev only" non-negotiable).
2. **Persists** each sample as a JSON line under
   ``<workspace>/_vq/samples.jsonl`` for forensic inspection.
3. **Enforces** four limits:

   * ``spec.mem_mb`` -- per-job declared memory cap. Kill -> OOM_KILLED.
   * ``host_total_mem_mb * max_rss_percent / 100`` -- absolute host
     ceiling. Kill -> OOM_KILLED.
   * ``spec.wall_time_seconds`` -- per-job time budget. Kill ->
     TIME_EXCEEDED.
   * CPU% < ``starve_threshold_percent`` for ``starve_window_seconds``
     -- the job is doing nothing. Kill -> STARVED.

4. **Escalates** kills via SIGTERM -> grace period -> SIGKILL. The kill
   targets the **process group** (``os.killpg(pgid, ...)``) so OMP /
   MPI children die together with the parent.

The watchdog never mutates or writes the spec, including
``last_heartbeat_at``. It returns a Verdict and lets the Daemon do the
bookkeeping. This keeps state-machine writes single-sourced inside Daemon
(per SPEC.md sec 3.4 "the watchdog is the only component that kills running
processes" plus the implicit corollary "but bookkeeping is the daemon's
job").
"""
from __future__ import annotations

import json
import logging
import os
import signal
import time
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path

from vq import cgroup, process_group
from vq.spec import JobSpec, JobState, utcnow_iso

log = logging.getLogger(__name__)


def read_host_memory_pressure_pct() -> float | None:
    """v0.6.20: read /proc/meminfo and return the host-memory pressure
    percentage (100 * (MemTotal - MemAvailable) / MemTotal). Higher =
    more pressure.

    MemAvailable is the "right" denominator because it accounts for
    reclaimable page cache + slab — `(MemTotal - MemFree) / MemTotal`
    would mark a typical Linux box at 95%+ permanently from page cache
    and trigger spurious pause cycles. MemAvailable subtracts that
    reclaimable headroom out and leaves the "actually under pressure"
    number.

    Returns None on hosts without ``/proc/meminfo`` (macOS dev box,
    BSD, container without /proc) — the caller treats None as
    "host-pressure check not available; no-op."

    Best-effort: any parse failure, missing field, or OSError returns
    None rather than raising. The daemon must NOT die because the
    pressure probe can't read /proc.
    """
    try:
        with open("/proc/meminfo", encoding="utf-8") as f:
            total_kb: int | None = None
            avail_kb: int | None = None
            for line in f:
                if line.startswith("MemTotal:"):
                    total_kb = int(line.split()[1])
                elif line.startswith("MemAvailable:"):
                    avail_kb = int(line.split()[1])
                if total_kb is not None and avail_kb is not None:
                    break
    except (OSError, ValueError, IndexError):
        return None
    if total_kb is None or avail_kb is None or total_kb <= 0:
        return None
    used = max(0, total_kb - avail_kb)
    return 100.0 * used / total_kb


class HostPressureAction(StrEnum):
    """v0.6.20: watchdog verdict for the host-pressure pass.

    NO_OP — pressure within acceptable range, or pressure unreadable
      (e.g. macOS / no /proc), or no running jobs to act on. The
      daemon does nothing this tick.
    PAUSE — pressure is at or above ``host_pressure_pause_pct`` AND
      we haven't yet auto-paused; daemon should SIGSTOP every
      currently-RUNNING job (the watchdog has already recorded the
      jobid list so the resume pass below can reverse them).
    RESUME — pressure has receded below ``host_pressure_resume_pct``
      AND we previously auto-paused; daemon should SIGCONT the
      jobids the watchdog recorded on the prior PAUSE.

    The hysteresis (pause threshold > resume threshold) prevents
    flapping when memory hovers around the boundary.
    """

    NO_OP = "no_op"
    PAUSE = "pause"
    RESUME = "resume"


@dataclass
class HostPressureVerdict:
    """v0.6.20: outcome of ``Watchdog.check_host_pressure()``.

    ``action`` tells the daemon what to do this tick. ``jobids``
    carries the relevant jobid list:

      * PAUSE: jobids the daemon should pause
      * RESUME: jobids the daemon should resume
      * NO_OP: empty

    ``pressure_pct`` is the current measured pressure for logging.
    ``reason`` is a human-readable explanation for the daemon log.
    """

    action: HostPressureAction
    jobids: list[str]
    pressure_pct: float | None
    reason: str = ""


class WatchdogAction(StrEnum):
    """What the watchdog decided this iteration for one running job."""

    OK = "ok"
    SIGTERM = "sigterm"
    SIGKILL = "sigkill"
    REAPED = "reaped"  # process is gone; daemon should mark with `reason`


@dataclass
class Verdict:
    action: WatchdogAction
    reason: str = ""
    terminal_state: JobState | None = None  # for SIGTERM / SIGKILL / REAPED


@dataclass
class WatchdogJobState:
    """Per-job tracking the watchdog accumulates across sample passes."""

    started_monotonic: float
    last_sample_monotonic: float = 0.0
    last_cputime_seconds: float | None = None
    # WD-1 (v0.8.21): which reader produced ``last_cputime_seconds`` —
    # "cgroup" / "pgid" / "pid". The three counters have different baselines
    # (cgroup is the scope's cumulative; the pgid/pid walks sum live
    # processes), so a delta taken across a source switch is meaningless. We
    # only compute a CPU% when this sample's source matches the last one's.
    last_cputime_source: str | None = None
    last_wall_monotonic: float | None = None
    starve_since_monotonic: float | None = None
    sigterm_sent_at_monotonic: float | None = None
    sigterm_terminal_state: JobState | None = None
    # v0.6.17 audit fix: stamp the moment we emitted SIGKILL so the
    # next evaluate() can suppress further SIGKILL re-emissions for
    # the same job. Pre-v0.6.17, every evaluate after grace-expiry
    # would re-return WatchdogAction.SIGKILL — harmless against a
    # dying process (killpg is idempotent), but for a truly stuck
    # process (D-state on broken NFS, unkillable kernel hang) the
    # daemon would log "SIGKILL emitted" lines forever. The daemon's
    # _reap_finished_jobs loop still handles the actual termination
    # detection regardless — the watchdog's job ends once SIGKILL
    # has been dispatched.
    sigkill_emitted: bool = False
    # v0.5.48 (Bug B fix): track the spec.state we saw on the last
    # evaluate() so we can detect SUSPENDED -> RUNNING transitions and
    # call reset_sampling_for_resume automatically. Pre-v0.5.48,
    # reset_sampling_for_resume was defined but never invoked from
    # anywhere — pausing a CPU-bound job for >= starve_window_seconds
    # and then resuming would STARVED-kill it on the first post-resume
    # sample because the CPU% delta was computed against pre-pause
    # values (near-zero) while starve_since_monotonic was already
    # ticking. See docs/audit_2026-05-17 § 2f.
    last_observed_spec_state: JobState | None = None


@dataclass
class Watchdog:
    """Per-daemon watchdog. One instance shared across all running jobs.

    Defaults are conservative: sample every 5s, kill at 90% host RSS,
    declare CPU-starved after 5 minutes below 5%, escalate SIGTERM to
    SIGKILL after a 10s grace.

    ``enforce_memory`` toggles the watchdog's per-job memory kill path.
    The daemon turns it off when cgroup-v2 enforcement is active (v0.4+)
    so the kernel's MemoryMax handles that kill in-cgroup without a
    duplicate watchdog kill.

    ``enforce_wall_time`` is on by default and stays on regardless of
    cgroup state (v0.5.8): the watchdog is the single owner of wall-time
    enforcement, because it correctly subtracts ``paused_seconds_total``
    from elapsed and systemd's ``RuntimeMaxSec`` (a) is not pause-aware
    and (b) is not runtime-mutable via ``set-property``. Tests can pass
    ``enforce_wall_time=False`` to suppress the kill path for harness
    purposes.

    Important: ``enforce_memory=False`` does NOT disable the host-percent
    RSS ceiling -- only the per-job ``spec.mem_mb`` check. The host
    ceiling is the safety net for jobs that don't declare ``mem_mb``
    (which most current vibe-qc submissions don't), and cgroups can't
    cover that case because they only enforce declared per-job caps.

    CPU-starvation detection is unrelated to cgroups and stays on
    regardless of either flag.
    """

    interval_seconds: float = 5.0
    grace_seconds: float = 10.0
    max_rss_percent: float = 90.0
    starve_threshold_percent: float = 5.0
    starve_window_seconds: float = 300.0
    host_total_mem_mb: int | None = None
    enforce_memory: bool = True
    enforce_wall_time: bool = True

    # v0.6.20: host-pressure auto-pause. When global memory pressure
    # (100 * (MemTotal - MemAvailable) / MemTotal) crosses
    # host_pressure_pause_pct, the watchdog signals PAUSE to the
    # daemon — which SIGSTOPs every running job to free pressure
    # before the kernel OOM-killer wakes up. When pressure recedes
    # below host_pressure_resume_pct (hysteresis margin), the
    # daemon SIGCONTs the same jobs.
    #
    # Why pause not kill: a long-running calculation shouldn't be
    # murdered because Steam started up. Pausing freezes its RAM
    # at the current footprint (RAM still allocated, not growing);
    # the kernel preferentially OOM-kills cgroups that are still
    # ALLOCATING — so frozen jobs become low-priority OOM-kill
    # targets. By the time pressure drops, SIGCONT resumes them.
    #
    # Default thresholds (85 / 70) are conservative: pause when the
    # box is 85% memory-pressured, resume when it drops to 70%.
    # That leaves a 15% headroom for the page-cache + small spikes
    # without flapping. The existing 90% max_rss_percent kill path
    # stays as a last-resort: pause is a softer EARLIER intervention.
    host_pressure_pause_pct: float = 85.0
    host_pressure_resume_pct: float = 70.0
    enforce_host_pressure_pause: bool = True

    _states: dict[str, WatchdogJobState] = field(default_factory=dict)
    # v0.6.20: track which jobids we paused due to host pressure so
    # the resume pass only SIGCONTs the ones we paused — operator-
    # paused jobs stay paused. Cleared on each PAUSE / RESUME cycle.
    _host_pressure_paused_jobids: set[str] = field(default_factory=set)
    _host_pressure_active: bool = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def register(self, jobid: str, *, started_monotonic: float | None = None) -> None:
        """Register a newly-dispatched job. Idempotent."""
        if jobid in self._states:
            return
        self._states[jobid] = WatchdogJobState(
            started_monotonic=started_monotonic
            if started_monotonic is not None
            else time.monotonic()
        )

    def unregister(self, jobid: str) -> None:
        self._states.pop(jobid, None)

    def reset_sampling_for_resume(self, jobid: str) -> None:
        """Call when a SUSPENDED job becomes RUNNING again so that the
        next sample's CPU% delta isn't computed against the pre-pause
        cputime (would yield a near-zero CPU% reading and trip the
        starvation detector). Also clears any in-flight starve counter."""
        st = self._states.get(jobid)
        if st is None:
            return
        st.last_cputime_seconds = None
        st.last_cputime_source = None  # WD-1: re-baseline source on resume too
        st.last_wall_monotonic = None
        st.starve_since_monotonic = None
        st.last_sample_monotonic = 0.0  # force a fresh sample asap

    # ------------------------------------------------------------------
    # v0.6.20: host-pressure pass — global memory monitoring
    # ------------------------------------------------------------------

    def check_host_pressure(
        self,
        running_jobids: list[str],
        *,
        _pressure_reader: callable = read_host_memory_pressure_pct,  # type: ignore[type-arg, valid-type]
    ) -> HostPressureVerdict:
        """v0.6.20: decide whether to pause-all-running or
        resume-the-jobs-we-paused based on current host memory
        pressure. Called once per daemon iterate() tick — NOT
        per-job — because the decision is host-wide.

        ``running_jobids`` is the daemon's current list of
        RUNNING-state job IDs (i.e. spec.state == RUNNING). The
        watchdog uses this to:
          * On PAUSE: record the set so the eventual RESUME
            tick targets exactly the jobs we paused (operator-
            paused jobs are NOT in this list and stay paused).
          * On RESUME: replay the set we recorded.

        ``_pressure_reader`` is injectable for testing — production
        uses :func:`read_host_memory_pressure_pct` which probes
        ``/proc/meminfo``.

        Returns :class:`HostPressureVerdict`. ``NO_OP`` is the
        default; the daemon does nothing in that case.

        Disabled in two ways: ``enforce_host_pressure_pause=False``
        on the watchdog, or ``_pressure_reader`` returns None
        (macOS / no /proc / parse error). Either path is a clean
        NO_OP — the pause path doesn't kick in on hosts where it
        can't measure pressure.
        """
        if not self.enforce_host_pressure_pause:
            return HostPressureVerdict(
                action=HostPressureAction.NO_OP,
                jobids=[],
                pressure_pct=None,
                reason="enforce_host_pressure_pause=False",
            )

        pressure = _pressure_reader()
        if pressure is None:
            return HostPressureVerdict(
                action=HostPressureAction.NO_OP,
                jobids=[],
                pressure_pct=None,
                reason="pressure probe unavailable (no /proc/meminfo)",
            )

        # Hysteresis state machine: pause once we cross the upper
        # threshold; only resume once we drop below the lower one.
        if not self._host_pressure_active:
            # Currently inactive. Pause if pressure crossed up.
            if pressure >= self.host_pressure_pause_pct and running_jobids:
                # Record the jobs we're about to pause so RESUME
                # targets exactly them.
                self._host_pressure_paused_jobids = set(running_jobids)
                self._host_pressure_active = True
                return HostPressureVerdict(
                    action=HostPressureAction.PAUSE,
                    jobids=list(running_jobids),
                    pressure_pct=pressure,
                    reason=(
                        f"host memory pressure {pressure:.1f}% "
                        f">= pause threshold "
                        f"{self.host_pressure_pause_pct:.1f}%; "
                        f"SIGSTOPping {len(running_jobids)} running "
                        f"job(s) to avoid OOM cascade"
                    ),
                )
            return HostPressureVerdict(
                action=HostPressureAction.NO_OP,
                jobids=[],
                pressure_pct=pressure,
                reason=f"pressure {pressure:.1f}% within bounds",
            )

        # Currently active. Resume if pressure dropped under the
        # lower threshold. Otherwise stay paused.
        if pressure <= self.host_pressure_resume_pct:
            paused = list(self._host_pressure_paused_jobids)
            self._host_pressure_paused_jobids.clear()
            self._host_pressure_active = False
            return HostPressureVerdict(
                action=HostPressureAction.RESUME,
                jobids=paused,
                pressure_pct=pressure,
                reason=(
                    f"host memory pressure {pressure:.1f}% "
                    f"<= resume threshold "
                    f"{self.host_pressure_resume_pct:.1f}%; "
                    f"SIGCONTing {len(paused)} previously-paused "
                    f"job(s)"
                ),
            )
        return HostPressureVerdict(
            action=HostPressureAction.NO_OP,
            jobids=[],
            pressure_pct=pressure,
            reason=(
                f"still paused; pressure {pressure:.1f}% above "
                f"resume threshold {self.host_pressure_resume_pct:.1f}%"
            ),
        )

    @property
    def host_pressure_active(self) -> bool:
        """HP-2 (v0.8.24): True when we've auto-paused running jobs for host
        memory pressure and not yet resumed them. The daemon gates new
        dispatch on this — putting fresh jobs onto a host we're actively
        relieving would defeat the pause."""
        return self._host_pressure_active

    def reseed_host_pressure_paused(self, jobids: list[str]) -> None:
        """HP-1 (v0.8.24): re-establish, after a daemon restart, the in-memory
        record of which jobs we auto-paused for host pressure.

        That record (``_host_pressure_paused_jobids`` / ``_host_pressure_
        active``) lives only in the process; a restart wipes it, so the
        SUSPENDED jobs we paused would be stranded — the RESUME tick only
        SIGCONTs jobs it remembers pausing. The daemon calls this at startup
        with the jobids of SUSPENDED specs tagged ``watchdog_host_pressure``
        (operator-paused jobs are excluded by the caller and stay paused), so
        the next time pressure recedes they resume as if we'd never restarted.
        """
        if not jobids:
            return
        self._host_pressure_paused_jobids = set(jobids)
        self._host_pressure_active = True

    # ------------------------------------------------------------------
    # Per-iteration evaluation
    # ------------------------------------------------------------------

    def evaluate(
        self,
        jobid: str,
        pid: int,
        pgid: int | None,
        spec: JobSpec,
        *,
        cgroup_unit_name: str | None = None,
        cgroup_multi_user: bool = False,
    ) -> Verdict:
        """Return what to do with this running job, this iteration.

        Side effects (when sampling fires):
        * appends one record to ``<spec.cwd>/_vq/samples.jsonl``
        * mutates the per-job state (sample times, starve counter, kill stamps)

        The caller is the Daemon; it interprets the Verdict and acts:
        * OK -- no-op
        * SIGTERM -- caller must os.killpg(pgid, SIGTERM); record reason
        * SIGKILL -- caller must os.killpg(pgid, SIGKILL); same reason
        * REAPED -- the process is already gone; mark with `reason`/state
        """
        st = self._states.get(jobid)
        if st is None:
            # First sight; auto-register so callers don't have to remember.
            self.register(jobid)
            st = self._states[jobid]

        # v0.5.48 (Bug B fix): SUSPENDED -> RUNNING transition triggers a
        # fresh sampling baseline so the first post-resume sample doesn't
        # compute CPU% against pre-pause cputime (which would yield a
        # near-zero CPU% over a wall-clock window that includes the entire
        # pause duration, tripping the starvation detector). Also clears
        # any in-flight starve counter from before the pause.
        #
        # last_observed_spec_state is updated unconditionally below so a
        # SUSPENDED job that takes the early-OK return still records
        # "we saw it suspended" for the next evaluate to detect the resume.
        if (
            st.last_observed_spec_state == JobState.SUSPENDED
            and spec.state == JobState.RUNNING
        ):
            self.reset_sampling_for_resume(jobid)
        st.last_observed_spec_state = spec.state

        now_mono = time.monotonic()

        # Already-escalated takes precedence over every other check. Once
        # we've sent SIGTERM the only question is "has the grace expired
        # yet?" -- re-firing wall-time / RSS / starvation here would
        # overwrite the original kill reason and reset the grace clock.
        if st.sigterm_sent_at_monotonic is not None:
            # v0.6.17 audit fix: once we've ALREADY emitted SIGKILL,
            # don't emit it again. The daemon's _reap_finished_jobs
            # loop handles the actual termination detection
            # (popen.poll); the watchdog's role ends with the SIGKILL
            # dispatch. Without this guard, a stuck D-state process
            # (broken NFS, kernel hang) would generate "SIGKILL
            # emitted" log spam every evaluate-tick forever.
            if st.sigkill_emitted:
                return Verdict(action=WatchdogAction.OK)
            if now_mono - st.sigterm_sent_at_monotonic >= self.grace_seconds:
                st.sigkill_emitted = True
                return Verdict(
                    action=WatchdogAction.SIGKILL,
                    reason=f"grace expired after {self.grace_seconds}s",
                    terminal_state=st.sigterm_terminal_state,
                )
            return Verdict(action=WatchdogAction.OK)

        # SUSPENDED jobs are SIGSTOP'd. They use no CPU, their RSS is
        # frozen (so the OOM check is moot for the duration), and we
        # do not want pause time to count against wall_time_seconds.
        # Skip every kill path; the user resumes via `vq resume`.
        if spec.state == JobState.SUSPENDED:
            return Verdict(action=WatchdogAction.OK)

        # Hard wall-clock limit fires regardless of sampling cadence.
        # The watchdog is the single owner of wall-time since v0.5.8;
        # we subtract paused_seconds_total so a job that was paused for
        # an hour doesn't lose an hour of its wall budget.
        if (
            self.enforce_wall_time
            and spec.wall_time_seconds is not None
        ):
            elapsed_active = (now_mono - st.started_monotonic) - spec.paused_seconds_total
            if elapsed_active >= spec.wall_time_seconds:
                return self._escalate(
                    st,
                    now_mono,
                    terminal_state=JobState.TIME_EXCEEDED,
                    reason=(
                        f"wall_time_seconds exceeded "
                        f"({spec.wall_time_seconds}s, "
                        f"paused total {spec.paused_seconds_total:.0f}s)"
                    ),
                )

        # Sample only if interval has elapsed (cheap when called from a
        # daemon loop with poll_interval << interval).
        if now_mono - st.last_sample_monotonic < self.interval_seconds:
            return Verdict(action=WatchdogAction.OK)

        # v0.5.12: sample the WHOLE process group, not just popen.pid.
        # The orphan-recovery wrapper makes popen.pid the collector, which
        # spends most of its time waiting and reports ~0% CPU. Sampling pgid-wide
        # captures the inner crystal/python/orca + any MPI ranks /
        # OMP forks. Bonus: the earlier per-pid watchdog under-reported
        # MPI/OMP child usage too, so this is strictly more accurate.
        # Fallback to per-pid only when pgid is None (macOS test
        # fixtures; pre-v0.3 specs that lacked pgid get ABORTED_BY_QUEUE
        # at startup so they never reach the watchdog).
        #
        # v0.5.38: PREFER cgroup-v2 counters when the job runs under a
        # systemd-user scope (every modern vq host: see cgroup.available()
        # in `daemon._start_job`). The cgroup contains all descendants by
        # kernel-managed hierarchy, so pgid escapes (ninja's setsid /
        # PR_SET_PGID for cc1plus, mpirun's per-rank sessions, ...) are
        # invisible to the pgid-walk readers but fully visible to
        # ``cpu.stat`` / ``memory.current``. Concrete prior bug fix: a
        # vibe-qc rebuild via `pip install -e .` got STARVED-killed at
        # the 5-minute starve window even though 32 cc1plus instances
        # were saturating workstation; the pgid-walk saw ~0% the whole time
        # because ninja escapes the parent pgid. cgroup readers also
        # avoid the shared-page double-count problem of the pid-sum.
        # Falls back to pgid-walk on macOS (no /proc) and Linux hosts
        # without cgroup-v2 delegation.
        rss_mb: int | None = None
        cputime: float | None = None
        cputime_source: str | None = None  # WD-1: tag the reader (see below)
        cgroup_path: str | None = None
        cgroup_lookup: str | None = None
        if cgroup_unit_name is not None:
            cgroup_path = cgroup.cgroup_path_for_scope(
                cgroup_unit_name, multi_user=cgroup_multi_user
            )
            if cgroup_path is not None:
                cgroup_lookup = "scope"
        if cgroup_path is None:
            cgroup_path = cgroup.cgroup_path_for_pid(pid)
            if cgroup_path is not None:
                cgroup_lookup = "pid"
        if cgroup_path is not None:
            cputime = cgroup.read_cpu_usage_seconds(cgroup_path)
            if cputime is not None:
                cputime_source = "cgroup"
            rss_mb = cgroup.read_memory_current_mb(cgroup_path)
        # Fall back to the pgid-walk for any field cgroup didn't fill
        # (covers: (a) macOS / no cgroup-v2, (b) cgroup present but
        # cpu.stat / memory.current absent — e.g. host without
        # delegation of the relevant controllers).
        if cputime is None or rss_mb is None:
            if pgid is not None:
                if rss_mb is None:
                    rss_mb = read_rss_mb_pgid(pgid)
                if cputime is None:
                    cputime = read_cputime_seconds_pgid(pgid)
                    if cputime is not None:
                        cputime_source = "pgid"
            else:
                if rss_mb is None:
                    rss_mb = read_rss_mb(pid)
                if cputime is None:
                    cputime = read_cputime_seconds(pid)
                    if cputime is not None:
                        cputime_source = "pid"
        if rss_mb is None and cputime is None:
            # Either the process is gone or /proc isn't available
            # (macOS dev box). On Linux, "both None" almost always means
            # the pid disappeared between the daemon's reconciler and
            # this sampler -- nothing to do here, the reconciler will
            # pick it up next iteration.
            st.last_sample_monotonic = now_mono
            return Verdict(action=WatchdogAction.OK)

        # Compute CPU percent over the wall delta since last sample.
        cpu_pct: float | None = None
        if (
            cputime is not None
            and st.last_cputime_seconds is not None
            # WD-1: only diff against a baseline from the SAME reader — a
            # cgroup↔pgid↔pid switch makes the delta meaningless. A baseline
            # whose source we never recorded (None — the very first sample, a
            # post-resume re-baseline, or a test that seeds last_cputime_seconds
            # directly) isn't second-guessed; once a source IS known a switch
            # invalidates the diff. In production the two fields are always set
            # and cleared together, so "source None" only ever coincides with
            # "no baseline" — this branch changes nothing there.
            and (
                st.last_cputime_source is None
                or cputime_source == st.last_cputime_source
            )
        ):
            wall_delta = now_mono - (st.last_wall_monotonic or now_mono)
            cputime_delta = cputime - st.last_cputime_seconds
            # WD-1: a negative delta means cputime DROPPED between samples — a
            # child exited and left the pgid / cgroup aggregate. The ratio is
            # garbage (a negative cpu_percent in samples.jsonl, and a spurious
            # STARVED reading), so skip this sample's CPU% and re-baseline from
            # the new cputime for the next interval.
            if wall_delta > 0 and cputime_delta >= 0:
                cpu_pct = 100.0 * cputime_delta / wall_delta
        st.last_cputime_seconds = cputime
        st.last_cputime_source = cputime_source
        st.last_wall_monotonic = now_mono
        st.last_sample_monotonic = now_mono

        # Persist the sample line. Watchdog evaluation does not mutate JobSpec.
        elapsed = now_mono - st.started_monotonic
        sample = {
            "ts": utcnow_iso(),
            "elapsed_seconds": round(elapsed, 3),
            "rss_mb": rss_mb,
            "cpu_percent": round(cpu_pct, 1) if cpu_pct is not None else None,
            "cpu_time_seconds": round(cputime, 3) if cputime is not None else None,
            "cpu_time_source": cputime_source,
            "cgroup_lookup": cgroup_lookup,
            "cgroup_path": cgroup_path,
            "sample_pid": pid,
            "sample_pgid": pgid,
        }
        try:
            _append_sample(Path(spec.cwd), sample)
        except OSError as e:
            log.warning("watchdog: cannot write samples for %s: %s", jobid, e)

        # Memory-cap check. Two ceilings; whichever hits first wins.
        # 1) Per-job declared cap: spec.mem_mb (skipped when cgroup
        #    enforces this -- enforce_memory=False -- to avoid the
        #    double-kill from kernel + watchdog).
        # 2) Absolute host ceiling: host_total * max_rss_percent. ALWAYS
        #    on regardless of enforce_memory, because cgroups only enforce
        #    declared per-job caps, NOT a host-wide percentage. A job
        #    that doesn't declare mem_mb has no kernel cap, so the
        #    watchdog stays as the host-percent backstop. Undeclared
        #    jobs are exactly the ones most likely to need the safety
        #    net; disabling it would regress v0.3 behaviour.
        if rss_mb is not None:
            if (
                self.enforce_memory
                and spec.mem_mb is not None
                and rss_mb > spec.mem_mb
            ):
                return self._escalate(
                    st,
                    now_mono,
                    terminal_state=JobState.OOM_KILLED,
                    reason=(
                        f"RSS {rss_mb} MB exceeded declared mem_mb={spec.mem_mb}"
                    ),
                )
            # v0.6.17 audit fix: guard against host_total_mem_mb=0
            # in addition to None. A misconfigured /proc/meminfo
            # parser, a future psutil-free fallback that returns 0
            # on parse failure, or a CLI passing --host-total-mem-mb
            # 0 would otherwise set host_cap=0 and OOM-kill every
            # job with rss > 0 — and the operator-facing reason
            # would read "host cap 90% of 0 MB (0 MB)", which is
            # confusing rather than diagnostic.
            if self.host_total_mem_mb is not None and self.host_total_mem_mb > 0:
                host_cap = int(self.host_total_mem_mb * self.max_rss_percent / 100.0)
                if rss_mb > host_cap:
                    return self._escalate(
                        st,
                        now_mono,
                        terminal_state=JobState.OOM_KILLED,
                        reason=(
                            f"RSS {rss_mb} MB exceeded host cap "
                            f"{self.max_rss_percent:.0f}% of "
                            f"{self.host_total_mem_mb} MB ({host_cap} MB)"
                        ),
                    )

        # CPU-starvation check. Only meaningful once we have at least one
        # cpu_pct measurement (i.e. second sample onwards).
        if cpu_pct is not None and cpu_pct < self.starve_threshold_percent:
            if st.starve_since_monotonic is None:
                st.starve_since_monotonic = now_mono
            elif now_mono - st.starve_since_monotonic >= self.starve_window_seconds:
                source_detail = f", source={cputime_source or 'unknown'}"
                if cgroup_lookup is not None:
                    source_detail += f", cgroup_lookup={cgroup_lookup}"
                if cgroup_path is not None:
                    source_detail += f", cgroup={cgroup_path}"
                return self._escalate(
                    st,
                    now_mono,
                    terminal_state=JobState.STARVED,
                    reason=(
                        f"CPU < {self.starve_threshold_percent:.1f}% for "
                        f">= {self.starve_window_seconds:.0f}s "
                        f"(elapsed {elapsed:.0f}s{source_detail})"
                    ),
                )
        else:
            st.starve_since_monotonic = None

        return Verdict(action=WatchdogAction.OK)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _escalate(
        self,
        st: WatchdogJobState,
        now_mono: float,
        *,
        terminal_state: JobState,
        reason: str,
    ) -> Verdict:
        st.sigterm_sent_at_monotonic = now_mono
        st.sigterm_terminal_state = terminal_state
        return Verdict(
            action=WatchdogAction.SIGTERM,
            reason=reason,
            terminal_state=terminal_state,
        )


# ----------------------------------------------------------------------
# /proc readers (free functions so they're trivially mocked in tests)
# ----------------------------------------------------------------------


def read_rss_mb(pid: int) -> int | None:
    """Return resident-set size in MB for ``pid``, or None if unavailable.

    Reads ``/proc/<pid>/status`` and parses ``VmRSS:``. Returns None on
    macOS (no /proc), permission errors, or if the process has exited.
    """
    try:
        with open(f"/proc/{pid}/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) // 1024  # kB -> MB
    except (OSError, ValueError):
        return None
    return None


def read_cputime_seconds(pid: int) -> float | None:
    """Return cumulative user+system CPU time in seconds, or None.

    Parses /proc/<pid>/stat with the closing-paren trick so the comm
    field's spaces / parens / unicode don't break field counting.
    """
    try:
        with open(f"/proc/{pid}/stat") as f:
            content = f.read()
        # Skip past "(comm)" by finding the last ')'.
        rparen = content.rfind(")")
        if rparen == -1:
            return None
        # After comm: state ppid pgrp session tty_nr tpgid flags
        #             minflt cminflt majflt cmajflt utime stime ...
        # Indices (0-based after comm): utime=11, stime=12.
        rest = content[rparen + 1 :].split()
        utime_ticks = int(rest[11])
        stime_ticks = int(rest[12])
        clock_ticks_per_sec = os.sysconf("SC_CLK_TCK") or 100
        return (utime_ticks + stime_ticks) / clock_ticks_per_sec
    except (OSError, ValueError, IndexError):
        return None


def _pgid_pids(pgid: int) -> list[int]:
    """Return PIDs whose process-group ID matches ``pgid``.

    Walks /proc; returns empty list on macOS (no /proc) or any
    unrecoverable error. Tolerates pid disappearance during the walk
    (FileNotFoundError on individual /proc/<pid>/stat reads) since
    long-lived watchdog samples will overlap with rapid fork/exec
    cycles in workloads like Pcrystal startup.

    Single /proc scan per call is acceptable cost at the 5s sample
    cadence: even on a busy box /proc has O(N) entries where N is
    process count, and each stat read is < 1KB. Total per sample:
    ~milliseconds.
    """
    pids: list[int] = []
    try:
        entries = os.listdir("/proc")
    except OSError:
        return []
    for entry in entries:
        if not entry.isdigit():
            continue
        pid = int(entry)
        try:
            with open(f"/proc/{pid}/stat") as f:
                content = f.read()
        except (OSError, FileNotFoundError):
            # pid disappeared between listdir and open; skip
            continue
        # Skip past "(comm)" — comm can contain spaces / parens, so
        # find the LAST ')' to anchor field parsing.
        rparen = content.rfind(")")
        if rparen == -1:
            continue
        rest = content[rparen + 1 :].split()
        try:
            # Fields after comm:
            #   state(0) ppid(1) pgrp(2) session(3) ...
            pgrp = int(rest[2])
        except (IndexError, ValueError):
            continue
        if pgrp == pgid:
            pids.append(pid)
    return pids


def read_rss_mb_pgid(pgid: int) -> int | None:
    """Sum VmRSS across every process in pgroup ``pgid``, in MB.

    Returns None on macOS (no /proc) or if no live processes match
    the pgid. Otherwise returns the aggregate RSS — a conservative
    upper bound on memory in use by the job, since processes in the
    same pgid may share pages (fork-without-exec) that this sum
    double-counts. For watchdog purposes the slight over-count is
    fine: false-positive memory kills on shared-page-heavy
    workloads aren't a known failure mode, and the alternative
    (USS / PSS via smaps_rollup) is several × more expensive to
    read.
    """
    pids = _pgid_pids(pgid)
    if not pids:
        return None
    total_mb = 0
    seen_any = False
    for pid in pids:
        mb = read_rss_mb(pid)
        if mb is not None:
            total_mb += mb
            seen_any = True
    return total_mb if seen_any else None


def read_cputime_seconds_pgid(pgid: int) -> float | None:
    """Sum user+system CPU time across every process in pgroup
    ``pgid``, in seconds. Returns None when /proc isn't readable or
    the pgid has no live processes.

    Cumulative since each process started. A child that forks IN between
    samples adds fresh cputime starting at ~0, so the next delta correctly
    counts its work. But a child that EXITS between samples takes its
    accumulated cputime out of this sum with it — so the total can *drop*,
    and the next delta goes negative (it does NOT "stay on a flat plateau",
    as an earlier version of this comment wrongly claimed). The watchdog's
    sampler guards against that (WD-1): a negative delta — or a switch
    between the cgroup / pgid / pid readers, whose baselines differ — is
    discarded and re-baselined rather than logged as a bogus CPU%.
    """
    pids = _pgid_pids(pgid)
    if not pids:
        return None
    total = 0.0
    seen_any = False
    for pid in pids:
        ct = read_cputime_seconds(pid)
        if ct is not None:
            total += ct
            seen_any = True
    return total if seen_any else None


def _append_sample(workspace: Path, sample: dict) -> None:
    """Atomic-ish append of one JSON line to ``<workspace>/_vq/samples.jsonl``."""
    vq_dir = workspace / "_vq"
    vq_dir.mkdir(exist_ok=True)
    with (vq_dir / "samples.jsonl").open("a", encoding="utf-8") as f:
        f.write(json.dumps(sample, sort_keys=True) + "\n")


# ----------------------------------------------------------------------
# Kill helper (lives here so the daemon doesn't need to know signal semantics)
# ----------------------------------------------------------------------


def killpg(pgid: int, sig: int) -> bool:
    """Send ``sig`` to process group ``pgid``. Returns True if signal landed,
    False if the group is gone (already exited), including one that was still
    tearing down, which macOS answers with ``EPERM`` for a moment (#27)."""
    try:
        process_group.signal_process_group(pgid, sig)
        return True
    except ProcessLookupError:
        return False
    except PermissionError as e:
        log.warning("killpg: permission denied for pgid %s: %s", pgid, e)
        return False


# Re-export common signals for callers that don't want to import signal.
SIGTERM = signal.SIGTERM
SIGKILL = signal.SIGKILL
