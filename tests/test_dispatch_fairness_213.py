"""Slow scheduler admission cannot monopolize independent resources (#213)."""
from tests.test_daemon_scheduler import daemon, _submit_scheduler  # noqa: F401
from vq import daemon as daemon_mod, config
from vq.spec import JobState, JobSpec


def slow_dispatch(daemon, monkeypatch):
    clock = [0.0]
    order = []
    monkeypatch.setattr(daemon_mod.time, 'monotonic', lambda: clock[0])
    monkeypatch.setattr(daemon_mod, 'SCHEDULER_DISPATCH_RECONCILE_QUANTUM_SECONDS', .5)
    monkeypatch.setattr(daemon, '_reconcile_scheduler', lambda: None)
    def start(spec):
        order.append(spec.id)
        if spec.scheduler_target is not None: clock[0] += 1
        spec.state = JobState.RUNNING
        spec.write(daemon._spec_path(spec.id))
        return True
    monkeypatch.setattr(daemon, '_start_job', start)
    return order


def test_local_admission_precedes_slow_scheduler_quantum(daemon, monkeypatch):
    for i in range(3): _submit_scheduler(daemon, f'old{i}')
    local = _submit_scheduler(daemon, 'local')
    local.scheduler_target = None
    local.write(daemon._spec_path(local.id))
    order = slow_dispatch(daemon, monkeypatch)
    daemon._dispatch_pending()
    assert order == ['local', 'old0']


def test_independent_targets_get_turns_across_quanta(daemon, monkeypatch):
    for i in range(3): _submit_scheduler(daemon, f'a{i}', target='host_a')
    for i in range(2): _submit_scheduler(daemon, f'b{i}', target='host_b')
    order = slow_dispatch(daemon, monkeypatch)
    for _ in range(4): daemon._dispatch_pending()
    assert order == ['a0','b0','a1','b1']


def test_aliases_share_priority_fifo_and_one_turn(daemon, monkeypatch):
    cfg = config.Config(hosts={
        'localhost': config.HostConfig(ssh='localhost'),
        'host_a': config.HostConfig(ssh='a', scheduler='pbs', scheduler_dialect='torque', scratch_root='/tmp/work', scheduler_driver='localhost'),
        'alias_a': config.HostConfig(ssh='a', scheduler='pbs', scheduler_dialect='torque', scratch_root='/tmp/work', scheduler_driver='localhost',
                                   fleet_role='alias', fleet_canonical_host='host_a'),
        'host_b': config.HostConfig(ssh='b', scheduler='pbs', scheduler_dialect='torque', scratch_root='/tmp/work', scheduler_driver='localhost'),
    })
    monkeypatch.setattr(daemon_mod, 'load_config', lambda: cfg)
    for name, target, priority in [('a-low','host_a',0),('a-high','alias_a',3),
                                   ('b-low','host_b',0),('a-high2','host_a',3)]:
        spec = _submit_scheduler(daemon,name,target=target)
        spec.priority = priority; spec.write(daemon._spec_path(spec.id))
    order = slow_dispatch(daemon, monkeypatch)
    for _ in range(4): daemon._dispatch_pending()
    assert order == ['a-high','b-low','a-high2','a-low']


def test_local_cap_is_not_bypassed(daemon, monkeypatch):
    local = _submit_scheduler(daemon,'local',cpus=5)
    local.scheduler_target=None; local.write(daemon._spec_path(local.id))
    _submit_scheduler(daemon,'cluster')
    order=slow_dispatch(daemon,monkeypatch)
    daemon._dispatch_pending()
    assert order==['cluster']
    assert JobSpec.read(daemon._spec_path('local')).state == JobState.PENDING
