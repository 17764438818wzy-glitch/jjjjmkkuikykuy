import threading

from test_control_admission import ready_control
from paperflow_prefect.controlled_first_step import run_controlled
from paperflow_prefect.first_step_limits import GROUP_CONCURRENCY


def test_configured_group_workers_pause_and_resume_without_new_group(tmp_path):
    ctl, scheduler, task = ready_control(tmp_path, groups=GROUP_CONCURRENCY + 1)
    generation = scheduler.calls[-1][1]
    entered, release = threading.Event(), threading.Event()
    starts, lock = [], threading.Lock()

    def process(seq, path):
        with lock:
            starts.append(seq)
            if len(starts) == GROUP_CONCURRENCY:
                entered.set()
        assert release.wait(5)
        return {'group_root': path, 'error': '合成测试'}

    thread = threading.Thread(target=run_controlled, args=(ctl, task, generation, 'run-one', process))
    thread.start()
    assert entered.wait(5)
    state = ctl.pause(task)
    assert state['task_status'] == 'pausing' and state['active_groups'] == GROUP_CONCURRENCY
    release.set()
    thread.join(5)
    assert not thread.is_alive() and len(starts) == GROUP_CONCURRENCY
    state = ctl.summary.read(task, None, 1, 50)
    assert state['task_status'] == 'paused' and state['processed_groups'] == GROUP_CONCURRENCY
    ctl.start(task, state['control_revision'])
    run_controlled(ctl, task, scheduler.calls[-1][1], 'run-two', process)
    state = ctl.summary.read(task, None, 1, 50)
    assert len(starts) == GROUP_CONCURRENCY + 1 and len(set(starts)) == GROUP_CONCURRENCY + 1
    assert state['task_status'] == 'complete' and state['processed_groups'] == GROUP_CONCURRENCY + 1


def test_outdated_run_cannot_claim_after_pause_and_new_start(tmp_path):
    ctl, scheduler, task = ready_control(tmp_path, groups=1)
    old_generation = scheduler.calls[-1][1]
    assert ctl.pause(task)['task_status'] == 'paused'
    current = ctl.summary.read(task, None, 1, 50)
    ctl.start(task, current['control_revision'])
    called = []
    run_controlled(ctl, task, old_generation, 'old-run', lambda *args: called.append(args))
    assert not called
