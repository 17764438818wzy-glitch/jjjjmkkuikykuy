import threading

from test_control_admission import ready_control
from paperflow_prefect.controlled_first_step import run_controlled


def test_eight_real_group_workers_pause_and_resume_without_new_group(tmp_path):
    ctl, scheduler, task = ready_control(tmp_path, groups=9)
    generation = scheduler.calls[-1][1]
    entered, release = threading.Event(), threading.Event()
    starts, lock = [], threading.Lock()

    def process(seq, path):
        with lock:
            starts.append(seq)
            if len(starts) == 8:
                entered.set()
        assert release.wait(5)
        return {'group_root': path, 'error': '合成测试'}

    thread = threading.Thread(target=run_controlled, args=(ctl, task, generation, 'run-one', process))
    thread.start()
    assert entered.wait(5)
    state = ctl.pause(task)
    assert state['task_status'] == 'pausing' and state['active_groups'] == 8
    release.set()
    thread.join(5)
    assert not thread.is_alive() and len(starts) == 8
    state = ctl.summary.read(task, None, 1, 50)
    assert state['task_status'] == 'paused' and state['processed_groups'] == 8
    ctl.start(task, state['control_revision'])
    run_controlled(ctl, task, scheduler.calls[-1][1], 'run-two', process)
    state = ctl.summary.read(task, None, 1, 50)
    assert len(starts) == 9 and len(set(starts)) == 9
    assert state['task_status'] == 'complete' and state['processed_groups'] == 9


def test_outdated_run_cannot_claim_after_pause_and_new_start(tmp_path):
    ctl, scheduler, task = ready_control(tmp_path, groups=1)
    old_generation = scheduler.calls[-1][1]
    assert ctl.pause(task)['task_status'] == 'paused'
    current = ctl.summary.read(task, None, 1, 50)
    ctl.start(task, current['control_revision'])
    called = []
    run_controlled(ctl, task, old_generation, 'old-run', lambda *args: called.append(args))
    assert not called


def test_offline_first_step_also_runs_eight_groups(tmp_path, monkeypatch):
    from paperflow_prefect import group_classification
    library = tmp_path / 'library'
    groups = [library / f'person-{i}' for i in range(9)]
    for path in groups:
        path.mkdir(parents=True)
    starts, lock, entered, release = [], threading.Lock(), threading.Event(), threading.Event()
    def process(path, *args, **kwargs):
        with lock:
            starts.append(path)
            if len(starts) == 8:
                entered.set()
        assert release.wait(5)
        return {'group_root': str(path), 'error': '合成测试'}
    monkeypatch.setattr(group_classification, 'process_one_group', process)
    outcome = []
    def run():
        outcome.extend(group_classification.classify_batch(groups, library, tmp_path / 'output',
                                                           lambda: None, lambda _: {}))
    thread = threading.Thread(target=run)
    thread.start()
    try:
        assert entered.wait(5)
        assert len(starts) == 8
    finally:
        release.set()
        thread.join(5)
    assert not thread.is_alive() and len(outcome) == 9 and len(starts) == 9
