import threading
import time
import uuid

import pytest

from paperflow_prefect.first_step_control import FirstStepControl, PauseRequested, UnknownRequest, ControlError, _MODEL_GATE


class Scheduler:
    def __init__(self): self.calls = []
    def start(self, task_id, generation):
        self.calls.append((task_id, generation))
        return {'flow_run_id': 'synthetic'}


def ready_control(tmp_path, groups=4):
    scheduler = Scheduler()
    ctl = FirstStepControl(tmp_path / 'output', tmp_path / 'data', scheduler)
    task = ctl.create_task('测试任务')['task_id']
    upload = ctl.create_upload(task, 'root')['upload_id']
    files = [{'relative_path': f'root/person-{i}/paper.txt', 'size': 5} for i in range(groups)]
    ctl.tus.manifest(upload, files)
    ctl.tus.seal(upload, len(files))
    statuses = {}
    ctl.tus_dir.mkdir(parents=True, exist_ok=True)
    for entry in files:
        meta = {'task_id': task, 'folder_upload_id': upload, 'relativePath': entry['relative_path']}
        event = {'Type': 'pre-create', 'Event': {'Upload': {'Size': 5, 'MetaData': meta},
                 'HTTPRequest': {'Header': {'X-Request-Id': [str(uuid.uuid4())]}}}}
        tus_id = ctl.tus.hook(event)['ChangeFileInfo']['ID']
        (ctl.tus_dir / tus_id).write_bytes(b'paper')
        statuses[tus_id] = {'offset': 5, 'length': 5, 'metadata': meta}
    ctl._tus_head = lambda tus_id: statuses[tus_id]
    ready = ctl.tus.finish(upload)
    ctl.start(task, ready['control_revision'])
    return ctl, scheduler, task


def test_unknown_model_request_is_not_repeated_in_another_task(tmp_path):
    ctl, _, first = ready_control(tmp_path, groups=1)
    seq, _ = ctl.claim_next(first)
    ctl.bind_group_source(first, seq, 'same-source')
    def fail_receive(_):
        raise TimeoutError('synthetic unknown response')
    with pytest.raises(UnknownRequest):
        ctl.invoke_model(first, seq, 'body', 'first-snapshot', 'first-payload',
                         lambda: object(), fail_receive, timeout=1)
    other, _, second = ready_control(tmp_path, groups=1)
    seq2, _ = other.claim_next(second)
    other.bind_group_source(second, seq2, 'same-source')
    sent = []
    with pytest.raises(UnknownRequest):
        other.invoke_model(second, seq2, 'body', 'other-snapshot', 'other-payload',
                           lambda: sent.append(True), lambda _: {}, timeout=1)
    assert not sent


def test_unknown_request_blocks_only_matching_step_and_is_not_copied(tmp_path):
    first, _, task1 = ready_control(tmp_path, groups=1)
    seq1, _ = first.claim_next(task1)
    attachment_a = {'scope': 'root/person', 'kind': 'other', 'members': ['body', 'attachment-a']}
    def lost(_): raise TimeoutError('synthetic lost response')
    with pytest.raises(UnknownRequest):
        first.invoke_model(task1, seq1, 'other:a', 'snapshot', 'payload', lambda: object(), lost,
                           timeout=1, request_identity=attachment_a)

    second, _, task2 = ready_control(tmp_path, groups=1)
    seq2, _ = second.claim_next(task2)
    sent = []
    for step, key in [('body', {'scope': 'root/person', 'kind': 'body', 'members': ['body']}),
                      ('other:b', {'scope': 'root/person', 'kind': 'other', 'members': ['body', 'attachment-b']})]:
        second.invoke_model(task2, seq2, step, 'snapshot', step, lambda: sent.append(step), lambda _: {},
                            timeout=1, request_identity=key)
    with pytest.raises(UnknownRequest):
        second.invoke_model(task2, seq2, 'other:different-upload-id', 'snapshot', 'payload',
                            lambda: sent.append('unexpected'), lambda _: {}, timeout=1, request_identity=attachment_a)
    assert sent == ['body', 'other:b']
    with second.summary._connect() as db, db:
        assert db.execute("SELECT COUNT(*) FROM model_decisions WHERE task_id=? AND state='unknown'", (task2,)).fetchone()[0] == 0
        db.execute("UPDATE model_decisions SET state='complete' WHERE task_id=? AND seq=?", (task1, seq1))
    third, _, task3 = ready_control(tmp_path, groups=1)
    seq3, _ = third.claim_next(task3)
    third.invoke_model(task3, seq3, 'other:a', 'snapshot', 'payload', lambda: sent.append('after-confirmed'),
                       lambda _: {}, timeout=1, request_identity=attachment_a)
    assert sent[-1] == 'after-confirmed'


def test_changed_completed_request_does_not_become_a_false_unknown(tmp_path):
    control, _, task = ready_control(tmp_path, groups=1)
    seq, _ = control.claim_next(task)
    key = {'scope': 'root/person', 'kind': 'body', 'members': ['paper']}
    control.invoke_model(task, seq, 'body', 'snapshot', 'old-payload', lambda: None,
                         lambda _: {'answer': 'known'}, timeout=1, request_identity=key)
    with pytest.raises(ControlError):
        control.invoke_model(task, seq, 'body', 'snapshot', 'new-payload', lambda: None,
                             lambda _: {}, timeout=1, request_identity=key)
    with control.summary._connect() as db:
        assert db.execute('SELECT state FROM model_decisions WHERE task_id=?', (task,)).fetchone()[0] == 'complete'
    other, _, task2 = ready_control(tmp_path, groups=1)
    seq2, _ = other.claim_next(task2)
    assert other.invoke_model(task2, seq2, 'body', 'other-snapshot', 'new-payload', lambda: None,
                              lambda _: {'answer': 'new'}, timeout=1, request_identity=key) == {'answer': 'new'}


def test_legacy_unknown_attachment_survives_changed_uploaded_file_id(tmp_path):
    first, _, task1 = ready_control(tmp_path, groups=1)
    seq1, _ = first.claim_next(task1)
    first.bind_group_source(task1, seq1, 'same-group-files')
    def lost(_): raise TimeoutError('synthetic old unknown')
    with pytest.raises(UnknownRequest):
        first.invoke_model(task1, seq1, 'other:old-id', 'snapshot', 'payload', lambda: None, lost, timeout=1)
    second, _, task2 = ready_control(tmp_path, groups=1)
    seq2, _ = second.claim_next(task2)
    second.bind_group_source(task2, seq2, 'same-group-files')
    sent = []
    key = {'scope': 'root/person-0', 'kind': 'other', 'members': ['body', 'attachment']}
    with pytest.raises(UnknownRequest):
        second.invoke_model(task2, seq2, 'other:new-id', 'new-snapshot', 'payload',
                            lambda: sent.append(True), lambda _: {}, timeout=1, request_identity=key)
    assert not sent


def test_legacy_unknown_does_not_block_a_different_relative_group(tmp_path):
    first, _, task1 = ready_control(tmp_path, groups=1)
    seq1, _ = first.claim_next(task1)
    first.bind_group_source(task1, seq1, 'same-content')
    def lost(_): raise TimeoutError('synthetic old unknown')
    with pytest.raises(UnknownRequest):
        first.invoke_model(task1, seq1, 'body', 'snapshot', 'payload', lambda: None, lost, timeout=1)
    second, _, task2 = ready_control(tmp_path, groups=1)
    with second.summary._connect() as db, db:
        old_path = db.execute('SELECT group_root FROM control_groups WHERE task_id=?', (task2,)).fetchone()[0]
        from pathlib import Path
        db.execute('UPDATE control_groups SET group_root=? WHERE task_id=?',
                   (str(Path(old_path).with_name('other-person')), task2))
    seq2, _ = second.claim_next(task2)
    second.bind_group_source(task2, seq2, 'same-content')
    key = {'scope': 'root/other-person', 'kind': 'body', 'members': ['paper']}
    assert second.invoke_model(task2, seq2, 'body', 'new-snapshot', 'payload', lambda: None,
                               lambda _: {'answer': 'ok'}, timeout=1, request_identity=key) == {'answer': 'ok'}


def test_completed_task_remains_completed_after_worker_exits(tmp_path):
    from contextlib import closing
    ctl, scheduler, task = ready_control(tmp_path, groups=1)
    generation = scheduler.calls[-1][1]
    assert ctl.begin_run(task, generation, 'synthetic')
    seq, _ = ctl.claim_next(task, generation)
    ctl.finish_group(task, seq, generation, {'status': 'passed'})
    with closing(ctl.summary._connect()) as db, db:
        db.execute('UPDATE tasks SET worker_owner_pid=2147483647,worker_owner_started=1 WHERE task_id=?', (task,))
    ctl.refresh_task(task)
    assert ctl.summary.read(task, None, 1, 50)['task_status'] == 'complete'


def test_queue_timeout_rechecks_pause_before_recording_failure(tmp_path, monkeypatch):
    ctl, _, task = ready_control(tmp_path, groups=1)
    states = iter([True, False])
    monkeypatch.setattr(ctl, '_running', lambda *args: next(states))
    with pytest.raises(PauseRequested):
        ctl.invoke_model(task, 0, 'body', 'snapshot', 'payload', lambda: None,
                         lambda _: None, timeout=0)


def test_process_queue_wait_does_not_consume_model_response_timeout(tmp_path):
    control, _, task = ready_control(tmp_path, groups=1)
    seq, _ = control.claim_next(task)
    outcome = []
    def work():
        try: outcome.append(control.invoke_model(task, seq, 'body', 'snapshot', 'payload',
                                                  lambda: None, lambda _: {'answer': 'ok'}, timeout=.1))
        except Exception as exc: outcome.append(exc)
    assert _MODEL_GATE.acquire(timeout=1)
    thread = threading.Thread(target=work)
    try:
        thread.start()
        time.sleep(.25)
        assert thread.is_alive()
    finally:
        _MODEL_GATE.release()
    thread.join(2)
    assert outcome == [{'answer': 'ok'}]


def test_unknown_optional_title_does_not_block_other_groups_or_resume(tmp_path):
    ctl, scheduler, task = ready_control(tmp_path, groups=2)
    generation = scheduler.calls[-1][1]
    seq, _ = ctl.claim_next(task, generation)
    def unknown(_):
        raise TimeoutError('synthetic title response lost')
    with pytest.raises(UnknownRequest):
        ctl.invoke_model(task, seq, 'filename-title', 'snapshot', 'payload', lambda: object(),
                         unknown, timeout=1, generation=generation)
    assert ctl.summary.read(task, None, 1, 10)['task_status'] == 'running'
    next_group = ctl.claim_next(task, generation)
    assert next_group is not None
    ctl.pause(task)
    ctl.defer_group(task, seq, generation)
    ctl.defer_group(task, next_group[0], generation)
    state = ctl.summary.read(task, None, 1, 10)
    assert state['task_status'] == 'paused'
    assert state['unknown_requests'] == 1
    assert ctl.start(task, state['control_revision'])['task_status'] == 'starting'


def test_unknown_group_is_isolated_and_other_group_can_resume(tmp_path):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from paperflow_prefect.group_task_summary import install_group_summary_routes
    ctl, scheduler, task = ready_control(tmp_path, groups=2)
    generation = scheduler.calls[-1][1]
    seq, _ = ctl.claim_next(task, generation)
    def lost(_): raise TimeoutError('synthetic outcome unknown')
    with pytest.raises(UnknownRequest):
        ctl.invoke_model(task, seq, 'body', 'snapshot', 'payload', lambda: object(), lost,
                         timeout=1, generation=generation)
    ctl.mark_group_unknown(task, seq, generation, '正文请求结果未知')
    state = ctl.summary.read(task, None, 1, 10)
    assert state['task_status'] == 'running' and state['unknown_groups'] == 1
    assert ctl.pause(task)['task_status'] == 'paused'
    revision = ctl.summary.read(task, None, 1, 10)['control_revision']
    ctl.start(task, revision)
    generation = scheduler.calls[-1][1]
    seq2, _ = ctl.claim_next(task, generation)
    assert seq2 != seq
    ctl.finish_group(task, seq2, generation, {'status': 'passed'})
    state = ctl.summary.read(task, None, 1, 10)
    assert state['task_status'] == 'needs_review' and state['total_groups'] == 2
    assert state['processed_groups'] == 1 and state['passed_groups'] == 1 and state['unknown_groups'] == 1
    app = FastAPI()
    install_group_summary_routes(app, tmp_path / 'output')
    filtered = TestClient(app).get(f'/api/v1/first-step/tasks/{task}?filter=unknown').json()
    assert filtered['list_total'] == 1 and filtered['items'][0]['status'] == 'unknown'
    assert filtered['items'][0]['reason'] == '正文请求结果未知'


def test_unknown_group_does_not_block_next_500_group_batch(tmp_path):
    ctl, scheduler, task = ready_control(tmp_path, groups=2)
    generation = scheduler.calls[-1][1]
    seq, _ = ctl.claim_next(task, generation)
    ctl.mark_group_unknown(task, seq, generation, 'synthetic unknown')
    with ctl.summary._connect() as db, db:
        db.execute("UPDATE control_groups SET state='done' WHERE task_id=? AND seq=1", (task,))
        db.execute("INSERT INTO control_groups (task_id,seq,group_root,state) VALUES (?,?,?,?)",
                   (task, 500, 'synthetic-next-batch', 'pending'))
    assert ctl.claim_next(task, generation)[0] == 500


def test_pause_blocks_new_group_and_model_admission_then_reuses_known_answer(tmp_path):
    ctl, scheduler, task = ready_control(tmp_path)
    claims = [ctl.claim_next(task) for _ in range(4)]
    assert all(claim is not None for claim in claims)
    entered, release = threading.Event(), threading.Event()
    sent, outcomes = [], []

    def send():
        sent.append('sent')
        entered.set()
        return 'handle'

    def receive(handle):
        assert release.wait(5)
        return {'choice': 'known'}

    def work(seq):
        try:
            ctl.invoke_model(task, seq, 'body', 'snapshot', 'payload', send, receive, timeout=5)
            ctl.invoke_model(task, seq, 'other:1', 'snapshot', 'payload-2', send, receive, timeout=5)
        except PauseRequested:
            outcomes.append('paused')
        finally:
            ctl.defer_group(task, seq)

    threads = [threading.Thread(target=work, args=(seq,)) for seq, _ in claims]
    threads[0].start()
    assert entered.wait(5)
    for t in threads[1:]: t.start()
    paused = ctl.pause(task)
    assert paused['task_status'] == 'pausing' and paused['inflight_requests'] == 1
    assert ctl.claim_next(task) is None
    time.sleep(0.15)
    assert len(sent) == 1
    release.set()
    for t in threads: t.join(timeout=5)
    assert all(not t.is_alive() for t in threads) and len(sent) == 1
    current = ctl.summary.read(task, None, 1, 50)
    assert current['task_status'] == 'paused' and len(outcomes) == 4

    restarted = FirstStepControl(tmp_path / 'output', tmp_path / 'data', scheduler)
    assert restarted.summary.read(task, None, 1, 50)['task_status'] == 'paused'
    revision = restarted.summary.read(task, None, 1, 50)['control_revision']
    restarted.start(task, revision)
    seq, _ = restarted.claim_next(task)
    reused = restarted.invoke_model(task, seq, 'body', 'snapshot', 'payload',
                                    lambda: sent.append('unexpected'), lambda _: {}, timeout=5)
    assert reused == {'choice': 'known'} and len(sent) == 1


def test_unknown_model_result_survives_restart_and_is_not_resent(tmp_path):
    ctl, scheduler, task = ready_control(tmp_path, groups=1)
    seq, _ = ctl.claim_next(task)
    sent = []

    def begin():
        sent.append('sent')
        return 'handle'

    def timeout(_):
        raise TimeoutError('outcome unknown')

    with pytest.raises(UnknownRequest):
        ctl.invoke_model(task, seq, 'body', 'snapshot', 'payload', begin, timeout, timeout=1)
    ctl.mark_group_unknown(task, seq, scheduler.calls[-1][1], '模型结果未知')
    restarted = FirstStepControl(tmp_path / 'output', tmp_path / 'data', scheduler)
    state = restarted.summary.read(task, None, 1, 50)
    assert state['task_status'] == 'needs_review' and state['unknown_groups'] == 1
    with pytest.raises(ControlError):
        restarted.start(task, state['control_revision'])
    assert len(sent) == 1 and len(scheduler.calls) == 1


def test_dead_worker_without_request_can_pause_and_resume(tmp_path):
    ctl, scheduler, task = ready_control(tmp_path, groups=1)
    seq, _ = ctl.claim_next(task)
    with ctl.summary._connect() as db:
        db.execute('UPDATE control_groups SET worker_pid=?,worker_started=? WHERE task_id=? AND seq=?',
                   (99999999, 1.0, task, seq))
    assert ctl.pause(task)['task_status'] == 'paused'
    state = ctl.summary.read(task, None, 1, 50)
    ctl.start(task, state['control_revision'])
    assert ctl.claim_next(task)[0] == seq


def test_dead_worker_with_inflight_request_is_unknown(tmp_path):
    ctl, _, task = ready_control(tmp_path, groups=1)
    seq, _ = ctl.claim_next(task)
    with ctl.summary._connect() as db:
        db.execute('UPDATE control_groups SET worker_pid=?,worker_started=? WHERE task_id=? AND seq=?',
                   (99999999, 1.0, task, seq))
        db.execute("INSERT INTO model_decisions (task_id,seq,step,snapshot_id,payload_hash,state) VALUES (?,?,?,?,?,'inflight')",
                   (task, seq, 'body', 'snapshot', 'payload'))
    result = ctl.pause(task)
    assert result['task_status'] == 'paused' and result['unknown_requests'] == 1
    assert ctl.summary.read(task, None, 1, 50)['unknown_groups'] == 1
    with pytest.raises(ControlError):
        ctl.start(task, result['control_revision'])


def test_dead_start_request_is_unknown_instead_of_permanently_pausing(tmp_path):
    ctl, _, task = ready_control(tmp_path, groups=1)
    with ctl.summary._connect() as db:
        db.execute("UPDATE tasks SET task_status='starting',start_pending=1,start_owner_pid=?,start_owner_started=? WHERE task_id=?",
                   (99999999, 1.0, task))
    assert ctl.pause(task)['task_status'] == 'unknown'
