from test_control_admission import ready_control

from paperflow_prefect.controlled_first_step import run_controlled
from paperflow_prefect.first_step_control import RequestNotSent, UnknownRequest
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import pytest


def test_unreachable_model_pauses_batch_and_keeps_all_groups_pending(tmp_path):
    control, scheduler, task_id = ready_control(tmp_path, groups=12)
    generation = scheduler.calls[-1][1]
    attempts = []

    def process(seq, path):
        def connect():
            attempts.append(seq)
            raise RequestNotSent('无法连接本机 Nimble，模型请求未发送')

        control.invoke_model(task_id, seq, 'body', f'snapshot-{seq}', f'payload-{seq}',
                             connect, lambda _: {}, timeout=1, generation=generation)
        raise AssertionError('unreachable model cannot return a result')

    run_controlled(control, task_id, generation, 'synthetic-run', process)

    with control.summary._connect() as db:
        task = db.execute('SELECT task_status,desired_state,stop_reason FROM tasks WHERE task_id=?',
                          (task_id,)).fetchone()
        groups = dict(db.execute('SELECT state,COUNT(*) FROM control_groups WHERE task_id=? GROUP BY state',
                                 (task_id,)).fetchall())
        outcomes = db.execute('SELECT COUNT(*) FROM group_results WHERE task_id=?', (task_id,)).fetchone()[0]
        decisions = db.execute('SELECT COUNT(*) FROM model_decisions WHERE task_id=?', (task_id,)).fetchone()[0]

    assert task['task_status'] == 'paused'
    assert task['desired_state'] == 'paused'
    assert 'Nimble' in task['stop_reason']
    assert groups == {'pending': 12}
    assert outcomes == 0
    assert decisions == 0
    assert len(attempts) == 1
    with control.summary._connect() as db:
        owner = db.execute('SELECT worker_run_id FROM tasks WHERE task_id=?', (task_id,)).fetchone()[0]
    assert owner is None
    control.refresh_task(task_id)
    with control.summary._connect() as db:
        assert 'Nimble' in db.execute('SELECT stop_reason FROM tasks WHERE task_id=?', (task_id,)).fetchone()[0]


def test_failed_startup_check_pauses_before_any_group_is_claimed(tmp_path):
    control, scheduler, task_id = ready_control(tmp_path, groups=3)
    generation = scheduler.calls[-1][1]
    processed = []

    def unavailable():
        raise RuntimeError('synthetic Nimble startup failure')

    run_controlled(control, task_id, generation, 'synthetic-run',
                   lambda seq, path: processed.append(seq), dependency_check=unavailable)

    with control.summary._connect() as db:
        task = db.execute('SELECT task_status,desired_state,stop_reason FROM tasks WHERE task_id=?',
                          (task_id,)).fetchone()
        states = dict(db.execute('SELECT state,COUNT(*) FROM control_groups WHERE task_id=? GROUP BY state',
                                 (task_id,)).fetchall())
    assert task['task_status'] == 'paused'
    assert task['desired_state'] == 'paused'
    assert 'Nimble' in task['stop_reason']
    assert states == {'pending': 3}
    assert processed == []
    with control.summary._connect() as db:
        assert db.execute('SELECT worker_run_id FROM tasks WHERE task_id=?', (task_id,)).fetchone()[0] is None


def test_failed_startup_check_does_not_override_delete_intent(tmp_path):
    control, scheduler, task_id = ready_control(tmp_path, groups=1)
    generation = scheduler.calls[-1][1]

    def delete_wins():
        with control.summary._connect() as db, db:
            db.execute("UPDATE tasks SET task_status='deleting',desired_state='paused' WHERE task_id=?",
                       (task_id,))
            db.execute('INSERT INTO task_deletions(task_id,requested_at_ns) VALUES (?,1)', (task_id,))
        raise RuntimeError('synthetic startup failure')

    run_controlled(control, task_id, generation, 'synthetic-run', lambda *_: None,
                   dependency_check=delete_wins)
    with control.summary._connect() as db:
        task = db.execute('SELECT task_status,desired_state FROM tasks WHERE task_id=?', (task_id,)).fetchone()
    assert task['task_status'] == 'deleting'
    assert task['desired_state'] == 'paused'


def test_worker_cleanup_does_not_override_delete_intent(tmp_path):
    for method in ('defer', 'unknown'):
        control, scheduler, task_id = ready_control(tmp_path / method, groups=1)
        generation = scheduler.calls[-1][1]
        assert control.begin_run(task_id, generation, 'synthetic-run')
        seq, _ = control.claim_next(task_id, generation)
        with control.summary._connect() as db, db:
            db.execute("UPDATE tasks SET task_status='deleting',desired_state='paused' WHERE task_id=?", (task_id,))
            db.execute('INSERT INTO task_deletions(task_id,requested_at_ns) VALUES (?,1)', (task_id,))
        if method == 'defer':
            control.defer_group(task_id, seq, generation)
        else:
            control.mark_group_unknown(task_id, seq, generation, 'synthetic problem')
        with control.summary._connect() as db:
            task = db.execute('SELECT task_status FROM tasks WHERE task_id=?', (task_id,)).fetchone()
        assert task['task_status'] == 'deleting'


def test_deleted_task_disappearing_during_startup_check_is_a_clean_stop(tmp_path):
    for fail in (False, True):
        control, scheduler, task_id = ready_control(tmp_path / str(fail), groups=1)
        generation = scheduler.calls[-1][1]
        processed = []

        def concurrent_delete():
            with control.summary._connect() as db, db:
                db.execute('DELETE FROM tasks WHERE task_id=?', (task_id,))
            if fail:
                raise RuntimeError('synthetic startup failure after deletion')

        run_controlled(control, task_id, generation, 'synthetic-run',
                       lambda *args: processed.append(args), dependency_check=concurrent_delete)
        assert processed == []


def test_old_worker_error_cannot_override_new_generation_or_delete(tmp_path):
    control, scheduler, task_id = ready_control(tmp_path, groups=1)
    generation = scheduler.calls[-1][1]
    with control.summary._connect() as db, db:
        db.execute("UPDATE tasks SET task_status='starting',start_generation=? WHERE task_id=?",
                   (generation + 1, task_id))
    control.mark_unknown(task_id, 'old error', generation=generation)
    with control.summary._connect() as db:
        assert db.execute('SELECT task_status FROM tasks WHERE task_id=?', (task_id,)).fetchone()[0] == 'starting'
    with control.summary._connect() as db, db:
        db.execute("UPDATE tasks SET task_status='deleting',desired_state='paused' WHERE task_id=?", (task_id,))
        db.execute('INSERT INTO task_deletions(task_id,requested_at_ns) VALUES (?,1)', (task_id,))
    control.mark_unknown(task_id, 'old error', generation=generation + 1)
    with control.summary._connect() as db:
        assert db.execute('SELECT task_status FROM tasks WHERE task_id=?', (task_id,)).fetchone()[0] == 'deleting'


def test_worker_ownership_is_retained_while_group_or_request_is_active(tmp_path):
    control, scheduler, task_id = ready_control(tmp_path, groups=1)
    generation = scheduler.calls[-1][1]
    assert control.begin_run(task_id, generation, 'synthetic-run')
    seq, _ = control.claim_next(task_id, generation)
    assert not control.finish_run(task_id, generation, 'synthetic-run')
    control.defer_group(task_id, seq, generation)
    with control.summary._connect() as db, db:
        db.execute("INSERT INTO model_decisions (task_id,seq,step,snapshot_id,payload_hash,state) "
                   "VALUES (?,?,?,?,?,'inflight')", (task_id, seq, 'body', 'snapshot', 'payload'))
    assert not control.finish_run(task_id, generation, 'synthetic-run')
    with control.summary._connect() as db, db:
        db.execute("UPDATE model_decisions SET state='complete' WHERE task_id=?", (task_id,))
    assert not control.finish_run(task_id, generation, 'other-run')
    assert control.finish_run(task_id, generation, 'synthetic-run')


def test_http_error_stops_batch_preserves_response_and_skips_group_on_resume(tmp_path):
    from paperflow_prefect.controlled_first_step import run_first_step
    control, scheduler, task_id = ready_control(tmp_path, groups=4)
    requests = []
    healthy = [False]

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass
        def do_POST(self):
            payload = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            requests.append(payload)
            if not healthy[0]:
                raw = b'{"error":"synthetic model unavailable"}'
                self.send_response(503)
            else:
                criteria = payload['questions']['decision']['criteria']
                choice = next((key for key in criteria if key != 'none'), 'none')
                raw = json.dumps({'answers': {'decision': {'choice': choice,
                    'probabilities': {key: 1 if key == choice else 0 for key in criteria}}}}).encode()
                self.send_response(200)
            self.send_header('Content-Length', str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        endpoint = f'http://127.0.0.1:{server.server_port}/v1/systemone'
        generation = scheduler.calls[-1][1]
        run_first_step(control, task_id, generation, 'first-run', endpoint=endpoint)
        with control.summary._connect() as db:
            task = db.execute('SELECT task_status,desired_state,stop_reason,control_revision FROM tasks WHERE task_id=?',
                              (task_id,)).fetchone()
            states = dict(db.execute('SELECT state,COUNT(*) FROM control_groups WHERE task_id=? GROUP BY state',
                                     (task_id,)).fetchall())
            decision = db.execute('SELECT state,response FROM model_decisions WHERE task_id=?', (task_id,)).fetchone()
        assert task['task_status'] == 'paused' and task['desired_state'] == 'paused'
        assert 'Nimble' in task['stop_reason']
        assert states == {'pending': 3, 'unknown': 1}
        assert decision['state'] == 'unknown' and json.loads(decision['response'])['status'] == 503
        assert len(requests) == 1

        healthy[0] = True
        control.start(task_id, task['control_revision'])
        run_first_step(control, task_id, scheduler.calls[-1][1], 'resume-run', endpoint=endpoint)
        with control.summary._connect() as db:
            states = dict(db.execute('SELECT state,COUNT(*) FROM control_groups WHERE task_id=? GROUP BY state',
                                     (task_id,)).fetchall())
        assert states == {'done': 3, 'unknown': 1}
        assert len(requests) == 4
    finally:
        server.shutdown()
        server.server_close()
        thread.join(5)


@pytest.mark.parametrize('raw', [b'not json', b'{"answers":{}}', b'x' * (256 * 1024 + 1),
                                b'[' * 1200 + b']' * 1200],
                         ids=['bad-json', 'missing-fields', 'oversized', 'deep-json'])
def test_invalid_success_response_stops_batch_and_keeps_result_for_review(tmp_path, raw, monkeypatch):
    from paperflow_prefect.controlled_first_step import run_first_step
    from paperflow_prefect.nimble_group import NimbleJudge
    original_parse = NimbleJudge._parse_response

    def slow_parse(self, *args):
        time.sleep(.2)
        return original_parse(self, *args)

    monkeypatch.setattr(NimbleJudge, '_parse_response', slow_parse)
    control, scheduler, task_id = ready_control(tmp_path, groups=4)
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass
        def do_POST(self):
            self.rfile.read(int(self.headers['Content-Length']))
            requests.append(True)
            self.send_response(200)
            self.send_header('Content-Length', str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        endpoint = f'http://127.0.0.1:{server.server_port}/v1/systemone'
        run_first_step(control, task_id, scheduler.calls[-1][1], 'synthetic-run', endpoint=endpoint)
        with control.summary._connect() as db:
            task = db.execute('SELECT task_status,desired_state,stop_reason FROM tasks WHERE task_id=?',
                              (task_id,)).fetchone()
            states = dict(db.execute('SELECT state,COUNT(*) FROM control_groups WHERE task_id=? GROUP BY state',
                                     (task_id,)).fetchall())
            decision = db.execute('SELECT state,response FROM model_decisions WHERE task_id=?', (task_id,)).fetchone()
        assert task['task_status'] == 'paused' and task['desired_state'] == 'paused'
        assert 'Nimble' in task['stop_reason']
        assert states == {'pending': 3, 'unknown': 1}
        assert decision['state'] == 'unknown' and json.loads(decision['response'])['status'] == 200
        assert len(requests) == 1
    finally:
        server.shutdown()
        server.server_close()
        thread.join(5)


@pytest.mark.parametrize('failure_phase', ['send', 'receive'])
def test_repeated_uncertain_transport_stops_batch_without_replay(tmp_path, failure_phase):
    control, scheduler, task_id = ready_control(tmp_path, groups=12)
    generation = scheduler.calls[-1][1]
    attempts = []

    def process(seq, path):
        def begin():
            attempts.append(seq)
            if failure_phase == 'send':
                raise ConnectionResetError('synthetic send interruption')
            return object()
        def receive(_):
            raise TimeoutError('synthetic lost response')
        control.invoke_model(task_id, seq, 'body', f'snapshot-{seq}', f'payload-{seq}',
                             begin, receive, timeout=1, generation=generation)

    run_controlled(control, task_id, generation, 'synthetic-run', process)
    with control.summary._connect() as db:
        task = db.execute('SELECT task_status,desired_state,stop_reason FROM tasks WHERE task_id=?',
                          (task_id,)).fetchone()
        states = dict(db.execute('SELECT state,COUNT(*) FROM control_groups WHERE task_id=? GROUP BY state',
                                 (task_id,)).fetchall())
        decisions = db.execute("SELECT COUNT(*) FROM model_decisions WHERE task_id=? AND state='unknown'",
                               (task_id,)).fetchone()[0]
    assert task['task_status'] == 'paused' and task['desired_state'] == 'paused'
    assert 'Nimble' in task['stop_reason']
    assert states == {'pending': 10, 'unknown': 2}
    assert decisions == 2 and len(attempts) == 2


def test_failed_http_result_blocks_resending_in_another_task_and_after_tombstone(tmp_path):
    first, _, task1 = ready_control(tmp_path, groups=1)
    seq1, _ = first.claim_next(task1)
    key = {'scope': 'root/person', 'kind': 'body', 'members': ['same-paper']}
    with pytest.raises(UnknownRequest):
        first.invoke_model(task1, seq1, 'body', 'snapshot', 'payload', lambda: None,
                           lambda _: {'status': 503, 'body': 'e30='}, timeout=1, request_identity=key)

    second, _, task2 = ready_control(tmp_path, groups=1)
    seq2, _ = second.claim_next(task2)
    sent = []
    with pytest.raises(UnknownRequest):
        second.invoke_model(task2, seq2, 'body', 'snapshot', 'payload', lambda: sent.append(True),
                            lambda _: {}, timeout=1, request_identity=key)
    assert not sent

    with first.summary._connect() as db, db:
        db.execute("INSERT INTO deleted_unknown_requests(task_id,seq,step,source_digest,group_scope,"
                   "request_scope,request_kind,request_members) SELECT task_id,seq,step,NULL,NULL,"
                   "request_scope,request_kind,request_members FROM model_decisions "
                   "WHERE task_id=? AND state='unknown'", (task1,))
        db.execute('DELETE FROM model_decisions WHERE task_id=?', (task1,))

    third, _, task3 = ready_control(tmp_path, groups=1)
    seq3, _ = third.claim_next(task3)
    with pytest.raises(UnknownRequest):
        third.invoke_model(task3, seq3, 'body', 'snapshot', 'payload', lambda: sent.append(True),
                           lambda _: {}, timeout=1, request_identity=key)
    assert not sent
