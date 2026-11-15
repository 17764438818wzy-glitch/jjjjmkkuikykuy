import json
import base64
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading
import time

from test_control_admission import ready_control
from paperflow_prefect.controlled_first_step import run_first_step
from paperflow_prefect.controlled_nimble import ControlledNimbleJudge
from paperflow_prefect.first_step_control import PauseRequested, RequestNotSent
from paperflow_prefect.first_step_control import UnknownRequest
from paperflow_prefect.nimble_group import _nimble_request_gate
import pytest


def test_pausing_interrupts_wait_for_the_outer_model_gate(tmp_path):
    control, scheduler, task = ready_control(tmp_path, groups=1)
    judge = ControlledNimbleJudge(control, task, 0, 'snapshot', scheduler.calls[-1][1], timeout=1.0)
    judge._step = 'body'
    outcome = []
    entered = threading.Event()

    def wait_for_model():
        entered.set()
        try:
            judge._decide({}, 'choose', {'yes': 'yes'})
        except Exception as exc:
            outcome.append(exc)

    assert _nimble_request_gate.acquire(timeout=1)
    try:
        thread = threading.Thread(target=wait_for_model)
        thread.start()
        assert entered.wait(1)
        time.sleep(.1)
        control.pause(task)
        thread.join(.5)
        assert not thread.is_alive(), '暂停后排队中的模型请求没有及时停止'
        assert len(outcome) == 1 and isinstance(outcome[0], PauseRequested)
    finally:
        _nimble_request_gate.release()
        thread.join(2)


def test_saved_decision_is_reused_without_waiting_for_model_gate(tmp_path):
    control, scheduler, task = ready_control(tmp_path, groups=1)
    generation = scheduler.calls[-1][1]
    seq, _ = control.claim_next(task, generation)
    judge = ControlledNimbleJudge(control, task, seq, 'snapshot', generation, timeout=.5)
    judge._step = 'filename-title'
    state, instruction, criteria = {'file': 'synthetic'}, 'choose', {'yes': 'yes'}
    payload = {'model': judge.model, 'state': state,
               'questions': {'decision': {'type': 'choice', 'instructions': instruction, 'criteria': criteria}},
               'keep_alive': '5m'}
    digest = hashlib.sha256(json.dumps(payload, ensure_ascii=False).encode('utf8')).hexdigest()
    answer = {'answers': {'decision': {'choice': 'yes', 'probabilities': {'yes': 1}}}}
    response = {'status': 200, 'body': base64.b64encode(json.dumps(answer).encode()).decode()}
    control.invoke_model(task, seq, judge._step, 'snapshot', digest, lambda: None,
                         lambda _: response, timeout=1, generation=generation)
    result = []
    assert _nimble_request_gate.acquire(timeout=1)
    try:
        thread = threading.Thread(target=lambda: result.append(judge._decide(state, instruction, criteria)))
        thread.start()
        thread.join(.25)
        assert not thread.is_alive(), '缓存结果不应等待模型排队锁'
        assert result[0]['choice'] == 'yes'
    finally:
        _nimble_request_gate.release()
        thread.join(1)


def test_queued_group_gets_full_model_response_timeout(tmp_path, monkeypatch):
    control, scheduler, task = ready_control(tmp_path, groups=1)
    seq, _ = control.claim_next(task)
    judge = ControlledNimbleJudge(control, task, seq, 'snapshot', scheduler.calls[-1][1], timeout=.1)
    judge._step = 'body'
    received = []
    answer = {'answers': {'decision': {'choice': 'yes', 'probabilities': {'yes': 1}}}}
    monkeypatch.setattr(judge, '_transmit', lambda request, remaining: (received.append(remaining), json.dumps(answer).encode())[1])
    outcome = []
    def work():
        try: outcome.append(judge._decide({}, 'choose', {'yes': 'yes'}))
        except Exception as exc: outcome.append(exc)
    assert _nimble_request_gate.acquire(timeout=1)
    thread = threading.Thread(target=work)
    try:
        thread.start()
        time.sleep(.25)
        assert thread.is_alive(), '排队时间不应耗尽模型响应时间'
    finally:
        _nimble_request_gate.release()
    thread.join(2)
    assert outcome[0]['choice'] == 'yes' and received[0] >= .09


def test_connection_refused_is_not_recorded_as_unknown(tmp_path, monkeypatch):
    import paperflow_prefect.controlled_nimble as controlled
    class RefusedConnection:
        def __init__(self, *args, **kwargs): pass
        def connect(self): raise ConnectionRefusedError('synthetic')
        def close(self): pass
    monkeypatch.setattr(controlled.http.client, 'HTTPConnection', RefusedConnection)
    control, scheduler, task = ready_control(tmp_path, groups=1)
    seq, _ = control.claim_next(task)
    judge = ControlledNimbleJudge(control, task, seq, 'snapshot', scheduler.calls[-1][1], timeout=1)
    judge._step = 'body'
    with pytest.raises(RequestNotSent):
        judge._decide({}, 'choose', {'yes': 'yes'})
    with control.summary._connect() as db:
        assert db.execute('SELECT COUNT(*) FROM model_decisions WHERE task_id=?', (task,)).fetchone()[0] == 0


def test_unknown_body_request_is_bound_to_the_actual_body_files_and_group(tmp_path, monkeypatch):
    import paperflow_prefect.controlled_nimble as controlled
    sends = []
    class FakeConnection:
        sock = None
        def __init__(self, *args, **kwargs): pass
        def connect(self): pass
        def request(self, method, path, body, headers):
            sends.append(json.loads(body))
        def getresponse(self):
            raise TimeoutError('synthetic unknown result')
        def close(self): pass
    monkeypatch.setattr(controlled.http.client, 'HTTPConnection', FakeConnection)
    digest = hashlib.sha256(b'paper').hexdigest()
    record = {'file_id': 'first-id', 'relative': 'paper.txt', 'bytes': 5, 'sha256': digest,
              'evidence': {'format': 'text', 'text': 'synthetic paper'}}
    first, scheduler, task1 = ready_control(tmp_path, groups=1)
    seq1, _ = first.claim_next(task1)
    judge1 = ControlledNimbleJudge(first, task1, seq1, 'snapshot-one', scheduler.calls[-1][1],
                                  group_scope='class/person', timeout=1)
    with pytest.raises(UnknownRequest):
        judge1.choose_body([record])
    second, scheduler2, task2 = ready_control(tmp_path, groups=1)
    seq2, _ = second.claim_next(task2)
    judge2 = ControlledNimbleJudge(second, task2, seq2, 'different-snapshot', scheduler2.calls[-1][1],
                                  group_scope='class/person', timeout=1)
    with pytest.raises(UnknownRequest):
        judge2.choose_body([dict(record, file_id='different-upload-id')])
    assert len(sends) == 1, '相同资料的未确认请求不应再次发送'

    # An added Word file must not hide the earlier unknown request.
    extra = dict(record, file_id='extra', relative='new-attachment.docx', sha256=hashlib.sha256(b'extra').hexdigest())
    third, scheduler3, task3 = ready_control(tmp_path, groups=1)
    seq3, _ = third.claim_next(task3)
    judge3 = ControlledNimbleJudge(third, task3, seq3, 'third-snapshot', scheduler3.calls[-1][1],
                                  group_scope='class/person', timeout=1)
    with pytest.raises(UnknownRequest):
        judge3.choose_body([dict(record, file_id='third-id'), extra])
    assert len(sends) == 1
    with third.summary._connect() as db:
        assert db.execute('SELECT COUNT(*) FROM model_decisions WHERE task_id=?', (task3,)).fetchone()[0] == 0

    # Identical files in a different group are independent.
    fourth, scheduler4, task4 = ready_control(tmp_path, groups=1)
    seq4, _ = fourth.claim_next(task4)
    judge4 = ControlledNimbleJudge(fourth, task4, seq4, 'fourth-snapshot', scheduler4.calls[-1][1],
                                  group_scope='other/person', timeout=1)
    with pytest.raises(UnknownRequest):
        judge4.choose_body([dict(record, file_id='fourth-id')])
    assert len(sends) == 2


def test_filename_title_unknown_uses_filename_without_requiring_a_body_hash(tmp_path, monkeypatch):
    import paperflow_prefect.controlled_nimble as controlled
    sends = []
    class FakeConnection:
        sock = None
        def __init__(self, *args, **kwargs): pass
        def connect(self): pass
        def request(self, method, path, body, headers): sends.append(True)
        def getresponse(self): raise TimeoutError('synthetic lost title')
        def close(self): pass
    monkeypatch.setattr(controlled.http.client, 'HTTPConnection', FakeConnection)
    for _ in range(2):
        control, scheduler, task = ready_control(tmp_path, groups=1)
        seq, _ = control.claim_next(task)
        judge = ControlledNimbleJudge(control, task, seq, 'snapshot', scheduler.calls[-1][1],
                                      group_scope='root/person', timeout=1)
        with pytest.raises(UnknownRequest):
            judge.choose_filename_title('20210001_张三_校园体育活动参与情况研究.docx')
    assert len(sends) == 1


def test_unknown_attachment_list_displays_confirmed_body_hash(tmp_path):
    from pathlib import Path
    import socket
    control, scheduler, task = ready_control(tmp_path, groups=1)
    with control.summary._connect() as db:
        group = Path(db.execute('SELECT group_root FROM control_groups WHERE task_id=?', (task,)).fetchone()[0])
    (group / 'data.csv').write_bytes(b'a,b\n1,2\n')
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args): pass
        def do_POST(self):
            payload = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            criteria = payload['questions']['decision']['criteria']
            if 'attachment' in criteria:
                self.connection.shutdown(socket.SHUT_RDWR)
                self.connection.close()
                return
            selected = next(name for name in criteria if name != 'none')
            raw = json.dumps({'answers': {'decision': {'choice': selected,
                'probabilities': {name: 1 if name == selected else 0 for name in criteria}}}}).encode()
            self.send_response(200)
            self.send_header('Content-Length', str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)
    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    try:
        run_first_step(control, task, scheduler.calls[-1][1], 'synthetic-run',
                       endpoint=f'http://127.0.0.1:{server.server_port}/v1/systemone')
        state = control.summary.read(task, 'unknown', 1, 50)
        assert state['unknown_groups'] == 1
        assert state['items'][0]['display_hash'] == hashlib.sha256(b'paper').hexdigest()
        assert state['items'][0]['hash_type'] == 'file_sha256'
    finally:
        server.shutdown()
        server.server_close()
        server_thread.join(5)


def test_real_classification_pauses_model_admission_and_resumes(tmp_path):
    control, scheduler, task = ready_control(tmp_path, groups=4)
    entered, release, requests = threading.Event(), threading.Event(), []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args): pass
        def do_POST(self):
            payload = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            requests.append(payload)
            if len(requests) == 1:
                entered.set()
                assert release.wait(5)
            criteria = payload['questions']['decision']['criteria']
            choice = next((key for key in criteria if key != 'none'), 'none')
            raw = json.dumps({'answers': {'decision': {'choice': choice, 'confidence': 1,
                'probabilities': {key: 1 if key == choice else 0 for key in criteria}}}}).encode()
            self.send_response(200)
            self.send_header('Content-Length', str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    try:
        endpoint = f'http://127.0.0.1:{server.server_port}/v1/systemone'
        run = threading.Thread(target=run_first_step,
            args=(control, task, scheduler.calls[-1][1], 'synthetic-run'), kwargs={'endpoint': endpoint})
        run.start()
        assert entered.wait(5)
        paused = control.pause(task)
        assert paused['task_status'] == 'pausing' and paused['inflight_requests'] == 1
        time.sleep(.15)
        assert len(requests) == 1
        release.set()
        run.join(10)
        assert not run.is_alive()
        state = control.summary.read(task, None, 1, 50)
        assert state['task_status'] == 'paused' and len(requests) == 1
        control.start(task, state['control_revision'])
        run_first_step(control, task, scheduler.calls[-1][1], 'resume-run', endpoint=endpoint)
        state = control.summary.read(task, None, 1, 50)
        assert state['task_status'] == 'complete' and state['processed_groups'] == 4
        assert len(requests) == 4
    finally:
        release.set()
        server.shutdown()
        server.server_close()
        server_thread.join(5)
