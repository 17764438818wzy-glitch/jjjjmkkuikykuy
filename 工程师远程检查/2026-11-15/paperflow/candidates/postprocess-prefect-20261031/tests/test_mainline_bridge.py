import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from test_handoff_inputs import fixture, compact
import mainline_bridge


def setup(tmp_path):
    _, identity, verified, admissions = fixture(tmp_path)
    db_path = tmp_path / 'control.db'
    digest = compact(verified['manifest'], sort=True)
    with sqlite3.connect(db_path) as db:
        db.execute('CREATE TABLE llm_folder_queue(task_id TEXT,group_id TEXT,paper_id TEXT,receipt_id TEXT,'
                   'attempt_id TEXT,parent_run_id TEXT,task_run_id TEXT,manifest_sha256 TEXT,state TEXT)')
        db.execute('INSERT INTO llm_folder_queue VALUES(?,?,?,?,?,?,?,?,?)', tuple(identity.values()) +
                   (digest, 'waiting_postprocessing'))
        columns = list(admissions[0])
        db.execute('CREATE TABLE llm_request_admissions(' + ','.join(name + ' TEXT' for name in columns) + ')')
        for row in admissions:
            db.execute('INSERT INTO llm_request_admissions VALUES(' + ','.join('?' for _ in columns) + ')',
                       tuple(row[name] for name in columns))
    class Binding:
        def load(self, supplied, *, require_running):
            assert supplied == identity and require_running is True
            return verified
    class Store:
        database = db_path
        admission_gate = lambda self, value: None
        observe_run = lambda self, value: True
    class Backend:
        def attachment_work(self, paper, row): return tmp_path / 'attachment-work'
    def observation(model):
        import json
        state = json.loads((Path(model) / 'request-state.json').read_bytes())
        return 'response_complete', 'postprocess', {'request_sha256': state['request_sha256'],
                                                     'response_sha256': state['response_sha256']}
    from folder_completion import expected_components, inspect_folder
    return dict(identity=identity, generation=2, control_database=db_path, binding=Binding(),
        backend=Backend(), store=Store(), catalog={}, paperflow_root=tmp_path, counter=object(),
        output_root=tmp_path / 'out', expected_components=expected_components, inspect_folder=inspect_folder,
        observe_request=observation, frozen_plan=lambda work, receipt: {'actual_work': str(work)})


def test_mainline_reads_canonical_ledger_and_binds_all_saved_components(tmp_path, monkeypatch):
    args = setup(tmp_path)
    def run_folder(**options):
        inputs = options['load_inputs']()
        assert inputs['identity'] == args['identity']
        assert len(inputs['body']['responses']) == len(inputs['attachments']) == 1
        assert inputs['body']['plan']['actual_work'] == args['binding'].load(args['identity'], require_running=True)['manifest']['body_work']
        return {'state': 'synthetic'}
    monkeypatch.setattr(mainline_bridge, 'process_folder', run_folder)
    assert mainline_bridge.process_mainline_folder(**args) == {'state': 'synthetic'}


def test_missing_stage_row_never_reaches_business_code(tmp_path, monkeypatch):
    args = setup(tmp_path)
    with sqlite3.connect(args['control_database']) as db:
        db.execute('DELETE FROM llm_folder_queue')
    monkeypatch.setattr(mainline_bridge, 'process_folder', lambda **kw: kw['load_inputs']())
    with pytest.raises(ValueError): mainline_bridge.process_mainline_folder(**args)


def test_control_gate_is_required_even_for_synthetic_runtime(tmp_path):
    args = setup(tmp_path)
    args['store'].admission_gate = None
    with pytest.raises(ValueError): mainline_bridge.process_mainline_folder(**args)


def test_reconcile_entry_uses_existing_claim_without_new_folder_processing(tmp_path, monkeypatch):
    args = setup(tmp_path)
    def reconcile(**options):
        assert options['claim_id'] == 'existing-claim'
        assert options['load_inputs']()['identity'] == args['identity']
        return {'state': 'waiting_rag'}
    monkeypatch.setattr(mainline_bridge, 'reconcile_folder', reconcile)
    monkeypatch.setattr(mainline_bridge, 'process_folder', lambda **kw: pytest.fail('must not claim again'))
    assert mainline_bridge.process_mainline_folder(**args, reconcile_claim_id='existing-claim')['state'] == 'waiting_rag'


def test_completion_can_recheck_a_suspended_folder_without_readmitting_it(tmp_path, monkeypatch):
    args = setup(tmp_path)
    verified = args['binding'].load(args['identity'], require_running=True)
    calls = []
    class Binding:
        def load(self, supplied, *, require_running):
            calls.append(require_running)
            if len(calls) > 1 and require_running:
                raise InterruptedError('synthetic folder is now suspended')
            return verified
    args['binding'] = Binding()
    def run_folder(**options):
        options['load_inputs']()
        assert options['load_inputs']()['identity'] == args['identity']
        return {'state': 'receipt_held'}
    monkeypatch.setattr(mainline_bridge, 'process_folder', run_folder)
    assert mainline_bridge.process_mainline_folder(**args)['state'] == 'receipt_held'
    assert calls == [True, False]
