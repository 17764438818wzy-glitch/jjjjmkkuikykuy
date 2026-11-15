"""One synthetic H2.1 folder through the actual bridge and old business scripts."""
import copy
import hashlib
import json
from pathlib import Path
import shutil
import sqlite3
import sys
from contextlib import nullcontext
from uuid import uuid4

import pytest

CANDIDATE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(CANDIDATE))
from test_body_adapter import fixture as body_fixture, save_upstream_plan_layout, FixtureCounter, RELEASE
from test_attachment_adapter import fixture as attachment_fixture
from test_handoff_inputs import compact
from folder_completion import expected_components, inspect_folder
from paperflow_prefect.stage3_handoff import read_receipt
from preextract_scheduler_adapter import request_observation
from body_adapter import frozen_plan_evidence
from folder_state import FolderStageStore
from mainline_bridge import process_mainline_folder


def file_ref(path):
    return {'path': str(path.resolve()), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding='utf8')
    return file_ref(path)


def replace_values(value, old_path, new_path, old_sha, new_sha):
    if isinstance(value, dict):
        return {key: replace_values(item, old_path, new_path, old_sha, new_sha) for key, item in value.items()}
    if isinstance(value, list):
        return [replace_values(item, old_path, new_path, old_sha, new_sha) for item in value]
    if isinstance(value, str):
        if value == old_path: return new_path
        if value == old_sha: return new_sha
        if value.startswith('{'):
            try: parsed = json.loads(value)
            except ValueError: return value
            return json.dumps(replace_values(parsed, old_path, new_path, old_sha, new_sha), ensure_ascii=False)
    return value


def synthetic_folder(root, body_parts=1):
    ticket, attachment, _, _ = attachment_fixture(root / 'attachment')
    receipt, paper = read_receipt(attachment['receipt_ref']['path'])
    identity = dict(ticket, task_id=str(uuid4()), parent_run_id=str(uuid4()), task_run_id=str(uuid4()))
    body, catalog = body_fixture(root / 'body', parts=body_parts)
    old_path, old_sha = body['source']['path'], body['source']['sha256']
    new_path, new_sha = paper['body']['path'], paper['body']['sha256']
    rewrite = lambda value: replace_values(value, old_path, new_path, old_sha, new_sha)
    for key in ('material', 'reading_material'):
        path = Path(body[key]['path'])
        body[key] = save(path, rewrite(json.loads(path.read_text(encoding='utf8'))))
    plan_path = Path(body['plan']['path'])
    plan = rewrite(json.loads(plan_path.read_text(encoding='utf8')))
    body['plan'] = save(plan_path, plan)
    mode = 'single' if body_parts == 1 else 'partitioned'
    plan_identity = dict(schema='paperflow-body-input-plan/1', source_sha256=new_sha,
        reading_sha256=body['reading_material']['sha256'], mode=mode, part_count=body_parts,
        parts=[dict(id=part['id'], request_sha256=compact(part['body']),
                    material_sha256=compact(part['bundle'], sort=True),
                    scope_sha256=compact(dict(ranges=part['ranges'], asset_ids=part['asset_ids']), sort=True))
               for part in plan['parts']],
        coverage_sha256=compact(plan['coverage'], sort=True), plan_sha256=compact(plan, sort=True))
    body['plan_identity'] = plan_identity
    body['source'] = {'path': new_path, 'sha256': new_sha}
    body['receipt'] = attachment['receipt_ref']
    body['identity'] = identity
    work = save_upstream_plan_layout(root / 'body', body)
    for part, response in zip(plan['parts'], body['responses']):
        old_model = Path(response['request']['path']).parent
        candidate = rewrite(json.loads((old_model / 'response.txt').read_text(encoding='utf8')))
        raw = json.dumps(candidate, ensure_ascii=False)
        model = work / 'model' if mode == 'single' else work / 'model' / part['id']
        model.mkdir(parents=True, exist_ok=True)
        save(model / 'request.json', part['body'])
        (model / 'response.txt').write_text(raw, encoding='utf8')
        save(model / 'response-events.jsonl', {'choices': [{'message': {'content': raw}, 'finish_reason': 'stop'}]})
        save(model / 'request-state.json', dict(status='response_complete', finish_reason='stop',
            request_sha256=compact(part['body']), response_sha256=file_ref(model / 'response.txt')['sha256']))
    attachment_model = Path(attachment['work_dir']) / 'model'
    attachment_raw = (attachment_model / 'response.txt').read_text(encoding='utf8')
    save(attachment_model / 'response-events.jsonl', {'choices': [{'message': {'content': attachment_raw}, 'finish_reason': 'stop'}]})
    manifest = dict(schema='paperflow-local-preprocessing-handoff/1',
        **{k: identity[k] for k in ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id')},
        body=paper['body'], attachments=paper['attachments'], receipt=attachment['receipt_ref'],
        material=body['material'], reading_material=body['reading_material'], body_work=str(work),
        capacity={'status': 'planned', 'plan_identity': plan_identity, 'parts': body_parts, 'mode': mode})
    admissions = []
    for component in expected_components(manifest, identity['task_run_id']):
        selected = (work / 'model' if mode == 'single' else work / 'model' / component['part_id']) \
            if component['kind'] == 'body' else attachment_model
        state = json.loads((selected / 'request-state.json').read_text(encoding='utf8'))
        admissions.append(dict(task_id=identity['task_id'], task_run_id=identity['task_run_id'],
            receipt_id=identity['receipt_id'], component_id=component['component_id'], kind=component['kind'],
            source_sha256=component['source_sha256'], state='raw_complete', request_sha256=state['request_sha256'],
            response_sha256=state['response_sha256'],
            work_dir=str(work if component['kind'] == 'body' else selected.parent), model_dir=str(selected)))
    return identity, manifest, paper, admissions, catalog, attachment['work_dir']


@pytest.mark.parametrize('pause_before_completion', [False, True])
@pytest.mark.parametrize('body_parts', [1, 2])
def test_real_business_scripts_produce_one_bound_folder_receipt(tmp_path, monkeypatch, pause_before_completion, body_parts):
    import socket
    monkeypatch.setattr(socket.socket, 'connect', lambda *a, **k: pytest.fail('network forbidden'))
    identity, manifest, paper, admissions, catalog, attachment_work = synthetic_folder(tmp_path, body_parts)
    database = tmp_path / 'control.sqlite3'
    digest = compact(manifest, sort=True)
    with sqlite3.connect(database) as db:
        db.execute('CREATE TABLE tasks(task_id TEXT PRIMARY KEY,desired_state TEXT,start_generation INTEGER,'
                   'start_pending INTEGER,task_status TEXT,prefect_run_id TEXT,prefect_order_model TEXT)')
        db.execute('CREATE TABLE task_deletions(task_id TEXT PRIMARY KEY)')
        db.execute('INSERT INTO tasks VALUES(?,?,?,?,?,?,?)',
                   (identity['task_id'], 'running', 2, 0, 'running', identity['parent_run_id'], 'v2'))
        db.execute('CREATE TABLE llm_folder_queue(task_id TEXT,group_id TEXT,paper_id TEXT,receipt_id TEXT,'
                   'attempt_id TEXT,parent_run_id TEXT,task_run_id TEXT,manifest_sha256 TEXT,state TEXT)')
        db.execute('INSERT INTO llm_folder_queue VALUES(?,?,?,?,?,?,?,?,?)', tuple(identity.values()) +
                   (digest, 'waiting_postprocessing'))
        columns = list(admissions[0])
        db.execute('CREATE TABLE llm_request_admissions(' + ','.join(k + ' TEXT' for k in columns) + ')')
        for row in admissions:
            db.execute('INSERT INTO llm_request_admissions VALUES(' + ','.join('?' for _ in columns) + ')',
                       tuple(row[k] for k in columns))
    store = FolderStageStore(database, observe_run=lambda ident: ident == identity,
                             admission_gate=lambda task: nullcontext())
    store.initialize()
    class Binding:
        def load(self, given, *, require_running):
            assert given == identity and isinstance(require_running, bool)
            return {'manifest': manifest, 'paper': paper}
    class Backend:
        def attachment_work(self, p, r): return Path(attachment_work)
    with pytest.raises(ValueError):
        process_mainline_folder(identity=identity, generation=2, control_database=database,
            binding=Binding(), backend=Backend(), store=store, catalog=catalog, paperflow_root=RELEASE / 'core',
            counter=FixtureCounter(), output_root=Path(attachment_work) / 'must-not-create',
            expected_components=expected_components, inspect_folder=inspect_folder,
            observe_request=request_observation, frozen_plan=frozen_plan_evidence)
    assert not (Path(attachment_work) / 'must-not-create').exists()
    with sqlite3.connect(database) as db:
        assert db.execute('SELECT COUNT(*) FROM postprocessing_claims').fetchone()[0] == 0
    if pause_before_completion:
        complete = store.complete
        def pause_and_complete(*args, **kwargs):
            with sqlite3.connect(database) as db:
                db.execute("UPDATE tasks SET desired_state='paused',task_status='paused'")
            return complete(*args, **kwargs)
        monkeypatch.setattr(store, 'complete', pause_and_complete)
    accepted = []
    def check_accepted(stage, validate_receipt):
        with sqlite3.connect(database) as db:
            saved = json.loads(db.execute('SELECT receipt_json FROM postprocessing_receipts').fetchone()[0])
        assert validate_receipt(copy.deepcopy(saved)) is True
        altered = copy.deepcopy(saved)
        altered['body']['manifest_sha256'] = '0' * 64
        assert validate_receipt(altered) is False
        accepted.append(stage['claim_id'])
    result = process_mainline_folder(identity=identity, generation=2, control_database=database,
        binding=Binding(), backend=Backend(), store=store, catalog=catalog, paperflow_root=RELEASE / 'core',
        counter=FixtureCounter(), output_root=tmp_path / 'output', expected_components=expected_components,
        inspect_folder=inspect_folder, observe_request=request_observation, frozen_plan=frozen_plan_evidence,
        accepted_hook=check_accepted)
    assert accepted == [result['claim_id']]
    if pause_before_completion:
        assert result['state'] == 'receipt_held'
        with sqlite3.connect(database) as db:
            db.execute("UPDATE tasks SET desired_state='running',task_status='running',start_generation=3")
        result = process_mainline_folder(identity=identity, generation=3, control_database=database,
            binding=Binding(), backend=Backend(), store=store, catalog=catalog, paperflow_root=RELEASE / 'core',
            counter=FixtureCounter(), output_root=tmp_path / 'output', expected_components=expected_components,
            inspect_folder=inspect_folder, observe_request=request_observation, frozen_plan=frozen_plan_evidence,
            reconcile_claim_id=result['claim_id'])
    assert result['state'] == 'waiting_rag'
    with sqlite3.connect(database) as db:
        saved = json.loads(db.execute('SELECT receipt_json FROM postprocessing_receipts').fetchone()[0])
        assert db.execute('SELECT state FROM llm_folder_queue').fetchone()[0] == 'waiting_postprocessing'
    assert saved['identity'] == identity
    assert len(saved['attachments']) == 1
    assert Path(saved['body']['package_path'], 'article.redacted.md').is_file()
    assert Path(saved['attachments'][0]['md_path']).is_file()
