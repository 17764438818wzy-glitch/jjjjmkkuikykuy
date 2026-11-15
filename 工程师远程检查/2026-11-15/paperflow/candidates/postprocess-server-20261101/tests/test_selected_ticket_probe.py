"""No real queue, network, claim or paper: temporary synthetic admission only."""
import copy
import hashlib
import importlib
import json
from pathlib import Path
import sqlite3
import sys
from types import SimpleNamespace
from uuid import uuid4

import pytest

HERE = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(HERE), str(HERE / 'tests')]
from test_snapshot_collect import fixture, bind_catalog, current_fixture


def module():
    try: return importlib.import_module('selected_ticket_probe')
    except ModuleNotFoundError as exc:
        if exc.name == 'selected_ticket_probe': pytest.fail('selected ticket evidence tool is missing')
        raise


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False), encoding='utf8')


@pytest.fixture
def case(tmp_path, monkeypatch):
    m = module()
    workspace = tmp_path / 'workspace'; workspace.mkdir()
    data = tmp_path / 'independent-data'; data.mkdir()
    classification = tmp_path / 'classification'; classification.mkdir()
    journal = tmp_path / 'unrelated-journal'; journal.mkdir()
    library = tmp_path / 'unrelated-library'; library.mkdir()
    inputs = fixture(data / 'one', rich=True, delivered=True)
    catalog = workspace / 'catalog.json'
    bind_catalog(inputs, catalog, {'synthetic': 1})
    runtime = dict(embedding_model_budget=2000, embedding_admission=dict(policy=dict(
        model_sha256='1' * 64, libllama_sha256='2' * 64, cl100k_sha256='3' * 64,
        pipeline_limit=8000, adapter_limit=8192)))
    runtime_file = workspace / 'runtime.json'; save(runtime_file, runtime)
    settings = dict(data_root=str(data), library_root=str(library), catalog_path=str(catalog),
        runtime_config=str(runtime_file), prefect_api_url='http://127.0.0.1:4200/api')
    save(workspace / 'prefect-paperflow/config.json', settings)
    save(workspace / 'candidates/attachment-prefect-live-link-20261031/attachment-queue-config.json',
         dict(queue_db=str(tmp_path / 'queue.sqlite3'), receipt_root=str(data), journal_root=str(journal)))
    save(workspace / 'candidates/first-step-control-20261029/runtime/workspace-config.json',
         dict(classification_root=str(classification), prefect_api_url=settings['prefect_api_url']))
    (tmp_path / 'queue.sqlite3').touch()
    sources = m.load_sources(workspace)
    dbpath = sources.control_database; dbpath.parent.mkdir()
    with sqlite3.connect(dbpath) as db:
        db.executescript('''CREATE TABLE tasks(task_id TEXT PRIMARY KEY,desired_state TEXT,
            start_generation INTEGER,start_pending INTEGER,task_status TEXT,prefect_run_id TEXT,prefect_order_model TEXT);
            CREATE TABLE task_deletions(task_id TEXT);
            CREATE TABLE llm_folder_queue(task_id TEXT,group_id TEXT,paper_id TEXT,receipt_id TEXT,
            attempt_id TEXT,parent_run_id TEXT,task_run_id TEXT,manifest_sha256 TEXT,state TEXT,generation INTEGER);
            CREATE TABLE llm_request_admissions(task_id TEXT,task_run_id TEXT,kind TEXT,state TEXT);
            CREATE TABLE postprocessing_claims(task_id TEXT,group_id TEXT);
            CREATE TABLE postprocessing_results(task_id TEXT,group_id TEXT);
            CREATE TABLE paperflow_postprocess_stage_jobs(task_id TEXT,group_id TEXT);''')
    owned = tmp_path / 'candidate'; owned.mkdir()
    monkeypatch.setattr(m, 'EVIDENCE_ROOT', owned / 'selected-ticket-evidence')
    x = SimpleNamespace(module=m, workspace=workspace, sources=sources, inputs=inputs,
        data=data, classification=classification, journal=journal, library=library,
        catalog=catalog, runtime=runtime, settings=settings, database=dbpath,
        output=m.EVIDENCE_ROOT / 'selected.json', task_id=inputs['identity']['task_id'])
    x.refresh = lambda: refresh(x)
    x.refresh()
    return x


def refresh(x):
    value = x.inputs
    identity = value['identity']
    receipt = json.loads(Path(value['body']['receipt']['path']).read_bytes())
    manifest = dict(**{k: identity[k] for k in ('task_id','group_id','paper_id','receipt_id','attempt_id')},
        body=receipt['inventory']['body'], attachments=receipt['inventory']['attachments'],
        receipt=value['body']['receipt'], material=value['body']['material'],
        reading_material=value['body']['reading_material'], body_work=value['body']['plan']['work'])
    value['manifest_sha256'] = x.module.digest(manifest)
    registered = dict(**{k: identity[k] for k in ('task_id','group_id','paper_id','receipt_id')},
        receipt_path=value['body']['receipt']['path'], receipt_sha256=value['body']['receipt']['sha256'],
        delivery_dir=str(Path(receipt['delivery_ref']['path']).parent), library_root=str(x.library))
    verified = dict(manifest=manifest, registered=registered)
    backend = SimpleNamespace(invoke=None, settings=SimpleNamespace(path=lambda k: Path(x.settings[k])),
                              runtime=x.runtime)
    x.loader = lambda actual, rows: (copy.deepcopy(verified), copy.deepcopy(value), backend)
    with sqlite3.connect(x.database) as db:
        db.execute('DELETE FROM tasks'); db.execute('DELETE FROM llm_folder_queue')
        db.execute('DELETE FROM llm_request_admissions')
        db.execute('INSERT INTO tasks VALUES(?,?,?,?,?,?,?)',
            (identity['task_id'],'running',1,0,'running',identity['parent_run_id'],'v2'))
        db.execute('INSERT INTO llm_folder_queue VALUES(?,?,?,?,?,?,?,?,?,?)',
            tuple(identity.values()) + (value['manifest_sha256'],'waiting_postprocessing',1))
        db.executemany('INSERT INTO llm_request_admissions VALUES(?,?,?,?)',
            [(identity['task_id'],identity['task_run_id'],kind,'raw_complete') for kind in ('body','attachment')])


def run(x):
    return x.module.select_and_record(sources=x.sources, task_id=x.task_id, output=x.output, loader=x.loader)


def test_one_original_identity_is_recorded_without_inputs_or_control_writes(case):
    x = case; before = x.database.read_bytes()
    result = run(x)
    assert x.database.read_bytes() == before
    raw = x.output.read_bytes(); record = json.loads(raw)
    assert record['identity'] == x.inputs['identity']
    assert record['claim_created'] is False and record['server_execution_started'] is False
    assert record['dependency_file_count'] > 0
    assert record['dependency_total_bytes'] >= record['dependency_largest_file_bytes'] > 0
    assert 0 < record['dependency_json_bytes'] <= record['dependency_total_bytes']
    assert 'inputs' not in record and 'files' not in record and 'catalog' not in record
    assert record['configuration']['trusted_files'] == [dict(path=str(x.catalog),
        sha256=hashlib.sha256(x.catalog.read_bytes()).hexdigest())]
    roots = [Path(p) for p in record['configuration']['allowed_roots']]
    assert x.data in roots and x.journal not in roots and x.library not in roots
    assert x.catalog.parent not in roots
    assert result['evidence_sha256'] == hashlib.sha256(raw).hexdigest()
    assert result['identity_printed'] is False
    verified = x.module.verify_record(sources=x.sources, evidence=x.output,
        expected_sha256=result['evidence_sha256'], loader=x.loader)
    assert verified['same_original_identity'] is True
    assert x.output.read_bytes() == raw


def test_explicit_original_folder_can_be_body_only_without_changing_test_selection_default(case):
    x = case
    with sqlite3.connect(x.database) as db:
        db.execute("DELETE FROM llm_request_admissions WHERE kind='attachment'")
    with pytest.raises(ValueError):
        x.module._snapshot(x.sources, x.task_id, x.inputs['identity'])
    selected = x.module._snapshot(x.sources, x.task_id, x.inputs['identity'], require_attachment=False)
    assert selected['identity'] == x.inputs['identity']
    assert [row['kind'] for row in selected['admissions']] == ['body']


def test_exact_folder_inspection_reuses_registered_identity_without_writes(case):
    x = case; before = x.database.read_bytes()
    record = x.module.inspect_exact_folder(sources=x.sources, identity=x.inputs['identity'], loader=x.loader)
    assert record['identity'] == x.inputs['identity']
    assert record['claim_created'] is False and record['server_execution_started'] is False
    assert x.database.read_bytes() == before and not x.output.exists()
    wrong = dict(x.inputs['identity'], group_id='different')
    with pytest.raises(ValueError):
        x.module.inspect_exact_folder(sources=x.sources, identity=wrong, loader=x.loader)


@pytest.mark.parametrize('change', ['paused','pending','deleted','parent','raw','no_attachment','claim','result','stage'])
def test_only_a_running_original_unclaimed_raw_complete_folder_is_selected(case, change):
    x = case
    with sqlite3.connect(x.database) as db:
        if change=='paused': db.execute("UPDATE tasks SET desired_state='paused'")
        elif change=='pending': db.execute('UPDATE tasks SET start_pending=1')
        elif change=='deleted': db.execute('INSERT INTO task_deletions VALUES(?)',(x.task_id,))
        elif change=='parent': db.execute('UPDATE tasks SET prefect_run_id=?',(str(uuid4()),))
        elif change=='raw': db.execute("UPDATE llm_request_admissions SET state='unknown' WHERE kind='attachment'")
        elif change=='no_attachment': db.execute("DELETE FROM llm_request_admissions WHERE kind='attachment'")
        else:
            table={'claim':'postprocessing_claims','result':'postprocessing_results','stage':'paperflow_postprocess_stage_jobs'}[change]
            db.execute('INSERT INTO '+table+' VALUES(?,?)',(x.task_id,x.inputs['identity']['group_id']))
    with pytest.raises(ValueError): run(x)
    assert not x.output.exists()


def test_pause_during_collection_refuses_record_without_reselecting(case, monkeypatch):
    x = case
    collect = x.module.collect_inputs
    def pause(*a, **kw):
        result = collect(*a, **kw)
        with sqlite3.connect(x.database) as db: db.execute("UPDATE tasks SET desired_state='paused'")
        return result
    monkeypatch.setattr(x.module,'collect_inputs',pause)
    with pytest.raises(ValueError): run(x)
    assert not x.output.exists()


def test_config_change_during_check_is_not_written_as_a_valid_selection(case):
    x = case; load = x.loader
    def changed(*a):
        result = load(*a)
        with (x.workspace/'prefect-paperflow/config.json').open('ab') as stream: stream.write(b' ')
        return result
    x.loader = changed
    with pytest.raises(ValueError): run(x)
    assert not x.output.exists()


def test_existing_or_outside_output_is_never_overwritten(case):
    x = case
    x.module.EVIDENCE_ROOT.mkdir()
    x.output.write_bytes(b'keep')
    with pytest.raises(ValueError): run(x)
    assert x.output.read_bytes()==b'keep'
    x.output=x.data/'forbidden.json'
    with pytest.raises(ValueError): run(x)
    assert not x.output.exists()


def test_material_cannot_authorize_an_outside_dependency_or_parent(case):
    from test_snapshot_collect import rewrite_ref
    x = case; outside=x.journal/'unregistered.json'; outside.write_bytes(b'{"private":"synthetic"}')
    rewrite_ref(x.inputs['body']['material'],lambda v:v.setdefault('file_hashes',{}).update({
        str(outside):hashlib.sha256(outside.read_bytes()).hexdigest()}))
    x.refresh()
    with pytest.raises(ValueError): run(x)
    assert not x.output.exists()


def test_current_origin_narrow_root_requires_original_registered_hash_and_source(case):
    x=case; origin=x.classification/'one-group/handoff.json'
    origin.parent.mkdir()
    x.inputs,_,_,_=current_fixture(x.data/'current',delivered=True,origin_path=origin)
    bind_catalog(x.inputs,x.catalog)
    x.task_id=x.inputs['identity']['task_id'];x.refresh()
    run(x)
    record=json.loads(x.output.read_bytes())
    assert str(origin.parent) in record['configuration']['allowed_roots']
    assert str(x.classification) not in record['configuration']['allowed_roots']
    assert record['current_read_reference_count']>0


def test_record_verification_never_selects_a_replacement_after_original_changes(case):
    x=case; result=run(x); original=x.output.read_bytes()
    with sqlite3.connect(x.database) as db:
        db.execute('UPDATE llm_folder_queue SET task_run_id=?',(str(uuid4()),))
    with pytest.raises(ValueError):
        x.module.verify_record(sources=x.sources,evidence=x.output,
            expected_sha256=result['evidence_sha256'],loader=x.loader)
    assert x.output.read_bytes()==original


def test_live_readonly_boundary_rejects_business_writes_and_nonreadonly_connections(case):
    x=case
    boundary=getattr(x.module,'_read_only_boundary',None)
    assert callable(boundary), 'live readonly boundary is missing'
    business=x.data/'must-not-exist.txt'
    with boundary(x.sources,x.output):
        with pytest.raises(ValueError): business.write_bytes(b'no')
        with pytest.raises(ValueError): sqlite3.connect(x.database)
        with sqlite3.connect(x.database.as_uri()+'?mode=ro',uri=True) as db:
            assert db.execute('SELECT COUNT(*) FROM tasks').fetchone()[0]==1
        x.module.EVIDENCE_ROOT.mkdir()
        with x.output.open('xb') as stream: stream.write(b'candidate only')
        with pytest.raises(ValueError): x.output.write_bytes(b'no overwrite')
    assert not business.exists()


def test_configured_prefect_access_is_get_only(case):
    import httpx
    x=case; sent=[]
    with httpx.Client(transport=httpx.MockTransport(lambda request:
            (sent.append(request),httpx.Response(200,json={}))[1])) as client:
        with x.module._get_only_prefect(x.settings['prefect_api_url']):
            assert client.get(x.settings['prefect_api_url']+'/flow_runs/synthetic').status_code==200
            with pytest.raises(ValueError): client.post(x.settings['prefect_api_url']+'/flow_runs/filter',json={})
            with pytest.raises(ValueError): client.get('http://127.0.0.1:9999/api/model')
    assert len(sent)==1


def test_cli_failure_never_prints_exception_contents(case,monkeypatch,capsys):
    x=case
    def broken(**kw): raise ValueError('PRIVATE-PATH PRIVATE-PAPER')
    monkeypatch.setattr(x.module,'select_and_record',broken)
    assert x.module.main(['--workspace',str(x.workspace),'select','--task-id',x.task_id,
                          '--output',str(x.output)])==2
    output=capsys.readouterr().out
    assert 'PRIVATE' not in output and x.task_id not in output and str(x.data) not in output


@pytest.mark.parametrize('damage',['outside','sha','entry'])
def test_current_origin_never_self_authorizes_a_path_or_changed_identity(case,damage):
    x=case
    origin=(x.journal if damage=='outside' else x.classification)/'group/handoff.json'
    origin.parent.mkdir()
    def mutate(record,row):
        if damage=='entry':
            for item in [record['archive_listing'],*record['text_samples'],*record['images'],record['decision_basis']]:
                if item is not None: item['provenance']['entry_ref']='wrong-upstream-id'
    x.inputs,_,_,_=current_fixture(x.data/'current',delivered=True,origin_path=origin,mutate_record=mutate)
    bind_catalog(x.inputs,x.catalog)
    if damage=='sha': origin.write_bytes(origin.read_bytes()+b' ')
    x.task_id=x.inputs['identity']['task_id'];x.refresh()
    with pytest.raises(ValueError): run(x)
    assert not x.output.exists()


def test_verify_requires_independent_original_evidence_hash(case):
    x=case;run(x)
    with pytest.raises(ValueError):
        x.module.verify_record(sources=x.sources,evidence=x.output,expected_sha256='0'*64,loader=x.loader)


def test_complete_synthetic_selection_operates_inside_the_live_readonly_boundary(case):
    x=case
    with x.module._read_only_boundary(x.sources,x.output):
        result=run(x)
    with x.module._read_only_boundary(x.sources):
        assert x.module.verify_record(sources=x.sources,evidence=x.output,
            expected_sha256=result['evidence_sha256'],loader=x.loader)['same_original_identity']
