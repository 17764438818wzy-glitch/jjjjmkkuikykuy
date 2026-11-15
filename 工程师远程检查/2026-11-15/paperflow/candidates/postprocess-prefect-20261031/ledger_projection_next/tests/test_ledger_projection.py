"""Synthetic ledger snapshots; no services, paper content or artifact reads."""
import hashlib
import importlib.util
import json
from pathlib import Path, PurePosixPath, PureWindowsPath
import sqlite3
from types import SimpleNamespace
from uuid import UUID, uuid4, uuid5

import pytest

SPEC = importlib.util.spec_from_file_location('ledger_projection_candidate',
    Path(__file__).resolve().parents[1] / 'ledger_projection.py')
projection = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(projection)
UPSTREAM_SPEC = importlib.util.spec_from_file_location('upstream_folder_completion',
    Path(__file__).resolve().parents[4] / 'prefect-paperflow' / 'paperflow_prefect' /
    'llm_queue' / 'folder_completion.py')
upstream = importlib.util.module_from_spec(UPSTREAM_SPEC)
UPSTREAM_SPEC.loader.exec_module(upstream)
FIELDS = ('task_id','group_id','paper_id','receipt_id','attempt_id','parent_run_id','task_run_id')
NAMESPACE = UUID('5e9b7732-458e-50cb-8916-6b53007c204f')


@pytest.fixture
def db(tmp_path):
    path = tmp_path / 'control.sqlite3'
    with sqlite3.connect(path) as conn:
        conn.executescript('''
            PRAGMA journal_mode=WAL;
            CREATE TABLE tasks(task_id TEXT PRIMARY KEY,name TEXT,prefect_order_model TEXT,
                prefect_run_id TEXT,desired_state TEXT,start_generation INTEGER,start_pending INTEGER,task_status TEXT);
            CREATE TABLE task_deletions(task_id TEXT PRIMARY KEY);
            CREATE TABLE llm_folder_queue(task_id TEXT,group_id TEXT,paper_id TEXT,receipt_id TEXT,
                attempt_id TEXT,parent_run_id TEXT,task_run_id TEXT,manifest_sha256 TEXT,state TEXT,
                generation INTEGER,updated_at_ns INTEGER,PRIMARY KEY(task_id,group_id));
            CREATE TABLE llm_request_admissions(task_id TEXT,component_id TEXT,generation INTEGER,
                request_sha256 TEXT,state TEXT,response_sha256 TEXT,kind TEXT,work_dir TEXT,model_dir TEXT,
                receipt_id TEXT,task_run_id TEXT,source_sha256 TEXT,created_at_ns INTEGER,updated_at_ns INTEGER,
                PRIMARY KEY(task_id,component_id));
            CREATE TABLE paperflow_postprocess_stage_jobs(job_id TEXT PRIMARY KEY,task_id TEXT,group_id TEXT,
                paper_id TEXT,receipt_id TEXT,attempt_id TEXT,parent_run_id TEXT,task_run_id TEXT,folder_run_id TEXT,
                generation INTEGER,manifest_sha256 TEXT,task_key TEXT,state TEXT,error_type TEXT,owner_pid INTEGER,
                owner_started REAL,created_at_ns INTEGER,updated_at_ns INTEGER,source_llm_generation INTEGER,
                UNIQUE(task_id,group_id));
            CREATE TABLE postprocessing_claims(task_id TEXT,group_id TEXT,paper_id TEXT,receipt_id TEXT,
                attempt_id TEXT,parent_run_id TEXT,task_run_id TEXT,manifest_sha256 TEXT,claim_id TEXT,
                generation INTEGER,state TEXT,detail TEXT,created_at_ns INTEGER,updated_at_ns INTEGER,
                PRIMARY KEY(task_id,group_id));
            CREATE TABLE postprocessing_results(task_id TEXT,group_id TEXT,paper_id TEXT,receipt_id TEXT,
                attempt_id TEXT,parent_run_id TEXT,task_run_id TEXT,manifest_sha256 TEXT,generation INTEGER,
                result_id TEXT,state TEXT,code TEXT,claim_id TEXT,stage_receipt_sha256 TEXT,
                proof_path TEXT,proof_sha256 TEXT,created_at_ns INTEGER,updated_at_ns INTEGER,
                PRIMARY KEY(task_id,group_id));
            CREATE TABLE postprocessing_receipts(claim_id TEXT PRIMARY KEY,receipt_json TEXT,
                receipt_sha256 TEXT,created_at_ns INTEGER);
        ''')
        task_id,parent=str(uuid4()),str(uuid4())
        conn.execute('INSERT INTO tasks VALUES (?,?,?,?,?,?,?,?)',
                     (task_id,'Synthetic order','v2',parent,'running',2,0,'running'))
    return SimpleNamespace(path=path,task=task_id,parent=parent,tmp=tmp_path)


def add_folder(db, n, *, job=None, outcome=None, claim=None, receipt=True, raw='raw_complete'):
    identity=dict(task_id=db.task,group_id=f'{n:032x}',paper_id=f'paper-{n}',receipt_id=f'{n:064x}',
                  attempt_id=f'attempt-{n}',parent_run_id=db.parent,task_run_id=str(uuid4()))
    manifest='c'*64
    job_id=str(uuid5(NAMESPACE,identity['task_run_id']))
    task_key='paperflow-folder-postprocess-stage-v1-'+UUID(job_id).hex
    claim_id=str(uuid4())
    body_dir=(db.tmp / 'output' / str(n) / 'body').absolute()
    package=body_dir / 'publication'
    body_path=str(package / 'article.redacted.md')
    value={'schema':'paperflow-postprocessing-receipt/1','identity':identity,'claim_id':claim_id,
           'manifest_sha256':manifest,'validation_status':'passed',
           'body':{'package_path':str(package),'manifest_sha256':'d'*64},
           'attachments':[],'artifacts':[{'path':body_path,'sha256':'e'*64},
               {'path':str(package/'manifest.json'),'sha256':'d'*64},
               {'path':str(package/'READY.json'),'sha256':'1'*64},
               {'path':str(body_dir/'body-result.json'),'sha256':'2'*64}]}
    encoded=json.dumps(value,ensure_ascii=False,separators=(',',':'),sort_keys=True)
    digest=hashlib.sha256(encoded.encode()).hexdigest()
    with sqlite3.connect(db.path) as conn:
        conn.execute('INSERT INTO llm_folder_queue VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                     tuple(identity[k] for k in FIELDS)+(manifest,'waiting_postprocessing',1,n))
        conn.execute('INSERT INTO llm_request_admissions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                     (db.task,'component-'+str(n),1,'a'*64,raw,'b'*64,'body','unused','unused',
                      identity['receipt_id'],identity['task_run_id'],'f'*64,n,n))
        if job is not None:
            conn.execute('INSERT INTO paperflow_postprocess_stage_jobs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                (job_id,*(identity[k] for k in FIELDS),identity['task_run_id'],2,manifest,task_key,job,
                 None,123 if job=='executing' else None,1.0 if job=='executing' else None,n,n,1))
        if claim is not None:
            conn.execute('INSERT INTO postprocessing_claims VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                (*(identity[k] for k in FIELDS),manifest,claim_id,2,claim,None,n,n))
        if outcome is not None:
            state,code=outcome
            proof_path=str((db.tmp/'nonexistent-private-proof.json').absolute()) if state in {'failed','unrecognized'} else None
            conn.execute('INSERT INTO postprocessing_results VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                (*(identity[k] for k in FIELDS),manifest,2,str(uuid4()),state,code,
                 claim_id if state=='passed' else None,digest if state=='passed' else None,
                 proof_path,'f'*64 if proof_path else None,n,n))
        if claim is not None and receipt:
            conn.execute('INSERT INTO postprocessing_receipts VALUES (?,?,?,?)',(claim_id,encoded,digest,n))
    return identity


def add_attachment_admission(db, identity, file_id='attachment-1', digest='f'*64):
    ident=upstream.component_id(task_id=identity['task_id'],group_id=identity['group_id'],
        receipt_id=identity['receipt_id'],kind='attachment',file_id=file_id,source_sha256=digest)
    with sqlite3.connect(db.path) as c:
        c.execute('INSERT INTO llm_request_admissions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
            (identity['task_id'],ident,1,'a'*64,'raw_complete','b'*64,'attachment','unused','unused',
             identity['receipt_id'],identity['task_run_id'],digest,1,1))
    root=(db.tmp/'attachment'/identity['group_id']/file_id).absolute()
    return {'paper_id':identity['paper_id'],'attachment_id':file_id,'source_sha256':digest,
            'source_path':str(root/'source.pdf'),'md_path':str(root/'attachment.md'),
            'md_sha256':'e'*64,'delivery_version':'d'*64}


def set_receipt_attachments(db, attachments, group_id=None, mutate=None):
    with sqlite3.connect(db.path) as c:
        if group_id is None:
            claim_id,encoded=c.execute('SELECT claim_id,receipt_json FROM postprocessing_receipts').fetchone()
        else:
            claim_id,encoded=c.execute('SELECT r.claim_id,r.receipt_json FROM postprocessing_receipts r '
                'JOIN postprocessing_claims c ON r.claim_id=c.claim_id WHERE c.group_id=?',(group_id,)).fetchone()
        value=json.loads(encoded)
        value['attachments']=attachments
        seen={item['path'] for item in value['artifacts']}
        for item in attachments:
            for path_key,hash_key in (('source_path','source_sha256'),('md_path','md_sha256')):
                path,digest=item.get(path_key),item.get(hash_key)
                if isinstance(path,str) and isinstance(digest,str) and path not in seen:
                    value['artifacts'].append({'path':path,'sha256':digest})
                    seen.add(path)
        if mutate is not None:
            mutate(value)
        encoded=json.dumps(value,ensure_ascii=False,separators=(',',':'),sort_keys=True)
        digest=hashlib.sha256(encoded.encode()).hexdigest()
        c.execute('UPDATE postprocessing_receipts SET receipt_json=?,receipt_sha256=? WHERE claim_id=?',
                  (encoded,digest,claim_id))
        c.execute('UPDATE postprocessing_results SET stage_receipt_sha256=? WHERE claim_id=?',(digest,claim_id))


@pytest.mark.parametrize('case',['correct','missing','duplicate','extra','wrong_id','wrong_source','wrong_paper',
    'component_fn_missing','component_fn_error','missing_source_path','missing_md_path','missing_md_hash',
    'invalid_version','same_source_md','source_artifact_mismatch','md_artifact_mismatch','missing_md_artifact',
    'incomplete_named_fields'])
def test_receipt_attachments_match_all_registered_attachment_admissions(db,case):
    ident=add_folder(db,1,job='finished',claim='waiting_rag',outcome=('passed','passed'))
    item=add_attachment_admission(db,ident)
    attachments=[item.copy()]
    if case=='missing': attachments=[]
    elif case=='duplicate': attachments=[item.copy(),item.copy()]
    elif case=='extra': attachments.append(dict(item,attachment_id='other'))
    elif case=='wrong_id': attachments[0]['attachment_id']='other'
    elif case=='wrong_source': attachments[0]['source_sha256']='a'*64
    elif case=='wrong_paper': attachments[0]['paper_id']='other'
    elif case=='missing_source_path': del attachments[0]['source_path']
    elif case=='missing_md_path': del attachments[0]['md_path']
    elif case=='missing_md_hash': del attachments[0]['md_sha256']
    elif case=='invalid_version': attachments[0]['delivery_version']='not-a-hash'
    elif case=='same_source_md': attachments[0]['md_path']=attachments[0]['source_path']
    elif case=='incomplete_named_fields': attachments[0]['named_proof_path']=str(db.tmp/'named-proof.json')
    def mutate(value):
        if case in {'source_artifact_mismatch','md_artifact_mismatch','missing_md_artifact'}:
            target=item['source_path'] if case=='source_artifact_mismatch' else item['md_path']
            for artifact in value['artifacts']:
                if artifact['path']==target:
                    if case=='missing_md_artifact': value['artifacts'].remove(artifact)
                    else: artifact['sha256']='0'*64
                    break
    set_receipt_attachments(db,attachments,mutate=mutate)
    fn=upstream.component_id
    if case=='component_fn_missing': fn=None
    elif case=='component_fn_error': fn=lambda **_: (_ for _ in ()).throw(ValueError('bad'))
    value=projection.snapshot(db.path,component_id_fn=fn)
    assert value['results']['counts']['passed']==int(case=='correct')
    assert value['stage']['counts']['waiting_rag_upload']==int(case=='correct')
    assert value['stage']['counts']['postprocessing_unresolved']==int(case!='correct')


def test_multiple_registered_attachments_match_independent_of_order(db):
    ident=add_folder(db,1,job='finished',claim='waiting_rag',outcome=('passed','passed'))
    first=add_attachment_admission(db,ident,file_id='attachment-1',digest='c'*64)
    second=add_attachment_admission(db,ident,file_id='attachment-2',digest='d'*64)
    set_receipt_attachments(db,[second,first])
    value=projection.snapshot(db.path,component_id_fn=upstream.component_id)
    assert value['results']['counts']['passed']==1
    assert value['stage']['counts']['waiting_rag_upload']==1


def test_named_attachment_requires_bound_original_and_proof_references(db):
    ident=add_folder(db,1,job='finished',claim='waiting_rag',outcome=('passed','passed'))
    item=add_attachment_admission(db,ident)
    item.update(original_source_path=str((db.tmp/'original.pdf').absolute()),
                named_proof_path=str((db.tmp/'proof.json').absolute()),named_proof_sha256='3'*64)
    def add_named_refs(value):
        value['artifacts'].extend([{'path':item['original_source_path'],'sha256':item['source_sha256']},
                                   {'path':item['named_proof_path'],'sha256':item['named_proof_sha256']}])
    set_receipt_attachments(db,[item],mutate=add_named_refs)
    assert projection.snapshot(db.path,component_id_fn=upstream.component_id)['results']['counts']['passed']==1
    def remove_proof(value):
        value['artifacts']=[a for a in value['artifacts'] if a['path']!=item['named_proof_path']]
    set_receipt_attachments(db,[item],mutate=remove_proof)
    value=projection.snapshot(db.path,component_id_fn=upstream.component_id)
    assert value['results']['counts']['passed']==0
    assert value['stage']['counts']['postprocessing_unresolved']==1


def test_duplicate_windows_artifact_paths_are_not_distinct_after_case_normalization(db):
    add_folder(db,1,job='finished',claim='waiting_rag',outcome=('passed','passed'))
    def mutate(value):
        package=PureWindowsPath('C:/paperflow/body/publication')
        value['body']['package_path']=str(package)
        value['artifacts']=[{'path':str(package/'manifest.json'),'sha256':'d'*64},
            {'path':str(package/'READY.json'),'sha256':'1'*64},
            {'path':str(package.parent/'body-result.json'),'sha256':'2'*64},
            {'path':'c:/PAPERFLOW/body/publication/MANIFEST.JSON','sha256':'d'*64}]
    set_receipt_attachments(db,[],mutate=mutate)
    value=projection.snapshot(db.path)
    assert value['results']['counts']['passed']==0
    assert value['stage']['counts']['postprocessing_unresolved']==1


@pytest.mark.parametrize('case',['missing_manifest','wrong_manifest_digest','missing_ready',
                                 'missing_body_result','wrong_package_path'])
def test_body_receipt_requires_registered_seal_references(db,case):
    add_folder(db,1,job='finished',claim='waiting_rag',outcome=('passed','passed'))
    def mutate(value):
        package=Path(value['body']['package_path'])
        names={'missing_manifest':package/'manifest.json','wrong_manifest_digest':package/'manifest.json',
               'missing_ready':package/'READY.json','missing_body_result':package.parent/'body-result.json'}
        if case=='wrong_package_path':
            value['body']['package_path']=str(package/'other')
            return
        target=str(names[case])
        for artifact in value['artifacts']:
            if artifact['path']==target:
                if case=='wrong_manifest_digest': artifact['sha256']='0'*64
                else: value['artifacts'].remove(artifact)
                break
    set_receipt_attachments(db,[],mutate=mutate)
    value=projection.snapshot(db.path)
    assert value['results']['counts']['passed']==0
    assert value['stage']['counts']['waiting_rag_upload']==0
    assert value['stage']['counts']['postprocessing_unresolved']==1


def test_registered_snapshot_separates_waiting_inflight_and_accepted(db):
    add_folder(db,1)
    add_folder(db,2,job='reserved')
    add_folder(db,3,job='executing')  # ownership is recorded before result reservation
    add_folder(db,4,job='finished',claim='waiting_rag',outcome=('passed','passed'))
    add_folder(db,5,job='finished',claim='receipt_held',outcome=('passed','passed'))
    add_folder(db,6,job='failed',claim='unknown',receipt=False,outcome=('failed','body_validation'))
    value=projection.snapshot(db.path)
    assert value['stage']['available'] is True
    assert value['stage']['basis']=='postprocessing-registered-ledger/1'
    counts=value['stage']['counts']
    assert counts['waiting_postprocessing']==1
    assert counts['postprocessing_registered_inflight']==2
    assert counts['postprocessing_registered_executing']==1
    assert counts['postprocessing_in_progress'] is None
    assert counts['waiting_rag_upload']==1 and counts['postprocessing_held']==1
    assert value['results']['counts']=={'passed':2,'failed':1,'unrecognized':None}
    assert value['stage']['as_of']==value['results']['as_of']==value['as_of']
    assert value['current_delivery_verified'] is False
    # No referenced artifact or diagnostic exists: this is a registration view.


@pytest.mark.parametrize('state',['submission_unknown','execution_unknown','identity_unknown','delivery_unknown',
                                  'stopped_before_submit','cancel_fenced_submitted'])
def test_unresolved_job_is_never_waiting_or_passed(db,state):
    add_folder(db,1,job=state)
    value=projection.snapshot(db.path)
    assert value['stage']['counts']['waiting_postprocessing']==0
    assert value['results']['counts']['passed']==0
    assert value['stage']['counts']['postprocessing_unresolved']==1


def test_pass_registered_before_job_finalization_is_not_reported_as_pass(db):
    add_folder(db,1,job='executing',claim='waiting_rag',outcome=('passed','passed'))
    value=projection.snapshot(db.path)
    assert value['stage']['counts']['postprocessing_registered_executing']==1
    assert value['stage']['counts']['waiting_rag_upload']==0
    assert value['results']['counts']['passed']==0
    assert value['results']['partial'] is True


@pytest.mark.parametrize('change', ["UPDATE paperflow_postprocess_stage_jobs SET attempt_id='other'",
                                    "UPDATE tasks SET task_status='unknown'",
                                    "UPDATE paperflow_postprocess_stage_jobs SET source_llm_generation=2"])
def test_identity_or_control_conflicts_cannot_count_as_registered_inflight(db,change):
    add_folder(db,1,job='executing')
    with sqlite3.connect(db.path) as c:
        c.execute(change)
    value=projection.snapshot(db.path)
    assert value['stage']['counts']['postprocessing_registered_inflight']==0
    assert value['stage']['counts']['postprocessing_registered_executing']==0
    assert value['stage']['counts']['postprocessing_unresolved']==1
    assert value['stage']['partial'] is True


@pytest.mark.parametrize('change',['job_identity','job_generation','raw_unknown','missing_result','missing_receipt',
                                   'receipt_content','receipt_hash','claim_identity','result_identity','failure_proof'])
def test_inconsistent_or_unknown_evidence_never_counts_as_pass(db,change):
    ident=add_folder(db,1,job='finished',claim='waiting_rag',outcome=('passed','passed'))
    with sqlite3.connect(db.path) as c:
        if change=='job_identity': c.execute("UPDATE paperflow_postprocess_stage_jobs SET attempt_id='other'")
        elif change=='job_generation': c.execute('UPDATE paperflow_postprocess_stage_jobs SET source_llm_generation=2')
        elif change=='raw_unknown': c.execute("UPDATE llm_request_admissions SET state='unknown'")
        elif change=='missing_result': c.execute('DELETE FROM postprocessing_results')
        elif change=='missing_receipt': c.execute('DELETE FROM postprocessing_receipts')
        elif change=='receipt_content': c.execute("UPDATE postprocessing_receipts SET receipt_json=receipt_json||' '")
        elif change=='receipt_hash': c.execute("UPDATE postprocessing_results SET stage_receipt_sha256=?",('0'*64,))
        elif change=='claim_identity': c.execute("UPDATE postprocessing_claims SET paper_id='other'")
        elif change=='result_identity': c.execute("UPDATE postprocessing_results SET receipt_id=?",('0'*64,))
        else:
            c.execute("UPDATE paperflow_postprocess_stage_jobs SET state='failed'")
            c.execute("UPDATE postprocessing_claims SET state='unknown'")
            c.execute("UPDATE postprocessing_results SET state='failed',code='body_validation',proof_path=NULL,proof_sha256=NULL")
    value=projection.snapshot(db.path)
    assert value['results']['counts']['passed']==0
    assert value['stage']['counts']['waiting_rag_upload']==0
    assert value['stage']['counts']['postprocessing_unresolved']==1
    assert value['stage']['partial'] is True
    if change=='failure_proof': assert value['results']['counts']['failed']==0


@pytest.mark.parametrize('change', ["UPDATE tasks SET task_status='unknown'",
    "UPDATE tasks SET task_status='deleting'", "UPDATE tasks SET task_status='delete_failed'",
    "UPDATE tasks SET task_status=NULL", "UPDATE tasks SET desired_state='unknown'",
    "UPDATE tasks SET start_pending=1", "UPDATE tasks SET start_pending=NULL"])
def test_ambiguous_task_control_state_is_not_reported_as_pass_or_waiting(db, change):
    add_folder(db,1,job='finished',claim='waiting_rag',outcome=('passed','passed'))
    with sqlite3.connect(db.path) as c:
        c.execute(change)
    value=projection.snapshot(db.path)
    assert value['available'] is True
    assert value['results']['counts']['passed']==0
    assert value['stage']['counts']['waiting_rag_upload']==0
    assert value['stage']['counts']['postprocessing_unresolved']==1
    assert value['stage']['partial'] is True


def test_paused_task_may_show_historically_registered_result(db):
    add_folder(db,1,job='finished',claim='waiting_rag',outcome=('passed','passed'))
    with sqlite3.connect(db.path) as c:
        c.execute("UPDATE tasks SET desired_state='paused',task_status='paused'")
    value=projection.snapshot(db.path)
    assert value['results']['counts']['passed']==1
    assert value['current_delivery_verified'] is False


def test_reconciled_claim_may_advance_generation_without_changing_finished_result(db):
    add_folder(db,1,job='finished',claim='waiting_rag',outcome=('passed','passed'))
    with sqlite3.connect(db.path) as c:
        c.execute('UPDATE tasks SET start_generation=3')
        c.execute('UPDATE postprocessing_claims SET generation=3')
    value=projection.snapshot(db.path)
    assert value['results']['counts']['passed']==1
    assert value['stage']['counts']['waiting_rag_upload']==1


@pytest.mark.parametrize('code',['body_validation','resource_unavailable','attachment_format'])
def test_failed_result_without_persisted_proof_is_unresolved(db,code):
    add_folder(db,1,job='failed',claim='unknown',receipt=False,outcome=('failed',code))
    with sqlite3.connect(db.path) as c:
        c.execute('UPDATE postprocessing_results SET proof_path=NULL,proof_sha256=NULL')
    value=projection.snapshot(db.path)
    assert value['results']['counts']['failed']==0
    assert value['stage']['counts']['postprocessing_unresolved']==1


def test_unrecognized_total_stays_unknown_and_registered_list_is_available(db):
    add_folder(db,1,job='failed',claim='unknown',receipt=False,outcome=('unrecognized','unrecognized'))
    value=projection.snapshot(db.path,result_filter='unrecognized')
    assert value['results']['counts']['unrecognized'] is None
    assert value['results']['list']['total'] is None
    assert len(value['results']['list']['items'])==1


def test_deleted_tasks_are_not_exposed_and_unknown_orphans_are_not_silently_dropped(db):
    add_folder(db,1,job='finished',claim='waiting_rag',outcome=('passed','passed'))
    with sqlite3.connect(db.path) as c:
        c.execute('INSERT INTO task_deletions VALUES (?)',(db.task,))
    value=projection.snapshot(db.path)
    assert value['results']['counts']['passed']==0
    assert value['stage']['counts']['waiting_rag_upload']==0
    with sqlite3.connect(db.path) as c:
        c.execute('DELETE FROM task_deletions')
        c.execute('DELETE FROM llm_folder_queue')
    value=projection.snapshot(db.path)
    assert value['stage']['counts']['postprocessing_unresolved']==1
    assert value['results']['counts']['passed']==0


def test_pagination_and_revision_prevent_mixing_snapshots(db):
    for n in range(1,5):
        add_folder(db,n,job='failed',claim='unknown',receipt=False,outcome=('failed','body_validation'))
    first=projection.snapshot(db.path,result_filter='failed',page=1,page_size=2)
    missing=projection.snapshot(db.path,result_filter='failed',page=2,page_size=2)
    assert missing['results']['available'] is False
    assert missing['reason']=='snapshot_revision_required'
    second=projection.snapshot(db.path,result_filter='failed',page=2,page_size=2,
                               expected_revision=first['revision'])
    assert first['results']['list']['total']==4 and first['results']['list']['has_next'] is True
    assert len(first['results']['list']['items'])==len(second['results']['list']['items'])==2
    assert {x['group_id'] for x in first['results']['list']['items']}.isdisjoint(
           {x['group_id'] for x in second['results']['list']['items']})
    add_folder(db,5,job='failed',claim='unknown',receipt=False,outcome=('failed','body_validation'))
    changed=projection.snapshot(db.path,result_filter='failed',page=2,page_size=2,
                                expected_revision=first['revision'])
    assert changed['results']['available'] is False


def test_pagination_revision_binds_dependency_behavior_without_binding_callable_identity(db):
    for n in (1,2):
        ident=add_folder(db,n,job='finished',claim='waiting_rag',outcome=('passed','passed'))
        item=add_attachment_admission(db,ident,file_id='attachment-'+str(n),digest=str(n)*64)
        set_receipt_attachments(db,[item],group_id=ident['group_id'])
    first=projection.snapshot(db.path,result_filter='passed',page=1,page_size=1,
                              component_id_fn=upstream.component_id)
    equivalent=projection.snapshot(db.path,result_filter='passed',page=2,page_size=1,
        expected_revision=first['revision'],component_id_fn=lambda **kw: upstream.component_id(**kw))
    assert first['results']['counts']['passed']==2
    assert equivalent['available'] is True
    assert first['revision']==equivalent['revision']
    changed=projection.snapshot(db.path,result_filter='passed',page=2,page_size=1,
        expected_revision=first['revision'],component_id_fn=lambda **_: '0'*64)
    assert changed['available'] is False
    assert changed['reason']=='snapshot_changed'
    missing=projection.snapshot(db.path,result_filter='passed',page=2,page_size=1,
        expected_revision=first['revision'])
    assert missing['available'] is False
    assert missing['reason']=='snapshot_changed'


@pytest.mark.parametrize(('table','column'), [('tasks','prefect_order_model'),
    ('llm_folder_queue','generation'), ('llm_request_admissions','response_sha256'),
    ('llm_request_admissions','component_id'),
    ('paperflow_postprocess_stage_jobs','source_llm_generation'),
    ('postprocessing_claims','manifest_sha256'), ('postprocessing_results','proof_sha256'),
    ('postprocessing_receipts','receipt_json')])
def test_empty_table_with_missing_required_column_is_unavailable(db,table,column):
    with sqlite3.connect(db.path) as c:
        if (table,column)==('llm_request_admissions','component_id'):
            columns=[row[1] for row in c.execute(f'PRAGMA table_info({table})') if row[1]!=column]
            c.execute(f'ALTER TABLE {table} RENAME TO temporary_admissions')
            c.execute(f'CREATE TABLE {table} ('+', '.join(name+' TEXT' for name in columns)+')')
            c.execute('DROP TABLE temporary_admissions')
        else:
            c.execute(f'ALTER TABLE {table} DROP COLUMN {column}')
    value=projection.snapshot(db.path)
    assert value['available'] is False
    assert all(v is None for v in value['stage']['counts'].values())
    assert value['results']['counts']['passed'] is None


@pytest.mark.parametrize(('package','artifact','valid'), [
    ('C:\\paperflow\\publication', 'C:\\paperflow\\publication\\article.md',True),
    ('/opt/paperflow/publication', '/opt/paperflow/publication/article.md',True),
    ('\\\\server\\share\\publication', '\\\\server\\share\\publication\\article.md',True),
    ('C:relative\\publication', 'C:relative\\article.md',False),
    ('relative/publication', 'relative/article.md',False),
])
def test_receipt_paths_are_checked_lexically_across_operating_systems(db,package,artifact,valid):
    add_folder(db,1,job='finished',claim='waiting_rag',outcome=('passed','passed'))
    with sqlite3.connect(db.path) as c:
        claim_id,encoded=c.execute('SELECT claim_id,receipt_json FROM postprocessing_receipts').fetchone()
        value=json.loads(encoded)
        value['body']['package_path']=package
        pure=PureWindowsPath(package) if PureWindowsPath(package).is_absolute() else PurePosixPath(package)
        value['artifacts']=[{'path':artifact,'sha256':'e'*64},
            {'path':str(pure/'manifest.json'),'sha256':'d'*64},
            {'path':str(pure/'READY.json'),'sha256':'1'*64},
            {'path':str(pure.parent/'body-result.json'),'sha256':'2'*64}]
        encoded=json.dumps(value,ensure_ascii=False,separators=(',',':'),sort_keys=True)
        digest=hashlib.sha256(encoded.encode()).hexdigest()
        c.execute('UPDATE postprocessing_receipts SET receipt_json=?,receipt_sha256=? WHERE claim_id=?',
                  (encoded,digest,claim_id))
        c.execute('UPDATE postprocessing_results SET stage_receipt_sha256=?',(digest,))
    value=projection.snapshot(db.path)
    assert value['results']['counts']['passed']==int(valid)
    assert value['stage']['counts']['postprocessing_unresolved']==int(not valid)


def test_missing_schema_and_unreadable_database_are_unknown(db,monkeypatch):
    with sqlite3.connect(db.path) as c: c.execute('DROP TABLE postprocessing_receipts')
    assert projection.snapshot(db.path)['stage']['available'] is False
    monkeypatch.setattr(projection.sqlite3,'connect',lambda *a,**k: (_ for _ in ()).throw(sqlite3.OperationalError('denied')))
    assert projection.snapshot(db.path)['results']['available'] is False


def test_duplicate_admission_in_an_inconsistent_schema_is_unavailable(db):
    add_folder(db, 1)
    with sqlite3.connect(db.path) as c:
        c.execute('ALTER TABLE llm_request_admissions RENAME TO old_admissions')
        c.execute('CREATE TABLE llm_request_admissions AS SELECT * FROM old_admissions')
        c.execute('INSERT INTO llm_request_admissions SELECT * FROM old_admissions')
    value = projection.snapshot(db.path)
    assert value['available'] is False
    assert value['stage']['counts']['waiting_postprocessing'] is None


def test_all_tables_share_one_read_transaction_despite_concurrent_commit(db,monkeypatch):
    add_folder(db,1)
    real_connect=sqlite3.connect
    committed=[]
    def wrapped(*args,**kwargs):
        connection=real_connect(*args,**kwargs)
        def trace(statement):
            if not committed and 'SELECT * FROM paperflow_postprocess_stage_jobs' in statement:
                committed.append(True)
                # A new stage appears after the reader's snapshot is established.
                add_folder(db,2,job='reserved')
        connection.set_trace_callback(trace)
        return connection
    monkeypatch.setattr(projection,'sqlite3',SimpleNamespace(connect=wrapped,Row=sqlite3.Row,Error=sqlite3.Error))
    first=projection.snapshot(db.path)
    assert committed and first['stage']['counts']['waiting_postprocessing']==1
    assert first['stage']['counts']['postprocessing_registered_inflight']==0
    second=projection.snapshot(db.path)
    assert second['stage']['counts']['postprocessing_registered_inflight']==1
