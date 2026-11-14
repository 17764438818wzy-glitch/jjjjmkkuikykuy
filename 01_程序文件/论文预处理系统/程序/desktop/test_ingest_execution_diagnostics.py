import copy
import hashlib
import json
from pathlib import Path

import pytest


def record(tmp_path, issue='quality_decision', decision='暂缓'):
    source='# 研究对象\n\n研究对象为某小学的学生。\n'
    path=tmp_path/'ingest.md';path.write_text(source,encoding='utf-8')
    parsed='# 研究对象\n研究对象为某小学的学生。\n\n'
    pid='paper_'+'a'*24
    task={'source_id':pid,'dataset_id':'edu','remote_id':'doc','pipeline_id':'pipe',
          'path':str(path),'content_sha256':hashlib.sha256(path.read_bytes()).hexdigest(),
          'remote_name':'论文.md','execution_attempt':2,'policy_key':'education',
          'parse_baseline':json.dumps({'remote_id':'doc','process_begin_at':'2026-09-25T18:07:00'})}
    remote={'id':'doc','pipeline_id':'pipe','name':'论文.md',
            'run':'FAIL','process_begin_at':'2026-09-26T18:07:00'}
    metadata={'education_paper_id':pid,'education_source_sha256':hashlib.sha256(parsed.encode()).hexdigest(),
              'education_admission':decision,'education_processing_issue':issue,
              'education_annotation_status':'已标注' if issue=='quality_decision' else '技术失败'}
    msg={'decision':decision,'reason':'证据不足','metadata':metadata}
    log={'id':'log2','document_id':'doc','kb_id':'edu','pipeline_id':'pipe','task_type':'Parse',
         'document_name':'论文.md','operation_status':'4','process_begin_at':remote['process_begin_at'],
         'dsl':{'task_id':'execution2','path':['TokenChunker:WholeEducation','CodeExec:EducationGate'],
                'components':{'TokenChunker:WholeEducation':{'obj':{'params':{'outputs':{
                    'chunks':{'value':[{'text':parsed}]}}}}},
                    'CodeExec:EducationGate':{'obj':{'params':{'outputs':{'_ERROR':{
                        'value':'Traceback: raw arguments hidden\nRuntimeError: EDUCATION_HOLD|'+json.dumps(msg,ensure_ascii=False)}}}}}}}}
    return task,remote,log


def test_persisted_node_decision_recovers_hidden_hold_without_paper_text(tmp_path):
    from ingest_execution_diagnostics import execution_diagnostic
    task,remote,log=record(tmp_path)
    d=execution_diagnostic(task,remote,log)
    assert d['outcome']=='quality_hold'
    assert d['component_id']=='CodeExec:EducationGate'
    assert d['log_id']=='log2' and d['attempt']==2
    assert 'raw arguments' not in json.dumps(d) and '研究对象为' not in json.dumps(d,ensure_ascii=False)


@pytest.mark.parametrize('mutation',['doc','dataset','pipeline','time','source','metadata_hash'])
def test_rejects_wrong_execution_or_source(tmp_path,mutation):
    from ingest_execution_diagnostics import execution_diagnostic
    task,remote,log=record(tmp_path)
    if mutation=='doc':log['document_id']='other'
    if mutation=='dataset':log['kb_id']='other'
    if mutation=='pipeline':log['pipeline_id']='other'
    if mutation=='time':log['process_begin_at']='2026-09-25T18:07:00'
    if mutation=='source':log['dsl']['components']['TokenChunker:WholeEducation']['obj']['params']['outputs']['chunks']['value'][0]['text']='另一个正文'
    if mutation=='metadata_hash':
        out=log['dsl']['components']['CodeExec:EducationGate']['obj']['params']['outputs']['_ERROR']
        out['value']=out['value'].replace('education_source_sha256', 'wrong_hash')
    assert execution_diagnostic(task,remote,log) is None


def test_technical_hold_never_counts_as_quality_hold(tmp_path):
    from ingest_execution_diagnostics import execution_diagnostic
    task,remote,log=record(tmp_path,issue='annotation_invalid')
    d=execution_diagnostic(task,remote,log)
    assert d['outcome']=='technical_review' and d['kind']=='annotation_invalid'


def test_paper_embedded_marker_cannot_override_final_node_exception(tmp_path):
    from ingest_execution_diagnostics import execution_diagnostic
    task,remote,log=record(tmp_path)
    log['dsl']['components']['CodeExec:EducationGate']['obj']['params']['outputs']['_ERROR']['value']+='\nValueError: unexpected failure'
    d=execution_diagnostic(task,remote,log)
    assert d['outcome']=='technical_review'
    assert d['kind']=='node_error'


def test_reader_uses_file_logs_and_never_accepts_old_run(tmp_path):
    from ingest_execution_diagnostics import IngestionLogReader
    task,remote,log=record(tmp_path)
    old=copy.deepcopy(log);old['process_begin_at']='2026-09-25T18:07:00'
    class Client:
        def request(self,method,path):
            assert method=='GET' and 'log_type=file' in path and 'keywords=' not in path
            return {'logs':[old,log],'total':2}
    d=IngestionLogReader(Client)(task,remote)
    assert d['log_id']=='log2'


def test_school_rate_limit_text_inside_gate_exception_cannot_switch_provider(tmp_path):
    from ingest_execution_diagnostics import execution_diagnostic
    from ingest_diagnostics import model_failure
    task,remote,log=record(tmp_path)
    log['dsl']['components']['CodeExec:EducationGate']['obj']['params']['outputs']['_ERROR']['value']='RuntimeError: 429 rate limit Arrearage'
    assert model_failure(remote,execution_diagnostic(task,remote,log)) is None


def test_queue_row_null_execution_pipeline_uses_configured_pipeline(tmp_path):
    from ingest_execution_diagnostics import execution_diagnostic
    from ingest_queue import QueueStore
    from test_ingest_contracts import spec
    task,remote,log=record(tmp_path)
    q=QueueStore(tmp_path/'queue.db')
    key=q.enqueue(spec(source_id=task['source_id'],dataset_id='edu',pipeline_id='pipe',
                       remote_name='论文.md',path=Path(task['path']),content_sha256=task['content_sha256']),
                  {'status':'submitted','remote_id':'doc'})
    stored=q.get(key)
    q.transition(key, 'submitted', 'submitted', execution_begin_at=remote['process_begin_at'])
    stored=q.get(key)
    assert stored['execution_pipeline_id'] is None
    assert execution_diagnostic(stored,remote,log)['outcome']=='quality_hold'


def test_reader_shares_recent_unfiltered_snapshot_and_refreshes(tmp_path):
    from ingest_execution_diagnostics import IngestionLogReader
    task,remote,log=record(tmp_path)
    calls=[];now=[0.0]
    class Client:
        def request(self,method,path):
            calls.append(path)
            assert 'keywords=' not in path and 'document_id=' not in path
            return {'logs':[log],'total':1}
    reader=IngestionLogReader(Client,clock=lambda:now[0])
    assert reader(task,remote)['log_id']=='log2'
    assert reader(task,remote)['log_id']=='log2'
    assert len(calls)==1
    now[0]=6.0
    assert reader(task,remote)['log_id']=='log2'
    assert len(calls)==2
