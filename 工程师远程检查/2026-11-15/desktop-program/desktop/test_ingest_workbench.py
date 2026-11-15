import json
import threading
from pathlib import Path

import pytest

from engine import digest
from evidence import export_package, package_fingerprint
from ingest_contracts import Outcome
from ingest_workbench import classify_finance, workbench_specs
from test_knowledge_package import sample


class Store:
    def __init__(self, manifest):
        self.manifest = manifest
        self.loads = 0
        self.lock = threading.RLock()

    def load(self, run_id):
        self.loads += 1
        return self.manifest

    def save(self, manifest):
        self.manifest = manifest


def prepared(tmp_path):
    package = tmp_path / 'package'
    package.mkdir()
    metadata = sample(package)
    export_package(package, metadata)
    manifest = json.loads((package / 'manifest.json').read_text(encoding='utf-8'))
    item = {'id': manifest['document_id'], 'paper_id': manifest['paper_id'],
            'status': 'pass', 'automatic_approval': True,
            'category': '经管财会类', 'major': '会计学', 'title': manifest['title'],
            'path': str(package / 'ingest.md'), 'package_path': str(package),
            'reviewed_hash': digest(package / 'ingest.md'),
            'reviewed_package_hash': package_fingerprint(package)}
    datasets = [{'id': 'finance-md', 'name': '财会 md', 'pipeline_id': 'finance-pipe'},
                {'id': 'finance-img', 'name': '财会图片'},
                {'id': 'manual-md', 'name': '指定 md', 'pipeline_id': 'manual-pipe'},
                {'id': 'manual-img', 'name': '指定图片'}]
    config = {'datasets': datasets, 'dataset_map': {'会计学': {
        'paper': 'finance-md', 'image': 'finance-img'}}, 'manual_request': False}
    return Store({'run_id': 'batch-1', 'items': [item]}), item, config


def test_workbench_modes_and_fixed_manual_targets(tmp_path):
    store, item, config = prepared(tmp_path)
    upload_only = workbench_specs(store, 'batch-1', [item['id']],
                                  {'paper': {'id': 'manual-md'},
                                   'image': {'id': 'manual-img'}},
                                  'upload_only', dict(config, manual_request=True),
                                  'http://localhost:9380')
    primary = next(task for task in upload_only if task.role == 'paper_primary')
    assert primary.dataset_id == 'manual-md'
    assert not primary.parse_required
    assert primary.priority == 10
    assert all(not task.parse_required for task in upload_only)
    assert all(task.dataset_id in ('manual-md', 'manual-img') for task in upload_only)
    parsed = workbench_specs(store, 'batch-1', [item['id']], None,
                             'upload_and_parse', config, 'http://localhost:9380')
    main = next(task for task in parsed if task.role == 'paper_primary')
    assert main.dataset_id == 'finance-md'
    assert main.pipeline_id == 'finance-pipe'
    assert main.parse_required
    assert sum(task.parse_required for task in parsed) == 1
    assert store.loads == 2
    assert item['paper_id'] in main.remote_name
    assert main.content_sha256[:16] in main.remote_name
    assert item['id'] in main.remote_name
    assert len(main.remote_name.encode('utf-8')) <= 255
    assert main.handoff_id==item['id']
    assert main.package_path==Path(item['package_path'])
    assert main.package_sha256==item['reviewed_package_hash']


def test_workbench_rejects_changed_checked_content(tmp_path):
    store, item, config = prepared(tmp_path)
    item['reviewed_hash'] = '0'*64
    with pytest.raises(ValueError):
        workbench_specs(store, 'batch-1', [item['id']], None,
                        'upload_and_parse', config, 'http://localhost:9380')


def test_finance_admission_distinguishes_quality_and_technical():
    task = {'source_id': 'paper_' + 'a'*24}
    def remote(decision, run='DONE'):
        return {'run': run, 'meta_fields': {'accounting_paper_id': task['source_id'],
                'accounting_admission': decision}}
    assert classify_finance(task, remote('入库')) == Outcome.accepted
    assert classify_finance(task, remote('暂缓')) == Outcome.quality_hold
    assert classify_finance(task, remote('剔除')) == Outcome.quality_reject
    assert classify_finance(task, {'run': 'FAIL',
        'progress_msg': '0 tasks are ahead in the queue...'}) == Outcome.outcome_unresolved
    assert classify_finance(task, remote('入库', 'FAIL')) == Outcome.technical_review
    assert classify_finance(dict(task,handoff_id='AUTO_legacy'),{
        'run':'DONE','meta_fields':{'accounting_paper_id':'AUTO_legacy',
                                  'accounting_admission':'入库'}})==Outcome.accepted


def test_shared_education_route_uses_verified_pipeline_when_dataset_default_is_empty(tmp_path):
    from test_ingest_education import prepared as education_prepared
    from ingest_education import education_specs
    item,package,deployment=education_prepared(tmp_path)
    item.update(major='小学教育',automatic_approval=True,path=str(package/'ingest.md'),
                package_path=str(package))
    store=Store({'run_id':'batch-1','items':[item]})
    config={'datasets':[{'id':'education-md','name':'教育 md','pipeline_id':None},
                        {'id':'education-images','name':'教育图片'}],
            'dataset_map':{},'category_dataset_map':{'教育教学类':{
                'paper':'education-md','image':'education-images'}},
            'dataset_pipeline_map':{'education-md':'education-pipeline'}}
    tasks=workbench_specs(store,'batch-1',[item['id']],None,'upload_and_parse',
                          config,'http://localhost:9380')
    direct=education_specs(item,deployment,package,'http://localhost:9380','batch-1')
    assert {(r.role,r.channel_key,r.content_sha256,r.dataset_id) for r in tasks}=={
        (r.role,r.channel_key,r.content_sha256,r.dataset_id) for r in direct}
    primary=next(r for r in tasks if r.role=='paper_primary')
    assert primary.pipeline_id=='education-pipeline'

def test_routed_official_identity_is_reused_with_legacy_receipt(tmp_path):
    from test_ingest_education import prepared as edu_prepared
    from ingest_education import education_specs
    from ingest_queue import QueueStore
    from ingest_workbench import enqueue_workbench_items
    from upload_tracking import service_key
    item,package,dep=edu_prepared(tmp_path)
    item.update(major='小学教育',automatic_approval=True,path=str(package/'ingest.md'),package_path=str(package))
    store=Store({'run_id':'batch-1','items':[item]})
    config={'datasets':[{'id':'education-md','name':'教育 md'},{'id':'education-images','name':'教育图片'}],
            'dataset_map':{},'category_dataset_map':{'教育教学类':{'paper':'education-md','image':'education-images'}},
            'dataset_pipeline_map':{'education-md':'education-pipeline'}}
    q=QueueStore(tmp_path/'q.sqlite3');spec=education_specs(item,dep,package,'http://localhost:9380','batch-1')[0]
    task_id=q.enqueue(spec,{'status':'parse_waiting','remote_id':'r1'})
    route={'service_key':service_key(spec.service_url),'dataset_id':spec.dataset_id,'source_pipeline_id':spec.pipeline_id,
           'pipeline_id':'official-pipe','channel_key':'official','model_id':'official-model','provider':'DeepSeek','instance':'ds','contract_version':'v1'}
    q.claim_routed_parse('w',100,120,route,8,2,{})
    class Index:
        def matches(self,*args): return [{'record':{'route':'paper','status':'uploaded','document_id':'r1','remote_name':spec.remote_name,'content_hash':spec.content_sha256}}]
    config['_legacy_index']=Index();config['_receipt_lookup']=lambda *args:[{'id':'r1','name':spec.remote_name,'run':'RUNNING','pipeline_id':'official-pipe'}]
    ids=enqueue_workbench_items(store,'batch-1',[item['id']],None,'upload_and_parse',q,config,spec.service_url)
    assert task_id in ids
    assert q.get(task_id)['status']=='parse_intent'
    assert q.get(task_id)['pipeline_id']=='official-pipe'
