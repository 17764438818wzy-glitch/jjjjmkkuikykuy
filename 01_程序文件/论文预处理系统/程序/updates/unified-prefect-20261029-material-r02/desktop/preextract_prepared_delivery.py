"""Prepared ownership around the existing sealed-package queue and publisher."""
import copy
import json
from pathlib import Path
import shutil
from urllib.parse import quote

from preextract_material import file_sha,write_json
from paperflow.prepared_protocol import fingerprint


def register_prepared(data,registry,processor):
    from filelock import FileLock
    from preextract_publish import _paperflow
    from preextract_delivery_queue import DeliveryQueue,queue_path,package_reference
    task=registry.store.get(data['task_id'])
    if task.get('prepared_delivery_id'):return dict(delivery_id=task['prepared_delivery_id'])
    observation=registry.observation(data['task_id']);work=Path(observation.evidence['work_dir'])
    registry.assert_running(data['scope_id'],data['generation'])
    with FileLock(str(work/'operation.lock'),timeout=0):
        prepared=json.loads((work/'private/prepared-publication.json').read_text(encoding='utf8'))
        package=_paperflow(prepared['paperflow_root'])
        packet=package.validate_package(prepared['package_path'],prepared['task'],prepared['catalog'],strict_seal=True)
        if packet['manifest_sha256']!=prepared['validation']['manifest_sha256']:raise ValueError('sealed publication identity changed')
        run_id='PREPARED_'+fingerprint(dict(scope=data['scope_id'],task=data['task_id']))[:28]
        item_id=data['task_id'];run=processor.store.runs/run_id
        published=run/'packages'/item_id
        if published.exists():
            existing=package.validate_package(published,prepared['task'],prepared['catalog'],strict_seal=True)
            if existing['manifest_sha256']!=packet['manifest_sha256']:raise ValueError('retained publication differs')
        else:
            published.parent.mkdir(parents=True,exist_ok=True)
            shutil.copytree(prepared['package_path'],published)
            if package.validate_package(published,prepared['task'],prepared['catalog'],strict_seal=True)['manifest_sha256']!=packet['manifest_sha256']:
                raise ValueError('publication copy differs')
        marker=dict(schema='prepared-scope-binding/1',database=str(registry.store.path),scope_id=data['scope_id'],
            generation=data['generation'],task_id=data['task_id'])
        write_json(work/'prepared-scope.json',marker)
        from archive_identity import member_fields
        record=next(m['record'] for m in registry.members(data['scope_id']) if m['task_id']==data['task_id'])
        item=dict(id=item_id,title=packet['profile']['article']['title'],status='pass',automatic_approval=True,
            source_path=record['path'],**member_fields(record),
            content_format='paper_preextract',source_hash=prepared['task']['source_sha256'],package_path=str(published),
            preextract_work_dir=str(work),preextract_routes=packet['routes'],prepared_scope=marker)
        manifest=dict(schema_version=2,run_id=run_id,pipeline='automatic',operation='prepared_scope',
            status='complete',source_count=1,processed_sources=1,items=[item],events=[])
        processor.store.save(manifest)
        settings=json.loads((processor.store.root/'settings.json').read_text(encoding='utf8'))
        payload=package_reference(processor.store,run_id,item_id,settings['ragflow_url'])
        queue=DeliveryQueue(queue_path(processor.store.root))
        queue.enqueue(payload,workspace_locked=True)
        queue.bind_prepared(payload['id'],marker)
        registry.store.update(data['task_id'],prepared_delivery_id=payload['id'],prepared_run_id=run_id,
            prepared_delivery_identity=dict(task_id=data['task_id'],scope_id=data['scope_id'],
                manifest_sha256=packet['manifest_sha256'],delivery_id=payload['id']))
        return dict(delivery_id=payload['id'])


class PreparedActions:
    def __init__(self,client,registry,data,identity):
        self.client,self.registry,self.data,self.identity=client,registry,data,identity

    def run(self,kind,resource,fn,*,total_timeout):
        result=[]
        def invoke(cancel):
            self.registry.assert_running(self.data['scope_id'],self.data['generation'])
            if self.client.local_cancel.is_set():raise InterruptedError('scope stopped before RAG request')
            result.append(fn(cancel))
            return dict(result_saved_locally=True)
        self.client.perform(kind,dict(resource,identity=self.identity),invoke,total_timeout=total_timeout)
        return result[0]


def deliver_prepared(data,registry,client,processor):
    from preextract_delivery_queue import DeliveryQueue,queue_path
    from preextract_delivery_worker import deliver_job
    from paperflow.delivery_control import ControlledRagClient
    from ragflow_client import RagflowClient
    from auto_config import load_key
    root=processor.store.root;task=registry.store.get(data['task_id'])
    queue=DeliveryQueue(queue_path(root));job=queue.get(task['prepared_delivery_id'])
    marker=job['payload']['item']['prepared_scope']
    if marker['scope_id']!=data['scope_id'] or marker['task_id']!=data['task_id']:raise ValueError('prepared delivery scope changed')
    marker=dict(marker,generation=data['generation']);queue.bind_prepared(job['id'],marker)
    work=Path(job['payload']['item']['preextract_work_dir']);write_json(work/'prepared-scope.json',marker)
    settings=json.loads((root/'settings.json').read_text(encoding='utf8'))
    native=RagflowClient(settings['ragflow_url'],load_key(root,'ragflow'))
    class Client(ControlledRagClient):
        def upload_file(self,parent,name,content):
            return self.call('rag_write','POST','/files/upload',content,lambda:self.native.upload_file(parent,name,content))
        def download_file(self,file_id):
            return self.call('rag_read','GET','/files/'+quote(file_id,safe=''),None,lambda:self.native.download_file(file_id))
    controlled=Client(native,PreparedActions(client,registry,data,task['prepared_delivery_identity']))
    controlled.prepared_scope=marker
    queue.update(job['id'],status='uploading')
    deliver_job(queue,queue.get(job['id']),controlled,client.local_cancel)
    return dict(delivery_id=job['id'],status=queue.get(job['id'])['status'])


def cleanup_prepared(data,registry,processor,stop):
    from preextract_cleanup import cleanup_queue_once
    from preextract_delivery_queue import DeliveryQueue,queue_path
    from preextract_scope_guard import member_delivery_proof
    from paperflow.scheduling_policy import ActionDeferred
    registry.assert_running(data['scope_id'],data['generation'])
    task=registry.store.get(data['task_id']);queue=DeliveryQueue(queue_path(processor.store.root))
    job=queue.get(task['prepared_delivery_id'])
    member=next(m for m in registry.members(data['scope_id']) if m['task_id']==data['task_id'])
    if job['status']!='complete' or not member_delivery_proof(registry,data['scope_id'],member):
        raise ActionDeferred('member_delivery_not_verified')
    cleanup_queue_once(processor.store.root,stop=stop,force=True,delivery_ids=[job['id']])
    projection=queue.get(job['id'])['projection']
    from preextract_cleanup import policy
    report=(projection.get('preextract_cleanup') if policy(processor.store.root).get('preserve_results',True)
            else projection.get('preextract_retirement') or projection.get('preextract_cleanup')) or {}
    if report.get('status')!='complete':raise ActionDeferred(report.get('reason') or 'cleanup_pending')
    return dict(status=report['status'],reason=report.get('reason'))
