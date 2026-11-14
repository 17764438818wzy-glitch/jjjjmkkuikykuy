"""Audit and adopt a stopped legacy education batch without remote writes."""
from __future__ import annotations
import hashlib
import json
import msvcrt
import re
from contextlib import ExitStack
from dataclasses import asdict, replace
from pathlib import Path
from engine import digest
from evidence import package_fingerprint
from education_stream_ingest import education_remote_name, write_json
from ingest_education import education_specs, classify_education
from ingest_contracts import Outcome
from upload_tracking import service_key


def legacy_locked(path: Path) -> bool:
    if not path.exists(): return False
    with path.open('r+b') as stream:
        try: msvcrt.locking(stream.fileno(),msvcrt.LK_NBLCK,1)
        except OSError: return True
        msvcrt.locking(stream.fileno(),msvcrt.LK_UNLCK,1)
    return False


def inventory(client,dataset):
    rows=[]
    for page in range(1,1001):
        docs=client.request('GET',f'/datasets/{dataset}/documents?page={page}&page_size=100').get('docs')
        if not isinstance(docs,list): raise ValueError('Invalid remote inventory')
        rows.extend(docs)
        if len(docs)<100: return rows
    raise ValueError('Incomplete remote inventory')


def _paths(data,run):
    if not re.fullmatch(r'[A-Za-z0-9_-]+',run): raise ValueError('Invalid batch ID')
    data=Path(data)
    return data/'runs'/run, data/'private'/'education_full_ingest'/run


def audit_batch(data_root: Path,run_id: str,client,queue) -> dict:
    batch_dir,legacy=_paths(data_root,run_id)
    batch=json.loads((batch_dir/'manifest.json').read_text(encoding='utf-8'))
    if batch.get('run_id')!=run_id: raise ValueError('Batch identity mismatch')
    receipts_path=legacy/'receipts.json'
    receipts=json.loads(receipts_path.read_text(encoding='utf-8'))
    dep=json.loads((Path(data_root)/'private/education_pilot/deployment.json').read_text(encoding='utf-8'))
    if dep['paper_dataset_id']=='01885874b4e311f1807c3d9794ef0978': raise ValueError('Finance destination forbidden')
    allowed={dep.get(k) for k in ('pipeline_id','full_ingest_pipeline_id','arrearage_retry_pipeline_id')}|{None}
    dual_state={}
    dual=Path(data_root)/'private/education_dual_channel/deployment.json'
    if dual.exists():
        dual_state=json.loads(dual.read_text(encoding='utf-8')); allowed.add(dual_state.get('official_pipeline_id'))
    verified=Path(data_root)/'private/education_dual_channel/verified_legacy_pipelines.json'
    if verified.exists():
        for row in json.loads(verified.read_text(encoding='utf-8')).get('pipelines',[]):
            if row.get('id')=='89ea3542b52a11f1b21a85c6855e5155' or not re.fullmatch(r'[0-9a-f]{64}',row.get('dsl_sha256','')):
                raise ValueError('Invalid legacy Pipeline verification')
            allowed.add(row['id'])
    effective={}
    from education_dual_deploy import NODE,SCHOOL_MODEL,OFFICIAL_MODEL
    for pipeline in allowed-{None}:
        definition=client.request('GET','/agents/'+pipeline)
        model=((((definition.get('dsl') or {}).get('components') or {}).get(NODE) or {}).get('obj') or {}).get('params',{}).get('llm_id')
        if model==SCHOOL_MODEL: channel='education-school-deepseek'
        elif model==OFFICIAL_MODEL: channel='education-official-deepseek'
        else: channel='legacy-education:'+pipeline
        effective[pipeline]={'execution_pipeline_id':pipeline,'execution_channel_key':channel,
                            'execution_contract_version':'legacy-import:unknown-execution-version'}
    remote={kind:inventory(client,dep[key]) for kind,key in [('paper','paper_dataset_id'),('image','image_dataset_id')]}
    items={row.get('paper_id'):row for row in batch.get('items',[]) if row.get('status')=='pass' and row.get('category')=='教育教学类'}
    entries=[]; conflicts=[]; file_checks=[]
    for paper_id,receipt in receipts.get('primary',{}).items():
        try:
            item=items[paper_id]
            source=Path(item['package_path'])
            if digest(source/'ingest.md')!=item['reviewed_hash'] or package_fingerprint(source)!=item['reviewed_package_hash']:
                raise ValueError('Source approval changed')
            canonical=legacy/'canonical_packages'/item['id']
            specs=education_specs(item,dep,canonical,client.base_url,run_id)
            primary=specs[0]
            if receipt.get('document_id')!=item['id'] or receipt.get('upload_name')!=education_remote_name(canonical/'ingest.md'):
                raise ValueError('Legacy primary identity mismatch')
            for spec in specs:
                prior=receipt if spec.role=='paper_primary' else receipts.get('images',{}).get(paper_id+':'+spec.role.split(':',1)[1])
                if not prior:
                    legacy_name=paper_id+'_'+Path(spec.path).name
                    if any(doc.get('name') in (legacy_name,spec.remote_name) for doc in remote['image']):
                        raise ValueError('Unreceipted image write requires reconciliation')
                    entries.append({'spec':spec,'initial':{'status':'upload_waiting'},'existing_id':None})
                    file_checks.append([paper_id,spec.role,spec.content_sha256,spec.package_sha256,'not_uploaded'])
                    continue
                name=prior.get('upload_name') if spec.role=='paper_primary' else prior.get('name')
                rid=prior.get('ragflow_document_id')
                candidates=[doc for doc in remote['paper' if spec.role=='paper_primary' else 'image'] if doc.get('name')==name]
                if not rid or len(candidates)!=1 or candidates[0].get('id')!=rid:
                    raise ValueError('Remote receipt is missing or ambiguous')
                doc=candidates[0]
                if spec.role!='paper_primary' and prior.get('sha256')!=spec.content_sha256:
                    raise ValueError('Legacy asset hash mismatch')
                if spec.role=='paper_primary' and doc.get('pipeline_id') not in allowed:
                    raise ValueError('Remote Pipeline is outside education allowlist')
                if spec.role=='paper_primary' and (doc.get('meta_fields') or {}).get('education_paper_id') not in (None,paper_id):
                    raise ValueError('Remote paper identity differs')
                spec=replace(spec,remote_name=name)
                existing=queue.find_identity(client.base_url,spec.dataset_id,paper_id,spec.role)
                if existing and (existing.get('remote_id') not in (None,rid) or existing['content_sha256']!=spec.content_sha256 or existing['remote_name']!=name):
                    raise ValueError('Existing queue receipt differs')
                if spec.role!='paper_primary': status='uploaded'
                elif doc.get('run') in ('DONE','FAIL'): status=classify_education({'source_id':paper_id},doc).value
                elif doc.get('run')=='RUNNING': status='running'
                elif prior.get('parse_requested_at'): status='parse_unknown'
                else: status='parse_waiting'
                initial={'status':status,'remote_id':rid,'remote_run':doc.get('run'),'execution_previous_pipeline_id':doc.get('pipeline_id')}
                if spec.role=='paper_primary' and doc.get('pipeline_id'):
                    initial.update(effective[doc['pipeline_id']])
                if status=='accepted': initial['post_status']='pending'
                if existing and existing['status'] in ('upload_intent','parse_intent','upload_unknown','parse_unknown','submitted','running'):
                    if doc.get('run') not in ('DONE','FAIL'): initial['status']=existing['status']
                entries.append({'spec':spec,'initial':initial,'existing_id':existing['id'] if existing else None})
                file_checks.append([paper_id,spec.role,spec.content_sha256,spec.package_sha256,name,rid,doc.get('run'),doc.get('pipeline_id'),status])
        except (KeyError,ValueError,OSError) as exc:
            conflicts.append({'paper_id':paper_id,'kind':type(exc).__name__,'reason':str(exc)[:180]})
    signature=hashlib.sha256(json.dumps({'receipts':digest(receipts_path),'files':file_checks,'conflicts':conflicts},sort_keys=True).encode()).hexdigest()
    return {'run_id':run_id,'service_key':service_key(client.base_url),'queue':str(queue.path.resolve()),'legacy_worker_running':legacy_locked(legacy/'worker.lock'),'fingerprint':signature,'entries':entries,'conflicts':conflicts}


def _adopt_locked(data_root: Path,run_id: str,client,queue,audit: dict) -> dict:
    _,legacy=_paths(data_root,run_id)
    marker_path=legacy/'queue_owner.json'
    if marker_path.exists():
        owner=json.loads(marker_path.read_text(encoding='utf-8'))
        if (owner.get('queue')!=str(queue.path.resolve()) or owner.get('run_id')!=run_id
                or owner.get('service_key')!=service_key(client.base_url)):
            raise ValueError('Existing batch owner requires coordinated reconciliation')
    fresh=audit_batch(data_root,run_id,client,queue)
    if fresh['fingerprint']!=audit['fingerprint']: raise ValueError('Adoption audit changed')
    if fresh['conflicts']: raise ValueError('Adoption audit has unresolved conflicts')
    ids=[]
    for entry in fresh['entries']:
        spec,initial=entry['spec'],entry['initial']
        task_id=queue.enqueue(spec,initial_state=initial if initial['status']!='upload_waiting' else None)
        existing=queue.get(task_id)
        # Existing upload-only tasks may have been queued before the legacy ownership audit.
        if entry['existing_id'] and existing['status'] in ('upload_waiting','technical_review','outcome_unresolved'):
            updates={key:value for key,value in initial.items() if key!='status'}
            queue.import_verified_receipt(task_id,existing['status'],initial['status'],updates)
        ids.append(task_id)
    marker={'version':1,'run_id':run_id,'service_key':fresh['service_key'],'queue':fresh['queue'],'audit_fingerprint':fresh['fingerprint'],'task_ids':ids}
    write_json(legacy/'queue_owner.json',marker)
    return {'tasks':len(ids),'states':queue.summary(),'owner':str(legacy/'queue_owner.json')}


def adopt_batch(data_root: Path,run_id: str,client,queue,audit: dict) -> dict:
    _,legacy=_paths(data_root,run_id)
    locks=[legacy/'worker.lock',queue.path.with_suffix(queue.path.suffix+'.worker.lock')]
    if any(legacy_locked(path) for path in locks): raise ValueError('Ingest worker must stop before adoption')
    with ExitStack() as stack:
        for path in locks:
            path.parent.mkdir(parents=True,exist_ok=True)
            stream=stack.enter_context(path.open('a+b'))
            if path.stat().st_size==0: stream.write(b'0');stream.flush()
            stream.seek(0)
            try: msvcrt.locking(stream.fileno(),msvcrt.LK_NBLCK,1)
            except OSError: raise ValueError('Ingest worker restarted during adoption') from None
        return _adopt_locked(data_root,run_id,client,queue,audit)
