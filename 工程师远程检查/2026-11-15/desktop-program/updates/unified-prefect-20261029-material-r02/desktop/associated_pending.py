"""Late attachment adapter around the existing queue and original-file executor.

No model, parser, body publication or cleanup is dispatched here. The original
publication remains sealed; attachment plans and receipts live in a sidecar.
"""
from __future__ import annotations
import copy
import hashlib
import json
from pathlib import Path
from filelock import FileLock,Timeout
from archive_identity import record_key,member_fields
from engine import digest
from preextract_publish import _write_json
from upload_tracking import service_key

KIND='associated_original'
def _load(path):return json.loads(Path(path).read_text(encoding='utf8'))
def _hash(value):return hashlib.sha256(json.dumps(value,sort_keys=True,ensure_ascii=False).encode()).hexdigest()
def _record(item):
    return dict(path=item['source_path'],name=Path(item['source_path']).name,**member_fields(item))
def _stopped(stop):return stop is not None and stop.is_set()
def _has_remote_intent(job):
    work=Path(job['payload']['item']['preextract_work_dir'])
    return any((work/name).exists() for name in
        ('delivery-binding.json','delivery-receipt.json','associated-delivery-receipt.json'))

def _status(queue,job,state,reason,*,expected_status=None,dispatch_scope=None):
    if dispatch_scope is None and not (job.get('delivery_authority') or {}).get('references'):return False
    if state in {'queued','waiting_parent'} and _has_remote_intent(job):
        state='needs_review'
        reason='已有附件交付意图或回执，保留原证据待明确核对；未自动重排'
    projection={'associated_delivery':dict(status=state,reason=reason,service=job['service'],
        parse_status=job['payload']['material'].get('parse_status','pending'))}
    if dispatch_scope is not None:
        return queue.update_scoped(job['id'],dispatch_scope=dispatch_scope,status=state,
            expected_status=expected_status,projection=projection)
    # Claimed jobs may settle received results after a pause. Refreshers must
    # always supply scope and use the atomic guarded update.
    return queue.update(job['id'],status=state,expected_status=expected_status,projection=projection)

def _input_authority(queue,run_id,item,service,dispatch_scope):
    from purpose_review import project_key
    from preextract_task_delivery import capture_delivery_authority,delivery_authority_open
    probe=dict(service=service_key(service),run_id=run_id,
        task=dict(source_sha256=item['source_hash']),item=dict(id=item['id'],source_hash=item['source_hash']))
    authority=capture_delivery_authority(queue.path,probe,dispatch_scope=dispatch_scope)
    if not authority['references']:
        record=_record(item)
        ident=_hash(dict(service=service_key(service),source_key=record_key(record),
            source_sha256=item['source_hash'],project_key=project_key(record),kind=KIND))
        authority=capture_delivery_authority(queue.path,probe,delivery_id=ident,dispatch_scope=dispatch_scope)
    authority=dict(authority,references=[reference for reference in authority['references'] if
        reference['run_id']==run_id and reference['item_id']==item['id'] and
        reference['source_hash']==item['source_hash']])
    if not authority['references']:return None
    return authority if delivery_authority_open(queue.path,authority) else None

def enqueue_saved(store,run_id,items,service,*,stop=None,retry=False,dispatch_scope=None):
    if not dispatch_scope or not dispatch_scope.get('scope_ids'):return []
    from preextract_delivery_queue import DeliveryQueue,queue_path
    from preextract_task_delivery import delivery_authority_open
    from auto_ragflow import _save_handoff_item
    queue=None;registered=[]
    for item in items:
        if _stopped(stop):break
        info=item.get('associated_material') or {}
        if item.get('content_format')!='associated_material' or not info or info.get('purpose_pending'):continue
        authority=None
        try:
            queue=queue or DeliveryQueue(queue_path(store.root))
            authority=_input_authority(queue,run_id,item,service,dispatch_scope)
            if authority is None:continue
            ident=_enqueue_one(queue,store,run_id,item,info,service,retry,authority)
            if ident is not None:registered.append(ident)
        except Exception as exc:
            # A corrupt or busy item must not prevent unrelated materials or
            # the normal body handoff from being registered.
            if authority is None or not delivery_authority_open(queue.path,authority):continue
            state='waiting_parent' if isinstance(exc,Timeout) else 'needs_review'
            _save_handoff_item(store,run_id,dict(id=item['id'],associated_delivery=dict(
                status=state,reason='附件登记暂无法核对，原件保留',service=service_key(service),
                parse_status=info.get('parse_status','pending'))))
    if queue is not None:refresh_waiting(queue,service,stop=stop,dispatch_scope=dispatch_scope)
    return registered


def _enqueue_one(queue,store,run_id,item,info,service,retry,authority):
    from purpose_review import project_key
    from preextract_task_delivery import delivery_authority_open
    if (info.get('source_sha256') or info['sha256'])!=item['source_hash']:
        raise ValueError('附件原始来源与保存记录不一致')
    record=_record(item)
    identity=dict(service=service_key(service),source_key=record_key(record),
        source_sha256=item['source_hash'],project_key=project_key(record),kind=KIND)
    ident=_hash(identity)
    if not delivery_authority_open(queue.path,authority):return None
    work=Path(store.root)/'private/associated_pending'/ident
    work.mkdir(parents=True,exist_ok=True)
    with FileLock(str(work/'operation.lock'),timeout=0):
        manifest=work/'manifest.json'
        descriptor=dict(identity=identity,record=record,material=info)
        if manifest.exists():
            prior=_load(manifest)
            if prior['identity']!=identity:raise ValueError('附件保存身份变化')
            # Preserve the first retained copy and the first remote intent.
            descriptor=prior
        else:_write_json(manifest,descriptor)
        payload=dict(id=ident,kind=KIND,service=identity['service'],store_root=str(store.root),run_id=run_id,
            task=dict(source_sha256=identity['source_sha256']),manifest_sha256=digest(manifest),
            record=descriptor['record'],material=descriptor['material'],project_key=identity['project_key'],
            item=dict(id=item['id'],source_hash=identity['source_sha256'],content_format='associated_material',
                package_path=str(work),preextract_work_dir=str(work)))
        if not delivery_authority_open(queue.path,authority):return None
        queue.enqueue(payload,retry=retry,workspace_locked=True)
    return ident

def _body_candidates(history,payload):
    from purpose_review import project_key,cached_role
    rows={}
    for manifest in history:
        for item in manifest.get('items',[]):
            if item.get('content_format')!='paper_preextract' or not item.get('source_path'):continue
            record=_record(item)
            if project_key(record)!=payload['project_key']:continue
            role=item.get('intake_role') or {}
            if role and (not role.get('confirmed') or role.get('action')!='review' or role.get('kind')!='document'):
                continue
            # A failed sibling body still prevents a guess between two papers.
            if item.get('source_hash'):
                rows.setdefault(item['source_hash'],[]).append((manifest['run_id'],item))
    # Purpose review can finish before the body has a processing result. Its
    # confirmed identity must still participate in ambiguity/parent selection.
    info=payload['material'];config=info.get('purpose_config') or {}
    for record in info.get('purpose_context',[]):
        if project_key(record)!=payload['project_key']:continue
        role=cached_role(record,config,stable=True)
        if role and role.get('confirmed') and role.get('kind')=='document' and role.get('action')=='review':
            source=Path(record['path']);sha=role.get('evidence_sha256')
            if not sha or not source.is_file() or digest(source)!=sha:
                raise ValueError('已确认正文来源发生变化')
            rows.setdefault(sha,[])
    return rows

def _unresolved(history,payload):
    from purpose_review import project_key,cached_role,DOCUMENTS,IMAGES
    confirmed=set();unread=[]
    for manifest in history:
        for item in manifest.get('items',[]):
            if not item.get('source_path') or project_key(_record(item))!=payload['project_key']:continue
            info=item.get('associated_material') or {};role=item.get('intake_role') or {}
            key=(record_key(_record(item)),item.get('source_hash'))
            if item.get('status') in {'pass','deleted'} or (info.get('kind') in {'drawing','source_code','image','supporting_document'} and not info.get('purpose_pending')):
                confirmed.add(key)
            elif (item.get('content_format')=='purpose_pending' or info.get('purpose_pending') or
                  (info.get('kind')=='unknown' and Path(item['source_path']).suffix.lower() in DOCUMENTS|IMAGES)):
                unread.append(key)
    if any(key not in confirmed for key in unread):return True
    config=payload['material'].get('purpose_config') or {}
    for record in payload['material'].get('purpose_context',[]):
        if Path(record['path']).suffix.lower() not in DOCUMENTS|IMAGES:continue
        role=cached_role(record,config,stable=True)
        if not role or not role.get('confirmed') or role.get('action')=='pending':
            # Completed bodies retain their purpose in the verified delivery
            # identity after the disposable purpose answer has been removed.
            source=Path(record['path']);retired=None
            if payload.get('store_root') and project_key(record)==payload['project_key'] and source.is_file():
                from preextract_retirement import find_terminal,parent_evidence
                for manifest in history:
                    for item in manifest.get('items',[]):
                        if (item.get('content_format')!='paper_preextract' or not item.get('source_path')
                                or not item.get('preextract_work_dir') or record_key(_record(item))!=record_key(record)
                                or item.get('source_hash')!=digest(source)):continue
                        job=find_terminal(payload['store_root'],work=item['preextract_work_dir'],
                            source_hash=item['source_hash'],service=payload['service'])
                        if job:
                            try:retired=parent_evidence(job)
                            except (ValueError,KeyError,TypeError):pass
            if retired is None:return True
    return False

def _parent(history,payload):
    from purpose_review import preferred_body_sha
    if history is None:
        from handoff_index import project_history
        history=project_history(payload['store_root'],payload['project_key'])
        if history is None:return None,'项目交付索引正在更新，附件保留等待'
    if _unresolved(history,payload):return None,'项目内仍有用途未确认材料，保留附件等待关联'
    candidates=_body_candidates(history,payload)
    selected=preferred_body_sha(payload['material'].get('purpose_config') or {},payload['project_key'],set(candidates))
    if selected:candidates={selected:candidates[selected]}
    if len(candidates)!=1:return None,'存在多份候选正文，待确认关联' if candidates else '附件已保存，等待同项目正文'
    matches=[]
    for run_id,item in next(iter(candidates.values())):
        if item.get('status')!='pass' or not item.get('preextract_work_dir'):continue
        work=Path(item['preextract_work_dir'])
        try:
            source=Path(item['source_path'])
            if source.exists() and digest(source)!=item['source_hash']:continue
            from preextract_retirement import find_terminal,parent_evidence
            retired=find_terminal(payload['store_root'],work=work,source_hash=item['source_hash'],service=payload['service']) if payload.get('store_root') else None
            if retired:
                matches.append(dict(parent_evidence(retired),record=_record(item)))
                continue
            prepared=_load(work/'private/prepared-publication.json')
            receipt=_load(work/'delivery-receipt.json');binding=_load(work/'delivery-binding.json')
            manifest=prepared['validation']['manifest_sha256']
            routes=binding['routes'];packet=dict(routes=routes,records={})
            # Receipt target facts are independently checked here; no package or
            # complete body response is required after cache retirement.
            if (binding['service']!=payload['service'] or binding['task']!=prepared['task']
                    or binding['manifest_sha256']!=manifest
                    or prepared['task']['source_sha256']!=item['source_hash']
                    or receipt.get('task')!=prepared['task'] or receipt.get('manifest_sha256')!=manifest
                    or receipt.get('status')!='complete'):
                continue
            if not routes or any(not receipt.get('targets',{}).get(r['code'],{}).get('verified')
                    or not receipt['targets'][r['code']].get('retrieval_verified')
                    or receipt['targets'][r['code']].get('body',{}).get('dataset_id')!=r['paper_dataset_id']
                    or not receipt['targets'][r['code']].get('body',{}).get('document_id') for r in routes):continue
            native=None
            if prepared.get('associated_upload_plan_sha256'):
                native=_load(work/'associated-delivery-receipt.json')
                expected=dict(service=payload['service'],task=prepared['task'],manifest_sha256=manifest,
                    plan_sha256=prepared['associated_upload_plan_sha256'])
                if native.get('identity')!=expected or native.get('status')!='complete':continue
            identity=dict(service=payload['service'],task=prepared['task'],manifest_sha256=manifest,routes=routes,
                targets={r['code']:receipt['targets'][r['code']]['body']['document_id'] for r in routes})
            matches.append(dict(identity=identity,work_dir=str(work),prepared=dict(prepared,packet=packet),
                receipt=receipt,native=native,record=_record(item)))
        except (OSError,ValueError,KeyError,TypeError):continue
    unique={_hash(v['identity']):v for v in matches}
    if len(unique)!=1:return None,'等待正文及原附件入库核验' if not unique else '正文存在多个已交付版本，待确认关联'
    return next(iter(unique.values())),''

def _files(info,root):
    from associated_delivery import KINDS,_reviewed_leaf_allowed
    if info.get('purpose_pending'):raise ValueError('附件用途待确认')
    if info.get('kind')=='archive':
        result=[]
        for row in info.get('members',[]):
            if row.get('kind')=='administrative':continue
            result.extend(_files(row,root))
        return result
    if info.get('kind') not in KINDS or not _reviewed_leaf_allowed(info):
        raise ValueError('附件用途缺少同源核验依据')
    path=Path(info['original_path'])
    if (not path.resolve().is_relative_to(Path(root).resolve()) or not path.is_file()
            or digest(path)!=info['sha256']):raise ValueError('附件原件缺失、越界或变化')
    return [dict(file_ref='late_'+info['sha256'],kind=info['kind'],format=info['format'],sha256=info['sha256'],
        original_path=str(path),parse_status=info.get('parse_status','pending'),bytes=path.stat().st_size)]

def _bound_plan(payload,parent):
    work=Path(payload['item']['preextract_work_dir'])
    if digest(work/'manifest.json')!=payload['manifest_sha256']:raise ValueError('附件登记清单发生变化')
    files=_files(payload['material'],payload['store_root']);reuse={}
    for row in files:
        entries=[(ref,e) for ref,e in (parent.get('native') or {}).get('files',{}).items() if e.get('sha256')==row['sha256']]
        if len(entries)>1:raise ValueError('正文已有多份同哈希附件，待核对')
        if entries:
            ref,entry=entries[0]
            if entry.get('status')!='verified':raise ValueError('原生附件尚未核验')
            row['file_ref']=ref
            reuse[row['sha256']]=dict(entry,parent_id=entry.get('parent_id') or parent['native']['folder']['id'])
    identity=parent['identity']
    plan=dict(schema='associated-supplement-plan/1',task=identity['task'],manifest_sha256=identity['manifest_sha256'],
        routes=identity['routes'],files=files,parent_identity=identity)
    path=work/'attachment-plan.json'
    if path.exists():
        previous=_load(path)
        if previous!=plan:raise ValueError('附件已绑定的正文或原件发生变化，保留原回执')
    else:_write_json(path,plan)
    return plan,reuse,digest(path)

def refresh_waiting(queue,service,*,stop=None,dispatch_scope=None):
    if not dispatch_scope or not dispatch_scope.get('scope_ids'):return
    pending=[j for j in queue.scoped_list(service,status='waiting_parent',dispatch_scope=dispatch_scope)
        if j['payload'].get('kind')==KIND]
    if not pending or _stopped(stop):return
    from engine import Store
    history=None
    resolved={}
    for job in pending:
        if _stopped(stop):break
        try:
            payload=job['payload'];material=payload['material']
            context=_hash(dict(project_key=payload['project_key'],service=payload['service'],
                config=material.get('purpose_config'),records=material.get('purpose_context')))
            if context not in resolved:resolved[context]=_parent(history,payload)
            parent,reason=resolved[context]
            if parent:
                try:
                    with FileLock(str(Path(parent['work_dir'])/'operation.lock'),timeout=0):pass
                except Timeout:
                    _status(queue,job,'waiting_parent','正文正在交付或使用，附件等待关联',expected_status='waiting_parent',dispatch_scope=dispatch_scope)
                    continue
                _status(queue,job,'queued','附件已保存，正文已核验，排队关联',expected_status='waiting_parent',dispatch_scope=dispatch_scope)
            else:_status(queue,job,'waiting_parent',reason,expected_status='waiting_parent',dispatch_scope=dispatch_scope)
        except (OSError,ValueError,KeyError,TypeError):
            _status(queue,job,'needs_review','附件身份暂无法核对，原件保留',expected_status='waiting_parent',dispatch_scope=dispatch_scope)

def deliver_job(queue,job,client,stop):
    from engine import Store
    from associated_delivery import _deliver_plan
    from preextract_handoff import _StopAwareClient
    from preextract_delivery_control import QueueStop
    from purpose_review import project_key
    if not (job.get('delivery_authority') or {}).get('references'):return
    stop=QueueStop(queue,job,stop)
    payload=job['payload'];work=Path(payload['item']['preextract_work_dir'])
    try:
        if _stopped(stop):return _status(queue,job,'queued','已停止，附件保留待关联')
        if service_key(client.base_url)!=payload['service']:raise ValueError('附件投递服务变化')
        parent,reason=_parent(None,payload)
        if parent is None:return _status(queue,job,'waiting_parent',reason)
        # Use the parent's existing workflow lock across all attachment jobs,
        # body delivery, and cleanup; never race two reverse-list updates.
        held_parent_identity=copy.deepcopy(parent['identity'])
        held_parent_work=Path(parent['work_dir']).resolve()
        with FileLock(str(held_parent_work/'operation.lock'),timeout=0),FileLock(str(work/'operation.lock'),timeout=0):
            parent,reason=_parent(None,payload)
            if parent is None:return _status(queue,job,'waiting_parent',reason)
            if (parent['identity']!=held_parent_identity or
                    Path(parent['work_dir']).resolve()!=held_parent_work):
                raise ValueError('持锁期间父正文身份或工作目录改变，保留原附件待核对')
            if (parent['identity']['service']!=payload['service'] or
                    project_key(parent['record'])!=payload['project_key']):
                raise ValueError('附件与已核验父正文的服务或项目关系不同')
            if _stopped(stop):return _status(queue,job,'queued','已停止，附件保留待关联')
            plan,reuse,sha=_bound_plan(payload,parent)
            if not plan['files']:raise ValueError('没有已确认附件成员')
            prepared=dict(parent['prepared'],associated_upload_plan_sha256=sha)
            guarded=_StopAwareClient(client,stop) if stop is not None else client
            receipt=_deliver_plan(prepared,parent['receipt'],plan,work/'associated-delivery-receipt.json',guarded,
                stop=stop,append=True,reuse=reuse)
            queue.update(job['id'],status='complete',projection={'associated_delivery':dict(status='complete',
                reason='附件原件及正文关联已逐目标读回核验',service=payload['service'],receipt_path=str(work/'associated-delivery-receipt.json'),
                parse_status=payload['material'].get('parse_status','pending'),verified_count=receipt['verified_count'])})
    except Timeout:
        _status(queue,job,'waiting_parent','正文正在交付或使用，附件等待关联')
    except InterruptedError:
        _status(queue,job,'needs_review','已停止，保留附件交付意图及回执待继续核对')
    except Exception as exc:
        reason=str(exc).replace(getattr(client,'api_key','') or '\0','[密钥]')[:200]
        _status(queue,job,'needs_review',reason)

def project_item(item,payload,projection,state):
    work=Path(payload['item']['preextract_work_dir'])
    try:
        if (item.get('source_hash')!=payload['task']['source_sha256'] or digest(work/'manifest.json')!=payload['manifest_sha256']):
            raise ValueError('附件身份变化')
        summary=copy.deepcopy(projection.get('associated_delivery') or {})
        summary.update(status=state,service=payload['service'])
        summary.setdefault('reason','附件已保存，等待正文核验及关联')
        summary.setdefault('parse_status',payload['material'].get('parse_status','pending'))
    except (OSError,ValueError,KeyError):summary=dict(status='needs_review',reason='附件登记身份变化，待核对')
    item['associated_delivery']=summary


def project_dependencies(items,jobs):
    """Display pending project materials without changing body receipts."""
    from purpose_review import project_key
    grouped={}
    for job in jobs:
        payload=json.loads(job['payload'])
        grouped.setdefault(payload['project_key'],[]).append(job)
    for item in items:
        if item.get('content_format')!='paper_preextract' or not item.get('source_path'):continue
        related=grouped.get(project_key(_record(item)),[])
        item['associated_pending_counts']={service:sum(r['status']!='complete' for r in related if r['service']==service)
            for service in {r['service'] for r in related}}


def describe_material(item):
    info=item.get('associated_material') or {};delivery=item.get('associated_delivery') or {}
    state=delivery.get('status')
    associations={'complete':'关联已核验','queued':'待关联','uploading':'关联处理中',
                  'waiting_parent':'等待正文或关联确认','needs_review':'关联待核对'}
    return dict(purpose='用途待确认' if info.get('purpose_pending') else '附件用途已确认',
        saved='原件已保存' if info.get('original_path') else '原件待保存',
        parse='待解析' if info.get('parse_status','pending')=='pending' else '已有静态预览',
        upload='已上传' if state=='complete' else '上传及读回待确认' if state=='needs_review' else '待上传',
        association=associations.get(state,'待确认关联正文'),reason=delivery.get('reason') or info.get('association_basis',''))
