"""Adapt sealed preextract publications to the desktop's durable handoff records."""
from __future__ import annotations

import copy
import json
import threading
from pathlib import Path

from preextract_publish import _paperflow, _write_json, deliver_publication, reconcile_publication
from upload_tracking import service_key


def _load(path):
    return json.loads(Path(path).read_text(encoding='utf8'))


class _StopAwareClient:
    """Use the existing stop event before each publisher network operation."""
    def __init__(self,client,stop):
        self.client=client
        self.stop=stop

    def __getattr__(self,name):
        value=getattr(self.client,name)
        if name=='opener':return _StopAwareClient(value,self.stop)
        if name in {'request','upload_bytes','upload_file','download_file','download_bytes','download_image','open'}:
            def checked(*args,**kwargs):
                readback=name in {'download_file','download_bytes','download_image'}
                if name=='request':
                    method=args[0] if args else kwargs.get('method','')
                    readback=isinstance(method,str) and method.upper()=='GET'
                elif name=='open':
                    request=args[0] if args else kwargs.get('fullurl')
                    method=request.get_method() if hasattr(request,'get_method') else 'GET'
                    data=args[1] if len(args)>1 else kwargs.get('data')
                    readback=method=='GET' and data is None
                stopped=getattr(self.stop,'readback_is_set',self.stop.is_set) if readback else self.stop.is_set
                if stopped():raise InterruptedError('已停止新增交付；已有回执保留，未重发请求')
                return value(*args,**kwargs)
            return checked
        return value


class _ObservationClient:
    """An explicitly supplied service client gets no business-write authority."""
    def __init__(self,client):self.client=client
    def __getattr__(self,name):
        if name=='opener' or name in {'upload_bytes','upload_file','open'}:
            raise AttributeError(name)
        return getattr(self.client,name)
    def request(self,method,*args,**kwargs):
        if method!='GET':raise ValueError('read-only observation requires GET')
        return self.client.request(method,*args,**kwargs)
    def download_image(self,image_id):
        # Keep image GET and byte verification without exposing the raw transport.
        from paperflow.rag import RagPublisher
        return RagPublisher(self.client).download_image(image_id)


def _observe_projection(store,run_id,item_id,report,service):
    observation=dict(copy.deepcopy(report),schema='preextract-native-observation/1',service=service,
        reason_code='native_reconciliation_required')
    with store.lock:
        current=next(row for row in store.load(run_id)['items'] if row['id']==item_id)
    delivery=copy.deepcopy(current.get('preextract_delivery') or {})
    delivery.update(status='uncertain',body_status='uncertain',service=service,
        reason_code='native_reconciliation_required',observed_at=report.get('observed_at'))
    _save(store,run_id,item_id,dict(preextract_native_observation=observation,preextract_delivery=delivery,handoff_status='pending',
        handoff_reason='原生目标已只读核对；待明确准入后接续，尚未确认整篇交付',preextract_handoff_blocked=True))
    return observation


def _save(store, run_id, item_id, fields):
    with store.lock:
        manifest = store.load(run_id)
        item = next(row for row in manifest['items'] if row['id'] == item_id)
        for key, value in fields.items():
            if key == 'uploads':
                item.setdefault(key, {}).update(value)
            else:
                item[key] = value
        store.save(manifest)


def _receipt_complete(receipt, packet):
    if receipt.get('status') != 'complete':
        return False
    targets = receipt.get('targets', {})
    from paperflow.rag import RagPublisher
    roles = [role for role, records in RagPublisher(None).records(packet).items() if records]
    for route in packet['routes']:
        target = targets.get(route['code'], {})
        if not target.get('verified') or not target.get('retrieval_verified'):
            return False
        for role in roles:
            record = target.get(role, {})
            dataset = route['paper_dataset_id' if role == 'body' else 'image_dataset_id']
            if record.get('dataset_id') != dataset or not record.get('document_id'):
                return False
    return True


def _projection(receipt, packet, service, receipt_path, *, attachments_required=False, attachments=None):
    body_complete = _receipt_complete(receipt, packet)
    complete = body_complete and (not attachments_required or (attachments or {}).get('status')=='complete')
    uploads = {}
    for route in packet['routes']:
        target = receipt.get('targets', {}).get(route['code'], {})
        for role, kind in (('body', 'paper'), ('images', 'image')):
            record = target.get(role)
            if not isinstance(record, dict):
                continue
            dataset = record.get('dataset_id')
            if not dataset:
                continue
            uploads[service + '|' + dataset] = dict(
                status='uploaded' if record.get('document_id') else 'unknown', route=kind,
                dataset_id=dataset, dataset_name=route.get('name', route['code']),
                document_id=record.get('document_id'), remote_name=record.get('name'),
                source_id=receipt.get('task', {}).get('source_id'),
                manifest_sha256=packet['manifest_sha256'], specialty_code=route['code'],
                content_format='paper_preextract', parse_status='not_required',
                ragflow_status='done' if complete else 'unknown',
                chunk_count=len(record.get('chunks', {})),
                preextract_readback_verified=bool(target.get('verified')),
                preextract_retrieval_verified=bool(target.get('retrieval_verified')),
                preextract_delivery_status=receipt.get('status'),
                readback=copy.deepcopy(record))
    summary = {key: copy.deepcopy(receipt.get(key)) for key in
               ('schema', 'stage', 'manifest_sha256', 'task', 'targets', 'source_retained', 'cleanup', 'last_remote_verified_at')}
    summary.update(status='complete' if complete else 'uncertain', body_status='complete' if body_complete else 'uncertain', service=service,
                   receipt_path=str(receipt_path),
                   routes=[{key: route.get(key) for key in ('code', 'name', 'paper_dataset_id', 'image_dataset_id')}
                           for route in packet['routes']])
    result=dict(uploads=uploads, preextract_delivery=summary,
                handoff_status='uploaded' if complete else 'pending',
                handoff_reason='' if complete else '正文已入库，关联附件尚待确认' if body_complete else '投递尚未逐目标完成读回确认，保留回执等待核对')
    if attachments is not None:result['preextract_associated_delivery']=copy.deepcopy(attachments)
    return result


def describe_preextract_item(item, service):
    """Project explicit direct-publication evidence, independently of parser status."""
    service = service_key(service)
    records = [row for key, row in (item.get('uploads') or {}).items()
               if '|' in key and service_key(key.rsplit('|', 1)[0]) == service]
    delivery = item.get('preextract_delivery') or {}
    if delivery.get('service') != service:
        delivery = {}
    routes = delivery.get('routes') or item.get('preextract_routes') or []
    target_ids = list(dict.fromkeys(row['paper_dataset_id'] for row in routes if row.get('paper_dataset_id')))
    image_ids = list(dict.fromkeys(row['image_dataset_id'] for row in routes if row.get('image_dataset_id')))
    state = dict(target='、'.join(row.get('name') or row['code'] for row in routes) or 'MD 最终专业名单',
        image_target='、'.join(image_ids) or '按封包图片保存决定', image_target_id=image_ids[0] if image_ids else None,
        target_id=target_ids[0] if len(target_ids) == 1 else None, target_ids=target_ids,
        upload='待上传', process='尚未提交', reason=item.get('handoff_reason', ''),
        assets='尚无逐目标读回记录', record=records[0] if len(records) == 1 else None,
        records=records, receipt_owner=None,
        can_retry=not bool(delivery) and not item.get('preextract_handoff_blocked', False))
    cleanup=item.get('preextract_cleanup') or {}
    labels={'complete':'清理完成','pending':'等待继续清理','deferred':'清理暂缓','error':'清理待核对','disabled':'等待清理'}
    reasons={'another_delivery_still_uses_workspace':'仍有任务引用这些材料',
        'paper_or_cleanup_busy':'文件或任务正在使用，稍后重试',
        'waiting_for_legacy_process_exit':'等待旧版本任务退出',
        'all_specialty_readback_not_confirmed':'等待全部目标库核验通过'}
    state.update(cleanup=labels.get(cleanup.get('status'),'等待清理'),
        cleanup_reason=reasons.get(cleanup.get('reason'),cleanup.get('reason') or cleanup.get('error_type') or ''),
        cleanup_removed_bytes=cleanup.get('removed_bytes',0))
    queued=item.get('preextract_delivery_queue') or {}
    if queued.get('service')==service and queued.get('status')=='needs_review':
        state.update(upload='上传待确认',process='处理状态待确认',can_retry=False)
        return state
    if queued.get('service')==service and queued.get('status') in {'queued','uploading'}:
        active=queued['status']=='uploading'
        state.update(upload='上传中' if active else '排队待上传',
                     process='处理中' if active else '排队中',can_retry=False)
        return state
    if not delivery:
        if not state['reason']:
            state['reason'] = '已通过本地封包检查，按 MD 最终专业名单交付'
        return state
    targets = delivery.get('targets') or {}
    readback = sum(bool(targets.get(route['code'], {}).get('verified')) for route in routes)
    retrieval = sum(bool(targets.get(route['code'], {}).get('retrieval_verified')) for route in routes)
    complete = (item.get('handoff_status')=='uploaded' and delivery.get('status') == 'complete' and bool(routes)
                and readback == len(routes) and retrieval == len(routes))
    all_documents = bool(routes) and all(
        targets.get(route['code'], {}).get('body', {}).get('document_id') for route in routes)
    state.update(upload='已上传' if all_documents else '上传待确认',
        process='已完成' if complete else '处理状态待确认', can_retry=False,
        assets=f'内容及图片读回 {readback}/{len(routes)} 个专业；检索确认 {retrieval}/{len(routes)}')
    pending=(item.get('associated_pending_counts') or {}).get(service,0)
    if pending and body_complete_for_view(delivery,routes):
        state.update(process='正文已完成，附件待关联',
            reason=f'本项目仍有 {pending} 份附件等待关联或核验；正文入库结果保留')
    if complete:
        state['assets']+='；'+state['cleanup']
        if state['cleanup_reason']: state['assets']+='（'+state['cleanup_reason']+'）'
    if not complete and not state['reason']:
        state['reason'] = '存在未确认投递，需核对既有回执，未重复上传'
    return state


def body_complete_for_view(delivery,routes):
    return bool(routes) and delivery.get('body_status')=='complete'


def handoff_preextract_item(store,run_id,item_id,client,*,progress=lambda *args:None,stop=None,
                           reconcile=False,continue_delivery=False,expected_generations=None,observation_client=None,
                           dispatch_scope=None):
    from global_task_control import GlobalTaskControl,GlobalPaused
    observing=reconcile and not continue_delivery and observation_client is not None
    from preextract_delivery_control import QueueStop
    if not observing and dispatch_scope is None and not isinstance(stop,QueueStop):
        return dict(status='paused',reason_code='dispatch_scope_required')
    control=GlobalTaskControl(store.root)
    if not observing and not control.snapshot()['admission_open']:
        with store.lock:
            item=next(row for row in store.load(run_id)['items'] if row['id']==item_id)
            if item.get('handoff_status')=='uploaded' and item.get('preextract_delivery',{}).get('status')=='complete':return
    try:
        # Explicit GET-only observation is independent of business admission.
        # It never clears/unwraps a stopped client or launches a later stage.
        if reconcile and not continue_delivery and observation_client is not None:
            return _handoff_admitted(store,run_id,item_id,client,progress=progress,stop=stop,reconcile=True,
                observation_client=observation_client,dispatch_scope=dispatch_scope)
        with control.stage('delivery',batch_id=run_id,paper_id=item_id):
            return _handoff_admitted(store,run_id,item_id,client,progress=progress,stop=stop,reconcile=reconcile,
                continue_delivery=continue_delivery,expected_generations=expected_generations,
                 observation_client=observation_client,dispatch_scope=dispatch_scope)
    except GlobalPaused:
        with store.lock:
            previous=next(row for row in store.load(run_id)['items'] if row['id']==item_id)
        if previous.get('handoff_status')=='uploaded' and previous.get('preextract_delivery',{}).get('status')=='complete':return
        # G may reject before this scope has ever authorized the item. Return
        # the stop without changing a possibly foreign task's saved projection.
        return dict(status='paused',reason_code='global_admission_paused')


def _handoff_admitted(store, run_id, item_id, client, *, progress=lambda *args: None, stop=None,
                      reconcile=False,continue_delivery=False,expected_generations=None,observation_client=None,
                      dispatch_scope=None):
    """Use the current client once; a durable uncertain receipt prevents replay."""
    from preextract_delivery_queue import load_for_item_ids
    item = next(row for row in load_for_item_ids(store,run_id,[item_id])['items'] if row['id'] == item_id)
    packet = None
    receipt_path = None
    service = service_key(client.base_url)
    from filelock import Timeout
    operation_acquired=False
    native_reconcile=[False]
    read_only_result=False
    observing=reconcile and not continue_delivery and observation_client is not None
    from preextract_delivery_control import DeliveryControls,QueueStop,sync_operator_pause
    queue=getattr(store,'queue',None) or DeliveryControls(store.root)
    sync_operator_pause(queue)
    stop=QueueStop(queue,dict(payload=dict(service=service,run_id=run_id,item=dict(id=item_id,source_hash=item.get('source_hash',''))),
        control_generations=expected_generations,dispatch_scope=dispatch_scope),stop or threading.Event())
    if not observing and stop.is_set():
        # A foreign/expired scope must not even project a stop onto this item.
        return dict(status='paused',reason_code='dispatch_scope_unavailable')
    from preextract_retirement import find_terminal
    retired=find_terminal(store.root,work=item.get('preextract_work_dir'),
        source_hash=item.get('source_hash'),service=service) if item.get('preextract_work_dir') else None
    if retired:
        _save(store,run_id,item_id,copy.deepcopy(retired['projection']))
        progress('RAGFlow 入库已确认','已核验上传记录仍在，本地产物已进入清理；未重复上传')
        return
    try:
        if type(continue_delivery) is not bool:raise ValueError('继续交付必须是显式布尔指令')
        if continue_delivery and (not reconcile or expected_generations is None
                or expected_generations!=queue.generations(run_id)):
            raise ValueError('继续交付缺少当前全局及批次上传代次，未发送请求')
        if not observing and stop.is_set():raise InterruptedError('已停止，封包保留待交接')
        from filelock import FileLock
        work = Path(item['preextract_work_dir']).resolve()
        from preextract_scope_guard import require_managed
        managed_registry=require_managed(work,getattr(client,'prepared_scope',None))
        if not work.is_dir():
            raise ValueError('预提取私有材料目录不存在')
        # Share the workflow lock with run_single, including the standalone CLI.
        with FileLock(str(work / 'operation.lock'), timeout=0):
            operation_acquired=True
            prepared = _load(work / 'private' / 'prepared-publication.json')
            published = Path(item['package_path']).resolve()
            if not published.is_relative_to((store.runs / run_id / 'packages').resolve()):
                raise ValueError('交付包不在当前批次的发布目录内')
            prepared['package_path'] = str(published)
            package = _paperflow(prepared['paperflow_root'])
            packet = package.validate_package(published, prepared['task'], prepared['catalog'], strict_seal=True)
            if item.get('source_hash')!=prepared['task'].get('source_sha256'):
                raise ValueError('交付条目与原封包来源身份不一致')
            prepared['packet']=packet
            from preextract_publish import assert_publication_private_paths
            assert_publication_private_paths(prepared,packet,work/'delivery-receipt.json')
            from associated_delivery import load_plan,deliver_associated,receipt_summary
            attachment_plan=load_plan(prepared,work)
            attachments_required=bool(attachment_plan and attachment_plan['files'])
            attachments_known=bool(attachment_plan)
            if not attachments_known and (work/'material.json').is_file():
                material=_load(work/'material.json')
                attachments_known=(material.get('source',{}).get('source_sha256')==prepared['task']['source_sha256']
                    and material.get('version')=='preextract-material/1.0' and material.get('attachments')==[])
            managed_closure=[None]
            def project(receipt):
                projection=_projection(receipt,packet,service,receipt_path,
                    attachments_required=attachments_required,attachments=receipt_summary(work))
                if managed_registry is not None and not (managed_closure[0] or {}).get('complete'):
                    projection['handoff_status']='pending'
                    projection['preextract_delivery']['status']='pending'
                if native_reconcile[0] and managed_registry is None and not attachments_known:
                    projection['handoff_status']='pending'
                    projection['preextract_delivery']['status']='pending'
                    projection['handoff_reason']='正文及必要图片已核验；缺少原附件范围证明，整篇交付仍待核对'
                return projection
            if (packet['manifest_sha256'] != prepared['validation'].get('manifest_sha256')
                    or not prepared['validation'].get('embedding', {}).get('admission_verified')):
                raise ValueError('已验证交付包的身份或容量检查发生变化')
            paper_datasets = [route['paper_dataset_id'] for route in packet['routes']]
            if len(set(paper_datasets)) != len(paper_datasets):
                raise ValueError('不同三级专业不能合并为同一论文知识库')
            receipt_path = work / 'delivery-receipt.json'
            binding_path = work / 'delivery-binding.json'
            binding = dict(service=service, manifest_sha256=packet['manifest_sha256'],
                           task=prepared['task'], routes=packet['routes'])
            had_intent=binding_path.exists() or receipt_path.exists()
            if binding_path.exists():
                if _load(binding_path) != binding:
                    raise ValueError('已有投递服务或专业路由身份不一致，保留原回执等待核对')
            elif receipt_path.exists():
                raise ValueError('已有投递回执缺少服务绑定，须先核对，未重复发送')
            else:
                if reconcile:raise ValueError('核对必须保留原服务绑定，未创建新的交付意图')
                _write_json(binding_path, binding)
            old_receipt=_load(receipt_path) if receipt_path.is_file() else {}
            if had_intent and old_receipt.get('transport')=='native_body_batch/1' and not reconcile:
                _save(store,run_id,item_id,dict(handoff_status='pending',preextract_handoff_blocked=True,
                    handoff_reason='已有原生交付回执，需经只读核对及明确继续入口；未重新创建目标或改用直投'))
                return dict(status='needs_review',reason_code='native_reconciliation_required')
            if had_intent and not reconcile:
                old_receipt=_load(receipt_path) if receipt_path.is_file() else {}
                if (old_receipt.get('task')!=prepared['task'] or old_receipt.get('manifest_sha256')!=packet['manifest_sha256']
                        or not _receipt_complete(old_receipt,packet)):
                    _save(store,run_id,item_id,dict(handoff_status='pending',preextract_handoff_blocked=True,
                        handoff_reason='已有交付意图或未完整核验的回执；保留原件，等待显式核对，不自动重发'))
                    return dict(status='needs_review',reason='existing_remote_intent')
            progress('RAGFlow 直接交付切片', item.get('title', ''))
            native_reconcile[0]=bool(reconcile and old_receipt.get('transport')=='native_body_batch/1')
            if native_reconcile[0]:
                from preextract_batch_delivery import reconcile_native_publication,load_native_batch_config
                if observing:
                    if service_key(observation_client.base_url)!=service:
                        raise ValueError('只读核对服务与原绑定不一致')
                    report=reconcile_native_publication(prepared,receipt_path=receipt_path,
                        client=_ObservationClient(observation_client))
                    return _observe_projection(store,run_id,item_id,report,service)
                if not continue_delivery:
                    raise ValueError('原生回执核对需要独立只读客户端；继续交付需要明确授权与代次')
            elif observing or continue_delivery:
                raise ValueError('该回执不是原生交付，未扩大原核对权限')
            try:
                active_client=_StopAwareClient(client,stop) if stop is not None else client
                if native_reconcile[0]:
                    # Reload the actual selected local policy and its verified
                    # transport, never a placeholder config bypassing admission.
                    active_client.native_batch_config=load_native_batch_config(store.root,active_client)
                    receipt=reconcile_native_publication(prepared,receipt_path=receipt_path,client=active_client,
                        continue_delivery=True,control=lambda:not stop.is_set())
                    if receipt.get('mode')=='read_only':
                        read_only_result=True
                        return _observe_projection(store,run_id,item_id,receipt,service)
                elif reconcile:
                    receipt = reconcile_publication(prepared, receipt_path=receipt_path, client=active_client)
                else:
                    receipt = deliver_publication(prepared, config=None, receipt_path=receipt_path, client=active_client)
            finally:
                if receipt_path.exists() and not read_only_result:
                    _save(store, run_id, item_id, project(_load(receipt_path)))
            if not _receipt_complete(receipt, packet):
                raise ValueError('投递回执尚未证明所有专业完成内容、图片和检索读回')
            if managed_registry is not None:
                from auto_config import load_config
                from preextract_association_supplement import ensure_associations
                managed_closure[0]=ensure_associations(prepared,receipt,work,active_client,data_root=store.root,
                    purpose_config=load_config(store.root).get('purpose_review',{}),stop=stop,progress=progress)
                _save(store,run_id,item_id,project(receipt))
                if not managed_closure[0]['complete']:raise ValueError('正文已保存，必需附件仍有缺项或用途待核对')
                from preextract_scope_guard import save_delivery_closure
                save_delivery_closure(work,item,prepared,receipt,managed_closure[0],getattr(client,'prepared_scope'))
            elif attachments_required:
                try:
                    deliver_associated(prepared,receipt,work,active_client,stop=stop,progress=progress)
                finally:
                    _save(store,run_id,item_id,project(_load(receipt_path)))
            if (work/'revision.json').exists():
                from preextract_pipeline import activate_revision
                try:
                    material=_load(work/'material.json')
                    activate_revision(material['source']['file'],work)
                    activation=dict(status='active',work_dir=str(work))
                except Exception as exc:
                    # The remote receipt is already verified. A local cache
                    # pointer failure must not erase that separate outcome.
                    activation=dict(status='pending',error_type=type(exc).__name__)
                    progress('本地复用登记待核对','入库已确认，修订版本的本地默认复用登记尚未完成')
                try:
                    _save(store,run_id,item_id,dict(preextract_revision_activation=activation))
                except Exception:
                    # This optional local projection is separate from the
                    # verified delivery projection already saved above.
                    progress('本地复用登记待核对','入库已确认，本地修订版本登记状态未能保存')
            # Cleanup is a local tail. Its failure must never erase the verified
            # remote outcome or interrupt another paper's processing.
            try:
                # Standalone handoffs join the same durable receipt table only
                # after their final all-target projection has been saved.
                from preextract_cleanup import policy
                if not hasattr(store,'queue') and policy(store.root).get('enabled'):
                    from preextract_delivery_queue import DeliveryQueue,queue_path,package_reference,DELIVERY_FIELDS
                    saved=next(r for r in load_for_item_ids(store,run_id,[item_id])['items'] if r['id']==item_id)
                    if saved.get('handoff_status')=='uploaded' and saved.get('preextract_delivery',{}).get('status')=='complete':
                        queue=DeliveryQueue(queue_path(store.root))
                        payload=package_reference(store,run_id,item_id,service,item=saved)
                        queue.enqueue(payload,workspace_locked=True)
                        queue.update(payload['id'],status='complete',projection={k:saved[k] for k in DELIVERY_FIELDS if k in saved})
                from preextract_cleanup import cleanup_item
                from global_task_control import GlobalTaskControl
                with GlobalTaskControl(store.root).stage('cleanup',batch_id=run_id,paper_id=item_id):
                    cleanup = (dict(status='deferred', reason='task_scoped_nas_release_unconfirmed')
                               if getattr(client, 'native_unscoped_tail_blocked', False)
                               else cleanup_item(store.root, run_id, item, already_locked=True, stop=stop))
                if cleanup.get('status') != 'disabled':
                    _save(store, run_id, item_id, dict(preextract_cleanup={key:cleanup[key] for key in
                        ('status','reason','error_type','removed_files','removed_bytes') if key in cleanup}))
                if cleanup.get('status') in {'complete', 'pending'}:
                    _save(store, run_id, item_id, project(_load(receipt_path)))
                    progress('中间产物已清理' if cleanup['status']=='complete' else '中间产物清理待继续',
                             '原稿、完整学习结果、复用依赖和入库回执保留')
            except Exception:
                progress('中间产物清理待核对', '入库已确认；本地清理未完成，原材料保留')
            progress('RAGFlow 入库已确认', item.get('title', ''))
        from preextract_cleanup import ensure_cleanup_worker
        from global_task_control import GlobalTaskControl
        try:
            if (not getattr(client, 'native_unscoped_tail_blocked', False)
                    and GlobalTaskControl(store.root).snapshot()['admission_open']):
                ensure_cleanup_worker(store.root)
        except Exception: pass
    except Exception as exc:
        if isinstance(exc,Timeout) and not operation_acquired:
            _save(store,run_id,item_id,dict(handoff_status='pending',handoff_reason='本篇工作锁忙碌，成品保留等待释放',
                preextract_handoff_blocked=True))
            progress('RAGFlow 等待工作锁','原封包与回执保留，未发送新的投递请求')
            if getattr(client,'prepared_scope',None):
                from paperflow.scheduling_policy import ActionDeferred
                raise ActionDeferred('local_file_lock') from exc
            return dict(status='waiting_workspace',reason='operation_lock_busy')
        reason = str(exc).replace(getattr(client, 'api_key', '') or '\0', '[密钥]')[:240]
        _save(store, run_id, item_id, dict(handoff_status='pending', handoff_reason=reason,
              preextract_handoff_blocked=bool(receipt_path and receipt_path.exists())))
        if packet is not None and native_reconcile[0] and receipt_path and receipt_path.exists():
            projection=project(_load(receipt_path))
            projection['handoff_status']='pending'
            projection['preextract_delivery'].update(status='uncertain',body_status='uncertain',
                reason_code='native_current_verification_failed')
            projection['handoff_reason']=reason
            _save(store,run_id,item_id,projection)
        progress('RAGFlow 等待核对', reason)
        from preextract_native_runtime import NativeReadbackPending, NativeExecutionUnavailable
        if not getattr(client,'prepared_scope',None):
            if isinstance(exc, NativeReadbackPending):
                return dict(status='native_followup_pending', identity=exc.identity, execution=exc.execution)
            return dict(status='handoff_error', error_type=type(exc).__name__,
                        transient=isinstance(exc, (NativeExecutionUnavailable, OSError, TimeoutError, RuntimeError)))
        if getattr(client,'prepared_scope',None):
            from paperflow.scheduling_policy import ActionDeferred
            if isinstance(exc,(ActionDeferred,InterruptedError)):raise
