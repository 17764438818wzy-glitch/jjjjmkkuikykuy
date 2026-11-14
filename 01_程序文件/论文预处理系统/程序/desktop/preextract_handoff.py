"""Adapt sealed preextract publications to the desktop's durable handoff records."""
from __future__ import annotations

import copy
import json
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
                if self.stop.is_set():raise InterruptedError('已停止交付；已有回执保留，未继续发送请求')
                return value(*args,**kwargs)
            return checked
        return value


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
    if not complete and not state['reason']:
        state['reason'] = '存在未确认投递，需核对既有回执，未重复上传'
    return state


def handoff_preextract_item(store, run_id, item_id, client, *, progress=lambda *args: None, stop=None, reconcile=False):
    """Use the current client once; a durable uncertain receipt prevents replay."""
    item = next(row for row in store.load(run_id)['items'] if row['id'] == item_id)
    packet = None
    receipt_path = None
    service = service_key(client.base_url)
    try:
        if stop is not None and stop.is_set():raise InterruptedError('已停止，封包保留待交接')
        from filelock import FileLock
        work = Path(item['preextract_work_dir']).resolve()
        if not work.is_dir():
            raise ValueError('预提取私有材料目录不存在')
        # Share the workflow lock with run_single, including the standalone CLI.
        with FileLock(str(work / 'operation.lock'), timeout=0):
            prepared = _load(work / 'private' / 'prepared-publication.json')
            published = Path(item['package_path']).resolve()
            if not published.is_relative_to((store.runs / run_id / 'packages').resolve()):
                raise ValueError('交付包不在当前批次的发布目录内')
            prepared['package_path'] = str(published)
            package = _paperflow(prepared['paperflow_root'])
            packet = package.validate_package(published, prepared['task'], prepared['catalog'], strict_seal=True)
            prepared['packet']=packet
            from associated_delivery import load_plan,deliver_associated,receipt_summary
            attachment_plan=load_plan(prepared,work)
            attachments_required=bool(attachment_plan and attachment_plan['files'])
            def project(receipt):
                return _projection(receipt,packet,service,receipt_path,
                    attachments_required=attachments_required,attachments=receipt_summary(work))
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
            if binding_path.exists():
                if _load(binding_path) != binding:
                    raise ValueError('已有投递服务或专业路由身份不一致，保留原回执等待核对')
            elif receipt_path.exists():
                raise ValueError('已有投递回执缺少服务绑定，须先核对，未重复发送')
            else:
                _write_json(binding_path, binding)
            progress('RAGFlow 直接交付切片', item.get('title', ''))
            try:
                active_client=_StopAwareClient(client,stop) if stop is not None else client
                if reconcile:
                    receipt = reconcile_publication(prepared, receipt_path=receipt_path, client=active_client)
                else:
                    receipt = deliver_publication(prepared, config=None, receipt_path=receipt_path, client=active_client)
            finally:
                if receipt_path.exists():
                    _save(store, run_id, item_id, project(_load(receipt_path)))
            if not _receipt_complete(receipt, packet):
                raise ValueError('投递回执尚未证明所有专业完成内容、图片和检索读回')
            if attachments_required:
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
                from preextract_cleanup import cleanup_item
                cleanup = cleanup_item(store.root, run_id, item, already_locked=True, stop=stop)
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
    except Exception as exc:
        reason = str(exc).replace(getattr(client, 'api_key', '') or '\0', '[密钥]')[:240]
        _save(store, run_id, item_id, dict(handoff_status='pending', handoff_reason=reason,
              preextract_handoff_blocked=bool(receipt_path and receipt_path.exists())))
        progress('RAGFlow 等待核对', reason)
