"""Automatic major routing and import, with reconciliation after uncertain uploads."""
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
import json
from engine import digest
from evidence import package_fingerprint
from auto_config import normalize_dataset_map
from ragflow_client import upload_routed_items




def _route_datasets(datasets, major, mapping, legacy_dataset_map_fallback=True):
    """Resolve the configured md/image pair without embedding dataset IDs."""
    configured = normalize_dataset_map(mapping).get(major, {'paper':'','image':''})
    paper_id = configured.get('paper','')
    image_id = configured.get('image','')
    if paper_id:
        paper_matches = [dataset for dataset in datasets if dataset.get('id') == paper_id]
    elif legacy_dataset_map_fallback:
        paper_matches = [dataset for dataset in datasets if dataset.get('name') == major]
    else:
        paper_matches = []
    if len(paper_matches) != 1:
        if not paper_id:
            raise ValueError(f'专业“{major}”还没选 md 知识库，请先在 RAGFlow 连接页选择并保存')
        raise ValueError(f'专业“{major}”配置的 md 知识库不存在或不唯一')
    paper = paper_matches[0]
    if image_id:
        image_matches = [dataset for dataset in datasets if dataset.get('id') == image_id]
        if len(image_matches) != 1:
            raise ValueError(f'专业“{major}”配置的图片知识库不存在或不唯一')
        image = image_matches[0]
    else:
        image = None
    return {'paper': paper, 'image': image}


def _save_handoff_item(store,run_id,item):
    with store.lock:
        manifest=store.load(run_id)
        current=next((row for row in manifest.get('items',[]) if row.get('id')==item.get('id')),None)
        if current is None: return
        for key,value in item.items():
            if key!='uploads': current[key]=value
        if isinstance(item.get('uploads'),dict): current.setdefault('uploads',{}).update(item['uploads'])
        store.save(manifest)


def _run_handoff_items(snapshots,process,concurrency):
    """Share the existing paper-level handoff limit across both formats."""
    options=concurrency if isinstance(concurrency,dict) else {}
    if options.get('enabled') and int(options.get('papers',1))>1:
        with ThreadPoolExecutor(max_workers=max(1,int(options['papers'])),thread_name_prefix='handoff') as pool:
            futures=[pool.submit(process,snapshot) for snapshot in snapshots]
            for future in as_completed(futures):future.result()
    else:
        for snapshot in snapshots:process(snapshot)


def handoff(store,run_id,client,mapping,progress=lambda *a:None,auto_parse=True,
            legacy_dataset_map_fallback=True,concurrency=None,target_datasets=None,item_ids=None,upload_config=None):
    manifest=store.load(run_id)
    batch_targets=manifest.get('task_settings',{}).get('destinations')
    requested_ids=set(item_ids) if item_ids is not None else None
    snapshots=[snapshot for snapshot in list(manifest['items'])
               if snapshot['status']=='pass' and (snapshot.get('automatic_approval') or
                    ((upload_config or {}).get('work_pool_enabled') and
                     (upload_config or {}).get('manual_reviewed') and item_ids is not None))
               and (requested_ids is None or snapshot['id'] in requested_ids)]
    if not snapshots:return
    # These packages already contain validated, redacted retrieval records.
    # Their sealed MD routes bypass the legacy profile and remote parser lanes.
    preextract=[row for row in snapshots if row.get('content_format')=='paper_preextract']
    if preextract:
        # Only explicitly opted-in new runs (or their registered packages) use
        # the durable lane. A late import in an already running old batch must
        # retain its original delivery semantics.
        queued=[row for row in preextract if
                (upload_config or {}).get('preextract_delivery_mode')=='disk_queue_v1'
                or manifest.get('task_settings',{}).get('preextract_delivery_mode')=='disk_queue_v1'
                or (row.get('preextract_delivery_queue') or {}).get('mode')=='disk_queue_v1']
        if queued:
            from preextract_delivery_queue import enqueue_items
            enqueue_items(store,run_id,queued,client.base_url,progress,
                          retry=(upload_config or {}).get('manual_request',item_ids is not None))
            if (upload_config or {}).get('work_pool_auto_start',True):
                from preextract_delivery_worker import wake_worker
                wake_worker(store.root,client.base_url)
            queued_ids={row['id'] for row in queued}
            preextract=[row for row in preextract if row['id'] not in queued_ids]
        from preextract_handoff import handoff_preextract_item
        def process_preextract(snapshot):
            handoff_preextract_item(store,run_id,snapshot['id'],client,progress=progress,
                                   stop=(upload_config or {}).get('_stop_event'))
        _run_handoff_items(preextract,process_preextract,concurrency)
        snapshots=[row for row in snapshots if row.get('content_format')!='paper_preextract']
        if not snapshots:return
        manifest=store.load(run_id)
    try:
        cached=(upload_config or {}).get('_work_pool_datasets')
        datasets=cached if cached is not None else client.datasets()
    except Exception:
        manifest['handoff_message']='RAGFlow 暂不可用，已通过结果保留等待重试';store.save(manifest)
        if item_ids is not None:raise
        return
    if (upload_config or {}).get('work_pool_enabled'):
        from ingest_queue import QueueStore
        from ingest_workbench import enqueue_workbench_items
        from ingest_service import validate_worker_service,ensure_work_pool_worker
        auto_start=(upload_config or {}).get('work_pool_auto_start',True)
        if auto_start:
            validate_worker_service(Path(store.root),client.base_url)
        profile_config=(upload_config or {}).get('research_profile') or {}
        queue_path=(profile_config.get('queue') if profile_config.get('enabled') else None) or (upload_config or {}).get('work_pool_queue') or (
            Path(store.root)/'private'/'universal_ingest'/'queue.sqlite3')
        queue=QueueStore(queue_path)
        if getattr(store,'root',None) and (Path(store.root)/'private'/'education_full_ingest'/run_id/'receipts.json').exists():
            raise ValueError('该教育批次由独立导入进程处理，已阻止第二个上传入口')
        mode='upload_and_parse' if auto_parse else 'upload_only'
        config=dict(upload_config or {}, datasets=datasets, dataset_map=mapping,
                    legacy_dataset_map_fallback=legacy_dataset_map_fallback,
                    manual_request=(upload_config or {}).get('manual_request',item_ids is not None))
        if hasattr(store,'history'):
            from upload_tracking import UploadIndex
            if getattr(store,'_work_pool_legacy_index',None) is None:
                store._work_pool_legacy_index=UploadIndex(store.history())
            config['_legacy_index']=store._work_pool_legacy_index
        config['_receipt_lookup']=lambda dataset,document:client.documents(dataset,document_id=document)
        config['_manifest_snapshot']=manifest
        enqueued=False
        updates={}
        groups={}
        for snapshot in snapshots:
            targets=(target_datasets if target_datasets is not None else
                     snapshot.get('handoff_destinations',batch_targets))
            group_key=json.dumps(targets,sort_keys=True,ensure_ascii=False)
            if group_key not in groups:
                groups[group_key]=(targets,[])
            groups[group_key][1].append(snapshot)
        def enqueue_group(group,targets):
            from upload_tracking import paper_id_for_item
            task_ids=enqueue_workbench_items(store,run_id,[row['id'] for row in group],
                targets,mode,queue,config,client.base_url)
            by_source=defaultdict(list)
            for task_id in task_ids:
                task=queue.get(task_id)
                if task:
                    by_source[task['source_id']].append(task)
            for snapshot in group:
                queued=by_source[paper_id_for_item(snapshot)]
                destination={}
                for role,kind in (('paper_primary','paper'),('image','image')):
                    match=next((task for task in queued if
                                (task['role']==role if kind=='paper' else
                                 task['lane']=='image')),None)
                    if match:
                        dataset=next((row for row in datasets if row.get('id')==match['dataset_id']),None)
                        if dataset:
                            destination[kind]={'id':dataset['id'],'name':dataset['name']}
                updates[snapshot['id']]={
                    'handoff_status':'queued','queue_task_ids':[row['id'] for row in queued],
                    'handoff_reason':'','handoff_destinations':destination,
                    'handoff_target_tasks':[{'task_id':row['id'],'dataset_id':row['dataset_id'],'role':row['role']} for row in queued]}
                progress('RAGFlow 已排队',snapshot.get('title',''))
        strict_error=None
        for targets,group in groups.values():
            try:
                enqueue_group(group,targets)
                enqueued=True
            except Exception:
                # Isolate invalid items; an idempotent retry retains any persisted IDs.
                for snapshot in group:
                    try:
                        enqueue_group([snapshot],targets)
                        enqueued=True
                    except Exception as exc:
                        reason=str(exc).replace(getattr(client,'api_key','') or '\0','[密钥]')[:240]
                        updates[snapshot['id']]={'handoff_status':'pending','handoff_reason':reason}
                        progress('RAGFlow 等待重试',reason)
                        if item_ids is not None and (upload_config or {}).get('work_pool_strict',True):
                            strict_error=exc
        with store.lock:
            current=store.load(run_id)
            for row in current.get('items',[]):
                if row['id'] in updates:
                    row.update(updates[row['id']])
            store.save(current)
        if enqueued and auto_start:
            ensure_work_pool_worker(Path(store.root),Path(queue_path),client.base_url,
                                    parse_limit=int(config.get('work_pool_parse_limit',3)))
        if strict_error:
            raise strict_error
        return

    def process(snapshot):
        manifest=store.load(run_id);item=next(i for i in manifest['items'] if i['id']==snapshot['id'])
        try:
            if digest(Path(item['path']))!=item['reviewed_hash'] or package_fingerprint(item['package_path'])!=item['reviewed_package_hash']:
                raise ValueError('自动检查后文件发生变化，需重新运行预处理')
            targets=target_datasets if target_datasets is not None else item.get('handoff_destinations',batch_targets)
            if targets is not None:
                # A batch's explicit selection applies to every paper in it.
                fixed={kind:(targets.get(kind) or {}).get('id','') for kind in ('paper','image')}
                routes=_route_datasets(datasets,item['major'],{item['major']:fixed},False)
            else:
                routes=_route_datasets(datasets,item['major'],mapping,legacy_dataset_map_fallback)
            dataset=routes['paper'];key=client.base_url+'|'+dataset['id']
            progress('RAGFlow 自动导入',item['title'])
            upload_options=concurrency if isinstance(concurrency,dict) and concurrency.get('enabled') else None
            if upload_options:
                upload_options={**upload_options,'enabled':True,'papers':max(2,int(upload_options.get('papers',2)))}
            upload_routed_items(store,run_id,[item['id']],client,routes,
                                lambda message:progress('RAGFlow 上传状态',message),concurrency=upload_options,
                                upload_config=upload_config)
            manifest=store.load(run_id);item=next(i for i in manifest['items'] if i['id']==snapshot['id']);record=item['uploads'][key]
            if auto_parse:
                # Only the paper primary document enters the accounting Pipeline.
                document_ids=[record['document_id']];parse_status=[]
                for document_id in dict.fromkeys(document_ids):
                    from rag_status import normalize_remote_doc
                    try:
                        docs=client.documents(dataset['id'],document_id=document_id)
                        if len(docs)!=1:raise ValueError('远端文档尚未唯一确认，未重复提交解析')
                        remote,message=normalize_remote_doc(docs[0])
                        record.update(ragflow_status=remote,ragflow_reason=message,query_error='',chunk_count=docs[0].get('chunk_count'))
                    except Exception as exc:
                        record['query_error']=str(exc).replace(getattr(client,'api_key','') or '\0','[密钥]')[:240]
                        _save_handoff_item(store,run_id,item);raise
                    if remote in {'unstarted','failed','cancelled'}:
                        try:client.parse(dataset['id'],document_id)
                        except Exception as exc:
                            record['parse_status']='unknown'
                            record['parse_error']=str(exc).replace(getattr(client,'api_key','') or '\0','[密钥]')[:240]
                            _save_handoff_item(store,run_id,item);raise
                        record.update(ragflow_status='submitted',parse_error='',ragflow_reason='解析请求已提交，等待服务器状态更新')
                    elif remote not in {'running','queued','submitted','done'}:
                        _save_handoff_item(store,run_id,item)
                        raise ValueError('远端处理状态尚未确认，未重复提交解析；请刷新状态后再试')
                    parse_status.append(remote=='done')
                record['parse_status']='done' if parse_status and all(parse_status) else 'submitted'
            item['handoff_status']='uploaded';item.pop('handoff_reason',None);_save_handoff_item(store,run_id,item)
            if item_ids is not None and auto_parse:
                from rag_operations import recover_failed_assets
                recover_failed_assets(store,run_id,item['id'],client,
                    dataset_ids={route['id'] for route in routes.values() if route})
        except Exception as exc:
            manifest=store.load(run_id);item=next(i for i in manifest['items'] if i['id']==snapshot['id'])
            item['handoff_status']='pending';item['handoff_reason']=str(exc).replace(getattr(client,'api_key','') or '\0','[密钥]')[:240]
            _save_handoff_item(store,run_id,item);progress('RAGFlow 等待重试',item['handoff_reason'])

    _run_handoff_items(snapshots,process,concurrency)
