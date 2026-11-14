"""Turn approved workbench items into universal queue artifact tasks."""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Literal
from dataclasses import replace

from auto_ragflow import _route_datasets
from engine import digest
from evidence import package_fingerprint
from ingest_contracts import ArtifactSpec, Outcome, canonical_remote_name
from ingest_queue import QueueStore
from ragflow_client import _package_asset_files
from upload_tracking import paper_id_for_item, UploadIndex


FINANCE_MAJORS = {'会计学', '财务管理', '审计学', '税务', '财政学',
                  '资产评估', '金融学'}


def _resolve_targets(datasets: list[dict], item: dict, targets: dict | None,
                     config: dict) -> dict:
    if targets is not None:
        fixed = {kind: (targets.get(kind) or {}).get('id', '')
                 for kind in ('paper', 'image')}
        return _route_datasets(datasets, item['major'], {item['major']: fixed}, False)
    mapping=dict(config.get('dataset_map') or {})
    if item['major'] not in mapping:
        fallback=(config.get('category_dataset_map') or {}).get(item.get('category'))
        if fallback:
            mapping[item['major']]=fallback
    return _route_datasets(datasets, item['major'], mapping,
                           config.get('legacy_dataset_map_fallback', True))


def workbench_specs(store, run_id: str, item_ids: list[str],
                    targets: dict | None,
                    mode: Literal['upload_only', 'upload_and_parse'],
                    config: dict, service_url: str) -> list[ArtifactSpec]:
    if mode not in ('upload_only', 'upload_and_parse'):
        raise ValueError('Unknown workbench import mode')
    datasets = config.get('datasets')
    if not isinstance(datasets, list):
        raise ValueError('Validated RAGFlow dataset inventory is required')
    manifest = config.get('_manifest_snapshot') or store.load(run_id)
    if manifest.get('run_id') != run_id:
        raise ValueError('Workbench manifest identity mismatch')
    by_id = {item['id']: item for item in manifest.get('items') or []}
    result = []
    for item_id in item_ids:
        item = by_id.get(item_id)
        if item is None or item.get('status') != 'pass':
            raise ValueError('Workbench item is missing or not approved')
        if not item.get('automatic_approval') and not config.get('manual_reviewed'):
            raise ValueError('Workbench item has not passed its review gate')
        package = Path(item['package_path'])
        primary = Path(item['path'])
        if (not primary.is_file() or digest(primary) != item.get('reviewed_hash')
                or package_fingerprint(package) != item.get('reviewed_package_hash')):
            raise ValueError('Workbench source or package changed after approval')
        package_manifest = json.loads((package / 'manifest.json').read_text(encoding='utf-8'))
        source_id = paper_id_for_item(item)
        if not source_id or package_manifest.get('paper_id') != source_id:
            raise ValueError('Workbench package has no stable paper identity')
        if package_manifest.get('document_id') != item['id']:
            raise ValueError('Workbench package document ID differs from manifest')
        if config.get('research_profile',{}).get('enabled'):
            result.extend(_profile_specs(item,package,package_manifest,source_id,datasets,config,service_url,run_id,mode))
            continue
        routes = _resolve_targets(datasets, item, targets, config)
        paper = routes['paper']
        image = routes['image']
        parse_required = mode == 'upload_and_parse'
        pipeline = (config.get('dataset_pipeline_map') or {}).get(paper['id']) or paper.get('pipeline_id')
        native = (config.get('dataset_native_parser_map') or {}).get(paper['id'])
        if native and (native != 'naive' or pipeline or paper.get('chunk_method') != native
                       or item.get('category') == '教育教学类'):
            raise ValueError('Native parser destination differs from its approved configuration')
        if parse_required and not pipeline and not native:
            raise ValueError('Selected md dataset has no bound paper Pipeline')
        policy = ('education' if item.get('category') == '教育教学类' else
                  'native_naive' if native else
                  'finance' if item.get('major') in FINANCE_MAJORS else 'generic')
        if policy=='education':
            from education_stream_ingest import checked_copy
            from ingest_education import education_specs
            from knowledge_package import knowledge_files
            try:
                knowledge_files(package,require_stable_paper_id=True)
                canonical=package
            except ValueError:
                if not getattr(store,'root',None):
                    raise ValueError('Education identity normalization needs the workbench data root')
                canonical,_=checked_copy(item,Path(store.root)/'runs'/run_id,
                    Path(store.root)/'private'/'education_work_pool'/run_id/'canonical_packages')
            deployment={'paper_dataset_id':paper['id'],'image_dataset_id':image['id'] if image else '',
                        'pipeline_id':pipeline or 'upload-only',
                        'model_channel_key':config.get('model_channel_key') or 'education-default'}
            education=education_specs(item,deployment,canonical,service_url,run_id)
            result.extend(replace(spec,parse_required=(parse_required and spec.role=='paper_primary'),
                                  pipeline_id=pipeline if spec.role=='paper_primary' else None,
                                  priority=10 if config.get('manual_request') else 0)
                          for spec in education if spec.role=='paper_primary' or image)
            continue
        common = dict(service_url=service_url, source_id=source_id,
                      priority=10 if config.get('manual_request') else 0,
                      batch_id=run_id, policy_key=policy,
                      channel_key=config.get('model_channel_key') or pipeline or ('native-naive' if native else 'upload-only'),
                      package_path=package,package_sha256=item['reviewed_package_hash'])
        handoff=re.search(r'\[PAPER_PIPELINE_METADATA_V1\]\s*(.*?)\s*\[/PAPER_PIPELINE_METADATA_V1\]',
                          primary.read_text(encoding='utf-8'),re.S)
        if not handoff:
            raise ValueError('Paper handoff block is missing')
        handoff_id=json.loads(handoff.group(1)).get('paper_id')
        if handoff_id not in {source_id,item['id']}:
            raise ValueError('Paper handoff identity differs from package')
        content_hash = digest(primary)
        result.append(ArtifactSpec(dataset_id=paper['id'], role='paper_primary',
            lane='paper', content_sha256=content_hash, path=primary,
            remote_name=canonical_remote_name(item.get('title') or package_manifest.get('title') or 'paper',
                                              source_id, content_hash, primary.suffix or '.md',
                                              legacy_id=item['id']),
            pipeline_id=pipeline, parse_required=parse_required,handoff_id=handoff_id, **common))
        for relative, path, route, _fallback in _package_asset_files(package):
            if route == 'paper':
                dataset = paper
            elif route == 'image' and image:
                dataset = image
            else:
                continue
            path = Path(path)
            file_hash = digest(path)
            name_tag = digest_name(relative)
            result.append(ArtifactSpec(dataset_id=dataset['id'],
                role=f'asset:{relative}', lane='image' if route == 'image' else 'paper',
                content_sha256=file_hash, path=path,
                remote_name=canonical_remote_name(
                    f'{path.stem}_{name_tag}', source_id, file_hash,
                    path.suffix or '.bin', legacy_id=item['id']),
                pipeline_id=None, parse_required=False, **common))
    return result


def _profile_specs(item,package,manifest,source_id,datasets,config,service_url,run_id,mode):
    from research_profile import checked_envelope
    from specialty_routing import load_catalog,confirmed_routes
    material=item.get('material_processing')=='research_material'
    if material:
        from research_material import checked_material
        envelope=checked_material(package)
    else:envelope=checked_envelope(package)
    if envelope.get('specialty_routing_policy')!='substantive-specialty/1.0' or not envelope.get('routing_review'):
        raise ValueError('research_profile_professional_review_required')
    from frontmatter_privacy import mask_cover_student_numbers
    if mask_cover_student_numbers(envelope['source_text'])!=envelope['source_text']:
        raise ValueError('research_profile_privacy_supplement_required')
    if envelope['admission']['status'] not in ('recommended','limited'):
        raise ValueError('research_profile_'+envelope['admission']['status'])
    catalog=load_catalog(Path(__file__).with_name('specialty_catalog.json'))
    routes=confirmed_routes(envelope,catalog)
    if not routes:raise ValueError('research_profile_classification_pending')
    by_dataset={d['id']:d for d in datasets};result=[]
    primary=Path(item['path']);content_hash=digest(primary)
    for route in routes:
        paper=by_dataset.get(route['paper_dataset_id']);image=by_dataset.get(route['image_dataset_id'])
        if not paper or not image or paper.get('name')!=f"【{route['code']}】{route['name']}｜论文库" or image.get('name')!=f"【{route['code']}】{route['name']}｜图片库":
            raise ValueError('research_profile_dataset_mapping_changed')
        pipeline=(config.get('dataset_pipeline_map') or {}).get(paper['id']) or paper.get('pipeline_id')
        if mode=='upload_and_parse' and not pipeline:raise ValueError('research_profile_pipeline_missing')
        common=dict(service_url=service_url,source_id=source_id,batch_id=run_id,policy_key='research_material' if material else 'research_profile',
            priority=10 if config.get('manual_request') else 0,channel_key='free-deepseek-v4-pro:'+route['code'],
            package_path=package,package_sha256=item['reviewed_package_hash'])
        primary_dataset=image if material and item.get('material_intake',{}).get('route')=='image' else paper
        result.append(ArtifactSpec(dataset_id=primary_dataset['id'],role='paper_primary',lane='image' if primary_dataset is image else 'paper',path=primary,
            content_sha256=content_hash,remote_name=canonical_remote_name('RP_'+route['code']+'__'+(item.get('title') or manifest['title']),source_id,content_hash,'.md',legacy_id=item['id']),
            pipeline_id=pipeline,parse_required=mode=='upload_and_parse',handoff_id=source_id,**common))
        for relative,path,kind,_fallback in _package_asset_files(package):
            path=Path(path);file_hash=digest(path);dataset=image if kind=='image' else paper
            result.append(ArtifactSpec(dataset_id=dataset['id'],role='asset:'+relative,lane='image' if kind=='image' else 'paper',path=path,
                content_sha256=file_hash,remote_name=canonical_remote_name(path.stem+'_'+digest_name(relative),source_id,file_hash,path.suffix,legacy_id=item['id']),
                parse_required=False,pipeline_id=None,**common))
    return result


def digest_name(relative: str) -> str:
    import hashlib
    return hashlib.sha256(relative.encode('utf-8')).hexdigest()[:8]


def enqueue_workbench_items(store, run_id: str, item_ids: list[str],
                            targets: dict | None, mode: str,
                            queue: QueueStore, config: dict,
                            service_url: str) -> list[int]:
    """Persist approved workbench intent without making a remote write."""
    specs = workbench_specs(store, run_id, item_ids, targets, mode,
                            config, service_url)
    manifest=config.get('_manifest_snapshot') or store.load(run_id)
    requested_ids=set(item_ids)
    items={paper_id_for_item(item):item for item in manifest.get('items',[])
           if item['id'] in requested_ids}
    legacy=config.get('_legacy_index') or UploadIndex([manifest])
    result=[]
    for spec in specs:
        existing=queue.find_identity(service_url,spec.dataset_id,spec.source_id,spec.role)
        if existing and existing.get('remote_id'):
            # Only a verified prior queue receipt may preserve a legacy remote name.
            if (existing['content_sha256']!=spec.content_sha256 or existing['package_sha256']!=spec.package_sha256
                    or existing['policy_key']!=spec.policy_key or existing.get('handoff_id')!=spec.handoff_id):
                raise ValueError('Existing queue content requires reconciliation')
            spec=replace(spec,remote_name=existing['remote_name'])
        hits=legacy.matches(items[spec.source_id],service_url,spec.dataset_id)
        receipts=[]
        for hit in hits:
            record=hit['record']
            if spec.role=='paper_primary' and record.get('route')!='image':
                receipts.append(record)
            elif spec.role!='paper_primary':
                relative=spec.role.split(':',1)[-1]
                receipts.extend(asset for asset in record.get('asset_uploads',[])
                                if asset.get('path')==relative)
        candidates=[row for row in receipts if row.get('status') in
                    ('uploaded','sending','unknown')]
        identities={(row.get('document_id'),row.get('remote_name') or row.get('name'))
                    for row in candidates}
        if len(identities)>1:
            raise ValueError('Multiple legacy remote receipts require reconciliation')
        receipt=next(iter(candidates),None)
        initial=None
        if receipt:
            content_hash=receipt.get('content_hash') or receipt.get('local_content_hash') or receipt.get('sha256')
            name=receipt.get('remote_name') or receipt.get('name')
            if content_hash!=spec.content_sha256 or not name:
                raise ValueError('Existing upload receipt needs identity/content reconciliation')
            spec=replace(spec,remote_name=name)
            if (spec.role=='paper_primary' and spec.pipeline_id and
                    receipt.get('parse_status') in ('unknown','submitted','running','done')):
                spec=replace(spec,parse_required=True)
            remote_id=receipt.get('document_id')
            initial={'status':'upload_unknown','remote_id':remote_id}
            lookup=config.get('_receipt_lookup')
            if remote_id and receipt.get('status')=='uploaded' and lookup:
                docs=lookup(spec.dataset_id,remote_id)
                if len(docs)!=1 or docs[0].get('id')!=remote_id or docs[0].get('name')!=name:
                    raise ValueError('Existing remote receipt identity cannot be confirmed')
                remote=docs[0]
                allowed_pipelines={None,spec.pipeline_id}
                if existing and existing.get('remote_id')==remote_id:
                    allowed_pipelines.update((existing.get('execution_pipeline_id'),existing.get('execution_previous_pipeline_id')))
                if spec.pipeline_id and remote.get('pipeline_id') not in allowed_pipelines:
                    raise ValueError('Existing document Pipeline differs; preserved for review')
                from rag_status import normalize_remote_doc
                state,_=normalize_remote_doc(remote)
                prior_parse=receipt.get('parse_status')
                if not spec.parse_required:
                    initial['status']='uploaded'
                elif state=='unstarted' and prior_parse=='unknown':
                    initial['status']='parse_unknown'
                elif state=='unstarted' and prior_parse not in ('submitted','running','done'):
                    initial['status']='parse_waiting'
                else:
                    initial['status']='submitted'
                initial['remote_run']=remote.get('run')
        result.append(queue.enqueue(spec,initial_state=initial))
    return result


def classify_finance(task: dict, remote: dict) -> Outcome:
    fields = remote.get('meta_fields') or {}
    if fields.get('accounting_paper_id') == (task.get('handoff_id') or task.get('source_id')):
        decision = fields.get('accounting_admission')
        if remote.get('run') == 'DONE' and decision == '入库':
            return Outcome.accepted
        if decision == '暂缓':
            return Outcome.quality_hold
        if decision == '剔除':
            return Outcome.quality_reject
    progress = (remote.get('progress_msg') or '').strip()
    if re.fullmatch(r'\d+ tasks? are ahead in the queue\.\.\.', progress):
        return Outcome.outcome_unresolved
    return Outcome.technical_review


def request_queued_review(queue: QueueStore,task_ids: list[int],lookup) -> int:
    """Record an explicit second pass on the original confirmed document."""
    from rag_status import normalize_remote_doc
    rows=queue.get_many(task_ids)
    count=0
    for task in rows.values():
        if task['role']!='paper_primary' or not task.get('remote_id'):
            continue
        if task.get('source_metadata_status')=='blocked':
            from ingest_source_metadata import verify_local
            verify_local(task)
            docs=lookup(task['dataset_id'],task['remote_id'])
            if (len(docs)!=1 or docs[0].get('id')!=task['remote_id']
                    or docs[0].get('name')!=task['remote_name']):
                raise ValueError('Metadata retry remote identity is not confirmed')
            count+=int(queue.request_source_metadata_retry(task['id'],task['updated_at'],
                task['remote_id'],reason='workbench_metadata_retry'))
            continue
        if task['status']=='accepted' and task['post_status']=='needs_review':
            queue.transition(task['id'],'accepted','accepted',post_status='pending')
            count+=1
            continue
        if task['status'] not in ('technical_review','outcome_unresolved','quality_hold','quality_reject'):
            continue
        docs=lookup(task['dataset_id'],task['remote_id'])
        if len(docs)!=1 or docs[0].get('id')!=task['remote_id'] or docs[0].get('name')!=task['remote_name']:
            raise ValueError('Review remote document identity is not confirmed')
        remote_state,_=normalize_remote_doc(docs[0])
        if remote_state in ('running','queued','submitted'):
            queue.transition(task['id'],task['status'],'submitted')
            continue
        if remote_state not in ('failed','cancelled','done'):
            raise ValueError('Review remote parse is not in a confirmed terminal state')
        queue.request_review(task['id'],'Workbench selected explicit second-pass review')
        count+=1
    return count
