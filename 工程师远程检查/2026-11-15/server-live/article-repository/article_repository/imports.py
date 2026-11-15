"""Read complete Qdrant folders and existing Prefect registration parameters."""
import json
import ntpath
from datetime import datetime
from pathlib import PureWindowsPath

import httpx
from paperflow_qdrant.library import stable_id


def folder_key(path):
    return 'folder:'+ntpath.normcase(ntpath.normpath(str(path))).replace('\\','/')


def original_title(reg,owners):
    """Use the source-bound intake title, never the abstract's translated title."""
    sha=reg['paper']['body']['sha256']
    title=reg.get('paper_title')
    if (reg.get('title_status')=='已提取' and isinstance(title,str) and title.strip()
            and reg.get('display_hash')==sha and owners
            and all(owner.get('paperflow_source_sha256')==sha for owner in owners)):
        return title.strip()
    return '题名待核实（'+sha[:12]+'）'


def completion_matches(run,body,completion,post):
    reg=run['parameters']['registered']
    final=completion.get('data') or {}; receipt=post.get('data') or {}
    data=final.get('data') or {}; prepared=receipt.get('data') or {}
    identity=data.get('identity') or {}
    owners=list(body.get('dataset_meta_fields',{}).values())
    if not owners or final.get('kind')!='rag_complete' or not final.get('folder_completion') or data.get('status')!='complete': return False
    if any(identity.get(k)!=reg.get(k) for k in ('task_id','group_id','paper_id','receipt_id')): return False
    if identity.get('task_run_id')!=run['id'] or identity.get('attempt_id')!=run['id']: return False
    if receipt.get('kind')!='postprocessing_complete' or prepared.get('validation_status')!='passed' or prepared.get('identity')!=identity: return False
    if datetime.fromisoformat(post['created'].replace('Z','+00:00'))>datetime.fromisoformat(completion['created'].replace('Z','+00:00')): return False
    return all(owner.get('paperflow_source_sha256')==reg['paper']['body']['sha256'] and
        owner.get('paperflow_manifest_sha256')==(prepared.get('body') or {}).get('manifest_sha256') for owner in owners)


def new_record(run,body,documents,majors):
    reg=run['parameters']['registered']; paper=reg['paper']
    meta=body['meta_fields']
    owners=list(body.get('dataset_meta_fields',{}).values())
    owner=owners[0] if owners else {}
    folder=stable_id(reg['task_id'],reg['group_id'],owner['paperflow_source_sha256'],owner['paperflow_manifest_sha256'])
    archive_root=PureWindowsPath(r'\\zwjy-nas\paper_ai\论文原件库')
    files=[]
    for row in paper['archive_files']:
        location=PureWindowsPath('Prefect五步/统一五步正式')/reg['task_id']/reg['group_id']/row['relative']
        files.append({'id':row['file_id'],'name':PureWindowsPath(row['relative']).name,
            'relative':location.as_posix(),'sha256':row.get('sha256'),'bytes':row.get('bytes'),
            'kind':'原文' if row['file_id']==paper['body']['file_id'] else '附件','availability':'unknown'})
    profile=json.loads(meta.get('two_pass_profile_json','{}'))
    codes=[m['code'] for m in majors if m['paper_dataset_id'] in body['dataset_ids']]
    raw_bucket=stable_id('paperflow-unstructured-retention','body')
    unstructured=(meta.get('paperflow_format_status')=='unstructured_retained'
        and profile.get('format_status')=='unstructured_retained' and not profile.get('routing_candidates')
        and body['dataset_ids']==[raw_bucket])
    if not unstructured and (not codes or len(codes)!=len(set(body['dataset_ids']))): raise ValueError('专业归属未全部确认')
    person=paper['person_dir']
    return {'source_key':folder_key(person),'folder_name':ntpath.basename(person),
            'title':original_title(reg,owners),
            'profile':profile,'files':files,'inventory_complete':True,'majors':codes,
            'source':'qdrant_prefect','migration':'complete','qdrant':{'folder_ids':[folder],'source_folder_id':folder},
            'source_updated_at':run.get('expected_start_time') or run['created'],
            'source_sha256':owner['paperflow_source_sha256']}


class Importer:
    def __init__(self,catalog,sync,files,prefect_url):
        self.catalog,self.sync,self.files,self.prefect_url=catalog,sync,files,prefect_url.rstrip('/')
        self.seen=set()
        self.restored=False

    def refresh(self):
        if not self.restored:
            # The persistent source ledger survives service restarts. A failed
            # read must stop this scan rather than replay old imports and edits.
            self.seen.update(self.catalog.imported_folders())
            self.restored=True
        manifests=self.sync.catalog_manifests()
        groups={}
        for row in manifests:
            # Repository revisions are already bound to their existing row.
            if row.meta['document'].get('meta_fields',{}).get('repository_article_id'): continue
            groups.setdefault(row.meta['folder_id'],[]).append(row.meta['document'])
        pending=set(groups)-self.seen
        if not pending: return
        body_by_identity={}
        for folder in pending:
            for doc in groups[folder]:
                owner=next(iter(doc.get('dataset_meta_fields',{}).values()),{})
                if owner.get('paperflow_role')=='body':
                    body_by_identity[(owner.get('paperflow_source_sha256'),owner.get('paperflow_manifest_sha256'),folder)]=doc
        matched=0; majors=self.catalog.majors(); accepted={}
        with httpx.Client(base_url=self.prefect_url,timeout=30,trust_env=False) as client:
            offset=0
            while pending and offset<100000:
                response=client.post('/flow_runs/filter',json={'flows':{'name':{'any_':['paperflow-v3-paper']}},
                    'limit':200,'offset':offset,'sort':'EXPECTED_START_TIME_DESC'})
                response.raise_for_status(); runs=response.json()
                if not runs: break
                candidates={}
                for run in runs:
                    reg=(run.get('parameters') or {}).get('registered') or {}
                    if not reg.get('paper'): continue
                    for (source,manifest,folder),body in body_by_identity.items():
                        if folder not in pending or source!=reg['paper']['body']['sha256']: continue
                        if stable_id(reg.get('task_id'),reg.get('group_id'),source,manifest)!=folder: continue
                        candidates[run['id']]=(run,folder,body)
                confirmed=set(); completions=[]
                if candidates:
                    artifact_offset=0
                    while True:
                        page=client.post('/artifacts/filter',json={'artifacts':{
                            'flow_run_id':{'any_':list(candidates)},
                            'type':{'any_':['paperflow-v3-folder-completion']}},
                            'limit':200,'offset':artifact_offset})
                        page.raise_for_status(); receipts=page.json()
                        for receipt in receipts:
                            data=receipt.get('data') or {}
                            if data.get('kind')=='rag_complete': completions.append(receipt)
                        artifact_offset+=len(receipts)
                        if len(receipts)<200: break
                    posts=[]; post_offset=0
                    while True:
                        reply=client.post('/artifacts/filter',json={'artifacts':{
                            'flow_run_id':{'any_':list(candidates)},'type':{'any_':['paperflow-v3-receipt']},
                            'key':{'like_':'postprocessingcomplete'}},'limit':200,'offset':post_offset})
                        reply.raise_for_status(); rows=reply.json(); posts.extend(rows); post_offset+=len(rows)
                        if len(rows)<200: break
                    for completion in completions:
                        run,folder,body=candidates[completion['flow_run_id']]
                        if any(p['flow_run_id']==run['id'] and completion_matches(run,body,completion,p) for p in posts):
                            confirmed.add(run['id'])
                for run_id in confirmed:
                        run,folder,body=candidates[run_id]
                        record=new_record(run,body,groups[folder],majors)
                        key=record['source_key']
                        previous=accepted.get(key)
                        if previous:
                            folders=previous['qdrant']['folder_ids']+[folder]
                            if previous['source_updated_at']>record['source_updated_at']: record=previous
                            record['qdrant']['folder_ids']=list(dict.fromkeys(folders))
                        accepted[key]=record
                        pending.remove(folder)
                offset+=len(runs)
                if len(runs)<200: break
        for record in accepted.values():
            try: record['files']=self.files.observe(record['files'])
            except OSError: pass
            item=self.catalog.register(record)
            self.seen.update(record['qdrant']['folder_ids']); matched+=1
            if item['manually_edited']:
                from psycopg.errors import LockNotAvailable
                try: self.catalog.synchronize(item['id'],self.sync)
                except LockNotAvailable: pass
        self.catalog.source_status('新链路',{'completed_folders_observed':len(groups),'registered_this_scan':matched,
            'unmatched_folders':len(pending),'scope':'仅登记已完整入库且匹配原始文件夹的文章'})
