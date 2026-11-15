"""Deliver retained originals via RAGFlow Files, inside the existing paper slot."""
from __future__ import annotations

import copy
import hashlib
import json
import time
from pathlib import Path
from urllib.parse import quote, urlencode

from engine import digest
from preextract_publish import _write_json
from upload_tracking import service_key

PLAN_NAME='associated-upload-plan.json'
RECEIPT_NAME='associated-delivery-receipt.json'
KINDS={'drawing','source_code','image','supporting_document'}


def _image_purpose_allowed(row):
    from material_intake import IMAGE_SUFFIXES
    path=Path(row['original_path'])
    if path.suffix.lower() not in IMAGE_SUFFIXES:return True
    from material_roles import classify_document
    name=row.get('source_name') or Path(row.get('source_path') or path).name
    role=classify_document(dict(path=str(path),name=name))
    return role['action']=='associated' and role['kind'] in KINDS


def _load(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def _stop(stop):
    if stop is not None and stop.is_set():raise InterruptedError('已停止附件交付，原件和回执保留')


def write_plan(bundle, prepared, work):
    """Seal a private plan only when creating a new qualified publication."""
    work=Path(work).resolve()
    files=[]
    def collect(row, public_ref):
        if row.get('kind')=='archive':
            # An original ZIP may also contain process paperwork. Send retained
            # leaves only; do not smuggle excluded members inside a raw archive.
            for number,member in enumerate(row.get('members',[]),1):
                collect(member,public_ref+'/'+str(number))
            return
        if row.get('kind') not in KINDS or row.get('purpose_pending'):return
        path=Path(row['original_path']).resolve()
        sha=row['sha256']
        if (not path.is_relative_to(work/'private') or not path.is_file()
                or bundle.get('file_hashes',{}).get(str(path))!=sha or digest(path)!=sha):
            raise ValueError('附件原件不在已核验的私有材料清单中')
        if not _image_purpose_allowed(row):return
        files.append(dict(file_ref=public_ref,kind=row['kind'],format=row['format'],sha256=sha,
            original_path=str(path),bytes=path.stat().st_size,parse_status=row.get('parse_status','pending'),
            source_name=Path(row.get('source_path') or path).name,
            association_basis=row.get('association_basis','同一选中项目唯一正文')))
    for number,row in enumerate(bundle.get('attachments',[]),1):
        if row.get('association_status')=='candidate':collect(row,'attachment_'+str(number))
    if not files:return None
    plan=dict(schema='associated-original-plan/1',task=prepared['task'],
        manifest_sha256=prepared['validation']['manifest_sha256'],routes=prepared['packet']['routes'],
        material_sha256=digest(work/'material.json'),files=files)
    target=work/'private'/PLAN_NAME
    if target.exists() and _load(target)!=plan:raise ValueError('已有附件交付计划不同，未覆盖')
    if not target.exists():_write_json(target,plan)
    prepared['associated_upload_plan_sha256']=digest(target)
    return plan


def load_plan(prepared, work):
    """Old sealed/in-flight jobs keep their original delivery contract."""
    expected=prepared.get('associated_upload_plan_sha256')
    if not expected:return None
    work=Path(work).resolve();path=work/'private'/PLAN_NAME
    if digest(path)!=expected:raise ValueError('附件交付计划发生变化')
    plan=_load(path)
    if (plan.get('schema')!='associated-original-plan/1' or plan.get('task')!=prepared['task']
            or plan.get('manifest_sha256')!=prepared['validation']['manifest_sha256']
            or plan.get('material_sha256')!=digest(work/'material.json')):
        raise ValueError('附件计划与正文封包或原材料身份不一致')
    bundle=_load(work/'material.json')
    if bundle['source']['source_sha256']!=prepared['task']['source_sha256']:
        raise ValueError('附件计划所属正文发生变化')
    if prepared.get('packet') and plan['routes']!=prepared['packet']['routes']:
        raise ValueError('附件与正文的专业路由不一致')
    for row in plan['files']:
        original=Path(row['original_path']).resolve()
        if (row['kind'] not in KINDS or not original.is_relative_to(work/'private')
                or bundle.get('file_hashes',{}).get(str(original))!=row['sha256']
                or not original.is_file() or digest(original)!=row['sha256']):
            raise ValueError('附件原件缺失或发生变化；未交付')
        if not _image_purpose_allowed(row):
            raise ValueError('历史附件计划含过程表单或用途未确认图片；原计划和原件保留，未上传')
    return plan


def _files(client, parent=None, name=None, stop=None):
    rows=[];seen=set()
    for page in range(1,10001):
        _stop(stop)
        query=dict(page=page,page_size=100)
        if parent:query['parent_id']=parent
        if name:query['keywords']=name
        result=client.request('GET','/files?'+urlencode(query))
        batch=result.get('files') if isinstance(result,dict) else None
        if not isinstance(batch,list):raise ValueError('RAGFlow 附件文件列表无效')
        for row in batch:
            if not row.get('id') or row['id'] in seen:raise ValueError('RAGFlow 附件分页身份异常')
            seen.add(row['id'])
            if name is None or row.get('name')==name:rows.append(row)
        if len(batch)<100:return rows
    raise ValueError('RAGFlow 附件文件列表超过核对上限')


def deliver_associated(prepared, body_receipt, work, client, *, stop=None, progress=lambda *args:None):
    from paperflow.rag import RagPublisher
    work=Path(work).resolve();plan=load_plan(prepared,work)
    if plan is None or not plan['files']:return None
    if body_receipt.get('status')!='complete':raise ValueError('正文尚未确认，不提交关联附件')
    if {r['code']:r for r in plan['routes']}!={r['code']:r for r in prepared['packet']['routes']}:
        raise ValueError('附件与正文的专业路由不一致')
    identity=dict(service=service_key(client.base_url),task=prepared['task'],
        manifest_sha256=plan['manifest_sha256'],plan_sha256=prepared['associated_upload_plan_sha256'])
    path=work/RECEIPT_NAME
    receipt=_load(path) if path.exists() else dict(schema='associated-original-delivery/1',
        identity=identity,status='pending',files={},folder={},body_links={})
    if receipt.get('schema')!='associated-original-delivery/1' or receipt.get('identity')!=identity:
        raise ValueError('已有附件回执的服务或正文身份不同')
    def save():_write_json(path,receipt)
    checker=RagPublisher(client)
    def one_file(parent,name):
        rows=_files(client,parent,name,stop)
        if len(rows)>1:raise ValueError('附件远端名称存在多份，保留回执等待核对')
        return rows[0] if rows else None
    try:
        _stop(stop);receipt['status']='pending';save()
        folder=receipt['folder'];folder_name='PF_attachments_'+plan['manifest_sha256']
        remote=one_file(None,folder_name)
        if remote and remote.get('type')!='folder':raise ValueError('附件文件夹名称冲突')
        if not remote:
            if folder.get('intent'):raise ValueError('附件文件夹写入未确认，未重复创建')
            folder.update(name=folder_name,intent=True);save();_stop(stop)
            remote=client.request('POST','/files',dict(name=folder_name,type='folder'))
        if not remote.get('id') or (folder.get('id') and folder['id']!=remote['id']):
            raise ValueError('附件远端文件夹身份不同')
        folder.update(id=remote['id'],name=folder_name);save()
        for number,row in enumerate(plan['files'],1):
            _stop(stop);progress('RAGFlow 保存关联原件',f'{number}/{len(plan["files"])}')
            entry=receipt['files'].setdefault(row['file_ref'],{})
            ref_hash=hashlib.sha256(row['file_ref'].encode()).hexdigest()[:12]
            name='PF_A_'+plan['manifest_sha256'][:16]+'_'+ref_hash+'_'+row['sha256'][:16]+row['format']
            remote=one_file(folder['id'],name)
            if not remote:
                if entry.get('upload_intent'):raise ValueError('附件上传未确认，未重复发送原件')
                entry.update(name=name,upload_intent=True,sha256=row['sha256']);save();_stop(stop)
                content=Path(row['original_path']).read_bytes()
                if hashlib.sha256(content).hexdigest()!=row['sha256']:raise ValueError('附件读取时发生变化')
                fid=client.upload_file(folder['id'],name,content)
                entry['file_id']=fid;save()
                remote=one_file(folder['id'],name)
            if not remote or (entry.get('file_id') and remote['id']!=entry['file_id']):
                raise ValueError('附件原件远端身份未确认')
            fid=remote['id']
            if hashlib.sha256(client.download_file(fid)).hexdigest()!=row['sha256']:
                raise ValueError('附件原件下载哈希不一致')
            entry.update(file_id=fid,name=name,sha256=row['sha256']);save()
            expected_datasets={r['paper_dataset_id'] for r in plan['routes']}
            def links(file):
                result={}
                for link in file.get('kbs_info',[]):
                    ds=link.get('kb_id');did=link.get('document_id')
                    if ds in expected_datasets:
                        if ds in result or not did:raise ValueError('附件知识库连接不唯一')
                        result[ds]=did
                return result
            attached=links(remote)
            if set(attached)!=expected_datasets:
                if not entry.get('link_intent'):
                    entry['link_intent']=True;save();_stop(stop)
                    client.request('POST','/files/link-to-datasets?mode=add',
                        dict(file_ids=[fid],kb_ids=sorted(expected_datasets-set(attached))))
                for attempt in range(40):
                    _stop(stop)
                    if stop is not None:stop.wait(.25)
                    else:time.sleep(.25)
                    remote=one_file(folder['id'],name)
                    if not remote or remote['id']!=fid:raise ValueError('附件连接期间文件身份变化')
                    attached=links(remote)
                    if set(attached)==expected_datasets:break
                if set(attached)!=expected_datasets:raise ValueError('附件连接尚未确认，未重复提交连接')
            entry['targets']={}
            for route in plan['routes']:
                _stop(stop);code=route['code'];ds=route['paper_dataset_id'];did=attached[ds]
                parent=body_receipt['targets'][code]['body']
                if parent['dataset_id']!=ds:raise ValueError('附件所属正文知识库不同')
                meta=dict(paperflow_role='associated_original',paperflow_task=prepared['task']['task_id'],
                    paperflow_source_sha256=prepared['task']['source_sha256'],
                    paperflow_manifest_sha256=plan['manifest_sha256'],paperflow_specialty=code,
                    paperflow_parent_document_id=parent['document_id'],paperflow_attachment_ref=row['file_ref'],
                    paperflow_original_sha256=row['sha256'],paperflow_parse_status=row['parse_status'])
                docs=checker.documents(ds,id=did)
                if len(docs)!=1:raise ValueError('附件知识库记录未唯一读回')
                observed=docs[0].get('meta_fields') or {}
                if observed and any(observed.get(k)!=v for k,v in meta.items()):
                    raise ValueError('附件已有其他正文关联，未覆盖')
                if hashlib.sha256(checker._download(ds,did)).hexdigest()!=row['sha256']:
                    raise ValueError('知识库附件原件下载不一致')
                if any(observed.get(k)!=v for k,v in meta.items()) or docs[0].get('status')!='0':
                    client.request('PATCH',f'/datasets/{quote(ds,safe="")}/documents/{quote(did,safe="")}',
                        dict(meta_fields=meta,enabled=0))
                docs=checker.documents(ds,id=did)
                if (len(docs)!=1 or docs[0].get('status')!='0'
                        or any((docs[0].get('meta_fields') or {}).get(k)!=v for k,v in meta.items())
                        or docs[0].get('chunk_count',0)!=0 or docs[0].get('run','UNSTART') not in {'UNSTART','0',0}):
                    raise ValueError('附件关联、存储或未解析状态未确认')
                entry['targets'][code]=dict(dataset_id=ds,document_id=did,parent_document_id=parent['document_id'],verified=True)
                save()
            entry['status']='verified';save()
        for route in plan['routes']:
            _stop(stop);code=route['code'];ds=route['paper_dataset_id'];parent=body_receipt['targets'][code]['body']
            records=[dict(file_ref=row['file_ref'],sha256=row['sha256'],format=row['format'],kind=row['kind'],
                parse_status=row['parse_status'],document_id=receipt['files'][row['file_ref']]['targets'][code]['document_id'])
                for row in plan['files']]
            value=json.dumps(records,ensure_ascii=False,separators=(',',':'))
            docs=checker.documents(ds,id=parent['document_id'])
            if len(docs)!=1:raise ValueError('附件所属正文记录缺失')
            fields=copy.deepcopy(docs[0].get('meta_fields') or {})
            for key,expected in (('paperflow_source_sha256',prepared['task']['source_sha256']),
                                 ('paperflow_manifest_sha256',plan['manifest_sha256'])):
                if fields.get(key)!=expected:raise ValueError('附件所属正文身份不同')
            if fields.get('paperflow_attachment_associations_json')!=value:
                if fields.get('paperflow_attachment_associations_json'):
                    raise ValueError('正文已有不同附件关联，未覆盖')
                fields['paperflow_attachment_associations_json']=value
                client.request('PATCH',f'/datasets/{ds}/documents/{parent["document_id"]}',dict(meta_fields=fields))
            observed=checker.documents(ds,id=parent['document_id'])
            if len(observed)!=1 or observed[0].get('meta_fields',{}).get('paperflow_attachment_associations_json')!=value:
                raise ValueError('正文的附件关联未读回确认')
            receipt['body_links'][code]=dict(dataset_id=ds,document_id=parent['document_id'],verified=True);save()
        receipt.update(status='complete',total_count=len(plan['files']),verified_count=len(plan['files']));save()
        return receipt
    except Exception as exc:
        receipt.update(status='pending',error_type=type(exc).__name__);save()
        raise


def receipt_summary(work):
    path=Path(work)/RECEIPT_NAME
    if not path.exists():return None
    receipt=_load(path)
    return dict(schema=receipt['schema'],status=receipt['status'],receipt_path=str(path),
        plan_sha256=receipt['identity']['plan_sha256'],service=receipt['identity']['service'],
        total_count=receipt.get('total_count',len(receipt['files'])),
        verified_count=sum(r.get('status')=='verified' for r in receipt['files'].values()))
