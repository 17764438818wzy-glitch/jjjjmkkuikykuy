"""Program-only retention. Never executes source or claims native CAD was read."""
from __future__ import annotations

import copy
import hashlib
import json
import shutil
import zipfile
import uuid
from collections import Counter
from functools import lru_cache
from pathlib import Path, PurePosixPath
from PIL.Image import DecompressionBombError

from archive_identity import record_key, member_fields
from engine import OFFICE_FORMATS, digest, write_json
from material_intake import IMAGE_SUFFIXES, TEXT_SUFFIXES, MaterialPending, read_material

PREVIEW_BYTES = 512 * 1024
METADATA_LIMIT = 64
DOCUMENTS = {'.docx', '.pdf', '.txt', '.md'} | OFFICE_FORMATS
ARCHIVES = {'.zip', '.rar', '.7z'}
DRAWINGS = {'.dwg', '.dxf', '.step', '.stp', '.iges', '.igs', '.sldprt', '.sldasm',
            '.slddrw', '.prt', '.asm', '.stl', '.obj', '.fbx', '.3ds', '.skp', '.rvt'}


def is_attachment_container(record,context):
    """Keep complete/uncertain projects on the original document expansion path."""
    if context is None or not context.relations.get(record_key(record),{}).get('related_record_keys'):
        return False
    source=Path(record['path'])
    try:
        if source.suffix.lower()=='.zip':
            with zipfile.ZipFile(source) as archive:names=[i.filename for i in archive.infolist() if not i.is_dir()]
        else:return False  # Unknown container contents keep existing handling.
    except (OSError,ValueError,zipfile.BadZipFile):return False
    paths=[PurePosixPath(n.replace('\\','/')) for n in names]
    drawing_stems={p.stem.casefold() for p in paths if p.suffix.lower() in DRAWINGS}
    for path in paths:
        suffix=path.suffix.lower()
        if suffix in ARCHIVES or suffix in OFFICE_FORMATS|{'.docx'}:return False
        if suffix in {'.txt','.md'} and path.stem.casefold() not in {'readme','license','changelog','requirements'}:return False
        if suffix=='.pdf' and path.stem.casefold() not in drawing_stems and not any(
                word in (str(path)+source.stem) for word in ('图纸','装配图','零件图','电路图','接线图','CAD')):
            return False
    return bool(paths)


def material_kind(record):
    path=Path(record.get('path') or record.get('source_path',''))
    suffix=path.suffix.lower()
    if suffix in ARCHIVES:return 'archive'
    if suffix in DOCUMENTS and path.stem.casefold() not in {'readme','license','changelog','requirements'}:
        from material_roles import classify_document
        return classify_document(record)['kind']
    if suffix in DRAWINGS:return 'drawing'
    if suffix in IMAGE_SUFFIXES:return 'image'
    if suffix in TEXT_SUFFIXES:return 'source_code'
    return 'unknown'


def _location(record):
    """Separate archive projects from neighboring loose files and other archives."""
    member=record.get('archive_member_path')
    if member:
        return ('archive',str(record.get('input_source_path','')).replace('\\','/').casefold(),
                record.get('archive_source_hash','')), PurePosixPath(member).parent.parts
    path=Path(record.get('path') or record.get('source_path','')).resolve()
    return ('folder',str(path.anchor).casefold()), tuple(p.casefold() for p in path.parent.parts)


class MaterialContext:
    """Index the selected batch once, then look up only ancestor directories."""
    def __init__(self,records):
        self.records=list(records)
        self.documents={};self.relations={};self.by_document={};self.uncertain_documents=set();self.primary_anchors=set()
        self.equivalents={};hash_groups={}
        kinds=[(r,material_kind(r)) for r in self.records]
        for record,kind in kinds:
            from material_roles import _name
            if kind=='unknown' and Path(record['path']).suffix.lower() in DOCUMENTS and any(
                    word in _name(record).casefold() for word in ('主修','正文','说明书','paper','thesis','explanation')):
                self.uncertain_documents.add(_location(record))
            if kind=='document':
                self.documents.setdefault(_location(record),set()).add(record_key(record))
                if any(word in _name(record) for word in ('主修','设计说明书','论文正文')):
                    self.primary_anchors.add(record_key(record))
                try:
                    path=Path(record['path']).resolve();stat=path.stat()
                    sha=_document_hash(str(path),stat.st_size,stat.st_mtime_ns)
                    hash_groups.setdefault(sha,set()).add(record_key(record))
                except OSError:pass
        for keys in hash_groups.values():
            for key in keys:self.equivalents[key]=keys
        for record,kind in kinds:
            if kind=='document':continue
            if kind=='administrative':continue
            relation=self.lookup(record);self.relations[record_key(record)]=relation
            for key in relation['related_record_keys']:
                self.by_document.setdefault(key,[]).append(record)

    def lookup(self,record):
        domain,parts=_location(record);closest=[]
        for size in range(len(parts),-1,-1):
            if (domain,parts[:size]) in self.uncertain_documents:break
            matches=self.documents.get((domain,parts[:size]))
            if matches:
                anchors=matches.intersection(self.primary_anchors)
                closest=sorted(anchors if len(anchors)==1 else matches);break
        blocked=any((domain,parts[:size]) in self.uncertain_documents for size in range(len(parts),-1,-1))
        if not closest and not blocked and domain[0]=='archive' and record.get('input_source_path'):
            # A separately selected explanation may sit beside its drawing/code
            # archive. Prefer selected documents inside the archive first.
            outer_domain,outer_parts=_location(dict(path=record['input_source_path']))
            for size in range(len(outer_parts),-1,-1):
                matches=self.documents.get((outer_domain,outer_parts[:size]))
                if matches:
                    closest=sorted(matches);break
        return dict(association_status='candidate' if len(closest)==1 else 'unassigned',
            related_record_keys=closest if len(closest)==1 else [],
            same_body_record_keys=sorted(self.equivalents.get(closest[0],{closest[0]})) if len(closest)==1 else [],
            association_basis='同一选中项目目录下只有一份最近层级说明文档；仅候选关联，未核实内容对应'
                if len(closest)==1 else '缺少唯一说明文档或存在多份候选，保留资料等待确认关联')


def association(record, records):
    context=records if isinstance(records,MaterialContext) else MaterialContext(records)
    return copy.deepcopy(context.relations.get(record_key(record)) or context.lookup(record))


def related_records(document, records):
    context=records if isinstance(records,MaterialContext) else MaterialContext(records)
    key=record_key(document);result={}
    for owner in sorted(context.equivalents.get(key,{key})):
        for record in context.by_document.get(owner,[]):result.setdefault(record_key(record),record)
    return list(result.values())


@lru_cache(maxsize=24000)
def _document_hash(path,size,mtime):
    return digest(Path(path))


def preserve_material(record, destination, records, *, stop=None, container_depth=0):
    source=Path(record['path']);destination=Path(destination)
    if stop is not None and stop.is_set():raise InterruptedError('已停止，原件保留')
    kind=material_kind(record)
    if kind=='administrative':
        from material_roles import classify_document
        administrative=classify_document(record)
        if administrative['action']!='delete' or not administrative['confirmed']:kind='unknown'
    if kind=='administrative':
        # Administrative forms are never copied into a primary's attachment
        # inventory, even when an associated ZIP is expanded before its worker.
        if container_depth:
            # Only safely expanded members in this container's private subtree
            # may be removed here. Top-level selected forms use their worker.
            if not source.resolve().is_relative_to(destination.resolve().parent):
                raise ValueError('过程表单成员不在当前压缩包私有目录内')
            from material_roles import classify_document,delete_confirmed_form
            role=classify_document(record,fresh=True)
            if role['action']=='delete' and role['confirmed']:
                audit=destination/'excluded'/hashlib.sha256(record_key(record).encode()).hexdigest()[:24]
                audit.mkdir(parents=True,exist_ok=True)
                delete_confirmed_form(record,audit,role,stop=stop)
        return dict(kind='administrative',parse_status='excluded',retained_file_hashes={},
            source_path=str(source),source_key=record_key(record),limits=['过程表单按用途排除，不保存为附件'])
    sha=digest(source);key=record_key(record)
    ref='associated_'+hashlib.sha256((key+'\0'+sha).encode()).hexdigest()[:24]
    folder=destination/ref;folder.mkdir(parents=True,exist_ok=True)
    original=folder/('original'+source.suffix.lower())
    if original.exists():
        if digest(original)!=sha:raise ValueError('已保存关联原件发生变化，未覆盖')
    else:shutil.copy2(source,original)
    if digest(original)!=sha or digest(source)!=sha:raise ValueError('关联原件复制期间发生变化')
    info=dict(file_ref=ref,source_key=key,source_path=str(source),sha256=sha,
        kind=kind,format=source.suffix.lower(),bytes=original.stat().st_size,
        original_path=str(original.resolve()),remote_original_uploaded=False,
        parse_status='pending',preview_path='',preview_truncated=False,
        limits=['原件先私有保存；RAGFlow 上传状态以关联交付回执为准；源代码从未执行',
                '附件未提交模型阅读；不能据目录关系推断设计功能、尺寸或装配关系'],
        **member_fields(record),**association(record,records))
    try:
        readable=original
        if info['kind']=='archive' and container_depth<2:
            from material_intake import extract_material_archive
            prior=folder/'associated-material.json'
            cached=json.loads(prior.read_text(encoding='utf8')) if prior.exists() else {}
            if (cached.get('sha256')==sha and cached.get('context_policy')=='nested-owning-body/1' and cached.get('members') is not None and
                    all(Path(p).is_file() and digest(Path(p))==h for p,h in cached.get('retained_file_hashes',{}).items())):
                members=copy.deepcopy(cached['members'])
            else:
                # Interrupted attempts keep their files; a fresh safe extraction
                # avoids open('xb') collisions without overwriting prior evidence.
                extracted=extract_material_archive(original,folder/'expanded-attempts'/uuid.uuid4().hex)
                members=[];member_records=[]
                for row in extracted:
                    if stop is not None and stop.is_set():raise InterruptedError('已停止，关联原件保留')
                    member=Path(row['extracted_path'])
                    member_record=dict(path=str(member),name=member.name,
                        input_source_path=record.get('input_source_path',str(source)),
                        archive_source_hash=record.get('archive_source_hash',sha),
                        archive_member_path='/'.join(v for v in (record.get('archive_member_path',''),row['inner_path']) if v))
                    member_records.append((row,member_record))
                context=records if isinstance(records,MaterialContext) else MaterialContext(records)
                member_context=MaterialContext([*context.records,*[r for _,r in member_records]])
                for row,member_record in member_records:
                    kept=preserve_material(member_record,folder/'members',member_context,stop=stop,container_depth=container_depth+1)
                    if kept['parse_status']!='excluded':members.append(kept)
                    else:info.setdefault('excluded_members',[]).append(dict(path=row['inner_path'],reason='过程表单'))
            info['members']=members
            info['context_policy']='nested-owning-body/1'
            readable_count=sum(m['parse_status']=='readable' for m in members)
            info['limits'].append(f'压缩包成员已逐件私有保存：{len(members)} 件，静态可读 {readable_count} 件，待解析 {len(members)-readable_count} 件')
            material=dict(status='readable' if members and readable_count==len(members) else 'pending',
                          reason='压缩包已展开；部分成员内容待解析',text='',images=[])
        else:material=None
        if source.suffix.lower() in TEXT_SUFFIXES and original.stat().st_size>PREVIEW_BYTES:
            # Reuse the existing multi-encoding reader on a bounded private copy.
            readable=folder/('reading-window'+source.suffix.lower())
            with original.open('rb') as stream:readable.write_bytes(stream.read(PREVIEW_BYTES))
            info['preview_truncated']=True
        if material is not None:pass
        elif source.suffix.lower() in TEXT_SUFFIXES | IMAGE_SUFFIXES | {'.dxf'}:
            material=read_material(readable,folder/'preview')
        else:material=dict(status='pending',reason='当前读取器尚不支持此原生格式')
        info['parse_status']='readable' if material['status']=='readable' else 'pending'
        info['limits']+=material.get('limitations',[])
        text=material.get('text','')
        if text:
            encoded=text.encode('utf8')
            if len(encoded)>PREVIEW_BYTES:
                text=encoded[:PREVIEW_BYTES].decode('utf8',errors='ignore');info['preview_truncated']=True
            preview=folder/'preview.txt';preview.write_bytes(text.encode('utf8'))
            info['preview_path']=str(preview.resolve())
        elif material.get('preview_path'):
            info['preview_path']=str(Path(material['preview_path']).resolve())
        elif material.get('images'):
            # Original image is already a valid local preview; never execute it.
            info['preview_path']=str(original.resolve())
        if material['status']!='readable':info['limits'].append(material.get('reason','尚未取得可读取内容'))
        if info['preview_truncated']:info['limits'].append('仅生成前512 KiB静态预览；完整原件已保留，未声称全文已读')
    except InterruptedError:
        raise
    except (OSError,ValueError,UnicodeError,ImportError,DecompressionBombError,MaterialPending):
        info['limits'].append('静态预览未成功；原件已保存，待解析')
        info['parse_status']='pending'
    retained={original.resolve()}
    if info['preview_path']:retained.add(Path(info['preview_path']))
    info['retained_file_hashes']={str(p):digest(p) for p in retained}
    for member in info.get('members',[]):info['retained_file_hashes'].update(member['retained_file_hashes'])
    write_json(folder/'associated-material.json',info)
    return info


def prepare_inventory(document, records, destination, *, stop=None):
    records=records if isinstance(records,MaterialContext) else MaterialContext(records)
    result=[]
    key=record_key(document);owners=records.equivalents.get(key,{key})
    def owned(row):
        if row['kind']=='archive' and row.get('members') is not None:
            row['members']=[m for m in row['members'] if owned(m)]
            return bool(row['members'])
        return row['kind'] not in {'document','administrative'} and bool(owners.intersection(
            set(row.get('related_record_keys',[]))|set(row.get('same_body_record_keys',[]))))
    for record in related_records(document,records):
        saved=preserve_material(record,destination,records,stop=stop)
        if saved['parse_status']!='excluded' and owned(saved):result.append(saved)
    return result


def inventory_hash(rows):
    facts=[{k:r.get(k) for k in ('file_ref','sha256','kind','format','bytes','parse_status',
            'association_status','related_record_keys','association_basis','preview_truncated','limits')} for r in rows]
    return hashlib.sha256(json.dumps(facts,sort_keys=True,ensure_ascii=False).encode()).hexdigest()


def reading_inventory(rows):
    """No filenames, source snippets, original paths or private storage locations."""
    allowed={'file_ref','kind','format','bytes','sha256','parse_status','preview_truncated',
             'association_status','association_basis','limits'}
    return [{k:copy.deepcopy(v) for k,v in row.items() if k in allowed} for row in rows[:METADATA_LIMIT]]


def inventory_summary(rows):
    return dict(retained_count=len(rows),listed_metadata_count=min(len(rows),METADATA_LIMIT),
        model_content_provided=False,by_kind=dict(Counter(r.get('kind','unknown') for r in rows)),
        by_parse_status=dict(Counter(r.get('parse_status','pending') for r in rows)),
        notes='仅展开有界登记信息；全部原件和逐件清单已私有保存，未展开不等于排除，附件正文与像素未提交模型')


def add_inventory_decisions(candidate, rows):
    """Program facts supplement, never replace, the model's semantic decisions."""
    attachments=candidate.get('attachments')
    if not isinstance(attachments,list):return candidate
    existing={a.get('file_ref'):a for a in attachments if isinstance(a,dict)}
    for row in rows[:METADATA_LIMIT]:
        if 'parse_status' not in row:continue  # Historical non-program inventories.
        if row['file_ref'] in existing:
            limits=existing[row['file_ref']].setdefault('limits',[])
            if isinstance(limits,list):
                for limit in row['limits']:
                    if limit not in limits:limits.append(limit)
            continue
        attachments.append(dict(file_ref=row['file_ref'],kind=row['kind'],decision='uncertain',
            reason='程序保存的关联资料；未进行语义审阅',
            description='原件已私有保存；'+('已有本地静态预览' if row['parse_status']=='readable' else '原生内容待解析'),
            learning_value=None,related_article_title=None,association_basis=row['association_basis'],
            unit_ids=[],source_refs=[],limits=copy.deepcopy(row['limits'])))
    if len(rows)>METADATA_LIMIT:
        attachments.append(dict(file_ref=None,kind='关联资料清单',decision='uncertain',
            reason='完整关联清单由程序私有保存',description=f'关联资料共 {len(rows)} 件；本条覆盖未展开的 {len(rows)-METADATA_LIMIT} 件',
            learning_value=None,related_article_title=None,association_basis='同一正文的程序关联清单；逐件依据保存在私有清单中',
            unit_ids=[],source_refs=[],limits=['未展开不等于排除；完整原件和逐件状态均保留',
                '所有附件内容均未提交模型阅读；原件未上传 RAGFlow']))
    return candidate


def process_associated(processor, source, record, work, item, stage, *, role=None):
    enter_prepare=getattr(processor,'_preextract_enter_prepare',None)
    if enter_prepare:enter_prepare()
    if role is not None:
        item['intake_role']=copy.deepcopy(role)
        if role['action']=='delete':
            from material_roles import delete_process_form
            delete_process_form(processor,record,work,item,stage,role)
            return
    inputs=getattr(processor,'_material_context',None) or getattr(processor,'_material_inputs',[]) or [record]
    info=preserve_material(record,Path(work)/'associated',inputs,stop=processor.stop)
    purpose_pending=bool(role and role['action']=='pending')
    if purpose_pending:
        info.update(parse_status='pending',purpose_pending=True,parse_reason=role['reason'])
    pending=info['parse_status']=='pending'
    reason=role['reason'] if purpose_pending else '原件已保存，内容待解析' if pending else '关联原件和静态预览已保存'
    if info['association_status']=='unassigned':reason+='；待确认关联文档'
    note=Path(work)/'associated-result.md'
    note.write_text('# '+('待解析' if pending else '关联资料已保存')+'\n\n'+reason+
        '\n\n原件保存在本批私有材料清单中，未执行源码。确认用途及关联正文后，随正文交付原件至 RAGFlow。',encoding='utf8')
    item.update(content_format='associated_material',status='pending' if pending else 'associated',
        title='关联设计资料',reason=reason,reason_kind='material_pending' if pending else 'associated_material',
        business_decision='pending' if pending else 'associated',associated_material=info,
        path=str(note),display_path=str(note),automatic_approval=False,
        script_result=dict(status='completed',reason=reason),
        ai_result=dict(status='not_started',reason='程序保存关联资料，未调用 AI',stage=''))
    stage('待解析' if pending else '关联资料保存',reason)


def duplicate_project_reference(record,sha):
    return dict(source_record_key=record_key(record),reused_source_sha256=sha,status='unresolved')


def resolve_duplicate_projects(manifest):
    """Retain per-project relations while respecting the existing body dedupe."""
    ready={i['source_hash']:i for i in manifest.get('items',[])
           if i.get('status')=='pass' and i.get('automatic_approval') and i.get('source_hash')}
    for event in manifest.get('events',[]):
        relation=event.get('project_reference')
        if not relation:continue
        primary=ready.get(relation['reused_source_sha256'])
        if primary:
            relation.update(status='linked',reused_run_id=manifest['run_id'],reused_item_id=primary['id'],
                            reused_package_path=primary['package_path'])
