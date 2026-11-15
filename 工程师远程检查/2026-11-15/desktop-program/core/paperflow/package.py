"""确定性技术检查与不可变发布；不调用模型、不做语义评分。"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path, PurePosixPath
import shutil
import uuid
import re


class PackageError(ValueError):
    def __init__(self, reason, check='missing_files'):
        super().__init__(reason)
        self.check = check


CORE_NAMES = ('article.redacted.md', 'profile.json', 'tree.json', 'assets.json', 'chunks.json')
AUXILIARY_NAMES = {'review.json', 'corrections.json', 'processing.json', 'failure.json'}


def sha(path: Path | str) -> str:
    h = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def load(path):
    return json.loads(Path(path).read_text(encoding='utf-8-sig'))


def save(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
    temp.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding='utf-8')
    temp.replace(path)


def optional_json(path, default=None):
    """诊断、映射和修正说明是辅助信息，不能阻塞完整交付。"""
    try:
        return load(path)
    except json.JSONDecodeError as exc:
        # Recover only the unexpected, isolated closing token at the parser's
        # exact error position. No file writes, value edits or guessed routes.
        if exc.msg == "Expecting ',' delimiter" and exc.doc[exc.pos:exc.pos+1] in ('}', ']'):
            left=exc.doc.rfind('\n',0,exc.pos)+1
            right=exc.doc.find('\n',exc.pos)
            if right<0:right=len(exc.doc)
            if exc.doc[left:right].strip()==exc.doc[exc.pos]:
                try:return json.loads(exc.doc[:exc.pos]+exc.doc[exc.pos+1:])
                except json.JSONDecodeError:pass
        return default
    except (OSError, ValueError, TypeError):
        return default


def normalize_json(path):
    """只去JSON围栏和字符串外的尾逗号；不重解释数值或字符串。"""
    path = Path(path)
    raw = path.read_text(encoding='utf-8-sig')
    candidate = raw.strip()
    if candidate.startswith('```') and candidate.endswith('```'):
        candidate = '\n'.join(candidate.splitlines()[1:-1])
    try:
        obj = json.loads(candidate)
    except json.JSONDecodeError:
        # 匹配完整字符串并原样保留，只有字符串外、紧邻闭括号的逗号可删除。
        candidate = re.sub(r'("(?:\\.|[^"\\])*")|,(?=\s*[}\]])',
                           lambda match: match.group(1) or '', candidate, flags=re.S)
        try:
            obj = json.loads(candidate)
        except json.JSONDecodeError:
            return False
        save(path, obj)
        return True
    if candidate != raw.strip():
        save(path, obj)
    return True


def _numbered_rows(rows, prefix, reserved=()):
    if not isinstance(rows, list):
        raise PackageError('unreadable delivery records: ' + prefix)
    used = set(reserved)
    result = []
    for index, row in enumerate(rows, 1):
        row = dict(row) if isinstance(row, dict) else {('content' if prefix == 'chunk' else 'title'): row}
        value = row.get('id')
        if not isinstance(value, str) or not value or value in used:
            value = f'{prefix}_{index}'
            while value in used:
                value += '_'
            row['id'] = value
        used.add(value)
        result.append(row)
    return result


def _compatible_tree(tree):
    nodes = _numbered_rows(tree.get('nodes', []), 'section')
    parents = {row['id']: row.get('parent_id') for row in nodes}
    for row in nodes:
        parent, seen = row.get('parent_id'), {row['id']}
        while parent is not None:
            if not isinstance(parent, str) or parent not in parents or parent in seen:
                row['parent_id'] = None
                break
            seen.add(parent)
            parent = parents[parent]
    return dict(tree, nodes=nodes)


def _profile_present(profile):
    metadata={'task_id','source_id','source_sha256','schema','revision','routing_candidates','material_disposition'}
    return isinstance(profile,dict) and any(value for key,value in profile.items() if key not in metadata)


def _chapter_applicable(profile):
    if profile.get('structure_applicable') is False or profile.get('has_chapter_structure') is False:return False
    kind=str(profile.get('material_type') or profile.get('material_nature') or '').strip().lower()
    return kind not in ('附件','图纸','代码','数据附件','软件资料','attachment','drawing','code','dataset','software')


def _recover_tree(root,profile,tree):
    if tree.get('nodes') or not _chapter_applicable(profile):return tree
    for key in ('chapter_tree','tree','sections'):
        existing=profile.get(key)
        nodes=existing.get('nodes') if isinstance(existing,dict) else existing
        if isinstance(nodes,list) and nodes:return dict(tree,nodes=nodes)
    article=Path(root)/'article.redacted.md'
    if not article.is_file():return tree
    nodes=[];stack=[];fence=False
    for number,line in enumerate(article.read_text(encoding='utf-8-sig').splitlines(),1):
        if line.lstrip().startswith(('```','~~~')):fence=not fence;continue
        match=re.match(r'^(#{1,6})[ \t]+(.+?)\s*$',line) if not fence else None
        if not match:continue
        level=len(match[1])
        while stack and stack[-1][0]>=level:stack.pop()
        ident='section_'+str(len(nodes)+1)
        nodes.append(dict(id=ident,title=match[2],parent_id=stack[-1][1] if stack else None,source_location=dict(line=number)))
        stack.append((level,ident))
    return dict(tree,nodes=nodes)


def normalize_delivery(root, catalog=None, task=None):
    """封包前机械整理字段和编号，保留模型决定及所有非空切片内容。"""
    root = Path(root)
    wrappers = {'tree.json': 'nodes', 'assets.json': 'items', 'chunks.json': 'chunks'}
    for name in CORE_NAMES:
        path = root / name
        if path.suffix != '.json' or not path.is_file():
            continue
        if not normalize_json(path):
            raise PackageError('unreadable delivery JSON: ' + name)
        obj = load(path)
        if isinstance(obj, list) and name in wrappers:
            obj = {wrappers[name]: obj}
        if not isinstance(obj, dict):
            raise PackageError('unreadable delivery object: ' + name)
        if task:
            for field in ('task_id','source_id','source_sha256'):
                expected=task.get('delivery_task_id',task.get(field)) if field=='task_id' else task.get(field)
                if expected is None:continue
                if field!='source_id' and field in obj and obj[field]!=expected:
                    raise PackageError('identity mismatch: '+field,'identity')
                obj.setdefault(field,expected)
        if name == 'tree.json':
            obj = _compatible_tree(obj)
        elif name == 'assets.json':
            obj = dict(obj, items=_numbered_rows(obj.get('items', []), 'asset'))
        elif name == 'chunks.json':
            assets = optional_json(root / 'assets.json', {})
            reserved = {'__profile', '__tree'} | {'asset_' + a['id'] for a in assets.get('items', [])}
            rows = _numbered_rows(obj.get('chunks', []), 'chunk', reserved)
            for row in rows:
                row.setdefault('kind', row.get('type', 'body'))
                row['kind'] = {'正文': 'body', '方法': 'learning', '图表': 'chart'}.get(row['kind'], row['kind'])
                if not isinstance(row.get('content'),str) or not row['content'].strip():
                    for alias in ('text','markdown'):
                        if isinstance(row.get(alias),str) and row[alias].strip():
                            row['content']=row[alias];break
                    else:
                        if 'line_start' not in row and 'line_end' not in row:
                            row['content']=json.dumps(row,ensure_ascii=False) if row.get('kind')!='body' else ''
            obj = dict(obj, chunks=rows)
        elif name == 'profile.json' and catalog:
            entries = {r['code']: r for r in catalog.get('majors', [])}
            for route in obj.get('routing_candidates', []):
                if isinstance(route, dict) and route.get('code') in entries:
                    route['name'] = entries[route['code']]['name']
        if obj != load(path):
            save(path, obj)
    profile=optional_json(root/'profile.json',{})
    if isinstance(profile,dict):
        if not _profile_present(profile):
            corrections=optional_json(root/'corrections.json',{})
            candidate=corrections.get('final_profile') if isinstance(corrections,dict) else None
            if _profile_present(candidate):
                for field in ('task_id','source_id','source_sha256'):
                    if field!='source_id' and field in candidate and field in profile and candidate[field]!=profile[field]:
                        raise PackageError('identity mismatch in recovered profile: '+field,'identity')
                if task:
                    for field in ('task_id','source_sha256'):
                        expected=task.get('delivery_task_id',task.get(field)) if field=='task_id' else task.get(field)
                        if field in candidate and expected is not None and candidate[field]!=expected:
                            raise PackageError('identity mismatch in recovered profile: '+field,'identity')
                save(root/'profile.json',dict(profile,**candidate));profile=load(root/'profile.json')
        tree=optional_json(root/'tree.json',{})
        if isinstance(tree,dict):
            restored=_compatible_tree(_recover_tree(root,profile,tree))
            if restored!=tree or (not (root/'tree.json').is_file() and not _chapter_applicable(profile)):
                save(root/'tree.json',restored)
    normalize_asset_storage(root)
    from .privacy import apply_explicit_identity
    apply_explicit_identity(root,task)


ASSET_BINDING_FIELDS = {'file','source_image_id','redactions','sha256','section_id','section_ids','body_anchor'}


def _check_asset_storage_conflicts(asset):
    details=asset.get('storage_details')
    if isinstance(details,dict) and any(key in details and asset.get(key)!=details[key] for key in ASSET_BINDING_FIELDS):
        raise PackageError('unresolved conflicting image storage instructions','missing_files')


def normalize_asset_storage(root):
    """只展开LLM的字段包装；不改变其存图/描述/省略决定或研究内容。"""
    path=Path(root)/'assets.json'
    if not path.is_file():return
    obj=load(path);changed=False
    for asset in obj.get('items',[]):
        _check_asset_storage_conflicts(asset)
        details=asset.get('storage')
        if isinstance(details,dict) and ('mode' in details or 'type' in details):
            values=[details[k] for k in ('mode','type') if details.get(k) not in (None,'')]
            if any(not isinstance(value,str) for value in values):
                raise PackageError('invalid asset storage mode/type','missing_files')
            modes=[value.strip().lower() for value in values if value.strip()]
            if len(set(modes))>1:raise PackageError('conflicting asset storage mode/type','missing_files')
            conflicts={}
            for key,value in details.items():
                if key in ('mode','type'):continue
                if key in asset and asset[key] not in (None,'') and asset[key]!=value:
                    if key in ASSET_BINDING_FIELDS:raise PackageError('conflicting image storage field: '+key,'missing_files')
                    conflicts[key]=value
                else:asset[key]=value
            if conflicts:asset['storage_details']=conflicts
            asset['storage']=modes[0] if modes else None;changed=True
        mode=asset.get('storage')
        if isinstance(mode,str) and mode.strip().lower() in ('image','description','omit'):
            normalized=mode.strip().lower()
            if normalized!=mode:asset['storage']=normalized;changed=True
        if asset.get('storage') in ('image','description') and not str(asset.get('description') or '').strip():
            for key in ('content','note','caption','alt_text','text'):
                if isinstance(asset.get(key),str) and asset[key].strip():
                    asset['description']=asset[key];changed=True;break
    if changed:save(path,obj)


def asset_storage_mode(asset):
    """Immutable consumers must reject an explicit but unmaterialized storage wrapper."""
    _check_asset_storage_conflicts(asset)
    mode=asset.get('storage','image' if asset.get('file') else 'description')
    values=[mode] if isinstance(mode,str) else [mode.get(k) for k in ('mode','type')] if isinstance(mode,dict) else []
    if any(isinstance(v,str) and v.strip().lower() in ('image','description','omit') for v in values):
        if not isinstance(mode,str) or mode not in ('image','description','omit'):
            raise PackageError('asset storage must be normalized and materialized before delivery','missing_files')
    return mode


def final_routing_candidates(root):
    """Read the model's explicit final list without changing an immutable draft."""
    root=Path(root)
    corrections=optional_json(root/'corrections.json',{})
    routes=corrections.get('final_routing_candidates') if isinstance(corrections,dict) else None
    if isinstance(routes,list) and all(isinstance(r,dict) and r.get('code') for r in routes):return routes
    return load(root/'profile.json').get('routing_candidates')


def synchronize_routes(root):
    """机械执行模型已确定的最终名单；不选择专业、不推断缺失名单。"""
    root=Path(root)
    routes=final_routing_candidates(root)
    if routes is None:return
    profile=load(root/'profile.json')
    previous=dict(profile)
    profile['routing_candidates']=routes
    corrections=optional_json(root/'corrections.json',{})
    if isinstance(corrections,dict) and 'additional_related_specialties' in corrections:
        profile['additional_related_specialties']=corrections['additional_related_specialties']
    if profile != previous:
        save(root/'profile.json',profile)


def validate_routing_selection(profile, catalog=None):
    """Check the model's decision without ranking, truncating or creating targets."""
    routes=profile.get('routing_candidates',[])
    if not isinstance(routes,list) or any(not isinstance(r,dict) or not isinstance(r.get('code'),str)
            or not r['code'].strip() for r in routes):
        raise PackageError('invalid final specialty list','wrong_library')
    codes=[r['code'] for r in routes]
    if len(codes)>3:raise PackageError('final specialty list must contain at most 3, in relevance order','wrong_library')
    if len(set(codes))!=len(codes):raise PackageError('duplicate final specialty','wrong_library')
    extra=profile.get('additional_related_specialties',[])
    if not isinstance(extra,list):raise PackageError('invalid additional related specialties','wrong_library')
    known={r['code']:r['name'] for r in (catalog or {}).get('majors',[]) if r.get('name')}
    seen=set(codes)
    for row in extra:
        if (not isinstance(row,dict) or not isinstance(row.get('code'),str) or not row['code'].strip()
                or not isinstance(row.get('name'),str) or not row['name'].strip() or row['code'] in seen):
            raise PackageError('additional specialties must be named, unique and non-delivery','wrong_library')
        if known and (row['code'] not in known or row['name']!=known[row['code']]):
            raise PackageError('additional specialty does not match catalog','wrong_library')
        seen.add(row['code'])


def article_presentation(profile, source_path):
    """Project the confirmed title; the original path and stable identity never change."""
    article=profile.get('article')
    title=article.get('title') if isinstance(article,dict) else None
    if not isinstance(title,str) or not title.strip():return {}
    title=title.strip()
    stem=re.sub(r'[<>:"/\\|?*\x00-\x1f]', '_', title).strip(' .')[:160].rstrip(' .')
    if not stem:return {}
    if stem.split('.')[0].upper() in {'CON','PRN','AUX','NUL',*[f'COM{i}' for i in range(1,10)],*[f'LPT{i}' for i in range(1,10)]}:
        stem='论文_'+stem
    suffix=Path(source_path).suffix
    if not re.fullmatch(r'\.[A-Za-z0-9]{1,12}',suffix):suffix=''
    return dict(article_title=title,display_name=stem+suffix)


def apply_article_name(root, task):
    path=Path(root)/'profile.json';profile=load(path)
    # Do not rename an old published version as a side effect of a content repair.
    if task.get('repair_request_id') and profile.get('article',{}).get('document_name_policy')!='paper-title/1':return {}
    fields=article_presentation(profile,task['source_path'])
    if fields:
        profile['article'].update(public_file_name=fields['display_name'],document_name_policy='paper-title/1')
        save(path,profile)
    return fields


def inside(root, relative):
    relative = str(relative).replace('\\', '/')
    p = PurePosixPath(relative)
    if p.is_absolute() or not p.parts or '..' in p.parts or ':' in relative:
        raise PackageError('unsafe path: ' + relative)
    path = Path(root).joinpath(*p.parts).resolve()
    if not path.is_relative_to(Path(root).resolve()):
        raise PackageError('escaped path: ' + relative)
    return path


def _require(condition, reason, check='missing_files'):
    if not condition:
        raise PackageError(reason, check)


def privacy_scan(root,task):
    patterns={
        'phone':re.compile(r'(?<![0-9A-Za-z])1[3-9]\d{9}(?![0-9A-Za-z])'),
        'email':re.compile(r'[A-Za-z0-9.!#$%&\x27*+/=?^_`{|}~-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+'),
        'identity_number':re.compile(r'(?<![0-9A-Za-z])[1-9]\d{5}(?:19|20)\d{2}(?:0[1-9]|1[0-2])(?:0[1-9]|[12]\d|3[01])\d{3}[0-9Xx](?![0-9A-Za-z])'),
        'student_id':re.compile(r'(?:学号|学生编号|student_id)[\s\x22\x27:：]*[A-Za-z]?\d{6,18}',re.I),
    }
    literals=[]
    if task.get('redaction_map_private'):
        mapping=optional_json(task['redaction_map_private'],{})
        entries=mapping.get('values',[]) if isinstance(mapping,dict) else []
        for item in entries if isinstance(entries,list) else []:
            if isinstance(item,str):value=item
            elif isinstance(item,dict):
                if item.get('allow_public'):continue
                value=item.get('original')
            else:continue
            # 单字/空值不足以跨全文判断身份，忽略该辅助映射即可，不能阻断整篇交付。
            if not isinstance(value,str) or len(value)<2:continue
            literals.append(value)
    notes=[]
    for name in ('article.redacted.md','chunks.json','profile.json','tree.json','assets.json'):
        path=Path(root)/name
        raw=path.read_text(encoding='utf-8-sig')
        text=raw
        if name.endswith('.json'):
            # 同时展开JSON转义，避免转义后的敏感值漏检；原JSON文本保留键值标签关系。
            def values(v):
                if isinstance(v,str):yield v
                elif isinstance(v,dict):
                    for key,value in v.items():yield key;yield from values(value)
                elif isinstance(v,list):
                    for value in v:yield from values(value)
            text+='\n'+'\n'.join(values(load(path)))
        for rule,pattern in patterns.items():
            match=pattern.search(text)
            if match:notes.append(dict(file=name,rule=rule))
        for literal in literals:
            # 身份语义由LLM负责，辅助映射只作无原值备注，不逐词否决其最终成果。
            if literal in text:notes.append(dict(file=name,rule='llm_private_map_residual'))
        for variant in (task.get('source_path'),str(task.get('source_path','')).replace('\\','/')):
            if variant and variant in text:notes.append(dict(file=name,rule='local_source_path'))
    return notes


def materialize_body_chunks(root):
    """只执行 LLM 明确给出的原文行边界，不推断/移动边界、不生成正文。"""
    root=Path(root);path=root/'chunks.json'
    if not path.is_file():return
    obj=load(path);rows=[r for r in obj.get('chunks',[]) if r.get('kind')=='body']
    if not any('line_start' in r or 'line_end' in r for r in rows):return
    lines=(root/'article.redacted.md').read_text(encoding='utf-8-sig').splitlines(keepends=True)
    next_line=1
    for row in rows:
        start,end=row.get('line_start'),row.get('line_end')
        _require(type(start) is int and type(end) is int and start==next_line and start<=end<=len(lines),
                 'body line ranges must be contiguous, ordered and within the final article', 'body_loss')
        row['content']=''.join(lines[start-1:end]);next_line=end+1
    _require(next_line==len(lines)+1,f'body line ranges must cover the complete final article: expected last line {len(lines)}, observed last line {next_line-1}; read the uncovered lines and extend/add only their corresponding body range', 'body_loss')
    save(path,obj)


def verify_delivery_identity(root,task, *, strict_seal=False):
    """只读核对原封包身份和字节；不能靠重封包掩盖变化。"""
    root=Path(root)
    try:
        ready = load(root / 'READY.json')
        manifest = load(root / 'manifest.json')
        digest = sha(root / 'manifest.json')
    except (OSError,ValueError,TypeError) as exc:
        raise PackageError('unreadable existing delivery seal','identity') from exc
    _require(isinstance(ready,dict) and isinstance(manifest,dict),'invalid existing delivery seal','identity')
    _require(ready.get('manifest_sha256') == digest, 'manifest hash mismatch','identity')
    for field in ('task_id', 'source_sha256'):
        expected = task.get('delivery_task_id', task.get(field)) if field == 'task_id' else task.get(field)
        _require(manifest.get(field) == expected and ready.get(field) == expected, 'identity mismatch: ' + field,'identity')
    listed = {}
    for entry in manifest.get('files', []):
        if isinstance(entry, str):
            hashes = manifest.get('file_hashes', {})
            entry = dict(path=entry, sha256=hashes.get(entry))
        _require(isinstance(entry, dict) and isinstance(entry.get('path'), str), 'invalid file entry')
        name = entry['path'].replace('\\', '/')
        path = inside(root, name)
        if name in AUXILIARY_NAMES and not strict_seal:
            continue
        _require(name not in listed, 'duplicate manifest path')
        _require(path.is_file(), 'missing file: ' + name)
        _require(name not in ('manifest.json', 'READY.json'), 'self-referential manifest')
        if entry.get('bytes') is not None:
            _require(path.stat().st_size == entry['bytes'], 'size mismatch: ' + name,'identity')
        _require(entry.get('sha256') and sha(path) == entry['sha256'], 'file hash mismatch: ' + name,'identity')
        listed[name] = entry
    required = {'article.redacted.md', 'profile.json', 'tree.json', 'assets.json', 'chunks.json'}
    _require(required.issubset(listed), 'missing required files: ' + ','.join(sorted(required - listed.keys())))
    publication = set(manifest.get('publication_files', []))
    _require((publication-AUXILIARY_NAMES).issubset(listed), 'publication references unlisted file')
    _require(not any('private' in PurePosixPath(p).parts or p.endswith('.dpapi') for p in publication), 'private publication path')
    return manifest,digest,listed,publication


def validate_core_text(root):
    """Existing mandatory text checks, also used before a phase becomes cacheable."""
    root=Path(root);profile=load(root/'profile.json');tree=load(root/'tree.json')
    _require(_profile_present(profile),'missing profile content')
    tree=_compatible_tree(_recover_tree(root,profile,tree))
    _require(bool(tree['nodes']) or not _chapter_applicable(profile),'missing chapter structure')
    _require(bool((root/'article.redacted.md').read_text(encoding='utf-8-sig').strip()),'missing article content')
    return tree


def validate_package(root, task, catalog, *, strict_seal=False):
    root = Path(root)
    try:
        manifest,digest,listed,publication = verify_delivery_identity(root,task, strict_seal=strict_seal)
        compatibility_notes=privacy_scan(root,task)
        profile, tree, assets, chunks_obj = [load(root / n) for n in ('profile.json', 'tree.json', 'assets.json', 'chunks.json')]
        for obj in (profile, tree, assets, chunks_obj):
            for field in ('task_id', 'source_sha256'):
                if field in obj:
                    _require(obj[field] == manifest[field], 'file identity mismatch: ' + field,'identity')
        tree=validate_core_text(root)
        from .privacy import explicit_identity_hits
        private_hits=explicit_identity_hits(root,task)
        if private_hits:
            error=PackageError('explicit private identity remains in '+private_hits[0]['file'],'privacy')
            error.privacy_hits=private_hits
            raise error
        ids = [n['id'] for n in tree['nodes']]
        asset_rows = assets.get('items', [])
        asset_rows=_numbered_rows(asset_rows,'asset')
        asset_ids=[a['id'] for a in asset_rows]
        for asset in asset_rows:
            mode = asset_storage_mode(asset)
            if mode not in ('image','description','omit'):
                compatibility_notes.append('unknown asset storage kept as metadata')
            if mode=='omit':continue
            if asset.get('section_id') not in ids:compatibility_notes.append('asset section reference missing')
            if mode == 'image':
                name = asset.get('file')
                _require(name in listed and name in publication, 'image absent from publication')
                _require(sha(inside(root, name)) == asset.get('sha256'), 'asset hash mismatch')
            elif mode == 'description':
                explanation=next((asset[k] for k in ('description','note','caption','alt_text','text')
                                  if isinstance(asset.get(k),str) and asset[k].strip()),None)
                _require(explanation is not None,'missing required asset description')
                if not asset.get('description'):asset['description']=explanation
        chunks = chunks_obj.get('chunks', [])
        _require(bool(chunks), 'empty chunks')
        chunks=_numbered_rows(chunks,'chunk',{'__profile','__tree'}|{'asset_'+a['id'] for a in asset_rows})
        nonempty=[]
        for row in chunks:
            if not isinstance(row.get('content'),str) or not row['content'].strip():
                compatibility_notes.append('empty chunk ignored');continue
            nonempty.append(row)
            for field,known,note in (('section_ids',ids,'chunk section reference missing'),
                                     ('asset_ids',asset_ids,'chunk asset reference missing')):
                refs=row.get(field,[])
                if not isinstance(refs,list) or any(ref not in known for ref in refs):compatibility_notes.append(note)
        chunks=nonempty
        body = ''.join((root/'article.redacted.md').read_text(encoding='utf-8-sig').split())
        covered = ''.join(''.join(row['content'].split()) for row in chunks if row.get('kind') == 'body')
        if not body or body!=covered:
            offset=next((i for i,(a,b) in enumerate(zip(body,covered)) if a!=b),min(len(body),len(covered)))
            detail=dict(offset=offset,expected_length=len(body),observed_length=len(covered),
                expected_context=body[max(0,offset-40):offset+80],observed_context=covered[max(0,offset-40):offset+80])
            raise PackageError('body coverage incomplete: '+json.dumps(detail,ensure_ascii=False),'body_loss')
        routes, pending_routes = resolve_routes(profile, asset_rows, catalog)
        packet=dict(root=str(root), manifest=manifest, manifest_sha256=digest, profile=profile,
                    tree=tree, assets=asset_rows, chunks=chunks, routes=routes,pending_routes=pending_routes,
                    compatibility_notes=compatibility_notes)
        from .structure_plan import load_sealed_plan
        return load_sealed_plan(task,packet)
    except PackageError:
        raise
    except (OSError, ValueError, KeyError, TypeError) as exc:
        raise PackageError(f'unreadable package: {type(exc).__name__}: {exc}') from exc


def resolve_routes(profile, asset_rows, catalog):
    """复用技术检查的专业映射规则；缺映射保留明确的问题记录。"""
    entries = {r['code']: r for r in catalog.get('majors', [])}
    routes, pending_routes, seen = [], [], set()
    needs_images = any(asset_storage_mode(a) == 'image'
                       for a in asset_rows)
    for route in profile.get('routing_candidates', []):
        code = route.get('code') if isinstance(route, dict) else str(route)
        if code in seen:
            continue
        seen.add(code)
        if code not in entries:
            pending_routes.append(dict(code=code, reason='unknown specialty'))
            continue
        entry = entries[code]
        if not entry.get('paper_dataset_id') or (needs_images and not entry.get('image_dataset_id')):
            pending_routes.append(dict(code=code, reason='specialty mapping incomplete'))
            continue
        routes.append(entry)
    if not routes and not pending_routes:
        pending_routes.append(dict(code=None, reason='no specialty supplied'))
    return routes, pending_routes


def seal_package(root, task, revision, *, created_at=None):
    """程序计算真实文件哈希并最后发布 READY，模型不填写散列。"""
    root = Path(root)
    normalize_delivery(root,task=task)
    for status in ('READY.json', 'NEEDS_REVIEW.json', 'manifest.json'):
        (root / status).unlink(missing_ok=True)
    ident = {k: task.get('delivery_task_id', task[k]) if k == 'task_id' else task[k]
             for k in ('task_id', 'source_id', 'source_sha256')}
    files = []
    for p in sorted(root.rglob('*')):
        if p.is_file():
            relative = p.relative_to(root).as_posix()
            inside(root, relative)
            _require('private' not in p.relative_to(root).parts, 'private file in delivery')
            files.append(dict(path=relative, bytes=p.stat().st_size, sha256=sha(p)))
    internal = AUXILIARY_NAMES | {'split-plan.json'}
    manifest = dict(**ident, schema='paperflow/1.0', revision=f'r{revision:03d}', files=files,
                    processing_history=task.get('processing_history',[]),
                    publication_files=[r['path'] for r in files if r['path'] not in internal])
    if created_at is not None:
        manifest['created_at'] = created_at
    save(root / 'manifest.json', manifest)
    save(root / 'READY.json', dict(**ident, status='ready', manifest='manifest.json',
                                 manifest_sha256=sha(root / 'manifest.json')))


def publish_package(staging, destination):
    staging, destination = Path(staging), Path(destination)
    if destination.exists():
        raise PackageError('immutable revision already exists')
    _require((staging / 'READY.json').is_file(), 'missing READY')
    destination.parent.mkdir(parents=True, exist_ok=True)
    temp = destination.with_name('.' + destination.name + '-' + uuid.uuid4().hex + '.staging')
    shutil.copytree(staging, temp)
    temp.rename(destination)
    return destination
