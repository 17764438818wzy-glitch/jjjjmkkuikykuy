"""只处理明确作者字段或模型准确定位；普通号码、人名和邮箱只备注。"""
import copy
import re
from pathlib import Path

from .package import CORE_NAMES, load, optional_json, save

FIELDS={
    'author_phone':'phone','author_mobile':'phone','作者手机号':'phone','作者手机':'phone',
    '作者电话':'phone','作者联系电话':'phone','author_email':'email','作者邮箱':'email',
    'author_student_id':'student','作者学号':'student','author_id_card':'identity','作者身份证号':'identity',
}
STUDENT_FIELDS={'student_id','student_number','学号','学生编号'}
AUTHOR_CONTEXT={'author','author_info','author_metadata','作者','作者信息','作者资料'}
PUBLIC={'references','bibliography','reference_authors','参考文献','public_research_objects',
        'public_entities','字段示例','examples'}
PATTERNS={
    'phone':re.compile(r'(?<![0-9A-Za-z])(?:\+?86[ -]?)?1[3-9](?:[ -]?\d){9}(?![0-9A-Za-z])'),
    'email':re.compile(r'[^\s@（）()<>]+@[^\s@（）()<>]+\.[^\s@（）()<>]+'),
    'student':re.compile(r'(?<![0-9A-Za-z])[A-Za-z]?\d{6,18}(?![0-9A-Za-z])'),
    'identity':re.compile(r'(?<![0-9A-Za-z])[1-9]\d{16}[0-9Xx](?![0-9A-Za-z])'),
}
LABEL=re.compile(r'^[ \t]*(?:[-*]\s*)?(?:\|\s*)?(?:\*\*)?('
                 +'|'.join(re.escape(k) for k in FIELDS if not k.isascii())
                 +r')(?:\*\*)?\s*[:：|]\s*([^|\r\n]+)')


def _at(obj,path):
    for key in path:obj=obj[key]
    return obj


def explicit_identity_hits(root,task=None):
    root=Path(root);hits=[]
    body_path=root/'article.redacted.md'
    chunks=optional_json(root/'chunks.json',{})
    rows=chunks.get('chunks',[]) if isinstance(chunks,dict) else []
    body_rows=[(i,row) for i,row in enumerate(rows) if isinstance(row,dict) and row.get('kind')=='body']
    # 已逐字对应正文的片段沿用整篇围栏语境，不在切片内部重新判断作者标签。
    known_body=set()
    if body_path.is_file() and all(isinstance(row.get('content'),str) for _,row in body_rows):
        if ''.join(row['content'] for _,row in body_rows)==body_path.read_text(encoding='utf-8-sig'):
            known_body={('chunks',i,'content') for i,_ in body_rows}
    def identifiers(value,kind,file,path,offset=0):
        if type(value) is int:
            if PATTERNS[kind].fullmatch(str(value)):
                hits.append(dict(file=file,path=list(path),original=value,replacement='[已脱敏]',source='field'))
        elif isinstance(value,str):
            for match in PATTERNS[kind].finditer(value):
                hits.append(dict(file=file,path=list(path),start=offset+match.start(),end=offset+match.end(),
                                 original=match[0],replacement='[已脱敏]',source='field'))
    def inspect(value,file,path=()):
        if file=='chunks.json' and path in known_body:return
        if isinstance(value,dict):
            for key,item in value.items():
                lowered=key.strip().lower()
                if lowered in PUBLIC:continue
                kind=FIELDS.get(lowered)
                author_context=(file=='profile.json' and not path) or any(
                    isinstance(part,str) and part.strip().lower() in AUTHOR_CONTEXT for part in path)
                if lowered in STUDENT_FIELDS and author_context:kind='student'
                if kind:identifiers(item,kind,file,path+(key,))
                else:inspect(item,file,path+(key,))
        elif isinstance(value,list):
            for index,item in enumerate(value):inspect(item,file,path+(index,))
        elif isinstance(value,str):
            offset=0;fence=None
            for line in value.splitlines(keepends=True):
                marker=re.match(r'^ {0,3}(`{3,}|~{3,})',line)
                if marker:
                    if fence is None:fence=marker[1]
                    elif marker[1][0]==fence[0] and len(marker[1])>=len(fence):fence=None
                elif fence is None:
                    match=LABEL.match(line)
                    if match:identifiers(match[2],FIELDS[match[1]],file,path,offset+match.start(2))
                offset+=len(line)
    for name in CORE_NAMES:
        path=root/name
        if path.is_file():inspect(load(path) if path.suffix=='.json' else path.read_text(encoding='utf-8-sig'),name)
    local=root/'private/redaction_map.json'
    mapping=optional_json(local if local.is_file() else (task or {}).get('redaction_map_private',''),{})
    if isinstance(mapping,dict) and (not task or mapping.get('source_sha256') in (None,task['source_sha256'])):
        for row in mapping.get('values',[]) if isinstance(mapping.get('values',[]),list) else []:
            if not isinstance(row,dict) or row.get('allow_public') or row.get('file') not in CORE_NAMES:continue
            if row.get('kind') not in ('author','author_name','student_id','phone','email','identity','signature'):continue
            file=row['file'];path=root/file
            if not path.is_file():continue
            original=row.get('original');replacement=row.get('replacement','[已脱敏]')
            if not isinstance(original,str) or not original or not isinstance(replacement,str) or any(c in replacement for c in '\r\n'):continue
            pointer=row.get('json_path')
            if file.endswith('.json') and isinstance(pointer,list) and pointer:
                if any(type(key) not in (str,int) or (isinstance(key,str) and key.strip().lower() in PUBLIC) for key in pointer):continue
                try:value=_at(load(path),pointer)
                except (KeyError,IndexError,TypeError):continue
                if value==original:
                    hits.append(dict(file=file,path=pointer,start=0,end=len(original),original=original,
                                     replacement=replacement,source='mapping'))
            elif file.endswith('.md') and type(row.get('line')) is int:
                lines=path.read_text(encoding='utf-8-sig').splitlines(keepends=True);line=row['line']
                if 1<=line<=len(lines) and lines[line-1].count(original)==1:
                    start=sum(map(len,lines[:line-1]))+lines[line-1].index(original)
                    hits.append(dict(file=file,path=[],start=start,end=start+len(original),original=original,
                                     replacement=replacement,source='mapping'))
    return hits


def _resolve(rows):
    """明确映射可覆盖其包含的机械标记；模型映射互相冲突时不猜。"""
    selected=[]
    for row in sorted(rows,key=lambda r:r['source']!='mapping'):
        duplicate=False
        for previous in selected:
            overlap=('start' not in row or 'start' not in previous or
                     max(row['start'],previous['start'])<min(row['end'],previous['end']))
            if not overlap:continue
            if all(row.get(k)==previous.get(k) for k in ('start','end','original','replacement')):
                duplicate=True;break
            if (previous['source']=='mapping' and row['source']=='field' and
                previous.get('start',0)<=row.get('start',0) and previous.get('end',0)>=row.get('end',0)):
                duplicate=True;break
            return None
        if not duplicate:selected.append(row)
    return selected


def _replace(value,rows):
    if any('start' not in row for row in rows):return rows[0]['replacement']
    for row in sorted(rows,key=lambda r:r['start'],reverse=True):
        if value[row['start']:row['end']]!=row['original']:return None
        value=value[:row['start']]+row['replacement']+value[row['end']:]
    return value


def _new_offset(offset,edits):
    delta=0
    for row in sorted(edits,key=lambda r:r['start']):
        if offset<=row['start']:break
        if offset<row['end']:return row['start']+delta+len(row['replacement'])
        delta+=len(row['replacement'])-(row['end']-row['start'])
    return offset+delta


def apply_explicit_identity(root,task=None):
    root=Path(root);hits=explicit_identity_hits(root,task)
    if not hits:return
    originals={};changed={};groups={};body='article.redacted.md';chunks='chunks.json'
    for name in {h['file'] for h in hits}|{body,chunks}:
        path=root/name
        if path.is_file():originals[name]=load(path) if path.suffix=='.json' else path.read_text(encoding='utf-8-sig')
    ranges={};offset=0
    for i,row in enumerate(originals.get(chunks,{}).get('chunks',[])):
        if row.get('kind')!='body':continue
        if not isinstance(row.get('content'),str):ranges={};break
        ranges[i]=(offset,offset+len(row['content']));offset+=len(row['content'])
    synchronized=bool(ranges) and ''.join(originals[chunks]['chunks'][i]['content'] for i in ranges)==originals.get(body)
    for hit in hits:
        pointer=hit['path']
        if (synchronized and hit['file']==chunks and len(pointer)==3 and pointer[0]=='chunks' and
            pointer[1] in ranges and pointer[2]=='content'):
            start,end=ranges[pointer[1]]
            hit=dict(hit,file=body,path=[],start=start+hit.get('start',0),end=start+hit.get('end',end-start))
        groups.setdefault((hit['file'],tuple(hit['path'])),[]).append(hit)
    body_edits=[]
    for (name,pointer),rows in groups.items():
        edits=_resolve(rows)
        if edits is None:continue
        obj=changed.setdefault(name,copy.deepcopy(originals[name]));value=_replace(_at(obj,pointer),edits)
        if value is None:continue
        if pointer:_at(obj,pointer[:-1])[pointer[-1]]=value
        else:changed[name]=value
        if name==body and not pointer:body_edits=edits
    if synchronized and body_edits:
        obj=changed.setdefault(chunks,copy.deepcopy(originals[chunks]));text=changed[body]
        for i,(start,end) in ranges.items():
            obj['chunks'][i]['content']=text[_new_offset(start,body_edits):_new_offset(end,body_edits)]
    for name,obj in changed.items():
        if name.endswith('.json'):save(root/name,obj)
        else:(root/name).write_text(obj,encoding='utf-8')
