"""Independent versioned reference ratings; no profile or retrieval publication."""
import copy
import hashlib
import json
import re
from pathlib import Path

RUBRICS=json.loads(Path(__file__).with_name('rubric-definitions.json').read_text(encoding='utf-8'))
TIER_NAMES={'correspondence':'函授参考','bachelor':'本科参考','key_bachelor':'重点本科参考'}

# All read surfaces and filters use exactly this content-identity predicate.
RATING_MATCH_SQL="""r.article_id=a.id AND r.article_revision=a.revision
 AND r.research_type=a.base#>>'{profile,research_profile,research_type}'
 AND (coalesce(r.payload->>'evidence_basis','original')<>'profile' OR
   EXISTS (SELECT 1 FROM article_rating_profile p WHERE p.article_id=a.id AND p.article_revision=a.revision
     AND p.catalog_guard=jsonb_build_object('profile',coalesce(a.base->'profile','{}'::jsonb),
       'manual_fields',coalesce(a.overrides->'fields','{}'::jsonb),'source_qdrant',coalesce(a.base->'qdrant','{}'::jsonb))
     AND p.profile_sha256=r.payload->>'profile_sha256'))
 AND NOT EXISTS (SELECT 1 FROM jsonb_array_elements(r.source_files) s
   WHERE NOT EXISTS (SELECT 1 FROM jsonb_array_elements(coalesce(a.base->'files','[]'::jsonb)) f
     WHERE f->>'id'=s->>'file_id' AND f->>'sha256'=s->>'sha256'))"""


def rating_select_sql(expression):
    return f'(SELECT {expression} FROM article_rating r WHERE {RATING_MATCH_SQL} ORDER BY r.created_at DESC,r.operation_id DESC LIMIT 1)'


RATING_RECORD_SQL=rating_select_sql("jsonb_build_object('operation_id',r.operation_id,'payload',r.payload,'created_at',r.created_at)")


def canonical(value):
    return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False)


def profile_snapshot(base,overrides):
    return {'profile':copy.deepcopy(base.get('profile') or {}),'manual_fields':copy.deepcopy(overrides.get('fields') or {})}


def profile_sha256(snapshot):return hashlib.sha256(canonical(snapshot).encode('utf-8')).hexdigest()


def profile_evidence(snapshot):
    """Keep research facts; old reference answers and author credentials stay out."""
    # Lazy import reuses the same production projection without a filters cycle.
    from .profiles import details,display
    raw,manual=snapshot['profile'],snapshot['manual_fields']
    projected=details(raw,manual)
    projected={k:v for k,v in projected.items() if k not in ('audience','legacy_summary','limits')}
    fields=display(raw)|manual
    return {'profile':projected,'fields':{k:v for k,v in fields.items() if k not in ('audience','level')}}


def profile_field(snapshot,path):
    if not isinstance(path,str) or not path.startswith('/profile_evidence/'):raise ValueError('画像证据路径无效')
    value={'profile_evidence':profile_evidence(snapshot)}
    try:
        for part in path.split('/')[1:]:
            part=part.replace('~1','/').replace('~0','~')
            value=value[int(part)] if isinstance(value,list) else value[part]
    except (KeyError,IndexError,ValueError,TypeError):raise ValueError('画像证据字段不存在') from None
    if value is None or value=='' or value==[] or value=={}:raise ValueError('画像证据字段为空')
    return value


def _int(value,lo,hi):
    return isinstance(value,int) and not isinstance(value,bool) and lo<=value<=hi


def _text(value,maximum=20000):
    return isinstance(value,str) and 0<len(value.strip())<=maximum


def _check_rating_size(value):
    snapshot=value.get('profile_snapshot')
    if (value.get('evidence_basis')=='profile' and isinstance(snapshot,dict)
        and set(snapshot)=={'profile','manual_fields'} and all(isinstance(v,dict) for v in snapshot.values())):
        if len(canonical(snapshot).encode('utf-8'))>16*1024*1024:raise ValueError('画像快照过大')
        core={key:item for key,item in value.items() if key!='profile_snapshot'}
    else:core=value
    if len(canonical(core).encode('utf-8'))>256000:raise ValueError('评分记录过大')


def validate_rating(body):
    """Check the fixed rubric and arithmetic without deriving a tier from score."""
    value=copy.deepcopy(body)
    _check_rating_size(value)
    for key in ('operation_id','run_id','request_id','model'):
        if not _text(value.get(key),200):raise ValueError('评分请求身份无效')
    if not _int(value.get('revision'),1,2147483647):raise ValueError('文章版本无效')
    category=value.get('research_type')
    rubric=RUBRICS.get(category) if isinstance(category,str) else None
    if not rubric or value.get('rubric_version')!=rubric['version'] or value.get('rubric_sha256')!=rubric['sha256']:
        raise ValueError('评分表类别、版本或哈希不匹配')
    basis=value.setdefault('evidence_basis','original')
    if basis not in ('original','profile'):raise ValueError('评分依据无效')
    if basis=='profile':
        snapshot=value.get('profile_snapshot')
        if not isinstance(snapshot,dict) or set(snapshot)!={'profile','manual_fields'} or any(not isinstance(v,dict) for v in snapshot.values()):raise ValueError('画像快照无效')
        if value.get('profile_sha256')!=profile_sha256(snapshot):raise ValueError('画像哈希不匹配')
    sources=value.get('source_files')
    if not isinstance(sources,list) or not (1 if basis=='original' else 0)<=len(sources)<=128:raise ValueError('评分原文身份无效')
    seen=set()
    for source in sources:
        if not isinstance(source,dict) or set(source)!={'file_id','sha256'}:raise ValueError('评分原文身份字段无效')
        if not _text(source.get('file_id'),300) or source['file_id'] in seen:raise ValueError('评分原文ID缺失或重复')
        if not isinstance(source.get('sha256'),str) or not re.fullmatch('[0-9a-f]{64}',source['sha256']):raise ValueError('评分原文哈希无效')
        seen.add(source['file_id'])
    value['source_files']=sorted(sources,key=lambda f:f['file_id'])
    rating=value.get('rating')
    if not isinstance(rating,dict) or rating.get('status')!='rated':raise ValueError('材料待核对时不能写入档位和分数')
    if not isinstance(rating.get('tier'),str) or rating['tier'] not in TIER_NAMES or not _text(rating.get('tier_reason')):raise ValueError('缺少独立定档及理由')
    if not _text(rating.get('brief_reason')):raise ValueError('缺少推荐分依据')
    rows=rating.get('dimension_scores')
    if not isinstance(rows,list) or len(rows)!=len(rubric['dimensions']):raise ValueError('评分维度不完整')
    if any(not isinstance(r,dict) or not isinstance(r.get('id'),str) for r in rows):raise ValueError('评分维度ID无效')
    by_id={r['id']:r for r in rows}
    if len(by_id)!=len(rows) or set(by_id)!={d['id'] for d in rubric['dimensions']}:raise ValueError('评分维度ID无效')
    normalized=[]
    for dim in rubric['dimensions']:
        row=by_id[dim['id']]
        subs=row.get('subitems')
        if not _text(row.get('reason')) or not isinstance(subs,list) or len(subs)!=len(dim['subitems']):raise ValueError('评分小项或依据不完整')
        if any(not isinstance(s,dict) or not isinstance(s.get('id'),str) for s in subs):raise ValueError('评分小项ID无效')
        submap={s['id']:s for s in subs}
        if len(submap)!=len(subs) or set(submap)!={s['id'] for s in dim['subitems']}:raise ValueError('评分小项ID无效')
        converted=[]
        for definition in dim['subitems']:
            sub=submap[definition['id']]
            if not _int(sub.get('level'),0,5) or not _text(sub.get('reason')):raise ValueError('评分小项级别或依据无效')
            score=sub['level']*definition['max']//5
            if 'score' in sub and sub['score']!=score:raise ValueError('评分小项换算不符')
            converted.append({**definition,'level':sub['level'],'score':score,'reason':sub['reason']})
        score=sum(s['score'] for s in converted)
        if not _int(row.get('score'),0,dim['max']) or row['score']!=score:raise ValueError('评分维度加总不符')
        normalized.append({**dim,'score':score,'reason':row['reason'],'subitems':converted})
    raw=sum(d['score'] for d in normalized)
    if not isinstance(rating.get('cap_applied'),bool):raise ValueError('封顶标记无效')
    expected=min(raw,rubric['cap']) if rating['cap_applied'] else raw
    if rating['cap_applied'] and not _text(rating.get('cap_reason')):raise ValueError('封顶缺少完全缺失的证据说明')
    if rating['cap_applied']:
        levels={s['id']:s['level'] for d in normalized for s in d['subitems']}
        if any(levels[sid]>maximum for sid,maximum in rubric['cap_conditions'].items()):
            raise ValueError('核心过程存在具体片段，不能按完全缺失封顶')
    if not _int(rating.get('score'),0,100) or rating['score']!=expected:raise ValueError('推荐分加总或封顶不符')
    refs=rating.get('evidence_refs')
    if not isinstance(refs,list) or not 1<=len(refs)<=200:raise ValueError('缺少原文证据')
    for ref in refs:
        if basis=='profile' and isinstance(ref,dict) and 'field_path' in ref:
            if 'file_id' in ref or 'page' in ref:raise ValueError('画像字段引用与原文页码引用必须互斥')
            profile_field(snapshot,ref['field_path'])
            if not _text(ref.get('note')):raise ValueError('画像证据说明为空')
            continue
        if not isinstance(ref,dict) or not isinstance(ref.get('file_id'),str) or ref['file_id'] not in seen or not _int(ref.get('page'),1,100000):raise ValueError('原文证据身份或页码无效')
        if not _text(ref.get('note')):raise ValueError('原文证据说明为空')
    uncertainties=rating.get('uncertainties',[])
    if not isinstance(uncertainties,list) or any(not isinstance(s,str) for s in uncertainties):raise ValueError('不确定性说明无效')
    rating.update(dimension_scores=normalized,raw_score=raw,uncertainties=uncertainties)
    _check_rating_size(value)
    return value


def payload_digest(aid,body):
    return hashlib.sha256(canonical({'article_id':aid,'body':body}).encode('utf-8')).hexdigest()


def rating_view(record,*,full=False):
    if not record:return None
    payload=record['payload']
    rating=copy.deepcopy(payload['rating']) if full else {k:payload['rating'][k] for k in ('tier','score','brief_reason','tier_reason','uncertainties')}
    return {**rating,'operation_id':record['operation_id'],'rubric_version':payload['rubric_version'],
            'evidence_basis':payload.get('evidence_basis','original'),'profile_sha256':payload.get('profile_sha256'),
            'rubric_sha256':payload['rubric_sha256'],'model':payload['model'],'created_at':str(record['created_at'])}


def apply_rating_reference(value):
    rating=value.get('rating')
    if rating:
        if value.get('reference',{}).get('source')!='article_rating':
            value['legacy_reference']=copy.deepcopy(value.get('reference',{}))
        value['reference']={'levels':[rating['tier']],'labels':[TIER_NAMES[rating['tier']]],
            'primary':TIER_NAMES[rating['tier']],'primary_known':True,'reference_only':False,
            'score':rating['score'],'source':'article_rating','manually_edited':False,
            'reason':rating['tier_reason'],'limits':'；'.join(rating.get('uncertainties',[]))}
    return value
