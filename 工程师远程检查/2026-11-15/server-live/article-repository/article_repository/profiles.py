"""Existing profile projections for the article list and detail; no re-evaluation."""
import re

from .filters import reference_levels,RESEARCH_TYPES,YEAR_PATTERN

FIELDS={'summary':'文章摘要','keywords':'主题关键词','audience':'适用人群','level':'适用层次',
        'object':'研究对象','methods':'研究方法','path':'整体研究路线','learn':'可借鉴内容'}

def article_research_type(profile):
    value=(profile.get('research_profile') or {}).get('research_type')
    return value if value in RESEARCH_TYPES else None


def article_year(profile):
    value=(profile.get('article') or {}).get('year')
    if type(value) is int:value=str(value)
    return value if isinstance(value,str) and re.fullmatch(YEAR_PATTERN,value) else None


def retain_research_type(prior,incoming):
    """Keep filled article annotations when an unchanged source omits them."""
    import copy
    before,after=prior.get('profile') or {},incoming.get('profile') or {}
    annotations=[('research_profile','research_type',article_research_type(before)),
                 ('article','year',article_year(before))]
    missing=[(container,key,value) for container,key,value in annotations
             if value and not (after.get(container) or {}).get(key)]
    if not missing:return
    old_hash,new_hash=prior.get('source_sha256'),incoming.get('source_sha256')
    def original_hashes(record):
        return {f['sha256'] for f in record.get('files',[]) if f.get('kind') in ('原文','原稿') and f.get('sha256')}
    old_originals,new_originals=original_hashes(prior),original_hashes(incoming)
    if old_hash and new_hash:
        same_body=old_hash==new_hash
    elif old_originals and new_originals:
        same_body=old_originals==new_originals
    else:
        def without_annotations(profile):
            value=copy.deepcopy(profile)
            for container,key,_ in annotations:
                fields=value.get(container)
                if isinstance(fields,dict):
                    fields.pop(key,None)
                    if not fields:value.pop(container)
            return value
        same_body=without_annotations(before)==without_annotations(after) and prior.get('source_updated_at')==incoming.get('source_updated_at')
    if same_body:
        for container,key,value in missing:
            incoming.setdefault('profile',{}).setdefault(container,{})[key]=value

def text(value):
    if isinstance(value,str): return value.strip()
    if isinstance(value,list): return '；'.join(dict.fromkeys(text(v) for v in value if text(v)))
    return ''

def display(profile):
    article=profile.get('article') or {}
    applicable=profile.get('applicable_level') or {}
    research=profile.get('research_profile') or {}
    units=profile.get('learning_units') or []
    return {'summary':text(article.get('summary') or profile.get('summary')),
            'keywords':text(article.get('keywords') or profile.get('keywords')),
            'audience':text(applicable.get('applicable_audience') or profile.get('applicable_audience')),
            'level':text(applicable.get('labels') or profile.get('applicable_levels')),
            'object':text(research.get('object') or (profile.get('research') or {}).get('object')),
            'methods':text([u.get('method') for u in units if isinstance(u,dict)]),
            'path':text(research.get('overall_path') or (profile.get('research') or {}).get('overall_path')),
            'learn':text([u.get('what_to_learn') or u.get('learn') for u in units if isinstance(u,dict)])
                    or text((profile.get('migration_boundary') or {}).get('reusable_scope') or profile.get('reusable_scope'))}


def descriptive(value):
    if isinstance(value,dict):
        return next((descriptive(value[k]) for k in ('description','content','operation','detail','reason','name','title','text') if value.get(k)),'')
    return text(value)


def strings(value):
    if value is None:return []
    return list(dict.fromkeys(v for item in (value if isinstance(value,list) else [value]) if (v:=descriptive(item))))


def reference(profile,manual=None):
    manual=manual or {};applicable=profile.get('applicable_level') or {}
    if not isinstance(applicable,dict):applicable={}
    raw=manual['level'] if 'level' in manual else applicable.get('labels',profile.get('applicable_levels',[]))
    labels=strings(raw)
    if not isinstance(raw,list):labels=[v.strip() for label in labels for v in re.split('[；;\n]',label) if v.strip()]
    levels=reference_levels(labels)
    return {'labels':labels,'levels':levels,'primary':'' if 'level' in manual else text(applicable.get('primary_label')),
            'reference_only':'key_bachelor' in levels and any('重点本科' in label and re.search('参照|参考',label) for label in labels),
            'reason':text(applicable.get('reason')),'limits':strings(applicable.get('usage_limits')),
            'manually_edited':'level' in manual}


def file_summary(files,inventory_complete=False):
    def counts(kind):
        rows=[f for f in files if f.get('kind')==kind]
        return {'registered':len(rows),'available':sum(f.get('availability')=='available' for f in rows),
                'missing':sum(f.get('availability')=='missing' for f in rows),
                'unknown':sum(f.get('availability','unknown') not in ('available','missing') for f in rows)}
    original,attachment=counts('原文'),counts('附件')
    return {'original':original,'attachment':attachment,'original_available':original['available']>0,
            'attachment_available':attachment['available'],'inventory_complete':bool(inventory_complete),
            'attachment_formats':sorted({str(f.get('name','')).rsplit('.',1)[-1].lower() for f in files
                if f.get('kind')=='附件' and f.get('availability')=='available' and '.' in f.get('name','')})}


def details(profile,manual=None):
    manual=manual or {};fields=display(profile)|manual
    research=profile.get('research_profile') or profile.get('research_understanding') or profile.get('research_process') or profile.get('research') or {}
    boundary=profile.get('migration_boundary') or profile.get('transfer_boundary') or {}
    applicable=profile.get('applicable_level') or {}
    units=[]
    for i,u in enumerate(profile.get('learning_units') or []):
        if not isinstance(u,dict):continue
        units.append({'id':text(u.get('id')) or str(i),'title':text(u.get('title') or u.get('name')) or f'学习单元 {i+1}',
            'core':u.get('is_core') is True,'question':descriptive(u.get('question')),
            'learn':descriptive(u.get('what_to_learn') or u.get('learning_value') or u.get('learn') or u.get('transfer')),
            'method':descriptive(u.get('method')),'evidence':descriptive(u.get('method_evidence') or u.get('evidence') or u.get('content')),
            'materials':strings(u.get('materials')),'steps':strings(u.get('steps')),'outputs':strings(u.get('outputs')),
            'interpretation':descriptive(u.get('interpretation')),'needs':strings(u.get('new_material_needs')),
            'limits':strings(u.get('limitations') or u.get('transfer_boundary')),'location':text(u.get('evidence_location'))})
    # Existing edit contract changes aggregate method/learning fields; do not present stale unit text as the edited value.
    if 'methods' in manual or 'learn' in manual:
        units=[{**u,**({'method':manual['methods'] if i==0 else ''} if 'methods' in manual else {}),
                **({'learn':manual['learn'] if i==0 else ''} if 'learn' in manual else {})} for i,u in enumerate(units)]
    gaps=[]
    for g in profile.get('gaps') or profile.get('gaps_and_limits') or []:
        if isinstance(g,dict):gaps.append({'type':text(g.get('type')) or 'uncertain','text':descriptive(g.get('description') or g),'impact':text(g.get('use_limit') or g.get('impact'))})
        elif isinstance(g,str):gaps.append({'type':'uncertain','text':g,'impact':''})
    return {'question':text(research.get('problem') or research.get('research_question') or research.get('overall_question')),
        'path':fields['path'] if 'path' in manual else fields['path'] or text(research.get('overall_path') or research.get('research_path') or research.get('path_summary')),
        'object':fields['object'],'summary':fields['summary'],'keywords':strings(fields['keywords']),
        'audience':fields['audience'] if 'audience' in manual else fields['audience'] or text(applicable.get('audience_labels')),
        'learn':fields['learn'] if 'learn' in manual else fields['learn'] or text(boundary.get('reusable_scope')),
        'units':units,'needs':strings(boundary.get('must_recollect') or boundary.get('new_task_requirement')),
        'limits':list(dict.fromkeys(strings(applicable.get('usage_limits'))+strings(boundary.get('global_limits'))+strings(profile.get('limitations')))),
        'gaps':gaps,'legacy_summary':'applicable_level' not in profile}

