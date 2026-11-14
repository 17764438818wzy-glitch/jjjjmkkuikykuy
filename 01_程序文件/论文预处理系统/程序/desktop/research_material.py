"""Research attachments reuse the existing evidence package and professional review."""
import copy
import hashlib
import io
import json
from pathlib import Path
from engine import write_json,digest
from knowledge_package import encoded,hash_file
from research_profile import ProfileProcessor,bridge,_route_review_payload
from specialty_routing import confirmed_routes


def material_carrier(source,material,work):
    """Derived container only. The input hash and private original remain authoritative."""
    from docx import Document
    from docx.shared import Inches
    work=Path(work);document=Document()
    if material.get('images'):
        for blob in material['images']:document.add_picture(io.BytesIO(blob),width=Inches(6))
    if material.get('text'):
        for line in material['text'].splitlines():document.add_paragraph(line)
    target=work/'material_carrier.docx';document.save(target)
    write_json(work/'material_derivation.json',{'original_sha256':digest(Path(source)),
        'carrier_sha256':digest(target),'method':'native-image-or-text-container','executed':False})
    return target


def process_material(package,agent,data_root,progress):
    package=Path(package);manifest=json.loads((package/'manifest.json').read_text(encoding='utf8'))
    source=(package/'document.md').read_text(encoding='utf8')
    catalog_source=bridge({'op':'catalog','original':source})
    spans=[{'id':'s'+str(i+1),'text':s['text']} for i,s in enumerate(catalog_source['spans'])]
    if sum(len(s['text']) for s in spans)>120000:raise ValueError('material_document_over_current_read_budget')
    p=ProfileProcessor(agent,data_root,progress=progress)
    p.receipts=Path(data_root)/'private/research_materials'/manifest['source_sha256'];p.receipts.mkdir(parents=True,exist_ok=True)
    previous=checked_material(package) if (package/'research_material.json').exists() else None
    groups=list({r['group_code']:{'code':r['group_code'],'name':r['group']} for r in p.catalog['majors']}.values())
    progress('材料用途画像','复用已脱敏材料，记录可学习用途和专业依据')
    def check_groups(r):
        if not isinstance(r.get('group_codes'),list) or not set(r['group_codes'])<={g['code'] for g in groups}:
            raise ValueError('material_group_invalid')
        if not isinstance(r.get('learning_use'),str) or not r['learning_use'].strip():raise ValueError('material_use_missing')
        return r
    context,_=p.invoke('context','为研究附件记录实际可学的一个独立用途和对应专业类，如材料结构、设计表达、真实实现过程。不要把来源或专业归属当作方法正确性。'
        '不强造论文目录、研究结论或统计过程。'
        '只用已提供内容；输出 {"group_codes":["给定代码"],"learning_use":"实际可以学习什么","limitations":["明确使用限制"]}。',
        {'specialty_groups':groups,'author_spans':spans},check_groups)
    choices=[{'code':r['code'],'name':r['name'],'group':r['group']} for r in p.catalog['majors'] if r['group_code'] in context['group_codes']]
    routes,_=p.invoke('routes','选择此附件实际设计、实现或描述的专业，可多选。泛用代码、背景提及不能扩大专业归属。'
        '独立图纸、代码无需有论文摘要或结论；用可见设计对象、真实实现和已有产物核对。'
        '输出 {"routes":[{"code":"给定代码","reason":"实质归属及用途","span_ids":["真实编号"]}]}，不能确认用空数组。',
        {'candidates':choices,'author_spans':spans,'learning_use':context['learning_use']},
        lambda r:p._check_routes(r,choices,{s['id'] for s in spans}))
    evidence=[]
    for row in routes['routes']:
        row['evidence_ids']=list(row['span_ids'])
        for ident in row['span_ids']:
            position=catalog_source['spans'][int(ident[1:])-1]
            if not any(e['id']==ident for e in evidence):evidence.append({'id':ident,'quote':position['text'],'start':position['start'],'end':position['end']})
    profile={'source_version':catalog_source['source_version'],'evidence':evidence}
    review=p.review_routes(routes['routes'],profile,source)
    checks={c['code']:c for c in review['checks']}
    for row in routes['routes']:
        row.update(verification='verified' if checks[row['code']]['status']=='supported' else 'pending')
    use_payload={'learning_use':context['learning_use'],'author_spans':spans,'source_version':profile['source_version']}
    visual=[];visual_scope=[]
    from knowledge_package import safe_path
    for entry in manifest['files']:
        if entry['path'].startswith('assets/') and Path(entry['path']).suffix.lower() in ('.png','.jpg','.jpeg','.webp'):
            if len(visual)>=3:break
            blob=safe_path(package,entry['path']).read_bytes()
            if sum(len(b) for b in visual)+len(blob)>8*1024*1024:break
            visual.append(blob);visual_scope.append({'path':entry['path'],'sha256':hashlib.sha256(blob).hexdigest()})
    if visual_scope:use_payload['visual_sources']=visual_scope
    def check_use(r):
        if (r.get('status') not in ('supported','contradicted','insufficient') or not isinstance(r.get('reason'),str)
                or not r['reason'].strip()):raise ValueError('material_use_review_invalid')
        refs=r.get('span_ids',[])
        if r['status']=='supported' and (not refs or not set(refs)<={s['id'] for s in spans} or len(refs)>12 or len(refs)!=len(set(refs))):raise ValueError('material_use_review_quotes_invalid')
        return r
    use,_=p.invoke('use_review','独立核实当前一个学习用途是否有原文依据，并能作为新题结构、方法或表达的范例。'
        '不要求外部核验旧数据。公式、推理、实现若有已明确错误，不能放行依赖它的用途；仍有独立用途时只允许真实限定范围。'
        '所附图片是同一份已经脱敏的原图，核对其实际表达和组织即可；不要要求证明工程可行性才能学习图纸的表达。'
        '只核对候选用途，不为通过改成别的用途；没有足够实际内容用insufficient，反证用contradicted。'
        '输出 {"status":"supported/contradicted/insufficient","reason":"实际依据或问题","span_ids":["足够支持这一用途的真实编号"]}。',use_payload,check_use,images=visual)
    use_proof={**use,'input_hash':hashlib.sha256(json.dumps(use_payload,sort_keys=True,ensure_ascii=False).encode()).hexdigest(),
        'source_version':profile['source_version'],'reviewer':p.model,
        **({'visual_sources':visual_scope} if visual_scope else {}),
        'read_scope':[{'id':'s'+str(i+1),'start':s['start'],'end':s['end'],'quote':s['text']} for i,s in enumerate(catalog_source['spans'])]}
    result={'schema':'research-material-envelope/1.0','profile':profile,'source_text':source,
        'source_text_sha256':hashlib.sha256(source.encode('utf8')).hexdigest(),'source_sha256':manifest['source_sha256'],
        'paper_id':manifest['paper_id'],'routes':routes['routes'],'routing_review':review,
        'specialty_routing_policy':'substantive-specialty/1.0','learning_use':context['learning_use'],
        'limitations':context.get('limitations',[])+['仅核实所述学习用途；未核实所有原数据、公式或软件功能','原件仅作范例；新题重新取得材料和完成分析'],
        'use_review':use_proof,
        'completeness':{'status':'complete' if use['status']=='supported' and not manifest.get('unrecovered_objects') else 'partial','scope':'material_use_description'},
        'admission':{'status':'limited' if use['status']=='supported' and confirmed_routes(routes,p.catalog) else 'pending'}}
    text=(package/'ingest.md').read_text(encoding='utf8')
    if previous:
        text=text.rsplit('\n\n[RESEARCH_MATERIAL_V1]\n',1)[0]
        if hashlib.sha256(text.encode('utf8')).hexdigest()!=previous['transport_body_sha256']:raise ValueError('material_previous_body_changed')
        archive=p.receipts/'superseded'/previous['profile_revision'];archive.mkdir(parents=True,exist_ok=True)
        write_json(archive/'research_material.json',previous)
    result['transport_body_sha256']=hashlib.sha256(text.encode('utf8')).hexdigest()
    result['profile_revision']=hashlib.sha256(encoded(result)).hexdigest()
    write_json(package/'research_material.json',result)
    (package/'ingest.md').write_text(text+'\n\n[RESEARCH_MATERIAL_V1]\n'+json.dumps(result,ensure_ascii=False,separators=(',',':'))+'\n[/RESEARCH_MATERIAL_V1]\n',encoding='utf8')
    manifest['entrypoints']['research_material']='research_material.json'
    manifest['files']=[{**f,'sha256':hash_file(package/f['path'])} if f['path']=='ingest.md' else f for f in manifest['files'] if f['path']!='research_material.json']
    manifest['files'].append({'path':'research_material.json','sha256':hash_file(package/'research_material.json')})
    manifest.pop('revision',None);manifest['revision']=hashlib.sha256(encoded(manifest)).hexdigest();write_json(package/'manifest.json',manifest)
    return result


def checked_material(package):
    from knowledge_package import knowledge_files
    package=Path(package);knowledge_files(package,require_stable_paper_id=True)
    result=json.loads((package/'research_material.json').read_text(encoding='utf8'));revision=result.pop('profile_revision')
    if revision!=hashlib.sha256(encoded(result)).hexdigest():raise ValueError('material_revision_changed')
    result['profile_revision']=revision;source=(package/'document.md').read_text(encoding='utf8')
    if source!=result['source_text'] or hashlib.sha256(source.encode('utf8')).hexdigest()!=result['source_text_sha256']:raise ValueError('material_source_changed')
    evidence={e['id']:e for e in result['profile']['evidence']};raw=source.encode('utf-16-le')
    for e in evidence.values():
        if raw[e['start']*2:e['end']*2].decode('utf-16-le')!=e['quote']:raise ValueError('material_quote_changed')
    if result['profile']['source_version']!=result['source_text_sha256']:raise ValueError('material_source_version_changed')
    p=ProfileProcessor.__new__(ProfileProcessor)
    from specialty_routing import load_catalog
    p.catalog=load_catalog(Path(__file__).with_name('specialty_catalog.json'))
    payload=_route_review_payload(result['routes'],result['profile'],source,p.catalog)
    if result['routing_review']['input_hash']!=hashlib.sha256(json.dumps(payload,sort_keys=True,ensure_ascii=False).encode()).hexdigest():raise ValueError('material_review_changed')
    checks={c['code']:c for c in result['routing_review']['checks']}
    from research_profile import _check_route_review
    _check_route_review({'routes':list(checks.values())},result['routes'],{s['id'] for s in payload['author_spans']})
    if (result['routing_review']['source_version']!=result['profile']['source_version'] or not result['routing_review'].get('reviewer')):raise ValueError('material_route_review_invalid')
    positions={'s'+str(i+1):s for i,s in enumerate(bridge({'op':'catalog','original':source})['spans'])}
    for check in checks.values():
        if check['status']=='supported':
            expected=[{'id':ident,'start':positions[ident]['start'],'end':positions[ident]['end'],'quote':positions[ident]['text']} for ident in check['basis']['span_ids']]
            if expected!=check['basis']['source_spans']:raise ValueError('material_route_basis_changed')
    for route in result['routes']:
        if route['verification']!=('verified' if checks[route['code']]['status']=='supported' else 'pending'):raise ValueError('material_route_decision_changed')
    use_payload={'learning_use':result['learning_use'],'author_spans':payload['author_spans'],'source_version':result['profile']['source_version']}
    proof=result['use_review']
    if proof.get('visual_sources'):
        use_payload['visual_sources']=proof['visual_sources']
        from knowledge_package import safe_path
        for asset in proof['visual_sources']:
            if hash_file(safe_path(package,asset['path']))!=asset['sha256']:raise ValueError('material_use_image_changed')
    expected_scope=[{'id':ident,'start':s['start'],'end':s['end'],'quote':s['text']} for ident,s in positions.items()]
    if proof.get('read_scope')!=expected_scope:raise ValueError('material_use_scope_changed')
    from generic_profile_publish import validate_material_packet
    validate_material_packet(result)
    if proof['input_hash']!=hashlib.sha256(json.dumps(use_payload,sort_keys=True,ensure_ascii=False).encode()).hexdigest():raise ValueError('material_use_changed')
    if proof['status']=='supported' and (not proof.get('span_ids') or not set(proof['span_ids'])<={s['id'] for s in use_payload['author_spans']}):raise ValueError('material_use_quotes_invalid')
    if result['admission']['status']!=('limited' if proof['status']=='supported' and confirmed_routes(result,p.catalog) else 'pending'):raise ValueError('material_admission_changed')
    return result
