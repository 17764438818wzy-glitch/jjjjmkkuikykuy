"""Run in RAGFlow. Extend the existing additive ES CAS/ledger/readback pattern."""
import json,hashlib,logging,sys,os,copy,re
from pathlib import Path


def encoded(value):return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False).encode('utf8')


def source_chunk_binding(source,chunks):
    """Reproduce native source slices without relying on API ordering."""
    expected=[];start=0
    while start<len(source):
        end=min(start+1000,len(source))
        if end<len(source):
            at=source.rfind('\n',start+400,end)
            if at>start:end=at+1
        if source[start:end].strip():expected.append(source[start:end])
        start=end
    available={};seen=set()
    for chunk in chunks:
        if not chunk.get('id') or chunk['id'] in seen:raise ValueError('material_chunk_id_changed')
        seen.add(chunk['id']);available.setdefault(chunk['content'],[]).append(chunk['id'])
    ordered=[]
    for content in expected:
        ids=available.get(content,[])
        if not ids:raise ValueError('material_index_source_changed')
        ordered.append(ids.pop(0))
    if any(available.values()):raise ValueError('material_index_source_changed')
    return ordered


def validate_material_packet(packet):
    source=packet['source_text'];raw=source.encode('utf-16-le');proof=packet['use_review']
    if (proof.get('status') not in ('supported','contradicted','insufficient') or not proof.get('reason')
            or not proof.get('reviewer') or proof.get('source_version')!=packet['source_text_sha256']):
        raise ValueError('material_use_review_invalid')
    scope=proof['read_scope']
    if len({s['id'] for s in scope})!=len(scope):raise ValueError('material_use_scope_invalid')
    for span in scope:
        if span['start']<0 or span['end']<=span['start'] or raw[span['start']*2:span['end']*2].decode('utf-16-le')!=span['quote']:
            raise ValueError('material_use_source_changed')
    payload={'learning_use':packet['learning_use'],'author_spans':[{'id':s['id'],'text':s['quote']} for s in scope],
             'source_version':packet['profile']['source_version']}
    if proof.get('visual_sources'):payload['visual_sources']=proof['visual_sources']
    if proof['input_hash']!=hashlib.sha256(json.dumps(payload,sort_keys=True,ensure_ascii=False).encode()).hexdigest():
        raise ValueError('material_use_changed')
    refs=proof.get('span_ids',[])
    if proof['status']=='supported' and (not refs or len(refs)>12 or len(set(refs))!=len(refs) or not set(refs)<={s['id'] for s in scope}):
        raise ValueError('material_use_quotes_invalid')
    derived='limited' if proof['status']=='supported' and any(r.get('verification')=='verified' for r in packet['routes']) else 'pending'
    if packet['admission']['status']!=derived:raise ValueError('material_admission_changed')
    return derived


def metadata_state(es,index,document,dataset,read):
    # Reuse metadata_bootstrap's create-only, concurrent-read and confirmed-empty rules.
    status=lambda e:getattr(e,'status_code',None) or getattr(getattr(e,'meta',None),'status',None)
    try:hit=es.get(index=index,id=document)
    except Exception as error:
        if status(error)!=404:raise
        try:es.create(index=index,id=document,document={'id':document,'kb_id':dataset,'meta_fields':{}},refresh='wait_for')
        except Exception as error:
            if status(error)!=409:raise
        hit=es.get(index=index,id=document)
    native=hit['_source'];assert native.get('id')==document and native.get('kb_id')==dataset,'metadata_scope_mismatch'
    mapped=read();assert isinstance(mapped,dict),'metadata_read_failed'
    if document in mapped:return hit,mapped[document]
    fresh=es.get(index=index,id=document)
    assert fresh['_source'].get('id')==document and fresh['_source'].get('kb_id')==dataset and fresh['_source'].get('meta_fields')=={},'metadata_empty_unconfirmed'
    return fresh,{}


def invalidation_patch(old,revision,task_id,pipeline_id,begin=None):
    if old.get('research_profile_revision')!=revision:return None
    binding=json.loads(old.get('research_profile_binding','{}'))
    if binding.get('task_id')!=task_id or binding.get('execution_pipeline_id')!=pipeline_id:return None
    if begin is not None and binding.get('execution_begin_at')!=begin:return None
    binding.update(status='invalidated',reason='execution_or_local_postcheck_changed')
    return {**old,'research_profile_admission':'pending',
        'research_profile_binding':json.dumps(binding,ensure_ascii=False,separators=(',',':')),
        'research_exemplar':'','research_tree':'[]',**({'research_material':''} if 'research_material' in old else {}),
        **({'research_learning_uses':''} if 'research_learning_uses' in old else {})}


def specialty_audit_patch(old,request,packet,target_code,native_cases,body):
    """Supplement classification only, bound to an unchanged published execution."""
    from generic_profile_pipeline import PREPARE,PROMPT
    revision=packet['profile_revision'];audited=copy.deepcopy(request['audit_packet'])
    assert revision==request['profile_revision']==old.get('research_profile_revision'),'specialty_audit_revision_changed'
    binding=json.loads(old.get('research_profile_binding','{}'))
    assert all(binding.get(key)==request[key] for key in ('task_id','execution_pipeline_id','execution_begin_at')),'specialty_audit_execution_changed'
    audited_revision=audited.pop('profile_revision')
    assert hashlib.sha256(encoded(audited)).hexdigest()==audited_revision,'specialty_audit_packet_changed'
    audited['profile_revision']=audited_revision
    previous_audit=json.loads(old.get('research_specialty_routing_audit','{}'))
    if previous_audit:
        assert (previous_audit.get('audit_profile_revision')==audited_revision
                and previous_audit.get('native_audit_sha256')==request['native_audit_sha256']),'specialty_audit_competing_revision'
    allowed_changes={'routes','routing_review','routing_supplement','specialty_routing_policy','profile_revision'}
    assert all(packet.get(k)==audited.get(k) for k in (set(packet)|set(audited))-allowed_changes),'specialty_audit_facts_changed'
    supplement=audited['routing_supplement']
    assert (supplement.get('parent_profile_revision')==revision and supplement.get('source_unchanged') is True
            and supplement.get('core_and_coverage_facts_reused') is True),'specialty_audit_parent_changed'
    proof=audited['routing_review']
    assert (audited['specialty_routing_policy']==proof['policy']=='substantive-specialty/1.0'
            and proof['source_version']==packet['source_text_sha256']),'specialty_audit_proof_changed'
    before_routes={r['code']:r for r in packet['routes']};local={r['code']:r for r in proof['checks']}
    routes=copy.deepcopy(audited['routes'])
    assert (len({r['code'] for r in routes})==len(routes) and {r['code'] for r in routes}==set(before_routes)==set(local)
            and target_code in before_routes),'specialty_audit_scope_changed'
    for route in routes:
        assert all(route.get(k)==before_routes[route['code']].get(k) for k in
                   (set(route)|set(before_routes[route['code']]))-{'status','verification','review_reason'}),'specialty_audit_claim_changed'
    ns={};exec(PREPARE,ns)
    matches={};summary=[]
    for case in native_cases.values():
        if (case.get('source_sha256')!=packet['source_sha256'] or case.get('source_profile_revision')!=revision
                or case.get('audit_profile_revision')!=audited_revision):continue
        code=case['code'];assert code in before_routes and code not in matches,'specialty_audit_native_scope_changed'
        if code==target_code:assert case['task_id']==request['task_id'],'specialty_audit_native_task_changed'
        assert (case['source_text_sha256']==packet['source_text_sha256'] and case['status']=='verified'
                and case['model_id']=='33730f8686db4448934003a137207258'
                and case['instance_id']=='e0fc2eb0c29c11f1bac4857f73fd5f14'
                and case['requested_model']==case['served_model']=='deepseek-v4-pro'),'specialty_audit_native_binding_changed'
        ingest=body+'\n\n[RESEARCH_PROFILE_V1]\n'+json.dumps(audited,ensure_ascii=False,separators=(',',':'))+'\n[/RESEARCH_PROFILE_V1]'
        payload=ns['main']([{'text':ingest}],'RP_'+code+'__audit.md')[0]['text']
        assert hashlib.sha256((PROMPT+'\n'+payload).encode('utf8')).hexdigest()==case['request_sha256'],'specialty_audit_native_input_changed'
        assert case['review']['code']==code and case['review']['status'] in ('supported','insufficient','contradicted'),'specialty_audit_native_review_changed'
        matches[code]=case
    for route in routes:
        code=route['code'];check=local[code];native=matches.get(code)
        verified=(route.get('verification')=='verified' and check.get('status')=='supported'
                  and native is not None and native['review']['status']=='supported')
        route['verification']='verified' if verified else 'pending';route['status']='verified' if verified else 'pending'
        reason=check['reason'] if native is None else native['review']['reason']
        route['review_reason']=reason
        summary.append({'code':code,'status':'supported' if verified else 'specialty_pending',
                        'local_status':check['status'],'native_status':native['review']['status'] if native else 'not_requested',
                        'reason':reason,'native_request_sha256':native['request_sha256'] if native else None})
    current=next(r for r in routes if r['code']==target_code)
    after=copy.deepcopy(old)
    if current['verification']=='verified':assert binding.get('status')=='verified','specialty_audit_old_admission_changed'
    if current['verification']!='verified':
        after=invalidation_patch(old,revision,request['task_id'],request['execution_pipeline_id'],request['execution_begin_at'])
        assert after is not None,'specialty_audit_superseded'
        held=json.loads(after['research_profile_binding']);held.update(status='classification_pending',reason='classification:specialty_pending')
        after['research_profile_binding']=json.dumps(held,ensure_ascii=False,separators=(',',':'))
    after['research_applicable_specialties']=json.dumps(routes,ensure_ascii=False,separators=(',',':'))
    after['research_specialty_routing_audit']=json.dumps({
        'policy':proof['policy'],'source_profile_revision':revision,'audit_profile_revision':audited_revision,
        'source_text_sha256':packet['source_text_sha256'],'local_input_hash':proof['input_hash'],
        'native_audit_sha256':request['native_audit_sha256'],'target_code':target_code,
        'task_id':request['task_id'],'execution_pipeline_id':request['execution_pipeline_id'],
        'execution_begin_at':request['execution_begin_at'],'status':current['verification'],
        'checks':summary,'uncertainty_note':request.get('uncertainty_note','')},ensure_ascii=False,separators=(',',':'))
    return after


def main(request_path):
    logging.disable(logging.CRITICAL)
    from common import settings
    settings.init_settings()
    from api.db.db_models import Knowledgebase,Document
    from api.db.services.doc_metadata_service import DocMetadataService as M
    from api.db.services.file2document_service import File2DocumentService
    request=json.loads(Path(request_path).read_text(encoding='utf8'))
    catalog=json.loads(Path('/ragflow/logs/research_profiles/specialty_catalog.json').read_text(encoding='utf8'))
    allowed={r['paper_dataset_id']:r for r in catalog['majors']}
    if request.get('action')=='material':allowed.update({r['image_dataset_id']:r for r in catalog['majors']})
    assert request['dataset_id'] in allowed,'dataset_scope_mismatch'
    kb=Knowledgebase.get_by_id(request['dataset_id']);doc=Document.get_by_id(request['document_id'])
    assert kb.tenant_id=='88fd7476b00e11f199856dc9cfded896' and doc.kb_id==kb.id and doc.name==request['remote_name'],'remote_identity_mismatch'
    es=settings.docStoreConn.es;index=M._get_doc_meta_index_name(kb.tenant_id)
    if not es.indices.exists(index=index):
        assert settings.docStoreConn.create_doc_meta_idx(index) is not False,'metadata_index_unavailable'
    if request.get('action')=='invalidate':
        hit,old=metadata_state(es,index,doc.id,kb.id,lambda:M.get_metadata_for_documents([doc.id],kb.id))
        after=invalidation_patch(old,request['profile_revision'],request['task_id'],request['execution_pipeline_id'],request['execution_begin_at'])
        if after is None:
            print(json.dumps({'status':'superseded','document_id':doc.id}));return
        folder=Path('/ragflow/logs/research_profiles');folder.mkdir(parents=True,exist_ok=True)
        with (folder/'publication-ledger.jsonl').open('a',encoding='utf8') as log:
            event={'operation':'invalidate','task_id':request['task_id'],'document_id':doc.id,
                'profile_revision':request['profile_revision'],'execution_begin_at':request['execution_begin_at'],
                'seq_no':hit['_seq_no'],'primary_term':hit['_primary_term'],'before':old,'after':after}
            log.write(json.dumps(event,ensure_ascii=False)+'\n');log.flush();os.fsync(log.fileno())
            es.index(index=index,id=doc.id,document={**hit['_source'],'meta_fields':M._split_combined_values(after)},
                if_seq_no=hit['_seq_no'],if_primary_term=hit['_primary_term'],refresh='wait_for')
            assert M.get_metadata_for_documents([doc.id],kb.id)[doc.id]==after,'invalidation_readback_mismatch'
            log.write(json.dumps({'operation':'invalidate','document_id':doc.id,'committed':True})+'\n');log.flush();os.fsync(log.fileno())
        print(json.dumps({'status':'invalidated','document_id':doc.id,'metadata_readback':True}));return
    assert str(doc.run)=='3' and doc.progress>=1,'remote_execution_changed'
    from datetime import datetime
    assert doc.pipeline_id==request['execution_pipeline_id'] and datetime.fromisoformat(str(doc.process_begin_at))==datetime.fromisoformat(request['execution_begin_at']),'remote_execution_changed'
    bucket,location=File2DocumentService.get_storage_address(doc.id);raw=settings.STORAGE_IMPL.get(bucket,location)
    assert hashlib.sha256(raw).hexdigest()==request['upload_sha256'],'uploaded_source_changed'
    material=request.get('action')=='material'
    tag='RESEARCH_MATERIAL_V1' if material else 'RESEARCH_PROFILE_V1'
    text=raw.decode('utf-8-sig').replace('\r\n','\n');marker='\n\n['+tag+']\n'
    body,transport=text.rsplit(marker,1);packet=json.loads(transport.split('\n[/'+tag+']',1)[0])
    revision=packet.pop('profile_revision');assert revision==hashlib.sha256(encoded(packet)).hexdigest(),'packet_revision_mismatch';packet['profile_revision']=revision
    assert revision==request['profile_revision'] and hashlib.sha256(body.encode('utf8')).hexdigest()==packet['transport_body_sha256'],'transport_body_changed'
    source=packet['source_text'];assert hashlib.sha256(source.encode('utf8')).hexdigest()==packet['source_text_sha256'],'projection_source_changed'
    if material:
        validate_material_packet(packet)
        assert packet['schema']=='research-material-envelope/1.0' and packet['admission']['status']=='limited','material_admission_hold'
        route=next(r for r in packet['routes'] if r['code']==allowed[kb.id]['code'])
        assert route['verification']=='verified' and allowed[kb.id]['code'] in request['receipt']['codes'],'material_specialty_pending'
        assert (request['receipt']['profile_revision']==revision and request['receipt']['source_sha256']==packet['source_text_sha256']
                and request['receipt']['status']=='accepted'),'material_receipt_changed'
        patch=request['patch'];binding=json.loads(patch['research_profile_binding']);meta=json.loads(patch['research_material'])
        assert (binding['task_id']==request['task_id'] and binding['dataset_id']==kb.id and binding['document_id']==doc.id
                and binding['execution_pipeline_id']==doc.pipeline_id and binding['execution_begin_at']==request['execution_begin_at']
                and binding['profile_revision']==revision and binding['source_chunk_ids']),'material_binding_changed'
        assert meta=={'learning_use':packet['learning_use'],'use_review':packet['use_review'],'limitations':packet['limitations'],'routes':packet['routes'],
            'source_sha256':packet['source_text_sha256'],'binding':binding},'material_claim_changed'
        assert patch['research_profile_revision']==revision and patch['research_profile_admission']==packet['admission']['status'],'material_admission_metadata_changed'
        assert set(patch)=={'research_material','research_profile_revision','research_profile_admission','research_profile_binding','research_source_projection'},'material_publication_keys_changed'
        from rag.nlp.search import index_name
        from common.doc_store.doc_store_base import OrderByExpr
        native_index=index_name(kb.tenant_id)
        inventory=settings.docStoreConn.search(['id','content_with_weight','doc_id','kb_id'],[],{'doc_id':doc.id,'kb_id':[kb.id]},[],OrderByExpr(),0,5000,native_index,[kb.id])
        total=settings.docStoreConn.get_total(inventory)
        native_fields=settings.docStoreConn.get_fields(inventory,['id','content_with_weight','doc_id','kb_id'])
        assert total==len(native_fields) and total<=5000,'material_index_inventory_incomplete'
        actual=[];native_receipts=[]
        for ident,row in native_fields.items():
            assert row['doc_id']==doc.id and kb.id in row['kb_id'],'material_index_document_changed'
            content=row['content_with_weight']
            match=re.fullmatch(r'\[RP_RECEIPT_V1\](.*?)\[/RP_RECEIPT_V1\]',content,re.S)
            if match:native_receipts.append(json.loads(match.group(1)))
            else:actual.append({'id':ident,'content':content})
        assert native_receipts==[request['receipt']],'material_native_receipt_changed'
        assert source_chunk_binding(source,actual)==binding['source_chunk_ids'],'material_source_chunk_binding_changed'
        hit,old=metadata_state(es,index,doc.id,kb.id,lambda:M.get_metadata_for_documents([doc.id],kb.id))
        assert not old.get('research_profile_revision') or old['research_profile_revision']==revision,'material_competing_revision'
        after={**old,**patch};folder=Path('/ragflow/logs/research_profiles');folder.mkdir(parents=True,exist_ok=True)
        with (folder/'publication-ledger.jsonl').open('a',encoding='utf8') as log:
            log.write(json.dumps({'operation':'publish_material','task_id':request['task_id'],'document_id':doc.id,
                'dataset_id':kb.id,'before':old,'after':after},ensure_ascii=False)+'\n');log.flush();os.fsync(log.fileno())
            if old!=after:es.index(index=index,id=doc.id,document={**hit['_source'],'meta_fields':M._split_combined_values(after)},
                if_seq_no=hit['_seq_no'],if_primary_term=hit['_primary_term'],refresh='wait_for')
            assert M.get_metadata_for_documents([doc.id],kb.id)[doc.id]==after,'material_metadata_readback_changed'
            log.write(json.dumps({'operation':'publish_material','document_id':doc.id,'committed':True})+'\n');log.flush();os.fsync(log.fileno())
        print(json.dumps({'status':'verified','document_id':doc.id,'dataset_id':kb.id,'profile_revision':revision,'metadata_readback':True}));return
    if request.get('action')=='specialty_audit':
        audit_sha=request['native_audit_sha256'];assert re.fullmatch(r'[0-9a-f]{64}',audit_sha),'specialty_audit_file_invalid'
        audit_bytes=(Path('/ragflow/logs/research_profiles/audits')/(audit_sha+'.json')).read_bytes()
        assert hashlib.sha256(audit_bytes).hexdigest()==audit_sha,'specialty_audit_file_changed'
        native_cases=json.loads(audit_bytes)['cases']
        hit,old=metadata_state(es,index,doc.id,kb.id,lambda:M.get_metadata_for_documents([doc.id],kb.id))
        after=specialty_audit_patch(old,request,packet,allowed[kb.id]['code'],native_cases,body)
        folder=Path('/ragflow/logs/research_profiles');folder.mkdir(parents=True,exist_ok=True)
        with (folder/'publication-ledger.jsonl').open('a',encoding='utf8') as log:
            event={'operation':'specialty_audit','task_id':request['task_id'],'document_id':doc.id,'dataset_id':kb.id,
                'profile_revision':revision,'audit_profile_revision':request['audit_packet']['profile_revision'],
                'execution_begin_at':request['execution_begin_at'],'seq_no':hit['_seq_no'],'primary_term':hit['_primary_term'],
                'before':old,'after':after,'native_audit_sha256':audit_sha}
            log.write(json.dumps(event,ensure_ascii=False)+'\n');log.flush();os.fsync(log.fileno())
            if old!=after:es.index(index=index,id=doc.id,document={**hit['_source'],'meta_fields':M._split_combined_values(after)},
                if_seq_no=hit['_seq_no'],if_primary_term=hit['_primary_term'],refresh='wait_for')
            assert M.get_metadata_for_documents([doc.id],kb.id)[doc.id]==after,'specialty_audit_readback_mismatch'
            log.write(json.dumps({'operation':'specialty_audit','document_id':doc.id,'committed':True})+'\n');log.flush();os.fsync(log.fileno())
        audit=json.loads(after['research_specialty_routing_audit'])
        print(json.dumps({'status':'verified','document_id':doc.id,'dataset_id':kb.id,'profile_revision':revision,
                          'audit_profile_revision':audit['audit_profile_revision'],'target_verification':audit['status'],
                          'metadata_readback':True}));return
    assert allowed[kb.id]['code'] in request['receipt']['codes'],'unverified_specialty_target'
    patch=request['patch'];profile=json.loads(patch['research_exemplar']);binding=json.loads(patch['research_profile_binding'])
    assert profile['dataset_id']==kb.id and profile['document_id']==doc.id and profile['source_version']==packet['source_text_sha256'],'profile_target_mismatch'
    def neutral(p):
        p=copy.deepcopy(p)
        for key in ('dataset_id','document_id','document_name','semantic_review'):p.pop(key,None)
        for e in p['evidence']:e.pop('chunk_ids',None)
        return p
    assert neutral(profile)==neutral(packet['profile']),'profile_projection_claim_changed'
    proof=packet['profile']['semantic_review'];projected=profile['semantic_review']
    assert proof['checks']==projected['checks'] and proof['read_scopes']==projected['read_scopes'],'source_review_changed'
    assert binding['source_input_hash']==proof['input_hash'] and binding['target_input_hash']==projected['input_hash'],'binding_proof_mismatch'
    expected={'research_exemplar','research_exemplar_version','research_exemplar_source_sha256','research_profile_binding',
        'research_profile_revision','research_profile_completeness','research_profile_admission','research_tree','research_chapter_coverage',
        'research_asset_roles','research_applicable_specialties','research_source_transport_sha256','research_source_projection',
        'research_source_education_packet','research_source_education_verification','research_batch_background',
        'research_original_major','education_level','education_basis'}
    if packet.get('admission_policy')=='verified-learning-uses/2':
        from research_profile import learning_uses,admission
        uses=learning_uses(packet['profile'])
        assert uses==packet['learning_uses'],'learning_uses_changed'
        assert json.loads(patch['research_learning_uses'])=={'policy':packet['admission_policy'],'profile_revision':revision,
            'source_version':packet['source_text_sha256'],**uses},'learning_uses_metadata_changed'
        derived=admission(packet['profile'])
        if packet['completeness']['status']=='partial' and derived['status']=='recommended':derived['status']='limited'
        assert derived==packet['admission'] and patch['research_profile_admission']==derived['status'],'profile_admission_metadata_changed'
        expected.add('research_learning_uses')
    assert set(patch)==expected,'publication_keys_changed'
    hit,old=metadata_state(es,index,doc.id,kb.id,lambda:M.get_metadata_for_documents([doc.id],kb.id))
    native=hit['_source'];assert native['kb_id']==kb.id
    existing=old.get('research_profile_revision')
    assert not existing or existing==revision,'competing_profile_revision'
    after={**old,**patch};folder=Path('/ragflow/logs/research_profiles');folder.mkdir(parents=True,exist_ok=True)
    with (folder/'publication-ledger.jsonl').open('a',encoding='utf8') as log:
        log.write(json.dumps({'task_id':request['task_id'],'document_id':doc.id,'dataset_id':kb.id,'before':old,'after':after,
            'seq_no':hit['_seq_no'],'primary_term':hit['_primary_term'],'operation':'publish'},ensure_ascii=False)+'\n');log.flush();os.fsync(log.fileno())
        if old!=after:es.index(index=index,id=doc.id,document={**native,'meta_fields':M._split_combined_values(after)},
            if_seq_no=hit['_seq_no'],if_primary_term=hit['_primary_term'],refresh='wait_for')
        read=M.get_metadata_for_documents([doc.id],kb.id)[doc.id];assert read==after,'publication_readback_mismatch'
        log.write(json.dumps({'document_id':doc.id,'committed':True})+'\n');log.flush();os.fsync(log.fileno())
    print(json.dumps({'status':'verified','document_id':doc.id,'dataset_id':kb.id,'profile_revision':revision,'metadata_readback':True}))


if __name__=='__main__':main(sys.argv[1])
