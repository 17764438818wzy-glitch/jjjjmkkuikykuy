"""Per-library publication receipts, using native chunks and the existing trusted SSH helper."""
import json,re,subprocess,sys,uuid,hashlib
from pathlib import Path
from urllib.parse import quote
from engine import write_json
from ingest_source_metadata import verify_local
from research_profile import checked_envelope,bridge
from specialty_routing import load_catalog


def load_chunks(client,dataset,document):
    rows=[];seen=set();page=1;total=None
    while True:
        data=client.request('GET',f'/datasets/{quote(dataset,safe="")}/documents/{quote(document,safe="")}/chunks?page={page}&page_size=100')
        batch=data.get('chunks');count=data.get('total')
        if not isinstance(batch,list) or not isinstance(count,int) or (total is not None and count!=total):raise ValueError('chunk_inventory_incomplete')
        total=count
        for r in batch:
            if r.get('document_id')!=document or r.get('id') in seen:raise ValueError('chunk_document_binding_mismatch')
            seen.add(r['id']);rows.append(r)
        if len(rows)==total:break
        if not batch or len(rows)>total:raise ValueError('chunk_inventory_incomplete')
        page+=1
    return rows


class ProfileArtifacts:
    def __init__(self,client_factory,data_root):
        self.client_factory=client_factory;self.root=Path(data_root)/'private/research_profile_publications'
        self.root.mkdir(parents=True,exist_ok=True)
        self.helper=__import__('project_paths').ROOT / 'vendor/librechat_finance_bridge/remote_control.py'

    def record_path(self,task,kind,legacy=False):
        identity=[task.get('service_key') or task.get('service_url'),task['dataset_id'],task['source_id'],task['role']]
        namespace=hashlib.sha256(json.dumps(identity,separators=(',',':')).encode()).hexdigest()[:24]
        path=self.root/(namespace+'-'+str(task['id'])+'-'+kind+'.json')
        if legacy and not path.exists():
            request=self.root/(str(task['id'])+'-request.json')
            if request.exists():
                previous=json.loads(request.read_text(encoding='utf8'))
                if all(previous.get(k)==task.get(v) for k,v in (
                        ('dataset_id','dataset_id'),('document_id','remote_id'),('remote_name','remote_name'))):
                    return self.root/(str(task['id'])+'-'+kind+'.json')
        return path

    def __call__(self,task,remote):
        verify_local(task)
        material=task.get('policy_key')=='research_material'
        if material:
            from research_material import checked_material
            packet=checked_material(Path(task['package_path']))
        else:packet=checked_envelope(Path(task['package_path']))
        if packet.get('specialty_routing_policy')!='substantive-specialty/1.0' or not packet.get('routing_review'):
            raise ValueError('research_profile_professional_review_required')
        client=self.client_factory();dataset=task['dataset_id'];document=task['remote_id']
        expected_pipeline=task.get('execution_pipeline_id') or task['pipeline_id']
        if remote.get('id')!=document or remote.get('name')!=task['remote_name'] or remote.get('pipeline_id')!=expected_pipeline:raise ValueError('profile_remote_identity_mismatch')
        chunks=load_chunks(client,dataset,document)
        receipts=[];source=[]
        for c in chunks:
            content=c.get('content') or ''
            match=re.fullmatch(r'\[RP_RECEIPT_V1\](.*?)\[/RP_RECEIPT_V1\]',content,re.S)
            if match:receipts.append(json.loads(match.group(1)))
            else:source.append({'id':c['id'],'content':content})
        if len(receipts)!=1:raise ValueError('profile_native_receipt_missing')
        receipt=receipts[0]
        profession=next((m for m in load_catalog(Path(__file__).with_name('specialty_catalog.json'))['majors'] if m['paper_dataset_id']==dataset or material and m['image_dataset_id']==dataset),None)
        if not profession or profession['code'] not in receipt['codes'] or receipt['profile_revision']!=packet['profile_revision'] or receipt['source_sha256']!=packet['source_text_sha256'] or receipt['status']!='accepted':raise ValueError('profile_native_receipt_mismatch')
        if material:
            from generic_profile_publish import source_chunk_binding
            source_ids=source_chunk_binding(packet['source_text'],source)
            binding={'status':'verified','task_id':task['id'],'dataset_id':dataset,'document_id':document,
                'execution_pipeline_id':remote['pipeline_id'],'execution_begin_at':remote['process_begin_at'],
                'profile_revision':packet['profile_revision'],'source_chunk_ids':source_ids}
            patch={'research_material':json.dumps({'learning_use':packet['learning_use'],'use_review':packet['use_review'],'limitations':packet['limitations'],
                'routes':packet['routes'],'source_sha256':packet['source_text_sha256'],'binding':binding},ensure_ascii=False,separators=(',',':')),
                'research_profile_revision':packet['profile_revision'],'research_profile_admission':packet['admission']['status'],
                'research_profile_binding':json.dumps(binding,ensure_ascii=False,separators=(',',':')),'research_source_projection':'material_transport_v1'}
            request={'action':'material','task_id':task['id'],'dataset_id':dataset,'document_id':document,'remote_name':remote['name'],
                'upload_sha256':task['content_sha256'],'profile_revision':packet['profile_revision'],'receipt':receipt,'patch':patch,
                'execution_pipeline_id':remote['pipeline_id'],'execution_begin_at':remote['process_begin_at']}
            result=self._execute(task,request)
            if result.get('status')!='verified' or result.get('document_id')!=document:raise ValueError('material_publication_unconfirmed')
            try:verify_local(task)
            except Exception:
                self.invalidate(task);raise
            write_json(self.record_path(task,'receipt'),{**result,'source_chunk_ids':binding['source_chunk_ids'],'native_review':receipt})
            return
        # Establish exact native source ordering before mapping quotations across slice boundaries.
        from generic_profile_publish import source_chunk_binding
        source_ids=source_chunk_binding(packet['source_text'],source)
        by_id={c['id']:c for c in source};source=[by_id[i] for i in source_ids]
        projected=bridge({'op':'rebind','profile':packet['profile'],'original':packet['source_text'],'dataset_id':dataset,
            'document_id':document,'document_name':remote['name'],'chunks':source})
        projected['binding'].update(profile_revision=packet['profile_revision'],upload_sha256=task['content_sha256'],
            task_id=task['id'],execution_pipeline_id=remote['pipeline_id'],execution_begin_at=remote['process_begin_at'],status='verified')
        # Bind each immutable section to real source chunks, retaining empty associations as missing.
        blocks=json.loads((Path(task['package_path'])/'metadata/blocks.json').read_text(encoding='utf8'))
        if isinstance(blocks,dict):blocks=blocks.get('blocks',[])
        by_block={b['id']:b for b in blocks};tree=[]
        for section in packet['structure']['sections']:
            texts=[by_block[b].get('text','') for b in section['block_ids'] if b in by_block]
            matching=[c['id'] for c in source if any(len(t.strip())>=6 and (t.strip()[:80] in c['content'] or c['content'].strip()[:80] in t) for t in texts)]
            tree.append({**section,'chunk_ids':matching,'chunk_binding':'verified' if matching else 'pending'})
        jsonstr=lambda v:json.dumps(v,ensure_ascii=False,separators=(',',':'))
        patch={'research_exemplar':jsonstr(projected['profile']),'research_exemplar_version':projected['profile']['contract_version'],
            'research_exemplar_source_sha256':packet['source_text_sha256'],'research_profile_binding':jsonstr(projected['binding']),
            'research_profile_revision':packet['profile_revision'],'research_profile_completeness':packet['completeness']['status'],
            'research_profile_admission':packet['admission']['status'],'research_tree':jsonstr(tree),
            'research_chapter_coverage':jsonstr(packet['chapter_coverage']),'research_asset_roles':jsonstr(packet['asset_roles']),
            'research_applicable_specialties':jsonstr(packet['routes']),'research_source_transport_sha256':task['content_sha256'],
            'research_source_projection':'profile_transport_v1','research_source_education_packet':jsonstr(packet.get('source_education')),
            'research_source_education_verification':'pending','research_batch_background':jsonstr(packet['batch_background']),
            'research_original_major':jsonstr(packet.get('original_major')),'education_level':'unknown','education_basis':'[]'}
        if packet.get('admission_policy')=='verified-learning-uses/2':
            patch['research_learning_uses']=jsonstr({'policy':packet['admission_policy'],'profile_revision':packet['profile_revision'],
                'source_version':packet['source_text_sha256'],**packet['learning_uses']})
        request={'task_id':task['id'],'dataset_id':dataset,'document_id':document,'remote_name':remote['name'],
            'upload_sha256':task['content_sha256'],'profile_revision':packet['profile_revision'],'receipt':receipt,'patch':patch,
            'execution_pipeline_id':remote['pipeline_id'],'execution_begin_at':remote['process_begin_at']}
        result=self._execute(task,request)
        if result.get('status')!='verified' or result.get('document_id')!=document:raise ValueError('profile_publication_receipt_invalid')
        try:verify_local(task)
        except Exception:
            self.invalidate(task);raise
        write_json(self.record_path(task,'receipt'),{**result,'pipeline_id':remote['pipeline_id'],'source_chunk_ids':[c['id'] for c in source],
            'tree_pending_sections':[s['id'] for s in tree if s['chunk_binding']=='pending'],'native_review':receipt})

    def audit_specialties(self,task,audited_packet,native_audit_sha256,uncertainty_note=''):
        verify_local(task)
        previous=json.loads(self.record_path(task,'request',legacy=True).read_text(encoding='utf8'))
        assert all(previous.get(k)==task.get(v) for k,v in (('task_id','id'),('dataset_id','dataset_id'),
                   ('document_id','remote_id'),('remote_name','remote_name'),('upload_sha256','content_sha256'))),'specialty_audit_task_changed'
        request={k:previous[k] for k in ('task_id','dataset_id','document_id','remote_name','upload_sha256',
                                       'profile_revision','execution_pipeline_id','execution_begin_at')}
        request.update(action='specialty_audit',audit_packet=audited_packet,
                       native_audit_sha256=native_audit_sha256,uncertainty_note=uncertainty_note)
        result=self._execute(task,request,kind='specialty-audit-request')
        assert result.get('status')=='verified' and result.get('document_id')==task['remote_id'],'specialty_audit_publication_unconfirmed'
        assert result['profile_revision']==previous['profile_revision'] and result['audit_profile_revision']==audited_packet['profile_revision'],'specialty_audit_receipt_changed'
        write_json(self.record_path(task,'specialty-audit-receipt'),result)
        return result

    def _execute(self,task,request,kind='request'):
        request_path=self.record_path(task,kind);write_json(request_path,request)
        remote_path='/tmp/codex-profile-'+uuid.uuid4().hex+'.json'
        def execute(argv):
            result=subprocess.run([sys.executable,'-X','utf8','-B',str(self.helper),*argv],stdout=subprocess.PIPE,stderr=subprocess.PIPE,
                timeout=90,creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
            if result.returncode:raise ValueError('native_profile_publication_failed')
            return result.stdout.decode('utf8')
        execute(['--put',str(request_path),remote_path])
        container_path=remote_path
        command=f'chmod 600 {remote_path} && docker cp {remote_path} ragflow-server-ragflow-1:{container_path} && docker exec -w /ragflow ragflow-server-ragflow-1 python /ragflow/logs/research_profiles/publisher_v1.py {container_path}'
        output=execute(['--sudo',command]);return json.loads(output.strip().splitlines()[-1])

    def invalidate(self,task):
        request_path=self.record_path(task,'request',legacy=True)
        if request_path.exists():
            previous=json.loads(request_path.read_text(encoding='utf8'))
            request={k:previous[k] for k in ('task_id','dataset_id','document_id','remote_name','profile_revision','execution_pipeline_id','execution_begin_at')}
            request.update(action='invalidate')
            result=self._execute(task,request)
            if result.get('status') not in ('invalidated','superseded'):raise ValueError('profile_invalidation_unconfirmed')
        path=self.record_path(task,'receipt',legacy=True)
        if path.exists():
            receipt=json.loads(path.read_text(encoding='utf8'));receipt.update(status='invalidated',reason='remote_execution_changed');write_json(path,receipt)
