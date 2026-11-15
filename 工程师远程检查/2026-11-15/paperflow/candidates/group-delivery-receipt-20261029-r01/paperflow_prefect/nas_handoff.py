"""Evidence-bound stage 4 output. No upload, NAS access, model call or deletion."""
import copy
import hashlib
from pathlib import Path
import re

from .records import identity, read_json, sha_file
from .stage3_handoff import read_receipt
from .intake import verify_inventory

SCHEMA = 'paperflow-nas-handoff/1'
HEX = re.compile(r'[0-9a-f]{64}')
CACHE_KINDS = {'extracted_text','converted_preview','temporary_image','unpacked_member','working_copy'}
PROTECTED_WORDS = {'request','response','receipt','manifest','package','bundle','candidate',
                   'validation','inventory','journal','association','index','completed'}


def _require(condition, reason):
    if not condition: raise ValueError(reason)


def object_fingerprint(info):
    return {'device':info.st_dev,'inode':info.st_ino,'birth_ns':getattr(info,'st_birthtime_ns',None)}


def _ref(path):
    p=Path(path).resolve(); _require(p.is_file(),'evidence_file_missing')
    return {'path':str(p),'sha256':sha_file(p)}


def _read(ref):
    _require(sha_file(ref['path'])==ref['sha256'],'evidence_file_changed')
    value=read_json(ref['path'])
    _require(sha_file(ref['path'])==ref['sha256'],'evidence_changed_during_read')
    return value


def _add(refs, ref):
    if ref is not None:
        previous=refs.setdefault(ref['path'],ref['sha256'])
        _require(previous==ref['sha256'],'conflicting_evidence_hash')


def _body(paper, ref, prepared_ref, scope, refs):
    _add(refs,ref); _add(refs,prepared_ref)
    if ref is None: return {'verified':False,'receipt':None,'targets':{},'codes':[]}, ['body_receipt_missing']
    r=_read(ref); errors=[]; task=r.get('task') or {}; targets=r.get('targets') or {}; codes=list(targets)
    if r.get('fixture') and scope!='isolated_fixture': errors.append('synthetic_rag_evidence')
    if scope=='isolated_fixture' and r.get('fixture') is not True: errors.append('fixture_marker_missing')
    if r.get('schema')!='paper-preextract-delivery/1.0' or r.get('status')!='complete' or r.get('stage')!='verified':
        errors.append('body_not_verified')
    if task.get('source_sha256')!=paper['body']['sha256']: errors.append('body_source_mismatch')
    if not task.get('task_id') or not HEX.fullmatch(r.get('manifest_sha256','')): errors.append('body_identity_missing')
    packet=None; checker=None
    if scope=='production':
        if prepared_ref is None: errors.append('body_package_missing')
        elif not errors:
            from preextract_publish import _paperflow
            from paperflow.rag import RagPublisher
            prepared=_read(prepared_ref)
            package=_paperflow(prepared['paperflow_root'])
            packet=package.validate_package(prepared['package_path'],prepared['task'],prepared['catalog'],strict_seal=True)
            _require(prepared['task']==task and packet['manifest_sha256']==r['manifest_sha256'], 'body_sealed_package_mismatch')
            _require(prepared['validation']['embedding']['admission_verified'] is True, 'body_embedding_admission_missing')
            codes=[row['code'] for row in packet['routes']]; checker=RagPublisher(None)
            # All package members are recovery evidence, never disposable intermediates.
            package_path=Path(prepared['package_path']).resolve()
            paths=[package_path] if package_path.is_file() else sorted(p for p in package_path.rglob('*') if p.is_file())
            for p in paths:_add(refs,_ref(p))
    if not codes or set(codes)!=set(targets):errors.append('body_targets_incomplete')
    required_roles={role for role,records in checker.records(packet).items() if records} if checker else {'body'}
    for code,target in targets.items():
        if target.get('verified') is not True or target.get('retrieval_verified') is not True:
            errors.append('body_target_not_verified')
        roles={k for k,v in target.items() if isinstance(v,dict)}
        if not required_roles.issubset(roles) or not roles.issubset({'body','images'}):errors.append('body_roles_incomplete')
        for role in roles:
            obj=target[role]; chunks=obj.get('chunks') or {}
            expected=dict(paperflow_task=task.get('task_id'),paperflow_source_sha256=paper['body']['sha256'],
                paperflow_manifest_sha256=r.get('manifest_sha256'),paperflow_revision=str(task.get('revision')),
                paperflow_specialty=code,paperflow_role=role)
            if obj.get('identity')!=expected or not obj.get('dataset_id') or not obj.get('document_id') or not chunks:
                errors.append('body_document_identity_incomplete')
            if any(c.get('status')!='written' or not c.get('chunk_id') or not HEX.fullmatch(c.get('content_sha256','')) for c in chunks.values()):
                errors.append('body_chunk_receipt_incomplete')
            if checker:
                route=next((row for row in packet['routes'] if row['code']==code),{})
                expected_dataset=route.get('paper_dataset_id' if role=='body' else 'image_dataset_id')
                if not expected_dataset or obj.get('dataset_id')!=expected_dataset:
                    errors.append('body_target_library_mismatch')
                records=checker.records(packet).get(role,[])
                expected_chunks={row[0]:hashlib.sha256(checker.render(task,packet,row).encode()).hexdigest() for row in records}
                if set(chunks)!=set(expected_chunks) or any(chunks[k].get('content_sha256')!=v for k,v in expected_chunks.items()):
                    errors.append('body_chunk_package_mismatch')
    return {'verified':not errors,'receipt':ref,'prepared':prepared_ref,'task':task,
            'manifest_sha256':r.get('manifest_sha256'),'targets':targets,'codes':codes},errors


def _attachments(paper, attachment_refs, body, scope, refs):
    expected={r['file_id']:r for r in paper['attachments']}; seen=set(); result=[]; errors=[]
    for ref in attachment_refs:
        _add(refs,ref); r=_read(ref); b=r.get('binding') or {}; aid=b.get('attachment_id'); source=expected.get(aid)
        if not source or aid in seen: errors.append('attachment_coverage_mismatch'); continue
        seen.add(aid); raw=r.get('raw') or {}; mdsha=r.get('current_md_sha256',''); pubs=(r.get('publications') or {}).get(mdsha,{})
        if r.get('fixture') and scope!='isolated_fixture':errors.append('synthetic_rag_evidence')
        if scope=='isolated_fixture' and r.get('fixture') is not True:errors.append('fixture_marker_missing')
        if (r.get('schema')!='paperflow-attachment-ingest/1' or r.get('status')!='verified' or raw.get('status')!='verified'
                or b.get('paper_id')!=paper['paper_id'] or b.get('source_sha256')!=source['sha256']
                or b.get('body_source_sha256')!=paper['body']['sha256']
                or b.get('raw_identity')!=identity([paper['paper_id'],aid,source['sha256']])):
            errors.append('attachment_identity_or_status_mismatch')
        primary=b.get('primary') or {}
        if (not raw.get('file_id') or not raw.get('document_id') or raw.get('dataset_id')!=primary.get('dataset_id')
                or raw.get('parent_id')!=primary.get('parent_id') or raw.get('source_sha256')!=source['sha256']
                or raw.get('bytes')!=source['bytes']): errors.append('attachment_original_incomplete')
        if not HEX.fullmatch(mdsha) or not pubs:errors.append('attachment_md_missing')
        primary_md=pubs.get(primary.get('code')) or {}
        if not primary.get('dataset_id') or primary_md.get('dataset_id')!=primary['dataset_id']:
            errors.append('attachment_primary_library_mismatch')
        if body['verified'] and (set(pubs)!=set(body['codes']) or primary.get('code')!=body['codes'][0]):
            errors.append('attachment_professional_coverage_mismatch')
        for code,obj in pubs.items():
            chunks=obj.get('chunks') or {}; ids={c.get('id') for c in chunks.values()}; hits=obj.get('retrieval_chunk_ids') or []
            if (obj.get('status')!='verified' or obj.get('md_sha256')!=mdsha or not obj.get('dataset_id') or not obj.get('document_id')
                    or not chunks or obj.get('chunk_count')!=len(chunks) or None in ids or len(ids)!=len(chunks)
                    or any(not HEX.fullmatch(c.get('content_sha256','')) for c in chunks.values())
                    or obj.get('retrieval_verified') is not True or not obj.get('retrieval_verified_at')
                    or not hits or not set(hits).issubset(ids)):
                errors.append('attachment_md_target_not_verified')
        for key in ('saved_md_path',):
            if r.get(key):
                mdref=_ref(r[key]);_require(mdref['sha256']==mdsha,'attachment_md_file_changed');_add(refs,mdref)
            else: errors.append('attachment_md_file_missing')
        association={'status':'verified_paper_source_binding' if body['verified'] else 'body_verification_pending',
                     'paper_id':paper['paper_id'],'body_source_sha256':paper['body']['sha256'],
                     'body_targets':{code:{k:target['body'][k] for k in ('dataset_id','document_id')}
                                     for code,target in body['targets'].items() if target.get('body') and body['verified']}}
        result.append({'attachment_id':aid,'receipt':ref,'original':raw,'md_sha256':mdsha,'targets':pubs,
                       'native_parent_link_status':r.get('parent_link_status'),'association':association})
    if seen!=set(expected):errors.append('attachment_coverage_incomplete')
    return result,errors


def _intermediates(paper, items, protected, verify_files):
    accepted=[]; retained=[]; proofs={}; seen=set(); owner={'paper_id':paper['paper_id'],'revision':paper['revision']}
    protected={Path(p).resolve() for p in protected}
    # A producer record is recovery evidence regardless of its suffix or list order.
    for item in items:
        try:
            proof=item['producer_record']['path']
            if proof:protected.add(Path(proof).resolve())
        except (KeyError,TypeError,OSError,ValueError):pass
    for item in items:
        name=item.get('path','') if isinstance(item,dict) else ''
        try:
            _require(bool(name),'intermediate_identity_unknown'); path=Path(name).resolve(); root=Path(item['root']).resolve()
            _require(path!=root and path.is_relative_to(root) and str(path) not in seen,'intermediate_scope_unknown');seen.add(str(path))
            _require(path not in protected and path.suffix.lower() not in {'.md','.json','.jsonl','.sqlite','.sqlite3','.db','.log'}
                     and not any(word in path.name.lower() for word in PROTECTED_WORDS),'recovery_evidence_retained')
            _require(item.get('kind') in CACHE_KINDS and all(item.get(k)==v for k,v in owner.items()),'intermediate_owner_unknown')
            ref=item['producer_record']; producer=_read(ref); plain={k:v for k,v in item.items() if k!='producer_record'}
            _require(producer.get('schema')=='paperflow-intermediate-ownership/1'
                     and all(producer.get(k)==v for k,v in owner.items()) and producer.get('producer_state')=='complete'
                     and producer.get('finalized') is True and producer.get('reproducible') is True
                     and producer.get('owners')==[owner] and producer.get('references')==[] and producer.get('active_consumers')==[]
                     and plain in producer.get('files',[]),'intermediate_shared_referenced_or_unfinished')
            if verify_files:
                before=path.stat();_require(object_fingerprint(before)==item['object_fingerprint'],'intermediate_object_changed')
                _require(before.st_size==item['bytes'] and sha_file(path)==item['sha256'],'intermediate_content_changed')
                _require(object_fingerprint(path.stat())==item['object_fingerprint'],'intermediate_object_changed')
            accepted.append(copy.deepcopy(item));_add(proofs,ref)
        except (ValueError,OSError,KeyError,TypeError) as exc:
            retained.append({'path':name,'reason':str(exc) if isinstance(exc,ValueError) else 'intermediate_identity_or_access_unknown'})
    return accepted,retained,proofs


def _analyze(paper, intake, inputs, scope, verify_files):
    refs={};_add(refs,inputs['intake']);_add(refs,intake['handoff_ref'])
    for ref in inputs['retained']:_read_bytes_ref(ref);_add(refs,ref)
    body,errors=_body(paper,inputs['body'],inputs['prepared'],scope,refs)
    attachments,extra=_attachments(paper,inputs['attachments'],body,scope,refs);errors+=extra
    if (intake.get('unresolved') or any(r.get('state')!='已接收' for r in intake['items'])
            or (intake.get('delivery_ref') and (intake.get('state')!='已接收'
                or any(r.get('unverified_evidence') for r in intake['items'])))):
        errors.append('intake_incomplete')
    protected=set(refs)|{str(Path(r['path']).resolve()) for r in paper['archive_files']}
    accepted,retained,proofs=_intermediates(paper,inputs['intermediates'],protected,verify_files);refs.update(proofs)
    retained_paths={Path(p).resolve() for p in refs}
    _require(not any(Path(row['path']).resolve() in retained_paths for row in accepted),'cleanup_evidence_overlap')
    evidence=[{'path':p,'sha256':digest} for p,digest in sorted(refs.items())]
    return {'rag':{'body':body,'attachments':attachments},'reasons':sorted(set(errors)),
            'evidence':evidence,'retained_evidence':copy.deepcopy(evidence),
            'intermediates':accepted,'retained_intermediates':retained}


def _read_bytes_ref(ref):
    _require(Path(ref['path']).is_file() and sha_file(ref['path'])==ref['sha256'],'retained_evidence_changed')


def build_nas_handoff(intake_receipt_path, *, body_receipt_path=None, body_prepared_path=None,
                      attachment_receipt_paths=(), intermediates=(), retained_evidence_paths=(),
                      validation_scope='production', native_settings=None):
    _require(validation_scope in {'production','isolated_fixture'},'unknown_validation_scope')
    if native_settings is not None:
        from .legacy import bind
        bind(native_settings)
    intake_ref=_ref(intake_receipt_path);intake,paper=read_receipt(intake_ref['path'])
    _require(paper is not None and paper.get('body') is not None,'accepted_body_identity_missing')
    verify_inventory(paper)
    files=[]
    for row in paper['archive_files']:
        path=Path(row['path']);before=path.stat(); fp=object_fingerprint(before)
        _require(before.st_size==row['bytes'] and sha_file(path)==row['sha256'],'original_changed')
        after=path.stat();_require(fp==object_fingerprint(after) and before.st_mtime_ns==after.st_mtime_ns,'original_changed_during_freeze')
        files.append(dict(copy.deepcopy(row),object_fingerprint=fp))
    inputs={'intake':intake_ref,'body':_ref(body_receipt_path) if body_receipt_path else None,
            'prepared':_ref(body_prepared_path) if body_prepared_path else None,
            'attachments':[_ref(p) for p in attachment_receipt_paths],
            'retained':[_ref(p) for p in retained_evidence_paths],'intermediates':copy.deepcopy(list(intermediates))}
    derived=_analyze(paper,intake,inputs,validation_scope,True)
    out={'schema':SCHEMA,'validation_scope':validation_scope,'paper':copy.deepcopy(paper),'files':files,
         'intake_receipt':dict(intake_ref,receipt_id=intake['receipt_id']),'inputs':inputs,**derived}
    out['status']='not_ready' if out['reasons'] else 'ready'
    out['handoff_id']=identity(out)
    return validate_nas_handoff(out)


def validate_nas_handoff(handoff, verify_files=True):
    _require(isinstance(handoff,dict) and handoff.get('schema')==SCHEMA,'invalid_nas_handoff_schema')
    _require(handoff.get('handoff_id')==identity({k:v for k,v in handoff.items() if k!='handoff_id'}),'nas_handoff_identity_changed')
    scope=handoff.get('validation_scope');_require(scope in {'production','isolated_fixture'},'unknown_validation_scope')
    inputs=handoff['inputs'];_read_bytes_ref(inputs['intake'])
    intake,paper=read_receipt(inputs['intake']['path'])
    _require(paper==handoff['paper'] and handoff['intake_receipt']==dict(inputs['intake'],receipt_id=intake['receipt_id']), 'nas_handoff_intake_mismatch')
    files=handoff['files'];_require(len(files)==len(paper['archive_files']),'nas_original_coverage_mismatch')
    for row,original in zip(files,paper['archive_files']):
        _require({k:v for k,v in row.items() if k!='object_fingerprint'}==original,'nas_original_binding_mismatch')
        _require(set(row.get('object_fingerprint',{}))=={'device','inode','birth_ns'},'nas_original_object_missing')
        if verify_files:_require(object_fingerprint(Path(row['path']).stat())==row['object_fingerprint'],'nas_original_object_changed')
    if verify_files:verify_inventory(paper)
    for ref in handoff['evidence']:_read_bytes_ref(ref)
    derived=_analyze(paper,intake,inputs,scope,verify_files)
    _require(all(handoff.get(k)==v for k,v in derived.items()),'nas_handoff_evidence_or_decision_mismatch')
    _require(handoff['status']==('not_ready' if derived['reasons'] else 'ready'),'nas_handoff_ready_mismatch')
    return copy.deepcopy(handoff)
