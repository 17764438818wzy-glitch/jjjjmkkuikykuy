"""Read native prepared evidence. Discovery never authorizes model or delivery work."""
from __future__ import annotations
import hashlib
import json
from pathlib import Path

from paperflow.prepared_protocol import PreparedIdentity, PreparedObservation, fingerprint
from preextract_material import file_sha

def _read(path):
    return json.loads(Path(path).read_text(encoding='utf8'))

def context_identity(record):
    from purpose_review import project_key
    normalized=dict(record,path=record.get('path') or record.get('source_path'))
    return fingerprint(dict(project=project_key(normalized),
        source_reference=str(Path(normalized.get('input_source_path') or normalized['path']).resolve()).casefold(),
        member=normalized.get('archive_member_path'),archive=normalized.get('archive_sha256'),
        name=normalized.get('archive_member_path') or normalized.get('name') or Path(normalized['path']).name))

def body_permit(record,observation,config):
    """Reuse the existing purpose proof at its exact source/project/member identity."""
    denied=dict(allowed=False,action='review',reason='purpose_evidence_required')
    try:
        record=dict(record,path=record.get('path') or record.get('source_path'))
        if context_identity(record)!=observation.context_sha256:
            return dict(denied,reason='purpose_context_changed')
        if file_sha(record['path'])!=observation.identity.source_sha256:
            return dict(denied,reason='purpose_source_changed')
        if not config.get('enabled'):return denied
        from purpose_review import preclassified_role
        role=preclassified_role(dict(record,_prepared_purpose_context=observation.context_sha256),config)
        scoped=bool(role)
        if not role:role=_legacy_exact_purpose(record,config)
        if (not role or not role.get('confirmed') or not role.get('purpose_review_verified')
                or role.get('evidence_sha256')!=observation.identity.source_sha256):return denied
        path=role.get('purpose_review_receipt')
        if not path or not Path(path).is_file():return denied
        if scoped and _read(path).get('source_context_sha256')!=observation.context_sha256:
            return dict(denied,reason='purpose_evidence_changed')
        action=('allowed' if role.get('kind')=='document' and role.get('action')=='review' else
                role['action'] if role.get('action') in ('associated','delete') else 'review')
        return dict(allowed=action=='allowed',action=action,role=role,receipt_path=path,
                    receipt_sha256=file_sha(path),context_sha256=observation.context_sha256,
                    reason=None if action=='allowed' else 'confirmed_nonbody' if action!='review' else 'purpose_evidence_required')
    except (OSError,ValueError,KeyError,TypeError):return dict(denied,reason='purpose_evidence_changed')


def _legacy_exact_purpose(record,config):
    """Legacy loose-source receipts require their exact path/stat index proof.

    Old archive receipts did not bind the container. Only that ambiguous class
    needs a context-bound purpose review; saved full extraction is unaffected.
    """
    if record.get('input_source_path') or record.get('archive_member_path'):return None
    from purpose_review import _identity,preclassified_role
    key=_identity(record,config)
    index=Path(config['cache_root'])/'index'/(hashlib.sha256(repr(key).encode()).hexdigest()+'.json')
    if not index.is_file():return None
    ref=_read(index);role=preclassified_role(record,config)
    if (role and role.get('purpose_review_receipt')==ref['receipt']
            and file_sha(ref['receipt'])==ref['sha256']):return role
    return None

def _request_hash(request):
    return hashlib.sha256(json.dumps(request,ensure_ascii=False,separators=(',',':')).encode()).hexdigest()

def _event_response(directory):
    path=directory/'response-events.jsonl'
    if not path.is_file() or path.stat().st_size>64*1024**2:return None
    raw=path.read_bytes()
    try:
        if raw.lstrip().startswith(b'{'):
            entry=json.loads(raw)
            if entry.get('error'):return None
            choice=entry['choices'][0]
            return choice['message'].get('content','') if choice.get('finish_reason')=='stop' else None
        chunks=[];finish=None
        for line in raw.splitlines():
            if not line.startswith(b'data:'):continue
            payload=line[5:].strip()
            if not payload or payload==b'[DONE]':continue
            entry=json.loads(payload)
            if entry.get('error'):return None
            for choice in entry.get('choices',[]):
                if choice.get('index',0)!=0:continue
                chunks.append((choice.get('delta') or {}).get('content') or '')
                finish=choice.get('finish_reason') or finish
        return ''.join(chunks) if finish=='stop' else None
    except (KeyError,ValueError,TypeError,AttributeError,OSError):return None

def request_observation(directory):
    """Return a next step; timestamps alone never prove an HTTP result."""
    directory=Path(directory)
    path=directory/'request-state.json'
    if not path.exists():
        if any((directory/name).exists() for name in ('request.json','response.txt','response-events.jsonl','http-error.txt')):
            return 'outcome_unknown','reconcile',dict(reason='unbound_request_evidence')
        return 'no_model_request','llm',{}
    state=_read(path)
    evidence=dict(request_state_path=str(path),request_state_sha256=file_sha(path),
                  request_status=state.get('status'),http_started_at=state.get('http_started_at'),
                  finished_at=state.get('finished_at'),http_status=state.get('http_status'),
                  retry_after=state.get('retry_after',0),
                  error_type=state.get('error_type'),finish_reason=state.get('finish_reason'),usage=state.get('usage') or {},
                  usage_known=bool(state.get('usage')),error_kind=state.get('error_kind'),
                  model_error_code=state.get('model_error_code'),user_stopped=state.get('error_kind')=='user_stop' or state.get('error_type')=='InterruptedError')
    request=directory/'request.json'
    if (state.get('http_invoked') is False and state.get('status')=='request_failed_or_uncertain'
            and isinstance(state.get('started_at'),(int,float)) and isinstance(state.get('finished_at'),(int,float))
            and state['finished_at']>=state['started_at']>0
            and not any((directory/name).exists() for name in ('response.txt','response-events.jsonl'))):
        return 'failed_ended','recover',dict(evidence,request_sha256=state.get('request_sha256'),reason='transport_not_invoked')
    if not request.is_file():
        return 'outcome_unknown','reconcile',dict(evidence,reason='request_payload_missing')
    if _request_hash(_read(request))!=state.get('request_sha256'):
        raise ValueError('request_identity_changed')
    evidence['request_sha256']=state['request_sha256']
    payload=_read(request)
    evidence['output_limit']=payload.get('max_tokens',payload.get('max_completion_tokens'))
    response=directory/'response.txt'
    saved=file_sha(response) if response.is_file() else None
    # A stop delta is not an acknowledgement that the whole stream ended.
    # Later explicit errors, cancellation and socket failure retain that delta
    # in state; none may be adopted as a complete saved answer.
    kind=state.get('error_kind');error_type=state.get('error_type')
    if kind=='user_stop' or error_type=='InterruptedError':
        return 'material_review','review',dict(evidence,reason='user_stopped_response_retained')
    if kind in ('explicit_stream_error','explicit_model_error') or error_type=='ModelExplicitError':
        return 'material_review','review',dict(evidence,reason=kind or 'explicit_model_error')
    if error_type in ('ConnectionResetError','ConnectionAbortedError','ConnectionRefusedError',
                      'ConnectionError','BrokenPipeError','TimeoutError','URLError','HTTPError'):
        # HTTP errors with a complete explicit status retain their native
        # retry classification below, but a partial stream never proves success.
        if saved or not state.get('http_status'):
            return 'outcome_unknown','reconcile',dict(evidence,reason='transport_outcome_unknown')
    event_path=directory/'response-events.jsonl'
    events=_event_response(directory) if event_path.is_file() else None
    events_match=events is not None and response.is_file() and response.read_text(encoding='utf8')==events
    if state.get('status')=='response_complete':
        if not saved or saved!=state.get('response_sha256'):raise ValueError('response_identity_changed')
        if event_path.is_file() and not events_match:
            return 'outcome_unknown','reconcile',dict(evidence,reason='response_events_not_complete')
        return 'response_complete','postprocess',dict(evidence,response_sha256=saved)
    # A failed final replace must not discard an already-complete response.
    if response.is_file():
        complete=(events_match if event_path.is_file() else
                  state.get('finish_reason')=='stop' and saved==state.get('response_sha256'))
        complete=complete and state.get('finish_reason') in (None,'stop')
        if complete:
            adoption=directory/'adopted-response.json'
            if adoption.is_file():
                proof=_read(adoption)
                expected=dict(schema='verified-saved-response/1',request_state_sha256=evidence['request_state_sha256'],
                    request_sha256=evidence['request_sha256'],response_sha256=saved,
                    events_sha256=file_sha(directory/'response-events.jsonl') if (directory/'response-events.jsonl').is_file() else None)
                if proof!=expected:raise ValueError('adoption_identity_changed')
                return 'response_complete','postprocess',dict(evidence,response_sha256=saved,
                    events_sha256=expected['events_sha256'],adoption_sha256=file_sha(adoption))
            return 'response_complete','adopt_saved_response',dict(evidence,response_sha256=saved,
                events_sha256=file_sha(directory/'response-events.jsonl') if (directory/'response-events.jsonl').is_file() else None)
    ended=(isinstance(state.get('started_at'),(float,int)) and isinstance(state.get('finished_at'),(float,int))
           and state['finished_at']>=state['started_at']>0)
    if ended and state.get('status')=='partial_response':
        if state.get('finish_reason')=='length':
            return 'material_review','review',dict(evidence,reason='output_length_requires_changed_plan')
        return 'failed_ended','recover',dict(evidence,reason='incomplete_response')
    if ended and state.get('status')=='request_failed_or_uncertain':
        if state.get('http_invoked') is False:return 'failed_ended','recover',dict(evidence,reason='transport_not_invoked')
        if state.get('http_status') in (408,429,500,502,503,504) or state.get('model_error_status')==429:
            return 'failed_ended','recover',dict(evidence,reason='explicit_transient_response')
        if state.get('http_status'):
            return 'material_review','review',dict(evidence,reason='non_retryable_http')
        # Historical native states may lack typed error fields. Keep explicit
        # stream failures for review as well; never retry the whole saved body.
        events=directory/'response-events.jsonl'
        if events.is_file() and events.stat().st_size<=64*1024**2:
            for line in events.read_bytes().splitlines():
                if line.startswith(b'data:'):
                    try:entry=json.loads(line[5:].strip())
                    except (ValueError,TypeError):continue
                    error=entry.get('error')
                    if isinstance(error,dict) and error.get('code') in ('upstream_stream_error','server_error'):
                        return 'material_review','review',dict(evidence,reason='explicit_stream_error')
    return 'outcome_unknown','reconcile',dict(evidence,reason='external_outcome_unknown')

def _partition(work):
    from preextract_partition import digest
    from preextract_partition_run import _directory,re_safe_name
    model=work/'model';state=_read(model/'partition-state.json');plan=_read(model/'partition-plan.json')
    if (state.get('schema')!='preextract-partition-run/1' or file_sha(model/'partition-plan.json')!=state.get('plan_sha256')
            or digest(_read(work/'material.json'))!=plan.get('source_material_sha256')):
        raise ValueError('partition_identity_changed')
    children=[]
    for row in state.get('requests',[]):
        if not re_safe_name(row['id']):raise ValueError('partition_child_path_invalid')
        directory=_directory(model,state,row['id'])
        phase,step,evidence=request_observation(directory)
        if evidence.get('request_sha256') and evidence['request_sha256']!=row['request_sha256']:
            raise ValueError('partition_request_changed')
        if phase=='no_model_request':
            from preextract_request_control import registered_not_sent
            recovery=next((r for r in reversed(state.get('recoveries',[])) if r['id']==row['id']),None)
            if (not recovery or recovery.get('dispatch_started') is not False) and not registered_not_sent(work,directory,row['request_sha256']):
                phase,step,evidence='outcome_unknown','reconcile',dict(reason='registered_child_missing')
        children.append(dict(id=row['id'],directory=str(directory),phase=phase,next_step=step,**evidence))
    evidence=dict(partition_state_sha256=file_sha(model/'partition-state.json'),children=children)
    blocked=next((c for c in children if c['next_step'] in ('review','reconcile')),None)
    if blocked:return blocked['phase'],blocked['next_step'],dict(evidence,reason=blocked.get('reason','child_reconciliation'))
    if any(c['next_step']=='adopt_saved_response' for c in children):
        return 'parts_pending','adopt_parts',evidence
    if state.get('status')=='assembled':
        path=model/'assembled-candidate.json'
        if (file_sha(path)!=state.get('candidate_sha256') or digest(state.get('requests'))!=state.get('request_index_sha256')
                or not {p['id'] for p in plan['parts']}<={r['id'] for r in state['requests']}):
            raise ValueError('assembled_identity_changed')
        return 'assembled','postprocess',dict(evidence,response_sha256=file_sha(path))
    phase='synthesis_pending' if state.get('status')=='synthesis_pending' else 'parts_pending'
    return phase,'recover_parts' if any(c['next_step']=='recover' for c in children) else 'resume_parts',evidence

def observe_prepared(record,*,data_root,now):
    """No request, deletion, migration, manifest rewrite, or implicit recovery."""
    record=dict(record,path=record.get('path') or record.get('source_path'))
    checkpoint=record.get('prepared_checkpoint') or {}
    identity=PreparedIdentity('','','')
    context=''
    try:
        context=context_identity(record)
        from ai_resume import validate_prepared
        work,bundle=validate_prepared(record,Path(data_root),record.get('version',''))
        if checkpoint.get('format')!='paper-preextract-prepared/1':
            raise ValueError('legacy_material_migration_required')
        from project_paths import TEMPLATE_PATH
        template=Path(record.get('template_path') or TEMPLATE_PATH)
        identity=PreparedIdentity(bundle['source']['source_sha256'],file_sha(work/'material.json'),file_sha(template))
        from preextract_cleanup_scope import PROOF,verify_reuse
        if (work/'candidate-import.json').is_file():
            from preextract_pipeline import _revision,_import_candidate
            revision=_revision(record['path'],work)
            if not revision or revision['mode']!='candidate_import':raise ValueError('candidate_import_revision_missing')
            _,digest,proof=_import_candidate(work,revision)
            phase,step,evidence='response_complete','postprocess',dict(candidate_import_sha256=digest,import_evidence=proof)
        elif (work/PROOF).is_file():
            saved=_read(work/'private/prepared-publication.json')
            verify_reuse(work,source=record['path'],template=template,catalog=saved['catalog'])
            phase,step,evidence='response_complete','postprocess',dict(
                reuse_proof_path=str(work/PROOF),reuse_proof_sha256=file_sha(work/PROOF))
        else:
            phase,step,evidence=(_partition(work) if (work/'model/partition-state.json').exists()
                                 else request_observation(work/'model'))
        evidence=dict(evidence,work_dir=str(work),observed_at=now,source_path=record['path'],
                      material_path=str(work/'material.json'))
        required=tuple(bundle.get('attachments',[]))
        # Sealed publications are evidence only; final acceptance remains the
        # existing strict package/readback predicate plus required associations.
        prepared=work/'private/prepared-publication.json'
        if prepared.is_file():
            evidence['prepared_path']=str(prepared)
            evidence['prepared_sha256']=file_sha(prepared)
            if step=='postprocess':phase,step='publication_ready','delivery'
        elif step=='postprocess':
            from preextract_content_recovery import observation as content_observation
            content=content_observation(work)
            if content:
                phase,step,detail=content;evidence.update(detail)
        return PreparedObservation(identity,dict(checkpoint),phase,step,evidence.get('reason'),evidence,required,context)
    except (OSError,ValueError,KeyError,TypeError) as exc:
        return PreparedObservation(identity,dict(checkpoint),'material_review','review',
            str(exc) if isinstance(exc,ValueError) else type(exc).__name__,
            dict(observed_at=now),(),context)
