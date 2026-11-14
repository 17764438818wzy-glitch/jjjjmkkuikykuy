"""Bounded local corrections over native saved evidence and revision/import APIs."""
import copy
import json
from pathlib import Path

from paperflow.prepared_protocol import fingerprint
from preextract_material import file_sha,write_json
from preextract_contract import resolve_pointer

VERSION='prepared-content-recovery/1'


def _read(path):return json.loads(Path(path).read_text(encoding='utf8'))


def scope_context_gaps(candidate,subset):
    """A part's context-only limitation cannot mark every merged unit unread."""
    blocks={b['id']:b for b in subset.get('blocks',[])};changed=0
    for gap in candidate.get('gaps',[]):
        refs=gap.get('source_refs') or []
        if (gap.get('type')=='not_read' and gap.get('target_pointer')=='/profile/learning_units'
                and refs and all(r.get('block_id') in blocks and
                    blocks[r['block_id']].get('reading_role')=='context' for r in refs)):
            gap['target_pointer']='/source/reading_scope';changed+=1
    return changed


def compact_budget_limits(candidate,message):
    """Keep the original restriction verbatim in the same gap's description."""
    if message!='token budget cannot fit required context':return 0
    count=0
    for gap in candidate.get('gaps',[]):
        limit=gap.get('use_limit')
        if (str(gap.get('id','')).startswith('compat_') and gap.get('target_pointer')=='/source/reading_scope'
                and isinstance(limit,str) and len(limit)>500):
            gap['description']=(gap.get('description') or '')+'\n[原使用限制，逐字保留]\n'+limit
            gap['use_limit']='按本项 description 中完整保留的原使用限制核对后使用。';count+=1
    return count


def repair_targets(candidate,errors,bundle):
    """Only missing textual fields with existing, supplied block evidence."""
    blocks={b['id']:b for b in bundle.get('blocks',[])};targets=[];paths=set()
    def add(path,owner):
        if path in paths:return
        try:value=resolve_pointer(candidate,path)
        except (ValueError,KeyError,IndexError,TypeError):return
        refs=[r for r in owner.get('source_refs',[]) if r.get('block_id') in blocks]
        if value not in ('',None) or not refs:return
        targets.append(dict(path=path,old=value,source_refs=copy.deepcopy(refs)));paths.add(path)
    if any(e.get('code')=='learning_unusable' for e in errors):
        for i,unit in enumerate(candidate.get('profile',{}).get('learning_units',[])):
            base=f'/profile/learning_units/{i}'
            add(base+'/question',unit)
            for key,field in (('materials','description'),('steps','operation'),('outputs','content')):
                for j,row in enumerate(unit.get(key,[])):
                    add(f'{base}/{key}/{j}/{field}',row if row.get('source_refs') else unit)
    for error in errors:
        path=error.get('path','')
        if error.get('code') not in ('required_content','field_type'):continue
        # These semantic leaves cannot change identities, declared limits, routes or evidence.
        if path.startswith('/profile/learning_units/') and path.rsplit('/',1)[-1] in ('question','description','operation','content'):
            try:owner=resolve_pointer(candidate,path.rsplit('/',1)[0])
            except (ValueError,KeyError,TypeError,IndexError):continue
            if isinstance(owner,dict):add(path,owner)
    return targets[:16]


def apply_field_repairs(candidate,targets,reply):
    allowed={t['path']:t for t in targets};result=copy.deepcopy(candidate)
    if not isinstance(reply,dict) or set(reply)!={'repairs'} or not isinstance(reply['repairs'],list):
        raise ValueError('field repairs object required')
    seen=set()
    for patch in reply['repairs']:
        if not isinstance(patch,dict) or set(patch)!={'path','old','value'}:raise ValueError('invalid repair fields')
        path=patch['path']
        if path not in allowed or path in seen:raise ValueError('repair path is not allowed')
        seen.add(path);target=allowed[path]
        if patch['old']!=target['old'] or resolve_pointer(result,path)!=target['old']:raise ValueError('repair old value changed')
        if not isinstance(patch['value'],str) or not patch['value'].strip() or patch['value']==patch['old']:
            raise ValueError('repair must supply a nonempty textual value')
        parent,key=path.rsplit('/',1);resolve_pointer(result,parent)[key.replace('~1','/').replace('~0','~')]=patch['value']
    if not seen:raise ValueError('no field was repaired')
    return result


def failure(work):
    work=Path(work);status=_read(work/'status.json') if (work/'status.json').exists() else {}
    stage=status.get('stage')
    if stage=='candidate_requires_attention':
        return dict(stage=stage,errors=status.get('errors',[]))
    if stage=='stopped' and status.get('failed_stage') in ('candidate_received','validating_candidate','redacting_and_packaging'):
        error=_read(work/'private/error.json')
        if error.get('error_type') in ('InterruptedError','ModelSaved'):return None
        if error.get('winerror') in (32,33):return None
        return error
    return None


def observation(work):
    """Read only; an attempted unchanged correction cannot spin the scheduler."""
    work=Path(work);error=failure(work)
    if not error:return None
    key=fingerprint(error);path=work/'content-recovery.json'
    if (work/'content-recovery-rejected.json').exists():
        return ('material_review','review',dict(reason='content_correction_rejected'))
    if not path.exists():return ('content_failed','repair_local',dict(content_failure=key))
    report=_read(path)
    if report.get('schema')!=VERSION or report.get('failure')!=key:
        return ('material_review','review',dict(reason='content_recovery_evidence_changed'))
    if report.get('next_step')=='local_ready':
        return ('content_failed','repair_local',dict(content_failure=key))
    if report.get('next_step')=='repair_model':
        directory=work/'model'/report['request_directory']
        from preextract_scheduler_adapter import request_observation
        phase,step,proof=request_observation(directory)
        if step=='reconcile':return ('outcome_unknown','reconcile',dict(proof,reason='correction_request_outcome_unknown'))
        if step=='recover':return ('material_review','review',dict(proof,reason='correction_request_failed'))
        if step=='review':return (phase,step,proof)
        return ('content_failed','repair_model',dict(content_failure=key,repair_report_sha256=file_sha(path)))
    return ('material_review','review',dict(reason=report.get('reason','content_correction_requires_review')))


def _ancestors(source,work):
    from preextract_pipeline import _revision
    seen=set()
    while work not in seen:
        seen.add(work);yield work
        revision=_revision(source,work)
        if not revision:return
        work=Path(revision['parent']).resolve()
    raise ValueError('content recovery parent cycle')


def _load_candidate(source,work,template):
    from preextract_recovery_evidence import candidate_evidence
    from preextract_contract import parse_saved_candidate
    proof=candidate_evidence(source,work,template_path=template)
    path=Path(proof['candidate_path']);raw=_read(path)['candidate'] if proof['kind']=='candidate_import' else parse_saved_candidate(path.read_text(encoding='utf8'))
    return raw,proof


def _scope_saved_parts(source,work,candidate):
    from preextract_partition_run import _directory
    for parent in _ancestors(source,work):
        if not (parent/'model/partition-state.json').exists():continue
        model=parent/'model';state=_read(model/'partition-state.json');plan=_read(model/'partition-plan.json');changes=0
        for number,part in enumerate(plan['parts'],1):
            from preextract_contract import parse_saved_candidate
            raw=parse_saved_candidate((_directory(model,state,part['id'])/'response.txt').read_text(encoding='utf8'))
            subset=_read(model/part['id']/'material.json')
            for index,gap in enumerate(raw.get('gaps',[]),1):
                fixed=copy.deepcopy(gap)
                if not scope_context_gaps(dict(gaps=[fixed]),subset):continue
                target=next((g for g in candidate.get('gaps',[]) if g.get('id')==f'P{number:03d}_gaps_{index}'),None)
                if (target and target.get('target_pointer')=='/profile/learning_units'
                        and all(target.get(k)==gap.get(k) for k in ('type','description','impact','use_limit'))):
                    target['target_pointer']='/source/reading_scope';changes+=1
        return changes
    return 0


def _select_revision(record,work,candidate,report,control,template):
    from preextract_pipeline import create_revision,save_candidate_import
    from preextract_scope_guard import require_managed
    require_managed(work,control.marker)
    # Never make a second upload identity for an already registered version.
    from preextract_delivery_queue import publication_registered
    for p in _ancestors(record['path'],work):
        if any((p/name).exists() for name in ('delivery-binding.json','delivery-receipt.json')):
            raise ValueError('delivered version requires reconciliation before correction')
        prepared=p/'private/prepared-publication.json'
        if prepared.exists() and publication_registered(p,_read(prepared),roots=(control.registry.store.path.parent,)):
            raise ValueError('registered version requires reconciliation before correction')
    name='content-'+fingerprint(dict(candidate=candidate,report=report))[:24]
    child=work/'revisions'/name
    if not child.exists():
        child=create_revision(record['path'],work,name,mode='candidate_import',reason='bounded content correction',
            template_path=template,managed_scope=control.marker)
    require_managed(child,control.marker)
    if not (child/'candidate-import.json').exists():
        evidence=[work/'content-recovery.json']
        if report.get('request_directory'):
            directory=work/'model'/report['request_directory']
            evidence.extend(p for p in directory.iterdir() if p.is_file() and p.suffix in ('.json','.txt','.jsonl'))
        save_candidate_import(child,candidate,evidence=evidence)
    else:
        from preextract_pipeline import _revision,_import_candidate
        text,_,_=_import_candidate(child,_revision(record['path'],child))
        if json.loads(text)!=candidate:raise ValueError('content revision collision')
    control.registry.assert_running(control.scope_id,control.generation)
    checkpoint=dict(format='paper-preextract-prepared/1',path=str(child/'material.json'),sha256=file_sha(child/'material.json'))
    control.registry.store.update(control.task_id,prepared_checkpoint=checkpoint)
    return dict(stage='content_corrected',work_dir=str(child))


def run_repair(record,work,*,processor,control,model=False):
    from preextract_scope_guard import require_managed
    from filelock import FileLock
    work=Path(work)
    with FileLock(str(work/'operation.lock'),timeout=0):
        require_managed(work,control.marker)
        try:result=_prepare_repair(record,work,processor=processor,control=control,model=model)
        except (ValueError,KeyError,TypeError):
            error=failure(work)
            if not model and error and not (work/'content-recovery.json').exists():
                write_json(work/'content-recovery.json',dict(schema=VERSION,failure=fingerprint(error),
                    next_step='review',reason='saved_candidate_requires_review'))
            raise
    # create_revision owns the same native operation lock; do not nest a new
    # lock instance. It revalidates immutable parent proofs after this boundary.
    if '_selection' in result:
        candidate,report,template=result['_selection']
        return _select_revision(record,work,candidate,report,control,template)
    return result


def _prepare_repair(record,work,*,processor,control,model=False):
    from preextract_pipeline import DEFAULT_TEMPLATE,DEFAULT_RUNTIME,load_runtime
    from preextract_contract import validate_candidate
    work=Path(work);control.registry.assert_running(control.scope_id,control.generation)
    template=Path(record.get('template_path') or processor.config.get('preextract_template') or DEFAULT_TEMPLATE)
    runtime_dir=Path(processor.config.get('preextract_runtime_dir') or DEFAULT_RUNTIME);runtime=load_runtime(runtime_dir)
    catalog=_read(Path(runtime.get('catalog_path') or runtime_dir/'catalog.json'))
    original,proof=_load_candidate(record['path'],work,template)
    from preextract_recovery_evidence import inherited_bundle
    from preextract_image_alias import AliasContext
    alias_context=AliasContext()
    bundle=inherited_bundle(record['path'],work,_read(work/'material.json'),alias_context=alias_context,
                            template_path=template)
    checked=validate_candidate(original,bundle,catalog,redaction_policy='best_effort',admission_policy='learning',
                               alias_context=alias_context)
    candidate=checked['candidate'];error=failure(work)
    if not error:raise ValueError('content failure is no longer current')
    report_path=work/'content-recovery.json'
    if not model:
        changed=_scope_saved_parts(record['path'],work,candidate)
        changed+=compact_budget_limits(candidate,error.get('message'))
        if report_path.exists():
            saved=_read(report_path)
            if (saved.get('next_step')=='local_ready' and saved.get('failure')==fingerprint(error)
                    and saved.get('result_sha256')==fingerprint(candidate)):
                return dict(_selection=(candidate,saved,template))
            raise ValueError('local correction already attempted for this evidence')
        targets=repair_targets(candidate,checked['errors'],bundle)
        prior=[]
        for parent in _ancestors(record['path'],work):
            prior.extend(p for p in (parent/'model').glob('content-repair-*/request-state.json') if _read(p).get('http_invoked') is not False)
        report=dict(schema=VERSION,failure=fingerprint(error),parent_candidate_sha256=fingerprint(original),
            targets=targets,changes=changed,model_attempts=len(prior),next_step='review',reason='no_proven_local_or_field_repair')
        if changed:
            report.update(reason='local_correction_created',next_step='local_ready',result_sha256=fingerprint(candidate))
        elif targets and len(prior)<2:
            report.update(next_step='repair_model',request_directory=f'content-repair-{len(prior)+1:02d}',reason='missing_fields_with_source_evidence')
        elif len(prior)>=2:report['reason']='content_correction_budget_exhausted'
        write_json(report_path,report)
        if changed:return dict(_selection=(candidate,report,template))
        return dict(stage='content_classified',next_step=report['next_step'],reason=report['reason'])
    report=_read(report_path)
    if (report.get('schema')!=VERSION or report.get('next_step')!='repair_model' or report.get('model_attempts',2)>=2
            or report['failure']!=fingerprint(error) or report['parent_candidate_sha256']!=fingerprint(original)
            or report['targets']!=repair_targets(candidate,checked['errors'],bundle)):
        raise ValueError('content repair authorization changed')
    from preextract_workbench import model_connection,invoke_once
    from preextract_request import build_request
    from preextract_contract import parse_candidate
    connection=model_connection(processor)
    block_ids={r['block_id'] for t in report['targets'] for r in t['source_refs']}
    supplied=dict(source=bundle['source'],blocks=[b for b in bundle.get('blocks',[]) if b['id'] in block_ids],
        sections=[],assets=[],issues=[],attachments=[],reading_context=dict(repair_targets=report['targets'],errors=checked['errors']))
    rules='只补正 repair_targets 中的缺失文字字段，依据所附原文，不能编造或扩大结论。不能补则返回空 repairs。只返回 JSON {"repairs":[{"path":"给定路径","old":原值,"value":"补正文字"}]}；不得改其他字段、来源、未读或使用限制。'
    body,_=build_request(supplied,rules,{},connection)
    directory=work/'model'/report['request_directory']
    from preextract_scheduler_adapter import request_observation
    from preextract_request_control import adopt_saved_response,verified_saved_state
    phase,step,_=request_observation(directory)
    if step=='adopt_saved_response':adopt_saved_response(directory)
    if phase=='response_complete':
        verified_saved_state(directory)
        # Saved correction must be the exact frozen request, not a regenerated variant.
        from preextract_partition_run import _request_hash
        if _request_hash(_read(directory/'request.json'))!=_request_hash(body):raise ValueError('correction request changed')
        response=(directory/'response.txt').read_text(encoding='utf8')
    elif step!='llm':raise ValueError('correction request requires reconciliation')
    else:response=invoke_once(processor,body,connection,directory,stop=processor.stop,ledger_task='paper_preextract_correction')
    try:corrected=apply_field_repairs(candidate,report['targets'],parse_candidate(response))
    except (ValueError,TypeError,KeyError) as exc:
        # Keep a deterministic rejected result; do not send the same prompt again.
        write_json(work/'content-recovery-rejected.json',dict(reason=type(exc).__name__,response_sha256=file_sha(directory/'response.txt')))
        raise
    return dict(_selection=(corrected,report,template))
