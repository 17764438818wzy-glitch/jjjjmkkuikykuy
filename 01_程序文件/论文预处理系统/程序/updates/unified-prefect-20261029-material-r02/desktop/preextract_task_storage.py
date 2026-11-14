"""Task-command storage parameters and producer-closed target plans.

No model, transport, task start, policy toggle or database creation at import.
Only an explicit storage member of an existing applied task command is a permit.
"""
import copy
import hashlib
import json
import os
import time
from pathlib import Path
from contextlib import ExitStack


class Held(ValueError):pass
class Failed(Held):pass
class Disabled(Held):pass


def digest(value):
    return hashlib.sha256(json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode('utf8')).hexdigest()


def producer_projection(manifest):
    fields=('id','source_hash','source_path','input_source_path','content_format','status',
            'automatic_approval','preextract_work_dir','package_path','reason_kind','source_education','archive_member_path','archive_source_hash')
    return dict(run_id=manifest['run_id'],status=manifest.get('status'),inputs=manifest.get('inputs'),
        discovered_inputs=manifest.get('discovered_inputs',[]),child_runs=manifest.get('child_runs',[]),
        items=[{k:i.get(k) for k in fields} for i in manifest.get('items',[])],
        events=manifest.get('events',[]))


def mark_production(manifest,source_eof):
    """Called only on the original producer's normal, exhausted return path."""
    if source_eof and manifest.get('status')=='completed':
        manifest['storage_production']=dict(schema='native-production-closed/1',source_eof=True,
            projection_sha256=digest(producer_projection(manifest)))


def _db(root,reserve=False):
    from preextract_nas_scope import _database
    return _database(Path(root)/'private/workbench/intake.sqlite3',reserve=reserve)


def _global_generation(root,storage,command):
    from preextract_nas_scope import _database
    path=Path(root)/'private/global-task-control.sqlite3'
    with _database(path) as db:
        current=db.execute('SELECT generation,intent FROM control WHERE id=1').fetchone()
        expected=storage['global_generation']
        # Only the same initial Start's original G0 -> G1 receipt is adopted.
        # No later Continue/current generation is allowed to renew this permit.
        if expected==0 and not storage['global_was_open'] and command['action']=='start':
            saved=db.execute('SELECT operation,generation FROM commands WHERE request_id=?',
                ('first-task-start-'+command['request_id'],)).fetchone()
            if saved is None or saved['operation']!='resume' or saved['generation']!=1:
                raise Held('storage_initial_start_global_receipt_missing')
            expected=saved['generation']
        if current is None or current['intent']!='running' or current['generation']!=expected:
            raise Held('storage_original_global_activation_changed')
        return expected


def request(root,batch_id,*,connection=None):
    from preextract_nas_scope import _web
    if connection is None:
        with _db(root) as db:return request(root,batch_id,connection=db)
    meta,inputs=_web(connection,batch_id)
    if meta.get('storage_version')!=1:raise Disabled('storage_existing_task_preserved')
    rows=connection.execute('SELECT * FROM task_commands WHERE batch_id=? AND result_revision=?',
                            (batch_id,meta['revision'])).fetchall()
    if len(rows)!=1 or rows[0]['status']!='applied':raise Held('storage_command_not_applied')
    command=dict(rows[0]);body=json.loads(command['body']);storage=body.get('storage') or {}
    if storage.get('allow_release') is not True:raise Disabled('storage_explicit_release_not_allowed')
    if (storage.get('schema')!='task-storage-start/1' or storage['root']!=str(Path(root).resolve())
            or storage['batch_id']!=batch_id or storage['workspace_id']!=meta['workspace_id']
            or storage['web_revision']!=meta['revision'] or storage['frozen_manifest_sha256']!=meta['manifest_sha256']
            or digest(storage['request'])!=storage['request_sha256'] or storage['request']['allow_release'] is not True):
        raise Held('storage_release_request_identity_changed')
    activation=storage.get('activation') or {}
    native=body['native']['result'].get('command_revision')
    delivery=body['delivery']['result'].get('result',{}).get('generation')
    if (type(native) is not int or type(delivery) is not int
            or activation!={'native_revision':native,'delivery_generation':delivery}
            or native!=meta['native_revision'] or delivery!=meta['delivery_generation']):
        raise Held('storage_original_command_activation_changed')
    selected={i['id']:i for i in storage['selected_inputs']};actual={i['id']:i for i in inputs}
    if (set(selected)!=set(storage['request']['input_ids']) or not selected
            or any(i not in actual or selected[i]!={k:actual[i][k] for k in selected[i]} for i in selected)):
        raise Held('storage_permitted_frozen_inputs_changed')
    originals=storage['request']['originals']
    if (len(originals)!=len(selected) or {r['input_id'] for r in originals}!=set(selected)
            or any(str(Path(selected[r['input_id']]['path']).resolve())!=r['path']
                   or selected[r['input_id']]['sha256']!=r['sha256'] for r in originals)):
        raise Held('storage_permitted_original_range_changed')
    generation=_global_generation(root,storage,command)
    return storage,command,meta,inputs,generation


def _context(storage,command,generation):
    plan=storage.get('plan') or {}
    if plan.get('state')!='sealed' or digest(plan)!=storage.get('plan_sha256'):
        if plan.get('state')=='failed':raise Failed(plan.get('reason','storage_production_failed'))
        raise Held('storage_production_plan_not_sealed')
    return dict(request_id=command['request_id'],request_sha256=storage['request_sha256'],
        web_revision=storage['web_revision'],global_generation=generation,
        native_revision=storage['activation']['native_revision'],
        delivery_generation=storage['activation']['delivery_generation'],
        plan_sha256=storage['plan_sha256'],bindings_sha256=plan['bindings_sha256'],
        producer_owner_token=plan['producer_owner_token'])


def validate_context(db,root,batch_id,expected,*,input_id=None,path=None,sha256=None):
    """Short SQL metadata check, used under the original final WEB/G/Q locks."""
    row=db.execute('''SELECT c.request_id,c.action,c.status,c.result_revision,t.storage_version,
        json_extract(c.body,'$.storage') AS storage FROM task_commands c
        JOIN task_controls t ON t.batch_id=c.batch_id AND t.revision=c.result_revision
        WHERE c.batch_id=?''',(batch_id,)).fetchall()
    if len(row)!=1:raise Held('storage_release_command_missing_or_ambiguous')
    command=dict(row[0]);storage=json.loads(command['storage'] or '{}')
    if storage.get('allow_release') is not True or command['status']!='applied' or command['storage_version']!=1:
        raise Held('storage_explicit_release_not_allowed')
    if storage.get('recovery_state') in {'restore_requested','restored'}:
        raise Held('storage_local_isolation_restore_requested')
    # The persisted request/plan are written only by the matching command and
    # producer CAS. Compare their fixed digests, never reselect or renew them.
    plan=storage.get('plan') or {}
    current=dict(request_id=command['request_id'],request_sha256=storage.get('request_sha256'),
        web_revision=command['result_revision'],global_generation=_global_generation(root,storage,command),
        native_revision=(storage.get('activation') or {}).get('native_revision'),
        delivery_generation=(storage.get('activation') or {}).get('delivery_generation'),
        plan_sha256=storage.get('plan_sha256'),bindings_sha256=plan.get('bindings_sha256'),
        producer_owner_token=plan.get('producer_owner_token'))
    if current!=expected or plan.get('state')!='sealed':raise Held('storage_release_activation_or_plan_changed')
    if input_id is not None and input_id not in storage['request']['input_ids']:
        raise Held('storage_item_not_in_permitted_inputs')
    if path is not None:
        canonical=str(Path(path).resolve())
        if not any(r['path']==canonical and r['sha256']==sha256 for r in storage['request']['originals']):
            raise Held('storage_original_not_explicitly_permitted')


def _write_plan(root,batch_id,command,storage,plan):
    with _db(root,reserve=True) as db:
        row=db.execute('''SELECT c.body FROM task_commands c JOIN task_controls t
            ON t.batch_id=c.batch_id AND t.revision=c.result_revision
            WHERE c.batch_id=? AND c.request_id=? AND c.status='applied' ''',
            (batch_id,command['request_id'])).fetchone()
        if row is None:raise Held('storage_command_changed_before_seal')
        body=json.loads(row['body']);actual=body.get('storage') or {}
        if actual.get('request_sha256')!=storage['request_sha256']:raise Held('storage_request_changed_before_seal')
        if actual.get('plan'):
            if actual['plan']!=plan:raise Held('storage_existing_production_seal_changed')
            return
        actual.update(plan=plan,plan_sha256=digest(plan));body['storage']=actual
        db.execute('UPDATE task_commands SET body=? WHERE batch_id=? AND request_id=? AND status=\'applied\'',
            (json.dumps(body,ensure_ascii=False),batch_id,command['request_id']))
        db.commit()


def _item_plan(store,run_id,item):
    from preextract_delivery_queue import package_reference
    from preextract_publish import _paperflow
    from associated_delivery import load_plan
    from paperflow.rag import RagPublisher
    work=Path(item['preextract_work_dir']).resolve()
    prepared=json.loads((work/'private/prepared-publication.json').read_text(encoding='utf8'))
    packet=_paperflow(prepared['paperflow_root']).validate_package(item['package_path'],prepared['task'],prepared['catalog'],strict_seal=True)
    prepared['packet']=packet
    associated=load_plan(prepared,work)
    material=json.loads((work/'material.json').read_text(encoding='utf8'))
    from associated_delivery import KINDS
    expected_attachments={}
    def attachments(rows,prefix='attachment_',inherited=False):
        for number,row in enumerate(rows,1):
            reference=prefix+str(number)
            candidate=inherited or row.get('association_status')=='candidate'
            if row.get('purpose_pending'):raise Held('storage_attachment_purpose_not_final')
            if row.get('kind')=='archive':
                if not row.get('members'):raise Held('storage_attachment_container_members_unconfirmed')
                attachments(row['members'],reference+'/',candidate);continue
            review=row.get('purpose_review') or {}
            excluded=(review.get('purpose_review_verified') is True and review.get('confirmed') is True
                      and review.get('evidence_sha256')==row.get('sha256') and review.get('action') in {'exclude','delete'})
            if candidate and row.get('kind') in KINDS and not excluded:
                expected_attachments[reference]=row['sha256']
            elif row.get('association_status') in {'pending','unknown'}:
                raise Held('storage_attachment_association_not_final')
    attachments(material.get('attachments',[]))
    attachment_files=(associated or {}).get('files',[])
    actual_attachments={i['file_ref']:i['sha256'] for i in attachment_files}
    if len(actual_attachments)!=len(attachment_files):raise Held('storage_attachment_target_identity_ambiguous')
    if expected_attachments!=actual_attachments:raise Held('storage_attachment_expected_target_set_incomplete')
    if associated is None and not (material.get('version')=='preextract-material/1.0'
            and material.get('source',{}).get('source_sha256')==prepared['task']['source_sha256']
            and material.get('attachments')==[]):
        raise Held('storage_attachment_target_plan_not_closed')
    if not packet['routes'] or packet.get('pending_routes'):raise Held('storage_specialty_targets_not_fully_resolved')
    if len({r['code'] for r in packet['routes']})!=len(packet['routes']):raise Held('storage_specialty_targets_ambiguous')
    records=RagPublisher(None).records(packet)
    if any(len({record[0] for record in values})!=len(values) for values in records.values()):
        raise Held('storage_required_record_identity_ambiguous')
    targets=[dict(code=r['code'],role=role,dataset_id=r['paper_dataset_id' if role=='body' else 'image_dataset_id'],
                  record_ids=[record[0] for record in values]) for r in packet['routes'] for role,values in records.items() if values]
    if not targets or not any(t['role']=='body' for t in targets):raise Held('storage_body_target_plan_missing')
    payload=package_reference(store,run_id,item['id'],store.storage_service,item=item)
    return dict(delivery_id=payload['id'],payload=payload,targets=targets,
        attachment_plan=associated,attachment_plan_sha256=prepared.get('associated_upload_plan_sha256'))


def _source_key(record,binding,inputs):
    source_path=Path(record.get('source_path') or record.get('path') or '').resolve()
    sha=record.get('source_hash') or record.get('sha256')
    proofs=[p for p in binding.get('source_evidence',[]) if Path(p['source_path']).resolve()==source_path
            and (not sha or p['source_sha256']==sha)]
    if len(proofs)>1:raise Held('storage_leaf_path_proof_ambiguous')
    proof=proofs[0] if proofs else {}
    original=Path(proof.get('input_source_path') or record.get('input_source_path') or source_path).resolve()
    owners=[i for i in inputs if Path(i['path']).resolve()==original
            and (not proof or proof['input_source_sha256']==i['sha256'])]
    if len(owners)!=1:raise Held('storage_leaf_original_path_not_bound')
    owner=owners[0];sha=sha or proof.get('source_sha256')
    if not sha and source_path==original:sha=owner['sha256']
    if not sha:raise Held('storage_leaf_hash_not_bound')
    if not proof and (source_path!=original or sha!=owner['sha256']):
        raise Held('storage_derived_leaf_requires_original_proof')
    member=proof.get('archive_member_path') or record.get('archive_member_path') or ''
    return (owner['id'],str(source_path),sha,proof.get('relation','direct'),member)


def seal(native,batch,processor,produced_run_id):
    """Seal current input coverage, retaining every real retry-run audit row."""
    root=native.root;batch_id=batch['batch_id'];storage=command=None
    try:
        with _db(root) as db:
            enabled=db.execute('''SELECT 1 FROM task_commands c JOIN task_controls t
                ON t.batch_id=c.batch_id AND t.revision=c.result_revision WHERE c.batch_id=?
                AND c.status='applied' AND t.storage_version=1
                AND json_extract(c.body,'$.storage.allow_release')=1''',(batch_id,)).fetchone()
        if enabled is None:return
        storage,command,meta,inputs,generation=request(root,batch_id)
        if storage.get('plan'):return
        from native_batch_control import NativeBatchControl
        state=NativeBatchControl(root).assert_running(batch_id)
        opened=getattr(processor,'_native_task_execution',None) or {};owner=state.get('execution_owner') or {}
        if (not opened or owner.get('token')!=opened.get('owner_token') or owner.get('finished_at')
                or state['control_revision']!=storage['activation']['native_revision']):
            raise Held('storage_original_producer_owner_missing')
        bindings=state.get('run_bindings') or []
        if not bindings:raise Held('storage_producer_run_bindings_missing')
        roots={};audits=[];all_runs={};selected=set(storage['request']['input_ids'])
        for binding in bindings:
            manifest=native._task_bound_record(binding,{i['sha256'] for i in inputs},batch_id=batch_id)
            if manifest is None:raise Held('storage_unpublished_producer_run')
            all_runs[binding['run_id']]=(binding,manifest)
            audits.append(dict(path=str(native._task_manifest_path(binding)),projection_sha256=digest(producer_projection(manifest))))
            if Path(binding['store_root']).resolve()==Path(root).resolve():roots[binding['run_id']]=(binding,manifest)
        chain=[];current=produced_run_id
        while current:
            if current in chain or current not in roots:raise Held('storage_actual_retry_chain_missing_or_cyclic')
            chain.append(current);binding,manifest=roots[current]
            parent=binding.get('parent_run_id')
            if manifest.get('retry_of')!=parent:raise Held('storage_retry_manifest_parent_changed')
            current=parent
        if set(chain)!=set(roots):raise Held('storage_multiple_unclosed_root_branches')
        latest=roots[produced_run_id][1];closed=latest.get('storage_production') or {}
        if (latest.get('status')!='completed' or closed.get('source_eof') is not True
                or closed.get('projection_sha256')!=digest(producer_projection(latest))):
            raise Held('storage_current_producer_source_iteration_not_closed')
        for run_id,(binding,manifest) in all_runs.items():
            if run_id in roots:continue
            parent=binding.get('parent_run_id')
            if parent not in roots:raise Held('storage_private_child_actual_parent_missing')
            links=[c for c in roots[parent][1].get('child_runs',[]) if c.get('run_id')==run_id
                   and Path(c['manifest_path']).resolve()==native._task_manifest_path(binding)]
            if len(links)!=1:raise Held('storage_private_child_declared_link_missing')
        # Effective outcomes follow only the actual root retry chain. A passed A
        # in stopped R1 can coexist with B completed in its explicit successor.
        chosen={};container_leaves={};container_types={'.zip','.rar','.7z','.tar','.gz','.bz2'}
        for run_id in reversed(chain):
            binding,manifest=roots[run_id]
            mark=manifest.get('storage_production') or {}
            complete=(manifest.get('status')=='completed' and mark.get('source_eof') is True
                      and mark.get('projection_sha256')==digest(producer_projection(manifest)))
            if complete:
                for original in inputs:
                    path=Path(original['path']).resolve()
                    if path.suffix.lower() not in container_types:continue
                    if not any(Path(r['path']).resolve()==path for r in manifest.get('inputs',[])):continue
                    leaves=[r for r in manifest.get('discovered_inputs',[]) if
                            Path(r.get('input_source_path') or '').resolve()==path]
                    if not leaves:raise Held('storage_container_complete_member_inventory_missing')
                    keys={_source_key(r,binding,inputs) for r in leaves}
                    if len(keys)!=len(leaves):raise Held('storage_container_leaf_inventory_ambiguous')
                    container_leaves[original['id']]=keys
            for item in manifest.get('items',[]):
                if item.get('content_format')=='associated_material' or item.get('status')=='associated':
                    raise Held('storage_standalone_attachment_target_generation_not_closed')
                key=_source_key(item,binding,inputs)
                chosen[key]=(binding,manifest,item)
        expected_keys=set()
        for original in storage['selected_inputs']:
            key_id=original['id'];candidates={k for k in chosen if k[0]==key_id}
            if Path(original['path']).suffix.lower() in container_types:
                leaves=container_leaves.get(key_id)
                if not leaves:raise Held('storage_container_production_not_fully_enumerated')
                # A root-container interrupted outcome is superseded only by
                # this explicit full enumeration; unknown member outcomes stay.
                unknown={k for k in candidates-leaves if Path(k[1])!=Path(original['path']).resolve()}
                if unknown:raise Held('storage_container_retry_leaf_scope_changed')
                if not leaves<=set(chosen):raise Held('storage_container_expected_leaf_outcome_missing')
                expected_keys.update(leaves)
            else:
                if not candidates:raise Held('storage_selected_input_production_not_fully_accounted')
                expected_keys.update(candidates)
        settings=json.loads((native.settings_root/'settings.json').read_text(encoding='utf8'))
        store=type('PublicationStore',(),{'root':Path(root),'runs':Path(root)/'runs','storage_service':settings['ragflow_url']})()
        items=[]
        for key in sorted(expected_keys):
            binding,manifest,item=chosen[key]
            if item.get('status')!='pass' or item.get('content_format')!='paper_preextract' or item.get('automatic_approval') is not True:
                raise Failed('storage_selected_leaf_terminal_failure_or_pending')
            planned=_item_plan(store,binding['run_id'],item);planned['input_id']=key[0]
            planned['leaf_key']=list(key);items.append(planned)
        if not items or {i['input_id'] for i in items}!=selected:raise Held('storage_original_input_coverage_incomplete')
        if len({i['delivery_id'] for i in items})!=len(items):raise Held('storage_expected_delivery_identity_ambiguous')
        plan=dict(schema='task-storage-target-plan/1',state='sealed',source_generation_complete=True,
            global_generation=generation,producer_owner_token=opened['owner_token'],bindings_sha256=digest(bindings),
            manifests=audits,items=items,selected_input_ids=sorted(selected),retry_root_chain=chain)
        _write_plan(root,batch_id,command,storage,plan)
    except Disabled:return
    except Exception as exc:
        if storage is not None and command is not None:
            try:_write_plan(root,batch_id,command,storage,dict(schema='task-storage-target-plan/1',state='failed',
                reason=str(exc)[:180] if isinstance(exc,Held) else 'storage_production_seal_unconfirmed',error_type=type(exc).__name__))
            except Exception:pass


def _validate_plan(root,batch_id,storage,command,generation):
    from native_batch_control import NativeBatchControl
    context=_context(storage,command,generation);plan=storage['plan']
    state=NativeBatchControl(root).assert_running(batch_id,epoch=context['native_revision'])
    if (digest(state.get('run_bindings',[]))!=context['bindings_sha256']
            or (state.get('execution_owner') or {}).get('token')!=context['producer_owner_token']):
        raise Held('storage_producer_binding_or_owner_changed')
    for entry in plan['manifests']:
        current=json.loads(Path(entry['path']).read_text(encoding='utf8'))
        if digest(producer_projection(current))!=entry['projection_sha256']:
            raise Held('storage_sealed_production_manifest_changed')
    ids=[i['delivery_id'] for i in plan['items']]
    if not ids or len(set(ids))!=len(ids) or set(plan['selected_input_ids'])!=set(storage['request']['input_ids']):
        raise Held('storage_sealed_expected_set_invalid')
    return context,plan


def bind(scope,context):
    with _db(scope.root) as db:
        validate_context(db,scope.root,scope.batch_id,context,input_id=scope.identity['input_id'])
    scope.release_context=copy.deepcopy(context)
    scope.native_stamp=None
    scope.assert_open()


def selected(root,batch_id,*,stop=None,context=None):
    from preextract_nas_scope import selected_task_items
    storage,command,meta,inputs,generation=request(root,batch_id)
    actual,plan=_validate_plan(root,batch_id,storage,command,generation)
    if context is not None and context!=actual:raise Held('storage_original_release_selection_changed')
    expected={i['delivery_id']:i for i in plan['items']}
    scopes,held=selected_task_items(root,batch_id=batch_id,delivery_ids=list(expected),stop=stop)
    if held or set(scopes)!=set(expected):raise Held('storage_complete_expected_delivery_set_not_ready')
    from preextract_retirement import _gate
    for ident,scope in scopes.items():
        item=expected[ident]
        if scope.job['payload']!=item['payload']:raise Held('storage_expected_delivery_payload_changed')
        # _gate verifies every specialty/role/attachment receipt against the
        # strict original package and actual saved attachment contract.
        _,prepared,_,_=_gate(Path(root).resolve(),scope.job,verify_originals=False)
        current=_item_plan(type('PublicationStore',(),{'root':Path(root),'runs':Path(root)/'runs',
            'storage_service':scope.payload['service']})(),scope.run_id,scope.native_item)
        current['input_id']=scope.identity['input_id']
        current['leaf_key']=item['leaf_key']
        if current!=item:raise Held('storage_expected_specialty_or_attachment_targets_changed')
        bind(scope,actual)
    return scopes,actual


def drive(root,batch_id,*,stop=None):
    try:
        scopes,context=selected(root,batch_id,stop=stop)
        if released_originals(root,scopes):return dict(status='complete',reason='permitted_copies_isolated_recoverably')
        from preextract_nas import kick
        result=kick(root,stop=stop,task_items=scopes)
        return result or dict(status='held',reason='storage_wakeup_not_accepted')
    except Disabled:return dict(status='disabled',reason='storage_explicit_release_not_allowed')
    except Failed as exc:return dict(status='failed',reason=str(exc))
    except Exception as exc:return dict(status='held',reason=str(exc)[:180] if isinstance(exc,Held)
        else 'storage_scope_or_target_evidence_unconfirmed',error_type=type(exc).__name__)


def release_set(root,items):
    """All expected items and all their NAS bytes precede any local release."""
    if not items:raise Held('storage_explicit_complete_selection_missing')
    scopes=list(items.values());first=scopes[0];context=getattr(first,'release_context',None)
    if context is None or any(getattr(s,'release_context',None)!=context or s.batch_id!=first.batch_id for s in scopes):
        raise Held('storage_explicit_release_permission_missing')
    complete,_=selected(root,first.batch_id,context=context)
    if set(complete)!=set(items):raise Held('storage_partial_release_selection_forbidden')
    return context


def require_recoverable(root):
    # Per-file rename_checked additionally proves fixed local filesystem,
    # same volume, directory/file identity and no replacement at operation time.
    if os.name!='nt':raise Held('storage_recoverable_local_cleanup_requires_windows')
    return 'same-volume-isolation/1'


def submit(root,items):
    """Carry exact worker parameters in the same existing applied command."""
    from preextract_nas_scope import describe_items
    context=release_set(root,items);batch_id=next(iter(items.values())).batch_id
    value=dict(schema='task-storage-worker-selection/1',context=context,items=describe_items(items))
    with _db(root,reserve=True) as db:
        validate_context(db,root,batch_id,context)
        row=db.execute('SELECT body FROM task_commands WHERE batch_id=? AND request_id=? AND status=\'applied\'',
                       (batch_id,context['request_id'])).fetchone()
        body=json.loads(row['body']);storage=body['storage'];previous=storage.get('submission')
        if previous is not None and previous!=value:raise Held('storage_original_submitted_selection_changed')
        storage['submission']=value
        db.execute('UPDATE task_commands SET body=? WHERE batch_id=? AND request_id=? AND status=\'applied\'',
                   (json.dumps(body,ensure_ascii=False),batch_id,context['request_id']))
        db.commit()
    return context


def submissions(root):
    """Only parameters actually submitted by the existing formal caller.

    This is not an active-grant union: each original command remains a separate
    finite set. No historical record is imported or granted a release marker.
    """
    with _db(root) as db:
        rows=db.execute('''SELECT c.batch_id,c.body FROM task_commands c JOIN task_controls t
            ON t.batch_id=c.batch_id AND t.revision=c.result_revision WHERE c.status='applied'
            AND t.managed=1 AND json_extract(c.body,'$.storage.allow_release')=1
            AND json_extract(c.body,'$.storage.submission.schema')='task-storage-worker-selection/1' ''').fetchall()
    return [json.loads(r['body'])['storage']['submission']['items'] for r in rows]


def released_originals(root,items):
    """Completion needs exact isolation receipts and the recoverable file object."""
    from preextract_nas import policy
    from preextract_nas_scope import _database
    first=next(iter(items.values()))
    storage,_,_,_,_=request(root,first.batch_id)
    path=Path(policy(root)['database'])
    if not path.is_file():return False
    identities={i:scope.identity for i,scope in items.items()}
    with _database(path) as db:
        from paperflow.original_archive.lifecycle import path_key
        for original in storage['request']['originals']:
            row=db.execute("SELECT state,proof FROM nas_releases WHERE path=? AND sha256=?",
                           (path_key(original['path']),original['sha256'])).fetchone()
            if row is None or row['state']!='isolated' or Path(original['path']).exists():return False
            proof=json.loads(row['proof'])
            if proof.get('job_id') not in identities or proof.get('task_identity')!=identities[proof['job_id']]:return False
            from paperflow.original_archive.lifecycle import verify_isolation
            verify_isolation(proof['recovery'],proof['device'],proof['inode'])
    return True


def restore_local_isolation(root,batch_id,request_id):
    """Explicit operator recovery API; never called by any worker/start path.

    Restore only the same command's recorded file objects, without replacement.
    Revoke its automatic local isolation before attempting any restoration.
    """
    from filelock import FileLock
    root=Path(root).resolve()
    try:
        with FileLock(str(root/'private/original_archive/lifecycle.lock'),timeout=0):
            return _restore_local_isolation(root,batch_id,request_id)
    except Exception as exc:
        return dict(status='restore_pending',restored_paths=[],held=[dict(error_type=type(exc).__name__,
            reason=str(exc)[:180] if isinstance(exc,Held) else 'storage_restore_evidence_or_lock_unconfirmed')])


def _restore_local_isolation(root,batch_id,request_id):
    from filelock import FileLock
    from preextract_nas import policy
    from preextract_nas_scope import _database
    from paperflow.original_archive.lifecycle import safe_file,locked_file,verify_stream,rename_checked,verify_isolation
    from preextract_material import write_json
    from preextract_cleanup_scope import allowlisted
    root=Path(root).resolve()
    with _db(root,reserve=True) as db:
        row=db.execute('''SELECT c.body,t.storage_version FROM task_commands c JOIN task_controls t
            ON t.batch_id=c.batch_id WHERE c.batch_id=? AND c.request_id=?''',(batch_id,request_id)).fetchone()
        if row is None or row['storage_version']!=1:raise Held('storage_existing_task_preserved')
        body=json.loads(row['body']);storage=body.get('storage') or {}
        if (storage.get('schema')!='task-storage-start/1' or storage.get('root')!=str(root)
                or storage.get('batch_id')!=batch_id or storage.get('plan',{}).get('state')!='sealed'):
            raise Held('storage_original_isolation_command_unconfirmed')
        if digest(storage['request'])!=storage.get('request_sha256'):
            raise Held('storage_original_isolation_request_changed')
        expected_context=_context(storage,dict(request_id=request_id),storage['plan']['global_generation'])
        submission=storage.get('submission') or {}
        descriptors=submission.get('items') or []
        identities={d['delivery_id']:d['identity'] for d in descriptors}
        if (submission.get('schema')!='task-storage-worker-selection/1' or submission.get('context')!=expected_context
                or len(identities)!=len(descriptors) or set(identities)!={i['delivery_id'] for i in storage['plan']['items']}
                or any(d.get('release_context')!=expected_context for d in descriptors)):
            raise Held('storage_original_submitted_isolation_scope_unconfirmed')
        storage['recovery_state']='restore_requested'
        db.execute('UPDATE task_commands SET body=? WHERE batch_id=? AND request_id=?',
                   (json.dumps(body,ensure_ascii=False),batch_id,request_id));db.commit()
    recovered=[];held=[]
    originals={(r['path'],r['sha256']):r['input_id'] for r in storage['request']['originals']}
    database=Path(policy(root)['database'])
    def restore(value,device,inode,expected_identity):
        scope=value.get('scope') or {};context=scope.get('release_context') or {}
        identity=scope.get('task_identity') or {}
        if (value.get('workspace')!=str(root) or context!=expected_context or identity!=expected_identity
                or identity.get('batch_id')!=batch_id or identity.get('input_id') not in storage['request']['input_ids']):
            raise Held('storage_restore_scope_changed')
        original=safe_file(value['original_path'],[root])
        isolated=safe_file(value['path'],[root/'private/original_archive/quarantine'])
        if original.exists():
            if isolated.exists():raise Held('storage_restore_would_overwrite_existing_path')
            with locked_file(original) as (stream,_):
                info=os.fstat(stream.fileno())
                if (info.st_dev,info.st_ino)!=(device,inode):raise Held('storage_restore_existing_file_identity_changed')
                verify_stream(stream,value['sha256'],value['bytes'])
            return original
        verify_isolation(value,device,inode)
        with locked_file(isolated,delete=True) as (stream,_permanent_delete_unused):
            info=os.fstat(stream.fileno())
            if (info.st_dev,info.st_ino)!=(device,inode):raise Held('storage_restore_file_identity_changed')
            verify_stream(stream,value['sha256'],value['bytes'])
            rename_checked(stream,original)
        with locked_file(original) as (stream,_):verify_stream(stream,value['sha256'],value['bytes'])
        return original
    seen_originals=set()
    if database.is_file():
        with _database(database) as db:
            rows=db.execute("""SELECT path,sha256,proof FROM nas_releases WHERE state IN ('isolated','releasing','restored')
                AND json_extract(proof,'$.task_identity.batch_id')=?
                AND json_extract(proof,'$.recovery.scope.release_context.request_id')=?""",(batch_id,request_id)).fetchall()
        for row in rows:
            proof=json.loads(row['proof']);value=proof.get('recovery') or {}
            try:
                key=(value.get('original_path'),row['sha256'])
                expected_identity=identities.get(proof.get('job_id'))
                from paperflow.original_archive.lifecycle import path_key
                if (key not in originals or expected_identity is None or proof.get('task_identity')!=expected_identity
                        or expected_identity['input_id']!=originals[key] or value.get('sha256')!=row['sha256']
                        or path_key(value['original_path'])!=row['path']):
                    raise Held('storage_restore_original_receipt_scope_changed')
                seen_originals.add(key)
                original=restore(value,proof['device'],proof['inode'],expected_identity)
                with _database(database,reserve=True) as db:
                    changed=db.execute("UPDATE nas_releases SET state='restored',updated=? WHERE path=? AND sha256=? AND proof=?",
                               (time.time(),row['path'],row['sha256'],row['proof'])).rowcount
                    if changed!=1:raise Held('storage_restore_original_receipt_changed')
                    db.commit()
                recovered.append(str(original))
            except Exception as exc:held.append(dict(path=value.get('original_path'),error_type=type(exc).__name__))
    else:held.append(dict(path=str(database),error_type='RestoreNasLedgerMissing'))
    for key in set(originals)-seen_originals:
        held.append(dict(path=key[0],error_type='RestoreOriginalReceiptMissing'))
    for item in storage['plan']['items']:
        work=safe_file(item['payload']['item']['preextract_work_dir'],[root])
        receipt=work/'cleanup-receipt.json'
        try:
            with FileLock(str(work/'operation.lock'),timeout=0):
                if not receipt.is_file():raise Held('storage_restore_cleanup_receipt_missing')
                report=json.loads(receipt.read_text(encoding='utf8'))
                expected_identity=identities[item['delivery_id']]
                if (report.get('task_storage',{}).get('identity')!=expected_identity
                        or report.get('work_dir')!=str(work)):
                    raise Held('storage_restore_cleanup_receipt_identity_changed')
                for resource in report.get('resources',[]):
                    value=resource.get('recovery')
                    if not value:continue
                    try:
                        target=safe_file(value['original_path'],[root])
                        if (resource.get('kind')!='file' or resource.get('path')!=str(target)
                                or resource.get('sha256')!=value.get('sha256') or resource.get('bytes')!=value.get('bytes')
                                or not allowlisted(target,work) or target.is_relative_to(work/'model')):
                            raise Held('storage_restore_cache_outside_exact_item')
                        original=restore(value,resource['device'],resource['inode'],expected_identity)
                        resource['recovery_state']='restored';recovered.append(str(original))
                    except Exception as exc:held.append(dict(path=value.get('original_path'),error_type=type(exc).__name__))
                report['status']='restored' if not held else 'restore_pending';write_json(receipt,report)
        except Exception as exc:
            held.append(dict(path=str(receipt),error_type=type(exc).__name__,
                reason=str(exc)[:180] if isinstance(exc,Held) else 'storage_restore_cleanup_evidence_or_lock_unconfirmed'))
    if not held:
        with _db(root,reserve=True) as db:
            row=db.execute('SELECT body FROM task_commands WHERE batch_id=? AND request_id=?',(batch_id,request_id)).fetchone()
            body=json.loads(row['body']);body['storage']['recovery_state']='restored'
            db.execute('UPDATE task_commands SET body=? WHERE batch_id=? AND request_id=?',
                       (json.dumps(body,ensure_ascii=False),batch_id,request_id));db.commit()
    return dict(status='restored' if not held else 'restore_pending',restored_paths=recovered,held=held)

