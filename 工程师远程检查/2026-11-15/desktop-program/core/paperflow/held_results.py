"""Validate owned, completed model output without executing or publishing work.

The caller supplies ledger rows under its paused recovery lock. This module
copies evidence and runs the existing validators; it never clears a hold,
resets a budget, publishes a cache, or calls a model.
"""
import json
import hashlib
from pathlib import Path
import re
import shutil

from .package import load, sha
from .phase_cache import CACHE_STAGES, file_hashes, input_fingerprint
from .producer import HarnessProducer


def validate_completed_output(config,task,stage,actor,action,execution,destination,*,admission=None):
    if stage not in CACHE_STAGES:raise ValueError('unsupported retained recovery stage')
    fingerprint=input_fingerprint(config,task,stage)
    proof=json.loads(actor['proof'])
    retained=proof.get('retained_result') or {}
    request=json.loads(action['request'])
    result=json.loads(action['result'] or '{}')
    operation=request.get('resource',{}).get('operation','')
    matched=re.fullmatch(re.escape(stage)+r':([0-9]+)',operation)
    if (action['kind']!='llm' or action['state'] not in ('complete','diagnostic') or not matched
            or actor['state']!='closed' or execution['state']!='exited'
            or execution['unit_kind']!='graph' or execution['unit_key']!=task['task_id']
            or actor['execution_id']!=action['execution_id'] or action['execution_id']!=execution['execution_id']
            or actor['epoch']!=action['epoch'] or action['epoch']!=execution['epoch']
            or retained.get('action_id')!=action['action_id'] or retained.get('actor_id')!=actor['actor_id']
            or retained.get('source_sha256')!=task['source_sha256']
            or retained.get('group_identity',{}).get('execution_id')!=actor['group_id']
            or retained.get('group_identity',{}).get('epoch')!=actor['epoch']):
        raise ValueError('retained model completion or ownership unproven')
    observed=[value for value in (retained.get('stage_input_sha256'),result.get('_timing',{}).get('input_fingerprint'))
              if value is not None]
    if not observed or any(value!=fingerprint for value in observed):
        raise ValueError('retained phase input changed or unproven')
    if 'finish_reason' in retained and retained['finish_reason']!='completed':
        raise ValueError('retained model success evidence conflicts')
    for key in ('task_id','source_id','revision'):
        if key in retained and retained[key]!=task[key]:raise ValueError('retained source/version identity conflict')
    if sha(task['source_path'])!=task['source_sha256']:raise ValueError('original source changed')
    output=Path(retained['output']).resolve()
    root=(config.root/'work'/task['task_id']/'attempts').resolve()
    if not output.is_relative_to(root) or output.name!=stage:
        raise ValueError('retained output outside task attempts')
    if any(p.is_symlink() or p.is_junction() for p in (output,*output.parents,*output.rglob('*'))):
        raise ValueError('retained output links forbidden')
    calls=[]
    if action['state']=='diagnostic':
        from .remote_files import file_map,manifest
        from .handoff_store import canonical
        if (retained.get('finish_reason')!='completed' or retained.get('collection_pending') is not False
                or any(retained.get(k)!=task[k] for k in ('task_id','source_id','revision'))
                or retained.get('stage_input_sha256')!=fingerprint or retained.get('cache_stage')!=stage):
            raise ValueError('retained completed download unproven')
        native=Path(retained.get('stage','')).resolve()
        if native.parent!=output.parent or not native.name.startswith(stage+'.native-'):
            raise ValueError('retained download staging identity mismatch')
        if any(p.is_symlink() or p.is_junction() for p in (native,*native.parents,*native.rglob('*'))):
            raise ValueError('retained output links forbidden')
        files=manifest(file_map(native))
        if (hashlib.sha256(canonical(files).encode('utf-8')).hexdigest()!=retained.get('files_manifest_sha256')
                or len(files)!=retained.get('files_count')
                or sum(item['size'] for item in files)!=retained.get('files_bytes')):
            raise ValueError('retained downloaded files changed')
        output=native
    else:
        report=load(output/'processing.json')
        calls=report.get('calls',[])
        ordinal=int(matched.group(1))
        current=[r for r in calls if r.get('stage')==stage and r.get('syntax_attempt')==ordinal]
        if len(current)!=1 or calls[-1]!=current[0]:raise ValueError('retained successful call evidence missing')
        record=current[0];session=record.get('session_id','')
        if not re.fullmatch(re.escape(task['task_id'])+'-'+str(task['revision'])+'-'+re.escape(stage)+r'-[a-zA-Z0-9]+',session):
            raise ValueError('retained processing session identity mismatch')
        suffix='' if ordinal==0 else '-syntax'+str(ordinal)
        private=load(config.root/'private/attempts'/(session+suffix+'.json'))
        if (private.get('task_id')!=task['task_id'] or private.get('finish_reason')!='completed'
                or any(private.get(k)!=v for k,v in record.items())):
            raise ValueError('retained model success evidence changed')
    if stage=='round3_chunk':
        if HarnessProducer._chunk_inherited_hashes(output)!=HarnessProducer._chunk_inherited_hashes(Path(task['draft_dir'])):
            raise ValueError('retained chunk changed inherited input bytes')
    before=file_hashes(output)
    destination=Path(destination).absolute()
    if destination.exists() or destination.name!=stage or destination.resolve().is_relative_to(output):
        raise ValueError('fresh retained validation destination required')
    shutil.copytree(output,destination)
    producer=HarnessProducer(config,admission=admission)
    # Chunk preflight still requires the configured exact counter. Missing
    # counter evidence fails closed; no stand-in grants production approval.
    completed=producer._finish_output(task,stage,destination,calls)
    if stage=='round3_chunk':
        preflight=producer._check_chunk_packet(task,completed.pop('_chunk_stage_packet'))
        if not preflight['stage_preflight_passed']:raise ValueError('retained chunk preflight rejected')
        producer._attempt.chunk_resume_binding=dict(stage=stage,
            identity={k:task[k] for k in ('task_id','source_id','source_sha256')},
            input_basis=input_fingerprint(config,task,stage,chunk_reuse_basis=True))
        completed['chunk_stage_preflight']=producer._record_chunk_preflight(destination,preflight,'recovered')
    if file_hashes(output)!=before:raise ValueError('retained original output changed during validation')
    if sha(destination/'profile.json')!=before['profile.json']:
        raise ValueError('retained recovery must preserve profile bytes')
    completed['stage_input_sha256']=fingerprint
    return completed,dict(action_id=action['action_id'],actor_id=actor['actor_id'],
        stage=stage,input_fingerprint=fingerprint,source_files=before,
        profile_unchanged=True,model_calls=0,publication_granted=False)
