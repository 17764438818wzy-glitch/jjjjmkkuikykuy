"""Complete, hash-checked phase results, independent of LangGraph checkpoints."""
import hashlib
import itertools
import json
import sqlite3
from pathlib import Path
from .package import inside, load, save, sha, validate_core_text, optional_json

PHASES = {'round3_assets', 'round3_redact', 'round3_chunk'}
CACHE_STAGES = ('round1', 'round2', 'round3_assets', 'round3_redact', 'round3_chunk')
PREVIOUS = dict(zip(CACHE_STAGES[1:], CACHE_STAGES))
PROMPTS = Path(__file__).with_name('prompts')
PROTOCOL = 'paperflow-phase/2'
# Bump when the common prompt or deterministic output semantics change.
PROCESSING_PROTOCOL = 'three-rounds/2'
ASSET_STORAGE_PROTOCOL = 'mode-type-content/1'
# Frozen B2 template is a compatibility identity, never a current qualification.
LEGACY_CHUNK_PROMPT_SHA256 = 'e92494645509388ef6ae196bc7ef20d9641d108f31739828850a19bd3607527d'
INPUT_NAMES = ('article.redacted.md', 'profile.json', 'tree.json', 'assets.json',
               'chunks.json', 'review.json', 'corrections.json', 'private/redaction_map.json')


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False).encode('utf-8')).hexdigest()


def file_hashes(root):
    root = Path(root).resolve()
    return {p.relative_to(root).as_posix(): sha(inside(root, p.relative_to(root).as_posix()))
            for p in sorted(root.rglob('*')) if p.is_file()}


def content_token(root,stage):
    """Business output lineage excludes execution timing, reports and diagnostic files."""
    root=Path(root)
    names={'round1':('article.redacted.md','profile.json','tree.json'),
           'round2':('article.redacted.md','profile.json','tree.json','corrections.json'),
           'round3_assets':('assets.json',),
           'round3_redact':('article.redacted.md','profile.json','tree.json','assets.json'),
           'round3_chunk':('chunks.json',)}[stage]
    values={}
    for name in names:
        path=root/name
        if not path.is_file():continue
        if name=='corrections.json':
            values[name]=optional_json(path,dict(unreadable_optional_sha256=sha(path)))
        else:
            values[name]=load(path) if name.endswith('.json') else path.read_text(encoding='utf-8')
    if stage in ('round3_assets','round3_redact') and (root/'assets').is_dir():values['images']=file_hashes(root/'assets')
    return digest(values)


def phase_dependency(task,business,cache_stage,result):
    token=content_token(result['draft_dir'],business)
    prior=task.get('repair_dependencies',{}).get(business,{})
    repair=bool(task.get('repair_request_id'))
    lineage=set(prior.get('lineage',[])) if repair else set()
    if repair and prior.get('token'):lineage.add(prior['token'])
    lineage.add(token)
    resolved=set(prior.get('resolved_requests',[])) if repair else set()
    if repair and business==task['failed_stage']:resolved.add(task['repair_request_id'])
    return dict(token=token,lineage=sorted(lineage),lineage_complete=prior.get('lineage_complete',False) if repair else True,
        resolved_requests=sorted(resolved),cache_stage=cache_stage,cache_key=result['stage_input_sha256'],directory=result['draft_dir'])


def input_fingerprint(config, task, stage, *, chunk_reuse_basis=False):
    business=task.get('failed_stage') if stage=='repair' else stage
    if business not in CACHE_STAGES:raise ValueError('unknown business phase')
    previous = Path(task.get('draft_dir') or task.get('package_dir') or '.')
    files = {n: sha(inside(previous, n)) for n in INPUT_NAMES if inside(previous, n).is_file()}
    if (previous/'assets').is_dir():
        files.update({'assets/'+n: h for n, h in file_hashes(previous/'assets').items()})
    reading = file_hashes(task['reading_dir']) if task.get('reading_dir') else {}
    prompt = business+'.txt' if business in PHASES else business+'.original.txt'
    fields=dict(stage=stage, identity={k: task[k] for k in ('task_id','source_id','source_sha256')},
        inputs=files, reading=reading, source_path=str(Path(task['source_path']).resolve()),
        model=config.model, provider=config.provider, access=getattr(config, 'model_access_fingerprint', ''),
        route=sha(PROMPTS/'free-route.yml'), contract=config.contract_version, protocol=PROTOCOL,
        processing=PROCESSING_PROTOCOL, prompt=(LEGACY_CHUNK_PROMPT_SHA256
            if business=='round3_chunk' and chunk_reuse_basis else sha(PROMPTS/prompt)),
        repair=dict(source=task['repair_source']['manifest_sha256'],problem=task['repair_problem']) if task.get('repair_source') else None,
        catalog=sha(config.catalog_path) if Path(config.catalog_path).is_file() and business in ('round1','round2') else None)
    # Third-round caches produced before required delivery preflight are not
    # qualified by the new checks. Keep the first two rounds' exact fingerprints.
    if business in PHASES:
        fields['delivery_validation']='required-artifacts/1'
        fields['asset_storage']=ASSET_STORAGE_PROTOCOL
        repair=task.get('preparation_length_repair')
        if repair and CACHE_STAGES.index(business)>=CACHE_STAGES.index(repair['owner']):
            fields['preparation_length_repair']=repair
    if business=='round3_chunk' and not chunk_reuse_basis:
        from .chunk_stage_guard import contract_fingerprint
        fields['chunk_stage_preflight']=contract_fingerprint(config)
    if business=='round1':
        from .attachments import is_candidate
        if is_candidate(task['source_path']):fields['attachment_protocol']=sha(PROMPTS/'round1_attachment.txt')
    return digest(fields)


def cache_path(config, task, stage, fingerprint):
    return config.root/'work'/task['task_id']/'phase-cache'/stage/(fingerprint+'.json')


def verify_entry(config, task, stage, fingerprint, entry, *, allow_unqualified_chunk=False):
    if (entry['protocol'] != PROTOCOL or entry['stage'] != stage or entry['fingerprint'] != fingerprint
            or entry['identity'] != {k: task[k] for k in ('task_id','source_id','source_sha256')}):
        raise ValueError('cache identity mismatch')
    output = Path(entry['result']['draft_dir']).resolve()
    if not output.is_relative_to((config.root/'work'/task['task_id']).resolve()):
        raise ValueError('cache outside task directory')
    business=task.get('failed_stage') if stage=='repair' else stage
    required = {'profile.json'}
    attachment=stage=='round1' and entry['result'].get('attachment') is not None
    if attachment:
        from .attachments import attachment_note,is_candidate
        if not is_candidate(task['source_path']):raise ValueError('ordinary paper cannot use attachment cache')
        if attachment_note(load(output/'profile.json'))!=entry['result']['attachment']:raise ValueError('cached attachment note mismatch')
    if not (stage == 'round1' and (entry['result'].get('disposition') in ('exclude','uncertain') or attachment)):
        required.update(('article.redacted.md', 'tree.json'))
        if business in PHASES: required.add('assets.json')
        if business == 'round3_chunk': required.add('chunks.json')
    if not required.issubset(entry['files']) or file_hashes(output) != entry['files']:
        raise ValueError('incomplete or changed phase files')
    if business=='round3_chunk' and not allow_unqualified_chunk:
        from .chunk_stage_guard import CONTRACT,contract_fingerprint
        preflight=entry['result'].get('chunk_stage_preflight',{})
        if (preflight.get('schema')!=CONTRACT or preflight.get('stage_preflight_passed') is not True
                or preflight.get('final_identity_verified') is not False
                or preflight.get('admission_verified') is not False
                or preflight.get('contract_fingerprint')!=contract_fingerprint(config)):
            raise ValueError('chunk stage preflight missing or stale')
    for name in required:
        if name.endswith('.json') and not isinstance(load(output/name), dict):
            raise ValueError('invalid phase structure')
    if business in ('round1','round2') and entry['result'].get('disposition') not in ('exclude','uncertain') and not attachment:
        validate_core_text(output)
    reading = entry['result'].get('reading_dir')
    if reading and file_hashes(reading) != entry['reading_files']:
        raise ValueError('changed source view')
    return entry


def read_cache(config, task, stage, fingerprint, *, _allow_unqualified_chunk=False):
    try:
        ledger=getattr(config,'processing_ledger_path',None)
        if ledger:
            with sqlite3.connect(Path(ledger).as_uri()+'?mode=ro',uri=True,timeout=.1) as c:
                if c.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='defective_cache'").fetchone() and c.execute('SELECT 1 FROM defective_cache WHERE task_id=? AND stage=? AND fingerprint=?',
                             (task['task_id'],stage,fingerprint)).fetchone():return None
        entry = load(cache_path(config, task, stage, fingerprint))
        if not isinstance(entry, dict) or not entry.get('complete'): return None
        cache_paths_retained(config,entry)
        verify_entry(config, task, stage, fingerprint, entry,allow_unqualified_chunk=_allow_unqualified_chunk)
        result = dict(entry['result'], cache_hit=True, stage_input_sha256=fingerprint)
        if stage=='round1' and not result.get('attachment'):
            from .package import article_presentation
            result.update(article_presentation(load(Path(result['draft_dir'])/'profile.json'),task['source_path']))
        result['processing_history'] = task.get('processing_history', []) + entry['stage_records']
        return result
    except (OSError, ValueError, KeyError, TypeError):
        return None


def read_reusable_chunk_cache(config,task,stage):
    """Return hash-verified bytes only; producer must run the current guard.

The exact frozen B2 fingerprint locates legacy caches. New entries also retain
that stable input basis, so later prompt/rule changes do not regenerate text.
Other cache consumers never use this explicitly unqualified entry point.
"""
    business=task.get('failed_stage') if stage=='repair' else stage
    if business!='round3_chunk':return None
    basis=input_fingerprint(config,task,stage,chunk_reuse_basis=True)
    directory=cache_path(config,task,stage,basis).parent
    candidates=sorted(directory.glob('*.json'),key=lambda path:path.stat().st_mtime_ns,reverse=True)
    for path in candidates:
        try:
            entry=load(path);fingerprint=entry['fingerprint']
            if entry.get('chunk_reuse_fingerprint',fingerprint)!=basis:continue
            result=read_cache(config,task,stage,fingerprint,_allow_unqualified_chunk=True)
            if result is not None:
                return dict(result,cache_source_fingerprint=fingerprint,cache_requires_chunk_preflight=True)
        except (OSError,ValueError,KeyError,TypeError):continue
    return None


def read_prepared_cache(config,task,stage,fingerprint):
    """Recover a validated result whose publication was interrupted.

    This returns bytes, never publication authority. The producer must commit
    through its current admission before using the result as a phase cache.
    Explicit invalidation and changed evidence remain barriers to recovery.
    """
    if cache_path(config,task,stage,fingerprint).exists():return None
    ledger=getattr(config,'processing_ledger_path',None)
    if ledger:
        with sqlite3.connect(Path(ledger).as_uri()+'?mode=ro',uri=True,timeout=.1) as db:
            if (db.execute("SELECT 1 FROM sqlite_master WHERE name='defective_cache'").fetchone()
                    and db.execute('SELECT 1 FROM defective_cache WHERE task_id=? AND stage=? AND fingerprint=?',
                                   (task['task_id'],stage,fingerprint)).fetchone()):return None
    root=(config.root/'work'/task['task_id']/'attempts').resolve()
    paths=list(itertools.islice(root.glob('*/phase-result*.json'),129))
    if len(paths)>128:raise ValueError('prepared phase evidence lookup limit exceeded')
    selected=None
    for path in paths:
        if not path.resolve().is_relative_to(root):raise ValueError('prepared phase evidence outside task attempts')
        if path.stat().st_size>8*1024*1024:raise ValueError('prepared phase evidence exceeds bounded size')
        entry=load(path)
        if not isinstance(entry,dict):raise ValueError('invalid prepared phase evidence')
        if entry.get('stage')!=stage or entry.get('fingerprint')!=fingerprint:continue
        output=Path(entry['result']['draft_dir']).resolve()
        if output.parent!=path.parent.resolve() or output.name!=stage:
            raise ValueError('prepared phase output does not match attempt')
        inputs=entry.get('input_task')
        if not isinstance(inputs,dict) or input_fingerprint(config,inputs,stage)!=fingerprint:
            raise ValueError('prepared phase input changed')
        cache_paths_retained(config,entry)
        verify_entry(config,task,stage,fingerprint,entry)
        if selected is not None and (selected['files']!=entry['files'] or selected['result']!=entry['result']):
            raise ValueError('conflicting prepared phase results')
        selected=entry
    if selected is None:return None
    return dict(selected['result'],stage_input_sha256=fingerprint,_prepared_cache=True,
                processing_history=task.get('processing_history',[])+selected['stage_records'])


def prepare_cache(config, task, stage, fingerprint, result):
    entry = dict(protocol=PROTOCOL, stage=stage, fingerprint=fingerprint,
        input_task={k: v for k,v in task.items() if k in ('task_id','source_id','source_sha256','source_path',
                    'draft_dir','package_dir','reading_dir','failed_stage','repair_source','repair_problem',
                    'preparation_length_repair')},
        identity={k: task[k] for k in ('task_id','source_id','source_sha256')},
        result={k: v for k, v in result.items() if k not in ('processing_history','cache_hit')},
        stage_records=result.get('processing_history', [])[len(task.get('processing_history', [])):],
        files=file_hashes(result['draft_dir']),
        reading_files=file_hashes(result['reading_dir']) if result.get('reading_dir') else {})
    business=task.get('failed_stage') if stage=='repair' else stage
    if business=='round3_chunk':entry['chunk_reuse_fingerprint']=input_fingerprint(config,task,stage,chunk_reuse_basis=True)
    verify_entry(config, task, stage, fingerprint, entry)
    name=('phase-result-'+fingerprint+'.json' if result.get('cache_source_fingerprint') else 'phase-result.json')
    manifest = Path(result['draft_dir']).parent/name
    save(manifest, entry)
    return manifest


def publish_cache(config, task, stage, fingerprint, entry, qualification):
    # The caller has already validated and qualified these complete bytes. No worker-side authority.
    cache_paths_retained(config,entry)
    marker = cache_path(config, task, stage, fingerprint)
    existing = read_cache(config, task, stage, fingerprint)
    if existing: return existing
    save(marker, dict(entry, complete=True, qualification=qualification))
    return dict(entry['result'], stage_input_sha256=fingerprint,
                processing_history=task.get('processing_history', []) + entry['stage_records'])


def cache_paths_retained(config,entry):
    ledger=getattr(config,'processing_ledger_path',None)
    if ledger:
        from .cleanup_paths import check_path_reference
        with sqlite3.connect(Path(ledger).as_uri()+'?mode=ro',uri=True,timeout=.1) as c:
            for key in ('draft_dir','reading_dir'):
                if entry['result'].get(key):check_path_reference(c,entry['result'][key])


def write_cache(config, task, stage, fingerprint, result):
    manifest = prepare_cache(config, task, stage, fingerprint, result)
    return publish_cache(config, task, stage, fingerprint, load(manifest), {'legacy_local': True})
