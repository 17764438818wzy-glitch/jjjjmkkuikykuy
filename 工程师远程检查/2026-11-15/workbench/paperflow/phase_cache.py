"""Complete, hash-checked phase results, independent of LangGraph checkpoints."""
import hashlib
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


def input_fingerprint(config, task, stage):
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
        processing=PROCESSING_PROTOCOL, prompt=sha(PROMPTS/prompt),
        repair=dict(source=task['repair_source']['manifest_sha256'],problem=task['repair_problem']) if task.get('repair_source') else None,
        catalog=sha(config.catalog_path) if Path(config.catalog_path).is_file() and business in ('round1','round2') else None)
    # Third-round caches produced before required delivery preflight are not
    # qualified by the new checks. Keep the first two rounds' exact fingerprints.
    if business in PHASES:fields['delivery_validation']='required-artifacts/1'
    if business=='round1':
        from .attachments import is_candidate
        if is_candidate(task['source_path']):fields['attachment_protocol']=sha(PROMPTS/'round1_attachment.txt')
    return digest(fields)


def cache_path(config, task, stage, fingerprint):
    return config.root/'work'/task['task_id']/'phase-cache'/stage/(fingerprint+'.json')


def verify_entry(config, task, stage, fingerprint, entry):
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
    for name in required:
        if name.endswith('.json') and not isinstance(load(output/name), dict):
            raise ValueError('invalid phase structure')
    if business in ('round1','round2') and entry['result'].get('disposition') not in ('exclude','uncertain') and not attachment:
        validate_core_text(output)
    reading = entry['result'].get('reading_dir')
    if reading and file_hashes(reading) != entry['reading_files']:
        raise ValueError('changed source view')
    return entry


def read_cache(config, task, stage, fingerprint):
    try:
        ledger=getattr(config,'processing_ledger_path',None)
        if ledger:
            with sqlite3.connect(Path(ledger).as_uri()+'?mode=ro',uri=True,timeout=.1) as c:
                if c.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='defective_cache'").fetchone() and c.execute('SELECT 1 FROM defective_cache WHERE task_id=? AND stage=? AND fingerprint=?',
                             (task['task_id'],stage,fingerprint)).fetchone():return None
        entry = load(cache_path(config, task, stage, fingerprint))
        if not isinstance(entry, dict) or not entry.get('complete'): return None
        cache_paths_retained(config,entry)
        verify_entry(config, task, stage, fingerprint, entry)
        result = dict(entry['result'], cache_hit=True, stage_input_sha256=fingerprint)
        result['processing_history'] = task.get('processing_history', []) + entry['stage_records']
        return result
    except (OSError, ValueError, KeyError, TypeError):
        return None


def prepare_cache(config, task, stage, fingerprint, result):
    entry = dict(protocol=PROTOCOL, stage=stage, fingerprint=fingerprint,
        input_task={k: v for k,v in task.items() if k in ('task_id','source_id','source_sha256','source_path',
                    'draft_dir','package_dir','reading_dir','failed_stage','repair_source','repair_problem')},
        identity={k: task[k] for k in ('task_id','source_id','source_sha256')},
        result={k: v for k, v in result.items() if k not in ('processing_history','cache_hit')},
        stage_records=result.get('processing_history', [])[len(task.get('processing_history', [])):],
        files=file_hashes(result['draft_dir']),
        reading_files=file_hashes(result['reading_dir']) if result.get('reading_dir') else {})
    verify_entry(config, task, stage, fingerprint, entry)
    manifest = Path(result['draft_dir']).parent/'phase-result.json'
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
