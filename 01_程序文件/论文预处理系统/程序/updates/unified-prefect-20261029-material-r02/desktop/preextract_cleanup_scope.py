"""Explicit disposable resources and verified final-result reuse, without transport."""
from __future__ import annotations

from contextlib import closing
import bisect
import functools
import hashlib
import json
import os
from pathlib import Path
import sqlite3

from preextract_material import write_json

RULES = 'verified-cache-lifecycle/2'
PROOF = 'private/verified-cleanup-reuse.json'
DIRECTORIES = ('private/rendered', 'private/converted', 'private/request-images', 'private/images')
REQUEST_CACHES = {'request.json', 'planned-request.json', 'material.json', 'partition-material.json'}

def read(path):
    return json.loads(Path(path).read_text(encoding='utf8'))

def sha(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()

def overlaps(a, b):
    # Callers canonicalize at admission. Re-resolving thousands of immutable
    # dependency paths per file is particularly costly on Windows.
    a, b = Path(a), Path(b)
    return a == b or a.is_relative_to(b) or b.is_relative_to(a)

class Dependencies:
    """Reuse the prior maintenance tool's sorted-prefix overlap check."""
    def __init__(self, paths):
        self.names={os.path.normcase(str(p)).rstrip('\\/') for p in paths}
        self.ordered=sorted(self.names)
    def overlaps(self,path):
        value=os.path.normcase(str(path)).rstrip('\\/')
        for parent in (Path(value),*Path(value).parents):
            if str(parent).rstrip('\\/') in self.names: return True
        prefix=value+os.sep; index=bisect.bisect_left(self.ordered,prefix)
        return index<len(self.ordered) and self.ordered[index].startswith(prefix)

def content_hashes(value):
    """Native content identities, including shared inputs without path pointers."""
    found=set()
    def add(digest):
        if isinstance(digest,str) and len(digest)==64 and all(c in '0123456789abcdef' for c in digest):found.add(digest)
    def visit(v):
        if isinstance(v,dict):
            for key,child in v.items():
                if key in {'source_sha256','source_hash','sha256'}:add(child)
                if key in {'file_hashes','retained_file_hashes'} and isinstance(child,dict):
                    for digest in child.values():add(digest)
                visit(child)
        elif isinstance(v,list):
            for child in v:visit(child)
    visit(value)
    return found

@functools.lru_cache(maxsize=32768)
def _dependency_keys(path,stamp,kind):
    value=read(path)
    if kind=='references_and_hashes':
        from preextract_cleanup import _references
        return tuple(_references(value)),tuple(content_hashes(value))
    if kind=='material': values=value.get('file_hashes',{})
    elif kind=='proof': values=value.get('retained',{})
    else:
        from preextract_cleanup import _references
        return tuple(_references(value))
    return tuple(Path(p).resolve() for p in values)

def dependency_keys(path,kind):
    state=Path(path).stat()
    stamp=(state.st_mtime_ns,state.st_size,state.st_ino)
    if kind in {'references','content_hashes'}:
        return _dependency_keys(str(path),stamp,'references_and_hashes')[kind=='content_hashes']
    return _dependency_keys(str(path),stamp,kind)

def verify_reuse(work, *, source=None, template=None, catalog=None):
    """A final local result may outlive transport payloads, never its evidence."""
    from preextract_publish import _paperflow
    from preextract_handoff import _receipt_complete
    work = Path(work).resolve()
    proof = read(work/PROOF)
    if proof.get('schema') != RULES or proof.get('work_dir') != str(work):
        raise ValueError('verified final reuse identity changed')
    for path, digest in proof['retained'].items():
        if not Path(path).is_file() or sha(path) != digest:
            raise ValueError('verified final reuse evidence changed')
    substitutes=proof.get('source_substitutions') or {}
    for original,value in substitutes.items():
        replacement=Path(value['path']).resolve()
        if (not replacement.is_relative_to(work/'private') or not replacement.name.startswith('original.')
                or proof['retained'].get(str(replacement))!=value['sha256']
                or value['sha256']!=proof['identity']['task']['source_sha256']
                or original in proof['retained']):
            raise ValueError('verified final reuse source substitute changed')
    prepared = read(work/'private/prepared-publication.json')
    binding = read(work/'delivery-binding.json')
    receipt = read(work/'delivery-receipt.json')
    packet = _paperflow(prepared['paperflow_root']).validate_package(
        proof['package_path'], prepared['task'], prepared['catalog'], strict_seal=True)
    identity = dict(task=prepared['task'], manifest_sha256=packet['manifest_sha256'])
    if (proof['identity'] != identity or binding.get('service')!=proof['service']
            or any(binding.get(k) != v or receipt.get(k) != v for k,v in identity.items())
            or binding.get('routes') != packet['routes'] or not _receipt_complete(receipt,packet)
            or receipt.get('stage') != 'verified'):
        raise ValueError('verified final reuse delivery changed')
    if prepared.get('associated_upload_plan_sha256'):
        from associated_delivery import load_plan,receipt_summary
        prepared['packet']=packet
        plan=load_plan(prepared,work); associated=receipt_summary(work)
        if plan['files'] and (not associated or associated.get('status')!='complete'
                or associated.get('plan_sha256')!=prepared['associated_upload_plan_sha256']
                or associated.get('service')!=binding.get('service')
                or associated.get('verified_count')!=len(plan['files'])):
            raise ValueError('verified associated originals changed')
    if source is not None:
        value=substitutes.get(str(Path(source).resolve()))
        check=Path(source) if Path(source).is_file() else Path(value['path']) if value else None
        if check is None or sha(check)!=prepared['task']['source_sha256']:
            raise ValueError('verified final reuse source changed')
    if template is not None and sha(template) != proof['template_sha256']:
        raise ValueError('verified final reuse template changed')
    if catalog is not None:
        bundle=read(work/'material.json')
        if hashlib.sha256(json.dumps(catalog,ensure_ascii=False,sort_keys=True).encode()).hexdigest()!=proof['catalog_digest']:
            raise ValueError('verified final reuse catalog changed')
        if bundle['source']['source_sha256']!=prepared['task']['source_sha256']:
            raise ValueError('verified final reuse material source changed')
    return proof

def final_result(work, source, template, catalog, *, stage='package'):
    work=Path(work)
    if not (work/PROOF).is_file():
        return None
    proof=verify_reuse(work,source=source,template=template,catalog=catalog)
    return dict(schema='paper-preextract-run/1.0', work_dir=str(work),
        source_sha256=proof['identity']['task']['source_sha256'],
        stage='material_prepared' if stage=='prepare' else 'candidate_validated' if stage=='extract' else
              'package_verified',
        package_path=proof['package_path'], completed=False,
        verified_cleaned_reuse=True, response_sha256=proof.get('response_sha256'), stage_timings=[])

def _verify_models(work, bundle, catalog, template):
    """Check actual complete answers and exact request binding before retirement."""
    from preextract_partition_run import _directory, _request_hash, _saved_reply
    model=work/'model'
    if (model/'partition-state.json').is_file():
        state=read(model/'partition-state.json'); plan=read(model/'partition-plan.json')
        if (state.get('status')!='assembled' or sha(model/'partition-plan.json')!=state.get('plan_sha256')
                or sha(model/'assembled-candidate.json')!=state.get('candidate_sha256')):
            raise ValueError('complete partition assembly not verified')
        from preextract_partition_run import run_partition
        def forbidden(*a,**kw): raise ValueError('partition cache would require another model request')
        run_partition(bundle,work,template,catalog,forbidden,forbidden)
        return state['candidate_sha256']
    if (work/'candidate-import.json').is_file():
        from preextract_pipeline import _import_candidate, _revision
        _, digest, _ = _import_candidate(work, _revision(read(work/'material.json')['source']['file'],work))
        return digest
    state=read(model/'request-state.json')
    if state.get('status')!='response_complete' or sha(model/'response.txt')!=state.get('response_sha256'):
        raise ValueError('complete model answer not verified')
    request=read(model/'request.json')
    if _request_hash(request)!=state.get('request_sha256'):
        raise ValueError('complete model request not verified')
    material=json.loads(request['messages'][1]['content'][0]['text'])['material']
    if (not request['messages'][0]['content'].endswith(template)
            or any(material['source'].get(k)!=bundle['source'].get(k) for k in
                ('source_sha256','text_sha256','index_sha256','catalog_sha256'))
            or material.get('associated_materials_sha256')!=bundle.get('associated_materials_sha256')):
        raise ValueError('complete model material or template not verified')
    return state['response_sha256']

def _substitute_source(proof, values):
    """Replace a releasable raw path only with this work's same-byte original."""
    for original,value in (values or {}).items():
        original=str(Path(original).resolve());replacement=Path(value['path']).resolve()
        previous=(proof.get('source_substitutions') or {}).get(original)
        if previous is not None:
            if previous!=value:raise ValueError('verified source substitution identity changed')
            continue
        expected=proof['identity']['task']['source_sha256']
        storage=value['storage_identity']
        if (not replacement.is_relative_to(Path(proof['work_dir'])/'private')
                or not replacement.name.startswith('original.') or str(replacement)==original
                or value['sha256']!=expected or proof['retained'].get(original)!=expected
                or sha(original)!=expected or sha(replacement)!=expected
                or storage['source_sha256']!=expected or storage['work_dir']!=proof['work_dir']
                or storage['package_path']!=proof['package_path']
                or storage['package_manifest_sha256']!=proof['identity']['manifest_sha256']):
            raise ValueError('exact verified original substitute unavailable')
        proof['retained'].pop(original)
        proof['retained'][str(replacement)]=expected
        proof.setdefault('source_substitutions',{})[original]=value
    return proof


def prepare_reuse(work, prepared, published, bundle, *, retain_extra=(),source_substitutions=None):
    """Write the new terminal contract BEFORE retiring any old input payload."""
    work=Path(work).resolve()
    if (work/PROOF).is_file():
        proof=_substitute_source(verify_reuse(work),source_substitutions)
        write_json(work/PROOF,proof)
        return verify_reuse(work)
    context=work/'private/completed-source-context.json'
    from preextract_pipeline import DEFAULT_TEMPLATE
    from preextract_contract import load_template
    if context.is_file():
        data=read(context)
        if (data['material_sha256']!=sha(work/'material.json')
                or data['candidate_sha256']!=sha(work/'private/candidate.json')
                or data['source_sha256']!=prepared['task']['source_sha256']):
            raise ValueError('completed source context changed')
        template_sha=data['template_sha256']
        template_path=Path(data.get('template_path') or DEFAULT_TEMPLATE)
        if sha(template_path)!=template_sha: raise ValueError('completed source template changed')
        template,_=load_template(template_path)
    else:
        # Existing normal requests can be migrated only with the exact known MD.
        template,_=load_template(DEFAULT_TEMPLATE)
        request=read(work/'model/request.json')
        if not request['messages'][0]['content'].endswith(template):
            raise ValueError('historical extraction template not verified')
        template_sha=sha(DEFAULT_TEMPLATE)
    if (work/'revision.json').is_file():
        from preextract_pipeline import _revision
        _revision(bundle['source']['file'],work)
    response_sha=_verify_models(work,bundle,prepared['catalog'],template)
    retained=set(Path(p).resolve() for p in bundle.get('file_hashes',{}))
    retained.update(Path(p).resolve() for p in retain_extra)
    retained.update(work/n for n in ('material.json','private/candidate.json',
        'private/candidate-raw.json','private/论文预提取.md','private/prepared-publication.json',
        'private/completed-source-context.json','validation.json','normalization.json','revision.json','candidate-import.json')
        if (work/n).is_file())
    # Original raw replies, per-request state and assembly evidence remain readable.
    for p in (work/'model').rglob('*'):
        if p.is_file() and (p.name in {'response.txt','request-state.json','partition-state.json',
                'partition-plan.json','assembled-candidate.json','parts-candidate.json','assembly-audit.json','revision.json','http-error.txt'}):
            retained.add(p.resolve())
    proof=dict(schema=RULES, work_dir=str(work), package_path=str(published),
        identity=dict(task=prepared['task'], manifest_sha256=prepared['validation']['manifest_sha256']),
        service=read(work/'delivery-binding.json')['service'],
        template_sha256=template_sha, response_sha256=response_sha,
        catalog_digest=hashlib.sha256(json.dumps(prepared['catalog'],ensure_ascii=False,sort_keys=True).encode()).hexdigest(),
        retained={str(p):sha(p) for p in sorted(retained)})
    proof=_substitute_source(proof,source_substitutions)
    write_json(work/PROOF,proof)
    return verify_reuse(work)

def family_workspaces(root, work, source_hash):
    """Only explicit revision lineage and exact queue source identities qualify."""
    from preextract_cleanup import _safe_path
    from preextract_delivery_queue import queue_path
    found={work}; pending=[work]
    path=queue_path(root)
    if path.is_file():
        with closing(sqlite3.connect(path.as_uri()+'?mode=ro',uri=True,timeout=5)) as db:
            for payload, in db.execute("SELECT payload FROM deliveries WHERE json_extract(payload,'$.task.source_sha256')=?",(source_hash,)):
                value=json.loads(payload)['item'].get('preextract_work_dir')
                if value: pending.append(_safe_path(value,root))
    while pending:
        current=pending.pop()
        material=current/'material.json'
        if not material.is_file() or read(material)['source']['source_sha256']!=source_hash:
            continue
        found.add(current)
        revision=current/'revision.json'
        if revision.is_file():
            meta=read(revision); parent=_safe_path(meta['parent'],root)
            if current.parent!=parent/'revisions' or meta['source_sha256']!=source_hash:
                raise ValueError('cleanup revision lineage changed')
            if parent not in found:
                pending.append(parent)
        if (current/'revisions').is_dir():
            for child in (current/'revisions').iterdir():
                if child.is_dir() and child not in found and (child/'material.json').is_file():
                    pending.append(_safe_path(child,root))
    return found

def immutable_paths(bundle, candidate, family):
    from preextract_cleanup import _references
    protected=set(_references(bundle)) | set(_references(candidate))
    # Parent identity commitments stay protected even after the child succeeds.
    for member in family:
        for name in ('revision.json','candidate-import.json'):
            path=member/name
            if not path.is_file(): continue
            value=read(path)
            if name=='revision.json':
                parent=Path(value['parent']).resolve()
                protected.update(parent/rel for rel,digest in value.get('parent_hashes',{}).items() if digest)
                if value.get('parent_reuse_proof_sha256'): protected.add(parent/PROOF)
            else:
                protected.update(Path(p).resolve() for p in value.get('evidence',{}))
        partition=member/'model/partition-state.json'
        if partition.is_file():
            for recovery in read(partition).get('recoveries',[]):
                previous=(member/'model'/recovery['previous']).resolve()
                if not previous.is_relative_to(member/'model'):
                    raise ValueError('partition recovery evidence outside its model history')
                protected.update(previous/name for name,digest in recovery.get('evidence',{}).items() if digest)
    return protected

def protected_paths(root, work, bundle, candidate, family, *, changed_since=None):
    from preextract_cleanup import _references
    from preextract_delivery_queue import queue_path
    protected=immutable_paths(bundle,candidate,family)
    from preextract_scope_guard import protected_scope_paths
    protected.update(protected_scope_paths(work))
    from preextract_scope_guard import retirement_dependencies
    scope_paths,_=retirement_dependencies(root,{})
    protected.update(scope_paths)
    path=queue_path(root)
    if path.is_file():
        with closing(sqlite3.connect(path.as_uri()+'?mode=ro',uri=True,timeout=5)) as db:
            query=("SELECT status,json_extract(payload,'$.item.preextract_work_dir'),"
                   "json_extract(payload,'$.item.package_path') FROM deliveries")
            args=()
            if changed_since is not None:
                query+=" WHERE status!='complete' OR updated_at>=?"; args=(changed_since,)
            for status,other_path,package_path in db.execute(query,args):
                if package_path: protected.add(Path(package_path).resolve())
                if not other_path: continue
                other=Path(other_path).resolve()
                if status=='complete':
                    material=other/'material.json'
                    if material.is_file():
                        protected.update(dependency_keys(material,'material'))
                    proof=other/PROOF
                    if proof.is_file(): protected.update(dependency_keys(proof,'proof'))
                    continue
                if other in family: protected.add(other)
                for name in ('material.json','private/candidate.json','revision.json','candidate-import.json'):
                    file=other/name
                    if file.is_file(): protected.update(dependency_keys(file,'references'))
    return protected

def resources(root, work, protected, originals, *, retire_requests=False, published=None):
    """Inventory only allowlisted disposable files; no workspace-wide deletion."""
    from preextract_cleanup import _safe_path
    from paperflow.cleanup_paths import inventory
    result=[]; retained=[]
    dependencies=Dependencies(protected)
    directories=[work/name for name in DIRECTORIES]
    if published is not None and (work/'publication').is_dir() and not overlaps(work/'publication',published):
        # A duplicate staging package must independently pass its own strict seal.
        from preextract_publish import _paperflow
        prepared=read(work/'private/prepared-publication.json')
        manifest=read(work/'publication/manifest.json')
        task={k:manifest[k] for k in ('task_id','source_id','source_sha256','revision')}
        if any(task[k]!=prepared['task'][k] for k in ('source_id','source_sha256')):
            raise ValueError('staging source identity changed')
        _paperflow(prepared['paperflow_root']).validate_package(work/'publication',task,prepared['catalog'],strict_seal=True)
        directories.append(work/'publication')
    for target in directories:
        target=_safe_path(target,root)
        if not target.is_dir(): continue
        if not dependencies.overlaps(target):
            result.append(dict(kind='directory',inventory=inventory(target,[root],originals)))
        else:
            for p in target.rglob('*'):
                if not p.is_file(): continue
                if dependencies.overlaps(p): retained.append(str(p)); continue
                p=_safe_path(p,root)
                result.append(dict(kind='file',path=str(p),sha256=sha(p),bytes=p.stat().st_size))
    for p in (work/'model').rglob('*'):
        if not p.is_file() or (p.name!='response-events.jsonl' and not(retire_requests and p.name in REQUEST_CACHES)):
            continue
        p=_safe_path(p,root)
        if dependencies.overlaps(p): retained.append(str(p)); continue
        result.append(dict(kind='file',path=str(p),sha256=sha(p),bytes=p.stat().st_size))
    return result,retained

def allowlisted(path, work):
    path=Path(path).resolve(); work=Path(work).resolve()
    return (any(path==work/n or path.is_relative_to(work/n) for n in DIRECTORIES)
            or path==work/'publication' or path.is_relative_to(work/'publication')
            or path.is_relative_to(work/'model') and path.name in REQUEST_CACHES|{'response-events.jsonl'})

def finish_cleanup(root, work, item, prepared, published, bundle, candidate, identity, stop, admit,
                   *,storage_scope=None,storage_proof=None):
    """Same locks/inventory primitives, extended to a verified source family."""
    from contextlib import ExitStack
    from filelock import FileLock
    from preextract_cleanup import _safe_path, _mark_receipt, SCHEMA
    from paperflow.cleanup_paths import delete_inventory
    import time
    from preextract_task_storage import require_recoverable,Held
    if storage_scope is None or getattr(storage_scope,'release_context',None) is None:
        return dict(status='deferred',reason='storage_explicit_local_isolation_permission_missing')
    try:require_recoverable(root)
    except Held as exc:return dict(status='deferred',reason=str(exc))
    if storage_scope is None or not storage_proof:
        return dict(status='deferred',reason='storage_exact_task_or_nas_proof_missing')
    if storage_scope.work!=work or storage_scope.source!=prepared['task']['source_sha256']:
        return dict(status='deferred',reason='storage_cleanup_item_changed')
    family={work}
    with ExitStack() as locks:
        for member in sorted(family-{work}):
            locks.enter_context(FileLock(str(member/'operation.lock'),timeout=0))
        originals=[Path(bundle['source']['file']).resolve()]
        copies=[p for member in family for p in (member/'private').glob('original.*') if p.is_file()]
        if not any(sha(p)==prepared['task']['source_sha256'] for p in copies):
            return dict(status='deferred',reason='original_copy_not_verified')
        originals+=copies
        snapshot_at=time.time()
        protected=protected_paths(root,work,bundle,candidate,family)
        # All requests, full/partial replies and revisions survive this pass.
        protected.update({work/'model',work/'revisions'})
        source=Path(bundle['source']['file']).resolve()
        replacement=next(p for p in copies if sha(p)==prepared['task']['source_sha256'])
        substitutions={}
        if source!=replacement and not source.is_relative_to(work/'private'):
            substitutions[str(source)]=dict(path=str(replacement),sha256=prepared['task']['source_sha256'],
                storage_identity=storage_scope.identity,nas_manifest_sha256=storage_proof['nas_manifest_sha256'])
        proof=prepare_reuse(work,prepared,published,bundle,
            retain_extra=[*originals,*(p for p in immutable_paths(bundle,candidate,family) if p.is_file())],
            source_substitutions=substitutions)
        protected.update(Path(p) for p in proof['retained'])
        protected.add(published)
        # Other histories retain their checkpoint material and raw replies.
        for member in family-{work}:
            material=member/'material.json'
            old=read(material)
            protected.update(Path(p).resolve() for p in old.get('file_hashes',{}))
            states=list((member/'model').rglob('request-state.json'))
            if any((s:=read(p)).get('status')!='response_complete' and not
                   (s.get('status')=='request_failed_or_uncertain' and
                    isinstance(s.get('finished_at'),(int,float)) and s['finished_at']>=s.get('started_at',float('inf')))
                   for p in states):
                protected.add(member)
        dependencies=Dependencies(protected)
        planned=[]; retained=[]
        for member in sorted(family):
            retired=member==work or (member/PROOF).is_file()
            if retired and member!=work: verify_reuse(member)
            entries, held=resources(root,member,protected,originals,retire_requests=retired,
                published=published if member==work else None)
            planned+=entries; retained+=held
        report_path=work/'cleanup-receipt.json'
        previous=read(report_path) if report_path.is_file() else {}
        if previous and (previous.get('identity')!=identity or previous.get('work_dir')!=str(work)):
            return dict(status='deferred',reason='cleanup_intent_changed')
        # Persist the completed history separately from this intent's inventory.
        # A local pass can finish and then wait for shared purpose cleanup;
        # pending must not discard its earlier history or count partial files twice.
        base_files=previous.get('removed_files',0) if previous.get('status')=='complete' else previous.get('prior_removed_files',0)
        base_bytes=previous.get('removed_bytes',0) if previous.get('status')=='complete' else previous.get('prior_removed_bytes',0)
        if previous.get('schema')==SCHEMA:
            # Recovery entries are append-only even after completion: a crash
            # before the queue projection must not replace them with an empty plan.
            old={r.get('path') or r['inventory']['path']:r for r in previous.get('resources',[])}
            for record in planned:
                key=record.get('path') or record['inventory']['path']; old.setdefault(key,record)
            planned=list(old.values())
        exact=[]
        for resource in planned:
            target=_safe_path(resource.get('path') or resource['inventory']['path'],root)
            if not storage_scope.owns_cleanup(target):
                return dict(previous or {},status='pending',reason='storage_saved_plan_outside_exact_item',
                            resources=planned,identity=identity,work_dir=str(work))
            if resource['kind']=='directory':
                # Hash/inventory work precedes the short final unlink guard.
                # Empty synchronization directories need not be removed.
                for name,details in resource['inventory']['files'].items():
                    path=_safe_path(target/name,root)
                    if not storage_scope.owns_cleanup(path):
                        return dict(status='pending',reason='storage_saved_plan_outside_exact_item',
                                    resources=planned,identity=identity,work_dir=str(work))
                    exact.append(dict(kind='file',path=str(path),**details))
            else:exact.append(resource)
        recorded={}
        for resource in exact:
            key=resource['path'];saved=recorded.get(key)
            if saved is not None:
                if (saved['sha256'],saved['bytes'])!=(resource['sha256'],resource['bytes']):
                    return dict(previous or {},status='pending',reason='cleanup_saved_file_identity_changed')
                if saved.get('recovery') and resource.get('recovery') and saved['recovery']!=resource['recovery']:
                    return dict(previous or {},status='pending',reason='cleanup_saved_recovery_identity_changed')
                if saved.get('recovery'):continue
            recorded[key]=resource
        planned=list(recorded.values())
        report=dict(schema=SCHEMA,rules_version=RULES,status='pending',identity=identity,work_dir=str(work),
            resources=planned,created_at=previous.get('created_at',time.time()),
            preserved={str(p):sha(p) for p in map(Path,proof['retained'])},
            retained_dependencies=sorted(set(retained)),family_workspaces=[str(p) for p in sorted(family)],
            prior_removed_files=base_files,prior_removed_bytes=base_bytes,
            removed_files=previous.get('removed_files',0),removed_bytes=previous.get('removed_bytes',0),
            task_storage=storage_proof)
        write_json(report_path,report)
        try:
            for resource in planned:
                if stop is not None and stop.is_set() or admit is not None and not admit():
                    raise InterruptedError('cleanup admission changed')
                target=_safe_path(resource.get('path') or resource['inventory']['path'],root)
                member=next((p for p in sorted(family,key=lambda p:len(p.parts),reverse=True) if target.is_relative_to(p)),None)
                if member is None or not allowlisted(target,member) or dependencies.overlaps(target):
                    raise ValueError('cleanup resource is retained or outside allowlist')
                # Refresh pending cross-job references before every destructive operation.
                fresh=protected_paths(root,work,bundle,candidate,family,changed_since=snapshot_at)
                if Dependencies(fresh).overlaps(target):
                    raise InterruptedError('another task now uses this cache')
                if resource['kind']=='file':
                    from paperflow.original_archive.lifecycle import locked_file,verify_stream,rename_checked,isolation_record,verify_isolation
                    if target.exists():
                        recovery=isolation_record(root,dict(task_identity=storage_scope.identity,
                            release_context=storage_scope.release_context),target,resource['sha256'],resource['bytes'])
                        if resource.get('recovery') is not None and resource['recovery']!=recovery:
                            raise ValueError('cleanup_isolation_intent_changed')
                        if Path(recovery['path']).exists():raise ValueError('cleanup_isolation_target_exists')
                        with locked_file(target,delete=True) as (stream,_permanent_delete_unused):
                            verify_stream(stream,resource['sha256'],resource['bytes'])
                            info=os.fstat(stream.fileno());resource.update(recovery=recovery,device=info.st_dev,inode=info.st_ino)
                            write_json(report_path,report)
                            with storage_scope.commit_guard(target):
                                rename_checked(stream,recovery['path'])
                    if not resource.get('recovery'):raise ValueError('cleanup_missing_without_isolation_receipt')
                    verify_isolation(resource['recovery'],resource['device'],resource['inode'])
                else:
                    raise ValueError('unknown cleanup resource kind')
                if target.exists(): raise ValueError('cleanup removal not confirmed')
            leftovers=[]
            for member in sorted(family):
                entries,_=resources(root,member,protected,originals,
                    retire_requests=member==work or (member/PROOF).is_file())
                leftovers += [entry for entry in entries if entry['kind']!='directory' or entry['inventory']['files']]
            if leftovers: raise ValueError('eligible cache remains after cleanup')
            verify_reuse(work)
            removed=[]
            for resource in planned:
                removed+=list(resource['inventory']['files'].values()) if resource['kind']=='directory' else [resource]
            report.update(status='complete',completed_at=time.time(),eligible_residual_files=0,
                removed_files=base_files,removed_bytes=base_bytes,
                isolated_files=len(removed),isolated_bytes=sum(p['bytes'] for p in removed),local_mode='same-volume-isolation/1')
        except Exception as exc:
            report.update(status='pending',error_type=type(exc).__name__,updated_at=time.time())
        write_json(report_path,report)
        _mark_receipt(work,report,identity)
        return report
