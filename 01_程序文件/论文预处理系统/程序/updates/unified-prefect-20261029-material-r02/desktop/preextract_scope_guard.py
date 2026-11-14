"""A persistent binding keeps managed papers out of legacy bypass paths."""
import json
from pathlib import Path
from paperflow.prepared_protocol import fingerprint
from preextract_material import file_sha


def binding(work):
    path=Path(work)/'prepared-scope.json'
    return json.loads(path.read_text(encoding='utf8')) if path.exists() else None


def registry_for(marker):
    from paperflow.store import Store
    from paperflow.prepared_store import PreparedRegistry
    path=Path(marker['database'])
    if not path.is_file() or not path.with_suffix('.prepared-authority.json').is_file():
        raise ValueError('prepared authority missing; inventory only')
    return PreparedRegistry(Store(path))


def require_managed(work,actor):
    marker=binding(work)
    if marker is None:
        if actor:raise ValueError('prepared binding missing')
        return None
    if actor!=marker:raise ValueError('managed material requires its scope executor')
    registry=registry_for(marker)
    registry.assert_running(marker['scope_id'],marker['generation'])
    if not any(m['task_id']==marker['task_id'] for m in registry.members(marker['scope_id'])):
        raise ValueError('prepared member missing')
    return registry


def closure_path(work,scope_id):
    return Path(work)/('prepared-delivery-closure-'+fingerprint(scope_id)[:24]+'.json')


def member_delivery_proof(registry,scope_id,member):
    try:
        retired=retired_delivery(registry,scope_id,member)
        if retired:
            return dict(task_id=member['task_id'],closure_sha256=retired['scope_proof']['closure_sha256'])
        task=registry.store.get(member['task_id'])
        tombstone=registry.cleanup_tombstone(member['task_id'])
        if tombstone:
            if not Path(tombstone['report_path']).is_file() or file_sha(tombstone['report_path'])!=tombstone['report_sha256']:
                return None
            return dict(task_id=member['task_id'],closure_sha256=tombstone['delivery_closure_sha256'])
        exclusion=registry.exclusion(scope_id,member['task_id'])
        if exclusion:
            if all(Path(p).is_file() and file_sha(p)==sha for p,sha in exclusion['files'].items()):
                return dict(task_id=member['task_id'],exclusion_sha256=fingerprint(exclusion))
            return None
        record=dict(member['record'])
        checkpoint=task.get('prepared_checkpoint') or record['prepared_checkpoint']
        work=Path(checkpoint['path']).parent
        path=closure_path(work,scope_id)
        try:
            proof=json.loads(path.read_text(encoding='utf8'))
            valid=proof['complete'] and proof['task_id']==member['task_id'] and proof['scope_id']==scope_id
            valid=valid and all(Path(p).is_file() and file_sha(p)==sha for p,sha in proof['files'].items())
            if valid:
                receipt=json.loads(Path(proof['body_receipt_path']).read_text(encoding='utf8'))
                valid=body_receipt_fingerprint(receipt)==proof['body_receipt_sha256']
        except (OSError,ValueError,KeyError,TypeError):valid=False
        return dict(task_id=member['task_id'],closure_sha256=file_sha(path)) if valid else None
    except (OSError,ValueError,KeyError,TypeError):return None


def retired_delivery(registry,scope_id,member):
    """Read the original delivery fact after its local files were retired."""
    from preextract_retirement import find_terminal,FIELD,parent_evidence
    from preextract_delivery_queue import queue_path
    task=registry.store.get(member['task_id'])
    checkpoint=task.get('prepared_checkpoint') or member['record'].get('prepared_checkpoint')
    if not checkpoint or not checkpoint.get('path'):return None
    work=Path(checkpoint['path']).parent.resolve()
    for root in work.parents:
        if not queue_path(root).is_file():continue
        job=find_terminal(root,work=work,source_hash=member['identity']['source_sha256'])
        if job is None:return None
        try:parent_evidence(job)
        except (ValueError,KeyError,TypeError):return None
        record=job['projection'][FIELD];proof=record.get('scope_proof') or {};marker=proof.get('binding') or {}
        if (marker.get('database')!=str(registry.store.path) or marker.get('scope_id')!=scope_id
                or marker.get('task_id')!=member['task_id'] or proof.get('identity')!=member['identity']
                or proof.get('context_sha256')!=member['context_sha256'] or proof.get('checkpoint')!=checkpoint
                or job['id']!=task.get('prepared_delivery_id') or not proof.get('closure_sha256')):
            return None
        return record
    return None


def retirement_scope_proof(work,receipt):
    """Compact only a locally validated closure and its frozen authority."""
    marker=binding(work)
    if not marker:return None
    registry=registry_for(marker)
    member=next(m for m in registry.members(marker['scope_id']) if m['task_id']==marker['task_id'])
    path=closure_path(work,marker['scope_id']);proof=json.loads(path.read_text(encoding='utf8'))
    if (not proof['complete'] or proof['task_id']!=marker['task_id'] or proof['scope_id']!=marker['scope_id']
            or proof['body_receipt_sha256']!=body_receipt_fingerprint(receipt)
            or not all(Path(p).is_file() and file_sha(p)==sha for p,sha in proof['files'].items())):
        raise ValueError('scope closure identity changed before retirement')
    task=registry.store.get(member['task_id'])
    checkpoint=task.get('prepared_checkpoint') or member['record']['prepared_checkpoint']
    if Path(checkpoint['path']).resolve()!=Path(work).resolve()/'material.json' or file_sha(checkpoint['path'])!=checkpoint['sha256']:
        raise ValueError('scope checkpoint changed before retirement')
    return dict(binding=marker,identity=member['identity'],context_sha256=member['context_sha256'],
        checkpoint=checkpoint,closure_sha256=file_sha(path),association_sha256=fingerprint(proof.get('association',{})))


def retirement_admission(marker):
    """A verified member may retire; stopped scopes and unknown HTTP stay held."""
    if marker is None:return lambda:True
    registry=registry_for(marker);snapshot=registry.scope_snapshot(marker['scope_id'])
    generation=snapshot['generation']
    if snapshot['intent'] not in ('running','completed'):raise ValueError('scope stopped before retirement')
    def admit():
        if registry.stop_path.exists():return False
        with registry.store.db() as db:
            scope=db.execute('SELECT intent,generation FROM prepared_scopes WHERE scope_id=?',(marker['scope_id'],)).fetchone()
            unknown=db.execute("SELECT 1 FROM prepared_http WHERE task_id=? AND state IN ('possible','unknown') LIMIT 1",(marker['task_id'],)).fetchone()
        return bool(scope and scope['intent'] in ('running','completed') and scope['generation']==generation and not unknown)
    return admit


def retirement_dependencies(root,record):
    """Frozen unqueued/stopped members remain consumers of shared material."""
    from preextract_coordinator import control_path
    database=control_path(root)
    if not database.is_file():return set(),set()
    registry=registry_for(dict(database=str(database)));protected=set();hashes=set()
    with registry.store.db() as db:scopes=[r[0] for r in db.execute('SELECT scope_id FROM prepared_scopes')]
    own=(record.get('scope_proof') or {}).get('binding') or {}
    for scope in scopes:
        for member in registry.members(scope):
            if member['task_id']==own.get('task_id') and scope==own.get('scope_id'):continue
            if member_delivery_proof(registry,scope,member):continue
            task=registry.store.get(member['task_id'])
            checkpoint=task.get('prepared_checkpoint') or member['record']['prepared_checkpoint']
            material=Path(checkpoint['path']).resolve()
            bundle=json.loads(material.read_text(encoding='utf8'))
            protected.add(material.parent)
            from preextract_cleanup import _references
            from preextract_cleanup_scope import content_hashes
            protected.update(_references(bundle))
            hashes.update(content_hashes(bundle));hashes.update(content_hashes(member['identity']))
    return protected,hashes


def scope_delivery_closure(registry,scope_id):
    proofs=[];missing=[];members=registry.members(scope_id)
    for member in members:
        proof=member_delivery_proof(registry,scope_id,member)
        if proof:proofs.append(proof)
        else:missing.append(member['task_id'])
    with registry.store.db() as c:
        unresolved=[r[0] for r in c.execute("SELECT DISTINCT h.task_id FROM prepared_http h JOIN prepared_members m ON m.task_id=h.task_id WHERE m.scope_id=? AND h.state IN ('possible','unknown')",(scope_id,))]
    missing=sorted(set(missing+unresolved))
    return dict(complete=not missing and bool(proofs),verified=len(members)-len(missing),required=len(members),
                missing=missing,evidence_sha256=fingerprint(dict(scope_id=scope_id,proofs=proofs,missing=missing)))


def cleanup_admission(work):
    marker=binding(work)
    if marker is None:return None
    registry=registry_for(marker)
    def admit():
        try:
            closure=scope_delivery_closure(registry,marker['scope_id'])
            if not closure['complete']:return False
            if not registry.cleanup_authorized(marker['scope_id'],marker['generation'],closure['evidence_sha256']):return False
            # Protect every other frozen scope, including stopped scopes.
            with registry.store.db() as c:
                scopes=[r[0] for r in c.execute('SELECT scope_id FROM prepared_scopes WHERE scope_id!=?',(marker['scope_id'],))]
            candidates=[Path(work)/'private/rendered',Path(work)/'model/response-events.jsonl']
            for scope in scopes:
                for member in registry.members(scope):
                    if registry.cleanup_tombstone(member['task_id']):continue
                    material=Path(member['record']['prepared_checkpoint']['path'])
                    bundle=json.loads(material.read_text(encoding='utf8'))
                    for path in bundle.get('file_hashes',{}):
                        path=Path(path).resolve()
                        if any(path==p.resolve() or path.is_relative_to(p.resolve()) for p in candidates):return False
            return True
        except (OSError,ValueError,KeyError,TypeError):return False
    return admit


def save_tombstone(work,report):
    marker=binding(work)
    if not marker or report.get('status')!='complete':return
    registry=registry_for(marker)
    if registry.cleanup_tombstone(marker['task_id']):return
    closure=scope_delivery_closure(registry,marker['scope_id'])
    if not closure['complete']:raise ValueError('scope closure changed during cleanup')
    registry.record_tombstone(marker['scope_id'],marker['task_id'],dict(
        report_path=str(Path(work)/'cleanup-receipt.json'),report_sha256=file_sha(Path(work)/'cleanup-receipt.json'),
        closure_sha256=closure['evidence_sha256'],delivery_closure_sha256=file_sha(closure_path(work,marker['scope_id']))))


def protected_scope_paths(work):
    """Frozen members retain their inputs even without a legacy delivery row."""
    work=Path(work).resolve();marker=binding(work)
    if marker is None:return set()
    registry=registry_for(marker);protected=set()
    with registry.store.db() as c:
        scopes=[r[0] for r in c.execute('SELECT scope_id FROM prepared_scopes')]
    for scope in scopes:
        for member in registry.members(scope):
            if registry.cleanup_tombstone(member['task_id']):continue
            if member['task_id']==marker['task_id']:continue
            task=registry.store.get(member['task_id'])
            checkpoint=task.get('prepared_checkpoint') or member['record']['prepared_checkpoint']
            material=Path(checkpoint['path']).resolve()
            bundle=json.loads(material.read_text(encoding='utf8'))
            protected.add(material.parent)
            protected.update(Path(p).resolve() for p in bundle.get('file_hashes',{}))
    proof=closure_path(work,marker['scope_id'])
    if proof.is_file():
        protected.update(Path(p).resolve() for p in json.loads(proof.read_text(encoding='utf8'))['files'])
    return protected


def body_receipt_fingerprint(receipt):
    return fingerprint({k:v for k,v in receipt.items() if k not in ('cleanup','cleanup_receipt','cleanup_result')})


def save_delivery_closure(work,item,prepared,receipt,association,marker):
    from preextract_material import write_json
    work=Path(work)
    paths=[work/'material.json',work/'private/prepared-publication.json',work/'delivery-binding.json',
           Path(item['package_path'])/'manifest.json']
    for key in ('receipt_path','plan_path'):
        if association.get(key):paths.append(Path(association[key]))
    for row in association.get('required_inventory',{}).get('files',[]):
        mapping=row.get('purpose_mapping') or {}
        paths.append(Path(row['original_path']))
        paths += [Path(mapping[k]) for k in ('review_path','evidence_path','archive_path','request_path','request_state_path','response_path') if mapping.get(k)]
    proof=dict(schema='prepared-delivery-closure/1',complete=True,scope_id=marker['scope_id'],task_id=marker['task_id'],
        files={str(p):file_sha(p) for p in paths},body_receipt_path=str(work/'delivery-receipt.json'),
        body_receipt_sha256=body_receipt_fingerprint(receipt),association=association)
    write_json(closure_path(work,marker['scope_id']),proof)
