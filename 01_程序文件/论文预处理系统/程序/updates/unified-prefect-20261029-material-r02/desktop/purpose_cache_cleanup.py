"""Purpose evidence retirement after exact paper/attachment delivery admission."""
from __future__ import annotations

from contextlib import contextmanager, closing, ExitStack
from contextvars import ContextVar
import functools
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import threading
import time

from filelock import FileLock, Timeout
from preextract_material import write_json
from preextract_cleanup_scope import Dependencies, protected_paths, sha

RULES = 'purpose-cache-cleanup/1'
_lease = ContextVar('purpose_cache_lease', default=None)


def read(path):
    return json.loads(Path(path).read_text(encoding='utf8'))


def _generation(cache):
    result = []
    for p in (cache / 'reviews', cache / 'generation.json'):
        s = p.stat() if p.exists() else None
        result.append((s.st_mtime_ns, s.st_size) if s else None)
    return tuple(result)


def _changed(cache):
    write_json(cache / 'generation.json', {'at_ns': time.time_ns()})


@contextmanager
def review_scope(cache, folder):
    """A short catalog lock registers readers; network calls stay concurrent."""
    import psutil
    cache, folder = Path(cache).resolve(), Path(folder).resolve()
    folder.mkdir(parents=True, exist_ok=True)
    leases = cache / 'leases'; leases.mkdir(exist_ok=True)
    path = leases / (folder.name + f'-{os.getpid()}-{threading.get_ident()}.json')
    value = {'review': str(folder / 'review.json'), 'model_dir': str(folder / 'model'),
             'pid': os.getpid(), 'create_time': psutil.Process().create_time()}
    with FileLock(str(folder / 'purpose-operation.lock')):
        with FileLock(str(cache / 'lifecycle.lock')):
            write_json(path, value); _changed(cache)
        token = _lease.set((cache, path, value))
        try:
            yield
        finally:
            _lease.reset(token)
            with FileLock(str(cache / 'lifecycle.lock')):
                path.unlink(missing_ok=True); _changed(cache)


def bind_model(model_dir):
    current = _lease.get()
    if current:
        cache, path, value = current
        with FileLock(str(cache / 'lifecycle.lock')):
            value['model_dir'] = str(Path(model_dir).resolve())
            write_json(path, value); _changed(cache)


@functools.lru_cache(maxsize=2)
def _catalog(root, stamp):
    cache = Path(root); rows = []; errors = []
    for p in (cache / 'reviews').glob('*/review.json'):
        try:
            v = read(p)
            rows.append({'path': str(p.resolve()), 'source_sha256': v['source_sha256'],
                         'model_dir': str(Path(v['model_dir']).resolve())})
        except (OSError, KeyError, ValueError, TypeError):
            errors.append(str(p))
    return rows, errors


def _role_refs(value):
    if isinstance(value, dict):
        p = value.get('purpose_review_receipt')
        if p:
            yield str(p), value.get('evidence_sha256')
        for child in value.values():
            yield from _role_refs(child)
    elif isinstance(value, list):
        for child in value:
            yield from _role_refs(child)


@functools.lru_cache(maxsize=4096)
def _manifest(path, stamp):
    # Provenance queries need source identity/roles, not retained full model
    # results. Keep duplicate hashes and archive identities as separate rows.
    value = read(path)
    keys = ('source_hash', 'preextract_work_dir', 'source_path', 'prepared_checkpoint',
            'intake_role', 'path', 'source', 'id', 'name', 'input_source_path',
            'archive_member_path', 'archive_source_hash')
    items = [{k: row[k] for k in keys if k in row} for row in value.get('items', [])]
    by_source = {}
    for row in items:
        by_source.setdefault(row.get('source_hash'), []).append(row)
    return {'items': items, 'items_by_source': by_source,
            'inputs': value.get('inputs', []), 'discovered_inputs': value.get('discovered_inputs', [])}


def _documents(root, work):
    for parent in work.parents:
        if parent.parent.name != 'runs' or not parent.is_relative_to(root):
            continue
        path = parent / 'manifest.json'
        if path.is_file():
            st = path.stat()
            yield _manifest(str(path), (st.st_mtime_ns, st.st_size, st.st_ino))


def _lexical(path):
    value = Path(path)
    if not value.is_absolute():
        raise ValueError('source provenance is not absolute')
    return os.path.normcase(os.path.normpath(str(value)))


def _body_role(root, work, item):
    # Queue adoption assigns a NEW item id. Exact workspace + source identifies
    # the original row; its output item.path is never an original input path.
    roles = [item.get('intake_role') or item.get('material_role') or {}]
    for doc in _documents(root, work):
        for old in doc['items_by_source'].get(item.get('source_hash'), []):
            if (old.get('source_hash') == item.get('source_hash')
                    and old.get('preextract_work_dir')
                    and Path(old['preextract_work_dir']).resolve() == work):
                roles.append(old.get('intake_role') or {})
    return roles


def _raw_records(root, work, item, bundle):
    from archive_identity import record_key
    from preextract_pipeline import _revision
    expected = bundle['source']['source_sha256']; source = bundle['source']['file']
    if item.get('source_hash') != expected:
        raise ValueError('body source identity changed')
    family = {}; current = work
    for _ in range(64):
        material = current / 'material.json'
        family[str(current)] = (material.resolve(), sha(material))
        meta = _revision(source, current)
        if not meta:
            break
        current = Path(meta['parent']).resolve()
        if current in [Path(v) for v in family] or not current.is_relative_to(root):
            raise ValueError('body provenance lineage invalid')
    else:
        raise ValueError('body provenance lineage too deep')
    records = []
    for doc in _documents(root, work):
        raw = doc.get('inputs', []) + doc.get('discovered_inputs', [])
        matched = []
        for old in doc['items_by_source'].get(expected, []):
            w = old.get('preextract_work_dir')
            if not w or str(Path(w).resolve()) not in family or old.get('source_hash') != expected:
                continue
            if not old.get('source_path') or _lexical(old['source_path']) != _lexical(source):
                continue
            checkpoint = old.get('prepared_checkpoint') or {}
            bound_path, digest = family[str(Path(w).resolve())]
            if checkpoint and (Path(checkpoint['path']).resolve() != bound_path or checkpoint.get('sha256') != digest):
                raise ValueError('original material checkpoint changed')
            matched.append(record_key(old))
        for record in raw:
            if not record.get('path') or _lexical(record['path']) != _lexical(source):
                continue
            checkpoint = record.get('prepared_checkpoint') or {}
            direct = (checkpoint and Path(checkpoint['path']).resolve() == work / 'material.json'
                      and checkpoint.get('sha256') == family[str(work)][1])
            if direct or record_key(record) in matched:
                records.append(record)
    return records


def _signature(record, source_sha, config, *, legacy=False):
    from purpose_review import _identity
    if sha(record['path']) != source_sha:
        raise ValueError('purpose original source changed')
    context = _identity(record, config, legacy=legacy)[-1]
    return hashlib.sha256((source_sha + '\0' + context).encode()).hexdigest()


def _material_rows(rows):
    for row in rows:
        yield row
        yield from _material_rows(row.get('members', []))


def _select_reviews(root, work, item, bundle, plan, config, cache, catalog):
    from archive_identity import record_key
    known = {v['source_sha256'] for v in catalog}
    selected = {}
    def scope(records, source_sha, roles):
        refs = list(_role_refs(roles))
        if source_sha not in known and not refs:
            return
        from purpose_review import _contract, _legacy_usable
        paired = {}
        for legacy in (False, True):
            signatures = {_signature(r, source_sha, config, legacy=legacy) for r in records}
            if len(signatures) != 1:
                raise ValueError('purpose source context unknown or ambiguous')
            signature = signatures.pop()
            paired[cache / 'reviews' / signature / 'review.json'] = (_contract(config,legacy=legacy),legacy)
        referenced = set()
        for ref, evidence_sha in refs:
            path = _review_path(ref, cache, root)
            if evidence_sha != source_sha or path not in paired:
                raise ValueError('purpose receipt belongs to another source context')
            if not path.is_file():
                raise ValueError('purpose exact receipt missing')
            referenced.add(path)
        for path in referenced or paired.keys():
            if not path.is_file():continue
            contract, legacy = paired[path]
            proof = read(path)
            if any(r.get('_prepared_purpose_context') and
                   proof.get('source_context_sha256') != r['_prepared_purpose_context'] for r in records):
                raise ValueError('purpose prepared source context changed')
            if proof.get('contract') != contract or legacy and not _legacy_usable(proof):
                raise ValueError('purpose contract does not match its source context')
            selected[path] = source_sha
    body_sha = bundle['source']['source_sha256']
    body_refs = [_body_role(root, work, item), bundle.get('source', {})]
    if body_sha in known or list(_role_refs(body_refs)):
        records = _raw_records(root, work, item, bundle)
        body_refs += [r.get('_admission_role') or {} for r in records]
        scope(records, body_sha, body_refs)
    material = list(_material_rows(bundle.get('attachments', [])))
    for row in (plan or {}).get('files', []):
        refs = list(_role_refs(row)); source_sha = row['sha256']
        matches = [v for v in material if all(v.get(k) == row.get(k) for k in ('sha256', 'kind', 'format'))
                   and v.get('original_path') and _lexical(v['original_path']) == _lexical(row['original_path'])]
        if source_sha not in known and not refs and not any(list(_role_refs(v)) for v in matches):
            continue
        if len(matches) != 1:
            raise ValueError('purpose attachment row context unknown or ambiguous')
        original = matches[0]; refs += list(_role_refs(original))
        path = original.get('source_path')
        if not path or not original.get('source_key'):
            raise ValueError('purpose attachment original provenance missing')
        record = dict(path=path, **{k: original[k] for k in
            ('name', 'input_source_path', 'archive_member_path', 'archive_source_hash') if original.get(k)})
        if record_key(record) != original['source_key']:
            raise ValueError('purpose attachment native source key changed')
        if not record.get('name'):
            # Existing plain attachments persist their exact effective name in
            # the native evidence receipt. Never infer it from a private copy.
            names = set()
            for ref, _ in refs:
                evidence = read(_review_path(ref, cache, root).parent / 'evidence/evidence.json')
                if evidence.get('archive_member_path', '') != record.get('archive_member_path', ''):
                    raise ValueError('purpose attachment member provenance changed')
                names.add(evidence.get('source_name'))
            if len(names) != 1 or not next(iter(names)):
                raise ValueError('purpose attachment effective name unknown')
            record['name'] = names.pop()
            if not record.get('archive_member_path') and record['name'] != Path(path).name:
                raise ValueError('purpose attachment original effective name unproven')
        scope([record], source_sha, [row, original])
    return selected


from asset_evidence import verify_associated_receipt

def _queue_rows(root, *, pending_only=False):
    from preextract_delivery_queue import queue_path
    path = queue_path(root)
    if not path.is_file():
        return []
    with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=5)) as db:
        return [(status, json.loads(payload)) for status, payload in db.execute("SELECT status,payload FROM deliveries" + (" WHERE status!='complete'" if pending_only else ""))]


def _queue_token(root):
    from preextract_delivery_queue import queue_path
    path = queue_path(root)
    if not path.is_file():
        return None
    # Upload progress and cleanup receipts alter projection/updated_at, but do
    # not change a consumer. A coherent SQLite read covers every actual intent
    # and its completed/non-completed boundary; no deletion permit is cached.
    digest = hashlib.sha256()
    with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=5)) as db:
        for row in db.execute("SELECT id,status='complete',payload FROM deliveries ORDER BY id"):
            digest.update(json.dumps(row, ensure_ascii=False, separators=(',', ':')).encode('utf8'))
            digest.update(b'\n')
    return digest.hexdigest()


@functools.lru_cache(maxsize=20000)
def _claims(path, stamp):
    v = read(path)
    rows = v.get('files', []) if path.endswith('associated-upload-plan.json') else list(_material_rows(v.get('attachments', [])))
    return ({r.get('sha256') for r in rows}, {str(Path(p).resolve()) for p, _ in _role_refs(v)})


def _consumer_guard(root, work, selected, cache, config, catalog, locks, locked):
    from associated_delivery import load_plan
    from preextract_cleanup_scope import verify_reuse
    from paperflow.private_files import resolve_private_path
    selected_paths = {str(p) for p in selected}; selected_hashes = set(selected.values())
    for status, value in _queue_rows(root):
        item = value['item']; other = Path(item['preextract_work_dir']).resolve()
        if not resolve_private_path(other).is_relative_to(resolve_private_path(root)):
            raise ValueError('unowned consumer workspace')
        direct = {str(Path(p).resolve()) for p, _ in _role_refs(value)}
        hashes = {item.get('source_hash')}
        bundle_path = other / 'material.json'; plan_path = other / 'private/associated-upload-plan.json'
        for path in (bundle_path, plan_path):
            if path.is_file():
                st = path.stat(); more_hashes, more_refs = _claims(str(path), (st.st_mtime_ns, st.st_size, st.st_ino))
                hashes.update(more_hashes); direct.update(more_refs)
        if not (direct & selected_paths or hashes & selected_hashes):
            continue
        material = read(bundle_path) if bundle_path.is_file() else {}
        plan = read(plan_path) if plan_path.is_file() else None
        if not material:
            raise ValueError('purpose consumer source context unproven')
        actual = _select_reviews(root, other, item, material, plan, config, cache, catalog)
        if not set(actual) & set(selected):
            continue
        if status != 'complete':
            raise ValueError('purpose evidence still used by unfinished delivery')
        if other not in locked and other != work:
            locks.enter_context(FileLock(str(other / 'operation.lock'), timeout=0)); locked.add(other)
        proof = verify_reuse(other)
        if (item.get('status') != 'pass' or not item.get('automatic_approval')
                or item.get('source_hash') != proof['identity']['task']['source_sha256']
                or Path(item['package_path']).resolve() != Path(proof['package_path']).resolve()
                or value.get('manifest_sha256') != proof['identity']['manifest_sha256']
                or value.get('task') != proof['identity']['task']):
            raise ValueError('purpose consumer delivery identity not verified')
        prepared = read(other / 'private/prepared-publication.json')
        body = dict(read(other / 'delivery-receipt.json'), service=read(other / 'delivery-binding.json')['service'])
        verify_associated_receipt(prepared, other, body, load_plan(prepared, other))


def _pending_refs(root):
    # Also return source hashes for unknown adopted rows. A missing exact pointer
    # never converts a potentially shared consumer into permission to delete.
    result = set()
    for status, value in _queue_rows(root, pending_only=True):
        item = value['item']; work = Path(item['preextract_work_dir']).resolve()
        refs = {str(Path(p).resolve()) for p, _ in _role_refs([value, _body_role(root, work, item)])}
        for p in (work / 'material.json', work / 'private/associated-upload-plan.json'):
            if p.is_file():
                st = p.stat()
                _, file_refs = _claims(str(p), (st.st_mtime_ns, st.st_size, st.st_ino))
                refs.update(file_refs)
        result.update(refs)
        if not refs and item.get('source_hash'):
            result.add('unknown-sha:' + item['source_hash'])
    return result


def _review_path(value, cache, root):
    from preextract_cleanup import _safe_path
    # Historical C references resolve through the verified migration junction;
    # deletion itself always starts at the E physical root and retains link checks.
    path = _safe_path(Path(value).resolve(), root)
    if (path.name != 'review.json' or path.parent.parent != cache / 'reviews'
            or len(path.parent.name) != 64 or any(c not in '0123456789abcdef' for c in path.parent.name)):
        raise ValueError('purpose receipt outside owned cache')
    return path


def _verified_review(path, expected_sha, cache, root, config):
    from preextract_cleanup import _safe_path
    from purpose_review import POLICY, _contract, _known_contracts, _legacy_usable
    v = read(path); role = v['role']
    if (v.get('policy') != POLICY or v.get('contract') not in _known_contracts(config)
            or v.get('contract') != _contract(config) and not _legacy_usable(v) or v['source_sha256'] != expected_sha
            or role.get('evidence_sha256') != expected_sha or not role.get('confirmed')
            or role.get('action') not in {'review', 'associated'}):
        raise ValueError('purpose review ownership or final decision not confirmed')
    model = _safe_path(Path(v['model_dir']).resolve(), root)
    if model.name != 'model' or model.parent.parent != cache / 'reviews':
        raise ValueError('purpose model outside owned cache')
    state = read(model / 'request-state.json'); answer = model / 'response.txt'
    if state.get('status') != 'response_complete' or not answer.is_file() or sha(answer) != state.get('response_sha256'):
        raise ValueError('purpose answer incomplete or changed')
    retained = [path, model / 'request-state.json', answer]
    retained += [p for p in (path.parent / 'evidence').glob('*.json') if p.is_file()]
    return v, model, {str(p): sha(p) for p in retained}


def _admission_valid(path, root=None):
    try:
        v = read(path.parent / 'purpose-cleanup-receipt.json')
        valid = (v.get('schema') == RULES and v.get('delivery_verified') is True
                 and v.get('review') == str(path)
                 and all(Path(p).is_file() and sha(p) == h for p, h in v['retained'].items()))
        if not valid or root is None:
            return valid
        from preextract_cleanup_scope import verify_reuse
        from associated_delivery import load_plan
        work = Path(v['work_dir']).resolve()
        if not work.is_relative_to(root):
            return False
        proof = verify_reuse(work)
        if (proof['identity'] != {'task': v['task'], 'manifest_sha256': v['manifest_sha256']}
                or proof['service'] != v['service']):
            return False
        prepared = read(work / 'private/prepared-publication.json')
        body = dict(read(work / 'delivery-receipt.json'), service=read(work / 'delivery-binding.json')['service'])
        verify_associated_receipt(prepared, work, body, load_plan(prepared, work))
        return True
    except (OSError, KeyError, ValueError, TypeError):
        return False


def _guard_stable(root, work, scope, cache, config, catalog, locks, locked):
    for _ in range(3):
        token = _queue_token(root)
        _consumer_guard(root, work, scope, cache, config, catalog, locks, locked)
        if token == _queue_token(root):
            return token
    raise InterruptedError('purpose consumer set keeps changing')


def cleanup_purpose_cache(root, work, item, prepared, bundle, candidate, *, stop=None, admit=None):
    """Called only behind the existing all-specialty paper readback gate."""
    from associated_delivery import load_plan
    from preextract_cleanup import _safe_path
    from purpose_review import production_settings
    config = production_settings(root)
    if not config.get('enabled'):
        return {'status': 'not_applicable', 'schema': RULES}
    root, work = Path(root).resolve(), Path(work).resolve()
    cache = Path(config['cache_root']).resolve()
    if cache != root / 'private/purpose-review':
        return {'status': 'deferred', 'reason': 'purpose_cache_not_migrated', 'schema': RULES}
    if not cache.is_dir():
        return {'status': 'not_applicable', 'schema': RULES}
    stop = stop or threading.Event()
    try:
        _safe_path(cache, root)
        body = dict(read(work / 'delivery-receipt.json'), service=read(work / 'delivery-binding.json')['service'])
        plan = load_plan(dict(prepared), work)
        verify_associated_receipt(prepared, work, body, plan)
        stamp = _generation(cache); catalog, errors = _catalog(str(cache), stamp)
        if errors:
            raise ValueError('purpose catalog unreadable')
        selected = _select_reviews(root, work, item, bundle, plan, config, cache, catalog)
        if not selected:
            return {'status': 'not_applicable', 'schema': RULES}
        snapshot_at = time.time()
        protected = protected_paths(root, work, bundle, candidate, {work})
        dependencies = Dependencies(protected)
        records = []; removed_files = removed_bytes = 0; held = []
        with FileLock(str(cache / 'lifecycle.lock'), timeout=0), ExitStack() as consumer_locks:
            if _generation(cache) != stamp:
                return {'status': 'deferred', 'reason': 'purpose_catalog_changed', 'schema': RULES}
            locked = {work}
            models_used = {str(Path(read(p)['model_dir']).resolve()) for p in selected}
            consumer_scope = {Path(v['path']): v['source_sha256'] for v in catalog if v['model_dir'] in models_used}
            consumer_token = _guard_stable(root, work, consumer_scope, cache, config, catalog, consumer_locks, locked)
            pending = _pending_refs(root)
            leases = [read(p) for p in (cache / 'leases').glob('*.json')]
            models = {}
            for path, source_sha in selected.items():
                _, model, retained = _verified_review(path, source_sha, cache, root, config)
                if str(path) in pending or 'unknown-sha:' + source_sha in pending or any(str(path) == v['review'] or str(model) == v['model_dir'] for v in leases):
                    raise ValueError('purpose evidence still in use')
                proof = {'schema': RULES, 'review': str(path), 'source_sha256': source_sha,
                         'delivery_verified': True, 'task': prepared['task'],
                         'manifest_sha256': prepared['validation']['manifest_sha256'],
                         'work_dir': str(work), 'service': body['service'], 'retained': retained}
                old_path = path.parent / 'purpose-cleanup-receipt.json'
                old = read(old_path) if old_path.exists() else {}
                if old and (old.get('review') != str(path) or old.get('retained') != retained):
                    raise ValueError('retained purpose proof changed')
                proof.update(status='pending', resources=old.get('resources', []) if old.get('status') != 'complete' else [])
                write_json(old_path, proof); records.append((path, model, proof)); models[path] = model
            for path, model, proof in records:
                owners = [Path(r['path']) for r in catalog if r['model_dir'] == str(model)]
                shared_ready = all(_admission_valid(p, root) and str(p) not in pending for p in owners)
                if any(str(model) == v['model_dir'] for v in leases):
                    shared_ready = False
                targets = [p for p in (path.parent / 'evidence').rglob('*') if p.is_file() and
                           (p.suffix.lower() in {'.png', '.jpg', '.jpeg', '.webp', '.bmp', '.tif', '.tiff'}
                            or 'converted' in p.relative_to(path.parent / 'evidence').parts)]
                if shared_ready:
                    targets += [model / n for n in ('request.json', 'planned-request.json', 'response-events.jsonl') if (model / n).is_file()]
                else:
                    held.append(str(model))
                inventory = {r['path']: r for r in proof['resources']}
                for p in targets:
                    p = _safe_path(p, root)
                    if dependencies.overlaps(p):
                        held.append(str(p)); continue
                    inventory.setdefault(str(p), {'path': str(p), 'sha256': sha(p), 'bytes': p.stat().st_size})
                proof['resources'] = list(inventory.values()); write_json(path.parent / 'purpose-cleanup-receipt.json', proof)
                for resource in proof['resources']:
                    if stop.is_set() or admit is not None and not admit():
                        raise InterruptedError('purpose cleanup admission changed')
                    p = _safe_path(resource['path'], root)
                    if not (p.is_relative_to(path.parent / 'evidence') or shared_ready and p.parent == model and
                            p.name in {'request.json', 'planned-request.json', 'response-events.jsonl'}):
                        raise ValueError('purpose cleanup outside allowlist')
                    token = _queue_token(root)
                    if token != consumer_token:
                        consumer_token = _guard_stable(root, work, consumer_scope, cache, config, catalog, consumer_locks, locked)
                    pending_now = _pending_refs(root)
                    if any(str(r) in pending_now or 'unknown-sha:' + h in pending_now for r, h in selected.items()):
                        raise InterruptedError('another delivery now uses this purpose review')
                    fresh = protected_paths(root, work, bundle, candidate, {work}, changed_since=snapshot_at)
                    if Dependencies(fresh).overlaps(p) or dependencies.overlaps(p):
                        raise InterruptedError('purpose cache became a dependency')
                    if p.exists():
                        if sha(p) != resource['sha256'] or p.stat().st_size != resource['bytes']:
                            raise ValueError('purpose cache changed after intent')
                        p.unlink(); removed_files += 1; removed_bytes += resource['bytes']
                if not _admission_valid(path):
                    raise ValueError('retained purpose proof changed during cleanup')
                proof.update(status='complete' if shared_ready else 'pending', checked_at=time.time(),
                             removed_files=removed_files, removed_bytes=removed_bytes)
                write_json(path.parent / 'purpose-cleanup-receipt.json', proof)
        return {'schema': RULES, 'status': 'complete' if all(p[2]['status'] == 'complete' for p in records) else 'deferred',
                'reason': 'shared_model_owner_not_closed' if any(p[2]['status'] != 'complete' for p in records) else '',
                'reviews': len(records), 'removed_files': removed_files, 'removed_bytes': removed_bytes,
                'retained_dependencies': held, 'eligible_residual_files': 0 if not held else None}
    except Timeout:
        return {'schema': RULES, 'status': 'deferred', 'reason': 'purpose_cache_busy'}
    except (OSError, ValueError, KeyError, TypeError, InterruptedError) as exc:
        return {'schema': RULES, 'status': 'deferred', 'reason': str(exc), 'error_type': type(exc).__name__}


def batch_scope_stamp(root, payloads):
    """A complete batch marker is valid only while the physical scope stays so."""
    from purpose_review import production_settings
    config = production_settings(root)
    if not config.get('enabled'):
        return 'disabled'
    cache = Path(config['cache_root']).resolve(); root = Path(root).resolve()
    catalog, errors = _catalog(str(cache), _generation(cache))
    result = []
    for payload in payloads:
        item = payload['item']; work = Path(item['preextract_work_dir']).resolve()
        try:
            if errors:
                raise ValueError('unreadable catalog')
            bundle = read(work / 'material.json')
            plan_path = work / 'private/associated-upload-plan.json'
            plan = read(plan_path) if plan_path.is_file() else None
            selected = _select_reviews(root, work, item, bundle, plan, config, cache, catalog)
            for receipt, source_sha in sorted(selected.items()):
                saved = read(receipt); model = Path(saved['model_dir']).resolve()
                paths = [p for p in receipt.parent.rglob('*') if p.is_file() and
                         p.name != 'purpose-cleanup-receipt.json' and not p.name.endswith('.lock')]
                paths += [p for p in model.glob('*') if p.is_file() and not p.name.endswith('.lock')]
                result.extend((str(p), p.stat().st_size, p.stat().st_mtime_ns) for p in sorted(set(paths)))
        except (OSError, ValueError, KeyError, TypeError):
            # Unknown provenance must be retried, never cached as batch complete.
            result.append(('unresolved', str(work), time.time_ns()))
    return hashlib.sha256(json.dumps(result, sort_keys=True).encode()).hexdigest()
