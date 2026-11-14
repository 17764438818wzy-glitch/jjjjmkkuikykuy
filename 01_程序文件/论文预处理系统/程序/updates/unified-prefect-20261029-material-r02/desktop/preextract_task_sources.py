"""Read existing native source provenance before entering the queue lock.

This is an adapter over saved native run/manifests, not an extractor, converter,
scheduler or admission controller. Missing provenance never broadens WEB input.
"""
from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path, PurePosixPath, PureWindowsPath


def _digest(path):
    value = hashlib.sha256()
    with Path(path).open('rb') as source:
        for block in iter(lambda: source.read(1024 * 1024), b''):
            value.update(block)
    return value.hexdigest()


def _sha(value):
    if not isinstance(value, str) or re.fullmatch(r'[0-9a-f]{64}', value) is None:
        raise ValueError('exact source SHA256 required')
    return value


def _path(value):
    path = Path(value)
    if not path.is_absolute():
        raise ValueError('absolute native evidence path required')
    return path.resolve(strict=True)


def _pointer(value, pointer):
    if not isinstance(pointer, str) or not pointer.startswith('/'):
        raise ValueError('exact native JSON record pointer required')
    for part in pointer[1:].split('/'):
        part = part.replace('~1', '/').replace('~0', '~')
        if isinstance(value, list):
            if not part.isdecimal():raise ValueError('invalid native list pointer')
            value = value[int(part)]
        elif isinstance(value, dict):
            value = value[part]
        else:
            raise ValueError('native evidence pointer is not a record')
    if not isinstance(value, dict):raise ValueError('native evidence record is not an object')
    return value


def validate_source_evidence(root, *, batch_id, run_id, frozen_sources, source_hashes,
                             source_evidence=(), binding_token=None):
    """Return immutable lineage facts; this function takes no G or Q lock."""
    root = Path(root).resolve()
    frozen = set(frozen_sources)
    sources = set(source_hashes)
    if isinstance(source_evidence, (str, bytes, dict)):
        raise ValueError('source evidence must be a collection of native records')
    evidence = list(source_evidence)
    if not evidence and binding_token is None:
        if not sources <= frozen:raise ValueError('derived source has no native provenance')
        return [], None
    native_path = root / 'private/native-batch-control' / (hashlib.sha256(batch_id.encode()).hexdigest() + '.json')
    native_bytes = native_path.read_bytes()
    native = json.loads(native_bytes)
    if (native.get('schema') != 'native-batch-control/1' or native.get('batch_id') != batch_id
            or set(native.get('source_hashes', [])) != frozen):
        raise ValueError('native WEB input identity differs from delivery registration')
    native_runs=native.get('run_bindings', [])
    if not isinstance(native_runs,list) or any(not isinstance(row,dict) or not isinstance(row.get('run_id'),str) for row in native_runs):
        raise ValueError('native run bindings are invalid')
    runs = {row['run_id']: row for row in native_runs}
    if len(runs)!=len(native_runs):raise ValueError('native run binding identity is ambiguous')
    binding = runs.get(run_id)
    if binding is None or set(binding.get('source_hashes', [])) != sources:
        raise ValueError('native run must first persist its exact pending source subset')
    pending = binding.get('delivery_binding') or {}
    if binding_token is not None and (not isinstance(binding_token, str) or not binding_token
            or pending.get('token') != binding_token or pending.get('state') not in {'pending', 'ready'}):
        raise ValueError('native delivery binding token changed')
    allowed_producers = set()
    current = run_id
    while current:
        if current in allowed_producers or current not in runs:
            raise ValueError('native run parent chain is missing or cyclic')
        allowed_producers.add(current)
        current = runs[current].get('parent_run_id')
    result, seen, checked = [], set(), {}
    for proof in evidence:
        if not isinstance(proof, dict):raise ValueError('native source proof must be an object')
        source_hash, input_hash = _sha(proof['source_sha256']), _sha(proof['input_source_sha256'])
        if source_hash in frozen or source_hash in seen or input_hash not in frozen:
            raise ValueError('derived proof must bind one new run source to a frozen WEB input')
        producer = proof['producer_run_id']
        if producer not in allowed_producers:
            raise ValueError('source evidence is outside the actual native parent chain')
        producer_binding = runs[producer]
        store = _path(producer_binding['store_root'])
        if not store.is_relative_to(root):raise ValueError('native evidence store is outside data root')
        runs_path = (store / 'runs').resolve(strict=True)
        run_path = (runs_path / producer).resolve(strict=True)
        if not runs_path.is_relative_to(store) or not run_path.is_relative_to(runs_path) or not run_path.is_relative_to(root):
            raise ValueError('native evidence run resolves outside its real store')
        path, original = _path(proof['source_path']), _path(proof['input_source_path'])
        parent_hash = _sha(proof.get('parent_source_sha256', input_hash))
        parent_path = _path(proof.get('parent_source_path', proof['input_source_path']))
        if parent_hash != input_hash:
            parent = checked.get(parent_hash)
            if (not parent or parent['input_source_sha256'] != input_hash
                    or parent['input_source_path'] != str(original) or parent['source_path'] != str(parent_path)):
                raise ValueError('derived parent needs its validated native relation first')
        elif parent_path != original:
            raise ValueError('native root input path changed')
        manifest = _path(proof['manifest_path'])
        if not path.is_relative_to(run_path) or not manifest.is_relative_to(run_path):
            raise ValueError('derived file/evidence must remain in its actual native run')
        raw = manifest.read_bytes()
        evidence_hash = hashlib.sha256(raw).hexdigest()
        if evidence_hash != _sha(proof['manifest_sha256']):raise ValueError('native evidence changed')
        value = json.loads(raw)
        pointer = proof['record_pointer']
        record = _pointer(value, pointer)
        relation = proof['relation']
        if relation == 'archive_member':
            if parent_hash != input_hash:
                raise ValueError('archive adapter uses the existing root/member native relation')
            if manifest != (run_path / 'manifest.json') or value.get('run_id') != producer:
                raise ValueError('archive member requires its real native run manifest')
            parts = pointer[1:].split('/')
            if len(parts) != 2 or parts[0] not in {'inputs', 'discovered_inputs', 'items'}:
                raise ValueError('archive provenance must select an existing native source record')
            member = record.get('archive_member_path')
            if not isinstance(member, str) or not member or '\\' in member:
                raise ValueError('exact original archive member path required')
            member_path = PurePosixPath(member)
            if member_path.is_absolute() or PureWindowsPath(member).drive or PureWindowsPath(member).root or '..' in member_path.parts:
                raise ValueError('archive member path is invalid')
            if (_path(record.get('path') or record.get('source_path')) != path
                    or _path(record['input_source_path']) != original
                    or record.get('archive_source_hash') != input_hash):
                raise ValueError('native archive source relation differs from actual files')
            detail = dict(archive_member_path=member)
        elif relation in {'converted', 'normalized'}:
            if record.get('schema_version') != 'source-education/1.0':
                raise ValueError('source lineage requires the existing native source_education schema')
            parts = pointer[1:].split('/')
            if manifest == (run_path / 'manifest.json') and value.get('run_id') == producer:
                if len(parts) != 3 or parts[0] not in {'items','active_items'} or parts[2] != 'source_education':
                    raise ValueError('conversion requires its native item source_education record')
                item = _pointer(value,'/'+'/'.join(parts[:2]))
                if (_path(item['source_path']) != parent_path or item.get('source_hash') != parent_hash):
                    raise ValueError('conversion item differs from the actual parent source')
            elif manifest.name == 'material.json' and value.get('version') == 'preextract-material/1.0' and pointer == '/cover_facts':
                material_source=value.get('source') or {}
                hashes=value.get('file_hashes') or {}
                if (material_source.get('source_sha256') != parent_hash or _path(material_source['file']) != parent_path
                        or not any(_path(file)==path and digest==source_hash for file,digest in hashes.items())):
                    raise ValueError('material source/hashed normalized file differs from native conversion')
            else:
                raise ValueError('conversion proof is not a known native manifest/material source record')
            derivation = record.get('derivation') or {}
            if (record.get('source_relation') != relation or record.get('original_source_sha256') != parent_hash
                    or record.get('evidence_source_sha256') != source_hash
                    or derivation.get('input_sha256') != parent_hash or derivation.get('output_sha256') != source_hash
                    or not derivation.get('method') or not derivation.get('record_id')):
                raise ValueError('existing source_education derivation does not bind these bytes')
            detail = dict(method=derivation['method'], record_id=derivation['record_id'])
        else:
            raise ValueError('source relation has no supported existing native provenance adapter')
        if _digest(original) != input_hash or _digest(parent_path) != parent_hash or _digest(path) != source_hash:
            raise ValueError('native input/derived bytes changed')
        fact=dict(source_sha256=source_hash, input_source_sha256=input_hash,
            relation=relation, producer_run_id=producer, source_path=str(path), input_source_path=str(original),
            parent_source_sha256=parent_hash,parent_source_path=str(parent_path),
            manifest_path=str(manifest), manifest_sha256=evidence_hash, record_pointer=pointer, **detail)
        result.append(fact)
        checked[source_hash]=fact
        seen.add(source_hash)
    if not sources - frozen <= seen:
        raise ValueError('every derived run source needs exact original-input provenance')
    used=set()
    for source in sources-frozen:
        while source not in frozen:
            if source in used:break
            proof=checked.get(source)
            if proof is None:raise ValueError('derived lineage parent is missing')
            used.add(source)
            source=proof['parent_source_sha256']
    if used != seen:
        raise ValueError('source evidence contains unrelated derived records')
    # No G lock is taken. The native caller must recheck its own pending token
    # and epoch under G after this adapter has returned and released Q.
    return result, dict(path=str(native_path), sha256=hashlib.sha256(native_bytes).hexdigest(),
                        control_revision=native.get('control_revision'), binding_token=binding_token)
