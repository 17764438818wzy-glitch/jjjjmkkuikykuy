"""Freeze only explicitly owned, finalized, disposable cache files."""
from pathlib import Path
from .intake import object_fingerprint
from .records import read_json, sha_file

KINDS = {'extracted_text', 'converted_preview', 'temporary_image', 'unpacked_member', 'working_copy'}
PROTECTED_WORDS = {'request', 'response', 'receipt', 'manifest', 'package', 'bundle',
                   'candidate', 'validation', 'inventory', 'journal', 'association'}


def freeze_intermediates(paper, work, data_root, candidates, evidence):
    from paperflow.original_archive.lifecycle import safe_file, locked_file, verify_stream
    accepted, retained, proofs, seen = [], [], {}, set()
    work, data_root = Path(work).resolve(), Path(data_root).resolve()
    owner = dict(paper_id=paper['paper_id'], revision=paper['revision'])
    protected = {Path(p).resolve() for p in evidence}
    if paper.get('receipt_schema') == 'paperflow-receipt/1':
        from .stage5_evidence import evidence_refs, frozen_handoff
        protected.update(Path(r['path']).resolve()
                         for r in evidence_refs(frozen_handoff(work)['handoff']))
    # Proof names/extensions do not determine their role. Protect every
    # candidate's producer up front, including later or rejected candidates.
    for item in candidates:
        try:
            protected.add(Path(item['producer_record']['path']).resolve())
        except (KeyError, TypeError, ValueError, OSError):
            pass
    forbidden = [work / name for name in ('stages', 'archive', 'cleanup', 'stage4')]
    forbidden.append(data_root / 'cleanup-index')
    for item in candidates:
        name = item.get('path', '') if isinstance(item, dict) else ''
        try:
            if not isinstance(item, dict) or not name:
                raise ValueError('intermediate_identity_unknown')
            path = safe_file(name, [data_root])
            root = safe_file(item['root'], [data_root])
            if (root in {data_root, work} or not root.is_dir() or path == root or
                    not path.is_relative_to(root) or str(path).casefold() in seen):
                raise ValueError('intermediate_scope_unknown')
            seen.add(str(path).casefold())
            lowered = path.name.casefold()
            if (path in protected or any(path == p or path.is_relative_to(p) for p in forbidden) or
                    path.suffix.casefold() in {'.md', '.json', '.jsonl', '.sqlite', '.sqlite3', '.db', '.log'} or
                    any(word in lowered for word in PROTECTED_WORDS)):
                raise ValueError('recovery_evidence_retained')
            if item.get('kind') not in KINDS or any(item.get(k) != v for k, v in owner.items()):
                raise ValueError('intermediate_owner_unknown')
            ref = item['producer_record']
            producer_path = safe_file(ref['path'], [data_root])
            if producer_path == path or sha_file(producer_path) != ref['sha256']:
                raise ValueError('intermediate_producer_changed')
            producer = read_json(producer_path)
            expected_item = {k: v for k, v in item.items() if k != 'producer_record'}
            if (producer.get('schema') != 'paperflow-intermediate-ownership/1' or
                    any(producer.get(k) != v for k, v in owner.items()) or
                    producer.get('producer_state') != 'complete' or producer.get('finalized') is not True or
                    producer.get('reproducible') is not True or producer.get('owners') != [owner] or
                    producer.get('references') != [] or producer.get('active_consumers') != [] or
                    expected_item not in producer.get('files', [])):
                raise ValueError('intermediate_shared_referenced_or_unfinished')
            with locked_file(path) as (stream, _):
                import os
                if object_fingerprint(os.fstat(stream.fileno())) != item.get('object_fingerprint'):
                    raise ValueError('intermediate_file_object_changed')
                verify_stream(stream, item['sha256'], item['bytes'])
            accepted.append(dict(item, path=str(path), kind='intermediate', cache_kind=item['kind']))
            proofs[str(producer_path)] = ref['sha256']
        except (ValueError, OSError, KeyError, TypeError) as exc:
            retained.append(dict(path=name, reason=str(exc) if isinstance(exc, ValueError)
                                 else 'intermediate_identity_or_access_unknown'))
    if {Path(r['path']).resolve() for r in accepted} & (protected | {Path(p).resolve() for p in proofs}):
        raise ValueError('cleanup candidates overlap retained recovery evidence')
    return accepted, retained, proofs
