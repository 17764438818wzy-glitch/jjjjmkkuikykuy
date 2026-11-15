"""Resolve retained work before model signatures; missing history is not a new request."""
from pathlib import Path
import json
import re
from .records import identity, read_json, sha_file
from .attachments import AttachmentError, has_request_history, _immutable_json


def _work_identity(paper, row, kind):
    file_id = row.get('file_id') if kind == 'attachment' else row.get('file_id', identity([row['relative'], row['sha256']])[:24])
    return {'paper_id': paper['paper_id'], 'file_id': file_id, 'source_sha256': row['sha256'], 'kind': kind}


def _binding_path(root, paper, row, kind):
    return Path(root) / 'source-work-bindings' / (identity(_work_identity(paper, row, kind)) + '.json')


def bind_work(root, paper, row, kind, work):
    _immutable_json(_binding_path(root, paper, row, kind),
        {'schema': 'source-work-binding/1', 'identity': _work_identity(paper, row, kind),
         'work_dir': str(Path(work).resolve())})


def retained_work(root, paper, row, kind, default):
    selected = resolve_work(row, kind, default)
    binding = _binding_path(root, paper, row, kind)
    if binding.exists():
        value = read_json(binding)
        if value.get('schema') != 'source-work-binding/1' or value.get('identity') != _work_identity(paper, row, kind):
            raise AttachmentError('evidence_changed')
        pinned = Path(value['work_dir'])
        if not pinned.is_dir(): raise AttachmentError('history_unresolved')
        if row.get('history', {}).get(kind + '_work') and selected.resolve() != pinned.resolve():
            raise AttachmentError('history_unresolved')
        return pinned
    if row.get('history', {}).get(kind + '_work'): return selected
    # Older adapters did not have a source-work binding. Search this paper/item's
    # retained signatures before accepting a new signature directory.
    matches = []
    if Path(default).parent.is_dir():
        for work in Path(default).parent.iterdir():
            if not work.is_dir(): continue
            facts = work / ('material.json' if kind == 'body' else 'description.json')
            if kind == 'attachment' and not facts.exists(): facts = work / 'evidence.json'
            if facts.is_file():
                value = read_json(facts)
                digest = value.get('source', {}).get('source_sha256') if kind == 'body' else value.get('source_sha256')
                if digest == row['sha256']: matches.append(work)
            elif has_request_history(work):
                raise AttachmentError('history_unresolved')
    if len(matches) > 1: raise AttachmentError('history_unresolved')
    return matches[0] if matches else selected


def resolve_work(row, kind, default):
    history = row.get('history')
    if history is None: return Path(default)
    paths = {str(Path(p).resolve()) for p in history.get('work_dirs', [])}
    chosen = history.get(kind + '_work')
    if len(paths) > 1 or (paths and not chosen): raise AttachmentError('history_unresolved')
    if chosen:
        work = Path(chosen).resolve()
        if str(work) not in paths or not work.is_dir(): raise AttachmentError('history_unresolved')
        return work
    if paths: raise AttachmentError('history_unresolved')
    return Path(default)


def _checked_model_history(paper, row, work):
    """Check immutable local bindings before any first-request history decision."""
    history = row.get('history')
    if history is None: return
    for ref in row.get('legacy_inventory_refs', []):
        if not Path(ref['path']).is_file() or sha_file(ref['path']) != ref['sha256']:
            raise AttachmentError('evidence_changed')
    if history.get('limitations'): raise AttachmentError('history_unresolved')
    if has_request_history(work): return  # Native observation decides complete/unknown, never this flag.
    # An empty upstream list or allow_new_request flag alone is not proof.
    proof_path = Path(work) / 'first-request.json'
    if not proof_path.is_file(): raise AttachmentError('history_unresolved')
    proof = read_json(proof_path)
    expected = {'paper_id': paper['paper_id'], 'file_id': row['file_id'],
                'source_binding': row.get('source_binding'), 'source_sha256': row['sha256']}
    if (proof.get('schema') != 'first-request-confirmation/1' or proof.get('identity') != expected
            or proof.get('identity_sha256') != identity(expected) or not proof.get('history_roots')
            or proof.get('request_history_found') is not False):
        raise AttachmentError('history_unresolved')
    for path in history.get('work_dirs', []):
        if has_request_history(path): raise AttachmentError('history_unresolved')
    return proof


def require_model_history(paper, row, work):
    """Only existing journals or an explicit, freshly checked receipt admit dispatch."""
    proof = _checked_model_history(paper, row, work)
    if proof is not None:
        _audit_first_request(paper, row, work, proof['history_roots'])


def _journal_owner(folder, journal):
    """Attribute a journal by its retained lane and work facts, not the new item."""
    parts = journal.relative_to(folder).parts
    if parts[0] == 'body' and len(parts) >= 4:
        kind, file_id, prefix = 'body', None, 2
    elif parts[0] == 'attachments' and len(parts) >= 5:
        kind, file_id, prefix = 'attachment', parts[1], 3
    else:
        raise AttachmentError('history_unresolved')
    tail = parts[prefix:]
    facts_work = folder.joinpath(*parts[:prefix])
    old_work = facts_work
    revised = len(tail)>2 and tail[0]=='body-plan-revisions'
    if revised:
        try:
            from uuid import UUID
            UUID(tail[1])
            old_work=facts_work/'body-plan-revisions'/tail[1]
            binding=read_json(old_work/'body-plan-revision.json')
            old_ref,new_ref=binding['old_manifest'],binding['new_manifest']
            if sha_file(old_ref['path'])!=old_ref['sha256'] or sha_file(new_ref['path'])!=new_ref['sha256']:
                raise ValueError('revision manifest changed')
            old,new=read_json(old_ref['path']),read_json(new_ref['path'])
            if (Path(old['body_work']).resolve()!=facts_work.resolve()
                    or Path(new['body_work']).resolve()!=old_work.resolve() or new.get('body_plan_revision')!=tail[1]
                    or any(new.get(k)!=old.get(k) for k in ('task_id','group_id','paper_id','receipt_id','attempt_id',
                                                         'body','attachments','receipt','material','reading_material'))):
                raise ValueError('revision source changed')
            tail=tail[2:]
        except (OSError,ValueError,TypeError,KeyError) as exc:
            raise AttachmentError('history_unresolved') from exc
    legacy = (len(tail) == 2 and tail[0] == 'model'
              or len(tail) == 4 and tail[0] == 'revisions' and tail[2] == 'model')
    multipart = kind == 'body' and len(tail) == 3 and tail[0] == 'model' and re.fullmatch(r'part-\d{3,}', tail[1])
    ranking = (kind == 'body' and len(tail) == 4 and tail[0] == 'whole-paper-ranking'
               and re.fullmatch(r'[0-9a-f]{64}', tail[1]) and tail[2] == 'model')
    retry = (kind=='body' and len(tail)==5 and tail[0]=='retries' and tail[3]=='model'
             and all(re.fullmatch(r'[0-9a-f]{64}',value) for value in tail[1:3]))
    if not (legacy or multipart or ranking or retry): raise AttachmentError('history_unresolved')
    marker = facts_work / ('material.json' if kind == 'body' else 'evidence.json')
    if not marker.is_file(): raise AttachmentError('history_unresolved')
    facts = read_json(marker)
    digest = facts.get('source', {}).get('source_sha256') if kind == 'body' else facts.get('source_sha256')
    if not isinstance(digest, str) or len(digest) != 64 or any(c not in '0123456789abcdef' for c in digest):
        raise AttachmentError('history_unresolved')
    if multipart or ranking or revised or retry:
        try:
            from .llm_queue.raw_request import _body_hash
            model = journal.parent
            request = read_json(model / 'request.json')
            state = read_json(model / 'request-state.json')
            request_sha = _body_hash(request)
            if state.get('request_sha256') != request_sha:
                raise ValueError('retained request changed')
            if multipart:
                parent = old_work / 'model'
                plan = read_json(parent / 'partition-plan.json')
                partition = read_json(parent / 'partition-state.json')
                rows = [row for row in plan.get('parts', []) if row.get('id') == tail[1]]
                if (not plan.get('schema') or plan.get('schema') != partition.get('schema')
                        or partition.get('plan_sha256') != sha_file(parent / 'partition-plan.json')
                        or plan.get('source_material_sha256') != identity(facts)
                        or plan.get('material_sha256') != sha_file(parent / 'partition-material.json')
                        or len(rows) != 1 or rows[0].get('request_sha256') != request_sha
                        or rows[0].get('material_sha256') != sha_file(model / 'material.json')
                        or _body_hash(read_json(model / 'planned-request.json')) != request_sha
                        or read_json(model / 'material.json').get('source', {}).get('source_sha256') != digest):
                    raise ValueError('partition ownership changed')
            else:
                material = next(json.loads(part['text'])['material'] for message in request['messages']
                    if message.get('role') == 'user' for part in message.get('content', [])
                    if isinstance(part, dict) and part.get('type') == 'text' and '"material"' in part.get('text', ''))
                if (material.get('source', {}).get('source_sha256') != digest
                        or ranking and not material.get('reading_context', {}).get('ranking_inputs')):
                    raise ValueError('ranking ownership changed')
        except (OSError, ValueError, TypeError, KeyError, StopIteration) as exc:
            raise AttachmentError('history_unresolved') from exc
    return kind, file_id, digest


def _audit_first_request(paper, row, work, history_roots):
    from .receipt_history import discover_legacy, item_legacy, observe_history
    ref = row.get('handoff_ref') or {}
    if not ref.get('path') or sha_file(ref['path']) != ref.get('sha256'):
        raise AttachmentError('history_unresolved')
    handoff = read_json(ref['path'])
    roots = [Path(p).resolve() for p in history_roots]
    if not roots or any(not p.is_dir() for p in roots): raise AttachmentError('history_unresolved')
    if has_request_history(work): raise AttachmentError('history_unresolved')
    legacy = discover_legacy(handoff, roots)
    if legacy['errors']: raise AttachmentError('history_unresolved')
    old = item_legacy(legacy, row['kind'], row)
    paths = set(old['work_dirs']) | set(row.get('history', {}).get('work_dirs', []))
    observed = observe_history(row['kind'], paper['paper_id'], row['file_id'], row['sha256'], paths)
    if observed['request_records'] or observed['existing_identification'] or observed['limitations']:
        raise AttachmentError('history_unresolved')
    if legacy.get('paper_id') and legacy['paper_id'] != paper['paper_id']:
        raise AttachmentError('history_unresolved')
    # Detect orphan journals under the known paper before a new work signature.
    for root in roots:
        folder = root / 'papers' / paper['paper_id']
        if not folder.is_dir(): continue
        for name in ('request.json', 'request-state.json'):
            for journal in folder.rglob(name):
                old_kind, old_id, digest = _journal_owner(folder, journal)
                if (old_kind == row['kind'] and (old_kind == 'body' or old_id == row['file_id'])
                        and digest == row['sha256']):
                    raise AttachmentError('history_unresolved')
    return [str(p) for p in roots]


def confirm_first_request(paper, row, work, *, history_roots):
    """Explicit local reconciliation entry; never sends or clears an old journal."""
    if has_request_history(work): raise AttachmentError('history_unresolved')
    roots = _audit_first_request(paper, row, work, history_roots)
    expected = {'paper_id': paper['paper_id'], 'file_id': row['file_id'],
                'source_binding': row.get('source_binding'), 'source_sha256': row['sha256']}
    result = {'schema': 'first-request-confirmation/1', 'identity': expected,
              'identity_sha256': identity(expected), 'history_roots': roots, 'request_history_found': False}
    _immutable_json(Path(work) / 'first-request.json', result)
    _checked_model_history(paper, row, work)
    return result

