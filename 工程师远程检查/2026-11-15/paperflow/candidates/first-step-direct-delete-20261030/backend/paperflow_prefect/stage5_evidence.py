"""Durable evidence representation; Stage5Receiver owns the RAG admission gate."""
from contextlib import contextmanager, ExitStack
import copy
import hashlib
import os
from pathlib import Path
import shutil
from .records import Records, identity, read_json, sha_file, write_json


def execution_inventory(handoff):
    paper = copy.deepcopy(handoff['paper'])
    rows = handoff['files']
    if len(rows) != len(paper['archive_files']):
        raise ValueError('handoff file membership differs')
    by_path = {r['path']: r for r in rows}
    if len(by_path) != len(rows):
        raise ValueError('duplicate handoff path')
    def augment(row):
        source = by_path.get(row['path'])
        keys = ('file_id', 'kind', 'path', 'relative', 'bytes', 'sha256')
        if (not source or any(source.get(k) != row.get(k) for k in keys) or
                not source.get('object_fingerprint')):
            raise ValueError('handoff augmented file identity differs')
        return dict(row, object_fingerprint=source['object_fingerprint'])
    paper['archive_files'] = [augment(r) for r in paper['archive_files']]
    paper['body'] = augment(paper['body'])
    paper['body_copies'] = [augment(r) for r in paper['body_copies']]
    paper['attachments'] = [augment(r) for r in paper['attachments']]
    return paper


def evidence_refs(handoff):
    refs = [*handoff['evidence'], *handoff.get('retained_evidence', []), handoff['intake_receipt']]
    seen = {}
    for ref in refs:
        path = str(Path(ref['path']).resolve())
        digest = ref['sha256']
        if not isinstance(digest, str) or len(digest) != 64:
            raise ValueError('invalid handoff evidence hash')
        key = path.casefold()
        if key in seen and seen[key]['sha256'] != digest:
            raise ValueError('conflicting handoff evidence references')
        seen[key] = dict(path=path, sha256=digest)
    return list(seen.values())


@contextmanager
def held_handoff_evidence(handoff):
    from paperflow.original_archive.lifecycle import locked_file
    with ExitStack() as stack:
        for ref in evidence_refs(handoff):
            stream, _ = stack.enter_context(locked_file(Path(ref['path'])))
            if hashlib.file_digest(stream, 'sha256').hexdigest() != ref['sha256']:
                raise ValueError('handoff evidence changed before acceptance')
        yield


def snapshot_handoff(handoff, work):
    """Persist an already admitted handoff; never rewrites upstream identities."""
    if handoff.get('schema') != 'paperflow-nas-handoff/1' or handoff.get('status') != 'ready':
        raise ValueError('only ready handoffs may be committed')
    work = Path(work).resolve()
    paper = execution_inventory(handoff)
    if Records(work).cached('stage4-handoff'):
        old = frozen_handoff(work)
        if old['handoff'] != handoff:
            raise ValueError('another immutable handoff already owns this work')
        return old
    sources = {str(Path(r['path']).resolve()).casefold(): r for r in handoff['files']}
    snapshots, nas_sources = [], []
    from paperflow.original_archive.lifecycle import locked_file
    with held_handoff_evidence(handoff):
        for ref in evidence_refs(handoff):
            if ref['path'].casefold() in sources:
                if ref['sha256'] != sources[ref['path'].casefold()]['sha256']:
                    raise ValueError('source proof differs from NAS inventory')
                nas_sources.append(ref)
                continue
            target = work / 'stage4/evidence' / ref['sha256']
            target.parent.mkdir(parents=True, exist_ok=True)
            if target.exists():
                if sha_file(target) != ref['sha256']:
                    raise ValueError('existing handoff snapshot differs')
            else:
                with locked_file(Path(ref['path'])) as (stream, _), target.open('xb') as output:
                    shutil.copyfileobj(stream, output)
                    output.flush(); os.fsync(output.fileno())
                if sha_file(target) != ref['sha256']:
                    raise ValueError('handoff snapshot readback differs')
            snapshots.append(dict(ref, snapshot_path=str(target)))
    value = dict(schema='paperflow-stage5-frozen-evidence/1', handoff=handoff, paper=paper,
                 snapshots=snapshots, nas_source_evidence=nas_sources)
    path = work / 'stage4/frozen-handoff.json'
    inventory_path = work / 'inventory.json'
    if inventory_path.exists() and read_json(inventory_path) != paper:
        raise ValueError('existing stage5 inventory differs')
    write_json(inventory_path, paper)
    write_json(path, value)
    result = dict(status='accepted', handoff_id=handoff['handoff_id'], handoff_sha256=identity(handoff),
                  paper_id=paper['paper_id'], revision=paper['revision'],
                  validation_scope=handoff['validation_scope'], frozen_path=str(path))
    Records(work).complete('stage4-handoff', result,
                          [path, inventory_path, *[r['snapshot_path'] for r in snapshots]])
    return frozen_handoff(work)


def frozen_handoff(work):
    work = Path(work).resolve()
    records = Records(work)
    result = records.cached('stage4-handoff')
    if not result or result.get('status') != 'accepted' or Path(result['frozen_path']) != work / 'stage4/frozen-handoff.json':
        raise ValueError('stage4 handoff was not durably accepted')
    value = read_json(result['frozen_path'])
    handoff, paper = value['handoff'], value['paper']
    if (value['schema'] != 'paperflow-stage5-frozen-evidence/1' or handoff['status'] != 'ready' or
            result['handoff_sha256'] != identity(handoff) or result['handoff_id'] != handoff['handoff_id'] or
            result['validation_scope'] != handoff['validation_scope'] or paper != execution_inventory(handoff) or
            paper != read_json(work / 'inventory.json') or
            any(result[k] != paper[k] for k in ('paper_id', 'revision'))):
        raise ValueError('frozen handoff identity differs')
    refs = {r['path']: r['sha256'] for r in evidence_refs(handoff)}
    for row in [*value['snapshots'], *value['nas_source_evidence']]:
        if refs.pop(row['path'], None) != row['sha256']:
            raise ValueError('frozen evidence coverage differs')
        if 'snapshot_path' in row:
            path = Path(row['snapshot_path'])
            if path != work / 'stage4/evidence' / row['sha256'] or sha_file(path) != row['sha256']:
                raise ValueError('frozen evidence snapshot changed')
    if refs:
        raise ValueError('missing frozen handoff evidence')
    return value
