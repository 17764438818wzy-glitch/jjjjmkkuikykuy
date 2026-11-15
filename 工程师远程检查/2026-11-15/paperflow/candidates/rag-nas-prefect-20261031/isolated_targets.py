"""Prepare private per-specialty targets for one isolated batch, sequentially.

Routes and catalog are already validated by the package boundary. The caller
supplies the batch's shared effect journal and admission; no paper is uploaded.
"""
import json
import os
from pathlib import Path
import re
from concurrent.futures import Future
from urllib.parse import quote

from effect_journal import EffectUnknown
from paperflow_prefect.records import identity


class TargetCreations:
    """Share only an identical test-library creation already in this process."""
    def __init__(self):
        self.pending = {}

    def __call__(self, key, operation):
        future = Future()
        existing = self.pending.setdefault(key, future)
        if existing is not future:
            return existing.result()
        try:
            result = operation()
        except BaseException as exc:
            future.set_exception(exc)
            raise
        else:
            future.set_result(result)
            return result
        finally:
            self.pending.pop(key)


def dataset_by_id(client, dataset_id):
    rows = client.request('GET', f'/datasets?id={quote(dataset_id, safe="")}&page=1&page_size=100')
    return rows[0] if len(rows) == 1 and rows[0].get('id') == dataset_id else None


def ensure_test_targets(routes, needs_images, has_attachments, run_id, catalog, client, journal, admit, receipt_dir,
                        *, coordinate=None):
    receipt_dir = Path(receipt_dir).resolve(strict=True)
    if not receipt_dir.is_dir():
        raise ValueError('batch receipt directory is unavailable')
    formal = {row['code']: row for row in catalog['majors']}
    formal_ids = {row[field] for row in catalog['majors']
                  for field in ('paper_dataset_id', 'image_dataset_id') if row.get(field)}
    targets = {}
    for route in routes:
        code = route['code']
        targets[code] = {}
        for role, field in [('paper', 'paper_dataset_id')] + ([('image', 'image_dataset_id')] if needs_images or has_attachments else []):
            formal_id = formal[code][field]
            if route[field] != formal_id:
                raise ValueError('specialty route differs from the validated catalog')
            name = f'PaperFlow SELFTEST {run_id} {code} {role}'
            suffix = '' if role == 'paper' else '-image'
            receipt_path = receipt_dir / f'rag-dataset-{code}{suffix}.json'
            effect_id = identity({'run_id': run_id, 'operation': 'create-test-dataset', 'name': name})
            def resolve():
                original = dataset_by_id(client, formal_id)
                if original is None or not all(original.get(key) for key in ('embedding_model', 'tenant_id', 'created_by')):
                    raise ValueError('formal library ownership or embedding is unavailable')
                body = {'name': name, 'permission': 'me', 'chunk_method': 'naive',
                        'embedding_model': original['embedding_model']}
                request_sha = identity(body)
                is_new = not receipt_path.exists()
                if not is_new:
                    receipt = json.loads(receipt_path.read_text(encoding='utf8'))
                    dataset_id = receipt['dataset_id']
                    recorded = journal._read(effect_id)
                    if recorded is None:
                        raise EffectUnknown('test library creation history is unavailable')
                    created = journal._interpret(recorded, request_sha)
                    if (receipt.get('effect_id') != effect_id or receipt.get('request_sha256') != request_sha
                            or created.get('id') != dataset_id):
                        raise ValueError('test library creation receipt differs from its journal')
                else:
                    named = client.request('GET', f'/datasets?keywords={quote(name, safe="")}&page=1&page_size=100')
                    if any(row.get('name') == name for row in named):
                        raise ValueError('unrecorded test library needs review')
                    created = journal.perform(effect_id, request_sha, lambda: client.request('POST', '/datasets', body),
                                              admit(effect_id, request_sha), reuse_returned=False)
                    dataset_id = created['id']
                    if not isinstance(dataset_id, str) or not re.fullmatch('[0-9a-f]{32}', dataset_id):
                        raise ValueError('new test library identity is not new')
                    receipt = {'run_id': run_id, 'code': code, 'role': role, 'dataset_id': dataset_id,
                               'name': name, 'formal_dataset_id': formal_id, 'effect_id': effect_id,
                               'request_sha256': request_sha, 'tenant_id': original['tenant_id'],
                               'created_by': original['created_by'], 'embedding_model': original['embedding_model'],
                               'empty_at_readback': True}
                observed = dataset_by_id(client, dataset_id)
                if observed is None:
                    raise ValueError('test library is not uniquely readable')
                if is_new and (observed.get('document_count') != 0 or observed.get('chunk_count') != 0):
                    raise ValueError('new test library is not empty')
                if (dataset_id in formal_ids or receipt['run_id'] != run_id or receipt['code'] != code
                        or receipt['name'] != name or receipt['formal_dataset_id'] != formal_id
                        or observed.get('name') != name or observed.get('permission') != 'me'
                        or any(observed.get(key) != original.get(key) or receipt.get(key) != original.get(key)
                               for key in ('embedding_model', 'tenant_id', 'created_by'))):
                    raise ValueError('test library identity or isolation differs')
                if is_new:
                    with receipt_path.open('x', encoding='utf8', newline='\n') as stream:
                        json.dump(receipt, stream, ensure_ascii=False, indent=2)
                        stream.write('\n')
                        stream.flush()
                        os.fsync(stream.fileno())
                return dataset_id
            targets[code][field] = coordinate(effect_id, resolve) if coordinate else resolve()
    return targets
