"""Prepare one RAG Files folder dedicated to this isolated batch."""
from pathlib import Path

from associated_delivery import _files
from paperflow_prefect.records import identity, read_json, write_json

from effect_journal import EffectUnknown


def ensure_test_files_folder(raw, journal, admit, run_id, receipt_path):
    root = raw.request('GET', '/files?page=1&page_size=1')['parent_folder']
    if root.get('name') != '/' or root.get('type') != 'folder' or root.get('id') != root.get('parent_id'):
        raise ValueError('RAG Files root is unavailable')
    name = f'PaperFlow SELFTEST {run_id} attachments'
    payload = {'parent_id': root['id'], 'name': name, 'type': 'folder'}
    binding = {'service': raw.base_url.rstrip('/'), 'run_id': run_id, 'payload': payload,
               'journal_id': journal.identity()}
    effect = identity({'service': binding['service'], 'create_folder': payload})
    path = Path(receipt_path)
    state = read_json(path) if path.is_file() else None
    if state is not None and state.get('binding') != binding:
        raise ValueError('test folder receipt binding changed')
    rows = [row for row in _files(raw, root['id'], name, None)
            if row.get('parent_id') == root['id'] and row.get('name') == name]
    if len(rows) > 1 or any(row.get('type') != 'folder' for row in rows):
        raise ValueError('test folder is not unique')
    if state is None:
        if rows or journal.state(effect, effect) is not None:
            raise ValueError('unrecorded test folder already exists')
        state = {'schema': 'paperflow-isolated-files-folder/1', 'binding': binding, 'status': 'pending'}
        write_json(path, state)
    status = journal.state(effect, effect)
    if status == 'intent':
        raise EffectUnknown('test folder creation requires reconciliation')
    if not rows and status is None:
        journal.perform(effect, effect, lambda: raw.request('POST', '/files', payload),
                        admit(effect, effect), reuse_returned=False)
        rows = [row for row in _files(raw, root['id'], name, None)
                if row.get('parent_id') == root['id'] and row.get('name') == name]
        status = 'returned'
    if len(rows) != 1 or rows[0].get('type') != 'folder' or status != 'returned':
        raise EffectUnknown('test folder has not been read back')
    folder = rows[0]['id']
    if state.get('folder_id') and state['folder_id'] != folder:
        raise ValueError('test folder identity changed')
    state.update(folder_id=folder, status='verified')
    write_json(path, state)
    return folder
