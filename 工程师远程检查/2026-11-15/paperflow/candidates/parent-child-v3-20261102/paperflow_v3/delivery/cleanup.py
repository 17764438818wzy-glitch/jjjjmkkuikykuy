"""Release only the explicit local files of a fully delivered paper.

``save_state`` must durably save the supplied progress and return its receipt
ID. Production uses the existing local recovery file; the caller publishes
whole-paper completion to Prefect. The caller also supplies the actual
per-paper file list and the admission check.
"""

import copy
import errno
import os
from pathlib import Path

def _persist(state, value, save_state):
    if not save_state(copy.deepcopy(value)):
        raise ValueError('server receipt was not saved')
    state.clear()
    state.update(value)


def cleanup_paper(paper, identity, handoff, packet, body_targets, attachment_receipts, archive,
                  additional_files, state, save_state, check_running, *, retry_failed=False):
    routes = packet['routes']
    if not 1 <= len(routes) <= 3:
        raise ValueError('no selected formal targets')
    for route in routes:
        target = body_targets.get(route['code']) or {}
        body = target.get('body') or {}
        owner = body.get('identity') or {}
        if (target.get('body_delivery_state') != 'complete' or target.get('verified') is not True
                or target.get('retrieval_verified') is not True
                or target.get('formal_route') != route
                or target.get('ticket_identity') != identity
                or body.get('dataset_id') != route['paper_dataset_id'] or not body.get('document_id')
                or owner.get('paperflow_source_sha256') != paper['body']['sha256']
                or owner.get('paperflow_specialty') != route['code']):
            raise ValueError('not all body targets completed')
    for item in paper['attachments']:
        receipt = attachment_receipts.get(item['file_id']) or {}
        binding = receipt.get('binding') or {}
        publications = receipt.get('publications', {}).get(receipt.get('current_md_sha256'), {})
        if (receipt.get('status') != 'verified' or receipt.get('ticket_identity') != identity
                or binding.get('paper_id') != paper['paper_id']
                or binding.get('attachment_id') != item['file_id']
                or binding.get('source_sha256') != item['sha256']
                or any((publications.get(route['code']) or {}).get('status') != 'verified'
                       or (publications.get(route['code']) or {}).get('dataset_id') != route['image_dataset_id']
                       for route in routes)):
            raise ValueError('not all attachment targets completed')
    archived = archive.get('files') or {}
    expected = [row['file_id'] for row in paper['archive_files']]
    if (archive.get('identity') != identity or archive.get('status') != 'complete' or set(archived) != set(expected)
            or any(archived[file_id].get('status') != 'complete' or not archived[file_id].get('destination')
                   for file_id in expected)):
        raise ValueError('body or attachment archive has not completed')

    normalize = lambda path: os.path.normcase(os.path.abspath(os.fspath(path)))
    protected = {normalize(Path(paper['person_dir']) / name) for name in paper.get('excluded', [])}
    protected.update(normalize(row['destination']) for row in archived.values())
    paths = []
    for row in paper['archive_files']:
        paths.extend([row.get('source_path') or row['path'], row['path']])
    paths.extend(additional_files)
    paths = list(dict.fromkeys(normalize(path) for path in paths))
    if any(path in protected for path in paths):
        raise ValueError('cleanup list contains excluded or archived material')

    if state:
        if state.get('identity') != identity or set(state.get('files', {})) != set(paths):
            raise ValueError('cleanup state belongs to another paper or file list')
        if any(row['status'] in ('in_flight', 'unknown') for row in state['files'].values()):
            raise ValueError('cleanup outcome unknown; reconciliation required')
        if any(row['status'] == 'failed' for row in state['files'].values()) and not retry_failed:
            raise ValueError('cleanup file requires a user-selected retry')
    else:
        if check_running() is not True:
            raise InterruptedError('parent task is paused')
        value = {'identity': copy.deepcopy(identity), 'status': 'incomplete',
                 'routes': copy.deepcopy(routes),
                 'body_targets': copy.deepcopy(body_targets),
                 'attachment_receipts': copy.deepcopy(attachment_receipts), 'archive': copy.deepcopy(archive),
                 'files': {path: {'status': 'pending'} for path in paths}}
        _persist(state, value, save_state)

    for path in paths:
        if state['files'][path]['status'] == 'complete':
            continue
        value = copy.deepcopy(state)
        value['files'][path]['status'] = 'in_flight'
        _persist(state, value, save_state)
        try:
            if check_running() is not True:
                raise InterruptedError('parent task is paused')
        except Exception:
            # No unlink has run. A failed/revoked check is known not to have
            # deleted this file; failed persistence still leaves it unknown.
            value = copy.deepcopy(state)
            value['files'][path]['status'] = 'pending'
            _persist(state, value, save_state)
            raise
        try:
            Path(path).unlink()
        except OSError as exc:
            if (isinstance(exc, (PermissionError, FileNotFoundError, IsADirectoryError, NotADirectoryError))
                    or exc.errno in (errno.EACCES, errno.EPERM, errno.ENOENT, errno.EISDIR,
                                     errno.ENOTDIR, errno.EROFS, errno.EBUSY)
                    or getattr(exc, 'winerror', None) in (2, 3, 5, 32, 33)):
                value = copy.deepcopy(state)
                value['files'][path].update(status='failed', error_type=type(exc).__name__)
                _persist(state, value, save_state)
            raise
        value = copy.deepcopy(state)
        value['files'][path]['status'] = 'complete'
        _persist(state, value, save_state)
    value = copy.deepcopy(state)
    value['status'] = 'complete'
    _persist(state, value, save_state)
    return state
