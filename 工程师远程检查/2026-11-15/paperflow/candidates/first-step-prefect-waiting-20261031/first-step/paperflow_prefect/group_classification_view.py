"""Read the saved first-step classification without reprocessing a group."""
from functools import lru_cache
from hashlib import sha256
import json
from pathlib import Path

from .group_delivery import SCHEMA
from .intake_handoff import _header
from .records import identity


def _stamp(path):
    info = path.stat()
    return (info.st_size, info.st_mtime_ns, info.st_ctime_ns)


@lru_cache(maxsize=2048)
def _read_saved_delivery(directory, group_id, handoff_stamp, manifest_stamp):
    """Cache small immutable manifests; a changed or missing file is rechecked."""
    root = Path(directory)
    handoff_bytes = (root / 'handoff.json').read_bytes()
    manifest = json.loads((root / 'delivery.json').read_text(encoding='utf-8-sig'))
    handoff = json.loads(handoff_bytes.decode('utf-8-sig'))
    if (not isinstance(manifest, dict) or manifest.get('schema') != SCHEMA
            or manifest.get('delivery_id') != identity({k: v for k, v in manifest.items() if k != 'delivery_id'})
            or manifest.get('handoff_sha256') != sha256(handoff_bytes).hexdigest()
            or not isinstance(handoff, dict) or handoff.get('group_id') != group_id
            or any(manifest.get(key) != handoff.get(key) for key in ('group_id', 'handoff_id', 'result_version'))):
        raise ValueError('分类清单与本组交付记录不符')
    _header(handoff)
    unresolved = handoff['unresolved']
    complete = handoff.get('classification_complete')
    if type(complete) is not bool or complete != (not bool(unresolved)):
        raise ValueError('分类完成状态与清单不符')

    def display_file(row, *, reason=False):
        file_id, relative = row['file_id'], row['relative']
        if not isinstance(file_id, str) or not isinstance(relative, str) or not file_id or not relative:
            raise ValueError('分类文件记录无效')
        item = {'file_id': file_id, 'relative_path': relative}
        if reason:
            item['reason'] = row.get('reason') or '未确认'
        return item

    body = [handoff['body']] if handoff.get('body') else []
    files = {'body': [display_file(r) for r in body],
             'attachment': [display_file(r) for r in handoff['attachments']],
             'trash': [display_file(r) for r in handoff['trash']],
             'unresolved': [display_file(r, reason=True) for r in unresolved]}
    return {'classification_state': 'complete' if complete else 'partial',
            'confirmed_counts': {name: len(files[name]) for name in ('body', 'attachment', 'trash')},
            'unresolved_count': len(files['unresolved']), 'files': files}


def classification_view(row):
    unavailable = {'classification_state': 'unavailable', 'confirmed_counts': None,
                   'unresolved_count': None, 'files': None}
    directory, group_id = row.get('delivery_dir'), row.get('group_id')
    if not isinstance(directory, str) or not directory or not isinstance(group_id, str) or not group_id:
        return unavailable
    try:
        root = Path(directory)
        return _read_saved_delivery(str(root), group_id, _stamp(root / 'handoff.json'),
                                    _stamp(root / 'delivery.json'))
    except (OSError, ValueError, TypeError, KeyError, AttributeError, RuntimeError):
        return unavailable
