"""Stable document identity across private archive extraction directories."""
from pathlib import PurePosixPath


ARCHIVES = {'.zip', '.rar', '.7z'}
MEMBER_FIELDS = ('input_source_path', 'archive_member_path', 'archive_source_hash')


def source_key(path):
    return str(path).replace('\\', '/').casefold()


def member_path(record):
    explicit = record.get('archive_member_path')
    if explicit:
        return source_key(explicit)
    path = source_key(record.get('source_path') or record.get('path') or record.get('source', ''))
    parent = source_key(record.get('input_source_path', ''))
    if parent and parent != path and PurePosixPath(parent).suffix in ARCHIVES and '/archive/' in path:
        # Older batches did not persist member identity. The extractor's
        # relative layout is stable even though each worker root is random.
        return path.split('/archive/', 1)[1]
    return ''


def record_key(record):
    path = record.get('source_path') or record.get('path') or record.get('source') or record.get('id', '')
    member = member_path(record)
    parent = record.get('input_source_path', '')
    if member and parent:
        return source_key(parent) + '::archive::' + member
    return source_key(path)


def member_fields(record):
    return {key: record[key] for key in MEMBER_FIELDS if record.get(key)}
