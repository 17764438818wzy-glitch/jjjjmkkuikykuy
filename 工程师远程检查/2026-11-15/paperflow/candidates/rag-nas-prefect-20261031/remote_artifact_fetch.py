"""Copy artifacts from an already verified remote receipt, without modifying them.

The caller supplies the artifact map accepted by RemoteReceiptInspector and an
authenticated read-only SFTP session. This does not authorize an upload or
advance a Prefect ticket.
"""
import hashlib
import json
from pathlib import Path, PureWindowsPath
import tempfile


def fetch_artifacts(artifact_map, sftp, destination):
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)

    files = [(item, Path('body') / item['relative_path']) for item in artifact_map['body_package']]
    attachments = []
    for item in artifact_map['attachments']:
        folder = Path('attachments') / hashlib.sha256(item['attachment_id'].encode('utf8')).hexdigest()
        markdown = folder / 'processed.md'
        source = folder / 'source' / PureWindowsPath(item['source']['path']).name
        files.extend([(item['markdown'], markdown), (item['source'], source)])
        attachments.append({'attachment_id': item['attachment_id'],
                            'markdown_path': destination / markdown, 'source_path': destination / source})

    inventory = {'schema': 'paperflow-remote-artifact-copy/1',
                 'files': [{'path': str(relative), 'sha256': item['sha256'], 'bytes': item['bytes']}
                           for item, relative in files]}
    result = {'body_path': destination / 'body', 'attachments': attachments}
    if destination.exists():
        marker = destination / 'artifact-copy.json'
        if not marker.is_file() or json.loads(marker.read_bytes()) != inventory:
            raise ValueError('existing artifact copy is not the recorded copy')
        for item, relative in files:
            with (destination / relative).open('rb') as stream:
                digest = hashlib.file_digest(stream, 'sha256').hexdigest()
                size = stream.seek(0, 2)
            if (digest, size) != (item['sha256'], item['bytes']):
                raise ValueError('local artifact copy changed')
        return result

    with tempfile.TemporaryDirectory(prefix='.paperflow-fetch-', dir=destination.parent) as temporary:
        payload = Path(temporary) / 'payload'
        payload.mkdir()
        for item, relative in files:
            target = payload / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            digest, size = hashlib.sha256(), 0
            with sftp.open(item['linux_path'], 'rb') as remote, target.open('xb') as local:
                while data := remote.read(1024 * 1024):
                    local.write(data)
                    digest.update(data)
                    size += len(data)
            if (digest.hexdigest(), size) != (item['sha256'], item['bytes']):
                raise ValueError('remote artifact changed while copying')
        (payload / 'artifact-copy.json').write_text(json.dumps(inventory, ensure_ascii=False), encoding='utf8')
        payload.rename(destination)
    return result
