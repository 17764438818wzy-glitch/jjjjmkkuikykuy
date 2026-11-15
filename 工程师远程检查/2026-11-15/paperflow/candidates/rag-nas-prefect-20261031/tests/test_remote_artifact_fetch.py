import hashlib
import io

import pytest

from remote_artifact_fetch import fetch_artifacts


def row(path, content, **extra):
    return {'linux_path': path, 'path': r'F:\\pfwire\\' + path.rsplit('/', 1)[-1],
            'sha256': hashlib.sha256(content).hexdigest(), 'bytes': len(content), **extra}


class Sftp:
    def __init__(self, files):
        self.files = files

    def open(self, path, mode):
        assert mode == 'rb'
        return io.BytesIO(self.files[path])


def test_reuses_package_and_attachment_files(tmp_path):
    files = {'/package/chunks.json': b'pre-existing chunks', '/package/manifest.json': b'manifest',
             '/attachment/result.md': b'processed attachment', '/attachment/调查资料.pdf': b'original'}
    artifact_map = {'body_package': [row('/package/chunks.json', files['/package/chunks.json'],
                                         relative_path='chunks.json'),
                                    row('/package/manifest.json', files['/package/manifest.json'],
                                         relative_path='manifest.json')],
                    'attachments': [{'attachment_id': 'attachment-1',
                        'markdown': row('/attachment/result.md', files['/attachment/result.md']),
                        'source': row('/attachment/调查资料.pdf', files['/attachment/调查资料.pdf'])}]}
    output = fetch_artifacts(artifact_map, Sftp(files), tmp_path / 'copy')
    assert (output['body_path'] / 'chunks.json').read_bytes() == files['/package/chunks.json']
    attachment = output['attachments'][0]
    assert attachment['markdown_path'].read_bytes() == files['/attachment/result.md']
    assert attachment['source_path'].read_bytes() == files['/attachment/调查资料.pdf']
    assert attachment['source_path'].name == '调查资料.pdf'


def test_changed_remote_bytes_do_not_produce_a_complete_copy(tmp_path):
    artifact_map = {'body_package': [row('/package/chunks.json', b'expected', relative_path='chunks.json')],
                    'attachments': []}
    with pytest.raises(ValueError, match='changed'):
        fetch_artifacts(artifact_map, Sftp({'/package/chunks.json': b'changed'}), tmp_path / 'copy')
    assert not (tmp_path / 'copy').exists()


def test_existing_copy_is_not_overwritten(tmp_path):
    (tmp_path / 'copy').mkdir()
    (tmp_path / 'copy' / 'keep').write_text('keep')
    with pytest.raises(ValueError):
        fetch_artifacts({'body_package': [], 'attachments': []}, Sftp({}), tmp_path / 'copy')
    assert (tmp_path / 'copy' / 'keep').read_text() == 'keep'


def test_verified_copy_can_resume_without_recopied_or_overwritten_files(tmp_path):
    data = b'existing package'
    artifact_map = {'body_package': [row('/package/manifest.json', data, relative_path='manifest.json')],
                    'attachments': []}
    result = fetch_artifacts(artifact_map, Sftp({'/package/manifest.json': data}), tmp_path / 'copy')
    assert fetch_artifacts(artifact_map, Sftp({}), tmp_path / 'copy') == result
    (result['body_path'] / 'manifest.json').write_bytes(b'changed')
    with pytest.raises(ValueError, match='changed'):
        fetch_artifacts(artifact_map, Sftp({}), tmp_path / 'copy')
