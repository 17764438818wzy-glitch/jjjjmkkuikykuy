import hashlib
from pathlib import Path

import pytest

from isolated_copy import create_test_copy


def test_creates_separate_body_and_attachment_copies(tmp_path):
    originals = tmp_path / 'delivered'
    originals.mkdir()
    rows = []
    for kind, name, data in [('body', '正文.docx', b'body'), ('attachment', '附件.pdf', b'attachment')]:
        path = originals / name
        path.write_bytes(data)
        rows.append({'file_id': name, 'kind': kind, 'relative': name, 'path': str(path),
                     'sha256': hashlib.sha256(data).hexdigest(), 'bytes': len(data)})
    inventory = {'group_id': 'group-1', 'paper_id': 'paper-1', 'receipt_id': 'receipt-1', 'files': rows}
    result = create_test_copy(inventory, tmp_path / 'desktop-test')
    assert result['paper_id'] == 'paper-1'
    assert len(result['files']) == 2
    for item, original in zip(result['files'], rows):
        assert Path(item['path']).read_bytes() == Path(original['path']).read_bytes()
        assert not Path(item['path']).samefile(original['path'])
    assert Path(result['receipt_path']).is_file()
    assert all(Path(row['path']).is_file() for row in rows)


def test_bad_input_never_produces_a_complete_group_copy(tmp_path):
    source = tmp_path / 'source.docx'
    source.write_bytes(b'changed')
    row = {'file_id': 'body', 'kind': 'body', 'relative': 'source.docx', 'path': str(source),
           'sha256': hashlib.sha256(b'expected').hexdigest(), 'bytes': len(b'expected')}
    inventory = {'group_id': 'group-1', 'paper_id': 'paper-1', 'receipt_id': 'receipt-1', 'files': [row]}
    with pytest.raises(ValueError, match='changed'):
        create_test_copy(inventory, tmp_path / 'desktop-test')
    assert not (tmp_path / 'desktop-test' / 'group-1').exists()
    assert source.read_bytes() == b'changed'


def test_does_not_overwrite_an_existing_group_copy(tmp_path):
    target = tmp_path / 'desktop-test' / 'group-1'
    target.mkdir(parents=True)
    (target / 'keep').write_text('keep')
    with pytest.raises(FileExistsError):
        create_test_copy({'group_id': 'group-1'}, target.parent)
    assert (target / 'keep').read_text() == 'keep'
