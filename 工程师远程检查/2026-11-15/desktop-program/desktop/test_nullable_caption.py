"""AI recovery may explicitly return null for an absent table/figure caption."""
import copy
import pytest
from evidence import package_fingerprint
from knowledge_package import knowledge_files
from test_knowledge_package import sample, exported, read

@pytest.mark.parametrize('kind', ['table', 'image'])
def test_absent_caption_exports_without_losing_asset_or_inventing_title(tmp_path, kind):
    meta = sample(tmp_path)
    affected = [b for b in meta['blocks'] if b['type']==kind]
    for block in affected:
        block['caption'] = None
    before = copy.deepcopy(meta['blocks'])
    manifest = exported(tmp_path, meta)
    assert meta['blocks'] == before
    entries = manifest['tables' if kind=='table' else 'figures']
    assert len(entries) == len(affected)
    for entry in entries:
        obj = read(tmp_path, entry['path'])
        assert obj['caption'] == ''
        if kind=='table':
            original = next(b for b in affected if b['id']==obj['source_block_id'])
            assert obj['cells'] == original['cells']
        else:
            assert (tmp_path/obj['image']['path']).is_file()
    assert '![None]' not in (tmp_path/'document.md').read_text(encoding='utf-8')
    knowledge_files(tmp_path)
    package_fingerprint(tmp_path)
