import json
import re

import pytest

from evidence import export_package
from knowledge_package import knowledge_files
from source_education import extract_source_education, source_education_fields
from test_knowledge_package import sample


@pytest.mark.parametrize('classified', [True, False])
@pytest.mark.parametrize('canonical', [True, False])
def test_each_export_has_one_unambiguous_handoff_and_keeps_inline_recipes(tmp_path, classified, canonical):
    meta = sample(tmp_path)
    original = tmp_path/'original.txt'
    original.write_text('成人本科毕业论文\n摘要', encoding='utf8')
    meta.update(source_education_fields(extract_source_education(original)))
    if classified:
        meta['classification'] = {'major':'会计学', 'method':'original_rules', 'source':'front_matter'}
    before = json.dumps(meta['blocks'], ensure_ascii=False, sort_keys=True)
    export_package(tmp_path, meta, canonical_paper_id=canonical)
    for name in ('ingest.md', 'ragflow_assets.md'):
        text = (tmp_path/name).read_text(encoding='utf8')
        blocks = re.findall(r'\[PAPER_PIPELINE_METADATA_V1\]\s*(.*?)\s*\[/PAPER_PIPELINE_METADATA_V1\]', text, re.S)
        assert len(blocks)==1
        handoff=json.loads(blocks[0])
        assert handoff['source_education']==meta['source_education']
        assert handoff['source_education_revision']==meta['source_education_revision']
        assert handoff['document_id']==meta['id']
        assert 'chart_spec' in text and '80.60' in text
    assert before==json.dumps(meta['blocks'], ensure_ascii=False, sort_keys=True)
    knowledge_files(tmp_path, require_stable_paper_id=canonical)
