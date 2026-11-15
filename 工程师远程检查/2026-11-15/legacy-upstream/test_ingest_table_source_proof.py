"""Permit only a deterministic literal Markdown table rendering in source proof."""
import copy
import hashlib
import pytest
from test_ingest_artifact_provenance import admitted_source
from ingest_execution_diagnostics import execution_source_proof

TABLE='| 指令 | 作用 |\n| --- | --- |\n| forward() | 向前移动20像素 |\n| right() | 向右转动90度 |\n'
HTML='<table>\n<thead>\n<tr>\n<th>指令</th>\n<th>作用</th>\n</tr>\n</thead>\n<tbody>\n<tr>\n<td>forward()</td>\n<td>向前移动20像素</td>\n</tr>\n<tr>\n<td>right()</td>\n<td>向右转动90度</td>\n</tr>\n</tbody>\n</table>\n'

def table_fixture(tmp_path,source_table=TABLE,parsed_table=HTML):
    task,remote,log,chunks,client=admitted_source(tmp_path)
    path=__import__('pathlib').Path(task['path'])
    original=path.read_text(encoding='utf-8')
    path.write_text(original+source_table,encoding='utf-8')
    task['content_sha256']=hashlib.sha256(path.read_bytes()).hexdigest()
    outputs=log['dsl']['components']['TokenChunker:WholeEducation']['obj']['params']['outputs']
    parsed=outputs['chunks']['value'][0]['text']+parsed_table
    outputs['chunks']['value'][0]['text']=parsed
    newhash=hashlib.sha256(parsed.encode()).hexdigest()
    remote['meta_fields']['education_source_sha256']=newhash
    for node,field in [('CodeExec:EducationGate','json'),('Tokenizer:EducationIndex','chunks')]:
        for row in log['dsl']['components'][node]['obj']['params']['outputs'][field]['value']:
            row['metadata']['education_source_sha256']=newhash
    return task,remote,log,chunks

def test_literal_markdown_table_rendering_keeps_source_proof(tmp_path):
    args=table_fixture(tmp_path)
    assert execution_source_proof(*args) is not None

@pytest.mark.parametrize('html',[HTML.replace('20','21'),HTML.replace('作用','别的列名'),HTML.replace('<td>right()</td>','<td>left()</td>'),HTML.replace('<td>向右转动90度</td>',''),HTML.replace('<td>forward()</td>','<td>forward() 改写</td>')])
def test_any_changed_or_missing_table_cell_still_fails(tmp_path,html):
    assert execution_source_proof(*table_fixture(tmp_path,parsed_table=html)) is None

def test_fenced_table_must_remain_literal(tmp_path):
    assert execution_source_proof(*table_fixture(tmp_path,'```md\n'+TABLE+'```\n','```md\n'+HTML+'```\n')) is None

def test_inline_link_markup_is_not_treated_as_literal_cell_text(tmp_path):
    source=TABLE.replace('forward()', '[前进](https://example.invalid)')
    parsed=HTML.replace('forward()', '[前进](https://example.invalid)')
    assert execution_source_proof(*table_fixture(tmp_path,source,parsed)) is None

def test_table_equivalence_does_not_relax_prose_or_metadata(tmp_path):
    args=table_fixture(tmp_path)
    args[2]['dsl']['components']['TokenChunker:WholeEducation']['obj']['params']['outputs']['chunks']['value'][0]['text']+='伪造额外结论。'
    assert execution_source_proof(*args) is None

def test_lost_row_order_is_rejected(tmp_path):
    altered=HTML.replace('<td>forward()</td>','<td>TEMP</td>').replace('<td>right()</td>','<td>forward()</td>').replace('<td>TEMP</td>','<td>right()</td>')
    assert execution_source_proof(*table_fixture(tmp_path,parsed_table=altered)) is None
