import base64
import zipfile

from paperflow.reading import prepare_reading
from paperflow.package import sha,load


def test_docx_raw_view_keeps_order_tables_and_actual_image_type(tmp_path):
    source=tmp_path/'original.docx'
    xml='''<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"><w:body>
    <w:p><w:r><w:t>研究问题</w:t></w:r></w:p>
    <w:tbl><w:tr><w:tc><w:p><w:r><w:t>指标关系</w:t></w:r></w:p></w:tc></w:tr></w:tbl>
    <w:p><w:r><w:t>研究步骤</w:t></w:r></w:p></w:body></w:document>'''
    pixel=base64.b64decode('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+j2ioAAAAASUVORK5CYII=')
    with zipfile.ZipFile(source,'w') as z:
        z.writestr('word/document.xml',xml)
        z.writestr('word/media/image1.jpg',pixel)
    before=sha(source)
    index=prepare_reading(source,tmp_path/'view')
    text=(tmp_path/'view'/'source.txt').read_text(encoding='utf-8')
    assert text.index('研究问题')<text.index('指标关系')<text.index('研究步骤')
    assert index['source_sha256']==before==sha(source)
    assert index['images'][0]['file'].endswith('.png')
    assert index['images'][0]['original_part']=='word/media/image1.jpg'
    assert index['reading_scope']=='raw_source_no_semantic_decisions'


def test_docx_native_chart_and_relationship_parts_are_readable(tmp_path):
    source=tmp_path/'chart.docx'
    with zipfile.ZipFile(source,'w') as z:
        z.writestr('word/document.xml','<document/>')
        z.writestr('word/charts/chart1.xml','<chart><title>资产周转率</title><value>1.5</value></chart>')
        z.writestr('word/charts/_rels/chart1.xml.rels','<Relationships/>')
        z.writestr('word/diagrams/data1.xml','<diagram>分析流程</diagram>')
    index=prepare_reading(source,tmp_path/'view')
    parts={r['part']:r for r in index['parts']}
    assert 'word/charts/chart1.xml' in parts
    assert 'word/charts/_rels/chart1.xml.rels' in parts
    assert 'word/diagrams/data1.xml' in parts
    assert '资产周转率' in (tmp_path/'view'/parts['word/charts/chart1.xml']['file']).read_text(encoding='utf-8')


def test_archive_member_images_have_distinct_ids_and_correct_bytes(tmp_path,monkeypatch):
    import paperflow.reading as reading
    from paperflow.package import save
    def fake_pdf(source,output):
        file=output/'images'/'page_00001.png';file.parent.mkdir(parents=True,exist_ok=True)
        file.write_bytes(source.read_bytes())
        return 'text',[dict(id='page_00001',file='images/page_00001.png',sha256=sha(file))],[]
    monkeypatch.setattr(reading,'_pdf',fake_pdf)
    source=tmp_path/'papers.zip'
    with zipfile.ZipFile(source,'w') as z:
        z.writestr('a.pdf',b'first image');z.writestr('b.pdf',b'second image')
    view=tmp_path/'view';index=prepare_reading(source,view)
    first,second=index['images']
    assert first['id']!=second['id']
    assets=tmp_path/'assets.json'
    save(assets,dict(items=[dict(id='a',storage='image',source_image_id=first['id'],file='assets/a.png')]))
    reading.materialize_images(view,assets,tmp_path/'published')
    assert (tmp_path/'published'/'assets/a.png').read_bytes()==b'first image'
