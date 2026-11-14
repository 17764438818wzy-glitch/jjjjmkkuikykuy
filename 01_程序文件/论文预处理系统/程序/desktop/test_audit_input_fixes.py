"""Input audit regressions using synthetic documents and model responses."""
import copy
import hashlib
import io
import json

import pytest
from PIL import Image, ImageDraw

from auto_evidence import Rejected, media_privacy, text_privacy
from evidence import build_package, export_package
from knowledge_package import publish_knowledge
from test_graphics_recovery import docx, parse


PRIVATE_MEDIA = 'Private_Submitter_Name'


def cropped_document(path):
    stream = io.BytesIO()
    image = Image.new('RGB', (160, 120), 'white')
    ImageDraw.Draw(image).rectangle((30, 30, 130, 90), fill='blue')
    image.save(stream, format='PNG')
    picture = '''<w:p><w:r><w:drawing>
      <pic:pic xmlns:pic="http://schemas.openxmlformats.org/drawingml/2006/picture">
        <pic:blipFill><a:blip r:embed="rId1"/>
          <a:srcRect l="10000" t="10000" r="10000" b="10000"/>
          <a:stretch><a:fillRect/></a:stretch>
        </pic:blipFill>
        <pic:spPr><a:prstGeom prst="rect"><a:avLst/></a:prstGeom></pic:spPr>
      </pic:pic></w:drawing></w:r></w:p>'''
    part = 'word/media/' + PRIVATE_MEDIA + '.png'
    docx(path, picture,
         '<Relationship Id="rId1" Target="media/' + PRIVATE_MEDIA + '.png"/>',
         {part: stream.getvalue()})
    return hashlib.sha256(stream.getvalue()).hexdigest()


class ResearchAgent:
    """Replace only external model calls; extraction and publication stay real."""
    def call(self, task, payload, **kwargs):
        if task == 'privacy_image':
            return {'decision': 'pass', 'reason': 'Readable research data',
                    'research': True, 'text': 'Original value 123',
                    'description': 'Research chart', 'sensitive_boxes': []}
        if task == 'privacy_text':
            return {'decision': 'pass', 'reason': 'Research text', 'sensitive_strings': []}
        raise AssertionError(task)


@pytest.mark.parametrize('old_checkpoint', [False, True])
def test_published_crop_metadata_never_contains_original_media_identity(tmp_path, old_checkpoint):
    source = tmp_path / 'source.docx'
    original_hash = cropped_document(source)
    before = source.read_bytes()
    package = tmp_path / 'package'
    meta = build_package(source, package,
                         {'id': 'audit', 'title': 'Research data', 'major': 'Accounting'},
                         redact_content=False)
    media_privacy(meta, package, ResearchAgent(), lambda *args: None)
    text_privacy(meta, ResearchAgent(), lambda *args: None)
    if old_checkpoint:
        for value in meta['resources'] + meta['blocks']:
            if 'display_crop' in value:
                crop = value['display_crop']
                crop['source_part'] = 'word/media/' + PRIVATE_MEDIA + '.png'
                crop['annotation'] = PRIVATE_MEDIA
                crop['source_rectangle']['annotation'] = PRIVATE_MEDIA
    meta.update(preprocessing_complete=True, privacy_status='approved')
    export_package(package, meta)
    published = tmp_path / 'published'
    publish_knowledge(package, published)

    for path in published.rglob('*'):
        if path.suffix in {'.json', '.md'}:
            assert PRIVATE_MEDIA not in path.read_text(encoding='utf-8'), path
    public = json.loads((published / 'metadata.json').read_text(encoding='utf-8'))
    crop = public['resources'][0]['display_crop']
    assert crop['source_sha256'] == original_hash
    assert crop['source_rectangle'] == {'l': 10000, 't': 10000, 'r': 10000, 'b': 10000}
    assert crop['pixel_bbox'] == [16, 12, 144, 108]
    assert Image.open(published / public['resources'][0]['local_path']).size == (128, 96)
    assert 'Original value 123' in (published / 'ingest.md').read_text(encoding='utf-8')
    assert source.read_bytes() == before


def small_resource(package, protection):
    (package / 'assets').mkdir()
    (package / '_review').mkdir()
    image = Image.new('RGB', (48, 48), 'white')
    ImageDraw.Draw(image).line((2, 40, 44, 8), fill='blue', width=2)
    image.save(package / '_review/source.png')
    resource = {'id': 'r', 'kind': 'table_screenshot' if protection == 'table' else 'image',
                'local_path': '_review/source.png', 'position': {'page': 1}}
    block = {'id': 'b', 'type': 'image', 'resource_id': 'r',
             'caption': 'Figure 1 Research chart' if protection == 'caption' else '',
             'native_data_available': protection == 'native'}
    blocks = [block]
    if protection == 'table':
        blocks.append({'id': 't', 'type': 'table', 'screenshot_ids': ['r'],
                       'cells': [{'row': 0, 'column': 0, 'text': '123'}]})
    return {'resources': [resource], 'blocks': blocks}


class DecorationAgent:
    def call(self, task, payload, **kwargs):
        assert task == 'privacy_image'
        return {'decision': 'pass', 'reason': 'Decoration', 'research': False,
                'nonresearch_kind': 'decoration', 'text': '',
                'description': 'Line', 'sensitive_boxes': []}


@pytest.mark.parametrize('protection', ['table', 'native', 'caption'])
def test_small_protected_research_evidence_is_not_discarded_without_coverage(tmp_path, protection):
    meta = small_resource(tmp_path, protection)
    before = copy.deepcopy(meta)
    with pytest.raises(Rejected):
        media_privacy(meta, tmp_path, DecorationAgent(), lambda *args: None)
    assert meta == before
    assert (tmp_path / '_review/source.png').is_file()


def test_small_unlabelled_decoration_can_still_be_removed(tmp_path):
    meta = small_resource(tmp_path, '')
    media_privacy(meta, tmp_path, DecorationAgent(), lambda *args: None)
    assert not meta['resources'] and not meta['blocks']


def connector_document(path, *, arrow=False, caption=''):
    arrowhead = '<a:ln><a:tailEnd type="triangle"/></a:ln>' if arrow else ''
    connector = '<w:drawing><a:graphic><a:graphicData><wps:wsp><wps:spPr>'
    connector += '<a:prstGeom prst="line"><a:avLst/></a:prstGeom>' + arrowhead
    connector += '</wps:spPr></wps:wsp></a:graphicData></a:graphic></w:drawing>'
    body = '<w:p><w:r>' + connector + '</w:r></w:p>'
    if caption:
        body += '<w:p><w:r><w:t>' + caption + '</w:t></w:r></w:p>'
    return docx(path, body)


@pytest.mark.parametrize('arrow,caption', [
    (True, ''), (True, 'Figure 1 Experimental direction'),
    (False, '图1 实验路径'), (False, 'Figure 1 Experimental direction'),
])
def test_research_connector_keeps_recovery_issue_and_original_object(tmp_path, arrow, caption):
    source = connector_document(tmp_path / 'connector.docx', arrow=arrow, caption=caption)
    before = source.read_bytes()
    blocks, _, issues = parse(source)
    assert any(b.get('graphic_object') for b in blocks)
    assert any(i['code'] == 'embedded_object_review' for i in issues)
    assert source.read_bytes() == before


def test_plain_unlabelled_connector_remains_ignorable_layout(tmp_path):
    source = connector_document(tmp_path / 'connector.docx')
    blocks, _, issues = parse(source)
    assert not any(b.get('graphic_object') for b in blocks)
    assert not issues


def test_unlabelled_line_in_research_drawing_keeps_its_member_position(tmp_path):
    source = connector_document(tmp_path / 'connector.docx')
    import zipfile
    with zipfile.ZipFile(source) as archive:
        parts = {name: archive.read(name) for name in archive.namelist()}
    textbox = '''<w:p><w:r><w:pict><v:rect><w:txbxContent>
        <w:p><w:r><w:t>Experimental stage</w:t></w:r></w:p>
        </w:txbxContent></v:rect></w:pict></w:r></w:p>'''
    parts['word/document.xml'] = parts['word/document.xml'].replace(
        b'</w:body>', textbox.encode() + b'</w:body>')
    with zipfile.ZipFile(source, 'w') as archive:
        for name, data in parts.items():
            archive.writestr(name, data)
    blocks, _, issues = parse(source)
    group = next(b for b in blocks if b.get('graphic_object'))
    assert len(group.get('member_positions', [])) == 2
    assert any(i['code'] == 'embedded_object_review' for i in issues)
