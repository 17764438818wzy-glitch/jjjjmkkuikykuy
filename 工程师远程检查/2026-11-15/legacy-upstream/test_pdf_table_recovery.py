"""PDF table targets need original visual evidence and unchanged native data."""
import copy
import io
import json
import math

import fitz
import pytest
from PIL import Image

from auto_evidence import Rejected, finish_evidence, media_privacy
from evidence_tasks import resolve_scoped


SOURCE_TEXT = 'Label Amount Rate\nNorth (1,250.00) -2.50%\nSouth 0.00 3.00%'
SOURCE_ROWS = [['Label', 'Amount', 'Rate'], ['North', '(1,250.00)', '-2.50%'], ['South', '0.00', '3.00%']]


def table_target(tmp_path):
    source = tmp_path / 'source.pdf'
    with fitz.open() as pdf:
        page = pdf.new_page(width=600, height=800)
        page.insert_text((60, 90), 'Table 3 Research records')
        for y in (100, 140, 220):page.draw_line((60, y), (530, y))
        for row, y in zip(SOURCE_ROWS, (125, 165, 205)):
            for value, x in zip(row, (70, 220, 420)):page.insert_text((x, y), value, fontsize=10)
        pdf.save(source)
    position = {'page': 1, 'bbox': [60, 100, 530, 220], 'vector_region': True}
    meta = {'id': 'table_source', 'title': 'Research records', 'formulas': [], 'resources': [],
            'issues': [{'code': 'pdf_vector_review', 'position': position, 'status': 'open', 'message': 'Uncertain table grid'}],
            'blocks': [{'id': 'table_region', 'type': 'paragraph', 'text': '[解析说明：疑难表格需恢复。]',
                        'position': position, 'unresolved_object': True, 'table_candidate': True,
                        'table_fragment': False, 'caption': 'Table 3 Research records',
                        'locator_texts': SOURCE_TEXT.splitlines()},
                       {'id': 'native_rows', 'type': 'paragraph', 'text': SOURCE_TEXT,
                        'position': {'page': 1, 'bbox': [70, 115, 510, 208]}, 'heading_level': 0}]}
    package = tmp_path / 'package';(package / 'assets').mkdir(parents=True)
    return source, package, meta


def image_entry(**changes):
    entry = {'after_id': 'table_region', 'kind': 'image', 'bbox': [.1, .125, 530 / 600, .275],
             'text': SOURCE_TEXT, 'description': 'Original table region', 'caption': 'Table 3 Research records',
             'structured_table': {'rows': copy.deepcopy(SOURCE_ROWS), 'header_rows': [0], 'units': '', 'notes': []}}
    entry.update(changes)
    return entry


def rendered_crop(source,page,bbox,*,native_screenshot=False):
    with fitz.open(source) as pdf:
        original=pdf[page-1]
        if native_screenshot:
            blob=original.get_pixmap(clip=fitz.Rect(bbox),matrix=fitz.Matrix(2,2)).tobytes('png')
            return Image.open(io.BytesIO(blob)).convert('RGB')
        blob=original.get_pixmap(matrix=fitz.Matrix(1.5,1.5)).tobytes('png')
        image=Image.open(io.BytesIO(blob)).convert('RGB')
        box=[bbox[0]/original.rect.width*image.width,bbox[1]/original.rect.height*image.height,
             bbox[2]/original.rect.width*image.width,bbox[3]/original.rect.height*image.height]
        return image.crop((math.floor(box[0]),math.floor(box[1]),math.ceil(box[2]),math.ceil(box[3])))


class Agent:
    def __init__(self, entries=(), ignored=(), *, source=None, visual_rows=None):
        self.entries = list(entries);self.ignored = list(ignored);self.calls = [];self.payload = None
        self.source=source;self.visual_rows=visual_rows;self.privacy_checks=[]

    def visual_result(self,payload,images):
        assert len(images)==1
        assert payload['phase'] in {'identify','locate_privacy_regions','verify_masked'}
        assert payload['position']['page']==1
        observed=Image.open(io.BytesIO(images[0])).convert('RGB')
        expected=rendered_crop(self.source,1,[60,100,530,220])
        assert observed.size==expected.size and observed.tobytes()==expected.tobytes()
        self.privacy_checks.append((payload['id'],payload['phase']))
        result={'decision':'pass','reason':'Original crop pixels checked independently','research':True,
                'sensitive_boxes':[],'text':SOURCE_TEXT,'description':'Original table region'}
        if self.visual_rows is not None:
            result['structured_table']={'rows':copy.deepcopy(self.visual_rows),'header_rows':[0],'units':'','notes':[]}
        return result

    def call(self, task, payload, images=()):
        self.calls.append(task);self.payload = payload if task == 'repair' else self.payload
        if task == 'repair':
            return {'decision': 'pass', 'reason': 'Original table examined', 'insertions': self.entries,
                    'nonresearch_objects': self.ignored, 'sensitive_boxes': []}
        if task == 'privacy_text':
            return {'decision': 'pass', 'reason': 'No identity fields', 'sensitive_strings': []}
        if task == 'privacy_image':
            return self.visual_result(payload,images)
        raise AssertionError('Unexpected task: ' + task)


def test_recognized_pdf_table_cannot_be_resolved_as_decoration(tmp_path):
    source, package, meta = table_target(tmp_path)
    agent = Agent(ignored=[{'after_id': 'table_region', 'reason': 'Only table rules, not a research picture'}])
    with pytest.raises(Rejected, match='表格'):
        resolve_scoped(meta, package, source, tmp_path / 'work', 80, agent, lambda *a: None)
    assert meta['issues'][0]['status'] == 'open'


@pytest.mark.parametrize('kind', ['text', 'table'])
def test_pdf_table_data_without_original_crop_is_not_complete(tmp_path, kind):
    source, package, meta = table_target(tmp_path)
    entry = {'after_id': 'table_region', 'kind': kind, 'text': SOURCE_TEXT, 'rows': SOURCE_ROWS}
    with pytest.raises(Rejected, match='原图|截图'):
        resolve_scoped(meta, package, source, tmp_path / 'work', 80, Agent([entry]), lambda *a: None)
    assert meta['issues'][0]['status'] == 'open'


def test_pdf_table_crop_cannot_omit_the_last_row(tmp_path):
    source, package, meta = table_target(tmp_path)
    with pytest.raises(Rejected, match='区域|完整'):
        resolve_scoped(meta, package, source, tmp_path / 'work', 80,
                       Agent([image_entry(bbox=[.1, .125, 530 / 600, .225])]), lambda *a: None)


def test_pdf_table_ocr_cannot_change_known_numeric_strings(tmp_path):
    source, package, meta = table_target(tmp_path)
    entry = image_entry(text=SOURCE_TEXT.replace('-2.50%', '2.50%'))
    with pytest.raises(Rejected, match='原生|数值|文字'):
        resolve_scoped(meta, package, source, tmp_path / 'work', 80, Agent([entry]), lambda *a: None)


@pytest.mark.parametrize('alteration', ['decimal', 'missing', 'added', 'invented_label', 'invented_header', 'swapped_rows'])
def test_unproven_model_grid_is_not_published_but_original_visual_and_text_survive(tmp_path, alteration):
    source, package, meta = table_target(tmp_path)
    entry = image_entry()
    rows = entry['structured_table']['rows']
    if alteration == 'decimal':rows[1][1] = '(1,250.0)'
    if alteration == 'missing':rows.pop()
    if alteration == 'added':rows[2][1] = '999.00'
    if alteration == 'invented_label':rows[1][0] = 'NEW_LABEL North'
    if alteration == 'invented_header':
        meta['blocks'][0]['table_fragment'] = True
        rows.insert(0, ['Invented heading', 'Balance', 'Percentage'])
    if alteration == 'swapped_rows':rows[1][1], rows[2][1] = rows[2][1], rows[1][1]
    agent = Agent([entry],source=source)
    finish_evidence(meta, package, agent, source, tmp_path / 'work', 80, lambda *a: None)
    image = next(b for b in meta['blocks'] if b['type'] == 'image')
    assert not image.get('structured_table')
    assert not meta['resources'][0].get('structured_table')
    assert '程序生成' in image['description']['text'] and '未发布' in image['description']['text']
    assert not list((package / 'assets' / 'tables').glob('*.csv'))
    assert next(b for b in meta['blocks'] if b['id'] == 'native_rows')['text'] == SOURCE_TEXT
    from knowledge_package import knowledge_files
    inventory = knowledge_files(package)
    assert not any(name.startswith('assets/tables/') for name in inventory)
    figure_path = next((package / 'assets' / 'figures').glob('*.json'))
    figure = json.loads(figure_path.read_text(encoding='utf-8'))
    assert figure['related_table_ids'] == [] and figure['chart_spec']['dataset_id'] is None
    assert (package / meta['resources'][0]['local_path']).is_file()


def test_invalid_extra_table_insertion_cannot_bypass_image_grid_validation(tmp_path):
    source, package, meta = table_target(tmp_path)
    rows = copy.deepcopy(SOURCE_ROWS)
    rows[1][1], rows[2][1] = rows[2][1], rows[1][1]
    entries = [image_entry(), {'after_id': 'table_region', 'kind': 'table', 'rows': rows, 'header_rows': [0]}]
    finish_evidence(meta, package, Agent(entries,source=source), source, tmp_path / 'work', 80, lambda *a: None)
    assert not any(block['type'] == 'table' for block in meta['blocks'])
    assert not any(block.get('structured_table') for block in meta['blocks'])
    assert not any(resource.get('structured_table') for resource in meta['resources'])
    from knowledge_package import knowledge_files
    assert not any(name.startswith('assets/tables/') for name in knowledge_files(package))
    assert len([block for block in meta['blocks'] if block['type'] == 'image']) == 1


def test_table_candidate_context_contains_original_location_and_text(tmp_path):
    source, package, meta = table_target(tmp_path)
    agent = Agent([image_entry()])
    resolve_scoped(meta, package, source, tmp_path / 'work', 80, agent, lambda *a: None)
    known = agent.payload['known_blocks'][0]
    assert known['table_candidate'] is True
    assert known['table_bbox'] == pytest.approx([.1, .125, 530 / 600, .275])
    assert known['native_text'] == SOURCE_TEXT
    assert known['caption'] == 'Table 3 Research records'


def test_one_repair_exports_original_table_after_independent_pixel_recheck(tmp_path):
    source, package, meta = table_target(tmp_path)
    agent = Agent([image_entry()],source=source,visual_rows=SOURCE_ROWS)
    finish_evidence(meta, package, agent, source, tmp_path / 'work', 80, lambda *a: None)
    assert agent.calls == ['repair', 'privacy_image', 'privacy_image', 'privacy_text']
    assert agent.privacy_checks==[('visual_p1_1','locate_privacy_regions'),('visual_p1_1','verify_masked')]
    assert meta['resources'][0]['masked_observation_verified'] is True
    image = next(b for b in meta['blocks'] if b['type'] == 'image')
    assert image['structured_table']['rows'] == SOURCE_ROWS
    assert len(meta['resources']) == 1 and (package / meta['resources'][0]['local_path']).is_file()
    assert meta['issues'][0]['status'] == 'resolved'
    assert meta['blocks'][0]['locator_texts'] == SOURCE_TEXT.splitlines()
    table_files = list((package / 'assets' / 'tables').glob('*.json'))
    assert table_files and '(1,250.00)' in table_files[0].read_text(encoding='utf-8')
    from knowledge_package import knowledge_files
    knowledge_files(package)
    table = json.loads(table_files[0].read_text(encoding='utf-8'))
    figure_file = next((package / 'assets' / 'figures').glob('*.json'))
    figure = json.loads(figure_file.read_text(encoding='utf-8'))
    assert figure['related_table_ids'] == [table['id']]
    assert table['original_visuals'] and table['preview']['original_appearance']
    assert (package / table['csv_path']).is_file()


@pytest.mark.parametrize('verdict',['reject','not_research'])
def test_protected_table_crop_cannot_pass_when_independent_recheck_fails(tmp_path,verdict):
    source,package,meta=table_target(tmp_path)
    original_bytes=source.read_bytes()
    class RejectingReview(Agent):
        def visual_result(self,payload,images):
            result=super().visual_result(payload,images)
            if payload['phase']=='verify_masked':
                if verdict=='reject':
                    result.update(decision='reject',failure_kind='content',reason='Table content cannot be verified')
                else:
                    result.update(research=False,reason='Original research content not established')
            return result
    agent=RejectingReview([image_entry()],source=source,visual_rows=SOURCE_ROWS)
    with pytest.raises(Rejected,match='复核'):
        finish_evidence(meta,package,agent,source,tmp_path/'work',80,lambda *args:None)
    assert agent.calls==['repair','privacy_image','privacy_image']
    assert agent.privacy_checks==[('visual_p1_1','locate_privacy_regions'),('visual_p1_1','verify_masked')]
    assert not meta['resources'][0].get('masked_observation_verified')
    assert not (package/'ingest.md').exists()
    assert not list((package/'assets'/'tables').glob('*.json'))
    assert source.read_bytes()==original_bytes


@pytest.mark.parametrize('phase',['identify','locate_privacy_regions'])
def test_captionless_pdf_table_candidate_cannot_be_discarded_as_decoration(tmp_path,phase):
    source,package,meta=table_target(tmp_path)
    # The selected original crop contains only the table; its caption lies
    # outside the crop. Protection must come from native table recognition.
    meta['blocks'][0]['caption']=''
    class DecorationAgent(Agent):
        def visual_result(self,payload,images):
            result=super().visual_result(payload,images)
            assert payload['caption']==''
            result.update(research=False,nonresearch_kind='decoration',reason='Model wrongly classifies table rules as decoration')
            return result
    agent=DecorationAgent([image_entry(caption='')],source=source)
    resolve_scoped(meta,package,source,tmp_path/'work',80,agent,lambda *args:None)
    resource=meta['resources'][0];image=next(block for block in meta['blocks'] if block['type']=='image')
    assert image['table_candidate'] and image['caption']==''
    if phase=='identify':resource.update(visual_processed=False,privacy_status='pending')
    with pytest.raises(Rejected,match='不能按装饰'):
        media_privacy(meta,package,agent,lambda *args:None)
    assert agent.privacy_checks==[('visual_p1_1',phase)]
    assert resource in meta['resources'] and image in meta['blocks']
    assert not meta.get('excluded_resources')
    assert not resource.get('masked_observation_verified')
    assert next(block for block in meta['blocks'] if block['id']=='native_rows')['text']==SOURCE_TEXT


def test_visual_recheck_cannot_overrule_failed_native_grid_validation(tmp_path):
    source,package,meta=table_target(tmp_path)
    entry=image_entry();entry['structured_table']['rows'][1][1]='(1,250.0)'
    # Even a plausible grid read from the real crop needs native geometry
    # validation; a privacy check must not erase the earlier image-only ruling.
    agent=Agent([entry],source=source,visual_rows=SOURCE_ROWS)
    finish_evidence(meta,package,agent,source,tmp_path/'work',80,lambda *args:None)
    image=next(block for block in meta['blocks'] if block['type']=='image')
    assert image['structured_table_review']['status']=='image_only'
    assert not image.get('structured_table')
    assert not meta['resources'][0].get('structured_table')
    assert '程序生成' in image['description']['text'] and '未发布' in image['description']['text']
    assert not list((package/'assets'/'tables').glob('*.csv'))
    assert next(b for b in meta['blocks'] if b['id']=='native_rows')['text']==SOURCE_TEXT


@pytest.mark.parametrize('change',['rows','columns','numeric_lexeme'])
def test_fresh_visual_grid_changes_cannot_replace_a_native_validated_grid(tmp_path,change):
    source,package,meta=table_target(tmp_path)
    class DivergingReview(Agent):
        def visual_result(self,payload,images):
            result=super().visual_result(payload,images)
            if payload['phase']=='verify_masked':
                rows=result['structured_table']['rows']
                if change=='rows':rows[1],rows[2]=rows[2],rows[1]
                elif change=='columns':
                    for row in rows:row[1],row[2]=row[2],row[1]
                else:rows[1][1]='(1,250.0)'
            return result
    agent=DivergingReview([image_entry()],source=source,visual_rows=SOURCE_ROWS)
    finish_evidence(meta,package,agent,source,tmp_path/'work',80,lambda *args:None)
    assert agent.privacy_checks==[('visual_p1_1','locate_privacy_regions'),('visual_p1_1','verify_masked')]
    image=next(block for block in meta['blocks'] if block['type']=='image')
    assert image['structured_table_review']['status']=='image_only'
    assert not image.get('structured_table') and not meta['resources'][0].get('structured_table')
    assert '程序生成' in image['description']['text'] and '未发布' in image['description']['text']
    assert not list((package/'assets'/'tables').glob('*.csv'))
    assert next(b for b in meta['blocks'] if b['id']=='native_rows')['text']==SOURCE_TEXT
    assert (package/meta['resources'][0]['local_path']).is_file()


def test_uncertain_grid_keeps_original_image_and_native_text_without_inventing_cells(tmp_path):
    source, package, meta = table_target(tmp_path)
    agent = Agent([image_entry(structured_table=None)])
    resolve_scoped(meta, package, source, tmp_path / 'work', 80, agent, lambda *a: None)
    assert not any(b['type'] == 'table' for b in meta['blocks'])
    assert any(b['type'] == 'image' for b in meta['blocks'])
    assert next(b for b in meta['blocks'] if b['id'] == 'native_rows')['text'] == SOURCE_TEXT


def test_fragment_exports_its_own_page_image_and_rows_without_inheriting_headers(tmp_path):
    from evidence import build_package
    from knowledge_package import knowledge_files
    from test_common_pdf import continuation_document
    source = tmp_path / 'fragment.pdf'
    pdf = continuation_document();pdf.save(source);pdf.close()
    package = tmp_path / 'package'
    meta = build_package(source, package, {'id': 'fragment', 'title': 'Quarterly records'}, redact_content=False)
    rows = [['Supplement', '44.00', '-3.00%', '0'], ['Ratio', '5.40%', '', '7.00%']]
    native_table=next(b for b in meta['blocks'] if b['type']=='table')
    native_rows=[[native_table['cells'][i]['text'] for i in row] for row in native_table['grid']]
    fragment_target=next(b for b in meta['blocks'] if b.get('table_fragment'))
    expected_pixels={1:rendered_crop(source,1,native_table['position']['bbox'],native_screenshot=True),
                     2:rendered_crop(source,2,fragment_target['position']['bbox'])}

    class FragmentAgent(Agent):
        def call(self, task, payload, images=()):
            if task == 'repair':
                target = next(b for b in payload['known_blocks'] if b.get('table_fragment'))
                self.entries = [image_entry(after_id=target['id'], page=2, bbox=target['table_bbox'],
                    text=target['native_text'], caption='', structured_table={
                        'rows': rows, 'header_rows': [], 'units': '', 'notes': []})]
            return super().call(task, payload, images)

        def visual_result(self,payload,images):
            assert len(images)==1
            page=payload['position']['page'];expected=expected_pixels[page]
            observed=Image.open(io.BytesIO(images[0])).convert('RGB')
            assert observed.size==expected.size and observed.tobytes()==expected.tobytes()
            assert payload['phase'] in {'identify','locate_privacy_regions','verify_masked'}
            self.privacy_checks.append((page,payload['phase']))
            actual_rows=native_rows if page==1 else rows
            return {'decision':'pass','reason':'No identity in the actual table pixels','research':True,
                    'sensitive_boxes':[],'text':'\n'.join(' '.join(row) for row in actual_rows),
                    'description':'Original first-page table' if page==1 else 'Original continuation table fragment',
                    'structured_table':{'rows':copy.deepcopy(actual_rows),'header_rows':[0] if page==1 else [],'units':'','notes':[]}}

    agent = FragmentAgent()
    finish_evidence(meta, package, agent, source, tmp_path / 'work', 80, lambda *a: None)
    assert agent.calls == ['repair', 'privacy_image', 'privacy_image', 'privacy_image', 'privacy_text']
    assert agent.privacy_checks==[(1,'identify'),(2,'locate_privacy_regions'),(2,'verify_masked')]
    assert all(r['masked_observation_verified'] for r in meta['resources'])
    knowledge_files(package)
    figures = [json.loads(p.read_text(encoding='utf-8')) for p in (package / 'assets' / 'figures').glob('*.json')]
    fragment = next(f for f in figures if f['position']['page'] == 2)
    assert '程序生成' in fragment['description']['text'] and '尚未确认' in fragment['description']['text']
    table = json.loads((package / 'assets' / 'tables' / (fragment['related_table_ids'][0] + '.json')).read_text(encoding='utf-8'))
    assert table['rows'] == rows and table['header_rows'] == []
    assert table['position']['page'] == 2 and table['original_visuals']


def test_pixel_crop_coordinates_are_checked_against_pdf_page_geometry(tmp_path):
    source, package, meta = table_target(tmp_path)
    agent = Agent([image_entry(bbox=[90, 150, 795, 330], coordinate_space='pixel')])
    resolve_scoped(meta, package, source, tmp_path / 'work', 80, agent, lambda *a: None)
    image = next(b for b in meta['blocks'] if b['type'] == 'image')
    assert image['position']['bbox'] == pytest.approx([60, 100, 530, 220])
