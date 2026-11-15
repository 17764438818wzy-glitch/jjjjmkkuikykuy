import json
from pathlib import Path
import subprocess
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'local-upstream-alignment-20261030' / 'tests'))
from local_prepare import prepare_registered, prepare_registered_attachments


@pytest.fixture
def registered(tmp_path):
    upstream = Path(__file__).resolve().parents[2] / 'first-step-control-20261029'
    fixtures = Path(__file__).resolve().parents[2] / 'first-step-auto-registration-20261030' / 'tests'
    script = """
import json, sys
from pathlib import Path
sys.path[:0] = sys.argv[1:3]
from paperflow_prefect.first_step_registration import register_delivery
from paperflow_prefect.group_classification import process_one_group
from test_first_step_registration import FixtureJudge, evidence
base=Path(sys.argv[3]); group=base/'library'/'person'; group.mkdir(parents=True)
(group/'paper.txt').write_text('封面\\n摘要\\n测试正文。', encoding='utf8')
(group/'data.csv').write_text('x,y\\n1,2', encoding='utf8')
delivery=process_one_group(group, group.parent, base/'task', FixtureJudge, evidence, verified_new=True)
root=base/'receipts'
receipt=register_delivery(delivery['delivery_dir'], receipt_root=root, history_roots=(), lock_path=base/'receipt.lock')
item=dict(status='passed', registration_status='registered', delivery_complete=True,
          group_id=receipt['group_id'], paper_id=receipt['paper_id'], receipt_id=receipt['receipt_id'],
          receipt_path=receipt['record_path'], delivery_dir=delivery['delivery_dir'])
print(json.dumps(dict(group=str(group), root=str(root), receipt=receipt, item=item), ensure_ascii=False))
"""
    proc = subprocess.run([sys.executable, '-B', '-X', 'utf8', '-c', script,
                           str(upstream), str(fixtures), str(tmp_path)], capture_output=True,
                          text=True, encoding='utf8', timeout=30)
    assert proc.returncode == 0, proc.stderr
    value = json.loads(proc.stdout)
    return Path(value['group']), Path(value['root']), value['receipt'], value['item']


def test_registered_delivery_uses_same_body_and_keeps_attachments_separate(registered, tmp_path):
    _, receipt_root, receipt, item = registered
    original_receipt = Path(item['receipt_path']).read_bytes()
    catalog = tmp_path / 'catalog.json'
    catalog.write_text('{"majors": []}', encoding='utf8')
    result = prepare_registered(item, receipt_root=receipt_root, work_dir=tmp_path / 'work', catalog_path=catalog)
    bundle = json.loads(Path(result['material_path']).read_text(encoding='utf8'))
    assert result['paper_id'] == receipt['paper_id']
    assert result['receipt_id'] == receipt['receipt_id']
    assert Path(bundle['source']['file']).is_relative_to(Path(item['delivery_dir']) / 'files')
    assert len(result['attachments']) == 1
    assert not bundle['attachments']
    assert Path(item['receipt_path']).read_bytes() == original_receipt


def test_direct_preparation_cannot_replace_registered_source_binding(registered, tmp_path):
    import hashlib
    from local_prepare import prepare_source
    _, receipt_root, _, item = registered
    catalog = tmp_path / 'catalog.json'
    catalog.write_text('{"majors": []}', encoding='utf8')
    work = tmp_path / 'work'
    prepare_registered(item, receipt_root=receipt_root, work_dir=work, catalog_path=catalog)
    other = tmp_path / 'other.txt'
    other.write_text('摘要\n不同论文', encoding='utf8')
    with pytest.raises(ValueError, match='绑定'):
        prepare_source(other, hashlib.sha256(other.read_bytes()).hexdigest(), work, catalog)


def test_registered_delivery_source_cannot_use_new_unbound_direct_workdir(registered, tmp_path):
    from local_prepare import prepare_source
    _, receipt_root, _, item = registered
    catalog = tmp_path/'catalog.json'; catalog.write_text('{"majors": []}',encoding='utf8')
    first = prepare_registered(item, receipt_root=receipt_root, work_dir=tmp_path/'bound', catalog_path=catalog)
    binding = json.loads((Path(first['material_path']).parent/'registered-binding.json').read_text(encoding='utf8'))
    with pytest.raises(ValueError,match='登记'):
        prepare_source(binding['source_path'],binding['source_sha256'],tmp_path/'unbound',catalog)


def test_registered_work_cannot_be_placed_inside_delivery(registered, tmp_path):
    _, receipt_root, _, item = registered
    catalog = tmp_path / 'catalog.json'
    catalog.write_text('{"majors": []}', encoding='utf8')
    with pytest.raises(ValueError):
        prepare_registered(item, receipt_root=receipt_root, work_dir=Path(item['delivery_dir']) / 'work', catalog_path=catalog)


def test_attachment_inventory_is_independent_and_reuses_delivery_file(registered, tmp_path):
    _, receipt_root, receipt, item = registered
    out = prepare_registered_attachments(item, receipt_root=receipt_root, work_dir=tmp_path / 'attachments')
    inventory = json.loads(Path(out['inventory_path']).read_text(encoding='utf8'))
    assert inventory['paper_id'] == receipt['paper_id']
    assert len(inventory['items']) == 1
    entry = inventory['items'][0]
    assert Path(entry['path']).is_relative_to(Path(item['delivery_dir']) / 'files')
    assert entry['content_status'] == 'preview'
    assert 'x,y' in entry['preview']['text']
    assert not list((tmp_path / 'attachments').rglob('*.csv'))
    assert prepare_registered_attachments(item, receipt_root=receipt_root, work_dir=tmp_path / 'attachments') == out

def test_registered_attachment_continuation_keeps_identity_and_history(registered, tmp_path):
    _, receipt_root, receipt, item = registered
    first = prepare_registered_attachments(item, receipt_root=receipt_root, work_dir=tmp_path/'pass1',
                                           preview_limits={'text_chars':2}, preview_max_chunks=2)
    first_inv=json.loads(Path(first['inventory_path']).read_text(encoding='utf8'))
    row=first_inv['items'][0];p=row['preview'];assert p['chunks_processed']==2
    cursor=p['next_cursor_after_pass'];assert cursor and row['sha256']==cursor['source_sha256']
    second=prepare_registered_attachments(item, receipt_root=receipt_root, work_dir=tmp_path/'pass1',
        preview_limits={'text_chars':2},preview_max_chunks=2,preview_cursors={row['file_id']:cursor})
    other=json.loads(Path(second['inventory_path']).read_text(encoding='utf8'))['items'][0]['preview']
    pieces=[p['text']]+[x['text'] for x in p['continuations']]+[other['text']]+[x['text'] for x in other['continuations']]
    assert ''.join(pieces)==Path(row['path']).read_bytes().decode('utf8')
    assert other['next_cursor_after_pass'] is None
    assert second['inventory_path'] != first['inventory_path']
    assert prepare_registered_attachments(item, receipt_root=receipt_root, work_dir=tmp_path/'pass1',
        preview_limits={'text_chars':2},preview_max_chunks=2,preview_cursors={row['file_id']:cursor}) == second
    assert Path(first['inventory_path']).exists() and Path(item['receipt_path']).exists()

def test_incomplete_inventory_write_recovers_without_overwriting_history(registered, tmp_path, monkeypatch):
    import local_prepare
    _, root, _, item=registered;work=tmp_path/'attachments'
    original=local_prepare._write_json_new
    def fail_once(path, data):
        if path.name=='attachment-inventory.json':
            (path.parent/'.attachment-inventory.json.interrupted.tmp').write_text('{',encoding='utf8')
            raise OSError('synthetic interrupted write')
        return original(path,data)
    monkeypatch.setattr(local_prepare,'_write_json_new',fail_once)
    with pytest.raises(OSError,match='synthetic interrupted'):
        prepare_registered_attachments(item,receipt_root=root,work_dir=work)
    assert (work/'attachment-binding.json').is_file()
    monkeypatch.setattr(local_prepare,'_write_json_new',original)
    resumed=prepare_registered_attachments(item,receipt_root=root,work_dir=work)
    assert Path(resumed['inventory_path']).is_file()
    assert (work/'.attachment-inventory.json.interrupted.tmp').read_text(encoding='utf8')=='{'

@pytest.mark.parametrize('corrupted',['{broken','[]','"string"','{}'])
def test_corrupt_root_inventory_stays_in_place_and_same_identity_can_retry(registered, tmp_path, corrupted):
    _, root, _, item=registered;work=tmp_path/'attachments'
    first=prepare_registered_attachments(item,receipt_root=root,work_dir=work)
    path=Path(first['inventory_path']);path.write_text(corrupted,encoding='utf8')
    retried=prepare_registered_attachments(item,receipt_root=root,work_dir=work)
    assert path.read_text(encoding='utf8')==corrupted
    assert Path(retried['inventory_path']).is_file() and retried['inventory_path']!=str(path)

def test_orphaned_attachment_attempts_cannot_be_rebound_to_another_paper(registered, tmp_path):
    _, root, _, item=registered;work=tmp_path/'attachments';attempt=work/'attempts'/'attempt-0001'
    attempt.mkdir(parents=True);(attempt/'attachment-inventory.json').write_text('{"paper_id":"other"}',encoding='utf8')
    with pytest.raises(ValueError,match='历史'):
        prepare_registered_attachments(item,receipt_root=root,work_dir=work)
    assert not (work/'attachment-inventory.json').exists()

def test_attachment_attempt_directory_cannot_link_to_delivery(registered, tmp_path):
    import os
    _, root, _, item=registered;work=tmp_path/'attachments'
    prepare_registered_attachments(item,receipt_root=root,work_dir=work)
    try:
        os.symlink(item['delivery_dir'],work/'attempts',target_is_directory=True)
    except OSError:
        if sys.platform != 'win32':pytest.skip('directory symlink unavailable')
        made=subprocess.run(['cmd','/c','mklink','/J',str(work/'attempts'),item['delivery_dir']],capture_output=True)
        if made.returncode:pytest.skip('directory junction unavailable')
    with pytest.raises(ValueError,match='历史|交付'):
        prepare_registered_attachments(item,receipt_root=root,work_dir=work,preview_limits={'text_chars':2})
    assert not list(Path(item['delivery_dir']).glob('attempt-*'))


def test_attachments_cannot_write_into_delivery(registered):
    _, receipt_root, _, item = registered
    with pytest.raises(ValueError):
        prepare_registered_attachments(item, receipt_root=receipt_root,
                                       work_dir=Path(item['delivery_dir']) / 'attachment-work')


def test_work_cannot_be_written_into_another_registered_delivery(registered, tmp_path):
    _, receipt_root, _, item = registered
    other = tmp_path / 'other' / 'delivery'
    (other/'files').mkdir(parents=True)
    (other/'delivery.json').write_text('{}', encoding='utf8')
    catalog = tmp_path/'catalog.json'
    catalog.write_text('{"majors": []}', encoding='utf8')
    with pytest.raises(ValueError):
        prepare_registered(item, receipt_root=receipt_root, work_dir=other/'body-work', catalog_path=catalog)
    with pytest.raises(ValueError):
        prepare_registered_attachments(item, receipt_root=receipt_root, work_dir=other/'attachment-work')
    assert not (other/'body-work').exists() and not (other/'attachment-work').exists()


def test_damaged_delivery_directory_is_still_protected(registered, tmp_path):
    _, receipt_root, _, item = registered
    other = tmp_path / 'tasks' / 'deliveries' / 'damaged-group'
    other.mkdir(parents=True)
    catalog = tmp_path/'catalog.json'
    catalog.write_text('{"majors": []}', encoding='utf8')
    with pytest.raises(ValueError):
        prepare_registered(item, receipt_root=receipt_root, work_dir=other/'body-work', catalog_path=catalog)
    with pytest.raises(ValueError):
        prepare_registered_attachments(item, receipt_root=receipt_root, work_dir=other/'attachment-work')
    assert not (other/'body-work').exists() and not (other/'attachment-work').exists()


def test_registered_planning_requires_and_includes_separate_attachment_inventory(registered, tmp_path):
    from local_prepare import plan_clean_material
    from test_capacity_plan import config
    _, receipt_root, _, item = registered
    catalog = tmp_path / 'catalog.json'
    catalog.write_text('{"majors": []}', encoding='utf8')
    body = prepare_registered(item, receipt_root=receipt_root, work_dir=tmp_path / 'body', catalog_path=catalog)
    attachments = prepare_registered_attachments(item, receipt_root=receipt_root, work_dir=tmp_path / 'attachments')
    pending=plan_clean_material(body['material_path'], '模板', catalog, config(200000), tmp_path / 'missing')
    saved=json.loads(Path(pending['plan_path']).read_text(encoding='utf8'))
    assert saved['attachment_inventory_pending'] is True
    assert saved['source_completeness']=='partial'
    result = plan_clean_material(body['material_path'], '模板', catalog, config(200000), tmp_path / 'plan',
                                 attachment_inventory_path=attachments['inventory_path'])
    content = result['plan']['parts'][0]['body']['messages'][1]['content'][0]['text']
    model_material = json.loads(content)['material']
    assert len(model_material['attachments']) == 1
    assert model_material['attachments'][0]['parse_status'] == 'metadata_only'
    assert 'x,y' not in content


def test_missing_attachment_remains_in_inventory_and_planned_gaps(registered, tmp_path):
    from local_prepare import plan_clean_material
    from test_capacity_plan import config
    _, receipt_root, receipt, item = registered
    attachment = next(x for x in receipt['items'] if x['kind'] == 'attachment')
    Path(attachment['path']).write_bytes(b'damaged fixture')
    catalog = tmp_path / 'catalog.json'
    catalog.write_text('{"majors": []}', encoding='utf8')
    body = prepare_registered(item, receipt_root=receipt_root, work_dir=tmp_path / 'body', catalog_path=catalog)
    out = prepare_registered_attachments(item, receipt_root=receipt_root, work_dir=tmp_path / 'attachments')
    inv = json.loads(Path(out['inventory_path']).read_text(encoding='utf8'))
    assert len(inv['items']) == 1 and inv['items'][0]['availability'] == 'unavailable'
    result = plan_clean_material(body['material_path'], '模板', catalog, config(200000), tmp_path / 'plan',
                                 attachment_inventory_path=out['inventory_path'])
    content = result['plan']['parts'][0]['body']['messages'][1]['content'][0]['text']
    assert json.loads(content)['material']['attachments'][0]['parse_status'] == 'unavailable'


def test_forged_attachment_entry_is_not_accepted_as_registered(registered, tmp_path):
    from local_prepare import plan_clean_material
    from test_capacity_plan import config
    import hashlib
    _, receipt_root, receipt, item = registered
    catalog = tmp_path / 'catalog.json'
    catalog.write_text('{"majors": []}', encoding='utf8')
    body = prepare_registered(item, receipt_root=receipt_root, work_dir=tmp_path / 'body', catalog_path=catalog)
    out = prepare_registered_attachments(item, receipt_root=receipt_root, work_dir=tmp_path / 'attachments')
    invpath = Path(out['inventory_path'])
    inv = json.loads(invpath.read_text(encoding='utf8'))
    wrong = next(Path(row['path']) for row in receipt['items'] if row['kind'] == 'body')
    inv['items'][0].update(path=str(wrong), sha256=hashlib.sha256(wrong.read_bytes()).hexdigest(), bytes=wrong.stat().st_size)
    invpath.write_text(json.dumps(inv, ensure_ascii=False), encoding='utf8')
    with pytest.raises(ValueError, match='附件'):
        plan_clean_material(body['material_path'], '模板', catalog, config(200000), tmp_path / 'plan',
                            attachment_inventory_path=invpath)


def test_changed_delivery_manifest_stops_an_old_attachment_inventory(registered, tmp_path):
    from local_prepare import plan_clean_material
    from test_capacity_plan import config
    _, receipt_root, _, item = registered
    catalog = tmp_path/'catalog.json'
    catalog.write_text('{"majors": []}', encoding='utf8')
    body = prepare_registered(item, receipt_root=receipt_root, work_dir=tmp_path/'body', catalog_path=catalog)
    attachments = prepare_registered_attachments(item, receipt_root=receipt_root, work_dir=tmp_path/'attachments')
    (Path(item['delivery_dir'])/'delivery.json').write_text('{}', encoding='utf8')
    with pytest.raises(ValueError):
        plan_clean_material(body['material_path'], '模板', catalog, config(200000), tmp_path/'plan',
                            attachment_inventory_path=attachments['inventory_path'])


def test_forged_attachment_observation_flags_are_not_accepted(registered, tmp_path):
    from local_prepare import plan_clean_material
    from test_capacity_plan import config
    _, receipt_root, _, item = registered
    catalog = tmp_path/'catalog.json'
    catalog.write_text('{"majors": []}', encoding='utf8')
    body = prepare_registered(item, receipt_root=receipt_root, work_dir=tmp_path/'body', catalog_path=catalog)
    out = prepare_registered_attachments(item, receipt_root=receipt_root, work_dir=tmp_path/'attachments')
    path = Path(out['inventory_path'])
    inv = json.loads(path.read_text(encoding='utf8'))
    inv['items'][0]['evidence_unverified'] = not inv['items'][0]['evidence_unverified']
    path.write_text(json.dumps(inv, ensure_ascii=False), encoding='utf8')
    with pytest.raises(ValueError, match='附件'):
        plan_clean_material(body['material_path'], '模板', catalog, config(200000), tmp_path/'plan',
                            attachment_inventory_path=path)


def test_attachment_change_during_planning_does_not_publish_plan(registered, tmp_path, monkeypatch):
    from local_prepare import plan_clean_material, _native_paths
    from test_capacity_plan import config
    _, receipt_root, receipt, item = registered
    catalog = tmp_path/'catalog.json'
    catalog.write_text('{"majors": []}', encoding='utf8')
    body = prepare_registered(item, receipt_root=receipt_root, work_dir=tmp_path/'body', catalog_path=catalog)
    out = prepare_registered_attachments(item, receipt_root=receipt_root, work_dir=tmp_path/'attachments')
    _native_paths()
    import preextract_request
    original = preextract_request.plan_requests
    attachment = Path(next(row['path'] for row in receipt['items'] if row['kind']=='attachment'))
    def change_after_planning(*args, **kwargs):
        result = original(*args, **kwargs)
        attachment.write_bytes(b'changed synthetic fixture')
        return result
    monkeypatch.setattr(preextract_request, 'plan_requests', change_after_planning)
    with pytest.raises(ValueError):
        plan_clean_material(body['material_path'], '模板', catalog, config(200000), tmp_path/'plan',
                            attachment_inventory_path=out['inventory_path'])
    assert not (tmp_path/'plan'/'offline-plan.json').exists()


def test_attachment_text_and_zip_previews_do_not_copy_originals(tmp_path):
    import zipfile
    from attachment_preview import preview_attachment
    text = tmp_path / 'table.csv'
    text.write_text('column,value\nx,42', encoding='utf8')
    archive = tmp_path / 'parts.zip'
    with zipfile.ZipFile(archive, 'w') as z:
        z.writestr('a/diagram.txt', 'inside')
        z.writestr('b.csv', 'x,y')
    before = {p: p.read_bytes() for p in (text, archive)}
    text_preview = preview_attachment(text, 'unknown')
    zip_preview = preview_attachment(archive, 'zip')
    assert 'x,42' in text_preview['text'] and text_preview['status'] == 'preview'
    assert zip_preview['status'] == 'preview' and zip_preview['member_count'] == 2
    assert {p: p.read_bytes() for p in before} == before
    assert not (tmp_path / 'a').exists()


def test_attachment_docx_preview_rejects_oversized_xml_before_parsing(tmp_path,monkeypatch):
    import zipfile,docx
    from attachment_preview import preview_attachment
    source=tmp_path/'huge.docx'
    with zipfile.ZipFile(source,'w',compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr('word/document.xml',b'x'*(9*1024*1024))
    monkeypatch.setattr(docx,'Document',lambda path: (_ for _ in ()).throw(AssertionError('unbounded parse')))
    result=preview_attachment(source,'docx')
    assert result['status']=='unavailable' and '限' in result['reason']
