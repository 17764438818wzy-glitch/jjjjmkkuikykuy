"""Regression coverage for purpose routing and honest per-run progress."""
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from test_preextract_workbench_flow import flow


FORM = '武汉科技大学2023届\n本科毕业设计（论文）答辩记录单\n姓名\n学号\n答辩时间\n答辩小组\n答辩问答记录'


def image(tmp_path, name='random.jpg'):
    from PIL import Image
    path = tmp_path / name
    Image.new('RGB', (240, 320), 'white').save(path)
    return path


@pytest.mark.parametrize('name', ['外文翻译原文-彭海.txt', '翻译英文.txt', '翻译英文+中文.txt',
                                 '翻译-王涛.txt', '翻译_黄河.txt', '论文翻译—翁宇峰.txt',
                                 '主修_信息学院_张三_外文翻译.txt'])
def test_explicit_reference_labels_bypass_paper_ai(tmp_path, name):
    from material_roles import classify_document
    path = tmp_path / name
    path.write_text('Abstract\nIntroduction\n' + 'This study explains the research method and results. ' * 15, encoding='utf-8')
    role = classify_document({'path': str(path)})
    assert (role['kind'], role['action']) == ('supporting_document', 'associated')


def test_translation_research_title_remains_a_body(tmp_path):
    from material_roles import classify_document
    path = tmp_path / '基于神经网络的外文翻译方法研究.txt'
    path.write_text('摘要\n本研究分析翻译算法。\n1 绪论\n' + '本文通过仿真实验研究算法。' * 40, encoding='utf-8')
    assert classify_document({'path': str(path)})['action'] == 'review'


def test_photographed_form_uses_same_pre_ai_gate(tmp_path, monkeypatch):
    import material_roles
    from associated_materials import material_kind
    path = image(tmp_path)
    monkeypatch.setattr(material_roles, '_read', lambda p: (FORM, 'Windows.Media.Ocr', True))
    assert material_kind({'path': str(path)}) == 'administrative'


def test_unknown_picture_is_pending_and_cannot_be_an_uploadable_attachment(tmp_path, monkeypatch):
    import material_roles
    from associated_materials import preserve_material
    path = image(tmp_path)
    monkeypatch.setattr(material_roles, '_read', lambda p: ('', 'Windows.Media.Ocr', True))
    row = preserve_material({'path': str(path)}, tmp_path / 'private', [])
    assert row.get('purpose_pending') is True
    assert path.exists()


def test_form_member_is_deleted_in_private_archive_expansion(tmp_path, monkeypatch):
    import material_roles
    from associated_materials import preserve_material
    folder = tmp_path / 'private'
    folder.mkdir()
    source = image(folder)
    monkeypatch.setattr(material_roles, '_read', lambda p: (FORM, 'Windows.Media.Ocr', True))
    row = preserve_material({'path': str(source), 'archive_member_path': source.name},
                            folder / 'members', [], container_depth=1)
    assert row['parse_status'] == 'excluded' and not source.exists()
    assert not row['retained_file_hashes']


def test_special_basename_form_photo_still_uses_role_gate(tmp_path, monkeypatch):
    import material_roles
    from associated_materials import preserve_material
    folder = tmp_path / 'private'
    folder.mkdir()
    source = image(folder, 'README.jpg')
    monkeypatch.setattr(material_roles, '_read', lambda p: (FORM, 'Windows.Media.Ocr', True))
    row = preserve_material({'path': str(source), 'archive_member_path': source.name},
                            folder / 'members', [], container_depth=1)
    assert row['parse_status'] == 'excluded' and not source.exists()


def test_combined_form_and_body_image_is_held(tmp_path, monkeypatch):
    import material_roles
    path = image(tmp_path)
    monkeypatch.setattr(material_roles, '_read', lambda p: (
        FORM + '\n摘要\n本研究通过仿真实验比较控制系统方法。\n' + '本文研究控制方法。' * 15, '', True))
    role = material_roles.classify_document({'path': str(path)})
    assert role['action'] == 'pending' and path.exists()


def test_old_image_cache_cannot_bypass_new_archive_gate(tmp_path, monkeypatch):
    import hashlib
    import zipfile
    import material_roles
    from engine import digest
    from archive_identity import record_key
    from associated_materials import preserve_material
    photo = image(tmp_path)
    archive = tmp_path / '附件.zip'
    with zipfile.ZipFile(archive, 'w') as z:
        z.write(photo, 'random.jpg')
    before = digest(archive)
    record = {'path': str(archive)}
    ref = 'associated_' + hashlib.sha256((record_key(record)+'\0'+before).encode()).hexdigest()[:24]
    folder = tmp_path / 'private' / ref
    folder.mkdir(parents=True)
    (folder / 'associated-material.json').write_text(json.dumps(dict(sha256=before,
        context_policy='nested-owning-body/1', members=[dict(kind='image', parse_status='readable',
        retained_file_hashes={}, file_ref='old')], retained_file_hashes={})), encoding='utf-8')
    monkeypatch.setattr(material_roles, '_read', lambda p: (FORM, 'Windows.Media.Ocr', True))
    row = preserve_material(record, tmp_path / 'private', [])
    assert not row['members'] and row.get('excluded_members')
    assert digest(archive) == before


def test_old_image_inventory_is_gated_before_new_upload_plan(tmp_path, monkeypatch):
    import material_roles
    from associated_delivery import write_plan
    from engine import digest
    work = tmp_path / 'work'
    (work / 'private').mkdir(parents=True)
    photo = image(work / 'private')
    bundle = dict(file_hashes={str(photo.resolve()): digest(photo)}, attachments=[dict(kind='image',
        original_path=str(photo.resolve()), sha256=digest(photo), format='.jpg', association_status='candidate')])
    (work / 'material.json').write_text(json.dumps(bundle), encoding='utf-8')
    prepared = dict(task={'task_id': 'fixture'}, validation={'manifest_sha256': 'a'*64}, packet={'routes': []})
    monkeypatch.setattr(material_roles, '_read', lambda p: (FORM, 'Windows.Media.Ocr', True))
    assert write_plan(bundle, prepared, work) is None
    assert photo.exists(), 'Request-bound old material is excluded without editing/deleting it'


def test_top_level_form_photo_is_deleted_without_ai(flow, tmp_path, monkeypatch):
    import material_roles
    processor, opener, _ = flow
    photo = image(tmp_path)
    monkeypatch.setattr(material_roles, '_read', lambda p: (FORM, 'Windows.Media.Ocr', True))
    events = []
    result = processor.run([{'path': str(photo), 'name': photo.name}], callback=events.append)
    assert not photo.exists() and result['items'][0]['status'] == 'deleted'
    assert not opener.calls
    assert any(e.get('intake_progress') for e in events)
    assert result['items'][0]['ai_result']['status'] == 'not_started'


def test_preparation_count_is_not_finished_paper_count(tmp_path):
    import tkinter as tk
    from app import Workbench
    root = tk.Tk()
    root.withdraw()
    app = Workbench(root, tmp_path / 'data', recover=False)
    try:
        app.show_script_progress(dict(run_id=None, source_count=379, processed_sources=0,
            intake_progress=dict(phase='用途识别', completed=23, total=379), progress={'file': 'fixture.pdf'}))
        assert '用途识别 23/379' in app.progress_count.get()
        assert '尚未进入 AI' in app.progress_count.get()
        assert '已结束 23' not in app.progress_count.get()
    finally:
        app._model_activity_panel.destroy()
        for timer in root.tk.call('after', 'info'):
            root.after_cancel(timer)
        root.destroy()


def test_package_and_verified_ingestion_are_displayed_separately():
    from progress_summary import delivery_label, display_phase
    item = dict(ai_result={'status':'passed'}, automatic_approval=True, package_path='sealed')
    assert delivery_label(item) == '待入库'
    item.update(handoff_status='uploaded', preextract_delivery={'status':'complete'})
    assert delivery_label(item) == '已入库'
    assert display_phase(dict(ai_result={'status':'running'},stages=[{'stage':'执行脱敏、切片与封包'}])) == '后处理'


def test_bad_history_does_not_hide_twenty_live_requests(tmp_path):
    from test_model_activity import put, batch, request, owner, reader
    work = tmp_path / 'private/paper'
    for n in range(20):
        request(work, str(n))
    batch(tmp_path, 'current', [work])
    owner(tmp_path, 'current')
    broken = tmp_path / 'runs/old/manifest.json'
    broken.parent.mkdir(parents=True)
    broken.write_text('{', encoding='utf-8')
    value = reader(tmp_path).read('current')
    assert value['current'] == value['total'] == 20 and value['partial']


def test_unreadable_current_is_unknown_not_an_exact_zero(tmp_path):
    from test_model_activity import reader
    broken = tmp_path / 'runs/current/manifest.json'
    broken.parent.mkdir(parents=True)
    broken.write_text('{', encoding='utf-8')
    value = reader(tmp_path).read('current')
    assert value['partial'] and not value['current_complete']
    from model_activity_ui import activity_text
    assert '暂不可用' in activity_text(value) and '总数 0' not in activity_text(value)


def test_unreadable_current_with_live_background_is_not_current_zero(tmp_path):
    from test_model_activity import batch, request, owner, reader
    from model_activity_ui import activity_text
    work = tmp_path / 'private/background'
    for n in range(6):request(work, str(n))
    batch(tmp_path, 'background', [work])
    owner(tmp_path, 'background')
    value = reader(tmp_path).read('missing-current')
    assert value['total'] == 6 and not value['current_complete']
    text = activity_text(value)
    assert '本批待确认' in text and '本批 0' not in text and '后台 6' in text


def test_unconfirmed_current_with_live_background_is_not_current_zero(tmp_path):
    from test_model_activity import batch, request, owner, reader
    from model_activity_ui import activity_text
    background = tmp_path / 'private/background'
    current = tmp_path / 'private/current'
    request(background)
    request(current)
    batch(tmp_path, 'background', [background])
    batch(tmp_path, 'current', [current])
    owner(tmp_path, 'background')
    value = reader(tmp_path).read('current')
    assert not value['current_complete']
    assert '本批待确认' in activity_text(value)


def test_material_context_observer_preserves_classification(tmp_path):
    from associated_materials import MaterialContext
    paper = tmp_path / '论文正文.txt'
    paper.write_text('摘要\n本研究分析控制系统。\n1 绪论\n' + '本文开展仿真研究。' * 30, encoding='utf-8')
    code = tmp_path / 'controller.py'
    code.write_text('x = 1', encoding='utf-8')
    records = [{'path': str(paper)}, {'path': str(code)}]
    plain = MaterialContext(records)
    events = []
    observed = MaterialContext(records, progress=lambda *args: events.append(args))
    assert events and observed.documents == plain.documents and observed.relations == plain.relations
    assert observed.equivalents == plain.equivalents


def test_retry_progress_separates_requests_and_postprocess(tmp_path):
    import tkinter as tk
    from app import Workbench
    root = tk.Tk()
    root.withdraw()
    app = Workbench(root, tmp_path / 'data', recover=False)
    try:
        manifest = dict(run_id='retry', status='running', pipeline='automatic', operation='retry_ai',
                        source_count=379, processed_sources=8, inputs=[], items=[], events=[], concurrency={'global_llm': 20})
        app.store.save(manifest)
        app.current = manifest
        counts = dict(llm_active=20, model_stage_active=20, preparation_active=2,
                      preparation_waiting=4, postprocess_active=2, postprocess_waiting=3)
        app.show_script_progress(dict(run_id='retry', source_count=379, processed_sources=8,
            pipeline_concurrency=counts, progress={'stage': 'AI 处理'}))
        value = app.ai_retry_hint.get()
        assert '8/379' in value and '模型请求 20/20' in value and '后处理 2' in value
        assert '20' in app._model_activity_panel.summary.get()
        observed_at = app._model_activity_panel._local_at
        app.show_script_progress(dict(run_id='retry', source_count=379, processed_sources=8,
            progress={'stage': '程序后处理开始'}))
        assert app._model_activity_panel._local_at == observed_at, 'Cached counters must not refresh their age'

        manifest['items'] = [dict(id='post', source_path='paper.pdf', status='pending',
            ai_result={'status': 'passed', 'stage': '完整 MD 提取'}, script_result={'status': 'running'},
            stages=[{'stage': '执行脱敏、切片与封包'}])]
        app.store.save(manifest)
        app.current = manifest
        app.show_script_results(manifest)
        app.render_preprocess_results()
        assert app.result_tree.item('post', 'values')[4] == '后处理'
    finally:
        app._model_activity_panel.destroy()
        for timer in root.tk.call('after', 'info'):
            root.after_cancel(timer)
        root.destroy()
