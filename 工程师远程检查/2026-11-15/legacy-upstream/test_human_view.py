import json
from pathlib import Path
from urllib.parse import unquote
import re

import pytest

from engine import Store, digest, write_json
from human_view import build_view, classify


@pytest.fixture
def batch(tmp_path):
    store = Store(tmp_path / 'data')
    manifest = {'run_id': 'UP_20260915_120000_abcdef', 'status': 'completed', 'items': [],
                'started_at': '2026-09-15T12:00:00', 'message': '处理完成', 'events': []}
    store.save(manifest)

    def add(number, status='pass', title='上市公司财务管理研究', package=True):
        folder = store.runs / manifest['run_id'] / 'candidates' / f'ACC_{number:06}'
        folder.mkdir(parents=True)
        display = folder / 'document.md'
        display.write_text('# ' + title + '\n\n完整正文。\n', encoding='utf-8')
        ingest = folder / 'ingest.md'
        ingest.write_text('用于入库的原始导出', encoding='utf-8')
        item = {'id': f'UP_{number:020}', 'title': title, 'major': '会计学',
                'category': '经管财会类', 'status': status, 'reason': '',
                'path': str(ingest), 'display_path': str(display), 'reviewed_hash': ''}
        if package:
            item['package_path'] = str(folder)
            write_json(folder / 'metadata.json', {'resources': [], 'issues': [], 'completeness_verified': False})
        manifest['items'].append(item)
        store.save(manifest)
        return item, folder

    return store, manifest, add


def test_three_chinese_folders_and_direct_documents_with_stable_collisions(batch):
    store, manifest, add = batch
    first, _ = add(1)
    second, _ = add(2)
    rejected, _ = add(3, status='reject')
    rejected['reason'] = '正文不足 3000 有效字符'
    result = build_view(store, manifest)
    root = Path(result['path'])
    assert {p.name for p in root.iterdir()} == {'通过', '有争议', '未通过'}
    docs = list((root / '有争议' / '会计学').iterdir())
    assert len(docs) == 2 and all(p.is_file() and p.suffix == '.md' for p in docs)
    assert all('上市公司财务管理研究' in p.name for p in docs)
    assert '3000' in next((root / '未通过' / '会计学').glob('*.md')).read_text(encoding='utf-8')
    before = {p: digest(p) for p in docs}
    manifest['items'].reverse()
    assert build_view(store, manifest)['counts'] == {'通过': 0, '有争议': 2, '未通过': 1}
    assert before == {p: digest(p) for p in docs}


def test_pass_requires_evidence_review_not_only_upload_approval(batch):
    store, manifest, add = batch
    item, folder = add(1)
    from evidence import package_fingerprint
    item['reviewed_hash'] = digest(Path(item['path']))
    item['reviewed_package_hash'] = package_fingerprint(folder)
    assert classify(item, folder)[0] == '有争议'
    meta = json.loads((folder / 'metadata.json').read_text(encoding='utf-8'))
    meta['completeness_verified'] = True
    write_json(folder / 'metadata.json', meta)
    item['reviewed_package_hash'] = package_fingerprint(folder)
    assert classify(item, folder)[0] == '通过'
    good = build_view(store, manifest)
    old_path = next((Path(good['path']) / '通过').rglob('*.md'))
    (folder / 'document.md').write_text('修改后的正文', encoding='utf-8')
    assert classify(item, folder)[0] == '有争议'
    new = build_view(store, manifest)
    assert not old_path.exists()
    assert new['counts']['有争议'] == 1


def test_image_links_rebased_and_machine_content_untouched(batch):
    store, manifest, add = batch
    item, folder = add(1)
    assets = folder / 'assets'
    assets.mkdir()
    asset = assets / '趋势 图(1).png'
    asset.write_bytes(b'image-fixture')
    (folder / 'document.md').write_text('![趋势](<assets/趋势 图(1).png>)\n[官网](https://example.com/a)\n[章节](#结果)', encoding='utf-8')
    before = {p: digest(p) for p in folder.rglob('*') if p.is_file()}
    view = build_view(store, manifest)
    doc = next(Path(view['path']).rglob('*.md'))
    text = doc.read_text(encoding='utf-8')
    target = re.search(r'!\[趋势\]\(<([^>]+)>\)', text)[1]
    assert (doc.parent / unquote(target)).resolve() == asset.resolve()
    assert (doc.parent / unquote(target)).is_file()
    assert '[官网](https://example.com/a)' in text and '[章节](#结果)' in text
    assert before == {p: digest(p) for p in folder.rglob('*') if p.is_file()}
    assert item['path'] == str(folder / 'ingest.md')


def test_legacy_missing_and_unrecognized_are_not_lost(batch):
    store, manifest, add = batch
    old, _ = add(1, package=False)
    old['major'] = 'unknown'
    missing, folder = add(2)
    (folder / 'document.md').unlink()
    result = build_view(store, manifest)
    root = Path(result['path'])
    assert list((root / '有争议' / '未识别专业').glob('*.md'))
    assert result['counts']['有争议'] == 2
    assert any('正文文件缺失' in p.read_text(encoding='utf-8') for p in root.rglob('*.md'))
    assert len(result['issues']) >= 1


def test_modified_human_copy_is_preserved_before_regeneration(batch):
    store, manifest, add = batch
    item, folder = add(1)
    first = build_view(store, manifest)
    doc = next(Path(first['path']).rglob('*.md'))
    doc.write_text('用户修改的注释', encoding='utf-8')
    (folder / 'document.md').write_text('原文导出已刷新', encoding='utf-8')
    second = build_view(store, manifest)
    assert len(second['preserved']) == 1
    assert Path(second['preserved'][0]).read_text(encoding='utf-8') == '用户修改的注释'
    assert '原文导出已刷新' in doc.read_text(encoding='utf-8')
    assert len(list(Path(first['path']).rglob('*.md'))) == 1
    assert build_view(store, manifest)['preserved'] == []


def test_unsafe_record_does_not_read_source_or_delete_outside_files(batch, tmp_path):
    store, manifest, add = batch
    item, folder = add(1)
    original = tmp_path / 'original.md'
    original.write_text('不能出现在浏览副本中的原始身份信息', encoding='utf-8')
    item['display_path'] = str(original)
    result = build_view(store, manifest)
    doc = next(Path(result['path']).rglob('*.md'))
    assert '原始身份信息' not in doc.read_text(encoding='utf-8')
    assert result['issues']
    index = store.runs / manifest['run_id'] / 'human_view.json'
    data = json.loads(index.read_text(encoding='utf-8'))
    data['files'].append({'path': str(original), 'sha256': digest(original)})
    write_json(index, data)
    with pytest.raises(ValueError, match='路径'):
        build_view(store, manifest)
    assert original.read_text(encoding='utf-8') == '不能出现在浏览副本中的原始身份信息'


def test_invalid_title_and_folder_characters_are_safe(batch):
    store, manifest, add = batch
    invalid, _ = add(1, title='会计学 201914117022 向同学')
    odd, _ = add(2, title='财务管理/内部:控制的比较研究')
    odd['major'] = '../会计学'
    result = build_view(store, manifest)
    paths = list(Path(result['path']).rglob('*.md'))
    assert len(paths) == 2
    assert any('待确认题名' in p.name for p in paths)
    assert all('201914117022' not in p.name for p in paths)
    assert all(p.resolve().is_relative_to(Path(result['path']).resolve()) for p in paths)


def test_current_review_with_pending_privacy_or_open_issue_cannot_pass(batch):
    _, _, add = batch
    item, folder = add(1)
    from evidence import package_fingerprint
    image = folder / 'image.png'
    image.write_bytes(b'image')
    meta = {'completeness_verified': True, 'resources': [
        {'id': 'figure', 'local_path': 'image.png', 'privacy_status': 'pending'}], 'issues': []}
    write_json(folder / 'metadata.json', meta)
    item['reviewed_hash'] = digest(Path(item['path']))
    item['reviewed_package_hash'] = package_fingerprint(folder)
    assert classify(item, folder)[0] == '有争议'
    meta['resources'][0]['privacy_status'] = 'approved'
    meta['issues'] = [{'status': 'open', 'message': '表格表头对应关系未核对'}]
    write_json(folder / 'metadata.json', meta)
    item['reviewed_package_hash'] = package_fingerprint(folder)
    assert classify(item, folder)[0] == '有争议'


def test_extraction_failure_is_a_disputed_report_not_a_rejected_paper(batch):
    store, manifest, _ = batch
    manifest['events'] = [{'status': 'failed', 'source': '学生姓名_原始文件.doc', 'reason': '无法解析'},
                          {'status': 'duplicate', 'source': 'duplicate.doc', 'reason': '重复'}]
    result = build_view(store, manifest)
    docs = list(Path(result['path']).rglob('*.md'))
    assert len(docs) == 1 and docs[0].parent.name == '未识别专业'
    assert docs[0].parent.parent.name == '有争议'
    assert result['failed_records'] == 1
    text = docs[0].read_text(encoding='utf-8')
    assert '学生姓名' not in text and '未生成正文' in text


def test_open_batch_uses_generated_chinese_directory_and_busy_guard(batch, monkeypatch):
    import tkinter as tk
    import app as ui_module
    store, manifest, add = batch
    add(1)
    root = tk.Tk()
    root.withdraw()
    app = ui_module.Workbench(root, store.root)
    app.current = manifest
    opened, errors = [], []
    monkeypatch.setattr(ui_module.os, 'startfile', lambda path: opened.append(Path(path)))
    monkeypatch.setattr(app, 'error', errors.append)
    monkeypatch.setattr(app, 'async_task', lambda fn, done, **kwargs: done(fn()))
    try:
        app.open_run()
        assert len(opened) == 1 and opened[0].parent.name == '处理结果'
        assert {p.name for p in opened[0].iterdir()} == {'通过', '不通过'}
        assert '不通过 1 项' in app.state.get()
        app.busy = True
        app.open_run()
        assert len(opened) == 1 and errors
    finally:
        for callback in root.tk.call('after', 'info'):
            root.after_cancel(callback)
        root.destroy()
