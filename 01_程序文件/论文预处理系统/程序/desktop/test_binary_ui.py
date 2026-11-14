"""Observable binary-result, history, privacy and safe browse-refresh contracts."""
import json
import re
import tkinter as tk
from pathlib import Path
from tkinter import ttk
from urllib.parse import unquote

import pytest

from auto_view import build_view
from engine import Store, digest, write_json


@pytest.fixture
def batch(tmp_path):
    store = Store(tmp_path / 'data')
    manifest = {'run_id': 'UP_20260915_binary', 'status': 'completed', 'pipeline': 'automatic',
                'items': [], 'inputs': [], 'events': [], 'message': '完成', 'started_at': '2026-09-15'}
    store.save(manifest)

    def add(number, status='pass', approved=True, **fields):
        package = store.runs / manifest['run_id'] / 'packages' / str(number)
        package.mkdir(parents=True)
        (package / 'document.md').write_text('# 财务管理研究\n\n研究结果为 100.00。', encoding='utf-8')
        (package / 'ingest.md').write_text('研究结果为 100.00。', encoding='utf-8')
        source = tmp_path / f'input-{number}.txt'
        source.write_text('source', encoding='utf-8')
        item = {'id': f'AUTO_{number:020}', 'title': '企业财务管理研究', 'major': '会计学',
                'category': '经管财会类', 'status': status, 'automatic_approval': approved,
                'reason': '', 'uploads': {}, 'source_path': str(source),
                'package_path': str(package), 'path': str(package / 'ingest.md'),
                'display_path': str(package / 'document.md'),
                'review_status':'ai_passed' if status=='pass' and approved else ''}
        item.update(fields)
        manifest['items'].append(item)
        manifest['inputs'].append({'path': str(source), 'name': source.name})
        store.save(manifest)
        return item, package

    return store, manifest, add


def test_binary_view_keeps_terminal_failures_and_legacy_meaning(batch):
    store, manifest, add = batch
    add(1)
    add(2, 'reject', False, reason='请求超时，重试次数已用完', reason_kind='technical')
    add(3, 'pending', False, reason='旧任务连接失败')
    add(4, 'review', False, reason='缺少参考文献')
    add(5, 'pass', False)
    view = build_view(store, manifest)
    root = Path(view['path'])
    assert {p.name for p in root.iterdir()} == {'通过', '不通过'}
    assert view['counts'] == {'通过': 1, '不通过': 4}
    documents = list(root.rglob('*.md'))
    assert len(documents) == 5
    assert all(p.parent.name == '会计学' for p in documents)
    rejected = '\n'.join(p.read_text(encoding='utf-8') for p in (root / '不通过').rglob('*.md'))
    assert '技术失败' in rejected and '请求超时' in rejected
    assert '旧版' in rejected and '未完成' in rejected
    assert '未完成全自动处理' in rejected
    assert '研究结果为 100.00' not in rejected


def test_old_event_only_failures_have_visible_private_safe_records(batch):
    store, manifest, _ = batch
    manifest['events'] = [{'status': 'failed', 'source': '张同学_论文.doc', 'reason': 'Office 解析失败'},
                          {'status': 'duplicate', 'source': '重复.doc', 'reason': '重复'}]
    result = build_view(store, manifest)
    documents = list(Path(result['path']).rglob('*.md'))
    assert len(documents) == 1 and result['failed_records'] == 1
    assert documents[0].parent.parent.name == '不通过'
    body = documents[0].read_text(encoding='utf-8')
    assert '技术失败' in body and '未生成' in body
    assert '张同学' not in body


@pytest.mark.parametrize('problem', ['missing_document', 'missing_link', 'outside_link', 'private_source', 'file_url'])
def test_bad_published_content_produces_failure_record_without_losing_item(batch, tmp_path, problem):
    store, manifest, add = batch
    item, package = add(1)
    if problem == 'missing_document':
        (package / 'document.md').unlink()
    elif problem in {'missing_link', 'outside_link', 'file_url'}:
        external = tmp_path / 'private.md'
        external.write_text('不可泄露的身份内容', encoding='utf-8')
        link = {'missing_link': 'assets/missing.png', 'outside_link': str(external),
                'file_url': external.as_uri()}[problem]
        (package / 'document.md').write_text(f'[证据](<{link}>)', encoding='utf-8')
    else:
        private = package.parents[1] / 'private' / 'raw.md'
        private.parent.mkdir()
        private.write_text('不可泄露的身份内容', encoding='utf-8')
        item.update(display_path=str(private), package_path=str(private.parent))
    result = build_view(store, manifest)
    docs = list(Path(result['path']).rglob('*.md'))
    assert len(docs) == 1 and result['counts'] == {'通过': 0, '不通过': 1}
    assert result['issues'] and isinstance(result['issues'][0], dict)
    assert '不可泄露的身份内容' not in docs[0].read_text(encoding='utf-8')
    assert '路径' in docs[0].read_text(encoding='utf-8') or '缺失' in docs[0].read_text(encoding='utf-8')


def test_valid_asset_links_stay_local_and_original_machine_files_unchanged(batch):
    store, manifest, add = batch
    _, package = add(1)
    asset = package / 'assets' / '图 表(1).png'
    asset.parent.mkdir()
    asset.write_bytes(b'image fixture')
    (package / 'document.md').write_text('![趋势](<assets/图 表(1).png>)\n[来源](https://example.com/paper)', encoding='utf-8')
    before = {p: digest(p) for p in package.rglob('*') if p.is_file()}
    result = build_view(store, manifest)
    doc = next(Path(result['path']).rglob('*.md'))
    body = doc.read_text(encoding='utf-8')
    link = re.search(r'!\[趋势\]\(<([^>]+)>\)', body)[1]
    assert (doc.parent / unquote(link)).resolve() == asset.resolve()
    assert '[来源](https://example.com/paper)' in body
    assert {p: digest(p) for p in before} == before


def test_refresh_backs_up_edits_and_removes_only_previously_generated_files(batch):
    store, manifest, add = batch
    item, package = add(1)
    first = build_view(store, manifest)
    doc = next(Path(first['path']).rglob('*.md'))
    doc.write_text('用户自己的修改', encoding='utf-8')
    custom = doc.parent / '自己的记录.md'
    custom.write_text('保留自己的记录', encoding='utf-8')
    item.update(status='reject', automatic_approval=False, reason_kind='privacy', reason='脱敏失败')
    result = build_view(store, manifest)
    assert len(result['preserved']) == 1
    assert Path(result['preserved'][0]).read_text(encoding='utf-8') == '用户自己的修改'
    assert not doc.exists() and custom.read_text(encoding='utf-8') == '保留自己的记录'
    assert result['counts'] == {'通过': 0, '不通过': 1}
    assert build_view(store, manifest)['preserved'] == []


def test_existing_old_view_is_untouched_and_unsafe_index_is_rejected(batch, tmp_path):
    store, manifest, add = batch
    add(1)
    old = store.root / '处理结果' / manifest['run_id'] / '未通过'
    old.mkdir(parents=True)
    sentinel = old / '旧文件.md'
    sentinel.write_text('old', encoding='utf-8')
    result = build_view(store, manifest)
    assert Path(result['path']) != old.parent and sentinel.read_text(encoding='utf-8') == 'old'
    index_path = store.runs / manifest['run_id'] / 'auto_view.json'
    index = json.loads(index_path.read_text(encoding='utf-8'))
    outside = tmp_path / 'original.md'
    outside.write_text('original', encoding='utf-8')
    index['files'].append({'path': str(outside), 'sha256': digest(outside)})
    write_json(index_path, index)
    with pytest.raises(ValueError, match='路径'):
        build_view(store, manifest)
    assert outside.read_text(encoding='utf-8') == 'original'


@pytest.fixture
def ui(tmp_path):
    from app import Workbench
    root = tk.Tk()
    root.attributes('-alpha', 0)
    app = Workbench(root, tmp_path / 'ui')
    root.update()
    yield root, app
    for callback in root.tk.call('after', 'info'):
        root.after_cancel(callback)
    root.destroy()


def descendants(widget):
    return [w for child in widget.winfo_children() for w in [child] + descendants(child)]


def test_ui_has_two_final_filters_and_retry_actions_without_manual_approval(ui, batch):
    root, app = ui
    _, manifest, add = batch
    add(1)
    add(2, 'reject', False, reason_kind='technical', reason='请求超时',processing_metrics=[{'task':'privacy_text','outcome':'error'}])
    add(3, 'pending', False)
    add(4, 'pass', False)
    add(5, 'reject', False,ai_result={'status':'rejected','reason':'脱敏未通过'})
    app.current = manifest
    app.render_results()
    labels = [str(w.cget('text')) for w in descendants(root) if isinstance(w, ttk.Button)]
    assert '继续未通过' not in labels and '重试交接' in labels
    assert not any('人工' in label or '已检查' in label or '未完成' in label for label in labels)
    filter_box = next(w for w in descendants(app.result_tab) if isinstance(w, ttk.Combobox)
                      and str(w.cget('textvariable')) == str(app.result_filter))
    assert {'通过','不通过','调用失败','未进入 AI','待核对'}.issubset(filter_box.cget('values'))
    assert '通过 1' in app.result_summary.get() and '调用失败 1' in app.result_summary.get()
    app.result_filter.set('不通过')
    app.render_results()
    rows = [app.result_tree.item(i, 'values') for i in app.result_tree.get_children()]
    assert len(rows) == 1 and {row[3] for row in rows} == {'不通过'}
    app.result_filter.set('调用失败');app.render_results()
    assert '请求超时' in app.result_tree.item(app.result_tree.get_children()[0],'values')[-1]
    app.result_filter.set('待核对');app.render_results()
    assert len(app.result_tree.get_children())==2


def test_retry_selection_includes_terminal_failed_sources_and_unstarted_inputs(batch):
    from app import retry_sources
    _, manifest, add = batch
    passed, _ = add(1)
    failed, _ = add(2, 'reject', False, reason_kind='technical')
    pending, _ = add(3, 'pending', False)
    old, _ = add(4, 'pass', False)
    manifest['items'].append(dict(failed, id='same-source-failure'))
    unstarted = str(Path(passed['source_path']).parent / 'unstarted.txt')
    manifest['inputs'].append({'path': unstarted, 'name': 'unstarted.txt'})
    manifest['status'] = 'stopped'
    selected = retry_sources(manifest)
    assert [r['path'] for r in selected] == [failed['source_path'], pending['source_path'], old['source_path'], unstarted]
    manifest['status'] = 'running'
    assert retry_sources(manifest) == []


def test_running_pending_is_processing_until_terminal(ui, batch):
    _, app = ui
    _, manifest, add = batch
    add(1, 'pending', False,ai_result={'status':'running','stage':'文本脱敏'})
    manifest['status'] = 'running'
    app.current = manifest
    app.render_results()
    row = app.result_tree.item(app.result_tree.get_children()[0], 'values')
    assert row[3] == '处理中'
    manifest['status'] = 'interrupted'
    app.render_results()
    row = app.result_tree.item(app.result_tree.get_children()[0], 'values')
    assert row[3] == '未完成' and '中断' in row[-1]


def test_browse_failure_also_marks_ui_unusable_and_retryable(ui, batch):
    from app import retry_sources
    _, app = ui
    _, manifest, add = batch
    item, _ = add(1)
    manifest['human_view'] = {'status': 'ready', 'issues': [
        {'id': item['id'], 'reason': '正文文件缺失、不可读取或路径无效'}]}
    app.current = manifest
    app.render_results()
    row = app.result_tree.item(item['id'], 'values')
    assert row[3] == '通过' and '本地结果尚不可用' in row[5]
    assert '缺失' in row[-1]
    assert [r['path'] for r in retry_sources(manifest)] == [item['source_path']]


def test_same_path_refresh_preserves_manual_edits_and_stable_collision_names(batch):
    store, manifest, add = batch
    _, first_package = add(1)
    add(2)
    first = build_view(store, manifest)
    docs = sorted(Path(first['path']).rglob('*.md'))
    before = {p: digest(p) for p in docs}
    manifest['items'].reverse()
    build_view(store, manifest)
    assert {p: digest(p) for p in docs} == before
    doc = next(p for p in docs if p.name == '企业财务管理研究.md')
    doc.write_text('人工注释内容', encoding='utf-8')
    (first_package / 'document.md').write_text('新的自动导出正文', encoding='utf-8')
    result = build_view(store, manifest)
    assert len(result['preserved']) == 1
    assert Path(result['preserved'][0]).read_text(encoding='utf-8') == '人工注释内容'
    assert '新的自动导出正文' in doc.read_text(encoding='utf-8')


def test_program_run_directory_cannot_escape_store_root(batch, tmp_path):
    store, manifest, _ = batch
    store.runs = tmp_path / 'outside-runs'
    (store.runs / manifest['run_id']).mkdir(parents=True)
    with pytest.raises(ValueError, match='路径'):
        build_view(store, manifest)
    assert not (store.root / '处理结果').exists()


def test_preview_does_not_read_private_or_outside_pass_document(ui, batch, monkeypatch, tmp_path):
    _, app = ui
    store, manifest, add = batch
    item, _ = add(1)
    secret = tmp_path / 'secret.md'
    secret.write_text('必须保留在源文件中的身份信息', encoding='utf-8')
    item['display_path'] = str(secret)
    app.store = store
    app.current = manifest
    app.render_results()
    app.result_tree.selection_set(item['id'])
    shown, errors = [], []
    monkeypatch.setattr(app, 'text_window', lambda title, content: shown.append(content))
    monkeypatch.setattr(app, 'error', errors.append)
    app.preview()
    assert errors and not shown
