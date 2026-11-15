"""Script-only task view, durable retry results and batch progress contracts."""
import copy
from pathlib import Path
import tkinter as tk

import pytest

from app import Workbench, retry_sources
from engine import Store


@pytest.fixture
def ui(tmp_path, monkeypatch):
    monkeypatch.delenv('RAGFLOW_API_KEY', raising=False)
    root = tk.Tk()
    root.withdraw()
    workbench = Workbench(root, tmp_path / 'data')
    yield root, workbench
    panel=getattr(workbench,'_model_activity_panel',None)
    if panel is not None:panel.destroy()
    for timer in root.tk.call('after', 'info'):
        root.after_cancel(timer)
    root.destroy()


def batch(store, tmp_path, run_id='first'):
    paths = [tmp_path / name for name in ('good.txt', 'bad.txt', 'duplicate.txt')]
    for path in paths:
        path.write_text('synthetic', encoding='utf-8')
    manifest = {'run_id': run_id, 'status': 'completed', 'pipeline': 'automatic',
                'started_at': '2026-09-20', 'message': 'test',
                'source_count': 3, 'processed_sources': 3,
                'inputs': [{'path': str(p), 'name': p.name} for p in paths],
                'items': [
                    {'id': 'good', 'source_path': str(paths[0]), 'status': 'pass',
                     'automatic_approval': True, 'title': '企业财务管理研究', 'major': '会计学'},
                    {'id': 'bad', 'source_path': str(paths[1]), 'status': 'reject',
                     'reason_kind': 'technical', 'reason': '读取图表失败',
                     'title': '失败文件', 'major': '会计学'},
                ],
                'events': [{'status': 'duplicate', 'source': str(paths[2]), 'reason': '源文件重复'}]}
    store.save(manifest)
    return manifest, paths


def configure(workbench):
    workbench.ai_vars['text_model'].set('text-fixture')
    workbench.ai_vars['vision_model'].set('vision-fixture')
    workbench.ai_vars['key'].set('synthetic-key')
    workbench.key.set('synthetic-rag-key')
    workbench.connected([{'id': 'paper', 'name': '论文库'}, {'id': 'images', 'name': '图片库'}])
    workbench.dataset.set('论文库')
    workbench.image_dataset.set('图片库')


def test_completed_script_results_remain_on_input_page(ui, tmp_path):
    root, workbench = ui
    manifest, paths = batch(workbench.store, tmp_path)
    workbench.tabs.select(workbench.input_tab)
    workbench.finished(manifest)
    assert workbench.tabs.select() == str(workbench.input_tab)
    rows = [workbench.source_tree.item(i, 'values') for i in workbench.source_tree.get_children()]
    assert len(rows) == 3
    assert [r[0] for r in rows] == [p.name for p in paths]
    assert [r[1] for r in rows] == ['通过', '不通过', '重复跳过']
    assert '读取图表失败' in rows[1][2]
    configure(workbench)
    workbench.source_tree.selection_set('0','1','2')
    workbench.update_controls()
    assert workbench.start_button.instate(['disabled']), 'A loaded batch uses failure retry, not a fresh full run'


def test_resume_recovers_complete_batch_but_selects_only_failure(ui, tmp_path, monkeypatch):
    _, workbench = ui
    manifest, paths = batch(workbench.store, tmp_path)
    workbench.current = manifest
    workbench.folder.set(str(tmp_path))
    selected = []
    monkeypatch.setattr(workbench, 'start_automatic', lambda rows: selected.extend(rows))
    workbench.retry_failed()
    assert [r['path'] for r in selected] == [str(paths[1])]
    assert len(workbench.source_tree.get_children()) == 3


def test_retry_history_preserves_success_and_previous_failure_after_restart(tmp_path):
    from script_task import task_snapshot, task_rows
    store = Store(tmp_path / 'data')
    original, paths = batch(store, tmp_path)
    before = store.manifest_path('first').read_bytes()
    latest = {**original, 'run_id': 'retry', 'retry_of': 'first',
              'source_count': 1, 'processed_sources': 1,
              'inputs': [original['inputs'][1]], 'events': [],
              'items': [{**original['items'][1], 'id': 'recovered', 'status': 'pass',
                         'automatic_approval': True, 'title': '企业资金管理研究', 'reason': '通过'}]}
    store.save(latest)
    # A fresh Store represents reopening the workbench after the retry.
    restored = task_snapshot(Store(store.root), store.load('retry'))
    rows = task_rows(restored)
    assert [r['script_status'] for r in rows] == ['通过', '通过', '重复跳过']
    assert rows[1]['previous_error'] == '技术失败：读取图表失败'
    assert not retry_sources(restored)
    assert store.manifest_path('first').read_bytes() == before


def test_pending_retry_keeps_previous_error_visible(ui, tmp_path):
    _, workbench = ui
    manifest, paths = batch(workbench.store, tmp_path)
    workbench.show_script_results(manifest)
    workbench.show_script_progress({'run_id': 'first', 'source_count': 3, 'processed_sources': 1,
        'message': 'retrying', 'progress': {'stage': '图表提取', 'source_path': str(paths[1]),
                                           'file': paths[1].name, 'completed': 0, 'total': 1}})
    assert '读取图表失败' in workbench.source_tree.item('1', 'values')[3]
    assert workbench.source_tree.item('0', 'values')[1] == '通过'


def test_parent_batch_progress_wins_over_child_counter(ui):
    _, workbench = ui
    workbench.events.put(('progress', {'run_id': 'synthetic', 'status': 'running',
        'message': '图表提取', 'source_count': 251, 'processed_sources': 220,
        'progress': {'stage': '图表提取', 'file': 'paper.docx', 'completed': 0, 'total': 1}}))
    workbench.poll()
    assert float(workbench.progress_bar['maximum']) == 251
    assert float(workbench.progress_bar['value']) == 220
    assert '220/251' in workbench.progress_count.get()


def test_remote_progress_does_not_replace_script_panel(ui):
    _, workbench = ui
    workbench.show_progress({'stage': '图表提取', 'file': 'paper.docx', 'completed': 2, 'total': 3})
    original = workbench.progress_detail.get()
    workbench.events.put(('text', '正在上传论文'))
    workbench.events.put(('progress', {'run_id': 'remote', 'status': 'completed',
        'message': '上传失败', 'processed_sources': 3, 'source_count': 3,
        'progress': {'stage': 'RAGFlow 等待重试', 'message': '上传失败'}}))
    workbench.poll()
    assert workbench.progress_detail.get() == original
    assert '上传失败' not in workbench.log_text.get('1.0', 'end')


def test_remote_operation_error_stays_out_of_script_log(ui,monkeypatch):
    _,workbench=ui
    workbench.task_kind='operation'
    monkeypatch.setattr(workbench,'error',lambda message:workbench.state.set(message))
    workbench.events.put(('error','synthetic remote failure'))
    workbench.poll()
    assert 'synthetic remote failure' not in workbench.log_text.get('1.0','end')


def test_task_models_share_actual_processor_configuration(ui):
    _, workbench = ui
    configure(workbench)
    workbench.ai_model_combos['text_model'].configure(values=['text-fixture', 'text-other'])
    workbench.refresh_task_models()
    workbench.task_text_model_combo.current(1)
    from auto_ui import read_ai_form
    config, _ = read_ai_form(workbench)
    assert config['text_model'] == 'text-other'
    workbench.ai_vars['vision_model'].set('vision-other')
    assert workbench.task_vision_model_combo.get() == 'vision-other'


def test_start_requires_models_but_can_defer_destinations(ui, tmp_path):
    _, workbench = ui
    manifest, _ = batch(workbench.store, tmp_path)
    workbench.scanned([{**manifest['inputs'][0], 'format': '.txt', 'size': 9}])
    assert workbench.start_button.instate(['disabled'])
    configure(workbench)
    workbench.update_controls()
    assert not workbench.start_button.instate(['disabled'])
    workbench.image_dataset.set('')
    workbench.update_controls()
    assert not workbench.start_button.instate(['disabled'])


def test_running_task_configuration_is_locked(ui):
    _, workbench = ui
    configure(workbench)
    workbench.busy = True
    workbench.task_kind = 'processing'
    workbench.update_controls()
    for control in (workbench.task_text_model_combo, workbench.task_vision_model_combo,
                    workbench.task_dataset_combo, workbench.task_image_dataset_combo):
        assert control.instate(['disabled'])


@pytest.mark.parametrize('size', ['980x660', '1200x790'])
def test_task_configuration_and_retry_controls_fit_window(ui, size):
    root, workbench = ui
    root.attributes('-alpha', 0)
    root.geometry(size)
    root.deiconify()
    root.update()
    workbench.panes.sash_place(0, 0, 360)
    root.update()
    parent = workbench.input_tab
    for control in (workbench.task_text_model_combo, workbench.task_vision_model_combo,
                    workbench.task_dataset_combo, workbench.task_image_dataset_combo,
                    workbench.start_button, workbench.stop_button, workbench.resume_button):
        assert control.winfo_ismapped()
        assert control.winfo_rootx() + control.winfo_width() <= parent.winfo_rootx() + parent.winfo_width()
        assert control.winfo_rooty() + control.winfo_height() <= parent.winfo_rooty() + parent.winfo_height()
    assert workbench.source_tree.winfo_height() >= 65


def test_parallel_events_report_parent_counts(tmp_path):
    from test_parallel_result_regressions import processor, record
    paths = [tmp_path / name for name in ('one.txt', 'two.txt')]
    for path in paths:
        path.write_text('short synthetic text', encoding='utf-8')
    events = []
    proc = processor(tmp_path)
    result = proc.run([record(p) for p in paths], lambda value: events.append(copy.deepcopy(value)))
    assert result['processed_sources'] == 2
    nested = [v for v in events if v.get('progress', {}).get('total') is not None]
    assert nested
    assert all(v['progress']['total'] == v['source_count'] == 2 for v in nested)
    assert all(v['progress']['completed'] == v['processed_sources'] for v in nested)


def test_stopped_unstarted_file_remains_retryable_after_another_retry(tmp_path):
    from script_task import task_snapshot, task_rows
    store=Store(tmp_path/'data')
    first,paths=batch(store,tmp_path)
    waiting=tmp_path/'waiting.txt';waiting.write_text('pending',encoding='utf-8')
    first['status']='stopped'
    first['inputs'].append({'path':str(waiting)})
    store.save(first)
    latest={**first,'run_id':'retry','retry_of':'first','status':'completed',
            'inputs':[first['inputs'][1]],'events':[],
            'items':[{**first['items'][1],'status':'pass','automatic_approval':True,'title':'企业财务管理研究'}]}
    store.save(latest)
    snapshot=task_snapshot(store,latest)
    assert [r['path'] for r in retry_sources(snapshot)]==[str(waiting)]
    row=next(r for r in task_rows(snapshot) if r['path']==str(waiting))
    assert row['script_status']=='不通过' and '未完成' in row['script_reason']


@pytest.mark.parametrize('papers',[1,2])
def test_run_persists_retry_link_and_nonsecret_settings(tmp_path,papers):
    from test_parallel_result_regressions import processor, record
    proc=processor(tmp_path)
    proc.config=copy.deepcopy(proc.config)
    proc.config['concurrency']['papers']=papers
    proc.config['retry_of']='original'
    proc.config['task_settings']={'text_model':'text','destinations':{'paper':{'id':'p'},'image':{'id':'i'}}}
    paths=[tmp_path/'one.txt',tmp_path/'two.txt']
    for p in paths:p.write_text(p.name,encoding='utf-8')
    result=proc.run([record(p) for p in paths],operation='retry_failed')
    saved=proc.store.load(result['run_id'])
    assert saved['retry_of']=='original'
    assert saved['task_settings']==proc.config['task_settings']


def test_retry_does_not_skip_a_result_with_broken_local_output(tmp_path):
    from test_parallel_result_regressions import processor, record
    from auto_pipeline import VERSION
    from engine import digest
    proc=processor(tmp_path)
    path=tmp_path/'paper.txt';path.write_text('short synthetic',encoding='utf-8')
    proc.store.save({'run_id':'broken','status':'completed','items':[{
        'id':'broken','source_path':str(path),'source_hash':digest(path),'status':'pass',
        'processing_version':VERSION,'automatic_approval':True,'title':'企业财务管理研究','major':'会计学'}],
        'human_view':{'issues':[{'id':'broken','reason':'正文文件缺失'}]}})
    result=proc.run([record(path)],operation='retry_failed')
    assert len(result['items'])==1
    assert result['items'][0]['reason_kind']=='screening'


def test_handoff_uses_saved_task_destinations_instead_of_legacy_routing(tmp_path,monkeypatch):
    from auto_ragflow import handoff
    import auto_ragflow
    store=Store(tmp_path/'data')
    package=tmp_path/'package';package.mkdir()
    body=package/'ingest.md';body.write_text('synthetic approved export',encoding='utf-8')
    targets={'paper':{'id':'chosen-paper','name':'论文库'},'image':{'id':'chosen-image','name':'图片库'}}
    manifest={'run_id':'approved','status':'completed','task_settings':{'destinations':targets},
              'items':[{'id':'p','status':'pass','automatic_approval':True,'major':'会计学','title':'企业财务管理研究',
                        'path':str(body),'package_path':str(package),'reviewed_hash':'fixture',
                        'reviewed_package_hash':'fixture','uploads':{}}]}
    store.save(manifest)
    monkeypatch.setattr(auto_ragflow,'digest',lambda path:'fixture')
    monkeypatch.setattr(auto_ragflow,'package_fingerprint',lambda path:'fixture')
    seen=[]
    def upload(store,run_id,ids,client,routes,*args,**kwargs):
        seen.append(routes)
        saved=store.load(run_id)
        saved['items'][0]['uploads'][client.base_url+'|'+routes['paper']['id']]={'status':'uploaded','document_id':'synthetic'}
        store.save(saved)
    monkeypatch.setattr(auto_ragflow,'upload_routed_items',upload)
    class Remote:
        base_url='http://fixture';api_key='fixture'
        def datasets(self):return list(targets.values())+[{'id':'legacy','name':'会计学'}]
    handoff(store,'approved',Remote(),{'会计学':{'paper':'legacy','image':''}},auto_parse=False)
    assert seen==[targets]
    assert store.load('approved')['items'][0]['handoff_status']=='uploaded'
