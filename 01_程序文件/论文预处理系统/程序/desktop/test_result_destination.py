"""RAGFlow result-page destination selection must route uploads to the visible choice."""
import time
import tkinter as tk

import pytest

from app import Workbench
from engine import digest
from ragflow_client import RagflowClient

DATASETS = [{'id':'test-kb','name':'会计学测试'}, {'id':'formal-kb','name':'会计学'}]


@pytest.fixture
def ui(tmp_path, monkeypatch):
    monkeypatch.delenv('RAGFLOW_API_KEY', raising=False)
    root = tk.Tk()
    root.attributes('-alpha', 0)
    app = Workbench(root, tmp_path / 'data')
    root.update()
    yield root, app
    for callback in root.tk.call('after','info'):
        root.after_cancel(callback)
    root.destroy()


def settle(root, app):
    deadline = time.monotonic() + 5
    root.update()
    while app.busy and time.monotonic() < deadline:
        root.update()
        time.sleep(.01)
    assert not app.busy


def test_result_page_loads_saved_connection_without_changing_tabs(ui, monkeypatch):
    root, app = ui
    app.key.set('fixture-key')
    app.dataset_map = {'会计学':'formal-kb'}
    monkeypatch.setattr(RagflowClient,'datasets',lambda _: DATASETS)
    app.tabs.select(app.rag_result_tab)
    settle(root, app)
    assert app.datasets == [], 'Viewing stored results must not connect implicitly'
    assert app.tabs.select() == str(app.rag_result_tab)


def test_result_destination_controls_fit_minimum_window(ui):
    root, app = ui
    root.geometry('980x660')
    app.tabs.select(app.rag_result_tab)
    root.update()
    for widget in (app.rag_upload_filter_combo, app.rag_process_filter_combo, app.rag_retry_button):
        assert widget.winfo_ismapped()
        assert widget.winfo_width() > 50
        assert widget.winfo_rootx() >= app.rag_result_tab.winfo_rootx()
        assert widget.winfo_rootx()+widget.winfo_width() <= app.rag_result_tab.winfo_rootx()+app.rag_result_tab.winfo_width()
        assert widget.winfo_rooty()+widget.winfo_height() <= app.rag_result_tab.winfo_rooty()+app.rag_result_tab.winfo_height()


def test_first_page_exposes_paper_and_image_destinations(ui):
    _, app = ui
    app.connected(DATASETS)
    assert app.task_image_dataset_combo.winfo_exists()
    assert tuple(app.task_image_dataset_combo['values']) == tuple(app.task_dataset_combo['values'])
    app.task_dataset_combo.current(0)
    app.task_image_dataset_combo.current(1)
    assert app.dataset.get() == '会计学测试'
    assert app.image_dataset.get() == '会计学'
    assert app.task_image_dataset_combo is not app.task_dataset_combo


def test_save_ragflow_settings_persists_visible_route_selection(ui):
    from auto_config import load_config
    from tkinter import ttk

    root,app=ui
    app.connected(DATASETS)
    app.route_major.set('会计学')
    app.dataset_combo.current(1)
    app.image_dataset_combo.current(0)

    def descendants(widget):
        for child in widget.winfo_children():
            yield child
            yield from descendants(child)

    button=next(widget for widget in descendants(app.rag_content)
                if isinstance(widget,ttk.Button) and widget.cget('text')=='保存 RAGFlow 设置')
    button.invoke();root.update()

    assert load_config(app.store.root)['dataset_map']['会计学']=={
        'paper':'formal-kb','image':'test-kb'}


def test_selected_destination_survives_refresh_order_and_syncs_both_pages(ui):
    _, app = ui
    app.connected(DATASETS)
    app.task_dataset_combo.current(1)
    assert app.dataset_combo.current() == 1
    app.connected(list(reversed(DATASETS)))
    assert app.task_dataset_combo.current() == 0
    assert app.dataset_combo.get() == '会计学'
    app.dataset_combo.current(1)
    assert app.task_dataset_combo.get() == '会计学测试'


def test_removed_selected_destination_does_not_silently_pick_another(ui):
    _, app = ui
    app.connected(DATASETS)
    app.task_dataset_combo.current(1)
    app.connected([DATASETS[0]])
    assert app.task_dataset_combo.current() == -1
    assert app.dataset_combo.current() == -1


def test_manual_upload_uses_selected_dataset_and_records_only_that_destination(ui, monkeypatch):
    root, app = ui
    app.url.set('http://localhost')
    app.key.set('fixture-key')
    app.connected(DATASETS)
    app.task_dataset_combo.current(1)
    path = app.store.root / 'clean.md'
    path.write_text('# 企业财务研究\n\n已脱敏测试正文。',encoding='utf-8')
    item = {'id':'approved-paper','title':'企业财务研究','major':'会计学',
        'status':'pass','automatic_approval':True,'reason':'通过',
        'path':str(path),'reviewed_hash':digest(path),'uploads':{}}
    app.current = {'run_id':'result-import','status':'completed','pipeline':'automatic',
        'started_at':'2026-09-15','message':'通过 1','items':[item],'events':[]}
    app.store.save(app.current)
    app.render_results()
    app.rag_result_tree.selection_set('approved-paper')
    received = []
    def upload(client, dataset_id, uploaded_path):
        received.append((dataset_id,uploaded_path.read_text(encoding='utf-8')))
        return 'remote-approved-paper'
    monkeypatch.setattr(RagflowClient,'upload',upload)
    app.upload()
    settle(root, app)
    assert received == [('formal-kb','# 企业财务研究\n\n已脱敏测试正文。')]
    records = app.store.load('result-import')['items'][0]['uploads']
    assert set(records) == {'http://localhost|formal-kb'}
    assert records['http://localhost|formal-kb']['document_id'] == 'remote-approved-paper'


def test_busy_import_prevents_switching_destination(ui):
    _, app = ui
    app.connected(DATASETS)
    app.busy = True
    app.update_controls()
    for widget in (app.task_dataset_combo,app.dataset_combo,
                   app.rag_status_button,app.rag_retry_button):
        assert widget.instate(['disabled'])


def test_refresh_button_fetches_new_datasets_in_place(ui, monkeypatch):
    root, app = ui
    app.key.set('fixture-key')
    app.connected(DATASETS)
    app.task_dataset_combo.current(1)
    app.tabs.select(app.rag_result_tab)
    root.update()
    updated = list(reversed(DATASETS)) + [{'id':'new-kb','name':'财务管理'}]
    monkeypatch.setattr(RagflowClient,'datasets',lambda _:updated)
    app.connect()
    settle(root,app)
    assert app.datasets == updated
    assert app.task_dataset_combo.get() == '会计学'
    assert app.tabs.select() == str(app.rag_result_tab)


def test_response_from_previous_server_cannot_become_new_destination(ui):
    _, app = ui
    app.connected(DATASETS)
    app.url.set('http://localhost:9380')
    app.connected(DATASETS,base_url='http://localhost')
    assert app.datasets == []
    assert app.task_dataset_combo.current() == -1


def test_duplicate_dataset_names_remain_distinguishable(ui):
    _, app = ui
    app.connected([{'id':'team-one','name':'会计学'},{'id':'team-two','name':'会计学'}])
    values = tuple(app.task_dataset_combo['values'])
    assert len(set(values)) == 2
    app.task_dataset_combo.current(1)
    assert app.datasets[app.dataset_combo.current()]['id'] == 'team-two'


def test_deleted_explicit_choice_stays_empty_on_repeated_refresh_with_saved_route(ui):
    _,app=ui
    app.dataset_map={'会计学':'formal-kb'}
    app.connected(DATASETS)
    app.task_dataset_combo.current(0)
    app.connected([DATASETS[1]])
    app.connected([DATASETS[1]])
    assert app.task_dataset_combo.current()==-1
    assert app.dataset_combo.current()==-1


def test_literal_name_cannot_collide_with_disambiguated_label(ui):
    _,app=ui
    app.connected([{'id':'a','name':'X'},{'id':'b','name':'X'},{'id':'c','name':'X  [a]'}])
    app.task_dataset_combo.current(2)
    assert app.dataset_combo.current()==2
    assert app.datasets[app.dataset_combo.current()]['id']=='c'
