"""Successful library reads must expose usable choices on the connection pages."""
import copy
import io
import json
import time

from ragflow_client import RagflowClient
from test_script_task_ui import ui


def test_successful_read_allows_library_selection_without_changing_active_task(ui,monkeypatch):
    root,app=ui
    app.url.set('http://localhost')
    app.key.set('synthetic-key')
    app.busy=True
    app.task_kind='processing'
    token=object()
    app._active_task=token
    frozen={'task_datasets':{'paper':{'id':'active-paper'},'image':{'id':'active-image'}},
            'task_settings':{'destinations':{'paper':{'id':'active-paper'},'image':{'id':'active-image'}}}}
    app.processor.config=copy.deepcopy(frozen)
    calls=[]
    class Transport:
        def open(self,request,timeout):
            calls.append(request.get_method())
            return io.BytesIO(json.dumps({'code':0,'data':[{'id':'later-paper','name':'论文库'},
                                {'id':'later-images','name':'图片库'}]}).encode())
    def client(url,key):
        instance=RagflowClient(url,key)
        instance.opener=Transport()
        return instance
    monkeypatch.setattr('app.RagflowClient',client)
    app.connect()
    deadline=time.monotonic()+2
    while app._dataset_read_task and time.monotonic()<deadline:
        root.update()
        time.sleep(.01)
    assert calls==['GET']
    for combo in (app.dataset_combo,app.image_dataset_combo,app.rag_dataset_combo,app.rag_image_dataset_combo):
        assert tuple(combo.cget('values'))==('论文库','图片库')
        assert combo.instate(['readonly','!disabled']), 'API succeeded but the actual dropdown remains disabled'
    app.dataset_combo.current(0)
    app.image_dataset_combo.current(1)
    root.update()
    assert app.dataset.get()=='论文库' and app.image_dataset.get()=='图片库'
    assert app.processor.config==frozen
    assert app.busy and app.task_kind=='processing' and app._active_task is token
    assert app.stop_button.instate(['!disabled'])
    assert app.task_dataset_combo.instate(['disabled']) and app.task_image_dataset_combo.instate(['disabled'])
    assert app.rag_import_button.instate(['disabled'])


def test_connection_feedback_is_next_to_read_button_above_library_choices(ui):
    root,app=ui
    root.attributes('-alpha',0)
    root.deiconify()
    app.tabs.select(app.rag_tab)
    app.connection.set('连接成功，共 12 个知识库。')
    root.update()
    status=getattr(app,'rag_connection_status',None)
    assert status is not None, 'Read status is buried below the scrollable fields'
    assert status.winfo_ismapped()
    assert status.winfo_rooty()<app.dataset_combo.winfo_rooty()
    assert status.winfo_rootx()>app.rag_connection_button.winfo_rootx()
    assert status.winfo_rooty()+status.winfo_height()<=app.rag_content.master.winfo_rooty()+app.rag_content.master.winfo_height()


def test_nonprocessing_operations_keep_destination_fields_locked(ui):
    _,app=ui
    app.busy=True
    app.task_kind='operation'
    app.update_controls()
    assert all(combo.instate(['disabled']) for combo in (
        app.dataset_combo,app.image_dataset_combo,app.rag_dataset_combo,app.rag_image_dataset_combo))


def test_idle_read_failure_replaces_previous_success_feedback_and_redacts_key(ui,monkeypatch):
    root,app=ui
    app.url.set('http://localhost')
    secret='synthetic-private-key'
    app.key.set(secret)
    app.connection.set('连接成功，共 12 个知识库。')
    errors=[]
    monkeypatch.setattr(app,'error',lambda error:errors.append(str(error)))
    class Client:
        base_url='http://localhost'
        api_key=secret
        def datasets(self):raise RuntimeError('synthetic failure '+secret)
    monkeypatch.setattr('app.RagflowClient',lambda *args:Client())
    app.connect()
    assert '正在读取' in app.connection.get()
    deadline=time.monotonic()+2
    while app.busy and time.monotonic()<deadline:
        root.update()
        time.sleep(.01)
    assert not app.busy and app._active_task is None
    assert '知识库读取失败' in app.connection.get()
    assert app.rag_activity.get()==app.connection.get()
    assert len(errors)==1
    assert secret not in app.connection.get() and secret not in errors[0]


def test_invalid_connection_parameters_show_failure_beside_read_button(ui,monkeypatch):
    _,app=ui
    app.url.set('invalid://synthetic')
    app.connection.set('连接成功，共 12 个知识库。')
    errors=[]
    monkeypatch.setattr(app,'error',lambda error:errors.append(str(error)))
    app.connect()
    assert errors
    assert '知识库读取失败' in app.connection.get()


def test_idle_success_replaces_reading_feedback_on_both_connection_pages(ui,monkeypatch):
    root,app=ui
    app.url.set('http://localhost')
    app.key.set('synthetic-key')
    class Client:
        base_url='http://localhost'
        api_key='synthetic-key'
        def datasets(self):return [{'id':'one','name':'synthetic library'}]
    monkeypatch.setattr('app.RagflowClient',lambda *args:Client())
    app.connect()
    deadline=time.monotonic()+2
    while app.busy and time.monotonic()<deadline:
        root.update()
        time.sleep(.01)
    assert not app.busy
    assert '连接成功' in app.connection.get()
    assert app.rag_activity.get()==app.connection.get()
