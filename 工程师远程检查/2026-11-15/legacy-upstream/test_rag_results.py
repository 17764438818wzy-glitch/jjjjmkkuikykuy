"""Third-page remote states must not imply unobserved success or retry hidden work."""
import copy
import pytest
from test_script_task_ui import ui


def load_batch(app):
    app.url.set('http://localhost')
    targets={'paper':{'id':'paper','name':'论文库'},'image':{'id':'images','name':'图片库'}}
    items=[]
    for number,remote in enumerate(('done','failed','queued')):
        items.append({'id':str(number),'title':'企业财务管理研究'+str(number),'source_path':f'C:/input/{number}.docx',
                      'major':'会计学','status':'pass','automatic_approval':True,'uploads':{
                          'http://localhost|paper':{'status':'uploaded','dataset_id':'paper',
                              'dataset_name':'论文库','document_id':'doc'+str(number),'ragflow_status':remote}}})
    manifest={'run_id':'remote','status':'completed','operation':'process','items':items,'events':[],
              'task_settings':{'destinations':targets},'source_count':3,'processed_sources':3}
    app.store.save(manifest);app.current=manifest
    return manifest


@pytest.mark.parametrize('document,expected',[
    ({'run':'0'},'unstarted'),({'run':'1'},'running'),({'run':'2'},'cancelled'),
    ({'run':'3'},'done'),({'run':'4'},'failed'),({'run':'5'},'queued'),
    ({'run':'QUEUED'},'queued'),({'run':'FAILED','progress':1},'failed'),
    ({},'unknown'),({'run':'new-server-value'},'unknown'),
])
def test_remote_status_normalization(document,expected):
    from app import Workbench
    assert Workbench._normalize_remote_doc(document)[0]==expected


def test_saved_target_and_dual_filters_preserve_selection(ui):
    root,app=ui;first=load_batch(app)
    app.connected([{'id':'different','name':'其他库'}]);app.dataset.set('其他库')
    app.render_rag_results()
    state=app._remote_item_status(first['items'][0])
    assert state['target']=='论文库'
    assert hasattr(app,'rag_upload_filter'), 'Upload and processing filters must be independent'
    app.rag_upload_filter.set('已上传');app.rag_process_filter.set('处理失败')
    root.update()
    assert app.rag_result_tree.get_children()==('1',)
    app.rag_select_all_button.invoke();root.update()
    app.render_rag_results();root.update()
    assert app.rag_result_tree.selection()==('1',)
    assert app.rag_selection_count.get()=='已选 1 / 1 个文件'
    app.rag_process_filter.set('已完成');root.update()
    assert not app.rag_result_tree.selection()


def test_query_failure_does_not_overwrite_remote_completion_or_operation(ui,monkeypatch):
    from ragflow_client import RagflowClient
    _,app=ui;first=load_batch(app);app.key.set('synthetic-key')
    errors=[]
    monkeypatch.setattr(app,'error',lambda message:errors.append(message))
    monkeypatch.setattr(app,'async_task',lambda work,done,**kw:done(work()))
    def fail(*args,**kwargs):raise RuntimeError('synthetic connection unavailable')
    monkeypatch.setattr(RagflowClient,'documents',fail)
    app.refresh_rag_status()
    saved=app.store.load(first['run_id'])
    assert saved['operation']=='process'
    record=saved['items'][0]['uploads']['http://localhost|paper']
    assert record['ragflow_status']=='done'
    assert record.get('query_error')
    assert app._remote_item_status(saved['items'][0])['process']=='状态暂不可获取'


def test_partial_image_upload_is_visible_and_filterable(ui):
    _,app=ui;first=load_batch(app)
    first['items'][0]['uploads']['http://localhost|images']={
        'dataset_id':'images','dataset_name':'图片库','route':'image','status':'pending',
        'asset_uploads':[{'path':'assets/figure.png','status':'failed'}]}
    app.store.save(first)
    state=app._remote_item_status(first['items'][0])
    assert state['upload']=='部分上传失败'
    assert '图片' in state['reason']


def test_result_page_shows_destination_pickers_without_script_panel(ui):
    root,app=ui
    root.attributes('-alpha',0);root.geometry('980x660');root.deiconify()
    app.tabs.select(app.rag_result_tab);root.update()
    assert not app.progress_panel.winfo_ismapped()
    for widget in (app.rag_dataset_combo,app.rag_image_dataset_combo,app.rag_import_button,
                   app.rag_select_all_button,app.rag_upload_filter_combo,app.rag_process_filter_combo,app.rag_retry_button):
        assert widget.winfo_ismapped()
        assert widget.winfo_rootx()+widget.winfo_width()<=app.rag_result_tab.winfo_rootx()+app.rag_result_tab.winfo_width()
    app.tabs.select(app.input_tab);root.update()
    assert app.progress_panel.winfo_ismapped()


@pytest.mark.parametrize('run,expected_calls',[('1',0),('3',0),('5',0),('4',1),('2',1),('0',1)])
def test_selected_handoff_only_retries_requested_document(tmp_path,run,expected_calls):
    from engine import Store
    from test_upload_dedup import processed,AssetRemote,DATASET
    from ragflow_client import upload_routed_items
    from auto_ragflow import handoff
    store=Store(tmp_path/'data');first=processed(store,'one');other=processed(store,'two',body='另一篇正文，避免重复。')
    manifest=store.load('one');manifest['items'].append(other);store.save(manifest)
    client=AssetRemote()
    upload_routed_items(store,'one',['one','two'],client,{'paper':DATASET,'image':None})
    saved=store.load('one');doc=saved['items'][0]['uploads']['http://localhost|accounting']['document_id']
    for remote in client.docs:
        if remote['id']==doc:remote['run']=run
    posts=client.posts
    handoff(store,'one',client,{'会计学':{'paper':'accounting','image':''}},item_ids=['one'])
    assert client.posts==posts
    assert len(client.parse_calls)==expected_calls
    assert not client.parse_calls or client.parse_calls==[doc]


def test_remote_retry_uses_selected_rows_only(ui,monkeypatch):
    import auto_ragflow
    root,app=ui;load_batch(app);app.key.set('synthetic-key')
    app.render_rag_results();app.rag_process_filter.set('处理失败');root.update()
    app.rag_select_all_button.invoke();root.update()
    received=[]
    monkeypatch.setattr(app,'async_task',lambda work,done,**kw:done(work()))
    monkeypatch.setattr(auto_ragflow,'handoff',lambda *args,**kwargs:received.append((args[1],kwargs['item_ids'])))
    app.retry_handoff()
    assert received==[('remote',['1'])]


def test_unknown_new_observation_does_not_reuse_old_parse_done():
    from rag_status import processing_state
    assert processing_state({'ragflow_status':'unknown','parse_status':'done'})=='处理状态待确认'


@pytest.mark.parametrize('service',['http://localhost','http://127.0.0.1:80'])
def test_refresh_linked_batch_updates_origin_and_preserves_retry_history(ui,monkeypatch,service):
    from ragflow_client import RagflowClient
    root,app=ui;first=load_batch(app);app.key.set('synthetic-key')
    app.url.set(service)
    retry={'run_id':'later','retry_of':'remote','status':'completed','operation':'retry_ai','items':[],
           'task_settings':first['task_settings'],'inputs':[],'events':[]}
    app.store.save(retry);app.current=retry
    monkeypatch.setattr(app,'async_task',lambda work,done,**kw:done(work()))
    monkeypatch.setattr(RagflowClient,'documents',lambda self,dataset,document_id:[{'id':document_id,'run':'3','chunk_count':42}])
    app.refresh_rag_status()
    assert app.store.load('remote')['items'][1]['uploads']['http://localhost|paper']['ragflow_status']=='done'
    assert app.store.load('later')['operation']=='retry_ai'
    assert len(app.rag_result_tree.get_children())==3
    assert '42' in app.rag_result_tree.item('1','values')[7]


def test_already_running_and_done_rows_do_not_enable_retry(ui):
    root,app=ui;first=load_batch(app);app.render_rag_results()
    app.rag_result_tree.selection_set('0','2');root.update()
    assert app.rag_retry_button.instate(['disabled'])


def test_no_server_contact_when_only_successful_rows_selected(ui,monkeypatch):
    from ragflow_client import RagflowClient
    root,app=ui;load_batch(app);app.key.set('synthetic-key');app.render_rag_results();app.rag_result_tree.selection_set('0','2')
    calls=[]
    monkeypatch.setattr(app,'async_task',lambda *args,**kwargs:calls.append(True))
    app.retry_handoff()
    assert not calls


def test_asset_processing_failure_and_query_error_are_visible(ui):
    _,app=ui;first=load_batch(app)
    record=first['items'][0]['uploads']['http://localhost|paper']
    record['asset_uploads']=[{'path':'tables/one.csv','status':'uploaded','document_id':'asset','ragflow_status':'failed'}]
    state=app._remote_item_status(first['items'][0])
    assert state['process']=='部分处理失败'
    assert 'one.csv' in state['reason']
    record['asset_uploads'][0]['query_error']='查询超时'
    assert app._remote_item_status(first['items'][0])['process']=='状态暂不可获取'


def test_latest_parse_submission_error_is_not_hidden_by_old_failure(ui):
    _,app=ui;first=load_batch(app)
    record=first['items'][1]['uploads']['http://localhost|paper']
    record.update(ragflow_reason='旧失败原因',parse_status='unknown',parse_error='最新解析提交超时')
    assert '最新解析提交超时' in app._remote_item_status(first['items'][1])['reason']


def test_legacy_retry_uses_displayed_receipt_destination_not_current_mapping(ui,monkeypatch):
    import auto_ragflow,auto_config
    root,app=ui;first=load_batch(app);first.pop('task_settings');app.store.save(first)
    app.key.set('synthetic-key');app.render_rag_results();app.rag_result_tree.selection_set('1')
    seen=[]
    monkeypatch.setattr(auto_config,'load_config',lambda root:{'dataset_map':{'会计学':{'paper':'other','image':''}}})
    monkeypatch.setattr(app,'async_task',lambda work,done,**kwargs:done(work()))
    monkeypatch.setattr(auto_ragflow,'handoff',lambda *args,**kwargs:seen.append(kwargs))
    app.retry_handoff()
    assert seen[0].get('target_datasets',{}).get('paper',{}).get('id')=='paper'


def test_selected_recovery_retries_only_failed_assets_not_unstarted_ones(tmp_path):
    from engine import Store
    from test_upload_dedup import processed,AssetRemote,DATASET
    from ragflow_client import upload_routed_items
    from auto_ragflow import handoff
    store=Store(tmp_path/'data');processed(store,'paper');client=AssetRemote()
    upload_routed_items(store,'paper',['paper'],client,{'paper':DATASET,'image':None},
                        upload_config={'skip_derived_upload':False})
    saved=store.load('paper');record=saved['items'][0]['uploads']['http://localhost|accounting']
    record['asset_uploads'].append({'path':'synthetic.csv','status':'uploaded','document_id':'unstarted-asset'})
    client.docs.append({'id':'unstarted-asset','run':'0','dataset_id':'accounting'})
    store.save(saved)
    assets=record['asset_uploads']
    failed=assets[0]['document_id']
    for doc in client.docs:
        doc['run']='3' if doc['id']==record['document_id'] else '4' if doc['id']==failed else '0'
    posts=client.posts
    handoff(store,'paper',client,{'会计学':{'paper':'accounting','image':''}},item_ids=['paper'])
    assert client.posts==posts
    assert client.parse_calls==[failed]


def test_refresh_clears_resolved_submission_error(ui,monkeypatch):
    from ragflow_client import RagflowClient
    _,app=ui;first=load_batch(app);app.key.set('synthetic-key')
    record=first['items'][1]['uploads']['http://localhost|paper']
    record.update(parse_error='上次提交超时',parse_status='unknown');app.store.save(first)
    monkeypatch.setattr(app,'async_task',lambda work,done,**kwargs:done(work()))
    monkeypatch.setattr(RagflowClient,'documents',lambda self,dataset,document_id:[{'id':document_id,'run':'3'}])
    app.refresh_rag_status()
    saved=app.store.load('remote')['items'][1]['uploads']['http://localhost|paper']
    assert not saved.get('parse_error')
