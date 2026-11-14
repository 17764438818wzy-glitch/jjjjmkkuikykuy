"""AI conclusions are distinct from execution failures and upload state."""
import copy
from pathlib import Path
from tkinter import ttk
import pytest
from test_script_task_ui import ui, batch, configure


@pytest.mark.parametrize('mode,script,ai',[
    ('short','rejected','not_started'),('pass','completed','passed'),
    ('reject','completed','rejected'),('timeout','failed','call_failed'),
    ('parse','failed','not_started'),('publish','failed','passed'),
])
def test_pipeline_persists_separate_phase_results(tmp_path,monkeypatch,mode,script,ai):
    from auto_pipeline import AutoProcessor
    from engine import Store
    from agent_client import AgentUnavailable
    from test_workbench import paper
    path=tmp_path/'paper.txt';path.write_text('short' if mode=='short' else paper(),encoding='utf-8')
    class Agent:
        def call(self,task,payload,**kwargs):
            if mode=='timeout':raise AgentUnavailable('合成接口超时')
            if mode=='reject':return {'decision':'reject','reason':'未能满足脱敏要求'}
            return {'decision':'pass','reason':'已检查','sensitive_strings':[],
                    'major':'会计学','title':'企业财务管理研究'}
    if mode=='parse':
        import auto_pipeline
        monkeypatch.setattr(auto_pipeline,'lightweight',lambda *args:(_ for _ in ()).throw(OSError('synthetic')))
    if mode=='publish':
        import knowledge_package
        monkeypatch.setattr(knowledge_package,'publish_knowledge',lambda *args:(_ for _ in ()).throw(OSError('synthetic')))
    processor=AutoProcessor(Store(tmp_path/'data'),{'auto_upload':False,'concurrency':{'enabled':False}})
    processor.agent=Agent()
    events=[]
    result=processor.run([{'path':str(path),'name':path.name}],lambda payload:events.append(copy.deepcopy(payload)))
    item=result['items'][0]
    assert item['script_result']['status']==script
    assert item['ai_result']['status']==ai
    assert item['status']==('pass' if mode=='pass' else 'reject'), 'Admission safety remains independent'
    if mode not in {'short','parse'}:
        assert item['ai_steps']
        assert any(e.get('ai_item') for e in events)
    if mode=='reject':
        assert any(e.get('progress',{}).get('stage')=='脚本执行完成' for e in events)
        assert not any(e.get('progress',{}).get('stage')=='不通过' for e in events)


def test_legacy_ambiguous_technical_error_is_not_model_rejection():
    from processing_outcomes import ai_outcome
    label,reason,stage=ai_outcome({'status':'reject','reason_kind':'technical','reason':'旧版处理失败'})
    assert label=='待核对'
    assert '旧' in reason
    assert ai_outcome({'status':'reject','reason_kind':'duplicate'})[0]=='未进入 AI'


def test_ai_page_removes_upload_and_processing_actions(ui,monkeypatch):
    root,app=ui
    assert 'AI 处理结果' in app.tabs.tab(app.result_tab,'text')
    def descendants(w):
        return [c for child in w.winfo_children() for c in [child]+descendants(child)]
    labels=[str(w.cget('text')) for w in descendants(app.result_tab) if isinstance(w,ttk.Button)]
    assert not {'继续未通过','导入所选','刷新知识库'} & set(labels)
    app.key.set('synthetic-key')
    connections=[]
    monkeypatch.setattr(app,'connect',lambda:connections.append(True))
    app.tabs.select(app.result_tab);root.update()
    assert not connections, 'Viewing local AI results must not connect to RAGFlow'


def test_ai_page_filters_call_failure_separately_and_keeps_retry_history(ui,tmp_path,monkeypatch):
    _,app=ui
    first,paths=batch(app.store,tmp_path)
    first['items'][0]['ai_result']={'status':'passed','reason':'脱敏完成','stage':'文本脱敏'}
    first['items'][1]['ai_result']={'status':'call_failed','reason':'请求超时','stage':'图表识别'}
    app.store.save(first)
    latest={**first,'run_id':'retry','retry_of':'first','inputs':[first['inputs'][1]],
            'items':[{**first['items'][1],'ai_result':{'status':'rejected','reason':'脱敏结果未达要求','stage':'文本脱敏'}}],
            'events':[]}
    app.store.save(latest);app.current=latest
    app.render_preprocess_results()
    assert len(app.result_tree.get_children())==3
    app.result_filter.set('不通过');app.render_preprocess_results()
    assert len(app.result_tree.get_children())==1
    app.result_tree.selection_set(app.result_tree.get_children())
    shown=[];monkeypatch.setattr(app,'text_window',lambda title,text:shown.append(text))
    app.preview()
    assert '脱敏结果未达要求' in shown[0]
    app.result_filter.set('调用失败');app.render_preprocess_results()
    assert not app.result_tree.get_children()


def test_script_retry_excludes_ai_business_rejection(ui,tmp_path,monkeypatch):
    _,app=ui;configure(app)
    manifest,paths=batch(app.store,tmp_path)
    manifest['items'][1].update(script_result={'status':'completed','reason':'脚本执行完成'},
                                ai_result={'status':'rejected','reason':'AI检查未通过'})
    app.store.save(manifest);app.current=manifest;app.show_script_results(manifest)
    selected=[];monkeypatch.setattr(app,'start_automatic',lambda rows:selected.extend(rows))
    app.retry_failed()
    assert not selected
    assert app.source_tree.item('1','values')[1]=='通过'


@pytest.mark.parametrize('size',['980x660','1200x790'])
def test_ai_result_controls_fit(ui,size):
    root,app=ui;root.attributes('-alpha',0);root.geometry(size);root.deiconify()
    app.tabs.select(app.result_tab);root.update()
    for control in (app.ai_result_filter,app.ai_preview_button,app.ai_report_button):
        assert control.winfo_ismapped()
        assert control.winfo_rootx()+control.winfo_width()<=app.result_tab.winfo_rootx()+app.result_tab.winfo_width()
        assert control.winfo_rooty()+control.winfo_height()<=app.result_tab.winfo_rooty()+app.result_tab.winfo_height()


def test_interrupted_active_ai_request_remains_retryable(tmp_path):
    from engine import Store
    from script_task import task_snapshot
    from app import retry_sources
    from processing_outcomes import ai_outcome
    store=Store(tmp_path/'data')
    item={'id':'active','source_path':str(tmp_path/'paper.txt'),'status':'pending',
          'script_result':{'status':'completed'},'ai_result':{'status':'running','stage':'文本脱敏'}}
    manifest={'run_id':'interrupted','status':'interrupted','items':[],
              'active_items':[item],'inputs':[{'path':item['source_path']}]}
    store.save(manifest)
    snapshot=task_snapshot(store,manifest)
    assert ai_outcome(snapshot['items'][0],'interrupted')[0]=='未完成'
    assert [r['path'] for r in retry_sources(snapshot,scope='script')]==[item['source_path']]


def test_parallel_request_failures_retain_distinct_ai_records(tmp_path):
    from auto_pipeline import AutoProcessor
    from engine import Store
    from agent_client import AgentUnavailable
    from test_workbench import paper
    class Agent:
        def call(self,*args,**kwargs):raise AgentUnavailable('合成接口超时')
    paths=[tmp_path/'one.txt',tmp_path/'two.txt']
    for n,path in enumerate(paths):path.write_text(paper()+str(n),encoding='utf-8')
    processor=AutoProcessor(Store(tmp_path/'data'),{'auto_upload':False,'concurrency':{'enabled':True,'papers':2}})
    processor.agent=Agent()
    result=processor.run([{'path':str(p),'name':p.name} for p in paths])
    assert len(result['items'])==2 and not result.get('active_items')
    assert all(i['ai_result']['status']=='call_failed' and i['ai_steps'] for i in result['items']), [(i.get('reason'),i.get('ai_result')) for i in result['items']]
    assert {i['source_path'] for i in result['items']}=={str(p) for p in paths}


def test_retry_history_previews_original_approved_artifact(ui,tmp_path,monkeypatch):
    _,app=ui
    first,_=batch(app.store,tmp_path)
    package=app.store.runs/'first'/'packages'/'good';package.mkdir(parents=True)
    body=package/'document.md';body.write_text('original approved result',encoding='utf-8')
    first['items'][0].update(package_path=str(package),display_path=str(body),
                             ai_result={'status':'passed','reason':'通过'})
    app.store.save(first)
    latest={**first,'run_id':'retry','retry_of':'first','items':[first['items'][1]],'events':[],
            'inputs':[first['inputs'][1]]}
    app.store.save(latest);app.current=latest;app.render_preprocess_results()
    app.result_tree.selection_set('good')
    seen=[];monkeypatch.setattr(app,'text_window',lambda title,text:seen.append(text))
    app.preview()
    assert 'original approved result' in seen[0]


def test_ai_page_receives_live_updates_without_ai_messages_in_script_log(ui,tmp_path):
    _,app=ui
    manifest,paths=batch(app.store,tmp_path)
    manifest['status']='running';manifest['items']=[];manifest['events']=[];manifest['processed_sources']=0
    active={'id':'active','source_path':str(paths[0]),'status':'pending','title':'企业财务管理研究',
            'script_result':{'status':'completed'},'ai_result':{'status':'running','stage':'文本脱敏'}}
    manifest['active_items']=[active];app.store.save(manifest)
    app.tabs.select(app.result_tab)
    app.show_script_progress({'run_id':manifest['run_id'],'source_count':3,'processed_sources':0,
        'ai_item':active,'progress':{'stage':'AI 处理','source_path':str(paths[0])}})
    assert app.result_tree.item('active','values')[3]=='处理中'
    assert 'AI 处理' not in app.log_text.get('1.0','end')
    assert float(app.progress_bar['value'])==0
    assert '已结束 0/3' in app.progress_count.get()


def test_historical_ai_messages_stay_out_of_script_progress_panel(ui):
    _,app=ui
    app.show_progress({'stage':'AI 文本脱敏','message':'model processing'})
    app.show_progress({'stage':'续跑复用','message':'cached model result'})
    assert not app.log_text.get('1.0','end').strip()
