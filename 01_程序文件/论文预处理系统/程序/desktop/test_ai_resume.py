"""AI-only recovery must not parse sources or regenerate native packages."""
import copy
from pathlib import Path

import pytest

from auto_pipeline import AutoProcessor
from engine import Store
from test_stage_resume import CONFIG, KEY, Transport, response, document
from test_script_task_ui import ui, configure


def rejected_run(tmp_path, store=None, parallel=False):
    store = store or Store(tmp_path/'data')
    sources=[]
    for number in range(2 if parallel else 1):
        path=tmp_path/f'paper{number}.docx';document(path)
        if number:
            from docx import Document
            doc=Document(path);doc.add_paragraph('第二篇研究补充材料');doc.save(path)
        sources.append({'path':str(path),'name':path.name})
    cfg=copy.deepcopy(CONFIG);cfg['concurrency']['enabled']=parallel
    processor=AutoProcessor(store,cfg,KEY)
    processor.agent.opener=Transport(lambda message: {'decision':'reject','reason':'文本脱敏需要调整'}
                                    if message['task']=='privacy_text' else response(message['task']))
    result=processor.run(sources)
    assert all(i['ai_result']['status']=='rejected' for i in result['items'])
    return store,cfg,result


def records(result):
    return [{'path':i['source_path'],'name':Path(i['source_path']).name,
             'prepared_checkpoint':i.get('prepared_checkpoint')} for i in result['items']]


@pytest.mark.parametrize('parallel',[False,True])
def test_ai_retry_publishes_without_native_preprocessing(tmp_path,monkeypatch,parallel):
    store,cfg,first=rejected_run(tmp_path,parallel=parallel)
    import auto_pipeline
    def forbidden(*args,**kwargs):raise AssertionError('AI retry re-entered native preprocessing')
    monkeypatch.setattr(auto_pipeline,'lightweight',forbidden)
    monkeypatch.setattr(auto_pipeline,'build_package',forbidden)
    monkeypatch.setattr(auto_pipeline,'convert_office',forbidden)
    cfg['retry_of']=first['run_id']
    processor=AutoProcessor(store,cfg,KEY);processor.agent.opener=Transport()
    result=processor.run(records(first),operation='retry_ai')
    assert len(result['items'])==len(first['items'])
    assert all(i['status']=='pass' and i['automatic_approval'] for i in result['items']),result['items']
    assert result['retry_of']==first['run_id']
    assert all(i.get('prepared_checkpoint') for i in result['items'])
    assert not any(m['task']=='privacy_image' for m,_ in processor.agent.opener.requests)
    assert all(Path(i['path']).is_file() for i in result['items'])


@pytest.mark.parametrize('damage',['source','missing','package','outside','legacy'])
def test_invalid_ai_resume_stops_before_model_and_native_work(tmp_path,damage):
    store,cfg,first=rejected_run(tmp_path)
    selected=records(first)
    checkpoint=first['items'][0].get('prepared_checkpoint')
    assert checkpoint, 'Native preparation must persist before AI starts'
    if damage=='source':Path(selected[0]['path']).write_text('changed',encoding='utf-8')
    elif damage=='missing':Path(checkpoint['path']).unlink()
    elif damage=='package':
        next((Path(checkpoint['path']).parent/'package').rglob('*.png')).write_bytes(b'changed')
    elif damage=='outside':selected[0]['prepared_checkpoint']={**checkpoint,'path':str(tmp_path/'outside.json')}
    else:selected[0].pop('prepared_checkpoint')
    processor=AutoProcessor(store,cfg,KEY);processor.agent.opener=Transport()
    with pytest.raises(ValueError):processor.run(selected,operation='retry_ai')
    assert not processor.agent.opener.requests


def test_ai_page_selects_filtered_rows_and_keeps_selection_through_settings(ui,tmp_path,monkeypatch):
    root,app=ui;configure(app)
    _,_,first=rejected_run(tmp_path,store=app.store,parallel=True)
    app.current=first;app.render_preprocess_results()
    captured=[]
    monkeypatch.setattr(app,'start_automatic',lambda selected:captured.append((app._pending_operation,selected)))
    app.result_filter.set('不通过')
    app.ai_result_search.set('paper0')
    assert hasattr(app,'ai_select_all_button'), 'Selection controls belong above the AI result list'
    app.ai_select_all_button.invoke();root.update()
    assert len(app.result_tree.selection())==1
    assert app.ai_selection_count.get()=='已选 1 / 1 个文件'
    app.ai_settings_button.invoke();root.update()
    app.ai_vars['text_model'].set('adjusted-model')
    app.tabs.select(app.result_tab);root.update()
    assert len(app.result_tree.selection())==1
    app.ai_retry_selected_button.invoke()
    assert captured[0][0]=='retry_ai' and len(captured[0][1])==1
    assert captured[0][1][0]['name']=='paper0.docx'
    app.ai_result_search.set('no match')
    app.retry_ai()
    assert len(captured)==1, 'Hidden rows must not be retried'
    app.ai_result_search.set('');app.ai_select_all_button.invoke();root.update()
    assert app.ai_selection_count.get()=='已选 2 / 2 个文件'
    app.ai_invert_button.invoke();root.update()
    assert not app.result_tree.selection()
    assert app.ai_retry_selected_button.instate(['disabled'])
    app.ai_select_all_button.invoke();root.update();app.ai_retry_selected_button.invoke()
    assert len(captured[1][1])==2
    app.ai_select_none_button.invoke();root.update()
    assert not app.result_tree.selection()
    app.busy=True;app.retry_ai()
    assert len(captured)==2


def test_retry_start_keeps_original_destinations_and_script_progress(ui,tmp_path,monkeypatch):
    _,app=ui;configure(app)
    _,_,first=rejected_run(tmp_path,store=app.store)
    first['task_settings']={'destinations':{'paper':{'id':'original-paper','name':'原论文库'},
                                           'image':{'id':'original-images','name':'原图片库'}}}
    app.store.save(first);app.current=first;app.show_script_results(first)
    app.progress_count.set('原脚本进度保留')
    app.log_text.configure(state='normal');app.log_text.insert('end','原脚本日志');app.log_text.configure(state='disabled')
    # Resume must use the saved destination even without a connected library picker.
    app.datasets=[];app.dataset.set('');app.image_dataset.set('')
    scheduled=[];errors=[]
    monkeypatch.setattr(app,'async_task',lambda work,done,**kw:scheduled.append((work,done)))
    monkeypatch.setattr(app,'error',lambda message:errors.append(str(message)))
    app._pending_operation='retry_ai';app.start_automatic(records(first))
    assert not errors,errors
    assert scheduled
    assert app.processor.config['task_datasets']['paper']['id']=='original-paper'
    assert app.processor.config['retry_of']==first['run_id']
    assert app.processor.config['text_model']=='text-fixture'
    assert app.progress_count.get()=='原脚本进度保留'
    assert '原脚本日志' in app.log_text.get('1.0','end')


def test_ai_retry_progress_does_not_enter_script_log(ui,tmp_path):
    _,app=ui
    _,_,first=rejected_run(tmp_path,store=app.store)
    first['operation']='retry_ai';app.store.save(first)
    app.progress_count.set('脚本已完成')
    app.show_script_progress({**first,'progress':{'stage':'生成论文知识包','message':'AI续跑中的收尾'}})
    assert app.progress_count.get()=='本轮已结束 1/1'
    assert 'AI续跑中的收尾' not in app.log_text.get('1.0','end')


def test_ai_retry_buttons_fit_and_disable_while_busy(ui,tmp_path):
    root,app=ui
    _,_,first=rejected_run(tmp_path,store=app.store)
    app.current=first;app.render_preprocess_results()
    root.attributes('-alpha',0);root.geometry('980x660');root.deiconify()
    app.tabs.select(app.result_tab);root.update()
    app.result_tree.selection_set(app.result_tree.get_children()[0]);root.update()
    assert app.ai_retry_selected_button.instate(['!disabled'])
    for button in (app.ai_select_all_button,app.ai_select_none_button,app.ai_invert_button,
                   app.ai_retry_selected_button,app.ai_settings_button,app.ai_stop_button):
        assert button.winfo_rootx()+button.winfo_width()<=app.result_tab.winfo_rootx()+app.result_tab.winfo_width()
        assert button.winfo_rooty()+button.winfo_height()<=app.result_tab.winfo_rooty()+app.result_tab.winfo_height()
    assert app.ai_select_all_button.winfo_rooty()+app.ai_select_all_button.winfo_height()<=app.result_tree.winfo_rooty()
    app.busy=True;app.update_controls()
    assert app.ai_retry_selected_button.instate(['disabled'])
    assert app.ai_select_all_button.instate(['disabled'])


def test_second_ai_retry_keeps_checkpoint_and_previous_reason(tmp_path):
    from ai_resume import retry_records
    store,cfg,first=rejected_run(tmp_path)
    retry=AutoProcessor(store,{**cfg,'retry_of':first['run_id']},KEY)
    retry.agent.opener=Transport(lambda message:{'decision':'reject','reason':'仍需调整'}
                                if message['task']=='privacy_text' else response(message['task']))
    again=retry.run(retry_records(first['items']),operation='retry_ai')
    assert again['items'][0]['ai_result']['status']=='rejected'
    assert again['items'][0]['previous_ai_reason']=='文本脱敏需要调整'
    final=AutoProcessor(store,{**cfg,'retry_of':again['run_id']},KEY);final.agent.opener=Transport()
    passed=final.run(retry_records(again['items']),operation='retry_ai')
    assert passed['items'][0]['automatic_approval']
    assert passed['items'][0]['previous_ai_reason']=='仍需调整'


def test_changed_vision_model_rechecks_images_but_skips_native_work(tmp_path,monkeypatch):
    store,cfg,first=rejected_run(tmp_path)
    import auto_pipeline
    monkeypatch.setattr(auto_pipeline,'build_package',lambda *a,**k:pytest.fail('Native extraction repeated'))
    cfg['vision_model']='different-vision-model'
    retry=AutoProcessor(store,cfg,KEY);retry.agent.opener=Transport()
    result=retry.run(records(first),operation='retry_ai')
    assert result['items'][0]['automatic_approval']
    assert sum(m['task']=='privacy_image' for m,_ in retry.agent.opener.requests)==2


def test_prepared_ai_timeout_belongs_to_ai_page_not_script_retry(tmp_path):
    from app import retry_sources
    from processing_outcomes import execution_outcome
    from agent_client import AgentUnavailable
    store,cfg,first=rejected_run(tmp_path)
    retry=AutoProcessor(store,cfg,KEY)
    def timeout(message):raise AgentUnavailable('合成接口超时')
    retry.agent.opener=Transport(timeout)
    result=retry.run(records(first),operation='retry_ai')
    assert result['items'][0]['ai_result']['status']=='call_failed'
    assert execution_outcome(result['items'][0],result)[0]=='通过'
    assert not retry_sources(result,scope='script')


def test_resume_rejects_checkpoint_version_and_source_deletion(tmp_path):
    from ai_resume import validate_prepared
    from auto_pipeline import VERSION
    store,_,first=rejected_run(tmp_path)
    record=records(first)[0]
    with pytest.raises(ValueError,match='版本'):
        validate_prepared(record,store.root,VERSION+'-changed')
    Path(record['path']).unlink()
    with pytest.raises(ValueError,match='缺失'):
        validate_prepared(record,store.root,VERSION)


def test_stop_during_resume_validation_is_not_discarded(tmp_path,monkeypatch):
    import ai_resume
    store,cfg,first=rejected_run(tmp_path)
    retry=AutoProcessor(store,cfg,KEY);retry.agent.opener=Transport()
    validate=ai_resume.validate_prepared
    validations=[]
    def stop_after_validation(*args):
        result=validate(*args)
        if not validations:retry.stop.set()
        validations.append(True)
        return result
    monkeypatch.setattr(ai_resume,'validate_prepared',stop_after_validation)
    result=retry.run(records(first),operation='retry_ai')
    assert result['status']=='stopped'
    assert not retry.agent.opener.requests


def test_stop_after_ai_pass_can_finish_from_ai_page_without_model_calls(tmp_path,monkeypatch):
    from ai_resume import retry_records
    path=tmp_path/'paper.docx';document(path)
    store=Store(tmp_path/'data');processor=AutoProcessor(store,CONFIG,KEY)
    processor.agent.opener=Transport()
    def stop_before_publish(payload):
        if payload.get('progress',{}).get('stage')=='生成论文知识包':processor.stop.set()
    stopped=processor.run([{'path':str(path),'name':path.name}],stop_before_publish)
    assert stopped['items'][0]['ai_result']['status']=='passed'
    selected=retry_records(stopped['items'])
    assert len(selected)==1, 'Passed AI with unfinished local publication must remain resumable here'
    import auto_pipeline
    monkeypatch.setattr(auto_pipeline,'build_package',lambda *a,**k:pytest.fail('Native extraction repeated'))
    retry=AutoProcessor(store,CONFIG,KEY);retry.agent.opener=Transport()
    result=retry.run(selected,operation='retry_ai')
    assert result['items'][0]['automatic_approval']
    assert not retry.agent.opener.requests


def test_parallel_merge_copy_failure_keeps_ai_recovery_checkpoint(tmp_path,monkeypatch):
    import shutil
    from ai_resume import retry_records,validate_prepared
    from auto_pipeline import VERSION
    copy2=shutil.copy2
    def fail_parent_copy(source,destination,*args,**kwargs):
        target=Path(destination)
        if target.parent.name=='rejected' and 'private' not in target.parts:
            raise PermissionError('synthetic merge copy failure')
        return copy2(source,destination,*args,**kwargs)
    monkeypatch.setattr(shutil,'copy2',fail_parent_copy)
    store,_,first=rejected_run(tmp_path,parallel=True)
    assert len(first['items'])==2
    for record in retry_records(first['items']):
        assert record['prepared_checkpoint'], 'Parent merge failure must retain child preparation'
        validate_prepared(record,store.root,VERSION)
