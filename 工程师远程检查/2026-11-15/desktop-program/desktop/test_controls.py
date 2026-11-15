import tkinter as tk
from tkinter import ttk
import pytest
from app import Workbench


def test_evidence_preview_and_legacy_page_filter(ui,tmp_path):
    import json
    from PIL import Image
    from evidence_ui import open_review
    root,app=ui
    package=tmp_path/'evidence';package.mkdir()
    Image.new('RGB',(120,80),'blue').save(package/'figure.png')
    Image.new('RGB',(120,180),'white').save(package/'page.png')
    resources=[{'id':'figure','kind':'image','local_path':'figure.png','privacy_status':'pending'},
               {'id':'page','kind':'source_page','local_path':'page.png','privacy_status':'pending'}]
    (package/'metadata.json').write_text(json.dumps({'resources':resources}),encoding='utf-8')
    window=open_review(app,{'package_path':str(package),'title':'图片预览测试'});window.attributes('-alpha',0)
    window.geometry('880x680');root.update()
    def descendants(w):
        return [c for child in w.winfo_children() for c in [child]+descendants(child)]
    widgets=descendants(window)
    tree=next(w for w in widgets if isinstance(w,ttk.Treeview))
    preview=next(w for w in widgets if isinstance(w,tk.Canvas))
    selector=next(w for w in widgets if isinstance(w,ttk.Combobox))
    check=next(w for w in widgets if isinstance(w,ttk.Checkbutton))
    assert tree.get_children()==('figure',)
    assert preview.type(preview.find_all()[0])=='image'
    for button in widgets:
        if isinstance(button,ttk.Button): assert inside(button,window)
    selector.set('历史整页截图');selector.event_generate('<<ComboboxSelected>>');root.update()
    assert tree.get_children()==('page',) and check.instate(['disabled'])
    assert preview.type(preview.find_all()[0])=='image'
    assert (package/'metadata.json').read_text(encoding='utf-8')==json.dumps({'resources':resources})
    window.destroy()


def test_automatic_ui_has_configuration_without_manual_approval(ui):
    root,app=ui
    def descendants(w):
        return [c for child in w.winfo_children() for c in [child]+descendants(child)]
    buttons=[str(w.cget('text')) for w in descendants(root) if isinstance(w,ttk.Button)]
    assert '确认所选已检查' not in buttons and any(label.startswith('继续跑未通过') for label in buttons)
    assert '保存配置' in buttons and '模型连通测试' in buttons and '读取可用模型' in buttons
    assert {'base_url','vision_base_url','text_model','vision_model','key','vision_key'}.issubset(app.ai_vars)
    assert len(app.tabs.tabs())==6


def test_concurrency_entry_is_visible_and_synced_with_run_parameters(ui):
    root,app=ui
    root.geometry('980x660');root.update()
    app.panes.sash_place(0,0,260);root.update()
    assert inside(app.concurrency_toggle,app.input_tab)
    assert inside(app.concurrency_status_label,app.input_tab)
    assert '并发' in app.concurrency_status.get()
    app.concurrency_toggle.invoke();root.update()
    assert app.ai_vars['concurrency_enabled'].get() is False
    assert '串行' in app.concurrency_status.get()
    app.ai_vars['concurrency_papers'].set('3');root.update()
    assert '论文 3' in app.concurrency_status.get()
    app.concurrency_settings_button.invoke();root.update()
    assert app.tabs.select()==str(app.ai_tab)
    assert app.ai_model_tabs.index(app.ai_model_tabs.select())==2


def test_agent_settings_separate_rag_and_switch_models(ui,monkeypatch):
    root,app=ui
    from agent_client import AgentClient
    from auto_ui import read_ai_form
    calls=[]
    def fake_models(self,vision=False): calls.append(vision);return ['model-a','model-b']
    monkeypatch.setattr(AgentClient,'models',fake_models)
    monkeypatch.setattr(app,'async_task',lambda fn,done,**kw:done(fn()))
    app.ai_vars['base_url'].set('https://example.test/v1');app.ai_vars['key'].set('test')
    app.ai_read_button.invoke();root.update()
    assert calls==[False] and tuple(app.ai_model_combos['text_model']['values'])==('model-a','model-b')
    app.ai_vars['text_model'].set('model-b')
    assert read_ai_form(app)[0]['text_model']=='model-b'
    app.tabs.select(app.ai_tab);root.geometry('980x660');root.update()
    app.panes.sash_place(0,0,260);root.update()
    assert inside(app.ai_test_button,app.ai_tab) and inside(app.ai_read_button,app.ai_tab)
    assert not str(app.rag_auto_frame).startswith(str(app.ai_tab))


def test_vision_model_buttons_read_and_test_primary_and_fallback_together(ui,monkeypatch):
    root,app=ui
    from agent_client import AgentClient
    app.tabs.select(app.ai_tab);app.ai_model_tabs.select(1);root.update()
    for key,value in {
        'vision_base_url':'https://primary.example/v1','vision_key':'primary-key','vision_model':'primary-model',
        'vision_fallback_enabled':True,'vision_fallback_base_url':'https://fallback.example/v1',
        'vision_fallback_key':'fallback-key','vision_fallback_model':'fallback-model',
    }.items(): app.ai_vars[key].set(value)
    calls=[]
    def fake_models(self,vision=False,channel='primary'):
        calls.append(('models',vision,channel))
        return [channel+'-a',channel+'-b']
    def fake_probe(self,vision=False,channel='primary'):
        calls.append(('probe',vision,channel))
        return channel+' ok'
    monkeypatch.setattr(AgentClient,'models',fake_models)
    monkeypatch.setattr(AgentClient,'probe',fake_probe)
    monkeypatch.setattr(app,'async_task',lambda fn,done,**kw:done(fn()))
    app.ai_read_button.invoke();root.update()
    assert calls==[('models',True,'primary'),('models',True,'fallback')]
    assert tuple(app.ai_model_combos['vision_model']['values'])==('primary-a','primary-b')
    assert tuple(app.ai_model_combos['vision_fallback_model']['values'])==('fallback-a','fallback-b')
    app.ai_test_button.invoke();root.update()
    assert calls[-2:]==[('probe',True,'primary'),('probe',True,'fallback')]
    assert '主通道' in app.ai_status.get() and '备用通道' in app.ai_status.get()


@pytest.fixture
def ui(tmp_path):
    root=tk.Tk()
    root.attributes('-alpha',0)
    app=Workbench(root,tmp_path)
    root.update()
    yield root,app
    root.destroy()


def inside(widget,parent):
    return (widget.winfo_ismapped() and widget.winfo_height()>10 and
            widget.winfo_rooty()>=parent.winfo_rooty() and
            widget.winfo_rooty()+widget.winfo_height()<=parent.winfo_rooty()+parent.winfo_height() and
            widget.winfo_rootx()+widget.winfo_width()<=parent.winfo_rootx()+parent.winfo_width())


@pytest.mark.parametrize('size',['1200x790','980x660'])
def test_actions_stay_visible_with_large_progress_panel(ui,size):
    root,app=ui
    root.geometry(size);root.update()
    app.panes.sash_place(0,0,260);root.update()
    assert inside(app.start_button,app.input_tab), 'Start button is clipped by the file list'
    assert inside(app.stop_button,app.input_tab), 'Stop button is clipped by the file list'
    assert inside(app.select_all_button,app.input_tab)
    assert inside(app.status_label,root)
    for tab in [app.result_tab,app.history_tab]:
        app.tabs.select(tab);root.update()
        for parent in tab.winfo_children():
            for widget in parent.winfo_children():
                if isinstance(widget,ttk.Button):assert inside(widget,tab), widget.cget('text')


def test_selection_and_run_controls(ui,monkeypatch):
    root,app=ui
    from test_script_task_ui import configure
    configure(app)
    app.scanned([{'name':f'{i}.txt','format':'.txt','size':10,'path':f'C:/test/{i}.txt'} for i in range(3)])
    root.update()
    assert '3' in app.selection_count.get()
    app.select_none_button.invoke();root.update()
    assert not app.source_tree.selection()
    assert app.start_button.instate(['disabled'])
    app.invert_button.invoke();root.update()
    assert len(app.source_tree.selection())==3
    app.source_tree.selection_set('1');root.update()
    app.invert_button.invoke();root.update()
    assert set(app.source_tree.selection())=={'0','2'}
    received=[]
    def fake_task(fn,done,kind='operation'):
        app.busy=True;app.task_kind=kind;app.update_controls()
        received.append(kind)
    monkeypatch.setattr(app,'async_task',fake_task)
    app.start_button.invoke();root.update()
    assert received==['processing']
    assert app.scan_button.instate(['disabled']) and app.start_button.instate(['disabled'])
    assert not app.stop_button.instate(['disabled'])
    app.stop_button.invoke();root.update()
    assert app.processor.stop.is_set() and app.stop_button.instate(['disabled'])
    assert '停止' in app.stop_button.cget('text')


def test_billing_visible_model_switch_and_history(ui):
    from billing import summarize
    from billing_ui import update_progress
    root,app=ui
    root.geometry('980x660');root.update()
    app.panes.sash_place(0,0,260);root.update()
    update_progress(app,{'run_id':None,'billing':summarize([]),'billing_item':summarize([])})
    root.update()
    assert inside(app.billing_button,app.progress_panel) and inside(app.billing_label,app.progress_panel)
    assert '人民币' in app.billing_total.get() or '¥' in app.billing_total.get()
    app.ai_vars['text_model'].set('gpt-5.6-sol');root.update()
    assert '输入 ¥28' in app.billing_prices.get()
    app.ai_vars['text_model'].set('unknown-model');root.update()
    assert '未收录官方价' in app.billing_prices.get()
    app.ai_vars['usd_cny'].set('NaN');root.update()
    from auto_ui import read_ai_form
    with pytest.raises(ValueError): read_ai_form(app)
    app.ai_vars['usd_cny'].set('7')
    app.tabs.select(app.ai_tab);app.ai_model_tabs.select(3);root.update()
    assert app.ai_test_button.instate(['disabled'])
    assert inside(app.ai_test_button,app.ai_tab)
    update_progress(app,{'run_id':'old','billing':None})
    assert '未记录' in app.billing_line.get()
