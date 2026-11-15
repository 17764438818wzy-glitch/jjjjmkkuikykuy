"""Resume selection and button behaviour; no model/network requests."""
import tkinter as tk
from pathlib import Path
import pytest
from app import Workbench, retry_sources


@pytest.fixture
def ui(tmp_path):
    root=tk.Tk();root.withdraw()
    app=Workbench(root,tmp_path/'data')
    from test_script_task_ui import configure
    configure(app)
    yield root,app
    root.destroy()


def batch(app, folder, name, started, status='interrupted'):
    folder.mkdir(exist_ok=True)
    paths=[]
    for n in ['passed.txt','failed.txt','unstarted.txt']:
        p=folder/n;p.write_text('synthetic',encoding='utf-8');paths.append(str(p))
    m={'run_id':name,'pipeline':'automatic','started_at':started,'status':status,'message':'test',
       'inputs':[{'path':p} for p in paths],'events':[],
       'items':[{'id':'p','source_path':paths[0],'status':'pass','automatic_approval':True,'title':'企业财务分析研究','major':'会计学'},
                {'id':'f','source_path':paths[1],'status':'reject','reason_kind':'technical'}]}
    app.store.save(m)
    return m,paths


def test_resume_uses_latest_matching_folder_and_includes_unstarted(ui,tmp_path,monkeypatch):
    _,app=ui;folder=tmp_path/'input'
    batch(app,folder,'AUTO_z_old','2026-09-15')
    latest,paths=batch(app,folder,'AUTO_a_new','2026-09-16')
    batch(app,tmp_path/'other','AUTO_other','2026-09-17')
    app.folder.set(str(folder));seen=[]
    monkeypatch.setattr(app,'start_automatic',lambda rows:seen.extend(rows))
    app.load_latest_button.invoke()
    assert not seen, 'Loading a batch must not start model requests'
    app.resume_button.invoke()
    assert app.current['run_id']==latest['run_id']
    assert [r['path'] for r in seen]==paths[1:]


def test_no_reentrant_run_and_missing_source_is_reported(ui,tmp_path,monkeypatch):
    _,app=ui;m,paths=batch(app,tmp_path/'input','AUTO_test','2026-09-16')
    app.folder.set(str(tmp_path/'input'));calls=[];errors=[]
    monkeypatch.setattr(app,'start_automatic',lambda rows:calls.append(rows))
    monkeypatch.setattr(app,'error',lambda msg:errors.append(msg))
    app.busy=True;app.update_controls()
    assert app.resume_button.instate(['disabled']) and not hasattr(app,'retry_button')
    app.resume_latest();assert not calls
    app.busy=False;app.update_controls()
    Path(paths[1]).unlink();app.resume_latest();app.retry_failed()
    assert not calls and '1' in errors[0]


def test_running_and_empty_history_do_not_launch(ui,tmp_path,monkeypatch):
    _,app=ui;app.folder.set(str(tmp_path/'input'));calls=[]
    monkeypatch.setattr(app,'start_automatic',lambda rows:calls.append(rows))
    app.resume_latest();assert not calls and '没有' in app.state.get()
    batch(app,tmp_path/'input','AUTO_test','2026-09-16','running')
    app.resume_latest();assert not calls and '仍在处理' in app.state.get()


def test_failed_batch_includes_remaining_inputs(ui,tmp_path):
    _,app=ui;m,paths=batch(app,tmp_path/'input','AUTO_test','2026-09-16','failed')
    assert [r['path'] for r in retry_sources(m)]==paths[1:]


def test_operation_selection_distinguishes_upload_retry_and_skip_processed(tmp_path):
    from app import select_operation
    passed = {'id':'p','source_path':str(tmp_path/'passed.txt'),'status':'pass','automatic_approval':True,
              'title':'企业财务分析研究','major':'会计学',
              'uploads':{}}
    failed = {'id':'f','source_path':str(tmp_path/'failed.txt'),'status':'reject','reason_kind':'technical'}
    manifest = {'status':'completed','items':[passed,failed],
                'inputs':[{'path':passed['source_path']},{'path':failed['source_path']},{'path':str(tmp_path/'new.txt')}]}
    assert [item['path'] for item in select_operation(manifest,'retry_failed')] == [str(tmp_path/'failed.txt')]
    assert [item['id'] for item in select_operation(manifest,'upload_only')] == ['p']
    assert [item['path'] for item in select_operation(manifest,'skip_processed')] == [str(tmp_path/'new.txt')]


@pytest.mark.parametrize('size',['980x660','1200x790'])
def test_resume_button_fits_small_window(ui,size):
    root,app=ui;root.attributes('-alpha',0);root.geometry(size);root.deiconify();root.update()
    app.panes.sash_place(0,0,260);root.update()
    button=app.resume_button;parent=app.input_tab
    assert button.winfo_ismapped() and button.winfo_height()>10
    assert button.winfo_rooty()+button.winfo_height()<=parent.winfo_rooty()+parent.winfo_height()
    assert button.winfo_rootx()+button.winfo_width()<=parent.winfo_rootx()+parent.winfo_width()
