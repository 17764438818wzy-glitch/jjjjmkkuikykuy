"""Filtering changes the visible list, never the batch or retry scope."""
import pytest
from test_script_task_ui import ui, batch, configure


def test_filter_preserves_batch_and_correct_source_identity(ui,tmp_path,monkeypatch):
    _,app=ui
    manifest,paths=batch(app.store,tmp_path)
    app.current=manifest
    app.show_script_results(manifest)
    app.script_filter.set('不通过')
    assert app.source_tree.get_children()==('1',)
    assert len(app.sources)==3
    app.select_files(app.source_tree,'all')
    previews=[]
    monkeypatch.setattr(app,'text_window',lambda title,text:previews.append(text))
    app.preview_script_row()
    assert str(paths[1]) in previews[0]
    app.script_filter.set('通过')
    assert app.source_tree.get_children()==('0',)
    assert not app.source_tree.selection(), 'Hidden selections must not affect file actions'
    app.script_filter.set('全部结果')
    assert app.source_tree.get_children()==('0','1','2')


def test_filter_survives_refresh_and_does_not_limit_retry(ui,tmp_path,monkeypatch):
    _,app=ui
    configure(app)
    manifest,paths=batch(app.store,tmp_path)
    app.current=manifest
    app.show_script_results(manifest)
    app.script_filter.set('通过')
    app.show_script_results(manifest)
    assert app.source_tree.get_children()==('0',)
    selected=[]
    monkeypatch.setattr(app,'start_automatic',lambda rows:selected.extend(rows))
    app.resume_button.invoke()
    assert [r['path'] for r in selected]==[str(paths[1])]
    assert app.script_filter.get()=='通过'


def test_empty_filter_and_scan_reset(ui,tmp_path):
    _,app=ui
    manifest,paths=batch(app.store,tmp_path)
    app.show_script_results(manifest)
    app.script_filter.set('处理中')
    assert not app.source_tree.get_children()
    assert '显示 0' in app.script_summary.get()
    app.scanned([{'path':str(paths[0]),'name':paths[0].name,'size':9,'format':'.txt'}])
    assert app.script_filter.get()=='全部结果'
    assert app.source_tree.get_children()==('0',)


def test_filtered_list_follows_live_status_changes(ui,tmp_path):
    _,app=ui
    manifest,paths=batch(app.store,tmp_path)
    app.show_script_results(manifest)
    app.script_filter.set('处理中')
    app.show_script_progress({'processed_sources':0,'source_count':1,
        'progress':{'stage':'图表提取','source_path':str(paths[1])}})
    assert app.source_tree.get_children()==('1',)
    app.script_filter.set('不通过')
    assert not app.source_tree.get_children()


@pytest.mark.parametrize('size',['980x660','1200x790'])
def test_filter_and_batch_summary_fit_below_table(ui,tmp_path,size):
    root,app=ui
    manifest,_=batch(app.store,tmp_path)
    app.show_script_results(manifest)
    app.render_script_rows([dict(path=f'C:/synthetic/{i}.docx',name=f'{i}.docx',
                                script_status='不通过' if i==0 else '待处理' if i==50 else '重复跳过')
                            for i in range(51)])
    app.script_filter.set('不通过')
    root.attributes('-alpha',0);root.geometry(size);root.deiconify();root.update()
    parent=app.input_tab
    for widget in (app.script_filter_combo,app.script_summary_label,app.resume_button,app.load_latest_button):
        assert widget.winfo_ismapped()
        assert widget.winfo_rootx()+widget.winfo_width()<=parent.winfo_rootx()+parent.winfo_width()
        assert widget.winfo_rooty()+widget.winfo_height()<=parent.winfo_rooty()+parent.winfo_height()
    assert app.script_summary_label.winfo_width()>=app.script_summary_label.winfo_reqwidth()
