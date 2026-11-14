"""The approved script page retries local failures independently of handoff."""
import pytest
from app import retry_sources
from test_script_task_ui import ui, batch, configure


def test_script_retry_ignores_remote_failure_on_passed_paper(ui,tmp_path,monkeypatch):
    _,app=ui
    configure(app)
    manifest,paths=batch(app.store,tmp_path)
    manifest['items'][0].update(handoff_status='pending',handoff_reason='synthetic upload failure')
    app.store.save(manifest)
    app.current=manifest
    app.show_script_results(manifest)
    seen=[]
    monkeypatch.setattr(app,'start_automatic',lambda rows:seen.extend(rows))
    app.resume_button.invoke()
    assert [r['path'] for r in seen]==[str(paths[1])]
    assert app.source_tree.item('0','values')[1]=='通过'


def test_script_retry_rechecks_latest_saved_result(ui,tmp_path,monkeypatch):
    _,app=ui
    configure(app)
    manifest,paths=batch(app.store,tmp_path)
    app.current=manifest
    app.show_script_results(manifest)
    manifest['items'][1].update(status='pass',automatic_approval=True,title='企业财务管理研究')
    app.store.save(manifest)
    seen=[]
    monkeypatch.setattr(app,'start_automatic',lambda rows:seen.extend(rows))
    app.resume_button.invoke()
    assert not seen
    assert '没有' in app.state.get()


@pytest.mark.parametrize('status',['completed','stopped'])
def test_preprocessing_retry_excludes_duplicate_and_uploaded_skip_events(status):
    manifest={'status':status,
              'items':[{'id':'failure','source_path':'failed.docx','status':'reject','reason_kind':'technical'}],
              'events':[{'status':'duplicate','source':'duplicate.docx'},
                        {'status':'uploaded_skip','source':'uploaded.docx'}],
              'inputs':[{'path':path} for path in ('failed.docx','duplicate.docx','uploaded.docx','unstarted.docx')]}
    expected=['failed.docx']+(['unstarted.docx'] if status=='stopped' else [])
    assert [row['path'] for row in retry_sources(manifest)]==expected
