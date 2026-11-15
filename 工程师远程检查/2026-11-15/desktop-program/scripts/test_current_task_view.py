from types import SimpleNamespace


def test_current_view_does_not_load_retry_ancestors(tmp_path):
    from current_task_view import CurrentReader
    manifest=dict(run_id='now',status='running',retry_of='old',inputs=[],source_count=1,
                  items=[dict(id='new',status='pass',source_path='paper.docx',ai_result=dict(status='passed'))])
    calls=[]
    def load(run_id):
        calls.append(run_id)
        assert run_id=='now','Historical failures must not be loaded into current stats'
        return manifest
    reader=CurrentReader(SimpleNamespace(root=tmp_path,load=load))
    result=reader.read('now')
    assert calls==['now'] and len(result['snapshot']['items'])==1
    assert manifest['retry_of']=='old' and result['snapshot']['items'][0]['ai_result']['status']=='passed'


def test_current_failures_are_still_visible_and_inputs_unchanged():
    from current_task_view import projection
    from processing_outcomes import ai_outcome
    original=dict(run_id='now',status='running',retry_of='old',inputs=[],
        items=[dict(id='failed',source_path='paper.docx',ai_result=dict(status='call_failed',reason='real current error'))])
    result=projection(original)
    assert ai_outcome(result['items'][0],'running')[0]=='调用失败'
    assert '_result_run_id' not in original['items'][0]
