from auto_pipeline import AutoProcessor
from test_ai_resume import rejected_run, records
from test_stage_resume import Transport


def test_current_checkpoint_is_rescreened_before_any_ai_call(tmp_path,monkeypatch):
    import auto_pipeline
    store,cfg,first=rejected_run(tmp_path)
    monkeypatch.setattr(auto_pipeline,'coarse_screen',lambda *args:
                        {'decision':'reject','reason':'已核实为查重报告','chars':10000})
    def forbidden(*args,**kwargs):raise AssertionError('Auxiliary report reached image recovery')
    monkeypatch.setattr(auto_pipeline,'finish_evidence',forbidden)
    processor=AutoProcessor(store,cfg,'fixture-key');processor.agent.opener=Transport()
    result=processor.run(records(first),operation='retry_ai')
    assert not processor.agent.opener.requests
    assert result['items'][0]['reason_kind']=='screening'


def test_current_checkpoint_refreshes_title_using_saved_raw_text(tmp_path,monkeypatch):
    import auto_pipeline
    store,cfg,first=rejected_run(tmp_path);calls=[]
    def title(raw,name):
        calls.append((raw,name));return '已校正的幼儿阅读活动研究'
    monkeypatch.setattr(auto_pipeline,'detect_title',title)
    processor=AutoProcessor(store,cfg,'fixture-key');processor.agent.opener=Transport()
    result=processor.run(records(first),operation='retry_ai')
    assert calls and len(calls[0][0])>1000
    assert result['items'][0]['title']=='已校正的幼儿阅读活动研究'


def test_docx_source_reaches_container_normalization_on_private_copy(tmp_path,monkeypatch):
    from pathlib import Path
    from engine import Store
    from test_stage_resume import CONFIG,document
    import office_input
    source=tmp_path/'thesis.docx';document(source);original=source.read_bytes();seen=[]
    normalize=office_input.normalize_docx_container
    def checked(path,destination):
        assert Path(path)!=source
        assert Path(path).read_bytes()==original
        seen.append(path);return normalize(path,destination)
    monkeypatch.setattr(office_input,'normalize_docx_container',checked)
    processor=AutoProcessor(Store(tmp_path/'data'),CONFIG,'fixture-key');processor.agent.opener=Transport()
    result=processor.run([{'path':str(source),'name':source.name}])
    assert seen
    assert result['items'][0]['status']=='pass'
    assert source.read_bytes()==original
