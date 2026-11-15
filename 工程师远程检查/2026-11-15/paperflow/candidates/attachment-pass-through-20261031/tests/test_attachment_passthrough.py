import asyncio
import copy
import hashlib
import json
from pathlib import Path

from test_registered_prepare import registered


def test_registered_attachment_is_forwarded_without_preview(registered, tmp_path, monkeypatch):
    import attachment_preview
    import direct_source
    from local_prepare import prepare_registered_attachments

    def forbidden(*args, **kwargs):
        raise AssertionError("附件不能进行格式探测或内容预览")

    monkeypatch.setattr(direct_source, "detect_format", forbidden)
    monkeypatch.setattr(attachment_preview, "preview_attachment_chunks", forbidden)
    _, receipt_root, receipt, item = registered
    original = next(row for row in receipt["items"] if row["kind"] == "attachment")
    source = Path(original["path"])
    before = source.read_bytes()
    result = prepare_registered_attachments(item, receipt_root=receipt_root, work_dir=tmp_path / "handoff")
    inventory = json.loads(Path(result["inventory_path"]).read_text(encoding="utf8"))
    row = inventory["items"][0]
    assert row["file_id"] == original["file_id"]
    assert row["path"] == original["delivered_path"]
    assert row["source_binding"] == original["source_binding"]
    assert row["handoff_ref"] == original["handoff_ref"]
    assert row["evidence_records"] == original["evidence_records"]
    assert row["content_status"] == "upstream_only"
    assert "preview" not in row
    assert source.read_bytes() == before


def test_model_input_without_upstream_content_does_not_read_attachment(tmp_path, monkeypatch):
    from paperflow_prefect import attachment_input, attachments

    source = tmp_path / "答辩.pptx"
    source.write_bytes(b"synthetic pptx payload; must not be parsed")
    row = {"path": str(source), "relative": "答辩.pptx", "file_id": "att-1",
           "sha256": hashlib.sha256(source.read_bytes()).hexdigest(), "bytes": source.stat().st_size}
    def forbidden(*args, **kwargs):
        raise AssertionError("不得补读附件")
    monkeypatch.setattr(attachment_input, "load_received_evidence", lambda _: None)
    monkeypatch.setattr(attachments, "_local_evidence", forbidden)
    evidence = attachments.evidence_for(row, tmp_path / "no-preview", upstream_only=True)
    request = attachments._request_body(evidence, {"model": "synthetic", "max_tokens": 2048})
    context = json.loads(request["messages"][1]["content"][0]["text"])
    assert context["name"] == "答辩.pptx"
    assert context["format"] == ".pptx"
    assert context["text"] == ""
    assert evidence["images"] == [] and evidence["refs"] == ["file-facts"]
    assert any("未读取" in s for s in evidence["limitations"])
    assert not (tmp_path / "no-preview").exists()


def test_saved_upstream_evidence_uses_delivered_copy_when_source_moved(registered):
    from paperflow_prefect.attachment_input import load_received_evidence

    group, _, receipt, item = registered
    row = copy.deepcopy(next(row for row in receipt["items"] if row["kind"] == "attachment"))
    assert row["evidence_records"] and row["delivery_ref"]
    group.rename(group.with_name("moved-synthetic-group"))
    assert not Path(row["source_path"]).exists()
    assert Path(row["delivered_path"]).is_file()
    record, _, report = load_received_evidence(row)
    assert record["source"]["file_id"] == row["upstream_file_id"]
    assert report["source_verified"] is True
    assert Path(row["path"]).is_relative_to(Path(item["delivery_dir"]) / "files")


def test_attachment_only_dispatch_continues_after_one_failure():
    from paperflow_prefect.workflow import execute_stage3

    paper = {"paper_id": "paper-a", "body": {"file_id": "body"}, "attachments": [
        {"file_id": "ppt", "relative": "答辩.pptx"}, {"file_id": "zip", "relative": "资料.zip"}]}
    calls = []
    async def call(stage, current, row=None):
        calls.append((stage, current["paper_id"], row["file_id"]))
        if row["file_id"] == "ppt":
            raise ValueError("synthetic unreadable attachment")
        return {"summary": "压缩包"}
    result = asyncio.run(execute_stage3(paper, call, attachments_only=True))
    assert sorted(calls) == [("attachment", "paper-a", "ppt"), ("attachment", "paper-a", "zip")]
    assert result["body"]["status"] == "not_selected"
    assert result["attachments"]["ppt"]["status"] == "failed"
    assert result["attachments"]["zip"]["status"] == "complete"


def test_registered_dispatch_sends_each_attachment_to_llm_stage(registered, tmp_path, monkeypatch):
    from attachment_handoff import dispatch_registered_attachments
    from paperflow_prefect import attachment_input, attachments

    def forbidden(*args, **kwargs):
        raise AssertionError("不得补读附件")
    monkeypatch.setattr(attachments, "_local_evidence", forbidden)
    group, receipt_root, receipt, item = registered
    requests = []
    async def call(stage, paper, row=None):
        assert stage == "attachment" and paper["attachment_input_policy"] == "upstream_only"
        evidence = attachments.evidence_for(row, tmp_path / "preview", upstream_only=True)
        payload = attachments._request_body(evidence, {"model": "synthetic", "max_tokens": 2048})
        requests.append((paper["paper_id"], row["file_id"], payload))
        return {"summary": "表格附件"}
    result = asyncio.run(dispatch_registered_attachments(item, receipt_root=receipt_root, call=call))
    row = next(row for row in receipt["items"] if row["kind"] == "attachment")
    assert [(p, f) for p, f, _ in requests] == [(receipt["paper_id"], row["file_id"])]
    assert result["attachments"][row["file_id"]]["status"] == "complete"
    assert not (tmp_path / "preview").exists()


def test_full_registered_dispatch_produces_one_description_offline(registered, tmp_path, monkeypatch):
    from contextlib import nullcontext
    from types import SimpleNamespace
    from threading import Event
    from attachment_handoff import dispatch_registered_attachments
    from local_prepare import _native_paths, _observe_registered
    from paperflow_prefect import attachments, legacy, named_delivery
    from paperflow_prefect.backend import Backend
    from paperflow_prefect.records import sha_file, write_json
    from paperflow_prefect.request_admission import confirm_first_request

    _native_paths()
    from preextract_scheduler_adapter import _request_hash
    _, receipt_root, receipt, item = registered
    observed = _observe_registered(item, receipt_root)
    paper = observed['paper']
    row = paper['attachments'][0]
    original = Path(row['path']).read_bytes()
    history_root = tmp_path/'history'; history_root.mkdir()
    work = tmp_path/'llm-work'
    confirm_first_request(paper, row, work, history_roots=[history_root])
    calls = []
    def fake_invoke(kind, request, connection, directory, **kwargs):
        calls.append(request)
        directory = Path(directory); directory.mkdir(parents=True, exist_ok=True)
        write_json(directory/'request.json', request)
        context = json.loads(request['messages'][1]['content'][0]['text'])
        raw = json.dumps({'kind':'CSV 表格', 'summary':'数据表附件。',
                          'evidence_refs':[context['refs'][0]], 'limitations':[]}, ensure_ascii=False)
        (directory/'response.txt').write_text(raw,encoding='utf8')
        write_json(directory/'request-state.json', {'status':'response_complete',
            'request_sha256':_request_hash(request), 'response_sha256':sha_file(directory/'response.txt'),
            'http_invoked':True, 'client_http_requests':1, 'finish_reason':'stop'})
    def forbidden(*args, **kwargs): raise AssertionError('直通模式不能补读或复制附件')
    monkeypatch.setattr(attachments, '_local_evidence', forbidden)
    monkeypatch.setattr(named_delivery, 'materialize_named_input', forbidden)
    monkeypatch.setattr(legacy, 'connection', lambda settings: {'model':'synthetic','max_tokens':2048})
    backend=Backend.__new__(Backend)
    backend.settings=SimpleNamespace(require_execution=lambda:None)
    backend.root=history_root
    backend.local_scope=nullcontext
    backend.stops={paper['paper_id']:Event()}
    backend.invoke=fake_invoke
    backend.attachment_work=lambda current, entry:work
    async def call(stage, current, entry=None):
        assert stage=='attachment'
        return backend.attachment(current, entry)
    result=asyncio.run(dispatch_registered_attachments(item,receipt_root=receipt_root,call=call))
    assert result['attachments'][row['file_id']]['status']=='complete', result
    description=json.loads((work/'description.json').read_text(encoding='utf8'))
    assert description['paper_id']==receipt['paper_id']
    assert description['attachment_id']==row['file_id']
    assert description['summary']=='数据表附件。'
    assert len(calls)==1 and Path(row['path']).read_bytes()==original
    assert list((work/'cards').rglob('attachment.md'))
    again=asyncio.run(dispatch_registered_attachments(item,receipt_root=receipt_root,call=call))
    assert again['attachments'][row['file_id']]['status']=='complete'
    assert len(calls)==1


def test_existing_unified_receipt_flow_uses_upstream_only_automatically(registered, tmp_path):
    import subprocess
    import sys
    group, _, receipt, item = registered
    helper = Path(__file__).parent/'fixtures'/'unified_attachment_offline.py'
    payload = {'group':str(group), 'tmp_path':str(tmp_path), 'receipt':receipt, 'item':item}
    result = subprocess.run([sys.executable,'-B','-X','utf8',str(helper)],
        input=json.dumps(payload,ensure_ascii=False),encoding='utf8',capture_output=True,timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr
    assert json.loads(result.stdout)['attachment_requests'] == 1


def test_detected_actual_format_reaches_model_without_reprocessing(registered, tmp_path):
    from local_prepare import _observe_registered
    from paperflow_prefect.attachment_input import load_received_evidence, model_evidence
    from paperflow_prefect.attachments import _request_body
    _, receipt_root, _, item=registered
    row=_observe_registered(item,receipt_root)['paper']['attachments'][0]
    record,handoff,report=load_received_evidence(row)
    record=copy.deepcopy(record)
    record['file_info']['name']='archive.zip'
    record['file_info']['actual_format']='7z'
    evidence=model_evidence(record,handoff,report=report)
    request=_request_body(evidence,{'model':'synthetic','max_tokens':2048})
    context=json.loads(request['messages'][1]['content'][0]['text'])
    assert context['name']=='archive.zip' and context['format']=='.zip'
    assert context['actual_format']=='7z'


def test_completed_description_survives_lost_upstream_evidence_without_new_call(registered, tmp_path):
    import subprocess
    import sys
    group, _, receipt, item=registered
    helper=Path(__file__).parent/'fixtures'/'unified_attachment_offline.py'
    payload={'group':str(group),'tmp_path':str(tmp_path),'receipt':receipt,'item':item,
             'delete_evidence_after_success':True}
    result=subprocess.run([sys.executable,'-B','-X','utf8',str(helper)],
        input=json.dumps(payload,ensure_ascii=False),encoding='utf8',capture_output=True,timeout=30)
    assert result.returncode==0,result.stdout+result.stderr
    report=json.loads(result.stdout)
    assert report['attachment_requests']==1
    assert report['reused_after_evidence_loss'] is True


def test_upstream_text_evidence_is_in_actual_model_request(tmp_path, monkeypatch):
    import subprocess
    import sys
    from local_prepare import _observe_registered
    from paperflow_prefect import attachments
    helper=Path(__file__).parent/'fixtures'/'make_upstream_text_evidence.py'
    candidates=Path(__file__).resolve().parents[2]
    proc=subprocess.run([sys.executable,'-B','-X','utf8',str(helper),str(tmp_path),str(candidates)],
                        text=True,encoding='utf8',capture_output=True,timeout=30)
    assert proc.returncode==0,proc.stderr
    case=json.loads(proc.stdout)
    observed=_observe_registered(case['item'],case['receipt_root'])
    row=observed['paper']['attachments'][0]
    def forbidden(*args,**kwargs): raise AssertionError('不得补读附件')
    monkeypatch.setattr(attachments,'_local_evidence',forbidden)
    evidence=attachments.evidence_for(row,tmp_path/'preview',upstream_only=True)
    request=attachments._request_body(evidence,{'model':'synthetic','max_tokens':2048})
    context=json.loads(request['messages'][1]['content'][0]['text'])
    assert 'marker-attachment,42' in context['text']
    assert any(ref.startswith('text-') for ref in context['refs'])
    assert not (tmp_path/'preview').exists()


def test_missing_model_journal_does_not_validate_cached_description(registered, tmp_path):
    import subprocess
    import sys
    group,_,receipt,item=registered
    helper=Path(__file__).parent/'fixtures'/'unified_attachment_offline.py'
    payload={'group':str(group),'tmp_path':str(tmp_path),'receipt':receipt,'item':item,
             'delete_evidence_after_success':True,'delete_model_journal_after_success':True}
    result=subprocess.run([sys.executable,'-B','-X','utf8',str(helper)],
        input=json.dumps(payload,ensure_ascii=False),encoding='utf8',capture_output=True,timeout=30)
    assert result.returncode==0,result.stdout+result.stderr
    report=json.loads(result.stdout)
    assert report['attachment_requests']==1
    assert report['reused_after_evidence_loss'] is False


def test_evidence_observation_survives_refreshed_receipt_with_missing_sidecar(registered, tmp_path):
    import subprocess
    import sys
    group,_,receipt,item=registered
    helper=Path(__file__).parent/'fixtures'/'unified_attachment_offline.py'
    payload={'group':str(group),'tmp_path':str(tmp_path),'receipt':receipt,'item':item,
             'delete_evidence_after_success':True,'reload_receipt_after_evidence_loss':True}
    proc=subprocess.run([sys.executable,'-B','-X','utf8',str(helper)],
        input=json.dumps(payload,ensure_ascii=False),encoding='utf8',capture_output=True,timeout=30)
    assert proc.returncode==0,proc.stdout+proc.stderr
    assert json.loads(proc.stdout)['attachment_requests']==1


def test_attachment_observation_survives_other_stage_failure():
    from paperflow_prefect.unified import execute_unified
    paper={'paper_id':'p','body':{'file_id':'body','sha256':'a','display_name':'paper'},
           'attachments':[{'file_id':'att'}]}
    receipt={'state':'已接收','unresolved':[],'items':[]}
    async def call(stage,paper,row=None):
        if stage=='attachment':return {'current_evidence_status':'unavailable'}
        if stage=='publish_attachments':raise RuntimeError('synthetic')
        return {}
    result=asyncio.run(execute_unified(paper,receipt,call))
    assert result['status']=='incomplete'
    assert result['attachment_observations']['att']['current_evidence_status']=='unavailable'


def test_stage3_receipt_entry_confirms_history_and_dispatches_only_attachments(registered, tmp_path):
    import subprocess
    import sys
    group,_,receipt,item=registered
    helper=Path(__file__).parent/'fixtures'/'unified_attachment_offline.py'
    payload={'group':str(group),'tmp_path':str(tmp_path),'receipt':receipt,'item':item,
             'use_stage3_entry':True}
    proc=subprocess.run([sys.executable,'-B','-X','utf8',str(helper)],
        input=json.dumps(payload,ensure_ascii=False),encoding='utf8',capture_output=True,timeout=30)
    assert proc.returncode==0,proc.stdout+proc.stderr
    assert json.loads(proc.stdout)['attachment_requests']==1


def test_actual_attachment_flow_holds_locks_and_delivers_existing_copy(registered, tmp_path):
    import subprocess
    import sys
    group,_,receipt,item=registered
    helper=Path(__file__).parent/'fixtures'/'unified_attachment_offline.py'
    payload={'group':str(group),'tmp_path':str(tmp_path),'receipt':receipt,'item':item,
             'use_prefect_entry':True}
    proc=subprocess.run([sys.executable,'-B','-X','utf8',str(helper)],
        input=json.dumps(payload,ensure_ascii=False),encoding='utf8',capture_output=True,timeout=30)
    assert proc.returncode==0,proc.stdout+proc.stderr
    assert json.loads(proc.stdout)=={'attachment_requests':1,'status':'stage3_complete'}


def test_actual_attachment_flow_reports_damaged_item_and_continues(tmp_path):
    import subprocess
    import sys
    candidates=Path(__file__).resolve().parents[2]
    script="""
import json,sys
from pathlib import Path
sys.path[:0]=sys.argv[1:3]
from paperflow_prefect.first_step_registration import register_delivery
from paperflow_prefect.group_classification import process_one_group
from test_first_step_registration import FixtureJudge,evidence
base=Path(sys.argv[3]);group=base/'library'/'person';group.mkdir(parents=True)
(group/'paper.txt').write_text('摘要\\n测试正文',encoding='utf8')
(group/'good.csv').write_text('x,y\\n1,2',encoding='utf8')
(group/'bad.csv').write_text('x,y\\n3,4',encoding='utf8')
delivery=process_one_group(group,group.parent,base/'task',FixtureJudge,evidence,verified_new=True)
receipt=register_delivery(delivery['delivery_dir'],receipt_root=base/'receipts',history_roots=(),lock_path=base/'receipt.lock')
item=dict(status='passed',registration_status='registered',delivery_complete=True,group_id=receipt['group_id'],
    paper_id=receipt['paper_id'],receipt_id=receipt['receipt_id'],receipt_path=receipt['record_path'],delivery_dir=delivery['delivery_dir'])
bad=next(row for row in receipt['items'] if row['kind']=='attachment' and row['relative']=='bad.csv')
path=Path(bad['delivered_path']);assert path.is_relative_to(base);path.write_text('synthetic damage',encoding='utf8')
print(json.dumps(dict(group=str(group),receipt=receipt,item=item,damaged_id=bad['file_id']),ensure_ascii=False))
"""
    proc=subprocess.run([sys.executable,'-B','-X','utf8','-c',script,
        str(candidates/'first-step-control-20261029'),
        str(candidates/'first-step-auto-registration-20261030'/'tests'),str(tmp_path)],
        encoding='utf8',capture_output=True,timeout=30)
    assert proc.returncode==0,proc.stdout+proc.stderr
    payload=json.loads(proc.stdout)
    payload.update(tmp_path=str(tmp_path),use_prefect_entry=True,expected_status='incomplete')
    helper=Path(__file__).parent/'fixtures'/'unified_attachment_offline.py'
    proc=subprocess.run([sys.executable,'-B','-X','utf8',str(helper)],
        input=json.dumps(payload,ensure_ascii=False),encoding='utf8',capture_output=True,timeout=30)
    assert proc.returncode==0,proc.stdout+proc.stderr
    assert json.loads(proc.stdout)=={'attachment_requests':1,'status':'incomplete'}
