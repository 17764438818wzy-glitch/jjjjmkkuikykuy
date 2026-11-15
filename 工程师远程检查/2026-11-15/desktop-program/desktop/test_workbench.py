from pathlib import Path
import hashlib
import threading
import json
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from engine import Processor, Store, approve_items, digest, scan_sources
from ragflow_client import RagflowClient, upload_items


def paper(title="幼儿阅读活动的案例研究", references=True):
    paragraphs = [f"第{i}次观察记录显示，幼儿在阅读活动中逐渐参与交流。教师通过提问引导幼儿表达自己的理解，并记录活动中的表现。" for i in range(100)]
    return f"论文题目：{title}\n专业：学前教育\n学生姓名：张小测\n学号：2026001234\n指导教师：李小测\n\n摘要\n研究幼儿阅读活动的组织方式。\n关键词：学前教育；阅读\n第一章 绪论\n" + "\n".join(paragraphs) + "\n结论\n阅读活动促进幼儿表达。\n" + ("参考文献\n[1] 阅读活动研究。" if references else "")


@pytest.fixture
def processed(tmp_path):
    source = tmp_path / "input"
    source.mkdir()
    (source / "paper.txt").write_text(paper(), encoding="utf-8")
    (source / "duplicate.txt").write_text(paper(), encoding="utf-8")
    (source / "short.txt").write_text("摘要\n短文本\n参考文献", encoding="utf-8")
    (source / "broken.docx").write_bytes(b"not a docx")
    store = Store(tmp_path / "output")
    manifest = Processor(store).run(scan_sources(source))
    return store, manifest, source


def test_real_legacy_pipeline_and_source_preservation(processed):
    store, manifest, source = processed
    assert manifest["status"] == "completed", manifest
    assert manifest['human_view']['status'] == 'ready'
    assert {p.name for p in Path(manifest['human_view']['path']).iterdir()} == {'通过', '不通过'}
    assert any(item["status"] == "pass" for item in manifest["items"])
    assert any(item["status"] == "reject" for item in manifest["items"])
    assert any(event["status"] == "failed" for event in manifest["events"])
    assert any(event["status"] == "duplicate" for event in manifest["events"])
    assert (source / "paper.txt").read_text(encoding="utf-8") == paper()
    accepted = next(item for item in manifest["items"] if item["status"] == "pass")
    assert accepted["major"] == "学前教育"
    text = Path(accepted["path"]).read_text(encoding="utf-8")
    assert "张小测" not in text and "李小测" not in text and "2026001234" not in text
    assert accepted["academic_rating"] is None
    assert not accepted["reviewed_hash"]
    assert not accepted["uploads"]
    rerun = Processor(store).run(scan_sources(source))
    assert not any(item["status"] == "pass" for item in rerun["items"])


def test_md_support_and_review_status(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "review.md").write_text(paper(references=False), encoding="utf-8")
    store = Store(tmp_path / "out")
    manifest = Processor(store).run(scan_sources(source))
    assert manifest["status"] == "completed"
    assert len(manifest["items"]) == 1
    assert manifest["items"][0]["status"] == "review"


def test_output_is_never_input(tmp_path):
    store = Store(tmp_path / "out")
    (store.root / "sample.txt").write_text("test")
    with pytest.raises(ValueError):
        scan_sources(store.root, excluded_root=store.root)
    assert scan_sources(tmp_path, excluded_root=store.root) == []


def test_upload_gates_and_idempotence(processed):
    store, manifest, _ = processed
    item = next(i for i in manifest["items"] if i["status"] == "pass")
    calls = []
    class Client:
        base_url = "http://localhost"
        def upload(self, dataset_id, path):
            calls.append((dataset_id, path))
            return "remote-doc-1"
    client = Client()
    dataset = {"id": "ds1", "name": "学前教育"}
    with pytest.raises(ValueError):
        upload_items(store, manifest["run_id"], [item["id"]], client, dataset)
    approve_items(store, manifest["run_id"], [item["id"]])
    assert upload_items(store, manifest["run_id"], [item["id"]], client, dataset) == 1
    assert upload_items(store, manifest["run_id"], [item["id"]], client, dataset) == 0
    assert len(calls) == 1
    Path(item["path"]).write_text("changed after approval", encoding="utf-8")
    with pytest.raises(ValueError):
        upload_items(store, manifest["run_id"], [item["id"]], client, dataset)


def test_uncertain_upload_not_repeated(processed):
    store, manifest, _ = processed
    item = next(i for i in manifest["items"] if i["status"] == "pass")
    approve_items(store, manifest["run_id"], [item["id"]])
    class Client:
        base_url = "http://localhost"
        calls = 0
        def upload(self, dataset_id, path):
            self.calls += 1
            raise TimeoutError("lost response")
    client = Client()
    with pytest.raises(TimeoutError):
        upload_items(store, manifest["run_id"], [item["id"]], client, {"id": "ds", "name": "学前教育"})
    with pytest.raises(RuntimeError, match="不确定"):
        upload_items(store, manifest["run_id"], [item["id"]], client, {"id": "ds", "name": "学前教育"})
    assert client.calls == 1


def test_http_upload_checks_business_codes_and_multipart(tmp_path):
    requests = []
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass
        def do_GET(self):
            requests.append((self.path, self.headers.get("Authorization")))
            self.send_response(200)
            self.end_headers()
            self.wfile.write(json.dumps({"code": 0, "data": [{"id": "ds", "name": "学前教育"}]}).encode())
        def do_POST(self):
            body = self.rfile.read(int(self.headers["Content-Length"]))
            requests.append((self.path, body))
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'{"code":0,"data":[{"id":"doc"}]}')
    server = HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        client = RagflowClient(f"http://127.0.0.1:{server.server_port}", "test-key")
        assert client.datasets()[0]["id"] == "ds"
        path = tmp_path / "UP_test.md"
        path.write_text("脱敏后的测试正文", encoding="utf-8")
        assert client.upload("ds", path) == "doc"
        assert requests[0][1] == "Bearer test-key"
        assert b'name="file"' in requests[1][1]
        assert "脱敏后的测试正文".encode() in requests[1][1]
    finally:
        server.shutdown()
        server.server_close()


def test_startup_marks_interruption_without_running(tmp_path):
    store = Store(tmp_path)
    store.save({"run_id": "UP_test", "status": "running", "items": []})
    store.recover()
    assert store.load("UP_test")["status"] == "interrupted"


def test_reject_remote_plaintext_key():
    with pytest.raises(ValueError):
        RagflowClient("http://example.com", "secret")


def test_render_rag_results_builds_upload_index_once(tmp_path,monkeypatch):
    import tkinter as tk
    from app import Workbench
    from upload_tracking import UploadIndex
    root=tk.Tk();root.withdraw()
    try:
        app=Workbench(root,tmp_path/'data')
        app.current={'run_id':'render','status':'completed','items':[
            {'id':str(n),'status':'pass','automatic_approval':True,'title':'企业财务管理研究'+str(n),
             'source_path':str(tmp_path/f'{n}.txt'),'major':'会计学','uploads':{}} for n in range(3)]}
        app.store.save(app.current)
        builds=[]
        def index(history):builds.append(True);return UploadIndex(history)
        monkeypatch.setattr('rag_results_ui.UploadIndex',index)
        app.render_rag_results()
        assert len(app.rag_result_tree.get_children())==3
        assert len(builds)==1
    finally:
        for timer in root.tk.call('after','info'):root.after_cancel(timer)
        root.destroy()


def test_missing_route_is_waiting_for_configuration_not_upload_failure():
    """A route validation error happens before any RAGFlow upload request."""
    import app as app_module
    from upload_tracking import UploadIndex

    class Var:
        def __init__(self,value=''):self.value=value
        def get(self):return self.value

    workbench=app_module.Workbench.__new__(app_module.Workbench)
    workbench.current={'status':'completed'}
    workbench.store=type('StoreStub',(),{'history':lambda self:[]})()
    workbench.url=Var('http://localhost')
    workbench.datasets=[]
    workbench.dataset_map={}
    workbench._selected_target_dataset=lambda:None
    item={'id':'waiting-route','status':'pass','automatic_approval':True,
          'title':'企业财务研究','major':'财务管理','uploads':{},
          'handoff_status':'pending',
          'handoff_reason':'专业“财务管理”还没选 md 知识库，请先在 RAGFlow 连接页选择并保存'}

    state=workbench._remote_item_status(item,UploadIndex([]))

    assert state['upload']=='配置异常'
    assert state['process']=='尚未提交'
    assert state['reason']==item['handoff_reason']


def test_removed_saved_route_does_not_hide_upload_to_visible_selection():
    import app as app_module
    from upload_tracking import UploadIndex

    class Var:
        def __init__(self,value=''):self.value=value
        def get(self):return self.value

    current={'id':'current-kb','name':'1410'}
    workbench=app_module.Workbench.__new__(app_module.Workbench)
    workbench.current={'status':'completed'}
    workbench.store=type('StoreStub',(),{'history':lambda self:[]})()
    workbench.url=Var('http://localhost')
    workbench.datasets=[current]
    workbench.dataset_map={'会计学':{'paper':'deleted-kb','image':''}}
    workbench._selected_target_dataset=lambda:current
    item={'id':'uploaded-current','status':'pass','automatic_approval':True,
          'title':'企业财务研究','major':'会计学','uploads':{
              'http://localhost|current-kb':{
                  'status':'uploaded','dataset_id':'current-kb','dataset_name':'1410',
                  'document_id':'remote-document'}}}

    state=workbench._remote_item_status(item,UploadIndex([]))

    assert state['target']=='1410'
    assert state['upload']=='已上传'


def test_zip_and_broken_archive_are_reported(tmp_path):
    import zipfile
    source = tmp_path / "source"
    source.mkdir()
    with zipfile.ZipFile(source / "papers.zip", "w") as z:
        z.writestr("专业/学前教育.txt", paper())
    (source / "broken.zip").write_bytes(b"not a zip")
    store = Store(tmp_path / "out")
    manifest = Processor(store).run(scan_sources(source))
    assert manifest["status"] == "completed", manifest
    assert any(item["major"] == "学前教育" for item in manifest["items"])
    assert any(event["status"] == "archive" and event["reason"] for event in manifest["events"])


def test_office_conversion_uses_existing_builder(tmp_path):
    if not Path(r"C:\Program Files\LibreOffice\program\soffice.exe").is_file():
        pytest.skip("LibreOffice not installed")
    source = tmp_path / "input"
    source.mkdir()
    text = "Abstract Reading activity research. First chapter. " + "Observation of reading activities and classroom interaction. " * 120 + " References."
    path = source / "学前教育案例研究.rtf"
    path.write_text("{\\rtf1\\ansi " + text + "}", encoding="ascii")
    before = digest(path)
    store = Store(tmp_path / "out")
    manifest = Processor(store).run(scan_sources(source))
    assert manifest["status"] == "completed", manifest
    assert manifest["items"], manifest
    assert digest(path) == before
    assert manifest["items"][0]["source_hash"] == before
    assert "Observation" in Path(manifest["items"][0]["path"]).read_text(encoding="utf-8")


def test_zip_traversal_is_rejected_before_original_script(tmp_path):
    import zipfile
    from engine import validate_zip
    path = tmp_path / "escape.zip"
    with zipfile.ZipFile(path, "w") as z:
        z.writestr("../../escape.txt", "bad")
    with pytest.raises(ValueError, match="越界"):
        validate_zip(path)
