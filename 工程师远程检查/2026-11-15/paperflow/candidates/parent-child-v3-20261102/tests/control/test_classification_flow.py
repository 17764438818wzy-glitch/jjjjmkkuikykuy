"""Exercise the existing one-group classifier and receipt with synthetic files."""

import asyncio
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread

from prefect import flow
from prefect.client.orchestration import get_client
from prefect.context import FlowRunContext
from prefect.testing.utilities import prefect_test_harness


FIRST_STEP = Path(__file__).resolve().parents[3] / "first-step-control-20261029"


def test_real_one_group_scripts_produce_a_reusable_registered_paper(tmp_path):
    from paperflow_v3.control.admission import Admission, begin_control_session
    from paperflow_v3.control.classification import classify_and_register

    calls = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            data = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            calls.append(data)
            criteria = data["questions"]["decision"]["criteria"]
            choice = next(key for key in criteria if key != "none")
            raw = json.dumps({"answers": {"decision": {"choice": choice, "confidence": 1,
                "probabilities": {key: 1 if key == choice else 0 for key in criteria}}}}).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def log_message(self, *_):
            pass

    library = tmp_path / "library"
    group = library / "one-paper"
    group.mkdir(parents=True)
    (group / "paper.txt").write_text("题目：合成论文\n摘要：合成文字\n关键词：测试", encoding="utf8")
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()

    @flow(persist_result=False)
    def parent():
        task_id = FlowRunContext.get().flow_run.id
        admission = Admission(task_id)
        asyncio.run(begin_control_session())
        asyncio.run(admission.start())
        kwargs = dict(first_step_root=FIRST_STEP, task_id=task_id, group_path=group,
            library_root=library, work_root=tmp_path / "work", admission=admission,
            nimble_endpoint=f"http://127.0.0.1:{server.server_port}/v1/systemone",
            nimble_lock_path=tmp_path / "nimble.lock", history_roots=())
        first = classify_and_register(**kwargs)
        second = classify_and_register(**kwargs)
        assert first["status"] == "passed"
        assert first["delivery_complete"] is True
        assert first["registration_status"] == "registered"
        assert first["paper"]["body"]["path"]
        assert first["receipt_id"] == second["receipt_id"]
        assert len(calls) == 1

    try:
        with prefect_test_harness(server_startup_timeout=60):
            parent()
    finally:
        server.shutdown()
        thread.join()
        server.server_close()


def test_admitted_folder_finishes_classification_and_registration_after_pause(tmp_path):
    from paperflow_v3.control.admission import Admission, begin_control_session
    from paperflow_v3.control.classification import classify_and_register
    from paperflow_v3.control.stage_admission import FolderStage, pause_progress

    controls, calls = {}, []
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            data = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            calls.append(data)
            if len(calls) == 1:
                asyncio.run_coroutine_threadsafe(controls['gate'].pause(), controls['loop']).result(10)
            criteria = data['questions']['decision']['criteria']
            choice = next(key for key in criteria if key != 'none')
            raw = json.dumps({'answers': {'decision': {'choice': choice, 'confidence': 1,
                'probabilities': {key: int(key == choice) for key in criteria}}}}).encode()
            self.send_response(200)
            self.send_header('Content-Length', str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)
        def log_message(self, *_args):
            pass
    library = tmp_path / 'library'
    folder = library / 'one-paper'
    folder.mkdir(parents=True)
    (folder / 'paper.txt').write_text('题目：合成论文\n摘要：合成文字\n关键词：测试', encoding='utf8')
    (folder / '实验数据.csv').write_text('编号,数值\n1,2\n', encoding='utf8')
    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()

    @flow(persist_result=False)
    async def parent():
        task_id = FlowRunContext.get().flow_run.id
        await begin_control_session()
        gate = Admission(task_id)
        await gate.start()
        controls.update(gate=gate, loop=asyncio.get_running_loop())
        from paperflow_v3.control.classification import first_step_modules
        group_id = first_step_modules(FIRST_STEP)['classification']._group_id(folder, library)
        ids = {'task_id': str(task_id), 'group_id': group_id, 'task_run_id': str(task_id)}
        async with FolderStage(gate, ids, 'classification') as stage:
            result = await asyncio.to_thread(classify_and_register, first_step_root=FIRST_STEP,
                task_id=task_id, group_path=folder, library_root=library, work_root=tmp_path / 'work',
                admission=stage, nimble_endpoint=f'http://127.0.0.1:{server.server_port}/v1/systemone',
                nimble_lock_path=tmp_path / 'nimble.lock', history_roots=())
            assert result['status'] == 'passed' and result['registration_status'] == 'registered'
            assert len(result['paper']['attachments']) == 1
            assert not await gate.running()
            assert (await pause_progress(task_id))['state'] == 'pausing'
            from paperflow_v3.control.flows import paper_order
            from paperflow_v3.control.parent import schedule_registered_paper
            async with get_client() as client:
                deployment = await client.create_deployment(await client.create_flow(paper_order), name='isolated-queued')
            await schedule_registered_paper(deployment, result, stage)
        assert (await pause_progress(task_id))['state'] == 'paused'
    try:
        with prefect_test_harness(server_startup_timeout=60):
            asyncio.run(parent())
    finally:
        server.shutdown()
        thread.join()
        server.server_close()
