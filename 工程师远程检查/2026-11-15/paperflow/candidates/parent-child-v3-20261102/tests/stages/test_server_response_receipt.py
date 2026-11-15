"""The candidate saves exact response bytes to an isolated Prefect server."""
import asyncio
from contextlib import asynccontextmanager
import io
import json
from pathlib import Path
from uuid import uuid4

from prefect import flow
from prefect.context import FlowRunContext
from prefect.testing.utilities import prefect_test_harness


def test_actual_prefect_artifact_keeps_response_when_local_file_is_unavailable(tmp_path, monkeypatch):
    from paperflow_v3.stages import llm_request
    from paperflow_v3.control.receipts import read_receipts

    class Admission:
        def running_sync(self):
            return True

        async def wait(self):
            pass

    @asynccontextmanager
    async def isolated_slot(_admission, _limits):
        if hasattr(_admission, 'admit'):
            assert await _admission.admit()
        yield

    class Response(io.BytesIO):
        headers = {'Content-Type': 'application/json'}

    class SyntheticOpener:
        def open(self, _request, timeout):
            assert timeout > 0
            return Response(json.dumps({'choices': [{'message': {'content': '第一行\n第二行'},
                'finish_reason': 'stop'}]}, ensure_ascii=False).encode('utf8'))

    monkeypatch.setattr(llm_request, 'admitted_slot', isolated_slot)
    task_id = str(uuid4())

    @flow(persist_result=False)
    async def child():
        child_id = str(FlowRunContext.get().flow_run.id)
        result = await llm_request.run_raw_component(
            identity={'task_id': task_id, 'group_id': 'synthetic-folder', 'task_run_id': child_id},
            component_id='synthetic-body', request_id='synthetic-request', kind='body',
            request_body_factory=lambda: {'model': 'synthetic', 'messages': []},
            connection={'base_url': 'http://synthetic.invalid/v1', 'api_key': 'synthetic'},
            work=tmp_path / 'work', admission=Admission(), limit_names=['synthetic'],
            check_history=lambda: None, check_source=lambda: None, opener=SyntheticOpener())
        return child_id, result

    async def exercise():
        from paperflow_v3.control.admission import control_gate
        async with control_gate(task_id):
            pass
        child_id, result = await child()
        response_path = Path(result['response_path'])
        expected = response_path.read_bytes()
        response_path.rename(response_path.with_name('temporarily-unavailable.txt'))
        records = await read_receipts(child_id)
        saved = next(row for row in records if row['kind'] == 'llm_response_saved')
        assert saved['data']['response_text'].encode('utf8') == expected
        assert saved['data']['response_sha256'] == result['response_sha256']

    with prefect_test_harness(server_startup_timeout=60):
        asyncio.run(exercise())
