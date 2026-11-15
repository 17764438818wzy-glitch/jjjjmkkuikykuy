"""A failed final save can be retried on the original stopped folder ticket."""

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

import httpx
from prefect.client.orchestration import PrefectClient
from prefect.settings import PREFECT_API_URL, temporary_settings
from prefect.states import Failed, Scheduled
from prefect.variables import Variable


def test_completion_save_failure_uses_original_retry_endpoint(tmp_path, monkeypatch):
    from paperflow_v3.control import api, preflight
    from paperflow_v3.delivery import progress

    task_id, child_id, deployment_id = uuid4(), uuid4(), uuid4()
    registered = dict(task_id=str(task_id), group_id='folder', paper_id='paper', receipt_id='receipt')
    child = SimpleNamespace(id=child_id, state=Failed(),
        parameters=dict(task_id=str(task_id), group_id='folder', registered=registered))
    identity = api._folder_identity(child)
    state = progress.FolderProgress(SimpleNamespace(path=lambda _: tmp_path / 'data'), identity, [],
                                    lambda **_: False)
    try:
        state.complete('cleanup', {'identity': identity, 'status': 'complete'})
    except ValueError:
        pass
    saved, transitions, variables = [], [], {}

    async def no_version(_):
        pass
    async def get_variable(name, *args, **kwargs):
        return variables.get(name)
    async def set_variable(name, value, *args, **kwargs):
        variables[name] = value.copy()
    async def read_run(_client, _run_id):
        return SimpleNamespace(deployment_id=deployment_id)
    async def set_state(_client, run_id, _state):
        transitions.append(run_id)
        return SimpleNamespace(state=Scheduled())
    async def children(_):
        return [child]
    async def receipts(_):
        return saved[::-1]
    async def save(**row):
        saved.append({**row, 'recorded_at': datetime.now(timezone.utc).isoformat()})
    async def ready(_):
        return {'ready': True, 'checks': []}

    monkeypatch.setattr(PrefectClient, 'raise_for_api_version_mismatch_once', no_version)
    monkeypatch.setattr(PrefectClient, 'read_flow_run', read_run)
    monkeypatch.setattr(PrefectClient, 'set_flow_run_state', set_state)
    monkeypatch.setattr(Variable, 'aget', staticmethod(get_variable))
    monkeypatch.setattr(Variable, 'aset', staticmethod(set_variable))
    monkeypatch.setattr(api, '_children', children)
    monkeypatch.setattr(api, 'read_receipts', receipts)
    monkeypatch.setattr(api, 'save_receipt', save)
    monkeypatch.setattr(preflight, 'inspect_dependencies', ready)
    monkeypatch.setattr(progress, 'local_records', lambda _: list(state.value['records'].values()))
    app = api.create_app({'deployment_id': deployment_id})

    async def exercise():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
            url = f'/api/v1/prefect/tasks/{task_id}/folders/folder/retry'
            response = await client.post(url, json={'request_id': str(uuid4())})
            assert response.status_code == 200, response.text
            assert response.json()['ticket_id'] == str(child_id)
            assert transitions == [child_id] and saved[0]['data']['stage'] == 'cleanup'
            assert (await client.post(url, json={'request_id': str(uuid4())})).status_code == 409
            assert transitions == [child_id]

    with temporary_settings({PREFECT_API_URL: 'http://127.0.0.1:1/api'}):
        asyncio.run(exercise())
