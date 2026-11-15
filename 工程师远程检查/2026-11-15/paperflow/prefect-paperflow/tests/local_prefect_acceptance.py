"""Opt-in integration against Prefect's temporary local SQLite test server."""
import asyncio
import json
import os
from pathlib import Path
import socket
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ['PREFECT_SERVER_ANALYTICS_ENABLED'] = 'false'
os.environ['PREFECT_LOGGING_TO_API_ENABLED'] = 'false'
os.environ['NO_PROXY'] = '127.0.0.1,localhost,::1'

from paperflow_prefect import flows
from paperflow_prefect.records import write_json
from paperflow_prefect.settings import PROJECT, Settings
from prefect.client.orchestration import get_client
from prefect.client.schemas.actions import GlobalConcurrencyLimitCreate, GlobalConcurrencyLimitUpdate
from prefect.testing.utilities import prefect_test_harness


async def exercise():
    cfg = Settings.load()
    async with get_client() as api:
        assert str(api.api_url).startswith(('http://127.0.0.1:', 'http://localhost:')), 'non-local API forbidden'
        for key, value in cfg['limits'].items():
            await api.create_global_concurrency_limit(GlobalConcurrencyLimitCreate(
                name=flows.LIMIT_PREFIX + key, limit=value, active=True))
    checked = await asyncio.to_thread(flows.verify_limits, cfg)
    release = threading.Event(); entered = []; mutex = threading.Lock()
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        papers = [{'paper_id':f'local-{i}', 'revision':'fixture', 'attachments':[]} for i in range(25)]
        def publish(paper):
            with mutex: entered.append(paper['paper_id'])
            if not release.wait(25): raise TimeoutError('local acceptance release deadline')
        backend = SimpleNamespace(root=root, settings=cfg, publish_body=publish,
            stops={p['paper_id']:threading.Event() for p in papers}, paper_root=lambda p: root/p['paper_id'])
        for key, rows in (('local-a', papers[:13]), ('local-b', papers[13:])):
            flows._batches[key] = {'backend':backend,'papers':{p['paper_id']:p for p in rows},
                'gates':{kind:asyncio.Semaphore(value) for kind,value in cfg['limits'].items()}}
        tasks = [asyncio.create_task(flows.business_stage.fn(
            'local-a' if i < 13 else 'local-b', p['paper_id'], 'publish_body')) for i,p in enumerate(papers)]
        try:
            deadline = time.monotonic() + 15
            while len(entered) < 20 and time.monotonic() < deadline: await asyncio.sleep(.05)
            assert len(entered) == 20, f'only {len(entered)} stages entered'
            await asyncio.sleep(.25)
            assert len(entered) == 20, 'twenty-first stage exceeded shared limit'
            async with get_client() as api:
                row = await api.read_global_concurrency_limit_by_name('paperflow-connected-rag')
                assert row.limit == 20 and row.active_slots == 20, row
                observed = {'limit':row.limit, 'active_slots':row.active_slots, 'entered':len(entered),
                            'waiting':25-len(entered)}
        finally:
            release.set()
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for key in ('local-a','local-b'): flows._batches.pop(key, None)
        assert all(not isinstance(r, BaseException) for r in results), results
        assert len(entered) == 25
        async with get_client() as api:
            row = await api.read_global_concurrency_limit_by_name('paperflow-connected-rag')
            assert row.active_slots == 0, row
            await api.update_global_concurrency_limit(row.id, GlobalConcurrencyLimitUpdate(limit=4))
        try:
            await asyncio.to_thread(flows.verify_limits, cfg)
        except ValueError: mismatch_rejected = True
        else: raise AssertionError('local 20 / remote 4 mismatch was accepted')
        async with get_client() as api:
            await api.update_global_concurrency_limit(row.id, GlobalConcurrencyLimitUpdate(limit=20))
        return dict(local_prefect_server=True, configured_limits=checked, peak=observed,
                    final_active_slots=0, all_tasks_completed=25, mismatch_rejected=mismatch_rejected,
                    real_papers=0, real_rag_calls=0, real_model_calls=0, real_nas_writes=0)


def main():
    original_connect = socket.socket.connect
    def loopback_only(sock, address):
        if isinstance(address, tuple) and address[0] not in ('127.0.0.1','localhost','::1'):
            raise AssertionError('Local acceptance forbids non-loopback connections')
        return original_connect(sock, address)
    with patch.object(socket.socket, 'connect', loopback_only):
        with prefect_test_harness(server_startup_timeout=45):
            result = asyncio.run(exercise())
    write_json(PROJECT/'evidence/local-prefect-acceptance.json', result)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == '__main__': main()
