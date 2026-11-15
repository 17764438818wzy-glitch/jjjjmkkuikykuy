"""Opt-in synthetic acceptance against a disposable loopback Prefect server."""
import asyncio
import json
import os
from pathlib import Path
import socket
import sys
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ['PREFECT_SERVER_ANALYTICS_ENABLED'] = 'false'
os.environ['PREFECT_LOGGING_TO_API_ENABLED'] = 'false'
os.environ['NO_PROXY'] = '127.0.0.1,localhost,::1'

from prefect.client.orchestration import get_client
from prefect.client.schemas.actions import GlobalConcurrencyLimitCreate
from prefect.testing.utilities import prefect_test_harness

from prefect_slot import POSTPROCESS_LIMIT, postprocessing_slot


async def exercise():
    async with get_client() as api:
        api_url = str(api.api_url)
        assert api_url.startswith(('http://127.0.0.1:', 'http://localhost:'))
        await api.create_global_concurrency_limit(GlobalConcurrencyLimitCreate(
            name=POSTPROCESS_LIMIT, limit=8, active=True))

    release = asyncio.Event()
    entered = []

    async def synthetic_folder(index):
        async with postprocessing_slot(api_url):
            entered.append(index)
            await release.wait()

    tasks = [asyncio.create_task(synthetic_folder(i)) for i in range(9)]
    try:
        async with asyncio.timeout(30):
            while len(entered) < 8:
                await asyncio.sleep(.05)
        await asyncio.sleep(.5)
        assert len(entered) == 8, entered
        async with get_client() as api:
            observed = await api.read_global_concurrency_limit_by_name(POSTPROCESS_LIMIT)
            assert observed.limit == 8 and observed.active_slots == 8
    finally:
        release.set()
    async with asyncio.timeout(30):
        await asyncio.gather(*tasks)
    async with get_client() as api:
        final = await api.read_global_concurrency_limit_by_name(POSTPROCESS_LIMIT)
        assert final.active_slots == 0
    assert len(entered) == 9
    print(json.dumps({'limit': 8, 'first_wave': 8, 'remaining_waited': 1,
                      'finished': 9, 'final_active_slots': final.active_slots,
                      'real_papers': 0, 'real_model_calls': 0, 'real_rag_calls': 0}))


def main():
    original_connect = socket.socket.connect

    def loopback_only(sock, address):
        if isinstance(address, tuple) and address[0] not in ('127.0.0.1', 'localhost', '::1'):
            raise AssertionError('synthetic acceptance forbids non-loopback connections')
        return original_connect(sock, address)

    with patch.object(socket.socket, 'connect', loopback_only):
        with prefect_test_harness(server_startup_timeout=45):
            asyncio.run(exercise())


if __name__ == '__main__':
    main()
