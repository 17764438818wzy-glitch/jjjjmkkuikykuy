import sys
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import prefect_slot


def run(coro):
    import asyncio
    return asyncio.run(coro)


def install_prefect_fakes(monkeypatch, rows):
    events = []
    rows = iter(rows)

    class FakeClient:
        api_url = 'http://127.0.0.1:4200/api'

        async def read_global_concurrency_limit_by_name(self, name):
            events.append(('read', name))
            return next(rows)

    @asynccontextmanager
    async def client():
        yield FakeClient()

    @asynccontextmanager
    async def concurrency(name, **kwargs):
        events.append(('acquire', name, kwargs))
        try:
            yield
        finally:
            events.append(('release', name))

    monkeypatch.setattr(prefect_slot, 'get_client', client)
    monkeypatch.setattr(prefect_slot, 'concurrency', concurrency)
    return events


def limit(**changes):
    return SimpleNamespace(**({'id': 'limit-a', 'active': True, 'limit': 8,
                               'slot_decay_per_second': 0} | changes))


def test_admission_requires_prefect_limit_8_and_releases_on_error(monkeypatch):
    events = install_prefect_fakes(monkeypatch, [limit(), limit()])

    async def work():
        async with prefect_slot.postprocessing_slot('http://127.0.0.1:4200/api'):
            events.append(('work',))
            raise ValueError('fixture failed')

    with pytest.raises(ValueError, match='fixture failed'):
        run(work())
    assert [item[0] for item in events] == ['read', 'acquire', 'read', 'work', 'release']
    name, opts = events[1][1:]
    assert name == prefect_slot.POSTPROCESS_LIMIT
    assert opts['occupy'] == 1 and opts['strict'] is True
    assert opts['raise_on_lease_renewal_failure'] is True


@pytest.mark.parametrize('bad', [limit(limit=2), limit(active=False), limit(slot_decay_per_second=1)])
def test_wrong_or_inactive_limit_never_starts_work(monkeypatch, bad):
    events = install_prefect_fakes(monkeypatch, [bad])

    async def work():
        async with prefect_slot.postprocessing_slot('http://127.0.0.1:4200/api'):
            pytest.fail('work must not start')

    with pytest.raises(RuntimeError, match='并发'):
        run(work())
    assert [e[0] for e in events] == ['read']


def test_replaced_limit_during_acquisition_is_released(monkeypatch):
    events = install_prefect_fakes(monkeypatch, [limit(), limit(id='limit-b')])

    async def work():
        async with prefect_slot.postprocessing_slot('http://127.0.0.1:4200/api'):
            pytest.fail('work must not start')

    with pytest.raises(RuntimeError, match='并发'):
        run(work())
    assert [e[0] for e in events] == ['read', 'acquire', 'read', 'release']


def test_wrong_prefect_service_cannot_acquire_a_matching_name(monkeypatch):
    events = install_prefect_fakes(monkeypatch, [limit()])
    async def work():
        async with prefect_slot.postprocessing_slot('http://127.0.0.1:4291/api'):
            pytest.fail('work must not start')
    with pytest.raises(RuntimeError, match='Prefect'):
        run(work())
    assert events == []
