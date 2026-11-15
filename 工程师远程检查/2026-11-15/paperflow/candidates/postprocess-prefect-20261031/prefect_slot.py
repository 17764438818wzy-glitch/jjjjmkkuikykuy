"""Prefect-owned capacity for the new folder postprocessing stage.

This module does not create work, resume flows, or change server settings.
It only permits processing while the separately configured limit is held.
"""
from contextlib import asynccontextmanager

from prefect.client.orchestration import get_client
from prefect.concurrency.asyncio import concurrency


POSTPROCESS_LIMIT = 'paperflow-folder-postprocessing-v2'
POSTPROCESS_CONCURRENCY = 8


async def _checked_limit_id(expected_api_url):
    if not isinstance(expected_api_url, str) or not expected_api_url.strip():
        raise RuntimeError('Prefect 服务身份不可核验，未开始处理')
    async with get_client() as api:
        if str(getattr(api, 'api_url', '')).rstrip('/') != expected_api_url.rstrip('/'):
            raise RuntimeError('Prefect 服务身份不符，未开始处理')
        row = await api.read_global_concurrency_limit_by_name(POSTPROCESS_LIMIT)
    if (not row.active or row.limit != POSTPROCESS_CONCURRENCY
            or row.slot_decay_per_second != 0):
        raise RuntimeError('Prefect 后处理并发配置不符，未开始处理')
    return str(row.id)


@asynccontextmanager
async def postprocessing_slot(expected_api_url):
    """Hold one native Prefect lease over a complete folder's postprocessing."""
    expected_id = await _checked_limit_id(expected_api_url)
    async with concurrency(POSTPROCESS_LIMIT, occupy=1, strict=True,
                           raise_on_lease_renewal_failure=True):
        if await _checked_limit_id(expected_api_url) != expected_id:
            raise RuntimeError('Prefect 后处理并发名额已改变，未开始处理')
        yield
