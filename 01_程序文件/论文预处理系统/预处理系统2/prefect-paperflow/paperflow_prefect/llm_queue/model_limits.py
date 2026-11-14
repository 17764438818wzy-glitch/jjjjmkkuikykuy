"""Shared model-request leases on the explicitly selected Prefect server.

These are resource names only; no old flow, deployment, or dispatch link is
used. Reusing the total lease also counts any concurrent generated-title call.
"""
from contextlib import contextmanager
from prefect.settings import PREFECT_API_URL


TOTAL = 'paperflow-connected-total_llm'
BODY = 'paperflow-connected-body_llm'
ATTACHMENT = 'paperflow-connected-attachment_llm'
LIMITS = {TOTAL: 50, BODY: 50, ATTACHMENT: 20}


@contextmanager
def model_lease(kind, stop, expected_api_url, acquire):
    """Acquire the shared and lane-specific lease at the HTTP boundary.

    acquire is the existing strict cancellable Prefect concurrency helper.
    This function creates no limits and does not start any requests.
    """
    if kind not in {'body', 'attachment'} or not callable(acquire):
        raise ValueError('模型并发类型无效')
    actual = PREFECT_API_URL.value()
    if (not isinstance(expected_api_url, str) or not expected_api_url
            or not isinstance(actual, str)
            or actual.rstrip('/') != expected_api_url.rstrip('/')):
        raise ValueError('模型并发未连接到指定的新 Prefect')
    if stop is None or stop.is_set():
        raise InterruptedError('任务已暂停，未领取模型名额')
    lane = BODY if kind == 'body' else ATTACHMENT
    names = [TOTAL, lane]
    with acquire(names, stop, expected={name: LIMITS[name] for name in names}):
        if stop.is_set():
            raise InterruptedError('任务已暂停，未发送模型请求')
        yield
