"""Bound education model calls; let the durable queue own provider retries."""
import asyncio
from copy import copy
import json
import math

import openai


EDUCATION_BINDINGS = {
    '72ff5454b8f711f1a8846d29005ec004': ('afb95246b4d311f1b74c89b9f52f3fbe', 180),
    '2df63160b98911f199ca358d29021dae': ('e7e4a8e0b01511f199856dc9cfded896', 300),
}
EDUCATION_DATASETS = {
    'ecf88dc4b82b11f1a8846d29005ec004',
    '3e8ef88cbc4e11f1a8e165c0e34ea629',
}


def _education_failure(kind, status=None, retry_after=60):
    return RuntimeError('EDUCATION_LLM_FAILURE|' + json.dumps({
        'version': 1, 'kind': kind, 'status_code': status,
        'retry_after': retry_after}, separators=(',', ':')))


async def _education_request_error(error, attempt):
    """Only actual SDK exception types/HTTP headers establish provider failure."""
    status = None
    delay = 60
    if isinstance(error, openai.APITimeoutError):
        kind = 'timeout'
    elif isinstance(error, openai.APIConnectionError):
        kind = 'unavailable'
    elif isinstance(error, openai.APIStatusError):
        status = error.status_code
        kind = ('rate_limit' if status == 429 else
                'authentication' if status in (401, 403) else
                'arrearage' if status == 402 else
                'unavailable' if 500 <= status <= 599 else 'request_invalid')
        try:
            value = float(error.response.headers.get('retry-after', '60'))
            if math.isfinite(value) and value >= 0:
                delay = min(3600, max(1, value))
        except (TypeError, ValueError):
            pass
    else:
        # Do not turn arbitrary model output or an untyped exception into a
        # provider circuit decision, nor persist private response bodies.
        raise RuntimeError('Education model failure: ' + type(error).__name__) from None
    raise _education_failure(kind, status, delay) from None


async def education_generate(component, delegate, msg, kwargs):
    canvas = component._canvas
    binding = EDUCATION_BINDINGS.get(getattr(canvas, '_flow_id', None))
    if (getattr(canvas, '_kb_id', None) not in EDUCATION_DATASETS
            or getattr(component, '_id', None) != 'Extractor:EducationAnnotation'
            or not binding or component._param.llm_id != binding[0]):
        return await delegate(msg, **kwargs)
    original = component.chat_mdl.mdl
    model = copy(original)
    # LLMBundle owns this model; the clone leaves other calls' retry policies
    # unchanged. SDK and RAGFlow retry layers both yield to the durable queue.
    if hasattr(original, 'async_client'):
        model.async_client = original.async_client.with_options(max_retries=0, timeout=binding[1])
    elif getattr(original, 'provider', None) == 'DeepSeek' and hasattr(original, '_construct_completion_args'):
        # Official DeepSeek is LiteLLMBase, which derives num_retries from
        # max_retries and uses timeout directly in acompletion.
        model.timeout = binding[1]
    else:
        raise RuntimeError('Education model adapter changed; verify its retry contract')
    model.max_retries = 0
    model._exceptions_async = _education_request_error
    component.chat_mdl.mdl = model
    try:
        try:
            return await asyncio.wait_for(delegate(msg, **kwargs), timeout=binding[1])
        except asyncio.TimeoutError:
            raise _education_failure('timeout') from None
    finally:
        component.chat_mdl.mdl = original
