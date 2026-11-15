"""Candidate native folder task with cache and automatic retries disabled.

The module only defines a task type; importing it starts no worker or model call.
"""
from prefect import Task
from prefect.cache_policies import NO_CACHE


class FolderTask(Task):
    def __init__(self, fn, **kwargs):
        kwargs['cache_policy'] = NO_CACHE
        kwargs['retries'] = 0
        super().__init__(fn=fn, **kwargs)

    def with_options(self, **kwargs):
        # Prefect 3.9 TaskWorker normally replaces NO_CACHE with DEFAULT when
        # cloning a task; a cached first phase would silently skip the LLM.
        kwargs['cache_policy'] = NO_CACHE
        kwargs['retries'] = 0
        return super().with_options(**kwargs)
