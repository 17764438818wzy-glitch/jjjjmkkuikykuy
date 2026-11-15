"""Explicit, bounded candidate task snapshot using original Prefect controls.

No automatic scan, process-wide lock, LLM, RAG or local business fallback is
installed here. A caller still must solve shared server admission/capacity
reconciliation before using this to process a live backlog.
"""
import asyncio
from uuid import UUID

from remote_scoped_batch import run_scoped_batch


def _uuid(value):
    try:
        return isinstance(value, str) and str(UUID(value)) == value
    except (ValueError, TypeError, AttributeError):
        return False


async def run_explicit_task(runtime, launcher, task_id, *, max_folders, batch_size):
    if (not _uuid(task_id) or type(max_folders) is not int or not 1 <= max_folders <= 10000
            or type(batch_size) is not int or not 1 <= batch_size <= 8
            or runtime.execution_enabled is not True or not callable(getattr(launcher, 'prepare', None))
            or not callable(getattr(launcher, 'launch', None))):
        raise ValueError('one enabled original task and explicit bounded selections are required')
    from prefect.settings import (PREFECT_API_URL, PREFECT_TASKS_SCHEDULING_DELETE_FAILED_SUBMISSIONS,
                                  temporary_settings)
    with temporary_settings({PREFECT_API_URL: runtime.api,
                             PREFECT_TASKS_SCHEDULING_DELETE_FAILED_SUBMISSIONS: False}):
        await asyncio.to_thread(runtime.check_configuration)
        authority = await asyncio.to_thread(runtime._batch_task_authority, task_id)
        if not isinstance(authority, tuple) or len(authority) != 3:
            raise ValueError('original task authority is unavailable')
        selection = await asyncio.to_thread(runtime.batch.snapshot_waiting,
            runtime.original_runtime['database'], task_id, max_folders)
        if not isinstance(selection, list) or len(selection) > max_folders:
            raise ValueError('original task snapshot is unavailable')
        if await asyncio.to_thread(runtime._batch_task_authority, task_id) != authority:
            raise ValueError('original task authority changed during selection')
        frozen, seen = [], set()
        for pair in selection:
            if not isinstance(pair, (list, tuple)) or len(pair) != 2:
                raise ValueError('original folder snapshot is invalid')
            identity, generation = pair
            identity = runtime.batch._identity(identity)
            if (identity.get('task_id') != task_id or identity.get('parent_run_id') != authority[1]
                    or generation != authority[0] or type(generation) is not int
                    or identity.get('task_run_id') in seen):
                raise ValueError('original folder identity, parent or generation changed')
            seen.add(identity['task_run_id'])
            frozen.append((identity, generation))
        async def launch(identity, prepared):
            return await launcher.launch(identity, prepared['generation'], prepared)
        return await run_scoped_batch(task_id=task_id, selections=frozen, batch_size=batch_size,
            check_configuration=runtime.check_configuration,
            authority=lambda: runtime._batch_task_authority(task_id), prepare=launcher.prepare, launch=launch)
