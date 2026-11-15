"""Run one exact Prefect folder with its independently derived server scope."""
import argparse
import asyncio
import json
import sys
from uuid import UUID


FIELDS = ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id', 'parent_run_id', 'task_run_id')


async def run_exact_folder(runtime, identity, generation, *, retry_job_id=None):
    if (not isinstance(identity, dict) or set(identity) != set(FIELDS)
            or any(not isinstance(identity[key], str) or not identity[key] for key in FIELDS)
            or type(generation) is not int or generation < 1 or runtime.execution_enabled is not True):
        raise ValueError('one enabled original folder identity and generation are required')
    await asyncio.to_thread(runtime.check_configuration)
    from prefect.settings import (PREFECT_API_URL, PREFECT_TASKS_SCHEDULING_DELETE_FAILED_SUBMISSIONS,
                                  temporary_settings)
    with temporary_settings({PREFECT_API_URL: runtime.api,
                             PREFECT_TASKS_SCHEDULING_DELETE_FAILED_SUBMISSIONS: False}):
        current, current_generation = await asyncio.to_thread(runtime.live.select_folder,
            runtime.original_runtime['database'], identity['task_id'], identity['group_id'])
        if current != identity or current_generation != generation:
            raise ValueError('the original folder identity or generation changed')
        await asyncio.to_thread(runtime.check_configuration)
        if retry_job_id is None:
            return await runtime.remote.entry.run_one(identity, generation)
        return await runtime.remote.entry.run_one(identity, generation, retry_job_id=retry_job_id)


async def resume_stopped_folder(runtime, identity, generation, job_id):
    """Resume the exact never-submitted stage after its original policy was rechecked."""
    if (not isinstance(identity, dict) or set(identity) != set(FIELDS)
            or any(not isinstance(identity[key], str) or not identity[key] for key in FIELDS)
            or type(generation) is not int or generation < 1 or runtime.execution_enabled is not True
            or not isinstance(job_id, str) or str(UUID(job_id)) != job_id):
        raise ValueError('one enabled original folder and stopped stage are required')
    await asyncio.to_thread(runtime.check_configuration)
    from prefect.settings import (PREFECT_API_URL, PREFECT_TASKS_SCHEDULING_DELETE_FAILED_SUBMISSIONS,
                                  temporary_settings)
    with temporary_settings({PREFECT_API_URL: runtime.api,
                             PREFECT_TASKS_SCHEDULING_DELETE_FAILED_SUBMISSIONS: False}):
        current, current_generation = await asyncio.to_thread(runtime.live.select_folder,
            runtime.original_runtime['database'], identity['task_id'], identity['group_id'])
        if current != identity or current_generation != generation:
            raise ValueError('the original stopped folder or generation changed')
        await asyncio.to_thread(runtime.check_configuration)
        # Do not rearm even a stopped stage when the remote journal already
        # contains a transfer or start intent for this original folder.
        await asyncio.to_thread(runtime.remote.journal._unreserved, identity, job_id=job_id)
        return await runtime.remote.entry.resume_stopped(job_id, generation)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', required=True)
    parser.add_argument('--config-sha256', required=True)
    args = parser.parse_args()
    try:
        raw = sys.stdin.buffer.read(1024 * 1024 + 1)
        if len(raw) > 1024 * 1024:
            raise ValueError('folder identity input is too large')
        request = json.loads(raw)
        if not isinstance(request, dict) or set(request) != {'identity', 'generation'}:
            raise ValueError('explicit folder identity input is required')
        from remote_live_launch import build_from_config
        runtime = build_from_config(args.config, args.config_sha256)
        result = asyncio.run(run_exact_folder(runtime, request['identity'], request['generation']))
        print(json.dumps({'state': result.get('state'), 'rag_handoff': 'not_checked'}))
        return 0 if result.get('state') == 'finished' else 1
    except Exception as exc:
        print(json.dumps({'state': 'not_confirmed', 'error_type': type(exc).__name__}), file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
