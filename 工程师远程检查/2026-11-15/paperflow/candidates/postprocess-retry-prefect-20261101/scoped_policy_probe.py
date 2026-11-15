"""Read one registered folder's source policy in a short-lived process.

This does not claim or dispatch work, create a file, call an LLM, or contact
RAG. The underlying read-only probe verifies original Prefect and receipt data.
"""
import copy
import json
import sys


FIELDS = ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id', 'parent_run_id', 'task_run_id')


def read_one(sources, identity, generation, *, inspect=None, resume_job_id=None, retry_job_id=None):
    if (not isinstance(identity, dict) or set(identity) != set(FIELDS)
            or any(not isinstance(identity[key], str) or not identity[key] for key in FIELDS)
            or type(generation) is not int or generation < 1):
        raise ValueError('one complete original folder and generation are required')
    if inspect is None:
        from selected_ticket_probe import inspect_exact_folder
        inspect = inspect_exact_folder
    original = copy.deepcopy(identity)
    supplied = copy.deepcopy(identity)
    if resume_job_id is None and retry_job_id is None:
        record = inspect(sources=sources, identity=supplied)
    elif retry_job_id is not None:
        record = inspect(sources=sources, identity=supplied, retry_job_id=retry_job_id)
    else:
        record = inspect(sources=sources, identity=supplied, resume_job_id=resume_job_id)
    if (supplied != original or not isinstance(record, dict)
            or record.get('schema') != 'paperflow-selected-ticket-evidence/1'
            or record.get('identity') != original or record.get('generation') != generation
            or record.get('resume_job_id') != resume_job_id or record.get('retry_job_id') != retry_job_id
            or record.get('claim_created') is not False or record.get('server_execution_started') is not False):
        raise ValueError('the registered original folder or generation changed')
    return copy.deepcopy(record)


def main():
    try:
        raw = sys.stdin.buffer.read(1024 * 1024 + 1)
        if len(raw) > 1024 * 1024:
            raise ValueError('one bounded folder identity is required')
        request = json.loads(raw)
        if (not isinstance(request, dict) or set(request) not in
                ({'identity', 'generation'}, {'identity', 'generation', 'resume_job_id'},
                 {'identity', 'generation', 'retry_job_id'})):
            raise ValueError('one original folder and generation are required')
        from selected_ticket_probe import load_sources
        record = read_one(load_sources(), request['identity'], request['generation'],
                          resume_job_id=request.get('resume_job_id'), retry_job_id=request.get('retry_job_id'))
        print(json.dumps(record, ensure_ascii=False, sort_keys=True, allow_nan=False))
        return 0
    except Exception as exc:
        print(json.dumps({'state': 'unavailable', 'error_type': type(exc).__name__}), file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
