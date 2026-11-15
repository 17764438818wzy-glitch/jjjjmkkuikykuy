import copy
import sqlite3

import pytest

from test_remote_runtime_adapter import sample  # original synthetic Prefect/remote fixture


@pytest.mark.parametrize('max_attempts', [2, None])
def test_capacity_wait_reuses_original_prefect_folder_and_admission_gate(sample, max_attempts):
    from remote_runtime_adapter import make_remote_coordinator
    x, attempts = sample, []
    x['wait_release'].set()
    def submit(job, snapshot, staging):
        assert x['gate_held']()
        attempts.append(copy.deepcopy(job))
        if len(attempts) == 1:
            return dict(job=job, server_identity=x['server'], accepted=False,
                        reason='capacity_full', this_attempt_started=False)
        return dict(job=job, server_identity=x['server'], accepted=True)
    def wait(): assert not x['gate_held']()
    options = dict(x['options'], submit_once=submit, admission_wait={'wait': wait, 'max_attempts': max_attempts})
    remote = make_remote_coordinator(**options)
    identity = x['evidence'][0]['identity']
    job = remote.reserve(identity, 1)
    with sqlite3.connect(x['database']) as db:
        db.execute("update paperflow_postprocess_stage_jobs set state='submitted' where job_id=?", (job['job_id'],))
    result = remote._execute_sync(job['job_id'], job['job_id'])
    assert result['identity'] == identity and result['state'] == 'waiting_rag'
    assert len(attempts) == 2 and attempts[0] == attempts[1]


def test_default_path_rechecks_closed_gate_after_staging_before_send(sample):
    from remote_runtime_adapter import make_remote_coordinator
    from stage_runtime import StageConflict
    x, opened = sample, [True]
    x['base'].postprocess_is_open = lambda: opened[0]
    original_validate = x['options']['validate_staging']
    def close_after_staging(*args):
        assert original_validate(*args)
        opened[0] = False
        return True
    def should_not_submit(*args):
        pytest.fail('a closed gate must be rechecked before server admission')
    remote = make_remote_coordinator(**dict(x['options'], validate_staging=close_after_staging,
                                           submit_once=should_not_submit))
    identity = x['evidence'][0]['identity']
    job = remote.reserve(identity, 1)
    with sqlite3.connect(x['database']) as db:
        db.execute("update paperflow_postprocess_stage_jobs set state='submitted' where job_id=?", (job['job_id'],))
    with pytest.raises(StageConflict):
        remote._execute_sync(job['job_id'], job['job_id'])
    assert [event for event, _ in x['events']] == ['transfer']
