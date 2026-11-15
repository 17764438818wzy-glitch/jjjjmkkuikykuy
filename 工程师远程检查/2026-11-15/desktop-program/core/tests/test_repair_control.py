from concurrent.futures import ThreadPoolExecutor
import time

import pytest

from paperflow.control import Manager
from paperflow.package import save
from paperflow.processing_control import ProcessingCommits
from test_control import start_worker
from test_processing_commits import admission
from test_repair_store import repair_case,report


@pytest.fixture
def repair_manager(repair_case,tmp_path):
    c,s,_,_,_=repair_case
    m=Manager(s,tmp_path/'manager').start();ProcessingCommits(m,c);m.resume()
    yield m
    m.shutdown()


def commit(m,ex,op,descriptor):
    tid=admission(m,ex,op)
    try:return m.processing.handle(ex.execution_id,op,descriptor)
    finally:m.record_result(ex.execution_id,tid,m.epoch,'complete',{})


def test_managed_budget_imports_file_max_and_replay_is_free(repair_case,repair_manager,tmp_path):
    c,_,task,ledger,identity=repair_case;m=repair_manager
    save(c.root/'private/delivery-corrections'/(task['task_id']+'.json'),dict(source_sha256=task['source_sha256'],count=1))
    ex,_=start_worker(m,tmp_path,name=task['task_id'])
    assert commit(m,ex,'correction_count',{})['used']==1
    desc=dict(stage='round3_chunk',fingerprint='f'*64,ordinal=1)
    assert commit(m,ex,'correction_reserve',desc)['used']==2
    assert commit(m,ex,'correction_reserve',desc)['used']==2
    assert ledger.used(identity)==2


def test_budget_db_wait_does_not_bypass_pause(repair_case,repair_manager,tmp_path):
    _,_,task,ledger,identity=repair_case;m=repair_manager
    ex,_=start_worker(m,tmp_path,name=task['task_id'])
    admission(m,ex,'correction_reserve')
    with m.store.db() as writer,ThreadPoolExecutor(1) as pool:
        writer.execute('BEGIN IMMEDIATE')
        future=pool.submit(m.processing.handle,ex.execution_id,'correction_reserve',dict(stage='round1',fingerprint='f'*64,ordinal=1))
        time.sleep(.06);assert not future.done()
        m.pause('blocked budget');writer.commit()
        with pytest.raises(ValueError):future.result(timeout=5)
    assert ledger.used(identity)==0 and not ledger.rows('SELECT * FROM correction_attempts')


def test_pause_before_repair_start_keeps_request_and_original_task(repair_case,repair_manager):
    from paperflow.repair_control import launch_repair
    _,s,task,ledger,_=repair_case;req=report(repair_case);m=repair_manager
    m.pause('before repair')
    with pytest.raises(ValueError):launch_repair(m,m.processing.config,req['repair_request_id'])
    assert ledger.get(req['repair_request_id'])['state']=='queued'
    assert s.get(task['task_id'])['stage']=='handed_off'
