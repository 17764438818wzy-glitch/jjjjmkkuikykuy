"""The operator confirmed 192 processing + 1 packaging + 2 delivery slots."""
import json
from pathlib import Path
import sys
import time

import pytest

from paperflow.control import Manager
from paperflow.package import save
from paperflow.scheduler import Scheduler
from paperflow.scheduling_policy import ActionDeferred
from paperflow.store import Store
from test_managed_entry import entry_case
from test_scheduling_resources import worker
from test_winjob import wait_until


def test_two_delivery_slots_and_refill_keep_packaging_serial_and_pause(entry_case):
    root,_,_,c=entry_case
    c.article_workers=c.llm_workers=192;c.rag_workers=2;c.local_workers=1;c.max_live_units=195
    c.prepare();configured=Manager(Store(c.root/'state.sqlite3'),root/'manager').start()
    try:
        Scheduler(configured,c,isolation_root=root)
        limits=dict(configured.resource_limits);units=dict(configured.unit_limits)
        assert units==dict(graph=192,delivery=2,cleanup=1) and configured.max_live_units==195
    finally:configured.shutdown()
    # Probe the configured generic Job/IPC lanes with marker-only actions. Real
    # delivery identity/commit checks stay covered by the delivery suites.
    m=Manager(Store(root/'probe.sqlite3'),root/'probe-manager',resource_limits=limits,max_live_units=195).start()
    m.unit_limits=units;m.wait_resources=True
    started=time.monotonic()
    try:
        m.resume()
        assert m.unit_limits==dict(graph=192,delivery=2,cleanup=1)
        first,a=worker(m,root,'delivery-a','rag_write');second,b=worker(m,root,'delivery-b','rag_write')
        (a/'go').touch();(b/'go').touch();wait_until(lambda:(a/'performed').exists() and (b/'performed').exists())
        assert m.status()['resources']['rag']['used']==2
        with pytest.raises(ActionDeferred,match='unit_capacity'):
            m.launch('delivery','third',[sys.executable,'-c','pass'],cwd=Path.cwd())
        assert m.status()['resources']['local']['limit']==1
        (a/'release').touch();wait_until(lambda:first.exit_saved)
        third,d=worker(m,root,'delivery-c','rag_write');(d/'go').touch();wait_until(lambda:(d/'performed').exists())
        assert not second.exit_saved and m.status()['resources']['rag']['used']==2
        paused=time.monotonic();m.pause('two-delivery isolated acceptance')
        assert time.monotonic()-paused<.5 and not m.status()['admission_open']
        # Issued remote writes retain their existing 30-second finish window;
        # simulate successful in-flight completion instead of expecting cancel.
        (b/'release').touch();(d/'release').touch()
        wait_until(lambda:not m.status()['live_units'],seconds=10)
        assert m.stopped.is_set() and all(e.exit_saved for e in m.executions.values())
        save(Path('evidence/two-deliveries.json'),dict(passed=True,kind='synthetic-actions-real-Job-IPC',
            article_workers=192,packaging=1,rag=2,max_live_units=195,delivery_peak=2,third_rejected_at_capacity=True,
            refill=True,formal_network_calls=0,seconds=time.monotonic()-started,final=m.status()))
    finally:m.shutdown()
