import hashlib
import json
import time
from pathlib import Path
from types import SimpleNamespace

import pytest
from paperflow.config import Config
from paperflow.package import save
from paperflow.managed import validate_scope
from paperflow.delivery_control import ControlledRagClient
from test_managed_entry import entry_case


def authorize(case):
    root,old,source,config=case
    config.rag_url='http://192.168.1.120:8088'
    config.production_pilot=str(root/'pilot-scope.json')
    report=json.loads(Path(config.delivery_acceptance_report).read_text())
    report['endpoint']=config.rag_url;save(config.delivery_acceptance_report,report)
    save(config.production_pilot,dict(mode='production-pilot',endpoint=config.rag_url,root=str(root),
        expires_at=time.time()+3600,maximum_seconds=3600,catalog_sha256=hashlib.sha256(Path(config.catalog_path).read_bytes()).hexdigest(),
        sources=[dict(path=str(source.resolve()),sha256=hashlib.sha256(source.read_bytes()).hexdigest())]))
    return root,old,source,config


def test_formal_gate_requires_exact_selected_sources_and_catalog(entry_case):
    root,old,source,config=authorize(entry_case)
    assert validate_scope(config,root,[source],old)[0]==root
    extra=root/'extra.txt';extra.write_text('not selected')
    with pytest.raises(ValueError,match='selection'):validate_scope(config,root,[extra],old)
    source.write_text('changed bytes')
    with pytest.raises(ValueError,match='selection'):validate_scope(config,root,[source],old)


def test_formal_gate_rejects_stale_or_wrong_scope(entry_case):
    root,old,source,config=authorize(entry_case)
    scope=json.loads(Path(config.production_pilot).read_text());scope['expires_at']=time.time()-1
    save(config.production_pilot,scope)
    with pytest.raises(ValueError,match='expired'):validate_scope(config,root,[source],old)
    scope['expires_at']=time.time()+1000;scope['catalog_sha256']='bad';save(config.production_pilot,scope)
    with pytest.raises(ValueError,match='catalog'):validate_scope(config,root,[source],old)


def test_production_identity_uses_authenticated_request_under_action_gate():
    calls=[]
    class Native:
        base_url='http://192.168.1.120:8088'
        def request(self,*args):calls.append(args);return {'nonce':'fresh'}
    class Actions:
        identity={'manifest_sha256':'m'}
        config=SimpleNamespace(production_pilot='explicit-scope.json')
        def run(self,kind,resource,call,**kw):
            assert kind=='rag_read' and resource['endpoint']=='/paperflow/deployment'
            return call(None)
    assert ControlledRagClient(Native(),Actions()).deployment_identity()=={'nonce':'fresh'}
    assert len(calls)==1 and calls[0][:2]==('GET','/paperflow/deployment')


def test_formal_original_identity_and_unknown_legacy_budget_survive_restart(entry_case):
    from paperflow.managed import run_managed
    from paperflow.store import Store
    from paperflow.repair_store import RepairLedger
    root,old,source,config=authorize(entry_case)
    scope=json.loads(Path(config.production_pilot).read_text())
    scope['sources'][0].update(legacy_budget_unknown=True,legacy_ids=['old-native-paper'])
    save(config.production_pilot,scope)
    for _ in range(2):
        result=run_managed(config,root,[source],legacy_root=old,seconds=1)
        store=Store(config.root/'state.sqlite3');task=store.get(result['tasks'][0]['task_id'])
        assert task['source_id']=='src_'+hashlib.sha256(str(source.resolve()).casefold().encode()).hexdigest()[:24]
        assert task['source_path']!=str(source) and task['original_source_path']==str(source.resolve())
        assert task['legacy_budget_unknown'] and task['correction_count']==2
        assert RepairLedger(store).used(task)==2
        with store.db() as db:assert db.execute('SELECT COUNT(*) FROM correction_attempts').fetchone()[0]==0
