import json
import time

import pytest

from test_rating_executor import endpoints,inputs,runner,save


def rows(instance):
    with instance.connect() as conn:return conn.execute('SELECT * FROM jobs ORDER BY article_id').fetchall()


def test_exact_capacity_is_parked_without_halt_or_automatic_resend(tmp_path):
    with endpoints('capacity') as (url,state):
        registry,manifest=inputs(tmp_path,url)
        instance=runner(tmp_path,registry,manifest,url)
        result=instance.run()
        assert result['counts']=={'deferred_not_dispatched':1}
        assert result['dispatch_halt'] is None
        work=instance.root/'requests'/rows(instance)[0][4]
        receipt=json.loads((work/'http-receipt.json').read_text())
        assert receipt['header_pairs']
        proof=json.loads((work/'admission-proof.json').read_text())
        assert proof['http_status']==429 and proof['header_provenance']=='raw_pairs'
        state['mode']='complete'
        assert instance.run()['counts']=={'deferred_not_dispatched':1}
        assert state['model_calls']==1 and state['write_calls']==0


@pytest.mark.parametrize('mode',['ordinary429','duplicate_capacity','protected','partial'])
def test_incomplete_or_ambiguous_outcomes_remain_unknown(tmp_path,mode):
    with endpoints(mode) as (url,state):
        registry,manifest=inputs(tmp_path,url)
        result=runner(tmp_path,registry,manifest,url).run()
        assert result['counts']=={'uncertain':1}
        assert result['dispatch_halt'] is not None
        assert state['model_calls']==1 and state['write_calls']==0


def old_unknown(instance):
    row=rows(instance)[0]
    instance._set(row[0],'uncertain','old generic HTTPError')
    instance._halt(row[0],'old generic outcome halt')
    return row


def test_reconcile_prepares_then_applies_without_dispatch_or_gate_change(tmp_path):
    with endpoints('capacity') as (url,state):
        registry,manifest=inputs(tmp_path,url)
        instance=runner(tmp_path,registry,manifest,url);instance.run()
        row=old_unknown(instance)
        config=json.loads(registry.read_text());config['dispatch_gate']['allow_new_scoring_requests']=False;save(registry,config)
        halt=(instance.root/'dispatch-halt.json').read_bytes()
        plan=instance.reconcile_admission()
        assert rows(instance)[0][6]=='uncertain'
        applied=instance.reconcile_admission(apply_plan=plan['plan_path'])
        assert applied['applied'] and rows(instance)[0][6]=='deferred_not_dispatched'
        assert instance.status()['dispatch_halt'] is None
        assert (instance.root/'dispatch-halt.json').read_bytes()==halt
        assert state['model_calls']==1 and state['write_calls']==0
        assert json.loads(registry.read_text())['dispatch_gate']['allow_new_scoring_requests'] is False
        assert json.loads((instance.root/'status.json').read_text())['dispatch_blocked'] is True


@pytest.mark.parametrize('tamper',['receipt','new_unknown','halt','empty'])
def test_apply_revalidates_every_proof_and_halt_atomically(tmp_path,tamper):
    with endpoints('capacity') as (url,state):
        registry,manifest=inputs(tmp_path,url,count=2)
        instance=runner(tmp_path,registry,manifest,url);instance.run()
        row=old_unknown(instance);plan=instance.reconcile_admission()
        if tamper=='receipt':
            path=instance.root/'requests'/row[4]/'http-error.txt';path.write_bytes(b'{}')
        elif tamper=='new_unknown':instance._set('article-1','uncertain','no terminal proof')
        elif tamper=='halt':
            with instance.connect() as conn:conn.execute("UPDATE metadata SET value='{}' WHERE key='dispatch_halt'")
        else:
            changed=json.loads(open(plan['plan_path'],encoding='utf-8').read());changed['proofs']=[];save(__import__('pathlib').Path(plan['plan_path']),changed)
        with pytest.raises(ValueError):instance.reconcile_admission(apply_plan=plan['plan_path'])
        assert rows(instance)[0][6]=='uncertain'
        assert instance.status()['dispatch_halt'] is not None
        assert state['model_calls']==1 and state['write_calls']==0


def test_legacy_dict_headers_require_explicit_source_attestation(tmp_path):
    from rating_admission import verify_not_dispatched
    with endpoints('capacity') as (url,state):
        registry,manifest=inputs(tmp_path,url);instance=runner(tmp_path,registry,manifest,url);instance.run()
        row=rows(instance)[0];work=instance.root/'requests'/row[4]
        receipt=json.loads((work/'http-receipt.json').read_text());receipt.pop('header_pairs');save(work/'http-receipt.json',receipt)
        with pytest.raises(ValueError,match='原始响应头'):
            verify_not_dispatched(instance.root,row,instance.fixed,instance.run_id)


def test_runtime_limits_do_not_change_fixed_binding_and_bound_submission(tmp_path,monkeypatch):
    from rating_executor import RatingRunner
    with endpoints() as (url,state):
        registry,manifest=inputs(tmp_path,url,count=6)
        first=runner(tmp_path,registry,manifest,url);first.run()
        instance=RatingRunner(registry,[manifest],tmp_path/'run',api_url=url,max_workers=1,per_category_workers=1)
        calls=[];original=instance._work
        def close_after_one(row):
            calls.append(row[0]);original(row)
            value=json.loads(registry.read_text());value['dispatch_gate']['allow_new_scoring_requests']=False;save(registry,value)
        monkeypatch.setattr(instance,'_work',close_after_one)
        result=instance.run(acceptance_only=False)
        assert result['counts']=={'prepared':4,'written':2}
        assert calls==['article-1'] and state['model_calls']==2 and state['peak']==1
        assert instance.fixed.signature==first.fixed.signature
        assert result['runtime_limits']=={'max_workers':1,'per_category_workers':1}


def test_waiting_send_permit_observes_gate_close_without_holding_lock(tmp_path):
    import threading
    from rating_executor import DispatchBlocked
    with endpoints() as (url,state):
        registry,manifest=inputs(tmp_path,url);instance=runner(tmp_path,registry,manifest,url)
        with instance.connect() as conn:conn.execute("INSERT INTO metadata VALUES('admission_not_before',?)",(str(time.time()+10),))
        def close():
            time.sleep(.1)
            with instance.dispatch_lock:
                value=json.loads(registry.read_text());value['dispatch_gate']['allow_new_scoring_requests']=False;save(registry,value)
        task=threading.Thread(target=close);task.start();started=time.monotonic()
        with pytest.raises(DispatchBlocked):instance._permit()
        task.join();assert time.monotonic()-started<1.5
        assert state['model_calls']==0


@pytest.mark.parametrize('file,key,value',[
    ('binding.json','endpoint','http://127.0.0.1/wrong'),
    ('binding.json','model','wrong-model'),
    ('request-state.json','client_http_requests',2),
    ('request-state.json','error_response_sha256','0'*64),
])
def test_native_proof_cannot_be_detached_from_its_binding(tmp_path,file,key,value):
    from rating_admission import verify_not_dispatched
    with endpoints('capacity') as (url,state):
        registry,manifest=inputs(tmp_path,url);instance=runner(tmp_path,registry,manifest,url);instance.run()
        row=rows(instance)[0];path=instance.root/'requests'/row[4]/file
        changed=json.loads(path.read_text());changed[key]=value;save(path,changed)
        with pytest.raises(ValueError):verify_not_dispatched(instance.root,row,instance.fixed,instance.run_id)


def test_reconciliation_requires_exclusive_executor_ownership(tmp_path):
    from filelock import FileLock,Timeout
    with endpoints('capacity') as (url,state):
        registry,manifest=inputs(tmp_path,url);instance=runner(tmp_path,registry,manifest,url);instance.run();old_unknown(instance)
        with FileLock(str(instance.root/'executor.lock')):
            with pytest.raises(Timeout):instance.reconcile_admission()
        assert rows(instance)[0][6]=='uncertain' and state['model_calls']==1


def test_http_date_proof_is_stable_between_prepare_and_apply(tmp_path):
    from email.utils import formatdate
    with endpoints('capacity') as (url,state):
        registry,manifest=inputs(tmp_path,url);instance=runner(tmp_path,registry,manifest,url);instance.run();row=old_unknown(instance)
        path=instance.root/'requests'/row[4]/'http-receipt.json';receipt=json.loads(path.read_text())
        receipt['header_pairs']=[[key,formatdate(time.time()+10,usegmt=True) if key.lower()=='retry-after' else value] for key,value in receipt['header_pairs']]
        save(path,receipt);plan=instance.reconcile_admission();time.sleep(.02)
        assert instance.reconcile_admission(apply_plan=plan['plan_path'])['applied']


def test_long_retry_after_is_not_shortened_to_thirty_seconds():
    from rating_admission import retry_delay
    assert retry_delay([['Retry-After','120']],time.time())==120
