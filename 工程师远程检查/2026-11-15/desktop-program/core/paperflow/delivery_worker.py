"""Explicit single-specialty Job entry; never starts a scheduler or a graph."""
import argparse
from pathlib import Path
import sys
import uuid

from .config import Config
from .control_ipc import WorkerClient
from .delivery import DeliveryWorkflow
from .delivery_control import DeliveryActions,DeliveryCommits,ControlledRagClient
from .delivery_store import DeliveryLedger,target_key
from .handoff_store import canonical
from .package import load,save
from .rag import native_client
from .store import Store
from .worker_retry import short_attempts,report_failure
from .scheduling_policy import ActionDeferred
from .instrumentation import safe_span,using,diagnostic,best_effort
from .timing_artifacts import configure_timing


def launch_delivery(manager,config,manifest_sha256,specialty_code,*,total_timeout=900,schedule_attempt=None):
    if not config.delivery_acceptance_report:raise ValueError('deployment acceptance required before launch')
    report=load(config.delivery_acceptance_report)
    if report.get('passed') is not True or report.get('endpoint')!=config.rag_url:raise ValueError('deployment acceptance endpoint mismatch')
    _controller(manager,config)
    return _launch(manager,config,manifest_sha256,specialty_code,total_timeout=total_timeout,schedule_attempt=schedule_attempt)


def _controller(manager,config):
    if manager.delivery is None:DeliveryCommits(manager,config)
    elif manager.delivery.config!=config:raise ValueError('controller delivery configuration mismatch')


def launch_legacy_readback(manager,config,manifest_sha256,specialty_code,*,total_timeout=600):
    guard=getattr(manager,'transition_guard',None)
    if not guard:raise ValueError('legacy readback requires held transition locks')
    guard.check()
    _controller(manager,config)
    ledger=manager.delivery.ledger
    with manager.store.db() as c:
        c.execute('BEGIN')
        package=c.execute('SELECT identity_json FROM delivery_packages WHERE manifest_sha256=?',(manifest_sha256,)).fetchone()
        if not package:raise ValueError('registered legacy package required')
        import json
        source=json.loads(package[0])['source_id']
        current=c.execute('SELECT v.manifest_sha256 FROM sources s JOIN target_versions v ON v.task_id=s.task_id '
                          'WHERE s.source_id=? AND v.specialty_code=?',(source,specialty_code)).fetchall()
        if len(current)!=1:raise ValueError('one current specialty owner required')
        ctx=ledger.context_sql(c,current[0][0],specialty_code)
        from .legacy_readback import subject_sql
        subject_sql(ledger,c,ctx['identity'],manifest_sha256)
    return _launch(manager,config,ctx['identity']['manifest_sha256'],specialty_code,
                   total_timeout=total_timeout,legacy_readback=manifest_sha256)


def launch_active_readback(manager,config,expected_manifest,specialty_code,*,total_timeout=300):
    _controller(manager,config)
    return _launch(manager,config,expected_manifest,specialty_code,total_timeout=total_timeout,audit_active=True)


def launch_late_report(manager,config,source_manifest,specialty_code,stage,problem_type,evidence,*,total_timeout=120):
    """Explicit local evidence transfer under a fresh, current specialty owner.

    No deployment acceptance, RAG client, model, scheduler or old-owner revival.
    An unavailable target or pause leaves the caller's evidence unconsumed.
    """
    with manager.gate_lock:
        if manager.stopped.is_set() or manager.closing:raise ValueError('report launch paused')
    _controller(manager,config)
    ledger=manager.delivery.ledger
    source=ledger.registration(source_manifest)
    if not source or source['lifecycle']!='retained':raise ValueError('retained report source required')
    with manager.store.db(timeout=.1) as c:
        c.execute('BEGIN')
        expected=ledger._expectation(c,source['task_id'],specialty_code)
        ctx=ledger.context_sql(c,expected['manifest_sha256'],specialty_code)
        ledger.report_source_sql(c,ctx['identity'],source_manifest)
    packet=manager.root/'delivery-reports'/(uuid.uuid4().hex+'.json')
    save(packet,dict(source_manifest=source_manifest,stage=stage,problem_type=problem_type,evidence=evidence))
    return _launch(manager,config,ctx['identity']['manifest_sha256'],specialty_code,
                   total_timeout=total_timeout,report_path=packet)


def _launch(manager,config,manifest_sha256,specialty_code,*,total_timeout,report_path=None,schedule_attempt=None,legacy_readback=False,audit_active=False):
    ctx=manager.delivery.ledger.context(manifest_sha256,specialty_code);identity=ctx['identity']
    with manager.store.db(timeout=.1) as c:
        safety=manager.delivery.ledger.legacy_safety_allowed_sql(c,identity)
        manager.delivery.ledger.check_identity(c,identity,allow_frozen=True,allow_legacy=bool(legacy_readback or safety))
    key=target_key(identity);scope='unit:'+canonical(['target',key])
    with manager.gate_lock:
        if any(ex.scope==scope and (not ex.exit_saved or ex.execution_id in manager.publications) for ex in manager.executions.values()):
            raise ActionDeferred('specialty already owned')
    path=manager.root/'delivery-configs'/(uuid.uuid4().hex+'.json')
    path.parent.mkdir(parents=True,exist_ok=True);config.save(path)
    argv=[sys.executable,'-X','utf8','-B','-m','paperflow.delivery_worker',
        '--config',str(path),'--ledger',str(manager.store.path),'--manifest',manifest_sha256,'--specialty',specialty_code]
    if report_path:argv.extend(['--late-report',str(report_path)])
    if legacy_readback:argv.extend(['--legacy-readback',legacy_readback])
    if audit_active:argv.append('--audit-active')
    return manager.launch('delivery',key,argv,
        cwd=Path(__file__).parent.parent,total_timeout=total_timeout,schedule_attempt=schedule_attempt,
        run_id='legacy-readback:'+legacy_readback if legacy_readback else None,
        expected_generation=identity['generation'])


def main():
    parser=argparse.ArgumentParser()
    for name in ('config','ledger','manifest','specialty'):parser.add_argument('--'+name,required=True)
    parser.add_argument('--late-report')
    parser.add_argument('--legacy-readback')
    parser.add_argument('--audit-active',action='store_true')
    args=parser.parse_args();client=WorkerClient.from_environment();config=Config.load(args.config)
    store=Store(args.ledger);ledger=DeliveryLedger(store)
    identity=ledger.context(args.manifest,args.specialty)['identity']
    configure_timing(client,config.root,identity['task_id'],execution_kind='delivery',
        **{key:identity[key] for key in ('source_id','source_sha256','manifest_sha256','specialty_code') if key in identity})
    result_path=config.root/'delivery-results'/(client.execution_id+'.json')
    result_path.parent.mkdir(parents=True,exist_ok=True)
    try:
        actions=DeliveryActions(client,config,identity)
        if args.audit_active:
            from .legacy_readback import audit_active_version
            result=audit_active_version(config,ledger,actions,ControlledRagClient(native_client(config),actions),identity)
            save(result_path,dict(identity=identity,**result))
            client.report_execution('complete',dict(state=result['state']))
            return 0
        if args.legacy_readback:
            from .legacy_readback import readback
            result=readback(config,ledger,actions,ControlledRagClient(native_client(config),actions),identity,source_manifest=args.legacy_readback)
            save(result_path,dict(identity=identity,**result))
            client.report_execution('complete' if result['state'] in ('active','reconciled') else 'blocked',dict(state=result['state']))
            return 0
        if args.late_report:
            packet=load(args.late_report)
            if set(packet)!={'source_manifest','stage','problem_type','evidence'}:raise ValueError('invalid content report')
            result=actions.commit('late_repair_report',packet)
            save(result_path,dict(identity=identity,state='reported',source_manifest=packet['source_manifest'],report=result))
            return 0
        flow=DeliveryWorkflow(config,ledger,actions,ControlledRagClient(native_client(config),actions),identity)
        with using(client.metrics),safe_span(client.metrics,'delivery_attempt'):
            result=short_attempts(client,flow.run,interval=config.short_retry_interval)
        save(result_path,dict(identity=identity,**result,_timing=best_effort(client.metrics.summary)))
        client.report_execution('complete' if result['state']=='active' else 'blocked' if result['state'] in ('blocked','frozen_pending') else 'held',
                                dict(state=result['state']))
        return 0 if result['state']=='active' else 2
    except Exception as exc:
        # Diagnostic file is a fact only; progress and active publication still require manager qualification.
        save(result_path,dict(identity=identity,state='stopped' if client.stopped.is_set() else 'failed',
            error=type(exc).__name__,diagnostic=diagnostic(exc),_timing=best_effort(client.metrics.summary)))
        report_failure(client,exc)
        return 1
    finally:client.close()


if __name__=='__main__':raise SystemExit(main())
