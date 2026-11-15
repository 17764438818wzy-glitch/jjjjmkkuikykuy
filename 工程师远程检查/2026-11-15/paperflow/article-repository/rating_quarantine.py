"""One evidence-bound unknown exception in the existing executor journal."""
import hashlib,json,sqlite3,time
from datetime import datetime,timezone
from pathlib import Path

from filelock import FileLock
from article_repository.ratings import canonical

SCHEMA='rating-single-unknown-quarantine/1'
NATIVE_FILES={'binding.json','claim-intent.json','claim-receipt.json','claim-send-permit.json',
    'http-error.txt','http-receipt.json','request-state.json','request.json'}
BINDINGS=('run_id','binding','api_url','transport_sha256','native_claim_protocol',
    'classification_binding','classification_rules_sha256')


def read(path):return json.loads(Path(path).read_text(encoding='utf-8-sig'))
def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _decision(path,expected):
    if sha(path)!=expected:raise ValueError('用户决定文件哈希不符')
    value=read(path)
    if (value.get('schema')!='rating-unknown-quarantine-user-decision/1'
        or value.get('source')!='direct_human_async_question_reply'
        or value.get('human_answer')!='隔离这1条，继续其余文章'
        or not value.get('source_thread_id')
        or any(value.get(k) is not True for k in ('preserve_original_unknown_and_claim','preserve_original_halt_evidence'))
        or any(value.get(k) is not False for k in ('resend_excluded_request_authorized','change_key_route_rubric_authorized','create_second_executor_authorized'))):
        raise ValueError('缺少精确的单条隔离用户决定')
    return value


def _snapshot(conn,root,registry,classifier,api_url,decision):
    from rating_executor import FixedChannels,DESKTOP
    meta=dict(conn.execute('SELECT key,value FROM metadata'))
    if any(k not in meta for k in BINDINGS):raise ValueError('原run执行绑定不完整')
    if (meta['run_id']!=decision['run_id'] or meta['api_url']!=api_url
        or meta['binding']!=FixedChannels.binding(read(registry))
        or meta['classification_binding']!=FixedChannels.binding(read(classifier))
        or meta['transport_sha256']!=sha(DESKTOP/'preextract_request.py')):
        raise ValueError('原run或固定执行绑定漂移')
    aid=decision['excluded_article_id']
    row=conn.execute('SELECT * FROM jobs WHERE article_id=?',(aid,)).fetchone()
    if not row or (row[4],row[5],row[6])!=(decision['excluded_request_id'],decision['excluded_operation_id'],'uncertain'):
        raise ValueError('原未知job身份或状态不符')
    unknown=[]
    for table in ('jobs','classification_jobs'):
        if conn.execute('SELECT 1 FROM sqlite_master WHERE name=?',(table,)).fetchone():
            unknown.extend((table,*r) for r in conn.execute('SELECT article_id,request_id,operation_id,status FROM '+table+" WHERE status IN ('uncertain','write_unknown')"))
    if unknown!=[('jobs',aid,row[4],row[5],'uncertain')]:raise ValueError('出现额外未知结果，保持全局停派')
    halt=meta.get('dispatch_halt')
    if not halt or json.loads(halt).get('article_id')!=aid:raise ValueError('原持久暂停不符')
    work=root/'requests'/row[4]
    if {p.name for p in work.iterdir()}!=NATIVE_FILES or any(not p.is_file() or p.is_symlink() for p in work.iterdir()):
        raise ValueError('原8文件集合漂移')
    return {'binding':{k:meta[k] for k in BINDINGS},'row':list(row),
        'native_files':{name:sha(work/name) for name in sorted(NATIVE_FILES)},
        'halt_metadata':halt,'halt_file_sha256':sha(root/'dispatch-halt.json'),
        'halt_events':[list(r) for r in conn.execute("SELECT id,payload,created_at FROM control_events WHERE kind='dispatch_halt' ORDER BY id")]}


def _check(certificate,conn,registry,root,classifier,api_url):
    expected={'schema','created_at','decision_path','decision_sha256','decision','registry_path',
        'classification_registry','run_root','api_url','snapshot','runtime_limits'}
    if not isinstance(certificate,dict) or set(certificate)!=expected or certificate['schema']!=SCHEMA:
        raise ValueError('隔离授权schema无效')
    if (certificate['run_root']!=str(root) or certificate['registry_path']!=str(registry)
        or certificate['classification_registry']!=str(classifier) or certificate['api_url']!=api_url
        or certificate['runtime_limits']!={'max_workers':6,'per_category_workers':1,'classification_workers':1}):
        raise ValueError('隔离授权运行范围不符')
    decision=_decision(certificate['decision_path'],certificate['decision_sha256'])
    if decision!=certificate['decision']:raise ValueError('用户决定内容漂移')
    if _snapshot(conn,root,registry,classifier,api_url,decision)!=certificate['snapshot']:
        raise ValueError('原job、8文件、暂停或新halt事件发生变化')


def prepare_certificate(registry,run_root,classifier,decision_path,expected_decision_sha256,*,api_url):
    registry=Path(registry).resolve();root=Path(run_root).resolve();classifier=Path(classifier).resolve()
    decision_path=Path(decision_path).resolve();decision=_decision(decision_path,expected_decision_sha256)
    with sqlite3.connect('file:'+(root/'journal.sqlite3').as_posix()+'?mode=ro',uri=True) as conn:
        snapshot=_snapshot(conn,root,registry,classifier,api_url,decision)
    return {'schema':SCHEMA,'created_at':datetime.now(timezone.utc).isoformat(),
        'decision_path':str(decision_path),'decision_sha256':expected_decision_sha256,'decision':decision,
        'registry_path':str(registry),'classification_registry':str(classifier),'run_root':str(root),
        'api_url':api_url,'snapshot':snapshot,'runtime_limits':{'max_workers':6,'per_category_workers':1,'classification_workers':1}}


def _activation(conn):
    row=conn.execute("SELECT value FROM metadata WHERE key='unknown_quarantine'").fetchone()
    if not row:
        if conn.execute("SELECT 1 FROM control_events WHERE kind='unknown_quarantine_activated' LIMIT 1").fetchone():
            raise ValueError('隔离控制记录缺失，原激活审计仍存在')
        return None
    value=json.loads(row[0])
    if set(value)!={'certificate_path','certificate_sha256','article_id','control_event_id'}:raise ValueError('隔离控制记录无效')
    event=conn.execute('SELECT kind,payload FROM control_events WHERE id=?',(value['control_event_id'],)).fetchone()
    if not event or event[0]!='unknown_quarantine_activated' or event[1]!=canonical({k:v for k,v in value.items() if k!='control_event_id'}):
        raise ValueError('隔离激活审计不符')
    return value


def activate_certificate(registry,run_root,classifier,decision_path,expected_decision_sha256,certificate_path,expected_certificate_sha256,*,api_url):
    registry=Path(registry).resolve();root=Path(run_root).resolve();classifier=Path(classifier).resolve()
    path=Path(certificate_path).resolve()
    if sha(path)!=expected_certificate_sha256:raise ValueError('隔离证书哈希不符')
    certificate=read(path)
    if certificate['decision_path']!=str(Path(decision_path).resolve()) or certificate['decision_sha256']!=expected_decision_sha256:
        raise ValueError('本次明确用户授权不符')
    with FileLock(str(root/'executor.lock'),timeout=0),sqlite3.connect(root/'journal.sqlite3',timeout=30) as conn:
        conn.execute('BEGIN IMMEDIATE')
        for gate in (registry,classifier):
            if read(gate).get('dispatch_gate',{}).get('allow_new_scoring_requests') is not False:
                raise ValueError('激活前必须关闭两个派单gate')
        _check(certificate,conn,registry,root,classifier,api_url)
        for table in ('jobs','classification_jobs'):
            if conn.execute('SELECT 1 FROM sqlite_master WHERE name=?',(table,)).fetchone():
                if conn.execute('SELECT 1 FROM '+table+" WHERE status IN ('inflight','write_pending','write_unknown') LIMIT 1").fetchone():
                    raise ValueError('激活前必须排空其他在途或未知写回')
        value=_activation(conn)
        if value:
            if (value.get('certificate_path'),value.get('certificate_sha256'))!=(str(path),expected_certificate_sha256):
                raise ValueError('已存在其他隔离授权，不能扩大或替换')
            return {'status':'already_activated',**value}
        value={'certificate_path':str(path),'certificate_sha256':expected_certificate_sha256,
            'article_id':certificate['decision']['excluded_article_id']}
        event=conn.execute('INSERT INTO control_events(kind,payload,created_at) VALUES(?,?,?)',
            ('unknown_quarantine_activated',canonical(value),time.time())).lastrowid
        value['control_event_id']=event
        conn.execute("INSERT INTO metadata VALUES('unknown_quarantine',?)",(canonical(value),))
        return {'status':'activated','original_unknown_preserved':True,'original_halt_preserved':True,**value}


def quarantine_scope(runner,conn):
    """Return the one validated exclusion, or preserve the original global stop."""
    from rating_executor import DispatchBlocked
    try:
        value=_activation(conn)
        if value is None:return None
        path=Path(value['certificate_path'])
        if sha(path)!=value['certificate_sha256']:raise ValueError('隔离证书缺失或漂移')
        certificate=read(path)
        _check(certificate,conn,runner.fixed.path,runner.root,Path(certificate['classification_registry']),runner.api_url)
        if runner.run_id!=certificate['decision']['run_id'] or value['article_id']!=certificate['decision']['excluded_article_id']:
            raise ValueError('隔离run或文章范围不符')
        if not 1<=runner.max_workers<=6 or runner.per_category_workers!=1:raise ValueError('隔离接续限总6、每类1')
        return {'article_id':value['article_id'],'request_id':certificate['decision']['excluded_request_id'],
            'operation_id':certificate['decision']['excluded_operation_id'],'certificate_sha256':value['certificate_sha256']}
    except Exception as exc:
        raise DispatchBlocked('隔离授权失效，保持全局停派：'+str(exc)) from exc
