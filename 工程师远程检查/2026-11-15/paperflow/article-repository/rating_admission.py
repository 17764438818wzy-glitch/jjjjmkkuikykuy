"""Fixed-channel adapter for the existing native non-dispatch proof contract."""
from datetime import datetime,timezone
from email.utils import parsedate_to_datetime
import hashlib
import importlib.util
import json
import math
import uuid
from pathlib import Path

NATIVE=Path('F:/论文预处理系统/预处理系统2/candidates/parent-child-v3-20261102/paperflow_v3/stages/gateway_admission.py')
_spec=importlib.util.spec_from_file_location('_rating_native_admission',NATIVE)
native=importlib.util.module_from_spec(_spec);_spec.loader.exec_module(native)


def digest(raw):return hashlib.sha256(raw).hexdigest()


def file_sha(path):return digest(Path(path).read_bytes())


def retry_delay(headers,reference_time):
    """Use the native bounded waiting policy, without its Prefect proof layout."""
    hints=native._values(headers,'retry-after');seconds=1.5
    if len(hints)==1:
        try:
            text=hints[0].strip()
            parsed=float(text) if text.isascii() and text.replace('.','',1).isdigit() else parsedate_to_datetime(text).timestamp()-reference_time
            if math.isfinite(parsed):seconds=max(1.5,parsed)
        except (TypeError,ValueError,OverflowError):pass
    return seconds


def verify_not_dispatched(root,row,fixed,run_id,legacy_attestation=None):
    """Read original bytes only. This proof never authorizes a model resend."""
    aid,category,input_digest,_,request_id,operation_id,_,_,_=row
    work=Path(root)/'requests'/request_id;channel=fixed.channels[category]
    state=native._json((work/'request-state.json').read_bytes())
    receipt=native._json((work/'http-receipt.json').read_bytes())
    binding=native._json((work/'binding.json').read_bytes())
    raw=(work/'http-error.txt').read_bytes();error=native._json(raw)
    stored=(work/'request.json').read_text(encoding='utf-8')
    request=native._json(stored)
    wire=json.dumps(request,ensure_ascii=False,separators=(',',':')).encode()
    events=work/'response-events.jsonl'
    item=native._json(row[3])
    canonical_item=json.dumps(item,ensure_ascii=False,sort_keys=True,separators=(',',':'))
    expected_request=str(uuid.uuid5(uuid.NAMESPACE_URL,run_id+':'+aid+':'+input_digest))
    suffix=':classification-write' if item.get('stage')=='classification' else ':rating-write'
    if (digest(canonical_item.encode())!=input_digest or expected_request!=request_id
        or str(uuid.uuid5(uuid.NAMESPACE_URL,request_id+suffix))!=operation_id
        or item.get('article_id')!=aid or item.get('research_type')!=category):
        raise ValueError('原job输入与请求/写回身份链不符')
    if (state.get('status')!='request_failed_or_uncertain' or state.get('http_status')!=429
        or receipt.get('status')!=429 or state.get('error_type')!='HTTPError'
        or state.get('client_http_requests')!=1 or state.get('error_response_sha256')!=digest(raw)
        or state.get('stored_request_sha256')!=digest(stored.encode()) or state.get('request_sha256')!=digest(wire)
        or state.get('model')!=channel['model'] or request.get('model')!=channel['model']
        or binding.get('model')!=channel['model'] or binding.get('endpoint')!=channel['endpoint']
        or binding.get('research_type')!=category or binding.get('key_id')!=channel['key_id']
        or binding.get('key_fingerprint_sha256_16')!=channel['key_fingerprint_sha256_16']
        or (work/'response.txt').exists() or (events.exists() and events.stat().st_size)
        or not isinstance(error,dict) or not isinstance(error.get('error'),dict)
        or error['error'].get('code')!=native.CODE):
        raise ValueError('原生未派发证明、请求哈希或固定渠道绑定不一致')
    headers=receipt.get('header_pairs');provenance='raw_pairs';attestation_sha=None
    if not isinstance(headers,list):
        if not legacy_attestation:raise ValueError('既存dict回执缺少原始响应头，须提供限定旧请求的来源审计')
        attestation=Path(legacy_attestation);source=native._json(attestation.read_bytes())
        code_path=Path(source.get('source_path',''))
        if (source.get('schema')!='rating-legacy-header-source/1.0'
            or source.get('approved_for_legacy_header_dict') is not True
            or source.get('receipts',{}).get(request_id)!=file_sha(work/'http-receipt.json')
            or not code_path.is_file() or file_sha(code_path)!=source.get('source_sha256')):
            raise ValueError('旧响应头来源审计未批准、身份不符或已改变')
        code=code_path.read_text(encoding='utf-8-sig')
        if native.MARKER not in code or native.CODE not in code:raise ValueError('来源代码缺少原生未派发协议')
        headers=receipt.get('headers');provenance='legacy_header_dict_with_explicit_source_attestation';attestation_sha=file_sha(attestation)
    if not native._signal_headers(headers):raise ValueError('未派发标记或JSON媒体头缺失、重复或冲突')
    names=['request-state.json','http-receipt.json','binding.json','http-error.txt','request.json','response-events.jsonl','response.txt']
    return {'article_id':aid,'research_type':category,'input_digest':input_digest,'request_id':request_id,
        'operation_id':operation_id,'run_id':run_id,'work_dir':str(work.resolve()),'endpoint':channel['endpoint'],
        'model':channel['model'],'fixed_binding_sha256':fixed.signature,'http_status':429,
        'request_sha256':state['request_sha256'],'error_response_sha256':digest(raw),
        'files':{name:file_sha(work/name) if (work/name).is_file() else None for name in names},
        'header_provenance':provenance,'legacy_attestation_sha256':attestation_sha,
        'native_helper_sha256':file_sha(NATIVE),'adapter_sha256':file_sha(__file__),
        'retry_after_seconds':retry_delay(headers,state.get('finished_at',0)),'automatic_resend_authorized':False}


def reconcile_admission(runner,*,apply_plan=None,legacy_attestation=None):
    from filelock import FileLock
    from article_repository.ratings import canonical
    from rating_executor import read_json,write_json
    with FileLock(str(runner.root/'executor.lock'),timeout=0),runner.connect() as conn:
        # Keep the executor lock and one SQL transaction across the complete audit.
        conn.execute('BEGIN IMMEDIATE')
        meta=dict(conn.execute('SELECT key,value FROM metadata'))
        if not runner.binding_ok or meta['binding']!=runner.fixed.signature:raise ValueError('固定执行绑定不符')
        rows=conn.execute("SELECT * FROM jobs WHERE status IN ('uncertain','inflight') ORDER BY article_id").fetchall()
        if conn.execute("SELECT 1 FROM sqlite_master WHERE name='classification_jobs'").fetchone():
            if conn.execute("SELECT 1 FROM classification_jobs WHERE status IN ('uncertain','inflight') LIMIT 1").fetchone():
                raise ValueError('还存在分类阶段未知请求，不能解除全队列暂停')
        halt=meta.get('dispatch_halt')
        if not halt or not rows:raise ValueError('没有可显式复核的暂停及未知请求')
        supplied=read_json(apply_plan) if apply_plan else None
        attestation=legacy_attestation or (supplied.get('legacy_attestation_path') if supplied else None)
        try:proofs=[verify_not_dispatched(runner.root,row,runner.fixed,runner.run_id,attestation) for row in rows]
        except (OSError,KeyError,TypeError) as exc:raise ValueError('存在缺少完整未派发证明的未知请求，整批保持暂停') from exc
        if json.loads(halt).get('article_id') not in {row[0] for row in rows}:raise ValueError('暂停原因不属于已核验的容量拒绝')
        halt_path=runner.root/'dispatch-halt.json'
        if not halt_path.is_file() or read_json(halt_path)!=json.loads(halt):raise ValueError('原暂停文件与持久状态不符')
        plan={'schema':'rating-admission-reconcile/1.0','run_id':runner.run_id,'run_root':str(runner.root),
            'fixed_binding_sha256':runner.fixed.signature,'api_url':runner.api_url,'halt_value':halt,
            'halt_file_sha256':file_sha(halt_path),'proofs':proofs,
            'legacy_attestation_path':str(Path(attestation).resolve()) if attestation else None,
            'automatic_resend_authorized':False,'model_requests_sent':0,'ratings_written':0}
        plan_id=digest(canonical(plan).encode())
        if not apply_plan:
            path=runner.root/'admission'/(plan_id+'.json')
            if path.exists() and read_json(path)!=plan:raise ValueError('审计计划身份冲突')
            if not path.exists():write_json(path,plan)
            return {'applied':False,'plan_path':str(path),'plan_sha256':file_sha(path),'requests':len(proofs),
                'model_requests_sent':0,'ratings_written':0}
        if supplied!=plan or Path(apply_plan).stem!=plan_id:raise ValueError('完整审计计划、原未知集合或暂停证据发生变化')
        for proof in proofs:
            conn.execute("UPDATE jobs SET status='deferred_not_dispatched',error=?,updated_at=? WHERE article_id=?",
                ('Exact local non-dispatch proof; original attempt retained; no automatic resend',__import__('time').time(),proof['article_id']))
        conn.execute('INSERT INTO control_events(kind,payload,created_at) VALUES(?,?,?)',
            ('admission_halt_explicitly_superseded',canonical({'plan_id':plan_id,'plan':plan}),__import__('time').time()))
        conn.execute("DELETE FROM metadata WHERE key='dispatch_halt' AND value=?",(halt,))
        # The original halt file and native attempt files deliberately remain intact.
    from rating_executor import DispatchBlocked
    blocked=False;reason=''
    try:runner._permit(wait_for_capacity=False)
    except DispatchBlocked as exc:blocked=True;reason=str(exc)
    runner.status(blocked=blocked,reason=reason)
    return {'applied':True,'plan_id':plan_id,'requests':len(proofs),'original_halt_retained':True,
            'model_requests_sent':0,'ratings_written':0,'dispatch_gate_modified':False}
