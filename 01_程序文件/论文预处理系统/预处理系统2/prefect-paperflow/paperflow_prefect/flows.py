"""Prefect owns scheduling; native business adapters own durable outcomes."""
import asyncio
import os
from pathlib import Path
import uuid
from contextlib import asynccontextmanager

from .settings import Settings, PROJECT
from .records import Records, identity, write_json
from .rag_status import RagProgress


def configure_environment():
    settings = Settings.load()
    os.environ['PREFECT_API_URL'] = settings['prefect_api_url']
    os.environ['PREFECT_HOME'] = str(PROJECT / 'runtime/prefect-home')
    os.environ['PREFECT_SERVER_ALLOW_EPHEMERAL_MODE'] = 'false'
    # This isolated LAN server does not use a Prefect Cloud credential.
    os.environ.pop('PREFECT_API_KEY', None)
    return settings


_LOADED_LIMITS = dict(configure_environment()['limits'])
from filelock import FileLock
from prefect import flow, task, get_run_logger
from prefect.artifacts import create_markdown_artifact
from prefect.cache_policies import NO_CACHE
from prefect.client.orchestration import get_client
from prefect.concurrency.asyncio import concurrency as async_concurrency
from prefect.concurrency.sync import concurrency
from . import legacy
from .backend import Backend
from .intake import discover
from .workflow import execute_paper
from .resources import DeadlineStop, cancellable_concurrency, drain_thread

LIMIT_PREFIX = 'paperflow-connected-'
RESOURCES = {'prepare':'local', 'generate_title':'title_llm', 'profile':'body_llm', 'attachment':'attachment_llm',
    'package':'package', 'admit_attachments':'package', 'publish_body':'rag',
    'publish_attachments':'rag', 'recover_body':'rag', 'archive':'nas', 'cleanup':'nas', 'finish':'local'}
LABELS = {'prepare':'② 本机准备正文', 'generate_title':'② 缺题目时生成论文题目', 'profile':'③ 正文 MD 画像', 'attachment':'③ 附件简单识别与 MD',
    'package':'④ 脱敏与切片封包', 'admit_attachments':'④ 附件说明嵌入预算检查',
    'publish_body':'④ 正文画像入 RAG 并核验', 'publish_attachments':'④ 附件说明关联入 RAG 并核验',
    'recover_body':'④ 沿用原对象恢复入库', 'archive':'⑤ 原稿附件复制 NAS 并读回',
    'cleanup':'⑤ 核验后清理本地', 'finish':'⑤ 保存实际完成记录'}
_batches = {}


def make_gates(settings):
    limits = settings['limits']
    gates = {key: asyncio.Semaphore(value) for key, value in limits.items()}
    gates['title_llm'] = asyncio.Semaphore(min(16, limits['body_llm']))
    return gates


class StageFailed(RuntimeError): pass
class PaperIncomplete(RuntimeError): pass
class BatchIncomplete(RuntimeError): pass


def stage_error_message(stage, error):
    if stage == 'profile':
        from .profile_recovery import safe_error
        details = safe_error(error)
        location = ('（行 %s，列 %s）' % (details['line'], details['column'])) if 'line' in details else ''
        return 'profile: ' + details['code'] + '；' + details['message'] + location + '；详见本地阶段记录'
    if stage == 'attachment':
        from .attachments import safe_error
        details = safe_error(error)
        return 'attachment: ' + details['code'] + '；' + details['message'] + '；详见本地阶段记录'
    return stage + ': ' + type(error).__name__ + '；详见本地阶段记录'


@task(name='① 接收已分流个人资料', persist_result=False, cache_policy=NO_CACHE, retries=0)
async def intake_stage(batch_id: str, person_folder: str):
    batch = _batches[batch_id]
    async with async_concurrency(LIMIT_PREFIX + 'local', occupy=1, strict=True):
        paper = await drain_thread(batch['backend'].intake, person_folder)
    batch['papers'][paper['paper_id']] = paper
    return paper['paper_id']


def verify_limits(settings):
    with get_client(sync_client=True) as api:
        for key, expected in settings['limits'].items():
            row = api.read_global_concurrency_limit_by_name(LIMIT_PREFIX + key)
            if row.limit != expected or not row.active or row.slot_decay_per_second:
                raise ValueError('Prefect 并发配置不符或未启用：' + key)


def invoke_model(kind, body, connection, work, *, admission_timeout=None, gateway_binding=None, **kwargs):
    from preextract_request import request_once
    # Native request journals measure http_started_at/http_finished_at and
    # protect unknown outcomes. Acquire slots at the actual dispatch boundary.
    def dispatch(send, transport):
        stop = kwargs.get('stop')
        names = [LIMIT_PREFIX + 'total_llm', LIMIT_PREFIX + kind + '_llm']
        if admission_timeout is not None:
            raise ValueError('模型名额应排队等待，不支持设置等待超时')
        expected = {names[0]: _LOADED_LIMITS['total_llm'], names[1]: _LOADED_LIMITS[kind + '_llm']}
        with cancellable_concurrency(names, stop, expected=expected):
            if isinstance(stop, DeadlineStop):
                stop.start()
            return send()
    if gateway_binding is None:
        return request_once(body, connection, work, dispatch=dispatch, **kwargs)
    settings = Settings.load()
    if settings.values.get('workbuddy_credit_pool') is not True or kwargs.get('opener') is not None:
        raise ValueError('题目网关排队协议尚未确认，未发送请求')
    from .gateway_credit_pool import load_protocol
    gateway = load_protocol(settings.path('gateway_admission_source'))
    kwargs['opener'] = gateway.GatewayOpener(connection=connection, model_dir=work,
        request_binding=gateway_binding, stop=kwargs.get('stop'))
    try:
        return request_once(body, connection, work, dispatch=dispatch, **kwargs)
    except gateway.GatewayNotDispatched:
        gateway.verify(work, gateway_binding)
        stop = kwargs.get('stop')
        if isinstance(stop, DeadlineStop):
            stop.deadline = None  # Verified local waiting does not spend model execution time.
        raise


@asynccontextmanager
async def stage_gate(gate, stage):
    async with gate: yield


def title_local_concurrency(stop):
    name = LIMIT_PREFIX + 'local'
    return cancellable_concurrency(name, stop, expected={name: _LOADED_LIMITS['local']})


@task(name='业务阶段', persist_result=False, cache_policy=NO_CACHE, retries=0)
async def business_stage(batch_id: str, paper_id: str, stage: str, attachment_id: str = ''):
    batch = _batches[batch_id]
    paper = batch['papers'][paper_id]
    backend = batch.get('paper_backends', {}).get(paper_id, batch['backend'])
    resource = RESOURCES[stage]
    stop = backend.stops[paper_id]
    if stop.is_set(): raise StageFailed('此篇已取消，停止进入新阶段')
    args = (paper,)
    if attachment_id:
        args += (next(row for row in paper['attachments'] if row['file_id'] == attachment_id),)
    observation = None
    if resource == 'rag':
        if 'rag_progress' not in batch:
            batch['rag_progress'] = RagProgress(backend.root / 'batches' / (batch_id + '-rag.json'),
                                                backend.settings['limits']['rag'])
        observation = batch['rag_progress'].observation(paper, stage)
        observation.update('waiting_local', occupied=False)
    async def run():
        if observation:
            with observation.bind(): return await drain_thread(getattr(backend, stage), *args, stop=stop)
        return await drain_thread(getattr(backend, stage), *args, stop=stop)
    def finished():
        if observation:
            records = Records(backend.paper_root(paper))
            verified = all((records.cached(s) or {}).get('status') == 'verified'
                           for s in ('rag-body', 'rag-attachments'))
            observation.update('verified' if verified else 'body_verified', occupied=False,
                               in_flight=False, paper_verified=verified)
    try:
        # Only an explicitly empty frozen inventory can skip RAG admission.
        # Keep the native inventory/parent checks and durable completion record.
        if stage == 'publish_attachments' and paper['attachments'] == []:
            observation.update('no_attachments', occupied=False)
            result = await run()
            finished()
            return {'stage': stage, 'paper_id': paper_id, 'status': 'stage_complete',
                    'reason': 'no_attachments', 'rag_slot_requested': False}
        async with stage_gate(batch['gates'][resource], stage):
            if resource in {'body_llm', 'attachment_llm', 'title_llm'}:
                # Keep HTTP waiters bounded; global LLM slots cover dispatch only.
                result = await run()
            else:
                if observation: observation.update('waiting_global', occupied=False)
                async with async_concurrency(LIMIT_PREFIX + resource, occupy=1, strict=True):
                    if observation: observation.update('ready', occupied=True)
                    result = await run()
        finished()
        return result if isinstance(result, dict) else {'stage': stage, 'paper_id': paper_id, 'status': 'stage_complete'}
    except BaseException as exc:
        if observation: observation.update('unknown', occupied=False, in_flight=False,
                                           paper_verified=False, error_type=type(exc).__name__)
        if not isinstance(exc, Exception): raise
        # API exception strings may include model text or credentials.
        raise StageFailed(stage_error_message(stage, exc)) from None


@flow(name='单篇论文五步处理', persist_result=False, retries=0, log_prints=False)
async def connected_paper(batch_id: str, person_folder: str):
    batch = _batches[batch_id]
    backend = batch['backend']
    backend.settings.require_execution()
    # One cross-process owner for the person's body and attachment records.
    # This is an OS lock (released on process exit), not another task scheduler.
    key = identity(str(Path(person_folder).resolve()).casefold())
    lock_path = backend.root / 'locks' / (key + '.lock')
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with FileLock(lock_path, timeout=0, thread_local=False):
        paper_id = await intake_stage(batch_id, person_folder)
        paper = batch['papers'][paper_id]
        async def call(stage, current, row=None):
            return await business_stage.with_options(name=LABELS[stage],
                task_run_name=paper['paper_id'][-10:] + '-' + stage)(
                batch_id, paper['paper_id'], stage, row['file_id'] if row else '')
        result = await execute_paper(paper, call)
        write_json(backend.paper_root(paper) / 'last-run.json', result)
        if result['status'] != 'complete':
            raise PaperIncomplete('阶段未完成：' + ', '.join(result['failures']))
        return result


@task(name='检查接线', persist_result=False, cache_policy=NO_CACHE, retries=0)
async def wiring_check(stage: str, checks: dict):
    if not all(checks.values()): raise ValueError('接线检查未通过：' + stage)
    return {'stage':stage, 'checks':checks, 'real_papers_read':0, 'business_external_calls':0}


async def check_connection(settings):
    # No input scan, credential load, model requests, RAG writes or NAS probes.
    preflight = await asyncio.to_thread(legacy.preflight, settings)
    await asyncio.to_thread(verify_limits, settings)
    await wiring_check.with_options(name='① 检查分流目录接线（不扫描原稿）')('intake',
        {'input_configured':bool(settings['library_root']), 'local_retention':True})
    await asyncio.gather(
        wiring_check.with_options(name='② 检查正文预处理模块')('prepare',
            {'native_module':bool(preflight['modules']['preextract_material'])}),
        wiring_check.with_options(name='③ 检查附件独立识别接线')('attachment', {'separate_from_body_template':True}))
    await wiring_check.with_options(name='③ 检查正文 MD 模板接线')('profile',
        {'template':preflight['template_exists'], 'catalog':preflight['catalog_exists']})
    await wiring_check.with_options(name='④ 检查封包与 RAG 接线（不联网入库）')('rag',
        {**preflight['counter_paths'], 'rag_module':bool(preflight['modules']['paperflow.rag'])})
    await wiring_check.with_options(name='⑤ 检查 NAS 与完成记录接线（不写 NAS）')('nas',
        {'native_module':bool(preflight['modules']['paperflow.original_archive.lifecycle']), 'local_retention':True})
    result = dict(preflight, prefect_limits_checked=True, mode='check', status='connection_check_passed')
    write_json(PROJECT / 'runtime/connection-check.json', result)
    await create_markdown_artifact(key='paperflow-connected-status', markdown=
        '# 论文五步流程已接线\n\n当前为接线检查。真实论文执行关闭；本次未读取原稿、未调用模型、未写 RAG 或 NAS。\n\n'
        '1. 接收每人文件夹中的正文和附件，排除垃圾材料。\n'
        '2. 正文本机预处理，保留附件读取证据。\n'
        '3. 正文按原 MD 模板画像，附件简单识别并生成独立说明。\n'
        '4. 原文、画像、必要图表与附件说明分别入 RAG 并核验关联。\n'
        '5. 原件复制 NAS 并核验，保存完成记录，本地保留。\n\n'
        '配置名额：正文 LLM 100，附件 LLM 20；此数值不是实测真实请求峰值。\n\n'
        '实际执行必须先开启本机配置，网页参数不能绕过。无自动扫描计划。',
        description='接线检查通过；不是论文交付或生产验收')
    return result


@flow(name='论文五步流程（已接线）', description='接收已分流资料；正文MD与附件简识别独立；RAG/NAS核验后记录完成，本地保留。默认仅检查，真实执行关闭。',
      persist_result=False, retries=0, log_prints=False)
async def paperflow_batch(run_mode: str = 'check', person_folders: list[str] | None = None, max_papers: int = 100):
    settings = Settings.load()
    if run_mode == 'check': return await check_connection(settings)
    if run_mode != 'process': raise ValueError('run_mode 只能为 check 或 process')
    # Must precede input discovery, key loading, backend construction or any effects.
    settings.require_execution()
    if not 1 <= max_papers <= 10000: raise ValueError('批次上限必须为 1 到 10000')
    await asyncio.to_thread(verify_limits, settings)
    backend = Backend(settings, invoke=invoke_model,
        local_scope=lambda: concurrency(LIMIT_PREFIX + 'local', occupy=1, strict=True),
        title_local_scope=title_local_concurrency)
    folders = person_folders if person_folders is not None else await asyncio.to_thread(discover, settings.path('library_root'))
    folders = list(dict.fromkeys(str(Path(folder).resolve()) for folder in folders))[:max_papers]
    batch_id = uuid.uuid4().hex
    _batches[batch_id] = {'backend':backend, 'papers':{},
        'gates':make_gates(settings)}
    queue = asyncio.Queue()
    for folder in folders: queue.put_nowait(folder)
    outcomes = []
    async def worker():
        while not queue.empty():
            folder = queue.get_nowait()
            try:
                result = await connected_paper(batch_id, folder)
            except Exception as exc:
                result = {'status':'incomplete', 'person_folder':folder, 'error_type':type(exc).__name__}
            outcomes.append(result)
    try:
        await asyncio.gather(*(worker() for _ in range(min(len(folders), settings['max_live_papers']))))
        summary = {'batch_id':batch_id, 'total':len(outcomes),
            'complete':sum(row['status']=='complete' for row in outcomes), 'papers':outcomes}
        write_json(backend.root / 'batches' / (batch_id + '.json'), summary)
        if summary['complete'] != summary['total']:
            raise BatchIncomplete(f"{summary['total']-summary['complete']} 篇未完成，成功阶段已保存")
        return summary
    finally:
        _batches.pop(batch_id, None)


@task(name='③ 已接收单项处理', persist_result=False, cache_policy=NO_CACHE, retries=0)
async def received_item_stage(batch_id: str, stage: str, attachment_id: str = ''):
    from .attachments import safe_error as attachment_error
    from .profile_recovery import safe_error as profile_error
    batch = _batches[batch_id]; backend = batch['backend']; paper = batch['paper']
    row = next((r for r in paper['attachments'] if r['file_id'] == attachment_id), None) if attachment_id else None
    args = (paper, row) if row else (paper,)
    try:
        async with stage_gate(batch['gates'][RESOURCES[stage]], stage):
            if stage == 'prepare':
                async with async_concurrency(LIMIT_PREFIX + 'local', occupy=1, strict=True):
                    result = await drain_thread(backend.prepare, paper, stop=backend.stops[paper['paper_id']])
            else:
                result = await drain_thread(getattr(backend, stage), *args, stop=backend.stops[paper['paper_id']])
        return {'status': 'complete', 'result': result} if stage == 'generate_title' else {'status': 'complete'}
    except Exception as exc:
        return {'status': 'failed', 'error': (attachment_error if row else profile_error)(exc)}


@flow(name='③ 接收回执到正文画像与附件 MD', persist_result=False, retries=0, log_prints=False)
async def received_handoff(receipt_path: str, attachment_id: str | None = None, config_path: str | None = None):
    """Additive H2.1 entry, intentionally stops before fourth/fifth stages."""
    from .stage3_handoff import read_receipt, register_received, attachment_deliveries
    from .workflow import execute_stage3, selected_attachments
    from .attachments import AttachmentError
    from .profile_recovery import ProfileRecoveryError
    settings = Settings.load(config_path) if config_path is not None else Settings.load()
    settings.require_execution()
    if config_path is not None and settings['prefect_api_url'].rstrip('/') != os.environ['PREFECT_API_URL'].rstrip('/'):
        raise ValueError('显式运行配置必须沿用当前 Prefect 服务地址，不能切换调度服务')
    receipt, paper = read_receipt(receipt_path)
    selected_attachments(paper, attachment_id)
    if paper is None:
        return {'status': 'no_material', 'receipt_id': receipt['receipt_id'], 'receipt_state': receipt['state'], 'deliveries': []}
    await asyncio.to_thread(verify_limits, settings)
    backend = Backend(settings, invoke=invoke_model,
        local_scope=lambda: concurrency(LIMIT_PREFIX + 'local', occupy=1, strict=True),
        title_local_scope=title_local_concurrency)
    register_received(backend, receipt_path)
    batch_id = uuid.uuid4().hex
    _batches[batch_id] = {'backend': backend, 'paper': paper,
        'gates': make_gates(settings)}
    lock_path = backend.root / 'locks' / (identity(str(Path(paper['person_dir']).resolve()).casefold()) + '.lock')
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with FileLock(lock_path, timeout=0, thread_local=False):
            async def call(stage, current, row=None):
                reply = await received_item_stage.with_options(name=LABELS[stage])(
                    batch_id, stage, row['file_id'] if row else '')
                if reply['status'] == 'failed':
                    error = reply['error']
                    cls = AttachmentError if row else ProfileRecoveryError
                    raise cls(error['code'], **{k: error[k] for k in ('line', 'column', 'diagnostic') if k in error})
                return reply.get('result')
            result = await execute_stage3(paper, call, attachment_id=attachment_id)
            result.update(receipt_id=receipt['receipt_id'], receipt_state=receipt['state'],
                          receipt_progress=receipt['progress'], deliveries=attachment_deliveries(backend, paper, result))
            target = backend.root / 'stage3' / receipt['receipt_id'] / 'runs' / (batch_id + '.json')
            write_json(target, result)
            if result['status'] == 'incomplete': raise PaperIncomplete('第三状态有未完成项；已成功项及具体原因已保存：' + str(target))
            return dict(result, record=str(target))
    finally:
        _batches.pop(batch_id, None)
