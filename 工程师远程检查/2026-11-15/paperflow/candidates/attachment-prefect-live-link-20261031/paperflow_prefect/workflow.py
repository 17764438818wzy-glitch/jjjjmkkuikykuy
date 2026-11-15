"""Dependency ordering; resource allocation belongs to Prefect."""
import asyncio


def needs_generated_title(paper):
    body = paper.get('body') or {}
    return bool(body.get('file_id') and body.get('sha256') and not body.get('display_name'))


async def prepare_and_generate_title(paper, call):
    await call('prepare', paper)
    if not needs_generated_title(paper):
        return None
    try:
        return await call('generate_title', paper)
    except Exception as exc:
        return {'status': 'unavailable', 'error_type': type(exc).__name__}


def selected_attachments(paper, attachment_id=None):
    rows = paper.get('attachments', []) if paper else []
    if attachment_id is None: return rows
    selected = [row for row in rows if row['file_id'] == attachment_id]
    if not attachment_id or len(selected) != 1:
        raise ValueError('指定编号不属于本回执唯一的已接收附件')
    return selected


async def execute_stage3(paper, call, *, attachment_id=None, attachments_only=False):
    """Advance received items independently, stopping before RAG/NAS."""
    from .attachments import safe_error as attachment_error
    from .profile_recovery import safe_error as body_error
    if attachments_only and attachment_id is not None:
        raise ValueError('附件整体处理不能同时指定单个附件')
    attachments = selected_attachments(paper, attachment_id)
    result = {'paper_id': paper.get('paper_id'), 'body': {'status': 'not_received'}, 'attachments': {}}
    if attachments_only:
        result['scope'] = {'kind': 'attachments'}
        result['body'] = {'status': 'not_selected'}
    if attachment_id is not None:
        result['scope'] = {'kind': 'attachment', 'attachment_id': attachment_id}
        result['body'] = {'status': 'not_selected'}
    if not paper.get('body') and not paper.get('attachments'):
        return dict(result, status='no_material')
    title_ready = asyncio.Event()
    if attachments_only or attachment_id is not None or not needs_generated_title(paper): title_ready.set()
    async def body_branch():
        if attachments_only or attachment_id is not None or paper.get('body') is None: return
        try:
            try:
                title_result = await prepare_and_generate_title(paper, call)
                if title_result is not None: result['title_generation'] = title_result
            finally:
                title_ready.set()
            await call('profile', paper)
            result['body'] = {'status': 'complete'}
        except Exception as exc:
            result['body'] = {'status': 'failed', 'error': body_error(exc)}
    queue = asyncio.Queue()
    for row in attachments: queue.put_nowait(row)
    async def attachment_worker():
        await title_ready.wait()
        while not queue.empty():
            row = queue.get_nowait()
            try:
                card = await call('attachment', paper, row)
                result['attachments'][row['file_id']] = {'status': 'complete', 'result': card}
            except Exception as exc:
                result['attachments'][row['file_id']] = {'status': 'failed', 'error': attachment_error(exc)}
    await asyncio.gather(body_branch(), *(attachment_worker() for _ in range(min(20, queue.qsize()))))
    failed = result['body']['status'] == 'failed' or any(x['status'] == 'failed' for x in result['attachments'].values())
    return dict(result, status='incomplete' if failed else 'stage3_complete')


async def execute_paper(paper, call):
    title_result = None
    title_ready = asyncio.Event()
    if not needs_generated_title(paper): title_ready.set()
    async def body_branch():
        nonlocal title_result
        try:
            title_result = await prepare_and_generate_title(paper, call)
        finally:
            title_ready.set()
        for stage in ('profile', 'package', 'publish_body'):
            await call(stage, paper)

    async def attachment_branch():
        await title_ready.wait()
        # Bound the number of task records even for a large unpacked project.
        queue = asyncio.Queue()
        for row in paper['attachments']: queue.put_nowait(row)
        results = []
        async def worker():
            while not queue.empty():
                row = queue.get_nowait()
                try: await call('attachment', paper, row)
                except Exception as exc: results.append(exc)
        await asyncio.gather(*(worker() for _ in range(min(20, len(paper['attachments'])))))
        if any(isinstance(row, BaseException) for row in results):
            raise RuntimeError('附件识别未全部完成')
        await call('admit_attachments', paper)

    outcomes = await asyncio.gather(body_branch(), attachment_branch(), return_exceptions=True)
    failures = {name: type(result).__name__ for name, result in zip(('body', 'attachments'), outcomes)
                if isinstance(result, BaseException)}
    if failures:
        result = {'status': 'incomplete', 'paper_id': paper['paper_id'], 'failures': failures}
        if title_result is not None: result['title_generation'] = title_result
        return result
    for stage in ('publish_attachments', 'archive', 'finish'):
        try:
            result = await call(stage, paper)
        except Exception as exc:
            return {'status': 'incomplete', 'paper_id': paper['paper_id'],
                    'failures': {stage: type(exc).__name__}}
    if title_result is not None and isinstance(result, dict):
        result = dict(result, title_generation=title_result)
    return result
