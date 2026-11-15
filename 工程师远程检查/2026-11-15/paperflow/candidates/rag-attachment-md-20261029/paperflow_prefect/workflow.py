"""Dependency ordering; resource allocation belongs to Prefect."""
import asyncio


async def execute_stage3(paper, call):
    """Advance received items independently, stopping before RAG/NAS."""
    from .attachments import safe_error as attachment_error
    from .profile_recovery import safe_error as body_error
    result = {'paper_id': paper.get('paper_id'), 'body': {'status': 'not_received'}, 'attachments': {}}
    if not paper.get('body') and not paper.get('attachments'):
        return dict(result, status='no_material')
    async def body_branch():
        if paper.get('body') is None: return
        try:
            await call('prepare', paper)
            await call('profile', paper)
            result['body'] = {'status': 'complete'}
        except Exception as exc:
            result['body'] = {'status': 'failed', 'error': body_error(exc)}
    queue = asyncio.Queue()
    for row in paper.get('attachments', []): queue.put_nowait(row)
    async def attachment_worker():
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
    async def body_branch():
        for stage in ('prepare', 'profile', 'package', 'publish_body'):
            await call(stage, paper)

    async def attachment_branch():
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
        return {'status': 'incomplete', 'paper_id': paper['paper_id'], 'failures': failures}
    for stage in ('publish_attachments', 'archive', 'finish'):
        try:
            result = await call(stage, paper)
        except Exception as exc:
            return {'status': 'incomplete', 'paper_id': paper['paper_id'],
                    'failures': {stage: type(exc).__name__}}
    return result
