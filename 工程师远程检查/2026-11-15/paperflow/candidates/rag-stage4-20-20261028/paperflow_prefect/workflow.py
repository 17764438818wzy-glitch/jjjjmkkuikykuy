"""Dependency ordering; resource allocation belongs to Prefect."""
import asyncio


async def execute_rag_resume(paper, call):
    """Only stage four, then the existing per-paper NAS handoff."""
    for stage in ('recover_body', 'publish_attachments', 'archive', 'finish'):
        try:
            result = await call(stage, paper)
        except Exception as exc:
            return {'status': 'incomplete', 'paper_id': paper['paper_id'],
                    'failures': {stage: type(exc).__name__}}
    return result


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
