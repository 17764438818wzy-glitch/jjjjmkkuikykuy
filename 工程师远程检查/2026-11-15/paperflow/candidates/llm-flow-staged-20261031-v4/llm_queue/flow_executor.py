"""Execute one real folder order through raw responses, never postprocessing."""
from concurrent.futures import ThreadPoolExecutor
import contextvars


def execute_folder(identity, generation, ledger, verify_upstream, operations, guard, stop):
    if stop is None or stop.is_set():
        raise InterruptedError('任务已暂停，文件夹未进入 LLM 处理')
    manifest = verify_upstream(identity)
    attachments = manifest.get('attachments') if isinstance(manifest, dict) else None
    if not isinstance(attachments, list):
        raise ValueError('文件夹附件清单不可核验')
    registered = ledger.register(identity, verify_upstream)
    if registered.get('state') == 'waiting_postprocessing':
        # A restarted folder never reissues a completed component.
        return registered
    ledger.begin(identity, generation, verify_upstream)

    def run_body():
        if stop.is_set():
            raise InterruptedError('任务已暂停，正文未发送')
        return operations.body()

    def run_attachments():
        for row in attachments:
            if stop.is_set():
                raise InterruptedError('任务已暂停，剩余附件未发送')
            operations.attachment(row)

    # Two bounded lanes per folder. Shared Prefect leases enforce the actual
    # cross-process total and per-lane limits at each HTTP boundary.
    with ThreadPoolExecutor(max_workers=2, thread_name_prefix='folder-llm') as executor:
        body = executor.submit(contextvars.copy_context().run, run_body)
        attachments_future = executor.submit(contextvars.copy_context().run, run_attachments)
        failures = []
        for future in (body, attachments_future):
            try:
                future.result()
            except Exception as exc:
                failures.append(exc)
    if failures:
        raise failures[0]
    result = ledger.finish(identity, verify_upstream, guard)
    if result.get('state') != 'waiting_postprocessing':
        raise ValueError('文件夹仍有未完成的模型响应')
    return result
