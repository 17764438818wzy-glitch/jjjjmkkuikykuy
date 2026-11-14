"""Reuse the V3 no-dispatch proof for optional title requests and their journals."""
from functools import lru_cache
import importlib.util
import json
from pathlib import Path
import time


@lru_cache(maxsize=2)
def load_protocol(source):
    source = Path(source).resolve()
    spec = importlib.util.spec_from_file_location('_paperflow_shared_gateway_admission', source)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TitleAdmission:
    def __init__(self, source):
        self.protocol = load_protocol(source)

    def wait(self, receipt, prior_waits, stop):
        seconds = self.protocol.retry_delay(receipt, prior_waits)
        if stop is not None:
            if stop.is_set() or stop.wait(seconds):
                raise InterruptedError('题目仍在等待模型名额，原请求证据已保留')
        else:
            time.sleep(seconds)

    def run(self, work, binding, payload, connection, invoke, stop):
        """Every HTTP attempt keeps its immutable native journal; only proof advances."""
        work = Path(work)
        gateway_binding = self.protocol.binding(binding, binding['body_file_id'], binding['request_sha256'], 'title')
        while True:
            if stop is not None and stop.is_set():
                raise InterruptedError('题目请求已停止，原始记录保留')
            # Keep the successful journal at the native `model` path used by
            # named delivery. Only verified refusal journals are atomically moved
            # to the same title's history; every original byte is retained.
            history = work.parent / 'gateway-waits'
            archives = sorted(history.iterdir()) if history.exists() else []
            for number, archive in enumerate(archives, 1):
                if (archive.name != '%06d' % number or not archive.is_dir() or archive.is_symlink()
                        or archive.resolve().parent != history.resolve()):
                    raise ValueError('题目网关等待历史不完整，禁止新增请求')
                self.protocol.verify(archive, gateway_binding)
            state = work / 'request-state.json'
            if state.exists():
                saved = json.loads(state.read_text(encoding='utf-8-sig'))
                if saved.get('error_type') != 'GatewayNotDispatched':
                    # The caller validates success; generic failures/unknown stay final.
                    return work
                receipt = self.protocol.verify(work, gateway_binding)
                self.wait(receipt, len(archives), stop)
                # Re-read original bytes after waiting; a modified proof never authorizes POST.
                self.protocol.verify(work, gateway_binding)
                history.mkdir(exist_ok=True)
                work.rename(history / ('%06d' % (len(archives) + 1)))
                continue
            if work.exists() and any((work / name).exists() for name in
                    ('request.json','response.txt','response-events.jsonl','http-error.txt')):
                raise ValueError('存在未绑定的题目请求证据，禁止新增请求')
            try:
                invoke('body', payload, connection, work, gateway_binding=gateway_binding)
            except self.protocol.GatewayNotDispatched:
                # request_once has saved its final one-call journal by this point.
                self.protocol.verify(work, gateway_binding)
                continue
            return work
