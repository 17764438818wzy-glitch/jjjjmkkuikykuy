"""H2.1 receipt consumption and a single-document attachment delivery list."""
from pathlib import Path
import threading
from .records import identity, read_json, sha_file


def read_receipt(path):
    from .intake_handoff import accepted_paper, _header
    path = Path(path).resolve(); receipt = read_json(path)
    if receipt.get('schema') != 'paperflow-receipt/1': raise ValueError('不是 H2.1 接收回执')
    original = {k: v for k, v in receipt.items() if k not in {'receipt_id', 'record_path'}}
    if identity(original) != receipt.get('receipt_id'): raise ValueError('接收回执身份校验不符')
    paper = accepted_paper(receipt)
    if paper != receipt.get('inventory'): raise ValueError('接收回执与逐项清单不符')
    ref = receipt.get('handoff_ref') or {}
    if not ref.get('path') or not Path(ref['path']).is_file() or sha_file(ref['path']) != ref.get('sha256'):
        raise ValueError('原交接清单引用不可核验；请接收已保存的交接清单')
    handoff = read_json(ref['path']); _header(handoff)
    if any(receipt[key] != handoff[key] for key in ('group_id', 'handoff_id', 'result_version')):
        raise ValueError('接收回执与原资料组不符')
    for item in receipt['items']:
        if item['state'] != '已接收': continue
        rows = ([handoff['body']] if handoff.get('body') else []) if item['kind'] == 'body' else handoff['attachments']
        source = next((row for row in rows if row['file_id'] == item['source']['file_id']), None)
        if not source or any(source[key] != item[key] for key in ('relative', 'sha256', 'bytes')):
            raise ValueError('已接收项没有匹配原件成员')
    return receipt, paper


def register_received(backend, path):
    backend.settings.require_execution()
    receipt, paper = read_receipt(path)
    if paper is not None: backend.stops.setdefault(paper['paper_id'], threading.Event())
    return receipt, paper


def attachment_deliveries(backend, paper, result):
    """Downstream receives the original plus integrated MD, never a second summary."""
    deliveries = []
    if paper is None: return deliveries
    for row in paper['attachments']:
        if result['attachments'].get(row['file_id'], {}).get('status') != 'complete': continue
        from .unified_attachments import delivery_for
        deliveries.append(delivery_for(backend, paper, row))
    return deliveries
