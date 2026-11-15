"""H2.1 receipt consumption and a single-document attachment delivery list."""
import copy
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
    handoff = read_json(ref['path']); _header(handoff, lexical_paths=receipt.get('delivery_ref') is not None)
    if any(receipt[key] != handoff[key] for key in ('group_id', 'handoff_id', 'result_version')):
        raise ValueError('接收回执与原资料组不符')
    delivery = None
    if receipt.get('delivery_ref') is not None:
        from .delivery_receipt import reload_receipt_delivery
        # Only received body/attachments belong to this stage; a discarded
        # upstream file must not turn a valid attachment into a damaged receipt.
        delivery = reload_receipt_delivery(receipt['delivery_ref'], handoff, verify_files=False)
        # Keep the persisted receipt immutable. Current damage is an observed
        # partial state, so an unaffected body can still be handed onward.
        receipt = copy.deepcopy(receipt)
        stored_state = receipt['state']
    for item in receipt['items']:
        if item['state'] != '已接收': continue
        rows = ([handoff['body']] if handoff.get('body') else []) if item['kind'] == 'body' else handoff['attachments']
        source = next((row for row in rows if row['file_id'] == item['source']['file_id']), None)
        if not source or any(source[key] != item[key] for key in ('relative', 'sha256', 'bytes')):
            raise ValueError('已接收项没有匹配原件成员')
        if delivery is not None:
            if item.get('delivery_ref') != receipt['delivery_ref']:
                raise ValueError('接收项的交付清单绑定不符')
            from .intake_handoff import verify_received_file
            try:
                verify_received_file(item, delivery=delivery)
                delivery['verified_files'][item['upstream_file_id']] = item['path']
            except (OSError,ValueError,KeyError,TypeError) as exc:
                item.update(state='未接收',error='交付副本复核失败：'+type(exc).__name__)
                delivery['verified_files'].pop(item['upstream_file_id'],None)
                delivery['file_errors'].append(dict(file_id=item['upstream_file_id'],
                    relative=item.get('relative'),reason='交付副本再次复核失败：'+type(exc).__name__))
            if item['kind']=='attachment':
                for problem in delivery['evidence_issues']:
                    owner=problem.get('file_id') if isinstance(problem,dict) else None
                    if owner is None or owner==item['upstream_file_id']:
                        item.setdefault('unverified_evidence',[]).append(
                            dict(reference='delivery-evidence',reason='交付证据复核未通过'))
    if delivery is not None:
        observed_problem=(bool(delivery['file_errors']) or bool(delivery['evidence_issues'])
                          or delivery['manifest']['ready_for_receipt'] is not True
                          or any(item['state']!='已接收' or item.get('unverified_evidence')
                                 for item in receipt['items']))
        if observed_problem:
            # This is a transient view, not a new paperflow-receipt/1 object.
            # receipt_id continues to identify only the unchanged stored receipt.
            receipt['schema']='paperflow-receipt-observation/1'
            receipt['state']='未接收'
            receipt['observation']=dict(stored_state=stored_state,
                receipt_id=receipt['receipt_id'],receipt_path=str(path),receipt_sha256=sha_file(path),
                file_errors=delivery['file_errors'],evidence_errors=delivery['evidence_issues'])
            view=receipt.get('delivery')
            if isinstance(view,dict):
                view['files_complete']=bool(delivery['manifest']['files_complete'] and not delivery['file_errors'])
                view['ready_for_receipt']=False
                view['file_errors']=copy.deepcopy(delivery['file_errors'])
                view['evidence_errors']=copy.deepcopy(delivery['manifest']['evidence_errors'])+copy.deepcopy(delivery['evidence_issues'])
                for entry in view.get('files',[]):
                    physical=delivery['verified_files'].get(entry.get('file_id'))
                    entry['verified_copy']=physical is not None
                    entry['delivered_path']=physical
            receipt['progress']=dict(
                body_received=sum(r['kind']=='body' and r['state']=='已接收' for r in receipt['items']),
                body_total=sum(r['kind']=='body' for r in receipt['items']),
                attachments_received=sum(r['kind']=='attachment' and r['state']=='已接收' for r in receipt['items']),
                attachments_total=sum(r['kind']=='attachment' for r in receipt['items']),
                unresolved=len(receipt.get('unresolved',[])))
            paper=accepted_paper(receipt)
            receipt['inventory']=paper
    return receipt, paper


def register_received(backend, path):
    backend.settings.require_execution()
    receipt, paper = read_receipt(path)
    if paper is not None: backend.stops.setdefault(paper['paper_id'], threading.Event())
    return receipt, paper


def registered_attachment_backend(settings, *, invoke, local_scope, title_local_scope=None):
    """Use existing receipt/history handling for attachment-only dispatch."""
    from .unified import UnifiedBackend
    return UnifiedBackend(settings, invoke=invoke, local_scope=local_scope,
                          title_local_scope=title_local_scope)


def attachment_deliveries(backend, paper, result, *, use_named=True):
    """Downstream receives the original plus integrated MD, never a second summary."""
    deliveries = []
    if paper is None: return deliveries
    for row in paper['attachments']:
        if result['attachments'].get(row['file_id'], {}).get('status') != 'complete': continue
        from .unified_attachments import delivery_for
        if use_named:
            deliveries.append(delivery_for(backend, paper, row))
        else:
            deliveries.append(delivery_for(backend, paper, row, use_named=False))
    return deliveries


def report_unreceived_attachments(receipt, result):
    """Retain an individual failure without hiding other completed attachments."""
    for row in receipt.get('items', []):
        if row.get('kind') == 'attachment' and row.get('state') != '已接收':
            result['attachments'][row['file_id']] = {
                'status': 'failed', 'error': {'code': 'attachment_unavailable'}}
            result['status'] = 'incomplete'
    return result
