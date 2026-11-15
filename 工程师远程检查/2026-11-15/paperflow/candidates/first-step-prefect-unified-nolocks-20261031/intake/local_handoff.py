"""Read a registered first-step delivery without dispatching processing."""

import importlib
import importlib.util
from pathlib import Path
import re
import sys
from threading import RLock


UPSTREAM_PACKAGE = (Path(__file__).resolve().parent.parent /
                    'first-step-control-20261029' / 'paperflow_prefect')
UPSTREAM_NAMESPACE = '_paperflow_first_step_receipt_reader'
_reader_lock = RLock()


def _upstream_reader():
    """Keep the first-step package distinct from the older same-named package."""
    with _reader_lock:
        package = sys.modules.get(UPSTREAM_NAMESPACE)
        if package is None:
            spec = importlib.util.spec_from_file_location(
                UPSTREAM_NAMESPACE, UPSTREAM_PACKAGE / '__init__.py',
                submodule_search_locations=[str(UPSTREAM_PACKAGE)])
            if spec is None or spec.loader is None:
                raise RuntimeError('第一步回执读取器不可用')
            package = importlib.util.module_from_spec(spec)
            sys.modules[UPSTREAM_NAMESPACE] = package
            try:
                spec.loader.exec_module(package)
            except BaseException:
                sys.modules.pop(UPSTREAM_NAMESPACE, None)
                raise
        reader = importlib.import_module(UPSTREAM_NAMESPACE + '.stage3_handoff')
        delivery_reader = importlib.import_module(UPSTREAM_NAMESPACE + '.delivery_receipt')
        if not getattr(delivery_reader, '_local_reception_filtered', False):
            original = delivery_reader.reload_receipt_delivery
            delivered_file = importlib.import_module(UPSTREAM_NAMESPACE + '.group_delivery').delivered_file

            def reload_received_only(reference, handoff=None, *, verify_files=True):
                delivery = original(reference, handoff, verify_files=False)
                if verify_files:
                    source = delivery['handoff']
                    rows = ([source['body']] if source.get('body') else []) + source['attachments']
                    for row in rows:
                        try:
                            delivery['verified_files'][row['file_id']] = str(delivered_file(delivery, row))
                        except (OSError, ValueError, KeyError, TypeError) as exc:
                            declared = next((entry for entry in delivery['manifest']['files']
                                             if entry.get('file_id') == row.get('file_id')), {})
                            delivery['file_errors'].append(dict(file_id=row.get('file_id'),
                                relative=row.get('relative'), copied=declared.get('copied'),
                                upstream_error=declared.get('error'),
                                reason=str(exc) if isinstance(exc, ValueError) else type(exc).__name__))
                return delivery

            delivery_reader.reload_receipt_delivery = reload_received_only
            delivery_reader._local_reception_filtered = True
        return reader


def read_registered_item(item, *, receipt_root):
    """Return a transient observation of one first-step API item.

    The caller supplies the actual first-step registration root. This function
    does not create a durable claim on the upstream task or its files.
    """
    if (not isinstance(item, dict) or item.get('status') != 'passed'
            or item.get('registration_status') != 'registered'
            or item.get('delivery_complete') is not True):
        raise ValueError('该资料组尚未完成分类与登记')

    group_id, paper_id, receipt_id = item.get('group_id'), item.get('paper_id'), item.get('receipt_id')
    if (not isinstance(group_id, str) or not group_id or not isinstance(paper_id, str)
            or not paper_id or not isinstance(receipt_id, str)):
        raise ValueError('登记身份缺失')
    if not re.fullmatch(r'[0-9a-f]{64}', receipt_id):
        raise ValueError('登记回执编号无效')
    try:
        root = Path(receipt_root).resolve(strict=True)
        receipt_path = Path(item['receipt_path']).resolve(strict=True)
        expected = (root / 'receipts' / (receipt_id + '.json')).resolve(strict=True)
        delivery_dir = Path(item['delivery_dir']).resolve(strict=True)
    except (OSError, KeyError, TypeError, ValueError) as exc:
        raise ValueError('登记回执或交付目录不可用') from exc
    if not receipt_path.is_relative_to(root) or receipt_path != expected:
        raise ValueError('回执不属于指定的登记目录')

    reader = _upstream_reader()
    before = reader.sha_file(receipt_path)
    receipt, paper = reader.read_receipt(receipt_path)
    if reader.sha_file(receipt_path) != before:
        raise ValueError('读取期间登记回执发生变化')
    if (receipt.get('receipt_id') != receipt_id or receipt.get('paper_id') != paper_id
            or receipt.get('group_id') != group_id
            or Path(receipt.get('record_path', '')).resolve() != receipt_path):
        raise ValueError('上游接口与登记回执身份不一致')
    reference = receipt.get('delivery_ref')
    if not isinstance(reference, dict) or not reference.get('path'):
        raise ValueError('登记回执没有新版交付目录绑定')
    if Path(reference['path']).resolve().parent != delivery_dir:
        raise ValueError('上游接口与登记回执交付目录不一致')
    observation = receipt.get('observation') or {}
    original_state = observation.get('stored_state', receipt.get('state'))
    if original_state != '已接收':
        raise ValueError('登记回执未确认接收')
    if paper is not None and paper.get('paper_id') != paper_id:
        raise ValueError('接收材料与论文编号不一致')

    return {
        'schema': 'paperflow-local-input-observation/1',
        'paper_id': paper_id,
        'group_id': receipt.get('group_id'),
        'origin_group_id': receipt.get('origin_group_id'),
        'handoff_id': receipt.get('handoff_id'),
        'result_version': receipt.get('result_version'),
        'receipt_id': receipt_id,
        'receipt_path': str(receipt_path),
        'receipt_sha256': before,
        'receipt_schema': receipt.get('schema'),
        'receipt_state': receipt.get('state'),
        'delivery_dir': str(delivery_dir),
        'delivery_ref': reference,
        'body_available': bool(paper and paper.get('body')),
        'paper': paper,
        'items': receipt.get('items', []),
        'observation': observation,
    }
