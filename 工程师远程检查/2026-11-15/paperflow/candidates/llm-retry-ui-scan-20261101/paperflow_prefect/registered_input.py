"""Read the already retained upstream registration; never claim or scan it."""
from pathlib import Path
from .attachment_queue import AttachmentQueue
from .records import read_json


def verify_registered_receipt(receipt_path, receipt_id):
    config_path = (Path(__file__).resolve().parent.parent.parent /
        'candidates' / 'attachment-prefect-live-link-20261031' / 'attachment-queue-config.json')
    config = read_json(config_path)
    if not isinstance(config, dict) or set(config) != {'queue_db', 'receipt_root', 'journal_root'}:
        raise ValueError('已登记回执的来源配置无效')
    queue = AttachmentQueue(config['queue_db'], config['receipt_root'])
    record = queue.verified(receipt_id, require_attachments=False)
    if Path(record['receipt_path']).resolve() != Path(receipt_path).resolve():
        raise ValueError('回执路径与已保留的登记不一致')
    return record
