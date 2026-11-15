from pathlib import Path
import sys
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from paperflow_prefect.llm_queue import attachment_raw_input as module


def test_user_retry_can_prepare_previously_failed_attachment(monkeypatch, tmp_path):
    row = {'history': {'status': 'unknown'}}
    monkeypatch.setattr(module, 'check_attachment_source', lambda _: None)
    monkeypatch.setattr(module, '_other_revisions', lambda _: False)
    monkeypatch.setattr(module.attachment_input, 'require_upstream_history_located', lambda _: None)
    monkeypatch.setattr(module.attachments, 'evidence_for', lambda _: {'limitations': []})
    monkeypatch.setattr(module.attachments, '_model_context_v1', lambda _: {'text_truncated': False})
    monkeypatch.setattr(module.attachments, '_immutable_json', lambda *_: None)
    monkeypatch.setattr(module.attachments, '_request_body', lambda *_: {'model': 'synthetic'})
    with pytest.raises(module.attachments.AttachmentError):
        module.prepare_attachment_body(row, tmp_path, {})
    assert module.prepare_attachment_body(row, tmp_path, {}, allow_manual_retry=True) == {'model': 'synthetic'}
