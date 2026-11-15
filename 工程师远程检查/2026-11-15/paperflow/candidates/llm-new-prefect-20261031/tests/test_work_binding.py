"""Resolve existing source-bound work before any body or attachment request."""
import importlib.util
from pathlib import Path
import sys

import pytest


HERE = Path(__file__).resolve().parents[1]
ROOT = HERE.parents[1]
sys.path[:0] = [str(HERE), str(ROOT / 'prefect-paperflow')]
spec = importlib.util.spec_from_file_location('work_binding', HERE / 'work_binding.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
from paperflow_prefect import request_admission


@pytest.fixture
def binding(tmp_path, monkeypatch):
    body_work, attachment_work = tmp_path / 'body', tmp_path / 'attachment'
    body_work.mkdir()
    attachment_work.mkdir()
    body = {'kind': 'body', 'file_id': 'body-1', 'sha256': 'a' * 64, 'relative': 'body.pdf'}
    attachment = {'kind': 'attachment', 'file_id': 'att-1', 'sha256': 'b' * 64, 'relative': 'att.zip'}
    paper = {'paper_id': 'paper-1', 'revision': 'rev-1', 'body': body, 'attachments': [attachment]}
    manifest = {'paper_id': 'paper-1', 'revision': 'rev-1', 'body': body,
                'attachments': [attachment], 'body_work': str(body_work)}
    calls = []
    class Backend:
        root = tmp_path
        def body_work(self, given):
            assert given == paper
            return body_work
        def attachment_work(self, given, row):
            assert given == paper and row == attachment
            return attachment_work
        def confirm_history(self, given, row): calls.append(('confirm', row['file_id']))
    monkeypatch.setattr(request_admission, 'require_model_history',
                        lambda p, r, w: calls.append(('history', r['file_id'], Path(w))))
    monkeypatch.setattr(request_admission, 'bind_work',
                        lambda root, p, r, kind, w: calls.append(('bind', r['file_id'], kind, Path(w))))
    return module.WorkBinding(Backend(), paper, manifest), body_work, attachment_work, body, attachment, calls


def test_body_and_attachment_use_their_existing_source_bound_directories(binding):
    selected, body_work, att_work, body, att, calls = binding
    assert selected.body_work() == body_work
    assert selected.attachment_work(att) == att_work
    selected.check_body_history()
    selected.verify_attachment_work(att, att_work)
    selected.check_attachment_history(att, att_work)
    assert [('confirm', 'body-1'), ('confirm', 'att-1')] == [x for x in calls if x[0] == 'confirm']
    assert {x[1] for x in calls if x[0] == 'history'} == {'body-1', 'att-1'}
    assert {x[2] for x in calls if x[0] == 'bind'} == {'body', 'attachment'}


def test_foreign_work_or_changed_receipt_is_rejected_before_history(binding, tmp_path):
    selected, _, _, _, att, calls = binding
    foreign = dict(att, sha256='c' * 64)
    with pytest.raises(ValueError, match='附件'):
        selected.attachment_work(foreign)
    with pytest.raises(ValueError, match='目录'):
        selected.verify_attachment_work(att, tmp_path / 'foreign')
    assert not calls
