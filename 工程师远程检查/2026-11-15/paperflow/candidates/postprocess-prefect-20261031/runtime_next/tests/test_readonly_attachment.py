"""Existing synthetic attachment results must be checked without any writes."""
import json
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT), str(ROOT / 'tests'), str(ROOT / 'runtime_next')]
from test_attachment_adapter import fixture, adapter
from paperflow_prefect.records import sha_file, write_json
from generation_attachment_adapter import process_attachment


@pytest.mark.parametrize('rich', [False, True])
def test_old_attachment_card_is_revalidated_without_writing_any_file(tmp_path, monkeypatch, rich):
    ticket, handoff, _, _ = fixture(tmp_path, rich=rich)
    out = tmp_path / 'output'
    original = adapter().process_attachment(ticket, handoff, out)
    before = {p: (sha_file(p), p.stat().st_mtime_ns) for p in tmp_path.rglob('*') if p.is_file()}
    old_open = Path.open
    def no_write(path, mode='r', *args, **kwargs):
        if any(char in mode for char in 'wax+'):
            pytest.fail('revalidation attempted to write')
        return old_open(path, mode, *args, **kwargs)
    def no_mkdir(*_args, **_kwargs): pytest.fail('revalidation attempted to create directory')
    monkeypatch.setattr(Path, 'open', no_write)
    monkeypatch.setattr(Path, 'mkdir', no_mkdir)
    assert process_attachment(ticket, handoff, out) == original
    assert {p: (sha_file(p), p.stat().st_mtime_ns) for p in before} == before


def test_self_rehashed_but_altered_attachment_card_is_rejected(tmp_path):
    ticket, handoff, _, _ = fixture(tmp_path, rich=True)
    out = tmp_path / 'output'
    result = adapter().process_attachment(ticket, handoff, out)
    md = Path(result['delivery']['md_path'])
    md.write_bytes(md.read_bytes().replace('压缩包'.encode(), '资料包'.encode()))
    result['delivery']['md_sha256'] = sha_file(md)
    result['render']['md_sha256'] = sha_file(md)
    result['artifacts']['md']['sha256'] = sha_file(md)
    delivery_path = md.parent / 'delivery.json'
    write_json(delivery_path, result['render'])
    result['artifacts']['delivery']['sha256'] = sha_file(delivery_path)
    write_json(out / 'receipt.json', result)
    with pytest.raises(ValueError): process_attachment(ticket, handoff, out)
