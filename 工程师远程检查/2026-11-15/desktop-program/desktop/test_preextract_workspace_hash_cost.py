import json
from pathlib import Path

import pytest


def workspace_fixture(tmp_path, monkeypatch, entries=200):
    import preextract_pipeline as module
    monkeypatch.setattr(module, 'ROOT', tmp_path)
    source = tmp_path / 'paper.docx'
    source.write_bytes(b'complete immutable source bytes')
    template = tmp_path / 'template.md'
    template.write_text('all existing extraction rules', encoding='utf8')
    registry = tmp_path / 'data/single-request/source-index.json'
    registry.parent.mkdir(parents=True)
    records = {f'{i:064x}|template|old-parser': str(tmp_path / f'old-{i}') for i in range(entries)}
    registry.write_text(json.dumps(records), encoding='utf8')
    original = module.file_sha
    calls = []

    def observed(path):
        if Path(path) == source:
            calls.append(str(path))
        return original(path)

    monkeypatch.setattr(module, 'file_sha', observed)
    return module, source, template, registry, records, calls, original


@pytest.mark.parametrize('entries', [0, 200])
def test_workspace_lookup_reads_source_once_independent_of_registry_size(tmp_path, monkeypatch, entries):
    module, source, template, registry, before, calls, original = workspace_fixture(tmp_path, monkeypatch, entries)
    work = module.default_work_dir(source, template)
    assert work.parent == registry.parent
    assert work.name.startswith(original(source)[:16] + '-')
    assert len(calls) == 1
    assert json.loads(registry.read_text()) == before


@pytest.mark.parametrize('entries', [0, 200])
def test_workspace_registration_reads_source_once_and_keeps_history(tmp_path, monkeypatch, entries):
    module, source, template, registry, before, calls, original = workspace_fixture(tmp_path, monkeypatch, entries)
    work = tmp_path / 'new-private-workspace'
    module.register_workspace(source, template, work)
    after = json.loads(registry.read_text())
    identity = original(source) + '|' + original(template) + '|' + module.PARSER_VERSION
    assert after == {**before, identity: str(work.resolve())}
    assert len(calls) == 1


def test_old_model_history_is_reused_across_template_versions_and_cannot_be_rebound(tmp_path, monkeypatch):
    module, source, template, registry, records, calls, original = workspace_fixture(tmp_path, monkeypatch)
    prior = tmp_path / 'prior-model-workspace'
    (prior / 'model').mkdir(parents=True)
    (prior / 'model/request-state.json').write_text('{"status":"request_in_flight"}')
    records[original(source) + '|old-template|old-parser'] = str(prior)
    registry.write_text(json.dumps(records), encoding='utf8')
    assert module.default_work_dir(source, template) == prior.resolve()
    assert len(calls) == 1
    calls.clear()
    with pytest.raises(RuntimeError, match='模型请求记录'):
        module.register_workspace(source, template, tmp_path / 'other')
    assert len(calls) == 1
    assert json.loads(registry.read_text()) == records
    assert json.loads((prior / 'model/request-state.json').read_text())['status'] == 'request_in_flight'


def test_source_hash_is_fresh_for_each_operation(tmp_path, monkeypatch):
    module, source, template, registry, records, calls, original = workspace_fixture(tmp_path, monkeypatch)
    first = module.default_work_dir(source, template)
    source.write_bytes(b'a changed source must have a new identity')
    second = module.default_work_dir(source, template)
    assert second != first
    assert second.name.startswith(original(source)[:16] + '-')
    assert len(calls) == 2


def test_registration_hashes_after_registry_lock_if_source_changes_while_waiting(tmp_path, monkeypatch):
    import filelock
    module, source, template, registry, records, calls, original = workspace_fixture(tmp_path, monkeypatch)
    changed = b'new bytes already owned by an unresolved request'
    other = tmp_path / 'other-model-workspace'
    (other / 'model').mkdir(parents=True)
    (other / 'model/request-state.json').write_text('{"status":"request_in_flight"}')
    import hashlib
    records[hashlib.sha256(changed).hexdigest() + '|other-template|old-parser'] = str(other)
    registry.write_text(json.dumps(records), encoding='utf8')
    original_lock = filelock.FileLock

    class ChangeWhileWaiting:
        def __init__(self, *args, **kwargs):
            self.lock = original_lock(*args, **kwargs)

        def __enter__(self):
            source.write_bytes(changed)
            return self.lock.__enter__()

        def __exit__(self, *args):
            return self.lock.__exit__(*args)

    monkeypatch.setattr(filelock, 'FileLock', ChangeWhileWaiting)
    with pytest.raises(RuntimeError, match='模型请求记录'):
        module.register_workspace(source, template, tmp_path / 'new-workspace')
    assert json.loads(registry.read_text()) == records
    assert len(calls) == 1
