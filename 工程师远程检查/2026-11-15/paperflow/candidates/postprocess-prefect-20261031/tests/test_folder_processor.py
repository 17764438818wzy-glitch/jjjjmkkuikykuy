import copy
import hashlib
import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import folder_processor


def ref(path):
    return {'path': str(path.resolve()), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False), encoding='utf8')
    return ref(path)


class Store:
    def __init__(self): self.calls = []
    def claim(self, identity, generation, digest):
        self.calls.append(('claim', generation, digest))
        return {'claim_id': 'claim-1'}
    def complete(self, identity, claim_id, receipt, *, validate_receipt):
        self.calls.append(('complete', claim_id))
        assert validate_receipt(copy.deepcopy(receipt)) is True
        self.receipt = receipt
        return {'state': 'waiting_rag'}
    def mark_unknown(self, identity, claim_id, reason): self.calls.append(('unknown', claim_id))
    def reconcile(self, identity, claim_id, generation, *, validate_receipt):
        self.calls.append(('reconcile', claim_id, generation))
        assert validate_receipt(copy.deepcopy(self.receipt)) is True
        return {'state': 'waiting_rag'}


def scenario(tmp_path):
    identity = dict(task_id='task', group_id='group', paper_id='paper', receipt_id='r' * 64,
                    attempt_id='attempt', parent_run_id='parent', task_run_id='folder')
    original = write(tmp_path / 'original' / 'attachment.zip', 'synthetic source')
    inputs = dict(identity=copy.deepcopy(identity), manifest_sha256='a' * 64,
                  body={'identity': copy.deepcopy(identity)},
                  attachments=[{'ticket': copy.deepcopy(identity), 'attachment_id': 'att-1'}])
    store, counts = Store(), {'body': 0, 'attachment': 0}

    def body(data, directory):
        counts['body'] += 1
        root = Path(directory); package = root / 'publication'
        if counts['body'] == 1:
            article = write(package / 'article.redacted.md', 'example')
            manifest = write(package / 'manifest.json', {'files': [dict(path='article.redacted.md',
                sha256=article['sha256'], bytes=(package / 'article.redacted.md').stat().st_size)]})
            write(package / 'READY.json', {'manifest_sha256': manifest['sha256']})
            write(root / 'body-result.json', {'synthetic': True})
        return dict(schema='body-postprocess-result/1', status='body_package_verified',
                    identity=identity, package_path=str(package.resolve()),
                    manifest_sha256=ref(package / 'manifest.json')['sha256'])

    def attachment(ticket, data, directory):
        counts['attachment'] += 1
        root = Path(directory)
        if counts['attachment'] == 1:
            md = write(root / 'attachment.md', 'attachment')
            write(root / 'delivery.json', {'synthetic': True})
            write(root / 'receipt.json', {'synthetic': True})
        md = ref(root / 'attachment.md')
        return dict(schema='attachment-postprocess-result/1', status='complete', ticket=identity,
            delivery=dict(paper_id='paper', attachment_id=data['attachment_id'], source_path=original['path'],
                source_sha256=original['sha256'], md_path=md['path'], md_sha256=md['sha256'], delivery_version='v' * 64),
            artifacts={'md': md, 'delivery': ref(root / 'delivery.json')})

    def run(**changes):
        reconcile = changes.pop('_reconcile', False)
        check_existing = changes.pop('_check_existing', False)
        args = dict(identity=identity, generation=2, manifest_sha256='a' * 64, store=store,
            load_inputs=lambda: copy.deepcopy(inputs), body_processor=body, attachment_processor=attachment,
            output_root=tmp_path / 'safe-output', output_guard=lambda evidence, directory: None)
        args.update(changes)
        if check_existing:
            args.pop('store')
            args.pop('generation')
            validator = folder_processor.make_existing_receipt_validator(claim_id='claim-1', **args)
            return validator(copy.deepcopy(store.receipt))
        if reconcile:
            args['claim_id'] = 'claim-1'
            return folder_processor.reconcile_folder(**args)
        return folder_processor.process_folder(**args)
    return run, store, counts, inputs


def test_whole_folder_is_validated_and_receipt_includes_all_deliveries(tmp_path):
    run, store, counts, _ = scenario(tmp_path)
    assert run()['state'] == 'waiting_rag'
    assert counts == {'body': 2, 'attachment': 2}
    assert [item['attachment_id'] for item in store.receipt['attachments']] == ['att-1']
    names = {Path(item['path']).name for item in store.receipt['artifacts']}
    assert {'body-result.json', 'manifest.json', 'READY.json', 'article.redacted.md', 'attachment.md',
            'delivery.json', 'receipt.json'} <= names


def test_missing_or_conflicting_inputs_do_not_claim(tmp_path):
    run, store, _, inputs = scenario(tmp_path)
    inputs['attachments'][0]['ticket']['task_run_id'] = 'other-folder'
    with pytest.raises(ValueError): run()
    assert store.calls == []


def test_failure_after_claim_retains_unknown_without_advancing(tmp_path):
    run, store, _, _ = scenario(tmp_path)
    def broken(*args): raise ValueError('damaged saved reply')
    with pytest.raises(ValueError, match='damaged'):
        run(attachment_processor=broken)
    assert [c[0] for c in store.calls] == ['claim', 'unknown']


def test_inputs_changed_while_processing_are_not_handed_off(tmp_path):
    run, store, _, inputs = scenario(tmp_path)
    count = 0
    def reload():
        nonlocal count
        count += 1
        value = copy.deepcopy(inputs)
        if count > 1: value['manifest_sha256'] = 'changed'
        return value
    with pytest.raises(ValueError): run(load_inputs=reload)
    assert [c[0] for c in store.calls] == ['claim', 'unknown']


def test_attachment_original_is_bound_in_persisted_receipt(tmp_path):
    run, store, _, _ = scenario(tmp_path)
    run()
    delivery = store.receipt['attachments'][0]
    assert {'path': delivery['source_path'], 'sha256': delivery['source_sha256']} in store.receipt['artifacts']


def test_output_guard_runs_before_claim_or_any_directory_creation(tmp_path):
    run, store, _, _ = scenario(tmp_path)
    protected = tmp_path / 'original' / 'new-directory'
    def reject(evidence, directory):
        assert not protected.exists()
        raise ValueError('protected output')
    with pytest.raises(ValueError, match='protected output'):
        run(output_root=protected, output_guard=reject)
    assert not protected.exists()
    assert store.calls == []


def test_reconcile_rechecks_existing_receipt_without_new_claim_or_output(tmp_path):
    run, store, counts, _ = scenario(tmp_path)
    assert run()['state'] == 'waiting_rag'
    before = {path: path.read_bytes() for path in (tmp_path / 'safe-output').rglob('*') if path.is_file()}
    assert run(_reconcile=True, generation=3)['state'] == 'waiting_rag'
    assert counts == {'body': 3, 'attachment': 3}
    assert [call[0] for call in store.calls] == ['claim', 'complete', 'reconcile']
    assert all(path.read_bytes() == raw for path, raw in before.items())


def test_existing_receipt_can_be_independently_verified_without_transition(tmp_path):
    run, store, _, _ = scenario(tmp_path)
    run()
    before = {path: path.read_bytes() for path in (tmp_path / 'safe-output').rglob('*') if path.is_file()}
    assert run(_check_existing=True) is True
    assert [call[0] for call in store.calls] == ['claim', 'complete']
    assert {path: path.read_bytes() for path in before} == before
    store.receipt['attachments'][0]['md_sha256'] = '0' * 64
    assert run(_check_existing=True) is False


def test_existing_receipt_validation_cannot_recreate_missing_outputs(tmp_path):
    run, store, counts, _ = scenario(tmp_path)
    run()
    target = next(path for path in (tmp_path / 'safe-output').rglob('receipt.json'))
    target.unlink()
    before = dict(counts)
    with pytest.raises(ValueError, match='attachment result is missing'):
        run(_check_existing=True)
    assert not target.exists()
    assert counts == before
    assert [call[0] for call in store.calls] == ['claim', 'complete']


@pytest.mark.parametrize('missing', ['body-result.json', 'receipt.json'])
def test_reconcile_does_not_recreate_missing_result(tmp_path, missing):
    run, store, counts, _ = scenario(tmp_path)
    run()
    target = next(path for path in (tmp_path / 'safe-output').rglob(missing))
    target.unlink()
    before = dict(counts)
    with pytest.raises(ValueError): run(_reconcile=True, generation=3)
    assert not target.exists()
    if missing == 'body-result.json': assert counts == before
    else: assert counts['attachment'] == before['attachment']
