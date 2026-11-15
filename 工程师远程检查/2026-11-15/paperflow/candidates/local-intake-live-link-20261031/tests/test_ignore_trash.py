import json
from pathlib import Path
import subprocess
import sys

import pytest


@pytest.fixture
def registered_with_trash(tmp_path):
    from test_registered_input import UPSTREAM, FIXTURES

    script = r'''
import json, sys
from pathlib import Path
sys.path[:0] = sys.argv[1:3]
from paperflow_prefect.first_step_registration import register_delivery
from paperflow_prefect.group_classification import process_one_group
from test_first_step_registration import FixtureJudge, evidence

class TrashJudge(FixtureJudge):
    def choose_other(self, record, body_id):
        return {'choice': 'trash', 'model': 'fixture', 'confidence': 1,
                'probabilities': {'attachment': 0, 'trash': 1, 'uncertain': 0}}

base = Path(sys.argv[3])
library = base / 'library'
group = library / 'person'
group.mkdir(parents=True)
(group / 'paper.txt').write_text('题目：合成论文\n摘要：测试正文', encoding='utf8')
(group / '开题报告.rtf').write_text('开题报告\n合成过程材料', encoding='utf8')
(group / 'data.csv').write_text('date,value\n1,2', encoding='utf8')
delivery = process_one_group(group, library, base / 'output', TrashJudge, evidence, verified_new=True)
assert delivery['delivery_complete'] is True, delivery
root = base / 'receipts'
receipt = register_delivery(delivery['delivery_dir'], receipt_root=root, history_roots=(),
                            lock_path=base / 'locks' / 'receipt.lock')
handoff = json.loads((Path(delivery['delivery_dir']) / 'handoff.json').read_text(encoding='utf8'))
assert len(handoff['trash']) == 1, handoff
assert len(receipt['inventory']['attachments']) == 1, receipt
item = dict(status='passed', registration_status='registered', delivery_complete=True,
            group_id=receipt['group_id'], paper_id=receipt['paper_id'],
            receipt_id=receipt['receipt_id'], receipt_path=receipt['record_path'],
            delivery_dir=delivery['delivery_dir'])
attachment = receipt['inventory']['attachments'][0]
print(json.dumps({'item': item, 'root': str(root), 'trash': str(Path(delivery['delivery_dir']) /
                 'files' / handoff['trash'][0]['relative']), 'body': receipt['inventory']['body']['path'],
                 'attachment': attachment['path'], 'evidence': attachment['evidence_records'][0]['path'],
                 'receipt': receipt['record_path']}, ensure_ascii=False))
'''
    result = subprocess.run([sys.executable, '-B', '-X', 'utf8', '-c', script,
                             str(UPSTREAM), str(FIXTURES), str(tmp_path)], capture_output=True,
                            encoding='utf8', text=True, timeout=30,
                            creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def test_missing_discarded_file_does_not_block_verified_body_and_attachment(registered_with_trash, tmp_path):
    from local_queue import LocalQueue

    fixture = registered_with_trash
    Path(fixture['trash']).unlink()
    queue = LocalQueue(tmp_path / 'local-queue.sqlite3')
    accepted = queue.receive('synthetic-task', fixture['item'], receipt_root=fixture['root'])
    assert accepted['state'] == 'waiting'
    assert accepted['body']['file_id']
    assert len(accepted['attachments']) == 1
    assert 'trash' not in accepted


@pytest.mark.parametrize('damage', ['body', 'attachment', 'evidence', 'receipt'])
def test_missing_trash_does_not_weaken_body_attachment_or_evidence_checks(registered_with_trash, tmp_path, damage):
    from local_queue import LocalQueue

    fixture = registered_with_trash
    Path(fixture['trash']).unlink()
    Path(fixture[damage]).write_bytes(b'synthetic corruption')
    queue = LocalQueue(tmp_path / 'local-queue.sqlite3')
    with pytest.raises((OSError, ValueError, KeyError)):
        queue.receive('synthetic-task', fixture['item'], receipt_root=fixture['root'])
    assert queue.list_waiting() == []
