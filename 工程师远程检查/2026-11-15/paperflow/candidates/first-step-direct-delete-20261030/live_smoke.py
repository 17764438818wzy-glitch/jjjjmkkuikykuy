"""Exercise live intake and deletion with disposable files; never start classification."""
import json
import os
from pathlib import Path
import stat
import tempfile
import uuid
from urllib.request import ProxyHandler, Request, build_opener

BASE = 'http://127.0.0.1:14203/api/v1/first-step/tasks'
OUTPUT = (Path(__file__).resolve().parent.parent / 'first-step-control-20261029' /
          'runtime' / 'workspace' / 'output')
opener = build_opener(ProxyHandler({}))


def call(url, method='GET', data=None):
    payload = None if data is None else json.dumps(data, ensure_ascii=False).encode('utf-8')
    request = Request(url, data=payload, method=method, headers={'Content-Type': 'application/json'})
    with opener.open(request, timeout=30) as response:
        return json.load(response)


before = call(BASE)
prior_ids = {row['task_id'] for row in before['items']}
with tempfile.TemporaryDirectory(prefix='paperflow-first-step-smoke-') as tmp:
    source = Path(tmp) / '原始资料'
    group = source / '测试资料组'
    group.mkdir(parents=True)
    files = [group / '正文.docx', group / '附件.zip']
    for path in files:
        path.write_text('合成测试文件', encoding='utf-8')
    task = call(BASE, 'POST', {'name': '合成删除功能验证', 'request_id': str(uuid.uuid4())})
    task_id = task['task_id']
    print('SYNTHETIC_TASK', task_id, flush=True)
    selected = call(f'{BASE}/{task_id}/local-source', 'POST', {'path': str(source)})
    assert selected['input_files'] == 2 and selected['total_groups'] == 1, selected
    artifact = OUTPUT / 'tasks' / task_id
    for relative in ('deliveries/group/交付文件.docx', 'classification-intents/group.json',
                     'conversion/office/cache/正文.docx', 'attachment-evidence/group/证据.json'):
        path = artifact / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('合成中间产物', encoding='utf-8')
    readonly = artifact / 'conversion/office/cache/正文.docx'
    os.chmod(readonly, stat.S_IREAD)
    deleted = call(f'{BASE}/{task_id}', 'DELETE')
    assert deleted['deleted'] and not artifact.exists(), deleted
    assert all(path.is_file() for path in files)
    after = call(BASE)
    assert {row['task_id'] for row in after['items']} == prior_ids
    print('LIVE_SMOKE_OK discovered=1 synthetic_artifacts_removed=4 original_files_preserved=2', flush=True)
    print('DELETE_RESPONSE', json.dumps(deleted, ensure_ascii=False), flush=True)
