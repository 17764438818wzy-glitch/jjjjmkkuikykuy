"""Run one synthetic Word document through the isolated Prefect and real local Nimble."""
import base64
from io import BytesIO
import json
import os
from pathlib import Path
import uuid

from docx import Document
import httpx

from synthetic_prefect_e2e import API, PREFECT, expect, wait_state
from paperflow_prefect.group_delivery import load_delivery


def main():
    profile = Path(os.environ['PAPERFLOW_CONFIG']).resolve()
    if profile.name != 'integration-config.json':
        raise RuntimeError('real-model synthetic test requires the integration profile')
    original = profile.read_bytes()
    config = json.loads(original)
    if config['prefect_api_url'] != PREFECT or config['nimble_endpoint'] != 'http://127.0.0.1:11436/v1/systemone':
        raise RuntimeError('unexpected integration configuration')
    config['nimble_endpoint'] = 'http://127.0.0.1:11435/v1/systemone'
    tmp = profile.with_name(profile.name + '.synthetic.tmp')
    tmp.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding='utf8')
    os.replace(tmp, profile)
    try:
        document = Document()
        document.add_paragraph('题目：校园体育活动参与情况研究')
        document.add_paragraph('摘要：本文以校园体育活动为例，讨论学生参与现状和改进建议。')
        document.add_paragraph('关键词：校园体育；活动参与；学生')
        document.add_paragraph('第一章 绪论')
        document.add_paragraph('本文使用虚构样本分析校园体育活动的参与情况。')
        body = BytesIO()
        document.save(body)
        data = body.getvalue()
        relative = '虚构论文样本/论文.docx'
        writer = str(uuid.uuid4())
        with httpx.Client(timeout=15) as client:
            task = expect(client.post(API + '/api/v1/first-step/tasks',
                json={'name': '本机模型合成验收', 'request_id': str(uuid.uuid4())}))['task_id']
            upload = expect(client.post(API + f'/api/v1/first-step/tasks/{task}/uploads',
                                       json={'root_name': '虚构论文样本'}))['upload_id']
            folder = API + f'/api/v1/first-step/uploads/{upload}'
            expect(client.post(folder + '/manifest', json={'files': [{'relative_path': relative, 'size': len(data)}]}))
            expect(client.post(folder + '/seal', json={'expected_files': 1}))
            meta = {'task_id': task, 'folder_upload_id': upload, 'relativePath': relative}
            encoded = ','.join(k + ' ' + base64.b64encode(v.encode()).decode() for k, v in meta.items())
            bound = {'X-Task-ID': task, 'X-Folder-Upload-ID': upload, 'X-Writer-ID': writer,
                     'Tus-Resumable': '1.0.0'}
            created = client.post(API + '/tus/files/', content=b'', headers={**bound,
                'Upload-Length': str(len(data)), 'Upload-Metadata': encoded, 'X-Request-ID': str(uuid.uuid4())})
            expect(created, 201)
            expect(client.patch(created.headers['Location'], content=data, headers={**bound,
                'Upload-Offset': '0', 'Content-Type': 'application/offset+octet-stream'}), 204)
            ready = expect(client.post(folder + '/finish'))
            assert ready['total_groups'] == 1
            expect(client.post(API + f'/api/v1/first-step/tasks/{task}/start',
                               json={'expected_revision': ready['control_revision']}))
            done = wait_state(client, task, lambda s: s['task_status'] in ('complete', 'needs_review', 'unknown'), 120)
            assert done['task_status'] == 'complete' and done['passed_groups'] == 1, done
            row = expect(client.get(API + f'/api/v1/first-step/tasks/{task}', params={'filter': 'passed'}))['items'][0]
            assert row['title_status'] == '已提取' and row['paper_title'] == '校园体育活动参与情况研究', row
            handoff, delivery = load_delivery(row['delivery_dir'])
            assert handoff.is_file() and delivery['manifest']['ready_for_receipt']
            print('task_id', task)
            print('local real Nimble synthetic Word classified; title status', row['title_status'])
            print('delivery validated, processed', done['processed_groups'])
    finally:
        tmp.write_bytes(original)
        os.replace(tmp, profile)


if __name__ == '__main__':
    main()
