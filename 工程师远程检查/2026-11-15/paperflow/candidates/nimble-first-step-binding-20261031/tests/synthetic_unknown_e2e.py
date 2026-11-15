"""Exercise unknown-group isolation in the synthetic Prefect integration instance."""
import base64
import json
import uuid

import httpx

from synthetic_prefect_e2e import API, expect, wait_state


def main():
    root = '合成未知-' + uuid.uuid4().hex[:8]
    files = [(root + '/甲/MOCK_UNKNOWN.txt', b'synthetic body unknown-' + root.encode()),
             (root + '/乙/论文.txt', '题目：合成论文\n摘要：这是测试。'.encode())]
    writer = str(uuid.uuid4())
    with httpx.Client(timeout=15) as client:
        task = expect(client.post(API + '/api/v1/first-step/tasks',
                                  json={'name': '合成未知分组', 'request_id': str(uuid.uuid4())}))['task_id']
        upload = expect(client.post(API + f'/api/v1/first-step/tasks/{task}/uploads',
                                    json={'root_name': root}))['upload_id']
        folder = API + f'/api/v1/first-step/uploads/{upload}'
        expect(client.post(folder + '/manifest', json={'files': [
            {'relative_path': path, 'size': len(data)} for path, data in files]}))
        expect(client.post(folder + '/seal', json={'expected_files': 2}))
        bound = {'X-Task-ID': task, 'X-Folder-Upload-ID': upload, 'X-Writer-ID': writer,
                 'Tus-Resumable': '1.0.0'}
        for path, data in files:
            meta = {'task_id': task, 'folder_upload_id': upload, 'relativePath': path}
            encoded = ','.join(k + ' ' + base64.b64encode(v.encode()).decode() for k, v in meta.items())
            created = client.post(API + '/tus/files/', content=b'', headers={**bound,
                'Upload-Length': str(len(data)), 'Upload-Metadata': encoded, 'X-Request-ID': str(uuid.uuid4())})
            expect(created, 201)
            expect(client.patch(created.headers['Location'], content=data, headers={**bound,
                'Upload-Offset': '0', 'Content-Type': 'application/offset+octet-stream'}), 204)
        ready = expect(client.post(folder + '/finish'))
        expect(client.post(API + f'/api/v1/first-step/tasks/{task}/start',
                           json={'expected_revision': ready['control_revision']}))
        done = wait_state(client, task, lambda s: s['task_status'] in ('complete', 'needs_review', 'unknown'))
        unknown = expect(client.get(API + f'/api/v1/first-step/tasks/{task}', params={'filter': 'unknown'}))
        assert done['task_status'] == 'needs_review' and done['unknown_groups'] == 1, done
        assert done['processed_groups'] == 1 and done['passed_groups'] == 1, done
        assert unknown['list_total'] == 1 and unknown['items'][0]['display_hash'] and unknown['items'][0]['reason'], unknown
        print('task_id', task)
        print(json.dumps({key: done[key] for key in ('task_status', 'total_groups', 'processed_groups',
                    'passed_groups', 'unknown_groups', 'unknown_requests')}, ensure_ascii=False))
        print('unknown reason', unknown['items'][0]['reason'])


if __name__ == '__main__':
    main()
