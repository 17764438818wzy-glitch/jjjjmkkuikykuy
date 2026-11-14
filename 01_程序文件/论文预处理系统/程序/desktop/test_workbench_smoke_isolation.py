import json
import subprocess
import sys
from pathlib import Path


def test_smoke_test_does_not_mark_a_live_batch_interrupted(tmp_path):
    run = tmp_path / 'runs' / 'AUTO_test'
    run.mkdir(parents=True)
    manifest = {'run_id': 'AUTO_test', 'status': 'running',
                'started_at': '2026-09-25T00:00:00', 'message': '正在处理',
                'items': [], 'inputs': [], 'events': [],
                'processed_sources': 0, 'source_count': 1}
    path = run / 'manifest.json'
    path.write_text(json.dumps(manifest, ensure_ascii=False), encoding='utf-8')
    result = subprocess.run([sys.executable, str(Path(__file__).with_name('app.py')),
                             '--smoke-test', '--data-dir', str(tmp_path)],
                            capture_output=True, text=True, timeout=25)
    assert result.returncode == 0, result.stderr
    assert json.loads(path.read_text(encoding='utf-8'))['status'] == 'running'
