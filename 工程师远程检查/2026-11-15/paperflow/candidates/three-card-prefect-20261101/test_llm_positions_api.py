"""The existing summary publishes folder positions with their stage counts."""
import importlib.util
from pathlib import Path
import sys
import time
from types import SimpleNamespace

from fastapi.testclient import TestClient


HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('three_card_candidate_api', HERE / 'waiting_api.py')
api = importlib.util.module_from_spec(spec)
spec.loader.exec_module(api)


def test_summary_exposes_deduplicated_current_folder_positions(monkeypatch):
    observed = {'waiting_llm': 0, 'llm_body_in_progress': 1, 'llm_attachment_in_progress': 1,
                'llm_processing_folders': 1, 'llm_processing_folders_partial': False,
                'llm_folder_positions': [{'task_id': 'task-a', 'group_id': 'folder-a',
                    'stage': 'llm_processing', 'body': True, 'attachment': True}],
                'waiting_llm_partial': False, 'llm_body_in_progress_partial': False,
                'llm_attachment_in_progress_partial': False, 'as_of': '2026-11-01T00:00:00Z'}
    monkeypatch.setitem(sys.modules, 'llm_queue_summary', SimpleNamespace(summarize_llm=lambda *_: dict(observed)))
    class Queue:
        def snapshot(self, **_):
            return dict(receiver_status='running', receiver_session_id='test', scan_source_instance='test',
                        waiting_available=True, upstream_scan_verified=True, waiting_llm_total=1,
                        in_progress_count=0, as_of='2026-11-01T00:00:00Z')
    class Observer:
        index = 'synthetic'
        def summary(self, _): return {'source': 'prefect', 'scope': 'all_tasks', 'available': True, 'counts': {}}
        def preprocessing_results(self, *_): return {'available': True, 'counts': {}}
        def llm_results(self, *_): return {'available': True, 'counts': {}}
    with TestClient(api.create_app(Queue(), ticket_observer=Observer())) as client:
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            summary = client.get('/api/v1/local-intake/preprocessing/summary').json()['prefect_ticket_summary']
            if summary.get('llm_folder_positions'):
                break
            time.sleep(.01)
    assert summary['counts']['llm_processing_folders'] == 1
    assert summary['llm_folder_positions'] == observed['llm_folder_positions']
    assert summary['llm_processing_folders_partial'] is False
