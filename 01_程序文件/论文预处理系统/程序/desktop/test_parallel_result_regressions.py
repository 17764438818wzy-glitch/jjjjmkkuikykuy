"""Exercise archive fan-out and retry paths without any model requests."""
import json
from pathlib import Path
import threading
import zipfile

from auto_pipeline import AutoProcessor
from engine import Store, digest


CONFIG = {'concurrency': {'enabled': True, 'papers': 2, 'images_per_paper': 1,
                          'global_llm': 2, 'max_retries': 1},
          'resume': {'enabled': False}, 'auto_upload': False}


class NoNetwork:
    def open(self, *args, **kwargs):
        raise AssertionError('These synthetic documents must fail screening before AI')


def processor(tmp_path):
    proc = AutoProcessor(Store(tmp_path/'data'), CONFIG)
    proc.agent.opener = NoNetwork()
    return proc


def record(path):
    return {'path': str(path), 'name': path.name}


def archive(path, members):
    with zipfile.ZipFile(path, 'w') as stream:
        for name, text in members.items():
            stream.writestr(name, text)
    return path


def test_parallel_archive_keeps_every_terminal_item_and_durable_retry_source(tmp_path):
    zipped = archive(tmp_path/'pair.zip', {'one.txt': 'short one', 'two.txt': 'short two'})
    loose = tmp_path/'loose.txt'
    loose.write_text('short loose', encoding='utf-8')
    original = {path: path.read_bytes() for path in (zipped, loose)}
    proc = processor(tmp_path)
    result = proc.run([record(zipped), record(loose)])
    assert result['status'] == 'completed'
    assert len(result['items']) == 3
    assert result['processed_sources'] == result['source_count'] == 2
    assert len(result['child_runs']) == 2
    for child in result['child_runs']:
        assert Path(child['manifest_path']).is_file()
        assert json.loads(Path(child['manifest_path']).read_text(encoding='utf-8'))['run_id'] == child['run_id']
    for item in result['items']:
        assert item['status'] == 'reject' and item['reason_kind'] == 'screening'
        assert Path(item['path']).is_file()
        assert Path(item['source_path']).is_file()
        assert digest(Path(item['source_path'])) == item['source_hash']
    from app import retry_sources
    retries = retry_sources(result)
    assert len(retries) == 3 and all(Path(row['path']).is_file() for row in retries)
    assert all(path.read_bytes() == before for path, before in original.items())


def test_archive_and_loose_copy_are_attempted_only_once_per_batch(tmp_path):
    loose = tmp_path/'duplicate.txt'
    loose.write_text('same short source', encoding='utf-8')
    zipped = archive(tmp_path/'pair.zip', {'duplicate.txt': 'same short source'})
    result = processor(tmp_path).run([record(zipped), record(loose)])
    assert result['status'] == 'completed'
    assert len(result['items']) == 1
    assert sum(e.get('status') == 'duplicate' for e in result['events']) == 1
    assert Path(result['items'][0]['source_path']).is_file()


def test_parallel_children_with_only_skip_events_do_not_crash(tmp_path, monkeypatch):
    import auto_pipeline

    class EmptyWorker:
        def __init__(self, store, config):
            self.store, self.config = store, config
            self.stop = threading.Event()
            self.agent = type('Agent', (), {})()
            self.stage_cache = None
        def run(self, selected, callback):
            return {'run_id': Path(selected[0]['path']).stem, 'status': 'completed',
                    'items': [], 'events': [{'status': 'duplicate', 'source': selected[0]['path'],
                                            'reason': 'already processed'}]}

    proc = processor(tmp_path)
    monkeypatch.setattr(auto_pipeline, 'AutoProcessor', EmptyWorker)
    result = proc.run([record(tmp_path/'a.zip'), record(tmp_path/'b.zip')])
    assert result['status'] == 'completed'
    assert result['processed_sources'] == 2
    assert result['items'] == []
    assert sum(e.get('status') == 'duplicate' for e in result['events']) == 2
    assert not any(e.get('status') == 'worker_error' for e in result['events'])


def test_new_parallel_run_clears_previous_stop_flag(tmp_path):
    first = tmp_path/'first.txt'; first.write_text('short first', encoding='utf-8')
    second = tmp_path/'second.txt'; second.write_text('short second', encoding='utf-8')
    proc = processor(tmp_path)
    proc.stop.set()
    result = proc.run([record(first), record(second)])
    assert result['status'] == 'completed'
    assert len(result['items']) == 2


def test_parallel_stop_does_not_drain_unstarted_sources(tmp_path):
    from app import retry_sources

    paths=[]
    for index in range(24):
        path=tmp_path/f'paper_{index}.txt'
        path.write_text(f'short source {index}',encoding='utf-8')
        paths.append(path)
    proc=processor(tmp_path)
    def stop_after_first_result(event):
        if event.get('processed_sources')==1:
            proc.stop.set()
    result=proc.run([record(path) for path in paths],stop_after_first_result)
    assert result['status']=='stopped'
    assert result['processed_sources']<=2
    assert len(retry_sources(result,scope='script'))==len(paths)


def test_parallel_archive_preserves_all_pass_packages_and_content_hashes(tmp_path):
    from evidence import package_fingerprint
    from test_workbench import paper

    class SyntheticAgent:
        def call(self, task, payload, **kwargs):
            if task == 'privacy_text':
                return {'decision': 'pass', 'reason': 'synthetic fixture', 'sensitive_strings': []}
            if task == 'classify':
                return {'decision': 'pass', 'reason': 'synthetic fixture',
                        'major': '学前教育', 'title': '幼儿阅读活动的案例研究'}
            return {'decision': 'pass', 'reason': 'synthetic fixture'}

    zipped = archive(tmp_path/'two_papers.zip', {'one.txt': paper(), 'two.txt': paper()+'\n附录：另一份案例。'})
    loose = tmp_path/'short.txt'; loose.write_text('short', encoding='utf-8')
    proc = processor(tmp_path)
    proc.agent = SyntheticAgent()
    result = proc.run([record(zipped), record(loose)])
    passed = [i for i in result['items'] if i['status'] == 'pass']
    assert len(result['items']) == 3 and len(passed) == 2
    for item in passed:
        assert Path(item['package_path']).parent == proc.store.runs/result['run_id']/'packages'
        assert Path(item['display_path']).is_file()
        assert Path(item['knowledge_manifest_path']).is_file()
        assert digest(Path(item['path'])) == item['reviewed_hash']
        assert package_fingerprint(item['package_path']) == item['reviewed_package_hash']
        assert Path(item['source_path']).is_file()
