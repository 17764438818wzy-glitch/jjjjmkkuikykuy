from pathlib import Path

from paperflow.config import Config
from paperflow.graph import Workflow
from paperflow.package import PackageError, sha
from paperflow.store import Store


def setup_flow(tmp_path, producer=None, publisher=None):
    config = Config(home=str(tmp_path/'runtime'), handoff_root=str(tmp_path/'handoff'), retry_seconds=0)
    config.prepare()
    store = Store(config.root/'state.sqlite3')
    original = tmp_path/'original.docx'
    original.write_bytes(b'original')
    task = store.register_source(original, sha(original))
    flow = Workflow(config, store, producer=producer, publisher=publisher, catalog={'majors': []})
    return flow, store, task


def test_infrastructure_failure_retries_same_stage_without_content_repair(tmp_path):
    class Producer:
        def run_stage(self, task, stage):
            raise TimeoutError('model transport unavailable')
    flow, store, task = setup_flow(tmp_path, producer=Producer())
    result = flow.step(task)
    assert result['stage'] == 'round1'
    assert result['repair_count'] == 0
    assert result['error_kind'] == 'infrastructure'
    assert result['retry_count'] == 1
    flow.close()


def test_content_failure_creates_repair_packet_and_new_revision(tmp_path):
    class Producer:
        def run_stage(self, task, stage):
            raise PackageError('missing image link')
    flow, store, task = setup_flow(tmp_path, producer=Producer())
    result = flow.step(task)
    assert result['stage'] == 'repair'
    assert result['kind'] == 'repair'
    assert result['revision'] == 2
    assert Path(result['failure_packet'], 'failure.json').is_file()
    flow.close()


def test_same_error_without_artifact_progress_eventually_parks_one_paper(tmp_path):
    class Producer:
        def run_stage(self, task, stage):
            raise PackageError('same missing link')
    flow, store, task = setup_flow(tmp_path, producer=Producer())
    for _ in range(5):
        task = flow.step(task)
        if task['stage'] == 'parked':
            break
    assert task['stage'] == 'parked'
    assert task['repair_count'] == 2 and task['correction_count'] == 2
    assert task['alert'] == 'delivery_correction_exhausted'
    flow.close()


def test_graph_checkpoints_survive_reopening(tmp_path):
    class Producer:
        def run_stage(self, task, stage):
            return {'draft_dir': str(tmp_path/'draft'), 'disposition': 'keep'}
    flow, store, task = setup_flow(tmp_path, producer=Producer())
    updated = flow.step(task)
    assert updated['stage'] == 'round2'
    checkpoint = flow.graph.get_state(flow.graph_config(task))
    assert checkpoint.values['task_id'] == task['task_id']
    flow.close()
    resumed = Workflow(flow.config, store, producer=Producer(), catalog={'majors': []})
    assert resumed.graph.get_state(resumed.graph_config(task)).values['stage'] == 'round2'
    assert resumed.step(store.get(task['task_id']))['stage'] == 'round3'
    resumed.close()


def test_partial_files_are_included_in_repair_context(tmp_path):
    draft=tmp_path/'partial'; draft.mkdir()
    (draft/'profile.json').write_text('{"partial":true}',encoding='utf-8')
    class Producer:
        def run_stage(self,task,stage):
            error=PackageError('missing tree')
            error.draft_dir=str(draft)
            raise error
    flow,store,task=setup_flow(tmp_path,producer=Producer())
    result=flow.step(task)
    from paperflow.package import load
    packet=load(Path(result['failure_packet'])/'failure.json')
    assert packet['artifact_directory']==str(draft)
    assert result['repair_input_dir']==str(draft)
    flow.close()


def test_repair_of_first_round_still_requires_second_and_third_round(tmp_path):
    class Producer:
        def run_stage(self,task,stage):
            if stage=='round1' and not task.get('repair_count'):raise PackageError('broken first round JSON')
            return dict(draft_dir=str(tmp_path/'repaired'),disposition='keep')
    flow,store,task=setup_flow(tmp_path,producer=Producer())
    task=flow.step(task)
    assert task['failed_stage']=='round1'
    task=flow.step(task)
    assert task['stage']=='round2'
    task=flow.step(task)
    assert task['stage']=='round3'
    flow.close()


def test_native_langgraph_contains_business_and_repair_edges(tmp_path):
    flow,store,task=setup_flow(tmp_path)
    edges={(edge.source,edge.target) for edge in flow.graph.get_graph().edges}
    assert ('round1','round2') in edges
    assert ('repair','publish') in edges
    assert ('verify','activate') in edges
    flow.close()
