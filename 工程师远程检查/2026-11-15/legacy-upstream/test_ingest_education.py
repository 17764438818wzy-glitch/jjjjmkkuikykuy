import json
from pathlib import Path

import pytest

from engine import digest
from evidence import export_package, package_fingerprint
from ingest_contracts import Outcome
from ingest_education import EducationArtifacts, classify_education, education_specs
from ingest_queue import QueueStore
from ingest_worker import enqueue_education_batch
from education_stream_status import queue_paper_status
from knowledge_package import knowledge_files
from pilot_package_copy import prepare_pilot_copy
from test_knowledge_package import sample
from test_ingest_artifact_provenance import admitted_source


def prepared(tmp_path):
    source = tmp_path / 'source'
    source.mkdir()
    metadata = sample(source)
    metadata.update(major='小学教育', category='教育教学类')
    export_package(source, metadata, canonical_paper_id=False)
    package = tmp_path / 'canonical'
    prepare_pilot_copy(source, package)
    knowledge_files(package, require_stable_paper_id=True)
    manifest = json.loads((package / 'manifest.json').read_text(encoding='utf-8'))
    item = {'id': manifest['document_id'], 'paper_id': manifest['paper_id'],
            'status': 'pass', 'category': '教育教学类', 'title': manifest['title'],
            'reviewed_hash': digest(package / 'ingest.md'),
            'reviewed_package_hash': package_fingerprint(package)}
    deployment = {'paper_dataset_id': 'education-md',
                  'image_dataset_id': 'education-images',
                  'pipeline_id': 'education-pipeline'}
    return item, package, deployment


def test_education_specs_route_primary_and_approved_image_roles(tmp_path):
    item, package, deployment = prepared(tmp_path)
    tasks = education_specs(item, deployment, package, 'http://localhost:9380', 'batch-1')
    primary = [task for task in tasks if task.role == 'paper_primary']
    assert len(primary) == 1
    assert primary[0].dataset_id == 'education-md'
    assert primary[0].parse_required
    assert primary[0].source_id == item['paper_id']
    assert item['paper_id'] in primary[0].remote_name
    assert primary[0].content_sha256[:16] in primary[0].remote_name
    images = [task for task in tasks if task.lane == 'image']
    assert images
    assert all(task.dataset_id == 'education-images' for task in images)
    assert all(not task.parse_required for task in images)
    assert len({task.role for task in images}) == len(images)
    assert any(task.role.startswith('image:') for task in images)


def test_education_specs_reject_mismatched_handoff_or_hash(tmp_path):
    item, package, deployment = prepared(tmp_path)
    with pytest.raises(ValueError):
        education_specs(dict(item, paper_id='paper_' + '0'*24), deployment,
                        package, 'http://localhost:9380', 'batch-1')
    with pytest.raises(ValueError):
        education_specs(dict(item, reviewed_hash='0'*64), deployment,
                        package, 'http://localhost:9380', 'batch-1')


def test_education_classification_preserves_quality_and_uncertainty():
    task = {'source_id': 'paper_' + 'a'*24}
    meta = {'education_paper_id': task['source_id'], 'education_admission': '入库'}
    assert classify_education(task, {'run': 'DONE', 'meta_fields': meta}) == Outcome.accepted
    assert classify_education(task, {'run': 'FAIL', 'meta_fields':
                                     dict(meta, education_admission='暂缓')}) == Outcome.quality_hold
    assert classify_education(task, {'run': 'FAIL', 'meta_fields':
                                     dict(meta, education_admission='剔除')}) == Outcome.quality_reject
    assert classify_education(task, {'run': 'FAIL',
        'progress_msg': '0 tasks are ahead in the queue...'}) == Outcome.outcome_unresolved
    assert classify_education(task, {'run': 'DONE', 'meta_fields':
                                     dict(meta, education_paper_id='other')}) == Outcome.technical_review


def test_education_worker_enqueues_new_batch_without_network_writes(tmp_path):
    run_id = 'AUTO_sample_batch'
    batch_dir = tmp_path / 'data' / 'runs' / run_id
    source = batch_dir / 'packages' / 'AUTO_test'
    source.mkdir(parents=True)
    metadata = sample(source)
    metadata.update(major='小学教育', category='教育教学类')
    export_package(source, metadata, canonical_paper_id=False)
    manifest = json.loads((source / 'manifest.json').read_text(encoding='utf-8'))
    item = {'id': manifest['document_id'], 'paper_id': manifest['paper_id'],
            'status': 'pass', 'category': '教育教学类', 'title': manifest['title'],
            'package_path': str(source), 'reviewed_hash': digest(source / 'ingest.md'),
            'reviewed_package_hash': package_fingerprint(source)}
    batch_dir.joinpath('manifest.json').write_text(json.dumps({'run_id': run_id,
        'status': 'completed', 'items': [item]}), encoding='utf-8')
    queue = QueueStore(tmp_path / 'data' / 'private' / 'queue.sqlite3')
    deployment = {'paper_dataset_id': 'education-md',
                  'image_dataset_id': 'education-images',
                  'pipeline_id': 'education-pipeline'}
    summary = enqueue_education_batch(queue, tmp_path, run_id, deployment,
                                      'http://localhost:9380', set())
    assert summary['papers'] == 1, summary
    assert summary['errors'] == 0
    assert queue.summary()['upload_waiting'] >= 2
    assert enqueue_education_batch(queue, tmp_path, run_id, deployment,
                                   'http://localhost:9380', set())['papers'] == 1
    row = queue_paper_status(queue, 'http://localhost:9380', 'education-md',
                             item['paper_id'])
    assert row['upload'] == '已排队'
    assert row['process'] == '等待上传'


def test_education_cutover_does_not_requeue_a_paper_from_the_previous_dataset(tmp_path):
    run_id = 'AUTO_cutover_batch'
    batch_dir = tmp_path / 'data' / 'runs' / run_id
    source = batch_dir / 'packages' / 'AUTO_test'
    source.mkdir(parents=True)
    metadata = sample(source)
    metadata.update(major='小学教育', category='教育教学类')
    export_package(source, metadata, canonical_paper_id=False)
    package = json.loads((source / 'manifest.json').read_text(encoding='utf-8'))
    item = {'id': package['document_id'], 'paper_id': package['paper_id'],
            'status': 'pass', 'category': '教育教学类', 'title': package['title'],
            'package_path': str(source), 'reviewed_hash': digest(source / 'ingest.md'),
            'reviewed_package_hash': package_fingerprint(source)}
    batch_dir.joinpath('manifest.json').write_text(json.dumps({
        'run_id': run_id, 'status': 'completed', 'items': [item]}), encoding='utf-8')
    queue = QueueStore(tmp_path / 'data' / 'private' / 'queue.sqlite3')
    old = {'paper_dataset_id': 'education-cloud', 'image_dataset_id': 'education-images',
           'pipeline_id': 'education-pipeline'}
    assert enqueue_education_batch(queue, tmp_path, run_id, old,
                                   'http://localhost:9380', set())['papers'] == 1
    new = {'paper_dataset_id': 'education-local', 'image_dataset_id': 'education-images',
           'pipeline_id': 'education-pipeline',
           'previous_paper_dataset_id': 'education-cloud'}
    assert enqueue_education_batch(queue, tmp_path, run_id, new,
                                   'http://localhost:9380', set())['papers'] == 0
    assert queue.find_identity('http://localhost:9380', 'education-local',
                               item['paper_id'], 'paper_primary') is None


def test_accepted_paper_builds_source_linked_tree_and_method_graph(tmp_path):
    task, remote, _, chunks, client = admitted_source(tmp_path)
    paper_id = task['source_id']
    chunks[0]['id'] = 'chunk-1'
    artifacts = EducationArtifacts(lambda: client, tmp_path / 'artifacts')
    artifacts(task, remote)
    tree = json.loads((tmp_path / 'artifacts' / 'direct_trees' /
                       (paper_id + '.json')).read_text(encoding='utf-8'))
    graph = json.loads((tmp_path / 'artifacts' / 'method_graphs' /
                        (paper_id + '.json')).read_text(encoding='utf-8'))
    assert tree['nodes'][0]['source_chunk_ids'] == ['chunk-1']
    assert graph['edges'][0]['source_chunk_ids'] == ['chunk-1']
    artifacts(task, remote)

def test_artifact_attempt_versions_keep_prior_chunks_and_withdraw_current(tmp_path):
    task, remote, log, chunks, client = admitted_source(tmp_path)
    paper_id=task['source_id']
    chunks[0]['id']='c1'
    artifacts=EducationArtifacts(lambda:client,tmp_path/'artifacts')
    artifacts(task,remote)
    task2=dict(task,execution_attempt=2,pipeline_id='official',execution_begin_at='2026-09-27T02:00:00')
    artifacts.invalidate(task2)
    latest=tmp_path/'artifacts/latest'/f'{paper_id}.json'
    assert json.loads(latest.read_text())['valid'] is False
    assert not (tmp_path/'artifacts/direct_trees'/f'{paper_id}.json').exists()
    chunks[0]['id']='c2'
    remote.update(pipeline_id='official',process_begin_at=task2['execution_begin_at'])
    log.update(pipeline_id='official',process_begin_at=task2['execution_begin_at'])
    artifacts=EducationArtifacts(lambda:client,tmp_path/'artifacts')
    artifacts(task2,remote)
    assert json.loads(latest.read_text())['attempt']==2
    versions=list((tmp_path/'artifacts/attempts'/paper_id).glob('*/verified-v3/method_graph.json'))
    assert len(versions)==2
    assert {'c1','c2'}=={json.loads(p.read_text(encoding="utf-8"))['edges'][0]['source_chunk_ids'][0] for p in versions}


def test_method_graph_revision_keeps_previous_attempt_artifact(tmp_path):
    task, remote, _, _, client = admitted_source(tmp_path)
    base = tmp_path/'artifacts'
    previous = base/'attempts'/task['source_id']/str(task['execution_attempt'])/'verified-v2'/'method_graph.json'
    previous.parent.mkdir(parents=True)
    previous.write_text('{"prior_graph":true}', encoding='utf-8')
    before = previous.read_bytes()
    EducationArtifacts(lambda:client, base)(task, remote)
    pointer = json.loads((base/'latest'/(task['source_id']+'.json')).read_text())
    assert Path(pointer['version_path']).name == 'verified-v3'
    assert previous.read_bytes() == before
    assert (Path(pointer['version_path'])/'method_graph.json').is_file()
