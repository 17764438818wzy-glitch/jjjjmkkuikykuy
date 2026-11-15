import copy
import hashlib
import json
from pathlib import Path

import pytest

from ingest_education import EducationArtifacts
from ingest_execution_diagnostics import IngestionLogReader


def admitted_source(tmp_path):
    source = '# 第一章 问卷调查\n\n本研究发放问卷并分析学生学习情况。\n'
    parsed = '# 第一章 问卷调查\n本研究发放问卷并分析学生学习情况。\n'
    path = tmp_path / 'ingest.md'
    path.write_bytes(source.encode())
    paper_id = 'paper_' + 'a' * 24
    metadata = {
        'education_paper_id': paper_id, 'education_admission': '入库',
        'education_source_sha256': hashlib.sha256(parsed.encode()).hexdigest(),
        'education_method_details': json.dumps([{'name': '问卷调查法'}]),
        'education_research_steps': json.dumps([{'name': '问卷调查',
            'status': '实际执行', 'quote': '发放问卷并分析学生学习情况'}]),
    }
    task = {'source_id': paper_id, 'role': 'paper_primary', 'remote_id': 'doc',
            'remote_name': 'paper.md', 'dataset_id': 'edu', 'pipeline_id': 'pipe',
            'execution_attempt': 1, 'execution_begin_at': '2026-09-27T01:00:00',
            'path': str(path), 'content_sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
            'policy_key': 'education'}
    remote = {'id': 'doc', 'name': 'paper.md', 'run': 'DONE', 'pipeline_id': 'pipe',
              'process_begin_at': task['execution_begin_at'], 'meta_fields': metadata}
    chunks = [
        {'id': 'body-1', 'document_id': 'doc', 'available': True,
         'important_keywords': ['role=body'], 'content': parsed},
        {'id': 'index-1', 'document_id': 'doc', 'available': False,
         'important_keywords': ['role=index'], 'content': '[AI标注索引]\n问卷调查法'},
        {'id': 'assets-1', 'document_id': 'doc', 'available': False,
         'important_keywords': ['role=assets'], 'content': '# 图表/表格写作参考结构'},
    ]
    tokenizer = [{'text': c['content'], 'important_kwd': list(c['important_keywords']),
                  'keywords': ','.join(c['important_keywords']),
                  'available_int': int(c['available']), 'metadata': copy.deepcopy(metadata)}
                 for c in chunks]
    log = {'id': 'log-1', 'document_id': 'doc', 'document_name': 'paper.md',
           'kb_id': 'edu', 'pipeline_id': 'pipe', 'task_type': 'Parse',
           'operation_status': '3', 'process_begin_at': remote['process_begin_at'],
           'dsl': {'task_id': 'execution-1',
                   'path': ['TokenChunker:WholeEducation', 'CodeExec:EducationGate',
                            'Tokenizer:EducationIndex'], 'components': {}}}
    values = [('TokenChunker:WholeEducation', 'chunks', [{'text': parsed}]),
              ('CodeExec:EducationGate', 'json', [{'metadata': copy.deepcopy(metadata)}]),
              ('Tokenizer:EducationIndex', 'chunks', tokenizer)]
    for node, field, value in values:
        log['dsl']['components'][node] = {'obj': {'params': {'outputs': {field: {'value': value}}}}}

    class Client:
        logs = [log]
        current = remote
        fetched = chunks

        def request(self, method, endpoint):
            assert method == 'GET'
            if '/ingestions?' in endpoint:
                return {'logs': copy.deepcopy(self.logs), 'total': len(self.logs)}
            assert '/documents/doc/chunks?' in endpoint
            return {'chunks': copy.deepcopy(self.fetched), 'total': len(self.fetched)}

        def documents(self, dataset, document_id=None):
            assert dataset == 'edu' and document_id == 'doc'
            return [copy.deepcopy(self.current)]

    return task, remote, log, chunks, Client()


@pytest.mark.parametrize('mismatch', [
    'missing_log', 'unfinished_log', 'old_log', 'unbound_attempt', 'wrong_input',
    'parsed_hash', 'method_metadata', 'step_metadata', 'chunk_text', 'chunk_role',
    'chunk_doc', 'missing_chunk_id', 'duplicate_chunk_id', 'missing_chunk',
    'chunk_availability', 'unexecuted_tokenizer',
])
def test_artifacts_without_final_execution_source_proof_are_not_published(tmp_path, mismatch):
    task, remote, log, chunks, client = admitted_source(tmp_path)
    outputs = log['dsl']['components']['TokenChunker:WholeEducation']['obj']['params']['outputs']
    if mismatch == 'missing_log': client.logs = []
    if mismatch == 'unfinished_log': log['operation_status'] = '1'
    if mismatch == 'old_log': log['process_begin_at'] = '2026-09-26T01:00:00'
    if mismatch == 'unbound_attempt': task.pop('execution_begin_at')
    if mismatch == 'wrong_input': outputs['chunks']['value'][0]['text'] = '# 另一篇论文'
    if mismatch == 'parsed_hash': remote['meta_fields']['education_source_sha256'] = 'b' * 64
    if mismatch == 'method_metadata': remote['meta_fields']['education_method_details'] = '[{"name":"访谈法"}]'
    if mismatch == 'step_metadata': remote['meta_fields']['education_research_steps'] = '[]'
    if mismatch == 'chunk_text': chunks[0]['content'] = '# 另一篇论文'
    if mismatch == 'chunk_role': chunks[0]['important_keywords'] = ['role=index']
    if mismatch == 'chunk_doc': chunks[0]['document_id'] = 'other'
    if mismatch == 'missing_chunk_id': chunks[0].pop('id')
    if mismatch == 'duplicate_chunk_id': chunks[1]['id'] = chunks[0]['id']
    if mismatch == 'missing_chunk': chunks.pop()
    if mismatch == 'chunk_availability': chunks[0]['available'] = False
    if mismatch == 'unexecuted_tokenizer': log['dsl']['path'].remove('Tokenizer:EducationIndex')
    with pytest.raises(ValueError, match='source proof'):
        EducationArtifacts(lambda: client, tmp_path / 'artifacts')(task, remote)
    assert not (tmp_path / 'artifacts/latest' / (task['source_id'] + '.json')).exists()


@pytest.mark.parametrize('change', ['FAIL', 'RUNNING', 'name', 'pipeline', 'begin', 'hash'])
def test_remote_change_during_artifact_build_prevents_publication(tmp_path, change):
    from ingest_execution import RemoteExecutionChanged
    task, remote, _, _, client = admitted_source(tmp_path)
    client.current = copy.deepcopy(remote)
    if change in ('FAIL', 'RUNNING'): client.current['run'] = change
    if change == 'name': client.current['name'] = 'another.md'
    if change == 'pipeline': client.current['pipeline_id'] = 'another-pipe'
    if change == 'begin': client.current['process_begin_at'] = '2026-09-27T02:00:00'
    if change == 'hash': client.current['meta_fields']['education_source_sha256'] = 'b' * 64
    with pytest.raises(RemoteExecutionChanged) as error:
        EducationArtifacts(lambda: client, tmp_path / 'artifacts')(task, remote)
    assert error.value.remote == client.current
    assert not (tmp_path / 'artifacts/latest' / (task['source_id'] + '.json')).exists()


def test_source_already_failed_reports_remote_execution_change(tmp_path):
    from ingest_execution import RemoteExecutionChanged
    task, remote, _, _, client = admitted_source(tmp_path)
    remote['run'] = 'FAIL'
    with pytest.raises(RemoteExecutionChanged) as error:
        EducationArtifacts(lambda: client, tmp_path / 'artifacts')(task, remote)
    assert error.value.remote['run'] == 'FAIL'


def test_remembered_final_log_proves_historical_source_after_reader_restart(tmp_path):
    task, remote, log, chunks, _ = admitted_source(tmp_path)

    class NoRequests:
        def request(self, *args):
            pytest.fail('Historical proof should use the persisted final log')

    directory = tmp_path / 'proof-cache'
    first = IngestionLogReader(NoRequests, cache_dir=directory)
    assert first.remember(log)
    second = IngestionLogReader(NoRequests, cache_dir=directory)
    proof = second.source_proof(task, remote, chunks)
    assert proof['log_id'] == 'log-1'
    assert proof['execution_id'] == 'execution-1'
    assert proof['chunk_ids'] == ['assets-1', 'body-1', 'index-1']
    assert proof['source_sha256'] != proof['parsed_sha256']


def test_unfinished_log_is_not_persisted_as_historical_evidence(tmp_path):
    _, _, log, _, client = admitted_source(tmp_path)
    log['operation_status'] = '1'
    directory = tmp_path / 'proof-cache'
    reader = IngestionLogReader(lambda: client, cache_dir=directory)
    assert reader.remember(log) is False
    assert not list(directory.glob('*.json'))


def test_verified_revision_preserves_legacy_attempt_and_publishes_proof(tmp_path):
    task, remote, _, _, client = admitted_source(tmp_path)
    output = tmp_path / 'artifacts'
    old = output / 'attempts' / task['source_id'] / '1' / 'tree.json'
    old.parent.mkdir(parents=True)
    old.write_text('{"legacy": true}', encoding='utf-8')
    artifacts = EducationArtifacts(lambda: client, output)
    artifacts(task, remote)
    latest = json.loads((output / 'latest' / (task['source_id'] + '.json')).read_text(encoding='utf-8'))
    assert latest['valid'] is True
    assert latest['source_proof']['execution_id'] == 'execution-1'
    assert latest['source_proof']['chunk_ids'] == ['assets-1', 'body-1', 'index-1']
    assert Path(latest['version_path']) == old.parent / 'verified-v3'
    assert json.loads(old.read_text()) == {'legacy': True}
    tree = json.loads((Path(latest['version_path']) / 'tree.json').read_text(encoding='utf-8'))
    assert tree['nodes'][0]['source_chunk_ids'] == ['body-1']
    artifacts(task, remote)


def test_failed_reverification_withdraws_previous_valid_pointer(tmp_path):
    task, remote, _, _, client = admitted_source(tmp_path)
    output = tmp_path / 'artifacts'
    artifacts = EducationArtifacts(lambda: client, output)
    artifacts(task, remote)
    latest = output / 'latest' / (task['source_id'] + '.json')
    before = json.loads(latest.read_text(encoding='utf-8'))
    client.fetched[0]['content'] = '# 正文被替换'
    with pytest.raises(ValueError, match='source proof'):
        artifacts(task, remote)
    assert json.loads(latest.read_text(encoding='utf-8'))['valid'] is False
    assert (Path(before['version_path']) / 'tree.json').exists()


def test_source_proof_accepts_detailed_gate_metadata_only_on_index_chunk(tmp_path):
    task, remote, log, chunks, client = admitted_source(tmp_path)
    components = log['dsl']['components']
    tokenized = components['Tokenizer:EducationIndex']['obj']['params']['outputs']['chunks']['value']
    for row in tokenized:
        if row['keywords'] != 'role=index':
            row['metadata'].pop('education_method_details')
            row['metadata'].pop('education_research_steps')
    components['CodeExec:EducationGate']['obj']['params']['outputs']['json']['value'] = [
        {'keywords': row['keywords'], 'metadata': copy.deepcopy(row['metadata'])} for row in tokenized
    ]
    assert IngestionLogReader(lambda: client).source_proof(task, remote, chunks)['log_id'] == 'log-1'


def test_invalidation_preserves_conflicting_legacy_alias_and_can_repeat(tmp_path):
    task, _, _, _, client = admitted_source(tmp_path)
    output = tmp_path / 'artifacts'
    paper_id = task['source_id']
    archive = output / 'attempts' / paper_id / '1' / 'tree.json'
    alias = output / 'direct_trees' / (paper_id + '.json')
    latest = output / 'latest' / (paper_id + '.json')
    for path, data in ((archive, {'version': 'original'}), (alias, {'version': 'repaired'}),
                       (latest, {'valid': False, 'paper_id': paper_id, 'attempt': 1})):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data), encoding='utf-8')
    artifacts = EducationArtifacts(lambda: client, output)
    artifacts.invalidate(task)
    artifacts.invalidate(task)
    assert json.loads(archive.read_text()) == {'version': 'original'}
    assert not alias.exists()
    assert any(json.loads(path.read_text()) == {'version': 'repaired'}
               for path in archive.parent.glob('legacy-*/tree.json'))


@pytest.mark.parametrize('variant_parts', [False, True])
def test_identical_disabled_asset_text_can_have_one_indexed_representative(tmp_path, variant_parts):
    task, remote, log, chunks, client = admitted_source(tmp_path)
    tokenized = log['dsl']['components']['Tokenizer:EducationIndex']['obj']['params']['outputs']['chunks']['value']
    original = copy.deepcopy(tokenized[-1])
    original['important_kwd'] = ['role=assets', 'part=3/3']
    representative = copy.deepcopy(original)
    if variant_parts:
        representative['important_kwd'] = ['role=assets', 'part=2/2']
    tokenized[-1:] = [original, copy.deepcopy(representative), representative]
    chunks[-1]['important_keywords'] = list(representative['important_kwd'])
    proof = IngestionLogReader(lambda: client).source_proof(task, remote, chunks)
    assert proof is not None
    assert proof['chunk_ids'] == ['assets-1', 'body-1', 'index-1']


@pytest.mark.parametrize('collision_role', ['body', 'index'])
def test_same_text_in_assets_and_other_roles_is_not_safe_to_deduplicate(tmp_path, collision_role):
    task, remote, log, chunks, client = admitted_source(tmp_path)
    tokenized = log['dsl']['components']['Tokenizer:EducationIndex']['obj']['params']['outputs']['chunks']['value']
    collision = next(c['content'] for c in chunks if c['important_keywords'] == ['role=' + collision_role])
    tokenized[-1]['text'] = chunks[-1]['content'] = collision
    assert IngestionLogReader(lambda: client).source_proof(task, remote, chunks) is None


def test_assets_cannot_be_enabled_even_when_tokenizer_and_remote_agree(tmp_path):
    task, remote, log, chunks, client = admitted_source(tmp_path)
    tokenized = log['dsl']['components']['Tokenizer:EducationIndex']['obj']['params']['outputs']['chunks']['value']
    tokenized[-1]['available_int'] = 1
    chunks[-1]['available'] = True
    assert IngestionLogReader(lambda: client).source_proof(task, remote, chunks) is None


@pytest.mark.parametrize('mismatch', [
    'missing_distinct_material', 'unemitted_asset_keywords', 'extra_asset_copy',
    'body_deduplication', 'index_deduplication', 'enabled_asset', 'foreign_asset',
])
def test_asset_deduplication_does_not_relax_other_source_requirements(tmp_path, mismatch):
    task, remote, log, chunks, client = admitted_source(tmp_path)
    tokenized = log['dsl']['components']['Tokenizer:EducationIndex']['obj']['params']['outputs']['chunks']['value']
    tokenized.append(copy.deepcopy(tokenized[-1]))
    if mismatch == 'missing_distinct_material':
        tokenized[-1]['text'] = '另一份附件材料'
    if mismatch == 'unemitted_asset_keywords':
        chunks[-1]['important_keywords'].append('part=9/9')
    if mismatch == 'extra_asset_copy':
        chunks.append(dict(chunks[-1], id='assets-extra'))
    if mismatch == 'body_deduplication':
        tokenized.append(copy.deepcopy(tokenized[0]))
    if mismatch == 'index_deduplication':
        tokenized.append(copy.deepcopy(tokenized[1]))
    if mismatch == 'enabled_asset':
        chunks[-1]['available'] = True
    if mismatch == 'foreign_asset':
        chunks[-1]['document_id'] = 'other'
    assert IngestionLogReader(lambda: client).source_proof(task, remote, chunks) is None


def test_missing_final_evidence_can_be_rechecked_without_reparsing(tmp_path):
    from ingest_execution import ArtifactEvidencePending
    task, remote, log, _, client = admitted_source(tmp_path)
    client.logs = []
    now = [0.0]
    reader = IngestionLogReader(lambda: client, clock=lambda: now[0])
    output = tmp_path / 'artifacts'
    artifacts = EducationArtifacts(lambda: client, output, source_reader=reader)
    with pytest.raises(ArtifactEvidencePending, match='no_matching_final') as error:
        artifacts(task, remote)
    assert error.value.artifact_evidence_reason == 'no_matching_final'
    assert not (output / 'latest' / (task['source_id'] + '.json')).exists()
    client.logs = [log]
    now[0] = 60.0
    artifacts(task, remote)
    latest = json.loads((output / 'latest' / (task['source_id'] + '.json')).read_text(encoding='utf-8'))
    assert latest['valid'] is True
    assert latest['attempt'] == 1
    assert latest['source_proof']['execution_id'] == 'execution-1'


@pytest.mark.parametrize('record_kind', ['old_final', 'unfinished'])
def test_only_current_final_records_end_evidence_wait(tmp_path, record_kind):
    from ingest_execution import ArtifactEvidencePending
    task, remote, log, chunks, client = admitted_source(tmp_path)
    if record_kind == 'old_final':
        log['process_begin_at'] = '2026-09-26T01:00:00'
    else:
        log['operation_status'] = '1'
    with pytest.raises(ArtifactEvidencePending, match='source proof'):
        IngestionLogReader(lambda: client).source_proof(task, remote, chunks)


def test_final_source_conflict_is_not_reclassified_as_pending_evidence(tmp_path):
    from ingest_execution import ArtifactEvidencePending
    task, remote, _, chunks, client = admitted_source(tmp_path)
    chunks[0]['content'] = '# 另一篇论文'
    with pytest.raises(ValueError, match='source proof') as error:
        EducationArtifacts(lambda: client, tmp_path / 'artifacts')(task, remote)
    assert not isinstance(error.value, ArtifactEvidencePending)
    assert error.value.artifact_evidence_reason == 'proof_conflict'
