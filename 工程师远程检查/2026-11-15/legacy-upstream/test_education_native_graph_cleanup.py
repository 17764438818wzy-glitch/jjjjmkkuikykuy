import json

import pytest

import education_native_graph_cleanup as cleanup
from engine import digest


def test_structure_allows_only_the_persistent_document_summary():
    valid = {
        'templates': [{'entities': [{}], 'relations': [{}]}],
        'returned_entities': 1, 'total_entities': 2,
        'returned_relations': 1, 'total_relations': 1,
    }
    assert cleanup.complete_structure(valid)
    assert not cleanup.complete_structure({**valid, 'total_entities': 3})
    assert not cleanup.complete_structure({**valid, 'total_relations': 2})


def test_uncertain_delete_is_never_reissued(tmp_path):
    paper_id = 'paper_test'
    doc_id = 'document_test'
    source_id = 'source_test'
    run = tmp_path / 'run'
    source = run / 'canonical_packages' / source_id / 'ingest.md'
    source.parent.mkdir(parents=True)
    source.write_text('body', encoding='utf-8')
    method = run / 'method_graphs' / (paper_id + '.json')
    method.parent.mkdir()
    method.write_text(json.dumps({
        'paper': {'id': paper_id, 'source_doc_id': doc_id},
        'ingest_sha256': digest(source),
    }), encoding='utf-8')

    class Client:
        def documents(self, dataset_id, document_id):
            return [{'id': doc_id, 'name': 'paper.md', 'run': 'DONE',
                     'pipeline_id': 'old', 'chunk_count': 5,
                     'meta_fields': {'education_paper_id': paper_id,
                                     'education_admission': '入库'}}]

        def request(self, method, endpoint, *args):
            assert method == 'GET', 'uncertain DELETE was reissued'
            return {'templates': [{'kind': 'knowledge_graph', 'template_id': 'old_graph',
                                   'entities': [{}], 'relations': []}],
                    'returned_entities': 1, 'total_entities': 2,
                    'returned_relations': 0, 'total_relations': 0}

    receipt = {'status': 'done', 'admission': '入库',
               'ragflow_document_id': doc_id, 'document_id': source_id,
               'upload_name': 'paper.md'}
    ledger = {'items': {paper_id: {'status': 'unknown',
                                  'template_id': 'old_graph'}}}
    with pytest.raises(ValueError, match='上次删除结果未确定'):
        cleanup.cleanup_one(Client(), 'education_dataset', 'new', paper_id,
                            receipt, ledger, run_dir=run, work_dir=run / 'cleanup')
