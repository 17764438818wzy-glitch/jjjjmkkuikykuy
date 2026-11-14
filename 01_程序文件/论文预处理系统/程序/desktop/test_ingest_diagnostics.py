import pytest


def test_paper_text_and_queue_placeholder_cannot_trigger_model_switch():
    from ingest_diagnostics import model_failure
    assert model_failure({'run': 'FAIL', 'progress_msg': '2 tasks are ahead in the queue...',
                          'body': '429 rate limit Arrearage'}, None) is None
    assert model_failure({'run': 'FAIL'}, {'body': '429 rate limit'}) is None


@pytest.mark.parametrize('dependency,node,kind,code', [
    ('llm', 'Extractor:EducationAnnotation', 'rate_limit', 429),
    ('llm', 'Extractor:EducationAnnotation', 'authentication', 401),
    ('embedding', 'Indexer:Embedding', 'arrearage', 402),
])
def test_trusted_node_failure_keeps_dependency_identity(dependency,node,kind,code):
    from ingest_diagnostics import model_failure
    signal = {'source': 'execution_node', 'component_id': node, 'dependency': dependency,
              'kind': kind, 'status_code': code, 'retry_after': 17}
    result = model_failure({'run': 'FAIL'}, signal)
    assert result == {'dependency': dependency, 'kind': kind, 'status_code': code, 'retry_after': 17.0}


def test_success_and_unknown_nodes_do_not_supply_failure_signal():
    from ingest_diagnostics import model_failure
    signal = {'source': 'execution_node', 'component_id': 'CodeExec:PaperQuote',
              'dependency': 'llm', 'kind': 'rate_limit', 'status_code': 429}
    assert model_failure({'run': 'FAIL'}, signal) is None
    signal['component_id'] = 'Extractor:EducationAnnotation'
    assert model_failure({'run': 'DONE'}, signal) is None


def test_non_numeric_or_negative_retry_after_does_not_crash_dispatch():
    from ingest_diagnostics import model_failure
    signal = {'source': 'execution_node', 'component_id': 'Extractor:EducationAnnotation',
              'dependency': 'llm', 'kind': 'rate_limit', 'status_code': 429, 'retry_after': 'invalid'}
    assert model_failure({'run': 'FAIL'}, signal)['retry_after'] == 60.0
