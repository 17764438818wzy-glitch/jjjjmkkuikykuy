"""Durable, source-scoped resume through the real adapter; no live requests."""
import io
import hashlib
import json
from pathlib import Path

import pytest
from PIL import Image

from agent_client import AgentClient, AgentError
from auto_pipeline import AutoProcessor
from engine import Store, scan_sources
from test_workbench import paper


CONFIG = {'base_url': 'https://fixture.invalid/v1', 'text_model': 'gpt-5.5',
          'vision_model': 'gpt-5.5', 'attempts': 1, 'auto_upload': False,
          'concurrency': {'enabled': False, 'papers': 2, 'images_per_paper': 2,
                         'global_llm': 2, 'max_retries': 1, 'backoff_base_seconds': 0}}
KEY = 'SYNTHETIC_STAGE_RESUME_KEY'


def picture(color='white'):
    buffer = io.BytesIO()
    from PIL import ImageDraw
    figure=Image.new('RGB', (160, 100), color)
    ImageDraw.Draw(figure).line([(10,90),(80,50),(150,10)],fill='blue',width=2)
    figure.save(buffer, format='PNG')
    return buffer.getvalue()


def response(task):
    result = {'decision': 'pass', 'reason': 'synthetic evidence checked'}
    if task == 'privacy_image':
        result.update(research=True, sensitive_boxes=[], text='原始观测值100', description='观察记录图')
    elif task == 'privacy_text':
        result['sensitive_strings'] = []
    elif task == 'classify':
        result.update(major='学前教育', title='幼儿阅读活动的案例研究')
    return result


class Transport:
    """Only replace the external HTTP boundary; record actual wire requests."""
    def __init__(self, handler=None):
        self.requests = []
        self.handler = handler

    def open(self, request, timeout):
        data = json.loads(request.data)
        content = data['messages'][1]['content']
        message = json.loads(content[0]['text'] if isinstance(content, list) else content)
        self.requests.append((message, data))
        result = self.handler(message) if self.handler else response(message['task'])
        envelope = {'choices': [{'finish_reason': 'stop', 'message': {'content': json.dumps(result)}}],
                    'usage': {'prompt_tokens': 100, 'completion_tokens': 20, 'total_tokens': 120}}
        return io.BytesIO(json.dumps(envelope).encode())


def document(path):
    from docx import Document
    doc = Document()
    for line in paper().splitlines():
        doc.add_paragraph(line)
    # Distinct image bytes ensure this exercises two actual image units.
    for color in ['white', 'lightblue']:
        doc.add_picture(io.BytesIO(picture(color)))
    doc.save(path)


def test_new_processor_retry_reuses_accepted_image_after_stop(tmp_path):
    source_dir = tmp_path/'input'; source_dir.mkdir()
    source = source_dir/'paper.docx'; document(source)
    original = source.read_bytes()
    store = Store(tmp_path/'data')
    first = AutoProcessor(store, CONFIG, KEY)
    first.agent.opener = Transport()

    image_stages = []
    def stop_after_first_image(state):
        # Stage events run before the next unit starts, after the first is accepted.
        if state.get('progress', {}).get('stage') == 'AI 图表识别与脱敏':
            image_stages.append(state['progress'])
        if len(image_stages) == 2:
            first.stop.set()

    stopped = first.run(scan_sources(source_dir), stop_after_first_image)
    assert stopped['items'][0]['reason_kind'] == 'stopped', stopped['items'][0]
    assert not stopped['items'][0]['automatic_approval']
    assert [m['task'] for m, _ in first.agent.opener.requests] == ['privacy_image']
    second = AutoProcessor(Store(store.root), CONFIG, KEY)
    second.agent.opener = Transport()
    progress = []
    resumed = second.run(scan_sources(source_dir), lambda state: progress.append(state.get('progress', {})))
    item = resumed['items'][0]
    assert item['status'] == 'pass', item
    assert item['id'] != stopped['items'][0]['id']
    assert [m['task'] for m, _ in second.agent.opener.requests].count('privacy_image') == 1
    hits = [m for m in item['processing_metrics'] if m.get('cache_hit')]
    assert len(hits) == 1 and hits[0]['task'] == 'privacy_image'
    reuse = [event for event in progress if event.get('stage') == '续跑复用']
    assert len(reuse) == 1 and '图像' in reuse[0]['message']
    assert len(second.agent.ledger.records(run_id=resumed['run_id'])) == len(second.agent.opener.requests)
    assert source.read_bytes() == original
    assert not list(Path(item['package_path']).rglob('*cache*'))
    for path in store.root.rglob('*.json'):
        assert KEY not in path.read_text(encoding='utf-8')


def cached_client(root, source='a'*64, version='fixture-v1', config=None, transport=None):
    from stage_cache import StageCache
    client = AgentClient(config or CONFIG, KEY)
    client.bind_stage_cache(StageCache(root), source, version)
    client.opener = transport or Transport()
    return client


def test_accepted_text_response_is_immutable_and_replayed_as_fresh_data(tmp_path):
    first = cached_client(tmp_path)
    result = first.call('privacy_text', {'text': 'original input'})
    result['sensitive_strings'].append('downstream mutation')
    first.accept_cached_result(result)
    second = cached_client(tmp_path)
    replay = second.call('privacy_text', {'text': 'original input'})
    assert second.last_call_cache_hit
    assert replay['sensitive_strings'] == []
    assert second.opener.requests == []
    replay['reason'] = 'changed'
    third = cached_client(tmp_path)
    assert third.call('privacy_text', {'text': 'original input'})['reason'] == 'synthetic evidence checked'


@pytest.mark.parametrize('decision', ['unaccepted', 'reject'])
def test_unaccepted_and_rejected_results_are_not_durable(tmp_path, decision):
    result = response('privacy_text')
    if decision == 'reject':
        result = {'decision': 'reject', 'reason': 'insufficient evidence'}
    first = cached_client(tmp_path, transport=Transport(lambda _: result))
    returned = first.call('privacy_text', {'text': 'original'})
    if decision == 'reject':
        first.accept_cached_result(returned)
    second = cached_client(tmp_path)
    assert second.call('privacy_text', {'text': 'original'})['decision'] == 'pass'
    assert not second.last_call_cache_hit
    assert len(second.opener.requests) == 1


@pytest.mark.parametrize('change', ['source', 'version', 'model', 'endpoint', 'max_tokens',
                                    'prompt', 'image', 'payload_id', 'position', 'path'])
def test_cache_identity_covers_exact_source_and_request(tmp_path, monkeypatch, change):
    import agent_client
    payload = {'id': 'image_0001', 'position': {'page': 1}, 'path': 'evidence/one'}
    blob = picture()
    first = cached_client(tmp_path)
    result = first.call('privacy_image', payload, [blob])
    first.accept_cached_result(result)
    config = dict(CONFIG); source = 'a'*64; version = 'fixture-v1'
    if change == 'source': source = 'b'*64
    if change == 'version': version = 'fixture-v2'
    if change == 'model': config['vision_model'] = 'different-model'
    if change == 'endpoint': config['base_url'] = 'https://another.invalid/v1'
    if change == 'max_tokens': config['max_tokens'] = 9000
    if change == 'prompt': monkeypatch.setitem(agent_client.CONTRACTS, 'privacy_image', 'Changed contract')
    if change == 'image': blob = picture('blue')
    if change == 'payload_id': payload['id'] = 'image_0002'
    if change == 'position': payload['position']['page'] = 2
    if change == 'path': payload['path'] = 'evidence/two'
    second = cached_client(tmp_path, source, version, config)
    assert second.call('privacy_image', payload, [blob])['decision'] == 'pass'
    assert not second.last_call_cache_hit
    assert len(second.opener.requests) == 1


@pytest.mark.parametrize('damage', ['truncated', 'digest', 'schema', 'coordinates', 'key'])
def test_corrupt_or_invalid_cache_is_miss_and_never_bypasses_validation(tmp_path, damage):
    first = cached_client(tmp_path)
    result = first.call('privacy_image', {'page': 1}, [picture()])
    first.accept_cached_result(result)
    path, = list((tmp_path/'private'/'stage_cache').glob('*.json'))
    record = json.loads(path.read_text(encoding='utf-8'))
    if damage == 'truncated':
        path.write_text('{', encoding='utf-8')
    else:
        if damage == 'digest': record['result']['text'] = 'tampered text'
        if damage == 'schema': record['result']['research'] = 'true'
        if damage == 'coordinates': record['result']['sensitive_boxes'] = [[0, 0, 5, 5]]
        if damage == 'key': record['cache_key'] = 'f'*64
        if damage in {'schema', 'coordinates'}:
            encoded = json.dumps(record['result'], ensure_ascii=False, sort_keys=True,
                                 separators=(',', ':'), allow_nan=False).encode('utf-8')
            record['result_sha256'] = hashlib.sha256(encoded).hexdigest()
        path.write_text(json.dumps(record), encoding='utf-8')
    second = cached_client(tmp_path)
    assert second.call('privacy_image', {'page': 1}, [picture()])['text'] == '原始观测值100'
    assert not second.last_call_cache_hit
    assert len(second.opener.requests) == 1


def test_stop_prevents_cache_hit_and_does_not_charge(tmp_path):
    import threading
    from billing import Ledger
    first = cached_client(tmp_path)
    result = first.call('privacy_text', {'text': 'original'})
    first.accept_cached_result(result)
    second = cached_client(tmp_path)
    second.stop = threading.Event(); second.stop.set()
    second.ledger = Ledger(tmp_path)
    with pytest.raises(AgentError, match='已停止'):
        second.call('privacy_text', {'text': 'original'})
    assert second.opener.requests == []
    assert second.ledger.records() == []


def test_unaccepted_replay_is_evicted_when_downstream_rejects(tmp_path):
    first = cached_client(tmp_path)
    result = first.call('privacy_text', {'text': 'original'})
    first.accept_cached_result(result)
    second = cached_client(tmp_path)
    second.call('privacy_text', {'text': 'original'})
    assert second.last_call_cache_hit
    second.discard_unaccepted_results()
    third = cached_client(tmp_path)
    third.call('privacy_text', {'text': 'original'})
    assert not third.last_call_cache_hit


def test_retry_after_later_failure_reuses_completed_text_scan(tmp_path, monkeypatch):
    import auto_pipeline
    source_dir = tmp_path/'input'; source_dir.mkdir()
    (source_dir/'paper.txt').write_text(paper(), encoding='utf-8')
    store = Store(tmp_path/'data')
    first = AutoProcessor(store, CONFIG, KEY); first.agent.opener = Transport()
    with monkeypatch.context() as patch:
        def failed_export(*args, **kwargs): raise AgentError('synthetic later failure')
        patch.setattr(auto_pipeline, 'export_package', failed_export)
        failed = first.run(scan_sources(source_dir))
    assert failed['items'][0]['status'] == 'reject'
    assert [m['task'] for m, _ in first.agent.opener.requests] == ['privacy_text']
    second = AutoProcessor(Store(store.root), CONFIG, KEY); second.agent.opener = Transport()
    passed = second.run(scan_sources(source_dir))
    assert passed['items'][0]['status'] == 'pass', passed['items'][0]
    assert second.agent.opener.requests == []
    assert passed['items'][0]['billing']['requests'] == 0


def test_bad_image_semantics_are_not_cached_or_approved(tmp_path):
    source_dir = tmp_path/'input'; source_dir.mkdir()
    document(source_dir/'paper.docx')
    store = Store(tmp_path/'data')
    def blank_research(message):
        result = response(message['task'])
        if message['task'] == 'privacy_image': result.update(text='', description='')
        return result
    first = AutoProcessor(store, CONFIG, KEY); first.agent.opener = Transport(blank_research)
    failed = first.run(scan_sources(source_dir))
    assert failed['items'][0]['reason_kind'] == 'evidence'
    assert not failed['items'][0]['automatic_approval']
    assert not list((store.root/'private'/'stage_cache').glob('*.json'))
    second = AutoProcessor(store, CONFIG, KEY); second.agent.opener = Transport()
    passed = second.run(scan_sources(source_dir))
    assert passed['items'][0]['status'] == 'pass', passed['items'][0]
    assert [m['task'] for m, _ in second.agent.opener.requests].count('privacy_image') == 2


def test_cache_is_optional_when_disk_write_fails(tmp_path, monkeypatch):
    first = cached_client(tmp_path)
    result = first.call('privacy_text', {'text': 'original'})
    def failed_replace(*args): raise OSError('synthetic disk failure')
    with monkeypatch.context() as patch:
        patch.setattr('stage_cache.os.replace', failed_replace)
        first.accept_cached_result(result)
    assert not list((tmp_path/'private'/'stage_cache').iterdir())
    second = cached_client(tmp_path)
    assert second.call('privacy_text', {'text': 'original'})['decision'] == 'pass'
    assert len(second.opener.requests) == 1


def test_no_key_or_prompt_is_persisted_even_if_server_echoes_key(tmp_path):
    def secret_response(message): return {**response(message['task']), 'reason': KEY}
    first = cached_client(tmp_path, transport=Transport(secret_response))
    result = first.call('privacy_text', {'text': 'PROMPT_MUST_STAY_OUT_OF_CACHE'})
    first.accept_cached_result(result)
    assert not list((tmp_path/'private'/'stage_cache').glob('*.json'))
    second = cached_client(tmp_path)
    result = second.call('privacy_text', {'text': 'PROMPT_MUST_STAY_OUT_OF_CACHE'})
    second.accept_cached_result(result)
    path, = list((tmp_path/'private'/'stage_cache').glob('*.json'))
    persisted = path.read_text(encoding='utf-8')
    assert KEY not in persisted and 'PROMPT_MUST_STAY_OUT_OF_CACHE' not in persisted


def test_ledger_failure_does_not_leave_accepted_checkpoint(tmp_path):
    from billing import Ledger
    first = cached_client(tmp_path)
    class FailedLedger(Ledger):
        def finish(self, *args): raise OSError('synthetic usage write failure')
    first.ledger = FailedLedger(tmp_path)
    with pytest.raises(AgentError, match='费用记录'):
        first.call('privacy_text', {'text': 'original'})
    first.discard_unaccepted_results()
    second = cached_client(tmp_path)
    second.call('privacy_text', {'text': 'original'})
    assert len(second.opener.requests) == 1


def test_completed_scanned_page_replays_after_later_page_is_stopped(tmp_path):
    import fitz
    import threading
    from engine import digest
    from evidence_tasks import resolve_scoped
    source = tmp_path/'pages.pdf'
    with fitz.open() as pdf:
        for y in [10, 20]:
            page = pdf.new_page(width=100, height=100)
            page.draw_line((10, y), (80, 80))
        pdf.save(source)
    def scanned_response(message):
        assert message['task'] == 'extract_page'
        return {'decision': 'pass', 'reason': 'page read', 'sensitive_boxes': [],
                'blocks': [{'kind': 'text', 'text': '完整研究正文'+str(message['document_data']['page'])}]}
    def metadata():
        return {'blocks': [], 'resources': [], 'issues': [
            {'code': 'scanned_page', 'position': {'page': n}, 'status': 'open'} for n in [1, 2]]}
    first = cached_client(tmp_path/'store', digest(source), transport=Transport(scanned_response))
    first.stop = threading.Event()
    progress = []
    def stop_second(*event):
        progress.append(event)
        if len(progress) == 2: first.stop.set()
    with pytest.raises(AgentError, match='已停止'):
        resolve_scoped(metadata(), tmp_path, source, tmp_path/'visual1', 80, first, stop_second)
    first.discard_unaccepted_results()
    second = cached_client(tmp_path/'store', digest(source), transport=Transport(scanned_response))
    meta = metadata()
    resolve_scoped(meta, tmp_path, source, tmp_path/'visual2', 80, second, lambda *a: None)
    assert [b['text'] for b in meta['blocks']] == ['完整研究正文1', '完整研究正文2']
    assert [m['document_data']['page'] for m, _ in second.opener.requests] == [2]
    assert all(issue['status'] == 'resolved' for issue in meta['issues'])


def test_stop_after_last_model_result_prevents_paper_approval(tmp_path):
    source_dir = tmp_path/'input'; source_dir.mkdir()
    (source_dir/'paper.txt').write_text(paper(), encoding='utf-8')
    processor = AutoProcessor(Store(tmp_path/'data'), CONFIG, KEY)
    processor.agent.opener = Transport()
    def stop_before_publish(state):
        if state.get('progress', {}).get('stage') == '生成论文知识包': processor.stop.set()
    result = processor.run(scan_sources(source_dir), stop_before_publish)
    item = result['items'][0]
    assert item['reason_kind'] == 'stopped'
    assert not item['automatic_approval'] and not item.get('package_path')
    assert len(list((processor.store.root/'private'/'stage_cache').glob('*.json'))) == 1


@pytest.mark.parametrize('replay_before_rejection', [False, True])
def test_later_image_privacy_failure_invalidates_visuals_for_corrected_retry(tmp_path, replay_before_rejection):
    source_dir = tmp_path/'input'; source_dir.mkdir()
    source = source_dir/'paper.docx'; document(source)
    store = Store(tmp_path/'data')
    hidden = 'HIDDEN_STUDENT_NAME'
    def model_response(message, corrected=False):
        result = response(message['task'])
        if message['task'] == 'privacy_image':
            if corrected:
                boxes=[] if message['document_data'].get('phase')=='verify_masked' else [[0,0,.2,.2]]
                result.update(text='原始观测值100；署名：[已脱敏]', sensitive_boxes=boxes)
            else:
                result.update(text='原始观测值100；署名：'+hidden)
        elif message['task'] == 'privacy_text':
            result['sensitive_strings'] = [hidden] if hidden in message['document_data']['text'] else []
        return result
    if replay_before_rejection:
        seed = AutoProcessor(store, CONFIG, KEY); seed.agent.opener = Transport(model_response)
        def stop_before_text(state):
            if state.get('progress', {}).get('stage') == 'AI 文本脱敏': seed.stop.set()
        stopped = seed.run(scan_sources(source_dir), stop_before_text)
        assert stopped['items'][0]['reason_kind'] == 'stopped'
        assert len(list((store.root/'private'/'stage_cache').glob('*.json'))) == 2
    first = AutoProcessor(store, CONFIG, KEY)
    first.agent.opener = Transport(model_response)
    failed = first.run(scan_sources(source_dir))
    assert failed['items'][0]['reason_kind'] == 'privacy'
    assert '图片中的额外敏感内容' in failed['items'][0]['reason']
    assert not failed['items'][0]['automatic_approval']
    if replay_before_rejection:
        assert [m['task'] for m, _ in first.agent.opener.requests] == ['privacy_text']
    second = AutoProcessor(Store(store.root), CONFIG, KEY)
    second.agent.opener = Transport(lambda message: model_response(message, corrected=True))
    retried = second.run(scan_sources(source_dir))
    assert retried['items'][0]['status'] == 'pass', retried['items'][0]
    assert [m['task'] for m, _ in second.agent.opener.requests].count('privacy_image') == 4
    assert hidden not in Path(retried['items'][0]['path']).read_text(encoding='utf-8')


@pytest.mark.parametrize('replay', [False, True])
def test_invalidate_accepted_results_is_bound_to_current_source(tmp_path, replay):
    from stage_cache import StageCache
    client = cached_client(tmp_path, source='a'*64)
    payload = {'text': 'same submitted fragment'}
    result = client.call('privacy_text', payload); client.accept_cached_result(result)
    client.bind_stage_cache(StageCache(tmp_path), 'b'*64, 'fixture-v1')
    result = client.call('privacy_text', payload); client.accept_cached_result(result)
    if replay:
        client.bind_stage_cache(StageCache(tmp_path), 'b'*64, 'fixture-v1')
        result = client.call('privacy_text', payload)
        assert client.last_call_cache_hit
        client.accept_cached_result(result)
    client.invalidate_accepted_results()
    preserved = cached_client(tmp_path, source='a'*64)
    preserved.call('privacy_text', payload)
    assert preserved.last_call_cache_hit and preserved.opener.requests == []
    invalidated = cached_client(tmp_path, source='b'*64)
    invalidated.call('privacy_text', payload)
    assert not invalidated.last_call_cache_hit


def test_stop_during_final_export_prevents_publish_and_preserves_cache(tmp_path, monkeypatch):
    import auto_pipeline
    source_dir = tmp_path/'input'; source_dir.mkdir()
    (source_dir/'paper.txt').write_text(paper(), encoding='utf-8')
    processor = AutoProcessor(Store(tmp_path/'data'), CONFIG, KEY)
    processor.agent.opener = Transport()
    original_export = auto_pipeline.export_package
    def stop_during_export(*args, **kwargs):
        result = original_export(*args, **kwargs)
        processor.stop.set()
        return result
    monkeypatch.setattr(auto_pipeline, 'export_package', stop_during_export)
    result = processor.run(scan_sources(source_dir))
    item = result['items'][0]
    assert item['reason_kind'] == 'stopped'
    assert not item['automatic_approval'] and not item.get('package_path')
    assert not list(processor.store.runs.glob('*/packages/*'))
    assert len(list((processor.store.root/'private'/'stage_cache').glob('*.json'))) == 1


@pytest.mark.parametrize('cached_classification', [False, True])
def test_classification_invalid_after_redaction_is_retried(tmp_path, cached_classification):
    from auto_pipeline import VERSION
    from engine import digest
    source_dir = tmp_path/'input'; source_dir.mkdir()
    source = source_dir/'paper.txt'
    source.write_text('题目：企业内部控制\n有效性\n研究\n专业：会计学\n摘要\n'
                      + '企业内部控制和财务管理的研究资料。'*300
                      + '\n参考文献\n[1]示例文献', encoding='utf-8')
    store = Store(tmp_path/'data')
    hidden_title = '需要移除的署名标题'
    def model_response(message, corrected=False):
        result = response(message['task'])
        if message['task'] == 'classify':
            result.update(major='会计学', title='企业内部控制有效性研究' if corrected else hidden_title)
        elif message['task'] == 'privacy_text':
            result['sensitive_strings'] = [hidden_title] if hidden_title in message['document_data']['text'] else []
        return result
    first = AutoProcessor(store, CONFIG, KEY); first.agent.opener = Transport(model_response)
    failed = first.run(scan_sources(source_dir))
    assert failed['items'][0]['reason_kind'] == 'classification'
    assert '脱敏后的题名' in failed['items'][0]['reason']
    assert not failed['items'][0]['automatic_approval']
    if cached_classification:
        # Simulate an older accepted checkpoint to exercise rejection of replay,
        # with the exact classifier payload captured from the actual processor.
        payload, = [m['document_data'] for m, _ in first.agent.opener.requests if m['task'] == 'classify']
        seed = cached_client(store.root, digest(source), VERSION, transport=Transport(model_response))
        result = seed.call('classify', payload); seed.accept_cached_result(result)
        replay = AutoProcessor(Store(store.root), CONFIG, KEY); replay.agent.opener = Transport(model_response)
        failed_again = replay.run(scan_sources(source_dir))
        assert failed_again['items'][0]['reason_kind'] == 'classification'
        assert any(m['task'] == 'classify' and m['cache_hit'] for m in failed_again['items'][0]['processing_metrics'])
    second = AutoProcessor(Store(store.root), CONFIG, KEY)
    second.agent.opener = Transport(lambda message: model_response(message, corrected=True))
    result = second.run(scan_sources(source_dir))
    assert result['items'][0]['status'] == 'pass', result['items'][0]
    assert result['items'][0]['title'] == '企业内部控制有效性研究'
    assert [m['task'] for m, _ in second.agent.opener.requests].count('classify') == 1


def test_cache_miss_diagnostic_reports_first_request_field_without_body(tmp_path):
    first = cached_client(tmp_path)
    result = first.call('privacy_text', {'text': 'original', 'page': 1})
    first.accept_cached_result(result)
    second = cached_client(tmp_path)
    second.call('privacy_text', {'text': 'changed', 'page': 1})
    diagnostic = second.last_cache_diagnostic
    assert diagnostic['status'] == 'miss'
    assert diagnostic['reason'] == 'request_field_changed'
    assert diagnostic['first_difference'] == 'messages[1].document_data.text'
    assert diagnostic['source_hash'] == 'a' * 64
    assert len(diagnostic['cache_key_prefix']) == 12
    assert 'original' not in json.dumps(diagnostic, ensure_ascii=False)
    assert KEY not in json.dumps(diagnostic, ensure_ascii=False)
    miss_log = list((tmp_path/'private'/'stage_cache').glob('misses.jsonl'))
    assert miss_log and 'document_data.text' in miss_log[0].read_text(encoding='utf-8')


def test_same_request_is_hit_and_no_miss_diagnostic_is_written(tmp_path):
    first = cached_client(tmp_path)
    result = first.call('privacy_text', {'text': 'original'})
    first.accept_cached_result(result)
    second = cached_client(tmp_path)
    second.call('privacy_text', {'text': 'original'})
    assert second.last_call_cache_hit
    assert second.last_cache_diagnostic['status'] == 'hit'
    assert not list((tmp_path/'private'/'stage_cache').glob('misses.jsonl'))
