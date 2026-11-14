import copy
import threading
import time

import pytest
from PIL import Image, ImageDraw

import auto_evidence as ae
from processing_state import ProcessingState


def document(root, count=3):
    (root / '_review').mkdir(parents=True)
    (root / 'assets').mkdir()
    im = Image.new('RGB', (40, 40), 'white')
    ImageDraw.Draw(im).ellipse((5, 5, 35, 35), outline='blue', width=2)
    resources, blocks = [], []
    for i in range(count):
        rid = f'image_{i}'
        im.save(root / '_review' / (rid + '.png'))
        position = {'page': i + 1, 'bbox': [2, 3, 42, 43]}
        resources.append({'id': rid, 'kind': 'image', 'position': position,
                          'local_path': f'_review/{rid}.png'})
        blocks.append({'id': f'b{i}', 'type': 'image', 'resource_id': rid,
                       'position': copy.deepcopy(position), 'caption': ''})
    return {'source_sha256': 'a' * 64, 'blocks': blocks, 'resources': resources}


class Agent:
    def __init__(self, result=None):
        self.result = result or {'decision': 'pass', 'reason': '校徽，没有研究内容',
                                'research': False, 'nonresearch_kind': 'logo',
                                'sensitive_boxes': [], 'text': '', 'description': ''}
        self.calls = []
        self.processing_metrics = []
        self.config = {'text_model': 'saved-model'}
        self.lock = threading.Lock()
        self.accepted = []

    def call(self, task, payload, images=()):
        with self.lock:
            self.calls.append(copy.deepcopy(payload))
        time.sleep(.005)
        if payload['id'] == getattr(self, 'fail_id', None):
            raise RuntimeError('unavailable')
        return copy.deepcopy(self.result)

    def accept_cached_result(self, result):
        self.accepted.append(result)


@pytest.mark.parametrize('workers', [0, 1, 4])
def test_duplicate_nonresearch_images_call_once_keep_each_location_and_checkpoint(tmp_path, workers):
    meta = document(tmp_path, 8)
    before = copy.deepcopy(meta)
    agent = Agent()
    state = ProcessingState(tmp_path, 'state.json')
    ae.media_privacy(meta, tmp_path, agent, lambda *_: None,
                     {'enabled': bool(workers), 'images_per_paper': workers or 1}, state, 'paper')
    assert len(agent.calls) == 1
    assert len(agent.processing_metrics) == 1
    assert len(agent.accepted) == 1  # Followers must not forge stage-cache records.
    assert not meta['resources'] and not meta['blocks']
    records = {r['id']: r for r in meta['excluded_resources']}
    for source in before['resources']:
        record = records[source['id']]
        assert record['position'] == source['position']
        assert record['nonresearch_evidence']['source_sha256'] == before['source_sha256']
        assert record['nonresearch_evidence']['representative_id'] == agent.calls[0]['id']
    assert sorted(state.snapshot()['papers']['paper']['images_done']) == sorted(records)


@pytest.mark.parametrize('change', ['caption', 'context', 'source_context', 'bytes', 'phase', 'source_missing'])
def test_changed_input_or_context_is_not_reused(tmp_path, monkeypatch, change):
    meta = document(tmp_path, 2)
    if change == 'caption': meta['blocks'][1]['caption'] = '另一种说明'
    if change == 'context': meta['blocks'][1]['context_before'] = ['图像是教学观察对象']
    if change == 'source_context':
        monkeypatch.setattr(ae, '_media_source_context', lambda m, b, r: {'source_context': {'text': r}})
    if change == 'bytes': Image.new('RGB', (40, 40), 'red').save(tmp_path / '_review/image_1.png')
    if change == 'phase': meta['resources'][1]['visual_processed'] = True
    if change == 'source_missing': meta.pop('source_sha256')
    agent = Agent()
    ae.media_privacy(meta, tmp_path, agent, lambda *_: None, {'enabled': True, 'images_per_paper': 4})
    assert len(agent.calls) == 2


@pytest.mark.parametrize('mode', ['research', 'reject_logo', 'unknown_kind', 'protected', 'failure'])
def test_nonshareable_verdict_never_spreads(tmp_path, mode):
    meta = document(tmp_path, 3)
    agent = Agent()
    if mode == 'research':
        agent.result.update(research=True, text='观察图像', description='研究材料')
    if mode == 'reject_logo': agent.result['decision'] = 'reject'
    if mode == 'unknown_kind': agent.result.pop('nonresearch_kind')
    if mode == 'protected':
        for b in meta['blocks']: b['native_data_available'] = True
    if mode == 'failure': agent.fail_id = 'image_0'
    try:
        ae.media_privacy(meta, tmp_path, agent, lambda *_: None,
                         {'enabled': True, 'images_per_paper': 4})
    except (ae.Rejected, RuntimeError):
        assert mode in ('protected', 'failure')
    # A failed leader must not block the remaining independent decisions.
    assert len(agent.calls) == 3


@pytest.mark.parametrize('change', ['image', 'context'])
def test_change_after_representative_applied_falls_back(tmp_path, monkeypatch, change):
    meta = document(tmp_path, 2)
    apply = ae._apply_media_job
    def changed(m, p, job):
        apply(m, p, job)
        if job['resource']['id'] == 'image_0':
            if change == 'image': Image.new('RGB', (40, 40), 'green').save(p / '_review/image_1.png')
            else:
                for block in m['blocks']:
                    if block.get('resource_id') == 'image_1': block['context_before'] = ['此图用于研究']
    monkeypatch.setattr(ae, '_apply_media_job', changed)
    agent = Agent()
    ae.media_privacy(meta, tmp_path, agent, lambda *_: None, {'enabled': True, 'images_per_paper': 4})
    assert len(agent.calls) == 2


def test_follower_apply_failure_is_not_checkpointed_done(tmp_path, monkeypatch):
    meta = document(tmp_path, 2)
    original = ae._apply_media_job
    def apply(m, p, job):
        if job['resource']['id'] == 'image_1': raise OSError('write failure')
        return original(m, p, job)
    monkeypatch.setattr(ae, '_apply_media_job', apply)
    state = ProcessingState(tmp_path, 'state.json')
    agent = Agent()
    with pytest.raises(OSError):
        ae.media_privacy(meta, tmp_path, agent, lambda *_: None,
                         {'enabled': True, 'images_per_paper': 4}, state, 'paper')
    row = state.snapshot()['papers']['paper']
    assert row['images_done'] == ['image_0'] and row['images_failed'] == ['image_1']


def test_reuse_does_not_cross_paper_invocations(tmp_path):
    agent = Agent()
    for part in ('a', 'b'):
        root = tmp_path / part
        meta = document(root, 2)
        ae.media_privacy(meta, root, agent, lambda *_: None)
    assert len(agent.calls) == 2


def test_identical_file_with_different_rendered_input_is_not_reused(tmp_path, monkeypatch):
    import evidence_render
    original = evidence_render.load_visual_image
    def render(path):
        image = original(path)
        if path.name == 'image_1.png': image.putpixel((0, 0), (255, 0, 0))
        return image
    monkeypatch.setattr(evidence_render, 'load_visual_image', render)
    meta = document(tmp_path, 2)
    agent = Agent()
    ae.media_privacy(meta, tmp_path, agent, lambda *_: None, {'enabled': True, 'images_per_paper': 4})
    assert len(agent.calls) == 2


def test_cancel_after_representative_prevents_follower_apply(tmp_path):
    meta = document(tmp_path, 2)
    agent = Agent()
    agent.stop = threading.Event()
    state = ProcessingState(tmp_path, 'state.json')
    complete = state.image_done
    def done(key, rid):
        complete(key, rid)
        agent.stop.set()
    state.image_done = done
    with pytest.raises(ae.AgentError):
        ae.media_privacy(meta, tmp_path, agent, lambda *_: None,
                         {'enabled': True, 'images_per_paper': 4}, state, 'paper')
    row = state.snapshot()['papers']['paper']
    assert row['images_done'] == ['image_0'] and row['images_failed'] == ['image_1']
    assert len(meta['resources']) == 1 and len(agent.calls) == 1
