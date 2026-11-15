import threading
from pathlib import Path
from types import SimpleNamespace

import pytest

from paperflow.config import Config
from paperflow.package import sha, save
from paperflow.producer import HarnessProducer, delivery_correction_count
from test_stage_cache import stages


class Admissions:
    """Only models the control seam here; real AF_PIPE/Job is tested separately."""
    def __init__(self):
        self.calls = []
        self.closed = threading.Event()
        self.cancellation = None
    def run(self, kind, resource, fn, *, total_timeout):
        if self.closed.is_set(): raise ValueError('admission stopped')
        self.calls.append((kind, resource))
        return fn(self.closed)
    def bind(self, cancellation): self.cancellation = cancellation
    def unbind(self, cancellation): self.cancellation = None
    def commit_cache(self, *args): raise AssertionError('paused output must not be cached')


def test_pause_before_json_correction_does_not_start_or_charge_it(tmp_path):
    control = Admissions()
    class Harness:
        calls = closes = 0
        def __init__(self, **options): self.output = Path(options['cwd'])
        def start(self): pass
        def close(self): type(self).closes += 1
        def run(self, *args, **kwargs):
            type(self).calls += 1
            (self.output/'profile.json').write_text('{broken', encoding='utf-8')
            control.closed.set()
            control.cancellation.request('pause')
            return SimpleNamespace(finish_reason='completed', events=[])
    c = Config(home=str(tmp_path/'runtime'))
    source = tmp_path/'source.txt';source.write_text('original')
    task = dict(task_id='task', source_id='source', source_sha256=sha(source),source_path=str(source),revision=1)
    with pytest.raises((ValueError, RuntimeError)):
        HarnessProducer(c, factory=Harness, control=control).run_stage(task, 'round1')
    assert Harness.calls == 1 and Harness.closes == 1
    assert delivery_correction_count(c, task) == 0
    assert not list((c.root/'work').rglob('phase-cache/*/*.json'))


def test_timer_and_pause_close_same_harness_once(tmp_path):
    control = Admissions()
    class Harness:
        closes = 0
        def __init__(self, **options): self.done=threading.Event()
        def start(self): pass
        def close(self):
            type(self).closes += 1
            self.done.set()
        def run(self, *args, **kwargs):
            assert self.done.wait(3)
            control.cancellation.request('pause')
            return SimpleNamespace(finish_reason='completed', events=[])
    c = Config(home=str(tmp_path/'runtime'),model_timeout=.05)
    source=tmp_path/'source.txt';source.write_text('original')
    task=dict(task_id='task',source_id='source',source_sha256=sha(source),source_path=str(source),revision=1)
    with pytest.raises(TimeoutError):
        HarnessProducer(c, factory=Harness, control=control).run_stage(task, 'round1')
    assert Harness.closes == 1


def test_same_session_correction_has_separate_admission_and_shared_budget(stages):
    c, task, _, Harness = stages
    control = Admissions()
    class SyntaxHarness(Harness):
        count = 0
        def run(self, *args, **kwargs):
            result = super().run(*args, **kwargs)
            type(self).count += 1
            if self.count == 1: (self.output/'profile.json').write_text('{broken', encoding='utf-8')
            return result
    from paperflow.phase_cache import write_cache
    control.commit_cache = lambda task, stage, fingerprint, result: write_cache(c,task,stage,fingerprint,result)
    result = HarnessProducer(c, factory=SyntaxHarness, control=control).run_stage(task,'round1')
    assert result['correction_count'] == 1
    assert [kind for kind, _ in control.calls].count('llm') == 2
    assert result['processing_history'][0]['session_id'] == result['processing_history'][1]['session_id']


def test_failed_stage_points_to_retained_attempt_not_an_old_fixed_directory(stages):
    c,task,_,Harness=stages
    class Empty(Harness):
        def run(self,*args,**kwargs):return SimpleNamespace(finish_reason='completed',events=[])
    task={k:v for k,v in task.items() if k!='draft_dir'}
    from paperflow.producer import ProductionError
    with pytest.raises(ProductionError) as failure:
        HarnessProducer(c,factory=Empty).run_stage(task,'round1')
    assert Path(failure.value.draft_dir).is_dir()
    assert 'attempts' in Path(failure.value.draft_dir).parts
