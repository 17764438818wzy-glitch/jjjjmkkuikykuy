"""Necessary final-review fixes exercised through existing public boundaries."""
from pathlib import Path
import sqlite3
import threading
import time
from types import SimpleNamespace

import pytest

from paperflow.config import Config
from paperflow.package import load, save, sha, PackageError
from paperflow.producer import HarnessProducer, SyntaxRepairExhausted
from test_scheduler import scheduler_case
from test_stage_cache import stages


def test_scheduler_survives_temporary_sqlite_writer(scheduler_case):
    manager, _, scheduler = scheduler_case
    stop = threading.Event()
    failures = []
    connection = sqlite3.connect(manager.store.path)
    connection.execute('BEGIN IMMEDIATE')
    def run():
        try:
            scheduler.run(stop, interval=.03)
        except Exception as exc:
            failures.append(type(exc).__name__)
    thread = threading.Thread(target=run)
    thread.start()
    try:
        time.sleep(1.6)
        connection.rollback()
        time.sleep(.3)
        assert thread.is_alive(), failures
        # A later normal tick is usable, without resuming business admission.
        assert manager.stopped.is_set()
    finally:
        connection.close()
        stop.set()
        thread.join(5)
    assert not thread.is_alive() and not failures


@pytest.mark.parametrize('failure', ['body_loss', 'missing_file'])
def test_required_delivery_defect_is_corrected_in_same_stage(tmp_path, failure):
    sessions, prompts = [], []
    class Harness:
        def __init__(self, **kwargs):
            self.root = Path(kwargs['cwd'])
        def start(self): pass
        def close(self): pass
        def run(self, prompt, **kwargs):
            sessions.append(kwargs['session_id']); prompts.append(prompt)
            save(self.root/'profile.json', {'material_disposition': 'keep', 'article': {'title': 'Methods'}})
            save(self.root/'tree.json', {'nodes': [{'id': 's', 'title': 'Methods'}]})
            save(self.root/'assets.json', {'items': []})
            (self.root/'article.redacted.md').write_text('first line\nsecond line\n', encoding='utf-8')
            if failure != 'missing_file' or len(sessions) > 1:
                save(self.root/'chunks.json', {'chunks': [{'id': 'b', 'kind': 'body',
                    'line_start': 1, 'line_end': 1 if len(sessions) == 1 else 2}]})
            return SimpleNamespace(finish_reason='completed', events=[])
    original = tmp_path/'source.txt'; original.write_text('first line\nsecond line\n')
    config = Config(home=str(tmp_path/'runtime'))
    task = dict(task_id='t', source_id='s', source_path=str(original), source_sha256=sha(original), revision=1)
    result = HarnessProducer(config, factory=Harness).run_stage(task, 'round3_chunk')
    assert len(sessions) == 2 and len(set(sessions)) == 1
    assert result['correction_count'] == 1
    assert load(Path(result['draft_dir'])/'chunks.json')['chunks'][0]['content'] == 'first line\nsecond line\n'
    assert '定点' in prompts[1] and '不重写正文' in prompts[1]


def test_syntax_and_delivery_defects_share_two_correction_slots(tmp_path):
    calls = []
    class Harness:
        def __init__(self, **kwargs): self.root = Path(kwargs['cwd'])
        def start(self): pass
        def close(self): pass
        def run(self, prompt, **kwargs):
            calls.append(prompt)
            save(self.root/'tree.json', {'nodes': []})
            save(self.root/'assets.json', {'items': []})
            (self.root/'article.redacted.md').write_text('a\nb\n')
            if len(calls) == 1:
                (self.root/'profile.json').write_text('{broken')
            else:
                save(self.root/'profile.json', {'material_disposition': 'keep'})
                save(self.root/'chunks.json', {'chunks': [{'kind': 'body', 'line_start': 1, 'line_end': 1}]})
            return SimpleNamespace(finish_reason='completed', events=[])
    original = tmp_path/'source.txt'; original.write_text('a\nb\n')
    config = Config(home=str(tmp_path/'runtime'))
    task = dict(task_id='t', source_id='s', source_path=str(original), source_sha256=sha(original), revision=1)
    with pytest.raises(SyntaxRepairExhausted):
        HarnessProducer(config, factory=Harness).run_stage(task, 'round3_chunk')
    assert len(calls) == 3
    assert load(config.root/'private/delivery-corrections/t.json')['count'] == 2


def test_asset_description_is_corrected_before_cache(stages):
    config, task, material, Base = stages
    calls = []
    class Harness(Base):
        def run(self, prompt, **kwargs):
            result = super().run(prompt, **kwargs); calls.append(prompt)
            if len(calls) == 1:
                assets = load(self.output/'assets.json')
                assets['items'][0].pop('description')
                save(self.output/'assets.json', assets)
            return result
    result = HarnessProducer(config, factory=Harness).run_stage(task, 'round3_assets')
    assert len(calls) == 2 and result['correction_count'] == 1
    assert '只补 assets.json' in calls[-1]
    assert load(Path(result['draft_dir'])/'assets.json')['items'][0]['description']


def test_text_body_coverage_is_corrected_before_cache(stages):
    config, task, _, Base = stages
    calls = []
    class Harness(Base):
        def run(self, prompt, **kwargs):
            result = super().run(prompt, **kwargs); calls.append(prompt)
            if len(calls) == 1:
                chunks = load(self.output/'chunks.json')
                chunks['chunks'][0]['content'] = 'missing rest'
                save(self.output/'chunks.json', chunks)
            return result
    result = HarnessProducer(config, factory=Harness).run_stage(task, 'round3_chunk')
    assert len(calls) == 2 and result['correction_count'] == 1
    assert '不重写正文' in calls[-1]


def test_conflicting_source_identity_never_spends_correction(stages):
    config, task, _, Base = stages
    calls = []
    class Harness(Base):
        def run(self, prompt, **kwargs):
            result = super().run(prompt, **kwargs); calls.append(prompt)
            profile = load(self.output/'profile.json'); profile['source_id'] = 'another-article'
            save(self.output/'profile.json', profile)
            return result
    with pytest.raises(PackageError) as failure:
        HarnessProducer(config, factory=Harness).run_stage(task, 'round3_chunk')
    assert failure.value.check == 'identity' and len(calls) == 1


def test_redaction_residual_is_repaired_in_redaction_and_diagnostic_is_private(stages):
    config,task,_,Base=stages
    calls=[];private_value='13912345678'
    class Harness(Base):
        def run(self,prompt,**kwargs):
            result=super().run(prompt,**kwargs);calls.append(prompt)
            if len(calls)==1:
                profile=load(self.output/'profile.json');profile['author_phone']=private_value
                save(self.output/'profile.json',profile)
                save(self.output/'private/redaction_map.json',{'values':[
                    {'file':'profile.json','kind':'phone','json_path':['author_phone'],'original':private_value,'replacement':value}
                    for value in ('[A]','[B]')]})
            else:
                diagnostic=(self.output/'failure.json').read_text(encoding='utf-8')
                assert private_value not in diagnostic
                assert 'private/' in diagnostic
                save(self.output/'private/redaction_map.json',{'values':[]})
            return result
    result=HarnessProducer(config,factory=Harness).run_stage(task,'round3_redact')
    assert len(calls)==2 and result['correction_count']==1
    assert '准确位置' in calls[1]


def test_chunk_does_not_spend_budget_to_repair_inherited_asset(stages):
    config,task,_,Base=stages;calls=[]
    class Harness(Base):
        def run(self,prompt,**kwargs):
            result=super().run(prompt,**kwargs);calls.append(prompt)
            assets=load(self.output/'assets.json');assets['items'][0].pop('description')
            save(self.output/'assets.json',assets)
            return result
    with pytest.raises(PackageError) as failure:
        HarnessProducer(config,factory=Harness).run_stage(task,'round3_chunk')
    assert len(calls)==1 and getattr(failure.value,'required_stage',None)=='round3_assets'


def test_missing_profile_content_is_repaired_in_original_stage(stages):
    config,task,_,Base=stages;calls=[]
    class Harness(Base):
        def run(self,prompt,**kwargs):
            result=super().run(prompt,**kwargs);calls.append(prompt)
            if len(calls)==1:
                save(self.output/'profile.json',{'material_disposition':'keep'})
                (self.output/'corrections.json').unlink(missing_ok=True)
            return result
    result=HarnessProducer(config,factory=Harness).run_stage(task,'round1')
    assert len(calls)==2 and result['correction_count']==1
