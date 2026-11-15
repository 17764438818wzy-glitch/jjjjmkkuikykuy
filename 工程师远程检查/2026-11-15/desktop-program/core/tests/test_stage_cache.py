"""Real producer file contracts; only the external model is a deterministic fixture."""
import shutil
from pathlib import Path
from types import SimpleNamespace

import pytest

from paperflow.config import Config
from paperflow.package import load, save, sha
from paperflow.phase_cache import input_fingerprint, read_cache
from paperflow.producer import HarnessProducer, ProductionError


@pytest.fixture
def stages(tmp_path, package_factory):
    source = tmp_path/'source.txt'
    source.write_text('研究方法\n通过指标分解解释盈利能力。', encoding='utf-8')
    material, identity, catalog = package_factory(source_sha=sha(source))
    config = Config(home=str(tmp_path/'runtime'), catalog_path=str(tmp_path/'catalog.json'))
    save(config.catalog_path, catalog)
    task = dict(identity, source_path=str(source), revision=1, draft_dir=str(material))
    class Harness:
        calls = []
        def __init__(self, **options): self.output = Path(options['cwd'])
        def start(self): pass
        def close(self): pass
        def run(self, *args, **kwargs):
            self.calls.append(self.output)
            for path in material.iterdir():
                if path.is_file() and path.name not in ('manifest.json', 'READY.json'):
                    shutil.copy2(path, self.output/path.name)
            return SimpleNamespace(finish_reason='completed', events=[], final_response='', session_id='fixture')
    return config, task, material, Harness


@pytest.mark.parametrize('stage', ['round1', 'round2', 'round3_assets', 'round3_redact', 'round3_chunk'])
def test_every_normal_stage_reuses_complete_result_across_thread_restart(stages, stage):
    c, task, _, harness = stages
    first = HarnessProducer(c, factory=harness).run_stage(task, stage)
    second = HarnessProducer(c, factory=harness).run_stage(dict(task, thread_id='another-run'), stage)
    assert second.get('cache_hit') is True
    assert len(harness.calls) == 1
    assert second['draft_dir'] == first['draft_dir']


def test_cache_cannot_hide_a_changed_source(stages):
    c, task, _, harness = stages
    producer = HarnessProducer(c, factory=harness)
    producer.run_stage(task, 'round3_assets')
    Path(task['source_path']).write_text('changed', encoding='utf-8')
    with pytest.raises(ProductionError, match='source'):
        producer.run_stage(task, 'round3_assets')


def test_rerun_never_overwrites_previously_complete_directory(stages):
    c, task, material, harness = stages
    producer = HarnessProducer(c, factory=harness)
    first = producer.run_stage(task, 'round3_assets')
    old = (Path(first['draft_dir'])/'article.redacted.md').read_bytes()
    (material/'article.redacted.md').write_text('新的实际输入', encoding='utf-8')
    second = producer.run_stage(task, 'round3_assets')
    assert second['draft_dir'] != first['draft_dir']
    assert (Path(first['draft_dir'])/'article.redacted.md').read_bytes() == old


def test_empty_hash_manifest_and_missing_completion_marker_are_not_cache(stages):
    from paperflow.phase_cache import cache_path
    c, task, _, harness = stages
    producer = HarnessProducer(c, factory=harness)
    first = producer.run_stage(task, 'round3_assets')
    fingerprint = input_fingerprint(c, task, 'round3_assets')
    marker = cache_path(c, task, 'round3_assets', fingerprint)
    entry = load(marker)
    entry['files'] = {}
    save(marker, entry)
    assert read_cache(c, task, 'round3_assets', fingerprint) is None
    marker.unlink()
    assert read_cache(c, task, 'round3_assets', fingerprint) is None
    assert Path(first['draft_dir']).is_dir()


def test_model_access_and_actual_private_dependencies_invalidate_cache(stages):
    c, task, material, _ = stages
    before = input_fingerprint(c, task, 'round3_chunk')
    save(material/'private/redaction_map.json', {'operations': []})
    assert input_fingerprint(c, task, 'round3_chunk') != before
    before = input_fingerprint(c, task, 'round3_chunk')
    c.model_access_fingerprint = 'new-upstream-config'
    assert input_fingerprint(c, task, 'round3_chunk') != before


def test_only_changed_stage_prompt_invalidates_its_input(stages, tmp_path, monkeypatch):
    import paperflow.phase_cache as cache
    c, task, _, _ = stages
    prompts = tmp_path/'prompts'
    shutil.copytree(cache.PROMPTS, prompts)
    monkeypatch.setattr(cache, 'PROMPTS', prompts)
    before = {s: input_fingerprint(c, task, s) for s in ('round1', 'round2', 'round3_chunk')}
    path = prompts/'round2.original.txt'
    path.write_text(path.read_text(encoding='utf-8')+'\nchanged prompt', encoding='utf-8')
    assert input_fingerprint(c, task, 'round1') == before['round1']
    assert input_fingerprint(c, task, 'round2') != before['round2']
    assert input_fingerprint(c, task, 'round3_chunk') == before['round3_chunk']
