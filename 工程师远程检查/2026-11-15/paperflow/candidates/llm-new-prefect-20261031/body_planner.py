"""Create a local body planner matching the current upstream capacity logic."""
import hashlib
import json
from pathlib import Path

from body_runner import _reference
from segment_plan_contract import verify_plan


def _file_sha(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def make_body_planner(backend, manifest, work, connection):
    """Load no key and issue no request; the supplied connection is only checked."""
    try:
        config_path = Path(backend.settings.path('model_config')).resolve(strict=True)
        template_path = Path(backend.settings.path('template_path')).resolve(strict=True)
        config = json.loads(config_path.read_text(encoding='utf8'))
        limits = dict(config.get('preextract_limits') or {})
        model, base = config['text_model'], config['base_url']
        valid = (limits and limits.get('model') == model
                 and str(limits.get('base_url', '')).rstrip('/') == str(base).rstrip('/')
                 and limits.get('supports_images') is True
                 and connection.get('model') == model
                 and str(connection.get('base_url', '')).rstrip('/') == str(base).rstrip('/'))
        work = Path(work).resolve(strict=True)
        valid = valid and work == Path(manifest['body_work']).resolve(strict=True)
        valid = valid and manifest['capacity']['status'] == 'planned'
    except (AttributeError, KeyError, OSError, TypeError, ValueError) as exc:
        raise ValueError('正文模型或前处理容量配置不可核验') from exc
    if not valid:
        raise ValueError('正文模型或前处理容量配置不一致')
    config_sha, template_sha = _file_sha(config_path), _file_sha(template_path)
    from preextract_contract import load_template
    template, _ = load_template(template_path)
    planning_connection = {**limits, 'model': model, 'max_tokens': config.get('max_tokens', 8192)}

    class Planner:
        def check_dependencies(self):
            if _file_sha(config_path) != config_sha or _file_sha(template_path) != template_sha:
                raise ValueError('正文模型或模板配置发生变化')

        def __call__(self):
            self.check_dependencies()
            material = _reference(manifest['material'], '正文材料')
            reading = _reference(manifest['reading_material'], '正文阅读材料')
            if (material.get('source', {}).get('source_sha256') != manifest['body']['sha256']
                    or reading.get('source', {}).get('source_sha256') != manifest['body']['sha256']):
                raise ValueError('正文阅读材料来源不一致')
            from paperflow_prefect.body_clean_handoff import plan_with_available_images
            from paperflow_prefect.body_segment_tokenizer import with_body_token_counter
            planning = with_body_token_counter(planning_connection)
            planning['context_window'] = min(int(planning['context_window']), 1000000)
            plan, adapted, _ = plan_with_available_images(
                reading, work, template, backend.catalog, planning_connection, planning)
            verify_plan(plan, manifest['capacity']['plan_identity'],
                        source_sha256=manifest['body']['sha256'],
                        reading_sha256=manifest['reading_material']['sha256'])
            return plan, material, adapted
    return template, Planner()
