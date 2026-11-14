"""Bind a body MD to its existing work/handoff, without changing request journals."""
import hashlib
import json
from pathlib import Path

LEGACY_NAME = '论文预提取_空白模板_v1.md'
REFERENCE_FILE = 'private/body-template-ref.json'


def template_reference(path):
    path = Path(path).resolve(strict=True)
    return {'path': str(path), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}


def _referenced_path(reference):
    if not isinstance(reference, dict) or not isinstance(reference.get('path'), str):
        raise ValueError('正文模板引用无效')
    path = Path(reference['path']).resolve(strict=True)
    if template_reference(path) != reference:
        raise ValueError('正文绑定模板发生变化')
    return path


def _legacy_path(configured):
    configured = Path(configured)
    original = configured.parent / LEGACY_NAME
    if original.is_file():
        return original.resolve(strict=True)
    # Preserve existing explicitly supplied custom templates. The named system
    # revision may never silently replace a missing original template.
    if configured.name.startswith('论文预提取_空白模板_v'):
        raise ValueError('原正文模板不可读取')
    return configured.resolve(strict=True)


def manifest_template_path(settings, manifest, *, work=None):
    if 'template_ref' in manifest:
        return _referenced_path(manifest['template_ref'])
    if work is not None:
        reference = Path(work) / REFERENCE_FILE
        if reference.exists():
            return _referenced_path(json.loads(reference.read_text(encoding='utf8')))
    # Handoffs created before the extension were all frozen using the original
    # MD. Native verify_plan/request checks still verify the exact bytes.
    return _legacy_path(settings.path('template_path'))


def work_template_path(work, configured):
    work = Path(work)
    reference = work / REFERENCE_FILE
    if reference.exists():
        return _referenced_path(json.loads(reference.read_text(encoding='utf8')))
    model = work / 'model'
    if model.is_dir() and any(model.iterdir()):
        return _legacy_path(configured)
    return Path(configured).resolve(strict=True)


def bind_work_template(work, configured):
    work = Path(work)
    reference_path = work / REFERENCE_FILE
    selected = work_template_path(work, configured)
    reference = template_reference(selected)
    reference_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with reference_path.open('x', encoding='utf8') as stream:
            json.dump(reference, stream, ensure_ascii=False, indent=2)
            stream.write('\n')
    except FileExistsError:
        if _referenced_path(json.loads(reference_path.read_text(encoding='utf8'))) != selected:
            raise ValueError('正文模板绑定冲突')
    return reference
