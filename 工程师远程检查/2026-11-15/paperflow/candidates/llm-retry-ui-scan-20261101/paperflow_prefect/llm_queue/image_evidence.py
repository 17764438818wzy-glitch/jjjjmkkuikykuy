"""Verify original and exact request-bound image bytes without model calls."""
from pathlib import Path


def verify_source_images(reading):
    """Check frozen original bytes even when the upstream recorded an image gap."""
    if not isinstance(reading, dict) or not isinstance(reading.get('assets'), list):
        raise ValueError('正文图片来源不可核验')
    from preextract_images import file_sha
    seen = set()
    for asset in reading['assets']:
        if not isinstance(asset, dict) or not asset.get('id') or asset['id'] in seen:
            raise ValueError('正文图片身份不可核验')
        seen.add(asset['id'])
        if asset.get('image_path'):
            path = Path(asset['image_path'])
            if not path.is_file() or file_sha(path) != asset.get('sha256'):
                raise ValueError('图片版本发生变化，原请求和结果保留')


def verify_body_images(reading, part, model_dir):
    if (not isinstance(part, dict) or not isinstance(part.get('bundle'), dict)
            or not isinstance(part.get('body'), dict)):
        raise ValueError('正文图片计划不可核验')
    verify_source_images(reading)
    from preextract_images import restore_request_images
    # This reads the locally constructed input request, never an LLM reply.
    # The adapter decodes each planned image and compares its exact bytes.
    restore_request_images(part['bundle'], Path(model_dir), part['body'])
