"""Verify original and exact request-bound image bytes without model calls."""
from pathlib import Path


def verify_body_images(reading, part, model_dir):
    if (not isinstance(reading, dict) or not isinstance(reading.get('assets'), list)
            or not isinstance(part, dict) or not isinstance(part.get('bundle'), dict)
            or not isinstance(part.get('body'), dict)):
        raise ValueError('正文图片计划不可核验')
    from preextract_images import _verified_image, restore_request_images
    seen = set()
    for asset in reading['assets']:
        if not isinstance(asset, dict) or not asset.get('id') or asset['id'] in seen:
            raise ValueError('正文图片身份不可核验')
        seen.add(asset['id'])
        if asset.get('image_path'):
            _verified_image(asset)
    # This reads the locally constructed input request, never an LLM reply.
    # The adapter compares every image payload against its actual local bytes.
    restore_request_images(part['bundle'], Path(model_dir), part['body'])
