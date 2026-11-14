"""Pixel-only compatibility at the existing request and publication boundaries."""
from __future__ import annotations

import base64
import copy
import hashlib
import io
import json
from pathlib import Path

from PIL import Image, ImageOps

from preextract_material import file_sha, write_json

MAX_SIDE = 2000
MAX_BYTES = 5 * 1024 * 1024
SCHEMA = 'preextract-request-images/1'
_BINDING_KEYS = ('image_path', 'local_path', 'sha256', 'width', 'height', 'image_adaptation')


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':')).encode()).hexdigest()


def image_limits(config):
    side = min(MAX_SIDE, int(config.get('max_image_side', MAX_SIDE)))
    size = min(MAX_BYTES, int(config.get('max_single_image_bytes', MAX_BYTES)))
    if side < 1 or size < 1:
        raise ValueError('图片容量配置无效')
    return side, size


def fit_image(picture, *, max_side=MAX_SIDE, max_bytes=MAX_BYTES):
    """Keep the whole frame. Prefer lossless PNG, then high quality RGB JPEG.

    No OCR/LLM, cropping or sharpening. Metadata is discarded. Extremely small
    byte budgets must not silently reduce a full page to an unreadable thumbnail.
    """
    if max_side < 1 or max_bytes < 1:
        raise ValueError('图片容量配置无效')
    picture = ImageOps.exif_transpose(picture)
    alpha = 'A' in picture.getbands() or 'transparency' in picture.info
    pixels = picture.convert('RGBA' if alpha else 'RGB')
    original_size = pixels.size
    scale = min(1, max_side / max(pixels.size))
    target = tuple(max(1, round(v * scale)) for v in pixels.size)
    minimum_side = min(800, max(target))
    for _ in range(12):
        frame = pixels if target == pixels.size else pixels.resize(target, Image.Resampling.LANCZOS)
        # A new image drops EXIF, ICC and comments, including source names.
        frame = Image.frombytes(frame.mode, frame.size, frame.tobytes())
        encodings = [('PNG', {'optimize': True})]
        if not alpha:
            # Avoid Pillow's fixed optimized-JPEG output buffer, which can be
            # smaller than a high-detail 4:4:4 frame. No global encoder tuning:
            # the original workbench processes several papers concurrently.
            encodings += [('JPEG', {'quality': q, 'subsampling': sub})
                          for q, sub in ((95, 0), (92, 0), (92, 2))]
        for fmt, options in encodings:
            output = io.BytesIO()
            frame.save(output, format=fmt, **options)
            blob = output.getvalue()
            if len(blob) <= max_bytes:
                return blob, dict(width=frame.width, height=frame.height, bytes=len(blob),
                    format=fmt, resized=frame.size != original_size, quality=options.get('quality'),
                    method='whole-frame-lanczos' if frame.size != original_size else 'reencode',
                    max_side=max_side, max_bytes=max_bytes)
        next_side = round(max(target) * .85)
        if next_side < minimum_side:
            break
        factor = next_side / max(original_size)
        target = tuple(max(1, round(v * factor)) for v in original_size)
    raise ValueError('图片压缩后仍超过容量，继续强缩可能影响阅读；原图和已完成内容保留')


def _verified_image(asset):
    path = Path(asset['image_path'])
    if not path.is_file() or file_sha(path) != asset.get('sha256'):
        raise ValueError('图片版本发生变化，原请求和结果保留')
    with Image.open(path) as image:
        if image.size != (asset.get('width'), asset.get('height')) or getattr(image, 'n_frames', 1) != 1:
            raise ValueError('图片版本尺寸或帧数发生变化')
        if image.format not in {'PNG', 'JPEG', 'GIF', 'WEBP'}:
            raise ValueError('图片尚未完成原流程的格式转换')
        image.load()
    return path


def prepare_request_images(bundle, work_dir, config, *, stop=None):
    """Create private derivatives before the one durable model request exists."""
    work = Path(work_dir)
    if (work / 'model/request-state.json').exists():
        raise ValueError('已有模型请求，必须复用当时的图片版本，不能重新适配')
    max_side, max_bytes = image_limits(config)
    result = copy.deepcopy(bundle)
    records = []
    seen = set()
    for asset in result.get('assets', []):
        if not asset.get('image_path'):
            continue
        if stop is not None and stop.is_set():
            raise InterruptedError('图片适配已停止，原图保留')
        if asset['id'] in seen:
            raise ValueError('图片编号重复，无法确定版本')
        seen.add(asset['id'])
        path = _verified_image(asset)
        row = dict(id=asset['id'], source_sha256=asset['sha256'], source_width=asset['width'],
                   source_height=asset['height'], source_bytes=path.stat().st_size, adjusted=False)
        with Image.open(path) as image:
            if (max(image.size) > max_side or path.stat().st_size > max_bytes
                    or image.getexif().get(274, 1) != 1):
                blob, detail = fit_image(image, max_side=max_side, max_bytes=max_bytes)
                sha = hashlib.sha256(blob).hexdigest()
                target = work / 'private/request-images' / (sha + ('.png' if detail['format'] == 'PNG' else '.jpg'))
                target.parent.mkdir(parents=True, exist_ok=True)
                if target.exists():
                    if file_sha(target) != sha:
                        raise ValueError('已有适配图片版本发生变化，未覆盖')
                else:
                    with target.open('xb') as output:
                        output.write(blob)
                adaptation = dict(source_sha256=row['source_sha256'], source_width=row['source_width'],
                                  source_height=row['source_height'], **detail)
                asset.update(image_path=str(target.resolve()), local_path=str(target.resolve()), sha256=sha,
                             width=detail['width'], height=detail['height'], image_adaptation=adaptation)
                row.update(adjusted=True, processing=detail)
        row['binding'] = {key: copy.deepcopy(asset[key]) for key in _BINDING_KEYS if key in asset}
        records.append(row)
    report = dict(schema=SCHEMA, material_sha256=_digest(bundle), max_side=max_side, max_bytes=max_bytes,
                  image_count=len(records), adjusted_count=sum(row['adjusted'] for row in records), images=records)
    write_json(work / 'private/request-images.json', report)
    return result, report


def restore_request_images(bundle, work_dir, request):
    """Restore exactly the local bytes bound to a saved request, never resize.

    Older replies without an adaptation record keep their original image version.
    This verifies local sent bytes, not whether an upstream subsequently changed
    them; upstream integrity failures remain recorded separately.
    """
    result = copy.deepcopy(bundle)
    work = Path(work_dir)
    checkpoint = work / 'private/request-images.json'
    assets = [a for a in result.get('assets', []) if a.get('image_path')]
    if checkpoint.exists():
        record = json.loads(checkpoint.read_text(encoding='utf8'))
        if record.get('schema') != SCHEMA or record.get('material_sha256') != _digest(bundle):
            raise ValueError('图片请求材料版本不匹配')
        rows = record.get('images', [])
        if [r.get('id') for r in rows] != [a['id'] for a in assets]:
            raise ValueError('图片请求编号或数量发生变化')
        for asset, row in zip(assets, rows):
            _verified_image(asset)
            if (asset['sha256'], asset['width'], asset['height']) != (row.get('source_sha256'), row.get('source_width'), row.get('source_height')):
                raise ValueError('图片请求原版本不匹配')
            binding = row.get('binding', {})
            if row.get('adjusted'):
                if not Path(binding.get('image_path', '')).resolve().is_relative_to((work / 'private/request-images').resolve()):
                    raise ValueError('图片请求派生文件不在原工作目录')
            elif binding != {k:asset[k] for k in _BINDING_KEYS if k in asset}:
                raise ValueError('图片请求原版本不匹配')
            asset.update({k:copy.deepcopy(v) for k, v in binding.items() if k in _BINDING_KEYS})
    content = request['messages'][1]['content']
    sent_assets = json.loads(content[0]['text'])['material'].get('assets', [])
    sent_by_id = {a['id']: a for a in sent_assets}
    sent_images = [c['image_url']['url'] for c in content if c.get('type') == 'image_url']
    if len(sent_images) != len(assets):
        raise ValueError('图片请求数量不匹配')
    for asset, url in zip(assets, sent_images):
        _verified_image(asset)
        declared = sent_by_id.get(asset['id'], {})
        if any(asset.get(k) != declared.get(k) for k in ('sha256', 'width', 'height')):
            raise ValueError('图片请求声明版本不匹配')
        try:
            header, encoded = url.split(',', 1)
            if not header.startswith('data:image/') or not header.endswith(';base64'):
                raise ValueError()
            digest = hashlib.sha256(base64.b64decode(encoded, validate=True)).hexdigest()
        except (ValueError, TypeError):
            raise ValueError('图片请求字节无法核对') from None
        if digest != asset['sha256']:
            raise ValueError('图片请求实际字节与版本不匹配')
    return result
