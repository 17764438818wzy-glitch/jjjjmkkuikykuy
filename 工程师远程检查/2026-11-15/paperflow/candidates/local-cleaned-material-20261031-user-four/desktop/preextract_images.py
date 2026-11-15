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


def same_image_binding(left,right):
    """An explicit alias needs equal pixels, dimensions and program object.

    This predicate compares metadata; callers still verify the actual files.
    Number, filename or neighboring page alone cannot establish this identity.
    """
    return bool(left.get('image_path') and right.get('image_path')
        and all(left.get(k) and left.get(k)==right.get(k) for k in ('sha256','width','height'))
        and left.get('block_ids') and set(left['block_ids'])==set(right.get('block_ids') or []))


def image_limits(config):
    # The caller binds these values to the target channel. Defaults retain the
    # previous limits when explicit channel limits are unavailable.
    side = int(config.get('max_image_side', MAX_SIDE))
    size = int(config.get('max_single_image_bytes', MAX_BYTES))
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
    if max_side < min(800, max(picture.size)):
        raise ValueError('目标通道图片尺寸过小，清晰度无法保障；保留原图待核')
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


def _tile_picture(picture, max_side, max_bytes):
    """Yield overlapping, native-resolution crops and their source coordinates."""
    if max_side < 64:
        raise ValueError('目标通道图片尺寸过小，无法安全分块')
    picture = ImageOps.exif_transpose(picture)
    width, height = picture.size
    overlap = min(32, max_side // 10)
    def intervals(length):
        if length <= max_side:return [(0,length)]
        step = max_side-overlap
        starts = list(range(0, max(1,length-max_side+1), step))
        if starts[-1] != length-max_side:starts.append(length-max_side)
        return [(start,min(length,start+max_side)) for start in starts]
    tiles=[]
    def emit(box, depth=0):
        if len(tiles) >= 256:
            raise ValueError('图片分块数量过多，原图保留待核')
        tile=picture.crop(box)
        try:
            blob,detail=fit_image(tile,max_side=max_side,max_bytes=max_bytes)
        except ValueError:
            x0,y0,x1,y1=box
            if depth >= 4 or max(x1-x0,y1-y0) < 256:
                raise
            if x1-x0 >= y1-y0:
                mid=(x0+x1)//2; margin=min(16,(x1-x0)//8)
                emit((x0,y0,min(x1,mid+margin),y1),depth+1)
                emit((max(x0,mid-margin),y0,x1,y1),depth+1)
            else:
                mid=(y0+y1)//2; margin=min(16,(y1-y0)//8)
                emit((x0,y0,x1,min(y1,mid+margin)),depth+1)
                emit((x0,max(y0,mid-margin),x1,y1),depth+1)
            return
        detail['source_crop']=list(box)
        tiles.append((blob,detail))
    for y0,y1 in intervals(height):
        for x0,x1 in intervals(width):
            emit((x0,y0,x1,y1))
    return tiles


def _replace_object_ids(bundle, replacements):
    for block in bundle.get('blocks', []):
        if block.get('object_ids'):
            block['object_ids'] = [new for ident in block['object_ids']
                                   for new in replacements.get(ident,[ident])]


def prepare_request_images(bundle, work_dir, config, *, stop=None, continue_on_image_error=False):
    """Create private derivatives before the one durable model request exists."""
    work = Path(work_dir)
    if (work / 'model/request-state.json').exists():
        raise ValueError('已有模型请求，必须复用当时的图片版本，不能重新适配')
    max_side, max_bytes = image_limits(config)
    result = copy.deepcopy(bundle)
    records = []
    seen = set()
    adapted_assets = []
    replacements = {}
    held_block_ids = set()
    for asset in result.get('assets', []):
        if not asset.get('image_path'):
            adapted_assets.append(asset)
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
            tile_needed = max(image.size) > max_side and (max_side < 800 or max(image.size) > max_side*2)
            needs_adjustment = (max(image.size) > max_side or path.stat().st_size > max_bytes
                                or image.getexif().get(274, 1) != 1)
            fitted = None
            tile_side = max_side
            if not tile_needed and needs_adjustment:
                try:
                    fitted=fit_image(image,max_side=max_side,max_bytes=max_bytes)
                except ValueError:
                    tile_needed=True
                    tile_side=min(max_side,max(64,max(image.size)//2))
            if tile_needed:
                try:
                    tiles = _tile_picture(image,tile_side,max_bytes)
                except ValueError:
                    if not continue_on_image_error:
                        raise
                    row.update(held=True,reason='图片无法在当前通道限制内安全适配，建议核对或调整通道。')
                    held_block_ids.update(asset.get('block_ids') or [])
                    replacements[asset['id']]=[]
                    records.append(row)
                    continue
                tile_rows=[];tile_assets=[]
                for number,(blob,detail) in enumerate(tiles,1):
                    ident=f"{asset['id']}__tile_{number:04d}"
                    if ident in seen or any(a.get('id') == ident for a in result.get('assets', [])):
                        raise ValueError('图片分块编号冲突')
                    sha=hashlib.sha256(blob).hexdigest()
                    target=work/'private/request-images'/(sha+('.png' if detail['format']=='PNG' else '.jpg'))
                    target.parent.mkdir(parents=True,exist_ok=True)
                    if target.exists():
                        if file_sha(target)!=sha:raise ValueError('已有适配图片版本发生变化，未覆盖')
                    else:
                        with target.open('xb') as output:output.write(blob)
                    adaptation=dict(source_sha256=row['source_sha256'],source_width=row['source_width'],
                                    source_height=row['source_height'],original_asset_id=asset['id'],
                                    tile_number=number,tile_count=len(tiles),**detail)
                    tile_asset=copy.deepcopy(asset)
                    tile_asset.update(id=ident,image_path=str(target.resolve()),local_path=str(target.resolve()),
                                      sha256=sha,width=detail['width'],height=detail['height'],
                                      image_adaptation=adaptation)
                    tile_asset['caption']=(asset.get('caption') or '')+f'（原图分块 {number}/{len(tiles)}）'
                    tile_rows.append({'id':ident,'binding':{key:copy.deepcopy(tile_asset[key])
                                      for key in _BINDING_KEYS if key in tile_asset},'caption':tile_asset['caption']})
                    tile_assets.append(tile_asset)
                row.update(adjusted=True,tiled=True,tiles=tile_rows)
                replacements[asset['id']]=[item['id'] for item in tile_assets]
                adapted_assets.extend(tile_assets);records.append(row)
                continue
            if needs_adjustment:
                blob, detail = fitted
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
        adapted_assets.append(asset)
    result['assets']=adapted_assets
    if held_block_ids:
        result['blocks']=[b for b in result.get('blocks',[])
                          if not (b.get('type')=='image' and b.get('id') in held_block_ids)]
    _replace_object_ids(result,replacements)
    report = dict(schema=SCHEMA, material_sha256=_digest(bundle), max_side=max_side, max_bytes=max_bytes,
                  image_count=sum(0 if row.get('held') else len(row.get('tiles',[])) or 1 for row in records),
                  held_count=sum(bool(row.get('held')) for row in records),
                  source_image_count=len(records), adjusted_count=sum(row['adjusted'] for row in records), images=records)
    write_json(work / 'private/request-images.json', report)
    return result, report


def restore_request_images(bundle, work_dir, request, *, alias_context=None, original_bundle=None):
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
        replacements={}
        expanded={}
        for asset, row in zip(assets, rows):
            _verified_image(asset)
            if (asset['sha256'], asset['width'], asset['height']) != (row.get('source_sha256'), row.get('source_width'), row.get('source_height')):
                raise ValueError('图片请求原版本不匹配')
            if row.get('held'):
                expanded[asset['id']]=[]
                replacements[asset['id']]=[]
                continue
            if row.get('tiled'):
                tiles=row.get('tiles') or []
                if not tiles:raise ValueError('图片分块记录缺失')
                with Image.open(asset['image_path']) as original_image:
                    display_width,display_height=ImageOps.exif_transpose(original_image).size
                copies=[]
                for number,tile in enumerate(tiles,1):
                    if tile.get('id') != f"{asset['id']}__tile_{number:04d}":
                        raise ValueError('图片分块编号不匹配')
                    binding=tile.get('binding') or {}
                    path=Path(binding.get('image_path','')).resolve()
                    if not path.is_relative_to((work/'private/request-images').resolve()):
                        raise ValueError('图片分块文件不在原工作目录')
                    adaptation=binding.get('image_adaptation') or {}
                    crop=adaptation.get('source_crop') or []
                    if (adaptation.get('source_sha256') != asset['sha256'] or
                            adaptation.get('original_asset_id') != asset['id'] or
                            adaptation.get('tile_number') != number or adaptation.get('tile_count') != len(tiles) or
                            len(crop)!=4 or not (0<=crop[0]<crop[2]<=display_width and
                                                 0<=crop[1]<crop[3]<=display_height)):
                        raise ValueError('图片分块来源范围不匹配')
                    item=copy.deepcopy(asset);item['id']=tile['id'];item['caption']=tile.get('caption','')
                    item.update({k:copy.deepcopy(v) for k,v in binding.items() if k in _BINDING_KEYS})
                    _verified_image(item);copies.append(item)
                expanded[asset['id']]=copies
                replacements[asset['id']]=[item['id'] for item in copies]
                continue
            binding = row.get('binding', {})
            if row.get('adjusted'):
                if not Path(binding.get('image_path', '')).resolve().is_relative_to((work / 'private/request-images').resolve()):
                    raise ValueError('图片请求派生文件不在原工作目录')
            elif binding != {k:asset[k] for k in _BINDING_KEYS if k in asset}:
                raise ValueError('图片请求原版本不匹配')
            asset.update({k:copy.deepcopy(v) for k, v in binding.items() if k in _BINDING_KEYS})
        if expanded:
            held_blocks={bid for asset in assets if asset['id'] in expanded and not expanded[asset['id']]
                         for bid in asset.get('block_ids',[])}
            result['assets']=[part for asset in result.get('assets',[])
                              for part in expanded.get(asset['id'],[asset])]
            result['blocks']=[b for b in result.get('blocks',[])
                              if not (b.get('type')=='image' and b.get('id') in held_blocks)]
            _replace_object_ids(result,replacements)
            assets=[a for a in result['assets'] if a.get('image_path')]
    content = request['messages'][1]['content']
    projection = json.loads(content[0]['text'])
    sent_assets = projection['material'].get('assets', [])
    sent_by_id = {a['id']: a for a in sent_assets}
    sent_images = [c['image_url']['url'] for c in content if c.get('type') == 'image_url']
    if 'request_layout' in projection:
        from preextract_catalog import LAYOUT
        if projection['request_layout'] != LAYOUT:raise ValueError('图片请求格式版本不兼容')
        groups=projection.get('image_groups',[])
        ids=[ident for group in groups for ident in group.get('asset_ids',[])]
        expected=[a['id'] for a in assets]
        all_expected=[a['id'] for a in result.get('assets',[])]
        if (len(expected)!=len(set(expected)) or len(ids)!=len(set(ids)) or set(ids)!=set(expected)
                or len(sent_images)!=len(groups) or any(not g.get('asset_ids') for g in groups)
                or len(sent_by_id)!=len(sent_assets) or len(all_expected)!=len(set(all_expected))
                or set(sent_by_id)!=set(all_expected)):
            raise ValueError('图片请求别名编号或数量不匹配')
        actual_by_id={a['id']:a for a in assets}
        for group,url in zip(groups,sent_images):
            try:
                header,encoded=url.split(',',1)
                if not header.startswith('data:image/') or not header.endswith(';base64'):raise ValueError()
                blob=base64.b64decode(encoded,validate=True)
                digest=hashlib.sha256(blob).hexdigest()
            except (ValueError,TypeError):
                raise ValueError('图片请求字节无法核对') from None
            for ident in group['asset_ids']:
                asset=actual_by_id[ident]
                _verified_image(asset)
                declared=sent_by_id.get(ident,{})
                if (digest!=asset['sha256'] or any(asset.get(k)!=group.get(k) or asset.get(k)!=declared.get(k)
                                                  for k in ('sha256','width','height')) or
                        asset.get('image_adaptation') != declared.get('image_adaptation')):
                    raise ValueError('图片请求别名实际字节或声明版本不匹配')
        if alias_context is not None:
            from preextract_image_alias import _record_verified_request
            _record_verified_request(alias_context, result, original_bundle or bundle,
                                     request, groups, sent_by_id, work)
        return result
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
