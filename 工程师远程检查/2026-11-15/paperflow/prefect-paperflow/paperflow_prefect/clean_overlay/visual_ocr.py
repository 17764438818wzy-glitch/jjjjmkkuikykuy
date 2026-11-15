"""CPU OCR for local PDF pages; source files and input pixels are never modified."""
from functools import lru_cache


@lru_cache(maxsize=1)
def _engine():
    from rapidocr_onnxruntime import RapidOCR
    return RapidOCR()


def read_pdf_page(page):
    """Return OCR line text, unrotated PDF coordinates and observed confidence."""
    import fitz
    import numpy as np

    pix = page.get_pixmap(matrix=fitz.Matrix(2, 2), alpha=False, annots=False)
    image = np.frombuffer(pix.samples, dtype=np.uint8).reshape(pix.height, pix.width, pix.n)
    found, _ = _engine()(image)
    result = []
    for box, text, score in found or []:
        xs = [float(point[0]) / 2 for point in box]
        ys = [float(point[1]) / 2 for point in box]
        if str(text).strip() and xs and ys:
            bounds = fitz.Rect(min(xs), min(ys), max(xs), max(ys))
            if page.rotation:
                bounds = bounds * page.derotation_matrix
            result.append({'text': str(text), 'bbox': list(bounds),
                           'confidence': float(score)})
    return result


def read_image(path):
    """Read a derived picture in memory; never write or delete the input."""
    import numpy as np
    from PIL import Image, ImageOps

    with Image.open(path) as source:
        picture = ImageOps.exif_transpose(source).convert('RGB')
        if max(picture.size) > 4096:
            picture.thumbnail((4096, 4096), Image.Resampling.LANCZOS)
        pixels = np.asarray(picture)
    found, _ = _engine()(pixels)
    return [{'text': str(text), 'confidence': float(score)}
            for _, text, score in found or [] if str(text).strip()]


def audit_images(assets, issue):
    """Flag explicit excluded-section headings inside retained body images."""
    import re
    excluded = re.compile(r'^(?:(?:主要)?参\s*考\s*文\s*献|参考书目|参考资料|references(?:\s+cited)?|'
                          r'bibliography|works\s+cited|致\s*谢|鸣\s*谢)'
                          r'(?:\s*$|\s*[:：]|\s*\[\d+\])', re.I)
    for asset in assets:
        path = asset.get('image_path')
        if not path:continue
        if asset.get('kind') == 'ocr_page_crop':
            asset['image_scope_audit'] = {'status': 'page_ocr_unverified'}
            continue
        try:
            lines = read_image(path)
        except (InterruptedError, MemoryError, PermissionError, OSError):
            raise
        except Exception as exc:
            asset['image_scope_audit'] = {'status': 'ocr_unavailable', 'error': type(exc).__name__}
            issue('image_ocr_review', '正文图片 OCR 未完成，图片范围待核。', asset.get('position') or {})
            continue
        found_excluded = any(excluded.match(str(line.get('text') or '').strip()) for line in lines)
        status = 'excluded_heading_detected' if found_excluded else 'ocr_checked_unverified'
        asset['image_scope_audit'] = {'status': status, 'detected_lines': len(lines),
            'minimum_confidence': min((line.get('confidence', 0) for line in lines), default=None)}
        issue('image_ocr_review', '图片 OCR 发现疑似排除章节标题，需核对。' if found_excluded else
              '图片已进行本机 OCR 检查；未识别内容及图中关系仍需核对。', asset.get('position') or {})
