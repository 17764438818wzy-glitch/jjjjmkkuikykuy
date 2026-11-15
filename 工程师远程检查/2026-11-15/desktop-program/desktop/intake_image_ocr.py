"""Read-only purpose OCR using the already installed Windows OCR script."""
import json
import os
from pathlib import Path
import subprocess
import tempfile


def read_image(path):
    from PIL import Image, ImageOps
    path = Path(path)
    if os.name != 'nt':
        raise ValueError('Local image-purpose OCR is unavailable')
    if path.stat().st_size > 32 * 1024 * 1024:
        raise ValueError('Image-purpose OCR input exceeds its bound')
    with Image.open(path) as source:
        if getattr(source, 'n_frames', 1) != 1:
            raise ValueError('A multi-frame image needs complete purpose review')
        source.load()
        image = ImageOps.exif_transpose(source).convert('RGB')
    scale = min(1, 2400 / max(image.size))
    if scale < 1:
        image = image.resize((max(1, int(image.width * scale)), max(1, int(image.height * scale))))
    powershell = Path(os.environ.get('SystemRoot', r'C:\Windows')) / 'System32/WindowsPowerShell/v1.0/powershell.exe'
    with tempfile.TemporaryDirectory(prefix='paper-purpose-') as temporary:
        preview = Path(temporary) / 'ocr-input.png'
        image.save(preview)
        try:
            result = subprocess.run([str(powershell), '-NoProfile', '-NonInteractive', '-ExecutionPolicy', 'Bypass',
                '-File', str(Path(__file__).with_name('windows_ocr.ps1')), '-ImagePath', str(preview.resolve())],
                capture_output=True, encoding='utf-8', errors='replace', timeout=40,
                creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise ValueError('Local image-purpose OCR is temporarily unavailable') from exc
    if result.returncode:
        raise ValueError('Local image-purpose OCR could not read this image')
    data = json.loads(result.stdout.lstrip('\ufeff'))
    if data.get('engine') != 'Windows.Media.Ocr' or not isinstance(data.get('lines'), list):
        raise ValueError('Unexpected local OCR result')
    text = '\n'.join(str(line['text']) for line in data['lines'])
    return text[:16000], 'Windows.Media.Ocr', len(text) <= 16000
