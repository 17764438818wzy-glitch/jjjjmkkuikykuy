"""Check that source and exact planned image bytes remain unchanged at send."""
import base64
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import sys

from PIL import Image
import pytest


HERE = Path(__file__).resolve().parents[1]
ROOT = HERE.parents[1]
sys.path[:0] = [str(HERE), str(ROOT / 'prefect-paperflow/paperflow_prefect/clean_overlay'),
                'F:/论文预处理系统/程序/updates/unified-prefect-20261029-material-r02/desktop',
                'F:/论文预处理系统/程序/desktop']
spec = importlib.util.spec_from_file_location('image_evidence', HERE / 'image_evidence.py')
evidence = importlib.util.module_from_spec(spec)
spec.loader.exec_module(evidence)


@pytest.fixture
def images(tmp_path):
    source, adapted = tmp_path / 'source.png', tmp_path / 'adapted.png'
    Image.new('RGB', (2, 3), 'red').save(source)
    Image.new('RGB', (2, 3), 'blue').save(adapted)
    def asset(path):
        return {'id': 'image-1', 'image_path': str(path), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
                'width': 2, 'height': 3}
    original, planned = asset(source), asset(adapted)
    encoded = base64.b64encode(adapted.read_bytes()).decode('ascii')
    request = {'messages': [{'role': 'system', 'content': 'synthetic'}, {'role': 'user', 'content': [
        {'type': 'text', 'text': json.dumps({'material': {'assets': [planned]}})},
        {'type': 'image_url', 'image_url': {'url': 'data:image/png;base64,' + encoded}}]}]}
    return {'assets': [original]}, {'bundle': {'assets': [planned]}, 'body': request}, source, adapted, tmp_path


def test_exact_original_and_planned_image_bytes_are_checked(images):
    reading, part, _, _, root = images
    assert evidence.verify_body_images(reading, part, root) is None


@pytest.mark.parametrize('changed', ['original', 'adapted', 'request'])
def test_changed_image_or_planned_request_fails_closed(images, changed):
    reading, part, source, adapted, root = images
    if changed == 'original':
        Image.new('RGB', (2, 3), 'green').save(source)
    elif changed == 'adapted':
        Image.new('RGB', (2, 3), 'green').save(adapted)
    else:
        part = copy.deepcopy(part)
        part['body']['messages'][1]['content'][1]['image_url']['url'] = 'data:image/png;base64,' + base64.b64encode(b'other').decode()
    with pytest.raises(ValueError, match='图片'):
        evidence.verify_body_images(reading, part, root)
