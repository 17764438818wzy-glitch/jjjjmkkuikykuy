"""Hash-gated patch for the two education annotation bindings only."""
import argparse
import ast
import hashlib
import os
from pathlib import Path
import tempfile

ORIGINAL = '38a80e843ded001ea09b5fd47448b183e175ad978063fca05b68f2e9c251f782'
PATCHED = '151279643180f663e3153dda097a57f7b827f930cf05cbdcf4d51210792e40ec'
PREVIOUS_PATCHED = 'c641a65830643aa366181bb1603be1e7f7908a26b57f26033f032fa5c3fff2cb'
PREVIOUS = '4c0127a57bccd8811d9abb19518e33c255aa2e3497ab143b7d6df543ddf9fc51'


def transform(source):
    runtime = Path(__file__).with_name('runtime.py').read_text(encoding='utf8')
    replacements = (
        ('class ExtractorParam(', runtime + '\n\nclass ExtractorParam('),
        ('    async def _build_TOC(self, docs):',
         '    async def _generate_async(self, msg, **kwargs):\n'
         '        return await education_generate(self, super()._generate_async, msg, kwargs)\n\n'
         '    async def _build_TOC(self, docs):'),
    )
    for before, after in replacements:
        if source.count(before) != 1:
            raise ValueError('Extractor layout differs from verified deployment')
        source = source.replace(before, after, 1)
    ast.parse(source)
    return source


def install(target):
    target = Path(target)
    original = target.read_bytes()
    digest = hashlib.sha256(original).hexdigest()
    if digest == PATCHED:
        return 'already_installed'
    backup = target.with_name(target.name + '.education-capacity-original')
    if digest in (PREVIOUS, PREVIOUS_PATCHED):
        original = backup.read_bytes()
        if hashlib.sha256(original).hexdigest() != ORIGINAL:
            raise ValueError('Education model original backup changed')
    elif digest != ORIGINAL:
        raise ValueError('Extractor source changed; inspect before applying')
    updated = transform(original.decode('utf8')).encode('utf8')
    if hashlib.sha256(updated).hexdigest() != PATCHED:
        raise ValueError('Education model patch checksum mismatch')
    if backup.exists() and backup.read_bytes() != original:
        raise ValueError('Existing backup differs from verified source')
    if not backup.exists():
        backup.write_bytes(original)
    fd, tmp = tempfile.mkstemp(prefix='.education-capacity-', dir=target.parent)
    try:
        with os.fdopen(fd, 'wb') as output:
            output.write(updated)
            output.flush()
            os.fsync(output.fileno())
        os.chmod(tmp, target.stat().st_mode)
        os.replace(tmp, target)
    finally:
        if os.path.exists(tmp): os.unlink(tmp)
    return 'installed'


if __name__ == '__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--target',default='/ragflow/rag/flow/extractor/extractor.py')
    print(install(parser.parse_args().target))
