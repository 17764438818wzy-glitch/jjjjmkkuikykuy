"""Hash-gated preservation of education body/index source occurrences.

Apply to the verified deployed dataflow service after other runtime installers.
This module never connects to RAGFlow or restarts a worker.
"""
import argparse
import ast
import hashlib
import os
from pathlib import Path
import tempfile


ORIGINAL = 'ddc5870fb9723a9199f06b4650a887074e4b4615edb87d7f2b50e94b07fa14d3'
PATCHED = '309ea86ae4a2f7932f3f8ebd34f70fa0bd7c08b17b7e3e7613da4b3b9e7e74c7'
PREVIOUS_PATCHED = '72eaac36c04be76cff18515fd8c3cc243db11d07ebb71097a1c636534e9e3dc5'

HELPER = '''def _education_chunk_id(ck, text, kb_id, doc_id, occurrences):
    """Preserve each education source position without altering its content."""
    if kb_id in ("ecf88dc4b82b11f1a8846d29005ec004",
                 "3e8ef88cbc4e11f1a8e165c0e34ea629"):
        # Match _process_chunks' later keyword conversion, including its
        # important_tks guard. Final Tokenizer outputs normally use the list.
        words = ck.get("important_kwd")
        if "keywords" in ck and "important_tks" not in ck:
            words = [k for k in re.split(r"[,，;；、\\r\\n]+", ck["keywords"]) if k.strip()]
        if isinstance(words, list) and all(isinstance(k, str) for k in words):
            roles = [k for k in words if k.startswith("role=")]
            if len(roles) == 1 and roles[0] in ("role=body", "role=index"):
                sections = tuple(sorted(k for k in words if k.startswith("section_id=")))
                parts = tuple(sorted(k for k in words if k.startswith("part=")))
                source = (str(doc_id), roles[0], sections, parts, text)
                occurrence = occurrences.get(source, 0)
                occurrences[source] = occurrence + 1
                payload = json.dumps(["education-chunk/1", kb_id, *source, occurrence],
                                     ensure_ascii=False, separators=(",", ":"))
                return xxhash.xxh64(payload.encode("utf-8")).hexdigest()
    return xxhash.xxh64((text + str(doc_id)).encode("utf-8")).hexdigest()


'''


def transform(original):
    """Build the candidate from the observed method; install also gates full hashes."""
    replacements = (
        ('import copy\n', 'import copy\nimport json\n'),
        ('class DataflowService:', HELPER + 'class DataflowService:'),
        ('        metadata = {}\n        for ck in chunks:',
         '        metadata = {}\n        education_occurrences = {}\n        for ck in chunks:'),
        ('                ck["id"] = xxhash.xxh64((text + str(ck["doc_id"])).encode("utf-8")).hexdigest()',
         '                ck["id"] = _education_chunk_id(ck, text, ctx.kb_id, ck["doc_id"], education_occurrences)'),
    )
    patched = original
    for before, after in replacements:
        if patched.count(before) != 1:
            raise ValueError('Dataflow chunk-processing source does not match the verified layout')
        patched = patched.replace(before, after, 1)
    ast.parse(patched)
    return patched


def install(target):
    target = Path(target)
    current = target.read_bytes()
    digest = hashlib.sha256(current).hexdigest()
    if digest == PATCHED:
        return 'already_installed'
    backup = target.with_name(target.name + '.education-identity-original')
    original = backup.read_bytes() if digest == PREVIOUS_PATCHED else current
    if hashlib.sha256(original).hexdigest() != ORIGINAL:
        raise ValueError('Dataflow source has changed; inspect before applying the identity repair')
    updated = transform(original.decode('utf-8')).encode('utf-8')
    if hashlib.sha256(updated).hexdigest() != PATCHED:
        raise ValueError('Generated identity repair checksum mismatch')
    compile(updated, str(target), 'exec')
    if backup.exists() and backup.read_bytes() != original:
        raise ValueError('Existing identity backup differs from the verified source')
    if not backup.exists():
        backup.write_bytes(original)
    fd, temporary = tempfile.mkstemp(prefix='.education-identity-', dir=target.parent)
    try:
        with os.fdopen(fd, 'wb') as output:
            output.write(updated)
            output.flush()
            os.fsync(output.fileno())
        os.chmod(temporary, target.stat().st_mode)
        os.replace(temporary, target)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    return 'installed'


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--target', type=Path,
        default=Path('/ragflow/rag/svr/task_executor_refactor/dataflow_service.py'))
    print(install(parser.parse_args().target))
