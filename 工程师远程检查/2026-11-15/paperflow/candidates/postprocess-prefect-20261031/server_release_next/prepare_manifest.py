"""Create one reviewable draft manifest in this isolated candidate only."""
import argparse
import importlib.metadata
import json
from pathlib import Path
import sys

from release_guard import inventory, file_hashes
from release_layout import HERE, PACKAGES, layout


def main(argv=None):
    parser = argparse.ArgumentParser(description='为服务器页面候选生成本地只读版本清单')
    parser.add_argument('--output', required=True)
    args = parser.parse_args(argv)
    output = Path(args.output)
    if not output.is_absolute() or not output.parent.resolve(strict=True).is_relative_to(HERE):
        raise ValueError('only an explicit path inside this candidate may be written')
    roots, required = layout()
    files = file_hashes(inventory(roots, required))
    value = dict(schema='paperflow-page-release/1', files=files,
                 python='.'.join(map(str, sys.version_info[:3])),
                 packages={name: importlib.metadata.version(name) for name in PACKAGES})
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode('utf8')
    with output.open('xb') as stream:
        stream.write(raw)
    import hashlib
    print(json.dumps({'files': len(files), 'manifest_sha256': hashlib.sha256(raw).hexdigest()}, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
