"""Create a separate static-site inventory; never change the served site."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(HERE))
from release_guard import MAX_FILES, file_hashes


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--site-root', required=True)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    site = Path(args.site_root).resolve(strict=True)
    output = Path(args.output)
    if not site.is_dir() or not output.is_absolute() or not output.parent.resolve(strict=True).is_relative_to(HERE):
        raise ValueError('an existing site and a new output inside this candidate are required')
    paths = {}
    for entry in site.rglob('*'):
        if entry.is_symlink() or (hasattr(entry, 'is_junction') and entry.is_junction()):
            raise ValueError('static site cannot contain links')
        if entry.is_file():
            resolved = entry.resolve(strict=True)
            if not resolved.is_relative_to(site):
                raise ValueError('static site escaped its root')
            paths[os.path.normcase(str(resolved))] = resolved
    if not paths or len(paths) > MAX_FILES:
        raise ValueError('static site inventory is empty or too large')
    files = file_hashes(paths)
    value = {'schema': 'paperflow-static-site-inventory/1', 'site_root': str(site), 'files': files}
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode('utf8')
    with output.open('xb') as stream:
        stream.write(raw)
    print(json.dumps({'files': len(files), 'site_manifest_sha256': hashlib.sha256(raw).hexdigest()}))


if __name__ == '__main__':
    main()
