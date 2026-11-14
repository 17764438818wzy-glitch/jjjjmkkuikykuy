"""Download public release attachments and verify SHA256 before installation."""
import argparse
import hashlib
import json
from pathlib import Path
import urllib.request
import urllib.error
import time

def matches(path, record):
    if not path.is_file() or path.stat().st_size != record['bytes']:
        return False
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest() == record['sha256']

def safe_name(record):
    name = record['name']
    if not name or name in ('.', '..') or '/' in name or '\\' in name:
        raise ValueError('Unsafe asset name')
    return name

def download_one(record, folder):
    dest = folder / safe_name(record)
    if dest.exists():
        if matches(dest, record):
            print('Verified existing', dest.name)
            return dest
        raise ValueError('Existing asset differs; keep it aside and retry: ' + str(dest))
    temporary = dest.with_name(dest.name + '.download')
    for attempt in range(3):
        try:
            # Only this downloader's unfinished temporary file is replaced on retry.
            with temporary.open('wb') as out, urllib.request.urlopen(record['url'], timeout=120) as response:
                while block := response.read(1024 * 1024):
                    out.write(block)
            break
        except (OSError, urllib.error.URLError):
            if attempt == 2:
                raise
            time.sleep(2)
    if not matches(temporary, record):
        raise ValueError('Downloaded asset checksum mismatch: ' + dest.name)
    temporary.rename(dest)
    print('Downloaded and verified', dest.name)
    return dest

def download_archive(asset, output):
    dest = output / safe_name(asset)
    if dest.exists():
        if matches(dest, asset):
            print('Verified existing', dest.name)
            return
        raise ValueError('Existing archive differs; keep it aside and retry: ' + str(dest))
    parts = asset.get('parts')
    if not parts:
        download_one(asset, output)
        return
    names = [safe_name(part) for part in parts]
    if len(names) != len(set(names)) or sum(part['bytes'] for part in parts) != asset['bytes']:
        raise ValueError('Invalid archive part manifest')
    folder = output / (dest.name + '.parts')
    folder.mkdir(exist_ok=True)
    paths = [download_one(part, folder) for part in parts]
    temporary = dest.with_name(dest.name + '.assembling')
    with temporary.open('wb') as out:
        for path in paths:
            with path.open('rb') as stream:
                while block := stream.read(1024 * 1024):
                    out.write(block)
    if not matches(temporary, asset):
        raise ValueError('Assembled archive checksum mismatch: ' + dest.name)
    temporary.rename(dest)
    print('Assembled and verified', dest.name)

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--manifest',type=Path,default=Path(__file__).resolve().parents[2]/'08_100篇样例数据/assets.json')
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--sample-only',action='store_true')
    args=parser.parse_args();args.output.mkdir(parents=True,exist_ok=True)
    for asset in json.loads(args.manifest.read_text(encoding='utf-8'))['assets']:
        if args.sample_only and not asset['name'].startswith('paperflow-sample100-'):continue
        download_archive(asset,args.output)

if __name__=='__main__':main()
