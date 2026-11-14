"""Download public release attachments and verify SHA256 before installation."""
import argparse
import hashlib
import json
from pathlib import Path
import urllib.request

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--manifest',type=Path,default=Path(__file__).resolve().parents[2]/'08_100篇样例数据/assets.json')
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--sample-only',action='store_true')
    args=parser.parse_args();args.output.mkdir(parents=True,exist_ok=True)
    for asset in json.loads(args.manifest.read_text(encoding='utf-8'))['assets']:
        if args.sample_only and not asset['name'].startswith('paperflow-sample100-'):continue
        if Path(asset['name']).name!=asset['name']:raise ValueError('Unsafe asset name')
        dest=args.output/asset['name']
        if dest.exists():
            with dest.open('rb') as f:valid=hashlib.file_digest(f,'sha256').hexdigest()==asset['sha256']
            if valid and dest.stat().st_size==asset['bytes']:print('Verified existing',dest.name);continue
            raise ValueError('Existing asset differs; keep it aside and retry: '+str(dest))
        temporary=dest.with_name(dest.name+'.download')
        with temporary.open('xb') as out,urllib.request.urlopen(asset['url'],timeout=120) as response:
            digest=hashlib.sha256();size=0
            while block:=response.read(1024*1024):out.write(block);digest.update(block);size+=len(block)
        if size!=asset['bytes'] or digest.hexdigest()!=asset['sha256']:
            raise ValueError('Downloaded asset checksum mismatch: '+dest.name)
        temporary.rename(dest);print('Downloaded and verified',dest.name)

if __name__=='__main__':main()
