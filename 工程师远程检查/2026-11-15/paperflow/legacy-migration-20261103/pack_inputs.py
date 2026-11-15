import json,re,tarfile,time
from pathlib import Path
root=Path(__file__).parent
images=json.loads((root/'image-sources.json').read_text())
start=time.monotonic()
with tarfile.open(root/'inputs.tar.gz','w:gz',compresslevel=1) as archive:
    for name in ('packages.jsonl','package-index.json','source-inventory.json','storage-inventory.json'):
        archive.add(root/name,arcname=name,recursive=False)
    for name in ('majors.json','legacy-records.json'):
        archive.add(root.parent/'article-repository'/name,arcname=name,recursive=False)
    for pos,(sha,file) in enumerate(images.items()):
        assert re.fullmatch('[a-f0-9]{64}',sha)
        archive.add(file,arcname='images/'+sha[:2]+'/'+sha,recursive=False)
        if (pos+1)%3000==0:print(json.dumps({'packed_images':pos+1,'total':len(images)}),flush=True)
print(json.dumps({'archive_bytes':(root/'inputs.tar.gz').stat().st_size,'seconds':round(time.monotonic()-start,2)}),flush=True)
