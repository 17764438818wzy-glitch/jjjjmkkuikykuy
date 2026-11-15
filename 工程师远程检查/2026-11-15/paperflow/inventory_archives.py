import concurrent.futures as cf
import json
import pathlib
import subprocess
import time
import zipfile

WORK = pathlib.Path(__file__).parent / '整理工作记录'
BASE = json.loads((WORK / 'source-inventory.json').read_text('utf-8'))
ROOT = pathlib.Path(BASE['root'])
SEVEN = r'C:\Program Files\NVIDIA Corporation\NVIDIA App\7z.exe'

def zip_name(info):
    if info.flag_bits & 0x800:
        return info.filename
    try:
        return info.filename.encode('cp437').decode('gb18030')
    except UnicodeError:
        return info.filename

def listing(path):
    if zipfile.is_zipfile(path):
        with zipfile.ZipFile(path) as z:
            return [{'name':zip_name(i),'raw_name':i.filename,'size':i.file_size,
                     'compressed':i.compress_size,'crc':f'{i.CRC:08x}',
                     'directory':i.is_dir(),'encrypted':bool(i.flag_bits&1),
                     'link':((i.external_attr>>16)&0o170000)==0o120000,
                     'index':idx} for idx,i in enumerate(z.infolist())]
    r = subprocess.run([SEVEN,'l','-slt','-ba','-sccUTF-8','-p-',str(path)],capture_output=True,timeout=90)
    if r.returncode:
        raise RuntimeError(r.stderr.decode('utf-8','replace')[:600])
    raw = r.stdout.decode('utf-8','replace').replace('\r','')
    members=[]
    for block in raw.split('\n\n'):
        item=dict(line.split(' = ',1) for line in block.splitlines() if ' = ' in line)
        if 'Path' not in item:continue
        members.append({'name':item['Path'],'raw_name':item['Path'],'size':int(item.get('Size') or 0),
                        'compressed':int(item.get('Packed Size') or 0),'crc':item.get('CRC','').lower(),
                        'directory':item.get('Folder')=='+' or item.get('Attributes','').startswith('D'),'encrypted':item.get('Encrypted')=='+',
                        'link':any(item.get(k) for k in ('Symbolic Link','Hard Link','Copy Link')),
                        'index':len(members)})
    return members

def inspect(f):
    try:return {'source':f['path'],'members':listing(ROOT/f['path'])}
    except Exception as e:return {'source':f['path'],'error':str(e)}

if __name__=='__main__':
    files=[f for f in BASE['files'] if pathlib.Path(f['path']).suffix.lower() in {'.zip','.rar','.7z','.tar','.gz'}]
    results=[]; start=time.time()
    with cf.ThreadPoolExecutor(max_workers=8) as ex:
        for item in ex.map(inspect,files):
            results.append(item)
            if len(results)%200==0:print(json.dumps({'listed':len(results),'total':len(files),'seconds':round(time.time()-start)},ensure_ascii=False),flush=True)
    (WORK/'archive-inventory.json').write_text(json.dumps(results,ensure_ascii=False),encoding='utf-8')
    members=[m for a in results for m in a.get('members',[]) if not m['directory']]
    print(json.dumps({'archives':len(results),'members':len(members),'expanded_GiB':round(sum(m['size'] for m in members)/2**30,3),'errors':[x for x in results if 'error'in x],'encrypted_members':sum(m['encrypted'] for m in members),'link_members':sum(m['link'] for m in members),'largest_archives':sorted([{'source':a['source'],'members':len(a.get('members',[])),'bytes':sum(m['size'] for m in a.get('members',[]))} for a in results],key=lambda a:a['members'],reverse=True)[:8]},ensure_ascii=False,indent=2),flush=True)
