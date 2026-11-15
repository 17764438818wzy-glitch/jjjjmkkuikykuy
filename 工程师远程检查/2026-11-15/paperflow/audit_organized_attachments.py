"""Read back attachment ZIP member records and ensure explicit process files are excluded."""
import concurrent.futures as cf
import json,pathlib,zipfile
from organize_library import WORK,OUTPUT,get_records,non_document_exclusion,dump


def check_generated(out):
    expected=out['members'];issues=[]
    with zipfile.ZipFile(OUTPUT/out['path']) as z:
        actual=z.infolist()
        if len(actual)!=len(expected):issues.append({'path':out['path'],'error':'Member count mismatch'})
        for info,e in zip(actual,expected):
            if info.filename!=e['name'] or info.file_size!=e['size'] or f'{info.CRC:08x}'!=e['crc']:
                issues.append({'path':out['path'],'error':'Member metadata mismatch','member':e['name']})
            if non_document_exclusion(info.filename):issues.append({'path':out['path'],'error':'Explicit process artifact in attachment ZIP','member':info.filename})
    return issues


if __name__=='__main__':
    with (WORK/'materialization.jsonl').open(encoding='utf-8') as f:results=[json.loads(l) for l in f if l.strip()]
    records=get_records();archives=json.loads((WORK/'archive-inventory.json').read_text('utf-8'))
    by_container={a['source']:a for a in archives};by_sha={}
    for r in records:
        if r['role']=='archive':by_sha[r['sha256']]=by_container[r.get('container_key',r['source'])]
    outputs={x['path']:x for r in results for x in r['outputs']}
    generated=[x for x in outputs.values() if x.get('generated')];issues=[];pure=0;direct=0
    for x in outputs.values():
        if '/附件/' not in x['path']:continue
        if not x.get('generated') and x['sha256'] in by_sha:
            pure+=1
            for m in by_sha[x['sha256']].get('members',[]):
                if not m['directory'] and non_document_exclusion(m['name']):issues.append({'path':x['path'],'error':'Explicit process artifact in unchanged attachment archive','member':m['name']})
        elif not x.get('generated'):
            direct+=1
            if non_document_exclusion(pathlib.Path(x['path']).name):issues.append({'path':x['path'],'error':'Explicit process artifact in standalone attachment'})
    with cf.ThreadPoolExecutor(max_workers=6) as ex:
        for found in ex.map(check_generated,generated):issues.extend(found)
    report={'generated_attachment_zips':len(generated),'generated_members':sum(len(x['members']) for x in generated),'unchanged_attachment_archives':pure,'standalone_attachments':direct,'issues':issues,'passed':not issues,'scope':'ZIP member name/size/CRC readback and explicit process-file routing; not a full semantic review'}
    dump(WORK/'attachment-audit.json',report);dump(OUTPUT/'_整理记录'/'附件分流核验.json',report)
    print(json.dumps(report,ensure_ascii=False,indent=2),flush=True)
    if issues:raise SystemExit(2)
