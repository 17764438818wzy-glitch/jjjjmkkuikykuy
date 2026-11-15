"""Independent read-only check of the fifth stage's isolated acceptance evidence."""
import json
import os
from pathlib import Path
import sys
import zipfile

ROOT=Path(__file__).resolve().parent
STAGE5=ROOT.parent.parent/'隔离验收/nas-stage45-20261029-r01'
SOURCE=STAGE5/'evidence/run-03-real-nas.json'
OUTPUT=ROOT/'evidence/stage5-independent-readback.json'
NAS=Path(r'\\zwjy-nas\paper_ai\_isolated_benchmarks\nas-stage45-20261029-r01\run-03')

def audit(event,args):
    if event in {'socket.connect','socket.bind','socket.getaddrinfo','subprocess.Popen',
                 'os.remove','os.rmdir','os.rename','os.link','os.symlink'}:
        raise PermissionError('verification cannot execute services or mutate files')
    if event=='open':
        path,mode,flags=args
        if isinstance(path,(str,bytes)):
            p=Path(os.fsdecode(path)).resolve()
            if str(p).startswith('\\\\') and not p.is_relative_to(NAS):
                raise PermissionError('NAS reads restricted to the completed test run')
            if flags & (os.O_WRONLY|os.O_RDWR|os.O_CREAT|os.O_TRUNC|os.O_APPEND) and p!=OUTPUT:
                raise PermissionError('writes restricted to this evidence report')

sys.addaudithook(audit)
from paperflow_prefect.records import read_json,sha_file

source=read_json(SOURCE)
assert source['success'] and source['production_cleanup_verified'] is False
assert source['rag_evidence']=='synthetic_fixture_except_real_not_ready_negative'
assert Path(source['nas_root'])==NAS and source['real_library_removed_files']==0
suite=read_json(STAGE5/'evidence/stage45-final-suite.json')
assert suite['success'] and suite['tests']==99 and suite['failures']==suite['errors']==0
module=ROOT/'paperflow_prefect/nas_handoff.py'
assert sha_file(module)==sha_file(STAGE5/'paperflow_prefect/nas_handoff.py')
checked=[];retained={};absent=[];evidence=[SOURCE,STAGE5/'evidence/stage45-final-suite.json']
for case in source['cases']:
    assert case['passed']
    for p,digest in case['after'].items():
        assert digest==case['before'][p] and sha_file(p)==digest
        retained[p]=digest
    if 'work' not in case:continue
    work=Path(case['work']);assert work.is_relative_to(STAGE5/'scratch/real-nas-run-03')
    receipt_path=work/'archive/nas-receipt.json';receipt=read_json(receipt_path)
    assert receipt['status']=='verified' and Path(receipt['root'])==NAS/case['name']
    manifest=Path(receipt['root'])/receipt['manifest_path']
    assert manifest.is_relative_to(NAS) and sha_file(manifest)==receipt['manifest_sha256']
    evidence.extend([receipt_path,manifest])
    for row in receipt['files']:
        obj=Path(receipt['root'])/row['object_path'];assert obj.is_relative_to(NAS)
        assert obj.stat().st_size==row['bytes'] and sha_file(obj)==row['sha256']
        if row['name'].endswith('.zip'):
            with zipfile.ZipFile(obj) as archive:
                assert archive.testzip() is None and archive.namelist()==['README.txt']
        checked.append({'case':case['name'],'name':row['name'],'bytes':row['bytes'],'sha256':row['sha256']})
    result=case['result'];record=read_json(result['record']);evidence.append(Path(result['record']))
    for key in ('status','paper_id','revision','handoff_id','archive_status','cleanup'):
        assert record[key]==result[key]
    for p in result['cleanup']['completed_paths']:
        assert Path(p).is_relative_to(STAGE5/'scratch/real-nas-run-03') and not Path(p).exists()
        absent.append(p)
live=source['real_not_ready'];assert live['result']=={
    'status':'not_ready','reasons':['body_receipt_missing'],'nas_operations':0,'removed_files':0}
for p,digest in live['after'].items():
    assert live['before'][p]==digest and sha_file(p)==digest
    retained[p]=digest
resume=next(c for c in source['cases'] if c['name']=='resume')
assert resume['phase_calls']==['cleanup','finish','cleanup','finish'] and resume['scan_counts']==[1,1]
assert len(checked)==4 and len(absent)==6
report={'schema':'stage45-independent-readback/1','status':'passed',
        'source_report':{'path':str(SOURCE),'sha256':sha_file(SOURCE)},
        'stage4_module_sha256':sha_file(module),'joint_tests':suite['tests'],
        'nas_files_checked':checked,'generated_files_confirmed_absent':absent,
        'retained_file_hashes':retained,'evidence_hashes':{str(p):sha_file(p) for p in evidence},
        'live_paper_status':'not_ready','production_removed_files':0,
        'rag_evidence':'synthetic_positive_and_real_negative','rag_or_model_calls':0,
        'read_only_verification':True,'production_cleanup_verified':False}
OUTPUT.write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n',encoding='utf8')
print(json.dumps({'status':'passed','joint_tests':suite['tests'],'nas_files':len(checked),
                  'isolated_files_absent':len(absent),'retained_hashes_checked':len(retained),
                  'report':str(OUTPUT)},ensure_ascii=False))
