"""Reuse real reviewed bytes, delete copies only, and time metadata indexing."""
import sys
sys.dont_write_bytecode=True
import hashlib
import json
import os
import shutil
import threading
import time
from pathlib import Path
from types import SimpleNamespace

ROOT=Path(__file__).resolve().parents[1]
UPDATE=ROOT/'updates/purpose-review-20261022'
HOME=ROOT/'_verification/purpose-review-20261022'/('real-copies-'+str(os.getpid()))
HOME.mkdir(parents=True)
sys.path[:0]=[str(UPDATE/'desktop'),str(ROOT/'desktop'),str(ROOT/'core'),str(ROOT/'scripts')]


def audit(event,args):
    if event.startswith(('socket.','subprocess.','os.system','os.posix_spawn')):
        raise RuntimeError('Real-copy acceptance prohibits model calls, uploads and subprocesses')
    paths=[]
    if event=='open':
        mode,flags=args[1:3]
        if ((isinstance(mode,str) and any(c in mode for c in 'wax+')) or
                (isinstance(flags,int) and flags & (os.O_WRONLY|os.O_RDWR|os.O_CREAT|os.O_TRUNC|os.O_APPEND))):paths.append(args[0])
    elif event in {'os.mkdir','os.remove','os.rmdir','os.chmod','os.utime'}:paths.append(args[0])
    elif event in {'os.rename','os.replace'}:paths.extend(args[:2])
    for path in paths:
        if not isinstance(path,(str,bytes,os.PathLike)):continue
        if not Path(os.fsdecode(path)).resolve().is_relative_to((ROOT/'_verification').resolve()):
            raise RuntimeError('Real-copy acceptance rejected a write outside verification')


sys.addaudithook(audit)
from verify_purpose_review import cases,sha
import preextract_workbench
import material_roles
import purpose_review
from purpose_cleanup import clean_archive
from associated_materials import MaterialContext


def denied(*args,**kwargs):raise AssertionError('Acceptance must reuse the completed purpose receipt')
preextract_workbench.invoke_once=denied
config=json.loads((ROOT/'config/purpose-review/settings.json').read_text(encoding='utf8'))
config['cache_root']=str(ROOT/'_verification/purpose-review-20261022/live/cache')
processor=SimpleNamespace(config={'purpose_review':config},stop=threading.Event())
rows=cases()
before={row['record']['path']:sha(row['record']['path']) for row in rows}
results=[];forms=[];drawing=None;body=None
for index,row in enumerate(rows):
    original=Path(row['record']['path'])
    folder=HOME/'copies'/str(index);folder.mkdir(parents=True)
    target=folder/original.name
    shutil.copy2(original,target)
    record={**row['record'],'path':str(target),'input_source_path':row['record'].get('input_source_path',str(original))}
    record.setdefault('name',row['record'].get('name',original.name))
    checked=material_roles.classify_document(record,processor=processor,work=HOME/'work')
    assert checked.get('purpose_review_receipt'),'Expected completed model receipt, not a new call'
    if row['expected']=='delete':
        material_roles.delete_confirmed_form(record,HOME/'deletions'/str(index),checked,processor=processor,stop=processor.stop)
        assert not target.exists()
        forms.append((row,checked))
        results.append(dict(id=row['id'],copy_deleted=True,source_unchanged=sha(original)==before[str(original)]))
    else:
        # An unrelated form's decision must never authorize deleting this file.
        wrong=dict(action='delete',confirmed=True,evidence_sha256='f'*64)
        try:material_roles.delete_confirmed_form(record,HOME/'rejected-deletions'/str(index),wrong,processor=processor,stop=processor.stop)
        except ValueError:pass
        else:raise AssertionError('Body/attachment accepted an unrelated form deletion')
        assert target.exists() and sha(target)==before[str(original)]
        if row['expected']=='drawing':drawing=(row,checked)
        if row['expected']=='body' and original.suffix.lower()=='.docx':body=(row,checked)
        results.append(dict(id=row['id'],copy_kept=True,wrong_deletion_rejected=True,source_unchanged=True))

assert len(forms)==4 and drawing and body
import zipfile
archive=HOME/'actual-mixed.zip'
files={'正文.docx':Path(body[0]['record']['path']),
       '图纸.pdf':Path(drawing[0]['record']['path']),
       '开题报告.pdf':Path(forms[0][0]['record']['path'])}
with zipfile.ZipFile(archive,'w') as z:
    for name,path in files.items():z.write(path,name)
source_hash=sha(archive)
cleaned=clean_archive(archive,HOME/'actual-clean.zip',{'开题报告.pdf':forms[0][1]})
with zipfile.ZipFile(cleaned['path']) as z:
    assert set(z.namelist())=={'正文.docx','图纸.pdf'}
    assert all(hashlib.sha256(z.read(name)).hexdigest()==sha(files[name]) for name in z.namelist())
assert sha(archive)==source_hash

# Read the existing manifest only. The index never reads document content.
data=Path(json.loads((ROOT/'config/paths.json').read_text(encoding='utf8'))['data_dir'])
manifest=json.loads((data/'runs/AUTO_20261022_052854_997b95/manifest.json').read_text(encoding='utf8'))
records=manifest['inputs']
material_roles._read=denied
start=time.monotonic()
with purpose_review.scope(processor,HOME,allow_requests=False):
    context=MaterialContext(records,stop=processor.stop)
seconds=round(time.monotonic()-start,3)
assert len(context.records)==len(records)
assert all(sha(path)==value for path,value in before.items())
report=dict(status='passed',original_file_cases=8,real_page_boundary_cases=2,
    actual_form_copies_deleted=len(forms),original_files_unchanged=True,extra_model_requests=0,
    cleanup=dict(removed=list(cleaned['removed']),retained=list(cleaned['kept']),original_archive_unchanged=True),
    indexing=dict(records=len(records),seconds=seconds,native_reads=0,llm_requests=0),cases=results,
    production_tasks_started=False,existing_uploads_modified=False,artifacts=str(HOME))
(HOME/'report.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf8')
print(json.dumps(report,ensure_ascii=False))
