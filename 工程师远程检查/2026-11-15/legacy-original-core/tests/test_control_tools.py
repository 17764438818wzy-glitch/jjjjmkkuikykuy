import json
from pathlib import Path
import sys
import uuid

from paperflow.winjob import Job, identity_alive, process_identity
from test_winjob import wait_until


def test_local_document_tools_execute_inside_owned_tree(tmp_path):
    code = '''
import json, os, sys, time, zipfile
from pathlib import Path
from xml.etree import ElementTree
import fitz
from PIL import Image
root=Path(sys.argv[1])
with zipfile.ZipFile(root/'source.docx','w') as z:
    z.writestr('word/document.xml','<document><text>synthetic</text></document>')
with zipfile.ZipFile(root/'source.docx') as z:
    assert ElementTree.fromstring(z.read('word/document.xml')).find('text').text=='synthetic'
doc=fitz.open();page=doc.new_page();page.insert_text((50,50),'Stage 2 synthetic fixture');doc.save(root/'source.pdf');doc.close()
with fitz.open(root/'source.pdf') as doc:
    assert 'Stage 2' in doc[0].get_text();doc[0].get_pixmap().save(root/'page.png')
with Image.open(root/'page.png') as image:
    image.resize((100,100)).save(root/'thumbnail.png')
(root/'done.json').write_text(json.dumps(dict(pid=os.getpid(),fitz=fitz.VersionBind,pillow=Image.__version__)))
time.sleep(120)
'''
    job = Job('Local\\pf-tools-' + uuid.uuid4().hex)
    process = None
    try:
        process=job.spawn([sys.executable,'-c',code,str(tmp_path)],cwd=Path.cwd());process.resume()
        wait_until(lambda:(tmp_path/'done.json').exists())
        report=json.loads((tmp_path/'done.json').read_text())
        assert report['pid'] in job.pids()
        identities=[process_identity(pid) for pid in job.pids()]
        job.terminate()
        wait_until(lambda:job.active_count()==0 and all(not identity_alive(i) for i in identities))
        report.update(identities=identities,all_exited=True)
        Path('evidence/document-tools.json').write_text(json.dumps(report,indent=2))
    finally:
        job.close()
        if process:process.close()


def test_libreoffice_profiles_do_not_transfer_work_to_another_instance(tmp_path):
    soffice=Path('C:/Program Files/LibreOffice/program/soffice.exe')
    assert soffice.exists(), 'reachable tool requires installed LibreOffice'
    jobs=[Job('Local\\pf-lo-'+uuid.uuid4().hex) for _ in range(2)]
    processes=[]
    code='''
from pathlib import Path
import json,os,subprocess,sys,time,zipfile
root=Path(sys.argv[1]);profile=(root/'profile').as_uri()
args=[sys.argv[2],'-env:UserInstallation='+profile,'--headless','--norestore','--nodefault','--nofirststartwizard']
log=(root/'libreoffice.log').open('w')
with zipfile.ZipFile(root/'source.docx','w') as archive:
    archive.writestr('[Content_Types].xml','<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/></Types>')
    archive.writestr('_rels/.rels','<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="word/document.xml"/></Relationships>')
    archive.writestr('word/document.xml','<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"><w:body><w:p><w:r><w:t>Stage 2 isolated conversion</w:t></w:r></w:p><w:sectPr/></w:body></w:document>')
try:
    conversion=subprocess.run(args+['--convert-to','pdf','--outdir',str(root),str(root/'source.docx')],capture_output=True,timeout=20)
    (root/'conversion.json').write_text(json.dumps(dict(returncode=conversion.returncode,stdout=conversion.stdout.decode(errors='replace'),stderr=conversion.stderr.decode(errors='replace'))))
    # A forwarded command can return before the persistent instance finishes writing.
    deadline=time.monotonic()+15
    while not (root/'source.pdf').exists() and time.monotonic()<deadline:time.sleep(.05)
    assert conversion.returncode==0 and (root/'source.pdf').exists(),conversion.stderr
except Exception as exc:
    (root/'failure.txt').write_text(repr(exc));raise
import fitz
with fitz.open(root/'source.pdf') as document:assert 'Stage 2 isolated conversion' in document[0].get_text()
# Keep a task-owned headless instance open after verifying conversion. Conversion mode itself may exit.
instance=subprocess.Popen(args+['--accept=pipe,name='+sys.argv[3]+';urp;StarOffice.ServiceManager'],creationflags=0x08000000,stdout=log,stderr=log)
time.sleep(1)
(root/'done.json').write_text(json.dumps(dict(pid=os.getpid(),launcher=instance.pid,profile=profile)))
time.sleep(120)
'''
    try:
        reports=[]
        for i,job in enumerate(jobs):
            root=tmp_path/str(i);root.mkdir()
            (root/'temp').mkdir()
            process=job.spawn([sys.executable,'-c',code,str(root),str(soffice),uuid.uuid4().hex],cwd=Path.cwd(),
                              env={'TEMP':str(root/'temp'),'TMP':str(root/'temp')})
            processes.append(process);process.resume()
            wait_until(lambda:(root/'done.json').exists(),seconds=40)
            reports.append(json.loads((root/'done.json').read_text()))
        import psutil
        names=[[psutil.Process(pid).name() for pid in job.pids()] for job in jobs]
        assert all(any(name.lower()=='soffice.bin' for name in group) for group in names)
        identities=[[process_identity(pid) for pid in job.pids()] for job in jobs]
        assert not set(i['pid'] for i in identities[0]) & set(i['pid'] for i in identities[1])
        jobs[0].terminate()
        wait_until(lambda:jobs[0].active_count()==0 and all(not identity_alive(i) for i in identities[0]))
        assert all(identity_alive(i) for i in identities[1])
        reports[0].update(identities=identities[0],exited=True)
        reports[1].update(identities=identities[1],survived_other_job=True)
        Path('evidence/libreoffice.json').write_text(json.dumps(dict(reports=reports,names=names),indent=2))
    finally:
        for job in jobs:job.close()
        for process in processes:process.close()
