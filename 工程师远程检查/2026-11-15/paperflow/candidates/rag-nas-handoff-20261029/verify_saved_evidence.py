"""Read-only compatibility check; writes only its candidate evidence report."""
import json
import os
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parent
REPORT=ROOT/'evidence/post-review-readback.json'

def audit(event,args):
    if event in {'socket.connect','socket.bind','socket.getaddrinfo','subprocess.Popen',
                 'os.remove','os.rmdir','os.rename','os.link','os.symlink'}:
        raise PermissionError('saved-evidence check is read-only and offline')
    if event=='open':
        path,mode,flags=args
        if flags & (os.O_WRONLY|os.O_RDWR|os.O_CREAT|os.O_TRUNC|os.O_APPEND):
            if not isinstance(path,(str,bytes)) or Path(os.fsdecode(path)).resolve()!=REPORT:
                raise PermissionError('writes restricted to this check report')

sys.addaudithook(audit)
from paperflow_prefect.records import read_json,sha_file
from paperflow_prefect.settings import Settings
from paperflow_prefect.legacy import bind
from paperflow_prefect.nas_handoff import _body,validate_nas_handoff

main=ROOT.parent.parent/'prefect-paperflow'
bind(Settings.load(main/'config.json'))
previous=read_json(ROOT/'evidence/retained-body-reader-check.json')
receipt=read_json(previous['body_receipt']['path'])
refs={}
body,errors=_body({'body':{'sha256':receipt['task']['source_sha256']}},previous['body_receipt'],
                  previous['prepared'],'production',refs)
assert body['verified'] and not errors
live=validate_nas_handoff(read_json(ROOT/'evidence/live-not-ready-handoff.json'))
assert live['status']=='not_ready' and live['reasons']==['body_receipt_missing']
report={'schema':'stage4-saved-evidence-check/1','status':'passed',
        'module_sha256':sha_file(ROOT/'paperflow_prefect/nas_handoff.py'),
        'retained_body':{'status':'verified','codes':body['codes'],'evidence_files':len(refs),
                         'receipt':previous['body_receipt'],'prepared':previous['prepared'],
                         'scope':'saved native receipt and sealed package, not new remote verification'},
        'live_attachment_only':{'status':live['status'],'reasons':live['reasons'],
                                'paper_id':live['paper']['paper_id'],'files_checked':len(live['files'])},
        'external_calls':0,'model_calls':0,'nas_operations':0,'deleted_files':0,
        'production_config_sha256':sha_file(main/'config.json')}
REPORT.write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n',encoding='utf8')
print(json.dumps({'status':'passed','body_codes':body['codes'],'live_status':live['status'],
                  'external_calls':0,'deleted_files':0,'report':str(REPORT)},ensure_ascii=False))
