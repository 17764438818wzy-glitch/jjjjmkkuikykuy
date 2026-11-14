"""Reuse a purpose answer only with its original context and physical member proof."""
import copy
import hashlib
import json
from pathlib import Path
import subprocess
import zipfile

from preextract_material import file_sha


def archive_member(archive,member):
    archive=Path(archive)
    if archive.suffix.lower()=='.zip':
        with zipfile.ZipFile(archive) as reader:
            matches=[row for row in reader.infolist() if row.filename==member and not row.is_dir()]
            if len(matches)!=1 or matches[0].file_size>100*1024**2:raise ValueError('archive member not unique or too large')
            raw=reader.read(matches[0])
    elif archive.suffix.lower()=='.rar':
        from archive_input import _sevenzip,_rar4_members,rar5_members
        seven=_sevenzip()
        if not seven:raise ValueError('archive reader unavailable')
        with archive.open('rb') as stream:signature=stream.read(8)
        rows=_rar4_members(archive,seven) if signature[:7]==b'Rar!\x1a\x07\0' else rar5_members(archive)
        matches=[row for row in rows if row['name']==member and not row['directory']]
        if len(matches)!=1 or matches[0]['size']>100*1024**2:raise ValueError('archive member not unique or too large')
        raw=subprocess.run([str(seven),'x','-so','-y','-bd','--',str(archive),member],stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=60,check=True,
            creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0)).stdout
        if len(raw)!=matches[0]['size']:raise ValueError('archive member length differs')
    else:raise ValueError('archive format requires verification')
    return hashlib.sha256(raw).hexdigest(),len(raw)


def confirmed_member(work,bundle,row,parent,config):
    from purpose_review import project_key,_contract,POLICY
    from preextract_scheduler_adapter import request_observation
    path=Path(row['original_path']).resolve();sha=row['sha256']
    if file_sha(path)!=sha:raise ValueError('retained member changed')
    physical=row.get('archive_member_path')
    archive=Path(parent['source_path']) if parent else None
    record=dict(row,path=str(path),name=Path(row.get('source_path') or path).name)
    if parent:record['input_source_path']=str(archive)
    project=project_key(record)
    names=[physical or record['name']]
    if physical and not physical.startswith('source/'):names.append('source/'+physical)
    for recorded in names:
        context='\0'.join((project,str(recorded),POLICY,config.get('model',''),config.get('reasoning_effort','max'),_contract(config)))
        key=hashlib.sha256((sha+'\0'+context).encode()).hexdigest()
        review_path=Path(config.get('cache_root',''))/'reviews'/key/'review.json'
        if not review_path.is_file():continue
        saved=json.loads(review_path.read_text(encoding='utf8'));role=saved['role']
        if (saved.get('policy')!=POLICY or saved.get('contract')!=_contract(config) or saved.get('source_sha256')!=sha
                or role.get('evidence_sha256')!=sha or not role.get('purpose_review_verified') or not role.get('confirmed')):continue
        evidence_path=review_path.parent/'evidence/evidence.json'
        evidence=json.loads(evidence_path.read_text(encoding='utf8'))
        if evidence.get('source_sha256')!=sha or (physical and evidence.get('archive_member_path')!=recorded):continue
        model=Path(saved['model_dir']);phase,_,request=request_observation(model)
        if phase!='response_complete':continue
        answer=(model/'response.txt').read_text(encoding='utf8').strip()
        if answer.startswith('```'):answer='\n'.join(answer.splitlines()[1:-1])
        if json.loads(answer)!=saved.get('decision'):continue
        proof=dict(mapping_kind='exact' if recorded==names[0] else 'single_source_wrapper',project_key=project,
            member_sha256=sha,material_member=physical,recorded_member=recorded,body_source_sha256=bundle['source']['source_sha256'],
            material_sha256=file_sha(Path(work)/'material.json'),review_path=str(review_path),review_sha256=file_sha(review_path),
            evidence_path=str(evidence_path),evidence_sha256=file_sha(evidence_path),contract=saved['contract'],
            request_path=str(model/'request.json'),request_state_path=str(model/'request-state.json'),response_path=str(model/'response.txt'),
            request_sha256=request['request_sha256'],response_sha256=request['response_sha256'])
        if parent:
            expected=row.get('archive_source_hash') or parent.get('sha256')
            if file_sha(archive)!=expected or project_key(dict(path=str(archive)))!=project:continue
            member_sha,size=archive_member(archive,physical)
            if member_sha!=sha:continue
            proof.update(archive_path=str(archive),archive_sha256=expected,physical_member=physical,member_bytes=size)
        result=copy.deepcopy(role);result['purpose_review_receipt']=str(review_path)
        return result,proof
    return None,None
