"""Pure required-vs-verified comparison; count summaries are never acceptance."""
import json
import subprocess
from pathlib import Path
from paperflow.prepared_protocol import fingerprint
from preextract_material import file_sha


def required_associations(work,*,purpose_config):
    from associated_delivery import KINDS,_reviewed_leaf_allowed,_private_original_scope
    from preextract_member_proof import confirmed_member
    work=Path(work).resolve();bundle=json.loads((work/'material.json').read_text(encoding='utf8'))
    original_allowed=_private_original_scope(bundle,work)
    files=[];review=[]
    def collect(row,ref,parent=None):
        if row.get('kind')=='archive':
            if not row.get('members'):review.append(dict(file_ref=ref,reason='archive_inventory_missing'))
            for i,member in enumerate(row.get('members',[]),1):collect(member,ref+'/'+str(i),row)
            for i,member in enumerate(row.get('excluded_members',[]),1):collect(member,ref+'/excluded/'+str(i),row)
            return
        path=Path(row.get('original_path') or row.get('source_path') or '')
        if not path.is_file() or file_sha(path)!=row.get('sha256'):
            review.append(dict(file_ref=ref,reason='original_missing_or_changed'));return
        member=dict(row,path=str(path),name=Path(row.get('source_path') or path).name)
        if parent:member['input_source_path']=parent['source_path']
        try:role,proof=confirmed_member(work,bundle,row,parent,purpose_config) if purpose_config.get('enabled') else (None,None)
        except (OSError,ValueError,KeyError,TypeError,subprocess.SubprocessError):role,proof=None,None
        current=dict(row)
        current.pop('purpose_review',None)
        if role:
            if role.get('confirmed') and role.get('purpose_review_verified') and role.get('evidence_sha256')==row['sha256']:
                if role.get('kind')=='document' or role.get('action')=='delete':return
                current.update(kind=role.get('kind'),purpose_review=role,purpose_policy='program-llm-purpose/1',purpose_mapping=proof)
        # Unreviewed native CAD/code can use the established exact suffix rule.
        if current.get('kind') not in KINDS or not _reviewed_leaf_allowed(current):
            review.append(dict(file_ref=ref,reason='purpose_not_confirmed'));return
        if not original_allowed(path.resolve()) or bundle.get('file_hashes',{}).get(str(path.resolve()))!=row['sha256']:
            review.append(dict(file_ref=ref,reason='private_material_binding_missing'));return
        files.append(dict(current,file_ref=ref,original_path=str(path.resolve())))
    for i,row in enumerate(bundle.get('attachments',[]),1):
        if row.get('association_status')=='candidate':collect(row,'attachment_'+str(i))
        else:review.append(dict(file_ref='attachment_'+str(i),reason='body_association_unconfirmed'))
    result=dict(checked=True,material_sha256=file_sha(work/'material.json'),files=files,review=review)
    return dict(result,evidence_sha256=fingerprint(result))


def association_closure(required,body_receipt,receipts,*,manifest_sha256,service):
    missing=[];conflicts=[];verified=[]
    body_targets=body_receipt.get('targets') or {}
    body_complete=body_receipt.get('status')=='complete' and bool(body_targets) and all(
        target.get('verified') and target.get('retrieval_verified') and target.get('body',{}).get('document_id')
        and target.get('body',{}).get('dataset_id') for target in body_targets.values())
    valid=[]
    for receipt in receipts:
        identity=receipt.get('identity') or {}
        if identity.get('manifest_sha256')!=manifest_sha256 or identity.get('service')!=service:
            conflicts.append('receipt_identity_changed');continue
        if receipt.get('status')!='complete':continue
        valid.append(receipt)
    for row in required.get('files',[]):
        ref=row['file_ref'];matches=[]
        for receipt in valid:
            entry=receipt.get('files',{}).get(ref)
            if not entry:continue
            good=entry.get('sha256')==row['sha256'] and entry.get('status')=='verified' and bool(entry.get('file_id'))
            for code,parent in body_targets.items():
                target=entry.get('targets',{}).get(code,{})
                link=receipt.get('body_links',{}).get(code,{})
                good=good and target.get('verified') and target.get('document_id') and (
                    target.get('dataset_id')==parent['body']['dataset_id'] and target.get('parent_document_id')==parent['body']['document_id']
                    and link.get('verified') and link.get('dataset_id')==parent['body']['dataset_id'] and link.get('document_id')==parent['body']['document_id'])
            if good:matches.append(entry)
            else:conflicts.append(ref)
        if matches and all(m==matches[0] for m in matches):verified.append(ref)
        else:missing.append(ref)
    result=dict(complete=bool(required.get('checked') and body_complete and not required.get('review') and not missing and not conflicts),
        required=len(required.get('files',[])),verified=len(verified),missing=missing,conflicts=conflicts,
        review=required.get('review') or [],body_verified=bool(body_complete))
    return dict(result,evidence_sha256=fingerprint(dict(required=required,body=body_receipt,receipts=receipts)))
