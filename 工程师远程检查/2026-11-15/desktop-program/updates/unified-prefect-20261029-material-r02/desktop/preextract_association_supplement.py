"""Append-only attachment plans, using native upload and verification operations."""
import copy
import json
from pathlib import Path
from paperflow.prepared_protocol import fingerprint
from preextract_material import file_sha,write_json
from preextract_association_closure import required_associations,association_closure


def _load(path):return json.loads(Path(path).read_text(encoding='utf8'))


def _prior_records(plan,receipt,code):
    return [dict(file_ref=row['file_ref'],sha256=row['sha256'],format=row['format'],kind=row['kind'],
        parse_status=row['parse_status'],document_id=receipt['files'][row['file_ref']]['targets'][code]['document_id']) for row in plan['files']]


def known_plans(prepared,work,data_root):
    from associated_delivery import load_plan,PLAN_NAME,RECEIPT_NAME
    result=[]
    current=load_plan(prepared,work)
    if current:
        result.append(dict(work_dir=str(work),plan=current,plan_path=str(Path(work)/'private'/PLAN_NAME),
            receipt_path=str(Path(work)/RECEIPT_NAME)))
    # Existing, independently produced corrections are evidence, not permission.
    root=Path(data_root)/'private/associated_corrections'
    if root.is_dir():
        for path in root.glob('*/private/'+PLAN_NAME):
            plan=_load(path)
            if plan.get('task')!=prepared['task'] or plan.get('manifest_sha256')!=prepared['validation']['manifest_sha256']:continue
            directory=path.parent.parent
            copy_prepared=dict(prepared,associated_upload_plan_sha256=file_sha(path))
            verified=load_plan(copy_prepared,directory)
            result.append(dict(work_dir=str(directory),plan=verified,plan_path=str(path),receipt_path=str(directory/RECEIPT_NAME)))
    for path in (Path(work)/'private/associated-supplements').glob('*/plan.json'):
        plan=_load(path)
        descriptor=_descriptor(path,plan)
        validate_supplement(prepared,work,descriptor)
        result.append(dict(work_dir=str(path.parent),plan=plan,plan_path=str(path),receipt_path=descriptor['receipt_path']))
    result.sort(key=lambda item:len(item['plan']['files']))
    return result


def _descriptor(path,plan):
    path=Path(path)
    return dict(plan=plan,plan_path=str(path),plan_sha256=file_sha(path),receipt_path=str(path.parent/'receipt.json'),
        evidence_files=plan['prior_evidence'],seed_files=plan.get('seed_files',{}),prior_associations=plan.get('prior_associations',{}))


def validate_supplement(prepared,work,supplement):
    plan=supplement['plan'];path=Path(supplement['plan_path'])
    if (file_sha(path)!=supplement['plan_sha256'] or _load(path)!=plan
            or plan['task']!=prepared['task'] or plan['manifest_sha256']!=prepared['validation']['manifest_sha256']
            or plan['routes']!=prepared['packet']['routes'] or file_sha(Path(work)/'material.json')!=plan['material_sha256']):
        raise ValueError('supplement identity changed')
    if not path.resolve().is_relative_to((Path(work)/'private/associated-supplements').resolve()):raise ValueError('supplement outside workspace')
    for name,sha in supplement['evidence_files'].items():
        if not Path(name).is_file() or file_sha(name)!=sha:raise ValueError('supplement evidence changed')
    for row in plan['files']:
        if file_sha(row['original_path'])!=row['sha256']:raise ValueError('supplement original changed')


def ensure_associations(prepared,body_receipt,work,client,*,data_root,purpose_config,stop=None,progress=lambda *_:None):
    from associated_delivery import deliver_associated
    from upload_tracking import service_key
    work=Path(work);required=required_associations(work,purpose_config=purpose_config)
    existing=known_plans(prepared,work,data_root)
    matching=[item for item in existing if item['plan'].get('schema')=='prepared-associated-supplement/1'
              and item['plan'].get('required_evidence_sha256')==required['evidence_sha256']]
    if matching:
        item=matching[-1];descriptor=_descriptor(item['plan_path'],item['plan'])
        receipt=deliver_associated(prepared,body_receipt,work,client,stop=stop,progress=progress,supplement=descriptor)
        closure=association_closure(required,body_receipt,[receipt],manifest_sha256=item['plan']['manifest_sha256'],service=service_key(client.base_url))
        return dict(closure,receipt_path=descriptor['receipt_path'],plan_path=item['plan_path'],required_inventory=required)
    prior=[];evidence={str(work/'material.json'):file_sha(work/'material.json')}
    union={row['file_ref']:copy.deepcopy(row) for row in required['files']}
    seed={};prior_associations={}
    for item in existing:
        plan=item['plan'];path=Path(item['receipt_path'])
        evidence[item['plan_path']]=file_sha(item['plan_path'])
        if not path.exists():continue
        receipt=_load(path);prior.append(receipt);evidence[str(path)]=file_sha(path)
        identity=receipt.get('identity',{})
        if (identity.get('task')!=prepared['task'] or identity.get('manifest_sha256')!=prepared['validation']['manifest_sha256']
                or identity.get('plan_sha256')!=file_sha(item['plan_path']) or identity.get('service')!=service_key(client.base_url)):
            raise ValueError('prior association receipt binding changed')
        for row in plan['files']:
            ref=row['file_ref']
            if ref in union and row['sha256']!=union[ref]['sha256']:raise ValueError('prior member identity differs')
            union.setdefault(ref,copy.deepcopy(row))
        for ref,entry in receipt.get('files',{}).items():
            old=seed.get(ref,{})
            if any(old.get(k) and entry.get(k) and old[k]!=entry[k] for k in ('sha256','file_id')):
                raise ValueError('prior original identities require reconciliation')
            seed[ref]=copy.deepcopy(entry)
        if receipt.get('status')=='complete':
            for route in plan['routes']:
                value=json.dumps(_prior_records(plan,receipt,route['code']),ensure_ascii=False,separators=(',',':'))
                prior_associations.setdefault(route['code'],[])
                if value not in prior_associations[route['code']]:prior_associations[route['code']].append(value)
    # Every new leaf carries an exact native purpose receipt or immutable mapping proof.
    for row in required['files']:
        role=row.get('purpose_review') or {}
        receipt=role.get('purpose_review_receipt')
        if receipt:evidence[receipt]=file_sha(receipt)
        mapping=row.get('purpose_mapping') or {}
        for key in ('review_path','evidence_path','archive_path','request_path','request_state_path','response_path'):
            if mapping.get(key):evidence[mapping[key]]=file_sha(mapping[key])
    if not union:
        return association_closure(required,body_receipt,[],manifest_sha256=prepared['validation']['manifest_sha256'],service=service_key(client.base_url))
    plan=dict(schema='prepared-associated-supplement/1',task=prepared['task'],manifest_sha256=prepared['validation']['manifest_sha256'],
        routes=prepared['packet']['routes'],material_sha256=file_sha(work/'material.json'),files=list(union.values()),
        required_evidence_sha256=required['evidence_sha256'],prior_evidence=evidence,
        seed_files=seed,prior_associations=prior_associations)
    key=fingerprint(plan);folder=work/'private/associated-supplements'/key;folder.mkdir(parents=True,exist_ok=True)
    path=folder/'plan.json'
    if path.exists() and _load(path)!=plan:raise ValueError('immutable supplement changed')
    if not path.exists():write_json(path,plan)
    descriptor=dict(plan=plan,plan_path=str(path),plan_sha256=file_sha(path),receipt_path=str(folder/'receipt.json'),
        evidence_files=evidence,seed_files=seed,prior_associations=prior_associations)
    receipt=deliver_associated(prepared,body_receipt,work,client,stop=stop,progress=progress,supplement=descriptor)
    closure=association_closure(required,body_receipt,[receipt],manifest_sha256=plan['manifest_sha256'],service=service_key(client.base_url))
    closure.update(receipt_path=str(folder/'receipt.json'),plan_path=str(path),required_inventory=required)
    return closure
