"""Synthetic RAG evidence with a real H2.1 receipt; never a production proof."""
from pathlib import Path
import io
import zipfile
from receipt_h21_fixture import handoff
from paperflow_prefect.records import identity,write_json,read_json,sha_file
from paperflow_prefect.receipts import ReceiptStore
from paperflow_prefect.intake_handoff import receive_handoff
from paperflow_prefect.stage3_handoff import read_receipt


def fixture(root, *, attachments=True):
    root=Path(root).resolve(); root.mkdir(parents=True,exist_ok=True)
    library=root/'library'; library.mkdir(exist_ok=True)
    archive=io.BytesIO()
    with zipfile.ZipFile(archive,'w',zipfile.ZIP_DEFLATED) as zipped:
        zipped.writestr('README.txt','Synthetic archive for stage 4 to 5 acceptance.\n')
    h=handoff(library,library/'person',attachments=[('data.zip',archive.getvalue())] if attachments else [])
    hp=root/'input.json'; write_json(hp,h)
    receipt=receive_handoff(hp,ReceiptStore(root/'records/receipts'))
    _,paper=read_receipt(receipt['record_path'])
    task={'task_id':'fixture-task','source_sha256':paper['body']['sha256'],'revision':1,'source_id':'fixture-source'}
    codes=['081301','080206']; targets={}
    for code in codes:
        meta=dict(paperflow_task=task['task_id'],paperflow_source_sha256=task['source_sha256'],
                  paperflow_manifest_sha256='a'*64,paperflow_revision='1',paperflow_specialty=code,paperflow_role='body')
        targets[code]={'verified':True,'retrieval_verified':True,'body':{'dataset_id':'body-'+code,
            'document_id':'body-doc-'+code,'identity':meta,'chunks':{'record':{'status':'written','chunk_id':'body-chunk-'+code,'content_sha256':'b'*64}}}}
    body={'schema':'paper-preextract-delivery/1.0','status':'complete','stage':'verified','task':task,
          'manifest_sha256':'a'*64,'targets':targets,'fixture':True}
    bp=root/'records/body-delivery.json'; write_json(bp,body)
    ap=[]
    for row in paper['attachments']:
        md=root/'records'/row['file_id']/'attachment.md'; md.parent.mkdir(parents=True); md.write_text('# Fixture attachment\n',encoding='utf8')
        mdsha=sha_file(md)
        raw={'status':'verified','file_id':'raw-fixture','document_id':'raw-doc','dataset_id':'materials-081301',
             'parent_id':'folder-081301','source_sha256':row['sha256'],'bytes':row['bytes']}
        pubs={code:{'status':'verified','dataset_id':'materials-'+code,'document_id':'md-'+code,
                    'md_sha256':mdsha,'projection_sha256':'c'*64,'chunk_count':1,
                    'chunks':{'0':{'id':'md-chunk-'+code,'content_sha256':'d'*64}},
                    'retrieval_verified':True,'retrieval_verified_at':1,'retrieval_chunk_ids':['md-chunk-'+code]} for code in codes}
        state={'schema':'paperflow-attachment-ingest/1','status':'verified','fixture':True,
            'binding':{'service':'http://127.0.0.1:9','paper_id':paper['paper_id'],'attachment_id':row['file_id'],
                'source_sha256':row['sha256'],'body_source_sha256':paper['body']['sha256'],
                'raw_identity':identity([paper['paper_id'],row['file_id'],row['sha256']]),
                'primary':{'code':'081301','dataset_id':raw['dataset_id'],'parent_id':raw['parent_id']}},
            'raw':raw,'current_md_sha256':mdsha,'saved_md_path':str(md),
            'delivery':{'paper_id':paper['paper_id'],'attachment_id':row['file_id'],'source_path':row['path'],
                'source_sha256':row['sha256'],'md_path':str(md),'md_sha256':mdsha,'delivery_version':'e'*64},
            'publications':{mdsha:pubs},'parent_link_status':'paper_identity_only'}
        p=md.parent/'delivery.json'; write_json(p,state); ap.append(str(p))
    return {'root':str(root),'paper':paper,'intake_receipt_path':receipt['record_path'],
            'body_receipt_path':str(bp),'attachment_receipt_paths':ap,'validation_scope':'isolated_fixture'}


def build(f,**overrides):
    from paperflow_prefect.nas_handoff import build_nas_handoff
    args={k:v for k,v in f.items() if k not in {'root','paper'}}; args.update(overrides)
    return build_nas_handoff(**args)


def cache_item(f, *, name='preview.txt'):
    from paperflow_prefect.nas_handoff import object_fingerprint
    root=Path(f['root']); cache=root/'records/cache';cache.mkdir(parents=True,exist_ok=True)
    p=cache/name;p.write_bytes(b'synthetic regenerable preview')
    owner={'paper_id':f['paper']['paper_id'],'revision':f['paper']['revision']}
    item={**owner,'path':str(p),'root':str(cache),'bytes':p.stat().st_size,'sha256':sha_file(p),
          'object_fingerprint':object_fingerprint(p.stat()),'kind':'converted_preview'}
    record={**owner,'schema':'paperflow-intermediate-ownership/1','producer_state':'complete',
            'finalized':True,'reproducible':True,'owners':[owner],'references':[],'active_consumers':[],'files':[item]}
    proof=root/'records/cache-owner.json';write_json(proof,record)
    return dict(item,producer_record={'path':str(proof),'sha256':sha_file(proof)})
