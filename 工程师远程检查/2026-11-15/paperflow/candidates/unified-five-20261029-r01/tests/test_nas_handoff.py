from pathlib import Path
import copy
import hashlib
import tempfile
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import patch
from paperflow_prefect.records import identity,read_json,write_json,sha_file
from nas_handoff_fixture import fixture,build,cache_item


class NasHandoffTests(unittest.TestCase):
    def test_ready_preserves_h21_identity_and_lists_all_originals(self):
        with tempfile.TemporaryDirectory() as root:
            f=fixture(root); out=build(f)
            self.assertEqual(out['status'],'ready'); self.assertEqual(out['paper'],f['paper'])
            self.assertEqual(len(out['files']),2)
            self.assertTrue(all(r['object_fingerprint']['inode'] for r in out['files']))
            self.assertEqual(out['rag']['attachments'][0]['association']['status'],'verified_paper_source_binding')
            from paperflow_prefect.nas_handoff import validate_nas_handoff
            self.assertEqual(validate_nas_handoff(out),out)

    def test_attachment_only_is_not_whole_paper_ready(self):
        with tempfile.TemporaryDirectory() as root:
            out=build(fixture(root),body_receipt_path=None)
            self.assertEqual(out['status'],'not_ready'); self.assertIn('body_receipt_missing',out['reasons'])

    def test_partial_body_and_missing_attachment_block_archiving(self):
        for which in ['partial','attachment']:
            with self.subTest(which=which),tempfile.TemporaryDirectory() as root:
                f=fixture(root)
                if which=='partial':
                    r=read_json(f['body_receipt_path']);r['status']='pending';write_json(f['body_receipt_path'],r)
                    out=build(f)
                else:out=build(f,attachment_receipt_paths=[])
                self.assertEqual(out['status'],'not_ready')

    def test_cross_paper_attachment_and_disabled_retrieval_are_rejected(self):
        for which in ['paper','retrieval']:
            with self.subTest(which=which),tempfile.TemporaryDirectory() as root:
                f=fixture(root);p=f['attachment_receipt_paths'][0];r=read_json(p)
                if which=='paper':r['binding']['paper_id']='PF_wrong'
                else:next(iter(r['publications'].values()))['081301']['retrieval_verified']=False
                write_json(p,r);self.assertEqual(build(f)['status'],'not_ready')

    def test_synthetic_receipts_cannot_enable_production(self):
        with tempfile.TemporaryDirectory() as root:
            out=build(fixture(root),validation_scope='production')
            self.assertEqual(out['status'],'not_ready');self.assertIn('synthetic_rag_evidence',out['reasons'])

    def test_no_attachment_is_an_explicit_empty_complete_set(self):
        with tempfile.TemporaryDirectory() as root:
            out=build(fixture(root,attachments=False));self.assertEqual(out['status'],'ready')
            self.assertEqual(out['rag']['attachments'],[]);self.assertEqual(len(out['files']),1)

    def test_changed_evidence_and_forged_ready_are_rejected(self):
        from paperflow_prefect.nas_handoff import validate_nas_handoff
        with tempfile.TemporaryDirectory() as root:
            f=fixture(root);out=build(f,body_receipt_path=None)
            out['status']='ready';out['reasons']=[];out['handoff_id']=identity({k:v for k,v in out.items() if k!='handoff_id'})
            with self.assertRaises(ValueError):validate_nas_handoff(out)
            out=build(f);Path(f['body_receipt_path']).write_text('{}',encoding='utf8')
            with self.assertRaises(ValueError):validate_nas_handoff(out)

    def test_frozen_recovery_can_validate_without_deleted_originals(self):
        from paperflow_prefect.nas_handoff import validate_nas_handoff
        with tempfile.TemporaryDirectory() as root:
            f=fixture(root);out=build(f)
            # These are newly created synthetic fixture originals within this temp root.
            for row in out['files']:
                p=Path(row['path']).resolve();self.assertTrue(p.is_relative_to(Path(root).resolve()));p.unlink()
            self.assertEqual(validate_nas_handoff(out,verify_files=False)['status'],'ready')
            with self.assertRaises((ValueError,FileNotFoundError)):validate_nas_handoff(out)

    def test_only_proven_single_owner_cache_is_a_cleanup_candidate(self):
        with tempfile.TemporaryDirectory() as root:
            f=fixture(root); item=cache_item(f);out=build(f,intermediates=[item])
            self.assertEqual(out['intermediates'],[item]);self.assertEqual(out['retained_intermediates'],[])
            self.assertIn(item['producer_record'],out['retained_evidence'])
            producer=read_json(item['producer_record']['path']);producer['owners'].append({'paper_id':'another','revision':'1'})
            write_json(item['producer_record']['path'],producer);item['producer_record']['sha256']=sha_file(item['producer_record']['path'])
            out=build(f,intermediates=[item]);self.assertEqual(out['intermediates'],[])
            self.assertEqual(len(out['retained_intermediates']),1)

    def test_final_md_and_changed_original_object_are_preserved(self):
        from paperflow_prefect.nas_handoff import validate_nas_handoff
        with tempfile.TemporaryDirectory() as root:
            f=fixture(root);item=cache_item(f,name='final.md');out=build(f,intermediates=[item])
            self.assertEqual(out['intermediates'],[]);self.assertTrue(Path(item['path']).exists())
            source=Path(out['files'][0]['path']); replacement=source.with_name('replacement.tmp')
            replacement.write_bytes(source.read_bytes());replacement.replace(source)
            with self.assertRaises(ValueError):validate_nas_handoff(out)

    def test_primary_attachment_md_must_use_the_originals_bound_library(self):
        with tempfile.TemporaryDirectory() as root:
            f=fixture(root);p=f['attachment_receipt_paths'][0];r=read_json(p)
            r['publications'][r['current_md_sha256']][r['binding']['primary']['code']]['dataset_id']='WRONG-MD-DATASET'
            write_json(p,r);out=build(f)
            self.assertEqual(out['status'],'not_ready')
            self.assertIn('attachment_primary_library_mismatch',out['reasons'])

    def test_body_and_image_libraries_must_match_the_sealed_routes(self):
        from paperflow_prefect.nas_handoff import _body, _ref
        with tempfile.TemporaryDirectory() as root:
            f=fixture(root,attachments=False);receipt=read_json(f['body_receipt_path']);receipt.pop('fixture')
            records={'body':[('record','sealed body',None)],'images':[('figure','sealed figure',None)]}
            routes=[]
            for code,target in receipt['targets'].items():
                routes.append({'code':code,'paper_dataset_id':'body-'+code,'image_dataset_id':'images-'+code})
                target['images']=copy.deepcopy(target['body']);target['images']['identity']['paperflow_role']='images'
                target['images']['dataset_id']='images-'+code;target['images']['document_id']='image-doc-'+code
                for role in records:
                    record=records[role][0]
                    target[role]['chunks']={record[0]:{'status':'written','chunk_id':role+'-chunk-'+code,
                        'content_sha256':hashlib.sha256(record[1].encode()).hexdigest()}}
            package_dir=Path(root)/'sealed-package';package_dir.mkdir()
            packet={'routes':routes,'manifest_sha256':receipt['manifest_sha256']}
            prepared=Path(root)/'prepared.json'
            write_json(prepared,{'paperflow_root':'fixture-only','package_path':str(package_dir),'catalog':{},
                                 'task':receipt['task'],'validation':{'embedding':{'admission_verified':True}}})
            # This isolated substitute exercises route binding without contacting native services.
            preextract=ModuleType('preextract_publish')
            preextract._paperflow=lambda _:SimpleNamespace(validate_package=lambda *a,**kw:packet)
            rag=ModuleType('paperflow.rag')
            rag.RagPublisher=lambda _:SimpleNamespace(records=lambda _:records,render=lambda task,pkt,row:row[1])
            with patch.dict('sys.modules',{'preextract_publish':preextract,'paperflow.rag':rag}):
                write_json(f['body_receipt_path'],receipt)
                verified,_=_body(f['paper'],_ref(f['body_receipt_path']),_ref(prepared),'production',{})
                self.assertTrue(verified['verified'])
                for role in records:
                    with self.subTest(role=role):
                        wrong=copy.deepcopy(receipt);wrong['targets']['081301'][role]['dataset_id']='WRONG-DATASET'
                        write_json(f['body_receipt_path'],wrong)
                        result,errors=_body(f['paper'],_ref(f['body_receipt_path']),_ref(prepared),'production',{})
                        self.assertFalse(result['verified']);self.assertIn('body_target_library_mismatch',errors)

    def test_a_cache_file_used_as_another_caches_proof_must_be_retained(self):
        from paperflow_prefect.nas_handoff import object_fingerprint
        for reverse in (False,True):
            with self.subTest(reverse=reverse),tempfile.TemporaryDirectory() as root:
                f=fixture(root);second=cache_item(f,name='second.txt')
                original_proof=second['producer_record'];producer=read_json(original_proof['path'])
                first_path=Path(second['root'])/'first.txt';write_json(first_path,producer)
                first={k:v for k,v in second.items() if k!='producer_record'}
                first.update(path=str(first_path),bytes=first_path.stat().st_size,sha256=sha_file(first_path),
                             object_fingerprint=object_fingerprint(first_path.stat()))
                producer['files']=[first];write_json(original_proof['path'],producer)
                first['producer_record']={'path':original_proof['path'],'sha256':sha_file(original_proof['path'])}
                second['producer_record']={'path':str(first_path),'sha256':sha_file(first_path)}
                items=[first,second];out=build(f,intermediates=items[::-1] if reverse else items)
                self.assertEqual(out['status'],'ready');self.assertEqual(out['intermediates'],[second])
                self.assertEqual(out['retained_intermediates'],[{'path':str(first_path),'reason':'recovery_evidence_retained'}])
                clean={Path(r['path']) for r in out['intermediates']}
                keep={Path(r['path']) for r in out['retained_evidence']}
                self.assertFalse(clean & keep)


if __name__=='__main__':unittest.main()
