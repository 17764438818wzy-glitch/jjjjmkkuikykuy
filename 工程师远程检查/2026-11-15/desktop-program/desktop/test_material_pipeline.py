import json
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch
from engine import scan_sources,Store
from auto_pipeline import AutoProcessor


class Agent:
    processing_metrics=[]
    def __init__(self,intake): self.intake=intake;self.calls=[]
    def call(self,task,payload,images=()):
        self.calls.append(task)
        return {'decision':'pass',**self.intake}
    def discard_unaccepted_results(self):pass


class MaterialPipelineTests(unittest.TestCase):
    def test_resume_reuses_bound_intake_before_reading_carrier_again(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);source=root/'code.py';source.write_text('print(1)',encoding='utf8')
            proc=AutoProcessor(Store(root/'store'),{'text_model':'saved','research_profile':{'enabled':True},'auto_upload':False})
            proc.agent=Agent({'intake':'keep','kind':'research_code','route':'text','reason':'实现附件'})
            item={'source_hash':__import__('engine').digest(source)}
            proc._material_screen(source,source,root,item,lambda *x:None)
            with patch('auto_pipeline.read_material',side_effect=AssertionError('re-read image carrier')):
                proc._material_screen(source,source,root,item,lambda *x:None,raw='')
            self.assertEqual(proc.agent.calls,['material_intake'])
    def test_scan_includes_research_assets_and_unreadable_files(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d)
            for name in ('paper.docx','diagram.png','program.py','drawing.dwg','new.asset'):
                (root/name).write_bytes(b'research')
            self.assertEqual({x['name'] for x in scan_sources(root)},
                             {'paper.docx','diagram.png','program.py','drawing.dwg','new.asset'})

    def run_material(self,root,source,intake):
        store=Store(root/'store')
        proc=AutoProcessor(store,{'research_profile':{'enabled':True},'auto_upload':False,
                                  'concurrency':{'enabled':False}})
        proc.agent=Agent(intake)
        manifest=proc.run([{'path':str(source),'name':source.name}],lambda x:None)
        return manifest,proc.agent

    def test_exclusion_happens_before_heavy_extraction(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);source=root/'report.txt';source.write_text('查重报告 总相似比 30%',encoding='utf8')
            with patch('auto_pipeline.build_package',side_effect=AssertionError('heavy work started')):
                manifest,agent=self.run_material(root,source,{'intake':'exclude','kind':'plagiarism_report','route':'none','reason':'属于查重报告'})
            self.assertEqual(agent.calls,['material_intake'])
            self.assertEqual(manifest['items'][0]['reason_kind'],'material_excluded')
            self.assertEqual(source.read_text(encoding='utf8'),'查重报告 总相似比 30%')

    def test_unreadable_pending_is_not_quality_rejection(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);source=root/'drawing.dwg';source.write_bytes(b'AC1032\x00opaque')
            manifest,agent=self.run_material(root,source,{})
            item=manifest['items'][0]
            self.assertEqual(agent.calls,[])
            self.assertEqual(item['status'],'pending')
            self.assertEqual(item['reason_kind'],'material_pending')
            self.assertFalse(item['automatic_approval'])

    def test_archive_keeps_code_and_diagrams_for_child_routing(self):
        from material_intake import extract_material_archive
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);source=root/'mixed.zip'
            with zipfile.ZipFile(source,'w') as z:
                for name in ('paper.txt','report.txt','design.dxf','code/main.py','unknown.binary'):
                    z.writestr(name,b'material')
            rows=extract_material_archive(source,root/'archive')
            self.assertEqual({r['inner_path'] for r in rows},
                             {'paper.txt','report.txt','design.dxf','code/main.py','unknown.binary'})
            self.assertTrue(all(Path(r['extracted_path']).is_relative_to((root/'archive').resolve()) for r in rows))

    def test_unknown_asset_does_not_use_paper_profile_pipeline(self):
        from material_intake import material_pipeline
        item={'material_intake':{'intake':'keep','kind':'software_source','route':'text'}}
        self.assertEqual(material_pipeline(item),'research_material')
        self.assertEqual(material_pipeline({'material_intake':{'intake':'keep','kind':'mixed_document','route':'mixed'}}),'paper')


if __name__=='__main__':unittest.main()
