"""Accept the selected source overlay and idle GUI without any external action."""
import sys
sys.dont_write_bytecode=True
import ast
import hashlib
import json
import os
import tempfile
from pathlib import Path

ROOT=Path(__file__).resolve().parent.parent
DEST=ROOT/'_verification'
DEST.mkdir(exist_ok=True)
CHECKS=[]
def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def check(name,value):
    if not value:raise AssertionError(name)
    CHECKS.append(name)

def audit(event,args):
    if event.startswith(('socket.','subprocess.','os.system','os.posix_spawn')):
        raise RuntimeError('Offline acceptance forbids network and child processes')
    candidates=[]
    if event=='open':
        mode,flags=args[1:3]
        if ((isinstance(mode,str) and any(c in mode for c in 'wax+')) or
                (isinstance(flags,int) and flags & (os.O_WRONLY|os.O_RDWR|os.O_CREAT|os.O_TRUNC|os.O_APPEND))):
            candidates.append(args[0])
    elif event in {'os.mkdir','os.remove','os.rmdir','os.chmod','os.utime'}:candidates.append(args[0])
    elif event in {'os.rename','os.replace'}:candidates.extend(args[:2])
    for p in candidates:
        if not isinstance(p,(str,bytes,os.PathLike)):continue
        if os.fsdecode(p).casefold() in {'nul',r'\\.\nul'}:continue
        resolved=str(Path(os.fsdecode(p)).resolve())
        if resolved.startswith('\\\\?\\UNC\\'):resolved='\\\\'+resolved[8:]
        elif resolved.startswith('\\\\?\\'):resolved=resolved[4:]
        if not Path(resolved).is_relative_to(DEST.resolve()):
            raise RuntimeError('Offline acceptance rejected a write outside its isolated directory')

def main():
    baseline=json.loads((ROOT/'SOURCE_MANIFEST.json').read_text(encoding='utf8'))
    check('all_1281_bundled_sources_unchanged',all(sha(ROOT/r['file'])==r['bundled_sha256'] for r in baseline['records']))
    check('all_1281_production_sources_unchanged',all(sha(Path(r['source']))==r['original_sha256'] for r in baseline['records']))
    with tempfile.TemporaryDirectory(prefix='token-idle-',dir=DEST) as temporary:
        scratch=Path(temporary);data=scratch/'data'
        os.environ['PAPER_UPSTREAM_DATA_DIR']=str(data)
        os.environ['PAPERFLOW_LEGACY_APP']=str(ROOT/'desktop')
        sys.path[:0]=[str(ROOT/'scripts'),str(ROOT/'desktop'),str(ROOT/'core')]
        sys.addaudithook(audit)
        from source_update import activate_source_update
        overlay=activate_source_update(ROOT)
        selected=json.loads((ROOT/'config/source-update.json').read_text(encoding='utf8'))
        check('selected_update_hashes_and_baseline_checked',overlay==ROOT/selected['directory']/'desktop')
        for p in overlay.glob('*.py'):ast.parse(p.read_text(encoding='utf8'))
        import preextract_request,preextract_images,preextract_partition_run,preextract_workbench
        check('all_four_overrides_import_selected_version',all(Path(m.__file__).parent==overlay for m in
            (preextract_request,preextract_images,preextract_partition_run,preextract_workbench)))
        from project_paths import DATA_DIR,ROOT as PATH_ROOT,TEMPLATE_PATH
        check('base_project_paths_keep_resources_and_isolated_data',PATH_ROOT==ROOT and DATA_DIR==data.resolve()
              and TEMPLATE_PATH.is_relative_to(ROOT/'resources'))
        from preextract_catalog import compact_catalog,expand_catalog,FIELDS
        catalog=json.loads((ROOT/'desktop/specialty_catalog.json').read_text(encoding='utf8'))
        original=dict(schema=catalog.get('schema'),majors=[{k:r.get(k) for k in FIELDS} for r in catalog['majors']])
        compact=compact_catalog(catalog)
        check('883_specialties_all_six_fields_order_roundtrip',expand_catalog(compact)==original and len(original['majors'])==883)
        old_bytes=len(json.dumps(original,ensure_ascii=False,separators=(',',':')).encode())
        new_bytes=len(json.dumps(compact,ensure_ascii=False,separators=(',',':')).encode())
        check('catalog_encoding_is_smaller_without_losing_fields',new_bytes<old_bytes)
        old_tree=ast.parse((ROOT/'desktop/preextract_request.py').read_text(encoding='utf8'))
        new_tree=ast.parse((overlay/'preextract_request.py').read_text(encoding='utf8'))
        def named(tree,name):
            for node in tree.body:
                if isinstance(node,ast.FunctionDef) and node.name==name:return ast.dump(node)
                if isinstance(node,ast.Assign) and any(isinstance(t,ast.Name) and t.id==name for t in node.targets):
                    return ast.literal_eval(node.value)
        check('review_rules_and_material_projection_exactly_unchanged',named(old_tree,'RULES')==named(new_tree,'RULES')
              and named(old_tree,'material_view')==named(new_tree,'material_view'))
        bundle=dict(source={},blocks=[dict(id='b1',type='paragraph',text='正文、公式、步骤、结果及限制保持完整。')],
                    sections=[],assets=[],issues=[],attachments=[])
        template=TEMPLATE_PATH.read_text(encoding='utf8');cfg=dict(model='offline-fixture',context_window=1000000,max_tokens=4000)
        body,cap=preextract_request.build_request(bundle,template,catalog,cfg,instruction='first')
        other,_=preextract_request.build_request(dict(bundle,blocks=[]),template,catalog,cfg,instruction='second')
        check('prefix_fixed_and_full_template_retained',body['messages'][0]==other['messages'][0]
              and body['messages'][0]['content'].endswith(template))
        check('full_body_and_thinking_and_budget_retained',json.loads(body['messages'][1]['content'][0]['text'])['material']['blocks']==bundle['blocks']
              and body['thinking']=={'type':'enabled'} and body['max_tokens']==4000)
        from app import main as native_main
        native_main(['--smoke-test','--data-dir',str(DATA_DIR)])
        check('selected_source_six_tab_gui_idle',True)
        check('idle_gui_created_no_processing_run',not any((DATA_DIR/'runs').iterdir()))
        report=dict(status='passed',checks=CHECKS,count=len(CHECKS),source_records_checked=len(baseline['records']),
                    catalog_original_bytes=old_bytes,catalog_compact_bytes=new_bytes,
                    catalog_encoding_reduction_percent=round(100*(1-new_bytes/old_bytes),2),
                    network_allowed=False,subprocess_allowed=False,production_writes_allowed=False,ai_called=False,
                    thinking_changed=False,activation='next safe unified launch; current process untouched')
        (DEST/'token-saving-source.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf8')
        print(json.dumps(report,ensure_ascii=False))

if __name__=='__main__':main()
