"""Native completed outputs remain reusable across fixed Python hash seeds.

The parent test starts only this same file as a synthetic worker. Workers reuse
complete_fixture/FixtureCounter, deny network/Popen and use NoCounter for reuse.
All material, package and logs are in pytest's tmp_path, never business storage.
"""
from datetime import datetime
from pathlib import Path
from unittest.mock import patch
import argparse
import asyncio
import hashlib
import httpx
import importlib
import importlib.util
import json
import os
import platform
import psutil
import subprocess
import sys
import time
import traceback

sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
read=lambda p:json.loads(Path(p).read_bytes())

def save(path,value):
    with path.open('x',encoding='utf-8',newline='\n') as stream:
        json.dump(value,stream,ensure_ascii=False,indent=2);stream.write('\n');stream.flush();os.fsync(stream.fileno())


def deny(*a,**k):
    raise AssertionError('Synthetic seed worker forbids Popen/network/real counter and completed-output writes')


def inventory(path):
    return {str(p.relative_to(path)):sha(p) for p in path.rglob('*') if p.is_file()}


def diff(a,b,path=''):
    if type(a) is not type(b):return [{'path':path,'kind':'type'}]
    if isinstance(a,dict):
        rows=[]
        for key in sorted(set(a)|set(b)):
            sub=path+'/'+str(key).replace('~','~0').replace('/','~1')
            if key not in a or key not in b:rows.append({'path':sub,'kind':'key_presence'})
            else:rows.extend(diff(a[key],b[key],sub))
        return rows
    if isinstance(a,list):
        rows=[]
        if len(a)!=len(b):rows.append({'path':path,'kind':'list_length'})
        for i,(x,y) in enumerate(zip(a,b)):rows.extend(diff(x,y,path+'/'+str(i)))
        return rows
    return [] if a==b else [{'path':path,'kind':'scalar','expected':a,'stored':b}]


class NoCounter:
    calls=0
    def count_many(self,*a,**k):
        self.calls+=1
        return deny()



def _worker(args):
    ROOT=args.audit_root.resolve()
    CORE=args.core_root.resolve()
    NATIVE=args.native_root.resolve()
    adapter_path=args.adapter_file.resolve()
    case=args.case.resolve()
    target=args.result.resolve()
    contract=args.contract.resolve()
    assert CORE==NATIVE.parents[1]/'prefect-paperflow'
    assert (NATIVE/'paperflow_v3').is_dir() and (CORE/'config.json').is_file()
    assert case.is_relative_to(ROOT) and target.is_relative_to(ROOT) and not target.exists()
    assert os.environ['PYTHONHASHSEED'] in {'1','2','3','4','5'}
    started=time.monotonic()
    sys.path[:0]=[str(NATIVE),str(CORE),str(NATIVE/'tests/stages'),
                  str(CORE.parent/'candidates/postprocess-prefect-20261031/tests')]
    os.environ['PAPERFLOW_V3_PROJECT_CONFIG']=str(CORE/'config.json')
    report={'at':datetime.now().astimezone().isoformat(),'mode':args.mode,'hashseed':os.environ['PYTHONHASHSEED'],
            'status':'not_started','actual_counter_calls':0,'model_calls':0,'network_calls':0,'business_writes':0,
            'worker_process_launches':0,'contract_file':str(contract),'contract_sha256':sha(contract)}
    with patch('subprocess.Popen',deny),patch('socket.socket.connect',deny),patch('socket.socket.connect_ex',deny),patch('socket.create_connection',deny):
        from paperflow_prefect import legacy
        from paperflow_prefect.settings import Settings
        # A local fixture data root only; no business data root is used.
        settings=Settings(dict(Settings.load(CORE/'config.json').values,data_root=str(case/'fixture-data-root')))
        legacy.bind(settings)
        def load(name,path):
            spec=importlib.util.spec_from_file_location(name,path)
            module=importlib.util.module_from_spec(spec);sys.modules[name]=module;spec.loader.exec_module(module)
            return module
        selected_contract=load('preextract_contract',contract)
        for name in ['preextract_partition_run','preextract_images','preextract_publish']:
            load(name,CORE/'paperflow_prefect/clean_overlay'/(name+'.py'))
        import paperflow_v3.stages as stages
        adapter=load('paperflow_v3.stages.body_adapter',adapter_path)
        stages.body_adapter=adapter;sys.modules['body_adapter']=adapter
        from test_profile_completion_guard import complete_fixture
        from test_body_adapter import FixtureCounter,RELEASE
        from paperflow.package import validate_package
        assert Path(importlib.import_module('preextract_contract').__file__).resolve()==contract
        report['adapter_sha256']=sha(adapter_path)
        report['actual_modules']={name:{'file':str(Path(sys.modules[name].__file__).resolve()),
            'sha256':sha(sys.modules[name].__file__)} for name in ['preextract_contract','preextract_partition_run',
            'preextract_images','preextract_publish','paperflow_v3.stages.body_adapter','paperflow.package']}
        output=case/'output'
        if args.mode=='create':
            assert not case.exists();case.mkdir()
            original_blank=selected_contract._blank
            def fixture_source_fields(shape):
                value=original_blank(shape)
                if isinstance(shape,dict) and {'source_id','source_sha256','reading_scope'} <= set(shape):
                    # The same extra field names and types as the real837 bundle,
                    # with this synthetic source's own SHA. No source is substituted.
                    source=case/'fixture/upstream/body.txt'
                    value['parsed_view']={'basis':'synthetic fixture same-source reading view',
                                          'format':'txt','sha256':sha(source)}
                    value['conversion_sha256']=None
                return value
            with patch.object(selected_contract,'_blank',fixture_source_fields):
                evidence,catalog=complete_fixture(case/'fixture',parts=2)
            save(case/'evidence.json',evidence);save(case/'catalog.json',catalog)
            material=read(evidence['reading_material']['path'])
            assert material['source']['source_sha256']==sha(evidence['source']['path'])
            assert material['source']['parsed_view']['sha256']==material['source']['source_sha256']
            save(case/'fixture-mutation.json',{'only_data_injection':'Two extra source metadata fields added while existing complete_fixture builds its complete original request/response/hash plan; no validator or guard patched',
                'parsed_view':material['source']['parsed_view'],'conversion_sha256':None,
                'same_source_sha256':material['source']['source_sha256'],
                'ranking':read(evidence['whole_paper_ranking']['response']['path']),
                'fixture_upstream_byte_sha256':inventory(case/'fixture/upstream')})
            class CountingFixture(FixtureCounter):
                calls=0
                def count_many(self,*a,**k):
                    self.calls+=1
                    return super().count_many(*a,**k)
            fake=CountingFixture()
            result=adapter.process_body(evidence,output,expected_identity=evidence['identity'],catalog=catalog,
                                        paperflow_root=RELEASE/'core',counter=fake)
            before=inventory(output);no_counter=NoCounter()
            with patch.object(adapter,'_save',deny):
                reused=adapter.process_body(evidence,output,expected_identity=evidence['identity'],catalog=catalog,
                                            paperflow_root=RELEASE/'core',counter=no_counter)
            assert reused==result and inventory(output)==before and no_counter.calls==0
            packet=validate_package(result['package_path'],result['task'],catalog,strict_seal=True)
            assert packet['manifest_sha256']==result['manifest_sha256'] and result['validation']['body_exact_reassembly']
            report.update(status='publication_created_same_process_reuse_passed',fake_character_counter_calls=fake.calls,
                same_process_reuse_counter_calls=0,learning_units=result['validation']['learning_units'],
                result_input_fingerprint=result['input_fingerprint'],manifest_sha256=result['manifest_sha256'],
                all_output_files_sha256=before,source_and_original_fixture_sha256=inventory(case/'fixture/upstream'),
                input_payload_sha256=sha(case/'evidence.json'),catalog_sha256=sha(case/'catalog.json'),
                whole_ranking_valid=True,source_identity_and_hash_match=True)
            assert result['validation']['learning_units']==2
        else:
            evidence,catalog=read(case/'evidence.json'),read(case/'catalog.json')
            before=inventory(output);upstream=inventory(case/'fixture/upstream');counter=NoCounter()
            try:
                with patch.object(adapter,'_save',deny):
                    result=adapter.process_body(evidence,output,expected_identity=evidence['identity'],catalog=catalog,
                                                paperflow_root=RELEASE/'core',counter=counter)
                assert result==read(output/'body-result.json')
                report.update(status='completed_reuse_passed',manifest_sha256=result['manifest_sha256'])
            except ValueError as exc:
                if str(exc)!='private_artifact_not_derived_from_input':raise
                trace=exc.__traceback__;frame=None
                while trace:
                    if trace.tb_frame.f_code is adapter.process_body.__code__:frame=dict(trace.tb_frame.f_locals)
                    trace=trace.tb_next
                assert frame
                stored_path=Path(frame['reference']['path']);expected_raw=frame['raw'];stored_raw=stored_path.read_bytes()
                expected,stored=json.loads(expected_raw),json.loads(stored_raw)
                issues=diff(expected,stored)
                canonical=lambda rows:sorted(json.dumps(v,ensure_ascii=False,sort_keys=True) for v in rows)
                warnings_only=all(v['path'].startswith('/warnings/') for v in issues)
                same_multiset=canonical(expected['warnings'])==canonical(stored['warnings'])
                report.update(status='native_private_byte_guard_rejected',reason=str(exc),private_key=frame['key'],
                    stored_private_sha256=sha(stored_path),reference_sha256=frame['reference']['sha256'],
                    derived_private_sha256=hashlib.sha256(expected_raw).hexdigest(),
                    semantic_difference_count=len(issues),differences=issues,
                    differences_only_warning_order=warnings_only and same_multiset,
                    expected_warning_sequence=expected['warnings'],stored_warning_sequence=stored['warnings'])
            assert counter.calls==0 and inventory(output)==before and inventory(case/'fixture/upstream')==upstream
            report.update(all_existing_output_bytes_unchanged=True,all_fixture_upstream_bytes_unchanged=True,
                completed_reuse_counter_calls=0,input_payload_sha256=sha(case/'evidence.json'),catalog_sha256=sha(case/'catalog.json'))
        report['elapsed_seconds']=time.monotonic()-started
    save(target,report)
    print(json.dumps({'mode':args.mode,'seed':os.environ['PYTHONHASHSEED'],'status':report['status'],
        'private_key':report.get('private_key'),'difference_paths':[v['path'] for v in report.get('differences',[])],
        'seconds':round(report['elapsed_seconds'],3),'counter_or_model_processes':0},ensure_ascii=True),flush=True)


def _active_sources():
    import paperflow_v3
    from paperflow_v3.stages import body_adapter
    import preextract_contract
    locations=sorted({Path(p).resolve().parent for p in paperflow_v3.__path__
                      if (Path(p)/'stages/body_adapter.py').is_file()})
    assert len(locations)==1, 'The native namespace package must resolve to one checkout'
    native=locations[0]
    core=native.parents[1]/'prefect-paperflow'
    return native,core,Path(body_adapter.__file__).resolve(),Path(preextract_contract.__file__).resolve()


def test_completed_output_diagnostics_are_stable_across_python_hash_seeds(tmp_path):
    native,core,adapter,contract=_active_sources()
    this_file=Path(__file__).resolve()
    case=tmp_path/'case'
    results=[]
    for index,seed in enumerate((1,2,3,4,5)):
        mode='create' if index==0 else 'reuse'
        stem='seed-'+str(seed)+'-'+mode
        result_path=tmp_path/(stem+'.json')
        command=[sys.executable,'-X','utf8','-B',str(this_file),'--diagnostic-order-worker',
            '--mode',mode,'--audit-root',str(tmp_path),'--case',str(case),'--result',str(result_path),
            '--core-root',str(core),'--native-root',str(native),
            '--adapter-file',str(adapter),'--contract',str(contract)]
        env=dict(os.environ,PYTHONHASHSEED=str(seed),PYTHONDONTWRITEBYTECODE='1')
        real_popen=subprocess.Popen
        def only_fixed_worker(argv,*a,**k):
            assert list(argv)==command and not k.get('shell',False), 'Only the fixed Python test worker may start'
            return real_popen(argv,*a,**k)
        with (tmp_path/(stem+'.stdout.txt')).open('xb') as stdout, (tmp_path/(stem+'.stderr.txt')).open('xb') as stderr:
            with patch('subprocess.Popen',only_fixed_worker):
                done=subprocess.run(command,stdout=stdout,stderr=stderr,env=env,timeout=60)
        assert done.returncode==0, (seed,(tmp_path/(stem+'.stderr.txt')).read_text(encoding='utf-8',errors='replace')[-1200:])
        report=read(result_path)
        assert report['actual_modules']['preextract_contract']['file']==str(contract)
        assert report['contract_sha256']==sha(contract) and report['adapter_sha256']==sha(adapter)
        assert report['actual_counter_calls']==report['model_calls']==report['network_calls']==report['business_writes']==0
        if index==0:
            assert report['status']=='publication_created_same_process_reuse_passed'
            assert report['learning_units']==2 and report['same_process_reuse_counter_calls']==0
            assert report['whole_ranking_valid'] and report['source_identity_and_hash_match']
            warning_paths={w['path'] for w in read(case/'output/private/validation.json')['warnings']
                           if w['code']=='learning_limit_unknown_field'}
            assert {'/source/parsed_view','/source/conversion_sha256'} <= warning_paths
        else:
            assert report['all_existing_output_bytes_unchanged'] and report['all_fixture_upstream_bytes_unchanged']
            assert report['completed_reuse_counter_calls']==0
        results.append(report)
    assert len({(r['input_payload_sha256'],r['catalog_sha256']) for r in results})==1
    assert all(r['status']=='completed_reuse_passed' for r in results[1:]), [
        {'seed':r['hashseed'],'status':r['status'],'private_key':r.get('private_key'),
         'difference_paths':[d['path'] for d in r.get('differences',[])]} for r in results[1:]]


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--diagnostic-order-worker',action='store_true',required=True)
    parser.add_argument('--mode',choices=['create','reuse'],required=True)
    for name in ['audit-root','case','result','core-root','native-root','adapter-file','contract']:
        parser.add_argument('--'+name,type=Path,required=True)
    _worker(parser.parse_args())
