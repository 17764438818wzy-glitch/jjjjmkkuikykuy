"""Opt-in integration check using generated files and a deterministic model reply."""
import io
import json
from pathlib import Path
import sys
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from paperflow_prefect.legacy import bind
from paperflow_prefect.settings import Settings, PROJECT
from paperflow_prefect.records import read_json, write_json, sha_file
from paperflow_prefect.body import prepare_body, extract_profile, package_body


def main():
    settings = Settings.load(); bind(settings)
    from docx import Document
    from fixture_profile import _candidate
    from preextract_request import request_once
    from preextract_images import restore_request_images
    from paperflow_prefect.attachments import identify_attachment
    from paperflow_prefect.delivery import admit_descriptions
    from paperflow_prefect.archive import archive_originals
    from paperflow_prefect.intake import inspect_person
    import uuid
    root = PROJECT / 'runtime/fixture-checks' / uuid.uuid4().hex
    person = root / 'library/school/fixture'
    (person / '正文').mkdir(parents=True); (person / '附件').mkdir()
    source = person / '正文/fixture.docx'
    document = Document()
    document.add_heading('1 排程研究', 1)
    for text in ('本研究比较两种排程方案，目标是减少作业等待时间。',
                 '材料为三个作业的到达时刻和持续时间。先按两种规则排序，再记录各作业等待时间，比较平均等待时间。',
                 '比较得到两组等待时间。本例仅说明比较过程，新问题需要重新采集作业数据。'):
        document.add_paragraph(text)
    document.save(source)
    (person / '附件/sort.py').write_text('def sort_jobs(jobs):\n    return sorted(jobs)\n', encoding='utf8')
    catalog = {'schema':'specialty-routing/1.0', 'majors':[
        dict(code='080901', name='计算机科学与技术', discipline_code='08', discipline='工学',
             group_code='0809', group='计算机类', paper_dataset_id='fake-paper', image_dataset_id='fake-images')]}
    write_json(root / 'catalog.json', catalog)
    work = root / 'body'
    env = {'template':settings.path('template_path'), 'catalog':catalog}
    connection = {'model':'fixture-model', 'base_url':'http://127.0.0.1:9/v1', 'api_key':'fixture-only',
        'max_tokens':10000, 'max_output_tokens':10000, 'context_window':1000000,
        'supports_images':True, 'max_images':600, 'max_image_bytes':24*1024*1024,
        'max_single_image_bytes':6*1024*1024, 'max_request_bytes':48*1024*1024}
    calls = []
    class Response(io.BytesIO): headers = {'Content-Type':'application/json'}
    def invoke(body, cfg, directory, **kw):
        calls.append(str(directory))
        if Path(directory).parent.name == 'attachment':
            candidate = {'kind':'Python程序源码', 'summary':'对输入作业进行排序。',
                         'evidence_refs':['text-preview'], 'limitations':[]}
        else:
            original = read_json(work / 'material.json')
            bundle = restore_request_images(original, work, body)
            candidate = _candidate(env, bundle)
        raw = json.dumps({'choices':[{'message':{'content':json.dumps(candidate,ensure_ascii=False)},
                                     'finish_reason':'stop'}]},ensure_ascii=False).encode()
        class LocalReply:
            def open(self, *a, **k): return Response(raw)
        return request_once(body, cfg, directory, opener=LocalReply(), **kw)
    def forbidden(*a, **k): raise AssertionError('Fixture must not use network HTTP')
    with patch('urllib.request.OpenerDirector.open', forbidden):
        prepare_body(source, work, root / 'catalog.json')
        profile = extract_profile(work, settings.path('template_path'), catalog, connection, invoke)
        extract_profile(work, settings.path('template_path'), catalog, connection, invoke)
        if len(calls) != 1: raise AssertionError('cached body repeated model invocation')
        prepared = package_body(work, read_json(settings.path('runtime_config')), catalog,
                                settings.path('release_root') / 'core')
        again = package_body(work, read_json(settings.path('runtime_config')), catalog,
                             settings.path('release_root') / 'core')
        if again['task'] != prepared['task']: raise AssertionError('publication identity changed')
        paper = inspect_person(person, root / 'library')
        description = identify_attachment(paper['attachments'][0], paper['paper_id'], root / 'attachment', connection, invoke)
        admission = admit_descriptions([description], read_json(settings.path('runtime_config')),
                                       settings.path('release_root') / 'core')
        archived = archive_originals(paper, root / 'archive', root / 'nas', reserve_bytes=0)
    result = {'fixture':True, 'real_papers':0, 'real_model_requests':0, 'rag_writes':0, 'real_nas_writes':0,
        'fixture_reply_count':len(calls), 'profile_validated':True,
        'package_path':prepared['package_path'], 'manifest_sha256':prepared['validation']['manifest_sha256'],
        'exact_embedding':prepared['validation']['embedding'], 'attachment_admission':admission,
        'nas_fixture_status':archived['status'], 'originals_retained':all(sha_file(r['path'])==r['sha256'] for r in paper['archive_files']),
        'root':str(root)}
    write_json(PROJECT / 'runtime/native-fixture-receipt.json', result)
    print(json.dumps({k:v for k,v in result.items() if k not in {'exact_embedding','attachment_admission'}}, ensure_ascii=False, indent=2))


if __name__ == '__main__': main()
