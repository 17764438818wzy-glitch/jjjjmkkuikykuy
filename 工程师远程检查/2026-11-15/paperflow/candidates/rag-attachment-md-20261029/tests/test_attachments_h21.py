import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from paperflow_prefect.records import Records, read_json, sha_file, write_json
from paperflow_prefect.settings import Settings


class AttachmentHistoryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from paperflow_prefect.legacy import bind
        bind(Settings.load())

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name); self.work = self.root / 'work'
        source = self.root / 'code.zip'; source.write_bytes(b'original attachment')
        self.row = dict(path=str(source), name=source.name, file_id='a1', size=source.stat().st_size,
                        sha256=sha_file(source))
        self.evidence = dict(schema='attachment-evidence/1', source_sha256=self.row['sha256'],
            format='.zip', refs=['archive-list'], text='main.py', images=[], limitations=['只看目录'])
        self.payload = dict(model='old-model', stream=True, max_tokens=2048, messages=[
            dict(role='system', content='fixture'), dict(role='user', content=[
                dict(type='text', text=json.dumps({k: v for k, v in self.evidence.items() if k != 'images'}, ensure_ascii=False))])])
        write_json(self.work / 'evidence.json', self.evidence)
        write_json(self.work / 'model/request.json', self.payload)
        from preextract_scheduler_adapter import _request_hash
        self.state = dict(status='request_failed_or_uncertain', request_sha256=_request_hash(self.payload),
            stored_request_sha256=sha_file(self.work / 'model/request.json'), http_invoked=True,
            client_http_requests=1, error_type='ReadTimeout')
        write_json(self.work / 'model/request-state.json', self.state)

    def no_call(self, *args, **kwargs): self.fail('must not invoke model or load connection')

    def original_hashes(self):
        return {str(p.relative_to(self.work)): sha_file(p) for p in self.work.rglob('*') if p.is_file()}

    def save_response(self, raw):
        target = self.work / 'model/response.txt'; target.write_text(raw, encoding='utf8')
        write_json(self.work / 'model/response-events.jsonl', {'choices': [
            {'message': {'content': raw}, 'finish_reason': 'stop'}]})
        self.state.update(status='response_complete', finish_reason='stop', response_sha256=sha_file(target))
        self.state.pop('error_type', None)
        write_json(self.work / 'model/request-state.json', self.state)

    def test_unknown_blocks_before_evidence_and_connection(self):
        from paperflow_prefect.attachments import identify_attachment, AttachmentError
        before = self.original_hashes()
        with patch('paperflow_prefect.attachments.evidence_for', side_effect=self.no_call):
            with self.assertRaises(AttachmentError) as raised:
                identify_attachment(self.row, 'p1', self.work, self.no_call, self.no_call)
        self.assertEqual(raised.exception.details['code'], 'timeout_unknown')
        for name, digest in before.items(): self.assertEqual(sha_file(self.work / name), digest)

    def test_saved_response_repairs_without_new_request_and_retains_diff(self):
        from paperflow_prefect.attachments import identify_attachment
        self.save_response('{"kind":"源码压缩包","summary":"包含程序文件。","evidence_refs":["archive-list"],"limitations":[],}')
        before = self.original_hashes()
        with patch('paperflow_prefect.attachments.evidence_for', side_effect=self.no_call):
            result = identify_attachment(self.row, 'p1', self.work, self.no_call, self.no_call)
        self.assertEqual(result['kind'], '源码压缩包')
        self.assertTrue(list(self.work.glob('private/attachment-recovery/format-*/changes.diff')))
        for name, digest in before.items(): self.assertEqual(sha_file(self.work / name), digest)

    def test_saved_request_cannot_bind_to_a_different_original(self):
        from paperflow_prefect.attachments import identify_attachment, AttachmentError
        self.save_response('{"kind":"源码","summary":"程序。","evidence_refs":[],"limitations":[]}')
        self.evidence['source_sha256'] = '0' * 64; write_json(self.work / 'evidence.json', self.evidence)
        with self.assertRaises(AttachmentError):
            identify_attachment(self.row, 'p1', self.work, self.no_call, self.no_call)

    def test_ambiguous_response_is_format_error_without_model(self):
        from paperflow_prefect.attachments import identify_attachment, AttachmentError
        self.save_response('{"kind":"before "unescaped" after"}')
        with self.assertRaises(AttachmentError) as raised:
            identify_attachment(self.row, 'p1', self.work, self.no_call, self.no_call)
        self.assertEqual(raised.exception.details['code'], 'attachment_format_error')

    def test_failed_stage_history_and_specific_error_are_retained(self):
        from paperflow_prefect.attachments import AttachmentError
        Records(self.work).failed('identify', AttachmentError('timeout_unknown'))
        original = (self.work / 'stages/identify.json').read_bytes()
        Records(self.work).started('identify')
        histories = list((self.work / 'stages/history/identify').glob('*.json'))
        self.assertTrue(any(p.read_bytes() == original for p in histories))
        self.assertIn(b'timeout_unknown', original)

    def test_saved_success_with_merged_limits_reuses_without_model(self):
        from paperflow_prefect.attachments import identify_attachment
        model_result = dict(kind='源码', summary='代码文件。', evidence_refs=['archive-list'],
                            limitations=['模型限制' + str(n) for n in range(12)])
        self.save_response(json.dumps(model_result, ensure_ascii=False))
        result = identify_attachment(self.row, 'p1', self.work, self.no_call, self.no_call)
        self.assertEqual(len(result['limitations']), 13)
        Records(self.work).complete('identify', result, [self.work / 'description.json'])
        self.assertEqual(result, identify_attachment(self.row, 'p1', self.work, self.no_call, self.no_call))


class AttachmentCardTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        source = self.root / 'code.zip'; source.write_bytes(b'archive')
        self.row = dict(path=str(source), name='code.zip', file_id='a1', size=7, sha256=sha_file(source))
        self.row['source_binding'] = dict(group_id='g1', handoff_id='h1', result_version='r1',
            file_id='upstream-a1', source_sha256=self.row['sha256'])
        self.description = dict(schema='attachment-description/1', paper_id='p1', attachment_id='a1',
            source_sha256=self.row['sha256'], format='.zip', kind='源码压缩包', summary='包含代码。',
            evidence_refs=['directory-1'], limitations=['未运行代码'])
        self.evidence = dict(schema='attachment-evidence-handoff/1', evidence_version=1,
            group_id='g1', source={k: v for k, v in self.row['source_binding'].items() if k != 'group_id'},
            archive_listing=dict(status='partial', total_entries=3, listed_entries=3, reason='历史类型不完整', entries=[
                dict(path='src/', type='directory', bytes=None), dict(path='src/a|b\nc.py', type='file', bytes=5),
                dict(path='unknown', type=None, bytes=2)]), limitations=['目录完整不等于已读内容'],
            read_scope={'directory': {'status': 'partial', 'reason': '类型缺失'}})

    def test_card_filters_unknown_types_keeps_true_total_and_does_not_mutate_evidence(self):
        from paperflow_prefect.attachment_card import render_attachment_card
        before = json.dumps(self.evidence)
        result = render_attachment_card(self.row, 'p1', self.description, self.evidence, self.root / 'cards')
        content = Path(result['path']).read_text(encoding='utf8')
        self.assertIn('总条目数：3', content); self.assertIn('已列条目数：2', content)
        self.assertIn('src/a&#124;b%0Ac.py', content)
        self.assertNotIn('| unknown |', content)
        self.assertIn('paper_id: "p1"', content); self.assertEqual(json.dumps(self.evidence), before)
        self.assertEqual(result, render_attachment_card(self.row, 'p1', self.description, self.evidence, self.root / 'cards'))

    def test_evidence_supplement_creates_new_card_and_preserves_old_description_and_card(self):
        from paperflow_prefect.attachment_card import render_attachment_card
        old_description = json.dumps(self.description)
        first = render_attachment_card(self.row, 'p1', self.description, self.evidence, self.root / 'cards')
        old_bytes = Path(first['path']).read_bytes()
        self.evidence['evidence_version'] = 2; self.evidence['limitations'].append('新增目录证据未用于旧识别')
        second = render_attachment_card(self.row, 'p1', self.description, self.evidence, self.root / 'cards')
        self.assertNotEqual(first['path'], second['path']); self.assertEqual(Path(first['path']).read_bytes(), old_bytes)
        self.assertEqual(json.dumps(self.description), old_description)

    def test_changed_card_or_description_identity_fails_closed(self):
        from paperflow_prefect.attachment_card import render_attachment_card
        first = render_attachment_card(self.row, 'p1', self.description, self.evidence, self.root / 'cards')
        Path(first['path']).write_text('changed', encoding='utf8')
        with self.assertRaises(ValueError):
            render_attachment_card(self.row, 'p1', self.description, self.evidence, self.root / 'cards')

    def test_same_hash_other_group_evidence_cannot_render_on_this_attachment(self):
        from paperflow_prefect.attachment_card import render_attachment_card
        self.evidence['group_id'] = 'other-group'
        with self.assertRaises(ValueError):
            render_attachment_card(self.row, 'p1', self.description, self.evidence, self.root / 'cards')
        self.description['attachment_id'] = 'wrong'
        with self.assertRaises(ValueError):
            render_attachment_card(self.row, 'p1', self.description, self.evidence, self.root / 'cards')


class AttachmentInputBudgetTests(unittest.TestCase):
    def test_large_directory_is_retained_locally_but_cannot_bypass_request_budget(self):
        from paperflow_prefect.attachments import _request_body
        entries=[dict(path=('long-directory/'*16)+str(n)+'.py',type='file',bytes=n) for n in range(2000)]
        evidence=dict(schema='attachment-evidence/1',model_input_version=1,name='code.zip',format='.zip',
            source_sha256='a'*64,text=('long-directory\\quoted\"\n'*2000)[:24000],images=[],
            refs=['archive-list'],bounded=True,limitations=['只看目录'],
            archive_listing=dict(status='complete',total_entries=2000,listed_entries=2000,entries=entries))
        before=json.dumps(evidence,ensure_ascii=False)
        request=_request_body(evidence,dict(model='fixture',max_tokens=2048))
        serialized=request['messages'][1]['content'][0]['text']
        self.assertLessEqual(len(serialized),24000)
        self.assertTrue(json.loads(serialized)['text_truncated'])
        self.assertNotIn('archive_listing',json.loads(serialized))
        self.assertEqual(json.dumps(evidence,ensure_ascii=False),before)

    def test_new_bounded_request_can_be_reconciled_without_changing_retained_evidence(self):
        from paperflow_prefect.attachments import _request_body, _saved_evidence
        with tempfile.TemporaryDirectory() as tmp:
            work=Path(tmp)
            evidence=dict(schema='attachment-evidence/1',model_input_version=1,name='code.zip',format='.zip',
                source_sha256='a'*64,text='main.py',images=[],refs=['archive-list'],bounded=True,
                limitations=[],archive_listing=dict(status='partial',entries=[]))
            write_json(work/'evidence.json',evidence)
            write_json(work/'model/request.json',_request_body(evidence,dict(model='fixture',max_tokens=2048)))
            self.assertEqual(_saved_evidence(dict(sha256='a'*64),work),evidence)


if __name__ == '__main__': unittest.main()
