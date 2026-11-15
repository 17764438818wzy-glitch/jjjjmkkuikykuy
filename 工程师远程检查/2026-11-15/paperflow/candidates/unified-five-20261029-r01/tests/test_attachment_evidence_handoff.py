import copy
import hashlib
import json
import subprocess
import sys
import tempfile
import threading
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

from paperflow_prefect import group_classification as groups

try:
    from paperflow_prefect import attachment_evidence_handoff as evidence
except ImportError:
    evidence = None


class FixtureJudge:
    def choose_body(self, records):
        return {'choice': next((row['file_id'] for row in records if row['relative'] == '论文.docx'), None)}

    def choose_other(self, record, body):
        return {'choice': 'attachment', 'model': 'fixture', 'confidence': 0.8,
                'probabilities': {'attachment': 0.8, 'trash': 0.1, 'uncertain': 0.1}}


def scope(status, reason=None):
    return {'status': status, 'reason': reason}


def current_origin():
    return {'kind': 'current_read', 'record_ref': None, 'run_ref': None,
            'entry_ref': None, 'record_sha256': None}


class EvidenceHandoffTests(unittest.TestCase):
    def setUp(self):
        self.assertIsNotNone(evidence, '缺少附件证据交接模块')
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.library = self.root / 'library'
        self.library.mkdir()
        self.group = self.library / 'school' / 'person'
        self.group.mkdir(parents=True)
        (self.group / '论文.docx').write_bytes(b'body')
        (self.group / '资料.zip').write_bytes(b'archive')
        self.handoff = groups.classify_group(self.group, self.library, FixtureJudge(),
                                            lambda path: {'text': 'preview'})
        row = self.handoff['attachments'][0]
        self.record = {
            'schema': 'attachment-evidence-handoff/1', 'group_id': self.handoff['group_id'],
            'source': {'handoff_id': self.handoff['handoff_id'],
                       'result_version': self.handoff['result_version'],
                       'file_id': row['file_id'], 'source_sha256': row['sha256']},
            'evidence_version': 1, 'previous_evidence_version': None,
            'read_scope': {'directory': scope('not_read'), 'text': scope('not_read'),
                           'images': scope('not_read'),
                           'request_records': scope('unknown', '历史状态未知'),
                           'errors': scope('unknown', '没有保存错误记录')},
            'archive_listing': {'evidence_id': None, 'status': 'not_read',
                                'total_entries': None, 'listed_entries': 0,
                                'entries': [], 'reason': '目录尚未读取',
                                'provenance': None},
            'text_samples': [], 'images': [],
            'decision_basis': {'classification': 'attachment', 'reason': row['reason'],
                               'provenance': current_origin()},
            'limitations': [], 'errors': [], 'existing_identification': None,
            'request_records': [],
        }

    def test_shape_only_is_not_a_verified_group_binding(self):
        report = evidence.validate_evidence_handoff(self.record)
        self.assertTrue(report['valid'])
        self.assertFalse(report['binding_verified'])
        self.assertFalse(report['source_verified'])
        with self.assertRaises(ValueError):
            evidence.validate_evidence_handoff(self.record, verify_files=True)

    def test_bound_record_checks_exact_group_membership_and_source(self):
        report = evidence.validate_evidence_handoff(self.record, self.handoff, verify_files=True)
        self.assertTrue(report['binding_verified'])
        self.assertTrue(report['source_verified'])
        other_group = self.library / 'school' / 'other'
        other_group.mkdir()
        (other_group / '论文.docx').write_bytes(b'body')
        (other_group / '资料.zip').write_bytes(b'archive')
        other = groups.classify_group(other_group, self.library, FixtureJudge(),
                                      lambda path: {'text': 'preview'})
        with self.assertRaises(ValueError):
            evidence.validate_evidence_handoff(self.record, other)
        forged = copy.deepcopy(self.record)
        forged['source']['file_id'] = self.handoff['body']['file_id']
        forged['source']['source_sha256'] = self.handoff['body']['sha256']
        with self.assertRaises(ValueError):
            evidence.validate_evidence_handoff(forged, self.handoff)

    def test_source_change_is_detected_without_checking_other_group_files(self):
        (self.group / '资料.zip').write_bytes(b'changed')
        with self.assertRaises(ValueError):
            evidence.validate_evidence_handoff(self.record, self.handoff, verify_files=True)

    def test_missing_or_malformed_source_identity_is_not_formal_evidence(self):
        for key, value in [('group_id', ''), ('evidence_version', 0)]:
            bad = copy.deepcopy(self.record)
            bad[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                evidence.validate_evidence_handoff(bad, self.handoff)
        for value in ('', 'not-a-sha', None):
            bad = copy.deepcopy(self.record)
            bad['source']['source_sha256'] = value
            with self.subTest(sha=value), self.assertRaises(ValueError):
                evidence.validate_evidence_handoff(bad, self.handoff)
        bad = copy.deepcopy(self.record)
        bad['source']['source_sha256'] = bad['source']['source_sha256'].upper()
        with self.assertRaises(ValueError):
            evidence.validate_evidence_handoff(bad, self.handoff)

    def test_unresolved_other_item_does_not_invalidate_a_confirmed_attachment(self):
        (self.group / 'unknown.pdf').write_bytes(b'unreadable')
        def reader(path):
            if path.name == 'unknown.pdf': raise ValueError('unreadable')
            return {'text': 'preview'}
        unresolved = groups.classify_group(self.group, self.library, FixtureJudge(), reader)
        record = copy.deepcopy(self.record)
        record['source']['handoff_id'] = unresolved['handoff_id']
        record['source']['result_version'] = unresolved['result_version']
        report = evidence.validate_evidence_handoff(record, unresolved, verify_files=True)
        self.assertTrue(report['source_verified'])

    def test_unknown_type_and_total_stay_unknown_in_a_partial_directory(self):
        record = copy.deepcopy(self.record)
        record['read_scope']['directory'] = scope('partial', '历史目录只有名称和大小')
        record['archive_listing'] = {'evidence_id': 'directory-1', 'status': 'partial',
            'total_entries': None, 'listed_entries': 1,
            'entries': [{'path': 'src/item', 'type': None, 'bytes': None}],
            'reason': '原记录未留存类型和总数', 'provenance': current_origin()}
        evidence.validate_evidence_handoff(record, self.handoff)
        record['archive_listing']['status'] = 'complete'
        record['read_scope']['directory'] = scope('available')
        with self.assertRaises(ValueError):
            evidence.validate_evidence_handoff(record, self.handoff)

    def test_complete_directory_can_have_partial_metadata_scope(self):
        record = copy.deepcopy(self.record)
        record['read_scope']['directory'] = scope('partial', '完整目录中的一个成员大小未知')
        record['archive_listing'] = {'evidence_id': 'directory-1', 'status': 'complete',
            'total_entries': 1, 'listed_entries': 1,
            'entries': [{'path': 'src/item', 'type': 'file', 'bytes': None}],
            'reason': None, 'provenance': current_origin()}
        self.assertTrue(evidence.validate_evidence_handoff(record, self.handoff)['valid'])

    def test_unknown_history_requires_reason_and_cannot_be_not_read_by_inference(self):
        bad = copy.deepcopy(self.record)
        bad['read_scope']['request_records'] = scope('unknown')
        with self.assertRaises(ValueError):
            evidence.validate_evidence_handoff(bad, self.handoff)
        evidence.validate_evidence_handoff(self.record, self.handoff)

    def test_historical_evidence_requires_an_actual_origin_reference(self):
        record = copy.deepcopy(self.record)
        record['text_samples'] = [{'evidence_id': 'text-1', 'text': '原始片段',
            'location': {'member_path': 'README.txt', 'page': None, 'line': None, 'region': None},
            'extraction_method': 'old_preview', 'truncated': None,
            'provenance': {'kind': 'historical_reuse', 'record_ref': None, 'run_ref': None,
                           'entry_ref': None, 'record_sha256': None}}]
        record['read_scope']['text'] = scope('partial', '未留存截断信息')
        with self.assertRaises(ValueError):
            evidence.validate_evidence_handoff(record, self.handoff)
        record['text_samples'][0]['provenance']['record_ref'] = 'existing/evidence.jsonl'
        self.assertTrue(evidence.validate_evidence_handoff(record, self.handoff)['valid'])

    def test_image_count_is_not_an_image_and_missing_file_is_not_verified(self):
        record = copy.deepcopy(self.record)
        record['images'] = [{'evidence_id': 'image-1', 'path': str(self.root / 'missing.png'),
            'sha256': hashlib.sha256(b'image').hexdigest(), 'mime_type': 'image/png',
            'width': 20, 'height': 10,
            'location': {'member_path': None, 'page': 1, 'line': None, 'region': None},
            'extraction_method': 'rendered_page', 'provenance': current_origin()}]
        record['read_scope']['images'] = scope('partial', '旧预览图片不可用')
        report = evidence.validate_evidence_handoff(record, self.handoff, verify_files=True)
        self.assertFalse(report['images'][0]['verified'])

    def test_evidence_versions_are_immutable_and_retry_is_idempotent(self):
        target = self.root / 'handoffs'
        first = evidence.write_evidence_handoff(self.record, target, handoff=self.handoff)
        saved = first.read_bytes()
        self.assertEqual(evidence.write_evidence_handoff(self.record, target, handoff=self.handoff), first)
        altered = copy.deepcopy(self.record)
        altered['limitations'] = ['补充读取限制']
        with self.assertRaises(ValueError):
            evidence.write_evidence_handoff(altered, target, handoff=self.handoff)
        self.assertEqual(first.read_bytes(), saved)
        altered['evidence_version'] = 2
        altered['previous_evidence_version'] = 1
        second = evidence.write_evidence_handoff(altered, target, handoff=self.handoff)
        self.assertNotEqual(first, second)
        self.assertEqual(first.read_bytes(), saved)
        history = evidence.read_evidence_history(second, handoff=self.handoff, verify_files=True)
        self.assertEqual([item['evidence_version'] for item in history], [1, 2])
        first.unlink()
        with self.assertRaises(ValueError):
            evidence.read_evidence_history(second, handoff=self.handoff)
        with self.assertRaises(ValueError):
            evidence.append_evidence_handoff(altered, target, handoff=self.handoff)
        changed_again = copy.deepcopy(altered)
        changed_again['evidence_version'] = 3
        changed_again['previous_evidence_version'] = 2
        changed_again['limitations'] = ['另一项限制']
        with self.assertRaises(ValueError):
            evidence.write_evidence_handoff(changed_again, target, handoff=self.handoff)
        with self.assertRaises(ValueError):
            evidence.write_evidence_handoff(self.record, self.library / 'not-allowed', handoff=self.handoff)

    def test_image_file_and_unrelated_record_do_not_prove_source_binding(self):
        from PIL import Image
        image_path = self.root / 'preview.png'
        Image.new('RGB', (20, 10), 'white').save(image_path)
        image_sha = hashlib.sha256(image_path.read_bytes()).hexdigest()
        origin_path = self.root / 'origin.json'
        origin_path.write_text(json.dumps({'source_sha256': '0' * 64, 'images': []}), encoding='utf8')
        origin = {'kind': 'historical_reuse', 'record_ref': str(origin_path), 'run_ref': None,
                  'entry_ref': 'image-1', 'record_sha256': hashlib.sha256(origin_path.read_bytes()).hexdigest()}
        record = copy.deepcopy(self.record)
        image = {'evidence_id': 'image-1', 'path': str(image_path), 'sha256': image_sha,
                 'mime_type': 'image/png', 'width': 20, 'height': 10,
                 'location': {'member_path': None, 'page': 1, 'line': None, 'region': None},
                 'extraction_method': 'rendered_page', 'provenance': origin}
        record['images'] = [image]
        record['read_scope']['images'] = scope('available')
        report = evidence.validate_evidence_handoff(record, self.handoff, verify_files=True)
        self.assertFalse(report['images'][0]['verified'])

        # A separately saved source record must bind this image and position to this exact source.
        origin_data = {'source_path': self.handoff['attachments'][0]['path'],
                       'source_sha256': record['source']['source_sha256'],
                       'images': [{k: v for k, v in image.items() if k != 'provenance'}]}
        origin_path.write_text(json.dumps(origin_data), encoding='utf8')
        origin['record_sha256'] = hashlib.sha256(origin_path.read_bytes()).hexdigest()
        report = evidence.validate_evidence_handoff(record, self.handoff, verify_files=True)
        self.assertFalse(report['images'][0]['verified'])
        trusted = {str(origin_path): origin['record_sha256']}
        report = evidence.validate_evidence_handoff(record, self.handoff, verify_files=True,
                                                    trusted_image_records=trusted)
        self.assertTrue(report['images'][0]['verified'])
        origin_data['source_path'] = str(self.root / 'different-person.zip')
        origin_path.write_text(json.dumps(origin_data), encoding='utf8')
        origin['record_sha256'] = hashlib.sha256(origin_path.read_bytes()).hexdigest()
        self.assertFalse(evidence.validate_evidence_handoff(record, self.handoff, verify_files=True,
            trusted_image_records={str(origin_path): origin['record_sha256']})['images'][0]['verified'])

    def test_batch_saves_original_attachment_evidence_without_new_judgments(self):
        from paperflow_prefect.group_evidence import read_evidence
        archive_path = self.group / '资料.zip'
        with zipfile.ZipFile(archive_path, 'w') as archive:
            archive.writestr('src/', b'')
            archive.writestr('src/main.py', b'print(1)')
        class CountingJudge(FixtureJudge):
            count = 0
            def choose_other(self, record, body):
                self.count += 1
                return super().choose_other(record, body)
        judge = CountingJudge()
        reader = lambda p: read_evidence(p) if p.suffix == '.zip' else {'text': 'body'}
        result = groups.classify_batch([self.group], self.library, self.root / 'out',
                                       lambda: judge, reader, verified_new_groups=[self.group])[0]
        self.assertEqual(judge.count, 1)
        handoff = json.loads(Path(result['handoff']).read_text(encoding='utf8'))
        self.assertEqual(len(result['evidence_handoffs']), 1)
        record = json.loads(Path(result['evidence_handoffs'][0]).read_text(encoding='utf8'))
        self.assertEqual(record['source']['file_id'], handoff['attachments'][0]['file_id'])
        self.assertEqual(record['source']['source_sha256'], handoff['attachments'][0]['sha256'])
        self.assertEqual(record['archive_listing']['status'], 'complete')
        self.assertEqual(record['archive_listing']['total_entries'], 2)
        self.assertEqual(record['decision_basis']['decision']['confidence'], 0.8)
        report = evidence.validate_evidence_handoff(record, handoff, verify_files=True)
        self.assertTrue(report['source_verified'])
        self.assertFalse(report['evidence_items'][0]['verified'])
        sidecar = Path(result['evidence_handoffs'][0])
        report = evidence.validate_evidence_handoff(record, handoff, verify_files=True,
            trusted_sidecar={'path': str(sidecar), 'sha256': hashlib.sha256(sidecar.read_bytes()).hexdigest()})
        self.assertTrue(report['evidence_items'][0]['verified'])

    def test_evidence_save_failure_does_not_erase_main_or_other_attachment(self):
        (self.group / '另一个.zip').write_bytes(b'other archive')
        real = evidence.write_evidence_handoff
        def fail_once(record, directory, *, handoff):
            if record['file_info']['name'] == '资料.zip': raise ValueError('fixture error')
            return real(record, directory, handoff=handoff)
        with patch.object(evidence, 'write_evidence_handoff', side_effect=fail_once):
            result = groups.classify_batch([self.group], self.library, self.root / 'out',
                                           FixtureJudge, lambda p: {'text': 'preview'},
                                           verified_new_groups=[self.group])[0]
        self.assertTrue(Path(result['handoff']).is_file())
        self.assertEqual(len(result['evidence_handoffs']), 1)
        self.assertEqual(len(result['evidence_errors']), 1)

    def test_legacy_archive_preserves_unknown_type_and_origin(self):
        row = self.handoff['attachments'][0]
        origin = {'kind': 'historical_reuse', 'record_ref': 'old/evidence.jsonl',
                  'run_ref': 'old', 'entry_ref': 'S01-F01', 'record_sha256': None}
        preview = {'archive_type': 'zip', 'members': [{'name': 'src/main.py', 'size': '12'}],
                   'limitations': ['旧记录为有界目录']}
        record = evidence.make_evidence_handoff(self.handoff, row, preview=preview, provenance=origin)
        self.assertEqual(record['archive_listing']['status'], 'partial')
        self.assertIsNone(record['archive_listing']['total_entries'])
        self.assertIsNone(record['archive_listing']['entries'][0]['type'])
        self.assertEqual(record['archive_listing']['provenance'], origin)
        self.assertTrue(evidence.validate_evidence_handoff(record, self.handoff)['valid'])

    def test_saved_legacy_text_keeps_exact_words_and_unknown_truncation(self):
        row = self.handoff['attachments'][0]
        origin = {'kind': 'historical_reuse', 'record_ref': 'old/evidence.jsonl',
                  'run_ref': 'old', 'entry_ref': 'S01-F01', 'record_sha256': None}
        preview = {'format': 'pdf', 'sections': [{'location': '第3页', 'text': '旧记录的原文片段'}]}
        record = evidence.make_evidence_handoff(self.handoff, row, preview=preview, provenance=origin)
        self.assertEqual(record['text_samples'][0]['text'], '旧记录的原文片段')
        self.assertIsNone(record['text_samples'][0]['truncated'])
        self.assertEqual(record['text_samples'][0]['location']['page'], 3)
        self.assertEqual(record['text_samples'][0]['provenance'], origin)

    def test_historical_text_needs_trusted_record_and_exact_original_binding(self):
        row = self.handoff['attachments'][0]
        origin_path = self.root / 'old.jsonl'
        preview = {'sections': [{'location': '第3页', 'text': '实际旧片段'}]}
        old = {'id': 'S01-F01', 'path': str(self.root / 'another-person.zip'),
               'sha256': row['sha256'], 'evidence': preview}
        origin_path.write_text(json.dumps(old, ensure_ascii=False) + '\n', encoding='utf8')
        origin = {'kind': 'historical_reuse', 'record_ref': str(origin_path), 'run_ref': 'old',
                  'entry_ref': old['id'], 'record_sha256': hashlib.sha256(origin_path.read_bytes()).hexdigest()}
        record = evidence.make_evidence_handoff(self.handoff, row, preview=preview, provenance=origin)
        trusted = {str(origin_path): origin['record_sha256']}
        report = evidence.validate_evidence_handoff(record, self.handoff, verify_files=True,
                                                    trusted_evidence_records=trusted)
        self.assertFalse(report['evidence_items'][0]['verified'])
        old['path'] = row['path']
        origin_path.write_text(json.dumps(old, ensure_ascii=False) + '\n', encoding='utf8')
        origin['record_sha256'] = hashlib.sha256(origin_path.read_bytes()).hexdigest()
        record['text_samples'][0]['provenance']['record_sha256'] = origin['record_sha256']
        trusted = {str(origin_path): origin['record_sha256']}
        report = evidence.validate_evidence_handoff(record, self.handoff, verify_files=True,
                                                    trusted_evidence_records=trusted)
        self.assertTrue(report['evidence_items'][0]['verified'])
        record['text_samples'][0]['text'] = '不是旧记录内容'
        report = evidence.validate_evidence_handoff(record, self.handoff, verify_files=True,
                                                    trusted_evidence_records=trusted)
        self.assertFalse(report['evidence_items'][0]['verified'])

    def test_historical_directory_does_not_invent_total_or_duplicate_members(self):
        row = self.handoff['attachments'][0]
        origin_path = self.root / 'archive-history.jsonl'
        preview = {'archive_type': 'zip', 'members': [{'name': 'a.py', 'size': '7'}]}
        old = {'id': 'S01-F01', 'path': row['path'], 'sha256': row['sha256'], 'evidence': preview}
        origin_path.write_text(json.dumps(old) + '\n', encoding='utf8')
        origin = {'kind': 'historical_reuse', 'record_ref': str(origin_path), 'run_ref': 'old',
                  'entry_ref': old['id'], 'record_sha256': hashlib.sha256(origin_path.read_bytes()).hexdigest()}
        record = evidence.make_evidence_handoff(self.handoff, row, preview=preview, provenance=origin)
        trusted = {str(origin_path): origin['record_sha256']}
        def report():
            return evidence.validate_evidence_handoff(record, self.handoff, verify_files=True,
                trusted_evidence_records=trusted)['evidence_items'][0]['verified']
        self.assertTrue(report())
        record['archive_listing']['total_entries'] = 500
        self.assertFalse(report())
        record['archive_listing']['total_entries'] = None
        record['archive_listing']['entries'].append(copy.deepcopy(record['archive_listing']['entries'][0]))
        record['archive_listing']['listed_entries'] = 2
        self.assertFalse(report())

    def test_changed_attachment_during_read_is_unresolved_while_body_is_saved(self):
        def reader(path):
            if path.name == '资料.zip':
                path.write_bytes(b'changed during reading')
            return {'text': 'preview'}
        result = groups.classify_batch([self.group], self.library, self.root / 'out', FixtureJudge, reader,
                                       verified_new_groups=[self.group])[0]
        handoff = json.loads(Path(result['unreceived']).read_text(encoding='utf8'))
        self.assertEqual(handoff['body']['relative'], '论文.docx')
        self.assertEqual(handoff['unresolved'][0]['relative'], '资料.zip')
        self.assertEqual(handoff['attachments'], [])

    def test_mutating_reader_exception_preserves_confirmed_body(self):
        def reader(path):
            if path.name == '资料.zip':
                path.write_bytes(b'changed before error')
                raise ValueError('fixture read error')
            return {'text': 'preview'}
        result = groups.classify_batch([self.group], self.library, self.root / 'out', FixtureJudge, reader,
                                       verified_new_groups=[self.group])[0]
        handoff = json.loads(Path(result['unreceived']).read_text(encoding='utf8'))
        self.assertEqual(handoff['body']['relative'], '论文.docx')
        self.assertEqual(handoff['unresolved'][0]['relative'], '资料.zip')
        self.assertIn('读取失败', handoff['unresolved'][0]['reason'])

    def test_unknown_history_does_not_call_nimble(self):
        def forbidden():
            raise AssertionError('unknown history cannot call model')
        result = groups.classify_batch([self.group], self.library, self.root / 'out', forbidden,
                                       lambda p: {'text': 'preview'})[0]
        self.assertIn('历史', result['error'])
        self.assertNotIn('handoff', result)

    def test_same_saved_result_is_reused_and_pending_intent_never_retries(self):
        output = self.root / 'out'
        first = groups.classify_batch([self.group], self.library, output, FixtureJudge,
            lambda p: {'text': 'preview'}, verified_new_groups=[self.group])[0]
        self.assertTrue(Path(first['handoff']).is_file())
        def forbidden():
            raise AssertionError('saved or unknown request cannot call model')
        second = groups.classify_batch([self.group], self.library, output, forbidden,
                                       lambda p: {'text': 'preview'})[0]
        self.assertEqual(second['handoff'], first['handoff'])
        self.assertTrue(second['reused'])
        self.assertEqual(second['evidence_handoffs'], first['evidence_handoffs'])
        # A separate new group with an interrupted request cannot be resent.
        pending_group = self.library / 'school' / 'pending'
        pending_group.mkdir()
        (pending_group / '论文.docx').write_bytes(b'body')
        class Interrupted:
            def choose_body(self, records): raise RuntimeError('simulated interruption')
        first_pending = groups.classify_batch([pending_group], self.library, output, Interrupted,
            lambda p: {'text': 'preview'}, verified_new_groups=[pending_group])[0]
        self.assertIn('unreceived', first_pending)
        retry = groups.classify_batch([pending_group], self.library, output, forbidden,
            lambda p: {'text': 'preview'}, verified_new_groups=[pending_group])[0]
        self.assertIn('未确认', retry['error'])

    def test_tampered_saved_handoff_does_not_clear_an_unknown_intent(self):
        output = self.root / 'out'
        first = groups.classify_batch([self.group], self.library, output, FixtureJudge,
            lambda p: {'text': 'preview'}, verified_new_groups=[self.group])[0]
        path = Path(first['handoff'])
        saved = json.loads(path.read_text(encoding='utf8'))
        saved['attachments'][0]['file_id'] = 'forged'
        saved['result_version'] = groups._version(saved)
        forged_path = path.with_name(saved['group_id'] + '-' + saved['handoff_id'] + '-' + saved['result_version'] + '.json')
        path.rename(forged_path)
        forged_path.write_text(json.dumps(saved), encoding='utf8')
        def forbidden(): raise AssertionError('tampered history must not call model')
        result = groups.classify_batch([self.group], self.library, output, forbidden,
            lambda p: {'text': 'preview'}, verified_new_groups=[self.group])[0]
        self.assertIn('历史', result['error'])

    def test_receipt_can_add_a_verified_paper_number_without_rerunning(self):
        output = self.root / 'out'
        first = groups.classify_batch([self.group], self.library, output, FixtureJudge,
            lambda p: {'text': 'preview'}, verified_new_groups=[self.group])[0]
        def forbidden(): raise AssertionError('historical handoff must be reused')
        second = groups.classify_batch([self.group], self.library, output, forbidden,
            lambda p: {'text': 'preview'}, known_paper_ids={str(self.group): 'PF_existing'})[0]
        self.assertEqual(second['handoff'], first['handoff'])
        self.assertTrue(second['reused'])

    def test_bad_complete_flags_do_not_make_attachment_only_history_complete(self):
        output = self.root / 'out'
        first = groups.classify_batch([self.group], self.library, output, FixtureJudge,
            lambda p: {'text': 'preview'}, verified_new_groups=[self.group])[0]
        path = Path(first['handoff'])
        saved = json.loads(path.read_text(encoding='utf8'))
        saved['trash'].append(saved['body'])
        saved['body'] = None
        saved['result_version'] = groups._version(saved)
        forged = path.with_name(saved['group_id'] + '-' + saved['handoff_id'] + '-' + saved['result_version'] + '.json')
        path.rename(forged)
        forged.write_text(json.dumps(saved), encoding='utf8')
        def forbidden(): raise AssertionError('invalid history must not call model')
        result = groups.classify_batch([self.group], self.library, output, forbidden,
            lambda p: {'text': 'preview'}, verified_new_groups=[self.group])[0]
        self.assertIn('历史', result['error'])

    def test_same_group_concurrent_batches_use_one_model_request(self):
        entered, release = threading.Event(), threading.Event()
        class SlowJudge(FixtureJudge):
            def choose_body(self, records):
                entered.set()
                if not release.wait(5): raise RuntimeError('fixture timeout')
                return super().choose_body(records)
        first_result = []
        def first():
            first_result.extend(groups.classify_batch([self.group], self.library, self.root / 'out1',
                SlowJudge, lambda p: {'text': 'preview'}, verified_new_groups=[self.group]))
        thread = threading.Thread(target=first, daemon=True)
        thread.start()
        self.assertTrue(entered.wait(5))
        def forbidden(): raise AssertionError('concurrent group must not send')
        try:
            other = groups.classify_batch([self.group], self.group.parent, self.root / 'out2',
                forbidden, lambda p: {'text': 'preview'}, verified_new_groups=[self.group])[0]
            self.assertIn('正在分类', other['error'])
        finally:
            release.set()
            thread.join(5)
        self.assertFalse(thread.is_alive())
        self.assertIn('handoff', first_result[0])

    def test_another_process_holding_group_lock_prevents_a_model_request(self):
        group_id = groups._group_lock_key(self.group.resolve())
        root = Path(tempfile.gettempdir()) / 'paperflow-group-classification-locks'
        root.mkdir(parents=True, exist_ok=True)
        lock_path = root / (group_id + '.lock')
        code = ('import sys; from filelock import FileLock; '
                'lock=FileLock(sys.argv[1]); lock.acquire(); '
                'print("held", flush=True); sys.stdin.readline(); lock.release()')
        proc = subprocess.Popen([sys.executable, '-c', code, str(lock_path)],
                                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            self.assertEqual(proc.stdout.readline().strip(), 'held')
            def forbidden(): raise AssertionError('locked group must not call model')
            result = groups.classify_batch([self.group], self.library, self.root / 'other-output',
                forbidden, lambda p: {'text': 'preview'}, verified_new_groups=[self.group])[0]
            self.assertIn('正在分类', result['error'])
        finally:
            proc.stdin.write('\n'); proc.stdin.flush()
            proc.communicate(timeout=5)


if __name__ == '__main__':
    unittest.main()
