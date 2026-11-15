"""Synthetic-only tests: never read live papers or invoke a model/service."""
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

CANDIDATE = Path(__file__).resolve().parents[1]
PROJECT = CANDIDATE.parents[1]
sys.path.insert(0, str(PROJECT / 'prefect-paperflow'))
# Reuse the retained business parser; do not run legacy.bind or load credentials.
CONFIG = json.loads((PROJECT / 'prefect-paperflow/config.json').read_text('utf8'))
RELEASE = Path(CONFIG['release_root'])
sys.path[:0] = [str(RELEASE / 'desktop'), str(Path(CONFIG['program_root']) / 'desktop'),
               str(RELEASE / 'core')]
from paperflow_prefect.records import identity, sha_file, write_json, read_json
from paperflow_prefect.intake_handoff import receive_handoff
from paperflow_prefect.receipts import ReceiptStore
from paperflow_prefect.attachments import _upstream_file_facts, _request_body
from preextract_scheduler_adapter import _request_hash


def adapter():
    path = CANDIDATE / 'attachment_adapter.py'
    if not path.exists():
        raise AssertionError('Missing deterministic attachment adapter')
    spec = importlib.util.spec_from_file_location('candidate_attachment_adapter', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def fixture(root, *, rich=False, delivered=False):
    """A real H2.1 receipt containing only temporary synthetic material."""
    library = root / 'library'; person = library / 'person'; person.mkdir(parents=True)
    gid = identity(['source-group/1', str(library).casefold(), 'person'])[:32]
    def row(relative, data):
        path = person / relative; path.parent.mkdir(parents=True, exist_ok=True); path.write_bytes(data)
        return dict(file_id=identity([gid, relative.casefold()])[:24], path=str(path),
                    relative=relative, bytes=len(data), sha256=sha_file(path))
    body = row('正文/original.docx', b'synthetic body')
    attachment = row('附件/code.zip', b'synthetic archive')
    rows = sorted([body, attachment], key=lambda r: r['relative'].casefold())
    h = dict(schema='paperflow-group-handoff/2', group_id=gid, library_root=str(library),
        group_root=str(person), group_relative='person', handoff_id=identity(['group-handoff/1', gid,
            [(r['relative'], r['bytes'], r['sha256']) for r in rows]])[:32],
        paper_id=None, existing_paper_id=None, body=body, attachments=[attachment], trash=[], unresolved=[],
        paper_title='Synthetic', paper_title_candidate=None, classification_complete=True,
        ready_for_receipt=True, attachment_status='有附件')
    h['result_version'] = identity(h)[:32]
    hp = root / 'handoff.json'; write_json(hp, h)
    evidence_paths = []
    if rich:
        from PIL import Image
        image_path = root / 'synthetic.png'; Image.new('RGB', (2, 2), 'white').save(image_path)
        origin = dict(kind='current_read', record_ref=None, run_ref=None, entry_ref=None, record_sha256=None)
        scopes = {key: dict(status='available', reason=None) for key in ('directory', 'text', 'images', 'errors')}
        scopes['request_records'] = dict(status='unknown', reason='not supplied')
        record = dict(schema='attachment-evidence-handoff/1', group_id=gid,
            source=dict(handoff_id=h['handoff_id'], result_version=h['result_version'],
                file_id=attachment['file_id'], source_sha256=attachment['sha256']),
            evidence_version=1, previous_evidence_version=None, read_scope=scopes,
            archive_listing=dict(evidence_id='directory-1', status='complete', total_entries=1, listed_entries=1,
                entries=[dict(path='main.py', type='file', bytes=12)], reason='synthetic listing', provenance=origin),
            text_samples=[dict(evidence_id='text-1', text='synthetic README',
                location=dict(member_path='README.txt', page=None, line=1, region=None),
                extraction_method='bounded_text', truncated=False, provenance=origin)],
            images=[dict(evidence_id='image-1', path=str(image_path), sha256=sha_file(image_path),
                mime_type='image/png', width=2, height=2, location=dict(member_path='plot.png', page=None, line=None, region=None),
                extraction_method='rendered_page', provenance=origin)],
            decision_basis=dict(classification='attachment', provenance=origin),
            limitations=['未执行代码'], errors=[], existing_identification=None, request_records=[])
        key = identity([gid, h['handoff_id'], h['result_version'], attachment['file_id'], attachment['sha256']])[:32]
        ep = root / 'evidence' / key / '1.json'; write_json(ep, record); evidence_paths = [ep]
    if delivered:
        from paperflow_prefect.group_delivery import package_result
        packaged = package_result(dict(handoff=str(hp), evidence_handoffs=[str(p) for p in evidence_paths]), root / 'copies')
        receipt = receive_handoff(store=ReceiptStore(root / 'ledger'), delivery_dir=packaged['delivery_dir'])
    else:
        receipt = receive_handoff(hp, ReceiptStore(root / 'ledger'), evidence_records=evidence_paths)
    rp = root / 'receipt.json'; write_json(rp, receipt)
    att = receipt['inventory']['attachments'][0]
    work = root / 'upstream'; model = work / 'model'; model.mkdir(parents=True)
    if rich:
        from paperflow_prefect.attachment_input import load_received_evidence, model_evidence
        record, upstream, report = load_received_evidence(att)
        evidence = model_evidence(record, upstream, report=report)
    else:
        evidence = _upstream_file_facts(att)
    evidence['model_input_version'] = 1
    write_json(work / 'evidence.json', evidence)
    request = _request_body(evidence, dict(model='synthetic-model', max_tokens=2048))
    write_json(model / 'request.json', request)
    response = dict(kind='压缩包', summary='仅有文件信息，未读取包内内容。',
                    evidence_refs=['directory-1', 'text-1'] if rich else ['file-facts'], limitations=['未读取压缩包成员'])
    raw = json.dumps(response, ensure_ascii=False)
    (model / 'response.txt').write_bytes(raw.encode('utf8'))
    state = dict(status='response_complete', finish_reason='stop', request_sha256=_request_hash(request),
        stored_request_sha256=sha_file(model / 'request.json'), response_sha256=sha_file(model / 'response.txt'),
        http_invoked=True, client_http_requests=1)
    write_json(model / 'request-state.json', state)
    ticket = dict(task_id='task_1', group_id=gid, paper_id=receipt['paper_id'], receipt_id=receipt['receipt_id'],
                  attempt_id='attempt_1', parent_run_id='parent_1', task_run_id='folder_flow_1')
    handoff = dict(schema='attachment-postprocess-input/1', ticket=copy.deepcopy(ticket),
        receipt_ref=dict(path=str(rp), sha256=sha_file(rp)), attachment_id=att['file_id'], work_dir=str(work),
        model_refs={})
    refresh(handoff)
    return ticket, handoff, att, response


def refresh(handoff):
    work = Path(handoff['work_dir'])
    handoff['model_refs'] = {name: dict(path=str(work / relative), sha256=sha_file(work / relative))
        for name, relative in dict(evidence='evidence.json', request='model/request.json',
            request_state='model/request-state.json', response='model/response.txt').items()}


class AttachmentAdapterTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.ticket, self.handoff, self.row, self.response = fixture(self.root)
        self.output = self.root / 'candidate-output'

    def run_adapter(self):
        return adapter().process_attachment(self.ticket, self.handoff, self.output)

    def save_response(self, raw):
        work = Path(self.handoff['work_dir']); (work / 'model/response.txt').write_bytes(raw.encode('utf8'))
        state = read_json(work / 'model/request-state.json')
        state['response_sha256'] = sha_file(work / 'model/response.txt')
        write_json(work / 'model/request-state.json', state); refresh(self.handoff)

    def test_complete_reply_produces_bound_card_without_network_or_source_mutation(self):
        before = {p: sha_file(p) for p in self.root.rglob('*') if p.is_file()}
        with patch('socket.socket.connect', side_effect=AssertionError('network forbidden')), \
             patch('paperflow_prefect.attachments.identify_attachment', side_effect=AssertionError('dispatch forbidden')):
            result = self.run_adapter()
        self.assertEqual(result['status'], 'complete')
        self.assertEqual(result['ticket'], self.ticket)
        self.assertEqual(result['model_requests_added'], 0)
        self.assertEqual(result['delivery']['source_path'], self.row['path'])
        self.assertNotIn('named_proof_path', result['delivery'])
        self.assertEqual(result['privacy_redaction'], 'not_performed')
        self.assertEqual(result['raw_response'], self.handoff['model_refs']['response'])
        md = Path(result['delivery']['md_path']).read_text('utf8')
        self.assertIn('not_read', md); self.assertIn('未读取压缩包成员', md)
        self.assertEqual({p: sha_file(p) for p in before}, before)

    def test_wrong_evidence_reference_rejected_before_card(self):
        self.response['evidence_refs'] = ['another-attachment-page']
        self.save_response(json.dumps(self.response, ensure_ascii=False))
        with self.assertRaises(ValueError): self.run_adapter()
        self.assertFalse(list(self.output.rglob('attachment.md')))

    def test_changed_source_rejected(self):
        Path(self.row['path']).write_bytes(b'changed source')
        with self.assertRaises(ValueError): self.run_adapter()

    def test_wrong_folder_or_paper_ticket_rejected(self):
        for key in self.ticket:
            with self.subTest(key=key):
                saved = self.ticket[key]; self.ticket[key] = 'wrong'
                with self.assertRaises(ValueError): self.run_adapter()
                self.ticket[key] = saved

    def test_unknown_outcome_never_adopts_or_resends(self):
        work = Path(self.handoff['work_dir']); state = read_json(work / 'model/request-state.json')
        state.update(status='request_failed_or_uncertain', error_type='ReadTimeout')
        state.pop('response_sha256'); write_json(work / 'model/request-state.json', state); refresh(self.handoff)
        with self.assertRaises(ValueError): self.run_adapter()
        self.assertFalse((work / 'model/adopted-response.json').exists())

    def test_changed_request_state_or_raw_hash_rejected(self):
        for name in ('request', 'request_state', 'response', 'evidence'):
            with self.subTest(name=name):
                path = Path(self.handoff['model_refs'][name]['path']); before = path.read_bytes()
                path.write_bytes(before + b' ')
                with self.assertRaises(ValueError): self.run_adapter()
                path.write_bytes(before)

    def test_retry_reuses_receipt_and_card_but_changed_reply_cannot_overwrite(self):
        first = self.run_adapter()
        before = {p: sha_file(p) for p in self.output.rglob('*') if p.is_file()}
        self.assertEqual(first, self.run_adapter())
        self.assertEqual({p: sha_file(p) for p in before}, before)
        self.response['summary'] = '不同的回答'
        self.save_response(json.dumps(self.response, ensure_ascii=False))
        with self.assertRaises(ValueError): self.run_adapter()
        self.assertEqual({p: sha_file(p) for p in before}, before)

    def test_md_tamper_even_with_rehashed_receipt_is_rejected_by_renderer_binding(self):
        result = self.run_adapter()
        md = Path(result['delivery']['md_path']); md.write_bytes(md.read_bytes().replace('压缩包'.encode(), '资料包'.encode()))
        # Updating a hash alone must not forge a deterministic renderer version.
        result['delivery']['md_sha256'] = sha_file(md)
        result['render']['md_sha256'] = sha_file(md)
        result['artifacts']['md']['sha256'] = sha_file(md)
        delivery_path = md.parent / 'delivery.json'
        write_json(delivery_path, result['render'])
        result['artifacts']['delivery']['sha256'] = sha_file(delivery_path)
        write_json(self.output / 'receipt.json', result)
        with self.assertRaises(ValueError): self.run_adapter()

    def test_version_tamper_rejected_not_just_valid_hex(self):
        result = self.run_adapter(); result['delivery']['delivery_version'] = 'b' * 64
        write_json(self.output / 'receipt.json', result)
        with self.assertRaises(ValueError): self.run_adapter()

    def test_syntax_repair_is_local_and_original_is_preserved(self):
        raw = json.dumps(self.response, ensure_ascii=False)[:-1] + ',}'
        self.save_response(raw)
        result = self.run_adapter()
        self.assertEqual(Path(result['raw_response']['path']).read_text('utf8'), raw)
        self.assertTrue(list(self.output.glob('private/attachment-recovery/format-*/receipt.json')))

    def test_invalid_json_keeps_diagnostic_without_publication(self):
        self.save_response('{"kind":"bad "quote""}')
        with self.assertRaises(ValueError): self.run_adapter()
        self.assertFalse((self.output / 'receipt.json').exists())
        self.assertTrue(list(self.output.glob('private/attachment-recovery/format-*/receipt.json')))

    def test_unregistered_output_cannot_be_adopted(self):
        self.output.mkdir(); (self.output / 'attachment.md').write_text('unregistered card', 'utf8')
        with self.assertRaises(ValueError): self.run_adapter()

    def test_evidence_payload_not_derived_from_receipt_is_rejected(self):
        work = Path(self.handoff['work_dir']); evidence = read_json(work / 'evidence.json')
        evidence['refs'] = ['invented-ref']; evidence['text'] = 'unsupported contents'
        write_json(work / 'evidence.json', evidence)
        request = _request_body(evidence, dict(model='synthetic-model', max_tokens=2048))
        write_json(work / 'model/request.json', request)
        state = read_json(work / 'model/request-state.json'); state.update(request_sha256=_request_hash(request),
            stored_request_sha256=sha_file(work / 'model/request.json'))
        write_json(work / 'model/request-state.json', state); refresh(self.handoff)
        with self.assertRaises(ValueError): self.run_adapter()

    def test_delivered_copy_is_used_in_place_even_when_original_is_unavailable(self):
        root = self.root / 'delivered'; root.mkdir()
        self.ticket, self.handoff, self.row, self.response = fixture(root, delivered=True, rich=True)
        Path(self.row['source_path']).unlink()
        result = self.run_adapter()
        self.assertEqual(result['delivery']['source_path'], self.row['path'])
        self.assertNotEqual(result['delivery']['source_path'], self.row['source_path'])
        Path(self.row['path']).write_bytes(b'damaged delivered file')
        with self.assertRaises(ValueError): self.run_adapter()

    def test_verified_listing_retained_and_unverified_images_not_claimed_as_read(self):
        root = self.root / 'rich'; root.mkdir()
        self.ticket, self.handoff, self.row, self.response = fixture(root, rich=True)
        result = self.run_adapter()
        md = Path(result['delivery']['md_path']).read_text('utf8')
        self.assertIn('| main.py | file | 12 |', md)
        self.assertIn('目录完整不表示成员内容已读', md)
        self.assertIn('原识别输入图片 0 张', md)
        self.assertIn('部分图片引用尚未核验', md)
        self.response['evidence_refs'].append('image-1')
        self.save_response(json.dumps(self.response, ensure_ascii=False))
        with self.assertRaises(ValueError): self.run_adapter()

    def test_rehashed_request_cannot_replace_the_actual_model_image(self):
        root = self.root / 'rich'; root.mkdir()
        self.ticket, self.handoff, self.row, self.response = fixture(root, rich=True)
        work = Path(self.handoff['work_dir']); request = read_json(work / 'model/request.json')
        request['messages'][1]['content'].append(dict(type='image_url', image_url=dict(url='data:image/png;base64,YmFk')))
        write_json(work / 'model/request.json', request)
        state = read_json(work / 'model/request-state.json'); state.update(request_sha256=_request_hash(request),
            stored_request_sha256=sha_file(work / 'model/request.json'))
        write_json(work / 'model/request-state.json', state); refresh(self.handoff)
        with self.assertRaises(ValueError): self.run_adapter()

    def test_missing_retained_artifact_is_not_repaired_as_completed_output(self):
        self.run_adapter(); (self.output / 'card-evidence.json').unlink()
        with self.assertRaises(ValueError): self.run_adapter()

    def test_output_cannot_be_inside_delivered_input_directory(self):
        root = self.root / 'delivered'; root.mkdir()
        self.ticket, self.handoff, self.row, self.response = fixture(root, delivered=True, rich=True)
        self.output = Path(self.row['path']).parent / 'new-output'
        with self.assertRaises(ValueError): self.run_adapter()

    def test_missing_artifacts_map_cannot_forge_completed_receipt(self):
        result = self.run_adapter(); result['artifacts'] = {}
        (self.output / 'card-evidence.json').write_text('{}', 'utf8')
        write_json(self.output / 'receipt.json', result)
        with self.assertRaises(ValueError): self.run_adapter()


if __name__ == '__main__': unittest.main()
