"""Synthetic Linux server receipt test, using no live paper or service."""
from pathlib import Path, PureWindowsPath
import importlib

import pytest

from test_server_receipt_inspect import case, hashed, inspect


@pytest.mark.parametrize('has_diagnostic', [False, True])
def test_inspector_accepts_body_with_explicit_failed_attachment(tmp_path, has_diagnostic):
    assert Path(importlib.import_module('server_receipt_inspect').__file__).parent == Path(__file__).parent
    args, save = case(tmp_path / 'snapshot')
    receipt, inputs = args['receipt'], args['manifest']['inputs']
    item = inputs['attachments'][0]
    source = args['manifest']['files'][2]
    folder = PureWindowsPath(args['output_root_windows']) / hashed(args['job']['identity'])
    directory = folder / 'attachments' / hashed(item['attachment_id'])
    diagnostic = save(directory / 'private/attachment-recovery/format-1/receipt.json',
                      {'code': 'attachment_format_error'}) if has_diagnostic else None
    failure = {'attachment_id': item['attachment_id'], 'status': 'failed',
        'code': 'attachment_format_error', 'message': '附件返回格式错误',
        'source_path': source['path'], 'source_sha256': source['sha256'], 'source_name': 'archive.zip',
        'raw_response': item['model_refs']['response'], 'diagnostic': diagnostic}
    result = {'schema': 'attachment-postprocess-result/1', 'status': 'failed',
        'ticket': args['job']['identity'], 'code': failure['code'], 'message': failure['message'],
        'diagnostic': diagnostic, 'raw_response': failure['raw_response'], 'model_requests_added': 0,
        'binding': {'attachment_id': item['attachment_id'], 'ticket': args['job']['identity'],
                    'handoff': item, 'source_path': source['path'], 'source_sha256': source['sha256']}}
    result_ref = save(directory / 'receipt.json', result)
    receipt['attachments'] = []
    receipt['failed_attachments'] = [failure]
    receipt['artifacts'] = receipt['artifacts'][:4] + [result_ref, {'path': source['path'], 'sha256': source['sha256']}]
    if diagnostic:
        receipt['artifacts'].append(diagnostic)
    mapped = inspect(args)
    assert mapped['body_package']
    assert mapped['attachments'] == []
    assert mapped['failed_attachments'] == [failure]
