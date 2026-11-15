"""Run real business adapters on synthetic evidence, inside the server only."""
import json
import os
from pathlib import Path
import socket
import sys


port_text = os.environ.get('PAPERFLOW_COUNTER_PORT')
local_port = int(port_text) if port_text else None
original_connect = socket.socket.connect
original_connect_ex = socket.socket.connect_ex


def guarded_connect(sock, address):
    if local_port is None or address != ('127.0.0.1', local_port):
        raise AssertionError('synthetic postprocessing may only reach its local tokenizer')
    return original_connect(sock, address)


def guarded_connect_ex(sock, address):
    if local_port is None or address != ('127.0.0.1', local_port):
        raise AssertionError('synthetic postprocessing may only reach its local tokenizer')
    return original_connect_ex(sock, address)


socket.socket.connect = guarded_connect
socket.socket.connect_ex = guarded_connect_ex

workspace = os.environ.get('PAPERFLOW_PROBE_WORKSPACE')
if workspace:
    WORKSPACE = Path(workspace)
    ROOT = WORKSPACE / 'candidates/postprocess-server-20261101'
    BASE = WORKSPACE.parent / '程序'
    RELEASE = BASE / 'updates/unified-prefect-20261029-material-r02'
    PREFECT = WORKSPACE / 'prefect-paperflow'
    CANDIDATE = WORKSPACE / 'candidates/postprocess-prefect-20261031'
else:
    ROOT = Path(r'C:\paperflow')
    RELEASE = ROOT / 'release'
    BASE = ROOT / 'base'
    PREFECT = ROOT / 'prefect'
    CANDIDATE = ROOT / 'candidate'
os.environ['PAPERFLOW_SELECTED_CORE'] = str(RELEASE / 'core')
os.environ['PAPER_UPSTREAM_DATA_DIR'] = str(ROOT / 'synthetic-data')
sys.path[:0] = [str(ROOT), str(CANDIDATE), str(PREFECT),
                str(PREFECT / 'paperflow_prefect/clean_overlay'),
                str(RELEASE / 'desktop'), str(BASE / 'desktop'), str(RELEASE / 'core')]

from body_adapter import process_body
from generation_attachment_adapter import process_attachment
from folder_processor import _attachment_evidence, _package_evidence


class SyntheticCounter:
    """This probe checks wiring and artifacts, not the production token budget."""
    identity = {'fixture': 'character-count-only'}
    model_budget = 2000
    cl_limit = 2038

    def count_many(self, texts):
        return [{'model_tokens': len(t), 'cl100k_tokens': len(t)} for t in texts]


def main():
    value = json.loads(Path(sys.argv[1]).read_text(encoding='utf8'))
    inputs, catalog = value['inputs'], value['catalog']
    identity = inputs['identity']
    output = Path(sys.argv[2])
    if output.exists():
        raise ValueError('synthetic output already exists')
    if local_port is None:
        counter = SyntheticCounter()
    else:
        from tokenizer_proxy import CounterProxy
        counter = CounterProxy(local_port, os.environ['PAPERFLOW_COUNTER_SECRET'],
                               json.loads(os.environ['PAPERFLOW_COUNTER_IDENTITY']),
                               model_budget=int(os.environ['PAPERFLOW_COUNTER_MODEL_BUDGET']),
                               cl_limit=int(os.environ['PAPERFLOW_COUNTER_CL_LIMIT']))
    result = process_body(inputs['body'], output / 'body', expected_identity=identity,
                          catalog=catalog, paperflow_root=RELEASE / 'core', counter=counter)
    body_files = _package_evidence(result, output / 'body', identity)
    attachments = []
    for item in inputs['attachments']:
        folder = output / 'attachment' / item['attachment_id']
        card = process_attachment(identity, item, folder)
        evidence, delivery = _attachment_evidence(card, folder, item, identity)
        attachments.append({'status': card['status'], 'evidence_count': len(evidence),
                            'attachment_id': item['attachment_id'], 'md_sha256': delivery['md_sha256']})
    print(json.dumps({'body_status': result['status'], 'body_parts': len(inputs['body']['responses']),
                      'body_evidence_count': len(body_files), 'body_manifest_sha256': result['manifest_sha256'],
                      'body_exact_reassembly': result['validation']['body_exact_reassembly'],
                      'attachments': attachments, 'python_socket_guard': 'local_tokenizer_only',
                      'counter_mode': 'offline_server' if local_port else 'synthetic'}, ensure_ascii=True))


if __name__ == '__main__':
    main()
