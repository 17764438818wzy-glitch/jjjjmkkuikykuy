"""Synthetic full-folder probe with the real server-side offline counter."""
import json
import os
from pathlib import Path
import sys
from uuid import uuid4

# Configures the pinned original source paths and blocks non-tokenizer sockets.
import server_synthetic_probe
from server_folder_executor import process_verified_folder
from tokenizer_proxy import CounterProxy


def main():
    input_file, output_root = Path(sys.argv[1]), Path(sys.argv[2])

    def load_inputs():
        return json.loads(input_file.read_text(encoding='utf8'))['inputs']

    value = json.loads(input_file.read_text(encoding='utf8'))
    inputs = value['inputs']
    counter = CounterProxy(int(os.environ['PAPERFLOW_COUNTER_PORT']), os.environ['PAPERFLOW_COUNTER_SECRET'],
        json.loads(os.environ['PAPERFLOW_COUNTER_IDENTITY']),
        model_budget=int(os.environ['PAPERFLOW_COUNTER_MODEL_BUDGET']),
        cl_limit=int(os.environ['PAPERFLOW_COUNTER_CL_LIMIT']))
    result = process_verified_folder(load_inputs=load_inputs, identity=inputs['identity'], generation=1,
        manifest_sha256=inputs['manifest_sha256'], claim_id=str(uuid4()), catalog=value['catalog'],
        paperflow_root=server_synthetic_probe.RELEASE / 'core', counter=counter, output_root=output_root)
    receipt = result['receipt']
    with (output_root / 'server-receipt.json').open('x', encoding='utf8') as stream:
        json.dump(result, stream, ensure_ascii=False, indent=2, allow_nan=False)
    print(json.dumps({'server_validation': result['server_validation'],
        'receipt_sha256': result['receipt_sha256'], 'body_parts': len(inputs['body']['responses']),
        'attachments': len(receipt['attachments']), 'body_manifest_sha256': receipt['body']['manifest_sha256'],
        'folder_artifacts': len(receipt['artifacts']), 'counter_mode': 'offline_server'}, ensure_ascii=True))


if __name__ == '__main__':
    main()
