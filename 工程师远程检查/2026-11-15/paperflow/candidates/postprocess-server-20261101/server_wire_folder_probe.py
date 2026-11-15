"""Run one synthetic SSH-staged folder with its original folder claim."""
import json
import os
from pathlib import Path
import sys

import server_synthetic_probe
from server_folder_executor import process_verified_folder
from tokenizer_proxy import CounterProxy


def main():
    input_file, output_root, result_file = map(Path, sys.argv[1:4])
    manifest = json.loads(input_file.read_text(encoding='utf8'))
    if manifest.get('schema') != 'paperflow-remote-snapshot/1':
        raise ValueError('unexpected synthetic snapshot')
    inputs = manifest['inputs']
    if (inputs['identity'] != manifest['identity']
            or inputs['manifest_sha256'] != manifest['manifest_sha256']):
        raise ValueError('synthetic snapshot identity mismatch')
    core = (server_synthetic_probe.RELEASE / 'core').resolve(strict=True)
    if str(core) != manifest['paperflow_root_windows']:
        raise ValueError('synthetic business core identity mismatch')
    counter = CounterProxy(int(os.environ['PAPERFLOW_COUNTER_PORT']), os.environ['PAPERFLOW_COUNTER_SECRET'],
        json.loads(os.environ['PAPERFLOW_COUNTER_IDENTITY']),
        model_budget=int(os.environ['PAPERFLOW_COUNTER_MODEL_BUDGET']),
        cl_limit=int(os.environ['PAPERFLOW_COUNTER_CL_LIMIT']))
    result = process_verified_folder(load_inputs=lambda: json.loads(input_file.read_text(encoding='utf8'))['inputs'],
        identity=manifest['identity'], generation=manifest['generation'],
        manifest_sha256=manifest['manifest_sha256'], claim_id=manifest['claim_id'], catalog=manifest['catalog'],
        paperflow_root=core, counter=counter, output_root=output_root)
    with result_file.open('x', encoding='utf8') as out:
        json.dump(result, out, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)
        out.flush()
        os.fsync(out.fileno())
    print(json.dumps({'server_validation': result['server_validation'],
                      'receipt_sha256': result['receipt_sha256'],
                      'body_parts': len(inputs['body']['responses']),
                      'attachments': len(result['receipt']['attachments'])}))


if __name__ == '__main__':
    main()
