"""Server-only Windows-path adapter for the existing folder business scripts.

The Linux service supplies a private per-job Wine prefix and a localhost-only
offline tokenizer. Input and output paths are sealed in the staged manifest.
There is no model request, queue dispatch or RAG upload in this entrypoint.
"""
import json
import os
from pathlib import Path, PureWindowsPath
import socket
import sys


def main():
    if len(sys.argv) != 3:
        raise ValueError('a frozen manifest and result path are required')
    manifest_path, result_path = map(Path, sys.argv[1:3])
    manifest = json.loads(manifest_path.read_text(encoding='utf8'))
    if manifest.get('schema') != 'paperflow-remote-snapshot/1':
        raise ValueError('unexpected server snapshot schema')
    identity, inputs = manifest['identity'], manifest['inputs']
    if inputs['identity'] != identity or inputs['manifest_sha256'] != manifest['manifest_sha256']:
        raise ValueError('folder input identity changed')
    workspace = Path(manifest['business_workspace_windows'])
    core = Path(manifest['paperflow_root_windows'])
    output_root = Path(manifest['output_root_windows'])
    if (not workspace.is_absolute() or not core.is_absolute() or not output_root.is_absolute()
            or str(core) != manifest['paperflow_root_windows']
            or PureWindowsPath(str(core)).drive != 'F:'
            or PureWindowsPath(str(workspace)).drive != 'F:'
            or PureWindowsPath(str(output_root)).drive != 'F:'):
        raise ValueError('server business paths are not the frozen Windows paths')
    server_code = workspace / 'candidates/postprocess-server-20261101'
    # Load only the pinned bootstrap module before resolving any business path.
    sys.path.insert(0, str(server_code))

    # Business scripts may use the offline token counter only. They never need
    # a model endpoint or RAG connection during postprocessing.
    port = int(os.environ['PAPERFLOW_COUNTER_PORT'])
    connect, connect_ex = socket.socket.connect, socket.socket.connect_ex
    def allowed_connect(sock, address):
        if address != ('127.0.0.1', port):
            raise RuntimeError('postprocessing network access is not permitted')
        return connect(sock, address)
    def allowed_connect_ex(sock, address):
        if address != ('127.0.0.1', port):
            raise RuntimeError('postprocessing network access is not permitted')
        return connect_ex(sock, address)
    socket.socket.connect, socket.socket.connect_ex = allowed_connect, allowed_connect_ex

    from server_wine_pathfix import install as install_long_path_verifier
    install_long_path_verifier(port=port, secret=os.environ['PAPERFLOW_COUNTER_SECRET'],
                               snapshot_sha256=os.environ['PAPERFLOW_SNAPSHOT_SHA256'])
    core = core.resolve(strict=True)
    if str(core) != manifest['paperflow_root_windows']:
        raise ValueError('server business core does not match its frozen path')
    release = core.parent
    program = workspace.parent / '程序'
    candidate = workspace / 'candidates/postprocess-prefect-20261031'
    prefect = workspace / 'prefect-paperflow'
    os.environ['PAPERFLOW_SELECTED_CORE'] = str(core)
    os.environ['PAPER_UPSTREAM_DATA_DIR'] = str(output_root / '_private_runtime')
    sys.path[:0] = [str(server_code), str(candidate), str(prefect), str(prefect / 'paperflow_prefect/clean_overlay'),
                    str(release / 'desktop'), str(program / 'desktop'), str(core)]

    from server_folder_executor import process_verified_folder
    from tokenizer_proxy import CounterProxy
    counter = CounterProxy(port, os.environ['PAPERFLOW_COUNTER_SECRET'],
        json.loads(os.environ['PAPERFLOW_COUNTER_IDENTITY']),
        model_budget=int(os.environ['PAPERFLOW_COUNTER_MODEL_BUDGET']),
        cl_limit=int(os.environ['PAPERFLOW_COUNTER_CL_LIMIT']))
    try:
        result = process_verified_folder(
            load_inputs=lambda: json.loads(manifest_path.read_text(encoding='utf8'))['inputs'],
            identity=identity, generation=manifest['generation'], manifest_sha256=manifest['manifest_sha256'],
            claim_id=manifest['claim_id'], catalog=manifest['catalog'], paperflow_root=core,
            counter=counter, output_root=output_root)
        with result_path.open('x', encoding='utf8') as out:
            json.dump(result, out, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)
            out.flush()
            os.fsync(out.fileno())
    finally:
        counter.close()


if __name__ == '__main__':
    main()
