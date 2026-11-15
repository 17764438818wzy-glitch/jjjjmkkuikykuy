"""Convert independently hashed audit metadata into Linux app source manifests.

This only writes a new local candidate directory and never contacts the server.
An audit snapshot is not proof of the current or future server state.
"""
import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import re


HERE = Path(__file__).resolve().parent
SHA = re.compile(r'[0-9a-f]{64}')


def _path(value):
    if not isinstance(value, str) or not value or '\x00' in value:
        raise ValueError('an absolute Linux path is required')
    path = PurePosixPath(value)
    if not path.is_absolute() or '..' in path.parts or str(path) != value:
        raise ValueError('an exact absolute Linux path is required')
    return path


def convert(audit, registry):
    if (not isinstance(audit, dict) or audit.get('schema') != 'paperflow-app-source-inventory/1'
            or not isinstance(audit.get('versions'), list) or not audit['versions']
            or not isinstance(registry, dict) or registry.get('schema') != 'paperflow-historical-service-versions/1'
            or not isinstance(registry.get('versions'), list)):
        raise ValueError('a fixed audit and historical registry are required')
    known = {}
    for row in registry['versions']:
        if not isinstance(row, dict) or not SHA.fullmatch(row.get('canonical_sha256', '')):
            raise ValueError('historical version identity is invalid')
        config = str(_path(row.get('service_config')))
        if config in known:
            raise ValueError('duplicate historical configuration')
        known[config] = row
    result, seen = [], set()
    for row in audit['versions']:
        if not isinstance(row, dict):
            raise ValueError('source snapshot row is invalid')
        config, app = str(_path(row.get('config_path'))), _path(row.get('app'))
        selected = known.get(config)
        if (config in seen or selected is None or row.get('unlisted_py') != []
                or row.get('listed_py_missing') != [] or not isinstance(row.get('sources'), list)
                or not row['sources'] or len(row['sources']) > 1000
                or not SHA.fullmatch(row.get('native_manifest_sha256', ''))):
            raise ValueError('source snapshot is incomplete or ambiguous')
        _path(row.get('native_manifest'))
        rpc = _path(selected.get('rpc_script'))
        if rpc != app / 'server_rpc.py' or not SHA.fullmatch(selected.get('rpc_script_sha256', '')):
            raise ValueError('RPC is not the original historical app entry')
        files = {}
        for item in row['sources']:
            if not isinstance(item, dict):
                raise ValueError('source record is invalid')
            path = _path(item.get('path'))
            digest, size = item.get('sha256'), item.get('bytes')
            if (path.parent != app or path.suffix != '.py' or not path.stem.isidentifier()
                    or not isinstance(digest, str) or not SHA.fullmatch(digest)
                    or type(size) is not int or not 0 <= size <= 1024 * 1024 or str(path) in files):
                raise ValueError('historical app source is invalid')
            files[str(path)] = digest
        if files.get(str(rpc)) != selected['rpc_script_sha256']:
            raise ValueError('historical RPC digest differs from the source audit')
        seen.add(config)
        result.append(dict(canonical_sha256=selected['canonical_sha256'], app=str(app),
            manifest={'schema': 'paperflow-page-release/1', 'files': files, 'python': None, 'packages': {}}))
    if seen != set(known):
        raise ValueError('a historical version lacks source evidence')
    return result


def _read(path, expected):
    raw = Path(path).read_bytes()
    if not SHA.fullmatch(expected or '') or hashlib.sha256(raw).hexdigest() != expected:
        raise ValueError('source metadata does not match its fixed digest')
    return json.loads(raw.decode('utf8'))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--audit', required=True)
    parser.add_argument('--audit-sha256', required=True)
    parser.add_argument('--history', required=True)
    parser.add_argument('--history-sha256', required=True)
    parser.add_argument('--output-dir', required=True)
    parser.add_argument('--remote-root', required=True)
    args = parser.parse_args()
    out = Path(args.output_dir)
    if not out.is_absolute() or not out.parent.resolve(strict=True).is_relative_to(HERE) or out.exists():
        raise ValueError('a new directory inside the isolated candidate is required')
    remote = _path(args.remote_root)
    audit, history = _read(args.audit, args.audit_sha256), _read(args.history, args.history_sha256)
    rows = convert(audit, history)
    out.mkdir()
    index = []
    for row in rows:
        name = 'app-' + row['canonical_sha256'] + '.json'
        raw = json.dumps(row['manifest'], ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode('utf8')
        with (out / name).open('xb') as stream:
            stream.write(raw)
        index.append(dict(canonical_sha256=row['canonical_sha256'], app=row['app'],
                          manifest_path=str(remote / name), manifest_sha256=hashlib.sha256(raw).hexdigest()))
    manifest_index = dict(schema='paperflow-guarded-app-versions/1', server_identity=history['server_identity'],
                          versions=index)
    raw = json.dumps(manifest_index, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode('utf8')
    with (out / 'index.json').open('xb') as stream:
        stream.write(raw)
    print(json.dumps(dict(versions=len(index), index_sha256=hashlib.sha256(raw).hexdigest())))


if __name__ == '__main__':
    main()
