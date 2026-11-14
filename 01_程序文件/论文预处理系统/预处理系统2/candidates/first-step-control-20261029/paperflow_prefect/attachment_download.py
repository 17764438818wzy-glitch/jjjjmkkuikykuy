"""Download only a receipt-bound native original; never expose API credentials."""
import hashlib
import os
from pathlib import Path
import time

from .records import identity, read_json, sha_file


def download_attachment(receipt_path, client, output_path):
    state = read_json(receipt_path); binding = state.get('binding', {}); raw = state.get('raw', {})
    if (state.get('schema') != 'paperflow-attachment-ingest/1' or raw.get('status') != 'verified'
            or binding.get('service') != client.base_url.rstrip('/')
            or raw.get('source_sha256') != binding.get('source_sha256')
            or raw.get('parent_id') != binding.get('primary', {}).get('parent_id')
            or raw.get('dataset_id') != binding.get('primary', {}).get('dataset_id')
            or not raw.get('file_id') or not raw.get('document_id')
            or binding.get('raw_identity') != identity([binding.get('paper_id'),binding.get('attachment_id'),binding.get('source_sha256')])):
        raise ValueError('下载回执未核验、归属不符或服务改变')
    output = Path(output_path).resolve()
    protected = [raw.get('local_original_path'), state.get('saved_md_path'),
                 state.get('delivery', {}).get('md_path'), str(Path(receipt_path).resolve())]
    if any(value and output == Path(value).resolve() for value in protected):
        raise ValueError('下载不能覆盖原件、交付 MD 或回执')
    existed = output.exists()
    if existed and (not output.is_file() or sha_file(output) != raw['source_sha256']):
        raise FileExistsError('下载目标已有不同文件，未覆盖')
    data = client.download_file(raw['file_id'])
    actual = hashlib.sha256(data).hexdigest()
    if actual != raw['source_sha256'] or len(data) != raw['bytes']:
        raise ValueError('远端下载内容与原件哈希或大小不符，未保存')
    if not existed:
        output.parent.mkdir(parents=True, exist_ok=True)
        with output.open('xb') as stream:
            stream.write(data); stream.flush(); os.fsync(stream.fileno())
    if sha_file(output) != actual: raise ValueError('下载保存后哈希不符')
    return {'schema':'paperflow-attachment-download/1','paper_id':binding['paper_id'],
            'attachment_id':binding['attachment_id'],'file_id':raw['file_id'],
            'path':str(output),'bytes':len(data),'sha256':actual,'verified':True,
            'existing_reused':existed,'remote_writes':0,'verified_at':time.time()}
