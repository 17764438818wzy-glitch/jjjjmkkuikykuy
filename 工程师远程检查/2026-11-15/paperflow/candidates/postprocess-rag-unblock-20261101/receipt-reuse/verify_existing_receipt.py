from pathlib import Path
import json
import sys
import time

here = Path(__file__).resolve().parent
workspace = here.parents[2]
server = workspace / 'candidates' / 'postprocess-server-20261101'
upstream = workspace / 'candidates' / 'postprocess-prefect-20261031'
sys.path[:0] = [str(path) for path in (here, upstream / 'runtime_next', upstream / 'results_next',
    upstream, workspace / 'prefect-paperflow', server)]

import remote_coordinator
import remote_runtime_adapter
import remote_server_transport

sys.path.insert(0, str(here.parent))
import receipt_access

access = receipt_access.build()
row = access.row('6cb5a766-94f9-4613-aa5e-17a883be9016', '018ad07a16e9ba0a4391eaeb64d3294b')
identity = {key: row[key] for key in receipt_access.FIELDS}
receipt = json.loads(row['receipt_json'])
inspector = access.inspector
request = inspector._request(receipt, identity, row['claim_id'])
started = time.perf_counter()
evidence = inspector._inspect_batch([request])[0]
remote_seconds = time.perf_counter() - started

def unexpected_read(_requests):
    raise AssertionError('same-call reuse made another remote request')

inspector._inspect_batch = unexpected_read
started = time.perf_counter()
job = request['job']
result = dict(job=job, server_identity=request['server_identity'], state='finished', server_validation='passed',
    receipt=receipt, receipt_json=request['receipt_json'], receipt_sha256=request['receipt_sha256'],
    server_receipt=evidence)
transport = object.__new__(remote_server_transport.RemoteServerTransport)
transport._server = request['server_identity']
transport._submitted = {job['job_id']: dict(job=job, state='fetched',
    result_json=remote_server_transport._json(result))}
assert transport.validate_result(job, result) is True
with inspector.prefetched([row], accepted_evidence=evidence):
    assert inspector(receipt, identity, row['claim_id']) is True
    assert inspector(receipt, identity, row['claim_id']) is True
local_seconds = time.perf_counter() - started
assert getattr(inspector._local, 'prefetched', None) is None
print(json.dumps(dict(existing_receipt_read=True, result_reused=True,
    remote_read_seconds=round(remote_seconds, 3), reuse_seconds=round(local_seconds, 3))))
