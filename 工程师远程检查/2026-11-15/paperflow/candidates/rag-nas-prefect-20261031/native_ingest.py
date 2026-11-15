"""Native body submission adapter for a sealed, already postprocessed package.

It does not establish a Prefect deployment, configure a native pipeline, or
prove that the caller's history/admission callbacks are production-ready.
"""
import math
import time
from typing import NamedTuple
from urllib.parse import quote

from effect_journal import EffectUnknown


class FreshSubmissionProof(NamedTuple):
    """An immutable expiry bound to every identity of one submission."""
    binding: tuple
    valid_until: float

    @classmethod
    def for_binding(cls, binding, valid_until):
        return cls(tuple(sorted(binding.items())), valid_until)


class NativeIngestClient:
    def __init__(self, guarded_client, task, packet, pipeline_id, fresh_submission_proof, *,
                 wait_before_submission=None, require_bound_proof=False):
        if not isinstance(pipeline_id, str) or not pipeline_id or not callable(fresh_submission_proof):
            raise ValueError('native pipeline and explicit history proof are required')
        if wait_before_submission is not None and not callable(wait_before_submission):
            raise ValueError('submission wait must be callable')
        if type(require_bound_proof) is not bool:
            raise ValueError('bound proof requirement must be boolean')
        self._guarded = guarded_client
        self.base_url = guarded_client.base_url
        self._pipeline_id = pipeline_id
        self._fresh_proof = fresh_submission_proof
        self._wait_before_submission = wait_before_submission
        self._require_bound_proof = require_bound_proof
        self._identities = {}
        for route in packet['routes']:
            ds = route['paper_dataset_id']
            identity = {'paperflow_source_sha256': task['source_sha256'],
                        'paperflow_manifest_sha256': packet['manifest_sha256'],
                        'paperflow_revision': str(task['revision']),
                        'paperflow_specialty': route['code'],
                        'paperflow_role': 'body', 'paperflow_task': task['task_id']}
            if not ds or ds in self._identities:
                raise ValueError('native body datasets must be unique')
            self._identities[ds] = identity
        if not self._identities:
            raise ValueError('sealed package has no native body target')

    def request(self, method, endpoint, body=None):
        return self._guarded.request(method, endpoint, body)

    def upload_bytes(self, dataset, name, content):
        return self._guarded.upload_bytes(dataset, name, content)

    def upload_file(self, parent, name, content):
        return self._guarded.upload_file(parent, name, content)

    def download_file(self, file_id):
        return self._guarded.download_file(file_id)

    def download_bytes(self, dataset, document):
        return self._guarded.download_bytes(dataset, document)

    def download_image(self, image_id):
        return self._guarded.download_image(image_id)

    def ingest(self, dataset_id, document_id, pipeline_id, *, timeout=600):
        if (dataset_id not in self._identities or pipeline_id != self._pipeline_id
                or not isinstance(document_id, str) or not document_id
                or type(timeout) not in (int, float) or not 0 < timeout <= 3600):
            raise ValueError('native submission identity or timeout differs')
        expected = self._identities[dataset_id]
        endpoint = (f'/datasets/{quote(dataset_id, safe="")}/documents'
                    f'?id={quote(document_id, safe="")}&page=1&page_size=100')
        submit_body = {'doc_ids': [document_id], 'run': '1', 'delete': False}
        binding = {**expected, 'dataset_id': dataset_id, 'document_id': document_id,
                   'pipeline_id': pipeline_id}

        def observe():
            data = self._guarded.request('GET', endpoint)
            rows = data.get('docs', []) if isinstance(data, dict) else data
            if not isinstance(rows, list) or len(rows) != 1:
                raise ValueError('native document is not uniquely readable')
            doc = rows[0]
            if (doc.get('id') != document_id or doc.get('pipeline_id') != pipeline_id
                    or str(doc.get('status')) != '0'
                    or any((doc.get('meta_fields') or {}).get(key) != value for key, value in expected.items())):
                raise ValueError('native document, route or ownership differs')
            return doc

        started = time.monotonic()
        deadline = started + timeout

        def check_deadline():
            if time.monotonic() >= deadline:
                raise TimeoutError('native admission deadline expired before submission')

        def read_proof(frozen_binding):
            proof = self._fresh_proof(dict(frozen_binding))
            if proof is True and not self._require_bound_proof:
                return True, None
            if (not isinstance(proof, FreshSubmissionProof)
                    or proof.binding != tuple(sorted(frozen_binding.items()))
                    or type(proof.valid_until) not in (int, float)
                    or not math.isfinite(proof.valid_until)):
                return False, None
            return True, proof.valid_until

        doc = observe()
        run = str(doc.get('run'))
        state = self._guarded._native_effect_state(submit_body, binding)
        if state == 'intent':
            raise EffectUnknown('native submission outcome requires original-identity reconciliation')
        if state is None:
            if run not in {'0', 'UNSTART'}:
                raise EffectUnknown('remote native run has no matching submission evidence')
            check_deadline()
            if self._wait_before_submission is not None:
                # Waiting is outside the document fence and only before a
                # recorded native effect exists. Final admission still checks
                # document identity, run state and fresh proof under the fence.
                if self._wait_before_submission(dict(binding), deadline) is not True:
                    raise ValueError('submission wait did not confirm admission')
                check_deadline()
            verified, _ = read_proof(binding)
            if not verified:
                raise ValueError('native submission history was not proven fresh')
            check_deadline()
            final_proof_deadline = None
            def preflight(frozen_binding):
                nonlocal final_proof_deadline
                # Recheck after admission waiting, before persisting intent.
                # A real integration still needs a shared document fence for
                # every possible writer; a GET alone cannot supply one.
                check_deadline()
                current = observe()
                if str(current.get('run')) not in {'0', 'UNSTART'}:
                    raise EffectUnknown('native document started while awaiting admission')
                check_deadline()
                verified, final_proof_deadline = read_proof(frozen_binding)
                check_deadline()
                return verified
            def final_deadline():
                # The final proof runs inside preflight; read its new expiry
                # only after that proof, including at intent and send time.
                valid_until = final_proof_deadline
                if valid_until is None and not self._require_bound_proof:
                    return deadline
                if type(valid_until) not in (int, float) or not math.isfinite(valid_until):
                    raise ValueError('fresh submission proof expiry is unavailable')
                return min(deadline, valid_until)
            self._guarded._submit_native(submit_body, binding, preflight,
                                         not_after=final_deadline)
        elif state != 'returned':
            raise EffectUnknown('native submission journal state is invalid')

        polls = 0
        while time.monotonic() - started < timeout:
            polls += 1
            doc = observe()
            run = str(doc.get('run'))
            if run in {'3', 'DONE'} and float(doc.get('progress', 0)) >= 1:
                return {'document_id': document_id, 'pipeline_id': pipeline_id, 'run': doc['run'],
                        'progress': doc['progress'], 'polls': polls,
                        'elapsed_seconds': time.monotonic() - started,
                        'reservation': 'held_until_exact_readback_and_receipt'}
            if run in {'FAIL', '4', '-1', '2', 'CANCEL'} or float(doc.get('progress', 0)) < 0:
                raise RuntimeError('native processing failed; preserve original submission evidence')
            if run not in {'0', 'UNSTART', '1', 'RUNNING', '3', 'DONE'}:
                raise EffectUnknown('native processing state is not recognized')
            time.sleep(min(0.25, max(0, timeout - (time.monotonic() - started))))
        raise EffectUnknown('native processing has not reached a verified terminal state')
