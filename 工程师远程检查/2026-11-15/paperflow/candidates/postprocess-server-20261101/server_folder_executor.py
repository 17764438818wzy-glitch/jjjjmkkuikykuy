"""Run the existing folder business contract without owning control state.

This pure execution seam must be called only after the original coordinator has
persisted its claim and a server supervisor has admitted the same identity.
It never declares the folder waiting for RAG or changes a control database.
"""
import copy
import hashlib
import json
from uuid import UUID


def process_verified_folder(*, load_inputs, identity, generation, manifest_sha256, claim_id,
                            catalog, paperflow_root, counter, output_root):
    from body_adapter import process_body, _output_boundary
    from generation_attachment_adapter import process_attachment, _inputs, _safe_output
    from folder_processor import process_folder

    try:
        if str(UUID(claim_id)) != claim_id:
            raise ValueError('noncanonical claim')
    except (TypeError, ValueError, AttributeError) as exc:
        raise ValueError('original claim identity is required') from exc
    if not callable(load_inputs) or counter is None:
        raise ValueError('verified input loader and counter are required')

    class ReceiptCapture:
        def __init__(self):
            self.claimed = False

        def claim(self, actual_identity, actual_generation, actual_manifest):
            if (self.claimed or actual_identity != identity or actual_generation != generation
                    or actual_manifest != manifest_sha256):
                raise ValueError('server execution does not match the original claim')
            self.claimed = True
            return {'claim_id': claim_id}

        def complete(self, actual_identity, actual_claim, receipt, *, validate_receipt):
            if not self.claimed or actual_identity != identity or actual_claim != claim_id:
                raise ValueError('server receipt does not match the original claim')
            encoded = json.dumps(receipt, ensure_ascii=False, sort_keys=True,
                                 separators=(',', ':'), allow_nan=False)
            detached = json.loads(encoded)
            if (not callable(validate_receipt) or validate_receipt(detached) is not True
                    or json.dumps(detached, ensure_ascii=False, sort_keys=True,
                                  separators=(',', ':'), allow_nan=False) != encoded):
                raise ValueError('server receipt did not pass full validation')
            return {'server_validation': 'passed', 'receipt': copy.deepcopy(receipt),
                    'receipt_json': encoded, 'receipt_sha256': hashlib.sha256(encoded.encode('utf8')).hexdigest()}

        def mark_unknown(self, actual_identity, actual_claim, detail):
            # The real supervisor owns durable unknown state; never manufacture
            # a completion receipt here when business processing raises.
            if actual_identity != identity or actual_claim != claim_id:
                raise ValueError('server execution identity changed')

    def body_processor(evidence, output):
        return process_body(evidence, output, expected_identity=identity, catalog=catalog,
                            paperflow_root=paperflow_root, counter=counter)

    def attachment_processor(ticket, handoff, output):
        return process_attachment(ticket, handoff, output)

    def output_guard(inputs, output):
        _output_boundary(inputs['body'], output)
        for item in inputs['attachments']:
            row, _, _, _, work, _ = _inputs(identity, item)
            _safe_output(output, work, row)

    return process_folder(identity=identity, generation=generation, manifest_sha256=manifest_sha256,
                          store=ReceiptCapture(), load_inputs=load_inputs, body_processor=body_processor,
                          attachment_processor=attachment_processor, output_root=output_root,
                          output_guard=output_guard)
