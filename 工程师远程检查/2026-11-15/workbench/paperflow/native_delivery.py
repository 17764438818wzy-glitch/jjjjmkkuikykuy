"""Optional native body batching; images and verification retain the existing contract."""
import hashlib

from .delivery import DeliveryPublisher
from .rag import RemoteConflict, compact


class NativeDeliveryPublisher(DeliveryPublisher):
    def __init__(self, client, on_change, config):
        super().__init__(client, on_change)
        self.config = config

    def document_bytes(self, task, packet, role, identity):
        if role != 'body':
            return super().document_bytes(task, packet, role, identity)
        rows = self.records(packet)['body']
        if not 1 <= len(rows) <= 1000:
            raise ValueError('native transport supports 1..1000 existing records')
        return compact(dict(schema='pf-native-bulk-proof/1',
                            chunks=[dict(text=self.render(task, packet, row)) for row in rows])).encode('utf-8')

    def publish_records(self, task, packet, role, records, obj, receipts):
        if role != 'body':
            return super().publish_records(task, packet, role, records, obj, receipts)
        ds, did = obj['dataset_id'], obj['document_id']
        payload = self.document_bytes(task, packet, role, obj['identity'])
        binding = dict(pipeline_id=self.config.native_pipeline_id,
                       transport_sha256=hashlib.sha256(payload).hexdigest(), record_count=len(records))
        native = obj.get('native_batch')
        if native and any(native.get(k) != v for k, v in binding.items()):
            raise RemoteConflict('native batch binding changed')
        expected = {self.render(task, packet, row) for row in records}
        if len(expected) != len(records):
            raise RemoteConflict('native records are not unique')
        existing = self.chunks(ds, did)
        if not native:
            if existing:
                raise RemoteConflict('unbound native document already contains chunks')
            self.client.request('PATCH', f'/datasets/{ds}/documents/{did}',
                                dict(enabled=0, pipeline_id=binding['pipeline_id']))
            doc = self.documents(ds, id=did)
            if len(doc) != 1 or str(doc[0].get('status')) != '0' or doc[0].get('pipeline_id') != binding['pipeline_id']:
                raise RemoteConflict('disabled native pipeline binding not confirmed')
            # Persist before the request. An interrupted intent never authorizes a resend.
            native = obj['native_batch'] = dict(binding, state='intent')
            self.on_change(receipts)
            terminal = self.client.ingest(ds, did, binding['pipeline_id'], timeout=self.config.native_ingest_timeout)
            native.update(state='terminal', terminal=terminal)
            self.on_change(receipts)
            existing = self.chunks(ds, did)
        else:
            doc = self.documents(ds, id=did)
            if (len(doc) != 1 or doc[0].get('pipeline_id') != binding['pipeline_id']
                    or str(doc[0].get('status')) != '0'):
                raise RemoteConflict('disabled native pipeline binding changed before resume')
            if (len(doc) == 1 and native.get('state') == 'intent' and not existing
                    and str(doc[0].get('run')) in ('0', 'UNSTART')):
                # The manager rechecks that no prior permit ever became possible.
                # This resumes only an intent interrupted BEFORE its first send.
                terminal = self.client.ingest(ds, did, binding['pipeline_id'], timeout=self.config.native_ingest_timeout)
                native.update(state='terminal', terminal=terminal)
                self.on_change(receipts)
                existing = self.chunks(ds, did)
                doc = self.documents(ds, id=did)
            if (len(doc) != 1 or str(doc[0].get('run')) not in ('3', 'DONE')
                    or float(doc[0].get('progress', 0)) < 1):
                raise RemoteConflict('recorded native intent requires terminal readback; no resubmission')
        if len(existing) != len(expected) or {r.get('content') for r in existing} != expected:
            raise RemoteConflict('native batch exact readback mismatch')
        by_content = {r['content']: r for r in existing}
        obj['chunks'] = {row[0]: dict(status='written', chunk_id=by_content[self.render(task, packet, row)]['id'],
            image_id=by_content[self.render(task, packet, row)].get('image_id'),
            content_sha256=hashlib.sha256(self.render(task, packet, row).encode()).hexdigest()) for row in records}
        native['state'] = 'verified'
        self.on_change(receipts)
