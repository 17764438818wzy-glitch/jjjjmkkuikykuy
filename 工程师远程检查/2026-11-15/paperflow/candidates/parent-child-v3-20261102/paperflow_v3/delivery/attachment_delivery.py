"""Reuse native attachment delivery while resuming only confirmed rejections."""

from pathlib import Path

from filelock import FileLock
from paperflow_prefect.attachment_ingest import read_attachment_delivery
from paperflow_prefect.attachment_publish import _Delivery, _validate_route, ensure_original_folder as _native_folder
from paperflow_prefect.records import identity, read_json, write_json
from ragflow_client import RagflowHTTPError

from .transport import TaskPaused, definite_rejection


def ensure_original_folder(client, root_id, code, name, receipt_root):
    path = Path(receipt_root).resolve() / 'folders' / (identity([client.base_url.rstrip('/'), code]) + '.json')
    effects = client.effects
    state = read_json(path) if path.is_file() else {}
    intent = state.get('intent') or {}
    failed_id = intent.get('server_failed_effect_id')
    failed = effects.states.get(failed_id, {}).get('status') == 'rag_effect_failed'
    if intent.get('not_sent') or (failed and effects.retry_failed):
        client._check()
        state.setdefault('resolved_intents', []).append(state.pop('intent'))
        write_json(path, state)
    effects.last_failed = effects.last_not_sent = None
    try:
        return _native_folder(client, root_id, code, name, receipt_root)
    except (TaskPaused, RagflowHTTPError) as exc:
        marker = (effects.last_not_sent if isinstance(exc, TaskPaused) else
                  effects.last_failed if definite_rejection(exc) else None)
        if marker and path.is_file():
            state = read_json(path)
            if state.get('intent'):
                if isinstance(exc, TaskPaused):
                    state['intent']['not_sent'] = True
                else:
                    state['intent']['server_failed_effect_id'] = marker
                write_json(path, state)
        raise


class _PaperDelivery(_Delivery):
    def send_once(self, state, key, payload, call):
        old = state.get(key) or {}
        effects = getattr(self.client, 'effects', None)
        failed_id = old.get('server_failed_effect_id')
        failed = effects and effects.states.get(failed_id, {}).get('status') == 'rag_effect_failed'
        if old.get('not_sent') or (failed and effects.retry_failed):
            self.client._check()
            self.state.setdefault('resolved_intents', []).append({'key': key, 'intent': state.pop(key)})
            self.save()
        try:
            return super().send_once(state, key, payload, call)
        except TaskPaused:
            state[key]['not_sent'] = True
            self.save()
            raise
        except RagflowHTTPError as exc:
            if definite_rejection(exc) and effects and effects.last_failed:
                state[key]['server_failed_effect_id'] = effects.last_failed
                self.save()
            raise


def ingest_attachment(delivery, route, client, receipt_root, budget):
    card = read_attachment_delivery(delivery)
    _validate_route(card, route)
    key = identity([card['metadata']['paper_id'], card['metadata']['attachment_id']])
    directory = Path(receipt_root).resolve() / key
    directory.mkdir(parents=True, exist_ok=True)
    with FileLock(str(directory / 'ingest.lock'), timeout=0):
        return _PaperDelivery(card, route, client, directory / 'receipt.json', budget, False).run()
