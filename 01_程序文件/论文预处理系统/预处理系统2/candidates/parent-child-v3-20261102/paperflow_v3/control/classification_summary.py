"""One current display summary; original classification receipts remain authoritative."""

import asyncio
from collections import Counter
from datetime import datetime, timezone
import logging

from prefect.variables import Variable

STATUSES = ('passed', 'failed', 'unrecognized', 'unresolved')
log = logging.getLogger(__name__)


def summary_name(task_id):
    return 'paperflow_v3_classification_summary_' + str(task_id).replace('-', '_')


class ClassificationSummary:
    """One coordinator publishes worker results; saves never trigger business retries."""

    def __init__(self, task_id, *, records=(), save=None, interval=1):
        self.task_id = str(task_id)
        self.results = {}
        self.changed = asyncio.Event()
        self.interval = interval
        self.revision = 0
        self.phase = 'open'
        self.stopping = False
        self.as_of = None
        self.save = save or self._save
        # Receipts are newest first. A repeated group replaces its outcome, not its count.
        for row in records:
            if row['kind'] == 'classification':
                self.results.setdefault(row['folder_id'], row['data'].get('status', 'unresolved'))
        for row in records:
            if row['kind'] == 'classification_started':
                self.results.setdefault(row['folder_id'], 'unresolved')
        self.as_of = next((row.get('recorded_at') for row in records
                           if row['kind'] in {'classification', 'classification_started'} and row.get('recorded_at')), None)

    async def _save(self, value):
        await Variable.aset(summary_name(self.task_id), value, overwrite=True)

    def record(self, group_id, status):
        status = status if status in STATUSES else 'unresolved'
        if self.results.get(group_id) != status:
            self.results[group_id] = status
            self.revision += 1
            self.as_of = datetime.now(timezone.utc).isoformat()
            self.changed.set()

    def value(self):
        counts = Counter(self.results.values())
        return {'version': 1, 'task_id': self.task_id, 'revision': self.revision,
                'phase': self.phase, 'as_of': self.as_of, 'observed': len(self.results),
                'counts': {status: counts[status] for status in STATUSES}}

    async def publish(self):
        self.changed.set()
        while True:
            await self.changed.wait()
            if self.interval and not self.stopping:
                await asyncio.sleep(self.interval)
            # Clear before taking the value: updates arriving during save keep the event set.
            self.changed.clear()
            value = self.value()
            try:
                await self.save(value)
            except Exception:
                log.warning('Classification display summary was not saved; original receipts retained', exc_info=True)
            if self.stopping and not self.changed.is_set():
                return

    async def close(self, *, complete=True):
        self.phase = 'closed' if complete else 'interrupted'
        self.revision += 1
        self.stopping = True
        self.changed.set()
