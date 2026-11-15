"""Candidate reuse of the existing source-bound work and history guards.

The caller must provide the paper from the verified registration receipt and
hold the corresponding body or attachment work locks while checking history.
This module does not connect to an old Prefect deployment or send requests.
"""
from pathlib import Path

from paperflow_prefect import request_admission


class WorkBinding:
    def __init__(self, backend, paper, manifest):
        if (not callable(getattr(backend, 'body_work', None))
                or not callable(getattr(backend, 'attachment_work', None))
                or not callable(getattr(backend, 'confirm_history', None))
                or not isinstance(paper, dict) or not isinstance(manifest, dict)):
            raise ValueError('资料来源和历史目录不可核验')
        self.backend, self.paper, self.manifest = backend, paper, manifest
        self._check_paper()

    def _check_paper(self):
        if (not isinstance(self.paper.get('body'), dict) or not isinstance(self.paper.get('attachments'), list)
                or any(self.paper.get(key) != self.manifest.get(key)
                       for key in ('paper_id', 'revision', 'body', 'attachments'))):
            raise ValueError('登记资料与前处理交接不一致')

    def body_work(self):
        self._check_paper()
        try:
            actual = Path(self.backend.body_work(self.paper)).resolve(strict=True)
            expected = Path(self.manifest['body_work']).resolve(strict=True)
        except (OSError, KeyError, TypeError) as exc:
            raise ValueError('正文历史工作目录不可核验') from exc
        if actual != expected or not actual.is_dir():
            raise ValueError('正文历史工作目录与前处理不一致')
        return actual

    def _attachment(self, row):
        self._check_paper()
        if not isinstance(row, dict):
            raise ValueError('附件身份不可核验')
        found = [item for item in self.paper['attachments']
                 if isinstance(item, dict) and item.get('file_id') == row.get('file_id')]
        if len(found) != 1 or found[0] != row:
            raise ValueError('附件身份与登记资料不一致')
        return found[0]

    def attachment_work(self, row):
        row = self._attachment(row)
        return Path(self.backend.attachment_work(self.paper, row)).resolve()

    def verify_attachment_work(self, row, work):
        if self.attachment_work(row) != Path(work).resolve():
            raise ValueError('附件历史工作目录身份不一致')

    def check_body_history(self):
        work = self.body_work()
        row = self.paper['body']
        self.backend.confirm_history(self.paper, row)
        request_admission.require_model_history(self.paper, row, work)
        request_admission.bind_work(self.backend.root, self.paper, row, 'body', work)

    def check_attachment_history(self, row, work):
        row = self._attachment(row)
        self.verify_attachment_work(row, work)
        self.backend.confirm_history(self.paper, row)
        request_admission.require_model_history(self.paper, row, work)
        request_admission.bind_work(self.backend.root, self.paper, row, 'attachment', work)
