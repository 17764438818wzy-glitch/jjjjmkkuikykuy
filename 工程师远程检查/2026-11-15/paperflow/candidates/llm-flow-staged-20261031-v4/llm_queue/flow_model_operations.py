"""Business adapters for one verified real folder FlowRun.

Only request/response journals are written. Parsing, merging and MD generation
belong to the later postprocessing stage and are intentionally not called.
"""
from pathlib import Path

from .attachment_raw_input import check_attachment_source, prepare_attachment_body
from .folder_completion import expected_components
from .frozen_body_plan import partition_plan, single_plan
from .image_evidence import verify_body_images
from .model_limits import model_lease
from .raw_partition import run_partition_raw
from .raw_request import run_raw_request
from .segment_plan_contract import verify_plan


class FolderModelOperations:
    def __init__(self, identity, generation, binding, backend, settings, guard, stop,
                 connection, expected_api_url, acquire, *, opener=None):
        self.identity = dict(identity)
        self.generation = generation
        self.binding = binding
        self.backend = backend
        self.settings = settings
        self.guard = guard
        self.stop = stop
        self.connection = connection
        self.expected_api_url = expected_api_url
        self.acquire = acquire
        self.opener = opener
        self.verified = binding.load(identity, require_running=True)
        self.manifest = self.verified['manifest']
        self.paper = self.verified['paper']
        parts = expected_components(self.manifest, identity['task_run_id'])
        self.body_parts = {p['part_id']: p for p in parts if p['kind'] == 'body'}
        self.attachments = {p['file_id']: p for p in parts if p['kind'] == 'attachment'}

    def _current(self):
        current = self.binding.load(self.identity, require_running=True)
        if current['manifest'] != self.manifest or current['paper'] != self.paper:
            raise ValueError('文件夹交接或登记资料发生变化')
        return current

    def _lease(self, kind, stop):
        return model_lease(kind, stop, self.expected_api_url, self.acquire)

    def _admission(self, component, kind, work, transport):
        expected = component.get('request_sha256')
        if expected and transport.get('request_sha256') != expected:
            raise ValueError('实际正文请求与前处理分段计划不一致')
        return self.guard.admit(self.identity['task_id'], self.generation, component['component_id'],
            transport['request_sha256'], kind=kind, model_dir=transport['work_dir'], work_dir=work,
            receipt_id=self.identity['receipt_id'], task_run_id=self.identity['task_run_id'],
            source_sha256=component['source_sha256'])

    def _planning_connection(self):
        from paperflow_prefect.records import read_json
        model = read_json(self.settings.path('model_config'))
        limits = dict(model.get('preextract_limits') or {})
        if (not limits or limits.get('model') != model.get('text_model')
                or str(limits.get('base_url', '')).rstrip('/') != str(model.get('base_url', '')).rstrip('/')
                or limits.get('supports_images') is not True
                or self.connection.get('model') != model.get('text_model')
                or str(self.connection.get('base_url', '')).rstrip('/') != str(model.get('base_url', '')).rstrip('/')):
            raise ValueError('正文模型或容量配置与前处理不一致')
        return {**limits, 'model': model['text_model'], 'max_tokens': model.get('max_tokens', 8192)}

    def body(self):
        from paperflow_prefect.body_clean_handoff import plan_with_available_images, save_projection
        from paperflow_prefect.body_segment_tokenizer import with_body_token_counter
        from paperflow_prefect.profile_recovery import INTENT, _other_revisions
        from paperflow_prefect.records import sha_file
        from paperflow_prefect.request_admission import bind_work, require_model_history
        from preextract_contract import load_template
        from preextract_images import _verified_image
        from preextract_partition_run import create_plan
        from preextract_request import request_once

        current = self._current()
        reading, original = current['reading_material'], current['material']
        source, receipt = self.paper['body'], self.manifest['capacity']['plan_identity']
        work = Path(self.manifest['body_work']).resolve(strict=True)
        if Path(self.backend.body_work(self.paper)).resolve() != work:
            raise ValueError('正文历史工作目录与前处理交接不一致')
        template, _ = load_template(self.settings.path('template_path'))
        used = {}

        def history():
            if (work / INTENT).exists() or _other_revisions(work):
                raise ValueError('正文存在待核对的恢复记录')
            if Path(self.backend.body_work(self.paper)).resolve() != work:
                raise ValueError('正文历史工作目录身份发生变化')
            self.backend.confirm_history(self.paper, source)
            require_model_history(self.paper, source, work)
            bind_work(self.backend.root, self.paper, source, 'body', work)

        def check_source():
            self._current()
            if sha_file(source['path']) != source['sha256']:
                raise ValueError('正文来源发生变化')
            for asset in reading.get('assets', []):
                if asset.get('image_path'):
                    _verified_image(asset)
            if receipt['mode'] == 'single' and 'single' in used:
                verify_body_images(reading, used['single'], work / 'model')

        def make_new_plan():
            connection = self._planning_connection()
            planning = with_body_token_counter(connection)
            planning['context_window'] = min(int(planning['context_window']), 1000000)
            plan, adapted, _ = plan_with_available_images(
                reading, work, template, self.backend.catalog, connection, planning)
            verify_plan(plan, receipt, source_sha256=source['sha256'],
                        reading_sha256=self.manifest['reading_material']['sha256'])
            return plan, original, adapted

        def plan_factory():
            save_projection(work, original, reading)
            if receipt['mode'] == 'partitioned':
                return partition_plan(work, receipt, source['sha256'],
                    self.manifest['reading_material']['sha256'], original_factory=lambda: original,
                    new_plan_factory=make_new_plan)
            plan = single_plan(work, receipt, source['sha256'],
                self.manifest['reading_material']['sha256'], new_plan_factory=lambda: make_new_plan()[0])
            return plan, original, None

        if receipt['mode'] == 'single':
            component = self.body_parts['part-001']
            def request_body():
                plan, _, _ = plan_factory()
                if plan['mode'] != 'single' or len(plan['parts']) != 1:
                    raise ValueError('正文单次输入计划发生变化')
                used['single'] = plan['parts'][0]
                verify_body_images(reading, used['single'], work / 'model')
                return used['single']['body']
            result = run_raw_request(kind='body', request_body_factory=request_body,
                connection=self.connection, work=work, stop=self.stop, request_once=request_once,
                lease=self._lease, admit=lambda transport: self._admission(component, 'body', work, transport),
                check_history=history, check_source=check_source, opener=self.opener)
            self.guard.complete(self.identity['task_id'], component['component_id'], result['response_sha256'])
            return result

        def admit(part_id, transport):
            return self._admission(self.body_parts[part_id], 'body', work, transport)

        def accept(part_id, result):
            self.guard.complete(self.identity['task_id'], self.body_parts[part_id]['component_id'],
                                result['response_sha256'])

        def check_part(part):
            verify_body_images(reading, part, work / 'model' / part['id'])

        return run_partition_raw(plan_factory=plan_factory, plan_receipt=receipt,
            source_sha256=source['sha256'], reading_sha256=self.manifest['reading_material']['sha256'],
            template=template, connection=self.connection, work=work, stop=self.stop, request_once=request_once,
            lease=self._lease, admit=admit, accept=accept, check_history=history,
            check_source=check_source, check_part=check_part, create_plan=create_plan, opener=self.opener)

    def attachment(self, row):
        from paperflow_prefect.request_admission import bind_work, require_model_history
        from preextract_request import request_once

        saved = next((item for item in self.paper['attachments'] if item['file_id'] == row.get('file_id')), None)
        if saved != row or row['file_id'] not in self.attachments:
            raise ValueError('附件不属于当前文件夹交接')
        component = self.attachments[row['file_id']]
        work = Path(self.backend.attachment_work(self.paper, row)).resolve()

        def history():
            if Path(self.backend.attachment_work(self.paper, row)).resolve() != work:
                raise ValueError('附件历史工作目录身份发生变化')
            self.backend.confirm_history(self.paper, row)
            require_model_history(self.paper, row, work)
            bind_work(self.backend.root, self.paper, row, 'attachment', work)

        def check_source():
            self._current()
            check_attachment_source(row)

        result = run_raw_request(kind='attachment',
            request_body_factory=lambda: prepare_attachment_body(row, work, self.connection),
            connection=self.connection, work=work, stop=self.stop, request_once=request_once,
            lease=self._lease, admit=lambda transport: self._admission(component, 'attachment', work, transport),
            check_history=history, check_source=check_source, opener=self.opener)
        self.guard.complete(self.identity['task_id'], component['component_id'], result['response_sha256'])
        return result
