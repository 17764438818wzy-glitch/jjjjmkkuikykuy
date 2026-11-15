import contextlib
import io
import sys
import unittest
from unittest.mock import AsyncMock, patch


class Stage3TargetCliTests(unittest.TestCase):
    def test_process_forwards_exact_selected_attachment_and_saved_config(self):
        import stage3_handoff_cli
        run=AsyncMock(return_value={'status':'stage3_complete'})
        with patch.object(sys,'argv',['stage3_handoff_cli.py','process','--receipt','receipt.json',
            '--file-id','attachment-a','--config','single-run.json']), \
             patch('paperflow_prefect.flows.received_handoff',run),contextlib.redirect_stdout(io.StringIO()):
            stage3_handoff_cli.main()
        run.assert_awaited_once_with('receipt.json',attachment_id='attachment-a',config_path='single-run.json')

    def test_selected_flow_still_enforces_disabled_config_before_reading_receipt(self):
        import asyncio
        from paperflow_prefect.flows import received_handoff
        from paperflow_prefect.settings import Settings, ExecutionDisabled
        with patch('paperflow_prefect.flows.Settings.load',return_value=Settings({'execution_enabled':False})) as load, \
             patch('paperflow_prefect.stage3_handoff.read_receipt',side_effect=AssertionError('must stop before receipt')):
            with self.assertRaises(ExecutionDisabled):
                asyncio.run(received_handoff.fn('receipt.json',attachment_id='a',config_path='disabled.json'))
        load.assert_called_once_with('disabled.json')

    def test_config_cannot_silently_use_another_prefect_server(self):
        import asyncio
        from paperflow_prefect.flows import received_handoff
        from paperflow_prefect.settings import Settings
        settings=Settings(dict(execution_enabled=True,library_root='C:/fixture-library',
            data_root='C:/fixture-data',prefect_api_url='http://different.invalid/api'))
        with patch('paperflow_prefect.flows.Settings.load',return_value=settings), \
             patch('paperflow_prefect.stage3_handoff.read_receipt',side_effect=AssertionError('must stop before receipt')):
            with self.assertRaisesRegex(ValueError,'Prefect'):
                asyncio.run(received_handoff.fn('receipt.json',attachment_id='a',config_path='other.json'))
