"""Account control contracts; the external gateway is a stateful boundary fake."""
import importlib.util
from pathlib import Path
import threading
import unittest
from urllib.error import URLError

spec = importlib.util.spec_from_file_location('account_controls_monitor', Path(__file__).resolve().parents[1] / 'workbuddy_monitor.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
MODEL = 'deepseek-v4.1-flash'


class Gateway:
    def __init__(self):
        self.accounts = [
            {'uid': 'new-cn', 'nickname': '新增国内', 'realm': 'cn', 'enabled': False, 'tasksEnabled': False},
            {'uid': 'off-intl', 'nickname': '禁用国际', 'realm': 'intl', 'enabled': False, 'tasksEnabled': True},
            {'uid': 'active-cn', 'nickname': '国内可用', 'realm': 'cn', 'enabled': True, 'tasksEnabled': False},
            {'uid': 'active-intl', 'nickname': '国际网络未知', 'realm': 'intl', 'enabled': True, 'tasksEnabled': False},
        ]
        self.calls = []
        self.probes = {}
        self.set_mode = 'ok'
        self.saved = False
        self.on_probe = None
        self.lock = threading.RLock()

    def __call__(self, path, *, token=None, data=None, headers=None, timeout=3):
        with self.lock:
            self.calls.append((path, data))
            if path == '/accounts?realm=all':
                if self.saved and self.set_mode == 'readback_lost':
                    raise URLError('Readback unavailable')
                return {'accounts': [dict(a, apiKey='NEVER_DISCLOSE') for a in self.accounts]}
            if path == '/accounts/set':
                if self.set_mode != 'not_applied':
                    account = next(a for a in self.accounts if a['uid'] == data['uid'])
                    account['enabled'] = data['enabled']
                    account['tasksEnabled'] = data['enabled']  # Native setter side effect.
                    if 'tasksEnabled' in data:
                        account['tasksEnabled'] = data['tasksEnabled']
                    self.saved = True
                if self.set_mode in ('lost_response', 'not_applied'):
                    raise URLError('Write response missing')
                return {'ok': True}
        if path == '/accounts/test':
            if self.on_probe:
                self.on_probe()
            answer = self.probes.get(data['uid'], {'uid': data['uid'], 'model': MODEL, 'ok': True})
            if isinstance(answer, Exception):
                raise answer
            return dict(answer)
        raise AssertionError(path)


class ControlsTests(unittest.TestCase):
    def setUp(self):
        self.gateway = Gateway()
        self.monitor = module.Monitor(self.gateway, prefect_transport=lambda: None, password_loader=lambda: None)
        self.monitor._token = 'fake-panel-session'

    def setters(self):
        return [data for path, data in self.gateway.calls if path == '/accounts/set']

    def test_manual_enable_confirms_switch_and_preserves_background_setting(self):
        result = self.monitor.enable_account('new-cn')
        self.assertEqual(result['status'], 'enabled')
        self.assertTrue(self.gateway.accounts[0]['enabled'])
        self.assertFalse(self.gateway.accounts[0]['tasksEnabled'])
        self.assertEqual(self.setters(), [{'uid': 'new-cn', 'enabled': True, 'tasksEnabled': False}])
        self.assertFalse(any(path == '/accounts/test' for path, _ in self.gateway.calls))

    def test_already_enabled_is_not_written_again(self):
        self.assertEqual(self.monitor.enable_account('active-cn')['status'], 'enabled')
        self.assertEqual(self.setters(), [])

    def test_lost_write_response_is_confirmed_by_readback_without_replay(self):
        self.gateway.set_mode = 'lost_response'
        self.assertEqual(self.monitor.enable_account('new-cn')['status'], 'enabled')
        self.assertEqual(len(self.setters()), 1)

    def test_unapplied_write_is_unknown_and_never_replayed(self):
        self.gateway.set_mode = 'not_applied'
        self.assertEqual(self.monitor.enable_account('new-cn')['status'], 'unknown')
        self.assertFalse(self.gateway.accounts[0]['enabled'])
        self.assertEqual(len(self.setters()), 1)

    def test_lost_readback_does_not_claim_confirmed_enable(self):
        self.gateway.set_mode = 'readback_lost'
        self.assertEqual(self.monitor.enable_account('new-cn')['status'], 'unknown')
        self.assertEqual(len(self.setters()), 1)

    def test_refresh_tests_disabled_and_both_realms_enables_only_successes(self):
        self.gateway.probes = {
            'off-intl': {'uid': 'off-intl', 'model': MODEL, 'ok': False, 'status': 429, 'error': '额度不足'},
            'active-intl': URLError('network unavailable'),
        }
        result = self.monitor.refresh_accounts(['new-cn', 'off-intl', 'active-cn', 'active-intl'])
        rows = {a['uid']: a for a in result['accounts']}
        self.assertEqual(rows['new-cn']['status'], 'success')
        self.assertTrue(rows['new-cn']['enabled'])
        self.assertFalse(rows['off-intl']['enabled'])
        self.assertEqual(rows['off-intl']['status'], 'failed')
        self.assertIn('额度不足', rows['off-intl']['error'])
        self.assertEqual(rows['active-intl']['status'], 'unknown')
        self.assertTrue(rows['active-intl']['enabled'])
        self.assertEqual(result['enabled_count'], 1)
        self.assertEqual([row['uid'] for row in self.setters()], ['new-cn'])
        sent = [data for path, data in self.gateway.calls if path == '/accounts/test']
        self.assertEqual({d['uid'] for d in sent}, {'new-cn', 'off-intl', 'active-cn', 'active-intl'})
        self.assertTrue(all(d.get('model') == MODEL and 'capacity_probe' not in d for d in sent))
        self.assertNotIn('NEVER_DISCLOSE', str(result))

    def test_bad_probe_identity_or_non_success_never_enables(self):
        for response in [
            {'uid': 'wrong', 'model': MODEL, 'ok': True},
            {'uid': 'new-cn', 'model': 'other-model', 'ok': True},
            {'uid': 'new-cn', 'model': MODEL, 'ok': 'true'},
            {'uid': 'new-cn', 'model': MODEL, 'ok': True, 'network': True},
            {'uid': 'new-cn', 'model': MODEL, 'ok': True, 'status': 500},
        ]:
            with self.subTest(response=response):
                self.gateway.probes['new-cn'] = response
                result = self.monitor.refresh_accounts(['new-cn'])
                self.assertEqual(result['accounts'][0]['status'], 'unknown')
                self.assertFalse(self.gateway.accounts[0]['enabled'])
                self.assertEqual(self.setters(), [])

    def test_successful_probe_with_unknown_enable_is_not_reported_as_confirmed(self):
        self.gateway.set_mode = 'not_applied'
        result = self.monitor.refresh_accounts(['new-cn'])
        self.assertEqual(result['enabled_count'], 0)
        self.assertEqual(result['accounts'][0]['status'], 'unknown')
        self.assertIn('待核对', result['accounts'][0]['error'])
        self.assertEqual(len(self.setters()), 1)

    def test_refresh_reloads_new_accounts_and_preserves_their_background_setting(self):
        self.gateway.accounts.append({'uid': 'added-later', 'nickname': '新导入', 'realm': 'intl',
                                      'enabled': False, 'tasksEnabled': True})
        result = self.monitor.refresh_accounts(['added-later'])
        self.assertTrue(result['accounts'][0]['enabled'])
        self.assertTrue(self.gateway.accounts[-1]['tasksEnabled'])

    def test_batches_reject_invalid_or_unknown_ids_without_testing_or_writing(self):
        for uids in ([], ['new-cn'] * 2, ['x'] * 5, ['not-present'], 'new-cn'):
            with self.subTest(uids=uids):
                result = self.monitor.refresh_accounts(uids)
                self.assertEqual(result['status'], 'unavailable')
        self.assertFalse(any(path in ('/accounts/set', '/accounts/test') for path, _ in self.gateway.calls))

    def test_existing_test_lock_blocks_enable_and_batch_refresh(self):
        self.monitor._test_lock.acquire()
        try:
            self.assertEqual(self.monitor.enable_account('new-cn')['status'], 'busy')
            self.assertEqual(self.monitor.refresh_accounts(['new-cn'])['status'], 'busy')
        finally:
            self.monitor._test_lock.release()
        self.assertEqual(self.gateway.calls, [])

    def test_unknown_background_setting_prevents_unsafe_enable(self):
        del self.gateway.accounts[0]['tasksEnabled']
        self.assertEqual(self.monitor.enable_account('new-cn')['status'], 'unknown')
        self.assertEqual(self.setters(), [])


if __name__ == '__main__':
    unittest.main()
