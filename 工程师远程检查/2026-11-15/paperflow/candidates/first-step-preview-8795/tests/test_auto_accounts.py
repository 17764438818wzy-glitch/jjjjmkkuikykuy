"""Domestic-account capacity checks using a fake gateway only."""

import importlib.util
from pathlib import Path
import unittest
from urllib.error import URLError


path = Path(__file__).resolve().parents[1] / "workbuddy_monitor.py"
spec = importlib.util.spec_from_file_location("auto_accounts_monitor", path)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class Gateway:
    def __init__(self):
        self.calls = []
        self.accounts = [
            {"uid": "cn-a", "nickname": "国内甲", "realm": "cn", "enabled": True},
            {"uid": "cn-b", "nickname": "国内乙", "realm": "cn", "enabled": True},
            {"uid": "cn-c", "nickname": "国内丙", "realm": "cn", "enabled": True},
            {"uid": "cn-reasoning", "nickname": "仅思考内容", "realm": "cn", "enabled": True},
            {"uid": "cn-length", "nickname": "未完成回复", "realm": "cn", "enabled": True},
            {"uid": "cn-off", "nickname": "国内停用", "realm": "cn", "enabled": False},
            {"uid": "intl", "nickname": "国际", "realm": "intl", "enabled": True},
        ]
        self.key = {"id": "project", "realm": None, "enabled": True, "accounts": ["cn-a"]}
        self.fail_save = False

    def __call__(self, path, *, token=None, data=None, headers=None, timeout=3):
        self.calls.append((path, data, headers))
        if path == "/accounts?realm=all":
            return {"accounts": [dict(account) for account in self.accounts]}
        if path == "/accounts/test":
            uid = data["uid"]
            if uid == "cn-c":
                raise URLError("unavailable")
            if uid == "cn-reasoning":
                return {"uid": uid, "ok": False, "network": True}
            return {"uid": uid, "ok": uid in {"cn-a", "cn-b"}}
        if path == "/settings/save":
            if self.fail_save:
                raise URLError("save outcome unknown")
            self.key.update(data["api_keys"][0])
            return {"ok": True}
        if path == "/settings":
            return {"api_keys": [dict(self.key)]}
        raise AssertionError(path)


class AutomaticCapacityTests(unittest.TestCase):
    def setUp(self):
        self.gateway = Gateway()
        self.monitor = module.Monitor(self.gateway, prefect_transport=lambda: None, password_loader=lambda: None)
        self.monitor._token = "fake-panel-token"

    def test_only_enabled_domestic_accounts_with_real_responses_count(self):
        result = self.monitor.test_domestic_accounts("deepseek-v4.1-flash")
        self.assertEqual(set(result["usable_uids"]), {"cn-a", "cn-b"})
        statuses = {account["uid"]: account["status"] for account in result["accounts"]}
        self.assertEqual(statuses["cn-c"], "unknown")
        self.assertEqual(statuses["cn-reasoning"], "unknown")
        self.assertEqual(statuses["cn-length"], "failed")
        paths = [path for path, _, _ in self.gateway.calls]
        self.assertEqual(set(paths) - {"/accounts?realm=all"}, {"/accounts/test"})
        self.assertEqual({data["uid"] for path, data, _ in self.gateway.calls if path == "/accounts/test"},
                         {"cn-a", "cn-b", "cn-c", "cn-reasoning", "cn-length"})
        self.assertFalse(any("/settings" in path for path in paths))
        for path, data, headers in self.gateway.calls:
            if path == "/accounts/test":
                self.assertEqual(data["realm"], "cn")
                self.assertEqual(data["model"], "deepseek-v4.1-flash")

    def test_setting_allowlist_retains_other_gateway_keys_and_reads_back(self):
        result = self.monitor.allow_domestic_accounts("project", ["cn-a", "cn-b"])
        self.assertEqual(result["status"], "confirmed")
        saves = [data for path, data, _ in self.gateway.calls if path == "/settings/save"]
        self.assertEqual(saves, [{"api_keys": [{"id": "project", "realm": "cn",
                                               "accounts": ["cn-a", "cn-b"]}],
                                  "deleted_api_key_ids": []}])
        self.assertEqual(self.gateway.calls[-1][0], "/settings")

    def test_no_success_does_not_open_an_unrestricted_allowlist(self):
        result = self.monitor.allow_domestic_accounts("project", [])
        self.assertNotEqual(result["status"], "confirmed")
        self.assertFalse(any(path == "/settings/save" for path, _, _ in self.gateway.calls))

    def test_uncertain_save_is_not_resent(self):
        self.gateway.fail_save = True
        result = self.monitor.allow_domestic_accounts("project", ["cn-a"])
        self.assertEqual(result["status"], "unknown")
        self.assertEqual(sum(path == "/settings/save" for path, _, _ in self.gateway.calls), 1)

    def test_disabled_key_is_not_confirmed(self):
        self.gateway.key["enabled"] = False
        result = self.monitor.allow_domestic_accounts("project", ["cn-a"])
        self.assertEqual(result["status"], "unknown")

    def test_pause_prevents_starting_a_later_batch_of_account_tests(self):
        self.gateway.accounts = [{"uid": f"batch-{i}", "realm": "cn", "enabled": True} for i in range(15)]
        calls = []

        def transport(path, **kwargs):
            if path == "/accounts?realm=all":
                return {"accounts": self.gateway.accounts}
            calls.append(path)
            return {"ok": True}

        self.monitor.transport = transport
        batches = 0

        def before_batch():
            nonlocal batches
            batches += 1
            if batches == 2:
                raise RuntimeError("paused")

        with self.assertRaisesRegex(RuntimeError, "paused"):
            self.monitor.test_domestic_accounts("model", before_batch=before_batch)
        self.assertEqual(len(calls), 10)


if __name__ == "__main__":
    unittest.main()
