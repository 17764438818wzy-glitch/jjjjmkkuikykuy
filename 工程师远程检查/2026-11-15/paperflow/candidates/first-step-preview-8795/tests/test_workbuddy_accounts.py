import importlib.util
from pathlib import Path
import unittest


MODULE_PATH = Path(__file__).resolve().parents[1] / "workbuddy_monitor.py"
spec = importlib.util.spec_from_file_location("workbuddy_accounts_monitor", MODULE_PATH)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class Gateway:
    def __init__(self):
        self.calls = []
        self.accounts = [
            {"uid": "a", "nickname": "甲", "realm": "intl", "enabled": True, "apiKey": '<REDACTED_SECRET>'},
            {"uid": "b", "nickname": "乙", "realm": "intl", "enabled": True},
            {"uid": "c", "nickname": "丙", "realm": "cn", "enabled": False},
            {"uid": "d", "nickname": "丁", "realm": "intl", "enabled": False},
            {"uid": "e", "nickname": "戊", "realm": "cn", "enabled": True},
            {"uid": "f", "nickname": "己", "realm": "cn", "enabled": True},
        ]

    def __call__(self, path, *, token=None, data=None, headers=None, timeout=3):
        self.calls.append((path, data))
        if path == "/accounts?realm=all":
            return {"accounts": [dict(row) for row in self.accounts]}
        if path == "/accounts/test":
            if data != {"realm": "cn"}:
                raise AssertionError("only domestic accounts may be probed")
            return {"ok": True, "results": [
                {"uid": "e", "realm": "cn", "model": "model", "ok": True},
                {"uid": "f", "realm": "cn", "model": "model", "ok": False,
                 "network": True, "error": "local network"},
            ]}
        if path == "/accounts/set":
            for row in self.accounts:
                if row["uid"] == data.get("uid"):
                    row["enabled"] = data.get("enabled")
                    return {"account": dict(row)}
        raise AssertionError(path)


class AccountTest(unittest.TestCase):
    def monitor(self):
        gateway = Gateway()
        monitor = module.Monitor(gateway, prefect_transport=lambda: None, password_loader=lambda: None)
        monitor._token = "panel-token"
        return monitor, gateway

    def test_listing_discloses_no_secrets_and_sends_no_test(self):
        monitor, gateway = self.monitor()
        result = monitor.list_accounts()
        self.assertEqual([a["nickname"] for a in result["accounts"]], ["甲", "乙", "丙", "丁", "戊", "己"])
        self.assertNotIn("apiKey", str(result))
        self.assertEqual([path for path, _ in gateway.calls], ["/accounts?realm=all"])

    def test_batch_only_probes_domestic_and_does_not_blame_account_for_network_failure(self):
        monitor, gateway = self.monitor()
        result = monitor.test_accounts()
        by_id = {a["uid"]: a for a in result["accounts"]}
        self.assertEqual(by_id["a"]["status"], "untested")
        self.assertEqual(by_id["b"]["status"], "untested")
        self.assertEqual(by_id["c"]["status"], "disabled")
        self.assertEqual(by_id["d"]["status"], "disabled")
        self.assertEqual(by_id["e"]["status"], "success")
        self.assertEqual(by_id["f"]["status"], "unknown")
        self.assertEqual([data for path, data in gateway.calls if path == "/accounts/test"], [{"realm": "cn"}])
        self.assertFalse(any(path == "/accounts/set" for path, _ in gateway.calls))

    def test_disable_is_explicit_and_confirmation_is_read_back(self):
        monitor, gateway = self.monitor()
        self.assertFalse(any(path == "/accounts/set" for path, _ in gateway.calls))
        result = monitor.disable_account("a")
        self.assertEqual(result["status"], "disabled")
        self.assertEqual([data for path, data in gateway.calls if path == "/accounts/set"],
                         [{"uid": "a", "enabled": False}])
        self.assertEqual(gateway.accounts[0]["enabled"], False)

    def test_disable_does_not_run_during_account_testing(self):
        monitor, gateway = self.monitor()
        monitor._test_lock.acquire()
        try:
            result = monitor.disable_account("a")
        finally:
            monitor._test_lock.release()
        self.assertEqual(result["status"], "busy")
        self.assertFalse(any(path == "/accounts/set" for path, _ in gateway.calls))


if __name__ == "__main__":
    unittest.main()
