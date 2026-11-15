import importlib.util
from pathlib import Path
import threading
import unittest


MODULE_PATH = Path(__file__).resolve().parents[1] / "workbuddy_monitor.py"


def load_monitor(test):
    test.assertTrue(MODULE_PATH.exists(), "the WorkBuddy monitor must exist")
    spec = importlib.util.spec_from_file_location("workbuddy_monitor", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class FakeGateway:
    def __init__(self):
        self.calls = []

    def __call__(self, path, *, token=None, data=None, headers=None, timeout=3):
        self.calls.append((path, token, data, headers))
        if path == "/health":
            return {"ok": True, "realm": "intl", "accounts": 6, "accounts_ready": 4}
        if path == "/panel/login":
            return {"ok": True, "token": "test-session"}
        if path.startswith("/usage/analytics"):
            return {"summary": {"window": {"prompt_tokens": 1200, "completion_tokens": 50,
                                            "requests": 3, "errors": 1}}}
        if path.startswith("/usage/recent"):
            return {"rows": [{"model": "sample-model", "realm": "intl"}]}
        if path.startswith("/v1/balance-summary"):
            return {"balance": 90, "accounts_enabled": 6, "accounts_with_credit_data": 5}
        if path == "/v1/chat/completions":
            return {"choices": [{"message": {"content": "OK"}}]}
        raise AssertionError(path)


class MonitorTests(unittest.TestCase):
    def test_connection_without_login_never_calls_a_model(self):
        module = load_monitor(self)
        gateway = FakeGateway()
        monitor = module.Monitor(gateway, prefect_transport=lambda: None, password_loader=lambda: None)
        state = monitor.summary()
        self.assertTrue(state["connected"])
        self.assertFalse(state["authorized"])
        self.assertEqual((state["accounts_ready"], state["accounts_total"]), (4, 6))
        self.assertIsNone(state["input_tokens"])
        self.assertIsNone(state["in_flight"])
        self.assertEqual([call[0] for call in gateway.calls], ["/health"])

    def test_authorized_summary_uses_real_fields_and_preserves_uncertainty(self):
        module = load_monitor(self)
        gateway = FakeGateway()
        monitor = module.Monitor(gateway, prefect_transport=lambda: None, password_loader=lambda: None)
        self.assertTrue(monitor.login("example"))
        state = monitor.summary()
        self.assertEqual((state["input_tokens"], state["output_tokens"]), (1200, 50))
        self.assertTrue(state["usage_available"])
        self.assertEqual((state["successful_requests"], state["failed_requests"]), (3, 1))
        self.assertEqual((state["recent_model"], state["recent_realm"]), ("sample-model", "intl"))
        self.assertEqual(state["credit_balance"], 90)
        self.assertTrue(state["credits_partial"])
        self.assertIsNone(state["in_flight"])
        self.assertNotIn("token", state)

    def test_only_an_explicit_test_sends_one_model_request(self):
        module = load_monitor(self)
        gateway = FakeGateway()
        monitor = module.Monitor(gateway, prefect_transport=lambda: None, password_loader=lambda: None)
        monitor.login("example")
        monitor.summary()
        self.assertFalse(any(call[0] == "/v1/chat/completions" for call in gateway.calls))
        result = monitor.test_line("sample-model", "intl")
        self.assertEqual(result["status"], "success")
        requests = [call for call in gateway.calls if call[0] == "/v1/chat/completions"]
        self.assertEqual(len(requests), 1)
        self.assertEqual(requests[0][3]["X-Realm"], "intl")
        self.assertFalse(any(call[0].startswith("/accounts/test") for call in gateway.calls))

    def test_auto_connects_and_labels_prefect_slots_separately_from_gateway_requests(self):
        module = load_monitor(self)
        gateway = FakeGateway()
        limit = {"name": "paperflow-connected-total_llm", "active": True,
                 "active_slots": 0, "limit": 50, "slot_decay_per_second": 0.0}
        monitor = module.Monitor(gateway, prefect_transport=lambda: limit,
                                 password_loader=lambda: "example")
        state = monitor.summary()
        self.assertTrue(state["authorized"])
        self.assertEqual((state["prefect_active_slots"], state["concurrency_limit"]), (0, 50))
        self.assertIsNone(state["in_flight"])
        self.assertEqual(sum(call[0] == "/panel/login" for call in gateway.calls), 1)
        self.assertFalse(any(call[0] == "/v1/chat/completions" for call in gateway.calls))

    def test_empty_model_response_is_not_reported_as_success(self):
        module = load_monitor(self)
        gateway = FakeGateway()
        def transport(path, **kwargs):
            if path == "/v1/chat/completions":
                return {"choices": [{"finish_reason": "error"}]}
            return gateway(path, **kwargs)
        monitor = module.Monitor(transport, prefect_transport=lambda: None, password_loader=lambda: None)
        monitor.login("example")
        monitor.summary()
        self.assertEqual(monitor.test_line("sample-model", "intl")["status"], "failed")

    def test_gateway_and_prefect_reads_do_not_block_each_other(self):
        module = load_monitor(self)
        gateway = FakeGateway()
        prefect_started = threading.Event()
        def transport(path, **kwargs):
            if path == "/health" and not prefect_started.wait(0.3):
                raise TimeoutError("Prefect read did not start alongside gateway read")
            return gateway(path, **kwargs)
        def prefect():
            prefect_started.set()
            return None
        monitor = module.Monitor(transport, prefect_transport=prefect, password_loader=lambda: None)
        self.assertTrue(monitor.summary()["connected"])


if __name__ == "__main__":
    unittest.main()
