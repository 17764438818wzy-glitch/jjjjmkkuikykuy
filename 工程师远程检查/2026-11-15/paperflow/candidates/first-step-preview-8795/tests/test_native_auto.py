from urllib.error import URLError

import importlib.util
from pathlib import Path

spec = importlib.util.spec_from_file_location("native_monitor_test", Path(__file__).resolve().parents[1] / "workbuddy_monitor.py")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
Monitor = module.Monitor


def test_automatic_start_reuses_native_test_and_only_routes_successful_domestic_accounts():
    calls = []
    accounts = [
        {"uid": "cn-good", "nickname": "good", "realm": "cn", "enabled": True},
        {"uid": "cn-timeout", "nickname": "timeout", "realm": "cn", "enabled": True},
        {"uid": "cn-fail", "nickname": "fail", "realm": "cn", "enabled": True},
        {"uid": "cn-off", "nickname": "off", "realm": "cn", "enabled": False},
        {"uid": "intl", "nickname": "intl", "realm": "intl", "enabled": True},
    ]

    def gateway(path, *, token=None, data=None, headers=None, timeout=3):
        calls.append((path, data))
        if path == "/accounts?realm=all":
            return {"accounts": accounts}
        if path == "/accounts/test":
            assert data["realm"] == "cn" and data["model"] == "paper-model"
            if data["uid"] == "cn-timeout":
                raise URLError("timeout")
            return {"uid": data["uid"], "ok": data["uid"] == "cn-good"}
        if path.startswith("/u/"):
            return {"choices": [{"message": {"content": "OK"}, "finish_reason": "stop"}]}
        raise AssertionError(path)

    monitor = Monitor(gateway, prefect_transport=lambda: None, password_loader=lambda: None)
    monitor._token = "fake"
    result = monitor.test_domestic_accounts("paper-model")

    assert result["usable_uids"] == ["cn-good"]
    statuses = {row["uid"]: row["status"] for row in result["accounts"]}
    assert statuses["cn-timeout"] == "unknown"
    assert statuses["cn-fail"] == "failed"
    assert {data["uid"] for path, data in calls if path == "/accounts/test"} == {
        "cn-good", "cn-timeout", "cn-fail"
    }
    assert not any(path.startswith("/u/") for path, _ in calls)
