"""Exercise the actual local Nimble API with contrasting synthetic examples."""
import json
import math
import os
from pathlib import Path
import time
import urllib.request

ROOT = Path(__file__).resolve().parent
BASE = "http://127.0.0.1:11434"
HTTP = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def request(path, body=None):
    data = None if body is None else json.dumps(body, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(BASE + path, data=data, headers={"Content-Type": "application/json"})
    with HTTP.open(req, timeout=300) as response:
        return json.load(response)


def main():
    template = json.loads((ROOT / "examples/classification-zh.json").read_text(encoding="utf-8-sig"))
    cases = [
        ("refund_zh", template["state"], "billing", True),
        ("login_error_zh", "登录页面一直显示错误500，我无法进入系统。请修复登录功能。我没有申请退款，也不需要退款。", "technical", False),
    ]
    receipt = {"url": BASE, "version": request("/api/version"), "models": request("/api/tags"), "cases": []}
    for name, state, expected_department, expected_refund in cases:
        payload = dict(template, state=state)
        started = time.perf_counter()
        result = request("/v1/systemone", payload)
        elapsed = time.perf_counter() - started
        answers = result["answers"]
        assert answers["department"]["choice"] == expected_department, result
        probability = answers["refund"]["noul"]
        assert 0 <= probability <= 1, result
        assert (probability > 0.5) == expected_refund, result
        assert 0 <= answers["urgency"]["score"] <= 2, result
        for field in ("department", "urgency"):
            probabilities = list(answers[field]["probabilities"].values())
            assert all(math.isfinite(value) and 0 <= value <= 1 for value in probabilities), result
            assert math.isclose(sum(probabilities), 1, abs_tol=1e-3), result
        assert result["usage"]["input_tokens"] > 0, result
        receipt["cases"].append({"name": name, "elapsed_seconds": round(elapsed, 3), "state": state, "response": result})
        print(json.dumps({"name": name, "seconds": round(elapsed, 3), "department": expected_department, "refund_probability": probability, "passed": True}, ensure_ascii=False), flush=True)
    receipt["loaded_models"] = request("/api/ps")
    receipt["passed"] = True
    receipt["completed_unix"] = time.time()
    destination = ROOT / "logs/verification.json"
    destination.write_text(json.dumps(receipt, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Verification receipt: {destination}", flush=True)


if __name__ == "__main__":
    main()
