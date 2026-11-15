import importlib.util
import json
from pathlib import Path
import threading
import unittest
from unittest import mock
from urllib.request import Request, urlopen


SERVER_PATH = Path(__file__).resolve().parents[1] / "host_resources_api.py"


class HostResourcesApiTest(unittest.TestCase):
    def load_server(self):
        self.assertTrue(SERVER_PATH.is_file(), "host resources API has not been implemented")
        spec = importlib.util.spec_from_file_location("host_resources_api", SERVER_PATH)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def get_response(self, module):
        server = module.ThreadingHTTPServer(("127.0.0.1", 0), module.HostResourcesHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        request = Request(
            f"http://127.0.0.1:{server.server_port}/api/host-resources",
            headers={"Origin": "http://127.0.0.1:8795"},
        )
        with urlopen(request, timeout=5) as response:
            return json.load(response), response.headers

    def test_host_metrics_are_available_through_read_only_http(self):
        module = self.load_server()
        data, headers = self.get_response(module)
        self.assertGreaterEqual(data["cpu_percent"], 0)
        self.assertLessEqual(data["cpu_percent"], 100)
        self.assertGreater(data["memory_total_bytes"], 0)
        self.assertGreaterEqual(data["memory_used_bytes"], 0)
        self.assertIn("gpu_percent", data)
        self.assertEqual(headers["Access-Control-Allow-Origin"], "http://127.0.0.1:8795")
        self.assertEqual(headers["Cache-Control"], "no-store")

    def test_gpu_failure_does_not_hide_cpu_and_memory(self):
        module = self.load_server()
        with mock.patch.object(module.subprocess, "run", side_effect=FileNotFoundError):
            data, _ = self.get_response(module)
        self.assertIsNone(data["gpu_percent"])
        self.assertIsNotNone(data["cpu_percent"])
        self.assertGreater(data["memory_total_bytes"], 0)


if __name__ == "__main__":
    unittest.main()
