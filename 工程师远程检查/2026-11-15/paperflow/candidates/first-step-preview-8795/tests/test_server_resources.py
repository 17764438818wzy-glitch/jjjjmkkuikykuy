import importlib.util
from pathlib import Path
import unittest


MODULE_PATH = Path(__file__).resolve().parents[1] / 'server_resources_api.py'


def load_module(test):
    test.assertTrue(MODULE_PATH.exists(), 'server resource adapter must exist')
    spec = importlib.util.spec_from_file_location('server_resources_api', MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ServerResourcesTests(unittest.TestCase):
    def test_normalizes_live_server_values_and_preserves_zero(self):
        module = load_module(self)
        result = module.normalize({'cpu_percent': 12.5, 'memory_total': 8 * 1024**3,
            'memory_available': 2 * 1024**3, 'gpu_status': 'ok',
            'gpus': [{'total': 8 * 1024**3, 'used': 1024**3, 'utilization': 0}]})
        self.assertEqual(result['cpu_percent'], 12.5)
        self.assertEqual(result['memory_percent'], 75)
        self.assertEqual(result['memory_used_bytes'], 6 * 1024**3)
        self.assertEqual(result['gpu_percent'], 0)
        self.assertEqual(result['gpu_memory_used_mib'], 1024)

    def test_unavailable_and_old_data_never_appear_as_zero(self):
        module = load_module(self)
        ticks = [100.0]
        samples = module.ResourceSamples(clock=lambda: ticks[0])
        samples.update({'cpu_percent': 4, 'memory_total': 1024, 'memory_available': 512,
                        'gpu_status': 'unavailable', 'gpus': []})
        self.assertEqual(samples.read()['cpu_percent'], 4)
        self.assertIsNone(samples.read()['gpu_percent'])
        ticks[0] += 5
        self.assertIsNone(samples.read()['cpu_percent'])


if __name__ == '__main__':
    unittest.main()
