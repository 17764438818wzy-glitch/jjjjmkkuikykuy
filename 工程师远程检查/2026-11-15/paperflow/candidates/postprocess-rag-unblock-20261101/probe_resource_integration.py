"""Run the candidate resource sampler briefly without starting its HTTP server."""
import getpass
import importlib.util
import json
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parent
password = getpass.getpass('') if sys.stdin.isatty() else sys.stdin.readline().rstrip('\r\n')
spec = importlib.util.spec_from_file_location('resource_candidate', ROOT / 'server_resources_api.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
cache = module.NATIVE_CONFIG.parent / 'private' / 'preextract_delivery' / 'native-runtime-observation.json'


class Done(BaseException):
    pass


class Samples:
    def __init__(self):
        self.count = 0
        self.observed = []

    def update(self, value):
        self.count += 1
        if cache.is_file():
            row = json.loads(cache.read_text(encoding='utf8'))
            self.observed.append(row.get('observed_at'))
        if self.count == 9:
            raise Done()

    def clear(self):
        pass


samples = Samples()
try:
    module.sample_loop(password, samples)
except Done:
    timestamps = [value for value in samples.observed if isinstance(value, (float, int))]
    print(json.dumps({'hardware_samples': samples.count, 'distinct_native_observations': len(set(timestamps)),
                      'last_observation_age_seconds': round(time.time() - max(timestamps), 2) if timestamps else None}),
          flush=True)
