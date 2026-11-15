import json
import stat
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import nas_status_api as nas


class NasStatusTests(unittest.TestCase):
    def test_reads_configured_directory_and_capacity_without_guessing_speed(self):
        with tempfile.TemporaryDirectory() as temp:
            config = Path(temp) / 'config.json'
            config.write_text(json.dumps({'nas_destination': r'\\nas\archive'}), encoding='utf-8')
            with patch.object(nas.os, 'stat', return_value=SimpleNamespace(st_mode=stat.S_IFDIR)), \
                 patch.object(nas.shutil, 'disk_usage', return_value=SimpleNamespace(total=100, free=75)):
                sample = nas.read_nas_status(config)
        self.assertEqual(sample['status'], 'connected')
        self.assertEqual((sample['storage_used_bytes'], sample['storage_free_bytes']), (25, 75))
        self.assertIsNone(sample['archive_write_bytes_per_second'])

    def test_inaccessible_and_stale_data_do_not_claim_zero(self):
        with tempfile.TemporaryDirectory() as temp:
            config = Path(temp) / 'config.json'
            config.write_text(json.dumps({'nas_destination': r'\\nas\archive'}), encoding='utf-8')
            with patch.object(nas.os, 'stat', side_effect=OSError('unavailable')):
                sample = nas.read_nas_status(config)
        self.assertEqual(sample['status'], 'unavailable')
        self.assertIsNone(sample['storage_free_bytes'])
        now = [10.0]
        samples = nas.NasSamples(clock=lambda: now[0], max_age=3)
        samples.update(sample)
        now[0] = 14.0
        self.assertEqual(samples.read(), nas.empty())

    def test_slow_sample_is_not_reported_as_fresh(self):
        now = iter([0.0, 4.0])
        samples = nas.NasSamples(clock=lambda: 4.0, max_age=3)
        nas.sample_once(samples, reader=lambda: nas.empty('connected'), clock=lambda: next(now))
        self.assertEqual(samples.read(), nas.empty())

    def test_sample_expires_from_start_of_collection(self):
        now = [0.0]
        samples = nas.NasSamples(clock=lambda: now[0], max_age=3)
        def reader():
            now[0] = 2.5
            return nas.empty('connected')
        nas.sample_once(samples, reader=reader, clock=lambda: now[0])
        self.assertEqual(samples.read()['status'], 'connected')
        now[0] = 3.1
        self.assertEqual(samples.read(), nas.empty())


if __name__ == '__main__':
    unittest.main()
