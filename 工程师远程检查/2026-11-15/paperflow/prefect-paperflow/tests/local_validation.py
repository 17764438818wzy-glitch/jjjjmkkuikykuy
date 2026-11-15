"""Run local regressions with non-loopback socket connections forbidden."""
import io
import json
import os
from pathlib import Path
import socket
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ['PREFECT_SERVER_ANALYTICS_ENABLED'] = 'false'
os.environ['PREFECT_LOGGING_TO_API_ENABLED'] = 'false'

from paperflow_prefect.records import write_json


def main():
    attempts = []
    def guarded(operation):
        def connect(sock, address):
            # Windows asyncio creates its self-pipe through a loopback socketpair.
            if isinstance(address, tuple) and address[0] in ('127.0.0.1', 'localhost', '::1'):
                return operation(sock, address)
            attempts.append('non-loopback socket connection attempted')
            raise AssertionError('Local regression must not connect outside this computer')
        return connect
    output = io.StringIO()
    with patch.object(socket.socket, 'connect', guarded(socket.socket.connect)), \
         patch.object(socket.socket, 'connect_ex', guarded(socket.socket.connect_ex)):
        suite = unittest.defaultTestLoader.discover(str(ROOT / 'tests'))
        result = unittest.TextTestRunner(stream=output, verbosity=2).run(suite)
    text = output.getvalue()
    (ROOT / 'evidence/all-tests-final.txt').write_text(text, encoding='utf8')
    report = dict(tests_run=result.testsRun, failures=len(result.failures), errors=len(result.errors),
                  skipped=len(result.skipped), non_loopback_socket_attempts=len(attempts),
                  passed=result.wasSuccessful() and not attempts)
    write_json(ROOT / 'evidence/local-regression.json', report)
    print(text)
    print(json.dumps(report, indent=2))
    return 0 if report['passed'] else 1


if __name__ == '__main__': raise SystemExit(main())
