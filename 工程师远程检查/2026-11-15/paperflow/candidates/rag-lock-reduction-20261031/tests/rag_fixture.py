"""Reuse the existing generated fixture; never read production papers."""
from contextlib import redirect_stdout
import io
from pathlib import Path

from paperflow_prefect.records import read_json
from paperflow_prefect.settings import PROJECT


def native_fixture():
    receipt = PROJECT / 'runtime/native-fixture-receipt.json'
    fixture = read_json(receipt) if receipt.is_file() else None
    if fixture is None or not (Path(fixture['root']) / 'body/prepared-publication.json').is_file():
        from native_fixture_check import main
        with redirect_stdout(io.StringIO()): main()
        fixture = read_json(receipt)
    return fixture
