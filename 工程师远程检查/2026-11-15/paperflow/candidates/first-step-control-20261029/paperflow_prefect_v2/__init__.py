"""New first-step order model; unchanged modules reuse the existing package."""
from pathlib import Path

__path__.append(str(Path(__file__).resolve().parent.parent / 'paperflow_prefect'))
