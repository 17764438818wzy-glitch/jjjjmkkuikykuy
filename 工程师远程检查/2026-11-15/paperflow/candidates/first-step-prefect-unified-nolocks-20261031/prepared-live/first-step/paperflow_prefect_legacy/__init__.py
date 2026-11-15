"""Keep the existing first-step control path available until its cutover."""
from pathlib import Path

# Modules not changed by the new order model continue to come from the
# existing package; changed modules in this package retain the older behavior.
__path__.append(str(Path(__file__).resolve().parent.parent / 'paperflow_prefect'))
