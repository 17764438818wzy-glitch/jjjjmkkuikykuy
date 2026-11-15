"""Install only the first-step classification rule while that workspace is idle."""
import hashlib
import importlib.util
from pathlib import Path

here = Path(__file__).resolve().parent
base = here.parent
previous = base / 'first-step-direct-delete-20261030' / 'cutover.py'
spec = importlib.util.spec_from_file_location('first_step_cutover', previous)
cutover = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cutover)

source = here / 'paperflow_prefect/group_classification.py'
destination = base / 'first-step-control-20261029/paperflow_prefect/group_classification.py'
current_hash = hashlib.sha256(destination.read_bytes()).hexdigest()
if current_hash != '9c77c49032b3a93018ebcfa201256d287f1175504e5a52442d2a0f17feb2616f':
    raise RuntimeError('Live classification changed; refusing to overwrite it')
cutover.FILES = [(source, destination, Path('backend/paperflow_prefect/group_classification.py'))]
cutover.main()
