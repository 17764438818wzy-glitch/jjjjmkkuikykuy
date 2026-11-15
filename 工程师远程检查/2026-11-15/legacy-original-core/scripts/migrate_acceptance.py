"""Run from the project root with .venv Python; idempotent and scoped to one task."""
import argparse
from pathlib import Path
import sys

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from paperflow.config import Config
from paperflow.migrations import migrate_acceptance
from paperflow.store import Store

parser=argparse.ArgumentParser();parser.add_argument('--config',type=Path,required=True)
args=parser.parse_args();config=Config.load(args.config)
task=migrate_acceptance(config,Store(config.root/'state.sqlite3'))
print(dict(task_id=task['task_id'],stage=task['stage'],original_failed_stage=task['original_failed_stage']))
