import argparse,json,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from paperflow.config import Config
from paperflow.probe import run_probe
parser=argparse.ArgumentParser();parser.add_argument('--config',type=Path)
args=parser.parse_args();report=run_probe(Config.load(args.config))
print(json.dumps(report,ensure_ascii=False,indent=2))
raise SystemExit(0 if report['passed'] else 1)
