"""Exercise the installed RAGFlow parser with synthetic Markdown only."""
import json
import subprocess
from pathlib import Path

root=Path(__file__).parent/'acceptance-evidence/parser-synthetic'
code='''import sys,json
from types import SimpleNamespace
from rag.flow.parser.parser import Parser
outputs={}
probe=SimpleNamespace(callback=lambda *a,**k:None,
    _param=SimpleNamespace(setups={'markdown':{'output_format':'text','delimiter':None,'flatten_media_to_text':True}}),
    set_output=lambda name,value:outputs.__setitem__(name,value))
Parser._markdown(probe,'ingest.md',sys.stdin.buffer.read())
text=outputs['text']
checks=['年份=2022','营业收入=100.00','净利润=(10)','同比=-2.50%','占比=[原文空值]','数据来源：公司公开年报','万元','image_0001']
missing=[s for s in checks if s not in text]
print(json.dumps({'parser':'installed Parser._markdown','synthetic_only':True,'checks':len(checks),'missing':missing},ensure_ascii=False))
sys.exit(bool(missing))
'''
result=subprocess.run(['docker','exec','-i','-w','/ragflow','docker-ragflow-cpu-1','./.venv/bin/python','-c',code],
    input=(root/'package/ingest.md').read_bytes(),capture_output=True,timeout=60)
output=result.stdout.decode('utf-8',errors='replace')
print(output)
if result.returncode:
    print(result.stderr.decode('utf-8',errors='replace'));raise SystemExit(result.returncode)
(root/'parser-result.json').write_text(output,encoding='utf-8')
