"""Run in the installed RAGFlow container on synthetic ingest.md only."""
import json
import sys
from pathlib import Path
from types import SimpleNamespace

from rag.flow.parser.parser import Parser

outputs={}
probe=SimpleNamespace(callback=lambda *a,**k:None,
    _param=SimpleNamespace(setups={'markdown':{'output_format':'text','delimiter':None,'flatten_media_to_text':True}}),
    set_output=lambda name,value:outputs.__setitem__(name,value))
Parser._markdown(probe,'ingest.md',Path(sys.argv[1]).read_bytes())
text=outputs['text']
checks=['年份=2022','营业收入=100.00','净利润=(10)','同比=-2.50%','占比=[原文空值]','数据来源：公司公开年报','万元','image_0001']
missing=[s for s in checks if s not in text]
Path(sys.argv[2]).write_text(json.dumps({'parser':'Parser._markdown','output_format':outputs['output_format'],'missing':missing,'text':text},ensure_ascii=False,indent=2),encoding='utf-8')
assert not missing, missing
print('PASS: installed RAGFlow Parser._markdown preserved all '+str(len(checks))+' evidence markers.')
