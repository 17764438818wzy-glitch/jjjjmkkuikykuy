"""只记录版本和计数，不记录提示词明文、凭据或模型思考内容。"""
import hashlib
from pathlib import Path


def usage_totals(events):
    totals={k:0 for k in ('inputTokens','outputTokens','totalTokens','cacheReadTokens')}
    available=False
    for event in events:
        if event.get('type')!='assistant/message':continue
        usage=event.get('data',{}).get('usage')
        if not isinstance(usage,dict):continue
        available=True
        for key in totals:
            if isinstance(usage.get(key),(int,float)):totals[key]+=usage[key]
    return dict(available=available,**(totals if available else {k:None for k in totals}))


def processing_record(config,stage,prompt,session,seconds,usage):
    from .package import sha
    from .reading import READER_VERSION
    root=Path(__file__).parent
    return dict(stage=stage,session_id=session,seconds=round(seconds,3),usage=usage,
        model=config.model,provider=config.provider,contract_version=config.contract_version,
        reader_version=READER_VERSION,prompt_sha256=hashlib.sha256(prompt.encode('utf-8')).hexdigest(),
        prompt_files={p.name:sha(p) for p in sorted((root/'prompts').glob('*.txt'))},
        protocol_sha256=sha(root/'producer.py'))
