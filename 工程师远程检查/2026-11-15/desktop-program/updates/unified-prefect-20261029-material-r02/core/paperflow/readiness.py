"""持续服务的验收约束；有限单篇测试不受此启动门槛影响。"""
import hashlib
from pathlib import Path
from .package import load

CHECKS=('production_paper','automatic_repair','ingest_process_restart','version_visibility','controller_fault','clean_pytest','api_probe')

class NotReady(RuntimeError):pass

def fingerprint(config):
    root=Path(__file__).resolve().parents[1];h=hashlib.sha256(config.contract_version.encode())
    for folder in ('paperflow','tests'):
        for p in sorted((root/folder).rglob('*')):
            if p.is_file() and p.suffix in ('.py','.txt','.yml','.json'):
                h.update(p.relative_to(root).as_posix().encode());h.update(p.read_bytes())
    h.update((config.rag_url+'|'+config.provider+'|'+config.model).encode())
    h.update((root/'pyproject.toml').read_bytes())
    catalog=Path(config.catalog_path)
    h.update(catalog.read_bytes() if catalog.is_file() else str(catalog).encode())
    return h.hexdigest()

def readiness(config):
    path=Path(config.acceptance_report) if config.acceptance_report else config.root/'acceptance-report.json'
    try:report=load(path)
    except (OSError,ValueError):return dict(ready=False,missing=list(CHECKS),reason='尚未完成全部验收')
    missing=[k for k in CHECKS if report.get('checks',{}).get(k) is not True]
    if report.get('contract_version')!=config.contract_version or report.get('rules_sha256')!=fingerprint(config):
        missing.append('current_code_version')
    return dict(ready=not missing,missing=missing,reason='验收已通过' if not missing else '当前版本仍有验收项未通过')

def require_ready(config):
    result=readiness(config)
    if not result['ready']:raise NotReady(result['reason']+'：'+', '.join(result['missing']))
