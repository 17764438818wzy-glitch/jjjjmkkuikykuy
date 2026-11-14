"""Loopback workbench behind the existing LAN proxy; always starts paused."""
import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
if (ROOT / '.webdeps').is_dir():
    sys.path.insert(0, str(ROOT / '.webdeps'))


def main():
    parser = argparse.ArgumentParser(description='Paperflow isolated control workbench')
    parser.add_argument('--config',required=True)
    parser.add_argument('--root',required=True,help='Explicit isolated runtime root')
    parser.add_argument('--legacy-root',default=r'E:\rag mid')
    parser.add_argument('--admin',required=True,help='Existing workbench admin hash file')
    parser.add_argument('--port',type=int,default=8099)
    parser.add_argument('--deployment',help='Owned operator deployment manifest; no automatic resume')
    parser.add_argument('--public-host',action='append',default=[],help='Exact trusted proxy Host names')
    args = parser.parse_args()
    if not 1024 <= args.port <= 65535:
        parser.error('port must be 1024..65535')
    if not Path(args.admin).is_file():
        parser.error('existing admin hash file required')
    from paperflow.config import Config
    from paperflow.workbench import Workbench
    from webworkbench.api import create_app
    import uvicorn
    workbench = Workbench(Config.load(args.config),args.root,legacy_root=args.legacy_root,deployment=args.deployment)
    hosts=['localhost','127.0.0.1','[::1]']+args.public_host
    if args.deployment:
        from paperflow.package import load
        item=load(args.deployment)
        if args.public_host != [item.get('public_host')]:
            parser.error('public host must match owned deployment manifest')
    print(f'Workbench: http://127.0.0.1:{args.port}/ (starts paused)',flush=True)
    uvicorn.run(create_app(workbench,args.admin,allowed_hosts=hosts),host='127.0.0.1',port=args.port,
                proxy_headers=True,forwarded_allow_ips='127.0.0.1',access_log=False,log_level='warning')


if __name__ == '__main__':
    main()
