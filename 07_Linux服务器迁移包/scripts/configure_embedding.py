"""Generate a new Linux systemd unit; installing/starting it is a separate command."""
import argparse
import ipaddress
import json
from pathlib import Path
import re
import subprocess

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--root',type=Path,default=Path('/opt/paperflow-migration-embedding'))
    parser.add_argument('--user',required=True)
    parser.add_argument('--group',required=True)
    parser.add_argument('--bridge-ip')
    args=parser.parse_args()
    for v in [args.user,args.group]:
        if not re.fullmatch('[a-z_][a-z0-9_-]*[$]?',v):parser.error('Invalid Linux account name')
    root=args.root.resolve()
    if not re.fullmatch('/[A-Za-z0-9_./-]+',str(root)):parser.error('Use an absolute root without spaces')
    if not (root/'runtime/bin/ollama').is_file() or not (root/'models/manifests/registry.ollama.ai/library/bge-m3/latest').is_file():
        parser.error('Unpack both release archives before configuring the unit')
    address=args.bridge_ip
    if not address:
        p=subprocess.run(['ip','-4','-j','addr','show','docker0'],check=True,capture_output=True,text=True)
        rows=json.loads(p.stdout);addresses=[a['local'] for r in rows for a in r['addr_info'] if a['family']=='inet']
        if len(addresses)!=1:parser.error('Cannot identify docker0 address; pass --bridge-ip')
        address=addresses[0]
    ip=ipaddress.ip_address(address)
    if ip.version!=4 or ip.is_unspecified or ip.is_loopback:parser.error('Use the Docker host gateway IPv4 address')
    text=f'''[Unit]
Description=Paperflow migration bge-m3 embedding
After=network-online.target docker.service
Wants=network-online.target
Requires=docker.service

[Service]
Type=simple
User={args.user}
Group={args.group}
WorkingDirectory={root}
Environment=HOME={root}
Environment=OLLAMA_HOST={address}:11436
Environment=OLLAMA_MODELS={root}/models
Environment=OLLAMA_NUM_PARALLEL=4
Environment=OLLAMA_CONTEXT_LENGTH=8192
Environment=OLLAMA_KEEP_ALIVE=-1
Environment=OLLAMA_NO_CLOUD=1
Environment=OLLAMA_NOPRUNE=true
ExecStart={root}/runtime/bin/ollama serve
Restart=on-failure
RestartSec=3
LimitNOFILE=65535
NoNewPrivileges=true
ProtectSystem=full
ProtectHome=true
PrivateTmp=true
ReadWritePaths={root}

[Install]
WantedBy=multi-user.target
'''
    target=Path(__file__).resolve().parents[1]/'deployment/paperflow-migration-embedding.service'
    with target.open('x',encoding='utf-8',newline='\n') as out:out.write(text)
    print('Generated',target,'; host gateway',address,'; not installed or started.')

if __name__=='__main__':main()
