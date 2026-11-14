"""Create new-machine credentials locally; never copy source-server credentials."""
import argparse
import os
from pathlib import Path
import secrets

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--sample-dir',required=True,type=Path)
    parser.add_argument('--public-host',default='127.0.0.1')
    parser.add_argument('--bind-host',default='127.0.0.1')
    parser.add_argument('--state-root',type=Path)
    args=parser.parse_args()
    deployment=Path(__file__).resolve().parents[1]/'deployment'
    path=deployment/'.env'
    sample=args.sample_dir.resolve()
    if not (sample/'catalog.json').is_file():parser.error('--sample-dir must contain catalog.json')
    state=(args.state_root or deployment/'state').resolve()
    values={'PUBLIC_HOST':args.public_host,'BIND_HOST':args.bind_host,'STATE_ROOT':str(state),'SAMPLE_DIR':str(sample),
            'QDRANT_PORT':'16333','GATEWAY_PORT':'18097','ARTICLE_PORT':'18098','PREFECT_PORT':'14200',
            'EMBEDDING_URL':'http://host.docker.internal:11436','ARTICLE_IMPORT_ENABLED':'false',
            'ARTICLE_NAS_ROOT':'/sample/nas','ARTICLE_NAS_USER':'','ARTICLE_NAS_PASSWORD':''}
    for k in ['QDRANT_API_KEY','PAPERFLOW_RAG_API_KEY','ARTICLE_POSTGRES_PASSWORD','PREFECT_POSTGRES_PASSWORD']:
        values[k]=secrets.token_hex(32)
    for v in values.values():
        if any(c in v for c in '\r\n\x00$\'"#'):parser.error('Unsupported character in configuration value')
    with path.open('x',encoding='utf-8',newline='\n') as out:
        out.write(''.join(k+'='+v+'\n' for k,v in values.items()))
    os.chmod(path,0o600)
    print('Created',path,'; credentials are stored in that local file only.')

if __name__=='__main__':main()
