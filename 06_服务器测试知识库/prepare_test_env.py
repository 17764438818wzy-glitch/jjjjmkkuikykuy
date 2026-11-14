# Run manually on the Linux server in this independent directory.
import os,secrets
from pathlib import Path
target=Path(__file__).resolve().parent/'.env'
fd=os.open(target,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
with os.fdopen(fd,'w') as f:
    f.write('QDRANT_API_KEY='+secrets.token_urlsafe(36)+'\n')
    f.write('PAPERFLOW_RAG_API_KEY='+secrets.token_urlsafe(36)+'\n')
print('Created private .env; existing credentials are never overwritten.')
