"""Publish the reviewed source and small final evidence to the existing durable project paths."""
import hashlib,json,re,shutil
from pathlib import Path
ROOT=Path(__file__).resolve().parent
project=Path(r'F:\论文预处理系统\预处理系统2').resolve()
migration=project/ROOT.name;repository=project/'article-repository'
assert migration.parent==project and repository.parent==project
migration.mkdir(exist_ok=True);(migration/'validation').mkdir(exist_ok=True)
for source in [*ROOT.glob('*.py'),ROOT/'Dockerfile',ROOT/'.dockerignore',ROOT/'README.md']:
 if source.name!='bundle_sources.py':assert not re.search(r'201314|ragflow-tk|admin-a3ad|4ccddf',source.read_text(encoding='utf-8'))
 shutil.copyfile(source,migration/source.name)
library=ROOT.parent/'qdrant-migration/paperflow_qdrant/library.py'
for name in ('library.py','catalog.py','sync.py'):
 source=library if name=='library.py' else ROOT.parent/'article-repository/article_repository'/name
 shutil.copyfile(source,migration/name)
for name in ('catalog.py','sync.py'):
 shutil.copyfile(ROOT.parent/'article-repository/article_repository'/name,repository/'article_repository'/name)
for name in ('Dockerfile','deploy.py','README.md'):
 shutil.copyfile(ROOT.parent/'article-repository'/name,repository/name)
shutil.copyfile(library,repository/'library.py')
for source in (ROOT.parent/'article-repository/tests').glob('*.py'):
 shutil.copyfile(source,repository/'tests'/source.name)
validation=repository/'validation';validation.mkdir(exist_ok=True)
for source in (ROOT.parent/'article-repository/validation').glob('*'):
 if source.is_file():shutil.copyfile(source,validation/source.name)
for name in ('final-evidence.json','repository-download-validation.json','empty-original-recovery.json','empty-file-plan.json'):
 shutil.copyfile(ROOT/name,migration/'validation'/name)
shutil.copyfile(ROOT/'final-evidence.json',validation/'legacy-migration-final-evidence.json')
shutil.copyfile(ROOT/'repository-download-validation.json',validation/'repository-download-validation.json')
for destination in (migration,repository):
 files=[p for p in destination.rglob('*.py') if '__pycache__' not in p.parts and 'node_modules' not in p.parts]
 files += [destination/'library.py',destination/'README.md']
 manifest={p.relative_to(destination).as_posix():hashlib.sha256(p.read_bytes()).hexdigest() for p in files if p.is_file()}
 (destination/'migration-source-manifest.json').write_text(json.dumps(manifest,indent=2),encoding='utf-8')
print(json.dumps({'migration_source':str(migration),'article_source':str(repository),'large_inputs_copied':False},ensure_ascii=False))
