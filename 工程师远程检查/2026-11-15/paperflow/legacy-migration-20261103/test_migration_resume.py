import hashlib,json,sqlite3
from types import SimpleNamespace

import pytest
from haystack_integrations.document_stores.qdrant import QdrantDocumentStore
from paperflow_qdrant.library import Library
from migrate import Migration


def test_superseded_migration_does_not_republish_old_attachment(tmp_path):
    from paperflow_qdrant.identity import stable_id
    migration=Migration.__new__(Migration);migration.ledger=tmp_path/'ledger.sqlite'
    identity=('legacy-document','old-id',None);source_key='folder:known'
    job=stable_id('legacy-migration','tenant',source_key,*identity)
    with sqlite3.connect(migration.ledger) as conn:
        conn.execute('CREATE TABLE result(job_id TEXT,status TEXT)')
        conn.execute('INSERT INTO result VALUES(?,?)',(job,'superseded'))
    migration.snapshot={'tenant':'tenant'}
    migration.payload=lambda identity:{'catalog_records':[{'source_key':source_key}]}
    migration.load=lambda *args:pytest.fail('Superseded source must not be republished')
    assert migration.migrate_group(identity,[])==[]


def test_attachment_recovery_preserves_flat_profile_paper_and_atomic_resume(tmp_path,monkeypatch):
    import repair_attachments as repair
    from article_repository.catalog import Catalog
    from psycopg import sql
    import psycopg
    dsn='postgresql://bench_owner@127.0.0.1:56164/postgres'
    schema='attachment_repair_'+tmp_path.name.replace('-','_')[:35]
    with psycopg.connect(dsn) as conn:
        assert 'pg-classification-isolated-20261103' in conn.execute('SHOW data_directory').fetchone()[0]
        conn.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(schema)))
    cat=Catalog(dsn,schema=schema);cat.initialize();cat.set_majors([{'code':'M','name':'专业'}])
    key='folder:real-paper';aid=hashlib.sha256(key.encode()).hexdigest()[:32]
    paper={'source_key':key,'folder_name':'真实文件夹','title':'真实论文','profile':{'summary':{'text':'保留画像'}},
        'majors':['M'],'source':'qdrant_prefect','qdrant':{'folder_ids':['real-body'],'source_folder_id':'real-body'},'files':[]}
    cat.register(paper)
    doc={'id':'old-note','name':'att_123_note.md','kb_id':'dataset','updated_at':0,'meta':{'paperflow_role':'attachment-note'}}
    text='# 附件说明\n原件编号：old-file\n原件SHA256：'+'a'*64
    rows={'old-note':[{'id':'chunk','content_with_weight':text}]}
    payload={'identity':[None,None,None],'catalog_records':[{'source_key':key,'folder_name':'真实文件夹','profile':{},'files':[]}],
        'originals':{'old-note':text}}
    migration=Migration.__new__(Migration);migration.ledger=tmp_path/'migration.sqlite'
    with sqlite3.connect(migration.ledger) as conn:
        conn.execute('CREATE TABLE result(job_id TEXT PRIMARY KEY,status TEXT,detail TEXT,finished_at TEXT)')
        conn.execute('INSERT INTO result VALUES(?,?,?,?)',('old-main','complete',json.dumps({'article_id':aid,'source_document_ids':['old-note']}),'before'))
        conn.execute("CREATE TRIGGER fail_supersede BEFORE UPDATE ON result WHEN NEW.status='superseded' BEGIN SELECT RAISE(ABORT,'simulated interruption'); END")
    migration.source=SimpleNamespace(metadata=lambda docs:docs,chunks=lambda doc,**kw:rows[doc['id']])
    migration.documents=[doc];migration.groups=lambda:{('legacy-document','old-note',None):[doc]}
    migration.payload=lambda identity:None
    file={'id':'raw','sha256':'a'*64,'relative':'objects/aa/'+('a'*64),'bytes':1,'availability':'available'}
    migration.load=lambda *args:([doc],rows,{'old-file':'a'*64},payload,[file])
    migration.snapshot={'tenant':'tenant'};migration.base_url='http://gateway';migration.catalog=cat
    migration.files=SimpleNamespace(observe=lambda files:files);migration.nas_object=lambda sha:None
    migration.major_by_dataset={'dataset':{'code':'M'}}
    migration.library=Library(QdrantDocumentStore(':memory:',index='paperflow_v1',embedding_dim=4,progress_bar=False),SimpleNamespace(encode=lambda texts:[[1.,0.,0.,0.] for text in texts]))
    monkeypatch.setattr(repair,'Migration',lambda:migration);monkeypatch.setattr(repair,'STATE',tmp_path)
    try:
        assert not repair.run()
        with sqlite3.connect(migration.ledger) as conn:
            assert conn.execute('SELECT job_id,status FROM result').fetchall()==[('old-main','complete')]
            conn.execute('DROP TRIGGER fail_supersede')
        assert repair.run()
        after=cat.raw(aid)['base']
        assert after['profile']==paper['profile'] and after['qdrant']==paper['qdrant']
        assert after.get('entry_kind','article')=='article' and after['files']==[file]
        with sqlite3.connect(migration.ledger) as conn:
            assert sorted(r[0] for r in conn.execute('SELECT status FROM result'))==['complete','superseded']
    finally:
        with psycopg.connect(dsn) as conn:conn.execute(sql.SQL('DROP SCHEMA {} CASCADE').format(sql.Identifier(schema)))
