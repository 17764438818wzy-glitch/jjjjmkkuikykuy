"""Folder intake must page a large manifest without spilling a full sort to temp."""
from types import SimpleNamespace

from paperflow.store import Store
from paperflow.workbench_intake import BatchIntake


def test_large_manifest_pages_keep_identity_order_without_a_disk_sort(tmp_path):
    store = Store(tmp_path / 'state.sqlite3')
    intake = BatchIntake(SimpleNamespace(
        store=store, config=SimpleNamespace(article_workers=192, llm_workers=192)))
    total = 22839
    with store.db() as db:
        db.execute('CREATE TABLE web_batches (id TEXT PRIMARY KEY, request_id TEXT)')
        intake.initialize(db)
        db.execute("INSERT INTO web_batches VALUES ('batch', 'request')")
        db.execute("INSERT INTO web_intakes VALUES ('batch', 'fingerprint', 'epoch', 1, 9999999999)")
        db.executemany('INSERT INTO web_intake_items VALUES (?,?,?,?,?,?,?,?,?,?)', (
            ('batch', f'item-{n:05}', n, f'paper-{n:05}.docx',
             ('nested-folder/' * 30) + f'paper-{n:05}.docx', 4096,
             'pending', None, None, '') for n in range(total)))

    for offset in (0, 5800, 22600):
        page = intake.listing('batch', offset, 200)
        assert [r['id'] for r in page['items']] == [
            f'item-{n:05}' for n in range(offset, min(offset + 200, total))]
        assert page['summary']['pending'] == total

    # A fixed page must not require a temporary sort of the whole selected
    # folder. On the affected Windows runtime the spill fails with CANTOPEN.
    with store.db() as db:
        plan = db.execute('EXPLAIN QUERY PLAN SELECT item_id,name,relative_path,bytes,state,input_id,task_id,error '
                          'FROM web_intake_items WHERE batch_id=? ORDER BY position LIMIT ? OFFSET ?',
                          ('batch', 200, 5800)).fetchall()
    assert not any('TEMP B-TREE' in row['detail'] for row in plan)
