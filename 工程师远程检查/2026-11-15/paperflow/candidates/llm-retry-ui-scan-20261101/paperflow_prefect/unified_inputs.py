"""Explicit batch input routing through existing classification and receipt journals."""
from pathlib import Path
from .records import read_json


def receive_inputs(settings, *, receipt_paths=(), handoff_paths=(), person_folders=(),
                   verified_new_groups=(), migrations=None, handoff_evidence=None):
    from .receipts import ReceiptStore
    from .intake_handoff import receive_handoff
    from .stage3_handoff import read_receipt
    settings.require_execution()
    migrations = migrations or {}
    handoff_evidence = handoff_evidence or {}
    supplied = [*receipt_paths, *handoff_paths, *person_folders]
    if not supplied: raise ValueError('必须明确指定个人目录、交接清单或接收回执；不会自动重跑历史批次')
    if len(supplied) > 10000 or len(set(supplied)) != len(supplied):
        raise ValueError('输入数量超过上限或存在重复输入')
    root = settings.path('library_root')
    history = list(dict.fromkeys([str(settings.path('data_root')), *settings.values.get('history_roots', [])]))
    if any(not Path(p).is_dir() for p in history):raise ValueError('历史根必须存在且可读；不能将缺失历史当作新资料')
    store = ReceiptStore(settings.path('receipt_root'), history_roots=history)
    result=[]; sources=[]
    for path in receipt_paths:
        try:
            receipt, _ = read_receipt(path)
            if Path(receipt['library_root']).resolve() != root:
                from .registered_input import verify_registered_receipt
                registered = verify_registered_receipt(path, receipt['receipt_id'])
                if registered['paper_id'] != receipt.get('paper_id'):
                    raise ValueError('接收回执论文身份不符')
            result.append(dict(receipt_path=str(Path(path).resolve()), receipt_id=receipt['receipt_id'],paper_id=receipt.get('paper_id')))
        except Exception as exc:
            result.append(dict(status='incomplete',input_path=str(path),failures={'receipt':type(exc).__name__}))
    for path in handoff_paths:
        try:
            if Path(read_json(path)['library_root']).resolve() != root: raise ValueError('交接资料库不符')
            sources.append(dict(handoff=str(Path(path).resolve()), evidence_handoffs=handoff_evidence.get(path,[])))
        except Exception as exc:
            result.append(dict(status='incomplete',input_path=str(path),failures={'handoff':type(exc).__name__}))
    if person_folders:
        from .group_classification import classify_batch
        from .group_sort_cli import _read_for_sort, _title_from_body
        from .nimble_group import NimbleJudge
        known=settings.values.get('known_paper_ids', {})
        sources.extend(classify_batch(person_folders, root, settings.path('classification_root'),
            lambda: NimbleJudge(endpoint=settings.values.get('nimble_endpoint','http://127.0.0.1:11434/v1/systemone')),
            _read_for_sort, title_reader=_title_from_body, known_paper_ids=known,
            verified_new_groups=verified_new_groups))
    for item in sources:
        path=item.get('handoff') or item.get('unreceived')
        if not path:
            result.append(dict(status='incomplete', group=item.get('group_root'), failures={'classification':item.get('error','unknown')}))
            continue
        try:
            h=read_json(path)
            mapping=migrations.get(h['group_id'], {})
            if set(mapping)-{'origin_group_id','previous_group_root'}: raise ValueError('不支持的迁移字段')
            receipt=receive_handoff(path,store,evidence_records=item.get('evidence_handoffs',[]),**mapping)
            result.append(dict(receipt_path=receipt['record_path'],receipt_id=receipt['receipt_id'],paper_id=receipt.get('paper_id')))
        except Exception as exc:
            result.append(dict(status='incomplete',input_path=path,failures={'receive':type(exc).__name__}))
    ids=[row['receipt_id'] for row in result if row.get('receipt_id')]
    if len(ids)!=len(set(ids)): raise ValueError('多个输入指向同一接收版本，未重复调度')
    paper_ids=[row['paper_id'] for row in result if row.get('paper_id')]
    if len(paper_ids)!=len(set(paper_ids)):raise ValueError('同批不能提交同一论文的多个接收版本或迁移目录')
    return result
