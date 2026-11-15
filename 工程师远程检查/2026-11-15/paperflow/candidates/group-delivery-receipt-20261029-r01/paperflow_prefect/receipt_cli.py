"""Explicit local receipt command. Never starts the workflow or calls a model."""
import argparse
import json
from pathlib import Path

from .intake_handoff import receive_handoff, batch_summary, UNRECEIVED
from .receipts import ReceiptStore, ReceiptError
from .records import identity


def main(argv=None):
    parser=argparse.ArgumentParser(description='H2.1逐组接收，只登记，不执行下游任务。')
    parser.add_argument('--handoff',action='append',default=[],help='已有分流交接JSON，可重复指定')
    parser.add_argument('--delivery-dir',action='append',default=[],help='已发布的资料组交付目录，可重复指定')
    parser.add_argument('--receipt-root',required=True,help='独立接收账本目录，不得位于原始资料库或旧数据目录中')
    parser.add_argument('--history-root',action='append',default=[],help='只读历史数据目录或inventory.json')
    parser.add_argument('--evidence',action='append',default=[],help='窗口1已有证据记录，可重复指定')
    parser.add_argument('--origin-group-id',help='明确迁移到的既有来源组标识，仅单组接收使用')
    parser.add_argument('--previous-group-root',help='明确的旧组路径，仅单组接收使用')
    args=parser.parse_args(argv)
    sources=[('handoff',p) for p in args.handoff]+[('delivery_dir',p) for p in args.delivery_dir]
    if not sources:parser.error('必须指定 --handoff 或 --delivery-dir')
    if len(sources)>1 and (args.origin_group_id or args.previous_group_root):
        parser.error('迁移映射只能对一份明确交接清单使用')
    store=ReceiptStore(args.receipt_root,history_roots=args.history_root)
    records=[];groups=[]
    for kind,source in sources:
        try:
            result=receive_handoff(source if kind=='handoff' else None,store,
                delivery_dir=source if kind=='delivery_dir' else None,evidence_records=args.evidence,
                origin_group_id=args.origin_group_id,previous_group_root=args.previous_group_root)
            records.append(result)
            groups.append({k:result[k] for k in ('group_id','state','paper_id','record_path','progress')})
        except (OSError,ValueError,KeyError,TypeError) as exc:
            key=identity(str(Path(source).resolve()))
            records.append(dict(origin_group_id=key,state=UNRECEIVED))
            groups.append(dict(handoff=str(source),state=UNRECEIVED,
                               reason=str(exc) if isinstance(exc,ReceiptError) else type(exc).__name__))
    summary=batch_summary(records)
    print(json.dumps(dict(summary=summary,groups=groups),ensure_ascii=False,indent=2))
    return 0 if summary['remaining']==0 else 2


if __name__=='__main__':raise SystemExit(main())
