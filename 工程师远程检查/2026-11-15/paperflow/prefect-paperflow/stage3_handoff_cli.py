"""Explicit H2.1 check, history reconciliation, or third-stage execution."""
import argparse
import asyncio
import json


def main():
    parser = argparse.ArgumentParser(description='第三状态 H2.1；交付原附件和整合 attachment.md')
    parser.add_argument('mode', choices=['check', 'confirm-first-request', 'process'])
    parser.add_argument('--receipt', required=True)
    parser.add_argument('--file-id', help='confirm-first-request 核对单项；process 仅处理指定的已接收附件，不处理正文或其他附件')
    parser.add_argument('--config', help='显式使用已保存的运行配置，不修改主配置；仍检查 execution_enabled')
    parser.add_argument('--history-root', action='append', default=[], help='需核对的历史运行根目录，可重复')
    args = parser.parse_args()
    from paperflow_prefect.stage3_handoff import read_receipt
    if args.mode == 'check':
        receipt, paper = read_receipt(args.receipt)
        print(json.dumps({'receipt_id':receipt['receipt_id'], 'receipt_state':receipt['state'],
            'progress':receipt['progress'], 'paper_id':paper['paper_id'] if paper else None,
            'model_requests_added':0},ensure_ascii=False,indent=2)); return
    if args.mode == 'confirm-first-request':
        if not args.file_id or not args.history_root:
            parser.error('首次请求核对需明确 --file-id 和 --history-root；空历史不会自动准许请求')
        from paperflow_prefect.backend import Backend
        from paperflow_prefect.settings import Settings
        from paperflow_prefect.request_admission import confirm_first_request, bind_work
        settings=Settings.load(args.config) if args.config else Settings.load()
        settings.require_execution()
        receipt,paper=read_receipt(args.receipt)
        if paper is None: parser.error('回执没有已接收材料')
        row=next((r for r in paper['archive_files'] if r['file_id']==args.file_id),None)
        if row is None: parser.error('指定编号不属于已接收项')
        backend=Backend(settings)
        work=backend.body_work(paper) if row['kind']=='body' else backend.attachment_work(paper,row)
        roots=list(dict.fromkeys([str(backend.root),*args.history_root]))
        result=confirm_first_request(paper,row,work,history_roots=roots)
        bind_work(backend.root,paper,row,row['kind'],work)
        print(json.dumps(result,ensure_ascii=False,indent=2)); return
    from paperflow_prefect.flows import received_handoff
    result=asyncio.run(received_handoff(args.receipt,attachment_id=args.file_id,config_path=args.config))
    print(json.dumps(result,ensure_ascii=False,indent=2))


if __name__ == '__main__': main()
