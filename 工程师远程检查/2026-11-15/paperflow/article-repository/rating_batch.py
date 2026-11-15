"""Operator CLI for the single fixed-channel scoring executor."""
import argparse
import json
from pathlib import Path

from rating_executor import FixedChannels,RatingRunner,read_json


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command',choices=['check','status','acceptance','continue','local-prepare','local-publish','admission-prepare','admission-apply','watch','watch-discover','quarantine-prepare','quarantine-activate'])
    parser.add_argument('--registry',required=True)
    parser.add_argument('--manifest',action='append',default=[])
    parser.add_argument('--run-root',required=True)
    parser.add_argument('--api-url',default='http://192.168.1.120:8098')
    parser.add_argument('--supersedes-run-root',help='Explicit prior input-version journal; only terminal or undispatched old attempts may be superseded.')
    parser.add_argument('--max-workers',type=int)
    parser.add_argument('--per-category-workers',type=int)
    parser.add_argument('--admission-plan')
    parser.add_argument('--legacy-header-attestation')
    parser.add_argument('--classification-registry')
    parser.add_argument('--cache-root')
    parser.add_argument('--bootstrap-updated-since')
    parser.add_argument('--new-wave',type=int,default=6)
    parser.add_argument('--history-wave',type=int,default=6)
    parser.add_argument('--poll-seconds',type=float,default=30)
    parser.add_argument('--max-rounds',type=int)
    parser.add_argument('--discover-pages',type=int,default=1)
    parser.add_argument('--only-article-id',action='append',default=[],help='Explicit bounded acceptance filter; no other article is dispatched.')
    parser.add_argument('--quarantine-decision')
    parser.add_argument('--expected-decision-sha256')
    parser.add_argument('--quarantine-certificate')
    parser.add_argument('--expected-certificate-sha256')
    args=parser.parse_args()
    fixed=FixedChannels(args.registry)
    if args.command in ('quarantine-prepare','quarantine-activate'):
        from rating_quarantine import prepare_certificate,activate_certificate
        if not all((args.classification_registry,args.quarantine_decision,args.expected_decision_sha256,args.quarantine_certificate)):
            parser.error('quarantine requires explicit decision SHA, certificate path and classification registry')
        if args.command=='quarantine-prepare':
            from rating_executor import write_json,sha
            path=Path(args.quarantine_certificate)
            if path.exists():raise ValueError('不得覆盖已冻结隔离证书')
            value=prepare_certificate(args.registry,args.run_root,args.classification_registry,args.quarantine_decision,args.expected_decision_sha256,api_url=args.api_url)
            write_json(path,value)
            result={'status':'prepared_only','certificate_path':str(path.resolve()),'certificate_sha256':sha(path),'model_requests':0,'journal_writes':0}
        else:
            if not args.expected_certificate_sha256:parser.error('activation requires explicit certificate SHA')
            result=activate_certificate(args.registry,args.run_root,args.classification_registry,args.quarantine_decision,args.expected_decision_sha256,args.quarantine_certificate,args.expected_certificate_sha256,api_url=args.api_url)
        print(json.dumps(result,ensure_ascii=False,indent=2));return
    if args.command in ('watch','watch-discover'):
        from rating_watch import RatingWatch
        if not args.classification_registry or not args.cache_root:parser.error('watch requires --classification-registry and --cache-root')
        runner=RatingRunner(args.registry,[],args.run_root,api_url=args.api_url,
            max_workers=args.max_workers,per_category_workers=args.per_category_workers)
        watch=RatingWatch(runner,args.classification_registry,cache_root=args.cache_root,bootstrap_since=args.bootstrap_updated_since)
        watch.run(poll_seconds=args.poll_seconds,max_rounds=args.max_rounds or (1 if args.command=='watch-discover' else None),
            new_wave=args.new_wave,history_wave=args.history_wave,discover_pages=args.discover_pages,
            only_article_ids=args.only_article_id,dispatch=args.command=='watch')
        return
    if args.command in ('admission-prepare','admission-apply'):
        if args.command=='admission-apply' and not args.admission_plan:parser.error('--admission-plan is required')
        runner=RatingRunner(args.registry,[],args.run_root,api_url=args.api_url)
        result=runner.reconcile_admission(apply_plan=args.admission_plan if args.command=='admission-apply' else None,
            legacy_attestation=args.legacy_header_attestation)
        print(json.dumps(result,ensure_ascii=False,indent=2));return
    if args.command in ('local-prepare','local-publish'):
        runner=RatingRunner(args.registry,[],args.run_root,api_url=args.api_url)
        print(json.dumps(runner.reconcile(publish=args.command=='local-publish'),ensure_ascii=False,indent=2));return
    if args.command=='status' and (Path(args.run_root)/'status.json').exists():
        result=read_json(Path(args.run_root)/'status.json')
        result['current_dispatch_gate']=read_json(args.registry).get('dispatch_gate')
        print(json.dumps(result,ensure_ascii=False,indent=2));return
    supplied={read_json(p).get('research_type') for p in args.manifest}
    missing=sorted(set(fixed.channels)-supplied)
    if args.command in ('check','status'):
        registry=read_json(args.registry)
        print(json.dumps({'command':args.command,'dispatch_gate':registry.get('dispatch_gate'),
            'categories_with_input':sorted(supplied),'missing_category_inputs':missing,
            'max_total_scoring_concurrency':fixed.total,'business_requests_sent':0,
            'note':'Configuration/input check only; this command never dispatches or publishes a rating.'},ensure_ascii=False,indent=2))
        return
    if missing:raise ValueError('六类输入未齐，缺少：'+','.join(missing))
    runner=RatingRunner(args.registry,args.manifest,args.run_root,api_url=args.api_url,supersedes_run_root=args.supersedes_run_root,
        max_workers=args.max_workers,per_category_workers=args.per_category_workers)
    print(json.dumps(runner.run(acceptance_only=args.command=='acceptance'),ensure_ascii=False,indent=2))


if __name__=='__main__':main()
