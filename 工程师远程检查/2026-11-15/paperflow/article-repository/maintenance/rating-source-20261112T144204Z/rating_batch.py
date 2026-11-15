"""Operator CLI for the single fixed-channel scoring executor."""
import argparse
import json
from pathlib import Path

from rating_executor import FixedChannels,RatingRunner,read_json


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command',choices=['check','status','acceptance','continue','local-prepare','local-publish'])
    parser.add_argument('--registry',required=True)
    parser.add_argument('--manifest',action='append',default=[])
    parser.add_argument('--run-root',required=True)
    parser.add_argument('--api-url',default='http://192.168.1.120:8098')
    parser.add_argument('--supersedes-run-root',help='Explicit prior input-version journal; only terminal or undispatched old attempts may be superseded.')
    args=parser.parse_args()
    fixed=FixedChannels(args.registry)
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
    runner=RatingRunner(args.registry,args.manifest,args.run_root,api_url=args.api_url,supersedes_run_root=args.supersedes_run_root)
    print(json.dumps(runner.run(acceptance_only=args.command=='acceptance'),ensure_ascii=False,indent=2))


if __name__=='__main__':main()
