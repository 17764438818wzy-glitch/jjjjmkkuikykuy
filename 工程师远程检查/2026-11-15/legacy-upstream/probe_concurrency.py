"""Probe the configured vision relay before enabling production concurrency.

This script sends only the AgentClient synthetic four-colour image.  It never
reads a paper, writes a knowledge package, or prints the API key.
"""
import argparse
import io
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

from PIL import Image, ImageDraw

from agent_client import AgentClient


def synthetic_image():
    image=Image.new('RGB',(160,160),'white')
    draw=ImageDraw.Draw(image)
    for colour,box in zip(['red','green','blue','yellow'],[(0,0,79,79),(80,0,159,79),(0,80,79,159),(80,80,159,159)]):
        draw.rectangle(box,fill=colour)
    output=io.BytesIO();image.save(output,format='PNG');return output.getvalue()


def one_call(client,blob):
    started=time.monotonic()
    try:
        result=client.call('vision_probe',{'test':'synthetic_image_only'},[blob])
        return {'ok':result.get('decision')=='pass','status':200,'seconds':time.monotonic()-started,'error':''}
    except Exception as exc:
        message=str(exc)
        status=429 if '429' in message else 0
        return {'ok':False,'status':status,'seconds':time.monotonic()-started,'error':message[:240]}


def probe_level(config,key,vision_key,level,rounds):
    blob=synthetic_image();started=time.monotonic();rows=[]
    def run():
        client=AgentClient(config,key,vision_key=vision_key)
        return one_call(client,blob)
    with ThreadPoolExecutor(max_workers=level,thread_name_prefix=f'probe-{level}') as pool:
        futures=[pool.submit(run) for _ in range(rounds)]
        for future in as_completed(futures): rows.append(future.result())
    elapsed=time.monotonic()-started;success=sum(row['ok'] for row in rows);rate=success/len(rows) if rows else 0
    return {'concurrency':level,'total':len(rows),'success':success,'failed':len(rows)-success,
            '429':sum(row['status']==429 for row in rows),'average_seconds':round(sum(row['seconds'] for row in rows)/max(1,len(rows)),3),
            'elapsed_seconds':round(elapsed,3),'throughput_per_second':round(success/max(elapsed,1e-9),3),
            'success_rate':round(rate,4),'errors':[row['error'] for row in rows if row['error']][:5]}


def main(argv=None):
    parser=argparse.ArgumentParser(description='探测图片识别中转站的安全并发度')
    parser.add_argument('--base-url',required=True)
    parser.add_argument('--api-key',required=True)
    parser.add_argument('--vision-key',default='')
    parser.add_argument('--model',required=True)
    parser.add_argument('--vision-base-url',default='')
    parser.add_argument('--rounds',type=int,default=20)
    parser.add_argument('--levels',default='4,8,16,32')
    parser.add_argument('--attempts',type=int,default=4)
    parser.add_argument('--timeout',type=int,default=90)
    parser.add_argument('--backoff',type=float,default=1.0)
    parser.add_argument('--output',default='concurrency_probe.json')
    args=parser.parse_args(argv)
    if args.rounds<1 or args.attempts<1: parser.error('rounds 和 attempts 必须大于 0')
    levels=[]
    for value in args.levels.split(','):
        try: level=int(value)
        except ValueError: parser.error('levels 必须是逗号分隔的整数')
        if level<1: parser.error('levels 必须大于 0')
        levels.append(level)
    config={'base_url':args.base_url,'vision_base_url':args.vision_base_url,'vision_model':args.model,
            'timeout':args.timeout,'attempts':args.attempts,'max_tokens':512,
            'concurrency':{'enabled':False,'max_retries':args.attempts,'backoff_base_seconds':args.backoff}}
    rows=[probe_level(config,args.api_key,args.vision_key,level,args.rounds) for level in levels]
    eligible=[row for row in rows if row['success_rate']>=.99]
    recommendation=max(eligible,key=lambda row:row['throughput_per_second']) if eligible else None
    report={'version':1,'measured_at':datetime.now(timezone.utc).isoformat(),'rounds':args.rounds,
            'levels':rows,'recommendation':recommendation['concurrency'] if recommendation else None,
            'recommendation_reason':'最高吞吐且成功率不低于 99%' if recommendation else '没有并发档位达到 99% 成功率'}
    output=Path(args.output);output.write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
    print('并发\t成功\t429\t平均耗时\t吞吐（次/秒）')
    for row in rows:
        print(f"{row['concurrency']}\t{row['success']}/{row['total']}\t{row['429']}\t{row['average_seconds']:.3f}s\t{row['throughput_per_second']:.3f}")
    print('推荐并发：'+(str(report['recommendation']) if report['recommendation'] else '无（请降低并发或检查中转站）'))
    print('详细结果：'+str(output.resolve()))
    return 0


if __name__=='__main__': raise SystemExit(main())
