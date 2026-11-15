"""独立测试库内的真实模型返修验收；不读取正式原稿，不启动持续服务。"""
from pathlib import Path
import sys,time
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from paperflow.config import Config
from paperflow.graph import Workflow
from paperflow.producer import HarnessProducer
from paperflow.package import load,save,sha
from paperflow.service import InstanceLock
from paperflow.store import Store

project=Path(__file__).resolve().parents[1]
home=project/'runtime/integration-live'
home.mkdir(parents=True,exist_ok=True)
probe=load(project/'runtime/acceptance-paper/probe/identity.json')
catalog=Config().catalog
major=dict(next(r for r in catalog['majors'] if r['code']=='120201K'))
major.update(paper_dataset_id=probe['dataset_id'],image_dataset_id=probe['dataset_id'])
save(home/'test-catalog.json',dict(majors=[major],purpose='isolated API acceptance only'))
c=Config(home=str(home),handoff_root=str(project/'runtime/integration-handoff'),source_roots=[],
         catalog_path=str(home/'test-catalog.json'),cleanup=False,short_retry_interval=.1)
c.save(home/'config.json')
source=home/'synthetic-source.txt'
if not source.exists():
    source.write_text('''# 指标分解法研究流程教学范例（虚构技术验收材料）
本材料只演示方法迁移和工作流，不代表真实企业数据或正式研究结论。
作者：张三。学号：202020201040。联系人电话：13800138000。邮箱：test.person@example.com。
## 第一章 研究问题
学习如何分析制造企业盈利能力。选题需要明确企业、期间和对比范围，本范例不提供任何真实企业结论。
## 第二章 方法与材料
采用杜邦分析，将净资产收益率分解为销售净利率、总资产周转率和权益乘数。新题须重新取得财务报表。
准备净利润、营业收入、平均资产和平均权益等字段，统一统计期间及口径，再计算指标、解释关系。
## 第三章 分析步骤
先确定问题与比较对象，再收集并核对新数据；计算分项指标，比较变化，再解释哪个因素推动结果。
图1（文字关系示例）：净资产收益率=销售净利率×总资产周转率×权益乘数。可用关系说明，不需要保存图片。
## 第四章 局限与迁移
此处没有真实数据，不能推断真实公司的盈利状况。可迁移问题组织、字段准备、指标关系和论证顺序。
新题的事实、样本、数值和结论必须重新取得和分析。
''',encoding='utf-8')

class FaultProducer(HarnessProducer):
    def run_stage(self,task,stage):
        result=super().run_stage(task,stage)
        marker=home/'fault-injected.json'
        if stage=='round3_chunk' and not marker.exists():
            path=Path(result['draft_dir'])/'chunks.json';obj=load(path)
            obj['chunks'][0]['section_ids']=['synthetic_missing_section']
            save(path,obj);save(marker,dict(task_id=task['task_id'],stage=stage,fault='invalid section reference'))
        return result

s=Store(home/'state.sqlite3');t=s.register_source(source,sha(source))
with InstanceLock(home/'controller.lock'):
    f=Workflow(c,s,producer=FaultProducer(c));started=time.time()
    try:
        for i in range(40):
            t=f.step(s.get(t['task_id']));save(home/'status-last.json',t)
            print('synthetic_phase',t['stage'],'revision',t['revision'],'error',t.get('error_kind'),'seconds',round(time.time()-started),flush=True)
            if t['stage'] in ('complete','held','parked','code_error','superseded') or t.get('error_kind')=='infrastructure':break
    finally:f.close()
print('synthetic_result',t['stage'],flush=True)
raise SystemExit(0 if t['stage']=='complete' else 1)
