/* Read-only wording for the Manager-owned exact counter. No control requests. */
(function(root){
 'use strict';
 const labels={ready:'就绪',starting:'启动中',backoff:'退避等待',failed:'恢复失败',paused:'已暂停',closed:'已关闭',unconfigured:'未配置'};
 const faults={process_exited:'词表进程退出',timeout:'计数超时',artifact_drift:'工件或规则已变化',identity_mismatch:'词表身份不匹配',invalid_reply:'计数返回非法',startup_unconfirmed:'启动未得到完整身份',exit_unconfirmed:'旧词表退出未确认',health_unconfirmed:'词表健康状态无法确认',restart_limit:'自动恢复次数已用完'};
 function describe(value){
  const v=value||{},state=Object.hasOwn(labels,v.state)?v.state:'unconfigured';
  const restarts=Number.isInteger(v.restarts)&&v.restarts>=0&&v.restarts<=3?v.restarts:0;
  const max=Number.isInteger(v.max_restarts)&&v.max_restarts>=0&&v.max_restarts<=3?v.max_restarts:3;
  let detail=`恢复次数 ${restarts} / ${max}`;
  if(Object.hasOwn(faults,v.failure_category))detail+=` · 最近故障：${faults[v.failure_category]}`;
  if(state==='backoff'&&Number.isFinite(v.retry_in_seconds))detail+=` · ${Math.max(0,Math.ceil(v.retry_in_seconds))} 秒后重试`;
  const impact=state==='ready'?'精确计数可用；每次新资格仍会核对当前服务代。':'依赖精确计数的新处理、封包和投递准入暂缓，不消耗内容纠正次数。已在途写入核对继续，原有保护保持。';
  return {state,label:labels[state],detail,impact};
 }
 const api={describe};
 if(typeof module==='object'&&module.exports)module.exports=api;
 root.PaperflowCounterHealth=api;
})(typeof globalThis==='object'?globalThis:this);
