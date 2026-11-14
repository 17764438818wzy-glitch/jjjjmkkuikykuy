"""School-first DeepSeek routing with durable capacity and bounded fallback."""
from __future__ import annotations

import time

from ingest_diagnostics import model_failure


class DeepSeekRouter:
    def __init__(self, queue, routes: dict):
        self.queue = queue
        self.routes = routes
        self.total_limit = routes['total_limit']
        self.shared_key = 'shared:' + routes['school']['service_key']

    def _state(self, route):
        defaults = {'limit':route['initial_limit'],'paused_until':0,'halted':False,
                    'valid_decisions':0,'accepted':0,'technical_failures':0,
                    'contract_failures':0,'unobservable_failures':0,'saturated':False,
                    'window_total':0,'window_valid':0,'window_contract':0,'window_start':None}
        defaults.update(self.queue.route_state(route['channel_key']))
        # Fixed quotas must follow current configuration after restart or a
        # capacity change, rather than retain an earlier adaptive limit.
        limit = defaults['limit'] if self.routes.get('adaptive') else route['initial_limit']
        defaults['limit'] = min(route['max_limit'], max(1,limit))
        return defaults

    def _save(self, route, state):
        self.queue.save_route_state(route['channel_key'],state)

    def eligible(self, task):
        return (task.get('policy_key')=='education' and task.get('role')=='paper_primary'
                and task.get('service_key')==self.routes['school']['service_key']
                and task.get('dataset_id')==self.routes['dataset_id']
                and task.get('requested_pipeline_id',task.get('pipeline_id'))==self.routes['source_pipeline_id'])

    def claim(self, owner: str, now: float, external_counts: dict) -> dict | None:
        shared = self.queue.route_state(self.shared_key)
        if shared.get('halted') or shared.get('paused_until',0)>now:
            return None
        for name in ('school','official'):
            route=self.routes[name]
            state=self._state(route)
            if state['halted'] or state['paused_until']>now:
                continue
            task=self.queue.claim_routed_parse(owner,now,120,route,self.total_limit,
                                               state['limit'],external_counts)
            if task:
                return task
            if name=='school' and self.queue.routed_waiting_count(route):
                active=self.queue.list_statuses(('parse_intent','submitted','running','parse_unknown'),100000)
                count=sum(row['service_key']==route['service_key'] and
                          (row['pipeline_id']==route['pipeline_id'] or row['channel_key']==route['channel_key'])
                          for row in active)+external_counts.get(route['channel_key'],0)
                if count>=state['limit']:
                    state['saturated']=True
                    self._save(route,state)
        return None

    def observe(self, task: dict, remote: dict, outcome: str,
                diagnostic: dict | None, now: float) -> bool:
        if not self.eligible(task) or not task.get('execution_attempt'):
            return False
        route=next((self.routes[name] for name in ('school','official')
                    if self.routes[name]['channel_key']==task['channel_key']),None)
        if not route:
            return False
        state=self._state(route)
        if (isinstance(diagnostic,dict) and diagnostic.get('source')=='execution_node'
                and diagnostic.get('component_id')=='Extractor:EducationAnnotation' and diagnostic.get('usage')):
            self.queue.record_parse_usage(task['id'],task['execution_attempt'],diagnostic['usage'])
        valid=outcome in ('accepted','quality_hold','quality_reject')
        contract=((diagnostic or {}).get('kind') in ('annotation_invalid','output_contract')
                  if diagnostic else
                  (remote.get('meta_fields') or {}).get('education_processing_issue')=='annotation_invalid')
        state['window_total']+=1
        state['window_valid']+=int(valid)
        state['window_contract']+=int(contract)
        if state['window_start'] is None:
            state['window_start']=task.get('parse_started_at') or now
        if valid:
            state['valid_decisions']+=1
            state['accepted']+=int(outcome=='accepted')
        else:
            state['technical_failures']+=1
            state['contract_failures']+=int(contract)
        failure=model_failure(remote,diagnostic) if not valid else None
        rerouted=False
        if failure:
            state.update(window_total=0,window_valid=0,window_contract=0,window_start=None,saturated=False)
            if failure['dependency']=='embedding':
                self.queue.save_route_state(self.shared_key,{'halted':True,'reason':'embedding:'+failure['kind']})
            else:
                if failure['kind']=='rate_limit':
                    if self.routes.get('adaptive'):
                        state['limit']=max(1,state['limit']//2)
                    state['paused_until']=now+max(1,failure['retry_after'])
                elif failure['kind'] in ('authentication','arrearage'):
                    state['halted']=True
                else:
                    state['paused_until']=now+max(1,failure['retry_after'])
                if route['channel_key']==self.routes['school']['channel_key'] and outcome=='technical_review':
                    rerouted=self.queue.retry_on_channel(task['id'],'technical_review',
                        self.routes['official'],'llm:'+failure['kind'])
        elif not valid and not contract and not diagnostic:
            state['unobservable_failures']+=1
        if (route['channel_key']==self.routes['school']['channel_key']
                and self.routes.get('adaptive') and not failure
                and state['window_total']>=self.routes.get('window_size',20)):
            rate=state['window_valid']/max(1,now-state['window_start'])
            healthy=(state['window_valid']==state['window_total'] and not state['window_contract']
                     and state['saturated'] and self.queue.routed_waiting_count(route)>0)
            if healthy and not state.get('plateau'):
                previous=state.get('last_rate')
                if previous is not None and rate<=previous*1.05:
                    state['limit']=state.get('previous_limit',state['limit'])
                    state['plateau']=True
                else:
                    state['previous_limit']=state['limit']
                    state['limit']=min(route['max_limit'],self.total_limit-self.routes['official']['initial_limit'],
                                       state['limit']+self.routes.get('growth_step',3))
                    state['last_rate']=rate
            state.update(window_total=0,window_valid=0,window_contract=0,window_start=None,saturated=False)
        self._save(route,state)
        return rerouted

    def snapshot(self) -> dict:
        result={name:self._state(self.routes[name]) for name in ('school','official')}
        result['shared']=self.queue.route_state(self.shared_key)
        result['official_usage']=self.queue.usage_summary(self.routes['official']['channel_key'],time.time())
        return result
