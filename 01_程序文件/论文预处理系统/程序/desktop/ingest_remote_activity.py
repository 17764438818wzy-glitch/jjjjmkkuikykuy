"""Count externally executing education docs without double-counting queue work."""
from __future__ import annotations

import time
from urllib.parse import quote


class RagflowInFlightCounts:
    def __init__(self, client_factory, queue, routes: dict, refresh_seconds: float = 15):
        self.client_factory=client_factory
        self.queue=queue
        self.routes=routes
        self.refresh_seconds=refresh_seconds
        self._snapshot=[]
        self._at=None
        self._identities_at=None
        self._pipeline_identities={}
        self._model_identities=None

    def __call__(self) -> dict:
        now=time.monotonic()
        if self._at is None or now-self._at>=self.refresh_seconds:
            if self._identities_at is None or now-self._identities_at>=300:
                self._pipeline_identities={}
                self._model_identities=None
                self._identities_at=now
            client=self.client_factory()
            snapshot=[]
            seen=set()
            expected_total=None
            dataset=quote(self.routes['dataset_id'],safe='')
            page=1
            while True:
                payload=client.request('GET',f'/datasets/{dataset}/documents?page={page}&page_size=100&run=RUNNING')
                docs=payload.get('docs') if isinstance(payload,dict) else None
                total=payload.get('total') if isinstance(payload,dict) else None
                if (not isinstance(docs,list) or type(total) is not int or total<0
                        or (expected_total is not None and total!=expected_total)):
                    raise ValueError('Remote activity inventory is incomplete')
                expected_total=total
                if any(not isinstance(row,dict) or row.get('run')!='RUNNING' for row in docs):
                    raise ValueError('Remote activity RUNNING filter was not honored')
                for row in docs:
                    if not isinstance(row.get('id'),str) or not row['id'] or row['id'] in seen:
                        raise ValueError('Remote activity inventory is incomplete')
                    seen.add(row['id'])
                    snapshot.append({'id':row['id'],'channels':self._pipeline_channels(client,row.get('pipeline_id'))})
                if len(snapshot)>total or (len(docs)<100 and len(snapshot)<total):
                    raise ValueError('Remote activity inventory is incomplete')
                if len(snapshot)==total:
                    break
                page+=1
            self._snapshot=snapshot
            self._at=now
        owned={row['remote_id'] for row in self.queue.list_statuses(
            ('parse_intent','submitted','running','parse_unknown'),100000)
               if (row['service_key']==self.routes['school']['service_key']
                   and row['dataset_id']==self.routes['dataset_id'] and row.get('remote_id'))}
        result={'total':0,self.routes['school']['channel_key']:0,self.routes['official']['channel_key']:0}
        for row in self._snapshot:
            if row['id'] in owned:
                continue
            result['total']+=1
            for channel in row['channels']:
                result[channel]+=1
        return result

    def _pipeline_channels(self,client,pipeline_id):
        if pipeline_id not in self._pipeline_identities:
            self._pipeline_identities[pipeline_id]=self._resolve_pipeline_channels(client,pipeline_id)
        return self._pipeline_identities[pipeline_id]

    def _resolve_pipeline_channels(self,client,pipeline_id):
        possible={self.routes[name]['channel_key'] for name in ('school','official')}
        if not isinstance(pipeline_id,str) or not pipeline_id:
            return possible
        for name in ('school','official'):
            route=self.routes[name]
            if pipeline_id==route['pipeline_id']:
                return {route['channel_key']}
        definition=client.request('GET','/agents/'+quote(pipeline_id,safe=''))
        if not isinstance(definition,dict) or definition.get('id')!=pipeline_id:
            raise ValueError('Remote Pipeline identity is unverified')
        models={component.get('obj',{}).get('params',{}).get('llm_id')
                for component in definition['dsl']['components'].values()}
        models.discard(None)
        if not models:
            return possible
        channels=set()
        for model_id in models:
            known={self.routes[name]['channel_key'] for name in ('school','official')
                   if self.routes[name]['model_id']==model_id}
            if known:
                channels.update(known)
                continue
            if self._model_identities is None:
                identities={}
                for row in client.request('GET','/models'):
                    model=row.get('model_id')
                    if not isinstance(model,str) or not model or model in identities:
                        raise ValueError('Remote model registry identity is unverified')
                    identities[model]=(row.get('provider_name'),row.get('instance_name'))
                self._model_identities=identities
            identity=self._model_identities.get(model_id)
            if not identity or not all(identity):
                channels.update(possible)
                continue
            channels.update(self.routes[name]['channel_key'] for name in ('school','official')
                            if (self.routes[name]['provider'],self.routes[name]['instance'])==
                            identity)
        return channels
