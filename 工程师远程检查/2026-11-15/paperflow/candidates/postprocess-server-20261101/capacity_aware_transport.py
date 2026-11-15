"""Unconnected transport candidate for definite capacity-busy admissions.

The exact saved start intent and current in-process preflight remain required.
Only a matching explicit no-start capacity response keeps that preflight alive;
lost or ambiguous replies permanently make this in-process attempt unknown.
"""
from remote_server_transport import RemoteServerTransport, TransportError, _copy, _json, _require


class CapacityAwareTransport(RemoteServerTransport):
    def coordinator_callbacks(self):
        callbacks = super().coordinator_callbacks()
        callbacks['submit_once'] = self.submit_attempt
        return callbacks

    def submit_attempt(self, job, snapshot, staging):
        self._intent(job, snapshot, staging, started=True)
        context = self._contexts.get(job['job_id'])
        _require(context and context.get('staging') == staging,
                 'the original in-process preflight is unavailable')
        previous = self._submitted.get(job['job_id'])
        if context.get('state') == 'prepared':
            _require(previous is None, 'the original admission has already been attempted')
            submitted = dict(job=_copy(job), state='admitting')
            _require(self._submitted.setdefault(job['job_id'], submitted) is submitted,
                     'the original admission has already been attempted')
        else:
            _require(context.get('state') == 'capacity_busy' and previous
                     and _json(previous.get('job')) == _json(job) and previous.get('state') == 'busy',
                     'only an authenticated no-start capacity response permits another attempt')
            submitted = previous
            submitted['state'] = 'admitting'
        context['state'] = 'admitting'
        try:
            preflight = context['preflight']
            response = self._rpc('admit', dict(job=job, preflight_id=preflight['preflight_id'],
                preflight_sha256=preflight['preflight_sha256']),
                timeout=self._config['admit_timeout_seconds'], small=True)
            self._bound(response, job)
            _require(_json(response.get('job')) == _json(job)
                     and _json(response.get('server_identity')) == _json(self._server)
                     and type(response.get('accepted')) is bool, 'server admission result is unconfirmed')
            accepted = dict(job=job, server_identity=self._server, accepted=True)
            reason = response.get('reason')
            busy = dict(job=job, server_identity=self._server, accepted=False,
                        reason=reason, this_attempt_started=False)
            if response == accepted:
                submitted['state'] = 'accepted'
                self._contexts.pop(job['job_id'], None)
            elif (reason in {'capacity_full', 'admission_busy'}
                    and type(response.get('this_attempt_started')) is bool and response == busy):
                submitted['state'] = 'busy'
                context['state'] = 'capacity_busy'
            else:
                raise TransportError('server admission result is unconfirmed')
            return _copy(response)
        except Exception as exc:
            submitted['state'] = 'unknown'
            context['state'] = 'unknown'
            raise TransportError('server admission outcome is unknown; never retry') from exc
