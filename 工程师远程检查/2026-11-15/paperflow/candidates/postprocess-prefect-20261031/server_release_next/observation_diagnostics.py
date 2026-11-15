"""Retain bounded failure categories without request or response contents."""
_STAGES = frozenset({'access', 'registration', 'transport', 'guarded_command', 'accept'})
_REASONS = frozenset((
    'authentication_failed', 'host_or_protocol_error', 'key_file_unavailable',
    'request_refused_or_unknown', 'helper_failed'))
_ERROR_TYPES = frozenset({'ValueError', 'RuntimeError', 'RemoteCallError', 'TimeoutError',
                          'OSError', 'ImportError', 'FileNotFoundError', 'PermissionError',
                          'ConnectionError', 'KeyError', 'TypeError', 'JSONDecodeError',
                          'OperationalError', 'IntegrityError', 'UnreadReviewRequired'})


def safe_exception_type(exc):
    try:
        name = type(exc).__name__
        return name if type(name) is str and name in _ERROR_TYPES else 'Unknown'
    except BaseException:
        return 'Unknown'


def wrap(stage, callback, observations):
    if stage not in _STAGES or not callable(callback) or not isinstance(observations, list):
        raise ValueError('a fixed diagnostic stage and bounded sink are required')
    def recorded(*args, **kwargs):
        try:
            return callback(*args, **kwargs)
        except Exception as exc:
            try:
                if len(observations) < 20:
                    item = {'stage': stage, 'error_type': safe_exception_type(exc)}
                    reason = object.__getattribute__(exc, '__dict__').get('observation_reason')
                    if type(reason) is str and reason in _REASONS:
                        item['reason'] = reason
                    observations.append(item)
            except BaseException:
                pass
            raise
    return recorded
