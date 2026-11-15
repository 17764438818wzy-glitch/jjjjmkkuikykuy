"""Bounded model failure facts; never persist arbitrary provider text or prompts."""
import json
import re


REASONS={
    11128:'channel_verification_failed',
    11129:'invalid_model_parameters',
    11133:'invalid_model_parameters',
    11140:'account_restricted',
}
PUBLIC_CODES={
    'workbuddy_channel_rejected':'channel_verification_failed',
    'workbuddy_invalid_parameters':'invalid_model_parameters',
    'workbuddy_account_restricted':'account_restricted',
}
PUBLIC_MESSAGES={
    'WorkBuddy 渠道校验未通过（11128）：请使用供应商支持的客户端或接口。':11128,
    'WorkBuddy 请求参数被模型供应商拒绝（11129）。':11129,
    'WorkBuddy 请求参数无效（11133）：请核对模型与工具参数格式。':11133,
    'WorkBuddy 账号被上游限制使用（11140）：请通过官方渠道核查账号状态。':11140,
}


def diagnostic(error,status):
    if not isinstance(error,dict):return {}
    source=error
    message=error.get('message')
    if isinstance(message,str) and len(message)<=16384:
        start=message.find('{')
        if start>=0:
            try:
                body,_=json.JSONDecoder().raw_decode(message[start:])
                if isinstance(body,dict):
                    candidate=body.get('error',body)
                    if isinstance(candidate,dict):source=candidate
            except (ValueError,RecursionError):pass
    result={}
    vendor=source.get('upstream_code',source.get('code'))
    if type(vendor) is not int:
        # OpenAI-compatible gateways/SDKs sometimes keep only error.message.
        # Match our exact safe public descriptions, never arbitrary body text.
        text=source.get('message')
        if isinstance(text,str) and len(text)<=512:
            text=re.sub(r'^(?:HTTP\s+)?[45]\d{2}\s*:?\s*','',text.strip(),flags=re.I)
            vendor=PUBLIC_MESSAGES.get(text)
    if type(vendor) is int and 1000<=vendor<=99999999:result['upstream_code']=vendor
    public=source.get('code')
    if isinstance(public,str) and public in PUBLIC_CODES:
        result['provider_error_code']=public
    request_id=source.get('upstream_request_id',source.get('requestId'))
    if isinstance(request_id,str) and re.fullmatch(r'[a-fA-F0-9]{8}-(?:[a-fA-F0-9]{4}-){3}[a-fA-F0-9]{12}',request_id):
        result['upstream_request_id']=request_id
    rejection=status in (400,404,413,415,422) or (status is None and error.get('code')=='INVALID_REQUEST')
    if rejection:
        reason=REASONS.get(result.get('upstream_code'),PUBLIC_CODES.get(public,'request_rejected') if isinstance(public,str) else 'request_rejected')
        result.update(model_rejection=True,retryable=False,reason=reason,
                      family='model_request:'+str(status or 'invalid')+':'+str(result.get('upstream_code',reason)))
    elif result:
        result['reason']=REASONS.get(result.get('upstream_code'),'upstream_rejected')
    return result
