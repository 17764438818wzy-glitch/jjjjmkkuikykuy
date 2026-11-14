"""Local settings; API keys encrypted for the current Windows account via DPAPI."""
import ctypes
from ctypes import wintypes
import os
import copy
import math
from engine import read_json,write_json

DEFAULTS={'base_url':'','vision_base_url':'','text_model':'','vision_model':{
             'enabled':True,
             'primary':{'base_url':'','api_key':'','model':''},
             'fallback':{'enabled':True,'base_url':'','api_key':'','model':''},
             'retry':{'max_tries':3,'backoff_base_seconds':2.0},
             'circuit_breaker':{'failure_threshold':3,'cooldown_seconds':300,
                                'cooldown_multiplier':2.0,'max_cooldown_seconds':3600}},
          'timeout':90,'attempts':3,'max_pages':80,'max_tokens':8192,'max_tokens_retry_limit':0,
          'auto_upload':True,'auto_parse':True,'dataset_map':{},'legacy_dataset_map_fallback':True,
          'work_pool_enabled':False,'work_pool_auto_start':True,'work_pool_queue':'',
          'work_pool_parse_limit':3,'category_dataset_map':{},'dataset_pipeline_map':{},
          'dataset_native_parser_map':{},
          'source_receiver_command':[],
          'skip_derived_upload':True,'derived_redundancy_threshold':0.95,'usd_cny':'7',
          'concurrency':{'enabled':True,'papers':2,'images_per_paper':2,'global_llm':4,
                         'max_retries':4,'backoff_base_seconds':1.0,'min_start_interval_seconds':0.0},
          'resume':{'enabled':True,'state_file':'processing_state.json'}}

DEFAULTS['preprocess_routing']={'enabled':False,'lan_limit':8,'official_limit':4,'strategy':'lan_first'}
DEFAULTS['research_profile']={'enabled':False,'version':'research-profile-local/1.0','server_parse_ceiling':4}
DEFAULTS['preextract_limits']={}
DEFAULTS['preextract_postprocess']={}
DEFAULTS['preextract_preparation']={'workers':4,'max_pending':8,'office_enabled':True}


def normalize_preparation(value):
    result={**DEFAULTS['preextract_preparation'],**(value if isinstance(value,dict) else {})}
    for key in ('workers','max_pending'):
        try:result[key]=int(result[key])
        except (ValueError,TypeError):raise ValueError('材料准备并发和等待容量必须为整数') from None
    if not 1<=result['workers']<=16 or not result['workers']<=result['max_pending']<=64:
        raise ValueError('材料准备并发须为1–16，等待容量须不小于并发且不超过64')
    result['office_enabled']=bool(result['office_enabled'])
    return result


def normalize_preprocess_routing(value):
    result={**DEFAULTS['preprocess_routing'],**(value if isinstance(value,dict) else {})}
    result['enabled']=bool(result['enabled'])
    for name in ('lan_limit','official_limit'):
        try:result[name]=int(result[name])
        except (TypeError,ValueError):raise ValueError('预处理通道并发必须为整数') from None
        if not 1<=result[name]<=64:raise ValueError('预处理通道并发必须在 1–64 范围内')
    if result['strategy'] not in ('lan_first','balanced','official_first'):
        raise ValueError('预处理通道策略必须为 lan_first、balanced 或 official_first')
    return {name:result[name] for name in ('enabled','lan_limit','official_limit','strategy')}


def normalize_vision(value, legacy_base='', legacy_model=''):
    """Normalize the nested two-channel schema while accepting old settings."""
    if isinstance(value, str):
        raw={'primary':{'model':value}}
    elif isinstance(value, dict):
        raw=value
    else:
        raw={}
    primary={**DEFAULTS['vision_model']['primary'], **(raw.get('primary') if isinstance(raw.get('primary'),dict) else {})}
    fallback={**DEFAULTS['vision_model']['fallback'], **(raw.get('fallback') if isinstance(raw.get('fallback'),dict) else {})}
    if legacy_base and not primary.get('base_url'): primary['base_url']=legacy_base
    if legacy_model and not primary.get('model'): primary['model']=legacy_model
    for channel in (primary,fallback):
        for key in ('base_url','api_key','model'):
            channel[key]=str(channel.get(key) or '').strip()
    retry={**DEFAULTS['vision_model']['retry'], **(raw.get('retry') if isinstance(raw.get('retry'),dict) else {})}
    try: retry['max_tries']=max(1,min(8,int(retry['max_tries'])))
    except (TypeError,ValueError): retry['max_tries']=3
    try: retry['backoff_base_seconds']=max(0.0,min(60.0,float(retry['backoff_base_seconds'])))
    except (TypeError,ValueError): retry['backoff_base_seconds']=2.0
    breaker={**DEFAULTS['vision_model']['circuit_breaker'], **(raw.get('circuit_breaker') if isinstance(raw.get('circuit_breaker'),dict) else {})}
    for key in ('failure_threshold',):
        try: breaker[key]=max(1,int(breaker[key]))
        except (TypeError,ValueError): breaker[key]=DEFAULTS['vision_model']['circuit_breaker'][key]
    for key in ('cooldown_seconds','cooldown_multiplier','max_cooldown_seconds'):
        try: breaker[key]=max(0.0,float(breaker[key]))
        except (TypeError,ValueError): breaker[key]=DEFAULTS['vision_model']['circuit_breaker'][key]
    breaker['cooldown_multiplier']=max(1.0,breaker['cooldown_multiplier'])
    breaker['max_cooldown_seconds']=max(breaker['cooldown_seconds'],breaker['max_cooldown_seconds'])
    return {'enabled':bool(raw.get('enabled',True)),'primary':primary,'fallback':fallback,
            'retry':retry,'circuit_breaker':breaker}


def normalize_concurrency(value):
    raw=value if isinstance(value,dict) else {}
    result={**DEFAULTS['concurrency'],**raw}
    result['enabled']=bool(result['enabled'])
    for key in ['papers','images_per_paper','global_llm','max_retries']:
        try: result[key]=int(result[key])
        except (TypeError,ValueError): raise ValueError(f'concurrency.{key} 必须是整数') from None
        if result[key]<1: raise ValueError(f'concurrency.{key} 必须大于 0')
    try: result['backoff_base_seconds']=float(result['backoff_base_seconds'])
    except (TypeError,ValueError): raise ValueError('concurrency.backoff_base_seconds 必须是数字') from None
    if not 0<=result['backoff_base_seconds']<=60: raise ValueError('concurrency.backoff_base_seconds 必须在 0–60 秒')
    try:result['min_start_interval_seconds']=float(result['min_start_interval_seconds'])
    except (TypeError,ValueError):raise ValueError('concurrency.min_start_interval_seconds 必须是数字') from None
    if not math.isfinite(result['min_start_interval_seconds']) or not 0<=result['min_start_interval_seconds']<=3600:
        raise ValueError('concurrency.min_start_interval_seconds 必须在 0–3600 秒')
    return result


def normalize_resume(value):
    raw=value if isinstance(value,dict) else {}
    result={**DEFAULTS['resume'],**raw}
    result['enabled']=bool(result['enabled'])
    if not isinstance(result['state_file'],str) or not result['state_file'].strip():
        raise ValueError('resume.state_file 必须是非空路径')
    return result


def normalize_derived_upload_policy(value, threshold=None):
    """Normalize the opt-out switch for redundant derived text uploads."""
    if isinstance(value,dict):
        raw=value
        enabled=raw.get('skip_derived_upload',DEFAULTS['skip_derived_upload'])
        threshold=raw.get('derived_redundancy_threshold',DEFAULTS['derived_redundancy_threshold'])
    else:
        enabled=value if value is not None else DEFAULTS['skip_derived_upload']
        threshold=threshold if threshold is not None else DEFAULTS['derived_redundancy_threshold']
    try:
        threshold=float(threshold)
    except (TypeError,ValueError):
        raise ValueError('derived_redundancy_threshold 必须是数字') from None
    if not 0.0 <= threshold <= 1.0:
        raise ValueError('derived_redundancy_threshold 必须在 0–1 之间') from None
    return {'skip_derived_upload':bool(enabled),'derived_redundancy_threshold':threshold}


def normalize_dataset_map(value):
    if value in (None, ''):
        return {}
    if not isinstance(value,dict):
        raise ValueError('dataset_map 必须是专业到知识库配置的对象')
    result={}
    for major,entry in value.items():
        if not isinstance(major,str) or not major.strip():
            raise ValueError('dataset_map 包含无效专业名称')
        if isinstance(entry,str):
            result[major]={'paper':entry.strip(),'image':''}
        elif isinstance(entry,dict):
            result[major]={
                'paper':str(entry.get('paper','') or '').strip(),
                'image':str(entry.get('image','') or '').strip(),
            }
        else:
            raise ValueError(f'dataset_map[{major}] 必须是旧字符串或 paper/image 对象')
    return result


def validate_dataset_pair(major,entry):
    normalized=normalize_dataset_map({major:entry}).get(major,{'paper':'','image':''})
    if not normalized['paper']:
        raise ValueError(f'专业“{major}”还没选 md 知识库，请先在 RAGFlow 连接页选择并保存')
    return {**normalized,'image_warning':not bool(normalized['image'])}


def crypt(data,protect):
    if os.name!='nt': raise RuntimeError('密钥保存需要 Windows DPAPI')
    class Blob(ctypes.Structure):
        _fields_=[('size',wintypes.DWORD),('data',ctypes.POINTER(ctypes.c_byte))]
    buffer=ctypes.create_string_buffer(data);source=Blob(len(data),ctypes.cast(buffer,ctypes.POINTER(ctypes.c_byte)));target=Blob()
    function=ctypes.windll.crypt32.CryptProtectData if protect else ctypes.windll.crypt32.CryptUnprotectData
    if not function(ctypes.byref(source),None,None,None,None,1,ctypes.byref(target)): raise RuntimeError('Windows 密钥加密操作失败')
    try: return ctypes.string_at(target.data,target.size)
    finally: ctypes.windll.kernel32.LocalFree(ctypes.cast(target.data,ctypes.c_void_p))


def validate_api_key(key, label='API Key'):
    """Reject display masks and characters unsafe in an Authorization header."""
    if not key:
        return
    if set(key) <= set('•●·*') or any(ord(ch) < 33 or ord(ch) > 126 for ch in key):
        raise ValueError(label+' 无效：请粘贴服务商提供的真实密钥，不能使用密码圆点、空格或换行')


def save_key(root,key,name='ai'):
    validate_api_key(key)
    path=root/(name+'_key.dpapi')
    if key: path.write_bytes(crypt(key.encode(),True))
    elif path.exists(): path.unlink()


def load_key(root,name='ai'):
    path=root/(name+'_key.dpapi')
    if not path.exists(): return ''
    try: return crypt(path.read_bytes(),False).decode()
    except Exception: return ''


def load_config(root):
    saved=read_json(root/'ai_settings.json',{})
    saved=saved if isinstance(saved,dict) else {}
    config={**copy.deepcopy(DEFAULTS),**saved}
    config['vision_model']=normalize_vision(config.get('vision_model'),config.get('vision_base_url',''),
                                           config.get('vision_model') if isinstance(config.get('vision_model'),str) else '')
    # API keys are always read from DPAPI files; never trust or re-emit a
    # plaintext key that may have existed in an edited settings file.
    config['vision_model']['primary']['api_key']=''
    config['vision_model']['fallback']['api_key']=''
    # Keep legacy aliases in memory for old callers/UI; saving writes only the
    # nested form and never serializes secrets.
    config['vision_base_url']=config['vision_model']['primary']['base_url']
    config['vision_model_legacy']=config['vision_model']['primary']['model']
    config['dataset_map']=normalize_dataset_map(config.get('dataset_map'))
    config['concurrency']=normalize_concurrency(config.get('concurrency'))
    config['preextract_preparation']=normalize_preparation(config.get('preextract_preparation'))
    config['preprocess_routing']=normalize_preprocess_routing(config.get('preprocess_routing'))
    config['resume']=normalize_resume(config.get('resume'))
    config.update(normalize_derived_upload_policy(config))
    from purpose_review import production_settings
    purpose=production_settings(root)
    if purpose:config['purpose_review']=purpose
    return config


def save_config(root,config,key=None,vision_key=None,fallback_key=None):
    clean={k:copy.deepcopy(config[k]) for k in DEFAULTS if k in config}
    clean['vision_model']=normalize_vision(clean.get('vision_model'),config.get('vision_base_url',''),
                                           config.get('vision_model_legacy','') if isinstance(config.get('vision_model_legacy'),str) else '')
    for channel in ('primary','fallback'):
        clean['vision_model'][channel]['api_key']=''
    clean['dataset_map']=normalize_dataset_map(clean.get('dataset_map'))
    clean['concurrency']=normalize_concurrency(clean.get('concurrency'))
    clean['preextract_preparation']=normalize_preparation(clean.get('preextract_preparation'))
    clean['preprocess_routing']=normalize_preprocess_routing(clean.get('preprocess_routing'))
    clean['resume']=normalize_resume(clean.get('resume'))
    clean.update(normalize_derived_upload_policy(clean))
    # Validate before any write so a bad pasted key cannot partially save
    # the form or overwrite a previously working credential.
    validate_api_key(key, '文字通道 API Key')
    validate_api_key(vision_key, '主通道 API Key')
    fallback_enabled=clean['vision_model']['fallback']['enabled']
    if fallback_enabled:
        validate_api_key(fallback_key, '备用通道 API Key')
    write_json(root/'ai_settings.json',clean)
    if key is not None: save_key(root,key)
    if vision_key is not None:
        save_key(root,vision_key,'ai_vision')
        save_key(root,vision_key,'ai_vision_primary')
    if fallback_enabled and fallback_key is not None: save_key(root,fallback_key,'ai_vision_fallback')
