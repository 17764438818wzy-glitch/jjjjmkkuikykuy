"""Syntax-preserving parsing and authoritative arithmetic, with an audit copy."""
import copy,difflib,hashlib,json,sys
from pathlib import Path
from article_repository.ratings import RUBRICS,canonical

DESKTOP=Path('F:/论文预处理系统/程序/desktop')
if str(DESKTOP) not in sys.path:sys.path.insert(0,str(DESKTOP))
from profile_json import loads_profile,_repair


def unique_object(pairs):
    result={}
    for key,value in pairs:
        if key in result:raise ValueError('重复JSON键，不能无损恢复：'+key)
        result[key]=value
    return result


def _decode(raw,approved_ids):
    audit=[]
    try:return json.loads(raw,object_pairs_hook=unique_object),audit
    except json.JSONDecodeError:pass
    # Reuse the existing mature parser and its supplied-scalar preservation guard.
    try:
        value=loads_profile(raw)
        text=json.dumps(value,ensure_ascii=False,separators=(',',':'))
        return json.loads(text,object_pairs_hook=unique_object),[{'policy':'existing_profile_json_scalar_guard'}]
    except ValueError:pass
    # The scalar lexer cannot read an unclosed ID string. Accept only a parser
    # candidate whose exact compact-text difference is insertion of that known
    # ID's closing quote, immediately before the existing integer-level field.
    repaired=json.loads(_repair(raw),object_pairs_hook=unique_object)
    compact=json.dumps(repaired,ensure_ascii=False,separators=(',',':'))
    for tag,a,b,x,y in difflib.SequenceMatcher(None,raw,compact,autojunk=False).get_opcodes():
        if tag=='equal':continue
        known=next((sid for sid in approved_ids if raw[:a].endswith('"id":"'+sid)),None)
        if tag!='insert' or compact[x:y]!='"' or not known or not raw[a:].startswith(',"level":'):
            raise ValueError('JSON修复需要更改实质值或不能唯一确认；保留待核对')
        audit.append({'policy':'known_item_id_closing_quote_only','position':a,'id':known,'inserted':'"'})
    if not audit:raise ValueError('未确认无损语法修复')
    return repaired,audit


def normalize_result(raw,category):
    rubric=RUBRICS[category];ids={s['id'] for d in rubric['dimensions'] for s in d['subitems']}
    decoded,syntax=_decode(raw,ids)
    if not isinstance(decoded,dict):raise ValueError('模型评分必须是对象')
    result=copy.deepcopy(decoded)
    audit={'policy':'rating-result-normalization/1.0','response_sha256':hashlib.sha256(raw.encode()).hexdigest(),
        'rubric_version':rubric['version'],'rubric_sha256':rubric['sha256'],'syntax_edits':syntax,
        'dimension_container_normalized':False,'arithmetic_changes':[],'semantic_values_changed':False,
        'parser_path':str(DESKTOP/'profile_json.py'),'parser_sha256':hashlib.sha256((DESKTOP/'profile_json.py').read_bytes()).hexdigest()}
    if result.get('status')!='rated':return result,audit
    rows=result.get('dimension_scores');expected=[d['id'] for d in rubric['dimensions']]
    if isinstance(rows,dict):
        if set(rows)!=set(expected) or any(not isinstance(v,dict) or v.get('id')!=k for k,v in rows.items()):raise ValueError('维度字典键与内部ID不一致，不能无损规范化')
        rows=[rows[k] for k in expected];result['dimension_scores']=rows;audit['dimension_container_normalized']=True
    if not isinstance(rows,list) or len(rows)!=len(expected) or any(not isinstance(r,dict) or not isinstance(r.get('id'),str) for r in rows):raise ValueError('模型维度不完整')
    by_id={r['id']:r for r in rows}
    if len(by_id)!=len(rows) or set(by_id)!=set(expected):raise ValueError('模型维度重复或缺失')
    def number(target,key,value,path):
        if key not in target or target[key]!=value:
            audit['arithmetic_changes'].append({'path':path,'model_present':key in target,'model_value':target.get(key),'computed_value':value})
        target[key]=value
    total=0
    for definition in rubric['dimensions']:
        row=by_id[definition['id']];subs=row.get('subitems')
        if not isinstance(subs,list) or len(subs)!=len(definition['subitems']) or any(not isinstance(s,dict) or not isinstance(s.get('id'),str) for s in subs):raise ValueError('模型小项不完整')
        mapped={s['id']:s for s in subs}
        if len(mapped)!=len(subs) or set(mapped)!={s['id'] for s in definition['subitems']}:raise ValueError('模型小项重复或缺失')
        subtotal=0
        for sub in definition['subitems']:
            model=mapped[sub['id']];level=model.get('level')
            if type(level) is not int or not 0<=level<=5:raise ValueError('模型未提供明确0至5整数级别，不能猜测')
            score=level*sub['max']//5;number(model,'score',score,sub['id']+'.score');subtotal+=score
        number(row,'score',subtotal,definition['id']+'.score');total+=subtotal
    if type(result.get('cap_applied')) is not bool:raise ValueError('模型未明确封顶标记，不能猜测')
    number(result,'score',min(total,rubric['cap']) if result['cap_applied'] else total,'score')
    audit['raw_score']=total;audit['final_score']=result['score']
    return result,audit
