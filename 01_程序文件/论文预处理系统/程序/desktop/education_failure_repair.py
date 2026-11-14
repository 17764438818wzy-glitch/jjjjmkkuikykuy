"""Apply bounded, source-preserving fixes to the two education Pipeline DSLs."""
from __future__ import annotations

import copy
import inspect

VERSION = 'education-failure-repair/1.0'
NODE = 'Extractor:EducationAnnotation'
HEAVY = ('education_research_steps', 'education_step_issues',
         'artifact_profile', 'artifact_evidence')

PROMPT = '''

# 教育研究类型与输出复核 education-failure-repair/1.0
先识别论文实际类型，再检查它是否完成该类型的研究工作。
文献研究、理论论证、文本分析可以用实际完成的文献比较、概念辨析、文本分析和论证作为研究步骤与发现，不要求自行采集问卷、访谈、实验数据。教学设计可以用实际完成的教学目标、任务、活动、评价设计及其依据作为设计研究成果，不要求已经课堂实施；没有实施证据时绝不能写成教学干预或效果验证。经验或实践案例可以用正文中具体事件、处理过程和反思作证据。只有泛泛建议、未实际完成的计划或无法定位的分析仍应暂缓。
入库所需是至少一项实际研究方法，不是多项；不得因只用一种方法或没有定量数据就判暂缓。不同类型仍须有研究问题、实际分析、发现或设计成果，并保留可定位的正文引文。
evidence 的四个核心 supports 均需覆盖：research_subject、research_methods、research_content、quality。quality 应引用支撑裁决的实际研究、分析或成果段落，不要引用作者自我评价；一条真实引文确实支持多个字段时可列多个 supports。原文短小的研究对象节可用，不能把可定位的正文引文改写成概述。研究步骤和 Profile 的引文也应与 evidence 相互一致。
相同研究方法合并实施说明。思想政治教育只作为候选专业；只有实际研究课程、教学、学生学习或教育实践才属于本链路，纯政治理论及政策宣传不能因此入库。
输出前自检完整字段、规范方法名、evidence 数量不超过12、四个核心支持字段、原文连续引文、paper_id 和结尾 coverage_token。'''


def normalize_methods(methods):
    """Coalesce known method aliases; retain every distinct implementation verbatim."""
    if not isinstance(methods, list):
        raise ValueError('研究方法列表类型错误')
    aliases={'文献分析法':'文献研究法','文献资料法':'文献研究法'}
    merged={}
    for method in methods:
        if (not isinstance(method,dict) or set(method)!={'name','implementation'}
                or not all(isinstance(method[k],str) and method[k].strip()
                           for k in ('name','implementation'))):
            raise ValueError('研究方法字段类型错误')
        name=aliases.get(method['name'],method['name'])
        implementations=merged.setdefault(name,[])
        if method['implementation'] not in implementations:
            implementations.append(method['implementation'])
    return [{'name':name,'implementation':'\n'.join(values)} for name,values in merged.items()]


def _replace_once(script, before, after):
    if script.count(before)!=1:
        raise ValueError('Education repair anchor changed')
    return script.replace(before,after,1)


def _major_candidate(script):
    return _replace_once(script,
        "    ('特殊教育', ('特殊教育',)), ('体育教育', ('体育教育', '体育教学')),",
        "    ('思想政治教育', ('思想政治教育',)),\n"
        "    ('特殊教育', ('特殊教育',)), ('体育教育', ('体育教育', '体育教学')),")


def repair_dsl(source: dict) -> dict:
    result=copy.deepcopy(source)
    components=result['components']
    gate=components['CodeExec:EducationGate']['obj']['params']
    prep=components['CodeExec:PrepareEducation']['obj']['params']
    llm=components[NODE]['obj']['params']
    if '# '+VERSION in gate['script']:
        if VERSION not in llm['sys_prompt'] or VERSION not in prep['script']:
            raise ValueError('Partially applied education repair')
        return result
    script=gate['script']
    script=_replace_once(script,"    merged, carry, top_role = [], [], 'body'",
                         "    merged, carry, top_role = [], [], 'body'\n    carry_header = None")
    script=_replace_once(script,"        h = dict(h, role=top_role)\n        if core_len(seg) < MIN_SECTION_BODY:",
        "        h = dict(h, role=top_role)\n"
        "        if carry and carry_header['role'] != top_role:\n"
        "            merged.append((carry_header, carry))\n"
        "            carry, carry_header = [], None\n"
        "        if core_len(seg) < MIN_SECTION_BODY:")
    script=_replace_once(script,'            carry.extend(seg[:2] if len(seg) > 1 else seg)',
        "            if not carry: carry_header = h\n            carry.extend(seg)")
    script=_replace_once(script,"        carry = []\n    if carry and merged:",
        "        carry, carry_header = [], None\n"
        "    if carry and merged and merged[-1][0]['role'] == carry_header['role']:")
    script=_replace_once(script,
        "        merged.append(({'line': 0, 'level': 0, 'num': '', 'title': '（正文前）',\n"
        "                        'sid': '', 'role': 'body'}, carry))",
        "        merged.append((carry_header, carry))")
    script=_replace_once(script,
        "    if merged and merged[0][0]['line'] > 0:\n        body = join_clean(lines[:merged[0][0]['line']])",
        "    if heads and heads[0]['line'] > 0:\n        body = join_clean(lines[:heads[0]['line']])")
    script=_major_candidate(script)
    script+='\n\n# '+VERSION+'\nHEAVY_METADATA_KEYS += '+repr(HEAVY)+'\n\n'
    script+=inspect.getsource(normalize_methods)
    script+='''
_education_original_validate = validate
def validate(raw, text, info):
    parsed = json.loads(json_object_text(raw), object_pairs_hook=no_duplicate_keys,
                        parse_constant=invalid_constant, strict=False)
    before = parsed.get('research_methods')
    parsed['research_methods'] = normalize_methods(before)
    result = _education_original_validate(packed(parsed), text, info)
    if before != parsed['research_methods']:
        result.setdefault('repair_notes', []).append('research_methods:合并同名方法及规范同义名，保留实施说明')
    return result
'''
    compile(script,'<education-gate-repair>','exec')
    gate['script']=script
    prep['script']=_major_candidate(prep['script'])+'\n# '+VERSION+'\n'
    llm['sys_prompt']+=PROMPT
    for node in result['graph']['nodes']:
        if node['id'] in ('CodeExec:PrepareEducation','CodeExec:EducationGate',NODE):
            node['data']['form']=copy.deepcopy(components[node['id']]['obj']['params'])
    return result
