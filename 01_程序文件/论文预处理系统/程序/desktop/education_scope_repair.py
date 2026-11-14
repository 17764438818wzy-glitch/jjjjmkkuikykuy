"""Bounded vocabulary repair for the two verified education Pipelines."""
from __future__ import annotations

import copy


VERSION = 'education-scope-repair/1.0'
ALLOWED_PIPELINES = frozenset({
    '72ff5454b8f711f1a8846d29005ec004',
    '2df63160b98911f199ca358d29021dae',
})
PREPARE = 'CodeExec:PrepareEducation'
GATE = 'CodeExec:EducationGate'
ANNOTATION = 'Extractor:EducationAnnotation'
_MARKER = '# ' + VERSION

_MAJOR_BEFORE = "    ('音乐教育', ('音乐教育',)), ('汉语国际教育', ('汉语国际教育',)),"
_MAJOR_AFTER = ("    ('音乐教育', ('音乐教育',)), ('汉语言教育', ('汉语言教育',)),\n"
                "    ('汉语国际教育', ('汉语国际教育',)),")
_METHOD_NAMES = ['文献研究法', '问卷调查法', '访谈法', '课堂观察法', '教学设计法',
                 '行动研究法', '实验法', '前后测比较法', '案例研究法', '统计分析法',
                 '文本分析法', '混合研究法', '其他']
_NEW_METHOD_NAMES = _METHOD_NAMES[:-1] + ['比较研究法', '历史研究法', '其他']
_METHODS_BEFORE = 'METHODS = [' + ','.join(repr(x) for x in _METHOD_NAMES) + ']'
_METHODS_AFTER = 'METHODS = [' + ','.join(repr(x) for x in _NEW_METHOD_NAMES) + ']'
_ALIASES_BEFORE = "    aliases={'文献分析法':'文献研究法','文献资料法':'文献研究法'}"
_ALIASES_AFTER = ("    aliases={'文献分析法':'文献研究法','文献资料法':'文献研究法',"
                  "'案例分析法':'案例研究法','历史法':'历史研究法'}")
_PROMPT_BEFORE = 'name 只能选' + '、'.join(_METHOD_NAMES) + '。'
_PROMPT_AFTER = 'name 只能选' + '、'.join(_NEW_METHOD_NAMES) + '。'

PROMPT = '''

# education-scope-repair/1.0
“汉语言教育”是独立的候选专业，涵盖本土汉语/普通话教育及第二语言教育，不自动改成“汉语国际教育”。专业候选只允许进入标注，不能代替教育范围及质量裁决；仍须由正文实际研究问题、对象、分析、成果和引文证明教育相关性。纯语言本体、隐喻或跨文化节目分析若没有实际教育研究问题，不能凭零星教学建议判入库。
研究方法规范名补充“比较研究法”“历史研究法”；“历史法”规范为“历史研究法”，“案例分析法”规范为“案例研究法”。比较研究不等于前后测比较，历史研究不等于实验；按正文实际实施填写并保留原实施说明，不能仅凭方法名称声称实际执行。
全文指纹、paper_id、coverage_token、四个核心 evidence 支持字段、连续原文引文、实际研究步骤及 Profile 证据仍按原规则逐项校验；不得补造 quality 引文或用专业/方法名称替代证据。'''


def _replace(text: str, before: str, after: str, applied: bool) -> str:
    expected, absent = (after, before) if applied else (before, after)
    if text.count(expected) != 1 or absent in text:
        raise ValueError('Education scope repair anchor changed')
    return text if applied else text.replace(before, after, 1)


def repair_scope_dsl(source: dict, pipeline_id: str) -> dict:
    """Return a patched copy; reject other routes, drift, and partial installs."""
    if pipeline_id not in ALLOWED_PIPELINES:
        raise ValueError('Pipeline is outside the two verified education routes')
    result = copy.deepcopy(source)
    try:
        params = {node: result['components'][node]['obj']['params']
                  for node in (PREPARE, GATE, ANNOTATION)}
        forms = {}
        for node in params:
            matches = [item['data'] for item in result['graph']['nodes']
                       if item['id'] == node]
            if len(matches) != 1 or matches[0]['form'] != params[node]:
                raise ValueError('Education scope form and execution params disagree')
            forms[node] = matches[0]
        prepare, gate = params[PREPARE]['script'], params[GATE]['script']
        prompt = params[ANNOTATION]['sys_prompt']
        if not all(isinstance(value, str) and 'education-failure-repair/1.0' in value
                   for value in (prepare, gate, prompt)):
            raise ValueError('Education failure repair prerequisite is missing')
    except (KeyError, TypeError) as exc:
        raise ValueError('Education scope repair topology changed') from exc

    flags = [_MARKER in value for value in (prepare, gate, prompt)]
    if any(flags) and not all(flags):
        raise ValueError('Partially applied education scope repair')
    applied = all(flags)
    if applied and (any(value.count(_MARKER) != 1 for value in (prepare, gate, prompt))
                    or prompt.count(PROMPT) != 1):
        raise ValueError('Education scope repair marker changed')

    prepare = _replace(prepare, _MAJOR_BEFORE, _MAJOR_AFTER, applied)
    gate = _replace(gate, _MAJOR_BEFORE, _MAJOR_AFTER, applied)
    gate = _replace(gate, _METHODS_BEFORE, _METHODS_AFTER, applied)
    gate = _replace(gate, _ALIASES_BEFORE, _ALIASES_AFTER, applied)
    prompt = _replace(prompt, _PROMPT_BEFORE, _PROMPT_AFTER, applied)
    if not applied:
        prepare += '\n' + _MARKER + '\n'
        gate += '\n' + _MARKER + '\n'
        prompt += PROMPT
    compile(prepare, '<education-scope-prepare>', 'exec')
    compile(gate, '<education-scope-gate>', 'exec')
    params[PREPARE]['script'] = prepare
    params[GATE]['script'] = gate
    params[ANNOTATION]['sys_prompt'] = prompt
    for node, data in forms.items():
        data['form'] = copy.deepcopy(params[node])
    return result
