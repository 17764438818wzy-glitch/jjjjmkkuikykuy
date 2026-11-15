"""Scoped education fixes: method vocabulary, candidates and complete input transport."""
from __future__ import annotations

import copy
import json

from education_scope_repair import ALLOWED_PIPELINES, PREPARE, GATE, ANNOTATION

VERSION = 'education-technical-repair/1.0'
MARKER = '# ' + VERSION
MAJOR_BEFORE = "    ('教育技术学', ('教育技术',)), ('职业教育', ('职业教育',)),"
MAJOR_AFTER = ("    ('数学教育', ('数学教育',)), ('人文教育', ('人文教育',)),\n" + MAJOR_BEFORE)
METHOD_BEFORE = "'比较研究法','历史研究法','其他']"
METHOD_AFTER = "'比较研究法','历史研究法','观察法','单一被试实验法','其他']"
ALIAS_BEFORE = "'案例分析法':'案例研究法','历史法':'历史研究法'}"
ALIAS_AFTER = "'案例分析法':'案例研究法','历史法':'历史研究法','个案研究法':'案例研究法'}"
PROMPT_BEFORE = '混合研究法、比较研究法、历史研究法、其他。'
PROMPT_AFTER = '混合研究法、比较研究法、历史研究法、观察法、单一被试实验法、其他。'
PROMPT = '''

# education-technical-repair/1.0
专业候选补充“数学教育”和“人文教育”，保留原专业，不自动转换成其他专业。仍按正文研究问题判断教育范围；纯历史邦交等范围外论文不能凭教育专业标签入库。
“个案研究法”规范为“案例研究法”；保留具体对象、实施、比较及分析的原说明。“观察法”用于不限定课堂的一般观察，“课堂观察法”仍用于课堂观察；“单一被试实验法”保留其具体设计名称和实施步骤，不把个案叙述冒充实验。
证据输出顺序：先从研究过程或分析结果摘取一段20至100字的连续原句作为裁决依据，确实支持裁决时把该条 supports 写为含 quality 的数组，再补其他证据。不能将几个章节标题拼接成“问题总结”当作引文，也不能把自己的 quality.reason 改写后放进 quote。若同一条连续原句同时支持实际研究方法、内容和裁决，可明确列出这些 supports。若找不到支持裁决的原句，保留缺口，不补造。
全文末尾另给出一个 coverage_token JSON 字段。输出时逐字复制其中字符串；应为32位小写十六进制，逐字符检查重复段和相邻字符。该字段可放在输出 JSON 的最后，不能根据 paper_id 或原文猜测。所有原文、身份、哈希、步骤和档位证据校验继续生效。
'''


def _replace(text, before, after, applied):
    expected = after if applied else before
    # Some replacements intentionally include the old text (e.g. added families).
    if text.count(expected) != 1 or (not applied and after in text):
        raise ValueError('Education technical repair anchor changed')
    return text if applied else text.replace(before, after, 1)


def _tail(text, token, applied):
    before = "'全文结束。请原样返回结尾校验值 coverage_token=' + " + token + ')'
    after = ("'全文结束。请原样返回结尾校验值 coverage_token=' + " + token +
             " + '\\n校验字段逐字复制，不附前缀：\\n' + json.dumps({'coverage_token': " + token + "}))")
    return _replace(text, before, after, applied)


def repair_technical_dsl(source: dict, pipeline_id: str) -> dict:
    """Pure candidate builder. Full source remains unchanged and source-bound."""
    if pipeline_id not in ALLOWED_PIPELINES:
        raise ValueError('Pipeline is outside the two verified education routes')
    result = copy.deepcopy(source)
    try:
        params = {node: result['components'][node]['obj']['params'] for node in (PREPARE, GATE, ANNOTATION)}
        forms = {}
        for node, value in params.items():
            matches = [item['data'] for item in result['graph']['nodes'] if item['id'] == node]
            if len(matches) != 1 or matches[0]['form'] != value:
                raise ValueError('Education form and execution params disagree')
            forms[node] = matches[0]
        prepare, gate, prompt = params[PREPARE]['script'], params[GATE]['script'], params[ANNOTATION]['sys_prompt']
        if not all(isinstance(x, str) and '# education-scope-repair/1.0' in x for x in (prepare, gate, prompt)):
            raise ValueError('Education scope repair prerequisite is missing')
    except (KeyError, TypeError) as exc:
        raise ValueError('Education technical repair topology changed') from exc
    flags = [MARKER in x for x in (prepare, gate, prompt)]
    if any(flags) and not all(flags):
        raise ValueError('Partially applied education technical repair')
    applied = all(flags)
    if applied and (any(x.count(MARKER) != 1 for x in (prepare, gate, prompt)) or prompt.count(PROMPT) != 1):
        raise ValueError('Education technical repair marker changed')
    prepare = _replace(prepare, MAJOR_BEFORE, MAJOR_AFTER, applied)
    gate = _replace(gate, MAJOR_BEFORE, MAJOR_AFTER, applied)
    prepare = _replace(prepare, 'MAX_INPUT_BYTES = 1024 * 1024', 'MAX_INPUT_BYTES = 3 * 1024 * 1024', applied)
    gate = _replace(gate, METHOD_BEFORE, METHOD_AFTER, applied)
    gate = _replace(gate, ALIAS_BEFORE, ALIAS_AFTER, applied)
    prompt = _replace(prompt, PROMPT_BEFORE, PROMPT_AFTER, applied)
    prepare = _tail(prepare, 'token', applied)
    gate = _tail(gate, "info['coverage_token']", applied)
    if not applied:
        prepare += '\n' + MARKER + '\n'
        gate += '\n' + MARKER + '\n'
        prompt += PROMPT
    compile(prepare, '<education-technical-prepare>', 'exec')
    compile(gate, '<education-technical-gate>', 'exec')
    params[PREPARE]['script'] = prepare
    params[GATE]['script'] = gate
    params[ANNOTATION]['sys_prompt'] = prompt
    for node, data in forms.items():
        data['form'] = copy.deepcopy(params[node])
    return result


def repair_assessment_dsl(source: dict, pipeline_id: str) -> dict:
    """Incremental repair for the real follow-up failure naming 测验法."""
    if pipeline_id not in ALLOWED_PIPELINES:
        raise ValueError('Pipeline is outside the two verified education routes')
    result = copy.deepcopy(source)
    marker = '# education-assessment-repair/1.0'
    suffix = ('\n\n' + marker + '\n'
              '“测验法”用于实际编制或使用试题、学习测验等工具采集表现；'
              '保留工具、实施和分析过程，不自动等同问卷调查或前后测比较。'
              '只有正文可定位的实际步骤才能证明执行，方法名称本身不能作证据。\n')
    params = {}
    forms = {}
    for name in (GATE, ANNOTATION):
        params[name] = result['components'][name]['obj']['params']
        matches = [n['data'] for n in result['graph']['nodes'] if n['id'] == name]
        if len(matches) != 1 or matches[0]['form'] != params[name]:
            raise ValueError('Education form and execution params disagree')
        forms[name] = matches[0]
    gate, prompt = params[GATE]['script'], params[ANNOTATION]['sys_prompt']
    if not all(MARKER in x for x in (gate, prompt)):
        raise ValueError('Education technical repair prerequisite is missing')
    flags = [marker in x for x in (gate, prompt)]
    if any(flags) and not all(flags):
        raise ValueError('Partially applied education assessment repair')
    applied = all(flags)
    if applied and (gate.count(marker) != 1 or prompt.count(suffix) != 1):
        raise ValueError('Education assessment marker changed')
    gate = _replace(gate, METHOD_AFTER, METHOD_AFTER.replace("'其他']", "'测验法','其他']"), applied)
    prompt = _replace(prompt, PROMPT_AFTER, PROMPT_AFTER.replace('、其他。', '、测验法、其他。'), applied)
    if not applied:
        gate += '\n' + marker + '\n'
        prompt += suffix
    compile(gate, '<education-assessment-gate>', 'exec')
    params[GATE]['script'], params[ANNOTATION]['sys_prompt'] = gate, prompt
    for name in params:
        forms[name]['form'] = copy.deepcopy(params[name])
    return result


def repair_followup_dsl(source: dict, pipeline_id: str) -> dict:
    """Repair observed vocabulary gaps without changing evidence validation."""
    if pipeline_id not in ALLOWED_PIPELINES:
        raise ValueError('Pipeline is outside the two verified education routes')
    result = copy.deepcopy(source)
    marker = '# education-followup-repair/1.0'
    suffix = ('\n\n' + marker + '\n'
        '“英语教育”是独立候选专业，仍须由正文证明实际教育研究问题。'
        '“课例研究法”保留对具体课例、教学实录的分析步骤；'
        '“逻辑分析法”须有实际归纳、演绎或论证过程，不能把泛泛建议当作执行。\n'
        'JSON 结构核对：research_content 只含 question、analysis、findings。'
        'findings 数组结束后必须先闭合 research_content 对象，再输出顶层 research_directions；'
        'audience_tags、quality、judgement_basis、uncertainty_reason、evidence、artifact_profile '
        '也都在顶层，不能嵌入 research_content。输出完整14个顶层字段并闭合最外层对象。\n'
        '研究对象证据必须另从实际方法、过程或结果段落复制20至100字的连续原句，'
        'supports 包含 research_subject。原句中的重复字、语病、书名和标点照抄；'
        '不能为通顺而改写。对象名称可以概括，quote 不可以概括；'
        '引文不必包含整个概括后的对象名称，但须确实描述该研究对象。'
        '没有可定位证据时如实判暂缓并说明缺口，不能补造证据。\n')
    params, forms = {}, {}
    for name in (PREPARE, GATE, ANNOTATION):
        params[name] = result['components'][name]['obj']['params']
        matches = [node['data'] for node in result['graph']['nodes'] if node['id'] == name]
        if len(matches) != 1 or matches[0]['form'] != params[name]:
            raise ValueError('Education form and execution params disagree')
        forms[name] = matches[0]
    prepare, gate, prompt = params[PREPARE]['script'], params[GATE]['script'], params[ANNOTATION]['sys_prompt']
    if MARKER not in prepare or not all('# education-assessment-repair/1.0' in s for s in (gate, prompt)):
        raise ValueError('Education assessment repair prerequisite is missing')
    flags = [marker in s for s in (prepare, gate, prompt)]
    if any(flags) and not all(flags):
        raise ValueError('Partially applied education followup repair')
    applied = all(flags)
    if applied and (any(s.count(marker) != 1 for s in (prepare, gate, prompt)) or prompt.count(suffix) != 1):
        raise ValueError('Education followup repair marker changed')
    for name, script in ((PREPARE, prepare), (GATE, gate)):
        script = _replace(script, MAJOR_AFTER, "    ('英语教育', ('英语教育',)),\n" + MAJOR_AFTER, applied)
        if name == GATE:
            before = METHOD_AFTER.replace("'其他']", "'测验法','其他']")
            script = _replace(script, before, before.replace("'其他']", "'课例研究法','逻辑分析法','其他']"), applied)
        if not applied: script += '\n' + marker + '\n'
        compile(script, '<education-followup-' + name + '>', 'exec')
        params[name]['script'] = script
    before = PROMPT_AFTER.replace('、其他。', '、测验法、其他。')
    prompt = _replace(prompt, before, before.replace('、其他。', '、课例研究法、逻辑分析法、其他。'), applied)
    if not applied: prompt += suffix
    params[ANNOTATION]['sys_prompt'] = prompt
    for name in params: forms[name]['form'] = copy.deepcopy(params[name])
    return result


def repair_structure_dsl(source: dict, pipeline_id: str) -> dict:
    """Make the observed nested-field error explicit with a complete JSON shape."""
    if pipeline_id not in ALLOWED_PIPELINES:
        raise ValueError('Pipeline is outside the two verified education routes')
    result = copy.deepcopy(source)
    params = result['components'][ANNOTATION]['obj']['params']
    forms = [n['data'] for n in result['graph']['nodes'] if n['id'] == ANNOTATION]
    if len(forms) != 1 or forms[0]['form'] != params:
        raise ValueError('Education form and execution params disagree')
    prompt = params['sys_prompt']
    marker = '# education-structure-example/1.0'
    shape = {'paper_id': '', 'coverage_token': '', 'research_subject': {'name': '', 'scope': ''},
        'education_context': {'stage': '', 'subject': '', 'setting': ''},
        'research_methods': [{'name': '', 'implementation': ''}],
        'research_steps': [{'name': '', 'status': '', 'materials': [], 'analysis_elements': [], 'quote': ''}],
        'research_content': {'question': '', 'analysis': [], 'findings': []},
        'research_directions': [], 'audience_tags': [], 'quality': {'decision': '', 'reason': '', 'issues': []},
        'judgement_basis': '', 'uncertainty_reason': '', 'evidence': [{'quote': '', 'supports': []}],
        'artifact_profile': {'paper_type': '', 'research_path': '', 'required_materials': [],
            'example_tier': '', 'tier_basis': '', 'evidence_quotes': []}}
    suffix = ('\n\n' + marker + '\n'
        '下面是必须遵循的完整 JSON 层级示例。所有空值均是结构占位，须按前文规则和原文填写，'
        '不能直接返回这个空模板。数组项数量按实际证据确定。特别注意 findings 后的对象闭合括号，'
        'research_directions 与 research_content 平级。输出多行缩进 JSON 以便逐层核对。\n'
        '<EDUCATION_JSON_STRUCTURE>\n' + json.dumps(shape, ensure_ascii=False, indent=2) +
        '\n</EDUCATION_JSON_STRUCTURE>\n'
        '原文可能含分页乱序、脚注和引用编号。不得自动重排或补齐 quote；'
        '优先选20至60字的短连续原文片段，可以在脚注或断裂之前截取，'
        '无须复制整段。尤其 research_subject 的证据应直接描述实际研究的学生、幼儿、'
        '教师、课堂、教材或活动，不能为迁就概括后的对象名称而改写原文。'
        '确实找不到可定位证据时按原准入规则裁决。只返回填写完成的 JSON，不返回结构标签。\n')
    if '# education-followup-repair/1.0' not in prompt:
        raise ValueError('Education followup repair prerequisite is missing')
    if marker in prompt:
        if prompt.count(suffix) != 1 or prompt.count(marker) != 1:
            raise ValueError('Education structure repair marker changed')
    else:
        params['sys_prompt'] += suffix
    forms[0]['form'] = copy.deepcopy(params)
    return result
