"""Optional main research type shared by validation and existing synthesis."""
RESEARCH_TYPES = frozenset({
    'engineering_system', 'experimental_testing', 'survey_case_applied',
    'theoretical_modeling', 'textual_normative', 'creative_design',
})

TYPE_INSTRUCTION = '''本次同时确定全文主研究类型。固定代码：engineering_system=工程设计与系统实现；experimental_testing=实验研究与测试；survey_case_applied=调查、案例与应用分析；theoretical_modeling=理论推导与计算建模；textual_normative=文本分析与规范论证；creative_design=艺术创作与设计实践。按承担核心问题及主要产出的工作单选，专业、工具或算法名称仅作线索；不足为null。只增加research_type，不增加分类依据字段或评级分数。'''


def has_research_type(candidates):
    return any('research_type' in row.get('profile', {}).get('research_profile', {})
               for row in candidates)


def resolve_research_type(candidates, *, final=None, authoritative=False):
    if authoritative:
        return final if isinstance(final, str) and final in RESEARCH_TYPES else None
    values = {value for row in candidates
              if isinstance(value := row.get('profile', {}).get('research_profile', {}).get('research_type'), str)
              and value in RESEARCH_TYPES}
    return next(iter(values)) if len(values) == 1 else None
