"""One declared reference vocabulary for display and parameterized catalog queries."""
import re
from .ratings import rating_select_sql

RESEARCH_TYPES={
    'engineering_system':'工程设计与系统实现',
    'experimental_testing':'实验研究与测试',
    'survey_case_applied':'调查、案例与应用分析',
    'theoretical_modeling':'理论推导与计算建模',
    'textual_normative':'文本分析与规范论证',
    'creative_design':'艺术创作与设计实践',
}
RESEARCH_TYPE_VALUE="a.base#>>'{profile,research_profile,research_type}'"
YEAR_VALUE="a.base#>>'{profile,article,year}'"
YEAR_PATTERN=r'^(19|20)[0-9]{2}$'
TITLE_VALUE="regexp_replace(lower(a.title),'[^[:alnum:]+#]','','g')"

LEVEL_PATTERNS={
    'correspondence': r'函授|继续教育',
    'bachelor': r'(^|[^点])本科',
    'key_bachelor': r'重点本科',
}
NEGATIVE=r'(不适合|不适用|不推荐|不建议|不能用于|未达到|未达|不达到)[^；;，,。]{0,10}'
CLAUSES=r'[；;，,。\n]'
LEVEL_VALUE="""coalesce(a.overrides#>'{fields,level}',
    a.base#>'{profile,applicable_level,labels}',a.base#>'{profile,applicable_levels}','""'::jsonb)"""
LEVEL_LABELS=f"jsonb_array_elements_text(jsonb_path_query_array({LEVEL_VALUE},'$[*]')) AS source_label CROSS JOIN LATERAL regexp_split_to_table(source_label,%s) AS label"
FILE_ROWS="jsonb_array_elements(coalesce(a.base->'files','[]'::jsonb))"
ORIGINAL=f"EXISTS(SELECT 1 FROM {FILE_ROWS} f WHERE f->>'kind'='原文' AND f->>'availability'='available')"
ATTACHMENT=f"EXISTS(SELECT 1 FROM {FILE_ROWS} f WHERE f->>'kind'='附件' AND f->>'availability'='available')"
UNVERIFIED=f"(coalesce(a.base->>'inventory_complete','false')<>'true' OR EXISTS(SELECT 1 FROM {FILE_ROWS} f WHERE coalesce(f->>'availability','unknown') NOT IN ('available','missing')))"
KEYWORD_VALUE='a.search_text'


def negative_pattern(level):
    return NEGATIVE+'('+('本科' if level=='bachelor' else LEVEL_PATTERNS[level])+')'


def reference_levels(labels):
    clauses=[c for label in labels for c in re.split(CLAUSES,label)]
    return [key for key,pattern in LEVEL_PATTERNS.items() if any(
        re.search(pattern,clause) and not re.search(negative_pattern(key),clause) for clause in clauses)]


def level_condition(level):
    if level=='unclassified':
        parts=['(label ~ %s AND label !~ %s)' for _ in LEVEL_PATTERNS]
        return (f'coalesce({rating_select_sql("false")},NOT EXISTS(SELECT 1 FROM {LEVEL_LABELS} WHERE '+' OR '.join(parts)+'))',
                [CLAUSES]+[arg for key,pattern in LEVEL_PATTERNS.items() for arg in (pattern,negative_pattern(key))])
    pattern=LEVEL_PATTERNS[level]
    return (f'coalesce({rating_select_sql("r.tier=%s")},EXISTS(SELECT 1 FROM {LEVEL_LABELS} WHERE label ~ %s AND label !~ %s))',
            [level,CLAUSES,pattern,negative_pattern(level)])


def normalized_keyword(keyword):
    return ''.join(c for c in keyword.lower() if c.isalnum() or c in '+#')


def keyword_condition(keyword):
    keyword=keyword.strip()
    normalized=normalized_keyword(keyword)
    parts=[f'strpos(lower({KEYWORD_VALUE}),lower(%s))>0'];args=[keyword]
    if normalized:
        parts.append(f'strpos({TITLE_VALUE},%s)>0');args.append(normalized)
        if len(normalized)>=3:
            fuzzy=[f'public.word_similarity(%s,{TITLE_VALUE})>=0.35'];args.append(normalized)
            for term in re.findall(r'[a-z0-9]+[+#]+',keyword.lower()):
                fuzzy.append(f'strpos({TITLE_VALUE},%s)>0');args.append(term)
            parts.append('('+' AND '.join(fuzzy)+')')
    return '('+' OR '.join(parts)+')',args


def search_order(keyword):
    normalized=normalized_keyword(keyword.strip())
    if not normalized:return 'a.updated_at DESC,a.id',[]
    return (f"CASE WHEN {TITLE_VALUE}=%s THEN 3 WHEN strpos({TITLE_VALUE},%s)>0 THEN 2 "
            f"WHEN public.word_similarity(%s,{TITLE_VALUE})>=0.35 THEN 1 ELSE 0 END DESC,"
            f'public.word_similarity(%s,{TITLE_VALUE}) DESC,public.similarity(%s,{TITLE_VALUE}) DESC,a.updated_at DESC,a.id',
            [normalized]*5)


def search_conditions(*,keyword='',major='',level='',file_condition='',attachment_ext='',research_type='',year=''):
    if level and level not in (*LEVEL_PATTERNS,'unclassified'):raise ValueError('参考层级无效')
    if file_condition not in ('','original','attachment','both','unverified'):raise ValueError('文件条件无效')
    if attachment_ext and not re.fullmatch('[a-zA-Z0-9]{1,12}',attachment_ext):raise ValueError('附件格式无效')
    if research_type and research_type not in (*RESEARCH_TYPES,'unclassified'):raise ValueError('文章分类无效')
    if year and year!='unclassified' and not re.fullmatch(YEAR_PATTERN,year):raise ValueError('文章年份无效')
    clauses=["coalesce(a.base->>'entry_kind','article')='article'"];args=[]
    if keyword.strip():
        clause,values=keyword_condition(keyword);clauses.append(clause);args.extend(values)
    if research_type:
        if research_type=='unclassified':
            clauses.append(f'coalesce({RESEARCH_TYPE_VALUE},\'\')<>ALL(%s)');args.append(list(RESEARCH_TYPES))
        else:
            clauses.append(f'{RESEARCH_TYPE_VALUE}=%s');args.append(research_type)
    if year:
        if year=='unclassified':
            clauses.append(f"coalesce({YEAR_VALUE},'') !~ %s");args.append(YEAR_PATTERN)
        else:
            clauses.append(f'{YEAR_VALUE}=%s');args.append(year)
    if major:
        clauses.append('EXISTS(SELECT 1 FROM article_major m WHERE m.article_id=a.id AND m.code=%s)');args.append(major)
    if level:
        clause,values=level_condition(level);clauses.append(clause);args.extend(values)
    if file_condition:
        clauses.append({'original':ORIGINAL,'attachment':ATTACHMENT,'both':f'({ORIGINAL} AND {ATTACHMENT})','unverified':UNVERIFIED}[file_condition])
    if attachment_ext:
        clauses.append(f"EXISTS(SELECT 1 FROM {FILE_ROWS} f WHERE f->>'kind'='附件' AND f->>'availability'='available' AND lower(substring(f->>'name' from '[^.]+$'))=lower(%s))")
        args.append(attachment_ext)
    return ' AND '.join(clauses),args
