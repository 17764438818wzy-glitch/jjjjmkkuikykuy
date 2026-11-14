from __future__ import annotations

import re
from dataclasses import dataclass

UNKNOWN_MAJOR = "未识别专业"
UNKNOWN_MAJOR_GROUP = "未识别专业"

MAJOR_GROUPS: dict[str, list[str]] = {
    "经管类": [
        "财务管理",
        "会计学",
        "审计学",
        "工商管理",
        "市场营销",
        "人力资源管理",
        "电子商务",
        "物流管理",
        "国际经济与贸易",
        "金融学",
        "经济学",
        "行政管理",
        "公共事业管理",
        "旅游管理",
        "酒店管理",
        "工程管理",
        "工程造价",
    ],
    "教育类": [
        "学前教育",
        "小学教育",
        "教育学",
        "特殊教育",
        "体育教育",
        "英语教育",
        "汉语言教育",
        "思想政治教育",
    ],
    "法学与社会类": [
        "法学",
        "社会工作",
        "社会学",
        "政治学与行政学",
        "治安学",
        "侦查学",
    ],
    "计算机与信息类": [
        "计算机科学与技术",
        "软件工程",
        "网络工程",
        "信息管理与信息系统",
        "数据科学与大数据技术",
        "物联网工程",
        "人工智能",
        "数字媒体技术",
        "电子信息工程",
        "通信工程",
    ],
    "医护类": [
        "护理学",
        "临床医学",
        "口腔医学",
        "药学",
        "中药学",
        "医学检验技术",
        "医学影像技术",
        "康复治疗学",
        "公共卫生",
        "预防医学",
        "助产学",
    ],
    "文学新闻类": [
        "汉语言文学",
        "新闻学",
        "传播学",
        "广告学",
        "广播电视学",
        "网络与新媒体",
        "编辑出版学",
        "秘书学",
    ],
    "外语类": [
        "英语",
        "商务英语",
        "日语",
        "法语",
        "德语",
        "俄语",
        "西班牙语",
        "翻译",
    ],
    "艺术设计类": [
        "视觉传达设计",
        "环境设计",
        "产品设计",
        "服装与服饰设计",
        "数字媒体艺术",
        "动画",
        "美术学",
        "音乐学",
        "舞蹈学",
        "播音与主持艺术",
        "广播电视编导",
    ],
    "工程类": [
        "土木工程",
        "机械设计制造及其自动化",
        "电气工程及其自动化",
        "自动化",
        "车辆工程",
        "建筑学",
        "城乡规划",
        "给排水科学与工程",
        "环境工程",
        "安全工程",
        "化学工程与工艺",
        "材料科学与工程",
        "能源与动力工程",
        "测绘工程",
        "交通工程",
    ],
    "农林类": [
        "农学",
        "园林",
        "园艺",
        "林学",
        "植物保护",
        "动物医学",
        "动物科学",
        "水产养殖学",
        "农业资源与环境",
    ],
    "理学类": [
        "数学与应用数学",
        "应用统计学",
        "统计学",
        "物理学",
        "应用物理学",
        "化学",
        "应用化学",
        "生物科学",
        "生物技术",
        "地理科学",
        "心理学",
        "应用心理学",
    ],
}

MAJOR_TO_GROUP = {
    major: group
    for group, majors in MAJOR_GROUPS.items()
    for major in majors
}

MAJOR_PREFIXES = {
    "财务管理": "FM",
    "会计学": "ACC",
    "审计学": "AUD",
    "工商管理": "BIZ",
    "市场营销": "MKT",
    "人力资源管理": "HR",
    "电子商务": "EC",
    "物流管理": "LOG",
    "国际经济与贸易": "TRADE",
    "金融学": "FIN",
    "经济学": "ECO",
    "行政管理": "ADM",
    "公共事业管理": "PUB",
    "旅游管理": "TOUR",
    "酒店管理": "HOTEL",
    "工程管理": "EM",
    "工程造价": "COST",
    "护理学": "NUR",
    "计算机科学与技术": "CS",
}

EXTRA_ALIASES = {
    "财管": "财务管理",
    "护理": "护理学",
    "计算机": "计算机科学与技术",
    "计算机专业": "计算机科学与技术",
    "计算机科学技术": "计算机科学与技术",
}

NOISE_RE = re.compile(
    r"(专业|本科|专科|班级|班|方向|学院|系|毕业论文|论文|设计|"
    r"\d{2,4}|[A-Za-z]+|（[^）]*）|\([^)]*\)|[\s_\\/\-]+)"
)
MAJOR_FIELD_RE = re.compile(
    r"(?m)^[^\S\r\n]*专[^\S\r\n]*业[^\S\r\n]*[:：]?[^\S\r\n]*(?P<value>.+?)\s*$"
)
BODY_ANCHOR_RE = re.compile(
    r"(?m)^[^\S\r\n]*(摘要|ABSTRACT|Abstract|目录|第一章|第1章|绪论)"
)


@dataclass(frozen=True)
class MajorDetectionResult:
    detected_major: str
    detected_major_group: str
    major_detect_source: str
    major_confidence: str


def _compact(value: str) -> str:
    return re.sub(r"\s+", "", value.strip())


def normalize_major_name(value: str) -> str | None:
    compact = _compact(value)
    if not compact:
        return None

    candidates = sorted(MAJOR_TO_GROUP, key=len, reverse=True)
    for major in candidates:
        if major in compact:
            return major

    for alias, major in sorted(EXTRA_ALIASES.items(), key=lambda item: len(item[0]), reverse=True):
        if alias in compact:
            return major

    cleaned = NOISE_RE.sub("", compact)
    for major in candidates:
        if major == cleaned or major in cleaned:
            return major

    for alias, major in sorted(EXTRA_ALIASES.items(), key=lambda item: len(item[0]), reverse=True):
        if alias == cleaned or alias in cleaned:
            return major
    return None


def major_group_for(major: str) -> str:
    return MAJOR_TO_GROUP.get(major, UNKNOWN_MAJOR_GROUP)


def prefix_for_major(major: str) -> str:
    if major == UNKNOWN_MAJOR:
        return "UNK"
    return MAJOR_PREFIXES.get(major, "MAJ")


def _result(major: str | None, source: str, confidence: str) -> MajorDetectionResult:
    if not major:
        return MajorDetectionResult(UNKNOWN_MAJOR, UNKNOWN_MAJOR_GROUP, "unknown", "none")
    return MajorDetectionResult(major, major_group_for(major), source, confidence)


def detect_major_from_front_matter(text: str) -> str | None:
    search_area = text[:3000]
    anchor = BODY_ANCHOR_RE.search(search_area)
    if anchor:
        search_area = search_area[: anchor.start()]
    for match in MAJOR_FIELD_RE.finditer(search_area):
        major = normalize_major_name(match.group("value"))
        if major:
            return major
    return None


def detect_major_in_text(value: str) -> str | None:
    return normalize_major_name(value)


def detect_major_info(text: str, original_relative_path: str = "") -> MajorDetectionResult:
    front_matter_major = detect_major_from_front_matter(text)
    if front_matter_major:
        return _result(front_matter_major, "front_matter", "high")

    path_major = detect_major_in_text(original_relative_path)
    if path_major:
        return _result(path_major, "path", "medium")

    body_major = detect_major_in_text(text[:3000])
    if body_major:
        return _result(body_major, "body", "low")

    return _result(None, "unknown", "none")
