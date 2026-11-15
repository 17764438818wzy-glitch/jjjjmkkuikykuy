from __future__ import annotations

import re
from dataclasses import dataclass

from .tech_classifier import TechClassificationResult, classify_tech
from .tech_taxonomy import UNKNOWN_TECH_TYPE

UNKNOWN_SUBJECT_GROUP = "未识别/其他类"
UNKNOWN_SUBJECT_GROUP_DIR = "未识别_其他类"
UNKNOWN_DIRECTION = "未识别方向"
NOT_APPLICABLE = "不适用"

SUBJECT_GROUP_DIRS = {
    UNKNOWN_SUBJECT_GROUP: UNKNOWN_SUBJECT_GROUP_DIR,
}

LOCKED_FRONT_MATTER_GROUPS = {
    "经管财会类",
    "教育教学类",
    "医护健康类",
    "法学社会公共类",
    "文学外语传媒类",
    "艺术设计类",
    "农林生物资源类",
    "理学统计心理类",
}

REFINABLE_FRONT_MATTER_GROUPS = {
    "计算机软件与数据类",
    "智能硬件与控制类",
    "机械制造与机电类",
    "土木建筑与交通类",
    "电气电子与通信能源类",
    "化工材料环境安全类",
}

SUBJECT_GROUPS: dict[str, list[str]] = {
    "经管财会类": [
        "财务管理", "会计学", "审计学", "工商管理", "市场营销", "人力资源管理", "电子商务", "物流管理",
        "金融学", "经济学", "国际经济与贸易", "旅游管理", "酒店管理", "工程管理", "工程造价", "行政管理", "公共事业管理",
        "经管财会方向",
    ],
    "教育教学类": [
        "学前教育", "小学教育", "教育学", "特殊教育", "体育教育", "思想政治教育", "英语教育", "汉语言教育",
        "教育教学方向",
    ],
    "医护健康类": [
        "护理学", "临床医学", "口腔医学", "药学", "中药学", "康复治疗学", "医学检验技术", "医学影像技术",
        "公共卫生", "预防医学", "助产学", "医护健康方向",
    ],
    "法学社会公共类": [
        "法学", "社会工作", "社会学", "政治学与行政学", "治安学", "侦查学", "行政管理", "公共事业管理",
        "知识产权", "法学社会公共方向",
    ],
    "文学外语传媒类": [
        "汉语言文学", "英语", "商务英语", "日语", "法语", "德语", "俄语", "西班牙语", "翻译", "新闻学", "传播学",
        "广告学", "广播电视学", "网络与新媒体", "秘书学", "文学外语传媒方向",
    ],
    "艺术设计类": [
        "视觉传达设计", "环境设计", "产品设计", "服装与服饰设计", "数字媒体艺术", "动画", "美术学", "音乐学",
        "舞蹈学", "播音与主持艺术", "广播电视编导", "艺术设计方向",
    ],
    "计算机软件与数据类": [
        "计算机科学与技术", "软件工程", "网络工程", "信息管理与信息系统", "数据科学与大数据技术", "人工智能",
        "数字媒体技术", "Java管理系统", "Spring Boot系统", "SSM系统", "JSP系统", "Vue前端", "React前端",
        "微信小程序", "uni-app", "Android App", "Python项目", "Python爬虫", "Python数据可视化",
        "Python爬虫与数据分析", "Python机器学习", "Python深度学习", "Python图像识别",
        "Python自然语言处理", "Python Web", "机器学习", "深度学习", "图像识别", "自然语言处理",
        "推荐系统", "数据库系统", "网络安全", "前端或移动应用", "计算机软件与数据方向",
    ],
    "智能硬件与控制类": [
        "单片机", "51单片机", "STM32", "Arduino", "ESP32", "ESP8266", "ESP32/ESP8266", "嵌入式", "PLC", "西门子PLC", "三菱PLC",
        "欧姆龙PLC", "台达PLC", "LabVIEW", "物联网", "物联网项目", "自动控制", "智能硬件", "智能小车", "智能家居",
        "温湿度检测", "数据采集", "测控系统", "机械手控制", "电梯控制", "交通灯控制", "恒压供水", "智能硬件与控制方向",
    ],
    "机械制造与机电类": [
        "机械设计制造及其自动化", "机械电子工程", "车辆工程", "工业设计", "材料成型及控制工程", "机电一体化",
        "机械结构设计", "机械零件设计", "夹具设计", "模具设计", "数控加工", "机械加工工艺", "CAD建模",
        "SolidWorks设计", "UG设计", "有限元分析", "机电设备设计", "车辆结构设计", "故障诊断", "生产工艺优化",
        "机械制造与机电方向",
    ],
    "土木建筑与交通类": [
        "土木工程", "建筑学", "城乡规划", "道路桥梁与渡河工程", "交通工程", "给排水科学与工程",
        "建筑结构设计", "施工组织设计", "BIM应用", "建筑设计", "道路工程", "桥梁工程", "隧道工程",
        "交通规划", "给排水设计", "项目管理", "施工安全", "土木建筑与交通方向",
    ],
    "电气电子与通信能源类": [
        "电气工程及其自动化", "电子信息工程", "通信工程", "能源与动力工程", "新能源科学与工程", "光电信息科学与工程",
        "供配电设计", "电力系统分析", "继电保护", "电机控制", "变电站设计", "新能源发电", "光伏发电",
        "风力发电", "电子电路设计", "通信系统", "信号处理", "传感器应用", "电气设备故障诊断",
        "电气电子与通信能源方向",
    ],
    "化工材料环境安全类": [
        "化学工程与工艺", "应用化学", "材料科学与工程", "环境工程", "安全工程", "食品科学与工程", "高分子材料与工程",
        "化工工艺设计", "化学实验分析", "材料性能研究", "新材料应用", "污水处理", "废气处理", "固废处理",
        "环境监测", "安全评价", "风险评估", "食品检测", "食品加工工艺", "质量控制", "化工材料环境安全方向",
    ],
    "农林生物资源类": [
        "农学", "园艺", "园林", "林学", "植物保护", "动物医学", "动物科学", "水产养殖学", "农业资源与环境",
        "生物科学", "生物技术", "农林生物资源方向",
    ],
    "理学统计心理类": [
        "数学与应用数学", "统计学", "应用统计学", "物理学", "应用物理学", "化学", "应用化学", "地理科学",
        "心理学", "应用心理学", "理学统计心理方向",
    ],
}

COMMON_KEYWORD_GROUPS: dict[str, tuple[str, tuple[str, ...]]] = {
    "经管财会方向": ("经管财会类", ("财务分析", "财务风险", "内部控制", "成本控制", "税务筹划", "预算管理", "绩效评价", "审计问题", "营销策略", "物流供应链", "物流成本", "信息共享", "电商运营", "经营发展模式", "会计信息披露", "企业社会责任")),
    "教育教学方向": ("教育教学类", ("教学设计", "课堂教学", "幼儿教育", "家园共育", "学生管理", "课程改革", "心理健康教育", "体育训练", "思政教育", "教学", "课程", "中学", "小学", "幼儿园", "语文", "竞赛策划")),
    "医护健康方向": ("医护健康类", ("护理干预", "临床护理", "疾病护理", "老年护理", "儿科护理", "药学服务", "用药分析", "康复训练", "健康管理", "检验检测", "影像诊断", "公共卫生调查")),
    "法学社会公共方向": ("法学社会公共类", ("民商法", "刑法", "劳动法", "知识产权", "行政法", "社会治理", "社区服务", "公共管理", "公共政策", "基层治理", "社会调查")),
    "文学外语传媒方向": ("文学外语传媒类", ("文学作品分析", "语言文字研究", "翻译策略", "新闻传播", "新媒体运营", "广告传播", "短视频传播", "品牌传播", "秘书实务", "电视节目", "节目研究", "语法", "量词", "语言")),
    "艺术设计方向": ("艺术设计类", ("品牌视觉设计", "包装设计", "海报设计", "文创设计", "室内设计", "景观设计", "产品造型", "服装设计", "动画设计", "影视编导", "音乐教学", "舞蹈教学", "声乐", "演唱", "音乐作品", "歌曲")),
    "理学统计心理方向": ("理学统计心理类", ("数学建模", "统计分析", "问卷调查", "数据建模", "物理实验", "化学实验", "地理分析", "心理测量", "心理健康")),
    "机械制造与机电方向": ("机械制造与机电类", ("机械结构设计", "机械零件设计", "夹具设计", "模具设计", "数控加工", "机械加工工艺", "CAD建模", "SolidWorks", "UG", "有限元分析", "机电设备设计", "车辆结构设计", "故障诊断", "生产工艺优化")),
    "土木建筑与交通方向": ("土木建筑与交通类", ("基础设施建设", "建筑结构设计", "施工组织设计", "BIM", "建筑设计", "道路工程", "桥梁工程", "隧道工程", "交通规划", "给排水设计", "施工安全")),
    "电气电子与通信能源方向": ("电气电子与通信能源类", ("供配电设计", "电力系统分析", "继电保护", "电机控制", "变电站设计", "新能源发电", "光伏发电", "风力发电", "电子电路设计", "通信系统", "信号处理", "传感器应用")),
    "化工材料环境安全方向": ("化工材料环境安全类", ("化工工艺设计", "化学实验分析", "材料性能研究", "新材料应用", "污水处理", "废气处理", "固废处理", "环境监测", "安全评价", "风险评估", "食品检测", "质量控制")),
    "农林生物资源方向": ("农林生物资源类", ("作物栽培", "病虫害防治", "园林景观", "动物疾病防治", "畜牧养殖", "水产养殖", "农业资源利用", "生物实验", "生物技术应用")),
}

MAJOR_ALIASES = {
    "护理": "护理学",
    "计算机": "计算机科学与技术",
    "计算机专业": "计算机科学与技术",
    "计算机科学技术": "计算机科学与技术",
    "财会": "会计学",
    "物联网系统": "物联网项目",
    "ESP32": "ESP32/ESP8266",
    "ESP8266": "ESP32/ESP8266",
}

TITLE_DIRECTION_ALIASES: tuple[tuple[str, str], ...] = (
    ("中考化学", "教育教学方向"),
    ("花式跳绳", "体育教育"),
    ("幼儿园", "学前教育"),
    ("学前音乐", "学前教育"),
    ("舞蹈教学", "舞蹈学"),
    ("武术教育", "体育教育"),
    ("营销策略", "市场营销"),
    ("财务分析", "财务管理"),
    ("高中物理", "教育教学方向"),
    ("流动资金", "财务管理"),
    ("产品策略", "市场营销"),
    ("前厅服务质量", "酒店管理"),
    ("利润操纵", "会计学"),
    ("公开招聘", "人力资源管理"),
    ("高管薪酬", "人力资源管理"),
    ("公安工作", "治安学"),
    ("网络攻击", "网络安全"),
    ("顧客", "市场营销"),
    ("忠誠度", "市场营销"),
    ("三大球", "体育教育"),
    ("全域旅游", "旅游管理"),
    ("品牌延伸", "市场营销"),
    ("咖啡厅", "艺术设计方向"),
    ("音乐教育", "音乐学"),
    ("利润分红", "财务管理"),
    ("会计行业", "会计学"),
    ("计算机软件的可专利性", "知识产权"),
    ("可专利性", "知识产权"),
    ("知识产权", "知识产权"),
    ("台湾问题", "政治学与行政学"),
    ("美国因素", "政治学与行政学"),
    ("抢劫出租车", "侦查学"),
    ("侦防", "侦查学"),
    ("补贴政策执行", "行政管理"),
    ("政策执行", "行政管理"),
    ("电力变压器", "电气工程及其自动化"),
    ("变压器绕组", "电气工程及其自动化"),
    ("OFDM", "通信系统"),
    ("卫星移动通信", "通信系统"),
    ("氮化铝晶须", "材料科学与工程"),
    ("太阳能集热器", "材料科学与工程"),
    ("药物制剂", "药学"),
    ("包衣材料", "药学"),
    ("管理会计", "会计学"),
    ("会计目标", "会计学"),
    ("会计核算", "会计学"),
    ("环境会计", "会计学"),
    ("会计岗位", "会计学"),
    ("可转换债券", "金融学"),
    ("商业银行", "金融学"),
    ("村镇银行", "金融学"),
    ("家庭理财", "金融学"),
    ("财务控制", "财务管理"),
    ("财务预算", "财务管理"),
    ("偿债能力", "财务管理"),
    ("客户关系管理", "工商管理"),
    ("生产经营", "工商管理"),
    ("企业成长", "工商管理"),
    ("战略选择", "工商管理"),
    ("企业家社会关系", "工商管理"),
    ("供应链", "物流管理"),
    ("物流", "物流管理"),
    ("库存控制", "物流管理"),
    ("入库作业", "物流管理"),
    ("配送优化", "物流管理"),
    ("贸易壁垒", "国际经济与贸易"),
    ("出口美国", "国际经济与贸易"),
    ("顾客忠诚度", "市场营销"),
    ("狂欢节", "旅游管理"),
    ("节庆", "旅游管理"),
    ("生态旅游", "旅游管理"),
    ("线性空间", "数学与应用数学"),
    ("欧式空间", "数学与应用数学"),
    ("注意偏向", "心理学"),
    ("男篮", "体育教育"),
    ("篮球", "体育教育"),
    ("跆拳道", "体育教育"),
    ("游泳", "体育教育"),
    ("健身", "体育教育"),
    ("社会体育", "体育教育"),
    ("展示设计", "艺术设计方向"),
    ("空间界面设计", "环境设计"),
    ("景观设计", "环境设计"),
    ("声乐", "音乐学"),
    ("演唱", "音乐学"),
    ("合唱", "音乐学"),
    ("素描", "美术学"),
    ("宋词", "汉语言文学"),
    ("话剧", "汉语言文学"),
    ("网络小说", "汉语言文学"),
    ("文学作品", "汉语言文学"),
    ("汉语学习", "汉语言教育"),
    ("语法", "汉语言教育"),
    ("电视节目", "文学外语传媒方向"),
    ("视频帧", "图像识别"),
    ("篡改检验", "图像识别"),
    ("图像取证", "图像识别"),
    ("数字图像", "图像识别"),
    ("自动识别", "图像识别"),
    ("云电表", "物联网项目"),
)

MAJOR_PREFIXES = {
    "财务管理": "FM", "会计学": "ACC", "审计学": "AUD", "工商管理": "BIZ", "市场营销": "MKT",
    "人力资源管理": "HR", "电子商务": "EC", "护理学": "NUR", "学前教育": "PRE", "法学": "LAW",
    "汉语言文学": "CHI", "视觉传达设计": "VCD", "计算机科学与技术": "CS", "软件工程": "SE",
    "机械设计制造及其自动化": "ME", "土木工程": "CE", "电气工程及其自动化": "EE", "环境工程": "ENV",
    "园林": "LA", "应用统计学": "STAT", "心理学": "PSY", "理学统计心理方向": "SCI",
    "Java管理系统": "JAVA", "Python数据可视化": "PYVIS", "Python爬虫与数据分析": "PYC",
    "Python机器学习": "PYML", "Python深度学习": "PYDL", "Python图像识别": "PYIMG",
    "Python自然语言处理": "PYNLP", "Python Web": "PYWEB", "微信小程序": "WX", "前端或移动应用": "APP",
    "STM32": "STM32", "51单片机": "MCS51", "Arduino": "ARD", "ESP32/ESP8266": "ESP",
    "PLC": "PLC", "LabVIEW": "LV", "物联网项目": "IOT",
    "经管财会方向": "ECO", "教育教学方向": "EDU", "医护健康方向": "MED", "法学社会公共方向": "LAW",
    "文学外语传媒方向": "LIT", "艺术设计方向": "ART", "机械制造与机电方向": "MEC",
    "土木建筑与交通方向": "CIV", "电气电子与通信能源方向": "ELE", "化工材料环境安全方向": "CHE",
    "农林生物资源方向": "AGR", UNKNOWN_DIRECTION: "UNK",
}

NOISE_RE = re.compile(r"(专业|本科|专科|班级|班|方向|学院|系|毕业论文|论文|设计|\d{2,4}|（[^）]*）|\([^)]*\)|[\s_\\/\-]+)")
MAJOR_FIELD_RE = re.compile(r"(?m)^[^\S\r\n]*专[^\S\r\n]*业[^\S\r\n]*[:：]?[^\S\r\n]*(?P<value>.+?)\s*$")
BODY_ANCHOR_RE = re.compile(r"(?m)^[^\S\r\n]*(摘要|ABSTRACT|Abstract|目录|第一章|第1章|绪论)")


EXTRA_TITLE_DIRECTION_ALIASES: tuple[tuple[str, str], ...] = (
    ("\u56f4\u57ce", "\u6c49\u8bed\u8a00\u6587\u5b66"),
    ("\u94b1\u949f\u4e66", "\u6c49\u8bed\u8a00\u6587\u5b66"),
    ("\u8bbd\u523a\u827a\u672f", "\u6c49\u8bed\u8a00\u6587\u5b66"),
    ("\u751f\u7269\u70ad", "\u73af\u5883\u5de5\u7a0b"),
    ("\u5438\u9644\u78f7", "\u73af\u5883\u5de5\u7a0b"),
    ("\u9891\u7387\u9009\u62e9\u8d85\u8868\u9762", "\u901a\u4fe1\u5de5\u7a0b"),
    ("\u8d85\u8868\u9762", "\u901a\u4fe1\u5de5\u7a0b"),
    ("\u57ce\u9645\u94c1\u8def", "\u65c5\u6e38\u7ba1\u7406"),
    ("\u516d\u76d8\u6c34", "\u65c5\u6e38\u7ba1\u7406"),
    ("\u65c5\u6e38\u53d1\u5c55", "\u65c5\u6e38\u7ba1\u7406"),
    ("Cu2Se", "\u6750\u6599\u79d1\u5b66\u4e0e\u5de5\u7a0b"),
    ("TMD", "\u6750\u6599\u79d1\u5b66\u4e0e\u5de5\u7a0b"),
    ("\u9541\u57fa\u590d\u5408\u6750\u6599", "\u6750\u6599\u79d1\u5b66\u4e0e\u5de5\u7a0b"),
)


@dataclass(frozen=True)
class SubjectClassificationResult:
    detected_subject_group: str
    detected_subject_group_dir: str
    detected_major_or_direction: str
    subject_detect_source: str
    subject_confidence: str
    tech_type: str
    tech_subtype: str
    tech_keywords: tuple[str, ...]
    tech_confidence: float
    tech_reason: str


def safe_group_dir(group: str) -> str:
    return SUBJECT_GROUP_DIRS.get(group, group.replace("/", "_"))


def _compact(value: str) -> str:
    return re.sub(r"\s+", "", value.strip())


def all_directions() -> dict[str, str]:
    return {direction: group for group, directions in SUBJECT_GROUPS.items() for direction in directions}


def normalize_direction(value: str) -> str | None:
    compact = _compact(value)
    if not compact:
        return None
    directions = sorted(all_directions(), key=len, reverse=True)
    for alias, direction in sorted(MAJOR_ALIASES.items(), key=lambda item: len(item[0]), reverse=True):
        if alias in compact:
            return direction
    for direction in directions:
        if direction in compact:
            return direction
    cleaned = NOISE_RE.sub("", compact)
    for direction in directions:
        if direction == cleaned or direction in cleaned:
            return direction
    return None


def detect_common_direction(value: str) -> str | None:
    compact = _compact(value)
    for fallback_direction, (_group, keywords) in COMMON_KEYWORD_GROUPS.items():
        if any(keyword in compact for keyword in keywords):
            return fallback_direction
    return None


def detect_title_direction(value: str) -> str | None:
    return detect_title_topic_direction(value) or normalize_direction(value) or detect_common_direction(value)


def detect_title_topic_direction(value: str) -> str | None:
    compact = _compact(value)
    for keyword, direction in EXTRA_TITLE_DIRECTION_ALIASES + TITLE_DIRECTION_ALIASES:
        if keyword.casefold() in compact.casefold():
            return direction
    return None


def looks_like_english_humanities_title(value: str) -> bool:
    title = value.strip()
    if not title or re.search(r"[\u4e00-\u9fff]", title):
        return False
    words = re.findall(r"[A-Za-z]{2,}", title)
    if len(words) < 4:
        return False
    technical_words = {
        "python", "java", "spring", "boot", "stm32", "plc", "labview", "android",
        "react", "vue", "system", "algorithm", "database", "mysql",
    }
    return not any(word.casefold() in technical_words for word in words)


def group_for_direction(direction: str) -> str:
    return all_directions().get(direction, UNKNOWN_SUBJECT_GROUP)


def prefix_for_direction(direction: str) -> str:
    return MAJOR_PREFIXES.get(direction, "MAJ")


def front_matter_area(text: str) -> str:
    search_area = text[:3000]
    anchor = BODY_ANCHOR_RE.search(search_area)
    return search_area[: anchor.start()] if anchor else search_area


def detect_major_from_front_matter(text: str) -> str | None:
    area = front_matter_area(text)
    for match in MAJOR_FIELD_RE.finditer(area):
        direction = normalize_direction(match.group("value"))
        if direction:
            return direction
    return detect_loose_major_from_front_matter(area)


def detect_loose_major_from_front_matter(text: str) -> str | None:
    normalized = _compact(text)
    if not normalized:
        return None
    for direction in sorted(all_directions(), key=len, reverse=True):
        key = _compact(direction)
        if key and key in normalized and _has_loose_major_context(normalized, key):
            return direction
    return None


def _has_loose_major_context(normalized: str, direction: str) -> bool:
    contexts = (
        f"\u4e13\u4e1a\u540d\u79f0{direction}",
        f"\u4e13\u4e1a{direction}",
        f"{direction}\u4e13\u4e1a",
        f"{direction}\u5b66\u9662",
        f"{direction}\u7cfb",
        f"\u5b66\u9662{direction}",
        f"\u6240\u5728\u5b66\u9662{direction}",
    )
    return any(context in normalized for context in contexts)


def subject_result(
    direction: str | None,
    source: str,
    confidence: str,
    tech: TechClassificationResult | None = None,
) -> SubjectClassificationResult:
    if direction:
        group = group_for_direction(direction)
        return SubjectClassificationResult(
            group,
            safe_group_dir(group),
            direction,
            source,
            confidence,
            tech.tech_type if tech else NOT_APPLICABLE,
            tech.tech_subtype if tech else NOT_APPLICABLE,
            tech.tech_keywords if tech else (),
            tech.tech_confidence if tech else 0.0,
            tech.tech_reason if tech else "front/path/title subject classification",
        )
    return SubjectClassificationResult(
        UNKNOWN_SUBJECT_GROUP,
        UNKNOWN_SUBJECT_GROUP_DIR,
        UNKNOWN_DIRECTION,
        "unknown",
        "none",
        NOT_APPLICABLE,
        NOT_APPLICABLE,
        (),
        0.0,
        "no subject keywords matched",
    )


def direction_from_tech(tech: TechClassificationResult) -> str | None:
    if tech.tech_type == UNKNOWN_TECH_TYPE:
        return None
    if tech.tech_type == "Java管理系统":
        return "Java管理系统"
    if tech.tech_type == "Python项目":
        return {
            "数据可视化": "Python数据可视化",
            "爬虫与数据分析": "Python爬虫与数据分析",
            "机器学习": "Python机器学习",
            "深度学习": "Python深度学习",
            "图像识别": "Python图像识别",
            "自然语言处理": "Python自然语言处理",
            "Python Web系统": "Python Web",
        }.get(tech.tech_subtype, "Python项目")
    if tech.tech_type == "微信小程序":
        return "微信小程序"
    if tech.tech_type in {"Android App", "Web前端项目", "前端/App项目"}:
        return "前端或移动应用"
    if tech.tech_type == "单片机":
        return tech.tech_subtype if tech.tech_subtype in {"STM32", "51单片机", "Arduino", "ESP32/ESP8266"} else "单片机"
    if tech.tech_type == "PLC":
        return "PLC"
    if tech.tech_type == "LabVIEW":
        return "LabVIEW"
    if tech.tech_type == "物联网项目":
        return "物联网项目"
    if tech.tech_type == "数据库系统":
        return "数据库系统"
    return None


def classify_subject(text: str, original_relative_path: str = "", detected_title: str = "") -> SubjectClassificationResult:
    front_direction = detect_major_from_front_matter(text)
    front_group = group_for_direction(front_direction) if front_direction else ""

    tech = classify_tech(
        detected_title=detected_title,
        original_file_name=original_relative_path.split("/")[-1],
        original_relative_path=original_relative_path,
        sanitized_text=text[:5000],
    )
    tech_direction = direction_from_tech(tech)

    if front_direction and front_group in LOCKED_FRONT_MATTER_GROUPS:
        return subject_result(front_direction, "front_matter", "high")

    title_topic_direction = detect_title_topic_direction(detected_title)
    if title_topic_direction:
        topic_tech = tech if direction_from_tech(tech) == title_topic_direction else None
        return subject_result(title_topic_direction, "title", "medium", topic_tech)

    primary_tech_sources = {"title", "filename", "path"}
    if (
        tech_direction
        and tech.tech_detect_source in primary_tech_sources
        and (not front_direction or front_group in REFINABLE_FRONT_MATTER_GROUPS)
    ):
        return subject_result(tech_direction, tech.tech_detect_source, "high", tech)

    if front_direction:
        return subject_result(front_direction, "front_matter", "high")

    for source, value, confidence in (
        ("title", detected_title, "medium"),
        ("path", original_relative_path, "medium"),
    ):
        direction = detect_title_direction(value)
        if direction:
            return subject_result(direction, source, confidence)
        if source == "title" and looks_like_english_humanities_title(value):
            return subject_result("文学外语传媒方向", source, "low")

    return subject_result(None, "unknown", "none")
