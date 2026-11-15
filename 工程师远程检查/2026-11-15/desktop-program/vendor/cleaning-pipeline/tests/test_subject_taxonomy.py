from __future__ import annotations

import pytest

from src.paper_cleaning.subject_taxonomy import classify_subject


def assert_subject(text: str, group: str, direction: str) -> None:
    result = classify_subject(text=text, original_relative_path="", detected_title=text)
    assert result.detected_subject_group == group
    assert result.detected_major_or_direction == direction


def test_finance_management_subject() -> None:
    assert_subject("专业：财务管理\n摘要\n正文", "经管财会类", "财务管理")


def test_nursing_subject() -> None:
    assert_subject("专业：护理学\n摘要\n正文", "医护健康类", "护理学")


def test_preschool_education_subject() -> None:
    assert_subject("专业：学前教育\n摘要\n正文", "教育教学类", "学前教育")


def test_law_subject() -> None:
    assert_subject("专业：法学\n摘要\n正文", "法学社会公共类", "法学")


def test_chinese_literature_subject() -> None:
    assert_subject("专业：汉语言文学\n摘要\n正文", "文学外语传媒类", "汉语言文学")


def test_visual_communication_subject() -> None:
    assert_subject("专业：视觉传达设计\n摘要\n正文", "艺术设计类", "视觉传达设计")


def test_spring_boot_direction_overrides_major() -> None:
    assert_subject("基于Spring Boot的学生信息管理系统设计", "计算机软件与数据类", "Java管理系统")


def test_python_visualization_direction() -> None:
    assert_subject("基于Python的旅游数据可视化分析系统", "计算机软件与数据类", "Python数据可视化")


def test_yolo_direction() -> None:
    assert_subject("基于Python和YOLO的水果识别系统", "计算机软件与数据类", "Python图像识别")


def test_stm32_direction() -> None:
    assert_subject("基于STM32的智能家居控制系统设计", "智能硬件与控制类", "STM32")


def test_51_mcu_direction() -> None:
    assert_subject("基于51单片机的温湿度检测系统设计", "智能硬件与控制类", "51单片机")


def test_plc_direction() -> None:
    assert_subject("基于PLC的电梯控制系统设计", "智能硬件与控制类", "PLC")


def test_labview_direction() -> None:
    assert_subject("基于LabVIEW的数据采集系统设计", "智能硬件与控制类", "LabVIEW")


def test_mechanical_subject() -> None:
    assert_subject("专业：机械设计制造及其自动化\n摘要", "机械制造与机电类", "机械设计制造及其自动化")


def test_civil_subject() -> None:
    assert_subject("专业：土木工程\n摘要", "土木建筑与交通类", "土木工程")


def test_electrical_subject() -> None:
    assert_subject("专业：电气工程及其自动化\n摘要", "电气电子与通信能源类", "电气工程及其自动化")


def test_environment_subject() -> None:
    assert_subject("专业：环境工程\n摘要", "化工材料环境安全类", "环境工程")


def test_landscape_subject() -> None:
    assert_subject("专业：园林\n摘要", "农林生物资源类", "园林")


def test_applied_statistics_subject() -> None:
    assert_subject("专业：应用统计学\n摘要", "理学统计心理类", "应用统计学")


def test_psychology_subject() -> None:
    assert_subject("专业：心理学\n摘要", "理学统计心理类", "心理学")


def test_unknown_subject() -> None:
    result = classify_subject(text="摘要\n无明显关键词", original_relative_path="", detected_title="无明显关键词")

    assert result.detected_subject_group == "未识别/其他类"
    assert result.detected_subject_group_dir == "未识别_其他类"
    assert result.detected_major_or_direction == "未识别方向"


def test_front_matter_major_locks_non_technical_subject() -> None:
    result = classify_subject(
        text=(
            "题目：会计信息披露质量研究\n"
            "专业：会计学\n"
            "摘要\n"
            "正文中可能提到数据库、React、信息系统，但这不是项目型论文。"
        ),
        original_relative_path="混乱目录/unknown.txt",
        detected_title="会计信息披露质量研究",
    )

    assert result.detected_subject_group == "经管财会类"
    assert result.detected_major_or_direction == "会计学"
    assert result.tech_type == "不适用"


def test_front_matter_education_major_not_replaced_by_body_direction() -> None:
    result = classify_subject(
        text="题目：幼儿园课程研究\n专业：学前教育\n摘要\n本文讨论课堂教学和学生管理。",
        original_relative_path="乱目录/sample.txt",
        detected_title="幼儿园课程研究",
    )

    assert result.detected_subject_group == "教育教学类"
    assert result.detected_major_or_direction == "学前教育"


def test_title_technical_route_beats_body_prefix_noise() -> None:
    result = classify_subject(
        text=(
            "摘要\n"
            "本文前部误提到51单片机和AT89C51作为对比方案。\n"
            "后文研究温度采集系统。"
        ),
        original_relative_path="2019年初整理890篇/(论文)基于labview的温度采集系统设计.txt",
        detected_title="基于LabVIEW的温度采集系统设计",
    )

    assert result.detected_subject_group == "智能硬件与控制类"
    assert result.detected_major_or_direction == "LabVIEW"
    assert result.tech_type == "LabVIEW"


def test_body_prefix_weak_database_word_does_not_create_software_project() -> None:
    result = classify_subject(
        text="摘要\nPPP模式在基础设施建设项目中的应用研究。正文提到资料数据库用于归档。",
        original_relative_path="PPP模式在基础设施建设项目中应用研究.txt",
        detected_title="PPP模式在基础设施建设项目中应用研究",
    )

    assert result.detected_subject_group != "计算机软件与数据类"
    assert result.detected_major_or_direction != "数据库系统"


def test_iot_project_maps_to_hardware_control_group() -> None:
    result = classify_subject(
        text="摘要\n基于物联网和MQTT的环境监测系统设计。",
        original_relative_path="基于物联网和MQTT的环境监测系统设计.txt",
        detected_title="基于物联网和MQTT的环境监测系统设计",
    )

    assert result.detected_subject_group == "智能硬件与控制类"
    assert result.detected_major_or_direction == "物联网项目"


def test_education_title_beats_body_prefix_legal_word() -> None:
    result = classify_subject(
        text="摘要\n正文后面可能出现法学、社会治理等对比词，但论文主题是语文教学。",
        original_relative_path="3.23 初中语文语法教学存在的问题及相应的解决策略.docx",
        detected_title="初中语文语法教学存在的问题及相应的解决策略",
    )

    assert result.detected_subject_group == "教育教学类"
    assert result.detected_major_or_direction == "汉语言教育"


def test_business_title_beats_body_prefix_python_noise() -> None:
    result = classify_subject(
        text="摘要\n正文算法部分提到Python、机器学习、预测模型作为工具。",
        original_relative_path="信息共享对物流成本影响的研究.docx",
        detected_title="信息共享对物流成本影响的研究",
    )

    assert result.detected_subject_group == "经管财会类"
    assert result.detected_major_or_direction == "物流管理"
    assert result.tech_type == "不适用"


def test_media_title_beats_body_prefix_python_noise() -> None:
    result = classify_subject(
        text="摘要\n正文中偶然出现Python、爬虫、数据分析等研究方法。",
        original_relative_path="定稿 周六夜现场.docx",
        detected_title="美国电视节目《周六夜现场》研究",
    )

    assert result.detected_subject_group == "文学外语传媒类"
    assert result.detected_major_or_direction == "文学外语传媒方向"
    assert result.tech_type == "不适用"


def test_music_title_goes_to_art_design() -> None:
    result = classify_subject(
        text="摘要\n本文分析声乐作品演唱处理。",
        original_relative_path="论声乐作品《桃花红杏花白》的演唱处理.docx",
        detected_title="论声乐作品《桃花红杏花白》的演唱处理",
    )

    assert result.detected_subject_group == "艺术设计类"
    assert result.detected_major_or_direction == "音乐学"


def test_accounting_title_keyword_goes_to_business_accounting() -> None:
    result = classify_subject(
        text="摘要\n正文较短。",
        original_relative_path="企业目标与会计目标的基本关系.docx",
        detected_title="企业目标与会计目标的基本关系",
    )

    assert result.detected_subject_group == "经管财会类"
    assert result.detected_major_or_direction == "会计学"


def test_bank_title_keyword_goes_to_finance() -> None:
    result = classify_subject(
        text="摘要\n正文中可能出现教学、教育等非主题词。",
        original_relative_path="关于陕西省村镇银行发展的调查报告.docx",
        detected_title="关于陕西省村镇银行发展的调查报告",
    )

    assert result.detected_subject_group == "经管财会类"
    assert result.detected_major_or_direction == "金融学"


def test_festival_tourism_title_goes_to_tourism_management() -> None:
    result = classify_subject(
        text="摘要\n正文可能出现建筑、交通等背景词。",
        original_relative_path="临沧市沧源县佤族摸你黑狂欢节开展现状研究.docx",
        detected_title="临沧市沧源县佤族摸你黑狂欢节开展现状研究",
    )

    assert result.detected_subject_group == "经管财会类"
    assert result.detected_major_or_direction == "旅游管理"


def test_english_literature_title_goes_to_literature_media() -> None:
    result = classify_subject(
        text="Abstract\nThis paper studies literature.",
        original_relative_path="the Escapism of Densher in the Wings of the Dove.docx",
        detected_title="the Escapism of Densher in the Wings of the Dove",
    )

    assert result.detected_subject_group == "文学外语传媒类"
    assert result.detected_major_or_direction == "文学外语传媒方向"


@pytest.mark.parametrize(
    ("title", "body", "group", "direction"),
    [
        ("信丰物流入库作业优化方案", "摘要\n本文使用LabVIEW进行数据采集。", "经管财会类", "物流管理"),
        ("大数据背景下中小企业管理会计的发展前景分析", "目录\n物联网技术概述", "经管财会类", "会计学"),
        ("台湾问题中的美国因素", "摘要\n本文采用文本分析方法。", "法学社会公共类", "政治学与行政学"),
        ("海口华夏通物流服务有限公司配送优化方案设计", "摘要\n正文提到CNN方法。", "经管财会类", "物流管理"),
        ("同源视频帧内复制-粘贴篡改检验", "摘要\n正文提到SSM。", "计算机软件与数据类", "图像识别"),
    ],
)
def test_title_subject_beats_secondary_technical_noise(
    title: str,
    body: str,
    group: str,
    direction: str,
) -> None:
    result = classify_subject(
        text=body,
        original_relative_path=f"{title}.docx",
        detected_title=title,
    )

    assert result.detected_subject_group == group
    assert result.detected_major_or_direction == direction
    assert result.tech_type == "不适用"


@pytest.mark.parametrize(
    ("title", "group", "direction"),
    [
        ("计算机软件的可专利性研究", "法学社会公共类", "知识产权"),
        ("发行可转换债券对公司持续经营的影响分析", "经管财会类", "金融学"),
        ("抢劫出租车案件的特点与侦防对策研究", "法学社会公共类", "侦查学"),
        ("电力变压器绕组变形检测技术", "电气电子与通信能源类", "电气工程及其自动化"),
        ("供应链环境下制造企业库存控制优化研究", "经管财会类", "物流管理"),
        ("展示设计中的场景化研究", "艺术设计类", "艺术设计方向"),
        ("论线性空间与欧式空间的对比", "理学统计心理类", "数学与应用数学"),
        ("宾川县宾居镇农业补贴政策执行的问题研究", "法学社会公共类", "行政管理"),
    ],
)
def test_clear_title_topics_are_classified_without_body_guessing(
    title: str,
    group: str,
    direction: str,
) -> None:
    result = classify_subject(
        text="摘要\n正文没有可用于分类的额外信息。",
        original_relative_path=f"{title}.docx",
        detected_title=title,
    )

    assert result.detected_subject_group == group
    assert result.detected_major_or_direction == direction


@pytest.mark.parametrize(
    ("title", "group", "direction"),
    [
        ("东莞市明德普华会计师事务所流动资金管理优化方案", "经管财会类", "财务管理"),
        ("从中美饮食文化差异分析肯德基的产品策略", "经管财会类", "市场营销"),
        ("上海万和昊美艺术酒店前厅服务质量管理探究", "经管财会类", "酒店管理"),
        ("浅谈企业利润操纵方法和防范", "经管财会类", "会计学"),
        ("北京市朝阳区事业单位公开招聘问题分析", "经管财会类", "人力资源管理"),
        ("我国上市公司高管薪酬与企业绩效相关性研究", "经管财会类", "人力资源管理"),
        ("论公安工作中如何正确对待少数民族风俗习惯禁忌", "法学社会公共类", "治安学"),
        ("网络攻击得行为分析", "计算机软件与数据类", "网络安全"),
        ("對澳門居民蘋果手機顧客的忠誠度分析", "经管财会类", "市场营销"),
        ("云南省三大球周末联赛发展研究", "教育教学类", "体育教育"),
        ("云南省全域旅游发展的思考", "经管财会类", "旅游管理"),
        ("品牌延伸问题与对策研究", "经管财会类", "市场营销"),
        ("形式美在工业风咖啡厅中的应用表现", "艺术设计类", "艺术设计方向"),
        ("改生态学视角下普通学校音乐教育研究", "艺术设计类", "音乐学"),
        ("对上市公司中小股东利润分红问题分析", "经管财会类", "财务管理"),
        ("“互联网+”时代会计行业的发展趋势", "经管财会类", "会计学"),
    ],
)
def test_additional_clear_title_topics_are_classified_conservatively(
    title: str,
    group: str,
    direction: str,
) -> None:
    result = classify_subject(
        text="摘要\n正文没有可用于分类的额外信息。",
        original_relative_path=f"{title}.docx",
        detected_title=title,
    )

    assert result.detected_subject_group == group
    assert result.detected_major_or_direction == direction


@pytest.mark.parametrize(
    ("title", "group", "direction"),
    [
        ("2019年秋季长沙市雨花区初级中学花式跳绳竞赛策划方案", "教育教学类", "体育教育"),
        ("幼儿园口语教学中的互动探究", "教育教学类", "学前教育"),
        ("学前音乐的游戏化教学中教学节奏研究", "教育教学类", "学前教育"),
        ("浅谈新媒体元素应用于民族舞蹈教学的思考", "艺术设计类", "舞蹈学"),
        ("小学武术教育的发展前景", "教育教学类", "体育教育"),
        ("中国移动龙胜分公司营销策略研究", "经管财会类", "市场营销"),
        ("东莞鑫德公司科研项目立项报告的财务分析调研报告", "经管财会类", "财务管理"),
        ("人教版高中物理“思考与讨论”栏目研究", "教育教学类", "教育教学方向"),
        ("中考化学复习浅谈", "教育教学类", "教育教学方向"),
    ],
)
def test_more_clear_title_topics_receive_specific_direction(
    title: str,
    group: str,
    direction: str,
) -> None:
    result = classify_subject(
        text="摘要\n正文没有可用于分类的额外信息。",
        original_relative_path=f"{title}.docx",
        detected_title=title,
    )

    assert result.detected_subject_group == group
    assert result.detected_major_or_direction == direction


def test_loose_cover_major_name_classifies_chinese_literature() -> None:
    result = classify_subject(
        text=(
            "DITY_CLASSIFICATION_HINT_START\n"
            "\u5b66\u9662\u540d\u79f0 \u4e13\u4e1a\u540d\u79f0 \u6c49\u8bed\u8a00\u6587\u5b66 "
            "\u5b66\u751f\u59d3\u540d \u67d0\u67d0\n"
            "DITY_CLASSIFICATION_HINT_END\n"
            "\u6458\u8981\n\u6b63\u6587"
        ),
        original_relative_path="unknown.txt",
        detected_title="\u5bf9\u94b1\u949f\u4e66\u300a\u56f4\u57ce\u300b\u8bbd\u523a\u827a\u672f\u7684\u7c97\u6d45\u63a2\u8ba8",
    )

    assert result.detected_subject_group == "\u6587\u5b66\u5916\u8bed\u4f20\u5a92\u7c7b"
    assert result.detected_major_or_direction == "\u6c49\u8bed\u8a00\u6587\u5b66"


def test_table_cover_major_classifies_computer_science() -> None:
    result = classify_subject(
        text=(
            "DITY_CLASSIFICATION_HINT_START\n"
            "\u5c42\u6b21 \u4e13\u5347\u672c \u4e13\u4e1a \u8ba1\u7b97\u673a\u79d1\u5b66\u4e0e\u6280\u672f "
            "\u6307\u5bfc\u6559\u5e08 \u67d0\u67d0\n"
            "DITY_CLASSIFICATION_HINT_END\n"
            "\u6458\u8981\n\u6b63\u6587"
        ),
        original_relative_path="unknown.txt",
        detected_title="\u6821\u56ed\u4e8c\u624b\u7269\u54c1\u4ea4\u6613\u7f51\u7ad9\u8bbe\u8ba1\u4e0e\u5b9e\u73b0",
    )

    assert result.detected_subject_group == "\u8ba1\u7b97\u673a\u8f6f\u4ef6\u4e0e\u6570\u636e\u7c7b"
    assert result.detected_major_or_direction == "\u8ba1\u7b97\u673a\u79d1\u5b66\u4e0e\u6280\u672f"


def test_college_name_hint_classifies_communication_engineering() -> None:
    result = classify_subject(
        text=(
            "DITY_CLASSIFICATION_HINT_START\n"
            "\u6240\u5728\u5b66\u9662\uff1a \u4fe1\u606f\u4e0e\u901a\u4fe1\u5de5\u7a0b\u5b66\u9662\n"
            "DITY_CLASSIFICATION_HINT_END\n"
            "\u6458\u8981\n\u6b63\u6587"
        ),
        original_relative_path="unknown.txt",
        detected_title="\u529f\u7387\u76f8\u5173\u7684\u53ef\u91cd\u6784\u9891\u7387\u9009\u62e9\u8d85\u8868\u9762\u7814\u7a76",
    )

    assert result.detected_subject_group == "\u7535\u6c14\u7535\u5b50\u4e0e\u901a\u4fe1\u80fd\u6e90\u7c7b"
    assert result.detected_major_or_direction == "\u901a\u4fe1\u5de5\u7a0b"


def test_title_keyword_fallback_classifies_biochar_as_environment_engineering() -> None:
    result = classify_subject(
        text="\u6458\u8981\n\u6b63\u6587\u6ca1\u6709\u89c4\u8303\u4e13\u4e1a\u5b57\u6bb5",
        original_relative_path="unknown.txt",
        detected_title="\u751f\u7269\u70ad\u5236\u5907\u539f\u6750\u6599\u5bf9\u5176\u5438\u9644\u78f7\u7684\u5f71\u54cd",
    )

    assert result.detected_subject_group == "\u5316\u5de5\u6750\u6599\u73af\u5883\u5b89\u5168\u7c7b"
    assert result.detected_major_or_direction == "\u73af\u5883\u5de5\u7a0b"
