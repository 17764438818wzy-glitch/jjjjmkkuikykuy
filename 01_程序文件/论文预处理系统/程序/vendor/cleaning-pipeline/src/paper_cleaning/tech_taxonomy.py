from __future__ import annotations

from dataclasses import dataclass

UNKNOWN_TECH_TYPE = "未识别技术类型"
UNKNOWN_TECH_SUBTYPE = "未识别技术子类"


@dataclass(frozen=True)
class TechRule:
    tech_type: str
    tech_subtype: str
    strong_keywords: tuple[str, ...]
    weak_keywords: tuple[str, ...]
    priority: int


TECH_RULES: tuple[TechRule, ...] = (
    TechRule(
        "Python项目",
        "图像识别",
        ("YOLO", "OpenCV", "图像识别", "图像分类", "目标检测", "人脸识别", "车牌识别", "病虫害识别", "缺陷检测"),
        ("Python", "CNN"),
        980,
    ),
    TechRule(
        "Python项目",
        "深度学习",
        ("深度学习", "神经网络", "CNN", "RNN", "LSTM", "Transformer", "PyTorch", "TensorFlow", "Keras"),
        ("Python", "YOLO", "目标检测"),
        960,
    ),
    TechRule(
        "Python项目",
        "自然语言处理",
        ("自然语言处理", "NLP", "文本分类", "文本摘要", "命名实体识别", "BERT", "词向量", "知识图谱"),
        ("Python", "情感分析"),
        950,
    ),
    TechRule(
        "单片机",
        "STM32",
        ("STM32", "STM32F103", "STM32F407", "STM32单片机", "HAL库", "CubeMX"),
        ("Keil", "单片机", "嵌入式控制"),
        940,
    ),
    TechRule(
        "单片机",
        "51单片机",
        ("51单片机", "STC89C52", "STC89C51", "AT89C51", "AT89C52", "8051"),
        ("单片机",),
        935,
    ),
    TechRule(
        "PLC",
        "西门子PLC",
        ("西门子PLC", "S7-200", "S7-300", "S7-1200", "S7-1500", "TIA Portal", "STEP7"),
        ("PLC", "可编程控制器"),
        930,
    ),
    TechRule(
        "PLC",
        "三菱PLC",
        ("三菱PLC", "FX2N", "FX3U", "GX Works", "GX Developer"),
        ("PLC", "可编程控制器"),
        925,
    ),
    TechRule(
        "PLC",
        "欧姆龙PLC",
        ("欧姆龙PLC", "Omron", "CP1H", "CP1E", "CX-Programmer"),
        ("PLC", "可编程控制器"),
        920,
    ),
    TechRule(
        "PLC",
        "台达PLC",
        ("台达PLC", "Delta PLC", "DVP系列"),
        ("PLC", "可编程控制器"),
        915,
    ),
    TechRule(
        "LabVIEW",
        "数据采集",
        ("LabVIEW", "数据采集", "DAQ", "传感器采集", "信号采集", "采集系统"),
        ("测控",),
        900,
    ),
    TechRule(
        "LabVIEW",
        "虚拟仪器",
        ("LabVIEW", "虚拟仪器", "上位机", "仪器控制"),
        (),
        890,
    ),
    TechRule(
        "LabVIEW",
        "自动测试",
        ("LabVIEW", "自动测试", "测试系统", "测试平台", "检测系统"),
        (),
        885,
    ),
    TechRule(
        "LabVIEW",
        "测控系统",
        ("LabVIEW", "测控系统", "测量控制", "温度控制", "压力检测", "转速检测"),
        (),
        880,
    ),
    TechRule(
        "Java管理系统",
        "管理系统",
        ("Spring Boot", "SpringBoot", "SSM", "SpringMVC", "Servlet", "JSP", "MyBatis", "Java"),
        ("Vue", "Element UI", "MySQL", "管理系统", "后台管理系统", "信息管理系统", "毕业设计管理系统", "图书管理系统", "学生管理系统", "宿舍管理系统", "停车管理系统", "商城系统"),
        870,
    ),
    TechRule(
        "微信小程序",
        "小程序系统",
        ("微信小程序", "uni-app", "云开发", "WXML", "WXSS"),
        ("小程序", "校园小程序", "商城小程序", "预约小程序", "点餐小程序"),
        850,
    ),
    TechRule(
        "Android App",
        "移动应用",
        ("Android", "Android Studio", "Java Android", "Kotlin"),
        ("安卓", "App", "移动应用", "移动端系统"),
        830,
    ),
    TechRule(
        "Python项目",
        "机器学习",
        ("机器学习", "分类模型", "预测模型", "随机森林", "支持向量机", "SVM", "决策树", "KNN", "朴素贝叶斯", "XGBoost", "LightGBM", "回归模型"),
        ("Python",),
        820,
    ),
    TechRule(
        "Python项目",
        "爬虫与数据分析",
        ("爬虫", "网络爬虫", "Scrapy", "Requests", "BeautifulSoup", "数据采集", "评论分析", "情感分析", "文本分析"),
        ("Python", "数据分析"),
        810,
    ),
    TechRule(
        "Python项目",
        "数据可视化",
        ("数据可视化", "可视化分析", "ECharts", "Matplotlib", "Pyecharts", "可视化系统", "可视化平台"),
        ("Python", "数据分析"),
        800,
    ),
    TechRule(
        "Python项目",
        "Python Web系统",
        ("Django", "Flask", "FastAPI"),
        ("Python", "Web系统", "后台系统", "管理平台", "信息系统"),
        790,
    ),
    TechRule(
        "Web前端项目",
        "前端系统",
        ("JavaScript", "前端", "前后端分离", "Ant Design", "可视化大屏"),
        ("Vue", "React", "HTML", "CSS", "Element UI"),
        760,
    ),
    TechRule(
        "单片机",
        "Arduino",
        ("Arduino", "Arduino UNO", "Arduino Mega"),
        ("单片机",),
        750,
    ),
    TechRule(
        "单片机",
        "ESP32/ESP8266",
        ("ESP32", "ESP8266", "WiFi模块", "物联网节点"),
        ("单片机", "物联网"),
        745,
    ),
    TechRule(
        "单片机",
        "其他单片机",
        ("单片机", "MCU", "嵌入式控制", "智能小车", "温湿度检测", "智能家居", "报警系统", "自动浇花", "智能门禁"),
        (),
        700,
    ),
    TechRule(
        "PLC",
        "其他PLC",
        ("PLC", "可编程控制器", "电梯控制", "机械手控制", "交通灯控制", "生产线控制", "恒压供水", "变频控制"),
        ("自动控制",),
        690,
    ),
    TechRule(
        "物联网项目",
        "物联网系统",
        ("物联网", "IoT", "MQTT", "NB-IoT", "LoRa", "ZigBee", "传感器网络", "云平台", "远程监控"),
        ("智能家居", "环境监测"),
        650,
    ),
    TechRule(
        "数据库系统",
        "数据库应用",
        ("数据库设计", "ER图", "数据表设计", "SQL Server", "Oracle"),
        ("数据库", "MySQL", "信息管理系统"),
        650,
    ),
    TechRule(
        "其他软件项目",
        "未细分软件项目",
        ("软件系统", "Web系统", "后台系统", "管理平台", "信息系统", "系统设计"),
        ("管理系统", "平台设计", "系统实现"),
        200,
    ),
    TechRule(
        "其他工程控制项目",
        "未细分工程控制项目",
        ("控制系统", "自动控制", "测控", "嵌入式", "电气控制"),
        ("检测系统", "控制设计"),
        180,
    ),
)
