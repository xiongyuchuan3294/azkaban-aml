"""
配置文件
"""

# 公司开票信息
COMPANY_INFO = {
    "name": "深圳前海微众银行股份有限公司",
    "tax_id": "9144030031977063XH",
    "bank_name": "招商银行股份有限公司深圳前海分行",
    "bank_account": "755924276310998",
    "address": "深圳市前海深港合作区南山街道金港街88号微众银行大厦1层、7-10层、12-21层、23-30层",
    "phone": "0755-89462525",
}

# 发票类型配置
INVOICE_TYPES = {
    "dining": {
        "name": "餐饮发票",
        "keywords": ["餐饮", "食品", "餐饮服务", "货物*食品", "无形资产*会员权益-餐饮权益"],
        "valid_categories": ["食品", "餐饮", "餐饮服务"],
        "invalid_patterns": ["充值权益", "会员卡", "预付卡"],
    },
    "transport": {
        "name": "交通发票",
        "keywords": [
            "客运服务", "交通运输", "加油", "停车", "过路费", "通行费",
            "高铁", "动车", "机票", "网约车", "出租车", "代驾", "租车",
            "车辆维修", "车辆检测", "电车充电"
        ],
    },
    "communication": {
        "name": "通讯发票",
        "keywords": ["通讯", "话费", "宽带", "网费", "通讯服务", "电信服务"],
    },
}

# 异常开票方黑名单（来自文档）
BLACKLIST_SUPPLIERS = [
    {"name": "青浦金泽状元楼饭店", "tax_id": "92310118MA1M03P30U"},
    {"name": "武汉东湖新技术开发区肖记关山坊牛肉杂鱼馆", "tax_id": "92420100MA4JKNCN8Q"},
    {"name": "武汉东湖新技术开发区丽波小吃店", "tax_id": "92420100MA4EMY608Y", "alias": "山城记·爆炒川菜馆"},
    {"name": "永川区丰鑫火锅店", "tax_id": "925502000000090576694"},
    {"name": "云岩一烙锅餐饮店", "tax_id": "92520103MADGY75YXP"},
    {"name": "武汉驰天餐饮管理有限公司", "tax_id": "91420105MA4K4EK93K", "alias": "烫锅鲜砂锅串串(钟家村店)"},
    {"name": "武汉市江汉区杨占峰川菜馆", "tax_id": "92420103MA4J0JM97N", "alias": "老四川菜馆"},
    {"name": "武汉市江汉区活力酷鱼餐厅", "tax_id": "92420103MA4EB3953G", "alias": "鱼酷(武汉江汉龙湖店)"},
]

# 路径配置
INPUT_DIR = "output/invoices"
OUTPUT_DIR = "output/reports"

# 发票允许的账期（月）
ALLOWED_BILLING_PERIOD_MONTHS = 3

# 识别配置
OCR_ENGINE = "paddle"  # 可选: "paddle" 或 "tesseract"
