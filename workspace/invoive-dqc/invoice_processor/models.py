"""
数据模型
"""
import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional


def sanitize_filename(name: str) -> str:
    """清理文件名中的非法字符"""
    if not name:
        return "未知"
    # 移除或替换非法字符
    name = re.sub(r'[\n\r\t]+', '', name)  # 移除换行、制表符
    name = re.sub(r'[<>:"/\\|?*]', '', name)  # 移除Windows非法字符
    name = name.strip()
    # 限制长度
    if len(name) > 50:
        name = name[:50]
    return name or "未知"


class InvoiceType(Enum):
    """发票类型"""
    DINING = "dining"
    TRANSPORT = "transport"
    COMMUNICATION = "communication"
    UNKNOWN = "unknown"

    @property
    def name_cn(self) -> str:
        """中文名称"""
        return {
            InvoiceType.DINING: "餐饮",
            InvoiceType.TRANSPORT: "交通",
            InvoiceType.COMMUNICATION: "通讯",
            InvoiceType.UNKNOWN: "未知",
        }[self]

    @property
    def limit_key(self) -> str:
        """对应补贴上限的key"""
        return self.value


class InvoiceStatus(Enum):
    """发票处理状态"""
    SELECTED = "selected"      # 选中
    UNUSED = "unused"          # 多余
    ERROR = "error"            # 错误


@dataclass
class InvoiceInfo:
    """发票信息"""
    # 文件信息
    file_path: str
    original_name: str

    # 识别信息
    invoice_type: InvoiceType = InvoiceType.UNKNOWN
    amount: float = 0.0              # 金额（不含税，用于累加）
    tax_amount: float = 0.0          # 税额
    total_amount: float = 0.0        # 价税合计
    invoice_date: Optional[datetime] = None
    invoice_code: Optional[str] = None
    invoice_number: Optional[str] = None

    # 销方信息
    seller_name: Optional[str] = None
    seller_tax_id: Optional[str] = None

    # 购方信息
    buyer_name: Optional[str] = None
    buyer_tax_id: Optional[str] = None
    buyer_individual: Optional[str] = None  # 个人抬头

    # 货物/服务信息
    items: list[str] = field(default_factory=list)

    # 校验结果
    is_valid: bool = False           # 基本校验通过
    within_3_months: bool = False
    has_seal: bool = False           # 发票专用章
    is_duplicate: bool = False       # 是否重复发票
    reject_reasons: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    # 选择结果
    final_status: InvoiceStatus = InvoiceStatus.ERROR
    error_code: str = ""             # 错误简码

    @property
    def _type_name(self) -> str:
        """类型中文名"""
        return self.invoice_type.name_cn

    @property
    def _seller_short(self) -> str:
        """销方简称（用于文件名）"""
        if not self.seller_name:
            return "未知销方"
        # 限制长度
        return self.seller_name[:20] if len(self.seller_name) <= 20 else self.seller_name[:20]

    @property
    def new_name(self) -> str:
        """生成新文件名（使用英文类型名，不包含商户名）"""
        ext = ".pdf"

        # 重复发票标记（使用错误格式）
        if self.is_duplicate:
            return f"[ERROR-DUP]{self.original_name}"

        # 获取唯一标识：优先使用发票号码后6位，否则使用原始文件名中的数字部分
        invoice_suffix = ""
        if self.invoice_number and len(self.invoice_number) >= 6:
            invoice_suffix = f"_{self.invoice_number[-6:]}"
        else:
            # 从原始文件名中提取数字作为备选唯一标识
            import re
            numbers = re.findall(r'\d+', self.original_name)
            if numbers:
                # 取最长的数字串的后6位
                longest = max(numbers, key=len)
                invoice_suffix = f"_{longest[-6:]}"

        # 英文类型名
        type_en = self.invoice_type.value

        if self.final_status == InvoiceStatus.SELECTED:
            # 选中: {类型}_{金额}_{唯一标识}.pdf
            return f"{type_en}_{self.amount:.2f}{invoice_suffix}{ext}"
        elif self.final_status == InvoiceStatus.UNUSED:
            # 多余: [EXTRA]{类型}_{金额}_{唯一标识}.pdf
            return f"[EXTRA]{type_en}_{self.amount:.2f}{invoice_suffix}{ext}"
        else:  # ERROR
            if not self.is_valid:
                # 识别失败
                if self.invoice_type == InvoiceType.UNKNOWN:
                    return f"[ERROR-RECOG]{self.original_name}"
                # 错误: [ERROR-{原因}]{类型}_{金额}_{唯一标识}.pdf
                reason = self.error_code if self.error_code else "UNKNOWN"
                return f"[ERROR-{reason}]{type_en}_{self.amount:.2f}{invoice_suffix}{ext}"
            else:
                # 校验通过但未被选中（理论上不会到这里）
                return f"[EXTRA]{type_en}_{self.amount:.2f}{invoice_suffix}{ext}"

    def to_dict(self) -> dict:
        """转换为字典（用于Excel输出）"""
        return {
            "原始文件名": self.original_name,
            "发票类型": self._type_name,
            "金额(元)": f"{self.amount:.2f}",
            "税额(元)": f"{self.tax_amount:.2f}",
            "价税合计(元)": f"{self.total_amount:.2f}",
            "开票日期": self.invoice_date.strftime("%Y-%m-%d") if self.invoice_date else "",
            "发票代码": self.invoice_code or "",
            "发票号码": self.invoice_number or "",
            "销方名称": self.seller_name or "",
            "销方税号": self.seller_tax_id or "",
            "购方名称": self.buyer_name or "",
            "购方税号": self.buyer_tax_id or "",
            "货物/服务": "; ".join(self.items) if self.items else "",
            "处理状态": self._get_status_text(),
            "错误原因": "; ".join(self.reject_reasons) if self.reject_reasons else "",
            "警告": "; ".join(self.warnings) if self.warnings else "",
        }

    def _get_status_text(self) -> str:
        """获取状态文本"""
        status_map = {
            InvoiceStatus.SELECTED: "选中",
            InvoiceStatus.UNUSED: "多余",
            InvoiceStatus.ERROR: "错误",
        }
        return status_map.get(self.final_status, "未知")


@dataclass
class SelectionResult:
    """选择结果"""
    month: str
    selected_invoices: list[InvoiceInfo] = field(default_factory=list)
    unused_invoices: list[InvoiceInfo] = field(default_factory=list)
    error_invoices: list[InvoiceInfo] = field(default_factory=list)

    @property
    def selected_count(self) -> int:
        return len(self.selected_invoices)

    @property
    def unused_count(self) -> int:
        return len(self.unused_invoices)

    @property
    def error_count(self) -> int:
        return len(self.error_invoices)

    @property
    def total_count(self) -> int:
        return self.selected_count + self.unused_count + self.error_count

    @property
    def amounts(self) -> dict[str, float]:
        """各类型选中金额"""
        amounts = {
            "dining": 0.0,
            "transport": 0.0,
            "communication": 0.0,
        }
        for inv in self.selected_invoices:
            amounts[inv.invoice_type.value] += inv.amount
        return amounts

    @property
    def total_amount(self) -> float:
        """选中总金额"""
        return sum(inv.amount for inv in self.selected_invoices)

    def get_summary(self) -> dict:
        """获取汇总信息"""
        from config import SUBSIDY_LIMITS

        amounts = self.amounts
        return {
            "报销月份": self.month,
            "总发票数": self.total_count,
            "可报销数": self.selected_count,
            "多余数": self.unused_count,
            "错误数": self.error_count,
            "餐饮金额": f"{amounts['dining']:.2f}",
            "餐饮上限": f"{SUBSIDY_LIMITS['dining']:.2f}",
            "餐饮达标": "是" if amounts['dining'] >= SUBSIDY_LIMITS['dining'] else "否",
            "交通金额": f"{amounts['transport']:.2f}",
            "交通上限": f"{SUBSIDY_LIMITS['transport']:.2f}",
            "交通达标": "是" if amounts['transport'] >= SUBSIDY_LIMITS['transport'] else "否",
            "通讯金额": f"{amounts['communication']:.2f}",
            "通讯上限": f"{SUBSIDY_LIMITS['communication']:.2f}",
            "通讯达标": "是" if amounts['communication'] >= SUBSIDY_LIMITS['communication'] else "否",
            "可报销总金额": f"{self.total_amount:.2f}",
        }
