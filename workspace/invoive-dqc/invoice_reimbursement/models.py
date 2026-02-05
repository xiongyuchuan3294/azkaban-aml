"""
数据模型定义
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List
from enum import Enum


class InvoiceType(Enum):
    """发票类型"""
    DINING = "dining"  # 餐饮
    TRANSPORT = "transport"  # 交通
    COMMUNICATION = "communication"  # 通讯
    UNKNOWN = "unknown"  # 未知


class InvoiceStatus(Enum):
    """发票校验状态"""
    VALID = "valid"  # 可报销
    INVALID = "invalid"  # 不可报销
    WARNING = "warning"  # 有警告但可报销


@dataclass
class InvoiceInfo:
    """发票信息模型"""
    # 基础信息
    file_name: str  # 来源文件名
    invoice_code: Optional[str] = None  # 发票代码
    invoice_number: Optional[str] = None  # 发票号码
    invoice_date: Optional[datetime] = None  # 开票日期

    # 金额信息
    amount: Optional[float] = None  # 金额
    tax_amount: Optional[float] = None  # 税额
    total_amount: Optional[float] = None  # 价税合计

    # 购方信息（我方）
    buyer_name: Optional[str] = None  # 购方名称
    buyer_tax_id: Optional[str] = None  # 购方税号
    buyer_individual: Optional[str] = None  # 个人抬头（通讯发票）

    # 销方信息
    seller_name: Optional[str] = None  # 销方名称
    seller_tax_id: Optional[str] = None  # 销方税号

    # 商品/服务信息
    items: List[str] = field(default_factory=list)  # 商品明细
    category: Optional[str] = None  # 发票类型分类

    # 其他
    invoice_type: InvoiceType = InvoiceType.UNKNOWN  # 报销类别
    has_seal: bool = False  # 是否有发票专用章

    # 校验结果
    status: InvoiceStatus = InvoiceStatus.INVALID
    reject_reasons: List[str] = field(default_factory=list)  # 不通过原因
    warnings: List[str] = field(default_factory=list)  # 警告信息

    def to_dict(self) -> dict:
        """转换为字典"""
        return {
            "文件名": self.file_name,
            "发票代码": self.invoice_code or "",
            "发票号码": self.invoice_number or "",
            "开票日期": self.invoice_date.strftime("%Y-%m-%d") if self.invoice_date else "",
            "金额": f"{self.amount:.2f}" if self.amount else "",
            "价税合计": f"{self.total_amount:.2f}" if self.total_amount else "",
            "购方名称": self.buyer_name or "",
            "购方税号": self.buyer_tax_id or "",
            "个人抬头": self.buyer_individual or "",
            "销方名称": self.seller_name or "",
            "销方税号": self.seller_tax_id or "",
            "商品明细": "; ".join(self.items),
            "报销类别": self.invoice_type.value,
            "状态": self.status.value,
            "不通过原因": "; ".join(self.reject_reasons),
            "警告": "; ".join(self.warnings),
        }


@dataclass
class ReimbursementReport:
    """报销报告"""
    month: str  # 报销月份 YYYY-MM
    employee_name: str  # 员工姓名
    total_invoices: int = 0  # 总发票数
    valid_invoices: List[InvoiceInfo] = field(default_factory=list)  # 可报销发票
    invalid_invoices: List[InvoiceInfo] = field(default_factory=list)  # 不可报销发票
    total_amount: float = 0.0  # 可报销总金额

    def add_invoice(self, invoice: InvoiceInfo):
        """添加发票到报告"""
        self.total_invoices += 1
        if invoice.status == InvoiceStatus.VALID:
            self.valid_invoices.append(invoice)
            if invoice.total_amount:
                self.total_amount += invoice.total_amount
        else:
            self.invalid_invoices.append(invoice)
