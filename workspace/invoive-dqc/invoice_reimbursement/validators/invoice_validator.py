"""
发票校验器
"""
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from typing import Optional

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from models import InvoiceInfo, InvoiceType, InvoiceStatus
from config import (
    COMPANY_INFO,
    INVOICE_TYPES,
    BLACKLIST_SUPPLIERS,
    ALLOWED_BILLING_PERIOD_MONTHS,
)


class InvoiceValidator:
    """发票校验器"""

    def __init__(self, reimbursement_month: str, employee_name: str):
        """
        初始化校验器

        Args:
            reimbursement_month: 报销月份，格式 "YYYY-MM"
            employee_name: 员工姓名
        """
        self.reimbursement_month = reimbursement_month
        self.employee_name = employee_name
        self.reimbursement_date = datetime.strptime(reimbursement_month, "%Y-%m")

        # 计算3个月账期范围
        self.earliest_date = self.reimbursement_date - relativedelta(
            months=ALLOWED_BILLING_PERIOD_MONTHS - 1
        ).replace(day=1)
        self.latest_date = self.reimbursement_date + relativedelta(months=0, day=31)

    def validate(self, invoice: InvoiceInfo) -> InvoiceInfo:
        """
        校验发票

        Args:
            invoice: 发票信息

        Returns:
            更新了校验结果的发票对象
        """
        invoice.reject_reasons = []
        invoice.warnings = []

        # 1. 分类发票类型
        invoice.invoice_type = self._classify_invoice(invoice)

        # 2. 检查是否在黑名单中
        if self._is_blacklisted(invoice):
            invoice.reject_reasons.append("销方在异常开票方黑名单中")
            invoice.status = InvoiceStatus.INVALID
            return invoice

        # 3. 检查发票日期是否在有效期内
        if not self._is_valid_period(invoice):
            invoice.reject_reasons.append(
                f"发票日期不在有效期内（需在{self.earliest_date.strftime('%Y-%m-%d')}至"
                f"{self.reimbursement_date.strftime('%Y-%m')}之间）"
            )
            invoice.status = InvoiceStatus.INVALID
            return invoice

        # 4. 根据发票类型进行校验
        type_valid, type_errors = self._validate_by_type(invoice)
        if not type_valid:
            invoice.reject_reasons.extend(type_errors)
            invoice.status = InvoiceStatus.INVALID
            return invoice

        # 5. 检查发票抬头
        header_valid, header_errors = self._validate_header(invoice)
        if not header_valid:
            invoice.reject_reasons.extend(header_errors)
            invoice.status = InvoiceStatus.INVALID
            return invoice

        # 6. 检查发票专用章
        if not invoice.has_seal:
            invoice.warnings.append("未检测到发票专用章")

        # 通过校验
        invoice.status = InvoiceStatus.VALID
        return invoice

    def _classify_invoice(self, invoice: InvoiceInfo) -> InvoiceType:
        """分类发票类型"""
        text = " ".join(invoice.items + [invoice.seller_name or ""])

        # 检查餐饮
        if any(kw in text for kw in INVOICE_TYPES["dining"]["keywords"]):
            return InvoiceType.DINING

        # 检查交通
        if any(kw in text for kw in INVOICE_TYPES["transport"]["keywords"]):
            return InvoiceType.TRANSPORT

        # 检查通讯
        if any(kw in text for kw in INVOICE_TYPES["communication"]["keywords"]):
            return InvoiceType.COMMUNICATION

        return InvoiceType.UNKNOWN

    def _is_blacklisted(self, invoice: InvoiceInfo) -> bool:
        """检查是否在黑名单中"""
        for supplier in BLACKLIST_SUPPLIERS:
            # 按名称匹配
            if invoice.seller_name and supplier["name"] in invoice.seller_name:
                return True
            # 按别名匹配
            if "alias" in supplier and supplier["alias"] in (invoice.seller_name or ""):
                return True
            # 按税号匹配
            if invoice.seller_tax_id and invoice.seller_tax_id == supplier["tax_id"]:
                return True
        return False

    def _is_valid_period(self, invoice: InvoiceInfo) -> bool:
        """检查发票日期是否在有效期内（3个月内）"""
        if not invoice.invoice_date:
            return False

        return (
            self.earliest_date
            <= invoice.invoice_date
            <= self.latest_date
        )

    def _validate_by_type(self, invoice: InvoiceInfo) -> tuple[bool, list[str]]:
        """根据发票类型校验"""
        errors = []

        if invoice.invoice_type == InvoiceType.DINING:
            # 餐饮发票检查
            items_text = " ".join(invoice.items)
            # 检查是否包含不合规的商品
            for pattern in INVOICE_TYPES["dining"]["invalid_patterns"]:
                if pattern in items_text:
                    errors.append(f"餐饮发票包含不可报销项目：{pattern}")
                    return False, errors

            # 检查超市购物是否附清单
            if "超市" in (invoice.seller_name or ""):
                if not invoice.items or len(invoice.items) == 0:
                    errors.append("超市购物发票未附销货清单")

        elif invoice.invoice_type == InvoiceType.TRANSPORT:
            # 交通发票检查
            pass  # 交通发票规则相对宽松

        elif invoice.invoice_type == InvoiceType.COMMUNICATION:
            # 通讯发票可以是个人抬头
            if invoice.buyer_individual:
                invoice.warnings.append("通讯发票为个人抬头")

        elif invoice.invoice_type == InvoiceType.UNKNOWN:
            errors.append("无法识别发票类型（非餐饮、交通、通讯）")

        return len(errors) == 0, errors

    def _validate_header(self, invoice: InvoiceInfo) -> tuple[bool, list[str]]:
        """检查发票抬头"""
        errors = []

        # 通讯发票允许个人抬头
        if invoice.invoice_type == InvoiceType.COMMUNICATION and invoice.buyer_individual:
            return True, []

        # 检查公司名称
        if invoice.buyer_name != COMPANY_INFO["name"]:
            errors.append(
                f"发票抬头错误：应为'{COMPANY_INFO['name']}'，"
                f"实际为'{invoice.buyer_name or ''}'"
            )

        # 检查税号
        if invoice.buyer_tax_id != COMPANY_INFO["tax_id"]:
            errors.append(
                f"纳税人识别号错误：应为'{COMPANY_INFO['tax_id']}'，"
                f"实际为'{invoice.buyer_tax_id or ''}'"
            )

        return len(errors) == 0, errors
