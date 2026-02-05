"""
发票校验器
"""
from datetime import datetime
from dateutil.relativedelta import relativedelta

from models import InvoiceInfo, InvoiceType, InvoiceStatus
from config import (
    COMPANY_INFO,
    INVOICE_TYPES,
    BLACKLIST_SUPPLIERS,
    ALLOWED_BILLING_PERIOD_MONTHS,
    ERROR_CODES,
)


class InvoiceValidator:
    """发票校验器"""

    def __init__(self, reimbursement_month: str):
        """
        初始化校验器

        Args:
            reimbursement_month: 报销月份，格式 "YYYY-MM"
        """
        self.reimbursement_month = reimbursement_month
        self.reimbursement_date = datetime.strptime(reimbursement_month, "%Y-%m")

        # 计算3个月账期范围
        # 最早日期：往前推2个月，然后设为当月1号
        earliest = self.reimbursement_date - relativedelta(
            months=ALLOWED_BILLING_PERIOD_MONTHS - 1
        )
        self.earliest_date = earliest.replace(day=1)
        # 最晚日期：报销月份的最后一天
        self.latest_date = (self.reimbursement_date + relativedelta(months=1, day=1)) - relativedelta(days=1)

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
        invoice.is_valid = False
        invoice.final_status = InvoiceStatus.ERROR

        # 1. 检查发票类型是否可识别
        if invoice.invoice_type == InvoiceType.UNKNOWN:
            invoice.error_code = ERROR_CODES["未知类型"]
            invoice.reject_reasons.append("无法识别发票类型（非餐饮、交通、通讯）")
            return invoice

        # 2. 检查开票日期
        if not invoice.invoice_date:
            invoice.error_code = ERROR_CODES["识别失败"]
            invoice.reject_reasons.append("无法识别开票日期")
            return invoice

        # 3. 检查是否在3个月账期内
        invoice.within_3_months = self._is_valid_period(invoice)
        if not invoice.within_3_months:
            invoice.error_code = ERROR_CODES["超期"]
            invoice.reject_reasons.append(
                f"发票日期不在有效期内（需在{self.earliest_date.strftime('%Y-%m-%d')}至"
                f"{self.reimbursement_date.strftime('%Y-%m')}之间）"
            )
            return invoice

        # 4. 检查是否在黑名单中
        if self._is_blacklisted(invoice):
            invoice.error_code = ERROR_CODES["黑名单"]
            invoice.reject_reasons.append("销方在异常开票方黑名单中")
            return invoice

        # 5. 检查发票抬头
        if not self._validate_header(invoice):
            invoice.error_code = ERROR_CODES["抬头错误"]
            return invoice

        # 6. 根据发票类型进行特殊校验
        type_valid, type_errors = self._validate_by_type(invoice)
        if not type_valid:
            invoice.error_code = type_errors[0] if type_errors else ERROR_CODES["未知类型"]
            invoice.reject_reasons.extend(type_errors)
            return invoice

        # 通过所有校验
        invoice.is_valid = True
        invoice.final_status = InvoiceStatus.SELECTED  # 默认选中，后续可能被改为UNUSED
        return invoice

    def _is_valid_period(self, invoice: InvoiceInfo) -> bool:
        """检查发票日期是否在有效期内（3个月内）"""
        if not invoice.invoice_date:
            return False
        return self.earliest_date <= invoice.invoice_date <= self.latest_date

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

    def _validate_header(self, invoice: InvoiceInfo) -> bool:
        """检查发票抬头"""
        # 通讯发票允许个人抬头
        if invoice.invoice_type == InvoiceType.COMMUNICATION and invoice.buyer_individual:
            invoice.warnings.append("通讯发票为个人抬头")
            return True

        # 检查公司名称
        if invoice.buyer_name != COMPANY_INFO["name"]:
            invoice.reject_reasons.append(
                f"发票抬头错误：应为'{COMPANY_INFO['name']}'，"
                f"实际为'{invoice.buyer_name or ''}'"
            )
            return False

        # 检查税号
        if invoice.buyer_tax_id != COMPANY_INFO["tax_id"]:
            invoice.reject_reasons.append(
                f"纳税人识别号错误：应为'{COMPANY_INFO['tax_id']}'，"
                f"实际为'{invoice.buyer_tax_id or ''}'"
            )
            return False

        return True

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
            if invoice.seller_name and "超市" in invoice.seller_name:
                if not invoice.items or len(invoice.items) == 0:
                    errors.append("超市购物发票未附销货清单")

        elif invoice.invoice_type == InvoiceType.TRANSPORT:
            # 交通发票检查
            pass  # 交通发票规则相对宽松

        elif invoice.invoice_type == InvoiceType.COMMUNICATION:
            # 通讯发票允许个人抬头
            if invoice.buyer_individual:
                invoice.warnings.append("通讯发票为个人抬头")

        return len(errors) == 0, errors
