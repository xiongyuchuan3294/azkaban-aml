"""
OCR基类
"""
from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Tuple
import re


class BaseOCR(ABC):
    """OCR识别基类"""

    @abstractmethod
    def recognize(self, file_path: str) -> str:
        """
        识别文件中的文字

        Args:
            file_path: 文件路径（支持PDF、图片）

        Returns:
            识别出的文本内容
        """
        pass

    def parse_invoice(self, text: str) -> dict:
        """
        从识别文本中解析发票信息

        Args:
            text: OCR识别的文本

        Returns:
            包含发票信息的字典
        """
        result = {}

        # 解析发票代码
        result["invoice_code"] = self._extract_invoice_code(text)

        # 解析发票号码
        result["invoice_number"] = self._extract_invoice_number(text)

        # 解析开票日期
        result["invoice_date"] = self._extract_date(text)

        # 解析金额
        result["amount"], result["total_amount"] = self._extract_amount(text)

        # 解析购方信息
        result["buyer_name"], result["buyer_tax_id"] = self._extract_buyer_info(text)
        result["buyer_individual"] = self._extract_individual_name(text)

        # 解析销方信息
        result["seller_name"], result["seller_tax_id"] = self._extract_seller_info(text)

        # 解析商品明细
        result["items"] = self._extract_items(text)

        return result

    def _extract_invoice_code(self, text: str) -> str:
        """提取发票代码"""
        patterns = [
            r"发票代码[：:]\s*(\d{10,12})",
            r"发票代码\s*(\d{10,12})",
            r"Code[：:]\s*(\d{10,12})",
        ]
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                return match.group(1)
        return ""

    def _extract_invoice_number(self, text: str) -> str:
        """提取发票号码"""
        patterns = [
            r"发票号码[：:]\s*(\d{8})",
            r"No[.:]\s*(\d{8})",
            r"号码[：:]\s*(\d{8})",
        ]
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                return match.group(1)
        return ""

    def _extract_date(self, text: str) -> str:
        """提取开票日期"""
        patterns = [
            r"开票日期[：:]\s*(\d{4})年(\d{1,2})月(\d{1,2})日",
            r"开票日期[：:]\s*(\d{4})-(\d{1,2})-(\d{1,2})",
            r"Date[：:]\s*(\d{4})-(\d{1,2})-(\d{1,2})",
        ]
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                year, month, day = match.groups()
                return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
        return ""

    def _extract_amount(self, text: str) -> Tuple[float, float]:
        """提取金额"""
        amount = 0.0
        total_amount = 0.0

        # 价税合计
        patterns_total = [
            r"价税合计[：:]\s*¥?([0-9,]+\.?\d*)",
            r"合计[：:]\s*¥?([0-9,]+\.?\d*)",
        ]
        for pattern in patterns_total:
            match = re.search(pattern, text)
            if match:
                total_amount = float(match.group(1).replace(",", ""))
                break

        # 金额
        patterns_amount = [
            r"金额[：:]\s*¥?([0-9,]+\.?\d*)",
        ]
        for pattern in patterns_amount:
            match = re.search(pattern, text)
            if match:
                amount = float(match.group(1).replace(",", ""))
                break

        return amount, total_amount

    def _extract_buyer_info(self, text: str) -> Tuple[str, str]:
        """提取购方信息"""
        name = ""
        tax_id = ""

        # 购方名称
        patterns_name = [
            r"购买方[名称]+[：:]\s*([^\n]+?)(?=\s|$|统一社会信用代码|纳税人识别号)",
            r"买方[：:]\s*([^\n]+?)(?=\s|$|统一社会信用代码|纳税人识别号)",
        ]
        for pattern in patterns_name:
            match = re.search(pattern, text)
            if match:
                name = match.group(1).strip()
                break

        # 购方税号
        patterns_tax = [
            r"购买方.*?纳税人识别号[：:]\s*([A-Z0-9]{15,20})",
            r"买方.*?纳税人识别号[：:]\s*([A-Z0-9]{15,20})",
            r"统一社会信用代码[：:]\s*([A-Z0-9]{15,20})",
        ]
        for pattern in patterns_tax:
            match = re.search(pattern, text)
            if match:
                tax_id = match.group(1).strip()
                break

        return name, tax_id

    def _extract_individual_name(self, text: str) -> str:
        """提取个人姓名（通讯发票可能是个抬头）"""
        # 查找个人名字模式
        patterns = [
            r"姓名[：:]\s*([\u4e00-\u9fa5]{2,4})",
            r"用户[：:]\s*([\u4e00-\u9fa5]{2,4})",
        ]
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                return match.group(1).strip()
        return ""

    def _extract_seller_info(self, text: str) -> Tuple[str, str]:
        """提取销方信息"""
        name = ""
        tax_id = ""

        # 销方名称
        patterns_name = [
            r"销售方[名称]+[：:]\s*([^\n]+?)(?=\s|$|统一社会信用代码|纳税人识别号)",
            r"卖方[：:]\s*([^\n]+?)(?=\s|$|统一社会信用代码|纳税人识别号)",
            r"开票方[：:]\s*([^\n]+)",
        ]
        for pattern in patterns_name:
            match = re.search(pattern, text)
            if match:
                name = match.group(1).strip()
                break

        # 销方税号
        patterns_tax = [
            r"销售方.*?纳税人识别号[：:]\s*([A-Z0-9]{15,20})",
            r"卖方.*?纳税人识别号[：:]\s*([A-Z0-9]{15,20})",
        ]
        for pattern in patterns_tax:
            match = re.search(pattern, text)
            if match:
                tax_id = match.group(1).strip()
                break

        return name, tax_id

    def _extract_items(self, text: str) -> List[str]:
        """提取商品明细"""
        items = []

        # 常见商品名称模式
        patterns = [
            r"货物或应税劳务[、:]?服务名称\s*([^\n]+)",
            r"项目名称[：:]\s*([^\n]+)",
            r"商品名称[：:]\s*([^\n]+)",
        ]

        for pattern in patterns:
            matches = re.findall(pattern, text)
            items.extend(matches)

        return list(set(items))  # 去重
