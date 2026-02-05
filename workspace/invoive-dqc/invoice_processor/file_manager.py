"""
文件管理器 - 处理发票文件的移动和重命名
"""
import shutil
from pathlib import Path
from typing import Optional

from models import InvoiceInfo, InvoiceStatus


class FileManager:
    """文件管理器"""

    def __init__(self, invoice_pool: str, output_base: str, month: str):
        """
        初始化文件管理器

        Args:
            invoice_pool: 发票池目录
            output_base: 输出基础目录
            month: 报销月份
        """
        self.invoice_pool = Path(invoice_pool)
        self.output_base = Path(output_base)
        self.month = month

    def process_file(self, invoice: InvoiceInfo) -> Optional[str]:
        """
        处理单个发票文件

        Args:
            invoice: 发票信息

        Returns:
            最终文件路径，如果处理失败返回None
        """
        src = self.invoice_pool / invoice.original_name

        # 检查源文件是否存在
        if not src.exists():
            print(f"  警告: 源文件不存在 - {src}")
            return None

        if invoice.final_status == InvoiceStatus.SELECTED:
            return self._move_selected(invoice)
        elif invoice.final_status == InvoiceStatus.ERROR:
            return self._move_error(invoice)
        elif invoice.final_status == InvoiceStatus.UNUSED:
            return self._rename_unused(invoice)
        else:
            print(f"  警告: 未知状态 - {invoice.final_status}")
            return None

    def _move_selected(self, invoice: InvoiceInfo) -> str:
        """移动选中发票到月份目录"""
        target_dir = self.output_base / self.month
        target_dir.mkdir(parents=True, exist_ok=True)

        src = self.invoice_pool / invoice.original_name
        dst = target_dir / invoice.new_name

        shutil.move(str(src), str(dst))
        return str(dst)

    def _move_error(self, invoice: InvoiceInfo) -> str:
        """移动错误发票到errors目录"""
        target_dir = self.output_base / "errors"
        target_dir.mkdir(parents=True, exist_ok=True)

        src = self.invoice_pool / invoice.original_name
        dst = target_dir / invoice.new_name

        shutil.move(str(src), str(dst))
        return str(dst)

    def _rename_unused(self, invoice: InvoiceInfo) -> str:
        """重命名多余发票（保留在原池中）"""
        src = self.invoice_pool / invoice.original_name
        dst = self.invoice_pool / invoice.new_name

        src.rename(dst)
        return str(dst)

    def ensure_directories(self):
        """确保所有输出目录存在"""
        (self.output_base / self.month).mkdir(parents=True, exist_ok=True)
        (self.output_base / "errors").mkdir(parents=True, exist_ok=True)
