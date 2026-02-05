# -*- coding: utf-8 -*-
"""
主处理器 - 协调整个处理流程
"""
import shutil
from pathlib import Path
from datetime import datetime

from models import InvoiceInfo, InvoiceType, InvoiceStatus, SelectionResult
from recognizer import InvoiceRecognizer
from validator import InvoiceValidator
from selector import InvoiceSelector
from file_manager import FileManager
from reporter import ReportGenerator


class InvoiceProcessor:
    """发票处理器"""

    def __init__(self, invoice_pool: str, output_dir: str, month: str):
        """
        初始化处理器

        Args:
            invoice_pool: 发票池目录
            output_dir: 输出目录
            month: 报销月份 (YYYY-MM)
        """
        self.invoice_pool = Path(invoice_pool)
        self.output_dir = output_dir
        self.month = month

        self.recognizer = InvoiceRecognizer()
        self.validator = InvoiceValidator(month)
        self.selector = InvoiceSelector()
        self.file_manager = FileManager(invoice_pool, output_dir, month)
        self.reporter = ReportGenerator()

    def process(self):
        """执行处理流程"""
        # 0. 备份发票池
        self._backup_invoice_pool()

        # 1. 读取已选中的发票（从output/{month}/）
        already_selected = self._load_already_selected()

        if already_selected:
            print(f"发现 {len(already_selected)} 张已选中的发票（从 output/{self.month}/）")

        # 2. 扫描发票池
        invoice_files = self._scan_invoice_pool()

        if not invoice_files:
            print(f"未在 {self.invoice_pool} 中找到发票文件")
            # 即使没有新发票，也生成报告（包含已选中的）
            if already_selected:
                return self._generate_report_with_already_selected(already_selected, [])
            return None

        print(f"找到 {len(invoice_files)} 个待处理发票文件，开始处理...")

        # 3. 识别并校验
        invoices = []
        seen_invoice_numbers = {}  # 用于检测重复发票：发票号 -> 文件路径

        for i, file_path in enumerate(invoice_files, 1):
            print(f"[{i}/{len(invoice_files)}] 处理：{file_path.name}")

            try:
                # 识别
                invoice = self.recognizer.recognize(str(file_path))

                # 检查发票号码是否重复
                if invoice.invoice_number:
                    if invoice.invoice_number in seen_invoice_numbers:
                        existing_file = seen_invoice_numbers[invoice.invoice_number]
                        print(f"  ⚠ 警告: 发票号码重复 {invoice.invoice_number}")
                        print(f"     已存在: {existing_file}")
                        print(f"     当前文件: {file_path.name}")
                        # 标记为重复，在文件名中添加[重复]前缀
                        invoice.is_duplicate = True
                    seen_invoice_numbers[invoice.invoice_number] = file_path.name

                # 校验
                self.validator.validate(invoice)

                # 显示初步结果
                status_icon = "✓" if invoice.is_valid else "✗"
                print(f"  {status_icon} {invoice.invoice_type.name_cn} | "
                      f"金额: {invoice.amount:.2f}元 | "
                      f"日期: {invoice.invoice_date.strftime('%Y-%m-%d') if invoice.invoice_date else 'N/A'}")

                if invoice.reject_reasons:
                    print(f"  原因: {'; '.join(invoice.reject_reasons)}")

                invoices.append(invoice)

            except Exception as e:
                print(f"  ✗ 处理失败: {e}")

        if not invoices:
            print("没有成功识别任何发票")
            if already_selected:
                return self._generate_report_with_already_selected(already_selected, [])
            return None

        # 4. 智能选择（只对新发票）
        print("\n执行智能选择...")
        new_result = self.selector.select(invoices)
        new_result.month = self.month

        # 5. 合并已选中的和新选中的
        final_result = self._merge_results(already_selected, new_result)

        # 6. 确保输出目录存在
        self.file_manager.ensure_directories()

        # 7. 处理文件（移动/重命名）- 只处理新发票
        print("\n处理文件...")
        for inv in invoices:
            try:
                self.file_manager.process_file(inv)
            except Exception as e:
                print(f"  警告: 文件处理失败 - {inv.original_name}: {e}")

        # 8. 生成报告（包含所有发票）
        print("\n生成报告...")
        report_file = self.reporter.generate(final_result, self.output_dir)

        # 9. 打印汇总
        self.reporter.print_summary(final_result)

        return final_result

    def _scan_invoice_pool(self) -> list[Path]:
        """
        扫描发票池

        扫描规则：
        1. 扫描所有.pdf文件（包括带前缀的）
        2. 带前缀的文件说明之前处理过，需要重新处理
        3. 文件名格式：
           - 普通发票: xxx.pdf
           - 多余发票: [多余]xxx.pdf
           - 错误发票: [错误-*]xxx.pdf
        """
        if not self.invoice_pool.exists():
            print(f"警告: 发票池目录不存在 - {self.invoice_pool}")
            return []

        # 获取所有PDF文件
        files = list(self.invoice_pool.glob("*.pdf"))
        files.extend(self.invoice_pool.glob("*.PDF"))

        return sorted(files, key=lambda x: x.name)

    def _load_already_selected(self) -> list[InvoiceInfo]:
        """
        从 output/{month}/ 读取已选中的发票

        从文件名中解析发票信息
        新格式：{类型}_{金额}_{唯一标识}.pdf (英文类型，无商户名)
        旧格式：{类型}_{金额}元_{销方名称}.pdf (中文类型)
        """
        month_dir = Path(self.output_dir) / self.month
        if not month_dir.exists():
            return []

        selected = []
        for file_path in month_dir.glob("*.pdf"):
            try:
                # 从文件名解析信息
                name = file_path.stem  # 去掉.pdf
                parts = name.split('_')

                if len(parts) >= 2:
                    # 提取类型（支持中文和英文）
                    type_str = parts[0]
                    invoice_type = self._parse_type(type_str)

                    # 提取金额（支持带"元"和不带"元"的格式）
                    amount = 0.0
                    for part in parts[1:]:
                        # 去除可能的后缀（如唯一标识数字）
                        amount_str = part
                        if '_' in amount_str:
                            amount_str = amount_str.split('_')[0]
                        # 移除"元"字符
                        amount_str = amount_str.replace('元', '')
                        try:
                            amount = float(amount_str)
                            break
                        except ValueError:
                            continue

                    # 创建发票对象
                    invoice = InvoiceInfo(
                        file_path=str(file_path),
                        original_name=file_path.name,
                        invoice_type=invoice_type,
                        amount=amount,
                        invoice_date=None,  # 已选中的发票，日期未知
                        is_valid=True,
                        within_3_months=True,
                        final_status=InvoiceStatus.SELECTED,
                    )
                    selected.append(invoice)
            except Exception:
                # 解析失败，跳过
                continue

        return selected

    def _parse_type(self, type_str: str) -> InvoiceType:
        """从类型字符串解析发票类型（支持中文和英文）"""
        type_map = {
            # 中文类型
            "餐饮": InvoiceType.DINING,
            "交通": InvoiceType.TRANSPORT,
            "通讯": InvoiceType.COMMUNICATION,
            # 英文类型
            "dining": InvoiceType.DINING,
            "transport": InvoiceType.TRANSPORT,
            "communication": InvoiceType.COMMUNICATION,
        }
        return type_map.get(type_str.lower(), InvoiceType.UNKNOWN)

    def _merge_results(
        self,
        already_selected: list[InvoiceInfo],
        new_result: SelectionResult
    ) -> SelectionResult:
        """
        合并已选中的和新选中的发票

        Args:
            already_selected: 已选中的发票列表
            new_result: 新发票的选择结果

        Returns:
            合并后的结果
        """
        from models import SelectionResult, InvoiceStatus

        merged = SelectionResult(month=self.month)

        # 已选中的发票直接加入
        merged.selected_invoices.extend(already_selected)

        # 新选中的发票加入
        merged.selected_invoices.extend(new_result.selected_invoices)

        # 多余和错误发票直接来自新结果
        merged.unused_invoices.extend(new_result.unused_invoices)
        merged.error_invoices.extend(new_result.error_invoices)

        return merged

    def _generate_report_with_already_selected(
        self,
        already_selected: list[InvoiceInfo],
        unused: list[InvoiceInfo]
    ) -> SelectionResult:
        """
        只生成已选中发票的报告（没有新发票时）

        Args:
            already_selected: 已选中的发票
            unused: 多余发票

        Returns:
            报告结果
        """
        from models import SelectionResult

        result = SelectionResult(month=self.month)
        result.selected_invoices = already_selected
        result.unused_invoices = unused

        # 生成报告
        self.reporter.generate(result, self.output_dir)
        self.reporter.print_summary(result)

        return result

    def _backup_invoice_pool(self):
        """
        备份发票池目录

        只复制invoice中新增的文件到invoice_bak目录（不覆盖已存在的文件）
        """
        if not self.invoice_pool.exists():
            return

        # 创建备份目录路径
        backup_dir = self.invoice_pool.parent / f"{self.invoice_pool.name}_bak"

        try:
            # 确保备份目录存在
            backup_dir.mkdir(parents=True, exist_ok=True)

            # 获取已备份的文件名集合
            existing_files = {f.name for f in backup_dir.glob("*.pdf") if f.is_file()}

            # 只复制新文件
            new_files_count = 0
            for file_path in self.invoice_pool.glob("*.pdf"):
                if file_path.is_file() and file_path.name not in existing_files:
                    shutil.copy2(file_path, backup_dir / file_path.name)
                    new_files_count += 1

            if new_files_count > 0:
                print(f"已备份 {new_files_count} 个新增文件到: {backup_dir}")
            else:
                print(f"没有新增文件需要备份")

        except Exception as e:
            print(f"警告: 备份失败 - {e}")
            # 备份失败不影响主流程，继续执行
