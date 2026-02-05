#!/usr/bin/env python3
"""
发票自动筛选报销系统 - 主入口
"""
import argparse
from pathlib import Path
from datetime import datetime
import pandas as pd

from ocr import PaddleOCR
from validators import InvoiceValidator
from models import InvoiceInfo, ReimbursementReport
from config import INPUT_DIR, OUTPUT_DIR


class InvoiceReimbursementSystem:
    """发票报销系统"""

    def __init__(self, reimbursement_month: str, employee_name: str):
        """
        初始化系统

        Args:
            reimbursement_month: 报销月份 YYYY-MM
            employee_name: 员工姓名
        """
        self.reimbursement_month = reimbursement_month
        self.employee_name = employee_name
        self.ocr = PaddleOCR()
        self.validator = InvoiceValidator(reimbursement_month, employee_name)
        self.report = ReimbursementReport(
            month=reimbursement_month,
            employee_name=employee_name
        )

    def process(self, input_dir: str = None):
        """
        处理发票

        Args:
            input_dir: 发票目录，默认使用配置中的目录
        """
        input_path = Path(input_dir or INPUT_DIR)

        if not input_path.exists():
            print(f"错误：发票目录不存在 - {input_path}")
            return

        # 获取所有发票文件
        invoice_files = self._get_invoice_files(input_path)

        if not invoice_files:
            print(f"未找到发票文件，支持格式：.pdf, .jpg, .jpeg, .png")
            return

        print(f"找到 {len(invoice_files)} 个发票文件，开始处理...")

        for i, file_path in enumerate(invoice_files, 1):
            print(f"[{i}/{len(invoice_files)}] 处理：{file_path.name}")

            try:
                # OCR识别
                text = self.ocr.recognize(str(file_path))

                # 解析发票信息
                parsed_data = self.ocr.parse_invoice(text)

                # 构建发票对象
                invoice = InvoiceInfo(
                    file_name=file_path.name,
                    invoice_code=parsed_data.get("invoice_code"),
                    invoice_number=parsed_data.get("invoice_number"),
                    invoice_date=self._parse_date(parsed_data.get("invoice_date")),
                    amount=parsed_data.get("amount"),
                    total_amount=parsed_data.get("total_amount"),
                    buyer_name=parsed_data.get("buyer_name"),
                    buyer_tax_id=parsed_data.get("buyer_tax_id"),
                    buyer_individual=parsed_data.get("buyer_individual"),
                    seller_name=parsed_data.get("seller_name"),
                    seller_tax_id=parsed_data.get("seller_tax_id"),
                    items=parsed_data.get("items", []),
                )

                # 校验发票
                self.validator.validate(invoice)

                # 添加到报告
                self.report.add_invoice(invoice)

                # 显示结果
                status_icon = "✅" if invoice.status.value == "valid" else "❌"
                print(
                    f"  {status_icon} {invoice.invoice_type.value} | "
                    f"金额: {invoice.total_amount or 0:.2f} | "
                    f"状态: {invoice.status.value}"
                )
                if invoice.reject_reasons:
                    print(f"  原因: {'; '.join(invoice.reject_reasons)}")

            except Exception as e:
                print(f"  ❌ 处理失败: {e}")

        # 输出报告
        self._output_report()

    def _get_invoice_files(self, directory: Path) -> list[Path]:
        """获取发票文件列表"""
        extensions = [".pdf", ".jpg", ".jpeg", ".png"]
        files = []

        for ext in extensions:
            files.extend(directory.glob(f"*{ext}"))
            files.extend(directory.glob(f"*{ext.upper()}"))

        return sorted(files, key=lambda x: x.name)

    def _parse_date(self, date_str: str) -> datetime | None:
        """解析日期字符串"""
        if not date_str:
            return None
        try:
            return datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            return None

    def _output_report(self):
        """输出报销报告"""
        output_path = Path(OUTPUT_DIR)
        output_path.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = output_path / f"报销报告_{self.reimbursement_month}_{self.employee_name}_{timestamp}.xlsx"

        # 准备数据
        valid_data = [inv.to_dict() for inv in self.report.valid_invoices]
        invalid_data = [inv.to_dict() for inv in self.report.invalid_invoices]

        # 创建Excel报告
        with pd.ExcelWriter(report_file, engine="openpyxl") as writer:
            # 可报销发票
            if valid_data:
                df_valid = pd.DataFrame(valid_data)
                df_valid.to_excel(writer, sheet_name="可报销发票", index=False)

            # 不可报销发票
            if invalid_data:
                df_invalid = pd.DataFrame(invalid_data)
                df_invalid.to_excel(writer, sheet_name="不可报销发票", index=False)

            # 汇总信息
            summary_data = {
                "项目": ["员工姓名", "报销月份", "总发票数", "可报销发票数", "不可报销发票数", "可报销总金额"],
                "值": [
                    self.employee_name,
                    self.reimbursement_month,
                    self.report.total_invoices,
                    len(self.report.valid_invoices),
                    len(self.report.invalid_invoices),
                    f"{self.report.total_amount:.2f}",
                ],
            }
            df_summary = pd.DataFrame(summary_data)
            df_summary.to_excel(writer, sheet_name="汇总", index=False)

        print(f"\n{'='*60}")
        print(f"处理完成！")
        print(f"{'='*60}")
        print(f"总发票数: {self.report.total_invoices}")
        print(f"可报销: {len(self.report.valid_invoices)} 张，金额: {self.report.total_amount:.2f} 元")
        print(f"不可报销: {len(self.report.invalid_invoices)} 张")
        print(f"\n报告已保存: {report_file}")
        print(f"{'='*60}")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="发票自动筛选报销系统")
    parser.add_argument("--month", required=True, help="报销月份，格式: YYYY-MM")
    parser.add_argument("--employee", required=True, help="员工姓名")
    parser.add_argument("--input", help="发票目录路径（可选）")

    args = parser.parse_args()

    # 验证月份格式
    try:
        datetime.strptime(args.month, "%Y-%m")
    except ValueError:
        print("错误：月份格式不正确，应为 YYYY-MM")
        return

    # 创建并运行系统
    system = InvoiceReimbursementSystem(args.month, args.employee)
    system.process(args.input)


if __name__ == "__main__":
    main()
