"""
报告生成器 - 生成Markdown报销报告
"""
from pathlib import Path
from datetime import datetime

from models import SelectionResult
from config import SUBSIDY_LIMITS


class ReportGenerator:
    """报告生成器"""

    def generate(self, result: SelectionResult, output_dir: str) -> str:
        """
        生成报销报告（同月份只保留最新一份）

        Args:
            result: 选择结果
            output_dir: 输出目录

        Returns:
            报告文件路径
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        # 使用不带时间戳的文件名，同月份会自动覆盖
        report_file = output_path / f"报销报告_{result.month}.md"

        # 生成Markdown报告
        content = self._generate_markdown(result)

        # 写入文件
        with open(report_file, "w", encoding="utf-8") as f:
            f.write(content)

        return str(report_file)

    def _generate_markdown(self, result: SelectionResult) -> str:
        """生成Markdown报告内容"""
        lines = []

        # 标题
        lines.append("# 发票报销报告")
        lines.append("")
        lines.append(f"**报销月份**: {result.month}")
        lines.append(f"**生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("")
        lines.append("---")
        lines.append("")

        # 汇总信息
        lines.append("## 一、汇总信息")
        lines.append("")
        lines.append("| 项目 | 数量 |")
        lines.append("|------|------|")
        lines.append(f"| 总发票数 | {result.total_count} |")
        lines.append(f"| 可报销数 | {result.selected_count} |")
        lines.append(f"| 多余数 | {result.unused_count} |")
        lines.append(f"| 错误数 | {result.error_count} |")
        lines.append(f"| **可报销总金额** | **{result.total_amount:.2f} 元** |")
        lines.append("")

        # 达标情况
        lines.append("## 二、金额达标情况")
        lines.append("")
        lines.append("| 类型 | 实际金额 | 补贴上限 | 是否达标 | 差额 |")
        lines.append("|------|----------|----------|----------|------|")

        type_names = {
            "dining": "餐饮",
            "transport": "交通",
            "communication": "通讯",
        }

        for key, name in type_names.items():
            actual = result.amounts.get(key, 0)
            limit = SUBSIDY_LIMITS[key]
            status = "✓" if actual >= limit else "✗"
            diff = max(0, limit - actual)
            lines.append(f"| {name} | {actual:.2f}元 | {limit:.2f}元 | {status} | {diff:.2f}元 |")

        lines.append("")

        # 可报销发票明细
        if result.selected_invoices:
            lines.append("## 三、可报销发票明细")
            lines.append("")
            lines.append("| 序号 | 类型 | 金额 | 开票日期 | 销方名称 |")
            lines.append("|------|------|------|----------|----------|")

            for i, inv in enumerate(result.selected_invoices, 1):
                date_str = inv.invoice_date.strftime("%Y-%m-%d") if inv.invoice_date else "N/A"
                seller = (inv.seller_name[:30] + "...") if inv.seller_name and len(inv.seller_name) > 30 else (inv.seller_name or "N/A")
                lines.append(f"| {i} | {inv.invoice_type.name_cn} | {inv.amount:.2f}元 | {date_str} | {seller} |")

            lines.append("")

        # 多余发票明细
        if result.unused_invoices:
            lines.append("## 四、多余发票（保留在发票池中）")
            lines.append("")
            lines.append("| 序号 | 类型 | 金额 | 开票日期 | 销方名称 |")
            lines.append("|------|------|------|----------|----------|")

            for i, inv in enumerate(result.unused_invoices, 1):
                date_str = inv.invoice_date.strftime("%Y-%m-%d") if inv.invoice_date else "N/A"
                seller = (inv.seller_name[:30] + "...") if inv.seller_name and len(inv.seller_name) > 30 else (inv.seller_name or "N/A")
                lines.append(f"| {i} | {inv.invoice_type.name_cn} | {inv.amount:.2f}元 | {date_str} | {seller} |")

            lines.append("")

        # 错误发票明细
        if result.error_invoices:
            lines.append("## 五、错误发票明细")
            lines.append("")
            lines.append("| 序号 | 类型 | 金额 | 开票日期 | 销方名称 | 错误原因 |")
            lines.append("|------|------|------|----------|----------|----------|")

            for i, inv in enumerate(result.error_invoices, 1):
                date_str = inv.invoice_date.strftime("%Y-%m-%d") if inv.invoice_date else "N/A"
                seller = (inv.seller_name[:20] + "...") if inv.seller_name and len(inv.seller_name) > 20 else (inv.seller_name or "N/A")
                reason = "; ".join(inv.reject_reasons) if inv.reject_reasons else "未知错误"
                # 限制错误原因长度
                if len(reason) > 30:
                    reason = reason[:30] + "..."
                lines.append(f"| {i} | {inv.invoice_type.name_cn} | {inv.amount:.2f}元 | {date_str} | {seller} | {reason} |")

            lines.append("")

        # 文件位置说明
        lines.append("---")
        lines.append("")
        lines.append("## 文件位置")
        lines.append("")
        lines.append(f"- **可报销发票**: `output/{result.month}/`")
        lines.append(f"- **多余发票**: 保留在 `invoice/` 目录")
        lines.append(f"- **错误发票**: `output/errors/`")
        lines.append("")

        return "\n".join(lines)

    def print_summary(self, result: SelectionResult):
        """打印汇总信息到控制台"""
        amounts = result.amounts
        type_names = {
            "dining": "餐饮",
            "transport": "交通",
            "communication": "通讯",
        }

        print(f"\n{'='*60}")
        print(f"处理完成 - {result.month}")
        print(f"{'='*60}")
        print(f"选中: {result.selected_count} 张 (移入 output/{result.month}/)")
        print(f"多余: {result.unused_count} 张 (保留在 invoice/)")
        print(f"错误: {result.error_count} 张 (移入 output/errors/)")
        print(f"\n金额达标情况:")

        for key, name in type_names.items():
            actual = amounts.get(key, 0)
            limit = SUBSIDY_LIMITS[key]
            status = "✓" if actual >= limit else "✗"
            print(f"  {name}: {actual:.2f} / {limit:.2f} {status}")

        print(f"\n可报销总金额: {result.total_amount:.2f} 元")
        print(f"{'='*60}\n")
