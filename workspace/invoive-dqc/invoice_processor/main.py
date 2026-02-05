#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
发票智能筛选系统 - 主入口
"""
import argparse
import sys
from datetime import datetime

from processor import InvoiceProcessor
from config import DEFAULT_INVOICE_POOL, DEFAULT_OUTPUT_DIR


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description="发票智能筛选系统 - 从发票池中筛选当月可报销发票"
    )
    parser.add_argument(
        "--month",
        required=True,
        help="报销月份，格式: YYYY-MM，如 2026-01"
    )
    parser.add_argument(
        "--invoice-pool",
        default=DEFAULT_INVOICE_POOL,
        help=f"发票池目录（默认: {DEFAULT_INVOICE_POOL}）"
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help=f"输出目录（默认: {DEFAULT_OUTPUT_DIR}）"
    )

    args = parser.parse_args()

    # 验证月份格式
    try:
        datetime.strptime(args.month, "%Y-%m")
    except ValueError:
        print("错误：月份格式不正确，应为 YYYY-MM，如 2026-01")
        sys.exit(1)

    # 创建并运行处理器
    processor = InvoiceProcessor(
        invoice_pool=args.invoice_pool,
        output_dir=args.output_dir,
        month=args.month
    )

    try:
        result = processor.process()
        if result is None:
            sys.exit(1)
    except Exception as e:
        print(f"\n处理失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
