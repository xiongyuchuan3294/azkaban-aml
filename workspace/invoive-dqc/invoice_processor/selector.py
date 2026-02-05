"""
智能选择器 - 按优先级选择发票

选择优先级：
1. 必须通过校验（日期、抬头、黑名单等）- 在selector之前完成
2. 尽量凑够补贴上限（300、500、680）
3. 优先选择月份较早、金额较小的发票，留更多发票给下个月
"""
from datetime import datetime
from models import InvoiceInfo, InvoiceType, InvoiceStatus, SelectionResult
from config import SUBSIDY_LIMITS


class InvoiceSelector:
    """发票选择器"""

    def select(self, invoices: list[InvoiceInfo]) -> SelectionResult:
        """
        执行选择算法

        Args:
            invoices: 所有发票列表

        Returns:
            SelectionResult: 选择结果
        """
        result = SelectionResult(
            month="",  # 由外部设置
        )

        # 按发票类型分别处理
        for invoice_type in [InvoiceType.DINING, InvoiceType.TRANSPORT, InvoiceType.COMMUNICATION]:
            limit = SUBSIDY_LIMITS[invoice_type.value]
            selected, unused = self._select_by_type(invoices, invoice_type, limit)
            result.selected_invoices.extend(selected)
            result.unused_invoices.extend(unused)

        # 错误发票
        for inv in invoices:
            if inv.final_status == InvoiceStatus.ERROR:
                result.error_invoices.append(inv)

        return result

    def _select_by_type(
        self,
        invoices: list[InvoiceInfo],
        invoice_type: InvoiceType,
        limit: float
    ) -> tuple[list[InvoiceInfo], list[InvoiceInfo]]:
        """
        按类型选择发票（全局组合优化）

        选择策略（按优先级）：
        1. 凑够限额：优先选择总金额最接近limit的组合
        2. 留足后路：在金额相近时，优先选择更少/更小/更早的发票

        Args:
            invoices: 所有发票列表
            invoice_type: 发票类型
            limit: 金额上限

        Returns:
            (选中发票列表, 多余发票列表)
        """
        # 1. 筛选该类型且校验通过的发票（排除重复发票）
        valid = [
            inv for inv in invoices
            if inv.invoice_type == invoice_type
            and inv.is_valid
            and inv.within_3_months
            and not inv.is_duplicate
        ]

        # 2. 收集重复发票，标记为错误（移入errors目录）
        duplicates = [
            inv for inv in invoices
            if inv.invoice_type == invoice_type
            and inv.is_duplicate
        ]
        for dup in duplicates:
            dup.final_status = InvoiceStatus.ERROR
            dup.error_code = "DUPLICATE"

        if not valid:
            return [], duplicates

        # 2. 按日期排序（日期靠前的优先处理）
        valid.sort(key=lambda x: (x.invoice_date or datetime.min, x.amount))

        # 3. 找到全局最优组合
        best_combo = self._find_best_combo(valid, limit)

        # 4. 设置状态
        selected = list(best_combo)
        selected_ids = {id(inv) for inv in selected}
        unused = []

        for inv in valid:
            if id(inv) in selected_ids:
                inv.final_status = InvoiceStatus.SELECTED
            else:
                unused.append(inv)
                inv.final_status = InvoiceStatus.UNUSED

        # 注意：重复发票状态为ERROR，会在select方法中自动收集到error_invoices

        return selected, unused

    def _find_best_combo(
        self,
        invoices: list[InvoiceInfo],
        limit: float
    ) -> list[InvoiceInfo]:
        """
        从发票列表中找到最优组合

        评分策略（按优先级）：
        1. 凑够限额：总金额越接近limit越好（低于或略高于都行）
        2. 留足后路：金额相近时，选择更少/更小/更早的发票

        Args:
            invoices: 已按(日期, 金额)排序的发票列表
            limit: 金额上限

        Returns:
            选中的发票列表
        """
        from itertools import combinations

        best_combo = []
        best_score = float('-inf')
        max_acceptable = limit * 1.3  # 允许30%超额，优先凑够

        # 尝试所有可能的组合
        for size in range(len(invoices), 0, -1):
            for combo in combinations(invoices, size):
                total = sum(inv.amount for inv in combo)

                # 超过最大可接受值，跳过
                if total > max_acceptable:
                    continue

                # 计算综合得分
                score = self._calculate_score(total, limit, len(combo))

                if score > best_score:
                    best_score = score
                    best_combo = list(combo)

                # 完美匹配，直接返回
                if abs(total - limit) < 0.01:
                    return list(combo)

        return best_combo

    def _calculate_score(self, total: float, limit: float, count: int) -> float:
        """
        计算组合得分

        评分规则：
        1. 主要得分：越接近limit越好
           - 达到或超过limit：得满分100
           - 未达到limit：按比例得分 (total/limit * 100)
        2. 次要得分：在总金额相近时，优先选择
           - 更少的发票（留更多给下个月）
           - 更小的总金额（留大额发票给下个月）

        Args:
            total: 组合总金额
            limit: 限额
            count: 发票数量

        Returns:
            得分（越高越好）
        """
        # 主要得分：接近限额的程度
        if total >= limit:
            main_score = 100  # 已达到或超过限额，满分
        else:
            main_score = (total / limit) * 100  # 未达到，按比例

        # 次要得分：鼓励"少而精"（留更多发票给下个月）
        # 1. 发票数量越少越好
        count_penalty = count * 0.5

        # 2. 超过限额时，总金额越小越好（留大额发票给下个月）
        overage_penalty = max(0, total - limit) * 0.01

        return main_score - count_penalty - overage_penalty
