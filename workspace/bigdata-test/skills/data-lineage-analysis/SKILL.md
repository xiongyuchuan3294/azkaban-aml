---
name: data-lineage-analysis
description: Deprecated compatibility skill. Data lineage and minimal closure DAG analysis have been merged into requirement-impact-analysis in the 3-skill analysis flow.
---

# Data Lineage Analysis（Deprecated）

该 skill 已并入 `requirement-impact-analysis`，不再作为独立步骤使用。

## 当前建议流程

1. `requirement-parser`
2. `diff-parser`
3. `requirement-impact-analysis`（包含需求映射 + 血缘扩展 + 最小闭包 DAG + 测试节点范围）

## 兼容说明

- 若历史流程仍引用 `data-lineage-analysis`，请迁移为调用：
  `skills/requirement-impact-analysis/scripts/analyze_requirement_impact.py`
- 迁移后产物统一为：
  `skills/requirement-impact-analysis/outputs/impact_analysis_report.json`
