---
name: requirement-impact-analysis
description: 将 requirement-parser 与 diff-parser 的输出合并分析，完成需求点到变更文件映射、血缘扩展、最小闭包 DAG 计算，以及测试节点/表范围产出。用于三段式流程的第 3 步（替代原 data-lineage-analysis 独立步骤）。
---

# Requirement Impact Analysis（需求影响分析）

该 skill 是三段式流程的最终阶段：

1. `requirement-parser`：需求文档结构化抽取  
2. `diff-parser`：git diff/commit 变更清单解析  
3. `requirement-impact-analysis`：需求+变更合并，输出血缘关系和测试范围

## 执行命令

```bash
python skills/requirement-impact-analysis/scripts/analyze_requirement_impact.py \
  --requirement-report skills/requirement-parser/outputs/requirement_report.md \
  --diff-report skills/diff-parser/outputs/commit_report.json \
  --repo /Users/xiongyuc/workspace/azkaban-aml
```

默认输出：`skills/requirement-impact-analysis/outputs/impact_analysis_report.json`

## 输入约定

- `--requirement-report`：支持 `json` 或 `md`（推荐优先使用 requirement-parser 输出）。
- `--diff-report`：使用 diff-parser 输出的 `commit_report.json`。
- `--repo`：用于提取 `jobs/*.job` 依赖和 `sql/*.sql` 表血缘。

## 输出内容

输出单一报告，包含：

- `requirement_diff_mapping`：需求点 ↔ 变更文件映射（含置信度、依据、未映射列表）。
- `lineage_analysis.minimal_closure_dag`：最小闭包 DAG（必跑节点和边）。
- `lineage_analysis.expanded_node_graph`：扩展后的上下游节点图。
- `lineage_analysis.node_test_scope`：节点与依赖表/结果表整理后的测试范围。
- `lineage_analysis.suggested_checks`：测试建议与执行顺序提示。

## 校验清单

- 至少应识别 `seed_nodes`（需求节点、映射节点或 diff 直接节点之一）。
- `must_run_nodes` 必须是可执行顺序（按拓扑排序）。
- 每条映射必须包含 `requirement_item_id`、`path`、`confidence`。
- 若覆盖率较低，应在 `warnings` 中给出明确提示。

## 参数调优

- `--min-map-confidence`：提高可减少误匹配；降低可提升召回。
- `--fallback-map-confidence`：无高分匹配时的回退阈值。
- `--max-file-matches-per-item`：控制单需求点映射文件数量。
- `--output-format md`：可直接输出可读报告。

## 参考文件

- `references/api_reference.md`：完整 CLI、输入输出结构与示例。
