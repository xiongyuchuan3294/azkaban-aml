# Requirement Impact Analysis 接口说明

## 1. 目标

将两个上游产物合并：

- `requirement-parser` 输出（需求结构化）
- `diff-parser` 输出（代码变更结构化）

并在同一阶段产出：

1. 需求点与变更文件映射  
2. 基于 repo 的节点/表血缘扩展  
3. 最小闭包 DAG 与测试节点范围

## 2. CLI

```bash
python skills/requirement-impact-analysis/scripts/analyze_requirement_impact.py [options]
```

### 2.1 参数

- `--requirement-report <path>`  
  requirement-parser 输出路径，支持 `.json` 或 `.md`。  
  默认：`skills/requirement-parser/outputs/requirement_report.md`

- `--diff-report <path>`  
  diff-parser 输出路径（`commit_report.json`）。  
  默认：`skills/diff-parser/outputs/commit_report.json`

- `--repo <path>`  
  代码仓库路径，用于提取 `jobs/*.job` 依赖和 `sql/*.sql` 表血缘。  
  默认：`/Users/xiongyuc/workspace/azkaban-aml`

- `--min-map-confidence <float>`  
  需求点映射最小置信度阈值。默认 `0.22`

- `--fallback-map-confidence <float>`  
  无高置信映射时的回退阈值。默认 `0.12`

- `--max-file-matches-per-item <int>`  
  每个需求点最多保留的映射文件数。默认 `3`

- `--output <path>`  
  输出文件路径。  
  默认：`skills/requirement-impact-analysis/outputs/impact_analysis_report.json`

- `--output-format <auto|json|md>`  
  输出格式，默认 `auto`（按后缀推断）。

## 3. 输入结构

### 3.1 requirement-report（json）

最小可用字段：

```json
{
  "functional_points": [{"id": "FP001", "type": "rule_change", "text": "..." }],
  "workflow_nodes": [{"node": "generate_aml_alert"}],
  "tables": [{"mapped_table": "aml_demo.dws_acct_risk_stat_di"}]
}
```

### 3.2 requirement-report（md）

支持解析 requirement-parser 生成的 Markdown，重点读取：

- `## 功能点`
- `## 节点与表关系`

### 3.3 diff-report（json）

需要包含：

```json
{
  "commits": [
    {
      "commit_id": "...",
      "changed_files": [
        {"path": "sql/03_dwd_to_dws.sql", "summary": "sql file modified; ..."}
      ]
    }
  ]
}
```

## 4. 输出结构

顶层字段：

- `status`: `ok | warning | blocked`
- `input_artifacts`: 输入文件信息
- `requirement_summary`: 需求点概况
- `diff_summary`: 变更文件概况
- `requirement_diff_mapping`: 映射结果
- `lineage_analysis`: 血缘、闭包 DAG、节点/表测试范围
- `warnings`: 风险提示

关键字段说明：

- `requirement_diff_mapping.mapped_items[]`
  - `requirement_item_id`
  - `requirement_text`
  - `matched_files[]`（`path`、`confidence`、`rationale`、`node_candidates`）

- `lineage_analysis.minimal_closure_dag`
  - `nodes`: 最小闭包必跑节点
  - `edges`: 节点依赖边

- `lineage_analysis.node_test_scope[]`
  - `node`
  - `scope`（`must_run`/`validation`）
  - `dependency_tables`
  - `result_tables`

## 5. 示例

### 5.1 输入命令

```bash
python skills/requirement-impact-analysis/scripts/analyze_requirement_impact.py \
  --requirement-report skills/requirement-parser/outputs/requirement_report.md \
  --diff-report skills/diff-parser/outputs/commit_report.json \
  --repo /Users/xiongyuc/workspace/azkaban-aml \
  --output skills/requirement-impact-analysis/outputs/impact_analysis_report.md \
  --output-format md
```

### 5.2 输出片段（json）

```json
{
  "status": "ok",
  "requirement_diff_mapping": {
    "mapped_ratio": 0.83
  },
  "lineage_analysis": {
    "must_run_nodes": ["init_ods_data", "ods_to_dwd", "dwd_to_dws", "generate_aml_alert"],
    "validation_nodes": ["check_alert_result"]
  }
}
```
