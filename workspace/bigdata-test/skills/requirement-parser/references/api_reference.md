# Requirement Parser 方案与接口说明

## Markdown 方案

### 1. 目标

将需求文档解析成可用于反洗钱大数据测试设计的结构化分析结果，即使文档不标准，也尽量输出可落地信息与引导问题。同时输出可复用的“转换蓝图”，让 AI 在新需求场景下优先复用现有脚本或快速生成新 adapter。

### 2. 处理流程

1. 读取需求文档并拆分段落/要点。
2. 提取功能点（新增/修改规则、指标、字段、产出）。
3. 提取输入输出要素（条件字段、阈值、输出字段、风险等级）。
4. 识别表名并做映射：
- 直接识别物理表名（如 `db.table`）。
- 用 `table_alias_map.json` 做简写中文表名到物理表名映射。
- 对非标准表述做 `algorithm/vector/hybrid` 相似度匹配，输出置信度和证据行。
- 对高置信映射自动沉淀到 `table_alias_map.auto.json`。
5. 拆解主要逻辑：
- 指标构建/规则定义/条件判断/风险分级/证据输出。
6. 推断工作流节点：
- 根据 ODS/DWD/DWS/ADS 层级与关键词推断。
- 可结合代码变更报告（diff-parser 输出）提升节点确认度。
7. 输出质量评估与引导问题：
- 缺失部分、低置信映射、建议补充信息。
8. 输出转换蓝图：
- 结构化输入契约。
- 结构化输出契约。
- 复用脚本与生成新脚本的决策指引。

### 3. 非标准需求兜底策略

- 无明确表名：先输出“候选层级表”并提示补充真实表。
- 无明确输入输出：从条件、证据字段、规则语义反推字段。
- 无完整流程：给出最小可执行流程建议（聚合 -> 规则 -> 预警 -> 校验）。
- 输出 `clarification_questions`，用于追问需求方。

## CLI 参数

```bash
python skills/requirement-parser/scripts/parse_requirement_doc.py [options]
```

- `--input <path>`: 需求文档路径（必填）。
- `--input-format <auto|markdown|text|json>`: 输入格式，默认 `auto`。
- `--profile <path>`: 解析配置文件，默认 `references/parser_profile.json`。
- `--repo <path>`: 项目路径，用于确认工作流节点。默认 `/Users/xiongyuc/workspace/azkaban-aml`。
- `--table-map <path>`: 表别名映射文件路径。默认 `skills/requirement-parser/references/table_alias_map.json`。
- `--auto-map-file <path>`: 自动沉淀映射文件路径，默认 `skills/requirement-parser/references/table_alias_map.auto.json`。
- `--match-mode <hybrid|algorithm|vector>`: 表名匹配策略，默认 `hybrid`。
- `--min-match-confidence <float>`: 算法映射接受阈值，默认 `0.58`。
- `--auto-map-min-confidence <float>`: 自动沉淀阈值，默认 `0.8`。
- `--disable-auto-map-update`: 关闭自动映射沉淀。
- `--diff-report <path>`: 可选，diff-parser 输出文件，用于优化工作流节点判断。
- `--output <path>`: 输出文件路径。默认 `skills/requirement-parser/outputs/requirement_report.md`。使用 `--output -` 可打印到终端。
- `--output-format <auto|md|json>`: 输出格式。默认 `auto`（按文件后缀推断，默认 md）。

## 输入示例

```bash
python skills/requirement-parser/scripts/parse_requirement_doc.py \
  --input skills/requirement-parser/references/case_01_requirement.md \
  --repo /Users/xiongyuc/workspace/azkaban-aml \
  --profile skills/requirement-parser/references/parser_profile.json \
  --match-mode hybrid
```

## 输出示例

```json
{
  "source_document": "skills/requirement-parser/references/case_01_requirement.md",
  "summary": "提取到 4 个功能点，识别 2 张候选表，生成 4 个逻辑步骤，推断 2 个工作流节点。",
  "functional_points": [
    {"id": "FP001", "type": "metric_change", "text": "在 DWS 账户风险聚合表中新增指标 avg_amt（日均交易额）"}
  ],
  "inputs": [
    {"name": "txn_cnt", "evidence": "命中条件：txn_cnt >= 3 且 avg_amt >= 60000"},
    {"name": "avg_amt", "evidence": "命中条件：txn_cnt >= 3 且 avg_amt >= 60000"}
  ],
  "outputs": [
    {"name": "risk_level", "value": "HIGH", "evidence": "风险等级：HIGH"},
    {"name": "txn_cnt", "evidence": "证据字段：输出 txn_cnt 与 avg_amt"}
  ],
  "tables": [
    {
      "mention": "DWS 账户风险聚合表",
      "mapped_table": "aml_demo.dws_acct_risk_stat_di",
      "match_type": "alias_exact",
      "confidence": 0.95
    }
  ],
  "main_logic": [
    {"step": 1, "type": "metric_change", "detail": "新增聚合指标 avg_amt"},
    {"step": 2, "type": "condition", "detail": "txn_cnt >= 3 且 avg_amt >= 60000"}
  ],
  "workflow_nodes": [
    {"node": "dwd_to_dws", "confidence": 0.9, "reason": "涉及 DWS 聚合层指标变更"},
    {"node": "generate_aml_alert", "confidence": 0.88, "reason": "涉及 ADS 预警规则生成"}
  ],
  "mapping_update": {
    "enabled": true,
    "auto_map_file": "skills/requirement-parser/references/table_alias_map.auto.json",
    "new_aliases": 1,
    "updated": true
  },
  "conversion_blueprint": {
    "structured_input_contract": {"required": ["title", "raw_text"]},
    "structured_output_contract": {"required": ["functional_points", "tables", "main_logic", "workflow_nodes"]},
    "script_reuse_policy": {"preferred_action_order": ["1. 更新 profile", "2. 补充映射", "3. 再生成 adapter"]}
  },
  "quality": {
    "completeness_score": 0.9,
    "missing_parts": []
  },
  "clarification_questions": []
}
```
