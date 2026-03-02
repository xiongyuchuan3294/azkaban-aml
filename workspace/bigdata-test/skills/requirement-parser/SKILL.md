---
name: requirement-parser
description: 将 skills/requirement-parser/references（或用户提供的 markdown/text/json 文件）中的 AML 需求文档解析为结构化结果，提取功能点、输入输出字段、中文表名到物理表映射、核心业务逻辑和推断工作流节点。适用于需求文本不完整或不标准、需要输出澄清问题与引导建议的场景。
---

# Requirement Parser（需求解析器）

使用 `scripts/parse_requirement_doc.py` 运行通用、可配置（profile 驱动）的需求解析流程，输出用于测试设计的结构化分析结果。

## 执行流程

1. 确认输入需求文件路径（`--input`）。
2. 运行解析脚本。默认输出路径为 `skills/requirement-parser/outputs/requirement_report.md`。
```bash
python skills/requirement-parser/scripts/parse_requirement_doc.py \
  --input skills/requirement-parser/references/case_01_requirement.md \
  --repo /Users/xiongyuc/workspace/azkaban-aml \
  --profile skills/requirement-parser/references/parser_profile.json
```
3. 按以下重点阅读输出：
- `functional_points`：从需求文本拆解出的功能点。
- `tables`：中文简写表名到物理表名的映射结果。
- `main_logic`：关键规则与处理逻辑步骤。
- `workflow_nodes`：基于逻辑/表信息推断的 Azkaban 节点，可结合 diff 报告进一步修正。
- `quality` 与 `clarification_questions`：当需求不完整时的质量诊断与引导问题。
- `conversion_blueprint`：结构化输入输出契约，以及“复用现有脚本/生成新脚本”的建议。

## 映射策略

- 优先使用文档中直接出现的物理表名（如 `aml_demo.dws_acct_risk_stat_di`）。
- 从 `references/table_alias_map.json` 与 `references/table_alias_map.auto.json` 加载别名映射。
- 支持可配置匹配模式：`hybrid` / `algorithm` / `vector`。
- 将高置信的新映射自动沉淀到 `table_alias_map.auto.json`。
- 为每条映射输出 `match_type` 与 `confidence`。

## 输出契约

- 默认输出 Markdown 报告（`.md`）；可通过 `--output-format json` 输出 JSON。
- 输出至少包含 `functional_points`、`inputs`、`outputs`、`tables`、`main_logic`、`workflow_nodes`。
- 当抽取置信度较低时必须包含质量诊断信息。
- 无法完整解析时也要给出引导建议。

## 校验清单

- 表映射需包含置信度与证据文本。
- 条件与阈值需在 `main_logic` 中体现。
- 工作流候选节点需包含推断依据。
- `conversion_blueprint` 需包含结构化输入输出契约。
- 缺失信息需转换为可执行的澄清问题。

## 按需加载参考

- `references/api_reference.md`: Markdown方案、输入输出示例、CLI参数和输出结构。
- `references/parser_profile.json`: 通用抽取配置，优先通过调参而非改代码适配新需求。
- `references/table_alias_map.json`: 人工维护的表别名字典。
- `references/table_alias_map.auto.json`: 历史运行自动沉淀的表映射字典。
