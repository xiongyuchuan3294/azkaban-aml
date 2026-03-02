# 需求解析报告：需求样例 01：新增平均交易额风险规则

## 概览
- 来源文档：`/Users/xiongyuc/workspace/bigdata-test/skills/requirement-parser/references/case_01_requirement.md`
- 生成时间：`2026-02-22T04:34:21.405969+00:00`
- 摘要：提取到 6 个功能点，识别 1 张候选表（高置信 1 张），生成 7 个逻辑步骤，推断 3 个工作流节点，自动沉淀 0 条新表映射。
- 输入格式：`markdown`
- 匹配模式：`hybrid`

## 功能点与主逻辑
- `FP001` [metric_change] [功能点] 在 DWS 账户风险聚合表中新增指标 `avg_amt`（日均交易额）。
- `FP002` [rule_change] [功能点] 在 ADS 预警生成逻辑中新增规则 `R004`：
- `FP003` [rule_change] [功能点] 规则含义：账户当日交易笔数较多且平均交易额较高，判定为高风险。
- `FP004` [rule_change] [功能点] 命中条件：`txn_cnt >= 3` 且 `avg_amt >= 60000`。
- `FP005` [rule_change] [功能点] 风险等级：`HIGH`。
- `FP006` [metric_change] [功能点] 证据字段：输出 `txn_cnt` 与 `avg_amt`。
- `LG007` [condition] [主逻辑补充] txn_cnt >= 3 且 avg_amt >= 60000

## 节点与表信息（待血缘确认）
- 说明：当前阶段仅基于需求文本推断候选节点与候选表，节点和表的精确关联需在血缘分析阶段确认。

### 候选节点
- `dwd_to_dws`（置信度=0.99；依据=需求涉及层级 DWS；需求关键词命中该节点规则）
- `generate_aml_alert`（置信度=0.96；依据=需求关键词命中该节点规则）
- `check_alert_result`（置信度=0.93；依据=需求涉及层级 DWS；需求关键词命中该节点规则）

### 候选表
- 分层统计：DWS=1
| 表名 | 层级 | 匹配方式 | 置信度 | 原始提及 |
| --- | --- | --- | --- | --- |
| aml_demo.dws_acct_risk_stat_di | DWS | alias_exact_manual | 0.96 | DWS 账户风险聚合表 |

## 质量评估
- 完整度：`1.0`
- 缺失项：无

## 澄清问题
- 无

## 映射沉淀
- 自动更新：`True`
- 新增映射：`0`
- 映射文件：`/Users/xiongyuc/workspace/bigdata-test/skills/requirement-parser/references/table_alias_map.auto.json`

## 转换蓝图
- 可复用策略：
  - 1. 更新 profile
  - 2. 补充表别名映射
  - 3. 必要时生成新的 adapter 脚本
- 输入契约：
  - 必填：`title, raw_text`
- 输出契约：
  - 必填：`source_document, summary, functional_points, tables, main_logic, workflow_nodes, quality, clarification_questions`
