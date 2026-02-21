# Azkaban Flow 1.0 Anti-Money-Laundering ETL Demo (Hive)

这个 demo 使用 Azkaban Flow 1.0（`jobs/` 子目录 `.job` 方案）实现一个最小可运行的反洗钱 ETL 流程，核心处理逻辑为 Hive SQL。

## 目录结构

- `jobs/basic.job`
- `jobs/init_ods_data.job`
- `jobs/ods_to_dwd.job`
- `jobs/dwd_to_dws.job`
- `jobs/generate_aml_alert.job`
- `jobs/check_alert_result.job`
- `common.properties`
- `sql/00_init_ddl.sql`
- `sql/01_init_ods.sql`
- `sql/02_ods_to_dwd.sql`
- `sql/03_dwd_to_dws.sql`
- `sql/04_generate_aml_alert.sql`
- `sql/05_check_alert_result.sql`

## 流程说明

1. `init_ods_data`：写入交易样本数据（ODS）。
2. `ods_to_dwd`：清洗并生成衍生标记（夜间交易、大额交易）进入 DWD。
3. `dwd_to_dws`：按账户聚合风险指标进入 DWS。
4. `generate_aml_alert`：执行反洗钱规则写入预警结果表。
5. `check_alert_result`：查询预警结果，便于在 Azkaban 日志中查看。

`jobs/basic.job` 是入口节点，依赖 `check_alert_result`，触发后会按依赖链路执行完整流程。

`sql/01~05` 为调度日常执行脚本（不含 DDL），`sql/00_init_ddl.sql` 为一次性初始化脚本（建库建表）。

## 参数配置

公共参数在 `common.properties`：

```properties
run_date_std=2026-02-18
hive_jdbc_url=jdbc:hive2://127.0.0.1:10000/default
hive_user=hive
warehouse_external_dir=/Users/xiongyuc/workspace/hive_native/hadoop_data/warehouse/external
warehouse_managed_dir=/Users/xiongyuc/workspace/hive_native/hadoop_data/warehouse/managed
```

执行时可在 Azkaban UI 覆盖 `run_date_std` 等参数。
其中 `warehouse_external_dir`、`warehouse_managed_dir` 仅在执行 `sql/00_init_ddl.sql` 时使用。

## 首次初始化（仅执行一次）

在首次跑 flow 之前先执行 DDL：

```bash
beeline -u ${hive_jdbc_url} -n ${hive_user} \
  --hivevar warehouse_external_dir=${warehouse_external_dir} \
  --hivevar warehouse_managed_dir=${warehouse_managed_dir} \
  -f sql/00_init_ddl.sql
```

## 打包与上传

```bash
cd /Users/xiongyuc/workspace/azkaban-flow2-demo
zip -r demo_flow1.zip jobs common.properties sql
```

Azkaban UI 操作：

1. Create Project
2. Upload `demo_flow1.zip`
3. Execute flow `basic`（若 UI 显示为带目录前缀名称，以 UI 展示为准）

## 运行前提

- Azkaban 执行机可调用 `beeline`。
- HiveServer2 已启动，且 `hive_jdbc_url` 可连通。
- 有权限创建 Hive 库表（默认库名：`aml_demo`）。
- `warehouse_external_dir` 和 `warehouse_managed_dir` 为可写目录，且两者不能相同。
