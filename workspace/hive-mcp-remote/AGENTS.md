# Hive MCP Remote

当前项目通过 `beeline` 连接 HiveServer2，并通过 MCP 暴露查询工具。

当前推荐连接方式是 SSH 隧道：

`本机 127.0.0.1:10010 -> 远端 122.51.14.171:10000`

## 项目结构

- `hive_exec_server.py`: MCP server 入口
- `hive_client.py`: Hive 连接与 SQL 执行封装（beeline）
- `env.json`: 运行配置（host/port/auth/beeline 路径等）
- `test_remote_connection.py`: 远程连通性测试脚本（TCP + SQL）
- `start.sh`: 启动脚本

## 前置条件

- 本机可用 `python3`
- 已安装 `mcp` Python 包
- `env.json` 中 `beeline_path` 指向可执行 beeline
- 可通过 SSH 登录远程跳板机/服务器

## 1. 建立 SSH 隧道

```bash
ssh -N -L 10010:127.0.0.1:10000 ubuntu@122.51.14.171
```

说明：
- 本命令会阻塞当前终端，建议在单独终端窗口长期保持。
- 如需后台运行可自行加 `-f`（注意密钥与 known_hosts 配置）。

## 2. 配置 `env.json`

当前远程隧道配置示例：

```json
{
  "name": "Hive MCP Server - Remote Tunnel",
  "mode": "remote_hs2_ssh_tunnel",
  "host": "127.0.0.1",
  "port": 10010,
  "username": "xiongyuc",
  "auth": "NOSASL",
  "beeline_path": "/Users/xiongyuc/workspace/hive_native/apache-hive-4.0.0-bin/bin/beeline"
}
```

关键字段说明：
- `host`/`port`: 这里应填隧道本地监听地址
- `username`: beeline 登录用户名
- `auth`: 认证方式，当前为 `NOSASL`
- `hive_conf`: 透传到执行环境的 Hive 参数

## 3. 连通性测试

默认测试（`schema=default`, `sql=SELECT 1`）：

```bash
python3 test_remote_connection.py
```

指定库和 SQL：

```bash
python3 test_remote_connection.py demo_hive_db "SHOW TABLES"
```

返回码语义：
- `0`: TCP 与 Hive SQL 都成功
- `1`: TCP 不通（通常是隧道没起来）
- `2`: TCP 通但 Hive SQL 失败（权限/认证/schema/sql 问题）

## 4. 启动 MCP Server

直接启动：

```bash
python3 hive_exec_server.py
```

或用脚本启动：

```bash
./start.sh
```

## 5. MCP 配置示例

可在客户端配置如下（路径按实际机器调整）：

```json
{
  "mcpServers": {
    "hive-exec-server": {
      "command": "python3",
      "args": ["/Users/xiongyuc/workspace/hive-mcp-remote/hive_exec_server.py"]
    }
  }
}
```

## 可用工具

`hive_execute_query`
- 参数: `schema`, `sql`
- 作用: 执行查询并返回 TSV 文本

`hive_describe_table`
- 参数: `schema`, `table_name`
- 作用: 表结构描述（`DESCRIBE`）

`hive_count_records`
- 参数: `schema`, `table_name`, `partition_filter`(可选)
- 作用: 统计行数

`hive_preview_data`
- 参数: `schema`, `table_name`, `partition_filter`(可选), `limit`(默认 10)
- 作用: 预览数据

`hive_execute_dml`
- 参数: `schema`, `sql`
- 作用: 执行 DDL/DML

`hive_show_tables`
- 参数: `schema`
- 作用: 列出库下表

`hive_close_connections`
- 参数: 无
- 作用: 关闭缓存连接（当前 beeline 实现下为 no-op）

## Python 直接调用示例

```python
from hive_client import JdbcHiveUtils

result = JdbcHiveUtils.execute_query(
    schema="default",
    sql="SELECT 1"
)
print(result)
```

## 故障排查

隧道未建立：
- 现象: `TCP connect failed`
- 检查: 是否正在运行 `ssh -N -L 10010:127.0.0.1:10000 ...`
- 检查: `lsof -iTCP:10010 -sTCP:LISTEN -n -P`

beeline 路径错误：
- 现象: `Beeline not found`
- 处理: 修正 `env.json` 的 `beeline_path`

Hive 认证或权限问题：
- 现象: TCP 通，但 SQL 执行失败
- 检查: `username`/`auth` 是否匹配远端 HiveServer2 配置
- 检查: schema 是否存在且账号有权限

Schema 参数非法：
- 现象: `Invalid schema for beeline execution`
- 原因: schema 仅允许字母/数字/下划线，且不能以数字开头

## References

- MCP: https://modelcontextprotocol.io/
- FastMCP: https://github.com/jlowin/fastmcp
