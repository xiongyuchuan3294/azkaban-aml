#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Hive execution MCP server"""

import logging
import sys
from pathlib import Path

project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from mcp.server.fastmcp import FastMCP

from hive_client import JdbcHiveUtils, ENV_NAME

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

mcp = FastMCP(f"hive_exec_server_{ENV_NAME}")


def _query_error(exc: Exception) -> str:
    logger.error("Hive request failed: %s", exc)
    return f"查询失败: {exc}"


@mcp.tool(
    name="hive_execute_query",
    description=(
        "Execute Hive SQL query and return tab-separated results. "
        f"Environment: {ENV_NAME}"
    ),
)
def hive_execute_query(
    schema: str,
    sql: str,
) -> str:
    """Execute a Hive query."""
    try:
        logger.info("Execute Hive query. schema=%s sql=%s", schema, sql[:100])
        return JdbcHiveUtils.execute_query(schema, sql)
    except Exception as exc:
        return _query_error(exc)


@mcp.tool(
    name="hive_describe_table",
    description=f"Describe a Hive table. Environment: {ENV_NAME}",
)
def hive_describe_table(
    schema: str,
    table_name: str,
) -> str:
    """Describe a table structure."""
    sql = f"DESCRIBE {table_name}"
    try:
        logger.info("Describe Hive table. schema=%s table=%s", schema, table_name)
        return JdbcHiveUtils.execute_query(schema, sql)
    except Exception as exc:
        return _query_error(exc)


@mcp.tool(
    name="hive_count_records",
    description=f"Count rows in a Hive table. Environment: {ENV_NAME}",
)
def hive_count_records(
    schema: str,
    table_name: str,
    partition_filter: str | None = None,
) -> str:
    """Count records in a table."""
    sql = f"SELECT COUNT(*) AS record_count FROM {table_name}"
    if partition_filter:
        sql += f" WHERE {partition_filter}"
    try:
        logger.info("Count Hive records. schema=%s table=%s partition=%s", schema, table_name, partition_filter)
        return JdbcHiveUtils.execute_query(schema, sql)
    except Exception as exc:
        return _query_error(exc)


@mcp.tool(
    name="hive_preview_data",
    description=f"Preview Hive table data. Environment: {ENV_NAME}",
)
def hive_preview_data(
    schema: str,
    table_name: str,
    partition_filter: str | None = None,
    limit: int = 10,
) -> str:
    """Preview table data."""
    sql = f"SELECT * FROM {table_name}"
    if partition_filter:
        sql += f" WHERE {partition_filter}"
    sql += f" LIMIT {limit}"
    try:
        logger.info("Preview Hive data. schema=%s table=%s limit=%s", schema, table_name, limit)
        return JdbcHiveUtils.execute_query(schema, sql)
    except Exception as exc:
        return _query_error(exc)


@mcp.tool(
    name="hive_execute_dml",
    description=f"Execute Hive DDL/DML. Environment: {ENV_NAME}",
)
def hive_execute_dml(
    schema: str,
    sql: str,
) -> str:
    """Execute DDL or DML."""
    try:
        logger.info("Execute Hive DML. schema=%s sql=%s", schema, sql[:100])
        JdbcHiveUtils.execute(schema, sql)
        return "执行成功"
    except Exception as exc:
        logger.error("Hive DML failed: %s", exc)
        return f"执行失败: {exc}"


@mcp.tool(
    name="hive_show_tables",
    description=f"Show tables in a Hive schema. Environment: {ENV_NAME}",
)
def hive_show_tables(
    schema: str,
) -> str:
    """Show all tables in the given schema."""
    try:
        logger.info("Show Hive tables. schema=%s", schema)
        return JdbcHiveUtils.execute_query(schema, "SHOW TABLES")
    except Exception as exc:
        return _query_error(exc)


@mcp.tool(
    name="hive_close_connections",
    description="Close cached Hive connections.",
)
def hive_close_connections() -> str:
    """Close cached connections."""
    try:
        logger.info("Close Hive connections")
        JdbcHiveUtils.close_all()
        return "所有连接已关闭"
    except Exception as exc:
        logger.error("Close Hive connections failed: %s", exc)
        return f"关闭连接失败: {exc}"


if __name__ == "__main__":
    logger.info(f"Start Hive execution MCP server. Environment: {ENV_NAME}")
    mcp.run()
