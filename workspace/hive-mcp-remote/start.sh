#!/bin/bash
# 启动 Hive MCP Server - Mac Local Environment

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "Starting Hive MCP Server - Mac Local Environment"
echo "=================================================="

# 检查 Python 环境
if ! command -v python3 &> /dev/null; then
    echo "Error: python3 not found"
    exit 1
fi

# 检查 mcp 包是否安装
if ! python3 -c "import mcp" 2>/dev/null; then
    echo "Installing mcp package..."
    pip install mcp
fi

# 启动 MCP 服务器
python3 hive_exec_server.py
