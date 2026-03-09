#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Hive connection and execution helpers - Mac Local Environment"""

import json
import os
import re
import shlex
import subprocess
from pathlib import Path
from typing import Any

# 加载环境配置
CONFIG_PATH = Path(__file__).parent / "env.json"


def load_config() -> dict:
    """加载环境配置"""
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Config file not found: {CONFIG_PATH}")
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


CONFIG = load_config()

# 从配置中提取环境变量
ENV_NAME = CONFIG.get("name", "Mac Local")
MODE = CONFIG.get("mode", "local_hs2_mac")
HOST = CONFIG.get("host", "127.0.0.1")
PORT = CONFIG.get("port", 10000)
USERNAME = CONFIG.get("username", "xiongyuc")
AUTH = CONFIG.get("auth", "NOSASL")
BEELINE_PATH = CONFIG.get("beeline_path", "/Users/xiongyuc/workspace/hive_native/apache-hive-4.0.0-bin/bin/beeline")
HIVE_CONF = CONFIG.get("hive_conf", {})


def _summarize_beeline_failure(stderr: str, stdout: str, max_lines: int = 4) -> str:
    """Extract a concise and meaningful beeline error message."""
    all_lines = [line.strip() for line in (stderr + "\n" + stdout).splitlines() if line.strip()]
    if not all_lines:
        return "Unknown beeline error (empty output)."

    # Drop noisy logs that often hide the actual Hive error line.
    filtered = [
        line
        for line in all_lines
        if not line.startswith("SLF4J:")
    ]
    if not filtered:
        filtered = all_lines

    error_lines = [
        line
        for line in filtered
        if re.search(r"\b(error|failed|exception)\b", line, re.IGNORECASE)
    ]
    if error_lines:
        return "\n".join(error_lines[-max_lines:])
    return "\n".join(filtered[-max_lines:])


def _strip_debug_jvm_opts(raw_opts: str | None) -> str:
    """Remove JDWP-related JVM flags from options strings."""
    if not raw_opts:
        return ""

    text = str(raw_opts).strip()
    if not text:
        return ""

    try:
        tokens = shlex.split(text)
    except ValueError:
        tokens = text.split()

    filtered: list[str] = []
    for token in tokens:
        lowered = token.lower()
        if "jdwp" in lowered:
            continue
        if lowered == "-xdebug":
            continue
        if lowered.startswith("-xrunjdwp"):
            continue
        filtered.append(token)
    return " ".join(filtered).strip()


def _build_beeline_env() -> dict[str, str]:
    """Build a subprocess env that avoids entering Hive debug mode."""
    env = dict(os.environ)

    for key in ("DEBUG", "HIVE_MAIN_CLIENT_DEBUG_OPTS", "HIVE_CHILD_CLIENT_DEBUG_OPTS", "HIVE_DEBUG_OPTS"):
        env.pop(key, None)

    option_keys = (
        "HADOOP_CLIENT_OPTS",
        "HADOOP_OPTS",
        "HIVE_OPTS",
        "JAVA_TOOL_OPTIONS",
        "_JAVA_OPTIONS",
        "JDK_JAVA_OPTIONS",
    )
    for key in option_keys:
        cleaned = _strip_debug_jvm_opts(env.get(key))
        if cleaned:
            env[key] = cleaned
        else:
            env.pop(key, None)

    return env


class HiveRuntimeConfig:
    """Hive runtime configuration for backward compatibility."""

    @classmethod
    def active_env(cls) -> str:
        """Get the active Hive environment name."""
        return os.environ.get("HIVE_ACTIVE_ENV", CONFIG.get("env", "local"))


class BeelineHiveExecutor:
    """Execute Hive queries via beeline."""

    SCHEMA_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

    @classmethod
    def _validate_schema(cls, schema: str) -> str:
        if not schema:
            raise ValueError("Schema is required for beeline execution.")
        if not cls.SCHEMA_RE.match(schema):
            raise ValueError(f"Invalid schema for beeline execution: {schema}")
        return schema

    @classmethod
    def _run(cls, schema: str, sql: str) -> str:
        cls._validate_schema(schema)

        beeline_path = os.environ.get("BEELINE_PATH", BEELINE_PATH)
        if not Path(beeline_path).exists():
            raise FileNotFoundError(f"Beeline not found: {beeline_path}")

        # 构建 JDBC URL
        jdbc_url = f"jdbc:hive2://{HOST}:{PORT}/{schema}"
        if AUTH:
            jdbc_url += f";auth={AUTH}"

        cmd = [
            beeline_path,
            "-u", jdbc_url,
            "-n", USERNAME,
            "--outputformat=tsv2",
            "-e", sql,
        ]

        # 设置环境变量（清理调试参数，避免 beeline/JVM 因 JDWP 端口冲突失败）
        env = _build_beeline_env()
        env["HADOOP_USER_NAME"] = USERNAME

        # 添加 Hive 配置
        for key, value in HIVE_CONF.items():
            env[key] = str(value)

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            env=env,
        )

        if result.returncode != 0:
            detail = _summarize_beeline_failure(result.stderr, result.stdout)
            combined_lower = f"{result.stderr}\n{result.stdout}".lower()
            if "could not open socket" in combined_lower or "connection refused" in combined_lower:
                raise ConnectionError(f"Hive connection failed: {detail}")
            raise RuntimeError(f"Beeline execution failed: {detail}")

        # Parse TSV output, skip logging lines
        lines = result.stdout.splitlines()
        output_lines = []
        for line in lines:
            # Skip SLF4J and other logging lines
            if line.startswith("SLF4J:") or line.startswith("Connecting to") or line.startswith("Connected to"):
                continue
            if line.startswith("Format tsv") or line.startswith("Driver:") or line.startswith("Transaction"):
                continue
            if line.startswith("INFO") or line.startswith("WARN") or line.startswith("ERROR"):
                continue
            if not line.strip():
                continue
            output_lines.append(line)

        return "\n".join(output_lines)

    @classmethod
    def execute_query(cls, schema: str, sql: str) -> str:
        return cls._run(schema, sql)

    @classmethod
    def execute(cls, schema: str, sql: str) -> None:
        cls._run(schema, sql)

    @classmethod
    def close_all(cls) -> None:
        return None


class JdbcHiveUtils:
    @staticmethod
    def execute_query(schema: str, sql: str, env: str | None = None) -> str:
        return BeelineHiveExecutor.execute_query(schema, sql)

    @staticmethod
    def execute(schema: str, sql: str, env: str | None = None) -> None:
        BeelineHiveExecutor.execute(schema, sql)

    @staticmethod
    def close_all() -> None:
        BeelineHiveExecutor.close_all()


if __name__ == "__main__":
    print(f"Environment: {ENV_NAME}")
    print(f"Mode: {MODE}")
    print(f"Host: {HOST}:{PORT}")
    print(f"Username: {USERNAME}")
    print(f"Beeline: {BEELINE_PATH}")
