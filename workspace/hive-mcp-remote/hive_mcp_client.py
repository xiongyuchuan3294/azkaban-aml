#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import atexit
import json
import os
import shlex
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from mcp import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client


ERROR_PREFIX_MAP = {
    "查询失败:": "Hive query failed",
    "执行失败:": "Hive execution failed",
    "关闭连接失败:": "Hive connection cleanup failed",
}

_RUNTIME_CACHE: dict[str, "McpHiveRuntime"] = {}
_RUNTIME_CACHE_LOCK = threading.RLock()
_CALL_LOG_LOCK = threading.RLock()


def _call_log_path() -> Path | None:
    raw_path = os.getenv("HIVE_MCP_CALL_LOG_PATH", "").strip()
    if not raw_path:
        return None
    return Path(raw_path).expanduser().resolve()


def _emit_call_log(event: str, **payload: Any) -> None:
    log_path = _call_log_path()
    if log_path is None:
        return

    record = {
        "ts": time.time(),
        "pid": os.getpid(),
        "event": event,
        **payload,
    }
    with _CALL_LOG_LOCK:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with log_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")


@dataclass(frozen=True)
class HiveMcpCall:
    tool_name: str
    arguments: dict[str, Any]


@dataclass(frozen=True)
class McpServerSpec:
    command: str
    args: tuple[str, ...]
    cwd: str | None = None
    env: dict[str, str] | None = None
    server_root: str | None = None


class SharedMcpSession:
    """Persistent MCP stdio session backed by one background event loop."""

    def __init__(self, server_spec: McpServerSpec):
        self.server_spec = server_spec
        self._lock = threading.RLock()
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._session: ClientSession | None = None
        self._stop_event: asyncio.Event | None = None
        self._ready_event = threading.Event()
        self._startup_error: Exception | None = None
        self._stderr_handle: Any | None = None
        self._session_starts = 0

    def stats(self) -> dict[str, Any]:
        with self._lock:
            return {
                "session_mode": "shared",
                "session_starts": self._session_starts,
                "session_active": self._session is not None,
            }

    def list_tools(self) -> list[str]:
        result = self._call_session("list_tools")
        return [tool.name for tool in result.tools]

    def call_tool(self, tool_name: str, arguments: dict[str, Any] | None = None) -> Any:
        return self._call_session("call_tool", tool_name, arguments or {})

    def close(self) -> None:
        with self._lock:
            self._close_locked()

    def _call_session(self, method_name: str, *args: Any) -> Any:
        last_error: Exception | None = None
        for attempt in range(2):
            with self._lock:
                self._ensure_session_locked()
                loop = self._loop
                session = self._session
            if loop is None or session is None:
                raise RuntimeError("MCP session is unavailable")

            try:
                method = getattr(session, method_name)
                future = asyncio.run_coroutine_threadsafe(method(*args), loop)
                return future.result()
            except Exception as exc:
                last_error = exc
                with self._lock:
                    self._close_locked()
                if attempt == 1:
                    raise
        if last_error is not None:
            raise last_error
        raise RuntimeError(f"MCP session call failed: {method_name}")

    def _ensure_session_locked(self) -> None:
        if self._thread is not None and self._thread.is_alive() and self._session is not None:
            return

        self._ready_event.clear()
        self._startup_error = None
        self._stderr_handle = sys.stderr
        if os.getenv("HIVE_MCP_LOG_STDERR", "").strip() != "1":
            self._stderr_handle = open(os.devnull, "w", encoding="utf-8")

        thread = threading.Thread(target=self._thread_main, name="hive-mcp-shared-session", daemon=True)
        self._thread = thread
        thread.start()
        self._ready_event.wait()
        if self._startup_error is not None:
            self._close_locked()
            raise self._startup_error
        if self._session is None or self._loop is None:
            self._close_locked()
            raise RuntimeError("Failed to initialize the shared Hive MCP session")

    def _thread_main(self) -> None:
        loop = asyncio.new_event_loop()
        self._loop = loop
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._session_runner())
        except Exception as exc:
            self._startup_error = exc
            self._ready_event.set()
        finally:
            self._session = None
            self._stop_event = None
            try:
                loop.close()
            finally:
                self._loop = None

    async def _session_runner(self) -> None:
        params = StdioServerParameters(
            command=self.server_spec.command,
            args=list(self.server_spec.args),
            cwd=self.server_spec.cwd,
            env=self.server_spec.env,
        )
        self._stop_event = asyncio.Event()
        async with stdio_client(params, errlog=self._stderr_handle) as (read_stream, write_stream):
            async with ClientSession(read_stream, write_stream) as session:
                await session.initialize()
                self._session = session
                self._session_starts += 1
                _emit_call_log(
                    "session_started",
                    command=self.server_spec.command,
                    args=list(self.server_spec.args),
                    cwd=self.server_spec.cwd,
                    server_root=self.server_spec.server_root,
                    session_starts=self._session_starts,
                )
                self._ready_event.set()
                await self._stop_event.wait()

    def _close_locked(self) -> None:
        loop = self._loop
        stop_event = self._stop_event
        thread = self._thread
        if loop is not None and stop_event is not None:
            try:
                loop.call_soon_threadsafe(stop_event.set)
            except Exception:
                pass
        if thread is not None and thread.is_alive():
            thread.join(timeout=5)

        self._thread = None
        self._loop = None
        self._session = None
        self._stop_event = None
        self._startup_error = None

        if self._stderr_handle not in (None, sys.stderr):
            try:
                self._stderr_handle.close()
            except Exception:
                pass
        self._stderr_handle = None


class McpHiveRuntime:
    """Synchronous Hive runtime built on a shared persistent MCP session."""

    def __init__(self, server_spec: McpServerSpec, default_env: str):
        self.server_spec = server_spec
        self.default_env = default_env
        self._session = SharedMcpSession(server_spec)
        self._call_history: list[HiveMcpCall] = []
        self._tool_names: list[str] | None = None
        self._tool_call_count = 0
        self._lock = threading.RLock()

    def describe(self) -> dict[str, Any]:
        return {
            "transport": "mcp-stdio",
            "session_mode": "shared",
            "command": self.server_spec.command,
            "args": list(self.server_spec.args),
            "cwd": self.server_spec.cwd,
            "server_root": self.server_spec.server_root,
            "default_env": self.default_env,
            **self._session.stats(),
            "tool_calls": self._tool_call_count,
        }

    def stats(self) -> dict[str, Any]:
        return self.describe()

    def get_call_history(self) -> list[HiveMcpCall]:
        with self._lock:
            return list(self._call_history)

    def clear_call_history(self) -> None:
        with self._lock:
            self._call_history.clear()

    def pop_call_history(self) -> list[HiveMcpCall]:
        with self._lock:
            calls = list(self._call_history)
            self._call_history.clear()
            return calls

    def list_tools(self) -> list[str]:
        with self._lock:
            if self._tool_names is None:
                self._tool_names = self._session.list_tools()
            return list(self._tool_names)

    def execute_query(self, schema: str, sql: str, env: str | None = None) -> str:
        _ = env
        return self._call_tool_text(
            "hive_execute_query",
            {
                "schema": schema,
                "sql": str(sql),
            },
        )

    def execute(self, schema: str, sql: str, env: str | None = None) -> None:
        _ = env
        self._call_tool_text(
            "hive_execute_dml",
            {
                "schema": schema,
                "sql": str(sql),
            },
        )

    def close_all(self) -> None:
        return None

    def shutdown(self) -> None:
        self._session.close()

    def _call_tool_text(self, tool_name: str, arguments: dict[str, Any]) -> str:
        normalized_arguments = dict(arguments)
        with self._lock:
            self._call_history.append(HiveMcpCall(tool_name=tool_name, arguments=normalized_arguments))
            self._tool_call_count += 1

        try:
            result = self._session.call_tool(tool_name, normalized_arguments)
            text = self._extract_text(result)
            status = "ok"
            error_detail = None

            if getattr(result, "isError", False):
                status = "is_error"
                error_detail = text or f"MCP tool {tool_name} returned an unknown error"
            else:
                normalized = text.strip()
                for prefix, english_label in ERROR_PREFIX_MAP.items():
                    if normalized.startswith(prefix):
                        status = "error_prefix"
                        detail = normalized.removeprefix(prefix).strip()
                        error_detail = f"{english_label}: {detail}"
                        break

            _emit_call_log(
                "tool_call",
                tool_name=tool_name,
                arguments=normalized_arguments,
                status=status,
                preview=text[:500],
                session_stats=self._session.stats(),
            )

            if error_detail:
                raise RuntimeError(error_detail)
            return text
        except Exception as exc:
            _emit_call_log(
                "tool_call",
                tool_name=tool_name,
                arguments=normalized_arguments,
                status="exception",
                error=str(exc),
                session_stats=self._session.stats(),
            )
            raise

    @staticmethod
    def _extract_text(result: Any) -> str:
        texts: list[str] = []
        for item in getattr(result, "content", []) or []:
            item_type = getattr(item, "type", "")
            if item_type == "text":
                text = getattr(item, "text", "")
                if text:
                    texts.append(str(text))
                continue
            if item_type == "resource":
                resource = getattr(item, "resource", None)
                text = getattr(resource, "text", "") if resource else ""
                if text:
                    texts.append(str(text))

        if texts:
            return "\n".join(texts)

        structured = getattr(result, "structuredContent", None)
        if structured is not None:
            return json.dumps(structured, ensure_ascii=False)
        return ""


def _detect_default_env(server_root: Path | None = None) -> str:
    explicit = os.getenv("HIVE_ACTIVE_ENV", "").strip()
    if explicit:
        return explicit

    if server_root:
        env_json_path = server_root / "env.json"
        if env_json_path.exists():
            try:
                payload = json.loads(env_json_path.read_text(encoding="utf-8-sig"))
            except Exception:
                payload = {}
            if isinstance(payload, dict):
                for field_name in ("env", "mode", "name"):
                    value = str(payload.get(field_name, "") or "").strip()
                    if value:
                        return value

    return "mcp"


def _extract_root_from_explicit_server() -> Path | None:
    explicit_cwd = os.getenv("HIVE_MCP_SERVER_CWD", "").strip()
    if explicit_cwd:
        candidate = Path(explicit_cwd).expanduser().resolve()
        if (candidate / "hive_exec_server.py").exists():
            return candidate

    explicit_args = shlex.split(os.getenv("HIVE_MCP_SERVER_ARGS", "").strip())
    for arg in explicit_args:
        candidate = Path(arg).expanduser()
        if candidate.name == "hive_exec_server.py" and candidate.exists():
            return candidate.resolve().parent
    return None


def resolve_mcp_server_spec(server_root: str | Path | None = None) -> tuple[McpServerSpec, str]:
    root_path: Path | None = None
    if server_root:
        root_path = Path(server_root).expanduser().resolve()
        server_script = root_path / "hive_exec_server.py"
        if not server_script.exists():
            raise FileNotFoundError(f"hive_exec_server.py not found under: {root_path}")
        python_command = os.getenv("HIVE_MCP_SERVER_PYTHON", "").strip() or sys.executable or "python3"
        return (
            McpServerSpec(
                command=python_command,
                args=(str(server_script),),
                cwd=os.getenv("HIVE_MCP_SERVER_CWD", "").strip() or str(root_path),
                env=dict(os.environ),
                server_root=str(root_path),
            ),
            _detect_default_env(root_path),
        )

    explicit_command = os.getenv("HIVE_MCP_SERVER_COMMAND", "").strip()
    explicit_args = shlex.split(os.getenv("HIVE_MCP_SERVER_ARGS", "").strip())
    explicit_cwd = os.getenv("HIVE_MCP_SERVER_CWD", "").strip() or None
    if explicit_command:
        root_path = _extract_root_from_explicit_server()
        return (
            McpServerSpec(
                command=explicit_command,
                args=tuple(explicit_args),
                cwd=explicit_cwd,
                env=dict(os.environ),
                server_root=str(root_path) if root_path else None,
            ),
            _detect_default_env(root_path),
        )

    root_path = Path(__file__).resolve().parent
    server_script = root_path / "hive_exec_server.py"
    python_command = os.getenv("HIVE_MCP_SERVER_PYTHON", "").strip() or sys.executable or "python3"
    return (
        McpServerSpec(
            command=python_command,
            args=(str(server_script),),
            cwd=str(root_path),
            env=dict(os.environ),
            server_root=str(root_path),
        ),
        _detect_default_env(root_path),
    )


def _runtime_cache_key(server_spec: McpServerSpec) -> str:
    return json.dumps(
        {
            "command": server_spec.command,
            "args": list(server_spec.args),
            "cwd": server_spec.cwd,
            "server_root": server_spec.server_root,
        },
        ensure_ascii=False,
        sort_keys=True,
    )


def build_hive_runtime(server_root: str | Path | None = None) -> tuple[McpHiveRuntime, str]:
    server_spec, default_env = resolve_mcp_server_spec(server_root=server_root)
    cache_key = _runtime_cache_key(server_spec)
    with _RUNTIME_CACHE_LOCK:
        runtime = _RUNTIME_CACHE.get(cache_key)
        if runtime is None:
            runtime = McpHiveRuntime(server_spec=server_spec, default_env=default_env)
            _RUNTIME_CACHE[cache_key] = runtime
        return runtime, default_env


def close_all_runtimes() -> None:
    with _RUNTIME_CACHE_LOCK:
        runtimes = list(_RUNTIME_CACHE.values())
        _RUNTIME_CACHE.clear()
    for runtime in runtimes:
        try:
            runtime.shutdown()
        except Exception:
            pass


atexit.register(close_all_runtimes)
