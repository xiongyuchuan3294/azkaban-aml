#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Smoke test Hive connectivity through SSH tunnel."""

import socket
import sys

from hive_client import ENV_NAME, HOST, PORT, JdbcHiveUtils


def tcp_probe(host: str, port: int, timeout: float = 3.0) -> tuple[bool, str]:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True, ""
    except Exception as exc:
        return False, str(exc)


def main() -> int:
    schema = sys.argv[1] if len(sys.argv) > 1 else "default"
    sql = sys.argv[2] if len(sys.argv) > 2 else "SELECT 1"

    print(f"[INFO] Environment: {ENV_NAME}")
    print(f"[INFO] Target: {HOST}:{PORT}")

    reachable, error = tcp_probe(HOST, PORT)
    if not reachable:
        print(f"[FAIL] TCP connect failed: {error}")
        print("[HINT] Ensure SSH tunnel is running:")
        print("       ssh -N -L 10010:127.0.0.1:10000 ubuntu@122.51.14.171")
        return 1

    print("[PASS] TCP port is reachable.")

    try:
        result = JdbcHiveUtils.execute_query(schema=schema, sql=sql)
    except Exception as exc:
        print(f"[FAIL] Hive query failed: {exc}")
        print("[HINT] Tunnel may be up, but Hive auth/schema/sql may be invalid.")
        return 2

    print("[PASS] Hive query succeeded.")
    print("[RESULT]")
    print(result.strip() if result.strip() else "<empty>")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
