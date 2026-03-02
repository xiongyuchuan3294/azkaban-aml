#!/usr/bin/env python3
"""
Merge requirement-impact mapping and data-lineage expansion into one analysis.

Input:
- requirement-parser output (json or markdown)
- diff-parser output (compact json)
- AML repository path

Output:
- Unified impact report with:
  1) requirement-to-changed-file mapping
  2) lineage expansion and minimal closure DAG
  3) node/table test scope
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SKILL_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_REQUIREMENT_REPORT = (
    SKILL_ROOT.parent / "requirement-parser" / "outputs" / "requirement_report.md"
)
DEFAULT_DIFF_REPORT = SKILL_ROOT.parent / "diff-parser" / "outputs" / "commit_report.json"
DEFAULT_OUTPUT_PATH = SKILL_ROOT / "outputs" / "impact_analysis_report.json"
DEFAULT_REPO_PATH = "/Users/xiongyuc/workspace/azkaban-aml"

FP_LINE_RE = re.compile(r"^-\s*`?(FP\d{3,})`?\s*\[([^\]]+)\]\s*(.+)$")
RULE_ID_RE = re.compile(r"\bR\d{3,}\b", re.IGNORECASE)
TOKEN_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*|[\u4e00-\u9fff]{2,}")
NODE_NAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_-]{1,80}$")
LAYER_TOKEN_RE = re.compile(r"\b(ODS|DWD|DWS|ADS)\b", re.IGNORECASE)

TABLE_PATTERN_RE = re.compile(
    r"\b(?:[A-Za-z_][A-Za-z0-9_]*\.)?[A-Za-z_][A-Za-z0-9_]*(?:_(?:raw|di|df|dd|dm|tmp))\b"
    r"|\b[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*\b"
)

INSERT_TABLE_RE = re.compile(
    r"(?i)\binsert\s+(?:overwrite|into)\s+table\s+`?([A-Za-z_][A-Za-z0-9_.]*)`?"
)
CREATE_TABLE_RE = re.compile(
    r"(?i)\bcreate\s+table(?:\s+if\s+not\s+exists)?\s+`?([A-Za-z_][A-Za-z0-9_.]*)`?"
)
FROM_TABLE_RE = re.compile(r"(?i)\bfrom\s+`?([A-Za-z_][A-Za-z0-9_.]*)`?")
JOIN_TABLE_RE = re.compile(r"(?i)\bjoin\s+`?([A-Za-z_][A-Za-z0-9_.]*)`?")
UPDATE_TABLE_RE = re.compile(r"(?i)\bupdate\s+`?([A-Za-z_][A-Za-z0-9_.]*)`?")

DEP_RE = re.compile(r"^\s*dependencies\s*=\s*(.+?)\s*$")
CMD_RE = re.compile(r"^\s*command\s*=\s*(.+?)\s*$")
SQL_REF_RE = re.compile(r"-f\s+([^\s\"']+\.sql)\b")

ZH_STOPWORDS = {
    "新增",
    "修改",
    "优化",
    "需求",
    "逻辑",
    "字段",
    "输入",
    "输出",
    "表名",
    "功能点",
    "规则",
    "条件",
    "命中",
    "风险",
    "阈值",
    "并且",
    "以及",
    "或者",
    "需要",
    "可以",
    "进行",
}

EN_STOPWORDS = {
    "the",
    "a",
    "an",
    "and",
    "or",
    "to",
    "for",
    "with",
    "without",
    "is",
    "are",
    "of",
    "on",
    "in",
    "by",
    "as",
    "from",
    "insert",
    "overwrite",
    "table",
    "select",
    "where",
    "join",
    "update",
    "create",
    "into",
    "sql",
    "file",
    "modified",
    "added",
    "deleted",
    "lines",
    "summary",
    "key",
    "points",
}

SQL_KEYWORDS = {
    "select",
    "from",
    "join",
    "where",
    "group",
    "by",
    "order",
    "insert",
    "overwrite",
    "table",
    "create",
    "if",
    "not",
    "exists",
    "union",
    "all",
    "left",
    "right",
    "inner",
    "outer",
    "on",
    "as",
    "and",
    "or",
    "case",
    "when",
    "then",
    "else",
    "end",
    "set",
    "use",
    "into",
    "values",
    "current_timestamp",
}

FILE_EXTENSIONS = {
    "md",
    "txt",
    "json",
    "yaml",
    "yml",
    "py",
    "sh",
    "sql",
    "job",
    "log",
    "csv",
    "xml",
    "ini",
    "cfg",
    "conf",
    "auto",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Analyze requirement impact in one stage: requirement-diff mapping, "
            "lineage expansion, and minimal closure DAG for test scope."
        )
    )
    parser.add_argument(
        "--requirement-report",
        default=str(DEFAULT_REQUIREMENT_REPORT),
        help=(
            "requirement-parser output path (json or markdown). "
            f"Default: {DEFAULT_REQUIREMENT_REPORT}"
        ),
    )
    parser.add_argument(
        "--diff-report",
        default=str(DEFAULT_DIFF_REPORT),
        help=f"diff-parser output JSON path. Default: {DEFAULT_DIFF_REPORT}",
    )
    parser.add_argument(
        "--repo",
        default=DEFAULT_REPO_PATH,
        help=f"AML repository path for lineage graph extraction. Default: {DEFAULT_REPO_PATH}",
    )
    parser.add_argument(
        "--min-map-confidence",
        type=float,
        default=0.22,
        help="Minimum confidence to accept requirement-file mapping. Default: 0.22.",
    )
    parser.add_argument(
        "--fallback-map-confidence",
        type=float,
        default=0.12,
        help="Fallback confidence when no file reaches min threshold. Default: 0.12.",
    )
    parser.add_argument(
        "--max-file-matches-per-item",
        type=int,
        default=3,
        help="Maximum mapped files per requirement item. Default: 3.",
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT_PATH),
        help=(
            "Output report path. Default: "
            "skills/requirement-impact-analysis/outputs/impact_analysis_report.json. "
            "Use '-' to print to stdout."
        ),
    )
    parser.add_argument(
        "--output-format",
        choices=["auto", "json", "md"],
        default="auto",
        help="Output format. Default: auto (infer by output suffix).",
    )
    return parser.parse_args()


def dedupe_keep_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        if not item:
            continue
        if item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result


def strip_bullet_prefix(text: str) -> str:
    value = text.strip()
    value = re.sub(r"^[\-\*\+]\s*", "", value)
    value = re.sub(r"^\d+[\.、\)]\s*", "", value)
    return value.strip()


def to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def infer_layer(text: str) -> str:
    lowered = text.lower()
    if "ods_" in lowered or " ods " in f" {lowered} ":
        return "ODS"
    if "dwd_" in lowered or " dwd " in f" {lowered} ":
        return "DWD"
    if "dws_" in lowered or " dws " in f" {lowered} ":
        return "DWS"
    if "ads_" in lowered or " ads " in f" {lowered} ":
        return "ADS"
    return "UNKNOWN"


def infer_layers(text: str) -> set[str]:
    layers = {match.upper() for match in LAYER_TOKEN_RE.findall(text)}
    for token in ("ods_", "dwd_", "dws_", "ads_"):
        if token in text.lower():
            layers.add(token[:3].upper())
    if not layers:
        layer = infer_layer(text)
        if layer != "UNKNOWN":
            layers.add(layer)
    return layers


def clean_identifier(token: str) -> str:
    value = token.strip().strip("`").strip(",;")
    value = value.rstrip(")")
    value = value.lstrip("(")
    return value.strip()


def is_table_identifier(value: str) -> bool:
    candidate = clean_identifier(value)
    if not candidate:
        return False
    if "${" in candidate:
        return False
    lowered = candidate.lower()
    if lowered in SQL_KEYWORDS:
        return False
    if "." in candidate:
        left, right = candidate.split(".", 1)
        if not left or not right:
            return False
        if "/" in left or "/" in right:
            return False
        if right.lower() in FILE_EXTENSIONS:
            return False
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", left):
            return False
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", right):
            return False
        if "_" not in right and left.lower() not in {"ods", "dwd", "dws", "ads", "aml_demo"}:
            return False
        return True
    return bool(
        re.match(
            r"^(?:ods|dwd|dws|ads)_[a-z0-9_]+(?:_(?:raw|di|df|dd|dm|tmp))?$",
            lowered,
        )
    )


def looks_like_node_name(value: str) -> bool:
    candidate = value.strip()
    if not NODE_NAME_RE.fullmatch(candidate):
        return False
    if is_table_identifier(candidate):
        return False
    return True


def token_set(text: str) -> set[str]:
    tokens: set[str] = set()
    if not text:
        return tokens

    for match in TOKEN_RE.findall(text):
        token = match.strip()
        if not token:
            continue
        lowered = token.lower()
        if len(lowered) <= 1:
            continue
        if re.fullmatch(r"\d+", lowered):
            continue
        if lowered in EN_STOPWORDS or lowered in ZH_STOPWORDS:
            continue
        tokens.add(lowered)

    for raw in re.split(r"[^A-Za-z0-9_]+", text):
        part = raw.strip().lower()
        if not part or len(part) <= 1:
            continue
        if part in EN_STOPWORDS:
            continue
        tokens.add(part)

    return tokens


def jaccard_similarity(left: set[str], right: set[str]) -> float:
    if not left or not right:
        return 0.0
    union = left | right
    if not union:
        return 0.0
    return len(left & right) / len(union)


def normalize_repo_relative_path(path_text: str, repo_path: Path) -> str:
    value = path_text.strip().replace("\\", "/").lstrip("./")
    if not value:
        return value

    repo_norm = str(repo_path).replace("\\", "/")
    if value.startswith(repo_norm + "/"):
        return value[len(repo_norm) + 1 :]

    repo_name = repo_path.name
    marker = f"{repo_name}/"
    if marker in value:
        return value.split(marker, 1)[1]

    return value


def extract_tables_from_text(text: str) -> list[str]:
    tables: list[str] = []
    for token in TABLE_PATTERN_RE.findall(text):
        candidate = clean_identifier(token)
        if is_table_identifier(candidate):
            tables.append(candidate)
    return dedupe_keep_order(tables)


def parse_node_table_section(lines: list[str]) -> list[dict[str, Any]]:
    sections: list[dict[str, Any]] = []
    current_node = ""
    dep_tables: list[str] = []
    result_tables: list[str] = []
    mode = ""

    def flush() -> None:
        nonlocal current_node, dep_tables, result_tables, mode
        if not current_node:
            return
        sections.append(
            {
                "node": current_node,
                "dependency_tables": dedupe_keep_order(dep_tables),
                "result_tables": dedupe_keep_order(result_tables),
            }
        )
        current_node = ""
        dep_tables = []
        result_tables = []
        mode = ""

    for raw_line in lines:
        line = strip_bullet_prefix(raw_line)
        if not line:
            continue
        if line.startswith("## "):
            break
        if line in {"依赖表：", "依赖表:", "依赖表"}:
            mode = "dependency"
            continue
        if line in {"结果表：", "结果表:", "结果表"}:
            mode = "result"
            continue

        if looks_like_node_name(line) and (
            not current_node or (mode in {"dependency", "result"} and (dep_tables or result_tables))
        ):
            flush()
            current_node = line
            mode = ""
            continue

        if not current_node:
            continue

        if mode == "dependency":
            dep_tables.extend(extract_tables_from_text(line) or [line])
            continue
        if mode == "result":
            result_tables.extend(extract_tables_from_text(line) or [line])
            continue

    flush()
    return sections


def parse_candidate_nodes_section(lines: list[str]) -> list[str]:
    nodes: list[str] = []
    in_nodes = False

    for raw_line in lines:
        line = raw_line.strip()
        if line.startswith("## "):
            in_nodes = False
        if line.startswith("### 候选节点"):
            in_nodes = True
            continue
        if not in_nodes:
            continue
        if line.startswith("### "):
            break
        matched = re.match(r"^-\s*`([^`]+)`", line)
        if not matched:
            continue
        node = matched.group(1).strip()
        if looks_like_node_name(node):
            nodes.append(node)

    return dedupe_keep_order(nodes)


def parse_requirement_markdown(path: Path) -> dict[str, Any]:
    raw = path.read_text(encoding="utf-8")
    lines = raw.splitlines()

    title = "未命名需求"
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("# "):
            title = stripped[2:].strip()
            title = title.replace("需求解析报告：", "").strip()
            break

    functional_points: list[dict[str, str]] = []
    fp_index = 1
    for line in lines:
        stripped = line.strip()
        matched = FP_LINE_RE.match(stripped)
        if matched:
            fp_id, fp_type, text = matched.groups()
            text = re.sub(r"^\[[^\]]+\]\s*", "", text.strip())
            functional_points.append(
                {
                    "id": fp_id,
                    "type": fp_type.strip() or "general",
                    "text": text,
                }
            )
            fp_index = max(fp_index, int(fp_id[2:]) + 1)

    if not functional_points:
        in_logic = False
        for line in lines:
            stripped = line.strip()
            if stripped.startswith("## 主逻辑"):
                in_logic = True
                continue
            if in_logic and stripped.startswith("## "):
                break
            if not in_logic:
                continue
            cleaned = strip_bullet_prefix(stripped)
            if not cleaned:
                continue
            functional_points.append(
                {"id": f"FP{fp_index:03d}", "type": "general", "text": cleaned}
            )
            fp_index += 1

    node_table_sections: list[dict[str, Any]] = []
    for idx, line in enumerate(lines):
        normalized = line.strip()
        if normalized.startswith("## 节点与表关系") or normalized.startswith("## 节点与表信息"):
            node_table_sections = parse_node_table_section(lines[idx + 1 :])
            break

    workflow_nodes: list[dict[str, Any]] = []
    for section in node_table_sections:
        workflow_nodes.append(
            {
                "node": section["node"],
                "confidence": 0.8,
                "reason": "来自 requirement_report.md 的“节点与表关系”章节",
            }
        )

    if not workflow_nodes:
        candidate_nodes = parse_candidate_nodes_section(lines)
        for node in candidate_nodes:
            workflow_nodes.append(
                {
                    "node": node,
                    "confidence": 0.75,
                    "reason": "来自 requirement_report.md 的“候选节点”章节",
                }
            )

    table_values: list[str] = []
    for section in node_table_sections:
        table_values.extend(section.get("dependency_tables", []))
        table_values.extend(section.get("result_tables", []))
    table_values.extend(extract_tables_from_text(raw))

    tables = [
        {
            "mention": table,
            "mapped_table": table,
            "layer": infer_layer(table),
            "confidence": 0.75,
            "match_type": "from_markdown_report",
        }
        for table in dedupe_keep_order(table_values)
    ]

    return {
        "title": title,
        "source_document": str(path),
        "functional_points": functional_points,
        "workflow_nodes": workflow_nodes,
        "tables": tables,
        "node_table_sections": node_table_sections,
        "input_format": "markdown",
    }


def parse_requirement_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("requirement report JSON must be an object")

    title = str(payload.get("title") or payload.get("source_document") or "未命名需求")

    functional_points: list[dict[str, str]] = []
    for idx, item in enumerate(payload.get("functional_points", []), start=1):
        if isinstance(item, dict):
            text = str(item.get("text", "")).strip()
            if not text:
                continue
            functional_points.append(
                {
                    "id": str(item.get("id") or f"FP{idx:03d}"),
                    "type": str(item.get("type") or "general"),
                    "text": text,
                }
            )

    if not functional_points:
        for idx, item in enumerate(payload.get("main_logic", []), start=1):
            if not isinstance(item, dict):
                continue
            detail = str(item.get("detail", "")).strip()
            if not detail:
                continue
            functional_points.append(
                {
                    "id": f"FP{idx:03d}",
                    "type": str(item.get("type") or "general"),
                    "text": detail,
                }
            )

    workflow_nodes: list[dict[str, Any]] = []
    for item in payload.get("workflow_nodes", []):
        if isinstance(item, dict):
            node = str(item.get("node", "")).strip()
            if not node:
                continue
            workflow_nodes.append(
                {
                    "node": node,
                    "confidence": to_float(item.get("confidence"), 0.7),
                    "reason": str(item.get("reason", "")),
                }
            )
        elif isinstance(item, str) and item.strip():
            workflow_nodes.append(
                {"node": item.strip(), "confidence": 0.7, "reason": "来自 JSON workflow_nodes"}
            )

    tables: list[dict[str, Any]] = []
    for item in payload.get("tables", []):
        if isinstance(item, dict):
            mapped = str(item.get("mapped_table") or item.get("table") or "").strip()
            mention = str(item.get("mention") or mapped).strip()
            if not mapped and not mention:
                continue
            final_table = mapped or mention
            tables.append(
                {
                    "mention": mention,
                    "mapped_table": final_table,
                    "layer": str(item.get("layer") or infer_layer(final_table)),
                    "confidence": to_float(item.get("confidence"), 0.7),
                    "match_type": str(item.get("match_type") or "from_json_report"),
                }
            )
        elif isinstance(item, str) and item.strip():
            table = item.strip()
            tables.append(
                {
                    "mention": table,
                    "mapped_table": table,
                    "layer": infer_layer(table),
                    "confidence": 0.7,
                    "match_type": "from_json_report",
                }
            )

    node_table_sections = payload.get("node_table_sections")
    if not isinstance(node_table_sections, list):
        node_table_sections = []

    return {
        "title": title,
        "source_document": str(payload.get("source_document", path)),
        "functional_points": functional_points,
        "workflow_nodes": workflow_nodes,
        "tables": tables,
        "node_table_sections": node_table_sections,
        "input_format": "json",
    }


def load_requirement_report(path_text: str) -> dict[str, Any]:
    path = Path(path_text).expanduser().resolve()
    if not path.exists() or not path.is_file():
        raise FileNotFoundError(f"requirement report does not exist: {path}")
    if path.suffix.lower() == ".json":
        return parse_requirement_json(path)
    return parse_requirement_markdown(path)


def load_diff_report(path_text: str, repo_path: Path) -> dict[str, Any]:
    path = Path(path_text).expanduser().resolve()
    if not path.exists() or not path.is_file():
        raise FileNotFoundError(f"diff report does not exist: {path}")

    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("diff report JSON must be an object")

    commits = payload.get("commits", [])
    flattened: dict[str, dict[str, Any]] = {}
    commit_count = 0
    for commit in commits:
        if not isinstance(commit, dict):
            continue
        commit_count += 1
        commit_id = str(commit.get("commit_id", "")).strip()
        commit_date = str(commit.get("date", "")).strip()
        subject = str(commit.get("subject", "")).strip()
        for file_item in commit.get("changed_files", []):
            if not isinstance(file_item, dict):
                continue
            raw_path = str(file_item.get("path", "")).strip()
            if not raw_path:
                continue
            relative_path = normalize_repo_relative_path(raw_path, repo_path)
            summary = str(file_item.get("summary", "")).strip()
            entry = flattened.setdefault(
                relative_path,
                {
                    "path": relative_path,
                    "raw_paths": [],
                    "summary_list": [],
                    "commit_ids": [],
                    "commit_dates": [],
                    "subjects": [],
                },
            )
            entry["raw_paths"].append(raw_path)
            if summary:
                entry["summary_list"].append(summary)
            if commit_id:
                entry["commit_ids"].append(commit_id)
            if commit_date:
                entry["commit_dates"].append(commit_date)
            if subject:
                entry["subjects"].append(subject)

    changed_files: list[dict[str, Any]] = []
    for path_key in sorted(flattened.keys()):
        entry = flattened[path_key]
        summaries = dedupe_keep_order(entry["summary_list"])
        changed_files.append(
            {
                "path": path_key,
                "raw_paths": dedupe_keep_order(entry["raw_paths"]),
                "summary": summaries[0] if summaries else "",
                "all_summaries": summaries,
                "commit_ids": dedupe_keep_order(entry["commit_ids"]),
                "commit_dates": dedupe_keep_order(entry["commit_dates"]),
                "subjects": dedupe_keep_order(entry["subjects"]),
            }
        )

    return {
        "source_document": str(path),
        "repo_path": str(payload.get("repo_path", "")),
        "generated_at": str(payload.get("generated_at", "")),
        "total_commits": int(payload.get("total_commits", commit_count)),
        "changed_files": changed_files,
    }


def parse_job_file(job_path: Path, repo_path: Path) -> tuple[list[str], list[str]]:
    dependencies: list[str] = []
    sql_refs: list[str] = []

    try:
        lines = job_path.read_text(encoding="utf-8", errors="ignore").splitlines()
    except OSError:
        return dependencies, sql_refs

    for line in lines:
        dep_match = DEP_RE.match(line)
        if dep_match:
            dep_text = dep_match.group(1).strip()
            dep_items = re.split(r"[,\s]+", dep_text)
            dependencies.extend(item.strip() for item in dep_items if item.strip())
            continue

        cmd_match = CMD_RE.match(line)
        if not cmd_match:
            continue

        command_text = cmd_match.group(1).strip()
        for raw_ref in SQL_REF_RE.findall(command_text):
            ref = raw_ref.strip().strip("'\"")
            if not ref:
                continue
            candidate = (job_path.parent / ref).resolve()
            if not candidate.exists():
                fallback = (repo_path / ref.lstrip("./")).resolve()
                candidate = fallback if fallback.exists() else candidate
            if candidate.exists() and candidate.is_file():
                try:
                    relative = str(candidate.relative_to(repo_path)).replace("\\", "/")
                except ValueError:
                    relative = str(candidate).replace("\\", "/")
                sql_refs.append(relative)
            else:
                sql_refs.append(ref.replace("\\", "/"))

    return dedupe_keep_order(dependencies), dedupe_keep_order(sql_refs)


def extract_sql_tables(sql_path: Path) -> tuple[list[str], list[str]]:
    try:
        content = sql_path.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return [], []

    output_tables: list[str] = []
    input_tables: list[str] = []

    for pattern in (INSERT_TABLE_RE, CREATE_TABLE_RE):
        for matched in pattern.findall(content):
            candidate = clean_identifier(matched)
            if is_table_identifier(candidate):
                output_tables.append(candidate)

    for pattern in (FROM_TABLE_RE, JOIN_TABLE_RE, UPDATE_TABLE_RE):
        for matched in pattern.findall(content):
            candidate = clean_identifier(matched)
            if is_table_identifier(candidate):
                input_tables.append(candidate)

    outputs = dedupe_keep_order(output_tables)
    inputs = [table for table in dedupe_keep_order(input_tables) if table not in set(outputs)]
    return inputs, outputs


def build_repo_lineage(repo_path_text: str) -> dict[str, Any]:
    repo_path = Path(repo_path_text).expanduser().resolve()
    if not repo_path.exists() or not repo_path.is_dir():
        raise FileNotFoundError(f"repo path does not exist: {repo_path}")

    jobs_dir = repo_path / "jobs"
    sql_dir = repo_path / "sql"
    warnings: list[str] = []

    node_profiles: dict[str, dict[str, Any]] = {}
    sql_to_nodes: dict[str, list[str]] = defaultdict(list)

    job_files = sorted(jobs_dir.glob("*.job")) if jobs_dir.is_dir() else []
    if not job_files:
        warnings.append("未在 repo/jobs 找到 *.job，节点图可能不完整。")

    for job_file in job_files:
        node = job_file.stem
        dependencies, sql_refs = parse_job_file(job_file, repo_path)
        node_profiles[node] = {
            "node": node,
            "job_file": str(job_file.relative_to(repo_path)).replace("\\", "/"),
            "dependencies": dependencies,
            "sql_files": sql_refs,
            "input_tables": [],
            "output_tables": [],
        }
        for sql_ref in sql_refs:
            sql_to_nodes[sql_ref].append(node)
            sql_to_nodes[Path(sql_ref).name].append(node)

    sql_cache: dict[str, tuple[list[str], list[str]]] = {}
    for node, profile in node_profiles.items():
        input_tables: list[str] = []
        output_tables: list[str] = []
        for sql_ref in profile["sql_files"]:
            sql_path = (repo_path / sql_ref).resolve()
            if not sql_path.exists() and sql_dir.is_dir():
                sql_path = (sql_dir / Path(sql_ref).name).resolve()
            if not sql_path.exists():
                continue
            cache_key = str(sql_path)
            if cache_key not in sql_cache:
                sql_cache[cache_key] = extract_sql_tables(sql_path)
            sql_inputs, sql_outputs = sql_cache[cache_key]
            input_tables.extend(sql_inputs)
            output_tables.extend(sql_outputs)
        profile["input_tables"] = dedupe_keep_order(input_tables)
        profile["output_tables"] = dedupe_keep_order(output_tables)

    table_producers: dict[str, list[str]] = defaultdict(list)
    table_consumers: dict[str, list[str]] = defaultdict(list)
    for node, profile in node_profiles.items():
        for table in profile["output_tables"]:
            table_producers[table].append(node)
        for table in profile["input_tables"]:
            table_consumers[table].append(node)

    node_edge_set: set[tuple[str, str, str]] = set()
    for node, profile in node_profiles.items():
        for dep in profile["dependencies"]:
            if dep in node_profiles:
                node_edge_set.add((dep, node, "dependency"))
        for table in profile["input_tables"]:
            for producer in table_producers.get(table, []):
                if producer != node:
                    node_edge_set.add((producer, node, "table_lineage"))

    table_edge_set: set[tuple[str, str, str]] = set()
    for node, profile in node_profiles.items():
        inputs = profile["input_tables"] or []
        outputs = profile["output_tables"] or []
        for input_table in inputs:
            for output_table in outputs:
                if input_table != output_table:
                    table_edge_set.add((input_table, output_table, node))

    node_edges = [
        {"from": left, "to": right, "type": edge_type}
        for left, right, edge_type in sorted(node_edge_set)
    ]
    table_edges = [
        {"from_table": left, "to_table": right, "via_node": node}
        for left, right, node in sorted(table_edge_set)
    ]

    return {
        "repo_path": str(repo_path),
        "node_profiles": node_profiles,
        "node_edges": node_edges,
        "table_edges": table_edges,
        "sql_to_nodes": {key: dedupe_keep_order(value) for key, value in sql_to_nodes.items()},
        "table_producers": {key: dedupe_keep_order(value) for key, value in table_producers.items()},
        "table_consumers": {key: dedupe_keep_order(value) for key, value in table_consumers.items()},
        "warnings": warnings,
    }


def file_to_node_candidates(
    relative_path: str,
    summary: str,
    node_profiles: dict[str, dict[str, Any]],
    sql_to_nodes: dict[str, list[str]],
) -> list[str]:
    candidates: list[str] = []
    path = relative_path.replace("\\", "/")
    path_name = Path(path).name

    if path.startswith("jobs/") and path.endswith(".job"):
        candidates.append(Path(path).stem)

    candidates.extend(sql_to_nodes.get(path, []))
    candidates.extend(sql_to_nodes.get(path_name, []))

    haystack = f"{path} {summary}".lower()
    for node in node_profiles:
        if node.lower() in haystack:
            candidates.append(node)

    return [node for node in dedupe_keep_order(candidates) if node in node_profiles]


def build_file_contexts(diff_payload: dict[str, Any], lineage_payload: dict[str, Any]) -> list[dict[str, Any]]:
    node_profiles = lineage_payload["node_profiles"]
    sql_to_nodes = lineage_payload["sql_to_nodes"]

    contexts: list[dict[str, Any]] = []
    for file_item in diff_payload.get("changed_files", []):
        if not isinstance(file_item, dict):
            continue
        path = str(file_item.get("path", "")).strip()
        if not path:
            continue
        summary = str(file_item.get("summary", "")).strip()
        text = f"{path} {summary}"
        context = {
            "path": path,
            "summary": summary,
            "tokens": token_set(text),
            "layers": infer_layers(text),
            "rule_ids": {rule.upper() for rule in RULE_ID_RE.findall(text)},
            "tables": set(extract_tables_from_text(text)),
            "node_candidates": file_to_node_candidates(path, summary, node_profiles, sql_to_nodes),
            "commit_ids": file_item.get("commit_ids", []),
            "subjects": file_item.get("subjects", []),
        }
        contexts.append(context)
    return contexts


def build_requirement_context(requirement_payload: dict[str, Any]) -> dict[str, Any]:
    functional_points = requirement_payload.get("functional_points", [])
    workflow_nodes = requirement_payload.get("workflow_nodes", [])
    tables = requirement_payload.get("tables", [])
    node_sections = requirement_payload.get("node_table_sections", [])

    items: list[dict[str, Any]] = []
    for idx, item in enumerate(functional_points, start=1):
        if not isinstance(item, dict):
            continue
        text = str(item.get("text", "")).strip()
        if not text:
            continue
        fp_id = str(item.get("id") or f"FP{idx:03d}")
        fp_type = str(item.get("type") or "general")
        items.append({"id": fp_id, "type": fp_type, "text": text})

    req_nodes: list[str] = []
    for node_item in workflow_nodes:
        if isinstance(node_item, dict):
            node = str(node_item.get("node", "")).strip()
            if node:
                req_nodes.append(node)
        elif isinstance(node_item, str) and node_item.strip():
            req_nodes.append(node_item.strip())
    for section in node_sections:
        if isinstance(section, dict):
            node = str(section.get("node", "")).strip()
            if node:
                req_nodes.append(node)

    req_tables: list[str] = []
    for table_item in tables:
        if isinstance(table_item, dict):
            mapped = str(
                table_item.get("mapped_table")
                or table_item.get("table")
                or table_item.get("mention")
                or ""
            ).strip()
            if mapped:
                req_tables.append(mapped)
        elif isinstance(table_item, str) and table_item.strip():
            req_tables.append(table_item.strip())
    for section in node_sections:
        if not isinstance(section, dict):
            continue
        for key in ("dependency_tables", "result_tables"):
            values = section.get(key, [])
            if isinstance(values, list):
                for value in values:
                    text = str(value).strip()
                    if text:
                        req_tables.append(text)

    for item in items:
        req_tables.extend(extract_tables_from_text(item["text"]))

    return {
        "title": requirement_payload.get("title", "未命名需求"),
        "items": items,
        "workflow_nodes": dedupe_keep_order(req_nodes),
        "tables": dedupe_keep_order(req_tables),
        "source_document": requirement_payload.get("source_document", ""),
        "input_format": requirement_payload.get("input_format", "unknown"),
    }


def score_item_file_mapping(
    item: dict[str, Any],
    file_ctx: dict[str, Any],
    req_nodes: set[str],
    req_tables: set[str],
) -> tuple[float, list[str]]:
    item_text = item["text"]
    item_type = item["type"].lower()
    item_tokens = token_set(item_text)
    item_layers = infer_layers(item_text)
    item_rules = {rule.upper() for rule in RULE_ID_RE.findall(item_text)}

    file_tokens = file_ctx["tokens"]
    file_layers = file_ctx["layers"]
    file_rules = file_ctx["rule_ids"]
    file_nodes = set(file_ctx["node_candidates"])
    file_tables = set(file_ctx["tables"])

    score = 0.0
    reasons: list[str] = []

    overlap = jaccard_similarity(item_tokens, file_tokens)
    if overlap > 0:
        overlap_score = min(0.42, overlap * 0.95 + 0.04)
        score += overlap_score
        reasons.append(f"关键词重合度={overlap:.2f}")

    if item_layers and file_layers:
        common_layers = item_layers & file_layers
        if common_layers:
            score += min(0.2, 0.08 + 0.06 * len(common_layers))
            reasons.append(f"层级匹配={','.join(sorted(common_layers))}")

    if item_rules and file_rules:
        common_rules = item_rules & file_rules
        if common_rules:
            score += 0.25
            reasons.append(f"规则编号匹配={','.join(sorted(common_rules))}")

    if req_nodes and file_nodes:
        common_nodes = req_nodes & file_nodes
        if common_nodes:
            score += 0.24
            reasons.append(f"节点匹配={','.join(sorted(common_nodes))}")

    if req_tables and file_tables:
        common_tables = req_tables & file_tables
        if common_tables:
            score += 0.2
            reasons.append(f"表名匹配={','.join(sorted(common_tables)[:2])}")

    path_lower = file_ctx["path"].lower()
    summary_lower = file_ctx["summary"].lower()
    if item_type == "rule_change" and any(
        token in path_lower or token in summary_lower for token in ("alert", "rule", "ads")
    ):
        score += 0.1
        reasons.append("规则型需求与告警/规则文件特征匹配")

    if item_type == "metric_change" and any(
        token in path_lower or token in summary_lower for token in ("dws", "aggregate", "stat")
    ):
        score += 0.1
        reasons.append("指标型需求与聚合层文件特征匹配")

    if item_type == "table_change" and file_tables:
        score += 0.08
        reasons.append("表结构相关需求命中含表名文件")

    final_score = min(0.98, round(score, 3))
    return final_score, reasons


def build_requirement_diff_mapping(
    requirement_ctx: dict[str, Any],
    file_contexts: list[dict[str, Any]],
    min_confidence: float,
    fallback_confidence: float,
    max_matches: int,
) -> dict[str, Any]:
    req_nodes = set(requirement_ctx["workflow_nodes"])
    req_tables = set(requirement_ctx["tables"])

    mapped_items: list[dict[str, Any]] = []
    unmapped_items: list[dict[str, Any]] = []
    mapped_file_paths: set[str] = set()

    for item in requirement_ctx["items"]:
        scored: list[dict[str, Any]] = []
        for file_ctx in file_contexts:
            confidence, reasons = score_item_file_mapping(item, file_ctx, req_nodes, req_tables)
            if confidence <= 0:
                continue
            scored.append(
                {
                    "path": file_ctx["path"],
                    "summary": file_ctx["summary"],
                    "confidence": confidence,
                    "node_candidates": file_ctx["node_candidates"],
                    "rationale": reasons,
                }
            )

        scored.sort(key=lambda value: (-value["confidence"], value["path"]))
        selected = [item for item in scored if item["confidence"] >= min_confidence][:max_matches]
        mapping_mode = "threshold"

        if not selected and scored and scored[0]["confidence"] >= fallback_confidence:
            fallback = dict(scored[0])
            fallback["rationale"] = fallback["rationale"] + ["未达到阈值，采用最高分回退映射"]
            selected = [fallback]
            mapping_mode = "fallback"

        if selected:
            for matched in selected:
                mapped_file_paths.add(matched["path"])
            mapped_items.append(
                {
                    "requirement_item_id": item["id"],
                    "requirement_type": item["type"],
                    "requirement_text": item["text"],
                    "mapping_mode": mapping_mode,
                    "matched_files": selected,
                }
            )
        else:
            unmapped_items.append(
                {
                    "requirement_item_id": item["id"],
                    "requirement_type": item["type"],
                    "requirement_text": item["text"],
                    "reason": "未找到达到最低置信度的变更文件映射",
                }
            )

    unmapped_files: list[dict[str, Any]] = []
    for file_ctx in file_contexts:
        if file_ctx["path"] in mapped_file_paths:
            continue
        unmapped_files.append(
            {
                "path": file_ctx["path"],
                "summary": file_ctx["summary"],
                "node_candidates": file_ctx["node_candidates"],
            }
        )

    mapped_ratio = (
        round(len(mapped_items) / len(requirement_ctx["items"]), 3)
        if requirement_ctx["items"]
        else 0.0
    )

    return {
        "min_confidence": min_confidence,
        "fallback_confidence": fallback_confidence,
        "mapped_ratio": mapped_ratio,
        "mapped_items": mapped_items,
        "unmapped_requirement_items": unmapped_items,
        "unmapped_changed_files": unmapped_files,
    }


def build_graph_helpers(node_edges: list[dict[str, str]]) -> tuple[dict[str, set[str]], dict[str, set[str]]]:
    forward: dict[str, set[str]] = defaultdict(set)
    reverse: dict[str, set[str]] = defaultdict(set)
    for edge in node_edges:
        left = edge.get("from", "")
        right = edge.get("to", "")
        if not left or not right:
            continue
        forward[left].add(right)
        reverse[right].add(left)
    return forward, reverse


def bfs_reachable(seeds: set[str], adjacency: dict[str, set[str]]) -> set[str]:
    visited: set[str] = set()
    queue = deque(sorted(seeds))
    while queue:
        node = queue.popleft()
        for nxt in adjacency.get(node, set()):
            if nxt in visited or nxt in seeds:
                continue
            visited.add(nxt)
            queue.append(nxt)
    return visited


def topological_sort(nodes: set[str], edges: list[dict[str, str]]) -> list[str]:
    adjacency: dict[str, set[str]] = defaultdict(set)
    indegree: dict[str, int] = {node: 0 for node in nodes}
    for edge in edges:
        left = edge.get("from", "")
        right = edge.get("to", "")
        if left not in nodes or right not in nodes:
            continue
        if right in adjacency[left]:
            continue
        adjacency[left].add(right)
        indegree[right] = indegree.get(right, 0) + 1

    queue = deque(sorted(node for node, degree in indegree.items() if degree == 0))
    ordered: list[str] = []
    while queue:
        node = queue.popleft()
        ordered.append(node)
        for nxt in sorted(adjacency.get(node, set())):
            indegree[nxt] -= 1
            if indegree[nxt] == 0:
                queue.append(nxt)

    if len(ordered) < len(nodes):
        for node in sorted(nodes):
            if node not in ordered:
                ordered.append(node)
    return ordered


def build_table_helpers(
    table_edges: list[dict[str, str]],
) -> tuple[dict[str, set[str]], dict[str, set[str]]]:
    forward: dict[str, set[str]] = defaultdict(set)
    reverse: dict[str, set[str]] = defaultdict(set)
    for edge in table_edges:
        left = edge.get("from_table", "")
        right = edge.get("to_table", "")
        if not left or not right:
            continue
        forward[left].add(right)
        reverse[right].add(left)
    return forward, reverse


def collect_tables_from_nodes(
    nodes: list[str], node_profiles: dict[str, dict[str, Any]]
) -> tuple[list[str], list[str]]:
    dependency_tables: list[str] = []
    result_tables: list[str] = []
    for node in nodes:
        profile = node_profiles.get(node, {})
        dependency_tables.extend(profile.get("input_tables", []))
        result_tables.extend(profile.get("output_tables", []))
    return dedupe_keep_order(dependency_tables), dedupe_keep_order(result_tables)


def build_lineage_and_scope(
    mapping_payload: dict[str, Any],
    requirement_ctx: dict[str, Any],
    file_contexts: list[dict[str, Any]],
    lineage_payload: dict[str, Any],
) -> dict[str, Any]:
    node_profiles = lineage_payload["node_profiles"]
    node_edges = lineage_payload["node_edges"]
    table_edges = lineage_payload["table_edges"]

    req_nodes = [node for node in requirement_ctx["workflow_nodes"] if node in node_profiles]

    diff_nodes: list[str] = []
    for file_ctx in file_contexts:
        diff_nodes.extend(file_ctx["node_candidates"])
    diff_nodes = [node for node in dedupe_keep_order(diff_nodes) if node in node_profiles]

    mapped_nodes: list[str] = []
    for item in mapping_payload.get("mapped_items", []):
        for matched in item.get("matched_files", []):
            mapped_nodes.extend(matched.get("node_candidates", []))
    mapped_nodes = [node for node in dedupe_keep_order(mapped_nodes) if node in node_profiles]

    seed_nodes = dedupe_keep_order(req_nodes + mapped_nodes + diff_nodes)
    if not seed_nodes:
        for node in node_profiles:
            if node in requirement_ctx["title"]:
                seed_nodes.append(node)

    seed_set = set(seed_nodes)
    forward_nodes, reverse_nodes = build_graph_helpers(node_edges)
    upstream_set = bfs_reachable(seed_set, reverse_nodes)
    downstream_set = bfs_reachable(seed_set, forward_nodes)

    must_run_set = seed_set | upstream_set
    validation_set = downstream_set - must_run_set
    expanded_set = must_run_set | validation_set

    topo_order = topological_sort(set(node_profiles.keys()), node_edges)
    must_run_nodes = [node for node in topo_order if node in must_run_set]
    validation_nodes = [node for node in topo_order if node in validation_set]
    expanded_nodes = [node for node in topo_order if node in expanded_set]

    minimal_edges = [
        edge
        for edge in node_edges
        if edge.get("from") in must_run_set and edge.get("to") in must_run_set
    ]
    expanded_edges = [
        edge
        for edge in node_edges
        if edge.get("from") in expanded_set and edge.get("to") in expanded_set
    ]

    direct_tables: list[str] = []
    direct_tables.extend(requirement_ctx["tables"])
    for file_ctx in file_contexts:
        direct_tables.extend(file_ctx["tables"])
    dep_tables, result_tables = collect_tables_from_nodes(seed_nodes, node_profiles)
    direct_tables.extend(dep_tables)
    direct_tables.extend(result_tables)
    direct_tables = [table for table in dedupe_keep_order(direct_tables) if is_table_identifier(table)]

    table_forward, table_reverse = build_table_helpers(table_edges)
    direct_table_set = set(direct_tables)
    upstream_tables = sorted(bfs_reachable(direct_table_set, table_reverse))
    downstream_tables = sorted(bfs_reachable(direct_table_set, table_forward))
    all_tables = dedupe_keep_order(direct_tables + upstream_tables + downstream_tables)

    node_scope_items: list[dict[str, Any]] = []
    for node in must_run_nodes + validation_nodes:
        scope = "must_run" if node in must_run_set else "validation"
        reasons: list[str] = []
        if node in req_nodes:
            reasons.append("来自需求文档推断节点")
        if node in mapped_nodes:
            reasons.append("由需求点映射到变更文件")
        if node in diff_nodes:
            reasons.append("对应 git diff 直接变更")
        if node in upstream_set and node not in seed_set:
            reasons.append("最小闭包的上游依赖节点")
        if node in validation_set:
            reasons.append("下游验证节点")
        profile = node_profiles.get(node, {})
        node_scope_items.append(
            {
                "node": node,
                "scope": scope,
                "dependency_tables": profile.get("input_tables", []),
                "result_tables": profile.get("output_tables", []),
                "reasons": reasons or ["由节点图推导得到"],
            }
        )

    suggested_checks: list[str] = []
    if must_run_nodes:
        suggested_checks.append(
            f"先执行最小闭包节点链路：{' -> '.join(must_run_nodes)}，确保依赖完整后再验证结果。"
        )
    if validation_nodes:
        suggested_checks.append(
            f"补充下游验证节点：{' -> '.join(validation_nodes)}，确认变更未引入回归。"
        )
    if result_tables:
        suggested_checks.append(
            f"重点核对结果表：{', '.join(result_tables[:4])}"
            + (" ..." if len(result_tables) > 4 else "")
        )

    return {
        "seed_nodes": seed_nodes,
        "direct_nodes": {
            "from_requirement": req_nodes,
            "from_mapping": mapped_nodes,
            "from_diff": diff_nodes,
        },
        "must_run_nodes": must_run_nodes,
        "validation_nodes": validation_nodes,
        "minimal_closure_dag": {"nodes": must_run_nodes, "edges": minimal_edges},
        "expanded_node_graph": {"nodes": expanded_nodes, "edges": expanded_edges},
        "tables": {
            "direct": sorted(direct_tables),
            "upstream": upstream_tables,
            "downstream": downstream_tables,
            "all": sorted(all_tables),
            "edges": table_edges,
        },
        "node_test_scope": node_scope_items,
        "suggested_checks": suggested_checks,
    }


def build_report(args: argparse.Namespace) -> dict[str, Any]:
    repo_path = Path(args.repo).expanduser().resolve()
    requirement_payload = load_requirement_report(args.requirement_report)
    diff_payload = load_diff_report(args.diff_report, repo_path)
    lineage_payload = build_repo_lineage(args.repo)

    requirement_ctx = build_requirement_context(requirement_payload)
    file_contexts = build_file_contexts(diff_payload, lineage_payload)

    mapping_payload = build_requirement_diff_mapping(
        requirement_ctx=requirement_ctx,
        file_contexts=file_contexts,
        min_confidence=args.min_map_confidence,
        fallback_confidence=args.fallback_map_confidence,
        max_matches=max(1, args.max_file_matches_per_item),
    )

    lineage_scope_payload = build_lineage_and_scope(
        mapping_payload=mapping_payload,
        requirement_ctx=requirement_ctx,
        file_contexts=file_contexts,
        lineage_payload=lineage_payload,
    )

    warnings = list(lineage_payload.get("warnings", []))
    if not requirement_ctx["items"]:
        warnings.append("需求报告中未提取到 functional_points，映射覆盖率参考价值有限。")
    if not file_contexts:
        warnings.append("diff 报告中无 changed_files。")
    if not lineage_scope_payload["seed_nodes"]:
        warnings.append("未识别到种子节点，无法形成有效最小闭包 DAG。")
    if mapping_payload["mapped_ratio"] < 0.4 and requirement_ctx["items"]:
        warnings.append("需求点映射覆盖率低于 40%，建议补充关键词、表映射或人工确认。")

    status = "ok"
    if not file_contexts or not lineage_scope_payload["seed_nodes"]:
        status = "blocked"
    elif mapping_payload["mapped_ratio"] < 0.4:
        status = "warning"

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "repo_path": str(repo_path),
        "input_artifacts": {
            "requirement_report": str(Path(args.requirement_report).expanduser().resolve()),
            "diff_report": str(Path(args.diff_report).expanduser().resolve()),
            "requirement_report_format": requirement_ctx["input_format"],
        },
        "pipeline": [
            "requirement-parser: 需求文档结构化抽取",
            "diff-parser: git diff/commit 变更清单解析",
            "requirement-impact-analysis: 需求点映射 + 血缘扩展 + 最小闭包 DAG + 测试节点范围",
        ],
        "requirement_summary": {
            "title": requirement_ctx["title"],
            "functional_point_count": len(requirement_ctx["items"]),
            "workflow_nodes": requirement_ctx["workflow_nodes"],
            "tables": requirement_ctx["tables"],
        },
        "diff_summary": {
            "total_commits": diff_payload["total_commits"],
            "changed_file_count": len(file_contexts),
            "changed_files": [
                {"path": file_ctx["path"], "summary": file_ctx["summary"]}
                for file_ctx in file_contexts
            ],
        },
        "requirement_diff_mapping": mapping_payload,
        "lineage_analysis": lineage_scope_payload,
        "warnings": warnings,
    }


def infer_output_format(output_path: str, requested: str) -> str:
    if requested != "auto":
        return requested
    if output_path == "-":
        return "json"
    suffix = Path(output_path).suffix.lower()
    if suffix == ".md":
        return "md"
    return "json"


def render_markdown_report(report: dict[str, Any]) -> str:
    lines: list[str] = []
    req_summary = report.get("requirement_summary", {})
    diff_summary = report.get("diff_summary", {})
    mapping = report.get("requirement_diff_mapping", {})
    lineage = report.get("lineage_analysis", {})

    lines.append(f"# 需求影响分析报告：{req_summary.get('title', '未命名需求')}")
    lines.append("")
    lines.append("## 概览")
    lines.append(f"- 状态：`{report.get('status', 'unknown')}`")
    lines.append(f"- 生成时间：`{report.get('generated_at', '')}`")
    lines.append(f"- 仓库：`{report.get('repo_path', '')}`")
    lines.append(f"- 需求点数量：`{req_summary.get('functional_point_count', 0)}`")
    lines.append(f"- 变更文件数：`{diff_summary.get('changed_file_count', 0)}`")
    lines.append(f"- 映射覆盖率：`{mapping.get('mapped_ratio', 0.0)}`")
    lines.append("")

    lines.append("## 需求点与变更文件映射")
    mapped_items = mapping.get("mapped_items", [])
    if mapped_items:
        for item in mapped_items:
            lines.append(
                f"- `{item.get('requirement_item_id')}` [{item.get('requirement_type')}] {item.get('requirement_text')}"
            )
            for matched in item.get("matched_files", []):
                rationale = "；".join(matched.get("rationale", []))
                lines.append(
                    f"  - `{matched.get('path')}` (confidence={matched.get('confidence')}) {rationale}"
                )
    else:
        lines.append("- 无可用映射。")
    lines.append("")

    unmapped_items = mapping.get("unmapped_requirement_items", [])
    if unmapped_items:
        lines.append("## 未映射需求点")
        for item in unmapped_items:
            lines.append(f"- `{item.get('requirement_item_id')}` {item.get('reason')}")
        lines.append("")

    lines.append("## 最小闭包 DAG")
    must_run_nodes = lineage.get("must_run_nodes", [])
    validation_nodes = lineage.get("validation_nodes", [])
    lines.append(
        "- 必跑节点："
        + (" -> ".join(must_run_nodes) if must_run_nodes else "无")
    )
    lines.append(
        "- 验证节点："
        + (" -> ".join(validation_nodes) if validation_nodes else "无")
    )
    lines.append("")

    lines.append("## 节点与表范围")
    node_scope = lineage.get("node_test_scope", [])
    if not node_scope:
        lines.append("无可用节点范围。")
    for item in node_scope:
        lines.append(f"{item.get('node')}（{item.get('scope')}）")
        lines.append("依赖表：")
        dep_tables = item.get("dependency_tables", [])
        if dep_tables:
            for table in dep_tables:
                lines.append(str(table))
        else:
            lines.append("无")
        lines.append("结果表：")
        res_tables = item.get("result_tables", [])
        if res_tables:
            for table in res_tables:
                lines.append(str(table))
        else:
            lines.append("无")
        lines.append("")

    lines.append("## 重点测试建议")
    suggestions = lineage.get("suggested_checks", [])
    if suggestions:
        for item in suggestions:
            lines.append(f"- {item}")
    else:
        lines.append("- 无")
    lines.append("")

    warnings = report.get("warnings", [])
    if warnings:
        lines.append("## 风险与提示")
        for warning in warnings:
            lines.append(f"- {warning}")
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def write_output(report: dict[str, Any], output_path: str, output_format: str) -> None:
    if output_format == "md":
        rendered = render_markdown_report(report)
    else:
        rendered = json.dumps(report, ensure_ascii=False, indent=2) + "\n"

    if output_path == "-":
        print(rendered, end="")
        return

    target = Path(output_path).expanduser().resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(rendered, encoding="utf-8")


def main() -> int:
    args = parse_args()
    try:
        report = build_report(args)
        output_format = infer_output_format(args.output, args.output_format)
        write_output(report, args.output, output_format)
    except Exception as exc:  # noqa: BLE001 - CLI tools should return full errors.
        print(f"[requirement-impact-analysis] {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
