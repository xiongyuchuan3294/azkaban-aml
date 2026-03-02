#!/usr/bin/env python3
"""
Generic requirement parser for AML big-data testing.

Design goals:
- Parse markdown/text OR structured JSON requirement input
- Extract functional points, I/O fields, conditions, tables, logic, workflow nodes
- Map Chinese shorthand table names to physical table names by configurable strategy
- Persist high-confidence alias mappings for future reuse
- Emit conversion blueprint so AI can reuse script or generate new adapter quickly
"""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
from collections import Counter
from datetime import datetime, timezone
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any


SKILL_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_REPO_PATH = "/Users/xiongyuc/workspace/azkaban-aml"
DEFAULT_OUTPUT_PATH = SKILL_ROOT / "outputs" / "requirement_report.md"
DEFAULT_TABLE_MAP_PATH = SKILL_ROOT / "references" / "table_alias_map.json"
DEFAULT_AUTO_TABLE_MAP_PATH = SKILL_ROOT / "references" / "table_alias_map.auto.json"
DEFAULT_PROFILE_PATH = SKILL_ROOT / "references" / "parser_profile.json"

PHYSICAL_TABLE_PATTERN = re.compile(
    r"\b[a-zA-Z][a-zA-Z0-9_]*\.[a-zA-Z][a-zA-Z0-9_]*\b"
    r"|\b(?:ods|dwd|dws|ads)_[a-z0-9_]+\b",
    re.IGNORECASE,
)
TABLE_REF_SQL_PATTERN = re.compile(
    r"(?i)\b(?:create\s+table(?:\s+if\s+not\s+exists)?|insert\s+into(?:\s+table)?|"
    r"overwrite\s+table|from|join|update)\s+([a-zA-Z_][a-zA-Z0-9_.]*)"
)
ZH_TABLE_MENTION_PATTERN = re.compile(
    r"([A-Za-z]{2,4}\s*[\u4e00-\u9fffA-Za-z0-9_]{1,22}?表|[\u4e00-\u9fffA-Za-z0-9_]{2,24}?表)"
)
RULE_ID_PATTERN = re.compile(r"\bR\d{3,}\b", re.IGNORECASE)
CONDITION_PATTERN = re.compile(r"([A-Za-z_][A-Za-z0-9_]*)\s*(>=|<=|=|>|<)\s*([A-Za-z0-9_.-]+)")
IDENTIFIER_PATTERN = re.compile(r"\b[A-Za-z_][A-Za-z0-9_]*\b")
BACKTICK_PATTERN = re.compile(r"`([^`]+)`")
HEADING_PATTERN = re.compile(r"^#{1,6}\s+(.+)$")

INPUT_CONTRACT = {
    "required": ["title", "raw_text"],
    "optional": ["sections", "business_terms", "metadata", "constraints"],
    "section_item_shape": {"title": "string", "content": "string"},
}

OUTPUT_CONTRACT = {
    "required": [
        "source_document",
        "summary",
        "functional_points",
        "tables",
        "main_logic",
        "workflow_nodes",
        "quality",
        "clarification_questions",
    ],
    "table_mapping_fields": ["mention", "mapped_table", "match_type", "confidence", "evidence"],
    "quality_fields": ["completeness_score", "missing_parts", "hints"],
}

DEFAULT_PROFILE = {
    "functional_keywords": [
        "新增",
        "修改",
        "优化",
        "删除",
        "规则",
        "指标",
        "字段",
        "预警",
        "风险",
        "阈值",
        "条件",
        "输出",
        "证据",
    ],
    "input_keywords": ["输入", "来源", "读取", "命中条件", "条件", "阈值", "依赖字段", "上游"],
    "output_keywords": ["输出", "产出", "写入", "生成", "落地", "证据字段", "风险等级", "下游"],
    "condition_keywords": ["命中条件", "条件", "阈值", ">=", "<=", "==", "判定"],
    "logic_type_keywords": {
        "rule_change": ["规则", "命中", "风险等级", "判定"],
        "metric_change": ["指标", "字段", "口径", "聚合"],
        "output_change": ["输出", "产出", "证据"],
        "table_change": ["表", "分层"],
        "general": [],
    },
    "workflow_rules": [
        {
            "node": "init_ods_data",
            "keywords": ["初始化", "ods", "原始数据"],
            "layers": ["ODS"],
            "base_confidence": 0.62,
        },
        {
            "node": "ods_to_dwd",
            "keywords": ["明细", "清洗", "dwd", "ods"],
            "layers": ["ODS", "DWD"],
            "base_confidence": 0.72,
        },
        {
            "node": "dwd_to_dws",
            "keywords": ["聚合", "统计", "dws"],
            "layers": ["DWS"],
            "base_confidence": 0.84,
        },
        {
            "node": "generate_aml_alert",
            "keywords": ["预警", "规则", "命中", "风险"],
            "layers": ["ADS"],
            "base_confidence": 0.86,
        },
        {
            "node": "check_alert_result",
            "keywords": ["校验", "核对", "证据", "结果"],
            "layers": ["ADS", "DWS"],
            "base_confidence": 0.75,
        },
    ],
}

STOPWORDS = {
    "and",
    "or",
    "if",
    "then",
    "else",
    "true",
    "false",
    "null",
    "select",
    "from",
    "where",
    "group",
    "by",
    "order",
    "insert",
    "update",
    "delete",
    "create",
    "table",
    "into",
    "as",
    "on",
    "in",
    "not",
    "is",
    "case",
    "when",
    "end",
    "count",
    "sum",
    "avg",
    "min",
    "max",
    "high",
    "medium",
    "low",
}

FILE_EXTENSIONS = {"md", "txt", "json", "yaml", "yml", "csv", "xml", "sql"}
TABLE_POSITIVE_HINTS = {"ods", "dwd", "dws", "ads", "交易", "风险", "预警", "账户", "明细", "聚合", "统计"}
TABLE_NEGATIVE_HINTS = {"涉及", "简写", "实际", "转换", "描述", "方案", "需求", "功能"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Parse requirement docs with a generic method and output structured "
            "analysis plus reusable conversion blueprint."
        )
    )
    parser.add_argument("--input", required=True, help="Requirement document path.")
    parser.add_argument(
        "--input-format",
        choices=["auto", "markdown", "text", "json"],
        default="auto",
        help="Input format. Default: auto.",
    )
    parser.add_argument(
        "--profile",
        default=str(DEFAULT_PROFILE_PATH),
        help="Parser profile JSON path for keyword/workflow customization.",
    )
    parser.add_argument(
        "--repo",
        default=DEFAULT_REPO_PATH,
        help=f"Repository path for workflow and table catalog inference. Default: {DEFAULT_REPO_PATH}",
    )
    parser.add_argument(
        "--table-map",
        default=str(DEFAULT_TABLE_MAP_PATH),
        help="Primary table alias map JSON path.",
    )
    parser.add_argument(
        "--auto-map-file",
        default=str(DEFAULT_AUTO_TABLE_MAP_PATH),
        help="Auto-generated table alias map JSON path.",
    )
    parser.add_argument(
        "--match-mode",
        choices=["hybrid", "algorithm", "vector"],
        default="hybrid",
        help="Table matching strategy. Default: hybrid.",
    )
    parser.add_argument(
        "--min-match-confidence",
        type=float,
        default=0.58,
        help="Minimum confidence to accept algorithm/vector table mapping. Default: 0.58.",
    )
    parser.add_argument(
        "--auto-map-min-confidence",
        type=float,
        default=0.8,
        help="Minimum confidence for persisting new alias mapping. Default: 0.8.",
    )
    parser.add_argument(
        "--disable-auto-map-update",
        action="store_true",
        help="Disable auto persistence of new alias mappings.",
    )
    parser.add_argument(
        "--diff-report",
        help="Optional diff-parser output JSON path for workflow refinement.",
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT_PATH),
        help=(
            "Output report path. Default: skills/requirement-parser/outputs/requirement_report.md. "
            "Use '-' for stdout."
        ),
    )
    parser.add_argument(
        "--output-format",
        choices=["auto", "md", "json"],
        default="auto",
        help="Output format. Default: auto (infer by file suffix).",
    )
    return parser.parse_args()


def normalize_text(text: str) -> str:
    return re.sub(r"[^a-z0-9\u4e00-\u9fff]", "", text.lower())


def strip_bullet_prefix(text: str) -> str:
    cleaned = text.strip()
    cleaned = re.sub(r"^[\-\*\+]\s*", "", cleaned)
    cleaned = re.sub(r"^\d+[\.、\)]\s*", "", cleaned)
    return cleaned.strip()


def dedupe_keep_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result


def load_json_file(path: Path, default: Any) -> Any:
    if not path.exists() or not path.is_file():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return default


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = dict(base)
    for key, value in override.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def load_profile(profile_path: str) -> tuple[dict[str, Any], Path | None]:
    profile_file = Path(profile_path).expanduser().resolve()
    payload = load_json_file(profile_file, default=None)
    if not isinstance(payload, dict):
        return DEFAULT_PROFILE, None
    return deep_merge(DEFAULT_PROFILE, payload), profile_file


def infer_input_format(input_path: Path, selected_format: str) -> str:
    if selected_format != "auto":
        return selected_format
    if input_path.suffix.lower() == ".json":
        return "json"
    if input_path.suffix.lower() in {".md", ".markdown"}:
        return "markdown"
    return "text"


def flatten_structured_sections(payload: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    sections = payload.get("sections")
    if isinstance(sections, list):
        for section in sections:
            if not isinstance(section, dict):
                continue
            title = str(section.get("title", "")).strip()
            content = str(section.get("content", "")).strip()
            if title:
                lines.append(f"## {title}")
            if content:
                lines.extend(content.splitlines())
    return lines


def load_input_document(path: str, input_format: str) -> dict[str, Any]:
    input_file = Path(path).expanduser().resolve()
    if not input_file.exists() or not input_file.is_file():
        raise FileNotFoundError(f"input file does not exist: {input_file}")

    resolved_format = infer_input_format(input_file, input_format)
    raw = input_file.read_text(encoding="utf-8")
    if not raw.strip():
        raise ValueError("input document is empty")

    if resolved_format == "json":
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError(f"input JSON decode failed: {exc}") from exc
        if not isinstance(payload, dict):
            raise ValueError("input JSON must be an object")

        title = str(payload.get("title", "")).strip()
        text_candidates: list[str] = []
        for key in ("raw_text", "content", "requirement_text", "description"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                text_candidates.append(value)
        text_candidates.extend(flatten_structured_sections(payload))

        merged_text = "\n".join(item for item in text_candidates if item.strip()).strip()
        if not merged_text:
            raise ValueError("input JSON does not contain usable requirement text")

        return {
            "source_document": str(input_file),
            "input_format": "json",
            "title_hint": title,
            "raw_text": merged_text,
            "metadata": payload.get("metadata", {}),
        }

    return {
        "source_document": str(input_file),
        "input_format": resolved_format,
        "title_hint": "",
        "raw_text": raw,
        "metadata": {},
    }


def collect_lines(raw_text: str) -> list[str]:
    return [line.rstrip() for line in raw_text.splitlines() if line.strip()]


def split_sections(lines: list[str]) -> list[dict[str, Any]]:
    sections: list[dict[str, Any]] = []
    current_title = "全文"
    current_lines: list[str] = []

    for line in lines:
        heading = HEADING_PATTERN.match(line.strip())
        if heading:
            if current_lines:
                sections.append({"title": current_title, "lines": current_lines[:]})
            current_title = heading.group(1).strip() or "未命名章节"
            current_lines = []
            continue
        current_lines.append(line)

    if current_lines:
        sections.append({"title": current_title, "lines": current_lines[:]})

    if not sections:
        sections.append({"title": "全文", "lines": lines[:]})

    return sections


def extract_title(lines: list[str], title_hint: str) -> str:
    if title_hint:
        return title_hint
    for line in lines:
        heading = HEADING_PATTERN.match(line.strip())
        if heading:
            return heading.group(1).strip()
    for line in lines:
        cleaned = strip_bullet_prefix(line)
        if cleaned:
            return cleaned[:80]
    return "未命名需求文档"


def contains_keyword(text: str, keywords: list[str]) -> bool:
    lowered = text.lower()
    return any(keyword.lower() in lowered for keyword in keywords)


def classify_function_type(text: str, profile: dict[str, Any]) -> str:
    logic_keywords = profile.get("logic_type_keywords", {})
    for logic_type, keywords in logic_keywords.items():
        if not keywords:
            continue
        if contains_keyword(text, keywords):
            return logic_type
    return "general"


def extract_functional_points(lines: list[str], profile: dict[str, Any]) -> list[dict[str, str]]:
    keywords: list[str] = profile.get("functional_keywords", [])
    candidates: list[str] = []

    for line in lines:
        clean_line = strip_bullet_prefix(line)
        if not clean_line or clean_line.startswith("#"):
            continue

        score = 0
        if contains_keyword(clean_line, keywords):
            score += 1
        if RULE_ID_PATTERN.search(clean_line):
            score += 1
        if CONDITION_PATTERN.search(clean_line):
            score += 1
        if "表" in clean_line:
            score += 1
        if len(clean_line) < 4:
            score = 0

        if score >= 1:
            candidates.append(clean_line)

    functional_points: list[dict[str, str]] = []
    for index, item in enumerate(dedupe_keep_order(candidates), start=1):
        functional_points.append(
            {
                "id": f"FP{index:03d}",
                "type": classify_function_type(item, profile),
                "text": item,
            }
        )

    return functional_points


def infer_layer(text: str) -> str:
    lowered = text.lower()
    uppered = text.upper()
    if "ods_" in lowered or ".ods_" in lowered or "ODS" in uppered:
        return "ODS"
    if "dwd_" in lowered or ".dwd_" in lowered or "DWD" in uppered:
        return "DWD"
    if "dws_" in lowered or ".dws_" in lowered or "DWS" in uppered:
        return "DWS"
    if "ads_" in lowered or ".ads_" in lowered or "ADS" in uppered:
        return "ADS"
    return "UNKNOWN"


def extract_conditions(lines: list[str], profile: dict[str, Any]) -> list[dict[str, Any]]:
    condition_keywords: list[str] = profile.get("condition_keywords", [])
    conditions: list[dict[str, Any]] = []

    for line in lines:
        clean_line = strip_bullet_prefix(line)
        if not clean_line:
            continue
        has_expression = bool(CONDITION_PATTERN.search(clean_line))
        has_explicit_condition = contains_keyword(clean_line, condition_keywords)
        if not has_expression and not has_explicit_condition:
            continue
        if not has_expression and not any(token in clean_line for token in ("命中条件", "阈值", "条件")):
            continue

        expressions: list[dict[str, str]] = []
        for field, operator, value in CONDITION_PATTERN.findall(clean_line):
            expressions.append(
                {
                    "field": field,
                    "operator": operator,
                    "value": value,
                    "expr": f"{field} {operator} {value}",
                }
            )

        conditions.append(
            {
                "text": clean_line,
                "expressions": expressions,
                "rule_ids": RULE_ID_PATTERN.findall(clean_line),
            }
        )

    return conditions


def extract_identifiers(text: str) -> list[str]:
    tokens: list[str] = []

    for value in BACKTICK_PATTERN.findall(text):
        candidate = value.strip()
        if not candidate:
            continue

        cond_matches = CONDITION_PATTERN.findall(candidate)
        if cond_matches:
            for field, _, _ in cond_matches:
                tokens.append(field)
            continue

        if any(op in candidate for op in (">=", "<=", "=", ">", "<")):
            for item in IDENTIFIER_PATTERN.findall(candidate):
                tokens.append(item)
            continue

        tokens.append(candidate)

    for value in IDENTIFIER_PATTERN.findall(text):
        tokens.append(value)

    normalized_tokens: list[str] = []
    for token in tokens:
        lower = token.lower()
        if lower in STOPWORDS:
            continue
        if len(token) <= 1:
            continue
        if RULE_ID_PATTERN.fullmatch(token):
            continue
        if PHYSICAL_TABLE_PATTERN.fullmatch(token):
            continue
        if token.isupper() and "_" not in token:
            continue
        normalized_tokens.append(token)

    return dedupe_keep_order(normalized_tokens)


def extract_inputs_outputs(
    sections: list[dict[str, Any]],
    conditions: list[dict[str, Any]],
    profile: dict[str, Any],
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    input_keywords: list[str] = profile.get("input_keywords", [])
    output_keywords: list[str] = profile.get("output_keywords", [])

    input_fields: dict[str, dict[str, str]] = {}
    output_fields: dict[str, dict[str, str]] = {}

    def add_input(name: str, evidence: str) -> None:
        if name in input_fields:
            return
        input_fields[name] = {"name": name, "evidence": evidence}

    def add_output(name: str, evidence: str, value: str | None = None) -> None:
        if name in output_fields:
            if value and "value" not in output_fields[name]:
                output_fields[name]["value"] = value
            return
        output_fields[name] = {"name": name, "evidence": evidence}
        if value:
            output_fields[name]["value"] = value

    for condition in conditions:
        evidence = condition["text"]
        for expr in condition.get("expressions", []):
            add_input(expr["field"], evidence)

    for section in sections:
        section_title = section.get("title", "")
        title_hint_input = contains_keyword(section_title, input_keywords)
        title_hint_output = contains_keyword(section_title, output_keywords)

        for line in section.get("lines", []):
            clean_line = strip_bullet_prefix(line)
            if not clean_line:
                continue

            line_has_input = title_hint_input or contains_keyword(clean_line, input_keywords)
            line_has_output = title_hint_output or contains_keyword(clean_line, output_keywords)

            fields = extract_identifiers(clean_line)
            if line_has_input:
                for field in fields:
                    add_input(field, clean_line)

            if line_has_output:
                for field in fields:
                    add_output(field, clean_line)

            if "风险等级" in clean_line:
                risk_value_match = re.search(r"[：:]\s*`?([A-Z_]{2,})`?\b", clean_line)
                risk_value = risk_value_match.group(1) if risk_value_match else None
                add_output("risk_level", clean_line, risk_value)

    return list(input_fields.values()), list(output_fields.values())


def extract_table_mentions(lines: list[str]) -> list[dict[str, str]]:
    mentions: list[dict[str, str]] = []

    for line in lines:
        clean_line = strip_bullet_prefix(line)
        if not clean_line:
            continue

        for table_name in PHYSICAL_TABLE_PATTERN.findall(clean_line):
            if is_probable_table_name(table_name):
                mentions.append({"mention": table_name, "evidence": clean_line})

        for zh_name in ZH_TABLE_MENTION_PATTERN.findall(clean_line):
            candidate = re.sub(r"\s+", " ", zh_name.strip())
            if not candidate:
                continue
            if len(candidate) > 28:
                continue
            if any(flag in candidate for flag in ("图表", "报表", "需求表述")):
                continue
            if not is_plausible_zh_table(candidate):
                continue
            mentions.append({"mention": candidate, "evidence": clean_line})

    unique: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for item in mentions:
        key = (item["mention"], item["evidence"])
        if key in seen:
            continue
        seen.add(key)
        unique.append(item)
    return unique


def is_probable_table_name(value: str) -> bool:
    candidate = value.strip()
    if "/" in candidate or "\\" in candidate:
        return False
    if candidate.count(".") == 1:
        left, right = candidate.split(".", 1)
        if right.lower() in FILE_EXTENSIONS:
            return False
        if not left or not right:
            return False
    return True


def is_plausible_zh_table(value: str) -> bool:
    normalized = normalize_text(value)
    if not normalized:
        return False
    if any(hint in value for hint in TABLE_NEGATIVE_HINTS):
        return False
    if any(hint in value.lower() for hint in TABLE_POSITIVE_HINTS):
        return True
    return value.startswith(("ODS", "DWD", "DWS", "ADS"))


def load_alias_entries(primary_map_path: Path, auto_map_path: Path) -> list[dict[str, str]]:
    entries: list[dict[str, str]] = []

    for source, path in (("manual", primary_map_path), ("auto", auto_map_path)):
        payload = load_json_file(path, default={})
        aliases = payload.get("aliases", []) if isinstance(payload, dict) else []
        for item in aliases:
            if not isinstance(item, dict):
                continue
            alias = str(item.get("alias", "")).strip()
            table = str(item.get("table", "")).strip()
            if not alias or not table:
                continue
            entries.append(
                {
                    "alias": alias,
                    "alias_norm": normalize_text(alias),
                    "table": table,
                    "source": source,
                }
            )

    deduped: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for entry in entries:
        key = (entry["alias_norm"], entry["table"].lower())
        if key in seen:
            continue
        seen.add(key)
        deduped.append(entry)
    return deduped


def extract_repo_table_catalog(repo_path: str) -> list[str]:
    repo = Path(repo_path).expanduser().resolve()
    if not repo.exists() or not repo.is_dir():
        return []

    sql_files = list(repo.rglob("*.sql"))
    tables: list[str] = []

    for sql_file in sql_files:
        try:
            content = sql_file.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        for table in TABLE_REF_SQL_PATTERN.findall(content):
            if table.lower().startswith(("select", "with", "values", "(")):
                continue
            tables.append(table.strip())

    return sorted(set(tables))


def char_jaccard_similarity(left: str, right: str) -> float:
    left_set = set(normalize_text(left))
    right_set = set(normalize_text(right))
    if not left_set or not right_set:
        return 0.0
    return len(left_set & right_set) / len(left_set | right_set)


def char_ngram_vector(text: str, n: int = 2) -> Counter[str]:
    normalized = normalize_text(text)
    if not normalized:
        return Counter()
    if len(normalized) <= n:
        return Counter({normalized: 1})
    grams = [normalized[index : index + n] for index in range(len(normalized) - n + 1)]
    return Counter(grams)


def cosine_similarity(left: Counter[str], right: Counter[str]) -> float:
    if not left or not right:
        return 0.0
    dot = sum(left[item] * right.get(item, 0) for item in left)
    norm_left = math.sqrt(sum(value * value for value in left.values()))
    norm_right = math.sqrt(sum(value * value for value in right.values()))
    if norm_left == 0.0 or norm_right == 0.0:
        return 0.0
    return dot / (norm_left * norm_right)


def algorithm_similarity(mention: str, candidate: str) -> float:
    mention_norm = normalize_text(mention)
    candidate_norm = normalize_text(candidate)
    if not mention_norm or not candidate_norm:
        return 0.0

    ratio = SequenceMatcher(None, mention_norm, candidate_norm).ratio()
    jaccard = char_jaccard_similarity(mention_norm, candidate_norm)
    containment = 1.0 if mention_norm in candidate_norm or candidate_norm in mention_norm else 0.0
    return 0.5 * ratio + 0.35 * jaccard + 0.15 * containment


def vector_similarity(mention: str, candidate: str) -> float:
    mention_vec = char_ngram_vector(mention, n=2)
    candidate_vec = char_ngram_vector(candidate, n=2)
    return cosine_similarity(mention_vec, candidate_vec)


def build_candidate_pool(alias_entries: list[dict[str, str]], catalog_tables: list[str]) -> list[dict[str, str]]:
    candidates: list[dict[str, str]] = []

    for entry in alias_entries:
        candidates.append(
            {
                "table": entry["table"],
                "match_text": entry["alias"],
                "source": f"alias_{entry['source']}",
            }
        )

    for table in catalog_tables:
        candidates.append({"table": table, "match_text": table, "source": "repo_catalog"})
        short = table.split(".")[-1]
        if short != table:
            candidates.append({"table": table, "match_text": short, "source": "repo_catalog_short"})

    deduped: list[dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    for item in candidates:
        key = (item["table"].lower(), normalize_text(item["match_text"]), item["source"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)

    return deduped


def map_one_mention(
    mention: str,
    alias_entries: list[dict[str, str]],
    candidate_pool: list[dict[str, str]],
    mode: str,
    min_confidence: float,
) -> tuple[str | None, str, float]:
    direct = PHYSICAL_TABLE_PATTERN.search(mention)
    if direct:
        return direct.group(0), "direct_table_name", 1.0

    mention_norm = normalize_text(mention)
    if not mention_norm:
        return None, "unmapped", 0.0

    for entry in alias_entries:
        if mention_norm == entry["alias_norm"]:
            return entry["table"], f"alias_exact_{entry['source']}", 0.96

    for entry in alias_entries:
        alias_norm = entry["alias_norm"]
        if alias_norm and (alias_norm in mention_norm or mention_norm in alias_norm):
            return entry["table"], f"alias_substring_{entry['source']}", 0.86

    mention_layer = infer_layer(mention)
    best: dict[str, Any] | None = None
    best_score = 0.0

    for candidate in candidate_pool:
        candidate_name = candidate["match_text"]
        algo_score = algorithm_similarity(mention, candidate_name)
        vec_score = vector_similarity(mention, candidate_name)

        if mode == "algorithm":
            score = algo_score
        elif mode == "vector":
            score = vec_score
        else:
            score = max(algo_score, 0.92 * vec_score)

        candidate_layer = infer_layer(candidate["table"])
        if mention_layer != "UNKNOWN" and mention_layer == candidate_layer:
            score += 0.08

        if candidate["source"].startswith("alias_"):
            score += 0.03

        score = min(score, 0.99)
        if score > best_score:
            best_score = score
            best = {
                "table": candidate["table"],
                "source": candidate["source"],
                "algo_score": algo_score,
                "vec_score": vec_score,
            }

    if not best or best_score < min_confidence:
        return None, "unmapped", round(best_score, 2)

    return best["table"], f"{mode}_{best['source']}", round(best_score, 2)


def extract_tables(
    lines: list[str],
    alias_entries: list[dict[str, str]],
    repo_tables: list[str],
    mode: str,
    min_confidence: float,
) -> list[dict[str, Any]]:
    mentions = extract_table_mentions(lines)
    candidate_pool = build_candidate_pool(alias_entries, repo_tables)

    mapped: list[dict[str, Any]] = []
    for item in mentions:
        mention = item["mention"]
        mapped_table, match_type, confidence = map_one_mention(
            mention=mention,
            alias_entries=alias_entries,
            candidate_pool=candidate_pool,
            mode=mode,
            min_confidence=min_confidence,
        )
        layer_source = mapped_table or mention
        mapped.append(
            {
                "mention": mention,
                "mapped_table": mapped_table,
                "match_type": match_type,
                "confidence": round(float(confidence), 2),
                "layer": infer_layer(layer_source),
                "evidence": item["evidence"],
            }
        )

    deduped: list[dict[str, Any]] = []
    seen: set[tuple[str, str | None]] = set()
    for item in mapped:
        key = (normalize_text(item["mention"]), item["mapped_table"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)

    deduped.sort(key=lambda value: (-value["confidence"], value["mention"]))
    return deduped


def load_repo_nodes(repo_path: str | None) -> set[str]:
    if not repo_path:
        return set()
    repo = Path(repo_path).expanduser().resolve()
    jobs_dir = repo / "jobs"
    if not jobs_dir.exists() or not jobs_dir.is_dir():
        return set()
    nodes = {job_file.stem for job_file in jobs_dir.glob("*.job")}
    return {node for node in nodes if node}


def load_diff_nodes(diff_report_path: str | None) -> set[str]:
    if not diff_report_path:
        return set()
    path = Path(diff_report_path).expanduser().resolve()
    payload = load_json_file(path, default={})
    if not isinstance(payload, dict):
        return set()

    nodes: set[str] = set()
    for commit in payload.get("commits", []):
        if not isinstance(commit, dict):
            continue
        for changed_file in commit.get("changed_files", []):
            if not isinstance(changed_file, dict):
                continue
            file_path = str(changed_file.get("path", ""))
            match = re.search(r"(?:^|/)jobs/([^/]+)\.job$", file_path)
            if match:
                nodes.add(match.group(1))
    return nodes


def infer_workflow_nodes(
    lines: list[str],
    tables: list[dict[str, Any]],
    profile: dict[str, Any],
    repo_nodes: set[str],
    diff_nodes: set[str],
) -> list[dict[str, Any]]:
    text_blob = "\n".join(lines)
    layers = {item.get("layer", "UNKNOWN") for item in tables}

    candidates: dict[str, dict[str, Any]] = {}

    def add_candidate(node: str, confidence: float, reason: str) -> None:
        if node not in candidates:
            candidates[node] = {"confidence": confidence, "reasons": [reason]}
            return
        candidates[node]["confidence"] = max(candidates[node]["confidence"], confidence)
        if reason not in candidates[node]["reasons"]:
            candidates[node]["reasons"].append(reason)

    workflow_rules = profile.get("workflow_rules", [])
    for rule in workflow_rules:
        if not isinstance(rule, dict):
            continue
        node = str(rule.get("node", "")).strip()
        if not node:
            continue
        keywords = [str(item) for item in rule.get("keywords", []) if str(item).strip()]
        rule_layers = {str(item).upper() for item in rule.get("layers", []) if str(item).strip()}
        base_confidence = float(rule.get("base_confidence", 0.7))

        layer_hit = bool(rule_layers & layers)
        keyword_hit = any(keyword.lower() in text_blob.lower() for keyword in keywords)

        if not layer_hit and not keyword_hit:
            continue

        confidence = base_confidence
        reasons: list[str] = []
        if layer_hit:
            confidence += 0.08
            reasons.append(f"需求涉及层级 {','.join(sorted(rule_layers & layers))}")
        if keyword_hit:
            confidence += 0.06
            reasons.append("需求关键词命中该节点规则")

        add_candidate(node, min(confidence, 0.95), "；".join(reasons) if reasons else "规则命中")

    for node in diff_nodes:
        add_candidate(node, 0.97, "代码改动中直接命中 jobs 节点")

    workflow_nodes: list[dict[str, Any]] = []
    for node, data in candidates.items():
        confirmed = node in repo_nodes
        confidence = float(data["confidence"]) + (0.04 if confirmed else 0.0)
        workflow_nodes.append(
            {
                "node": node,
                "confidence": round(min(confidence, 0.99), 2),
                "status": "confirmed" if confirmed else "inferred",
                "reason": "；".join(data["reasons"]),
            }
        )

    workflow_nodes.sort(key=lambda item: (-item["confidence"], item["node"]))
    return workflow_nodes


def build_main_logic(
    functional_points: list[dict[str, str]],
    conditions: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    steps: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()

    def add_step(step_type: str, detail: str, evidence: str, rule_id: str | None = None) -> None:
        key = (step_type, detail)
        if key in seen:
            return
        seen.add(key)
        item: dict[str, Any] = {
            "step": len(steps) + 1,
            "type": step_type,
            "detail": detail,
            "evidence": evidence,
        }
        if rule_id:
            item["rule_id"] = rule_id
        steps.append(item)

    for point in functional_points:
        rule_ids = RULE_ID_PATTERN.findall(point["text"])
        add_step(
            step_type=point.get("type", "general"),
            detail=point["text"],
            evidence=point["text"],
            rule_id=rule_ids[0] if rule_ids else None,
        )

    for condition in conditions:
        expressions = [expr["expr"] for expr in condition.get("expressions", [])]
        detail = " 且 ".join(expressions) if expressions else condition["text"]
        add_step(
            step_type="condition",
            detail=detail,
            evidence=condition["text"],
            rule_id=condition.get("rule_ids", [None])[0] if condition.get("rule_ids") else None,
        )

    steps.sort(key=lambda item: item["step"])
    for index, item in enumerate(steps, start=1):
        item["step"] = index
    return steps


def build_quality(
    functional_points: list[dict[str, Any]],
    tables: list[dict[str, Any]],
    workflow_nodes: list[dict[str, Any]],
    main_logic: list[dict[str, Any]],
) -> tuple[dict[str, Any], list[str]]:
    checks = {
        "functional_points": bool(functional_points),
        "tables": bool(tables),
        "main_logic": bool(main_logic),
        "workflow_nodes": bool(workflow_nodes),
    }

    passed = sum(1 for value in checks.values() if value)
    completeness_score = round(passed / len(checks), 2)
    missing_parts = [name for name, ok in checks.items() if not ok]

    hints: list[str] = []
    if not checks["functional_points"]:
        hints.append("未提取到明确功能点，建议补充新增/修改/规则等动作描述。")
    if not checks["tables"]:
        hints.append("未识别到表名，请补充中文表名或物理表名（库名.表名）。")
    if not checks["main_logic"]:
        hints.append("主逻辑信息不足，建议补充规则处理流程。")
    if not checks["workflow_nodes"]:
        hints.append("未推断到工作流节点，请补充 Azkaban 节点名或链路。")

    low_conf_tables = [item for item in tables if item.get("confidence", 0.0) < 0.7]
    if low_conf_tables:
        preview = "、".join(item["mention"] for item in low_conf_tables[:3])
        hints.append(f"表名映射置信度偏低，请确认：{preview}")

    questions: list[str] = []
    if not checks["tables"] or low_conf_tables:
        questions.append("请确认需求涉及的物理表名（至少提供库名.表名或标准缩写）。")
    if not checks["workflow_nodes"]:
        questions.append("请补充可能涉及的 Azkaban 节点（如 dwd_to_dws、generate_aml_alert）。")

    return {
        "completeness_score": completeness_score,
        "missing_parts": missing_parts,
        "hints": hints[:8],
    }, questions


def persist_auto_alias_mappings(
    auto_map_path: Path,
    alias_entries: list[dict[str, str]],
    table_mappings: list[dict[str, Any]],
    source_document: str,
    min_confidence: float,
    enabled: bool,
) -> dict[str, Any]:
    if not enabled:
        return {
            "enabled": False,
            "auto_map_file": str(auto_map_path),
            "new_aliases": 0,
            "updated": False,
        }

    payload = load_json_file(auto_map_path, default={})
    if not isinstance(payload, dict):
        payload = {}
    aliases = payload.get("aliases")
    if not isinstance(aliases, list):
        aliases = []

    existing_keys: set[tuple[str, str]] = set()
    for entry in alias_entries:
        existing_keys.add((entry["alias_norm"], entry["table"].lower()))
    for item in aliases:
        if not isinstance(item, dict):
            continue
        alias = str(item.get("alias", "")).strip()
        table = str(item.get("table", "")).strip()
        if alias and table:
            existing_keys.add((normalize_text(alias), table.lower()))

    new_aliases: list[dict[str, Any]] = []
    now = datetime.now(timezone.utc).isoformat()

    for mapping in table_mappings:
        mention = str(mapping.get("mention", "")).strip()
        mapped_table = mapping.get("mapped_table")
        confidence = float(mapping.get("confidence", 0.0))
        match_type = str(mapping.get("match_type", ""))

        if not mention or not mapped_table:
            continue
        if PHYSICAL_TABLE_PATTERN.search(mention):
            continue
        if confidence < min_confidence:
            continue
        if match_type in {"direct_table_name", "unmapped"}:
            continue

        key = (normalize_text(mention), str(mapped_table).lower())
        if key in existing_keys:
            continue

        new_alias = {
            "alias": mention,
            "table": mapped_table,
            "confidence": round(confidence, 2),
            "strategy": match_type,
            "evidence": mapping.get("evidence", ""),
            "source_document": source_document,
            "created_at": now,
        }
        new_aliases.append(new_alias)
        existing_keys.add(key)

    if new_aliases:
        aliases.extend(new_aliases)
        payload["aliases"] = aliases
        auto_map_path.parent.mkdir(parents=True, exist_ok=True)
        auto_map_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    return {
        "enabled": True,
        "auto_map_file": str(auto_map_path),
        "new_aliases": len(new_aliases),
        "updated": bool(new_aliases),
    }


def build_conversion_blueprint(profile_path: str, match_mode: str) -> dict[str, Any]:
    return {
        "thinking_framework": [
            "先把需求统一成结构化输入，再做字段、逻辑、表、节点四类抽取。",
            "表名映射先走显式映射，再走算法/向量相似度，最后人工确认低置信项。",
            "尽量通过 profile 调参而不是改代码，实现跨需求复用。",
            "对低完整度需求输出引导问题，保证可迭代补充。",
        ],
        "structured_input_contract": INPUT_CONTRACT,
        "structured_output_contract": OUTPUT_CONTRACT,
        "script_reuse_policy": {
            "reuse_when": [
                "目标是提取功能点、逻辑、表映射、工作流节点",
                "需求文档是 markdown/text/json，且业务域为数仓/AML 类规则"
            ],
            "generate_new_adapter_when": [
                "目标输出字段与本脚本输出契约差异较大",
                "行业术语或文档格式差异导致 profile 调参后仍无法满足"
            ],
            "preferred_action_order": [
                "1. 更新 profile",
                "2. 补充表别名映射",
                "3. 必要时生成新的 adapter 脚本"
            ],
        },
        "profile_path": profile_path,
        "table_match_mode": match_mode,
        "table_mapping_persistence": {
            "description": "高置信映射自动沉淀到 auto map 文件，供后续文档复用。",
            "manual_review_needed": "对低置信映射和关键核心表进行人工复核。",
        },
    }


def build_summary(
    functional_points: list[dict[str, Any]],
    tables: list[dict[str, Any]],
    main_logic: list[dict[str, Any]],
    workflow_nodes: list[dict[str, Any]],
    mapping_update: dict[str, Any],
) -> str:
    high_conf_tables = sum(1 for item in tables if item.get("confidence", 0.0) >= 0.8)
    auto_added = int(mapping_update.get("new_aliases", 0))
    return (
        f"提取到 {len(functional_points)} 个功能点，识别 {len(tables)} 张候选表"
        f"（高置信 {high_conf_tables} 张），生成 {len(main_logic)} 个逻辑步骤，"
        f"推断 {len(workflow_nodes)} 个工作流节点，自动沉淀 {auto_added} 条新表映射。"
    )


def build_report(args: argparse.Namespace) -> dict[str, Any]:
    input_document = load_input_document(args.input, args.input_format)
    lines = collect_lines(input_document["raw_text"])
    sections = split_sections(lines)

    profile, resolved_profile_path = load_profile(args.profile)
    title = extract_title(lines, input_document.get("title_hint", ""))

    primary_map_path = Path(args.table_map).expanduser().resolve()
    auto_map_path = Path(args.auto_map_file).expanduser().resolve()
    alias_entries = load_alias_entries(primary_map_path, auto_map_path)
    repo_tables = extract_repo_table_catalog(args.repo)

    functional_points = extract_functional_points(lines, profile)
    conditions = extract_conditions(lines, profile)
    inputs: list[dict[str, Any]] = []
    outputs: list[dict[str, Any]] = []
    tables = extract_tables(
        lines=lines,
        alias_entries=alias_entries,
        repo_tables=repo_tables,
        mode=args.match_mode,
        min_confidence=args.min_match_confidence,
    )

    repo_nodes = load_repo_nodes(args.repo)
    diff_nodes = load_diff_nodes(args.diff_report)
    workflow_nodes = infer_workflow_nodes(lines, tables, profile, repo_nodes, diff_nodes)
    main_logic = build_main_logic(functional_points, conditions)

    mapping_update = persist_auto_alias_mappings(
        auto_map_path=auto_map_path,
        alias_entries=alias_entries,
        table_mappings=tables,
        source_document=input_document["source_document"],
        min_confidence=args.auto_map_min_confidence,
        enabled=not args.disable_auto_map_update,
    )

    quality, clarification_questions = build_quality(
        functional_points=functional_points,
        tables=tables,
        workflow_nodes=workflow_nodes,
        main_logic=main_logic,
    )

    profile_path_used = str(resolved_profile_path) if resolved_profile_path else "built-in-default"

    report = {
        "source_document": input_document["source_document"],
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "document": {
            "title": title,
            "line_count": len(lines),
            "section_count": len(sections),
            "input_format": input_document.get("input_format", "unknown"),
        },
        "runtime": {
            "profile_path": profile_path_used,
            "repo_path": str(Path(args.repo).expanduser().resolve()),
            "table_match_mode": args.match_mode,
            "min_match_confidence": args.min_match_confidence,
            "auto_map_min_confidence": args.auto_map_min_confidence,
        },
        "functional_points": functional_points,
        "inputs": inputs,
        "outputs": outputs,
        "tables": tables,
        "conditions": conditions,
        "main_logic": main_logic,
        "workflow_nodes": workflow_nodes,
        "quality": quality,
        "clarification_questions": clarification_questions,
        "mapping_update": mapping_update,
        "conversion_blueprint": build_conversion_blueprint(profile_path_used, args.match_mode),
    }

    report["summary"] = build_summary(
        functional_points=functional_points,
        tables=tables,
        main_logic=main_logic,
        workflow_nodes=workflow_nodes,
        mapping_update=mapping_update,
    )

    return report


def md_escape(value: Any) -> str:
    text = str(value)
    return text.replace("|", "\\|").replace("\n", "<br>")


def render_markdown_table(headers: list[str], rows: list[list[Any]]) -> str:
    if not rows:
        return ""
    header_line = "| " + " | ".join(headers) + " |\n"
    splitter = "| " + " | ".join(["---"] * len(headers)) + " |\n"
    body = ""
    for row in rows:
        body += "| " + " | ".join(md_escape(item) for item in row) + " |\n"
    return header_line + splitter + body


def dedupe_keep_order_str(items: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result


def format_table_display(item: dict[str, Any]) -> str:
    mention = str(item.get("mention", "")).strip()
    mapped_table = str(item.get("mapped_table", "")).strip()
    return mapped_table or mention


def collect_tables_by_layer(tables: list[dict[str, Any]]) -> tuple[dict[str, list[str]], list[str]]:
    by_layer: dict[str, list[str]] = {
        "ODS": [],
        "DWD": [],
        "DWS": [],
        "ADS": [],
        "UNKNOWN": [],
    }

    all_tables: list[str] = []
    for item in tables:
        display = format_table_display(item)
        if not display:
            continue
        layer = str(item.get("layer", "UNKNOWN")).upper()
        if layer not in by_layer:
            layer = "UNKNOWN"
        by_layer[layer].append(display)
        all_tables.append(display)

    for layer in list(by_layer.keys()):
        by_layer[layer] = dedupe_keep_order_str(by_layer[layer])
    all_tables = dedupe_keep_order_str(all_tables)
    return by_layer, all_tables


def normalize_logic_text(text: str) -> str:
    lowered = text.lower().strip()
    lowered = re.sub(r"\s+", "", lowered)
    lowered = re.sub(r"[`'\"，。；：:,.!?！？()（）\[\]【】<>《》-]", "", lowered)
    return lowered


def build_merged_logic_items(
    functional_points: list[dict[str, Any]],
    main_logic: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    seen_norm_text: set[str] = set()

    for point in functional_points:
        text = str(point.get("text", "")).strip()
        if not text:
            continue
        norm_text = normalize_logic_text(text)
        if norm_text in seen_norm_text:
            continue
        seen_norm_text.add(norm_text)
        merged.append(
            {
                "id": str(point.get("id", "")),
                "type": str(point.get("type", "")),
                "text": text,
                "source": "functional_point",
            }
        )

    for index, item in enumerate(main_logic, start=1):
        detail = str(item.get("detail", "")).strip()
        if not detail:
            continue
        norm_detail = normalize_logic_text(detail)
        if norm_detail in seen_norm_text:
            continue
        seen_norm_text.add(norm_detail)
        merged.append(
            {
                "id": f"LG{index:03d}",
                "type": str(item.get("type", "")),
                "text": detail,
                "source": "main_logic",
            }
        )

    return merged


def collect_candidate_nodes(workflow_nodes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in workflow_nodes:
        node = str(item.get("node", "")).strip()
        if not node or node in seen:
            continue
        seen.add(node)
        candidates.append(
            {
                "node": node,
                "confidence": item.get("confidence"),
                "reason": str(item.get("reason", "")).strip(),
            }
        )
    return candidates


def collect_candidate_tables(tables: list[dict[str, Any]]) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in tables:
        display = format_table_display(item)
        if not display or display in seen:
            continue
        seen.add(display)
        candidates.append(
            {
                "table": display,
                "layer": str(item.get("layer", "UNKNOWN")).upper(),
                "confidence": item.get("confidence"),
                "match_type": str(item.get("match_type", "")).strip(),
                "mention": str(item.get("mention", "")).strip(),
            }
        )
    return candidates


def render_markdown_report(report: dict[str, Any]) -> str:
    lines: list[str] = []
    document = report.get("document", {})
    runtime = report.get("runtime", {})
    quality = report.get("quality", {})

    lines.append(f"# 需求解析报告：{document.get('title', '未命名需求')}")
    lines.append("")
    lines.append("## 概览")
    lines.append(f"- 来源文档：`{report.get('source_document', '')}`")
    lines.append(f"- 生成时间：`{report.get('generated_at', '')}`")
    lines.append(f"- 摘要：{report.get('summary', '')}")
    lines.append(f"- 输入格式：`{document.get('input_format', '')}`")
    lines.append(f"- 匹配模式：`{runtime.get('table_match_mode', '')}`")
    lines.append("")

    lines.append("## 功能点与主逻辑")
    functional_points = report.get("functional_points", [])
    merged_logic_items = build_merged_logic_items(
        functional_points=functional_points,
        main_logic=report.get("main_logic", []),
    )
    if merged_logic_items:
        for item in merged_logic_items:
            source = str(item.get("source", ""))
            if source == "functional_point":
                source_label = "功能点"
            elif source == "main_logic":
                source_label = "主逻辑补充"
            else:
                source_label = "补充信息"
            lines.append(
                f"- `{item.get('id', '')}` [{item.get('type', '')}] [{source_label}] {item.get('text', '')}"
            )
    else:
        lines.append("- 无")
    lines.append("")

    lines.append("## 节点与表信息（待血缘确认）")
    lines.append("- 说明：当前阶段仅基于需求文本推断候选节点与候选表，节点和表的精确关联需在血缘分析阶段确认。")
    lines.append("")

    lines.append("### 候选节点")
    node_candidates = collect_candidate_nodes(report.get("workflow_nodes", []))
    if node_candidates:
        for node_item in node_candidates:
            details: list[str] = []
            confidence = node_item.get("confidence")
            if confidence is not None and confidence != "":
                details.append(f"置信度={confidence}")
            reason = node_item.get("reason", "")
            if reason:
                details.append(f"依据={reason}")
            suffix = f"（{'；'.join(details)}）" if details else ""
            lines.append(f"- `{node_item.get('node', '')}`{suffix}")
    else:
        lines.append("- 无")
    lines.append("")

    lines.append("### 候选表")
    table_candidates = collect_candidate_tables(report.get("tables", []))
    if table_candidates:
        by_layer, _ = collect_tables_by_layer(report.get("tables", []))
        layer_stats = []
        for layer in ("ODS", "DWD", "DWS", "ADS", "UNKNOWN"):
            count = len(by_layer.get(layer, []))
            if count <= 0:
                continue
            layer_name = "UNKNOWN" if layer == "UNKNOWN" else layer
            layer_stats.append(f"{layer_name}={count}")
        if layer_stats:
            lines.append(f"- 分层统计：{', '.join(layer_stats)}")
        table_rows = []
        for item in table_candidates:
            confidence = item.get("confidence")
            confidence_text = (
                f"{float(confidence):.2f}"
                if isinstance(confidence, (float, int))
                else str(confidence or "")
            )
            table_rows.append(
                [
                    item.get("table", ""),
                    item.get("layer", "UNKNOWN"),
                    item.get("match_type", ""),
                    confidence_text,
                    item.get("mention", ""),
                ]
            )
        lines.append(render_markdown_table(["表名", "层级", "匹配方式", "置信度", "原始提及"], table_rows).rstrip())
    else:
        lines.append("- 无")
    lines.append("")

    lines.append("## 质量评估")
    lines.append(f"- 完整度：`{quality.get('completeness_score', '')}`")
    missing_parts = quality.get("missing_parts", [])
    lines.append(f"- 缺失项：{', '.join(missing_parts) if missing_parts else '无'}")
    hints = quality.get("hints", [])
    if hints:
        lines.append("- 建议：")
        for hint in hints:
            lines.append(f"  - {hint}")
    lines.append("")

    lines.append("## 澄清问题")
    questions = report.get("clarification_questions", [])
    if questions:
        for index, question in enumerate(questions, start=1):
            lines.append(f"{index}. {question}")
    else:
        lines.append("- 无")
    lines.append("")

    mapping_update = report.get("mapping_update", {})
    if mapping_update:
        lines.append("## 映射沉淀")
        lines.append(f"- 自动更新：`{mapping_update.get('enabled', False)}`")
        lines.append(f"- 新增映射：`{mapping_update.get('new_aliases', 0)}`")
        lines.append(f"- 映射文件：`{mapping_update.get('auto_map_file', '')}`")
        lines.append("")

    lines.append("## 转换蓝图")
    blueprint = report.get("conversion_blueprint", {})
    if blueprint:
        lines.append("- 可复用策略：")
        policy = blueprint.get("script_reuse_policy", {})
        for item in policy.get("preferred_action_order", []):
            lines.append(f"  - {item}")
        lines.append("- 输入契约：")
        lines.append(f"  - 必填：`{', '.join(blueprint.get('structured_input_contract', {}).get('required', []))}`")
        lines.append("- 输出契约：")
        lines.append(f"  - 必填：`{', '.join(blueprint.get('structured_output_contract', {}).get('required', []))}`")
    else:
        lines.append("- 无")
    lines.append("")

    return "\n".join(lines).strip() + "\n"


def resolve_output_format(output_path: str, output_format: str) -> str:
    if output_format != "auto":
        return output_format
    if output_path == "-":
        return "md"
    suffix = Path(output_path).suffix.lower()
    if suffix == ".json":
        return "json"
    if suffix in {".md", ".markdown"}:
        return "md"
    return "md"


def write_output(report: dict[str, Any], output_path: str, output_format: str) -> None:
    final_format = resolve_output_format(output_path, output_format)
    if final_format == "json":
        rendered = json.dumps(report, ensure_ascii=False, indent=2) + "\n"
    else:
        rendered = render_markdown_report(report)

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
        write_output(report, args.output, args.output_format)
    except Exception as exc:  # noqa: BLE001
        print(f"[requirement-parser] {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
