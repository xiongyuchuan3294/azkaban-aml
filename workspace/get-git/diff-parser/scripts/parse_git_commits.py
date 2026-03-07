#!/usr/bin/env python3
"""
Parse Git commits into structured JSON for AML/big-data testing impact analysis.

Supported filters:
- Commit subject keywords
- Author
- Date range
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ENTRY_DELIMITER = "\x1e"
FIELD_DELIMITER = "\x1f"

STATUS_MAP = {
    "A": "added",
    "M": "modified",
    "D": "deleted",
    "R": "renamed",
    "C": "copied",
    "T": "type_changed",
    "U": "unmerged",
}

SUMMARY_ACTION = {
    "added": "added",
    "modified": "modified",
    "deleted": "deleted",
    "renamed": "renamed",
    "copied": "copied",
    "type_changed": "type changed",
    "unmerged": "left unmerged",
}

HUNK_RE = re.compile(
    r"^@@ -(?P<old_start>\d+)(?:,(?P<old_count>\d+))? "
    r"\+(?P<new_start>\d+)(?:,(?P<new_count>\d+))? @@ ?(?P<context>.*)$"
)

MAX_HUNKS_PER_FILE = 20
MAX_CHANGED_LINES_PER_FILE = 80

AML_KEYWORDS = (
    "aml",
    "anti-money",
    "anti_money",
    "suspicious",
    "blacklist",
    "sanction",
    "risk",
    "threshold",
    "rule-engine",
    "alert",
)

DEFAULT_OUTPUT_PATH = (
    Path(__file__).resolve().parent.parent / "outputs" / "commit_report.json"
)
DEFAULT_FOCUS_FILE_PATH = Path(__file__).resolve().parent.parent / "focus_file.txt"
DEFAULT_CONTENT_SIZE_LIMIT = 200


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Filter commits by keyword/date/author and output a compact report "
            "with changed file paths and concise summaries."
        )
    )
    parser.add_argument(
        "--repo",
        default="/Users/xiongyuc/workspace/azkaban-aml",
        help="Target git repository path.",
    )
    parser.add_argument(
        "--keyword",
        action="append",
        default=[],
        help="Commit message keyword filter. Repeat for multiple keywords.",
    )
    parser.add_argument(
        "--all-keywords",
        action="store_true",
        help="Require all keywords to match one commit message.",
    )
    parser.add_argument(
        "--author",
        action="append",
        default=[],
        help=(
            "Commit author filter (same semantics as git log --author). "
            "Repeat this option for multiple authors, or use comma-separated values."
        ),
    )
    parser.add_argument("--since", help="Start date in YYYY-MM-DD.")
    parser.add_argument("--until", help="End date in YYYY-MM-DD.")
    parser.add_argument(
        "--max-count",
        type=int,
        default=50,
        help="Maximum number of commits to parse.",
    )
    parser.add_argument(
        "--branch",
        default="HEAD",
        help="Revision range to parse, e.g. HEAD, main, main..feature.",
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT_PATH),
        help=(
            "Write compact JSON to a file path. "
            "Default: diff-parser/outputs/commit_report.json. "
            "Use '-' to print to stdout."
        ),
    )
    parser.add_argument(
        "--markdown-output",
        help=(
            "Write Markdown report to a file path. "
            "If omitted and --output is a file path, writes to sibling .md file."
        ),
    )
    parser.add_argument(
        "--commit-id-output",
        help=(
            "Write filtered commit overview in Markdown format. "
            "If omitted and --output is a file path, writes to sibling "
            "*_commit_ids.md file."
        ),
    )
    parser.add_argument(
        "--include-file-content",
        action="store_true",
        help=(
            "Include full changed file content in output. "
            "Adds content_before/content_after for each changed file when available."
        ),
    )
    parser.add_argument(
        "--max-files-with-content",
        type=int,
        default=0,
        help=(
            "When --include-file-content is enabled, limit how many changed files "
            "per commit include full content. 0 means no limit."
        ),
    )
    parser.add_argument(
        "--content-size-limit",
        type=int,
        default=DEFAULT_CONTENT_SIZE_LIMIT,
        help=(
            "When --include-file-content is enabled, skip content output if "
            "content_before/content_after exceeds this character limit. "
            f"Default: {DEFAULT_CONTENT_SIZE_LIMIT}. 0 means no limit."
        ),
    )
    parser.add_argument(
        "--include-job-files",
        action="store_true",
        help=(
            "Only include changed files ending with .job. "
            "Can be combined with --include-file-name."
        ),
    )
    parser.add_argument(
        "--include-file-name",
        action="append",
        default=[],
        nargs="?",
        const=str(DEFAULT_FOCUS_FILE_PATH),
        help=(
            "Include changed files by exact file name, relative path, or absolute path. "
            "Repeat for multiple targets. If provided without a value, defaults to "
            "diff-parser/focus_file.txt. If a value is a .txt file path, "
            "load file names from that file (one per line)."
        ),
    )
    return parser.parse_args()


def assert_date(date_text: str | None, arg_name: str) -> None:
    if not date_text:
        return
    try:
        datetime.strptime(date_text, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(f"{arg_name} must be YYYY-MM-DD, got: {date_text}") from exc


def run_git(repo_path: str, git_args: list[str]) -> str:
    command = ["git", "-C", repo_path, "-c", "core.quotePath=false", *git_args]
    completed = subprocess.run(command, capture_output=True, text=True, check=False)
    if completed.returncode != 0:
        stderr = completed.stderr.strip() or completed.stdout.strip()
        raise RuntimeError(f"git command failed: {' '.join(command)}\n{stderr}")
    return completed.stdout


def run_git_bytes(repo_path: str, git_args: list[str]) -> subprocess.CompletedProcess[bytes]:
    command = ["git", "-C", repo_path, "-c", "core.quotePath=false", *git_args]
    return subprocess.run(command, capture_output=True, text=False, check=False)


def build_log_args(args: argparse.Namespace) -> list[str]:
    cmd = [
        "log",
        "--date=iso-strict",
        f"--max-count={args.max_count}",
        f"--pretty=format:%H{FIELD_DELIMITER}%an{FIELD_DELIMITER}%ae"
        f"{FIELD_DELIMITER}%ad{FIELD_DELIMITER}%s{ENTRY_DELIMITER}",
    ]

    for author in parse_author_selectors(args.author):
        cmd.append(f"--author={author}")
    if args.since:
        cmd.append(f"--since={args.since}")
    if args.until:
        cmd.append(f"--until={args.until}")
    for keyword in args.keyword:
        if keyword.strip():
            cmd.append(f"--grep={keyword}")
    if args.keyword and args.all_keywords:
        cmd.append("--all-match")

    cmd.append(args.branch)
    return cmd


def parse_author_selectors(author_args: list[str]) -> list[str]:
    parsed: list[str] = []
    seen: set[str] = set()
    for raw_author in author_args:
        for candidate in raw_author.split(","):
            author = candidate.strip()
            if not author or author in seen:
                continue
            seen.add(author)
            parsed.append(author)
    return parsed


def parse_log_entries(raw_log: str) -> list[dict[str, Any]]:
    commits: list[dict[str, Any]] = []
    for chunk in raw_log.strip("\n").strip(ENTRY_DELIMITER).split(ENTRY_DELIMITER):
        entry = chunk.strip()
        if not entry:
            continue
        fields = entry.split(FIELD_DELIMITER)
        if len(fields) < 5:
            continue
        commit_id, author, author_email, date, subject = fields[:5]
        commits.append(
            {
                "commit_id": commit_id,
                "author": author,
                "author_email": author_email,
                "date": date,
                "subject": subject,
            }
        )
    return commits


def append_unique(items: list[str], value: str, limit: int) -> None:
    if not value or value in items or len(items) >= limit:
        return
    items.append(value)


def append_with_limit(items: list[str], value: str, limit: int) -> bool:
    if not value:
        return False
    if len(items) >= limit:
        return True
    items.append(value)
    return False


def normalize_rename_path(path_text: str) -> str:
    if " => " not in path_text:
        return path_text

    brace_match = re.search(r"\{([^{}]+ => [^{}]+)\}", path_text)
    if brace_match:
        _, new_part = brace_match.group(1).split(" => ", 1)
        return (
            f"{path_text[:brace_match.start()]}"
            f"{new_part}"
            f"{path_text[brace_match.end():]}"
        )

    return path_text.split(" => ", 1)[1]


def parse_name_status(raw_name_status: str) -> dict[str, dict[str, str]]:
    parsed: dict[str, dict[str, str]] = {}
    for line in raw_name_status.splitlines():
        if not line.strip():
            continue
        fields = line.split("\t")
        if not fields:
            continue

        status_token = fields[0]
        status_code = status_token[0]
        change_type = STATUS_MAP.get(status_code, "modified")

        if status_code in {"R", "C"} and len(fields) >= 3:
            old_path = fields[1]
            new_path = fields[2]
            parsed[new_path] = {"change_type": change_type, "old_path": old_path}
            continue

        parsed[fields[-1]] = {"change_type": change_type}
    return parsed


def parse_numstat(raw_numstat: str) -> dict[str, dict[str, int]]:
    parsed: dict[str, dict[str, int]] = {}
    for line in raw_numstat.splitlines():
        if not line.strip():
            continue
        fields = line.split("\t")
        if len(fields) < 3:
            continue

        additions_raw, deletions_raw, path_raw = fields[:3]
        path = normalize_rename_path(path_raw)
        additions = int(additions_raw) if additions_raw.isdigit() else 0
        deletions = int(deletions_raw) if deletions_raw.isdigit() else 0
        parsed[path] = {"additions": additions, "deletions": deletions}
    return parsed


def clean_patch_line(line: str) -> str:
    text = line.rstrip("\r\n")
    if not text.strip():
        return ""
    return text


def empty_patch_info() -> dict[str, list[str]]:
    return {
        "hunks": [],
        "line_ranges": [],
        "added_examples": [],
        "removed_examples": [],
        "patch_notes": [],
    }


def format_hunk_range(start: int, count: int) -> str:
    if count <= 0:
        return f"{start}-{start} (0 lines)"
    end = start + count - 1
    if end == start:
        return f"{start}"
    return f"{start}-{end}"


def parse_patch(raw_patch: str) -> dict[str, dict[str, list[str]]]:
    parsed: dict[str, dict[str, list[str]]] = {}
    current_path = ""

    for line in raw_patch.splitlines():
        if line.startswith("diff --git "):
            match = re.match(r"diff --git a/(.+) b/(.+)$", line)
            if not match:
                current_path = ""
                continue
            old_path, new_path = match.group(1), match.group(2)
            current_path = new_path if new_path != "/dev/null" else old_path
            parsed.setdefault(current_path, empty_patch_info())
            continue

        if not current_path:
            continue

        if line.startswith("@@"):
            hunk_match = HUNK_RE.match(line)
            if hunk_match:
                old_start = int(hunk_match.group("old_start"))
                new_start = int(hunk_match.group("new_start"))
                old_count = int(hunk_match.group("old_count") or "1")
                new_count = int(hunk_match.group("new_count") or "1")
                context = hunk_match.group("context") or ""

                hunk_point = f"-{old_start} +{new_start}"
                if context.strip():
                    hunk_point += f" {context.strip()}"

                line_range = (
                    f"old {format_hunk_range(old_start, old_count)} "
                    f"-> new {format_hunk_range(new_start, new_count)}"
                )
                if hunk_point in parsed[current_path]["hunks"]:
                    pass
                elif append_with_limit(
                    parsed[current_path]["hunks"],
                    hunk_point,
                    MAX_HUNKS_PER_FILE,
                ):
                    append_unique(
                        parsed[current_path]["patch_notes"],
                        f"hunk points truncated after {MAX_HUNKS_PER_FILE} entries",
                        5,
                    )

                if line_range in parsed[current_path]["line_ranges"]:
                    pass
                elif append_with_limit(
                    parsed[current_path]["line_ranges"],
                    line_range,
                    MAX_HUNKS_PER_FILE,
                ):
                    append_unique(
                        parsed[current_path]["patch_notes"],
                        f"line_ranges truncated after {MAX_HUNKS_PER_FILE} entries",
                        5,
                    )
            else:
                if append_with_limit(
                    parsed[current_path]["hunks"],
                    line.strip(),
                    MAX_HUNKS_PER_FILE,
                ):
                    append_unique(
                        parsed[current_path]["patch_notes"],
                        f"hunk points truncated after {MAX_HUNKS_PER_FILE} entries",
                        5,
                    )
            continue

        if line.startswith("+++") or line.startswith("---"):
            continue

        if line.startswith("+"):
            if append_with_limit(
                parsed[current_path]["added_examples"],
                clean_patch_line(line[1:]),
                MAX_CHANGED_LINES_PER_FILE,
            ):
                append_unique(
                    parsed[current_path]["patch_notes"],
                    f"added_lines truncated after {MAX_CHANGED_LINES_PER_FILE} entries",
                    5,
                )
            continue

        if line.startswith("-"):
            if append_with_limit(
                parsed[current_path]["removed_examples"],
                clean_patch_line(line[1:]),
                MAX_CHANGED_LINES_PER_FILE,
            ):
                append_unique(
                    parsed[current_path]["patch_notes"],
                    f"removed_lines truncated after {MAX_CHANGED_LINES_PER_FILE} entries",
                    5,
                )

    return parsed


def infer_file_type(path: str) -> str:
    extension = Path(path).suffix.lower()
    lowered_path = path.lower()

    if lowered_path.startswith("jobs/") or extension in {".job", ".flow"}:
        return "schedule"
    if extension == ".sql":
        return "sql"
    if extension in {".properties", ".yaml", ".yml", ".conf", ".cfg", ".json", ".xml", ".ini"}:
        return "config"
    if extension in {".py", ".sh", ".java", ".scala", ".kt", ".js", ".ts", ".rb", ".go"}:
        return "script"
    if extension in {".md", ".txt", ".rst"}:
        return "docs"
    return "other"


def infer_domain_tags(path: str, subject: str, change_points: list[str]) -> list[str]:
    haystack = " ".join([path, subject, *change_points]).lower()
    tags: list[str] = []

    if any(keyword in haystack for keyword in AML_KEYWORDS):
        tags.append("aml")
    if path.lower().startswith("jobs/"):
        tags.append("scheduler")
    if path.lower().endswith(".sql"):
        tags.append("warehouse")

    return tags


def build_change_points(file_patch: dict[str, list[str]]) -> list[str]:
    def shorten_for_summary(text: str, limit: int = 120) -> str:
        if len(text) <= limit:
            return text
        return text[: limit - 3] + "..."

    points: list[str] = []
    for hunk in file_patch.get("hunks", [])[:2]:
        points.append(f"hunk {hunk}")
    for added in file_patch.get("added_examples", [])[:2]:
        points.append(f"+ {shorten_for_summary(added)}")
    for removed in file_patch.get("removed_examples", [])[:1]:
        points.append(f"- {shorten_for_summary(removed)}")
    return points[:5]


def build_summary(
    file_type: str,
    change_type: str,
    additions: int,
    deletions: int,
    change_points: list[str],
) -> str:
    action_text = SUMMARY_ACTION.get(change_type, "changed")
    summary = f"{file_type} file {action_text}; +{additions}/-{deletions} lines"
    if change_points:
        summary += f"; key points: {' ; '.join(change_points[:2])}"
    return summary


def normalize_git_error(stderr_bytes: bytes) -> str:
    text = stderr_bytes.decode("utf-8", errors="replace").strip()
    if text.lower().startswith("fatal: "):
        return text[7:]
    return text or "unknown git error"


def read_file_content_from_git(
    repo_path: str,
    revision: str,
    file_path: str,
) -> tuple[str | None, str | None]:
    completed = run_git_bytes(repo_path, ["show", f"{revision}:{file_path}"])
    if completed.returncode != 0:
        return None, normalize_git_error(completed.stderr)

    raw_content = completed.stdout
    if b"\x00" in raw_content:
        return None, "binary content omitted"

    try:
        return raw_content.decode("utf-8"), None
    except UnicodeDecodeError:
        return raw_content.decode("utf-8", errors="replace"), (
            "content decoded as utf-8 with replacement characters"
        )


def extract_file_content(
    repo_path: str,
    commit_id: str,
    path: str,
    change_type: str,
    old_path: str | None,
    content_size_limit: int,
) -> dict[str, Any]:
    file_content: dict[str, Any] = {}
    notes: list[str] = []
    parent_revision = f"{commit_id}^"
    before_path = old_path or path

    needs_before = change_type in {"modified", "deleted", "renamed", "copied", "type_changed", "unmerged"}
    needs_after = change_type in {"added", "modified", "renamed", "copied", "type_changed", "unmerged"}

    before_content: str | None = None
    before_note: str | None = None
    after_content: str | None = None
    after_note: str | None = None

    if needs_before:
        before_content, before_note = read_file_content_from_git(repo_path, parent_revision, before_path)
    if needs_after:
        after_content, after_note = read_file_content_from_git(repo_path, commit_id, path)

    content_too_large = bool(
        content_size_limit > 0
        and (
            (before_content is not None and len(before_content) > content_size_limit)
            or (after_content is not None and len(after_content) > content_size_limit)
        )
    )
    if content_too_large:
        notes.append("diff内容过长不展示")
    else:
        if before_content is not None:
            file_content["content_before"] = before_content
            if before_note:
                notes.append(f"content_before note: {before_note}")
        elif before_note:
            notes.append(f"content_before unavailable: {before_note}")

        if after_content is not None:
            file_content["content_after"] = after_content
            if after_note:
                notes.append(f"content_after note: {after_note}")
        elif after_note:
            notes.append(f"content_after unavailable: {after_note}")

    if notes:
        file_content["content_notes"] = notes

    return file_content


def select_content_target_paths(
    file_paths: list[str],
    name_status: dict[str, dict[str, str]],
    numstat: dict[str, dict[str, int]],
    max_files_with_content: int,
) -> set[str]:
    if max_files_with_content <= 0 or len(file_paths) <= max_files_with_content:
        return set(file_paths)

    def score(path: str) -> tuple[int, int, str]:
        change_type = name_status.get(path, {}).get("change_type", "modified")
        additions = int(numstat.get(path, {}).get("additions", 0))
        deletions = int(numstat.get(path, {}).get("deletions", 0))
        churn = additions + deletions
        priority = 0 if change_type in {"modified", "renamed", "copied", "type_changed"} else 1
        return priority, -churn, path

    ranked = sorted(file_paths, key=score)
    return set(ranked[:max_files_with_content])


def normalize_file_selector(value: str) -> str:
    return value.strip().replace("\\", "/").lstrip("./")


def strip_wrapping_quotes(value: str) -> str:
    text = value.strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in {'"', "'"}:
        return text[1:-1]
    return text


def load_file_selectors_from_text_file(file_path: Path) -> list[str]:
    try:
        content = file_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise RuntimeError(f"failed to read include-file-name list file: {file_path}") from exc

    selectors: list[str] = []
    for raw_line in content.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        selectors.append(line)
    return selectors


def build_file_name_selector_set(selectors: list[str]) -> set[str]:
    normalized_values: set[str] = set()
    for selector in selectors:
        raw_selector = selector.strip()
        if not raw_selector:
            continue

        selector_path = Path(raw_selector).expanduser()
        if selector_path.suffix.lower() == ".txt" and selector_path.exists() and selector_path.is_file():
            for file_selector in load_file_selectors_from_text_file(selector_path):
                normalized = normalize_file_selector(file_selector)
                if normalized:
                    normalized_values.add(normalized)
            continue

        normalized = normalize_file_selector(raw_selector)
        if normalized:
            normalized_values.add(normalized)
    return normalized_values


def should_keep_file(
    path: str,
    include_job_files: bool,
    include_file_names: set[str],
) -> bool:
    if not include_job_files and not include_file_names:
        return True

    normalized_path = normalize_file_selector(strip_wrapping_quotes(path))
    base_name = normalized_path.rsplit("/", 1)[-1]

    if include_job_files and normalized_path.lower().endswith(".job"):
        return True
    if normalized_path in include_file_names:
        return True
    if base_name in include_file_names:
        return True
    for selector in include_file_names:
        if "/" not in selector:
            continue
        if normalized_path.endswith(f"/{selector}") or selector.endswith(f"/{normalized_path}"):
            return True

    return False


def parse_commit_details(
    repo_path: str,
    commit_id: str,
    subject: str,
    include_file_content: bool,
    max_files_with_content: int,
    content_size_limit: int,
    include_job_files: bool,
    include_file_names: set[str],
) -> list[dict[str, Any]]:
    raw_name_status = run_git(
        repo_path,
        ["show", "--format=", "--name-status", "--find-renames", "--find-copies", commit_id],
    )
    raw_numstat = run_git(
        repo_path,
        ["show", "--format=", "--numstat", "--find-renames", "--find-copies", commit_id],
    )
    raw_patch = run_git(
        repo_path,
        ["show", "--format=", "--unified=0", "--no-color", "--find-renames", "--find-copies", commit_id],
    )

    name_status = parse_name_status(raw_name_status)
    numstat = parse_numstat(raw_numstat)
    patch = parse_patch(raw_patch)

    file_paths = sorted(set(name_status.keys()) | set(numstat.keys()) | set(patch.keys()))
    filtered_file_paths = [
        path
        for path in file_paths
        if should_keep_file(
            path=path,
            include_job_files=include_job_files,
            include_file_names=include_file_names,
        )
    ]

    content_target_paths: set[str] = set()
    if include_file_content:
        content_target_paths = select_content_target_paths(
            file_paths=filtered_file_paths,
            name_status=name_status,
            numstat=numstat,
            max_files_with_content=max_files_with_content,
        )

    files: list[dict[str, Any]] = []

    for path in filtered_file_paths:
        status_info = name_status.get(path, {})
        stats = numstat.get(path, {"additions": 0, "deletions": 0})
        patch_info = patch.get(path, empty_patch_info())

        change_type = status_info.get("change_type", "modified")
        additions = int(stats.get("additions", 0))
        deletions = int(stats.get("deletions", 0))
        file_type = infer_file_type(path)
        change_points = build_change_points(patch_info)
        domain_tags = infer_domain_tags(path, subject, change_points)
        summary = build_summary(file_type, change_type, additions, deletions, change_points)

        file_record: dict[str, Any] = {
            "path": path,
            "change_type": change_type,
            "file_type": file_type,
            "additions": additions,
            "deletions": deletions,
            "summary": summary,
        }

        old_path = status_info.get("old_path")
        if old_path:
            file_record["old_path"] = old_path
        if change_points:
            file_record["change_points"] = change_points
        if patch_info.get("line_ranges"):
            file_record["line_ranges"] = patch_info["line_ranges"]
        if patch_info.get("added_examples"):
            file_record["added_lines"] = patch_info["added_examples"]
        if patch_info.get("removed_examples"):
            file_record["removed_lines"] = patch_info["removed_examples"]
        if patch_info.get("patch_notes"):
            file_record["patch_notes"] = patch_info["patch_notes"]
        if domain_tags:
            file_record["domain_tags"] = domain_tags
        if include_file_content:
            if path in content_target_paths:
                file_record.update(
                    extract_file_content(
                        repo_path=repo_path,
                        commit_id=commit_id,
                        path=path,
                        change_type=change_type,
                        old_path=old_path,
                        content_size_limit=content_size_limit,
                    )
                )
            else:
                file_record["content_notes"] = [
                    "full content skipped due to --max-files-with-content limit"
                ]

        files.append(file_record)

    return files


def build_report(args: argparse.Namespace) -> dict[str, Any]:
    repo = Path(args.repo).expanduser().resolve()
    if not repo.exists():
        raise FileNotFoundError(f"repository path does not exist: {repo}")
    if args.max_count <= 0:
        raise ValueError("--max-count must be greater than 0")
    if args.max_files_with_content < 0:
        raise ValueError("--max-files-with-content must be greater than or equal to 0")
    if args.content_size_limit < 0:
        raise ValueError("--content-size-limit must be greater than or equal to 0")

    assert_date(args.since, "--since")
    assert_date(args.until, "--until")

    run_git(str(repo), ["rev-parse", "--is-inside-work-tree"])
    log_entries = parse_log_entries(run_git(str(repo), build_log_args(args)))
    authors = parse_author_selectors(args.author)
    include_file_names = build_file_name_selector_set(args.include_file_name)

    commits: list[dict[str, Any]] = []
    for commit in log_entries:
        changed_files = parse_commit_details(
            str(repo),
            commit["commit_id"],
            commit["subject"],
            args.include_file_content,
            args.max_files_with_content,
            args.content_size_limit,
            args.include_job_files,
            include_file_names,
        )
        if (args.include_job_files or include_file_names) and not changed_files:
            continue
        commit["changed_file_count"] = len(changed_files)
        commit["changed_files"] = changed_files
        commits.append(commit)

    return {
        "repo_path": str(repo),
        "filters": {
            "keywords": [value for value in args.keyword if value.strip()],
            "all_keywords": bool(args.all_keywords),
            "author": authors[0] if len(authors) == 1 else None,
            "authors": authors,
            "since": args.since,
            "until": args.until,
            "max_count": args.max_count,
            "branch": args.branch,
            "include_file_content": bool(args.include_file_content),
            "max_files_with_content": args.max_files_with_content,
            "content_size_limit": args.content_size_limit,
            "include_job_files": bool(args.include_job_files),
            "include_file_names": sorted(include_file_names),
        },
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_commits": len(commits),
        "commits": commits,
    }


def build_compact_report(report: dict[str, Any]) -> dict[str, Any]:
    compact_commits: list[dict[str, Any]] = []
    for commit in report.get("commits", []):
        compact_files: list[dict[str, Any]] = []
        for file_item in commit.get("changed_files", []):
            path = file_item.get("path")
            summary = file_item.get("summary")
            if not path:
                continue
            compact_item: dict[str, Any] = {
                "path": path,
                "summary": summary or "",
            }
            for optional_key in (
                "line_ranges",
                "added_lines",
                "removed_lines",
                "patch_notes",
                "content_before",
                "content_after",
                "content_notes",
            ):
                if optional_key in file_item:
                    compact_item[optional_key] = file_item[optional_key]
            compact_files.append(compact_item)

        compact_commits.append(
            {
                "commit_id": commit.get("commit_id"),
                "author": commit.get("author"),
                "date": commit.get("date"),
                "subject": commit.get("subject"),
                "changed_file_count": len(compact_files),
                "changed_files": compact_files,
            }
        )

    return {
        "repo_path": report.get("repo_path"),
        "filters": report.get("filters"),
        "generated_at": report.get("generated_at"),
        "total_commits": len(compact_commits),
        "commits": compact_commits,
    }


def write_output(report: dict[str, Any], output_path: str | None) -> None:
    rendered = json.dumps(report, ensure_ascii=False, indent=2) + "\n"
    if output_path == "-":
        print(rendered, end="")
        return

    output_file = Path(output_path).expanduser().resolve()
    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_text(rendered, encoding="utf-8")


def render_markdown(report: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# Commit Report")
    lines.append("")
    lines.append(f"- Repo: `{report.get('repo_path', '')}`")
    lines.append(f"- Generated At: `{report.get('generated_at', '')}`")
    lines.append(f"- Total Commits: `{report.get('total_commits', 0)}`")
    lines.append("")

    commits = report.get("commits", [])
    if not commits:
        lines.append("No commits matched.")
        lines.append("")
        return "\n".join(lines)

    for commit in commits:
        commit_id = str(commit.get("commit_id", ""))
        commit_short = commit_id[:8] if commit_id else "unknown"
        subject = str(commit.get("subject", ""))
        author = str(commit.get("author", ""))
        date = str(commit.get("date", ""))
        changed_files = commit.get("changed_files", [])

        lines.append(f"## `{commit_short}` {subject}")
        lines.append(f"- Commit ID: `{commit_id}`")
        lines.append(f"- Author: `{author}`")
        lines.append(f"- Date: `{date}`")
        lines.append(f"- Changed Files: `{len(changed_files)}`")
        lines.append("")

        for file_item in changed_files:
            path = str(file_item.get("path", ""))
            summary = str(file_item.get("summary", ""))
            lines.append(f"### `{path}`")
            lines.append(f"- Summary: {summary}")

            line_ranges = file_item.get("line_ranges")
            if line_ranges:
                lines.append("- Line Ranges:")
                for item in line_ranges:
                    lines.append(f"  - `{item}`")

            patch_notes = file_item.get("patch_notes")
            if patch_notes:
                lines.append("- Patch Notes:")
                for note in patch_notes:
                    lines.append(f"  - {note}")

            removed_lines = file_item.get("removed_lines")
            if removed_lines:
                lines.append("- Removed Lines:")
                lines.append("```diff")
                for removed in removed_lines:
                    lines.append(f"-{removed}")
                lines.append("```")

            added_lines = file_item.get("added_lines")
            if added_lines:
                lines.append("- Added Lines:")
                lines.append("```diff")
                for added in added_lines:
                    lines.append(f"+{added}")
                lines.append("```")

            content_notes = file_item.get("content_notes")
            if content_notes:
                lines.append("- Content Notes:")
                for note in content_notes:
                    lines.append(f"  - {note}")

            if "content_before" in file_item:
                lines.append("")
                lines.append("#### Content Before")
                lines.append("```text")
                lines.append(str(file_item["content_before"]))
                lines.append("```")

            if "content_after" in file_item:
                lines.append("")
                lines.append("#### Content After")
                lines.append("```text")
                lines.append(str(file_item["content_after"]))
                lines.append("```")

            lines.append("")

    return "\n".join(lines)


def resolve_markdown_output_path(json_output_path: str, markdown_output_path: str | None) -> str | None:
    if markdown_output_path:
        return markdown_output_path
    if json_output_path == "-":
        return None
    json_file = Path(json_output_path).expanduser().resolve()
    if json_file.suffix.lower() == ".json":
        return str(json_file.with_suffix(".md"))
    return str(json_file.parent / f"{json_file.name}.md")


def write_markdown(markdown_text: str, output_path: str | None) -> None:
    if output_path is None:
        return
    if output_path == "-":
        print(markdown_text, end="" if markdown_text.endswith("\n") else "\n")
        return

    output_file = Path(output_path).expanduser().resolve()
    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_text(markdown_text if markdown_text.endswith("\n") else markdown_text + "\n", encoding="utf-8")


def build_commit_overview(report: dict[str, Any]) -> dict[str, Any]:
    commits: list[dict[str, str]] = []
    for commit in report.get("commits", []):
        commit_id = str(commit.get("commit_id", "")).strip()
        if not commit_id:
            continue
        commits.append(
            {
                "commit_id": commit_id,
                "author": str(commit.get("author", "")),
                "date": str(commit.get("date", "")),
                "subject": str(commit.get("subject", "")),
            }
        )

    return {
        "repo_path": report.get("repo_path"),
        "generated_at": report.get("generated_at"),
        "total_commits": len(commits),
        "commits": commits,
    }


def render_commit_overview_markdown(overview: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# Commit ID Report")
    lines.append("")
    lines.append(f"- repo_path: `{overview.get('repo_path', '')}`")
    lines.append(f"- generated_at: `{overview.get('generated_at', '')}`")
    lines.append(f"- total_commits: `{overview.get('total_commits', 0)}`")
    lines.append("")
    lines.append("## Overview JSON")
    lines.append("```json")
    lines.append(json.dumps(overview, ensure_ascii=False, indent=2))
    lines.append("```")
    lines.append("")
    return "\n".join(lines)


def resolve_commit_id_output_path(json_output_path: str, commit_id_output_path: str | None) -> str | None:
    if commit_id_output_path:
        return commit_id_output_path
    if json_output_path == "-":
        return None

    json_file = Path(json_output_path).expanduser().resolve()
    return str(json_file.with_name(f"{json_file.stem}_commit_ids.md"))


def write_commit_overview(markdown_text: str, output_path: str | None) -> None:
    if output_path is None:
        return
    if output_path == "-":
        print(markdown_text, end="" if markdown_text.endswith("\n") else "\n")
        return

    output_file = Path(output_path).expanduser().resolve()
    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_text(markdown_text if markdown_text.endswith("\n") else markdown_text + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    try:
        detailed_report = build_report(args)
        compact_report = build_compact_report(detailed_report)
        write_output(compact_report, args.output)
        markdown_output_path = resolve_markdown_output_path(args.output, args.markdown_output)
        markdown_report = render_markdown(compact_report)
        write_markdown(markdown_report, markdown_output_path)
        commit_id_output_path = resolve_commit_id_output_path(args.output, args.commit_id_output)
        commit_overview = build_commit_overview(compact_report)
        commit_overview_markdown = render_commit_overview_markdown(commit_overview)
        write_commit_overview(commit_overview_markdown, commit_id_output_path)
    except Exception as exc:  # noqa: BLE001 - cli tool should display full reason
        print(f"[diff-parser] {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
