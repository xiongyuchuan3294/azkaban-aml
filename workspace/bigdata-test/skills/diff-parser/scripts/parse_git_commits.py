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

HUNK_RE = re.compile(r"^@@ -(\d+)(?:,\d+)? \+(\d+)(?:,\d+)? @@ ?(.*)$")

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
    parser.add_argument("--author", help="Commit author filter.")
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
            "Default: skills/diff-parser/outputs/commit_report.json. "
            "Use '-' to print to stdout."
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
    command = ["git", "-C", repo_path, *git_args]
    completed = subprocess.run(command, capture_output=True, text=True, check=False)
    if completed.returncode != 0:
        stderr = completed.stderr.strip() or completed.stdout.strip()
        raise RuntimeError(f"git command failed: {' '.join(command)}\n{stderr}")
    return completed.stdout


def build_log_args(args: argparse.Namespace) -> list[str]:
    cmd = [
        "log",
        "--date=iso-strict",
        f"--max-count={args.max_count}",
        f"--pretty=format:%H{FIELD_DELIMITER}%an{FIELD_DELIMITER}%ae"
        f"{FIELD_DELIMITER}%ad{FIELD_DELIMITER}%s{ENTRY_DELIMITER}",
    ]

    if args.author:
        cmd.append(f"--author={args.author}")
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
    text = line.strip()
    if not text:
        return ""
    if len(text) > 120:
        return text[:117] + "..."
    return text


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
            parsed.setdefault(
                current_path,
                {"hunks": [], "added_examples": [], "removed_examples": []},
            )
            continue

        if not current_path:
            continue

        if line.startswith("@@"):
            hunk_match = HUNK_RE.match(line)
            if hunk_match:
                old_start, new_start, context = hunk_match.groups()
                hunk_point = f"-{old_start} +{new_start}"
                if context.strip():
                    hunk_point += f" {context.strip()}"
                append_unique(parsed[current_path]["hunks"], hunk_point, 4)
            else:
                append_unique(parsed[current_path]["hunks"], line.strip(), 4)
            continue

        if line.startswith("+++") or line.startswith("---"):
            continue

        if line.startswith("+"):
            append_unique(
                parsed[current_path]["added_examples"],
                clean_patch_line(line[1:]),
                3,
            )
            continue

        if line.startswith("-"):
            append_unique(
                parsed[current_path]["removed_examples"],
                clean_patch_line(line[1:]),
                3,
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
    points: list[str] = []
    for hunk in file_patch.get("hunks", [])[:2]:
        points.append(f"hunk {hunk}")
    for added in file_patch.get("added_examples", [])[:2]:
        points.append(f"+ {added}")
    for removed in file_patch.get("removed_examples", [])[:1]:
        points.append(f"- {removed}")
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


def parse_commit_details(repo_path: str, commit_id: str, subject: str) -> list[dict[str, Any]]:
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
    files: list[dict[str, Any]] = []

    for path in file_paths:
        status_info = name_status.get(path, {})
        stats = numstat.get(path, {"additions": 0, "deletions": 0})
        patch_info = patch.get(path, {"hunks": [], "added_examples": [], "removed_examples": []})

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
        if domain_tags:
            file_record["domain_tags"] = domain_tags

        files.append(file_record)

    return files


def build_report(args: argparse.Namespace) -> dict[str, Any]:
    repo = Path(args.repo).expanduser().resolve()
    if not repo.exists():
        raise FileNotFoundError(f"repository path does not exist: {repo}")
    if args.max_count <= 0:
        raise ValueError("--max-count must be greater than 0")

    assert_date(args.since, "--since")
    assert_date(args.until, "--until")

    run_git(str(repo), ["rev-parse", "--is-inside-work-tree"])
    log_entries = parse_log_entries(run_git(str(repo), build_log_args(args)))

    commits: list[dict[str, Any]] = []
    for commit in log_entries:
        changed_files = parse_commit_details(str(repo), commit["commit_id"], commit["subject"])
        commit["changed_file_count"] = len(changed_files)
        commit["changed_files"] = changed_files
        commits.append(commit)

    return {
        "repo_path": str(repo),
        "filters": {
            "keywords": [value for value in args.keyword if value.strip()],
            "all_keywords": bool(args.all_keywords),
            "author": args.author,
            "since": args.since,
            "until": args.until,
            "max_count": args.max_count,
            "branch": args.branch,
        },
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_commits": len(commits),
        "commits": commits,
    }


def build_compact_report(report: dict[str, Any]) -> dict[str, Any]:
    compact_commits: list[dict[str, Any]] = []
    for commit in report.get("commits", []):
        compact_files: list[dict[str, str]] = []
        for file_item in commit.get("changed_files", []):
            path = file_item.get("path")
            summary = file_item.get("summary")
            if not path:
                continue
            compact_files.append(
                {
                    "path": path,
                    "summary": summary or "",
                }
            )

        compact_commits.append(
            {
                "commit_id": commit.get("commit_id"),
                "date": commit.get("date"),
                "subject": commit.get("subject"),
                "changed_file_count": len(compact_files),
                "changed_files": compact_files,
            }
        )

    return {
        "repo_path": report.get("repo_path"),
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


def main() -> int:
    args = parse_args()
    try:
        detailed_report = build_report(args)
        compact_report = build_compact_report(detailed_report)
        write_output(compact_report, args.output)
    except Exception as exc:  # noqa: BLE001 - cli tool should display full reason
        print(f"[diff-parser] {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
