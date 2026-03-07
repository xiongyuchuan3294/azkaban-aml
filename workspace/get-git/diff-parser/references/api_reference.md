# Diff Parser CLI Reference

## Command

```bash
python diff-parser/scripts/parse_git_commits.py [options]
```

## Parameters

- `--repo <path>`: Target Git repository path. Default: `/Users/xiongyuc/workspace/azkaban-aml`.
- `--keyword <text>`: Commit message keyword filter. Repeat this option for multiple keywords.
- `--all-keywords`: Require all provided keywords to match one commit message. Default behavior is OR.
- `--author <text>`: Commit author filter (same semantics as `git log --author`). Repeat for multiple authors, or use comma-separated values (for example: `--author "xiongyuc,author2"`).
- `--since <YYYY-MM-DD>`: Start date filter.
- `--until <YYYY-MM-DD>`: End date filter.
- `--max-count <int>`: Max number of commits to parse. Default: `50`.
- `--branch <rev>`: Revision range or branch to scan. Default: `HEAD`.
- `--output <path>`: Write compact JSON output file. Default: `diff-parser/outputs/commit_report.json`. Use `--output -` to print to stdout.
- `--markdown-output <path>`: Write Markdown report file. If omitted and `--output` is a file path, a sibling `.md` file is generated automatically.
- `--commit-id-output <path>`: Write filtered commit overview to Markdown file. If omitted and `--output` is a file path, a sibling `*_commit_ids.md` file is generated automatically.
- `--include-file-content`: Include full changed file content in output (`content_before`/`content_after` when available).
- `--max-files-with-content <int>`: Limit how many files per commit include full content when `--include-file-content` is enabled. `0` means no limit. Default: `0`.
- `--content-size-limit <int>`: When `--include-file-content` is enabled, if `content_before` or `content_after` exceeds this character count, content is omitted and `content_notes` includes `diff内容过长不展示`. `0` means no limit. Default: `200`.
- `--include-job-files`: Only include changed files with `.job` suffix. Can be combined with `--include-file-name`.
- `--include-file-name [text]`: Include changed files by basename, full relative path, or full absolute path. Repeat for multiple file names. If provided without a value, defaults to `diff-parser/focus_file.txt`. If a value is an existing `.txt` file path, the script reads selectors from that file (one file name per line; blank lines and `#` comments are ignored).
  - Template file available: `diff-parser/focus_file.txt`.
When `--include-job-files` or `--include-file-name` is used, commits with zero matched files are omitted from output.

## Output JSON Schema

Top-level fields:

- `repo_path`: Absolute repository path used for parsing.
- `filters`: Effective filter inputs used for this report (keywords, authors, date range, branch, and extra options).
- `generated_at`: UTC timestamp (ISO-8601).
- `total_commits`: Number of commits in output.
- `commits`: Parsed commit list.

Commit fields:

- `commit_id`: Full commit SHA.
- `author`: Commit author name (from git log).
- `date`: Commit date (from git log).
- `subject`: Commit subject line.
- `changed_file_count`: Number of changed files.
- `changed_files`: File-level details.

Changed-file fields:

- `path`: File path.
- `summary`: Human-readable file change summary.
- `line_ranges` (optional): Hunk-level line ranges, format like `old 12-14 -> new 12-13`.
- `added_lines` (optional): Specific added lines from patch (up to limit).
- `removed_lines` (optional): Specific removed lines from patch (up to limit).
- `patch_notes` (optional): Patch extraction/truncation notes (for example, line-level arrays truncated due to limits).
- `content_before` (optional): Full file text from parent revision (`commit^`) for modified/deleted/renamed/copied/type_changed files.
- `content_after` (optional): Full file text from target commit for added/modified/renamed/copied/type_changed files.
- `content_notes` (optional): Notes for content extraction behavior (e.g., binary omitted, unavailable, `diff内容过长不展示`, or skipped due to max-files limit).

## Output Markdown Structure

- Header metadata: repo path, generated time, total commits.
- Per-commit sections: `commit_id`, `author`, `subject`, `date`, changed file count.
- Per-file sections: `path`, `summary`, optional `line_ranges`, optional `added_lines`/`removed_lines`, optional `content_notes`, optional `content_before`/`content_after` code blocks.

## Output Commit-ID File

- Markdown file.
- Includes `repo_path`, `generated_at`, `total_commits`.
- Includes an `Overview JSON` block with:
  - `repo_path`
  - `generated_at`
  - `total_commits`
  - `commits` (each item only has `commit_id`, `author`, `date`, `subject`)
- Does not include `changed_files` or `changed_file_count`.

## Example Input

```bash
python diff-parser/scripts/parse_git_commits.py \
  --repo /Users/xiongyuc/workspace/azkaban-aml \
  --keyword aml \
  --author "xiongyuc" \
  --author "author2" \
  --since 2026-02-01 \
  --until 2026-02-21 \
  --max-count 20 \
  --include-file-content \
  --max-files-with-content 20 \
  --include-job-files \
  --include-file-name
```
`--markdown-output` and `--commit-id-output` are optional only for custom output paths.

## Example Output

```json
{
  "repo_path": "/Users/xiongyuc/workspace/azkaban-aml",
  "generated_at": "2026-02-21T02:20:15.447403+00:00",
  "total_commits": 1,
  "commits": [
    {
      "commit_id": "a369ec8b1d7e1f43b0dba38f87f40ce2e0dbe123",
      "author": "xiongyuc",
      "date": "2026-02-19T22:04:01+08:00",
      "subject": "aml-demo",
      "changed_file_count": 2,
      "changed_files": [
        {
          "path": "jobs/aml_daily_flow.job",
          "summary": "schedule file modified; +3/-1 lines; key points: hunk -12 +12 command=python scripts/run_rule_engine.py ; + retries=2",
          "content_before": "type=command\\ncommand=python scripts/run_rule_engine.py\\nretries=1\\n",
          "content_after": "type=command\\ncommand=python scripts/run_rule_engine.py\\nretries=2\\n"
        }
      ]
    }
  ]
}
```
