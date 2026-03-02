# Diff Parser CLI Reference

## Command

```bash
python skills/diff-parser/scripts/parse_git_commits.py [options]
```

## Parameters

- `--repo <path>`: Target Git repository path. Default: `/Users/xiongyuc/workspace/azkaban-aml`.
- `--keyword <text>`: Commit message keyword filter. Repeat this option for multiple keywords.
- `--all-keywords`: Require all provided keywords to match one commit message. Default behavior is OR.
- `--author <text>`: Commit author filter (same semantics as `git log --author`).
- `--since <YYYY-MM-DD>`: Start date filter.
- `--until <YYYY-MM-DD>`: End date filter.
- `--max-count <int>`: Max number of commits to parse. Default: `50`.
- `--branch <rev>`: Revision range or branch to scan. Default: `HEAD`.
- `--output <path>`: Write compact JSON output file. Default: `skills/diff-parser/outputs/commit_report.json`. Use `--output -` to print to stdout.

## Output JSON Schema

Top-level fields:

- `repo_path`: Absolute repository path used for parsing.
- `generated_at`: UTC timestamp (ISO-8601).
- `total_commits`: Number of commits in output.
- `commits`: Parsed commit list.

Commit fields:

- `commit_id`: Full commit SHA.
- `date`: Commit date (from git log).
- `subject`: Commit subject line.
- `changed_file_count`: Number of changed files.
- `changed_files`: File-level details.

Changed-file fields:

- `path`: File path.
- `summary`: Human-readable file change summary.

## Example Input

```bash
python skills/diff-parser/scripts/parse_git_commits.py \
  --repo /Users/xiongyuc/workspace/azkaban-aml \
  --keyword aml \
  --author "xiongyuc" \
  --since 2026-02-01 \
  --until 2026-02-21 \
  --max-count 20
```

## Example Output

```json
{
  "repo_path": "/Users/xiongyuc/workspace/azkaban-aml",
  "generated_at": "2026-02-21T02:20:15.447403+00:00",
  "total_commits": 1,
  "commits": [
    {
      "commit_id": "a369ec8b1d7e1f43b0dba38f87f40ce2e0dbe123",
      "date": "2026-02-19T22:04:01+08:00",
      "subject": "aml-demo",
      "changed_file_count": 2,
      "changed_files": [
        {
          "path": "jobs/aml_daily_flow.job",
          "summary": "schedule file modified; +3/-1 lines; key points: hunk -12 +12 command=python scripts/run_rule_engine.py ; + retries=2"
        }
      ]
    }
  ]
}
```
