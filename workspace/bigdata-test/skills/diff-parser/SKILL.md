---
name: diff-parser
description: Filter and parse git commits by message keywords, author, and date range, then extract changed files and concise per-file change summaries from diffs for AML and big-data testing impact analysis. Use when asked to analyze commit history in /Users/xiongyuc/workspace/azkaban-aml or similar repositories.
---

# Diff Parser

Use `scripts/parse_git_commits.py` to produce structured JSON from Git history.

## Execution Workflow

1. Confirm repository path. Use default `/Users/xiongyuc/workspace/azkaban-aml` when the user does not provide one.
2. Collect filter conditions from the request: `--keyword` (repeatable), `--author`, and `--since`/`--until` in `YYYY-MM-DD`.
3. Run parser script and write JSON output. By default, output goes to `skills/diff-parser/outputs/commit_report.json`.
```bash
python skills/diff-parser/scripts/parse_git_commits.py \
  --repo /Users/xiongyuc/workspace/azkaban-aml \
  --keyword aml \
  --author "xiongyuc" \
  --since 2026-01-01 \
  --until 2026-02-21
```
4. Read `commits[].changed_files[]` as impact-analysis payload and map `path` and `summary` into AML test scenarios.

## Output Contract

- Always output valid JSON.
- Include `repo_path`, `generated_at`, `total_commits`, and `commits`.
- Return empty list when no commit matches (`total_commits=0`, `commits=[]`).
- Exit with non-zero code and clear stderr message when git parsing fails.

## Validation Checklist

- Ensure each commit includes `commit_id`, `date`, `subject`, and `changed_files`.
- Ensure each changed file includes `path` and `summary`.
- Ensure the same inputs produce deterministic JSON ordering.

## References To Load On Demand

- Load `references/api_reference.md` for complete CLI parameters, JSON schema, and example input/output.
