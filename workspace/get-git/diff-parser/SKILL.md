---
name: diff-parser
description: Filter and parse git commits by message keywords, author, and date range, then extract changed files and concise per-file change summaries from diffs for AML and big-data testing impact analysis. Use when asked to analyze commit history in /Users/xiongyuc/workspace/azkaban-aml or similar repositories.
---

# Diff Parser

Use `scripts/parse_git_commits.py` to produce structured JSON from Git history.

## Execution Workflow

1. Confirm repository path. Use default `/Users/xiongyuc/workspace/azkaban-aml` when the user does not provide one.
2. Collect filter conditions from the request: `--keyword` (repeatable), `--author` (repeatable), and `--since`/`--until` in `YYYY-MM-DD`.
3. Run parser script. By default it writes both:
   - JSON: `diff-parser/outputs/commit_report.json`
   - Markdown: `diff-parser/outputs/commit_report.md`
   - Commit IDs (Markdown): `diff-parser/outputs/commit_report_commit_ids.md`
```bash
python diff-parser/scripts/parse_git_commits.py \
  --repo /Users/xiongyuc/workspace/azkaban-aml \
  --keyword aml \
  --author "xiongyuc" \
  --author "author2" \
  --since 2026-01-01 \
  --until 2026-02-21
```
`--markdown-output` and `--commit-id-output` are optional only when you need custom output paths.
4. When the user needs full file context for LLM input, add:
```bash
  --include-file-content \
  --max-files-with-content 20
```
`--content-size-limit` default is `200`. When content exceeds this limit, `content_before/content_after` are omitted and `content_notes` includes `diff内容过长不展示`.
5. When the user only wants scheduler nodes and specific script files, add:
```bash
  --include-job-files \
  --include-file-name
```
`--include-file-name` supports multiple values and matches either full relative path or basename.
If provided without a value, it automatically loads selectors from `diff-parser/focus_file.txt`.
If the value is an existing `.txt` file path, each non-empty line in the file is treated as one selector.
You can use the template file: `diff-parser/focus_file.txt`.
Selectors in `focus_file.txt` support basename, full relative path, and full absolute path.
6. Read `commits[].changed_files[]` as impact-analysis payload and map `path` and `summary` into AML test scenarios.

## Output Contract

- Always output valid JSON.
- Also write a Markdown report (default sibling `.md` file for the JSON output path).
- Also write a commit-id Markdown file (default sibling `*_commit_ids.md` file for the JSON output path).
- Include `repo_path`, `filters`, `generated_at`, `total_commits`, and `commits`.
- Ensure each commit includes `commit_id`, `author`, `date`, `subject`, and `changed_files`.
- Return empty list when no commit matches (`total_commits=0`, `commits=[]`).
- Exit with non-zero code and clear stderr message when git parsing fails.
- With `--include-file-content`, changed files may include:
  - `content_before`
  - `content_after`
  - `content_notes` (e.g., skipped by limit, binary content omitted, diff内容过长不展示)
- Each changed file includes line-level diff context for LLM analysis:
  - `line_ranges` (old/new line ranges by hunk)
  - `added_lines` (specific added lines)
  - `removed_lines` (specific removed lines)
  - `patch_notes` (line-level data truncation hints when limits are reached)
- With `--include-job-files` / `--include-file-name`, `changed_files` only contains matching files.
- With `--include-job-files` / `--include-file-name`, commits with no matched files are removed from output.

## Validation Checklist

- Ensure output contains `filters` so parsing conditions are traceable.
- Ensure each changed file includes `path` and `summary`.
- Ensure each changed file includes `line_ranges`, `added_lines`, and `removed_lines` when patch data exists.
- If `--include-file-content` is enabled, verify `content_before/content_after` only appear when available.
- Ensure commit-id Markdown output includes `repo_path`, `generated_at`, `total_commits`, and `commits`.
- Ensure each `commits[]` item in commit-id Markdown includes only `commit_id`, `author`, `date`, and `subject`.
- Ensure the same inputs produce deterministic JSON ordering.

## References To Load On Demand

- Load `references/api_reference.md` for complete CLI parameters, JSON schema, and example input/output.
